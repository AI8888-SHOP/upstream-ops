// 数据面：流式转发（缓冲 / 增量转换）。
package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// buildUpstreamHTTPRequest 构建上游 HTTP 请求。
func (rt *Runtime) buildUpstreamHTTPRequest(
	ctx context.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	kind protocolKind,
	stream bool,
) (*http.Request, error) {
	if target == nil {
		return nil, errors.New("upstream target is nil")
	}
	base := strings.TrimRight(target.BaseURL, "/")
	apiKey := target.APIKey
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	fullURL := base + path

	upKind := protocol.NormalizeKind(kind)
	if stream && (upKind == protocol.KindOpenAIChat || upKind == protocol.KindOpenAI) {
		body = EnsureStreamUsageOption(body, true)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for _, h := range []string{"Content-Type", "Accept", "User-Agent"} {
		if v := inHeader.Get(h); v != "" {
			req.Header.Set(h, v)
		}
	}
	// 请求相关 ID 原样转发上游；网关只在响应侧写 X-Upstream-Ops-Request-Id
	rt.copyClientRequestIDHeaders(req.Header, inHeader)
	if upKind == protocol.KindAnthropic {
		if v := inHeader.Get("anthropic-version"); v != "" {
			req.Header.Set("anthropic-version", v)
		}
		if v := inHeader.Get("anthropic-beta"); v != "" {
			req.Header.Set("anthropic-beta", v)
		}
	} else {
		if v := inHeader.Get("OpenAI-Beta"); v != "" {
			req.Header.Set("OpenAI-Beta", v)
		}
	}
	if req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	authStyle := storage.GatewayProviderAuthBoth
	if target.Provider != nil {
		authStyle = rt.normalizeProviderAuthStyle(target.Provider.AuthStyle)
	}
	switch authStyle {
	case storage.GatewayProviderAuthBearer:
		req.Header.Set("Authorization", "Bearer "+apiKey)
	case storage.GatewayProviderAuthXAPIKey:
		req.Header.Set("x-api-key", apiKey)
	default:
		req.Header.Set("Authorization", "Bearer "+apiKey)
		if upKind == protocol.KindAnthropic {
			req.Header.Set("x-api-key", apiKey)
		}
	}
	if upKind == protocol.KindAnthropic {
		if req.Header.Get("anthropic-version") == "" {
			req.Header.Set("anthropic-version", "2023-06-01")
		}
	}
	if target.Provider != nil && strings.TrimSpace(target.Provider.ExtraHeadersJSON) != "" {
		var extra map[string]string
		if json.Unmarshal([]byte(target.Provider.ExtraHeadersJSON), &extra) == nil {
			for hk, hv := range extra {
				if strings.TrimSpace(hk) != "" && strings.TrimSpace(hv) != "" {
					req.Header.Set(hk, hv)
				}
			}
		}
	}
	// 组+路由 UA 策略：非空则覆盖客户端/ExtraHeaders 中的 UA；空则保持透传。
	// 转发 / 模型测试 / 拉模型共用 resolve（见 applyRouteUserAgent）。
	if ua := strings.TrimSpace(target.UserAgentOverride); ua != "" {
		req.Header.Set("User-Agent", ua)
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}
	return req, nil
}

// errClientDisconnected 客户端主动断开/取消；已 commit 时仍会 drain 上游以同步 usage。
var errClientDisconnected = errors.New("client disconnected")

// forwardStream 真流式转发：边读上游 SSE 边写客户端。
// 首字超时从发起上游请求起算（含等响应头），与 usage.first_token_ms 同一时钟。
//
// 上游请求不绑定客户端 context：客户端中途断开后继续读完上游 SSE，
// 尽量拿到完整 usage/计费（对齐 sub2api “continue draining for billing”）。

func (rt *Runtime) forwardStream(
	ctx context.Context,
	c *gin.Context,
	target *upstreamTarget,
	path string,
	method string,
	inHeader http.Header,
	body []byte,
	inboundKind protocolKind,
	upstreamKind protocolKind,
	model string,
	converted bool,
	firstTokenTimeout time.Duration,
) streamAttemptResult {
	release, err := rt.acquireUpstreamConcurrency(ctx, target)
	if err != nil {
		return streamAttemptResult{Err: err}
	}
	defer release()

	clientCtx := context.Background()
	if c != nil && c.Request != nil && c.Request.Context() != nil {
		clientCtx = c.Request.Context()
	}
	// 剥离客户端取消：否则 client disconnect 会连带取消 upstream body，usage 永远为 0
	upBase := context.Background()
	if ctx != nil {
		// Normal requests deliberately drain after client disconnect. Hedge
		// attempts opt in to a preserved, attempt-local cancel so losers stop
		// promptly before their buffered prefix can grow.
		if preserveAttemptCancel(ctx) {
			upBase = ctx
		} else {
			upBase = context.WithoutCancel(ctx)
		}
	}
	gwCfg := rt.gatewayRuntime()
	upCtx, upCancel := context.WithTimeout(upBase, gwCfg.ForwardTimeout())
	defer upCancel()

	// 可取消：仅首字超时 / 未 commit 的客户端断开时 abort
	reqCtx, abortReq := context.WithCancel(upCtx)
	defer abortReq()

	req, err := rt.buildUpstreamHTTPRequest(reqCtx, target, path, method, inHeader, body, upstreamKind, true)
	if err != nil {
		return streamAttemptResult{Err: err}
	}

	client := rt.httpClientForTarget(target.Channel, target.Provider)
	start := time.Now()
	resp, err := rt.doHTTPWithFirstTokenDeadline(reqCtx, abortReq, client, req, start, firstTokenTimeout)
	if err != nil {
		return streamAttemptResult{Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	headers := resp.Header.Clone()
	status := resp.StatusCode

	// 非 2xx：整包读错误体，不 commit，供 retry/failover（错误体不受首字超时约束）
	if status < 200 || status >= 300 {
		errBody, readErr := io.ReadAll(io.LimitReader(resp.Body, int64(gwCfg.UsageErrorBodyBytes)+1))
		if readErr != nil && len(errBody) == 0 {
			return streamAttemptResult{Status: status, Headers: headers, Err: readErr}
		}
		return streamAttemptResult{Status: status, Headers: headers, Body: errBody}
	}

	// 响应头已到：若整体首字预算已耗尽，直接按首字超时断开
	if _, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout); timedOut {
		ms := time.Since(start).Milliseconds()
		ft := &ms
		return streamAttemptResult{
			Status: status, Headers: headers, FirstTokenMS: ft,
			Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout),
		}
	}

	incremental := protocol.SupportsIncrementalStream(inboundKind, upstreamKind, converted)
	if !incremental {
		return rt.forwardStreamBuffered(c, resp, start, firstTokenTimeout, inboundKind, upstreamKind, model, converted, headers, status)
	}
	return rt.forwardStreamIncremental(upCtx, clientCtx, abortReq, c, resp, start, firstTokenTimeout, inboundKind, upstreamKind, model, converted, headers, status)
}

// forwardStreamBuffered 仅兜底：三协议互转已走增量真流；此处保留给未知协议或未实现转换器。
// 行为：先读完再转换，再一次性写出（首写出前仍可 failover）——即「假流」，应尽量避免进入。

func (rt *Runtime) forwardStreamBuffered(
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	inbound, upstream protocolKind,
	model string,
	converted bool,
	headers http.Header,
	status int,
) streamAttemptResult {
	var ft *int64
	// 从请求发起起算剩余首字预算（含已花费的等响应头时间）
	bodyWait, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
	if timedOut {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		return streamAttemptResult{Status: status, Headers: headers, FirstTokenMS: ft, Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)}
	}
	firstChunk, firstErr := rt.readFirstChunk(resp.Body, bodyWait)
	if firstErr != nil {
		if rt.isFirstTokenTimeout(firstErr) {
			ms := time.Since(start).Milliseconds()
			ft = &ms
			return streamAttemptResult{Status: status, Headers: headers, FirstTokenMS: ft, Err: fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)}
		}
		return streamAttemptResult{Status: status, Headers: headers, Err: firstErr}
	}
	var data []byte
	if len(firstChunk) > 0 {
		ms := time.Since(start).Milliseconds()
		ft = &ms
		data = append(data, firstChunk...)
	}
	rest, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return streamAttemptResult{Status: status, Headers: headers, Body: data, FirstTokenMS: ft, Err: readErr}
	}
	data = append(data, rest...)

	tokens := rt.parseUsageByKind(data, true, upstream)
	clientBody := rt.convertUpstreamResponse(data, inbound, upstream, model, true, converted)
	if len(clientBody) == 0 {
		clientBody = data
	}

	if err := rt.commitSSEHeaders(c, headers); err != nil {
		return streamAttemptResult{Status: status, Headers: headers, Body: clientBody, FirstTokenMS: ft, Tokens: tokens, Err: err}
	}
	if _, err := c.Writer.Write(clientBody); err != nil {
		var rejected *responseRejectedError
		if errors.As(err, &rejected) {
			return streamAttemptResult{Status: status, Headers: headers, Body: clientBody, FirstTokenMS: ft, Tokens: tokens, Err: err, ValidationRejection: rejected.Result}
		}
		return streamAttemptResult{
			Status: status, Headers: headers, FirstTokenMS: ft, Tokens: tokens,
			Committed: streamWriterActuallyCommitted(c), ClientDisconnected: true, StreamErr: errClientDisconnected,
		}
	}
	rt.flushWriter(c)
	if ft == nil {
		ms := time.Since(start).Milliseconds()
		ft = &ms
	}
	// 缓冲路径整包写出成功即视为下游完整交付（含转换后的终端帧）。
	return streamAttemptResult{
		Status: status, Headers: headers, FirstTokenMS: ft, Tokens: tokens,
		Committed: streamWriterActuallyCommitted(c), DownstreamComplete: true,
	}
}

// forwardStreamIncremental 同协议透传或 Anthropic↔OpenAI 增量转换。
// upCtx：上游生命周期（与客户端取消解耦）；clientCtx：检测客户端断开。
// abortReq：仅未 commit 或首字超时时中止上游；已 commit 的客户端断开不调用，以便 drain 计费。

func (rt *Runtime) forwardStreamIncremental(
	upCtx context.Context,
	clientCtx context.Context,
	abortReq context.CancelFunc,
	c *gin.Context,
	resp *http.Response,
	start time.Time,
	firstTokenTimeout time.Duration,
	inbound, upstream protocolKind,
	model string,
	converted bool,
	headers http.Header,
	status int,
) streamAttemptResult {
	if upCtx == nil {
		upCtx = context.Background()
	}
	if clientCtx == nil {
		clientCtx = context.Background()
	}
	result := streamAttemptResult{Status: status, Headers: headers}
	clientKind := protocol.NormalizeKind(inbound)
	upKind := protocol.NormalizeKind(upstream)

	var (
		anth2oai  *protocol.AnthropicToOpenAIStream
		oai2anth  *protocol.OpenAIToAnthropicStream
		resp2anth *protocol.ResponsesToAnthropicStream
		resp2oai  *protocol.ResponsesToOpenAIStream
		anth2resp *protocol.AnthropicToResponsesStream
		chat2resp *protocol.ChatToResponsesStream
	)
	if converted {
		switch {
		case protocol.IsOpenAIFamily(clientKind) && clientKind != protocol.KindOpenAIResponses && upKind == protocol.KindAnthropic:
			// Chat ← Anthropic
			anth2oai = protocol.NewAnthropicToOpenAIStream(model)
		case clientKind == protocol.KindAnthropic && (upKind == protocol.KindOpenAIChat || upKind == protocol.KindOpenAI):
			// Anthropic ← Chat
			oai2anth = protocol.NewOpenAIToAnthropicStream(model)
		case clientKind == protocol.KindAnthropic && upKind == protocol.KindOpenAIResponses:
			// Anthropic ← Responses
			resp2anth = protocol.NewResponsesToAnthropicStream(model)
		case protocol.IsOpenAIFamily(clientKind) && clientKind != protocol.KindOpenAIResponses && upKind == protocol.KindOpenAIResponses:
			// Chat ← Responses
			resp2oai = protocol.NewResponsesToOpenAIStream(model)
		case clientKind == protocol.KindOpenAIResponses && upKind == protocol.KindAnthropic:
			// Responses ← Anthropic
			anth2resp = protocol.NewAnthropicToResponsesStream(model)
		case clientKind == protocol.KindOpenAIResponses && (upKind == protocol.KindOpenAIChat || upKind == protocol.KindOpenAI):
			// Responses ← Chat
			chat2resp = protocol.NewChatToResponsesStream(model)
		}
	}

	type scanEvent struct {
		line string
		err  error
	}
	events := make(chan scanEvent, 32)
	done := make(chan struct{})
	var lastReadAt int64
	atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())

	go func() {
		defer close(events)
		scanner := bufio.NewScanner(resp.Body)
		scanner.Buffer(make([]byte, 0, 64*1024), maxSSELineSize)
		for scanner.Scan() {
			atomic.StoreInt64(&lastReadAt, time.Now().UnixNano())
			line := strings.TrimRight(scanner.Text(), "\r")
			select {
			case events <- scanEvent{line: line}:
			case <-done:
				return
			}
		}
		if err := scanner.Err(); err != nil {
			select {
			case events <- scanEvent{err: err}:
			case <-done:
			}
		}
	}()
	defer close(done)

	// 首字超时：从 start（发起上游请求）起算剩余时间，与 first_token_ms 同一时钟。
	// 注意：不是「响应头之后再等 N 秒」。
	var firstTimer *time.Timer
	var firstCh <-chan time.Time
	if firstTokenTimeout > 0 {
		left, timedOut := rt.remainingFirstTokenWait(start, firstTokenTimeout)
		if timedOut {
			ms := time.Since(start).Milliseconds()
			result.FirstTokenMS = &ms
			result.Err = fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
			return result
		}
		firstTimer = time.NewTimer(left)
		defer firstTimer.Stop()
		firstCh = firstTimer.C
	}

	keepalive := defaultStreamKeepalive
	idleTimeout := defaultStreamIdleTimeout
	var keepTicker *time.Ticker
	if keepalive > 0 {
		keepTicker = time.NewTicker(keepalive)
		defer keepTicker.Stop()
	}
	var idleTicker *time.Ticker
	if idleTimeout > 0 {
		idleTicker = time.NewTicker(time.Second)
		defer idleTicker.Stop()
	}

	var (
		pendingLines     []string
		sawUpstreamData  bool
		streamStarted    bool
		errorEventSent   bool
		lastDownstreamAt = time.Now()
		usageBuf         bytes.Buffer
		tokens           UsageTokens
	)

	stopFirstTimer := func() {
		if firstTimer != nil {
			if !firstTimer.Stop() {
				select {
				case <-firstTimer.C:
				default:
				}
			}
			firstCh = nil
		}
	}

	writeFrames := func(frames [][]byte) error {
		if len(frames) == 0 {
			return nil
		}
		if !streamStarted {
			if err := rt.commitSSEHeaders(c, headers); err != nil {
				return err
			}
			streamStarted = true
			ms := time.Since(start).Milliseconds()
			result.FirstTokenMS = &ms
			stopFirstTimer()
		}
		result.Committed = streamWriterActuallyCommitted(c)
		if result.ClientDisconnected && result.Committed {
			return nil
		}
		for _, f := range frames {
			if len(f) == 0 {
				continue
			}
			if _, err := c.Writer.Write(f); err != nil {
				var rejected *responseRejectedError
				if errors.As(err, &rejected) {
					result.ValidationRejection = rejected.Result
					result.Err = err
					return err
				}
				if errors.Is(err, errStreamGateLost) {
					result.Err = err
					return err
				}
				result.ClientDisconnected = true
				return err
			}
			if rt.sseFrameIsTerminal(f) {
				result.DownstreamComplete = true
			}
		}
		rt.flushWriter(c)
		result.Committed = streamWriterActuallyCommitted(c)
		lastDownstreamAt = time.Now()
		return nil
	}

	processEvent := func(lines []string) error {
		if len(lines) == 0 {
			return nil
		}
		eventName, data := rt.parseSSEEventLines(lines)
		// 旁路 usage：累积原始 data
		if data != "" && data != "[DONE]" {
			usageBuf.WriteString(data)
			usageBuf.WriteByte('\n')
			rt.mergeStreamUsage(&tokens, data, upKind)
		}

		var frames [][]byte
		switch {
		case anth2oai != nil:
			frames = anth2oai.Feed(eventName, data)
		case oai2anth != nil:
			if !oai2anth.Started {
				frames = append(frames, oai2anth.EnsureStarted()...)
			}
			frames = append(frames, oai2anth.FeedData(data)...)
		case resp2anth != nil:
			frames = resp2anth.Feed(eventName, data)
		case resp2oai != nil:
			frames = resp2oai.Feed(eventName, data)
		case anth2resp != nil:
			frames = anth2resp.Feed(eventName, data)
		case chat2resp != nil:
			frames = chat2resp.FeedData(data)
		default:
			// 透传完整 event。commit 前丢弃纯注释/空事件，避免上游 :PING 过早
			// 提交响应头后无法再 retry/failover，并污染 first_token 统计。
			if !result.Committed && !rt.sseEventHasPayload(lines) {
				return nil
			}
			var b strings.Builder
			for _, ln := range lines {
				b.WriteString(ln)
				b.WriteByte('\n')
			}
			b.WriteByte('\n')
			frames = [][]byte{[]byte(b.String())}
		}
		return writeFrames(frames)
	}

	sendTerminalError := func(errType, msg string) {
		if errorEventSent || result.ClientDisconnected {
			return
		}
		if !result.Committed {
			return
		}
		errorEventSent = true
		_ = rt.writeStreamTerminalError(c, clientKind, errType, msg)
	}

	closeConverters := func() error {
		var frames [][]byte
		switch {
		case anth2oai != nil:
			frames = anth2oai.Close()
		case oai2anth != nil:
			frames = oai2anth.Close()
		case resp2anth != nil:
			frames = resp2anth.Close()
		case resp2oai != nil:
			frames = resp2oai.Close()
		case anth2resp != nil:
			frames = anth2resp.Close()
		case chat2resp != nil:
			frames = chat2resp.Close()
		}
		return writeFrames(frames)
	}

	for {
		var keepCh <-chan time.Time
		if keepTicker != nil && result.Committed && !result.ClientDisconnected {
			keepCh = keepTicker.C
		}
		var idleCh <-chan time.Time
		if idleTicker != nil {
			idleCh = idleTicker.C
		}

		select {
		case <-upCtx.Done():
			// 上游整体超时（非客户端取消）
			if !result.Committed {
				result.Err = upCtx.Err()
				if abortReq != nil {
					abortReq()
				}
				return result
			}
			result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
			if result.ClientDisconnected {
				// 客户端已断，上游超时：用已收集 usage 落库；若已交付终端帧则不当 client 失败
				rt.finalizeStreamClientDisconnect(&result)
				return result
			}
			msg := "upstream stream canceled"
			if err := upCtx.Err(); err != nil {
				msg = err.Error()
			}
			sendTerminalError("stream_timeout", msg)
			result.StreamErr = errors.New(msg)
			return result

		case <-clientCtx.Done():
			// 客户端断开 / 取消
			if !result.Committed {
				// 尚未向客户端提交：中止上游，允许上层记账为客户端取消（不 drain 计费，也无半截响应）
				result.ClientDisconnected = true
				if abortReq != nil {
					abortReq()
				}
				if err := clientCtx.Err(); err != nil {
					result.Err = err
				} else {
					result.Err = errClientDisconnected
				}
				return result
			}
			// 已 commit：不 abort 上游，标记后继续主循环 drain，对齐 sub2api billing drain。
			// 若已写出终端帧，结束时会清掉 client 标签（正常收尾关连接）。
			if !result.ClientDisconnected {
				result.ClientDisconnected = true
			}
			continue

		case <-firstCh:
			// 仅在尚未收到「有效」上游载荷时触发（ping/注释不算）
			if !sawUpstreamData && !result.Committed {
				result.Err = fmt.Errorf("%w after %s", errFirstTokenTimeout, firstTokenTimeout)
				ms := time.Since(start).Milliseconds()
				result.FirstTokenMS = &ms
				return result
			}

		case <-keepCh:
			if result.ClientDisconnected || !result.Committed {
				continue
			}
			if time.Since(lastDownstreamAt) < keepalive {
				continue
			}
			ping := rt.streamKeepaliveFrame(clientKind)
			if _, err := c.Writer.Write(ping); err != nil {
				result.ClientDisconnected = true
				continue
			}
			rt.flushWriter(c)
			lastDownstreamAt = time.Now()

		case <-idleCh:
			if !sawUpstreamData {
				continue
			}
			lastRead := time.Unix(0, atomic.LoadInt64(&lastReadAt))
			if time.Since(lastRead) < idleTimeout {
				continue
			}
			msg := fmt.Sprintf("upstream stream idle for %s", idleTimeout)
			if result.Committed {
				sendTerminalError("stream_timeout", msg)
				result.StreamErr = errors.New(msg)
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				return result
			}
			result.Err = errors.New(msg)
			return result

		case ev, ok := <-events:
			if !ok {
				// 上游结束
				if len(pendingLines) > 0 {
					if err := processEvent(pendingLines); err != nil {
						if result.Err == nil {
							result.Err = err
						}
						return result
					}
					pendingLines = pendingLines[:0]
				}
				if !streamStarted {
					// 空流
					result.Body = usageBuf.Bytes()
					result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
					// 仍写出转换收尾（可能只有 [DONE]）
					if err := closeConverters(); err != nil {
						if result.Err == nil {
							result.Err = err
						}
						return result
					}
					if !streamStarted {
						// 无任何输出：视为成功空流，commit 空 SSE 结束
						if anth2oai == nil && oai2anth == nil && resp2anth == nil && resp2oai == nil && anth2resp == nil && chat2resp == nil {
							doneFrame := []byte("data: [DONE]\n\n")
							if err := writeFrames([][]byte{doneFrame}); err != nil {
								if result.Err == nil {
									result.Err = err
								}
								return result
							}
							result.DownstreamComplete = true
						} else if err := rt.commitSSEHeaders(c, headers); err == nil {
							streamStarted = true
							result.Committed = streamWriterActuallyCommitted(c)
						} else {
							result.Err = err
							return result
						}
					}
					result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				// 已 commit：收尾转换（客户端已断则 writeFrames 会跳过写出）
				if err := closeConverters(); err != nil {
					if result.Err == nil {
						result.Err = err
					}
					return result
				}
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				rt.finalizeStreamClientDisconnect(&result)
				return result
			}
			if ev.err != nil {
				if !result.Committed {
					if errors.Is(ev.err, bufio.ErrTooLong) {
						result.Err = fmt.Errorf("upstream SSE line exceeded %d bytes", maxSSELineSize)
					} else {
						result.Err = ev.err
					}
					return result
				}
				// 已 commit：尽量落已收集 usage；中途 client 断开优先标记 client
				result.Tokens = rt.finalizeStreamTokens(tokens, usageBuf.Bytes(), upKind)
				if result.ClientDisconnected {
					rt.finalizeStreamClientDisconnect(&result)
					return result
				}
				msg := "upstream stream disconnected"
				if errors.Is(ev.err, bufio.ErrTooLong) {
					msg = fmt.Sprintf("upstream SSE line exceeded %d bytes", maxSSELineSize)
				}
				sendTerminalError("stream_read_error", msg)
				result.StreamErr = ev.err
				return result
			}

			line := ev.line
			if strings.TrimSpace(line) == "" {
				// 完整 SSE event 结束。commit 前的纯注释/ping 不计入首字、不停首字计时器，
				// 否则上游 :PING 会误取消 2s 首字超时，真实内容 7s 才到却无法顺延。
				if !result.Committed && anth2oai == nil && oai2anth == nil && resp2anth == nil && resp2oai == nil && anth2resp == nil && chat2resp == nil && !rt.sseEventHasPayload(pendingLines) {
					pendingLines = pendingLines[:0]
					continue
				}
				// 有效事件：才算「有首字」
				if !sawUpstreamData {
					sawUpstreamData = true
					stopFirstTimer()
					if result.FirstTokenMS == nil {
						ms := time.Since(start).Milliseconds()
						result.FirstTokenMS = &ms
					}
				}
				if err := processEvent(pendingLines); err != nil {
					if result.Err == nil {
						result.Err = err
					}
					return result
				}
				pendingLines = pendingLines[:0]
				continue
			}
			pendingLines = append(pendingLines, line)
		}
	}
}
