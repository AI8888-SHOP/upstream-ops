// gateway 与 protocol 包之间的请求/响应/错误体桥接。
package gateway

import (
	"bytes"
	"encoding/json"
	"strings"
	"sync"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

type protocolKind = protocol.Kind

const (
	protocolOpenAI    = protocol.KindOpenAI
	protocolAnthropic = protocol.KindAnthropic
)

type preparedUpstreamRequestKey struct {
	inbound       protocolKind
	upstream      protocolKind
	requestedModel string
	upstreamModel  string
	stream         bool
	inboundPath    string
}

type preparedUpstreamRequest struct {
	body      []byte
	path      string
	converted bool
	err       error
}

// upstreamRequestPrepareCache is request-scoped. Retry and hedge attempts
// commonly share the same protocol/model tuple, so the expensive JSON rewrite
// or protocol conversion only needs to run once for that tuple.
type upstreamRequestPrepareCache struct {
	mu      sync.Mutex
	entries map[preparedUpstreamRequestKey]preparedUpstreamRequest
}

func (cache *upstreamRequestPrepareCache) prepare(
	svc *Service,
	body []byte,
	inbound, upstream protocolKind,
	requestedModel, upstreamModel string,
	stream bool,
	inboundPath string,
) (fwd []byte, upstreamPath string, converted bool, err error) {
	if cache == nil {
		return svc.prepareUpstreamRequestKnownModel(
			body, inbound, upstream, requestedModel, upstreamModel, stream, inboundPath,
		)
	}
	key := preparedUpstreamRequestKey{
		inbound:        protocol.NormalizeKind(inbound),
		upstream:       protocol.NormalizeKind(upstream),
		requestedModel: strings.TrimSpace(requestedModel),
		upstreamModel:  strings.TrimSpace(upstreamModel),
		stream:         stream,
		inboundPath:    strings.TrimSpace(inboundPath),
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cached, ok := cache.entries[key]; ok {
		return cached.body, cached.path, cached.converted, cached.err
	}
	fwd, upstreamPath, converted, err = svc.prepareUpstreamRequestKnownModel(
		body, inbound, upstream, requestedModel, upstreamModel, stream, inboundPath,
	)
	if cache.entries == nil {
		cache.entries = make(map[preparedUpstreamRequestKey]preparedUpstreamRequest)
	}
	cache.entries[key] = preparedUpstreamRequest{
		body: fwd, path: upstreamPath, converted: converted, err: err,
	}
	return fwd, upstreamPath, converted, err
}

func (svc *Service) prepareUpstreamRequest(
	body []byte,
	inbound, upstream protocolKind,
	model string,
	stream bool,
	inboundPath string,
) (fwd []byte, upstreamPath string, converted bool, err error) {
	return svc.prepareUpstreamRequestKnownModel(body, inbound, upstream, ExtractModelFromBody(body), model, stream, inboundPath)
}

func (svc *Service) prepareUpstreamRequestKnownModel(
	body []byte,
	inbound, upstream protocolKind,
	requestedModel, model string,
	stream bool,
	inboundPath string,
) (fwd []byte, upstreamPath string, converted bool, err error) {
	in := protocol.NormalizeKind(inbound)
	up := protocol.NormalizeKind(upstream)
	upstreamPath = protocol.PathFor(up, inboundPath)
	fwd = body
	if strings.TrimSpace(requestedModel) != strings.TrimSpace(model) {
		fwd = RewriteModelInBody(body, model)
	}
	if !protocol.NeedsBodyConvert(in, up) {
		// 同形态：仍可能需要改 path（例如入站 chat、上游 responses 不走这里）
		if protocol.IsOpenAIFamily(in) && protocol.IsOpenAIFamily(up) &&
			protocol.NormalizeKind(in) == protocol.NormalizeKind(up) {
			// OpenAI Chat 流式：对齐 sub2api，强制 include_usage
			if stream && (up == protocol.KindOpenAIChat || up == protocol.KindOpenAI) {
				fwd = EnsureStreamUsageOption(fwd, stream)
			}
			return fwd, upstreamPath, false, nil
		}
		if in == up {
			if stream && (up == protocol.KindOpenAIChat || up == protocol.KindOpenAI) {
				fwd = EnsureStreamUsageOption(fwd, stream)
			}
			return fwd, upstreamPath, false, nil
		}
	}

	// 透传端点：不做 body 协议转换，只改 model（若有）
	if protocol.IsPassthroughEndpoint(inboundPath) {
		if stream && (up == protocol.KindOpenAIChat || up == protocol.KindOpenAI) {
			fwd = EnsureStreamUsageOption(fwd, stream)
		}
		return fwd, upstreamPath, false, nil
	}

	fwd, converted, err = protocol.ConvertRequest(in, up, fwd, model, stream)
	if err != nil {
		return nil, "", false, err
	}
	// 转换后若上游仍是 OpenAI Chat，强制 include_usage（sub2api 同款）
	if stream && (up == protocol.KindOpenAIChat || up == protocol.KindOpenAI) {
		fwd = EnsureStreamUsageOption(fwd, stream)
	}
	return fwd, upstreamPath, converted, nil
}

// convertUpstreamResponse 将上游响应转回客户端协议。
func (svc *Service) convertUpstreamResponse(
	respBody []byte,
	inbound, upstream protocolKind,
	model string,
	stream bool,
	converted bool,
) []byte {
	if !converted || len(respBody) == 0 {
		return respBody
	}
	in := protocol.NormalizeKind(inbound)
	up := protocol.NormalizeKind(upstream)

	// 流式：Anthropic↔OpenAI SSE 互转；Responses 相关尽力转换
	if stream {
		return protocol.ConvertStreamResponse(in, up, respBody, model, svc.wrapAsOpenAISSE, svc.tryExtractJSONObject)
	}
	return protocol.ConvertResponse(in, up, respBody, model)
}

// tryExtractJSONObject 从可能的 SSE 缓冲中取最后一段 JSON 对象（Responses 流结束时的完整事件）。
func (svc *Service) tryExtractJSONObject(body []byte) []byte {
	trim := bytes.TrimSpace(body)
	if len(trim) == 0 {
		return body
	}
	if trim[0] == '{' {
		return trim
	}
	// SSE: 找最后一个 data: { ... }
	var last []byte
	for _, line := range bytes.Split(body, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if bytes.HasPrefix(line, []byte("data:")) {
			payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
			if len(payload) > 0 && payload[0] == '{' {
				last = payload
			}
		}
	}
	if len(last) > 0 {
		return last
	}
	return body
}

func (svc *Service) wrapAsOpenAISSE(chatJSON []byte) []byte {
	// 将完整 chat.completion 拆成流式 chunk + DONE。
	// 工具调用：流式 delta.tool_calls 需带 index，否则多数客户端忽略。
	var m map[string]any
	if err := json.Unmarshal(chatJSON, &m); err != nil {
		return chatJSON
	}
	id := m["id"]
	model := m["model"]
	var frames [][]byte

	emit := func(delta map[string]any, finish any, usage any) {
		chunk := map[string]any{
			"id":     id,
			"object": "chat.completion.chunk",
			"model":  model,
			"choices": []any{
				map[string]any{
					"index":         0,
					"delta":         delta,
					"finish_reason": finish,
				},
			},
		}
		if usage != nil {
			chunk["usage"] = usage
		}
		raw, _ := json.Marshal(chunk)
		frames = append(frames, raw)
	}

	var finish any = "stop"
	var usage any
	if u, ok := m["usage"]; ok {
		usage = u
	}

	if choices, ok := m["choices"].([]any); ok && len(choices) > 0 {
		if ch, ok := choices[0].(map[string]any); ok {
			if fr, ok := ch["finish_reason"]; ok && fr != nil {
				finish = fr
			}
			if msg, ok := ch["message"].(map[string]any); ok {
				// role 首包（纯工具调用时 content 为 null，仍发 role）
				roleChunk := map[string]any{"role": "assistant"}
				if s, ok := msg["content"].(string); ok && s != "" {
					roleChunk["content"] = s
				}
				emit(roleChunk, nil, nil)

				if tcs, ok := msg["tool_calls"].([]any); ok && len(tcs) > 0 {
					// 每个 tool_call 带 index，符合 chat.completion.chunk 约定
					indexed := make([]any, 0, len(tcs))
					for i, raw := range tcs {
						tc, _ := raw.(map[string]any)
						if tc == nil {
							continue
						}
						item := map[string]any{
							"index": i,
							"id":    tc["id"],
							"type":  "function",
						}
						if typ, ok := tc["type"].(string); ok && typ != "" {
							item["type"] = typ
						}
						if fn, ok := tc["function"].(map[string]any); ok {
							item["function"] = fn
						}
						indexed = append(indexed, item)
					}
					if len(indexed) > 0 {
						emit(map[string]any{"tool_calls": indexed}, nil, nil)
						if finish == "stop" || finish == nil || finish == "" {
							finish = "tool_calls"
						}
					}
				}
			}
		}
	}

	// 结束帧（带 finish_reason + usage）
	emit(map[string]any{}, finish, usage)

	var b strings.Builder
	for _, f := range frames {
		b.WriteString("data: ")
		b.Write(f)
		b.WriteString("\n\n")
	}
	b.WriteString("data: [DONE]\n\n")
	return []byte(b.String())
}

func (svc *Service) parseUsageByKind(body []byte, stream bool, kind protocolKind) UsageTokens {
	return ParseUsage(body, stream, kind)
}

func (svc *Service) convertErrorBody(body []byte, client, upstream protocolKind, converted bool) []byte {
	if !converted {
		return body
	}
	in := protocol.NormalizeKind(client)
	up := protocol.NormalizeKind(upstream)
	if in == up {
		return body
	}
	// Anthropic 上游错误 → OpenAI 族客户端
	if protocol.IsOpenAIFamily(in) && up == protocol.KindAnthropic {
		if out, err := protocol.AnthropicToOpenAIResponse(body, ""); err == nil {
			return out
		}
	}
	// OpenAI Chat 上游错误 → Anthropic 客户端
	if in == protocol.KindAnthropic && (up == protocol.KindOpenAIChat || up == protocol.KindOpenAI) {
		if out, err := protocol.OpenAIToAnthropicResponse(body, ""); err == nil {
			return out
		}
	}
	// Responses 上游错误 → Anthropic 客户端（尽量包一层 message 错误）
	if in == protocol.KindAnthropic && up == protocol.KindOpenAIResponses {
		if out, err := protocol.OpenAIToAnthropicResponse(body, ""); err == nil {
			return out
		}
		// 裸 OpenAI error JSON
		var m map[string]any
		if json.Unmarshal(body, &m) == nil {
			if errObj, ok := m["error"].(map[string]any); ok {
				out, _ := json.Marshal(map[string]any{
					"type": "error",
					"error": map[string]any{
						"type":    errObj["type"],
						"message": errObj["message"],
					},
				})
				return out
			}
		}
	}
	// Responses 上游错误 → Chat 客户端：多数已是 OpenAI error 形态，原样
	return body
}
