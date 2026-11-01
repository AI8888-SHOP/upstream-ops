package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

// The fixed puzzle and expected answer follow ranxi2001/sub2api's candy test.
// Keep the question (including distinguishable shapes) intact; removing that
// condition changes the problem. See docs/candy-check.md for provenance.
const candyCheckPrompt = `在一个黑色的袋子里放有三种口味的糖果，每种糖果有两种不同的形状（圆形和五角星形，不同的形状靠手感可以分辨）。现已知不同口味的糖和不同形状的数量统计如下表。参赛者需要在活动前决定摸出的糖果数目，那么，最少取出多少个糖果才能保证手中同时拥有不同形状的苹果味和桃子味的糖？（同时手中有圆形苹果味匹配五角星桃子味糖果，或者有圆形桃子味匹配五角星苹果味糖果都满足要求）
苹果味 桃子味 西瓜味
圆形 7 9 8
五角星形 7 6 4

只输出最终整数，不要解释。`

const candyCheckTimeout = 90 * time.Second
const candyCheckBodyLimit = 2 << 20

var ErrCandyCheckBusy = errors.New("该渠道已在检测或检测任务繁忙，请稍后重试")
var ErrCandyCheckChanged = errors.New("检测期间配置或冷却状态已变更，本次结果未应用，请刷新后重试")

// Two workers bound cost and resource usage. Leases also exclude duplicate
// probes across instances; TryLock prevents overlapping cron ticks locally.
func (s *Service) RunCandyChecks(ctx context.Context) {
	if s == nil || s.Routes == nil || s.Groups == nil || !s.candyCheckMu.TryLock() {
		return
	}
	defer s.candyCheckMu.Unlock()
	routes, err := s.Routes.ListCandyCheckRoutes()
	if err != nil {
		s.logCandyCheckError("list candy check routes", 0, err)
		return
	}
	jobs := make(chan storage.GatewayRoute)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for route := range jobs {
				if ctx.Err() != nil {
					continue
				}
				s.runRouteCandyCheck(ctx, route)
			}
		}()
	}
	for _, route := range routes {
		state := route.CandyCheck
		now := time.Now()
		if state != nil && ((state.LeaseUntil != nil && state.LeaseUntil.After(now)) || (state.Active && state.NextCheckAt != nil && state.NextCheckAt.After(now))) {
			continue
		}
		select {
		case jobs <- route:
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		}
	}
	close(jobs)
	wg.Wait()
}

func (s *Service) logCandyCheckError(message string, routeID uint, err error) {
	if s.Log != nil {
		s.Log.Warn(message, "route_id", routeID, "err", err)
	}
}

func (s *Service) runRouteCandyCheck(ctx context.Context, route storage.GatewayRoute) {
	if _, err := s.runCandyCheck(ctx, route, false); err != nil && !errors.Is(err, ErrCandyCheckBusy) {
		s.logCandyCheckError("run candy check", route.ID, err)
	}
}

// RunCandyCheckNow uses the saved route and policy, without waiting for a
// scheduled interval or cooldown. Disabled automatic checks remain disabled.
func (s *Service) RunCandyCheckNow(ctx context.Context, routeID uint) (*storage.GatewayRouteCandyCheck, error) {
	if s == nil || s.Routes == nil || s.Groups == nil || routeID == 0 {
		return nil, errors.New("智商测试服务不可用")
	}
	route, err := s.Routes.FindByID(routeID)
	if err != nil {
		return nil, err
	}
	return s.runCandyCheck(ctx, *route, true)
}

func (s *Service) runCandyCheck(ctx context.Context, route storage.GatewayRoute, manual bool) (*storage.GatewayRouteCandyCheck, error) {
	// Scheduled and manual requests share the same process-wide budget.
	s.candyCheckSlotsOnce.Do(func() { s.candyCheckSlots = make(chan struct{}, 2) })
	select {
	case s.candyCheckSlots <- struct{}{}:
		defer func() { <-s.candyCheckSlots }()
	default:
		return nil, ErrCandyCheckBusy
	}
	group, err := s.Groups.FindByID(route.GatewayGroupID)
	if err != nil {
		return nil, err
	}
	if (!manual && !group.CandyCheckEnabled) || group.Status != storage.GatewayGroupStatusActive || !route.Enabled || route.RateLimitAutoDisabled {
		if !manual {
			return nil, nil
		}
		return nil, errors.New("请先启用网关组及该渠道路由")
	}
	if strings.TrimSpace(group.CandyCheckModel) == "" {
		return nil, errors.New("请先在编辑网关组的不降智检测中填写并保存检测模型")
	}
	model, _ := ResolveModel(group.CandyCheckModel, ParseModelMapping(route.ModelMappingJSON), ParseModelMapping(group.ModelMappingJSON))
	var provider *storage.GatewayProvider
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if s.Providers == nil {
			return nil, errors.New("直连渠道服务不可用")
		}
		provider, err = s.Providers.FindByID(route.GatewayProviderID)
		if err != nil || !provider.Enabled {
			if !manual {
				return nil, nil
			}
			return nil, errors.New("请先启用直连渠道")
		}
	}
	key := storage.GatewayCandyCheckConfigKey(group, &route, provider)
	claimCheck := s.Routes.ClaimCandyCheck
	if manual {
		claimCheck = s.Routes.ClaimManualCandyCheck
	}
	claim, err := claimCheck(route.ID, key, model, time.Now(), candyCheckTimeout+30*time.Second)
	if err != nil {
		return nil, err
	}
	if claim == nil {
		return nil, ErrCandyCheckBusy
	}
	result := s.probeCandyRouteWithSharing(ctx, group, route, model, manual)
	if result.SharedLeaseToken != "" {
		defer func() {
			_, _ = s.Routes.ReleaseSharedCandyCheck(storage.GatewayCandyCheckSharedLease{CacheKey: result.SharedCacheKey, LeaseToken: result.SharedLeaseToken})
		}()
	}
	if ctx.Err() != nil {
		result.Status = "deferred"
	}
	now := time.Now()
	var updated bool
	if result.SharedLeaseToken != "" && !result.Reused && result.Status != "deferred" {
		lease := storage.GatewayCandyCheckSharedLease{CacheKey: result.SharedCacheKey, LeaseToken: result.SharedLeaseToken, RouteID: claim.RouteID, ConfigKey: claim.ConfigKey}
		if manual {
			updated, err = s.Routes.FinishManualCandyCheckWithShared(*claim, result, now, lease)
		} else {
			updated, err = s.Routes.FinishCandyCheckWithShared(*claim, result, now, lease)
		}
	} else if manual {
		updated, err = s.Routes.FinishManualCandyCheck(*claim, result, now)
	} else {
		updated, err = s.Routes.FinishCandyCheck(*claim, result, now)
	}
	if err != nil {
		return nil, err
	}
	if !updated {
		// A route config/manual clear fence won. The atomic storage transaction
		// also rolled back the shared result, so no source row is published.
		return nil, ErrCandyCheckChanged
	}
	if result.SharedLeaseToken != "" && result.Status == "deferred" && s.Routes != nil {
		_, _ = s.Routes.ReleaseSharedCandyCheck(storage.GatewayCandyCheckSharedLease{CacheKey: result.SharedCacheKey, LeaseToken: result.SharedLeaseToken})
	}
	if !manual {
		return nil, nil
	}
	if result.Status == "deferred" {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, errors.New(result.Reason)
	}
	loaded, err := s.Routes.FindByID(route.ID)
	if err != nil {
		return nil, err
	}
	if loaded.CandyCheck == nil || !loaded.CandyCheck.Active || loaded.CandyCheck.Status != result.Status {
		return nil, ErrCandyCheckChanged
	}
	return loaded.CandyCheck, nil
}

// probeCandyRoute is kept as the direct, uncached probe used by focused
// protocol tests. Scheduled/manual route execution uses the shared wrapper.
func (s *Service) probeCandyRoute(parent context.Context, group *storage.GatewayGroup, route storage.GatewayRoute, model string) storage.GatewayRouteCandyCheck {
	return s.probeCandyRouteHTTP(parent, group, route, model)
}

func (s *Service) probeCandyRouteWithSharing(parent context.Context, group *storage.GatewayGroup, route storage.GatewayRoute, model string, manual bool) (result storage.GatewayRouteCandyCheck) {
	result.Status, result.Model, result.SourceRouteID = "error", model, route.ID
	start := time.Now()
	defer func() {
		if !result.Reused {
			result.LatencyMS = time.Since(start).Milliseconds()
		}
	}()
	allowed, err := RouteAllowsUpstreamModel(&route, model)
	if err != nil || !allowed || model == "" || mediaGenerationModel(model) {
		result.Status, result.Reason = "skipped", "检测模型不在该路由的支持范围内，或不是文本模型"
		return
	}
	target, err := s.resolveUpstreamTarget(&route)
	if err != nil {
		result.Status, result.Reason = "skipped", "上游未就绪，请检查渠道状态及上游密钥"
		return
	}
	if target.Provider != nil {
		allowed, err = ProviderAllowsUpstreamModel(target.Provider, model)
		if err != nil || !allowed {
			result.Status, result.Reason = "skipped", "直连渠道不支持检测模型"
			return
		}
	}
	s.applyRouteUserAgentForAdmin(target, group, &route)
	kindSetting := s.normalizeUpstreamProtocol(route.UpstreamProtocol)
	if kindSetting == storage.GatewayUpstreamProtocolAuto && target.Provider != nil {
		kindSetting = s.normalizeProviderProtocol(target.Provider.UpstreamProtocol)
	}
	kind := protocol.ResolveUpstream(kindSetting, protocol.KindOpenAIChat, model)
	body, path := candyCheckRequest(kind, model, group.CandyCheckReasoningEffort)
	ctx, cancel := context.WithTimeout(parent, candyCheckTimeout)
	defer cancel()
	req, err := s.buildUpstreamHTTPRequest(ctx, target, path, http.MethodPost, http.Header{"Accept": []string{"text/event-stream"}}, body, kind, true)
	if err != nil {
		result.Reason = "无法创建检测请求"
		return
	}
	actualBody := body
	if protocol.NormalizeKind(kind) == protocol.KindOpenAIChat {
		actualBody = EnsureStreamUsageOption(body, true)
	}
	proxy := s.proxyURLForTarget(target.Channel, target.Provider)
	if proxy == "" {
		if resolved, proxyErr := http.ProxyFromEnvironment(req); proxyErr == nil && resolved != nil {
			proxy = resolved.String()
		}
	}
	cacheKey := candyCheckSharedKey(target, kind, model, group.CandyCheckReasoningEffort, path, actualBody, req.Header, proxy)
	result.SharedCacheKey = cacheKey
	interval := group.CandyCheckIntervalMinutes
	if interval < 1 {
		interval = 5
	}
	maxAge := time.Duration(interval) * time.Minute
	for attempt := 0; attempt < 3; attempt++ {
		startedWaiting := time.Now()
		acquired, acquireErr := s.Routes.AcquireSharedCandyCheck(cacheKey, startedWaiting, candyCheckTimeout+30*time.Second, maxAge, manual)
		if acquireErr != nil {
			result.Reason = "共享检测状态不可用"
			return
		}
		if acquired.Cached != nil {
			return applySharedCandyResult(result, acquired.Cached)
		}
		if acquired.InFlight {
			waitCtx, waitCancel := context.WithTimeout(ctx, candyCheckTimeout+30*time.Second)
			row, waitErr := s.Routes.WaitForSharedCandyCheck(waitCtx, cacheKey, 50*time.Millisecond)
			waitCancel()
			if waitErr != nil {
				result.Status, result.Reason = "deferred", "等待同源检测完成超时"
				return
			}
			if candyCheckSharedUsable(row, time.Now(), maxAge) && sharedCandyObservationChanged(row, acquired) {
				// Consume only a newly published observation. A released lease can
				// expose an old row or pending seed and must be claimed again.
				return applySharedCandyResult(result, row)
			}
			continue
		}
		if acquired.Lease != nil {
			result.SharedLeaseToken = acquired.Lease.LeaseToken
			break
		}
	}
	if result.SharedLeaseToken == "" {
		result.Status, result.Reason = "deferred", "同源检测租约未取得"
		return
	}
	queueCtx, queueCancel := context.WithTimeout(ctx, 2*time.Second)
	release, err := s.runtime().acquireUpstreamConcurrency(queueCtx, target)
	queueCancel()
	if err != nil {
		result.Status, result.Reason = "deferred", "渠道忙，延后检测"
		return
	}
	defer release()
	return s.probeCandyRequest(ctx, target, kind, body, path, req, result)
}

func candyCheckSharedUsable(row *storage.GatewayCandyCheckSharedResult, now time.Time, maxAge time.Duration) bool {
	if row == nil || row.ObservedAt == nil || row.ObservedAt.After(now) {
		return false
	}
	switch row.Status {
	case "correct", "incorrect":
		return row.ObservedAt.Add(maxAge).After(now)
	case "error":
		return row.RetryUntil != nil && row.RetryUntil.After(now)
	default:
		return false
	}
}

func sharedCandyObservationChanged(row *storage.GatewayCandyCheckSharedResult, acquired *storage.GatewayCandyCheckSharedAcquire) bool {
	if row == nil || row.ObservedAt == nil || acquired == nil {
		return false
	}
	return acquired.PreviousObservedAt == nil || row.ObservedAt.After(*acquired.PreviousObservedAt) || row.SourceRequestID != acquired.PreviousRequestID
}

func applySharedCandyResult(result storage.GatewayRouteCandyCheck, shared *storage.GatewayCandyCheckSharedResult) storage.GatewayRouteCandyCheck {
	if shared == nil {
		return result
	}
	result.Status, result.Model = shared.Status, shared.Model
	result.AnswerPreview, result.Reason = shared.AnswerPreview, shared.Reason
	result.StatusCode, result.LatencyMS = shared.StatusCode, shared.LatencyMS
	result.Reused = true
	result.SourceRouteID, result.SourceRequestID = shared.SourceRouteID, shared.SourceRequestID
	result.ObservedAt = shared.ObservedAt
	result.SharedLeaseToken = ""
	return result
}

func (s *Service) probeCandyRouteHTTP(parent context.Context, group *storage.GatewayGroup, route storage.GatewayRoute, model string) (result storage.GatewayRouteCandyCheck) {
	result.Status, result.Model, result.SourceRouteID = "error", model, route.ID
	start := time.Now()
	defer func() {
		if !result.Reused {
			result.LatencyMS = time.Since(start).Milliseconds()
		}
	}()
	allowed, err := RouteAllowsUpstreamModel(&route, model)
	if err != nil || !allowed || model == "" || mediaGenerationModel(model) {
		result.Status, result.Reason = "skipped", "检测模型不在该路由的支持范围内，或不是文本模型"
		return
	}
	target, err := s.resolveUpstreamTarget(&route)
	if err != nil {
		result.Status, result.Reason = "skipped", "上游未就绪，请检查渠道状态及上游密钥"
		return
	}
	if target.Provider != nil {
		allowed, err = ProviderAllowsUpstreamModel(target.Provider, model)
		if err != nil || !allowed {
			result.Status, result.Reason = "skipped", "直连渠道不支持检测模型"
			return
		}
	}
	s.applyRouteUserAgentForAdmin(target, group, &route)
	kindSetting := s.normalizeUpstreamProtocol(route.UpstreamProtocol)
	if kindSetting == storage.GatewayUpstreamProtocolAuto && target.Provider != nil {
		kindSetting = s.normalizeProviderProtocol(target.Provider.UpstreamProtocol)
	}
	kind := protocol.ResolveUpstream(kindSetting, protocol.KindOpenAIChat, model)
	body, path := candyCheckRequest(kind, model, group.CandyCheckReasoningEffort)
	ctx, cancel := context.WithTimeout(parent, candyCheckTimeout)
	defer cancel()
	req, err := s.buildUpstreamHTTPRequest(ctx, target, path, http.MethodPost, http.Header{"Accept": []string{"text/event-stream"}}, body, kind, true)
	if err != nil {
		result.Reason = "无法创建检测请求"
		return
	}
	queueCtx, queueCancel := context.WithTimeout(ctx, 2*time.Second)
	release, err := s.runtime().acquireUpstreamConcurrency(queueCtx, target)
	queueCancel()
	if err != nil {
		result.Status, result.Reason = "deferred", "渠道忙，延后检测"
		return
	}
	defer release()
	return s.probeCandyRequest(ctx, target, kind, body, path, req, result)
}

func (s *Service) probeCandyRequest(ctx context.Context, target *upstreamTarget, kind protocol.Kind, body []byte, path string, req *http.Request, result storage.GatewayRouteCandyCheck) storage.GatewayRouteCandyCheck {
	// Create the audit identity only at the actual outbound attempt. Shared
	// cache consumers never enter this function and therefore create no row.
	requestID := apiaudit.NewRequestID()
	result.SourceRequestID = requestID
	meta := target.AuditMetadata
	meta.Purpose = storage.GatewayAPICallPurposeCandyCheck
	meta.RequestID = requestID
	meta.Model = result.Model
	meta.RouteID = result.SourceRouteID
	req = req.WithContext(apiaudit.WithMetadata(req.Context(), meta))
	resp, err := s.runtime().httpClientForTarget(target.Channel, target.Provider).Do(req)
	if err != nil {
		result.Reason = "检测请求失败或超时"
		return result
	}
	defer resp.Body.Close()
	result.StatusCode = resp.StatusCode
	data, err := s.runtime().readCandyCheckBody(resp.Body, resp.Header)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Reason = s.candyCheckHTTPError(resp.StatusCode, data, target, req.Header)
		return result
	}
	if err != nil {
		result.Reason = "检测响应未完整接收或超时"
		return result
	}
	if len(data) > candyCheckBodyLimit {
		result.Reason = "检测响应超过 2 MiB 限制"
		return result
	}
	answer, err := parseCandyCheckAnswer(data, resp.Header)
	result.AnswerPreview = string([]rune(answer)[:min(len([]rune(answer)), 512)])
	if err != nil {
		result.Reason = err.Error()
		return result
	}
	result.Status = "incorrect"
	result.Reason = "糖果题未通过：最终答案应为 21"
	if candyCheckAnswerCorrect(answer) {
		result.Status, result.Reason = "correct", ""
	}
	observed := time.Now()
	result.ObservedAt = &observed
	return result
}

func (rt *Runtime) readCandyCheckBody(body io.Reader, headers http.Header) ([]byte, error) {
	reader := bufio.NewReader(body)
	prefix, _ := reader.Peek(6)
	if !strings.Contains(strings.ToLower(headers.Get("Content-Type")), "text/event-stream") && !bytes.HasPrefix(prefix, []byte("data:")) && !bytes.HasPrefix(prefix, []byte("event:")) {
		return io.ReadAll(io.LimitReader(reader, candyCheckBodyLimit+1))
	}
	var data, frame bytes.Buffer
	scanner := bufio.NewScanner(io.LimitReader(reader, candyCheckBodyLimit+1))
	scanner.Buffer(make([]byte, 4096), candyCheckBodyLimit+1)
	for scanner.Scan() {
		line := scanner.Bytes()
		data.Write(line)
		data.WriteByte('\n')
		frame.Write(line)
		frame.WriteByte('\n')
		if data.Len() > candyCheckBodyLimit {
			return data.Bytes(), nil
		}
		if len(bytes.TrimSpace(line)) == 0 {
			if candyCheckFrameTerminal(frame.Bytes()) {
				return data.Bytes(), nil
			}
			frame.Reset()
		}
	}
	return data.Bytes(), scanner.Err()
}

func candyCheckFrameTerminal(frame []byte) bool {
	for _, payload := range sseDataPayloads(frame) {
		if bytes.Equal(bytes.TrimSpace(payload), []byte("[DONE]")) {
			return true
		}
		var obj map[string]any
		if json.Unmarshal(payload, &obj) != nil {
			continue
		}
		switch obj["type"] {
		case "message_stop", "error", "response.completed", "response.done", "response.failed", "response.incomplete", "response.error":
			return true
		}
		// Usage can arrive before any answer (Anthropic message_start) or be
		// null in chat deltas. Only protocol terminal markers end this read;
		// finish_reason and usage alone must leave room for the complete tail.
	}
	return false
}

func candyCheckSharedKey(target *upstreamTarget, kind protocol.Kind, model, effort, path string, body []byte, headers http.Header, proxyURL string) string {
	// The key is intentionally source-scoped, never gateway-group/route-scoped.
	// Secrets are represented only by SHA-256; raw API keys and extra-header
	// values never enter the database or the key string.
	parts := []string{
		strings.TrimRight(strings.TrimSpace(target.BaseURL), "/"),
		hex.EncodeToString(sha256Bytes([]byte(target.APIKey))),
		string(protocol.NormalizeKind(kind)), strings.TrimSpace(model), strings.TrimSpace(effort),
		path, hex.EncodeToString(sha256Bytes(body)), strings.TrimSpace(proxyURL),
	}
	parts = append(parts, "candy-puzzle-v1")
	canonical := make(map[string][]string, len(headers))
	for name, values := range headers {
		canonical[strings.ToLower(strings.TrimSpace(name))] = values
	}
	names := make([]string, 0, len(canonical))
	for name := range canonical {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		parts = append(parts, name, hex.EncodeToString(sha256Bytes([]byte(strings.Join(canonical[name], "\x00")))))
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x1f")))
	return hex.EncodeToString(sum[:])
}

func sha256Bytes(value []byte) []byte {
	sum := sha256.Sum256(value)
	return sum[:]
}

func candyCheckRequest(kind protocol.Kind, model, effort string) ([]byte, string) {
	// Some upstreams apply stricter limits to synchronous requests, even for
	// this short puzzle. Use the same streaming path as normal gateway traffic.
	payload := map[string]any{"model": model, "stream": true}
	path := "/v1/chat/completions"
	switch kind {
	case protocol.KindAnthropic:
		path = "/v1/messages"
		payload["messages"] = []map[string]string{{"role": "user", "content": candyCheckPrompt}}
		payload["max_tokens"] = 4096
	case protocol.KindOpenAIResponses:
		path = "/v1/responses"
		payload["input"], payload["max_output_tokens"] = candyCheckPrompt, 4096
		if effort != "" && effort != "default" {
			payload["reasoning"] = map[string]string{"effort": effort}
		}
	default:
		payload["messages"] = []map[string]string{{"role": "user", "content": candyCheckPrompt}}
		payload["max_completion_tokens"] = 4096
		if effort != "" && effort != "default" {
			payload["reasoning_effort"] = effort
		}
	}
	body, _ := json.Marshal(payload)
	return body, path
}
