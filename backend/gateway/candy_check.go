package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

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
	group, err := s.Groups.FindByID(route.GatewayGroupID)
	if err != nil {
		s.logCandyCheckError("load candy check group", route.ID, err)
		return
	}
	if !group.CandyCheckEnabled || group.Status != storage.GatewayGroupStatusActive {
		return
	}
	model, _ := ResolveModel(group.CandyCheckModel, ParseModelMapping(route.ModelMappingJSON), ParseModelMapping(group.ModelMappingJSON))
	var provider *storage.GatewayProvider
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if s.Providers == nil {
			return
		}
		provider, err = s.Providers.FindByID(route.GatewayProviderID)
		if err != nil || !provider.Enabled {
			return
		}
	}
	key := storage.GatewayCandyCheckConfigKey(group, &route, provider)
	claim, err := s.Routes.ClaimCandyCheck(route.ID, key, model, time.Now(), candyCheckTimeout+30*time.Second)
	if err != nil {
		s.logCandyCheckError("claim candy check", route.ID, err)
		return
	}
	if claim == nil {
		return
	}
	result := s.probeCandyRoute(ctx, group, route, model)
	if ctx.Err() != nil {
		result.Status = "deferred"
	}
	if _, err := s.Routes.FinishCandyCheck(*claim, result, time.Now()); err != nil {
		s.logCandyCheckError("finish candy check", route.ID, err)
	}
}

func (s *Service) probeCandyRoute(parent context.Context, group *storage.GatewayGroup, route storage.GatewayRoute, model string) (result storage.GatewayRouteCandyCheck) {
	result.Status, result.Model = "error", model
	start := time.Now()
	defer func() { result.LatencyMS = time.Since(start).Milliseconds() }()
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
	queueCtx, queueCancel := context.WithTimeout(ctx, 2*time.Second)
	release, err := s.runtime().acquireUpstreamConcurrency(queueCtx, target)
	queueCancel()
	if err != nil {
		result.Status, result.Reason = "deferred", "渠道忙，延后检测"
		return
	}
	defer release()
	req, err := s.buildUpstreamHTTPRequest(ctx, target, path, http.MethodPost, http.Header{"Accept": {"application/json"}}, body, kind, false)
	if err != nil {
		result.Reason = "无法创建检测请求"
		return
	}
	resp, err := s.runtime().httpClientForTarget(target.Channel, target.Provider).Do(req)
	if err != nil {
		result.Reason = "检测请求失败或超时"
		return
	}
	defer resp.Body.Close()
	result.StatusCode = resp.StatusCode
	data, err := s.runtime().readCandyCheckBody(resp.Body, resp.Header)
	if err != nil {
		result.Reason = "检测响应未完整接收或超时"
		return
	}
	if len(data) > candyCheckBodyLimit {
		result.Reason = "检测响应超过 2 MiB 限制"
		return
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		result.Reason = fmt.Sprintf("检测上游返回 HTTP %d", resp.StatusCode)
		return
	}
	answer, err := parseCandyCheckAnswer(data, resp.Header)
	result.AnswerPreview = string([]rune(answer)[:min(len([]rune(answer)), 512)])
	if err != nil {
		result.Reason = err.Error()
		return
	}
	result.Status = "incorrect"
	result.Reason = "糖果题未通过：最终答案应为 21"
	if candyCheckAnswerCorrect(answer) {
		result.Status, result.Reason = "correct", ""
	}
	return
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
		if choices, ok := obj["choices"].([]any); ok {
			for _, value := range choices {
				choice, _ := value.(map[string]any)
				if reason, _ := choice["finish_reason"].(string); reason != "" {
					return true
				}
			}
		}
	}
	return false
}

func candyCheckRequest(kind protocol.Kind, model, effort string) ([]byte, string) {
	payload := map[string]any{"model": model, "stream": false}
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
