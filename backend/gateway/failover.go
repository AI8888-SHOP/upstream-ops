// 故障转移状态判定与客户端断开语义。
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

func (svc *Service) isFailoverStatus(code int, failoverOn4xx bool) bool {
	if code == 0 {
		return true
	}
	if code == http.StatusTooManyRequests || code == http.StatusRequestTimeout {
		return true
	}
	// Every response-level 4xx is route-local from the gateway's point of
	// view.  A provider may use 400/401/403/404 for an unavailable model,
	// credential, group, or capability; forwarding the first such response
	// leaks a recoverable upstream failure to the downstream caller.  The
	// legacy failoverOn4xx argument is kept for API/source compatibility, but
	// 4xx failover is now always enabled.  Caller authentication and request
	// parsing errors are handled before an upstream route is selected and are
	// unaffected by this policy.
	if code >= http.StatusBadRequest && code < http.StatusInternalServerError {
		return true
	}
	if code >= 500 {
		return true
	}
	_ = failoverOn4xx
	return false
}

// A model/key/quota failure belongs to this upstream, not necessarily to the
// caller. Recognize only error envelopes by default; a caller that has already
// verified an HTTP error status may also opt into plain-text error bodies.
// Never scan successful assistant text or tool arguments.
func upstreamRouteUnavailable(body []byte, allowPlainText ...bool) bool {
	if matchUpstreamErrorEnvelope(body, func(code, message string) bool {
		if strings.Contains(code, "context_length") || strings.Contains(code, "content_filter") {
			return false
		}
		for _, marker := range []string{"model_not_found", "model_not_available", "model_unavailable", "unsupported_model", "model_not_supported", "invalid_api_key", "authentication_error", "permission_error", "access_denied", "unauthorized", "group_not_allowed", "insufficient_quota", "insufficient_balance"} {
			if strings.Contains(code, marker) {
				return true
			}
		}
		return isDeterministicRouteFailure(message)
	}) {
		return true
	}
	// Only a verified HTTP error may use plain text; malformed/unknown JSON or
	// successful assistant prose must never affect shared source health.
	trimmed := bytes.TrimSpace(body)
	return len(allowPlainText) > 0 && allowPlainText[0] && len(trimmed) > 0 &&
		trimmed[0] != '{' && trimmed[0] != '[' && !looksLikeSSEBody(trimmed) &&
		isDeterministicRouteFailure(string(trimmed))
}

// Match only error/error.response envelopes, never text or arguments emitted
// by the assistant. In SSE, inspect every event rather than only the first
// lifecycle event, which commonly precedes the actual failure.
func matchUpstreamErrorEnvelope(body []byte, match func(code, message string) bool) bool {
	check := func(payload []byte) bool {
		if code, message, ok := rootLevelFailure(payload); ok {
			if match(strings.ToLower(code), message) {
				return true
			}
		}
		var envelope struct {
			Error    json.RawMessage `json:"error"`
			Response struct {
				Error json.RawMessage `json:"error"`
			} `json:"response"`
		}
		if json.Unmarshal(payload, &envelope) != nil {
			return false
		}
		for _, raw := range []json.RawMessage{envelope.Error, envelope.Response.Error} {
			var failure struct {
				Code    json.RawMessage `json:"code"`
				Type    string          `json:"type"`
				Message string          `json:"message"`
			}
			if json.Unmarshal(raw, &failure) != nil {
				if json.Unmarshal(raw, &failure.Message) != nil {
					continue
				}
			}
			var code string
			// Providers may use a numeric code. Keep the message available even
			// when the code is not a string; only named codes affect classification.
			_ = json.Unmarshal(failure.Code, &code)
			if match(strings.ToLower(code+" "+failure.Type), strings.TrimSpace(failure.Message)) {
				return true
			}
		}
		return false
	}
	if check(body) {
		return true
	}
	if !looksLikeSSEBody(body) {
		return false
	}
	for _, payload := range sseDataPayloads(body) {
		if check(payload) {
			return true
		}
	}
	return false
}

func (svc *Service) isFailoverResponse(status int, failoverOn4xx bool, body []byte) bool {
	if svc.isFailoverStatus(status, failoverOn4xx) {
		return true
	}
	// Some adapters/proxies return a protocol error envelope with HTTP 200.
	// It is still an upstream failure and must be allowed to switch routes
	// before any response bytes are committed downstream.
	if status >= http.StatusOK && status < http.StatusMultipleChoices {
		return hasExplicitUpstreamError(body)
	}
	return false
}

// hasExplicitUpstreamError identifies a protocol-level failure envelope even
// when the upstream incorrectly returns HTTP 200.  It deliberately inspects
// only root error fields / response.error and failure event types; assistant
// prose, tool arguments, and arbitrary nested JSON are not considered.
func hasExplicitUpstreamError(body []byte) bool {
	check := func(payload []byte) bool {
		if _, _, ok := rootLevelFailure(payload); ok {
			return true
		}
		var envelope struct {
			Type     string          `json:"type"`
			Status   string          `json:"status"`
			Error    json.RawMessage `json:"error"`
			Response json.RawMessage `json:"response"`
		}
		if json.Unmarshal(payload, &envelope) != nil {
			return false
		}
		if streamErrorValuePresent(envelope.Error) {
			return true
		}
		if len(bytes.TrimSpace(envelope.Response)) > 0 && !bytes.Equal(bytes.TrimSpace(envelope.Response), []byte("null")) {
			var response struct {
				Status string          `json:"status"`
				Error  json.RawMessage `json:"error"`
			}
			if json.Unmarshal(envelope.Response, &response) == nil {
				if streamErrorValuePresent(response.Error) {
					return true
				}
				if isExplicitFailureState(response.Status) {
					return true
				}
			}
		}
		return isExplicitFailureState(envelope.Type) || isExplicitFailureState(envelope.Status)
	}

	if check(body) {
		return true
	}
	if !looksLikeSSEBody(body) {
		return false
	}
	for _, frame := range bytes.Split(bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n")), []byte("\n\n")) {
		if len(bytes.TrimSpace(frame)) == 0 {
			continue
		}
		eventName, data := (*Runtime)(nil).parseSSEEventLines(strings.Split(string(frame), "\n"))
		if upstreamStreamFailure(eventName, data) != nil || check([]byte(data)) {
			return true
		}
	}
	return false
}

// rootLevelFailure recognises the incompatible "error" shapes upstream panels
// put at the JSON root:
//
//	{"error": {...}}                              OpenAI / Anthropic
//	{"code": 500, "msg": "..."}                   one-api / new-api style
//	{"code": "model_not_found", "message": "..."} sub2api style
//	{"errors": [{"message": "..."}]}              proxy style
//
// A successful payload always wins: proxies sometimes mirror the HTTP status
// into a root code while returning a normal completion, so the usual success
// markers short-circuit the check. Only the root object is inspected, never
// assistant prose, tool arguments, or nested provider data.
func rootLevelFailure(payload []byte) (code, message string, ok bool) {
	var root map[string]json.RawMessage
	if json.Unmarshal(payload, &root) != nil || len(root) == 0 {
		return "", "", false
	}
	if _, hasError := root["error"]; !hasError {
		if _, hasErrors := root["errors"]; !hasErrors {
			for _, marker := range []string{"choices", "output", "candidates", "data"} {
				if raw, present := root[marker]; present && streamErrorValuePresent(raw) {
					return "", "", false
				}
			}
		}
	}
	if raw, present := root["error"]; present && streamErrorValuePresent(raw) {
		var failure struct {
			Code    json.RawMessage `json:"code"`
			Type    string          `json:"type"`
			Message string          `json:"message"`
		}
		if json.Unmarshal(raw, &failure) != nil {
			_ = json.Unmarshal(raw, &failure.Message)
		}
		var named string
		_ = json.Unmarshal(failure.Code, &named)
		return strings.TrimSpace(named + " " + failure.Type), strings.TrimSpace(failure.Message), true
	}
	if raw, present := root["errors"]; present {
		var list []json.RawMessage
		if json.Unmarshal(raw, &list) == nil && len(list) > 0 {
			var first struct {
				Code    json.RawMessage `json:"code"`
				Message string          `json:"message"`
			}
			_ = json.Unmarshal(list[0], &first)
			var named string
			_ = json.Unmarshal(first.Code, &named)
			return strings.TrimSpace(named), strings.TrimSpace(first.Message), true
		}
	}
	codeRaw, hasCode := root["code"]
	messageRaw, hasMessage := root["message"]
	if !hasMessage {
		messageRaw, hasMessage = root["msg"]
	}
	if !hasCode || !hasMessage {
		return "", "", false
	}
	failure, named := classifyRootCode(codeRaw)
	if !failure {
		return "", "", false
	}
	_ = json.Unmarshal(messageRaw, &message)
	return named, strings.TrimSpace(message), true
}

// classifyRootCode maps the two success conventions (0 and any 2xx, numeric or
// numeric string) and an explicit allowlist of success words. Anything else is
// a protocol failure code.
func classifyRootCode(raw json.RawMessage) (bool, string) {
	var number float64
	if json.Unmarshal(raw, &number) == nil {
		if number == 0 || (number >= 200 && number < 300) {
			return false, ""
		}
		return true, strconv.Itoa(int(number))
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return false, ""
	}
	trimmed := strings.TrimSpace(text)
	switch strings.ToLower(trimmed) {
	case "", "0", "ok", "success", "succeed", "succeeded", "fine", "completed", "done", "true":
		return false, ""
	}
	if numeric, err := strconv.Atoi(trimmed); err == nil {
		if numeric == 0 || (numeric >= 200 && numeric < 300) {
			return false, ""
		}
	}
	return true, trimmed
}

func isExplicitFailureState(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "error", "failed", "canceled", "cancelled", "response.error", "response.failed", "response.canceled", "response.cancelled":
		return true
	default:
		return false
	}
}

// isSameRouteRetryableUpstreamFailure distinguishes transient failures from
// responses that cannot succeed by sending the same request to the same route
// again. The caller may still fail over to another route when its policy allows
// it. Saturation and deterministic failures switch sources; other transient
// transport/HTTP failures retain the configured same-route retry policy.
func isSameRouteRetryableUpstreamFailure(status int, info usageErrorInfo) bool {
	// Replaying a 4xx request on the same source cannot repair an upstream
	// route/key/model mismatch.  Move to another source immediately; if no
	// source succeeds the caller receives the gateway's final failure.
	if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		return false
	}
	if status >= http.StatusOK && status < http.StatusMultipleChoices && hasExplicitUpstreamError([]byte(info.UpstreamBody)) {
		return false
	}
	if upstreamRouteUnavailable([]byte(info.UpstreamBody), status >= 400) {
		return false
	}
	// A deterministic upstream identity/permission/capability failure cannot
	// improve by replaying the same request on the same source, regardless of
	// whether an adapter surfaced it as HTTP 4xx or a pre-commit HTTP 200 error.
	if isDeterministicRouteFailure(info.Summary) {
		return false
	}
	if isUpstreamWaitFailure(info.Summary) || isPreconnectIntegrityFailure(info.Summary) {
		return false
	}
	// Validation errors may retain only a generic rule summary. Extract the
	// actual error from its envelope, rather than searching assistant content.
	if matchUpstreamErrorEnvelope([]byte(info.UpstreamBody), func(_ string, message string) bool {
		return isUpstreamWaitFailure(message) || isPreconnectIntegrityFailure(message)
	}) {
		return false
	}
	// Retrying a saturated source immediately amplifies its queue. Fail over
	// instead; other transient transport/HTTP failures retain their retry policy.
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable ||
		status == http.StatusRequestTimeout || status == http.StatusGatewayTimeout || status == 524 {
		return false
	}
	return true
}

// shouldCooldownUpstreamFailure separates route-health failures from caller
// input failures.  All 4xx responses are eligible for route failover, but a
// generic 400 such as invalid input must not cool every route in a group.  A
// source-specific error envelope (model/key/permission/quota/capability) is
// safe to cool; transient HTTP/transport failures are safe as well.
func shouldCooldownUpstreamFailure(status int, info usageErrorInfo, attemptErr error) bool {
	if status == 0 {
		return attemptErr != nil
	}
	if status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500 {
		return true
	}
	if status >= http.StatusBadRequest && status < http.StatusInternalServerError {
		body := []byte(info.UpstreamBody)
		return upstreamRouteUnavailable(body, true) || isDeterministicRouteFailure(info.Summary)
	}
	if status >= http.StatusOK && status < http.StatusMultipleChoices && hasExplicitUpstreamError([]byte(info.UpstreamBody)) {
		return hasStructuredUpstreamError([]byte(info.UpstreamBody))
	}
	return attemptErr != nil
}

func isDeterministicRouteFailure(value string) bool {
	text := strings.ToLower(strings.TrimSpace(value))
	for _, marker := range []string{
		"image generation is not enabled",
		"video generation is not enabled",
		"not enabled for this group",
		"insufficient account balance",
		"insufficient balance",
		"insufficient_quota",
		"model_not_found",
		"model not found",
		"the requested model is unavailable",
		"no available channel for model",
		"no available compatible accounts",
		"invalid api key",
		"invalid_api_key",
		"unauthorized",
		"permission denied",
		"not authorized",
		"access denied",
		"not allowed to access",
		"not allowed to use",
		"does not have permission",
		"group_not_allowed",
		"group deleted",
		"模型不存在",
		"找不到模型",
		"无权访问",
		"无权使用",
		"无权限",
		"没有权限",
		"分组已删除",
		"分组不再允许当前用户使用",
		"余额不足",
		"用户额度不足",
	} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return strings.Contains(text, "model") && (strings.Contains(text, "does not exist") ||
		strings.Contains(text, "was not found") || strings.Contains(text, "not supported by any configured account"))
}

// A costly wait should switch source without being classified as a permanent
// model, credential or permission failure.
func isUpstreamWaitFailure(value string) bool {
	text := strings.ToLower(strings.TrimSpace(value))
	return isFirstTokenBudgetFailure(text) ||
		strings.Contains(text, "upstream stream idle for ")
}

func isFirstTokenBudgetFailure(value string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(value)), "request first token budget exhausted")
}

// Preconnect integrity checks belong to the gateway's stream adapter. They
// must not disable another gateway group that shares the same upstream model.
func isPreconnectIntegrityFailure(value string) bool {
	return strings.Contains(strings.ToLower(strings.TrimSpace(value)), "responses preconnect ")
}

// Only protocol error envelopes count here. A content rule matching ordinary
// prose must never poison shared upstream health.
func hasStructuredUpstreamError(body []byte) bool {
	if upstreamRouteUnavailable(body) {
		return true
	}
	if matchUpstreamErrorEnvelope(body, func(_ string, message string) bool {
		return isUpstreamWaitFailure(message)
	}) {
		return true
	}
	check := func(payload []byte) bool {
		var envelope struct {
			Error    json.RawMessage `json:"error"`
			Response struct {
				Error json.RawMessage `json:"error"`
			} `json:"response"`
		}
		if json.Unmarshal(payload, &envelope) != nil {
			return false
		}
		for _, value := range []json.RawMessage{envelope.Error, envelope.Response.Error} {
			value = bytes.TrimSpace(value)
			var failure struct {
				Type    string          `json:"type"`
				Code    json.RawMessage `json:"code"`
				Message string          `json:"message"`
			}
			if json.Unmarshal(value, &failure) != nil {
				continue
			}
			var namedCode string
			_ = json.Unmarshal(failure.Code, &namedCode)
			code := strings.ToLower(failure.Type + " " + namedCode)
			// Caller errors must not cool a model for unrelated requests.
			if strings.Contains(code, "invalid_request") || strings.Contains(code, "context_length") || strings.Contains(code, "content_filter") {
				continue
			}
			for _, marker := range []string{"overloaded", "rate_limit", "server_error", "internal_error", "internal_server_error", "service_unavailable", "upstream_error", "model_not_found", "authentication_error", "permission_error"} {
				if strings.Contains(code, marker) {
					return true
				}
			}
			message := strings.ToLower(failure.Message)
			for _, marker := range []string{"currently overloaded", "rate limit", "too many requests", "at capacity", "service unavailable", "internal server error"} {
				if strings.Contains(message, marker) {
					return true
				}
			}
		}
		return false
	}
	if check(body) {
		return true
	}
	for _, frame := range bytes.Split(bytes.ReplaceAll(body, []byte("\r\n"), []byte("\n")), []byte("\n\n")) {
		_, data := (*Runtime)(nil).parseSSEEventLines(strings.Split(string(frame), "\n"))
		if check([]byte(data)) {
			return true
		}
	}
	return false
}

// isClientDisconnectAfterCommit 流已向客户端提交后，仅因客户端中途断开/取消结束。
// 此时上游通常已正常响应（甚至完整计费），应记成功而非失败。
// 注意：若下游已写出终端帧（DownstreamComplete），forwardStream 会先清掉 ClientDisconnected，
// 不会走到此处——收完再关连接记普通成功。
func (svc *Service) isClientDisconnectAfterCommit(clientDisconnected bool, streamErr error) bool {
	if !clientDisconnected {
		return false
	}
	if streamErr == nil {
		return true
	}
	return errors.Is(streamErr, errClientDisconnected)
}

// isClientContextError 判断失败是否由「客户端断开 / 入站请求 context 取消」引起。
// 典型表现：Post "...": context canceled。若继续用同一 c.Request.Context() 重试/顺延，
// 后续 attempt 会全部被父 context 污染成同样的 canceled，并误冷却路由。
func (svc *Service) isClientContextError(err error, c *gin.Context) bool {
	if err == nil {
		return false
	}
	// 首字超时是网关主动断开，不是客户端取消
	if svc.isFirstTokenTimeout(err) {
		return false
	}
	reqCanceled := false
	if c != nil && c.Request != nil {
		if ce := c.Request.Context().Err(); ce != nil {
			reqCanceled = true
		}
	}
	if errors.Is(err, context.Canceled) {
		// 仅当入站请求 context 也已取消时视为客户端侧；避免误伤其它 Canceled
		return reqCanceled || c == nil
	}
	if errors.Is(err, context.DeadlineExceeded) && reqCanceled {
		return true
	}
	// net/http 常包装为：Post "url": context canceled
	if reqCanceled {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "context canceled") ||
			strings.Contains(msg, "context deadline exceeded") ||
			strings.Contains(msg, "client disconnected") ||
			strings.Contains(msg, "request canceled") {
			return true
		}
	}
	return false
}

// annotateClientContextError 把 transport 的 context canceled 改写为可区分的 client 错误。
func (svc *Service) annotateClientContextError(info *usageErrorInfo, c *gin.Context, upstreamURL, method string, fwdErr error) {
	if info == nil {
		return
	}
	info.Type = "client"
	info.Summary = "客户端已断开或取消请求（context canceled）"
	var b strings.Builder
	fmt.Fprintf(&b, "client context canceled — stop retry/failover\n")
	fmt.Fprintf(&b, "method: %s\nurl: %s\n", method, upstreamURL)
	if fwdErr != nil {
		fmt.Fprintf(&b, "error: %s\n", fwdErr.Error())
	}
	if c != nil && c.Request != nil {
		if ce := c.Request.Context().Err(); ce != nil {
			fmt.Fprintf(&b, "request_context: %s\n", ce.Error())
		}
	}
	b.WriteString("note: 入站请求 context 已取消，后续重试/顺延会全部失败且污染统计，已停止\n")
	// 保留已有 headers 段（若 buildUpstreamErrorInfo 写过）
	if strings.TrimSpace(info.Detail) != "" && strings.Contains(info.Detail, "headers:") {
		if idx := strings.Index(info.Detail, "headers:"); idx >= 0 {
			b.WriteString(info.Detail[idx:])
		}
	} else if strings.TrimSpace(info.UpstreamHeaders) != "" {
		// JSON headers 转不上 plain 时至少提示字段有值
		b.WriteString("headers: (see upstream_error_headers)\n")
	}
	info.Detail = b.String()
}
