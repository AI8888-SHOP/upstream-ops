// SSE 帧解析与流式路径无状态辅助。
package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

const maxResponsesLifecycleBufferBytes = 1 << 20

func (rt *Runtime) parseSSEEventLines(lines []string) (eventName, data string) {
	var dataParts []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "event:") {
			eventName = strings.TrimSpace(strings.TrimPrefix(trimmed, "event:"))
			continue
		}
		if strings.HasPrefix(trimmed, "data:") {
			dataParts = append(dataParts, strings.TrimSpace(strings.TrimPrefix(trimmed, "data:")))
		}
	}
	data = strings.Join(dataParts, "\n")
	return eventName, data
}

// responsesSSEEventIsLifecycle reports metadata-only Responses events. They
// describe request state but do not contain client-visible model output.
func responsesSSEEventIsLifecycle(eventName, data string) bool {
	eventType := strings.TrimSpace(eventName)
	payload := strings.TrimSpace(data)
	if payload != "" && payload != "[DONE]" {
		var envelope struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(payload), &envelope); err == nil && strings.TrimSpace(envelope.Type) != "" {
			eventType = envelope.Type
		}
	}
	return isResponsesLifecycleEvent(eventType)
}

// sseEventHasPayload 判断 SSE 事件是否包含可提交的有效载荷（非纯注释）。
func (rt *Runtime) sseEventHasPayload(lines []string) bool {
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, ":") {
			continue
		}
		if strings.HasPrefix(trimmed, "event:") || strings.HasPrefix(trimmed, "data:") ||
			strings.HasPrefix(trimmed, "id:") || strings.HasPrefix(trimmed, "retry:") {
			return true
		}
		// 非注释、非空行：视为有效
		return true
	}
	return false
}

// sseFrameIsTerminal 判断已写出（或即将写出）的 SSE 帧是否为客户端侧流结束标记。
// 用于区分「中途断开」与「收完终端帧后正常关连接」。
// 覆盖：Chat [DONE]、Anthropic message_stop、Responses response.completed 等。
func (rt *Runtime) sseFrameIsTerminal(frame []byte) bool {
	if len(frame) == 0 {
		return false
	}
	s := string(frame)
	// OpenAI Chat 系：data: [DONE]
	if strings.Contains(s, "data: [DONE]") || strings.Contains(s, "data:[DONE]") {
		return true
	}
	// Anthropic：message_stop / error 事件
	if strings.Contains(s, "event: message_stop") || strings.Contains(s, "event:message_stop") {
		return true
	}
	if strings.Contains(s, `"type":"message_stop"`) || strings.Contains(s, `"type": "message_stop"`) {
		return true
	}
	if strings.Contains(s, "event: error") || strings.Contains(s, "event:error") {
		return true
	}
	// OpenAI Responses（/v1/responses）：无 [DONE]，以 response.completed / failed 等收尾。
	// 若漏识别，客户端正常收完后关连接会被误记为 error_type=client。
	if strings.Contains(s, "event: response.completed") || strings.Contains(s, "event:response.completed") ||
		strings.Contains(s, "event: response.done") || strings.Contains(s, "event:response.done") ||
		strings.Contains(s, "event: response.failed") || strings.Contains(s, "event:response.failed") ||
		strings.Contains(s, "event: response.incomplete") || strings.Contains(s, "event:response.incomplete") ||
		strings.Contains(s, "event: response.error") || strings.Contains(s, "event:response.error") ||
		strings.Contains(s, "event: response.canceled") || strings.Contains(s, "event:response.canceled") ||
		strings.Contains(s, "event: response.cancelled") || strings.Contains(s, "event:response.cancelled") {
		return true
	}
	if strings.Contains(s, `"type":"response.completed"`) || strings.Contains(s, `"type": "response.completed"`) ||
		strings.Contains(s, `"type":"response.done"`) || strings.Contains(s, `"type": "response.done"`) ||
		strings.Contains(s, `"type":"response.failed"`) || strings.Contains(s, `"type": "response.failed"`) ||
		strings.Contains(s, `"type":"response.incomplete"`) || strings.Contains(s, `"type": "response.incomplete"`) ||
		strings.Contains(s, `"type":"response.error"`) || strings.Contains(s, `"type": "response.error"`) ||
		strings.Contains(s, `"type":"response.canceled"`) || strings.Contains(s, `"type": "response.canceled"`) ||
		strings.Contains(s, `"type":"response.cancelled"`) || strings.Contains(s, `"type": "response.cancelled"`) ||
		strings.Contains(s, `"type":"error"`) || strings.Contains(s, `"type": "error"`) {
		return true
	}
	return false
}

// finalizeStreamClientDisconnect 在流结束时整理 client 断开标记：
// - 已向客户端交付终端帧：关连接属正常收尾，清除 ClientDisconnected / errClientDisconnected
// - 中途断开：补上 StreamErr=errClientDisconnected 供上层记 error_type=client
func (rt *Runtime) finalizeStreamClientDisconnect(r *streamAttemptResult) {
	if r == nil || !r.ClientDisconnected {
		return
	}
	if r.DownstreamComplete {
		r.ClientDisconnected = false
		if r.StreamErr != nil && errors.Is(r.StreamErr, errClientDisconnected) {
			r.StreamErr = nil
		}
		return
	}
	if r.StreamErr == nil {
		r.StreamErr = errClientDisconnected
	}
}

func (rt *Runtime) commitSSEHeaders(c *gin.Context, upstream http.Header) error {
	if c == nil {
		return errors.New("nil gin context")
	}
	if _, ok := c.Writer.(http.Flusher); !ok {
		return errors.New("streaming not supported")
	}
	if upstream != nil {
		rt.copyResponseHeaders(c.Writer.Header(), upstream)
	}
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")
	c.Writer.Header().Del("Content-Length")
	reqID := rt.ensureGatewayRequestID(c)
	rt.setGatewayRequestIDHeaders(c, reqID)
	c.Status(http.StatusOK)
	return nil
}

func (rt *Runtime) flushWriter(c *gin.Context) {
	if c == nil {
		return
	}
	if f, ok := c.Writer.(http.Flusher); ok {
		f.Flush()
	}
}

func (rt *Runtime) streamKeepaliveFrame(clientKind protocolKind) []byte {
	if protocol.NormalizeKind(clientKind) == protocol.KindAnthropic {
		return []byte("event: ping\ndata: {\"type\":\"ping\"}\n\n")
	}
	return []byte(":\n\n")
}

// writeStreamTerminalError 在已 commit 的 SSE 流上写入终端错误（只应调用一次）。
func (rt *Runtime) writeStreamTerminalError(c *gin.Context, kind protocolKind, errType, message string) error {
	if c == nil {
		return errors.New("nil context")
	}
	if errType == "" {
		errType = "api_error"
	}
	if message == "" {
		message = errType
	}
	var payload []byte
	if protocol.NormalizeKind(kind) == protocol.KindAnthropic {
		body, _ := json.Marshal(map[string]any{
			"type": "error",
			"error": map[string]string{
				"type":    errType,
				"message": message,
			},
		})
		payload = []byte(fmt.Sprintf("event: error\ndata: %s\n\n", body))
	} else {
		body, _ := json.Marshal(map[string]any{
			"error": map[string]any{
				"message": message,
				"type":    errType,
			},
		})
		payload = []byte(fmt.Sprintf("data: %s\n\ndata: [DONE]\n\n", body))
	}
	if _, err := c.Writer.Write(payload); err != nil {
		return err
	}
	rt.flushWriter(c)
	return nil
}

func (rt *Runtime) mergeStreamUsage(dst *UsageTokens, data string, kind protocolKind) {
	if dst == nil || data == "" {
		return
	}
	if !mayContainUsageFields(data, kind) {
		return
	}
	var u UsageTokens
	if protocol.NormalizeKind(kind) == protocol.KindAnthropic {
		u = ParseAnthropicUsage([]byte(data))
		// Anthropic SSE 中 usage 常嵌在 message_start / message_delta
		if !usageNonEmpty(u) {
			var raw map[string]any
			if json.Unmarshal([]byte(data), &raw) == nil {
				if msg, ok := raw["message"].(map[string]any); ok {
					if usageObj, ok := msg["usage"].(map[string]any); ok {
						b, _ := json.Marshal(map[string]any{"usage": usageObj})
						u = ParseAnthropicUsage(b)
					}
				}
				if usageObj, ok := raw["usage"].(map[string]any); ok {
					b, _ := json.Marshal(map[string]any{"usage": usageObj})
					u = ParseAnthropicUsage(b)
				}
			}
		}
	} else {
		u = ParseOpenAIUsage([]byte(data))
	}
	if usageNonEmpty(u) {
		*dst = mergeUsagePreferNewer(*dst, u)
	}
}

// mayContainUsageFields is intentionally a cheap substring prefilter. Most
// SSE chunks are ordinary content deltas and do not contain usage at all;
// avoiding json.Unmarshal for those chunks removes a large amount of
// allocation and CPU from the streaming hot path. The actual parsers remain
// authoritative.
func mayContainUsageFields(data string, kind protocolKind) bool {
	if len(data) == 0 {
		return false
	}
	for _, marker := range usageFieldMarkers {
		if strings.Contains(data, marker) {
			return true
		}
	}
	// JSON permits escaped object keys. Falling through to the authoritative
	// parser preserves accounting for payloads such as us\u0061ge even though
	// the cheap literal markers cannot recognize the decoded field name.
	if strings.Contains(data, `\u`) {
		return true
	}
	// Some providers emit bare numeric token fields without JSON quoting in a
	// pre-parsed adapter. Keep the conservative fallback for non-JSON data.
	return protocol.NormalizeKind(kind) == protocol.KindAnthropic && strings.Contains(data, anthropicInputTokensMarker)
}

// Keep these byte slices package-global.  This function runs for every SSE
// event; constructing a marker slice on each call would turn a cheap filter
// into an allocation-heavy hot path.
var usageFieldMarkers = []string{
	`"usage"`,
	`"prompt_tokens"`,
	`"completion_tokens"`,
	`"input_tokens"`,
	`"output_tokens"`,
	`"prompt_cache_hit_tokens"`,
	`"cache_hit_tokens"`,
	`"cache_read_input_tokens"`,
	`"cached_tokens"`,
	`"cache_read`,
	`"cache_creation`,
	`"cache_write`,
	`"image_tokens"`,
	`"reasoning_tokens"`,
}

var anthropicInputTokensMarker = "input_tokens"

func (rt *Runtime) finalizeStreamTokens(live UsageTokens, raw []byte, kind protocolKind) UsageTokens {
	if usageNonEmpty(live) {
		// 再用整包兜底补 cache 等字段
		if len(raw) > 0 {
			fallback := rt.parseUsageByKind(raw, true, kind)
			if live.CacheReadTokens == 0 && fallback.CacheReadTokens > 0 {
				live.CacheReadTokens = fallback.CacheReadTokens
			}
			if live.CacheCreationTokens == 0 && fallback.CacheCreationTokens > 0 {
				live.CacheCreationTokens = fallback.CacheCreationTokens
			}
			if live.InputTokens == 0 && fallback.InputTokens > 0 {
				live.InputTokens = fallback.InputTokens
			}
			if live.OutputTokens == 0 && fallback.OutputTokens > 0 {
				live.OutputTokens = fallback.OutputTokens
			}
		}
		return live
	}
	if len(raw) == 0 {
		return live
	}
	return rt.parseUsageByKind(raw, true, kind)
}
