package gateway

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

var errUpstreamStreamFailure = errors.New("upstream stream failed")

// Inspect only protocol envelopes, never assistant text or tool arguments.
// The caller invokes this on terminal events, not on each text delta.
func upstreamStreamFailure(eventName, data string) error {
	failed := func(value string) bool {
		switch strings.ToLower(strings.TrimSpace(value)) {
		case "error", "failed", "canceled", "cancelled", "response.error", "response.failed", "response.canceled", "response.cancelled":
			return true
		}
		return false
	}
	var envelope struct {
		Type     string          `json:"type"`
		Error    json.RawMessage `json:"error"`
		Response struct {
			Status string          `json:"status"`
			Error  json.RawMessage `json:"error"`
		} `json:"response"`
	}
	_ = json.Unmarshal([]byte(data), &envelope)
	// Root-level failure envelopes ({"error":...}, {"code":500,"msg":...},
	// {"errors":[...]}) are common on panel-style upstreams and must never be
	// mistaken for protocol content.
	if code, message, ok := rootLevelFailure([]byte(data)); ok {
		if message == "" {
			message = code
		}
		return fmt.Errorf("%w: %s", errUpstreamStreamFailure, message)
	}
	if !failed(eventName) && !failed(envelope.Type) && !failed(envelope.Response.Status) &&
		!streamErrorValuePresent(envelope.Error) && !streamErrorValuePresent(envelope.Response.Error) {
		// Incomplete due to max_output_tokens/content_filter is a normal stop,
		// unless the envelope also explicitly reports an error.
		return nil
	}
	message := extractResponseErrorMessage([]byte(data), nil)
	if message == "" {
		message = strings.TrimSpace(envelope.Type)
	}
	if message == "" {
		message = strings.TrimSpace(eventName)
	}
	if message == "" {
		message = envelope.Response.Status
	}
	return fmt.Errorf("%w: %s", errUpstreamStreamFailure, message)
}

func streamErrorValuePresent(raw []byte) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && !bytes.Equal(raw, []byte("null")) &&
		!bytes.Equal(raw, []byte("false")) && !bytes.Equal(raw, []byte(`""`))
}

func streamEnvelopeHasError(payload []byte) bool {
	value, ok := partialJSONRootMember(payload, "error")
	if !ok {
		return false
	}
	var raw json.RawMessage
	return json.NewDecoder(bytes.NewReader(value)).Decode(&raw) == nil && streamErrorValuePresent(raw)
}

// streamPayloadDeclaresFailure reports a root-level failure frame that arrives
// without a terminal event name: {"error":...} and the panel-style
// {"code":500,"msg":"..."} / {"code":"model_not_found","message":"..."}
// shapes. It stays cheap: the code member is only probed when the payload
// actually contains the key, so ordinary content frames pay one byte scan.
func streamPayloadDeclaresFailure(payload []byte) bool {
	if streamEnvelopeHasError(payload) {
		return true
	}
	if !bytes.Contains(payload, []byte(`"code"`)) {
		return false
	}
	code, ok := partialJSONRootMember(payload, "code")
	if !ok {
		return false
	}
	// The root scanner returns the remaining payload from the value start, so
	// decode exactly one JSON value before classifying it.
	var codeValue json.RawMessage
	if json.NewDecoder(bytes.NewReader(code)).Decode(&codeValue) != nil {
		return false
	}
	_, hasMessage := partialJSONRootMember(payload, "msg")
	if !hasMessage {
		_, hasMessage = partialJSONRootMember(payload, "message")
	}
	if !hasMessage {
		return false
	}
	failure, _ := classifyRootCode(codeValue)
	return failure
}

// isJSONLikePayload is the cheap fast path that keeps JSON protocol frames out
// of the plain-text scanner.
func isJSONLikePayload(data string) bool {
	if data == "" {
		return false
	}
	switch data[0] {
	case '{', '[', '"':
		return true
	}
	if data[0] == '-' || (data[0] >= '0' && data[0] <= '9') {
		return true
	}
	return data == "true" || data == "false" || data == "null"
}

// plainTextStreamFailure catches non-JSON SSE payloads that carry an upstream
// failure as a bare sentence (some panels stream "上游负载已饱和" instead of a
// protocol frame). It only runs before any downstream byte is committed and
// never inspects JSON payloads, so assistant deltas and protocol frames are
// left untouched.
func plainTextStreamFailure(data string) error {
	trimmed := strings.TrimSpace(data)
	if trimmed == "" || trimmed == "[DONE]" || isJSONLikePayload(trimmed) {
		return nil
	}
	switch trimmed[0] {
	case '{', '[', '"', '-', 't', 'f', 'n':
		return nil
	}
	if trimmed[0] >= '0' && trimmed[0] <= '9' {
		return nil
	}
	lower := strings.ToLower(trimmed)
	for _, marker := range []string{
		"error", "failed", "failure", "overloaded", "rate limit", "too many requests",
		"service unavailable", "internal server error", "bad gateway", "gateway timeout",
		"upstream", "no available", "not available", "unavailable", "insufficient",
		"quota", "unauthorized", "forbidden", "timeout", "not found",
		"错误", "失败", "不可用", "无可用", "限流", "超时", "余额不足", "额度不足",
		"无权", "不存在", "已饱和", "负载",
	} {
		if strings.Contains(lower, marker) {
			return fmt.Errorf("%w: %s", errUpstreamStreamFailure, truncateErrorText(trimmed, 160))
		}
	}
	return nil
}

func truncateErrorText(value string, limit int) string {
	runes := []rune(value)
	if len(runes) <= limit {
		return value
	}
	return string(runes[:limit]) + "…"
}
