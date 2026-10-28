package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

func streamFailurePhase(committed bool, firstTokenMS *int64) string {
	if committed && firstTokenMS != nil {
		return "after_upstream_first_token_committed"
	}
	if committed {
		return "after_stream_commit_before_upstream_first_token"
	}
	if firstTokenMS != nil {
		return "after_upstream_first_token_before_commit"
	}
	return "before_upstream_first_token"
}

func streamFailureKind(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, errUpstreamStreamFailure) {
		return "upstream_terminal_error"
	}
	if errors.Is(err, errRequestFirstTokenBudget) || errors.Is(err, errFirstTokenTimeout) ||
		errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "timeout") {
		return "timeout"
	}
	return "transport_read_error"
}

func upstreamRequestIDs(headers http.Header) []string {
	if headers == nil {
		return nil
	}
	seen := make(map[string]struct{})
	var ids []string
	for _, name := range []string{"X-Request-Id", "X-OpenAI-Request-Id", "Request-Id", "Cf-Ray", "X-Trace-Id"} {
		for _, value := range headers.Values(name) {
			value = sanitizeDiagnosticValue(value, 256)
			if value == "" {
				continue
			}
			if _, ok := seen[value]; ok {
				continue
			}
			seen[value] = struct{}{}
			ids = append(ids, value)
			if len(ids) == 16 {
				return ids
			}
		}
	}
	return ids
}

func sanitizeDiagnosticValue(value string, maxRunes int) string {
	value = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return ' '
		}
		return r
	}, strings.TrimSpace(value))
	return truncateDiagnosticValue(value, maxRunes)
}

func truncateDiagnosticValue(value string, maxRunes int) string {
	if maxRunes <= 0 {
		return ""
	}
	runes := []rune(value)
	if len(runes) <= maxRunes {
		return value
	}
	return string(runes[:maxRunes])
}

func upstreamStreamFailureMetadata(body []byte) (responseID, errorCode string) {
	payloads := sseDataPayloads(body)
	if len(payloads) == 0 && len(body) > 0 {
		payloads = [][]byte{body}
	}
	for _, payload := range payloads {
		var envelope struct {
			Type     string          `json:"type"`
			Error    json.RawMessage `json:"error"`
			Response struct {
				ID     string          `json:"id"`
				Status string          `json:"status"`
				Error  json.RawMessage `json:"error"`
			} `json:"response"`
		}
		if json.Unmarshal(payload, &envelope) != nil || upstreamStreamFailure(envelope.Type, string(payload)) == nil {
			continue
		}
		responseID = sanitizeDiagnosticValue(envelope.Response.ID, 256)
		code := streamErrorCode(envelope.Response.Error)
		if code == "" {
			code = streamErrorCode(envelope.Error)
		}
		return responseID, sanitizeDiagnosticValue(code, 256)
	}
	return "", ""
}

func streamErrorCode(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var envelope struct {
		Code json.RawMessage `json:"code"`
	}
	if json.Unmarshal(raw, &envelope) != nil || len(envelope.Code) == 0 {
		return ""
	}
	var value string
	if json.Unmarshal(envelope.Code, &value) == nil {
		return strings.TrimSpace(value)
	}
	return strings.TrimSpace(string(envelope.Code))
}

func appendStreamFailureDiagnostics(info *usageErrorInfo, committed bool, firstTokenMS *int64, err error, headers http.Header, body []byte) {
	if info == nil {
		return
	}
	responseID, errorCode := upstreamStreamFailureMetadata(body)
	var detail strings.Builder
	detail.WriteString(info.Detail)
	fmt.Fprintf(&detail, "failure_phase: %s\nfailure_kind: %s\n", streamFailurePhase(committed, firstTokenMS), streamFailureKind(err))
	if ids := upstreamRequestIDs(headers); len(ids) > 0 {
		fmt.Fprintf(&detail, "upstream_request_ids: %s\n", strings.Join(ids, ","))
	}
	if clientID := sanitizeDiagnosticValue(headers.Get("X-Client-Request-Id"), 256); clientID != "" {
		fmt.Fprintf(&detail, "upstream_client_request_id: %s\n", clientID)
	}
	if responseID != "" {
		fmt.Fprintf(&detail, "upstream_response_id: %s\n", responseID)
	}
	if errorCode != "" {
		fmt.Fprintf(&detail, "upstream_error_code: %s\n", errorCode)
	}
	info.Detail = detail.String()
}
