package gateway

import (
	"errors"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestStreamFailureDiagnosticsExtractCorrelationAndStage(t *testing.T) {
	body := []byte(`data: {"type":"response.failed","response":{"id":"resp_test_1","status":"failed","error":{"code":"upstream_stream_read_error","message":"interrupted"}}}`)
	responseID, errorCode := upstreamStreamFailureMetadata(body)
	if responseID != "resp_test_1" || errorCode != "upstream_stream_read_error" {
		t.Fatalf("metadata=(%q,%q)", responseID, errorCode)
	}

	headers := http.Header{
		"X-Request-Id":        {"req-a", "req-b", "req-a"},
		"X-Openai-Request-Id": {"req-c"},
		"X-Client-Request-Id": {"client-a"},
	}
	if got, want := upstreamRequestIDs(headers), []string{"req-a", "req-b", "req-c"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("upstream request IDs=%v want=%v", got, want)
	}
	if got := sanitizeDiagnosticValue("  id\r\nforged: value  ", 256); got != "id  forged: value" {
		t.Fatalf("diagnostic value was not sanitized: %q", got)
	}
	firstToken := int64(28166)
	info := usageErrorInfo{Detail: "original detail\n"}
	appendStreamFailureDiagnostics(&info, true, &firstToken, errUpstreamStreamFailure, headers, body)
	for _, want := range []string{
		"failure_phase: after_upstream_first_token_committed",
		"failure_kind: upstream_terminal_error",
		"upstream_request_ids: req-a,req-b,req-c",
		"upstream_client_request_id: client-a",
		"upstream_response_id: resp_test_1",
		"upstream_error_code: upstream_stream_read_error",
	} {
		if !strings.Contains(info.Detail, want) {
			t.Fatalf("detail missing %q: %s", want, info.Detail)
		}
	}
}

func TestStreamFailureDiagnosticClassification(t *testing.T) {
	firstToken := int64(10)
	for _, tc := range []struct {
		name      string
		committed bool
		first     *int64
		err       error
		phase     string
		kind      string
	}{
		{"pre-output-read", false, nil, errors.New("unexpected EOF"), "before_upstream_first_token", "transport_read_error"},
		{"committed-no-output", true, nil, errors.New("upstream stream idle timeout"), "after_stream_commit_before_upstream_first_token", "timeout"},
		{"output-before-commit", false, &firstToken, errors.New("unexpected EOF"), "after_upstream_first_token_before_commit", "transport_read_error"},
		{"after-output-upstream-error", true, &firstToken, errUpstreamStreamFailure, "after_upstream_first_token_committed", "upstream_terminal_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := streamFailurePhase(tc.committed, tc.first); got != tc.phase {
				t.Fatalf("phase=%q want=%q", got, tc.phase)
			}
			if got := streamFailureKind(tc.err); got != tc.kind {
				t.Fatalf("kind=%q want=%q", got, tc.kind)
			}
		})
	}
}
