package gateway

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestResponseModelExtractionUsesOnlyMetadata(t *testing.T) {
	for _, tc := range []struct{ name, body, want string }{
		{"chat", `{"model":"gpt-main","choices":[{"message":{"content":"gpt-mini"}}]}`, "gpt-main"},
		{"responses", `{"type":"response.created","response":{"model":"gpt-main","status":"in_progress"}}`, "gpt-main"},
		{"anthropic", `{"type":"message_start","message":{"model":"claude-main","usage":{"input_tokens":10}}}`, "claude-main"},
		{"escaped-key", `{"response":{"\u006dodel":"gpt-main"}}`, "gpt-main"},
		{"escaped-value", `{"model":"gpt\u002dmain"}`, "gpt-main"},
		{"assistant-text", `{"choices":[{"message":{"content":"model: gpt-mini"}}]}`, ""},
		{"tool-arguments", `{"response":{"output":[{"arguments":{"model":"gpt-mini"}}]}}`, ""},
		{"nested-unknown", `{"other":{"model":"gpt-mini"}}`, ""},
		{"missing", `{"usage":{"input_tokens":10}}`, ""},
		{"invalid-type", `{"model":123}`, ""},
		{"null", `{"model":null}`, ""},
		{"malformed", `{"model":"gpt-mini",`, ""},
		{"oversized", `{"model":"` + strings.Repeat("x", 257) + `"}`, ""},
		{"large-content-after-model", `{"model":"gpt-main","output":"` + strings.Repeat("x", 4096) + `"}`, "gpt-main"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := responseModelFromJSON([]byte(tc.body)); got != tc.want {
				t.Fatalf("model = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestResponseModelAuditRetainsConflictAndUnknownState(t *testing.T) {
	var audit responseModelAudit
	audit.Observe([]byte(`{"model":"main"}`), false)
	if changed := audit.Observe([]byte(`{"model":"main"}`), false); changed != "" {
		t.Fatal("repeated declaration should not trigger regex work")
	}
	audit.Observe([]byte(`{"model":"mini"}`), false)
	audit.Observe([]byte(`{"response":{"model":"main"}}`), true)
	if audit.Model != "main" || !audit.Conflict {
		t.Fatalf("audit = %+v", audit)
	}
	for _, tc := range []struct {
		sent, response  string
		known, mismatch bool
	}{
		{"main", "", false, false},
		{"", "main", false, false},
		{"main", " MAIN ", true, false},
		{"main", "mini", true, true},
	} {
		got := responseModelMismatch(tc.sent, tc.response)
		if (got != nil) != tc.known || got != nil && *got != tc.mismatch {
			t.Fatalf("mismatch(%q, %q) = %v", tc.sent, tc.response, got)
		}
	}
}

func TestResponseModelBodyAuditHandlesSSEAndRetainsFirstRejection(t *testing.T) {
	body := "data: {\"type\":\"response.created\",\"response\":{\"model\":\"main\"}}\r\n\r\n" +
		"data: {\"type\":\"response.in_progress\",\"response\":{\"model\":\"mini\"}}\r\n\r\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"model\":\"main\"}}\r\n\r\n"
	var audit responseModelAudit
	blocked := errors.New("blocked model")
	err := audit.ObserveBody([]byte(body), func(model string) error {
		if model == "mini" {
			return blocked
		}
		return nil
	})
	if !errors.Is(err, blocked) || audit.Model != "main" || !audit.Conflict {
		t.Fatalf("audit = %+v, rejection = %v", audit, err)
	}
}

func TestResponseModelRulesMatchRequestedModelAndIgnoreConversation(t *testing.T) {
	v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 7, Name: "model-substitution", Enabled: true, Target: "response_model",
		Pattern: `^mini$`, Models: []string{"public-*"}, Protocols: []string{"openai_responses"},
	})
	for _, tc := range []struct {
		protocol, requested, response string
		reject                        bool
	}{
		{"openai_responses", "public-main", "mini", true},
		{"openai_responses", "public-main", "main", false},
		{"openai_responses", "another-model", "mini", false},
		{"anthropic", "public-main", "mini", false},
		{"openai_responses", "public-main", "", false},
	} {
		if got := v.ValidateResponseModel(tc.protocol, tc.requested, tc.response); got.IsRejected() != tc.reject {
			t.Fatalf("case %+v: result %+v", tc, got)
		}
	}
	if result := v.Validate([]byte(`{"output":[{"content":[{"type":"output_text","text":"mini"}]}]}`), nil, "openai_responses", "public-main"); result.IsRejected() {
		t.Fatalf("model rule matched converted conversation: %+v", result)
	}
	// A model-only rule must not add the text-prefix timeout to TTFT.
	if result := v.NewStreamValidator("openai_responses", "public-main").Ready(time.Now()); !result.IsAccepted() {
		t.Fatalf("model-only validation is waiting for text: %+v", result)
	}
}

func TestResponseModelGateRejectsBeforeCommitAndOnlyAuditsAfter(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[committed], func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
				ID: 8, Enabled: true, Target: "response_model", Pattern: `^mini$`,
			})
			gate := newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator("openai_chat", "main"))
			c.Writer = gate
			if committed {
				if err := gate.Win(); err != nil {
					t.Fatal(err)
				}
			}
			err := validateStreamResponseModel(c, "mini")
			if committed {
				if err != nil || !gate.LateMatch().IsRejected() || !gate.LateMatch().PostCommit {
					t.Fatalf("late audit = %+v, err = %v", gate.LateMatch(), err)
				}
				return
			}
			var rejection *responseRejectedError
			if !errors.As(err, &rejection) || gate.DownstreamCommitted() || !gate.Rejection().IsRejected() {
				t.Fatalf("pre-commit rejection = %+v, err = %v", gate.Rejection(), err)
			}
			if err := gate.Win(); !errors.As(err, &rejection) {
				t.Fatalf("rejected attempt could win: %v", err)
			}
			gate.Lose()
		})
	}
}
