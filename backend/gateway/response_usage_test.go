package gateway

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestResponseUsageRequiresExplicitZeroMetadata(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		zero       bool
	}{
		{"chat", `{"usage":{"prompt_tokens":0,"completion_tokens":0}}`, true},
		{"responses", `{"response":{"usage":{"input_tokens":0,"output_tokens":0}}}`, true},
		{"anthropic", `{"message":{"usage":{"input_tokens":0,"output_tokens":0}}}`, true},
		{"data-envelope", `{"data":{"usage":{"input_tokens":0,"output_tokens":0}}}`, true},
		{"numeric-forms", `{"usage":{"input_tokens":0.0,"output_tokens":"0e0"}}`, true},
		{"escaped-keys", `{"us\u0061ge":{"input_\u0074okens":0,"output_tokens":0}}`, true},
		{"missing", `{}`, false},
		{"missing-input", `{"usage":{"output_tokens":0}}`, false},
		{"null-input", `{"usage":{"input_tokens":null,"output_tokens":0}}`, false},
		{"null-usage", `{"usage":null}`, false},
		{"invalid-count", `{"usage":{"input_tokens":false,"output_tokens":0}}`, false},
		{"negative-count", `{"usage":{"input_tokens":-1,"output_tokens":0}}`, false},
		{"malformed", `{"usage":{"input_tokens":0,"output_tokens":0}`, false},
		{"positive-input", `{"usage":{"input_tokens":1,"output_tokens":0}}`, false},
		{"positive-output", `{"usage":{"input_tokens":0,"output_tokens":1}}`, false},
		{"conflicting-aliases", `{"usage":{"input_tokens":0,"prompt_tokens":10,"output_tokens":0}}`, false},
		{"real-cache-read", `{"usage":{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":10}}`, false},
		{"real-cache-write", `{"usage":{"input_tokens":0,"output_tokens":0,"cache_creation_input_tokens":10}}`, false},
		{"cache-details", `{"usage":{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":10}}}`, false},
		{"cache-creation-details", `{"usage":{"input_tokens":0,"output_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":10}}}`, false},
		{"reasoning", `{"usage":{"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":10}}}`, false},
		{"conversation-text", `{"choices":[{"message":{"content":"input_tokens=0;output_tokens=0"}}]}`, false},
		{"tool-arguments", `{"choices":[{"message":{"tool_calls":[{"usage":{"input_tokens":0,"output_tokens":0}}]}}]}`, false},
		{"unknown-nested", `{"other":{"usage":{"input_tokens":0,"output_tokens":0}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var usage responseUsageObservation
			usage.ObserveBody([]byte(tc.body))
			if usage.IsZero() != tc.zero {
				t.Fatalf("usage=%+v, want zero=%v", usage, tc.zero)
			}
		})
	}
}

func TestResponseUsageAccumulatesSplitSSEWithoutLosingPositiveCounts(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		zero       bool
	}{
		{"split-zero", "data: {\"message\":{\"usage\":{\"input_tokens\":0}}}\n\ndata: {\"usage\":{\"output_tokens\":0}}\n\n", true},
		{"split-positive", "data: {\"message\":{\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}}\n\ndata: {\"usage\":{\"output_tokens\":2}}\n\n", false},
		{"positive-then-zero", "data: {\"usage\":{\"input_tokens\":5,\"output_tokens\":0}}\n\ndata: {\"usage\":{\"input_tokens\":0,\"output_tokens\":0}}\n\n", false},
		{"multiline", "data: {\"usage\":\r\ndata: {\"input_tokens\":0,\"output_tokens\":0}}\r\n\r\ndata: [DONE]\r\n\r\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var usage responseUsageObservation
			usage.ObserveBody([]byte(tc.body))
			if usage.IsZero() != tc.zero {
				t.Fatalf("usage=%+v, want zero=%v", usage, tc.zero)
			}
		})
	}
}

func TestZeroUsageRuleSelectorsAndNoTextPrefixDelay(t *testing.T) {
	v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 9, Name: "zero", Enabled: true, Target: "zero_usage",
		Models: []string{"public-*"}, Protocols: []string{"openai_responses"},
	})
	const body = `{"usage":{"input_tokens":0,"output_tokens":0}}`
	for _, tc := range []struct {
		protocol, model string
		reject          bool
	}{
		{"openai_responses", "public-main", true},
		{"anthropic", "public-main", false},
		{"openai_responses", "other", false},
	} {
		got := v.ValidateBodyUsage([]byte(body), tc.protocol, tc.model)
		if got.IsRejected() != tc.reject {
			t.Fatalf("%+v: validation=%+v", tc, got)
		}
		if v.Validate([]byte(body), nil, tc.protocol, tc.model).IsRejected() {
			t.Fatal("zero-usage rule must not inspect converted response text")
		}
	}
	if v.rules[0].Pattern != storage.GatewayResponseRuleZeroUsagePattern {
		t.Fatalf("unexpected built-in pattern: %q", v.rules[0].Pattern)
	}
	if !v.NewStreamValidator("openai_responses", "public-main").Ready(time.Now()).IsAccepted() {
		t.Fatal("zero-usage-only rule added a text-prefix wait")
	}
}

func TestZeroUsageGatePreCommitRejectionAndLateAudit(t *testing.T) {
	for _, committed := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{ID: 9, Enabled: true, Target: "zero_usage"})
		gate := newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator("openai_chat", "main"))
		c.Writer = gate
		if committed {
			if err := gate.Win(); err != nil {
				t.Fatal(err)
			}
		}
		err := validateStreamZeroUsage(c, responseUsageObservation{inputSeen: true, outputSeen: true})
		if committed {
			if err != nil || !gate.LateMatch().IsRejected() || !gate.LateMatch().PostCommit {
				t.Fatalf("late audit=%+v err=%v", gate.LateMatch(), err)
			}
		} else {
			var rejection *responseRejectedError
			if !errors.As(err, &rejection) || gate.DownstreamCommitted() || !gate.Rejection().IsRejected() {
				t.Fatalf("rejection=%+v err=%v", gate.Rejection(), err)
			}
			gate.Lose()
		}
	}
}

func TestZeroUsageMetadataClassifierReleasesContentAndTools(t *testing.T) {
	for _, tc := range []struct {
		kind     protocol.Kind
		data     string
		metadata bool
	}{
		{protocol.KindOpenAIChat, `{"choices":[{"delta":{"role":"assistant","content":""}}]}`, true},
		{protocol.KindOpenAIChat, `{"choices":[],"usage":{"prompt_tokens":0,"completion_tokens":0}}`, true},
		{protocol.KindOpenAIChat, `{"choices":[{"delta":{"content":"hi"}}]}`, false},
		{protocol.KindOpenAIChat, `{"choices":[{"delta":{"reasoning_content":"thinking"}}]}`, false},
		{protocol.KindOpenAIChat, `{"choices":[{"delta":{"tool_calls":[{"id":"call_1"}]}}]}`, false},
		{protocol.KindAnthropic, `{"type":"message_start","message":{"content":[]}}`, true},
		{protocol.KindAnthropic, `{"type":"message_start","message":{"content":[ ]}}`, true},
		{protocol.KindAnthropic, `{"type":"message_start","message":{"content":[{"type":"text","text":""}]}}`, true},
		{protocol.KindAnthropic, `{"type":"message_start","message":{"content":[{"type":"text","text":"hi"}]}}`, false},
		{protocol.KindAnthropic, `{"type":"content_block_start","content_block":{"type":"text","text":""}}`, true},
		{protocol.KindAnthropic, `{"type":"content_block_delta","delta":{"type":"text_delta","text":"hi"}}`, false},
		{protocol.KindAnthropic, `{"type":"content_block_delta","delta":{"type":"thinking_delta","thinking":"hi"}}`, false},
		{protocol.KindAnthropic, `{"type":"content_block_start","content_block":{"type":"tool_use","id":"call_1","name":"tool","input":{}}}`, false},
		{protocol.KindOpenAIChat, `{"unknown":"payload"}`, false},
		{protocol.KindOpenAIChat, `{"error":{"message":"failed"}}`, false},
	} {
		if got := zeroUsageMetadataEvent(tc.kind, "", tc.data); got != tc.metadata {
			t.Fatalf("kind=%s data=%s: metadata=%v, want %v", tc.kind, tc.data, got, tc.metadata)
		}
	}
}

func TestZeroUsageDisabledAndFilteredStreamsKeepExistingBehavior(t *testing.T) {
	for _, tc := range []struct {
		cfg   responseValidationConfig
		model string
	}{
		{responseValidationConfig{Enabled: false, StreamMode: "prefix"}, "main"},
		{responseValidationConfig{Enabled: true, StreamMode: "disabled"}, "main"},
		{responseValidationConfig{Enabled: true, StreamMode: "prefix"}, "other"},
	} {
		v, err := newResponseValidator(tc.cfg, []responseRuleSpec{{Enabled: true, Target: "zero_usage", Models: []string{tc.model}}})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Writer = newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator("openai_chat", "main"))
		if streamZeroUsageEnabled(c) {
			t.Fatalf("unselected zero-usage rule became active: %+v", tc)
		}
	}
}

func BenchmarkZeroUsageObservationIgnoresOrdinaryDeltas(b *testing.B) {
	payload := `{"choices":[{"delta":{"content":"` + strings.Repeat("x", 100) + `"}}]}`
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var usage responseUsageObservation
		usage.ObserveStreamData(payload)
	}
}
