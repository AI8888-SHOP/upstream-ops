package gateway

import (
	"net/http"
	"strings"
	"testing"
	"time"
)

func mustResponseValidator(t *testing.T, prefixBytes int, timeout time.Duration, rules ...responseRuleSpec) *responseValidator {
	t.Helper()
	v, err := newResponseValidator(responseValidationConfig{
		Enabled: true, StreamMode: "prefix", PrefixBytes: prefixBytes, PrefixTimeout: timeout,
	}, rules)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestResponseValidatorChecksConvertedOpenAIBody(t *testing.T) {
	v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 7, Name: "capacity", Enabled: true, Pattern: `(?i)temporarily unavailable`, Target: "assistant_text",
	})
	body := []byte(`{"id":"x","choices":[{"message":{"role":"assistant","content":"The service is temporarily unavailable."}}]}`)
	result := v.Validate(body, http.Header{"Content-Type": []string{"application/json"}}, "openai_chat", "gpt-test")
	if !result.IsRejected() || result.RuleID != 7 || result.Target != "assistant_text" || result.PostCommit {
		t.Fatalf("result=%+v", result)
	}
}

func TestResponseValidatorTargetsRawBodyAndErrorMessage(t *testing.T) {
	v := mustResponseValidator(t, 8192, time.Second,
		responseRuleSpec{ID: 1, Name: "raw", Enabled: true, Priority: 10, Pattern: `request-marker`, Target: "raw_body"},
		responseRuleSpec{ID: 2, Name: "error", Enabled: true, Priority: 1, Pattern: `quota exhausted`, Target: "error_message"},
	)
	body := []byte(`{"request":"request-marker","error":{"message":"quota exhausted"}}`)
	result := v.Validate(body, nil, "openai_chat", "gpt-test")
	if !result.IsRejected() || result.RuleID != 2 {
		t.Fatalf("result=%+v, want priority rule 2", result)
	}
}

func TestResponseValidatorModelAndProtocolSelectors(t *testing.T) {
	v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 3, Name: "selected", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
		Models: []string{"gpt-4*"}, Protocols: []string{"openai_*"},
	})
	body := []byte(`{"choices":[{"message":{"content":"blocked"}}]}`)
	if result := v.Validate(body, nil, "anthropic", "gpt-4o"); result.IsRejected() {
		t.Fatalf("protocol selector unexpectedly matched: %+v", result)
	}
	if result := v.Validate(body, nil, "openai_chat", "claude-3"); result.IsRejected() {
		t.Fatalf("model selector unexpectedly matched: %+v", result)
	}
	if result := v.Validate(body, nil, "openai_chat", "gpt-4o"); !result.IsRejected() {
		t.Fatalf("selectors did not match: %+v", result)
	}
}

func TestStreamResponseValidatorRejectsAcrossSSEFrames(t *testing.T) {
	v := mustResponseValidator(t, 4096, time.Second, responseRuleSpec{
		ID: 4, Name: "cross-frame", Enabled: true, Pattern: `forbidden`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	if result := stream.Consume([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"for\"}}]}\n\n")); !result.IsPending() {
		t.Fatalf("first result=%+v", result)
	}
	result := stream.Consume([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"bidden\"}}]}\n\n"))
	if !result.IsRejected() || result.PostCommit || result.RuleID != 4 {
		t.Fatalf("result=%+v", result)
	}
}

func TestStreamResponseValidatorPrefixTimeoutAndLateAudit(t *testing.T) {
	v := mustResponseValidator(t, 4096, 10*time.Millisecond, responseRuleSpec{
		ID: 5, Name: "late", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	if result := stream.Consume([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"safe\"}}]}\n\n")); !result.IsPending() {
		t.Fatalf("first result=%+v", result)
	}
	first := stream.FirstContentAt()
	if first.IsZero() {
		t.Fatal("prefix timer was not armed by the first payload")
	}
	if result := stream.Ready(first.Add(10 * time.Millisecond)); !result.IsAccepted() {
		t.Fatalf("timeout result=%+v", result)
	}
	stream.Commit()
	result := stream.Consume([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"blocked\"}}]}\n\n"))
	if result.IsRejected() {
		t.Fatalf("post-commit consume must not reject or trigger a retry: %+v", result)
	}
	result = stream.AuditPostCommit()
	if !result.IsRejected() || !result.PostCommit || result.RuleID != 5 {
		t.Fatalf("late audit result=%+v", result)
	}
}

func TestStreamResponseValidatorPostCommitRingPreservesOrder(t *testing.T) {
	v := mustResponseValidator(t, 4, time.Second, responseRuleSpec{
		ID: 8, Name: "ordered-tail", Enabled: true, Pattern: `x{4}tail-marker`, Target: "raw_body",
	})
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	if result := stream.Consume([]byte("safe")); !result.IsAccepted() {
		t.Fatalf("prefix result=%+v, want accepted", result)
	}
	stream.Commit()
	stream.Consume([]byte(strings.Repeat("x", postCommitValidationBytes)))
	stream.Consume([]byte("tail-marker"))
	result := stream.AuditPostCommit()
	if !result.IsRejected() || !result.PostCommit || result.RuleID != 8 {
		t.Fatalf("ring audit result=%+v", result)
	}
}

func TestStreamResponseValidatorResponsesLifecycleKeepsFailureRetryable(t *testing.T) {
	v := mustResponseValidator(t, 128, 10*time.Millisecond, responseRuleSpec{
		ID: 9, Name: "overloaded", Enabled: true,
		Pattern: `(?i)servers are currently overloaded`, Target: "error_message",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\",\"metadata\":\"" + strings.Repeat("x", 512) + "\"}}\n\n"
	if result := stream.Consume([]byte(created)); !result.IsPending() {
		t.Fatalf("created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("metadata-only timeout result=%+v, want pending", result)
	}
	failed := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n"
	result := stream.Consume([]byte(failed))
	if !result.IsRejected() || result.PostCommit || result.RuleID != 9 || result.Target != "error_message" {
		t.Fatalf("failed result=%+v, want pre-commit rejection", result)
	}
}

func TestStreamResponseValidatorResponsesLargeLifecycleKeepsFailureRetryable(t *testing.T) {
	v := mustResponseValidator(t, 128, 10*time.Millisecond, responseRuleSpec{
		ID: 13, Name: "overloaded", Enabled: true,
		Pattern: `(?i)servers are currently overloaded`, Target: "error_message",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\",\"metadata\":\"" + strings.Repeat("x", 128<<10) + "\"}}\n\n"
	if result := stream.Consume([]byte(created)); !result.IsPending() {
		t.Fatalf("large created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("large lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("large metadata timeout result=%+v, want pending", result)
	}
	failed := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n")
	result := stream.Consume(failed)
	if !result.IsRejected() || result.PostCommit || result.RuleID != 13 {
		t.Fatalf("failure result=%+v, want pre-commit rejection", result)
	}
}

func TestStreamResponseValidatorResponsesLargeTerminalUsesCompleteFrame(t *testing.T) {
	v := mustResponseValidator(t, 64, 10*time.Millisecond, responseRuleSpec{
		ID: 14, Name: "capacity", Enabled: true, Pattern: `capacity reached`, Target: "error_message",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
	if result := stream.Consume(created); !result.IsPending() {
		t.Fatalf("created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("created timeout result=%+v, want pending", result)
	}
	failed := "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"capacity reached\"},\"diagnostic\":\"" + strings.Repeat("x", 128<<10) + "\"}}\n\n"
	result := stream.Consume([]byte(failed))
	if !result.IsRejected() || result.PostCommit || result.RuleID != 14 {
		t.Fatalf("large terminal result=%+v, want complete-frame rejection", result)
	}
}

func TestStreamResponseValidatorResponsesLargeOutputUsesCompleteFrame(t *testing.T) {
	v := mustResponseValidator(t, 64, 10*time.Millisecond, responseRuleSpec{
		ID: 15, Name: "blocked-output", Enabled: true, Pattern: `blocked output`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
	if result := stream.Consume(created); !result.IsPending() {
		t.Fatalf("created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("created timeout result=%+v, want pending", result)
	}
	output := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"blocked output\",\"diagnostic\":\"" + strings.Repeat("x", 128<<10) + "\"}\n\n"
	result := stream.Consume([]byte(output))
	if !result.IsRejected() || result.PostCommit || result.RuleID != 15 {
		t.Fatalf("large output result=%+v, want complete-frame rejection", result)
	}
}

func TestStreamResponseValidatorResponsesLifecycleReleasesOnOutput(t *testing.T) {
	v := mustResponseValidator(t, 4096, 10*time.Millisecond, responseRuleSpec{
		ID: 10, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
	if result := stream.Consume(created); !result.IsPending() {
		t.Fatalf("created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("metadata-only timeout result=%+v, want pending", result)
	}
	output := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"safe\"}\n\n")
	if result := stream.Consume(output); !result.IsPending() {
		t.Fatalf("output result=%+v, want pending", result)
	}
	first := stream.FirstContentAt()
	if first.IsZero() {
		t.Fatal("content event did not arm the prefix timeout")
	}
	if result := stream.Ready(first.Add(10 * time.Millisecond)); !result.IsAccepted() {
		t.Fatalf("output timeout result=%+v, want accepted", result)
	}
}

func TestStreamResponseValidatorResponsesLifecycleAcrossChunksAndCRLF(t *testing.T) {
	v := mustResponseValidator(t, 64, 10*time.Millisecond, responseRuleSpec{
		ID: 11, Name: "overloaded", Enabled: true,
		Pattern: `(?i)servers are currently overloaded`, Target: "error_message",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	for _, chunk := range []string{
		"event: response.cre",
		"ated\r\ndata: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\r\n\r",
		"\n",
	} {
		if result := stream.Consume([]byte(chunk)); !result.IsPending() {
			t.Fatalf("created chunk result=%+v, want pending", result)
		}
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("lifecycle timeout result=%+v, want pending", result)
	}
	for index, chunk := range []string{
		"event: response.failed\r\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"Our servers are currently over",
		"loaded. Please try again later.\"}}}\r\n\r\n",
	} {
		result := stream.Consume([]byte(chunk))
		if index == 0 && !result.IsPending() {
			t.Fatalf("partial failure result=%+v, want pending", result)
		}
		if index == 1 && (!result.IsRejected() || result.PostCommit || result.RuleID != 11) {
			t.Fatalf("complete failure result=%+v, want pre-commit rejection", result)
		}
	}
}

func TestStreamResponseValidatorResponsesMultipleFramesPerChunk(t *testing.T) {
	v := mustResponseValidator(t, 64, 10*time.Millisecond, responseRuleSpec{
		ID: 12, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	lifecycle := "event: response.created\ndata: {\"type\":\"response.created\"}\n\n" +
		"data: {\"type\":\"response.in_progress\"}\n\n"
	if result := stream.Consume([]byte(lifecycle)); !result.IsPending() {
		t.Fatalf("lifecycle result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle events armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("lifecycle timeout result=%+v, want pending", result)
	}
	output := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"safe\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n"
	if result := stream.Consume([]byte(output)); !result.IsAccepted() {
		t.Fatalf("output result=%+v, want accepted", result)
	}
}

func TestStreamResponseValidatorResponsesCoalescedLifecycleStartsAtRealContent(t *testing.T) {
	v := mustResponseValidator(t, 4096, time.Hour, responseRuleSpec{
		ID: 16, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	combined := "event: response.created\ndata: {\"type\":\"response.created\"}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"safe\"}\n\n"
	if result := stream.Consume([]byte(combined)); !result.IsPending() {
		t.Fatalf("coalesced lifecycle/output result=%+v, want pending", result)
	}
	first := stream.FirstContentAt()
	if first.IsZero() {
		t.Fatal("coalesced output did not arm the prefix timeout")
	}
	if result := stream.Ready(first.Add(time.Hour)); !result.IsAccepted() {
		t.Fatalf("coalesced lifecycle/output timeout result=%+v, want accepted", result)
	}
}

func TestStreamResponseValidatorResponsesRejectsAcrossOutputDeltas(t *testing.T) {
	v := mustResponseValidator(t, 4096, time.Second, responseRuleSpec{
		ID: 17, Name: "cross-delta", Enabled: true, Pattern: `forbidden`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	first := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"for\"}\n\n")
	if result := stream.Consume(first); !result.IsPending() {
		t.Fatalf("first delta result=%+v, want pending", result)
	}
	second := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"bidden\"}\n\n")
	result := stream.Consume(second)
	if !result.IsRejected() || result.PostCommit || result.RuleID != 17 {
		t.Fatalf("second delta result=%+v, want pre-commit rejection", result)
	}
}

func TestStreamResponseValidatorResponsesDataOnlyTerminalEvents(t *testing.T) {
	cases := []struct {
		name    string
		frame   string
		pattern string
	}{
		{
			name:    "failed nested error",
			frame:   "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":\"capacity reached\"}}}\n\n",
			pattern: `capacity reached`,
		},
		{
			name:    "root error",
			frame:   "data: {\"type\":\"error\",\"error\":{\"message\":\"server overloaded\"}}\n\n",
			pattern: `server overloaded`,
		},
		{
			name:    "incomplete reason",
			frame:   "data: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"capacity_limit\"}}}\n\n",
			pattern: `capacity_limit`,
		},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := mustResponseValidator(t, 32, time.Second, responseRuleSpec{
				ID: uint(20 + index), Name: tc.name, Enabled: true, Pattern: tc.pattern, Target: "error_message",
			})
			stream := v.NewStreamValidator("openai_responses", "gpt-test")
			result := stream.Consume([]byte(tc.frame))
			if !result.IsRejected() || result.PostCommit || result.RuleID != uint(20+index) {
				t.Fatalf("result=%+v, want pre-commit rejection", result)
			}
		})
	}
}

func TestStreamResponseValidatorResponsesUnknownCompleteFrameDoesNotDeadlock(t *testing.T) {
	v := mustResponseValidator(t, 4096, 10*time.Millisecond, responseRuleSpec{
		ID: 30, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\"}\n\n")
	if result := stream.Consume(created); !result.IsPending() {
		t.Fatalf("created result=%+v, want pending", result)
	}
	if first := stream.FirstContentAt(); !first.IsZero() {
		t.Fatal("lifecycle event armed the prefix timeout")
	}
	if result := stream.Ready(time.Now().Add(10 * time.Millisecond)); !result.IsPending() {
		t.Fatalf("created timeout result=%+v, want pending", result)
	}
	unknown := []byte("event: vendor.notice\r\ndata: {not-json}\r\n\r\n")
	if result := stream.Consume(unknown); !result.IsPending() {
		t.Fatalf("unknown complete frame result=%+v, want pending", result)
	}
	first := stream.FirstContentAt()
	if first.IsZero() {
		t.Fatal("unknown content frame did not arm the prefix timeout")
	}
	if result := stream.Ready(first.Add(10 * time.Millisecond)); !result.IsAccepted() {
		t.Fatalf("unknown frame timeout result=%+v, want accepted", result)
	}
}

func TestStreamResponseValidatorResponsesClassifierHasUpperBound(t *testing.T) {
	v := mustResponseValidator(t, 128, time.Hour, responseRuleSpec{
		ID: 31, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	payload := []byte(strings.Repeat("x", maxResponsesPreCommitBytes+1))
	if result := stream.Consume(payload); !result.IsPending() {
		t.Fatalf("oversized unterminated frame result=%+v, want pending overflow", result)
	}
	if !stream.responsesPreCommitOverflow() {
		t.Fatal("oversized unterminated frame did not signal overflow")
	}
	if got := len(stream.responsesSSE.pending); got != 0 {
		t.Fatalf("classifier retained %d bytes after overflow", got)
	}
}

func TestExtractResponseErrorMessageFromResponsesFailure(t *testing.T) {
	body := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n")
	if got, want := extractResponseErrorMessage(body, nil), "Our servers are currently overloaded. Please try again later."; got != want {
		t.Fatalf("got=%q want=%q", got, want)
	}
}

func TestExtractResponseErrorMessageFromResponsesIncomplete(t *testing.T) {
	body := []byte("event: response.incomplete\ndata: {\"type\":\"response.incomplete\",\"response\":{\"incomplete_details\":{\"reason\":\"max_output_tokens\"}}}\n\n")
	if got, want := extractResponseErrorMessage(body, nil), "max_output_tokens"; got != want {
		t.Fatalf("got=%q want=%q", got, want)
	}
}

func TestResponseValidatorProtocolShapes(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "anthropic", body: `{"content":[{"type":"text","text":"hello"},{"type":"text","text":" world"}]}`, want: "hello world"},
		{name: "responses", body: `{"output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`, want: "hello"},
		{name: "gemini", body: `{"candidates":[{"content":{"parts":[{"text":"hello"},{"text":" world"}]}}]}`, want: "hello world"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractAssistantText([]byte(tc.body), nil); got != tc.want {
				t.Fatalf("got=%q want=%q", got, tc.want)
			}
		})
	}
}

func TestResponseValidatorChecksPartialSSEJSONAtPrefixBoundary(t *testing.T) {
	v := mustResponseValidator(t, 256, time.Second, responseRuleSpec{
		ID: 6, Name: "partial", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	payload := []byte(`data: {"model":"blocked","metadata":{"text":"blocked"},"choices":[{"delta":{"content":"blocked`)
	result := stream.Consume(payload)
	if !result.IsRejected() || result.RuleID != 6 || result.PostCommit {
		t.Fatalf("result=%+v, want pre-commit partial assistant match", result)
	}
}

func TestResponseValidatorPartialSSEJSONIgnoresMetadata(t *testing.T) {
	v := mustResponseValidator(t, 256, time.Second, responseRuleSpec{
		ID: 7, Name: "metadata", Enabled: true, Pattern: `blocked`, Target: "assistant_text",
	})
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	payload := []byte(`data: {"model":"blocked","metadata":{"text":"blocked"},"choices":[{"delta":{"content":"safe`)
	if result := stream.Consume(payload); result.IsRejected() {
		t.Fatalf("metadata unexpectedly matched assistant text: %+v", result)
	}
}

func BenchmarkStreamResponseValidatorPostCommit(b *testing.B) {
	v, err := newResponseValidator(responseValidationConfig{
		Enabled: true, StreamMode: "prefix", PrefixBytes: 8192, PrefixTimeout: time.Second,
	}, []responseRuleSpec{
		{ID: 8, Name: "late", Enabled: true, Pattern: `(?i)blocked`, Target: "assistant_text"},
	})
	if err != nil {
		b.Fatal(err)
	}
	stream := v.NewStreamValidator("openai_chat", "gpt-test")
	stream.Commit()
	payload := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"safe\"}}]}\n\n")
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stream.Consume(payload)
	}
	b.StopTimer()
	if result := stream.AuditPostCommit(); result.IsRejected() {
		b.Fatalf("unexpected audit rejection: %+v", result)
	}
}

func BenchmarkStreamResponseValidatorResponsesLifecycleAfterPrefix(b *testing.B) {
	v, err := newResponseValidator(responseValidationConfig{
		Enabled: true, StreamMode: "prefix", PrefixBytes: 128, PrefixTimeout: time.Hour,
	}, []responseRuleSpec{
		{ID: 40, Name: "overloaded", Enabled: true, Pattern: `(?i)servers are currently overloaded`, Target: "error_message"},
	})
	if err != nil {
		b.Fatal(err)
	}
	stream := v.NewStreamValidator("openai_responses", "gpt-test")
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\n")
	if result := stream.Consume(created); !result.IsPending() {
		b.Fatalf("created result=%+v", result)
	}
	payload := []byte("event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"status\":\"in_progress\"}}\n\n")
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if result := stream.Consume(payload); !result.IsPending() {
			b.Fatalf("lifecycle result=%+v", result)
		}
	}
}
