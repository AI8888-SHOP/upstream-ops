package gateway

import (
	"net/http"
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
