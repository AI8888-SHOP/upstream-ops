package gateway

import (
	"reflect"
	"testing"

	"github.com/bejix/upstream-ops/backend/apiaudit"
)

func TestAuditModelUsageMatchesBusinessRawBuckets(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
	}{
		{"stale zero cache aliases", `{"usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0},"cache_read_input_tokens":20,"cache_write_tokens":10}}`},
		{"nested cache writes", `{"usage":{"prompt_tokens":100,"completion_tokens":4,"input_tokens_details":{"cache_creation_tokens":5},"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":7}}}`},
		{"flat TTL aliases", `{"usage":{"input_tokens":100,"output_tokens":4,"cache_hit_tokens":20,"cache_creation_5m_tokens":4,"cache_creation_1h_tokens":6}}`},
		{"TTL input aliases", `{"usage":{"input_tokens":100,"output_tokens":4,"cache_write_input_tokens":10,"cache_creation_5m_input_tokens":4,"cache_creation_1h_input_tokens":6}}`},
		{"image and reasoning", `{"usage":{"input_tokens":100,"output_tokens":14,"output_tokens_details":{"image_tokens":8,"reasoning_tokens":6}}}`},
		{"nested Responses", `{"type":"response.completed","response":{"status":"completed","usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":20,"cache_write_tokens":10}}}}`},
		{"nested data", `{"data":{"usage":{"input_tokens":100,"output_tokens":4,"cache_read_tokens":20,"cache_write_tokens":10}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := auditModelUsage([]byte(tc.payload), "", apiaudit.UsageSnapshot{})
			want := ParseOpenAIUsage([]byte(tc.payload))
			if !got.Known || !got.InputSeen || !got.OutputSeen {
				t.Fatalf("usage not known: %+v", got)
			}
			if !reflect.DeepEqual(auditUsageTestTokens(got), want) {
				t.Fatalf("audit raw buckets=%+v, business=%+v", auditUsageTestTokens(got), want)
			}
		})
	}
}

func TestAuditModelUsageAnthropicDeltaPreservesPositiveStartAndTTL(t *testing.T) {
	start := `{"type":"message_start","message":{"usage":{"input_tokens":70,"output_tokens":0,"cache_read_input_tokens":20,"cache_creation_input_tokens":10,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_1h_input_tokens":6}}}}`
	delta := `{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":4,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}`
	got := auditModelUsage([]byte(start), "anthropic", apiaudit.UsageSnapshot{})
	got = auditModelUsage([]byte(delta), "anthropic", got)
	business := ParseAnthropicSSEUsage([]byte("data: " + start + "\n\ndata: " + delta + "\n\n"))
	if !got.Known || got.InputTokens != business.InputTokens || got.OutputTokens != business.OutputTokens || got.CacheReadTokens != business.CacheReadTokens || got.CacheCreationTokens != business.CacheCreationTokens {
		t.Fatalf("audit=%+v business=%+v", got, business)
	}
	if got.InputTokens != 70 || got.CacheCreation5mTokens != 4 || got.CacheCreation1hTokens != 6 {
		t.Fatalf("lost exclusive input or native TTL breakdown: %+v", got)
	}
}

func TestAuditModelUsagePresenceAndFinalZero(t *testing.T) {
	for _, payload := range []string{
		`{"usage":null}`, `{"usage":{}}`, `{"usage":{"total_tokens":100}}`,
		`{"usage":{"prompt_tokens":10}}`, `{"usage":{"cache_read_input_tokens":10}}`,
		`{"usage":{"input_tokens":null,"output_tokens":0}}`,
		`{"usage":{"input_tokens":-1,"output_tokens":0}}`,
		`{"usage":{"input_tokens":1.5,"output_tokens":0}}`,
		`{"usage":{"input_tokens":9223372036854775808,"output_tokens":0}}`,
		`{"choices":[{"delta":{"content":"usage input_tokens: 0"}}]}`,
	} {
		if got := auditModelUsage([]byte(payload), "", apiaudit.UsageSnapshot{}); got.Known {
			t.Fatalf("partial/invalid usage became known: %s: %+v", payload, got)
		}
	}
	got := auditModelUsage([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":0}}}`), "anthropic", apiaudit.UsageSnapshot{})
	if got.Known || !got.InputSeen || got.OutputSeen {
		t.Fatalf("input-only presence=%+v", got)
	}
	got = auditModelUsage([]byte(`{"type":"message_delta","usage":{"output_tokens":0}}`), "anthropic", got)
	if !got.Known || got.InputTokens != 0 || got.OutputTokens != 0 {
		t.Fatalf("split explicit zeros were not known: %+v", got)
	}
	got = auditModelUsage([]byte(`{"usage":{"input_tokens":100,"output_tokens":4,"cache_read_tokens":20}}`), "", apiaudit.UsageSnapshot{})
	got = auditModelUsage([]byte(`{"usage":{"input_tokens":0,"output_tokens":0}}`), "", got)
	if !got.Known || auditUsageTestTokens(got) != (UsageTokens{}) {
		t.Fatalf("authoritative final explicit zero did not replace prior counters: %+v", got)
	}
}

func auditUsageTestTokens(value apiaudit.UsageSnapshot) UsageTokens {
	return UsageTokens{
		InputTokens: value.InputTokens, OutputTokens: value.OutputTokens,
		CacheReadTokens: value.CacheReadTokens, CacheCreationTokens: value.CacheCreationTokens,
		CacheCreation5mTokens: value.CacheCreation5mTokens, CacheCreation1hTokens: value.CacheCreation1hTokens,
		ImageOutputTokens: value.ImageOutputTokens, ReasoningTokens: value.ReasoningTokens,
	}
}
