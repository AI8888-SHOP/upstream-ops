package gateway

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

// sub2APIOpenAIChargeBuckets mirrors the field precedence used by Sub2API's
// openAIUsageFromGJSON/openAICacheReadTokensFromUsage billing parser.
func sub2APIOpenAIChargeBuckets(t *testing.T, body []byte) (fresh, cached, created int) {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("decode downstream response: %v", err)
	}
	usage, _ := payload["usage"].(map[string]any)
	if usage == nil {
		t.Fatalf("downstream response has no usage: %s", body)
	}
	total := mapInt(usage, "input_tokens")
	if total == 0 {
		total = mapInt(usage, "prompt_tokens")
	}
	cached = sub2APICacheReadTokens(usage)
	created = sub2APICacheCreationTokens(usage)
	return total - cached - created, cached, created
}

func sub2APICacheReadTokens(usage map[string]any) int {
	for _, detailsKey := range []string{"input_tokens_details", "prompt_tokens_details"} {
		object, _ := usage[detailsKey].(map[string]any)
		if _, exists := object["cached_tokens"]; exists {
			return max0(mapInt(object, "cached_tokens"))
		}
	}
	return firstPositiveInt(usage, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens")
}

func sub2APICacheCreationTokens(usage map[string]any) int {
	for _, nested := range [][2]string{
		{"input_tokens_details", "cache_write_tokens"},
		{"prompt_tokens_details", "cache_write_tokens"},
		{"input_tokens_details", "cache_creation_tokens"},
		{"prompt_tokens_details", "cache_creation_tokens"},
	} {
		details, _ := usage[nested[0]].(map[string]any)
		if _, exists := details[nested[1]]; exists {
			return max0(mapInt(details, nested[1]))
		}
	}
	return firstPositiveInt(usage,
		"cache_write_tokens", "cache_creation_input_tokens", "cache_write_input_tokens", "cache_creation_tokens",
	)
}

func TestVirtualCacheResponseMatchesSub2APIBillingParser(t *testing.T) {
	tests := []struct {
		name        string
		kind        protocol.Kind
		body        string
		wantCached  int
		wantCreated int
		wantVirtual int
	}{
		{
			name:       "chat",
			kind:       protocol.KindOpenAIChat,
			body:       `{"usage":{"prompt_tokens":100,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":20},"cache_creation_input_tokens":10}}`,
			wantCached: 90, wantCreated: 10, wantVirtual: 70,
		},
		{
			name:       "responses with stale preferred alias",
			kind:       protocol.KindOpenAIResponses,
			body:       `{"usage":{"input_tokens":80,"output_tokens":4,"input_tokens_details":{"cached_tokens":0},"prompt_tokens_details":{"cached_tokens":15},"cache_creation_tokens":5}}`,
			wantCached: 75, wantCreated: 5, wantVirtual: 60,
		},
		{
			name:       "stale nested creation cannot hide top-level creation",
			kind:       protocol.KindOpenAIChat,
			body:       `{"usage":{"prompt_tokens":100,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":0},"cache_creation_tokens":10}}`,
			wantCached: 90, wantCreated: 10, wantVirtual: 70,
		},
		{
			name:       "duration creation buckets get canonical total",
			kind:       protocol.KindOpenAIResponses,
			body:       `{"usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":20},"cache_creation_5m_tokens":4,"cache_creation_1h_tokens":6}}`,
			wantCached: 90, wantCreated: 10, wantVirtual: 70,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := NormalizeUsageBuckets(ParseOpenAIUsage([]byte(test.body)), test.kind)
			if actual.InputTokens != test.wantVirtual {
				t.Fatalf("upstream fresh input=%d, want virtual credit=%d: %+v", actual.InputTokens, test.wantVirtual, actual)
			}
			rewritten, changed := rewriteVirtualCacheResponse([]byte(test.body), test.kind)
			if !changed {
				t.Fatalf("response was not rewritten: %s", rewritten)
			}
			fresh, cached, created := sub2APIOpenAIChargeBuckets(t, rewritten)
			if fresh != 0 || cached != test.wantCached || created != test.wantCreated {
				t.Fatalf("Sub2API buckets fresh=%d cached=%d created=%d, want 0/%d/%d: %s",
					fresh, cached, created, test.wantCached, test.wantCreated, rewritten)
			}
		})
	}
}

func TestVirtualCacheStreamMatchesSub2APIBillingParser(t *testing.T) {
	transformer := newVirtualCacheSSETransformer(protocol.KindOpenAIResponses)
	first := transformer.Transform([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n"), false)
	last := transformer.Transform([]byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":50,\"output_tokens\":2,\"input_tokens_details\":{\"cached_tokens\":5}}}}\n\n"), true)
	if !transformer.Applied() {
		t.Fatal("stream usage was not rewritten")
	}
	combined := append(append([]byte(nil), first...), last...)
	marker := []byte("data: ")
	start := bytes.LastIndex(combined, marker)
	if start < 0 {
		t.Fatalf("stream has no terminal data frame: %s", combined)
	}
	data := combined[start+len(marker):]
	if end := bytes.Index(data, []byte("\n\n")); end >= 0 {
		data = data[:end]
	}
	var event map[string]any
	if err := json.Unmarshal(data, &event); err != nil {
		t.Fatalf("decode terminal stream event: %v\n%s", err, combined)
	}
	response, _ := event["response"].(map[string]any)
	wrapper, err := json.Marshal(map[string]any{"usage": response["usage"]})
	if err != nil {
		t.Fatal(err)
	}
	fresh, cached, created := sub2APIOpenAIChargeBuckets(t, wrapper)
	if fresh != 0 || cached != 50 || created != 0 {
		t.Fatalf("Sub2API stream buckets fresh=%d cached=%d created=%d, want 0/50/0: %s", fresh, cached, created, combined)
	}
}
