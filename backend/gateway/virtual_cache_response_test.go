package gateway

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

func TestRewriteVirtualCacheResponseProtocols(t *testing.T) {
	tests := []struct {
		name      string
		kind      protocol.Kind
		body      string
		parse     func([]byte) UsageTokens
		wantRead  int
		wantFresh int
		inputKey  string
		wantTotal int
	}{
		{
			name: "chat", kind: protocol.KindOpenAIChat,
			body:  `{"usage":{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":20},"cache_creation_input_tokens":10}}`,
			parse: ParseOpenAIUsage, wantRead: 90, inputKey: "prompt_tokens", wantTotal: 100,
		},
		{
			name: "responses", kind: protocol.KindOpenAIResponses,
			body:  `{"usage":{"input_tokens":80,"output_tokens":4,"total_tokens":84,"input_tokens_details":{"cached_tokens":15},"cache_creation_input_tokens":5}}`,
			parse: ParseOpenAIUsage, wantRead: 75, inputKey: "input_tokens", wantTotal: 80,
		},
		{
			name: "anthropic", kind: protocol.KindAnthropic,
			body:  `{"usage":{"input_tokens":70,"output_tokens":3,"cache_read_input_tokens":10,"cache_creation_input_tokens":5}}`,
			parse: ParseAnthropicUsage, wantRead: 80,
		},
		{
			name: "chat input_tokens alias", kind: protocol.KindOpenAIChat,
			body:  `{"usage":{"input_tokens":60,"output_tokens":2,"input_tokens_details":{"cached_tokens":10}}}`,
			parse: ParseOpenAIUsage, wantRead: 60, inputKey: "input_tokens", wantTotal: 60,
		},
		{
			name: "responses prompt_tokens alias", kind: protocol.KindOpenAIResponses,
			body:  `{"usage":{"prompt_tokens":55,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":5}}}`,
			parse: ParseOpenAIUsage, wantRead: 55, inputKey: "prompt_tokens", wantTotal: 55,
		},
		{
			name: "stale input details cannot mask chat rewrite", kind: protocol.KindOpenAIChat,
			body:  `{"usage":{"prompt_tokens":40,"completion_tokens":1,"input_tokens_details":{"cached_tokens":0},"prompt_tokens_details":{"cached_tokens":5}}}`,
			parse: ParseOpenAIUsage, wantRead: 40, inputKey: "prompt_tokens", wantTotal: 40,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rewritten, changed := rewriteVirtualCacheResponse([]byte(test.body), test.kind)
			if !changed {
				t.Fatalf("response was not rewritten: %s", rewritten)
			}
			tokens := NormalizeUsageBuckets(test.parse(rewritten), test.kind)
			if tokens.InputTokens != test.wantFresh || tokens.CacheReadTokens != test.wantRead {
				t.Fatalf("rewritten usage = %+v, want fresh=%d cache_read=%d", tokens, test.wantFresh, test.wantRead)
			}
			var payload map[string]any
			if err := json.Unmarshal(rewritten, &payload); err != nil {
				t.Fatalf("decode rewritten response: %v", err)
			}
			usage, _ := payload["usage"].(map[string]any)
			if test.inputKey != "" && mapInt(usage, test.inputKey) != test.wantTotal {
				t.Fatalf("total input changed: key=%s want=%d body=%s", test.inputKey, test.wantTotal, rewritten)
			}
		})
	}
}

func TestVirtualCacheSSETransformerHandlesSplitAndCRLFEvents(t *testing.T) {
	transformer := newVirtualCacheSSETransformer(protocol.KindOpenAIChat)
	first := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\r\n\r\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":40,")
	second := []byte("\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":5}}}\r\n\r\ndata: [DONE]\r\n\r\n")
	var out bytes.Buffer
	out.Write(transformer.Transform(first, false))
	out.Write(transformer.Transform(second, true))
	if !transformer.Applied() {
		t.Fatal("transformer did not report a rewritten usage event")
	}
	tokens := NormalizeUsageBuckets(ParseOpenAISSEUsage(out.Bytes()), protocol.KindOpenAIChat)
	if tokens.InputTokens != 0 || tokens.CacheReadTokens != 40 || tokens.OutputTokens != 2 {
		t.Fatalf("rewritten SSE usage = %+v\n%s", tokens, out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("data: [DONE]")) || !bytes.Contains(out.Bytes(), []byte("\\\"content\\\":\\\"ok\\\"")) && !bytes.Contains(out.Bytes(), []byte(`"content":"ok"`)) {
		t.Fatalf("content or terminal frame lost: %s", out.String())
	}
}

func TestRewriteVirtualCacheResponseLeavesZeroFreshInputUntouched(t *testing.T) {
	body := []byte(`{"usage":{"prompt_tokens":12,"prompt_tokens_details":{"cached_tokens":12}}}`)
	rewritten, changed := rewriteVirtualCacheResponse(body, protocol.KindOpenAIChat)
	if changed || !bytes.Equal(rewritten, body) {
		t.Fatalf("zero-fresh response changed: %s", rewritten)
	}
}

func TestRewriteVirtualCacheResponseKeepsCompatibilityAliasesInSync(t *testing.T) {
	body := []byte(`{"usage":{"prompt_tokens":100,"prompt_tokens_details":{"cached_tokens":20,"cache_write_tokens":0},"cache_read_tokens":20,"cache_hit_tokens":20,"cache_creation_tokens":10}}`)
	rewritten, changed := rewriteVirtualCacheResponse(body, protocol.KindOpenAIChat)
	if !changed {
		t.Fatal("response was not rewritten")
	}
	var payload map[string]any
	if err := json.Unmarshal(rewritten, &payload); err != nil {
		t.Fatal(err)
	}
	usage, _ := payload["usage"].(map[string]any)
	for _, key := range []string{"cache_read_input_tokens", "cache_read_tokens", "cache_hit_tokens"} {
		if got := mapInt(usage, key); got != 90 {
			t.Fatalf("%s=%d, want 90: %s", key, got, rewritten)
		}
	}
	details, _ := usage["prompt_tokens_details"].(map[string]any)
	if got := mapInt(details, "cache_write_tokens"); got != 10 {
		t.Fatalf("nested cache creation=%d, want 10: %s", got, rewritten)
	}
}

func TestVirtualCacheSSETransformerNoUsageFastPath(t *testing.T) {
	transformer := newVirtualCacheSSETransformer(protocol.KindOpenAIChat)
	chunk := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
	out := transformer.Transform(chunk, false)
	if len(out) == 0 || &out[0] != &chunk[0] {
		t.Fatal("usage-free complete SSE frame was copied")
	}
	if transformer.Applied() {
		t.Fatal("usage-free frame unexpectedly marked as rewritten")
	}
}

func TestVirtualCacheSSETransformerSkipsNonObjectUsageAndTokenText(t *testing.T) {
	tests := [][]byte{
		[]byte("event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"usage\":null}}\n\n"),
		[]byte("data: {\"choices\":[{\"delta\":{\"content\":\"output_tokens is only text\"}}]}\n\n"),
	}
	for _, payload := range tests {
		transformer := newVirtualCacheSSETransformerPercent(protocol.KindOpenAIResponses, 50)
		out := transformer.Transform(payload, false)
		if len(out) == 0 || &out[0] != &payload[0] {
			t.Fatalf("unchanged event was copied: %s", payload)
		}
		if transformer.Applied() {
			t.Fatalf("unchanged event was marked as rewritten: %s", payload)
		}
	}
}

func TestVirtualCacheSSETransformerRewritesCompleteEventBatch(t *testing.T) {
	transformer := newVirtualCacheSSETransformerPercent(protocol.KindOpenAIChat, 50)
	payload := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":20}}}\n\ndata: [DONE]\n\n")
	out := transformer.Transform(payload, false)
	if !transformer.Applied() {
		t.Fatal("complete event batch was not rewritten")
	}
	got := NormalizeUsageBuckets(ParseOpenAISSEUsage(out), protocol.KindOpenAIChat)
	if got.InputTokens != 40 || got.CacheReadTokens != 60 || got.OutputTokens != 2 {
		t.Fatalf("rewritten batch usage=%+v, want fresh=40 read=60 output=2", got)
	}
	if !bytes.Contains(out, []byte(`"content":"ok"`)) || !bytes.Contains(out, []byte("data: [DONE]")) {
		t.Fatalf("rewritten batch lost content or terminal event: %s", out)
	}
}

func TestRewriteVirtualCacheResponsePreservesOuterJSON(t *testing.T) {
	body := []byte(`{ "type" : "response.completed", "response" : { "output" : [ { "text" : "keep formatting" } ], "usage" : {"input_tokens":100,"output_tokens":2,"input_tokens_details":{"cached_tokens":20}} }, "metadata" : {"usage":null} }`)
	rewritten, changed := rewriteVirtualCacheResponsePercent(body, protocol.KindOpenAIResponses, 50)
	if !changed {
		t.Fatal("nested response usage was not rewritten")
	}
	if !bytes.Contains(rewritten, []byte(`"output" : [ { "text" : "keep formatting" } ]`)) || !bytes.Contains(rewritten, []byte(`"metadata" : {"usage":null}`)) {
		t.Fatalf("outer response JSON was unnecessarily re-encoded: %s", rewritten)
	}
	var payload map[string]any
	if err := json.Unmarshal(rewritten, &payload); err != nil {
		t.Fatal(err)
	}
	response, _ := payload["response"].(map[string]any)
	usage, _ := response["usage"].(map[string]any)
	details, _ := usage["input_tokens_details"].(map[string]any)
	if got := mapInt(details, "cached_tokens"); got != 60 {
		t.Fatalf("nested cached_tokens=%d, want 60: %s", got, rewritten)
	}
}

func TestRewriteVirtualCacheResponseSupportsEscapedUsageKey(t *testing.T) {
	body := []byte(`{"response":{"us\u0061ge":{"input_tokens":10,"output_tokens":1}}}`)
	rewritten, changed := rewriteVirtualCacheResponse(body, protocol.KindOpenAIResponses)
	if !changed {
		t.Fatal("escaped usage key was not rewritten")
	}
	var payload map[string]any
	if err := json.Unmarshal(rewritten, &payload); err != nil {
		t.Fatal(err)
	}
	response, _ := payload["response"].(map[string]any)
	usage, _ := response["usage"].(map[string]any)
	details, _ := usage["input_tokens_details"].(map[string]any)
	if got := mapInt(details, "cached_tokens"); got != 10 {
		t.Fatalf("escaped usage cached_tokens=%d, want 10: %s", got, rewritten)
	}
}

func TestRewriteVirtualCacheResponsePercent(t *testing.T) {
	openAI := []byte(`{"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":20,"cache_write_tokens":10}}}`)
	rewritten, changed := rewriteVirtualCacheResponsePercent(openAI, protocol.KindOpenAIResponses, 50)
	if !changed {
		t.Fatal("partial OpenAI response was not rewritten")
	}
	got := NormalizeUsageBuckets(ParseOpenAIUsage(rewritten), protocol.KindOpenAIResponses)
	if got.InputTokens != 35 || got.CacheReadTokens != 55 || got.CacheCreationTokens != 10 {
		t.Fatalf("partial OpenAI usage=%+v, want fresh=35 read=55 creation=10", got)
	}

	anthropic := []byte(`{"usage":{"input_tokens":101,"cache_read_input_tokens":7,"cache_creation_input_tokens":3}}`)
	rewritten, changed = rewriteVirtualCacheResponsePercent(anthropic, protocol.KindAnthropic, 50)
	if !changed {
		t.Fatal("partial Anthropic response was not rewritten")
	}
	got = ParseAnthropicUsage(rewritten)
	if got.InputTokens != 51 || got.CacheReadTokens != 57 || got.CacheCreationTokens != 3 {
		t.Fatalf("partial Anthropic usage=%+v, want fresh=51 read=57 creation=3", got)
	}

	body := []byte(`{"usage":{"input_tokens":10}}`)
	rewritten, changed = rewriteVirtualCacheResponsePercent(body, protocol.KindOpenAIResponses, 0)
	if changed || !bytes.Equal(rewritten, body) {
		t.Fatalf("zero percent changed response: %s", rewritten)
	}
}

func TestVirtualCacheSSETransformerPercent(t *testing.T) {
	transformer := newVirtualCacheSSETransformerPercent(protocol.KindOpenAIChat, 50)
	out := transformer.Transform([]byte("data: {\"usage\":{\"prompt_tokens\":100,\"prompt_tokens_details\":{\"cached_tokens\":20}}}\n\n"), true)
	if !transformer.Applied() {
		t.Fatal("partial SSE response was not rewritten")
	}
	got := NormalizeUsageBuckets(ParseOpenAISSEUsage(out), protocol.KindOpenAIChat)
	if got.InputTokens != 40 || got.CacheReadTokens != 60 {
		t.Fatalf("partial SSE usage=%+v, want fresh=40 read=60", got)
	}
}

func TestVirtualCachePercentAfterAnthropicToResponsesConversion(t *testing.T) {
	converter := protocol.NewAnthropicToResponsesStream("m")
	transformer := newVirtualCacheSSETransformerPercent(protocol.KindOpenAIResponses, 25)
	var out bytes.Buffer
	write := func(frames [][]byte) {
		for _, frame := range frames {
			out.Write(transformer.Transform(frame, false))
		}
	}

	write(converter.Feed("message_start", `{"type":"message_start","message":{"id":"m1","usage":{"input_tokens":100,"cache_read_input_tokens":20,"cache_creation_input_tokens":10}}}`))
	write(converter.Feed("message_delta", `{"type":"message_delta","usage":{"input_tokens":0,"output_tokens":4,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}}`))
	write(converter.Feed("message_stop", `{"type":"message_stop"}`))
	out.Write(transformer.Transform(nil, true))

	if !transformer.Applied() {
		t.Fatal("virtual cache was not applied to the converted Responses usage")
	}
	got := NormalizeUsageBuckets(ParseOpenAISSEUsage(out.Bytes()), protocol.KindOpenAIResponses)
	if got.InputTokens != 75 || got.CacheReadTokens != 45 || got.CacheCreationTokens != 10 || got.OutputTokens != 4 {
		t.Fatalf("converted Responses usage=%+v, want fresh=75 read=45 creation=10 output=4; body=%s", got, out.String())
	}
}

func BenchmarkVirtualCacheSSETransformer(b *testing.B) {
	benchmarks := []struct {
		name    string
		kind    protocol.Kind
		payload []byte
	}{
		{
			name:    "chat_content",
			kind:    protocol.KindOpenAIChat,
			payload: []byte("data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"hello world\"}}]}\n\n"),
		},
		{
			name:    "chat_content_with_token_field",
			kind:    protocol.KindOpenAIChat,
			payload: []byte("data: {\"id\":\"chatcmpl-1\",\"choices\":[{\"delta\":{\"content\":\"the output_tokens field is documented here\"}}]}\n\n"),
		},
		{
			name:    "responses_lifecycle_null_usage",
			kind:    protocol.KindOpenAIResponses,
			payload: []byte("event: response.in_progress\ndata: {\"type\":\"response.in_progress\",\"response\":{\"id\":\"resp-1\",\"status\":\"in_progress\",\"usage\":null}}\n\n"),
		},
	}
	for _, benchmark := range benchmarks {
		b.Run(benchmark.name, func(b *testing.B) {
			transformer := newVirtualCacheSSETransformerPercent(benchmark.kind, 50)
			b.ReportAllocs()
			b.SetBytes(int64(len(benchmark.payload)))
			for i := 0; i < b.N; i++ {
				out := transformer.Transform(benchmark.payload, false)
				if len(out) != len(benchmark.payload) {
					b.Fatalf("transformed payload length=%d, want %d", len(out), len(benchmark.payload))
				}
			}
		})
	}

	b.Run("responses_completed_large", func(b *testing.B) {
		payload := []byte("event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-1\",\"output\":[{\"type\":\"message\",\"content\":[{\"type\":\"output_text\",\"text\":\"")
		payload = append(payload, bytes.Repeat([]byte("x"), 64<<10)...)
		payload = append(payload, []byte("\"}]}],\"usage\":{\"input_tokens\":1000,\"output_tokens\":100,\"input_tokens_details\":{\"cached_tokens\":100}}}}\n\n")...)
		transformer := newVirtualCacheSSETransformerPercent(protocol.KindOpenAIResponses, 50)
		b.ReportAllocs()
		b.SetBytes(int64(len(payload)))
		for i := 0; i < b.N; i++ {
			out := transformer.Transform(payload, false)
			if len(out) == 0 || !transformer.Applied() {
				b.Fatal("large completed response was not rewritten")
			}
		}
	})
}
