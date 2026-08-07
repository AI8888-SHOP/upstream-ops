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
			body: `{"usage":{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":20},"cache_creation_input_tokens":10}}`,
			parse: ParseOpenAIUsage, wantRead: 90, inputKey: "prompt_tokens", wantTotal: 100,
		},
		{
			name: "responses", kind: protocol.KindOpenAIResponses,
			body: `{"usage":{"input_tokens":80,"output_tokens":4,"total_tokens":84,"input_tokens_details":{"cached_tokens":15},"cache_creation_input_tokens":5}}`,
			parse: ParseOpenAIUsage, wantRead: 75, inputKey: "input_tokens", wantTotal: 80,
		},
		{
			name: "anthropic", kind: protocol.KindAnthropic,
			body: `{"usage":{"input_tokens":70,"output_tokens":3,"cache_read_input_tokens":10,"cache_creation_input_tokens":5}}`,
			parse: ParseAnthropicUsage, wantRead: 80,
		},
		{
			name: "chat input_tokens alias", kind: protocol.KindOpenAIChat,
			body: `{"usage":{"input_tokens":60,"output_tokens":2,"input_tokens_details":{"cached_tokens":10}}}`,
			parse: ParseOpenAIUsage, wantRead: 60, inputKey: "input_tokens", wantTotal: 60,
		},
		{
			name: "responses prompt_tokens alias", kind: protocol.KindOpenAIResponses,
			body: `{"usage":{"prompt_tokens":55,"completion_tokens":2,"prompt_tokens_details":{"cached_tokens":5}}}`,
			parse: ParseOpenAIUsage, wantRead: 55, inputKey: "prompt_tokens", wantTotal: 55,
		},
		{
			name: "stale input details cannot mask chat rewrite", kind: protocol.KindOpenAIChat,
			body: `{"usage":{"prompt_tokens":40,"completion_tokens":1,"input_tokens_details":{"cached_tokens":0},"prompt_tokens_details":{"cached_tokens":5}}}`,
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
