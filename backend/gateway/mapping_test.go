package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

func TestRewriteModelInBody(t *testing.T) {
	in := []byte(`{"model":"a","stream":true}`)
	out := RewriteModelInBody(in, "b")
	if ExtractModelFromBody(out) != "b" {
		t.Fatalf("%s", out)
	}
	if !ExtractStreamFlag(out) {
		t.Fatal("stream lost")
	}
}

func TestParseOpenAIUsage(t *testing.T) {
	body := []byte(`{"usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	u := ParseOpenAIUsage(body)
	if u.InputTokens != 3 || u.OutputTokens != 4 {
		t.Fatalf("%+v", u)
	}
}

func TestParseOpenAIUsage_PromptTokensDetailsCached(t *testing.T) {
	// 对齐 sub2api：prompt_tokens_details.cached_tokens
	body := []byte(`{"usage":{"prompt_tokens":3333,"completion_tokens":60,"prompt_tokens_details":{"cached_tokens":1536},"completion_tokens_details":{"reasoning_tokens":40}}}`)
	u := ParseOpenAIUsage(body)
	if u.InputTokens != 3333 || u.OutputTokens != 60 || u.CacheReadTokens != 1536 || u.ReasoningTokens != 40 {
		t.Fatalf("%+v", u)
	}
	// 拆桶后 input 不含 cache
	split := SplitOpenAIUsageBuckets(u)
	if split.InputTokens != 3333-1536 || split.CacheReadTokens != 1536 {
		t.Fatalf("split=%+v", split)
	}
}

func TestSplitOpenAIUsageBuckets(t *testing.T) {
	// sub2api: actualInput = total - cacheRead - cacheCreate
	raw := UsageTokens{InputTokens: 100, CacheReadTokens: 30, CacheCreationTokens: 10, OutputTokens: 5}
	s := SplitOpenAIUsageBuckets(raw)
	if s.InputTokens != 60 || s.CacheReadTokens != 30 || s.CacheCreationTokens != 10 || s.OutputTokens != 5 {
		t.Fatalf("%+v", s)
	}
	// 不过度扣成负数
	s2 := SplitOpenAIUsageBuckets(UsageTokens{InputTokens: 10, CacheReadTokens: 20})
	if s2.InputTokens != 0 {
		t.Fatalf("%+v", s2)
	}
}

func TestParseOpenAIUsage_InputTokensDetailsCached(t *testing.T) {
	// Responses 风格 details
	body := []byte(`{"usage":{"input_tokens":100,"output_tokens":10,"input_tokens_details":{"cached_tokens":40,"cache_write_tokens":5}}}`)
	u := ParseOpenAIUsage(body)
	if u.InputTokens != 100 || u.CacheReadTokens != 40 || u.CacheCreationTokens != 5 {
		t.Fatalf("%+v", u)
	}
}

func TestOpenAICacheReadSkipsStaleNestedZero(t *testing.T) {
	usage := map[string]any{
		"input_tokens_details":  map[string]any{"cached_tokens": 0},
		"prompt_tokens_details": map[string]any{"cached_tokens": 5},
		"cache_read_tokens":     12,
	}
	if got := openAICacheReadTokensFromUsage(usage); got != 5 {
		t.Fatalf("cache read=%d, want later nested positive value=5", got)
	}
}

func TestOpenAICacheCreationPriorityAndStaleZeroFallback(t *testing.T) {
	usage := map[string]any{
		"input_tokens_details":  map[string]any{"cache_creation_tokens": 5},
		"prompt_tokens_details": map[string]any{"cache_write_tokens": 7},
	}
	if got := openAICacheCreationTokensFromUsage(usage); got != 7 {
		t.Fatalf("cache creation=%d, want prompt cache_write_tokens=7", got)
	}
	usage["input_tokens_details"] = map[string]any{
		"cache_write_tokens":    0,
		"cache_creation_tokens": 5,
	}
	usage["prompt_tokens_details"] = map[string]any{}
	usage["cache_creation_tokens"] = 11
	if got := openAICacheCreationTokensFromUsage(usage); got != 11 {
		t.Fatalf("cache creation=%d, want top-level stale-zero fallback=11", got)
	}
}

func TestParseOpenAISSEUsage_LastUsageChunk(t *testing.T) {
	sse := "" +
		"data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n" +
		"data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":2,\"prompt_tokens_details\":{\"cached_tokens\":30}}}\n\n" +
		"data: [DONE]\n\n"
	u := ParseOpenAISSEUsage([]byte(sse))
	if u.InputTokens != 100 || u.OutputTokens != 2 || u.CacheReadTokens != 30 {
		t.Fatalf("%+v", u)
	}
}

func TestParseOpenAISSEUsage_UsageOnlyChunkLikeSub2API(t *testing.T) {
	// 模拟 sub2api/OpenAI：中间无 usage，末帧 choices=[] + usage
	sse := "" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"a\"}}]}\n\n" +
		"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"b\"},\"finish_reason\":\"stop\"}]}\n\n" +
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3350,\"completion_tokens\":59,\"total_tokens\":3409,\"prompt_tokens_details\":{\"cached_tokens\":1200,\"audio_tokens\":0}}}\n\n" +
		"data: [DONE]\n\n"
	u := ParseOpenAISSEUsage([]byte(sse))
	if u.InputTokens != 3350 || u.OutputTokens != 59 || u.CacheReadTokens != 1200 {
		t.Fatalf("want cache 1200, got %+v", u)
	}
}

func TestEnsureStreamUsageOption(t *testing.T) {
	in := []byte(`{"model":"g","stream":true,"messages":[]}`)
	out := EnsureStreamUsageOption(in, true)
	if !bytesContains(out, []byte(`"include_usage":true`)) {
		t.Fatalf("%s", out)
	}
	// 强制覆盖 false
	in2 := []byte(`{"stream":true,"stream_options":{"include_usage":false}}`)
	out2 := EnsureStreamUsageOption(in2, true)
	if !bytesContains(out2, []byte(`"include_usage":true`)) {
		t.Fatalf("%s", out2)
	}
}

func bytesContains(b, sub []byte) bool {
	return len(b) >= len(sub) && (string(b) == string(sub) || len(sub) == 0 ||
		(len(b) > 0 && containsBytes(b, sub)))
}

func containsBytes(b, sub []byte) bool {
	for i := 0; i+len(sub) <= len(b); i++ {
		ok := true
		for j := 0; j < len(sub); j++ {
			if b[i+j] != sub[j] {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

func TestExtractMetaFromBody_ReasoningEffortPriority(t *testing.T) {
	// 1) nested reasoning.effort 优先于扁平 reasoning_effort
	_, effort := ExtractMetaFromBody([]byte(`{
		"model":"gpt-x",
		"reasoning":{"effort":"high"},
		"reasoning_effort":"low",
		"output_config":{"effort":"medium"}
	}`))
	if effort != "high" {
		t.Fatalf("nested wins: got %q", effort)
	}

	// 2) 扁平 reasoning_effort
	_, effort = ExtractMetaFromBody([]byte(`{"model":"gpt-x","reasoning_effort":"Medium"}`))
	if effort != "medium" {
		t.Fatalf("flat: got %q", effort)
	}

	// 3) Claude output_config.effort（无 OpenAI 字段时）
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"claude-opus-4-6",
		"thinking":{"type":"adaptive"},
		"output_config":{"effort":"max"}
	}`))
	if effort != "max" {
		t.Fatalf("claude output_config max kept: got %q", effort)
	}

	// 4) 模型名后缀推导
	_, effort = ExtractMetaFromBody([]byte(`{"model":"openai/o3-high","messages":[]}`))
	if effort != "high" {
		t.Fatalf("model suffix: got %q", effort)
	}

	// 5) 无任何线索 → 空
	_, effort = ExtractMetaFromBody([]byte(`{"model":"gpt-4o","messages":[]}`))
	if effort != "" {
		t.Fatalf("want empty, got %q", effort)
	}
}

func TestExtractMetaFromBody_NormalizeVariants(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"x-high nested", `{"reasoning":{"effort":"x-high"}}`, "xhigh"},
		{"x_high flat", `{"reasoning_effort":"x_high"}`, "xhigh"},
		{"max openai → xhigh", `{"reasoning_effort":"max"}`, "xhigh"},
		{"extrahigh", `{"reasoning":{"effort":"ExtraHigh"}}`, "xhigh"},
		{"none → empty", `{"reasoning_effort":"none"}`, ""},
		{"minimal → empty", `{"reasoning":{"effort":"minimal"}}`, ""},
		{"unknown → empty", `{"reasoning_effort":"banana"}`, ""},
		{"claude x-high", `{"output_config":{"effort":"x-high"}}`, "xhigh"},
		{"claude max kept", `{"output_config":{"effort":"MAX"}}`, "max"},
		{"model xhigh suffix", `{"model":"gpt-5.1-xhigh"}`, "xhigh"},
		{"model none suffix empty", `{"model":"gpt-5-none"}`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, got := ExtractMetaFromBody([]byte(tc.body))
			if got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestExtractMetaFromBody_ServiceTier(t *testing.T) {
	tier, effort := ExtractMetaFromBody([]byte(`{
		"service_tier":"priority",
		"reasoning":{"effort":"low"}
	}`))
	if tier != "priority" || effort != "low" {
		t.Fatalf("tier=%q effort=%q", tier, effort)
	}
}

func TestNormalizeOpenAIReasoningEffort(t *testing.T) {
	if normalizeOpenAIReasoningEffort("  HIGH ") != "high" {
		t.Fatal()
	}
	if normalizeOpenAIReasoningEffort("max") != "xhigh" {
		t.Fatal()
	}
	if normalizeOpenAIReasoningEffort("none") != "" {
		t.Fatal()
	}
}

func TestNormalizeClaudeOutputEffort(t *testing.T) {
	if normalizeClaudeOutputEffort("max") != "max" {
		t.Fatal("claude max should stay max")
	}
	if normalizeClaudeOutputEffort("x_high") != "xhigh" {
		t.Fatal()
	}
	if normalizeClaudeOutputEffort("minimal") != "" {
		t.Fatal()
	}
}

func TestDeriveReasoningEffortFromModel(t *testing.T) {
	if deriveReasoningEffortFromModel("provider/o4-mini-high") != "high" {
		t.Fatal()
	}
	if deriveReasoningEffortFromModel("gpt-4o") != "" {
		t.Fatal()
	}
	if deriveReasoningEffortFromModel("claude-opus-4-6") != "" {
		// trailing "6" is not a known effort
		t.Fatal()
	}
}

func TestExtractMetaFromBody_ThinkingEnabledDefaultHigh(t *testing.T) {
	// Kimi + thinking enabled、无 effort → high
	_, effort := ExtractMetaFromBody([]byte(`{
		"model":"kimi-k2.5",
		"thinking":{"type":"adaptive"},
		"messages":[]
	}`))
	if effort != "high" {
		t.Fatalf("kimi thinking default: got %q", effort)
	}

	// GLM enabled
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"glm-5.2",
		"thinking":{"type":"enabled"}
	}`))
	if effort != "high" {
		t.Fatalf("glm: got %q", effort)
	}

	// MiniMax-M
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"MiniMax-M2.5",
		"thinking":{"type":"enabled"}
	}`))
	if effort != "high" {
		t.Fatalf("minimax: got %q", effort)
	}

	// Qwen thinking 变体
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"qwen3-235b-a22b-thinking-2507",
		"thinking":{"type":"enabled"}
	}`))
	if effort != "high" {
		t.Fatalf("qwen-thinking: got %q", effort)
	}

	// DeepSeek：有原生 effort，不注入默认
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"deepseek-v4-pro",
		"thinking":{"type":"enabled"}
	}`))
	if effort != "" {
		t.Fatalf("deepseek should not default: got %q", effort)
	}

	// Claude 官方：不注入
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"claude-sonnet-4-6",
		"thinking":{"type":"adaptive"}
	}`))
	if effort != "" {
		t.Fatalf("claude should not default: got %q", effort)
	}

	// thinking 关闭不注入
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"kimi-k2.5",
		"thinking":{"type":"disabled"}
	}`))
	if effort != "" {
		t.Fatalf("disabled thinking: got %q", effort)
	}

	// 显式 effort 不被覆盖
	_, effort = ExtractMetaFromBody([]byte(`{
		"model":"kimi-k2.5",
		"thinking":{"type":"enabled"},
		"output_config":{"effort":"low"}
	}`))
	if effort != "low" {
		t.Fatalf("explicit effort wins: got %q", effort)
	}
}

func TestApplyThinkingEnabledEffortFallback_MappedUpstream(t *testing.T) {
	// 客户端 claude-*，映射到 kimi-* 后才命中白名单
	got := ApplyThinkingEnabledEffortFallback("", true, "kimi-k2.5", "claude-sonnet-4-6")
	if got != "high" {
		t.Fatalf("mapped kimi: got %q", got)
	}
	// 已有 effort 不覆盖
	got = ApplyThinkingEnabledEffortFallback("medium", true, "kimi-k2.5")
	if got != "medium" {
		t.Fatalf("preserve: got %q", got)
	}
	// thinking 未开
	got = ApplyThinkingEnabledEffortFallback("", false, "kimi-k2.5")
	if got != "" {
		t.Fatalf("no thinking: got %q", got)
	}
	// 上游仍是 claude
	got = ApplyThinkingEnabledEffortFallback("", true, "claude-opus-4-6", "claude-opus-4-6")
	if got != "" {
		t.Fatalf("claude upstream: got %q", got)
	}
}

func TestIsPassbackRequiredThinkingModel(t *testing.T) {
	cases := map[string]bool{
		"kimi-k2.5":                   true,
		"moonshot-v1":                 true,
		"glm-4.6":                     true,
		"minimax-m2.7":                true,
		"MiniMax-M2":                  true,
		"qwen3-next-80b-a3b-thinking": true,
		"qwen-3-72b-thinking":         true,
		"qwen3-72b":                   false, // 无 -thinking
		"deepseek-v4-pro":             true,  // passback 族，但 default effort 会再排除
		"claude-sonnet-4-6":           false,
		"gpt-4o":                      false,
		"provider/kimi-k2.5":          true,
	}
	for model, want := range cases {
		if got := isPassbackRequiredThinkingModel(model); got != want {
			t.Fatalf("%s: got %v want %v", model, got, want)
		}
	}
	// DeepSeek passback 但不默认 high
	if defaultEffortForThinkingEnabled("deepseek-chat") != "" {
		t.Fatal("deepseek default should be empty")
	}
}

func TestParseAnthropicUsage(t *testing.T) {
	body := []byte(`{"usage":{"input_tokens":1,"output_tokens":2,"cache_read_input_tokens":5}}`)
	u := ParseAnthropicUsage(body)
	if u.InputTokens != 1 || u.OutputTokens != 2 || u.CacheReadTokens != 5 {
		t.Fatalf("%+v", u)
	}
}

func TestNormalizeUsageBucketsPreservesAnthropicFreshInput(t *testing.T) {
	raw := UsageTokens{InputTokens: 100, CacheReadTokens: 40, CacheCreationTokens: 10}
	anthropic := NormalizeUsageBuckets(raw, protocol.KindAnthropic)
	if anthropic.InputTokens != 100 || anthropic.CacheReadTokens != 40 || anthropic.CacheCreationTokens != 10 {
		t.Fatalf("anthropic buckets=%+v, want fresh input preserved", anthropic)
	}
	openAI := NormalizeUsageBuckets(raw, protocol.KindOpenAIChat)
	if openAI.InputTokens != 50 || openAI.CacheReadTokens != 40 || openAI.CacheCreationTokens != 10 {
		t.Fatalf("openai buckets=%+v, want cache fields subtracted", openAI)
	}
}
