package gateway

import (
	_ "embed"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

//go:embed pricing/default_prices.json
var defaultPricesJSON []byte

// ModelPricing per-token USD 单价。
type ModelPricing struct {
	InputPricePerToken         float64
	OutputPricePerToken        float64
	CacheCreationPricePerToken float64
	CacheReadPricePerToken     float64
}

// HasTokenPrice 是否具备 token 计费单价（对齐 sub2api TokenPricingAbsent 语义）。
func (p ModelPricing) HasTokenPrice() bool {
	return p.InputPricePerToken > 0 || p.OutputPricePerToken > 0 ||
		p.CacheCreationPricePerToken > 0 || p.CacheReadPricePerToken > 0
}

// LiteLLM 原始条目（字段可选；与 sub2api model_prices_and_context_window.json 一致）。
type defaultPriceEntry struct {
	InputCostPerToken           *float64 `json:"input_cost_per_token"`
	OutputCostPerToken          *float64 `json:"output_cost_per_token"`
	CacheCreationInputTokenCost *float64 `json:"cache_creation_input_token_cost"`
	CacheReadInputTokenCost     *float64 `json:"cache_read_input_token_cost"`
}

// PricingCatalog 内置价 + DB 覆盖 + 硬编码家族回退（对齐 sub2api BillingService）。
type PricingCatalog struct {
	mu        sync.RWMutex
	defaults  map[string]ModelPricing
	fallbacks map[string]ModelPricing
	overrides *storage.ModelPriceOverrides
}

func NewPricingCatalog(overrides *storage.ModelPriceOverrides) *PricingCatalog {
	c := &PricingCatalog{
		defaults:  map[string]ModelPricing{},
		fallbacks: map[string]ModelPricing{},
		overrides: overrides,
	}
	c.loadDefaults()
	c.seedFallbackPrices()
	return c
}

func (c *PricingCatalog) loadDefaults() {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(defaultPricesJSON, &raw); err != nil {
		return
	}
	for name, rawEntry := range raw {
		if name == "sample_spec" {
			continue
		}
		var e defaultPriceEntry
		if err := json.Unmarshal(rawEntry, &e); err != nil {
			continue
		}
		// 仅有图片价、无 token 价的条目跳过（避免 token 流量按 $0 计费）
		if e.InputCostPerToken == nil && e.OutputCostPerToken == nil {
			continue
		}
		p := ModelPricing{}
		if e.InputCostPerToken != nil {
			p.InputPricePerToken = *e.InputCostPerToken
		}
		if e.OutputCostPerToken != nil {
			p.OutputPricePerToken = *e.OutputCostPerToken
		}
		if e.CacheCreationInputTokenCost != nil {
			p.CacheCreationPricePerToken = *e.CacheCreationInputTokenCost
		}
		if e.CacheReadInputTokenCost != nil {
			p.CacheReadPricePerToken = *e.CacheReadInputTokenCost
		}
		key := strings.ToLower(strings.TrimSpace(name))
		if key == "" {
			continue
		}
		c.defaults[key] = p
	}
}

// seedFallbackPrices 对齐 sub2api billing_service.go 硬编码回退价。
// LiteLLM JSON 未必覆盖全部在用模型（如 grok-4.5），无表项时靠此表计费。
func (c *PricingCatalog) seedFallbackPrices() {
	// xAI Grok 4.5: $2 / $6 / cached $0.50 per MTok
	c.fallbacks["grok-4.5"] = ModelPricing{
		InputPricePerToken:     2e-6,
		OutputPricePerToken:    6e-6,
		CacheReadPricePerToken: 0.5e-6,
	}
	// xAI Grok 4.3: $1.25 / $2.50 / cached $0.20 per MTok
	c.fallbacks["grok-4.3"] = ModelPricing{
		InputPricePerToken:     1.25e-6,
		OutputPricePerToken:    2.5e-6,
		CacheReadPricePerToken: 0.2e-6,
	}
	// xAI Grok Build 0.1: $1 / $2 / cached $0.20 per MTok
	c.fallbacks["grok-build-0.1"] = ModelPricing{
		InputPricePerToken:     1e-6,
		OutputPricePerToken:    2e-6,
		CacheReadPricePerToken: 0.2e-6,
	}
	// DeepSeek 官方价（$/token），作家族前缀回退；精确表项见 default_prices.json
	// chat / V3 系常见计费：$0.28 / $0.42 per MTok，缓存读 $0.028
	c.fallbacks["deepseek-chat"] = ModelPricing{
		InputPricePerToken:     2.8e-7,
		OutputPricePerToken:    4.2e-7,
		CacheReadPricePerToken: 2.8e-8,
	}
	c.fallbacks["deepseek-reasoner"] = ModelPricing{
		InputPricePerToken:     2.8e-7,
		OutputPricePerToken:    4.2e-7,
		CacheReadPricePerToken: 2.8e-8,
	}
	// R1 系（LiteLLM deepseek/deepseek-r1）
	c.fallbacks["deepseek-r1"] = ModelPricing{
		InputPricePerToken:  5.5e-7,
		OutputPricePerToken: 2.19e-6,
	}
	c.fallbacks["deepseek-coder"] = ModelPricing{
		InputPricePerToken:  1.4e-7,
		OutputPricePerToken: 2.8e-7,
	}
	c.fallbacks["deepseek-v3"] = ModelPricing{
		InputPricePerToken:     2.7e-7,
		OutputPricePerToken:    1.1e-6,
		CacheReadPricePerToken: 7e-8,
	}
	c.fallbacks["deepseek-v3.2"] = ModelPricing{
		InputPricePerToken:  2.8e-7,
		OutputPricePerToken: 4e-7,
	}
	c.fallbacks["deepseek-v4-flash"] = ModelPricing{
		InputPricePerToken:     1.4e-7,
		OutputPricePerToken:    2.8e-7,
		CacheReadPricePerToken: 2.8e-9,
	}
	c.fallbacks["deepseek-v4-pro"] = ModelPricing{
		InputPricePerToken:     4.35e-7,
		OutputPricePerToken:    8.7e-7,
		CacheReadPricePerToken: 3.625e-9,
	}
}

// DefaultPriceItem 内置默认价（管理端只读展示）。
type DefaultPriceItem struct {
	ModelName                  string  `json:"model_name"`
	InputPricePerToken         float64 `json:"input_price_per_token"`
	OutputPricePerToken        float64 `json:"output_price_per_token"`
	CacheCreationPricePerToken float64 `json:"cache_creation_price_per_token"`
	CacheReadPricePerToken     float64 `json:"cache_read_price_per_token"`
}

// ListDefaults 返回内置价目表（含硬编码 fallback），可选子串过滤。
func (c *PricingCatalog) ListDefaults(query string) []DefaultPriceItem {
	c.mu.RLock()
	defer c.mu.RUnlock()
	q := strings.ToLower(strings.TrimSpace(query))
	merged := make(map[string]ModelPricing, len(c.defaults)+len(c.fallbacks))
	for k, v := range c.defaults {
		merged[k] = v
	}
	// fallback 仅在 defaults 未覆盖时展示
	for k, v := range c.fallbacks {
		if _, ok := merged[k]; !ok {
			merged[k] = v
		}
	}
	out := make([]DefaultPriceItem, 0, len(merged))
	for name, p := range merged {
		if q != "" && !strings.Contains(name, q) {
			continue
		}
		out = append(out, DefaultPriceItem{
			ModelName:                  name,
			InputPricePerToken:         p.InputPricePerToken,
			OutputPricePerToken:        p.OutputPricePerToken,
			CacheCreationPricePerToken: p.CacheCreationPricePerToken,
			CacheReadPricePerToken:     p.CacheReadPricePerToken,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ModelName < out[j].ModelName })
	return out
}

// Resolve 优先 override，再 LiteLLM 默认表（模糊匹配），再硬编码家族回退。
// 对齐 sub2api：BillingService.GetModelPricing → PricingService → fallbackPrices。
func (c *PricingCatalog) Resolve(model string) ModelPricing {
	model = strings.TrimSpace(model)
	if model == "" {
		return ModelPricing{}
	}
	// DB 覆盖：原名与小写都试
	if c.overrides != nil {
		if item, err := c.overrides.FindByModel(model); err == nil && item != nil && item.Enabled {
			return ModelPricing{
				InputPricePerToken:         item.InputPricePerToken,
				OutputPricePerToken:        item.OutputPricePerToken,
				CacheCreationPricePerToken: item.CacheCreationPricePerToken,
				CacheReadPricePerToken:     item.CacheReadPricePerToken,
			}
		}
		lower := strings.ToLower(model)
		if lower != model {
			if item, err := c.overrides.FindByModel(lower); err == nil && item != nil && item.Enabled {
				return ModelPricing{
					InputPricePerToken:         item.InputPricePerToken,
					OutputPricePerToken:        item.OutputPricePerToken,
					CacheCreationPricePerToken: item.CacheCreationPricePerToken,
					CacheReadPricePerToken:     item.CacheReadPricePerToken,
				}
			}
		}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, candidate := range modelLookupCandidates(model) {
		if p, ok := c.defaults[candidate]; ok && p.HasTokenPrice() {
			return p
		}
	}

	// 日期后缀模糊：claude-xxx-20250219 → claude-xxx
	for _, candidate := range modelLookupCandidates(model) {
		if i := strings.LastIndex(candidate, "-20"); i > 0 && len(candidate)-i >= 9 {
			base := candidate[:i]
			if p, ok := c.defaults[base]; ok && p.HasTokenPrice() {
				return p
			}
		}
		// 4-5 ↔ 4.5 变体
		alt := strings.ReplaceAll(candidate, "-4-5-", "-4.5-")
		alt = strings.ReplaceAll(alt, "-4-5", "-4.5")
		if alt != candidate {
			if p, ok := c.defaults[alt]; ok && p.HasTokenPrice() {
				return p
			}
		}
	}

	// 硬编码家族回退（含 grok-4.5）
	if p := c.fallbackForModel(model); p.HasTokenPrice() {
		return p
	}
	return ModelPricing{}
}

func modelLookupCandidates(model string) []string {
	model = strings.ToLower(strings.TrimSpace(model))
	model = strings.TrimLeft(model, "/")
	model = strings.TrimPrefix(model, "models/")
	candidates := []string{model}
	if idx := strings.LastIndex(model, "/"); idx >= 0 && idx+1 < len(model) {
		candidates = append(candidates, model[idx+1:])
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(candidates))
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if _, ok := seen[c]; ok {
			continue
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out
}

// fallbackForModel 对齐 sub2api getFallbackPricing 中与网关相关的关键分支。
// 调用方须已持有 c.mu 读锁（或单线程初始化后只读）。
func (c *PricingCatalog) fallbackForModel(model string) ModelPricing {
	modelLower := strings.ToLower(strings.TrimSpace(model))
	switch modelLower {
	case "grok", "grok-latest", "grok-4.5", "grok-4.5-latest", "grok-build-latest":
		return c.fallbacks["grok-4.5"]
	case "grok-4.3",
		"grok-4.20-0309-reasoning",
		"grok-4.20-0309-non-reasoning",
		"grok-4.20-multi-agent-0309",
		"grok-4.20-reasoning",
		"grok-4.20-non-reasoning":
		return c.fallbacks["grok-4.3"]
	case "grok-build", "grok-build-0.1", "grok-composer", "grok-composer-2.5-fast", "composer-2.5":
		return c.fallbacks["grok-build-0.1"]
	}
	// 前缀宽松匹配：grok-4.5-xxx
	if strings.HasPrefix(modelLower, "grok-4.5") {
		return c.fallbacks["grok-4.5"]
	}
	if strings.HasPrefix(modelLower, "grok-4.3") {
		return c.fallbacks["grok-4.3"]
	}
	if strings.HasPrefix(modelLower, "grok-build") {
		return c.fallbacks["grok-build-0.1"]
	}

	// DeepSeek 家族：上游常带 deepseek/ 前缀或日期/变体后缀
	if strings.Contains(modelLower, "deepseek") {
		base := modelLower
		if idx := strings.LastIndex(base, "/"); idx >= 0 && idx+1 < len(base) {
			base = base[idx+1:]
		}
		switch {
		case strings.Contains(base, "v4-pro") || strings.Contains(base, "v4.pro"):
			return c.priceOrFallback("deepseek-v4-pro")
		case strings.Contains(base, "v4-flash") || strings.Contains(base, "v4.flash"):
			return c.priceOrFallback("deepseek-v4-flash")
		case strings.Contains(base, "coder"):
			return c.priceOrFallback("deepseek-coder")
		case strings.Contains(base, "reasoner"):
			return c.priceOrFallback("deepseek-reasoner")
		case strings.Contains(base, "r1"):
			return c.priceOrFallback("deepseek-r1")
		case strings.Contains(base, "v3.2") || strings.Contains(base, "v3-2"):
			return c.priceOrFallback("deepseek-v3.2")
		case strings.Contains(base, "v3.1") || strings.Contains(base, "v3-1"):
			return c.priceOrFallback("deepseek-v3")
		case strings.Contains(base, "v3"):
			return c.priceOrFallback("deepseek-v3")
		default:
			return c.priceOrFallback("deepseek-chat")
		}
	}
	return ModelPricing{}
}

// priceOrFallback 优先 defaults 表，其次 seed fallbacks。
func (c *PricingCatalog) priceOrFallback(name string) ModelPricing {
	if p, ok := c.defaults[name]; ok && p.HasTokenPrice() {
		return p
	}
	if p, ok := c.fallbacks[name]; ok && p.HasTokenPrice() {
		return p
	}
	return ModelPricing{}
}

// UsageTokens token 计数。
//
// 约定（对齐 sub2api RecordUsage）：
//   - 从上游解析时，InputTokens 可能是「总输入」（含 cache 明细）
//   - 落库 / 计费前应调用 SplitOpenAIUsageBuckets，使 InputTokens 与
//     CacheRead / CacheCreation 互斥，避免双重计费
type UsageTokens struct {
	InputTokens           int
	OutputTokens          int
	CacheCreationTokens   int
	CacheReadTokens       int
	CacheCreation5mTokens int
	CacheCreation1hTokens int
	ImageOutputTokens     int
	// ReasoningTokens 来自 completion_tokens_details.reasoning_tokens（展示用，不单独计费）
	ReasoningTokens int
}

// SplitOpenAIUsageBuckets 对齐 sub2api：
//
//	OpenAI 的 prompt_tokens / input_tokens 是总输入，通常已包含 cache_read / cache_creation。
//	拆成互斥桶后再计费与落库，避免缓存部分既算「输入」又算「读/写缓存」。
//
//	actualInput = max(0, totalInput - cacheRead - cacheCreation)
func SplitOpenAIUsageBuckets(raw UsageTokens) UsageTokens {
	out := raw
	actual := raw.InputTokens - raw.CacheReadTokens - raw.CacheCreationTokens
	if actual < 0 {
		actual = 0
	}
	out.InputTokens = actual
	return out
}

// NormalizeUsageBuckets converts parser output into the mutually exclusive
// accounting buckets used by pricing and storage. OpenAI-compatible usage
// reports total input and therefore needs cache fields subtracted; Anthropic
// reports input_tokens as fresh input already, so subtracting again would
// undercount both billing and virtual hedge credits.
func NormalizeUsageBuckets(raw UsageTokens, kind protocol.Kind) UsageTokens {
	if protocol.NormalizeKind(kind) == protocol.KindAnthropic {
		return raw
	}
	return SplitOpenAIUsageBuckets(raw)
}

// CostBreakdown 费用拆分。
type CostBreakdown struct {
	InputCost         float64
	OutputCost        float64
	CacheCreationCost float64
	CacheReadCost     float64
	ImageOutputCost   float64
	TotalCost         float64
	ActualCost        float64
}

// Cost 按本价目与 token 桶计算费用；见 CalculateCost。
func (p ModelPricing) Cost(tokens UsageTokens, rateMultiplier, billingRateMultiplier float64) CostBreakdown {
	return CalculateCost(p, tokens, rateMultiplier, billingRateMultiplier)
}

// CalculateCost base = tokens * unit_price；actual = base × 账号计费倍率。
//
// tokens 应为已 SplitOpenAIUsageBuckets 的互斥桶：
// 输入 / 缓存读 / 缓存写 / 输出 各自乘单价，不再把 cache 算进普通输入。
//
// rateMultiplier / billingRateMultiplier 语义对齐上游同步：二者均为源分组倍率换算结果
// （原值时即源 ratio，不是强制 1）。优先用 billingRateMultiplier 作为账号计费倍率；
// 无效时回退 rateMultiplier。只乘一次，避免「有效倍率 × 计费倍率」双重放大。
func CalculateCost(p ModelPricing, tokens UsageTokens, rateMultiplier, billingRateMultiplier float64) CostBreakdown {
	accountRate := billingRateMultiplier
	if accountRate <= 0 {
		accountRate = rateMultiplier
	}
	if accountRate <= 0 {
		accountRate = 1
	}
	in := float64(tokens.InputTokens) * p.InputPricePerToken
	out := float64(tokens.OutputTokens) * p.OutputPricePerToken
	cc := float64(tokens.CacheCreationTokens) * p.CacheCreationPricePerToken
	cr := float64(tokens.CacheReadTokens) * p.CacheReadPricePerToken
	// image_output 暂用 output 单价
	img := float64(tokens.ImageOutputTokens) * p.OutputPricePerToken
	total := in + out + cc + cr + img
	return CostBreakdown{
		InputCost:         in,
		OutputCost:        out,
		CacheCreationCost: cc,
		CacheReadCost:     cr,
		ImageOutputCost:   img,
		TotalCost:         total,
		ActualCost:        total * accountRate,
	}
}
