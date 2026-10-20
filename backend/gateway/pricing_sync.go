package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const Sub2APIPriceSourceURL = "https://raw.githubusercontent.com/Wei-Shaw/sub2api/main/backend/resources/model-pricing/model_prices_and_context_window.json"
const priceDocumentLimit = 8 << 20

var contextPriceField = regexp.MustCompile(`^(input_cost_per_token|output_cost_per_token|cache_creation_input_token_cost(?:_above_1hr)?|cache_read_input_token_cost)_above_([0-9]+)k_tokens$`)

func contextTiersJSON(tiers []storage.ModelPriceContextTier) string {
	if tiers == nil {
		return "[]"
	}
	b, _ := json.Marshal(tiers)
	return string(b)
}

func pricingFromOverride(item storage.ModelPriceOverride) ModelPricing {
	p := ModelPricing{InputPricePerToken: item.InputPricePerToken, OutputPricePerToken: item.OutputPricePerToken, CacheCreationPricePerToken: item.CacheCreationPricePerToken, CacheReadPricePerToken: item.CacheReadPricePerToken}
	if item.CacheCreation1hPricePerToken != nil {
		p.CacheCreation1hPricePerToken = *item.CacheCreation1hPricePerToken
		p.CacheCreation1hPriceExplicit = true
	}
	_ = json.Unmarshal([]byte(item.ContextTiersJSON), &p.ContextTiers)
	sort.Slice(p.ContextTiers, func(i, j int) bool { return p.ContextTiers[i].AboveInputTokens < p.ContextTiers[j].AboveInputTokens })
	return p
}

func (p ModelPricing) forTokens(tokens UsageTokens) ModelPricing {
	fiveMinute, oneHour := storage.NormalizePriceCacheCreation(tokens.CacheCreationTokens, tokens.CacheCreation5mTokens, tokens.CacheCreation1hTokens)
	contextSize := max(0, tokens.InputTokens) + max(0, tokens.CacheReadTokens) + fiveMinute + oneHour
	for _, t := range p.ContextTiers {
		if contextSize < t.AboveInputTokens || (contextSize == t.AboveInputTokens && !t.Inclusive) {
			break
		}
		if t.InputPricePerToken != nil {
			p.InputPricePerToken = *t.InputPricePerToken
		}
		if t.OutputPricePerToken != nil {
			p.OutputPricePerToken = *t.OutputPricePerToken
		}
		if t.CacheCreationPricePerToken != nil {
			p.CacheCreationPricePerToken = *t.CacheCreationPricePerToken
		}
		if t.CacheCreation1hPricePerToken != nil {
			p.CacheCreation1hPricePerToken = *t.CacheCreation1hPricePerToken
			p.CacheCreation1hPriceExplicit = true
		}
		if t.CacheReadPricePerToken != nil {
			p.CacheReadPricePerToken = *t.CacheReadPricePerToken
		}
	}
	return p
}

func (p ModelPricing) hourlyPrice() *float64 {
	if !p.CacheCreation1hPriceExplicit && p.CacheCreation1hPricePerToken == 0 {
		return nil
	}
	v := p.CacheCreation1hPricePerToken
	return &v
}

// parsePriceDocument accepts the repository's LiteLLM-compatible token table.
// It deliberately ignores image-only/per-request entries and service tiers,
// rather than inventing a token price or mixing priority/flex with standard.
func parsePriceDocument(body []byte) ([]storage.ModelPriceOverride, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("价格源 JSON 无效: %w", err)
	}
	items := make([]storage.ModelPriceOverride, 0, len(raw))
	for name, data := range raw {
		if name == "sample_spec" {
			continue
		}
		var e defaultPriceEntry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, fmt.Errorf("模型 %s 价格字段无效: %w", name, err)
		}
		if e.InputCostPerToken == nil && e.OutputCostPerToken == nil {
			continue
		}
		item := storage.ModelPriceOverride{ModelName: strings.ToLower(strings.TrimSpace(name)), Enabled: true, Source: "sub2api", SourceURL: Sub2APIPriceSourceURL}
		if e.InputCostPerToken != nil {
			item.InputPricePerToken = *e.InputCostPerToken
		}
		if e.OutputCostPerToken != nil {
			item.OutputPricePerToken = *e.OutputCostPerToken
		}
		if e.CacheCreationInputTokenCost != nil {
			item.CacheCreationPricePerToken = *e.CacheCreationInputTokenCost
		}
		if e.CacheCreation1hInputTokenCost != nil {
			item.CacheCreation1hPricePerToken = e.CacheCreation1hInputTokenCost
		}
		if e.CacheReadInputTokenCost != nil {
			item.CacheReadPricePerToken = *e.CacheReadInputTokenCost
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		tiers := map[int]*storage.ModelPriceContextTier{}
		var provider string
		_ = json.Unmarshal(fields["litellm_provider"], &provider)
		for field, value := range fields {
			if strings.TrimSpace(string(value)) == "null" {
				continue
			}
			match := contextPriceField.FindStringSubmatch(field)
			if len(match) == 0 {
				continue
			}
			k, err := strconv.Atoi(match[2])
			if err != nil || k <= 0 || k > 1000000 {
				return nil, fmt.Errorf("模型 %s 上下文阈值无效", name)
			}
			var price float64
			if err := json.Unmarshal(value, &price); err != nil {
				return nil, fmt.Errorf("模型 %s 档位价格无效", name)
			}
			threshold := k * 1000
			tier := tiers[threshold]
			if tier == nil {
				tier = &storage.ModelPriceContextTier{AboveInputTokens: threshold, Inclusive: strings.EqualFold(provider, "xai")}
				tiers[threshold] = tier
			}
			switch match[1] {
			case "input_cost_per_token":
				tier.InputPricePerToken = &price
			case "output_cost_per_token":
				tier.OutputPricePerToken = &price
			case "cache_creation_input_token_cost":
				tier.CacheCreationPricePerToken = &price
			case "cache_creation_input_token_cost_above_1hr":
				tier.CacheCreation1hPricePerToken = &price
			case "cache_read_input_token_cost":
				tier.CacheReadPricePerToken = &price
			}
		}
		var threshold int
		var inputMultiplier, outputMultiplier float64
		_ = json.Unmarshal(fields["long_context_input_token_threshold"], &threshold)
		_ = json.Unmarshal(fields["long_context_input_cost_multiplier"], &inputMultiplier)
		_ = json.Unmarshal(fields["long_context_output_cost_multiplier"], &outputMultiplier)
		if threshold > 0 && (inputMultiplier > 0 || outputMultiplier > 0) {
			tier := tiers[threshold]
			if tier == nil {
				tier = &storage.ModelPriceContextTier{AboveInputTokens: threshold, Inclusive: strings.EqualFold(provider, "xai")}
				tiers[threshold] = tier
			}
			if inputMultiplier > 0 {
				in, read, write := item.InputPricePerToken*inputMultiplier, item.CacheReadPricePerToken*inputMultiplier, item.CacheCreationPricePerToken*inputMultiplier
				if tier.InputPricePerToken == nil {
					tier.InputPricePerToken = &in
				}
				if tier.CacheReadPricePerToken == nil {
					tier.CacheReadPricePerToken = &read
				}
				if tier.CacheCreationPricePerToken == nil {
					tier.CacheCreationPricePerToken = &write
				}
				if item.CacheCreation1hPricePerToken != nil && tier.CacheCreation1hPricePerToken == nil {
					oneHour := *item.CacheCreation1hPricePerToken * inputMultiplier
					tier.CacheCreation1hPricePerToken = &oneHour
				}
			}
			if outputMultiplier > 0 && tier.OutputPricePerToken == nil {
				price := item.OutputPricePerToken * outputMultiplier
				tier.OutputPricePerToken = &price
			}
		}
		list := make([]storage.ModelPriceContextTier, 0, len(tiers))
		for _, t := range tiers {
			// When a source specifies long-context input prices but omits cache
			// variants, inherit the same input multiplier, as sub2api does.
			if t.InputPricePerToken != nil && item.InputPricePerToken > 0 {
				ratio := *t.InputPricePerToken / item.InputPricePerToken
				if t.CacheReadPricePerToken == nil {
					v := item.CacheReadPricePerToken * ratio
					t.CacheReadPricePerToken = &v
				}
				if t.CacheCreationPricePerToken == nil {
					v := item.CacheCreationPricePerToken * ratio
					t.CacheCreationPricePerToken = &v
				}
				if t.CacheCreation1hPricePerToken == nil && item.CacheCreation1hPricePerToken != nil {
					v := *item.CacheCreation1hPricePerToken * ratio
					t.CacheCreation1hPricePerToken = &v
				}
			}
			list = append(list, *t)
		}
		item.ContextTiersJSON = contextTiersJSON(list)
		if err := storage.ValidateModelPrice(&item); err != nil {
			return nil, fmt.Errorf("模型 %s: %w", name, err)
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("价格源未包含可用 token 价格，未修改现有配置")
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ModelName < items[j].ModelName })
	return items, nil
}

func fetchSub2APIPrices(ctx context.Context, client *http.Client) ([]storage.ModelPriceOverride, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, Sub2APIPriceSourceURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "upstream-ops-price-sync")
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("同步价格失败: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("价格源返回 HTTP %d，未修改现有配置", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, priceDocumentLimit+1))
	if err != nil {
		return nil, err
	}
	if len(body) > priceDocumentLimit {
		return nil, fmt.Errorf("价格源超过 8 MiB 安全上限")
	}
	return parsePriceDocument(body)
}

type PriceSyncResult struct {
	Updated   int                             `json:"updated"`
	SourceURL string                          `json:"source_url"`
	Repricing *storage.ModelPriceRepricingJob `json:"repricing"`
}

func (s *Service) SyncModelPrices(ctx context.Context) (*PriceSyncResult, error) {
	if s.Prices == nil {
		return nil, fmt.Errorf("price storage unavailable")
	}
	client := &http.Client{Timeout: 30 * time.Second, CheckRedirect: func(req *http.Request, via []*http.Request) error { return fmt.Errorf("价格源不允许重定向") }}
	items, err := fetchSub2APIPrices(ctx, client)
	if err != nil {
		return nil, err
	}
	changed, job, err := s.Prices.ApplyWithRepricing(ctx, items, 0, true)
	if err != nil {
		return nil, err
	}
	s.WakePriceRepricing()
	return &PriceSyncResult{Updated: changed, SourceURL: Sub2APIPriceSourceURL, Repricing: job}, nil
}

// StartPriceRepricing resumes an unfinished cursor after process restart. A
// single background worker avoids tying large historical scans to HTTP timeouts.
func (s *Service) StartPriceRepricing(ctx context.Context) {
	s.priceWorkerOnce.Do(func() {
		s.priceWake = make(chan struct{}, 1)
		go func() {
			ticker := time.NewTicker(30 * time.Second)
			defer ticker.Stop()
			for {
				s.runPriceRepricing(ctx)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				case <-s.priceWake:
				}
			}
		}()
	})
}

func (s *Service) WakePriceRepricing() {
	if s.priceWake == nil {
		return
	}
	select {
	case s.priceWake <- struct{}{}:
	default:
	}
}

func (s *Service) runPriceRepricing(ctx context.Context) {
	if s.Prices == nil || s.Usage == nil || s.Pricing == nil {
		return
	}
	status, statusErr := s.Prices.RepricingStatus(ctx)
	if statusErr != nil || (status.Status != "pending" && status.Status != "running") {
		return
	}
	for ctx.Err() == nil {
		batchCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
		done, err := s.Prices.RepriceBatch(batchCtx, func(items []storage.ModelPriceOverride) func(storage.GatewayUsageLog) map[string]any {
			overrides := make(map[string]ModelPricing, len(items))
			for _, item := range items {
				if item.Enabled {
					overrides[strings.ToLower(item.ModelName)] = pricingFromOverride(item)
				}
			}
			// Resolve embedded/fallback prices without querying the database while the
			// repricing transaction owns a connection.
			base := &PricingCatalog{defaults: s.Pricing.defaults, fallbacks: s.Pricing.fallbacks, snapshot: overrides}
			return func(row storage.GatewayUsageLog) map[string]any {
				if row.BillingMode != "" && row.BillingMode != "token" && row.BillingMode != "image" {
					return nil
				}
				model := strings.TrimSpace(row.UpstreamModel)
				if model == "" {
					model = row.RequestedModel
				}
				price, ok := base.resolveKnown(model)
				if !ok {
					return nil
				}
				tokens := UsageTokens{InputTokens: row.InputTokens, OutputTokens: row.OutputTokens, CacheReadTokens: row.CacheReadTokens, CacheCreationTokens: row.CacheCreationTokens, CacheCreation5mTokens: row.CacheCreation5mTokens, CacheCreation1hTokens: row.CacheCreation1hTokens, ImageOutputTokens: row.ImageOutputTokens}
				cost := CalculateCost(price, tokens, row.RateMultiplier, row.BillingRateMultiplier)
				extra := cost.ActualCost
				if row.Winner {
					extra = 0
				}
				return map[string]any{"input_cost": cost.InputCost, "output_cost": cost.OutputCost, "cache_creation_cost": cost.CacheCreationCost, "cache_read_cost": cost.CacheReadCost, "image_output_cost": cost.ImageOutputCost, "total_cost": cost.TotalCost, "actual_cost": cost.ActualCost, "estimated_cost": cost.ActualCost, "estimated_extra_cost": extra, "account_stats_cost": cost.TotalCost}
			}
		})
		cancel()
		if err != nil {
			if s.Log != nil {
				s.Log.Warn("historical price recalculation paused; retrying", "err", err)
			}
			if ctx.Err() == nil {
				retryCtx, retryCancel := context.WithTimeout(ctx, 3*time.Second)
				s.Prices.MarkRepricingRetry(retryCtx)
				retryCancel()
			}
			return
		}
		s.Usage.InvalidatePriceStats()
		if done {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
