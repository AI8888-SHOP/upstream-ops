package gateway

import (
	"context"
	"io"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestEmbeddedPriceDocumentLoadsContextAndHourlyRates(t *testing.T) {
	items, err := parsePriceDocument(defaultPricesJSON)
	if err != nil || len(items) < 200 {
		t.Fatalf("embedded models=%d err=%v", len(items), err)
	}
	tiered, hourly := 0, 0
	for _, item := range items {
		if item.ContextTiersJSON != "[]" {
			tiered++
		}
		if item.CacheCreation1hPricePerToken != nil {
			hourly++
		}
	}
	if tiered == 0 || hourly == 0 {
		t.Fatalf("embedded context=%d hourly=%d", tiered, hourly)
	}
	catalog := NewPricingCatalog(nil)
	if len(catalog.defaults) != len(items) {
		t.Fatalf("embedded defaults silently dropped: %d/%d", len(catalog.defaults), len(items))
	}
}

func TestPriceDocumentContextTiersAndCacheTTL(t *testing.T) {
	body := []byte(`{"model-a":{"input_cost_per_token":0.000002,"output_cost_per_token":0.000006,"cache_creation_input_token_cost":0.0000025,"cache_creation_input_token_cost_above_1hr":0.000004,"cache_read_input_token_cost":0.0000002,"input_cost_per_token_above_200k_tokens":0.000004,"output_cost_per_token_above_200k_tokens":0.000009,"cache_creation_input_token_cost_above_1hr_above_200k_tokens":0.000009,"input_cost_per_token_above_1000k_tokens":0.000006},"image-only":{"output_cost_per_image":0.1}}`)
	items, err := parsePriceDocument(body)
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	p := pricingFromOverride(items[0])
	if p.InputPricePerToken != 2e-6 || len(p.ContextTiers) != 2 {
		t.Fatalf("per-token or tiers changed: %+v", p)
	}
	if got := p.forTokens(UsageTokens{InputTokens: 199900, CacheReadTokens: 100}).InputPricePerToken; got != 2e-6 {
		t.Fatalf("threshold boundary=%g", got)
	}
	if got := p.forTokens(UsageTokens{InputTokens: 199900, CacheReadTokens: 100, CacheCreation1hTokens: 1}).InputPricePerToken; got != 4e-6 {
		t.Fatalf("TTL-only input not counted: %g", got)
	}
	if got := p.forTokens(UsageTokens{InputTokens: 1000001}).InputPricePerToken; got != 6e-6 {
		t.Fatalf("highest tier=%g", got)
	}
	cost := CalculateCost(p, UsageTokens{InputTokens: 200001, OutputTokens: 100, CacheCreationTokens: 100, CacheCreation5mTokens: 30, CacheCreation1hTokens: 20, CacheReadTokens: 50}, 0.08, 0.08)
	wantCache := 80*5e-6 + 20*9e-6
	if math.Abs(cost.CacheCreationCost-wantCache) > 1e-12 {
		t.Fatalf("cache cost=%g want %g", cost.CacheCreationCost, wantCache)
	}
	if math.Abs(cost.CacheReadCost-50*4e-7) > 1e-12 || math.Abs(cost.ActualCost-cost.TotalCost*0.08) > 1e-12 {
		t.Fatalf("cost=%+v", cost)
	}
}

func TestPriceDocumentExplicitLongContextAndFreeHourly(t *testing.T) {
	items, err := parsePriceDocument([]byte(`{"x":{"litellm_provider":"xai","input_cost_per_token":0.000002,"output_cost_per_token":0.000004,"cache_creation_input_token_cost":0.0000025,"cache_creation_input_token_cost_above_1hr":0,"cache_read_input_token_cost":0.0000002,"long_context_input_token_threshold":272000,"long_context_input_cost_multiplier":2,"long_context_output_cost_multiplier":1.5}}`))
	if err != nil {
		t.Fatal(err)
	}
	p := pricingFromOverride(items[0])
	cost := CalculateCost(p, UsageTokens{InputTokens: 271990, CacheCreation1hTokens: 10, OutputTokens: 10}, 1, 1)
	if math.Abs(cost.InputCost-271990*4e-6) > 1e-12 || cost.CacheCreationCost != 0 || math.Abs(cost.OutputCost-10*6e-6) > 1e-12 {
		t.Fatalf("cost=%+v", cost)
	}
	missing := pricingFromOverride(storage.ModelPriceOverride{CacheCreationPricePerToken: 2e-6})
	if got := CalculateCost(missing, UsageTokens{CacheCreationTokens: 10, CacheCreation1hTokens: 10}, 1, 1).CacheCreationCost; math.Abs(got-10*2e-6) > 1e-12 {
		t.Fatalf("missing 1h fallback=%g", got)
	}
}

type priceRoundTripper func(*http.Request) (*http.Response, error)

func (f priceRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestFetchPriceSourceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		wantError  bool
	}{
		{"ok", `{"x":{"input_cost_per_token":0.000002}}`, 200, false},
		{"unavailable", `{}`, 503, true},
		{"empty", `{}`, 200, true},
		{"negative", `{"x":{"input_cost_per_token":-1}}`, 200, true},
		{"malformed", `not json`, 200, true},
		{"oversized", strings.Repeat(" ", priceDocumentLimit+1), 200, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Transport: priceRoundTripper(func(req *http.Request) (*http.Response, error) {
				if req.URL.String() != Sub2APIPriceSourceURL || req.Method != http.MethodGet {
					t.Fatalf("unexpected source: %s %s", req.Method, req.URL)
				}
				return &http.Response{StatusCode: tc.status, Body: io.NopCloser(strings.NewReader(tc.body)), Header: make(http.Header)}, nil
			})}
			_, err := fetchSub2APIPrices(context.Background(), client)
			if (err != nil) != tc.wantError {
				t.Fatalf("err=%v wantError=%v", err, tc.wantError)
			}
		})
	}
}

func TestSyncedPriceAliasesAndDisabledFallback(t *testing.T) {
	db := openGatewayTestDB(t)
	repo := storage.NewModelPriceOverrides(db)
	item := storage.ModelPriceOverride{ModelName: "claude-sonnet-4-5", InputPricePerToken: 0.123, Enabled: true}
	if _, _, err := repo.ApplyWithRepricing(context.Background(), []storage.ModelPriceOverride{item}, 0, true); err != nil {
		t.Fatal(err)
	}
	live := NewPricingCatalog(repo)
	snapshot := NewPricingCatalog(nil)
	snapshot.snapshot = map[string]ModelPricing{item.ModelName: pricingFromOverride(item)}
	for _, model := range []string{"claude-sonnet-4-5", "anthropic/claude-sonnet-4-5", "claude-sonnet-4-5-20261021"} {
		for _, cat := range []*PricingCatalog{live, snapshot} {
			if got := cat.Resolve(model).InputPricePerToken; got != 0.123 {
				t.Fatalf("%s price=%g", model, got)
			}
		}
	}
	item.Enabled = false
	if _, _, err := repo.ApplyWithRepricing(context.Background(), []storage.ModelPriceOverride{item}, 0, false); err != nil {
		t.Fatal(err)
	}
	if got := live.Resolve("anthropic/claude-sonnet-4-5"); !got.HasTokenPrice() || got.InputPricePerToken == 0.123 {
		t.Fatalf("disabled fallback=%+v", got)
	}
}

func TestExplicitHourlyTierWinsOverMultiplierAndNullInherits(t *testing.T) {
	items, err := parsePriceDocument([]byte(`{"x":{"input_cost_per_token":0.000002,"cache_creation_input_token_cost":0.0000025,"cache_creation_input_token_cost_above_1hr":0.000004,"cache_creation_input_token_cost_above_1hr_above_272k_tokens":0.000011,"input_cost_per_token_above_272k_tokens":null,"long_context_input_token_threshold":272000,"long_context_input_cost_multiplier":2}}`))
	if err != nil {
		t.Fatal(err)
	}
	price := pricingFromOverride(items[0]).forTokens(UsageTokens{InputTokens: 272001})
	if price.InputPricePerToken != 4e-6 || price.CacheCreation1hPricePerToken != 11e-6 {
		t.Fatalf("explicit/null prices=%+v", price)
	}
}

func TestSyncedFamilyFallbackAndCombinedAliases(t *testing.T) {
	catalog := NewPricingCatalog(nil)
	catalog.snapshot = map[string]ModelPricing{
		"grok-4.5":          {InputPricePerToken: 0.123},
		"grok-4.3":          {InputPricePerToken: 0.234},
		"grok-build-0.1":    {InputPricePerToken: 0.345},
		"claude-sonnet-4.5": {InputPricePerToken: 0.456},
		"deepseek-chat":     {}, // Explicit free family overrides must stay known.
	}
	for _, tc := range []struct {
		model string
		price float64
	}{
		{"xai/grok-latest", 0.123}, {"grok-4.5-preview", 0.123},
		{"grok-4.20-reasoning", 0.234}, {"grok-composer", 0.345},
		{"anthropic/claude-sonnet-4-5-20250929", 0.456}, {"deepseek-custom", 0},
	} {
		price, known := catalog.resolveKnown(tc.model)
		if !known || price.InputPricePerToken != tc.price {
			t.Errorf("%s known=%v price=%g want=%g", tc.model, known, price.InputPricePerToken, tc.price)
		}
	}
}

func TestHistoricalRepricingChangesReferencesOnly(t *testing.T) {
	db := openGatewayTestDB(t)
	prices := storage.NewModelPriceOverrides(db)
	usage := storage.NewGatewayUsageLogs(db)
	key := storage.GatewayKey{Name: "price-test", QuotaUsed: 9}
	if err := db.Create(&key).Error; err != nil {
		t.Fatal(err)
	}
	row := storage.GatewayUsageLog{GatewayKeyID: key.ID, RequestID: "price-history", RequestedModel: "alias", UpstreamModel: "price-model", AttemptStatus: "accepted", Winner: true, Success: true, InputTokens: 1000000, CacheCreationTokens: 100, CacheCreation1hTokens: 100, RateMultiplier: 0.1, BillingRateMultiplier: 0.08, ActualCost: 1, BilledCost: 0.7, AccountStatsCost: 1, VirtualCacheReadTokens: 5, VirtualCacheReadCost: 0.2}
	if err := usage.Create(&row); err != nil {
		t.Fatal(err)
	}
	final := storage.GatewayRequestFinalization{RequestID: row.RequestID, GatewayKeyID: key.ID, ActualCost: 1, BilledCost: 0.7}
	if err := db.Create(&final).Error; err != nil {
		t.Fatal(err)
	}
	settlement := storage.GatewayWinnerSettlement{RequestID: row.RequestID, GatewayKeyID: key.ID, ActualCost: 1, BilledCost: 0.7}
	if err := db.Create(&settlement).Error; err != nil {
		t.Fatal(err)
	}
	hourly := 4e-6
	if _, _, err := prices.ApplyWithRepricing(context.Background(), []storage.ModelPriceOverride{{ModelName: "price-model", InputPricePerToken: 2e-6, CacheCreationPricePerToken: 2.5e-6, CacheCreation1hPricePerToken: &hourly, Enabled: true}}, 0, false); err != nil {
		t.Fatal(err)
	}
	service := NewService(nil, nil, nil, usage, prices, nil, nil, nil, nil)
	service.runPriceRepricing(context.Background())
	if err := db.First(&row, row.ID).Error; err != nil {
		t.Fatal(err)
	}
	if math.Abs(row.ActualCost-(2+0.0004)*0.08) > 1e-12 || row.AccountStatsCost != row.TotalCost {
		t.Fatalf("repriced=%+v", row)
	}
	if row.BilledCost != 0.7 || row.VirtualCacheReadTokens != 5 || row.VirtualCacheReadCost != 0.2 {
		t.Fatalf("settlement fields changed: %+v", row)
	}
	if row.ReferenceSalesBase == nil || math.Abs(*row.ReferenceSalesBase-(999995*2e-6+0.0004)) > 1e-12 || row.ReferencePricingVersion != storage.ReferencePricingVersion {
		t.Fatalf("historical revenue basis was not repriced: %+v", row)
	}
	if err := db.First(&key, key.ID).Error; err != nil || key.QuotaUsed != 9 {
		t.Fatalf("key=%+v err=%v", key, err)
	}
	if err := db.First(&final, "request_id = ?", row.RequestID).Error; err != nil || final.ActualCost != 1 || final.BilledCost != 0.7 {
		t.Fatalf("final=%+v err=%v", final, err)
	}
	if err := db.First(&settlement, "request_id = ?", row.RequestID).Error; err != nil || settlement.ActualCost != 1 || settlement.BilledCost != 0.7 {
		t.Fatalf("settlement=%+v err=%v", settlement, err)
	}
}
