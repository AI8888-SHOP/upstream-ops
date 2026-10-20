package gateway

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestHistoricalReferencePrices(t *testing.T) {
	ptr := func(v float64) *float64 { return &v }
	catalog := &PricingCatalog{snapshot: map[string]ModelPricing{
		"mapped":   {InputPricePerToken: 2, OutputPricePerToken: 4, CacheReadPricePerToken: .2, CacheCreationPricePerToken: 3, CacheCreation1hPricePerToken: 5, CacheCreation1hPriceExplicit: true, ContextTiers: []storage.ModelPriceContextTier{{AboveInputTokens: 100, InputPricePerToken: ptr(4), OutputPricePerToken: ptr(8), CacheReadPricePerToken: ptr(.4), CacheCreationPricePerToken: ptr(6), CacheCreation1hPricePerToken: ptr(10)}}},
		"free":     {},
		"inverted": {InputPricePerToken: 2, CacheReadPricePerToken: 5},
	}}
	for _, tc := range []struct {
		name             string
		row              storage.GatewayUsageLog
		base, cost       float64
		unknown, skipped bool
	}{
		{name: "mapped model and original virtual tokens", row: storage.GatewayUsageLog{RequestedModel: "unpriced-alias", UpstreamModel: "mapped", InputTokens: 10, OutputTokens: 2, VirtualCacheReadTokens: 5, RateMultiplier: .5, BillingRateMultiplier: .2}, base: 19, cost: 5.6},
		{name: "same context tier before and after virtualization with TTL", row: storage.GatewayUsageLog{RequestedModel: "mapped", InputTokens: 90, OutputTokens: 2, CacheReadTokens: 5, CacheCreationTokens: 10, CacheCreation5mTokens: 4, CacheCreation1hTokens: 6, VirtualCacheReadTokens: 80, BillingRateMultiplier: .1}, base: 174, cost: 46.2},
		{name: "real cached only", row: storage.GatewayUsageLog{RequestedModel: "mapped", CacheReadTokens: 10, RateMultiplier: .2}, base: 2, cost: .4},
		{name: "TTL detail without aggregate", row: storage.GatewayUsageLog{RequestedModel: "mapped", CacheCreation5mTokens: 2, CacheCreation1hTokens: 3}, base: 21, cost: 21},
		{name: "virtual tokens clamped to fresh input", row: storage.GatewayUsageLog{RequestedModel: "mapped", InputTokens: 10, VirtualCacheReadTokens: 100}, base: 2, cost: 20},
		{name: "inverted cache prices never increase revenue basis", row: storage.GatewayUsageLog{RequestedModel: "inverted", InputTokens: 10, VirtualCacheReadTokens: 10}, base: 20, cost: 20},
		{name: "known explicit free price", row: storage.GatewayUsageLog{RequestedModel: "free", InputTokens: 10}},
		{name: "zero token convention", row: storage.GatewayUsageLog{RequestedModel: "mapped"}},
		{name: "unknown model not free", row: storage.GatewayUsageLog{RequestedModel: "missing", InputTokens: 10}, unknown: true},
		{name: "unknown upstream must not use request model price", row: storage.GatewayUsageLog{RequestedModel: "mapped", UpstreamModel: "missing", InputTokens: 10}, unknown: true},
		{name: "reasoning only cannot invent output usage", row: storage.GatewayUsageLog{RequestedModel: "mapped", ReasoningTokens: 10}, unknown: true},
		{name: "known cost must survive missing usage", row: storage.GatewayUsageLog{RequestedModel: "mapped", ActualCost: 2, TotalCost: 4}, skipped: true},
		{name: "known debit must survive missing usage", row: storage.GatewayUsageLog{RequestedModel: "mapped", BilledCost: 2}, skipped: true},
		{name: "missing price must not hide a charge without usage", row: storage.GatewayUsageLog{RequestedModel: "missing", BilledCost: 2}, skipped: true},
		{name: "fixed request mode", row: storage.GatewayUsageLog{RequestedModel: "mapped", BillingMode: "request"}, skipped: true},
		{name: "image without token pricing", row: storage.GatewayUsageLog{RequestedModel: "mapped", BillingMode: "image"}, skipped: true},
		{name: "image tokens use current output price", row: storage.GatewayUsageLog{RequestedModel: "mapped", BillingMode: "image", ImageOutputTokens: 3}, base: 12, cost: 12},
		{name: "negative bucket is not zero usage", row: storage.GatewayUsageLog{RequestedModel: "mapped", InputTokens: -10, OutputTokens: 10}, unknown: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := historicalReferencePrices(catalog, tc.row)
			if tc.skipped {
				if got != nil {
					t.Fatalf("expected untouched costs, got %+v", got)
				}
				return
			}
			if got["reference_pricing_version"] != storage.ReferencePricingVersion {
				t.Fatalf("missing version: %+v", got)
			}
			if tc.unknown {
				if got["reference_sales_base"] != nil || len(got) != 2 {
					t.Fatalf("unknown price overwrote cost: %+v", got)
				}
				return
			}
			if math.Abs(got["reference_sales_base"].(float64)-tc.base) > 1e-9 || math.Abs(got["actual_cost"].(float64)-tc.cost) > 1e-9 {
				t.Fatalf("got=%+v want base=%g cost=%g", got, tc.base, tc.cost)
			}
			if got["estimated_extra_cost"] != got["actual_cost"] {
				t.Fatal("losing attempt cost lost")
			}
		})
	}
}

func TestHistoricalPricesRecomputeRevenueWithoutRebilling(t *testing.T) {
	db := openGatewayTestDB(t)
	ctx := context.Background()
	prices := storage.NewModelPriceOverrides(db)
	usage := storage.NewGatewayUsageLogs(db)
	service := NewService(nil, nil, nil, usage, prices, nil, nil, nil, nil)
	date := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	sell := .5
	group := storage.GatewayGroup{Name: "historical-profit", SellRateMultiplier: &sell}
	key := storage.GatewayKey{Name: "historical-price-key", QuotaUsed: 9}
	for _, item := range []any{&group, &key} {
		if err := db.Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}
	row := storage.GatewayUsageLog{GatewayGroupID: group.ID, GatewayKeyID: key.ID, ChannelID: 1, RequestID: "history-winner", RequestedModel: "public-alias", UpstreamModel: "history-private-model", InputTokens: 100, OutputTokens: 10, AttemptStatus: "accepted", Winner: true, Success: true, VirtualCacheReadTokens: 40, RateMultiplier: .5, BillingRateMultiplier: .2, CreatedAt: date}
	retry := storage.GatewayUsageLog{GatewayGroupID: group.ID, GatewayKeyID: key.ID, ChannelID: 1, RequestID: row.RequestID, Attempt: 2, RequestedModel: row.RequestedModel, UpstreamModel: row.UpstreamModel, InputTokens: 10, AttemptStatus: "error", BillingRateMultiplier: .2, CreatedAt: date}
	for _, item := range []any{&row, &retry} {
		if err := db.Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}
	final := storage.GatewayRequestFinalization{RequestID: row.RequestID, GatewayKeyID: key.ID, Delivered: true, WinnerUsageLogID: row.ID, VirtualCacheReadTokens: 40}
	settlement := storage.GatewayWinnerSettlement{RequestID: row.RequestID, GatewayKeyID: key.ID}
	for _, item := range []any{&final, &settlement} {
		if err := db.Create(item).Error; err != nil {
			t.Fatal(err)
		}
	}
	assertReport := func(wantRevenue, wantCost float64, unpriced int64) {
		t.Helper()
		report, err := usage.DailyReport(ctx, date)
		if err != nil {
			t.Fatal(err)
		}
		got := report.Today
		if math.Abs(got.Cost-wantCost) > 1e-9 || got.UnpricedRequests != unpriced {
			t.Fatalf("report=%+v", got)
		}
		if unpriced > 0 {
			if got.Revenue != nil || got.Profit != nil {
				t.Fatal("missing price used stale revenue")
			}
			return
		}
		if got.Revenue == nil || math.Abs(*got.Revenue-wantRevenue) > 1e-9 || got.Profit == nil || math.Abs(*got.Profit-(wantRevenue-wantCost)) > 1e-9 {
			t.Fatalf("report=%+v want revenue=%g cost=%g", got, wantRevenue, wantCost)
		}
	}
	assertReport(0, 0, 1)
	for _, scale := range []float64{1, 2} {
		price := storage.ModelPriceOverride{ModelName: row.UpstreamModel, InputPricePerToken: 2 * scale, OutputPricePerToken: 4 * scale, CacheReadPricePerToken: .2 * scale, Enabled: true}
		if _, _, err := prices.ApplyWithRepricing(ctx, []storage.ModelPriceOverride{price}, 0, false); err != nil {
			t.Fatal(err)
		}
		service.runPriceRepricing(ctx)
		// (60*2 + 40*.2 + 10*4) * sell; all attempts cost (240+20)*.2.
		assertReport(84*scale, 52*scale, 0)
		service.runPriceRepricing(ctx) // Already completed: no second debit/backfill.
		assertReport(84*scale, 52*scale, 0)
	}
	if err := db.Model(&group).Update("sell_rate_multiplier", .25).Error; err != nil {
		t.Fatal(err)
	}
	assertReport(84, 104, 0)
	price, err := prices.FindByModel(row.UpstreamModel)
	if err != nil || price == nil {
		t.Fatalf("price=%+v err=%v", price, err)
	}
	if _, _, err := prices.ApplyWithRepricing(ctx, nil, price.ID, false); err != nil {
		t.Fatal(err)
	}
	service.runPriceRepricing(ctx)
	assertReport(0, 104, 1)
	// An explicitly configured free model is known, not missing pricing.
	if _, _, err := prices.ApplyWithRepricing(ctx, []storage.ModelPriceOverride{{ModelName: row.UpstreamModel, Enabled: true}}, 0, false); err != nil {
		t.Fatal(err)
	}
	service.runPriceRepricing(ctx)
	assertReport(0, 0, 0)
	if err := db.First(&key, key.ID).Error; err != nil || key.QuotaUsed != 9 {
		t.Fatalf("key changed: %+v err=%v", key, err)
	}
	if err := db.First(&row, row.ID).Error; err != nil || row.InputTokens != 100 || row.VirtualCacheReadTokens != 40 || row.BilledCost != 0 {
		t.Fatalf("raw usage/charge changed: %+v err=%v", row, err)
	}
	if err := db.First(&final, "request_id = ?", row.RequestID).Error; err != nil || final.ActualCost != 0 || final.BilledCost != 0 || final.VirtualCacheReadTokens != 40 {
		t.Fatalf("snapshot changed: %+v err=%v", final, err)
	}
	if err := db.First(&settlement, "request_id = ?", row.RequestID).Error; err != nil || settlement.BilledCost != 0 {
		t.Fatalf("settlement changed: %+v err=%v", settlement, err)
	}
}
