package storage

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"
)

func TestPriceValidationAndCacheNormalization(t *testing.T) {
	for _, tc := range []struct{ total, five, hour, wantFive, wantHour int }{
		{100, 30, 20, 80, 20}, {100, 0, 0, 100, 0}, {100, 200, 200, 50, 50}, {0, 30, 20, 30, 20}, {-1, -2, -3, 0, 0},
	} {
		five, hour := NormalizePriceCacheCreation(tc.total, tc.five, tc.hour)
		if five != tc.wantFive || hour != tc.wantHour {
			t.Errorf("%+v got %d/%d", tc, five, hour)
		}
	}
	for _, tc := range []ModelPriceOverride{
		{ModelName: ""}, {ModelName: "x", InputPricePerToken: -1}, {ModelName: "x", InputPricePerToken: math.NaN()},
		{ModelName: "x", ContextTiersJSON: `[{"above_input_tokens":0}]`},
		{ModelName: "x", ContextTiersJSON: `[{"above_input_tokens":100},{"above_input_tokens":100}]`},
	} {
		if err := ValidateModelPrice(&tc); err == nil {
			t.Errorf("accepted invalid price %+v", tc)
		}
	}
	item := ModelPriceOverride{ModelName: " x ", ContextTiersJSON: `[{"above_input_tokens":200},{"above_input_tokens":100,"cache_read_price_per_token":0}]`}
	if err := ValidateModelPrice(&item); err != nil || item.ModelName != "x" {
		t.Fatalf("validated %+v err=%v", item, err)
	}
}

func TestPriceSyncPreservesManualOverridesAndSchedulesHistory(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	repo := NewModelPriceOverrides(db)
	if _, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "MixedCase", InputPricePerToken: 9, Enabled: false}}, 0, false); err != nil {
		t.Fatal(err)
	}
	changed, job, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "mixedcase", InputPricePerToken: 1, Enabled: true}, {ModelName: "synced", InputPricePerToken: 2, Enabled: true}}, 0, true)
	if err != nil || changed != 1 || job.Status != "pending" {
		t.Fatalf("changed=%d job=%+v err=%v", changed, job, err)
	}
	manual, err := repo.FindByModel("mixedcase")
	if err != nil || manual.Enabled || manual.InputPricePerToken != 9 || manual.Source != "manual" {
		t.Fatalf("manual=%+v err=%v", manual, err)
	}
	if _, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "synced", InputPricePerToken: 3, Enabled: true}}, 0, true); err != nil {
		t.Fatal(err)
	}
	synced, err := repo.FindByModel("synced")
	if err != nil || synced.InputPricePerToken != 3 {
		t.Fatalf("synced=%+v err=%v", synced, err)
	}
	// Editing a synced model makes it manual and protects the edit from the
	// next one-click synchronization.
	if _, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "synced", InputPricePerToken: 4, Enabled: true}}, 0, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "synced", InputPricePerToken: 5, Enabled: true}}, 0, true); err != nil {
		t.Fatal(err)
	}
	synced, err = repo.FindByModel("synced")
	if err != nil || synced.InputPricePerToken != 4 || synced.Source != "manual" {
		t.Fatalf("manual edit=%+v err=%v", synced, err)
	}
}

func TestPriceRepricingResumesHighWatermarkAndSkipsInflight(t *testing.T) {
	ctx := context.Background()
	db := openTestDB(t)
	repo := NewModelPriceOverrides(db)
	rows := make([]GatewayUsageLog, 203)
	for i := range rows {
		rows[i] = GatewayUsageLog{RequestID: fmt.Sprintf("price-%d", i), ActualCost: 1, BilledCost: 0.75, InputTokens: 10, CreatedAt: time.Now()}
	}
	rows[201].AttemptStatus = "accepted" // not finalized, never touch its pending debit
	rows[202].AttemptStatus = "error"
	if err := db.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&GatewayRequestFinalization{RequestID: rows[202].RequestID}).Error; err != nil {
		t.Fatal(err)
	}
	_, job, err := repo.ApplyWithRepricing(ctx, []ModelPriceOverride{{ModelName: "x", InputPricePerToken: 1, Enabled: true}}, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	upper := job.UpperID
	calculate := func(_ []ModelPriceOverride) func(GatewayUsageLog) map[string]any {
		return func(row GatewayUsageLog) map[string]any {
			return map[string]any{"actual_cost": 2.0, "billed_cost": 99.0, "input_tokens": 999}
		}
	}
	done, err := repo.RepriceBatch(ctx, calculate)
	if err != nil || done {
		t.Fatalf("first done=%v err=%v", done, err)
	}
	job, err = repo.RepricingStatus(ctx)
	if err != nil || job.UpdatedRows != 200 || job.LastID != rows[199].ID {
		t.Fatalf("cursor=%+v err=%v", job, err)
	}
	later := GatewayUsageLog{RequestID: "after-highwatermark", ActualCost: 1, CreatedAt: time.Now()}
	if err := db.Create(&later).Error; err != nil {
		t.Fatal(err)
	}
	restarted := NewModelPriceOverrides(db)
	done, err = restarted.RepriceBatch(ctx, calculate)
	if err != nil || !done {
		t.Fatalf("second done=%v err=%v", done, err)
	}
	job, err = restarted.RepricingStatus(ctx)
	if err != nil || job.UpperID != upper || job.UpdatedRows != 202 || job.SkippedRows != 1 || job.Status != "completed" {
		t.Fatalf("job=%+v err=%v", job, err)
	}
	for _, id := range []uint{rows[0].ID, rows[201].ID, rows[202].ID, later.ID} {
		var row GatewayUsageLog
		if err := db.First(&row, id).Error; err != nil {
			t.Fatal(err)
		}
		want := 2.0
		if id == rows[201].ID || id == later.ID {
			want = 1
		}
		if row.ActualCost != want || row.BilledCost == 99 || row.InputTokens == 999 {
			t.Fatalf("row=%+v want cost %g", row, want)
		}
	}
	_, restartedJob, err := repo.ApplyWithRepricing(ctx, nil, 0, false)
	if err != nil || restartedJob.LastID != 0 || restartedJob.UpperID != later.ID || restartedJob.Generation != job.Generation+1 {
		t.Fatalf("restart=%+v err=%v", restartedJob, err)
	}
}

func TestPriceRepricingInvalidatesStatsImmediately(t *testing.T) {
	repo := NewGatewayUsageLogs(openTestDB(t))
	key := gatewayStatsQueryKey{}
	repo.statsCache[key] = gatewayStatsCacheEntry{at: time.Now(), value: &GatewayUsageStats{TotalActualCost: 99}}
	repo.InvalidatePriceStats()
	if len(repo.statsCache) != 0 || repo.statsEpoch != 1 {
		t.Fatal("stale aggregate survived price update")
	}
}
