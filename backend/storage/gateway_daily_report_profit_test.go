package storage

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestGatewayDailyReportProfit(t *testing.T) {
	runGatewayDailyReportProfitRegression(t, openTestDB(t))
}

func TestGatewayDailyReportProfitExtremeNumbersRemainJSONSafe(t *testing.T) {
	for _, revenue := range []float64{1e-308, math.Inf(1), math.NaN()} {
		metrics := (gatewayDailyReportRow{Revenue: revenue, Cost: 10, SuccessfulRequests: 1}).metrics()
		if metrics.ProfitMargin != nil {
			t.Fatalf("unsafe margin=%v for revenue=%g", metrics.ProfitMargin, revenue)
		}
		if _, err := json.Marshal(metrics); err != nil {
			t.Fatalf("metrics cannot serialize: %v", err)
		}
	}
}

// The PostgreSQL integration test runs the same cases so nullable arithmetic,
// snapshot joins and the five rollups cannot silently diverge by SQL dialect.
func runGatewayDailyReportProfitRegression(t *testing.T, db *gorm.DB) {
	t.Helper()
	runGatewayDailyReportZeroUsageRegression(t, db)
	runGatewayDailyReportRepricedRevenueRegression(t, db)
	date := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	newTx := func(t *testing.T) *gorm.DB {
		t.Helper()
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		t.Cleanup(func() { tx.Rollback() })
		return tx
	}
	create := func(t *testing.T, tx *gorm.DB, value any) {
		t.Helper()
		if err := tx.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	read := func(t *testing.T, tx *gorm.DB) *GatewayDailyReport {
		t.Helper()
		counter := &usageQueryCounter{Interface: tx.Logger}
		// The fixture already owns a rollback-only transaction. Count report
		// queries, not a test-only nested transaction SAVEPOINT.
		report, err := NewGatewayUsageLogs(tx.Session(&gorm.Session{Logger: counter, DisableNestedTransaction: true})).DailyReport(context.Background(), date)
		if err != nil {
			t.Fatal(err)
		}
		if counter.value() > 4 {
			t.Fatalf("profit report ran %d queries; want bounded enrichment", counter.value())
		}
		return report
	}
	assertMetrics := func(t *testing.T, m GatewayDailyReportMetrics, revenue, cost float64, unpriced int64) {
		t.Helper()
		if math.Abs(m.Cost-cost) > 1e-9 || m.UnpricedRequests != unpriced {
			t.Fatalf("cost/coverage: %+v, want cost=%g unpriced=%d", m, cost, unpriced)
		}
		if unpriced > 0 {
			if m.Revenue != nil || m.Profit != nil || m.ProfitMargin != nil {
				t.Fatalf("partial profit exposed as complete: %+v", m)
			}
			return
		}
		if m.Revenue == nil || m.Profit == nil || math.Abs(*m.Revenue-revenue) > 1e-9 || math.Abs(*m.Profit-(revenue-cost)) > 1e-9 {
			t.Fatalf("profit: %+v, want revenue=%g profit=%g", m, revenue, revenue-cost)
		}
		if revenue == 0 {
			if m.ProfitMargin != nil {
				t.Fatalf("zero-revenue margin=%v", m.ProfitMargin)
			}
		} else if m.ProfitMargin == nil || math.Abs(*m.ProfitMargin-(revenue-cost)/revenue*100) > 1e-8 {
			t.Fatalf("margin: %+v", m)
		}
	}
	t.Run("winner income all attempts mixed gateway groups and repricing", func(t *testing.T) {
		tx := newTx(t)
		rateA, rateB := .4, .5
		groupA := GatewayGroup{Name: "report-profit-a", SellRateMultiplier: &rateA}
		groupB := GatewayGroup{Name: "report-profit-b", SellRateMultiplier: &rateB}
		create(t, tx, &groupA)
		create(t, tx, &groupB)
		sourceGroup := int64(41)
		rows := []GatewayUsageLog{
			{GatewayGroupID: groupA.ID, ChannelID: 2001, RequestID: "profit-retry", ActualCost: .6},
			{GatewayGroupID: groupA.ID, ChannelID: 2002, RequestID: "profit-retry", Attempt: 2, Winner: true, Success: true, ActualCost: 4, TotalCost: 20, BilledCost: .5, BillingRateMultiplier: .2, InputTokens: 100, VirtualCacheReadTokens: 100},
			{GatewayGroupID: groupB.ID, ChannelID: 2002, RequestID: "profit-other-group", Winner: true, Success: true, ActualCost: 1, TotalCost: 10, BilledCost: .3, BillingRateMultiplier: .1},
			// A successful but non-delivered hedge must never produce sales.
			{GatewayGroupID: groupA.ID, GatewayProviderID: 2003, RequestID: "profit-retry", Attempt: 3, Success: true, ActualCost: .4, TotalCost: 2, BilledCost: 1, BillingRateMultiplier: .2},
		}
		for i := range rows {
			rows[i].GatewayKeyID, rows[i].SourceGroupID = 801, &sourceGroup
			rows[i].RequestedModel, rows[i].UpstreamModel = "requested-model", "settled-model"
			rows[i].AttemptStatus, rows[i].CreatedAt = GatewayAttemptStatusAccepted, date
			create(t, tx, &rows[i])
		}
		for _, index := range []int{1, 2} {
			row := rows[index]
			originalCost := row.ActualCost
			if index == 1 {
				originalCost = 2 // Reference costs have doubled since settlement.
			}
			create(t, tx, &GatewayRequestFinalization{RequestID: row.RequestID, GatewayKeyID: row.GatewayKeyID,
				Delivered: true, WinnerAttempt: row.Attempt, WinnerUsageLogID: row.ID,
				ActualCost: originalCost, BilledCost: row.BilledCost, VirtualCacheReadTokens: row.VirtualCacheReadTokens})
		}
		report := read(t, tx)
		assertMetrics(t, report.Today.GatewayDailyReportMetrics, 2.5, 6, 0)
		assertMetrics(t, report.Yesterday.GatewayDailyReportMetrics, 0, 0, 0)
		if report.Today.Attempts != 4 || report.Today.Requests != 2 || report.Today.SuccessfulRequests != 2 {
			t.Fatalf("snapshot join multiplied usage: %+v", report.Today)
		}
		for _, source := range report.Today.Sources {
			switch source.SourceID {
			case 2001:
				assertMetrics(t, source.GatewayDailyReportMetrics, 0, .6, 0)
			case 2002:
				assertMetrics(t, source.GatewayDailyReportMetrics, 2.5, 5, 0)
				assertMetrics(t, source.Groups[0].GatewayDailyReportMetrics, 2.5, 5, 0)
				assertMetrics(t, source.Groups[0].Models[0].GatewayDailyReportMetrics, 2.5, 5, 0)
				assertMetrics(t, source.TopCostModel.GatewayDailyReportMetrics, 2.5, 5, 0)
			case 2003:
				assertMetrics(t, source.GatewayDailyReportMetrics, 0, .4, 0)
			}
		}
		if err := tx.Model(&groupA).Update("sell_rate_multiplier", .8).Error; err != nil {
			t.Fatal(err)
		}
		assertMetrics(t, read(t, tx).Today.GatewayDailyReportMetrics, 3.5, 6, 0)
		if err := tx.Model(&rows[1]).Updates(map[string]any{"actual_cost": 8, "total_cost": 40}).Error; err != nil {
			t.Fatal(err)
		}
		assertMetrics(t, read(t, tx).Today.GatewayDailyReportMetrics, 3.5, 10, 0)
		var snapshot GatewayRequestFinalization
		if err := tx.Where("request_id = ?", rows[1].RequestID).First(&snapshot).Error; err != nil || snapshot.BilledCost != .5 || snapshot.ActualCost != 2 {
			t.Fatalf("report changed settlement: %+v, %v", snapshot, err)
		}
		if err := tx.Delete(&groupA).Error; err != nil {
			t.Fatal(err)
		}
		report = read(t, tx)
		assertMetrics(t, report.Today.GatewayDailyReportMetrics, 0, 10, 1)
		for _, source := range report.Today.Sources {
			if source.SourceID == 2002 {
				assertMetrics(t, source.GatewayDailyReportMetrics, 0, 9, 1)
				assertMetrics(t, source.Groups[0].GatewayDailyReportMetrics, 0, 9, 1)
				assertMetrics(t, source.Groups[0].Models[0].GatewayDailyReportMetrics, 0, 9, 1)
			}
			if source.SourceID == 2003 {
				assertMetrics(t, source.GatewayDailyReportMetrics, 0, .4, 0)
			}
		}
	})
	for _, tc := range []struct {
		name         string
		sell         *float64
		usage        GatewayUsageLog
		snapshot     *GatewayRequestFinalization
		wantRevenue  float64
		wantUnpriced int64
	}{
		{name: "unset is not free", usage: GatewayUsageLog{ActualCost: 2, TotalCost: 10}, wantUnpriced: 1},
		{name: "explicit free even unknown price", sell: reportProfitPointer(0), usage: GatewayUsageLog{ActualCost: 2}},
		{name: "legacy standard base is not divided twice", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 10, ActualCost: 2, BillingRateMultiplier: .2}, wantRevenue: 4},
		{name: "legacy positive billed is preferred", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 20, ActualCost: 4, BilledCost: .5, BillingRateMultiplier: .2}, wantRevenue: 1},
		{name: "legacy actual rate fallback", sell: reportProfitPointer(.4), usage: GatewayUsageLog{ActualCost: 2, RateMultiplier: .2}, wantRevenue: 4},
		{name: "legacy unit rate fallback", sell: reportProfitPointer(.4), usage: GatewayUsageLog{ActualCost: 2}, wantRevenue: .8},
		{name: "legacy virtual no basis", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 10, ActualCost: 2, VirtualCacheReadTokens: 10}, wantUnpriced: 1},
		{name: "modern no snapshot zero billed", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 10, ActualCost: 2, AttemptStatus: GatewayAttemptStatusAccepted}, wantUnpriced: 1},
		{name: "unknown zero model is not free", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 10, ActualCost: 2, InputTokens: 100}, snapshot: &GatewayRequestFinalization{}, wantUnpriced: 1},
		{name: "confirmed virtual free snapshot", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 10, ActualCost: 2}, snapshot: &GatewayRequestFinalization{ActualCost: 2, VirtualCacheReadTokens: 100}},
		{name: "snapshot rate fallback", sell: reportProfitPointer(.4), usage: GatewayUsageLog{TotalCost: 20, ActualCost: 4, RateMultiplier: .2}, snapshot: &GatewayRequestFinalization{ActualCost: 2, BilledCost: 1}, wantRevenue: 2},
		{name: "snapshot explicit bill rate wins", sell: reportProfitPointer(.4), usage: GatewayUsageLog{ActualCost: 4, RateMultiplier: .5, BillingRateMultiplier: .2}, snapshot: &GatewayRequestFinalization{ActualCost: 2, BilledCost: 1}, wantRevenue: 2},
		{name: "unknown per-request mode", sell: reportProfitPointer(.4), usage: GatewayUsageLog{ActualCost: 2, BillingMode: "request"}, snapshot: &GatewayRequestFinalization{ActualCost: 2, BilledCost: 2}, wantUnpriced: 1},
		{name: "image token pricing", sell: reportProfitPointer(.4), usage: GatewayUsageLog{ActualCost: 2, BillingMode: "image", ImageOutputTokens: 10}, snapshot: &GatewayRequestFinalization{ActualCost: 2, BilledCost: 2}, wantRevenue: .8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := newTx(t)
			group := GatewayGroup{Name: "profit-case", SellRateMultiplier: tc.sell}
			create(t, tx, &group)
			usage := tc.usage
			usage.GatewayGroupID, usage.GatewayKeyID, usage.GatewayProviderID = group.ID, 802, 2004
			usage.RequestID, usage.RequestedModel = "profit-case", "model"
			usage.CreatedAt, usage.Winner, usage.Success = date, true, true
			create(t, tx, &usage)
			// GORM default:1 otherwise replaces explicit zero fixture rates.
			if err := tx.Model(&usage).Updates(map[string]any{"rate_multiplier": tc.usage.RateMultiplier, "billing_rate_multiplier": tc.usage.BillingRateMultiplier}).Error; err != nil {
				t.Fatal(err)
			}
			if tc.snapshot != nil {
				snapshot := *tc.snapshot
				snapshot.RequestID, snapshot.GatewayKeyID = usage.RequestID, usage.GatewayKeyID
				snapshot.WinnerUsageLogID, snapshot.Delivered = usage.ID, true
				create(t, tx, &snapshot)
			}
			report := read(t, tx)
			assertMetrics(t, report.Today.GatewayDailyReportMetrics, tc.wantRevenue, usage.ActualCost, tc.wantUnpriced)
			source := report.Today.Sources[0]
			assertMetrics(t, source.GatewayDailyReportMetrics, tc.wantRevenue, usage.ActualCost, tc.wantUnpriced)
			assertMetrics(t, source.Groups[0].GatewayDailyReportMetrics, tc.wantRevenue, usage.ActualCost, tc.wantUnpriced)
			assertMetrics(t, source.Groups[0].Models[0].GatewayDailyReportMetrics, tc.wantRevenue, usage.ActualCost, tc.wantUnpriced)
		})
	}
}

func reportProfitPointer(value float64) *float64 { return &value }
