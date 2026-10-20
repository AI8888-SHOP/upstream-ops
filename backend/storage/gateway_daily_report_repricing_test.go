package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Shared by the SQLite fixture and PostgreSQL integration suite.
func runGatewayDailyReportRepricedRevenueRegression(t *testing.T, db *gorm.DB) {
	t.Helper()
	t.Run("repriced history overrides zero and positive settlements at every level", func(t *testing.T) {
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		t.Cleanup(func() { tx.Rollback() })
		date := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
		group := GatewayGroup{Name: "repriced-history", SellRateMultiplier: reportProfitPointer(.4)}
		if err := tx.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
		rows := []GatewayUsageLog{
			{RequestID: "newly-priced", ActualCost: 2, ReferenceSalesBase: reportProfitPointer(10), Winner: true, InputTokens: 100, CreatedAt: date},
			{RequestID: "changed-price", ActualCost: 3, ReferenceSalesBase: reportProfitPointer(8), Winner: true, InputTokens: 200, CreatedAt: date},
			{RequestID: "changed-price", Attempt: 2, ActualCost: 1, ReferenceSalesBase: reportProfitPointer(5), CreatedAt: date},
			{RequestID: "yesterday-newly-priced", ActualCost: 4, ReferenceSalesBase: reportProfitPointer(20), Winner: true, InputTokens: 100, CreatedAt: date.AddDate(0, 0, -1)},
		}
		for i := range rows {
			row := &rows[i]
			row.GatewayGroupID, row.GatewayKeyID, row.ChannelID = group.ID, 809, 4401
			row.SourceGroupName, row.RequestedModel = "source", "mapped-model"
			row.ReferencePricingVersion, row.AttemptStatus, row.Success = ReferencePricingVersion, GatewayAttemptStatusAccepted, row.Winner
			if err := tx.Create(row).Error; err != nil {
				t.Fatal(err)
			}
			if row.Winner {
				final := GatewayRequestFinalization{RequestID: row.RequestID, GatewayKeyID: row.GatewayKeyID, WinnerUsageLogID: row.ID, Delivered: true}
				if i == 1 {
					final.BilledCost, final.ActualCost = 100, 100
				}
				if err := tx.Create(&final).Error; err != nil {
					t.Fatal(err)
				}
			}
		}
		repo := NewGatewayUsageLogs(tx.Session(&gorm.Session{DisableNestedTransaction: true}))
		read := func() *GatewayDailyReport {
			t.Helper()
			r, err := repo.DailyReport(context.Background(), date)
			if err != nil {
				t.Fatal(err)
			}
			return r
		}
		check := func(m GatewayDailyReportMetrics, revenue, cost float64, unpriced int64) {
			t.Helper()
			if m.UnpricedRequests != unpriced || math.Abs(m.Cost-cost) > 1e-9 {
				t.Fatalf("metrics=%+v", m)
			}
			if unpriced > 0 {
				if m.Revenue != nil || m.Profit != nil {
					t.Fatal("stale snapshot masked missing price")
				}
				return
			}
			if m.Revenue == nil || math.Abs(*m.Revenue-revenue) > 1e-9 || m.Profit == nil || math.Abs(*m.Profit-(revenue-cost)) > 1e-9 {
				t.Fatalf("metrics=%+v want revenue=%g cost=%g", m, revenue, cost)
			}
		}
		r := read()
		for _, tc := range []struct {
			day           GatewayDailyReportDay
			revenue, cost float64
		}{{r.Today, 7.2, 6}, {r.Yesterday, 8, 4}} {
			s := tc.day.Sources[0]
			for _, m := range []GatewayDailyReportMetrics{tc.day.GatewayDailyReportMetrics, s.GatewayDailyReportMetrics, s.TopCostModel.GatewayDailyReportMetrics, s.Groups[0].GatewayDailyReportMetrics, s.Groups[0].Models[0].GatewayDailyReportMetrics} {
				check(m, tc.revenue, tc.cost, 0)
			}
		}
		// A second recalculation replaces the first basis, including yesterday.
		if err := tx.Model(&rows[0]).Update("reference_sales_base", 20).Error; err != nil {
			t.Fatal(err)
		}
		check(read().Today.GatewayDailyReportMetrics, 11.2, 6, 0)
		// Explicitly free is numeric zero even if old snapshot charged money.
		if err := tx.Model(&rows[1]).Update("reference_sales_base", 0).Error; err != nil {
			t.Fatal(err)
		}
		check(read().Today.GatewayDailyReportMetrics, 8, 6, 0)
		// Removing the only price clears the reference basis, not the debit.
		if err := tx.Model(&rows[1]).Update("reference_sales_base", nil).Error; err != nil {
			t.Fatal(err)
		}
		check(read().Today.GatewayDailyReportMetrics, 0, 6, 1)
		var final GatewayRequestFinalization
		if err := tx.First(&final, "request_id = ?", rows[1].RequestID).Error; err != nil || final.BilledCost != 100 {
			t.Fatalf("settlement changed: %+v %v", final, err)
		}
	})
}
