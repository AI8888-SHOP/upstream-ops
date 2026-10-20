package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Called by the shared profit regression, including PostgreSQL integration CI.
func runGatewayDailyReportZeroUsageRegression(t *testing.T, db *gorm.DB) {
	t.Helper()
	for _, tc := range []struct {
		name                    string
		usage                   GatewayUsageLog
		snapshot                *GatewayRequestFinalization
		missingGroup, unsetRate bool
		retryCost               float64
		revenue                 float64
		unpriced, zeroUsage     int64
	}{
		{name: "zero snapshot is reported as zero", snapshot: &GatewayRequestFinalization{}, zeroUsage: 1},
		{name: "zero modern record without snapshot", usage: GatewayUsageLog{AttemptStatus: GatewayAttemptStatusAccepted}, zeroUsage: 1},
		{name: "zero legacy record without snapshot", zeroUsage: 1},
		{name: "zero usage does not require selling rate", unsetRate: true, zeroUsage: 1},
		{name: "zero usage with deleted gateway group", missingGroup: true, zeroUsage: 1},
		{name: "paid failed attempt remains a loss", snapshot: &GatewayRequestFinalization{}, retryCost: 2, zeroUsage: 1},
		{name: "input usage without price remains unknown", usage: GatewayUsageLog{InputTokens: 10}, snapshot: &GatewayRequestFinalization{}, unpriced: 1},
		{name: "output usage without price remains unknown", usage: GatewayUsageLog{OutputTokens: 10}, unpriced: 1},
		{name: "cache read is real usage", usage: GatewayUsageLog{CacheReadTokens: 10}, unpriced: 1},
		{name: "cache creation is real usage", usage: GatewayUsageLog{CacheCreationTokens: 10}, unpriced: 1},
		{name: "5m cache detail is real usage", usage: GatewayUsageLog{CacheCreation5mTokens: 10}, unpriced: 1},
		{name: "1h cache detail is real usage", usage: GatewayUsageLog{CacheCreation1hTokens: 10}, unpriced: 1},
		{name: "reasoning is real usage", usage: GatewayUsageLog{ReasoningTokens: 10}, unpriced: 1},
		{name: "image tokens are real usage", usage: GatewayUsageLog{ImageOutputTokens: 10}, unpriced: 1},
		{name: "opposite signed counts cannot cancel to zero", usage: GatewayUsageLog{InputTokens: 10, OutputTokens: -10}, unpriced: 1},
		{name: "per request pricing is not token free", usage: GatewayUsageLog{BillingMode: "request"}, unpriced: 1},
		{name: "image pricing is not token free", usage: GatewayUsageLog{BillingMode: "image"}, unpriced: 1},
		{name: "duration pricing is not token free", usage: GatewayUsageLog{BillingMode: "duration"}, unpriced: 1},
		{name: "known reference cost survives missing tokens", usage: GatewayUsageLog{ActualCost: 2, TotalCost: 4, BillingRateMultiplier: .5}, revenue: 2, zeroUsage: 1},
		{name: "known usage billed cost survives", usage: GatewayUsageLog{BilledCost: 2, BillingRateMultiplier: .5}, revenue: 2, zeroUsage: 1},
		{name: "known snapshot billed cost survives", snapshot: &GatewayRequestFinalization{ActualCost: 1, BilledCost: 2}, revenue: 1, zeroUsage: 1},
		{name: "nonzero snapshot cost without basis stays unknown", snapshot: &GatewayRequestFinalization{ActualCost: 2}, unpriced: 1, zeroUsage: 1},
		{name: "known virtual free snapshot still needs rate", unsetRate: true, snapshot: &GatewayRequestFinalization{ActualCost: 2, VirtualCacheReadTokens: 10}, unpriced: 1, zeroUsage: 1},
		{name: "modern known cost without basis stays unknown", usage: GatewayUsageLog{ActualCost: 2, AttemptStatus: GatewayAttemptStatusAccepted}, unpriced: 1, zeroUsage: 1},
	} {
		t.Run("zero usage/"+tc.name, func(t *testing.T) {
			tx := db.Begin()
			if tx.Error != nil {
				t.Fatal(tx.Error)
			}
			t.Cleanup(func() { tx.Rollback() })
			create := func(value any) {
				t.Helper()
				if err := tx.Create(value).Error; err != nil {
					t.Fatal(err)
				}
			}
			date := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
			group := GatewayGroup{Name: "zero-usage-profit", SellRateMultiplier: reportProfitPointer(.5)}
			if tc.unsetRate {
				group.SellRateMultiplier = nil
			}
			create(&group)
			row := tc.usage
			row.GatewayGroupID, row.GatewayKeyID, row.ChannelID = group.ID, 803, 4001
			row.SourceGroupName, row.RequestedModel, row.RequestID = "source", "model", "zero-usage-profit"
			row.CreatedAt, row.Winner, row.Success = date, true, true
			create(&row)
			if tc.missingGroup {
				if err := tx.Delete(&group).Error; err != nil {
					t.Fatal(err)
				}
			}
			var snapshot GatewayRequestFinalization
			if tc.snapshot != nil {
				snapshot = *tc.snapshot
				snapshot.RequestID, snapshot.GatewayKeyID = row.RequestID, row.GatewayKeyID
				snapshot.Delivered, snapshot.WinnerUsageLogID = true, row.ID
				create(&snapshot)
			}
			if tc.retryCost > 0 {
				// A paid failed attempt and a zero-usage losing hedge share the
				// request. Neither creates income or another anomaly count.
				for i, cost := range []float64{tc.retryCost, 0} {
					create(&GatewayUsageLog{GatewayGroupID: group.ID, GatewayKeyID: row.GatewayKeyID,
						ChannelID: row.ChannelID, SourceGroupName: row.SourceGroupName, RequestedModel: row.RequestedModel,
						RequestID: row.RequestID, Attempt: i + 2, ActualCost: cost, CreatedAt: date,
						AttemptStatus: GatewayAttemptStatusError})
				}
			}
			report, err := NewGatewayUsageLogs(tx.Session(&gorm.Session{DisableNestedTransaction: true})).DailyReport(context.Background(), date)
			if err != nil {
				t.Fatal(err)
			}
			if len(report.Today.Sources) != 1 || len(report.Today.Sources[0].Groups) != 1 || len(report.Today.Sources[0].Groups[0].Models) != 1 {
				t.Fatalf("missing report hierarchy: %+v", report.Today)
			}
			source := report.Today.Sources[0]
			for _, metrics := range []GatewayDailyReportMetrics{report.Today.GatewayDailyReportMetrics,
				source.GatewayDailyReportMetrics, source.Groups[0].GatewayDailyReportMetrics,
				source.Groups[0].Models[0].GatewayDailyReportMetrics, source.TopCostModel.GatewayDailyReportMetrics} {
				wantCost := tc.usage.ActualCost + tc.retryCost
				if metrics.ZeroUsageRequests != tc.zeroUsage || metrics.UnpricedRequests != tc.unpriced || math.Abs(metrics.Cost-wantCost) > 1e-9 || metrics.SuccessfulRequests != 1 {
					t.Fatalf("coverage/cost: %+v; want zero=%d unpriced=%d cost=%g", metrics, tc.zeroUsage, tc.unpriced, wantCost)
				}
				if tc.unpriced > 0 {
					if metrics.Revenue != nil || metrics.Profit != nil {
						t.Fatalf("unknown price treated as zero: %+v", metrics)
					}
				} else if metrics.Revenue == nil || metrics.Profit == nil || math.Abs(*metrics.Revenue-tc.revenue) > 1e-9 || math.Abs(*metrics.Profit-(tc.revenue-wantCost)) > 1e-9 {
					t.Fatalf("incorrect zero usage profit: %+v", metrics)
				}
			}
			if report.Yesterday.ZeroUsageRequests != 0 {
				t.Fatal("zero usage leaked across day boundary")
			}
			var saved GatewayUsageLog
			if err := tx.First(&saved, row.ID).Error; err != nil || saved.ActualCost != row.ActualCost || saved.BilledCost != row.BilledCost {
				t.Fatalf("report changed recorded costs: %+v, %v", saved, err)
			}
			if tc.snapshot != nil {
				var savedSnapshot GatewayRequestFinalization
				if err := tx.Where("request_id = ?", snapshot.RequestID).First(&savedSnapshot).Error; err != nil || savedSnapshot.ActualCost != snapshot.ActualCost || savedSnapshot.BilledCost != snapshot.BilledCost {
					t.Fatalf("report rewrote settlement: %+v, %v", savedSnapshot, err)
				}
			}
		})
	}
	t.Run("zero usage/mixed known and unknown prices stay distinct", func(t *testing.T) {
		tx := db.Begin()
		if tx.Error != nil {
			t.Fatal(tx.Error)
		}
		t.Cleanup(func() { tx.Rollback() })
		date := time.Date(2026, 10, 25, 0, 0, 0, 0, time.UTC)
		group := GatewayGroup{Name: "mixed-zero-usage", SellRateMultiplier: reportProfitPointer(.5)}
		if err := tx.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
		rows := []GatewayUsageLog{
			{RequestID: "mixed-zero", ChannelID: 4101},
			{RequestID: "mixed-unknown", ChannelID: 4102, InputTokens: 100},
			{RequestID: "mixed-priced", ChannelID: 4101, InputTokens: 100, ActualCost: 1, BilledCost: 2, BillingRateMultiplier: .5},
		}
		for i := range rows {
			row := &rows[i]
			row.GatewayGroupID, row.GatewayKeyID = group.ID, 804
			row.SourceGroupName, row.RequestedModel = "source", "model"
			row.Winner, row.Success, row.AttemptStatus, row.CreatedAt = true, true, GatewayAttemptStatusAccepted, date
			if err := tx.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
		report, err := NewGatewayUsageLogs(tx.Session(&gorm.Session{DisableNestedTransaction: true})).DailyReport(context.Background(), date)
		if err != nil {
			t.Fatal(err)
		}
		if report.Today.ZeroUsageRequests != 1 || report.Today.UnpricedRequests != 1 || report.Today.Profit != nil || report.Today.Cost != 1 {
			t.Fatalf("zero usage hid missing price or changed costs: %+v", report.Today)
		}
		if len(report.Today.Sources) != 2 {
			t.Fatalf("sources: %+v", report.Today.Sources)
		}
		for _, source := range report.Today.Sources {
			if source.SourceID == 4101 {
				if source.ZeroUsageRequests != 1 || source.UnpricedRequests != 0 || source.Revenue == nil || source.Profit == nil || *source.Revenue != 2 || *source.Profit != 1 {
					t.Fatalf("zero usage blocked priced channel: %+v", source)
				}
			} else if source.SourceID != 4102 || source.ZeroUsageRequests != 0 || source.UnpricedRequests != 1 || source.Profit != nil {
				t.Fatalf("unknown-price channel treated as free: %+v", source)
			}
		}
	})
}
