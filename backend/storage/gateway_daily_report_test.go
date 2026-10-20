package storage

import (
	"context"
	"math"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestGatewayDailyReportCostsGroupsModelsAndCalendarBoundaries(t *testing.T) {
	db := openTestDB(t)
	repo := NewGatewayUsageLogs(db)
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	today := time.Date(2026, 10, 21, 0, 0, 0, 0, loc)
	groupA, groupB := int64(41), int64(42)
	for _, channel := range []Channel{{ID: 11, Name: "渠道甲", Type: ChannelTypeSub2API, SiteURL: "https://a.invalid", Username: "a"}, {ID: 12, Name: "渠道乙", Type: ChannelTypeSub2API, SiteURL: "https://b.invalid", Username: "b"}} {
		if err := db.Create(&channel).Error; err != nil {
			t.Fatal(err)
		}
	}
	rows := []GatewayUsageLog{
		{ChannelID: 11, SourceGroupID: &groupA, SourceGroupName: "昨天分组", RequestedModel: "expensive", RequestID: "yesterday", Success: true, Winner: true, ActualCost: 4, InputTokens: 20, CacheReadTokens: 80, OutputTokens: 5, CreatedAt: today.Add(-time.Nanosecond)},
		// These two records are one request but two paid attempts. The failed
		// attempt must affect cost/tokens, never real cache-health statistics.
		{ChannelID: 11, SourceGroupID: &groupA, SourceGroupName: "旧名称", RequestedModel: "expensive", RequestID: "retry", Attempt: 1, ActualCost: 2, InputTokens: 10, CacheReadTokens: 90, OutputTokens: 5, CreatedAt: today},
		{ChannelID: 11, SourceGroupID: &groupA, SourceGroupName: "新名称", RequestedModel: "expensive", RequestID: "retry", Attempt: 2, Success: true, Winner: true, ActualCost: 3, InputTokens: 50, CacheReadTokens: 50, OutputTokens: 20, CreatedAt: today.Add(time.Second)},
		{ChannelID: 11, SourceGroupID: &groupA, SourceGroupName: "新名称", RequestedModel: "cheap", RequestID: "cheap-1", Success: true, Winner: true, ActualCost: .2, InputTokens: 80, CacheReadTokens: 20, OutputTokens: 10, CreatedAt: today.Add(2 * time.Second)},
		{ChannelID: 11, SourceGroupID: &groupA, SourceGroupName: "新名称", RequestedModel: "cheap", RequestID: "cheap-2", Success: true, Winner: true, ActualCost: .2, InputTokens: 80, CacheReadTokens: 20, VirtualCacheReadTokens: 80, OutputTokens: 10, CreatedAt: today.Add(3 * time.Second)},
		{ChannelID: 11, SourceGroupID: &groupB, SourceGroupName: "另一组", RequestedModel: "cheap", RequestID: "cheap-3", Success: true, Winner: true, ActualCost: .2, InputTokens: 40, CacheReadTokens: 60, OutputTokens: 10, CreatedAt: today.Add(4 * time.Second)},
		// Reusing a source-group ID at another upstream never merges channels.
		{ChannelID: 12, SourceGroupID: &groupA, SourceGroupName: "新名称", RequestedModel: "expensive", RequestID: "other-site", Success: true, Winner: true, ActualCost: 4, InputTokens: 100, OutputTokens: 10, CreatedAt: today.Add(5 * time.Second)},
		// Provider ID colliding with a channel ID must remain a separate source.
		{GatewayProviderID: 11, ProviderName: "已删除直连快照", RequestedModel: "direct", RequestID: "direct", Success: true, Winner: true, ActualCost: 1, InputTokens: 100, OutputTokens: 10, CreatedAt: today.Add(6 * time.Second)},
		{ChannelID: 11, RequestedModel: "outside", RequestID: "before", ActualCost: 1000, CreatedAt: today.AddDate(0, 0, -1).Add(-time.Nanosecond)},
		{ChannelID: 11, RequestedModel: "outside", RequestID: "after", ActualCost: 1000, CreatedAt: today.AddDate(0, 0, 1)},
	}
	for i := range rows {
		rows[i].CreatedAt = rows[i].CreatedAt.UTC()
		if err := repo.Create(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	counter := &usageQueryCounter{Interface: db.Logger}
	report, err := NewGatewayUsageLogs(db.Session(&gorm.Session{Logger: counter})).DailyReport(context.Background(), today)
	if err != nil {
		t.Fatal(err)
	}
	if report.Timezone != "Asia/Shanghai" || report.Today.Date != "2026-10-21" || report.Yesterday.Date != "2026-10-20" {
		t.Fatalf("dates: %+v", report)
	}
	if report.Today.Requests != 6 || report.Today.Attempts != 7 || report.Today.SuccessfulRequests != 6 {
		t.Fatalf("today counts: %+v", report.Today.GatewayDailyReportMetrics)
	}
	if math.Abs(report.Today.Cost-10.6) > 1e-9 || report.Today.Tokens != 775 {
		t.Fatalf("today cost/tokens: %+v", report.Today.GatewayDailyReportMetrics)
	}
	if report.Today.CacheHitRate == nil || math.Abs(*report.Today.CacheHitRate-25) > 1e-9 {
		t.Fatalf("real cache rate=%v", report.Today.CacheHitRate)
	}
	if report.Yesterday.Requests != 1 || report.Yesterday.Cost != 4 || report.Yesterday.Tokens != 105 {
		t.Fatalf("yesterday: %+v", report.Yesterday)
	}
	if len(report.Today.Sources) != 3 {
		t.Fatalf("sources: %+v", report.Today.Sources)
	}
	first := report.Today.Sources[0]
	if first.SourceID != 11 || first.SourceKind != GatewayRouteSourceMonitor || first.ChannelName != "渠道甲" || math.Abs(first.Cost-5.6) > 1e-9 {
		t.Fatalf("top source: %+v", first)
	}
	if first.TopCostModel == nil || first.TopCostModel.Model != "expensive" || first.TopCostModel.Requests != 1 || first.TopCostModel.Attempts != 2 {
		t.Fatalf("cost leader: %+v", first.TopCostModel)
	}
	if first.TopRequestModel == nil || first.TopRequestModel.Model != "cheap" || first.TopRequestModel.Requests != 3 {
		t.Fatalf("request leader: %+v", first.TopRequestModel)
	}
	if len(first.Groups) != 2 || first.Groups[0].SourceGroupID == nil || *first.Groups[0].SourceGroupID != groupA || first.Groups[0].SourceGroupName != "新名称" {
		t.Fatalf("group merge: %+v", first.Groups)
	}
	if math.Abs(first.Groups[0].Cost-5.4) > 1e-9 || first.Groups[0].Requests != 3 || len(first.Groups[0].Models) != 2 {
		t.Fatalf("group totals: %+v", first.Groups[0])
	}
	last := report.Today.Sources[2]
	if last.SourceKind != GatewayRouteSourceProvider || last.ChannelName != "已删除直连快照" {
		t.Fatalf("provider fallback: %+v", last)
	}
	if counter.value() > 4 {
		t.Fatalf("report ran %d queries; want at most four independent of source/model count", counter.value())
	}
	// Reference repricing changes the report without rewriting charged amounts.
	if err := db.Model(&GatewayUsageLog{}).Where("id = ?", rows[1].ID).Update("actual_cost", 12).Error; err != nil {
		t.Fatal(err)
	}
	repriced, err := repo.DailyReport(context.Background(), today)
	if err != nil || math.Abs(repriced.Today.Cost-20.6) > 1e-9 {
		t.Fatalf("repriced report: %+v, %v", repriced, err)
	}
}

func TestGatewayDailyReportDistinctRequestsAcrossGroupsAndNoCacheSamples(t *testing.T) {
	db := openTestDB(t)
	repo := NewGatewayUsageLogs(db)
	date := time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
	one, two := int64(1), int64(2)
	rows := []GatewayUsageLog{
		{ChannelID: 1, SourceGroupID: &one, RequestID: "shared", RequestedModel: "model", ActualCost: 1, CreatedAt: date},
		{ChannelID: 1, SourceGroupID: &two, RequestID: "shared", RequestedModel: "model", ActualCost: 2, Winner: true, Success: true, CreatedAt: date},
		{ChannelID: 2, SourceGroupID: &one, RequestID: "shared", RequestedModel: "model", ActualCost: 3, CreatedAt: date},
	}
	for i := range rows {
		if err := repo.Create(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	report, err := repo.DailyReport(context.Background(), date)
	if err != nil {
		t.Fatal(err)
	}
	if report.Today.Requests != 1 || report.Today.Attempts != 3 || report.Today.Cost != 6 || report.Today.CacheHitRate != nil {
		t.Fatalf("global dedup/unknown cache: %+v", report.Today)
	}
	if len(report.Today.Sources) != 2 || report.Today.Sources[0].Requests != 1 || report.Today.Sources[1].Requests != 1 {
		t.Fatalf("source dedup: %+v", report.Today.Sources)
	}
	for _, source := range report.Today.Sources {
		if source.SourceID == 1 && (len(source.Groups) != 2 || source.TopRequestModel.Requests != 1) {
			t.Fatalf("model/group dedup: %+v", source)
		}
	}
	if len(report.Yesterday.Sources) != 0 || report.Yesterday.CacheHitRate != nil {
		t.Fatalf("empty yesterday: %+v", report.Yesterday)
	}
}
