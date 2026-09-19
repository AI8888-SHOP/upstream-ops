package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestGatewayUsageStatsUsesLongerStaleWindow(t *testing.T) {
	if gatewayStatsRefreshInterval <= 30*time.Second {
		t.Fatalf("stats refresh interval = %s, want longer than the 30-second UI poll", gatewayStatsRefreshInterval)
	}
	if gatewayStatsCacheTTL <= gatewayStatsRefreshInterval {
		t.Fatalf("stats cache TTL = %s, want longer than refresh interval %s", gatewayStatsCacheTTL, gatewayStatsRefreshInterval)
	}
}

// usageQueryCounter observes executed GORM statements without changing the
// underlying database or the repository code under test.
type usageQueryCounter struct {
	logger.Interface

	mu    sync.Mutex
	count int
}

func (l *usageQueryCounter) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	_, _ = fc()
	l.mu.Lock()
	l.count++
	l.mu.Unlock()
}

func (l *usageQueryCounter) reset() {
	l.mu.Lock()
	l.count = 0
	l.mu.Unlock()
}

func (l *usageQueryCounter) value() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

func TestGatewayUsageStatsOptionalEndpointAggregateAndCache(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	if err := logs.Create(&GatewayUsageLog{
		RequestID:       "stats-endpoint-request",
		InboundEndpoint: "/v1/responses",
		ActualCost:      0.5,
		CreatedAt:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create usage: %v", err)
	}

	counter := &usageQueryCounter{Interface: db.Logger}
	tracked := db.Session(&gorm.Session{Logger: counter})
	trackedLogs := NewGatewayUsageLogs(tracked)

	withEndpoints, err := trackedLogs.Stats(GatewayUsageQuery{IncludeEndpoints: true})
	if err != nil {
		t.Fatalf("stats with endpoints: %v", err)
	}
	withEndpointQueries := counter.value()
	if len(withEndpoints.Endpoints) != 1 || withEndpoints.Endpoints[0].Endpoint != "/v1/responses" {
		t.Fatalf("endpoint stats = %+v, want one /v1/responses row", withEndpoints.Endpoints)
	}

	counter.reset()
	withoutEndpoints, err := trackedLogs.Stats(GatewayUsageQuery{})
	if err != nil {
		t.Fatalf("stats without endpoints: %v", err)
	}
	withoutEndpointQueries := counter.value()
	if len(withoutEndpoints.Endpoints) != 0 {
		t.Fatalf("endpoint stats without opt-in = %+v, want empty", withoutEndpoints.Endpoints)
	}
	if withEndpointQueries != withoutEndpointQueries+1 {
		t.Fatalf("query counts with/without endpoint aggregate = %d/%d, want one fewer query", withEndpointQueries, withoutEndpointQueries)
	}

	// A repeated request for the same lightweight key is served from cache and
	// must not re-run either aggregate or RPM/TPM query.
	counter.reset()
	if _, err := trackedLogs.Stats(GatewayUsageQuery{}); err != nil {
		t.Fatalf("cached stats: %v", err)
	}
	if got := counter.value(); got != 0 {
		t.Fatalf("cached stats executed %d queries, want 0", got)
	}
}

func TestGatewayUsageListOptionalSumAggregate(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	if err := logs.Create(&GatewayUsageLog{
		RequestID:  "list-sum-request",
		ActualCost: 1.25,
		CreatedAt:  time.Now().UTC(),
	}); err != nil {
		t.Fatalf("create usage: %v", err)
	}

	counter := &usageQueryCounter{Interface: db.Logger}
	tracked := db.Session(&gorm.Session{Logger: counter})
	trackedLogs := NewGatewayUsageLogs(tracked)

	withSum, err := trackedLogs.List(GatewayUsageQuery{IncludeSum: true, PageSize: 20})
	if err != nil {
		t.Fatalf("list with sum: %v", err)
	}
	withSumQueries := counter.value()
	if withSum.SumCost != 1.25 {
		t.Fatalf("sum_cost = %v, want 1.25", withSum.SumCost)
	}

	counter.reset()
	withoutSum, err := trackedLogs.List(GatewayUsageQuery{PageSize: 20})
	if err != nil {
		t.Fatalf("list without sum: %v", err)
	}
	withoutSumQueries := counter.value()
	if withoutSum.SumCost != 0 {
		t.Fatalf("sum_cost without opt-in = %v, want 0", withoutSum.SumCost)
	}
	if withSumQueries != withoutSumQueries+1 {
		t.Fatalf("query counts with/without sum = %d/%d, want one fewer query", withSumQueries, withoutSumQueries)
	}
}

func TestGatewayUsageSourceFiltersTimelineAndGroupOverview(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	now := time.Now().UTC().Truncate(time.Second)
	groupID := int64(41)
	rows := []GatewayUsageLog{
		{
			GatewayGroupID: 7, ChannelID: 11, SourceGroupID: &groupID,
			SourceGroupName: "premium", RequestID: "source-monitor",
			Winner: true, Success: true, InputTokens: 50, CacheReadTokens: 50, OutputTokens: 20,
			ActualCost: 0.12, AccountRateMultiplier: 1.25, CreatedAt: now.Add(-30 * time.Minute),
		},
		{
			GatewayGroupID: 7, GatewayProviderID: 12, ProviderName: "direct-b",
			RequestID: "source-provider", Winner: true, Success: true,
			InputTokens: 50, ActualCost: 0.05, CreatedAt: now.Add(-10 * time.Minute),
		},
		{
			GatewayGroupID: 7, ChannelID: 11, SourceGroupID: &groupID,
			SourceGroupName: "premium-renamed", RequestID: "source-monitor-renamed",
			Winner: true, Success: true, InputTokens: 25, OutputTokens: 5,
			ActualCost: 0.03, AccountRateMultiplier: 1.5, CreatedAt: now.Add(-5 * time.Minute),
		},
	}
	for i := range rows {
		if err := logs.Create(&rows[i]); err != nil {
			t.Fatalf("create usage %d: %v", i, err)
		}
	}

	page, err := logs.List(GatewayUsageQuery{
		GatewayGroupID: 7, ChannelID: 11, SourceGroupID: &groupID, PageSize: 20,
	})
	if err != nil {
		t.Fatalf("list source group: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 2 {
		t.Fatalf("filtered page = %+v", page)
	}

	options, err := logs.ListSourceOptions(GatewayUsageQuery{GatewayGroupID: 7})
	if err != nil {
		t.Fatalf("source options: %v", err)
	}
	if len(options.Sources) != 2 || len(options.SourceGroups) != 1 {
		t.Fatalf("source options = %+v", options)
	}
	if got := options.SourceGroups[0]; got.GroupID == nil || *got.GroupID != groupID || got.GroupName != "premium-renamed" || got.Count != 2 {
		t.Fatalf("source group option = %+v", got)
	}

	from, to := now.Add(-time.Hour), now.Add(time.Minute)
	timeline, err := logs.Timeline(GatewayUsageQuery{GatewayGroupID: 7, From: &from, To: &to})
	if err != nil {
		t.Fatalf("timeline: %v", err)
	}
	var requests, tokens int64
	for _, point := range timeline {
		requests += point.Requests
		tokens += point.Tokens
	}
	if requests != 3 || tokens != 200 {
		t.Fatalf("timeline requests/tokens = %d/%d, want 3/200: %+v", requests, tokens, timeline)
	}

	// Older usage must not leak into the one-hour overview or its totals.
	if err := logs.Create(&GatewayUsageLog{
		GatewayGroupID: 7, ChannelID: 11, SourceGroupID: &groupID,
		SourceGroupName: "premium-renamed", RequestID: "source-monitor-old",
		Winner: true, Success: true, InputTokens: 10, CacheReadTokens: 990,
		CreatedAt: now.Add(-2 * time.Hour),
	}); err != nil {
		t.Fatalf("create older usage: %v", err)
	}

	overview, err := logs.GroupOverview(7, []GatewayRoute{{
		ID: 1, GatewayGroupID: 7, SourceKind: GatewayRouteSourceMonitor,
		SourceChannelID: 11, SourceGroupID: &groupID, SourceGroupName: "premium-renamed",
		Enabled: true, BillingRateMultiplier: 1.5,
	}})
	if err != nil {
		t.Fatalf("group overview: %v", err)
	}
	if len(overview.ActiveSourceGroups) != 1 {
		t.Fatalf("active sources = %+v", overview.ActiveSourceGroups)
	}
	active := overview.ActiveSourceGroups[0]
	if active.RequestCount != 2 || active.UsageCount != 2 || active.Tokens != 150 || active.CacheHitRate != 40 || active.AccountRateMultiplier != 1.5 {
		t.Fatalf("active source = %+v", active)
	}
	if overview.Totals.TotalRequests != 3 || overview.Totals.TotalTokens != 200 {
		t.Fatalf("one-hour overview totals = %+v", overview.Totals)
	}
	if active.LastUsedAt == nil || !active.LastUsedAt.Equal(now.Add(-5*time.Minute)) {
		t.Fatalf("last used = %v, want latest in-window usage", active.LastUsedAt)
	}
}

func TestGatewayUsageTimelineRealCacheRateAndShortWindows(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	start := time.Date(2026, time.September, 1, 12, 0, 0, 0, time.UTC)
	groupID, otherGroupID := int64(41), int64(42)
	base := GatewayUsageLog{
		GatewayGroupID: 7, GatewayKeyID: 9, ChannelID: 11, SourceGroupID: &groupID,
		RequestedModel: "cache-model", Winner: true, Success: true,
		CreatedAt: start.Add(20 * time.Second),
	}
	rows := make([]GatewayUsageLog, 0)
	add := func(id string, modify func(*GatewayUsageLog)) {
		row := base
		row.RequestID = id
		modify(&row)
		rows = append(rows, row)
	}
	add("real-read", func(row *GatewayUsageLog) {
		row.InputTokens, row.CacheReadTokens, row.VirtualCacheReadTokens = 50, 50, 50
	})
	// Successful non-winning attempts use the same real-cache definition as Stats.
	add("real-creation", func(row *GatewayUsageLog) {
		row.InputTokens, row.CacheCreationTokens = 100, 100
		row.Winner = false
	})
	add("failed-with-tokens", func(row *GatewayUsageLog) {
		row.InputTokens, row.CacheReadTokens = 9999, 9999
		row.Winner, row.Success = false, false
	})
	add("virtual-only", func(row *GatewayUsageLog) {
		row.CreatedAt = row.CreatedAt.Add(time.Minute)
		row.InputTokens, row.VirtualCacheReadTokens = 100, 100
	})
	add("failed-only", func(row *GatewayUsageLog) {
		row.CreatedAt = row.CreatedAt.Add(2 * time.Minute)
		row.InputTokens, row.CacheReadTokens = 100, 100
		row.Winner, row.Success = false, false
	})
	add("output-only", func(row *GatewayUsageLog) {
		row.CreatedAt = row.CreatedAt.Add(3 * time.Minute)
		row.OutputTokens = 20
	})
	add("fully-cached", func(row *GatewayUsageLog) {
		row.CreatedAt = row.CreatedAt.Add(4 * time.Minute)
		row.CacheReadTokens = 200
	})
	// Each row differs in just one filter, so leaks change both counts and rates.
	base.CacheReadTokens = 1000
	add("other-gateway", func(row *GatewayUsageLog) { row.GatewayGroupID++ })
	add("other-key", func(row *GatewayUsageLog) { row.GatewayKeyID++ })
	add("other-channel", func(row *GatewayUsageLog) { row.ChannelID++ })
	add("other-source-group", func(row *GatewayUsageLog) { row.SourceGroupID = &otherGroupID })
	add("other-model", func(row *GatewayUsageLog) { row.RequestedModel = "other-model" })
	add("before-window", func(row *GatewayUsageLog) { row.CreatedAt = start.Add(-time.Minute) })
	add("after-window", func(row *GatewayUsageLog) { row.CreatedAt = start.Add(2 * time.Hour) })
	for i := range rows {
		if err := logs.Create(&rows[i]); err != nil {
			t.Fatalf("create usage %s: %v", rows[i].RequestID, err)
		}
	}

	for _, window := range []time.Duration{5 * time.Minute, 10 * time.Minute, time.Hour} {
		t.Run(window.String(), func(t *testing.T) {
			to := start.Add(window)
			points, err := logs.Timeline(GatewayUsageQuery{
				GatewayGroupID: 7, GatewayKeyID: 9, ChannelID: 11, SourceGroupID: &groupID,
				Model: "cache-model", From: &start, To: &to,
			})
			if err != nil {
				t.Fatalf("timeline: %v", err)
			}
			want := []struct {
				requests int64
				rate     float64
				hasRate  bool
			}{
				{3, 50.0 / 300.0 * 100, true},
				{1, 0, true},
				{1, 0, false},
				{1, 0, false},
				{1, 100, true},
			}
			if len(points) != len(want) {
				t.Fatalf("minute buckets = %+v, want %d points", points, len(want))
			}
			for i, point := range points {
				if !point.Bucket.Equal(start.Add(time.Duration(i)*time.Minute)) || point.Requests != want[i].requests {
					t.Fatalf("bucket %d = %+v", i, point)
				}
				if (point.CacheHitRate != nil) != want[i].hasRate {
					t.Fatalf("bucket %d cache rate = %v, want hasRate=%v", i, point.CacheHitRate, want[i].hasRate)
				}
				if point.CacheHitRate != nil {
					if diff := *point.CacheHitRate - want[i].rate; diff < -0.0001 || diff > 0.0001 {
						t.Fatalf("bucket %d cache rate = %f, want %f", i, *point.CacheHitRate, want[i].rate)
					}
				}
			}
		})
	}
}
