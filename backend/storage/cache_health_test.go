package storage

import (
	"testing"
	"time"
)

func TestCacheHealthAggregateUsesRealCacheBucketsAndWindow(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	now := time.Now().Truncate(time.Millisecond)
	rows := []*GatewayUsageLog{
		{GatewayProviderID: 7, RequestID: "p1", Success: true, InputTokens: 50, CacheReadTokens: 50, VirtualCacheReadTokens: 100, CreatedAt: now.Add(-5 * time.Minute)},
		{GatewayProviderID: 7, RequestID: "p2", Success: true, InputTokens: 100, CacheCreationTokens: 100, CreatedAt: now.Add(-5 * time.Minute)},
		// Outside the rolling window.
		{GatewayProviderID: 7, RequestID: "old", Success: true, InputTokens: 1, CacheReadTokens: 999, CreatedAt: now.Add(-2 * time.Hour)},
		// A monitor channel with the same numeric id must not leak into provider stats.
		{ChannelID: 7, RequestID: "monitor", Success: true, InputTokens: 1, CacheReadTokens: 99, CreatedAt: now.Add(-5 * time.Minute)},
	}
	for _, row := range rows {
		if err := logs.Create(row); err != nil {
			t.Fatalf("create usage row: %v", err)
		}
	}
	agg, err := logs.CacheHealthAggregate(GatewayRouteSourceProvider, 7, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("aggregate: %v", err)
	}
	if agg.RequestCount != 2 || agg.InputTokens != 150 || agg.CacheReadTokens != 50 || agg.CacheCreationTokens != 100 {
		t.Fatalf("aggregate buckets = %+v", agg)
	}
	want := 50.0 / 300.0 * 100
	if diff := agg.HitRate - want; diff < -0.0001 || diff > 0.0001 {
		t.Fatalf("hit rate = %.6f, want %.6f", agg.HitRate, want)
	}
	stats, err := logs.Stats(GatewayUsageQuery{GatewayProviderID: 7})
	if err != nil {
		t.Fatalf("usage stats: %v", err)
	}
	if stats.CacheHitRate <= 0 || stats.CacheHealthReadTokens != 1049 {
		t.Fatalf("stats cache fields = %+v", stats)
	}
}

func TestCacheHealthStateUpsertBySource(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	until := time.Now().Add(time.Hour)
	state := &GatewayChannelCacheHealth{
		SourceKind: GatewayRouteSourceProvider, SourceID: 3,
		HitRate: 12.5, RequestCount: 8, BlacklistedUntil: &until,
	}
	if err := logs.UpsertCacheHealth(state); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	state.HitRate = 80
	state.BlacklistReason = "updated"
	if err := logs.UpsertCacheHealth(state); err != nil {
		t.Fatalf("second upsert: %v", err)
	}
	rows, err := logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{3})
	if err != nil || len(rows) != 1 {
		t.Fatalf("states = %#v err=%v", rows, err)
	}
	if rows[0].HitRate != 80 || rows[0].BlacklistReason != "updated" {
		t.Fatalf("state = %+v", rows[0])
	}
	if err := logs.ClearCacheHealthBlacklist(GatewayRouteSourceProvider, 3); err != nil {
		t.Fatalf("clear provider blacklist: %v", err)
	}
	rows, err = logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{3})
	if err != nil || len(rows) != 1 || rows[0].HitRate != 80 || rows[0].BlacklistedUntil != nil || rows[0].BlacklistReason != "" {
		t.Fatalf("source-cleared state = %#v err=%v", rows, err)
	}
	// The global cleanup remains idempotent after a source-specific clear.
	if err := logs.ClearCacheHealthBlacklists(); err != nil {
		t.Fatalf("clear blacklists: %v", err)
	}
	rows, err = logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{3})
	if err != nil || len(rows) != 1 || rows[0].BlacklistedUntil != nil || rows[0].BlacklistReason != "" {
		t.Fatalf("cleared state = %#v err=%v", rows, err)
	}
}

func TestUsageCleanupDropsDerivedCacheHealthState(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	if err := logs.UpsertCacheHealth(&GatewayChannelCacheHealth{SourceKind: GatewayRouteSourceProvider, SourceID: 9, HitRate: 1}); err != nil {
		t.Fatalf("seed state: %v", err)
	}
	if err := logs.Create(&GatewayUsageLog{GatewayProviderID: 9, RequestID: "cleanup-cache-health", Success: true, CreatedAt: time.Now().Add(-time.Hour)}); err != nil {
		t.Fatalf("seed usage: %v", err)
	}
	if _, err := logs.DeleteAll(); err != nil {
		t.Fatalf("delete all: %v", err)
	}
	rows, err := logs.CacheHealthStates(GatewayRouteSourceProvider, nil)
	if err != nil {
		t.Fatalf("read states: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("derived states remain after cleanup: %+v", rows)
	}
}
