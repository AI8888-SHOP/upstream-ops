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
	graceUntil := time.Now().Add(30 * time.Minute)
	if err := logs.ClearCacheHealthBlacklistWithSuppression(GatewayRouteSourceProvider, 3, graceUntil); err != nil {
		t.Fatalf("clear provider blacklist: %v", err)
	}
	rows, err = logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{3})
	if err != nil || len(rows) != 1 || rows[0].HitRate != 80 || rows[0].BlacklistedUntil != nil || rows[0].BlacklistReason != "" || rows[0].ManualClearUntil == nil {
		t.Fatalf("source-cleared state = %#v err=%v", rows, err)
	}
	// The global cleanup remains idempotent after a source-specific clear.
	if err := logs.ClearCacheHealthBlacklists(); err != nil {
		t.Fatalf("clear blacklists: %v", err)
	}
	rows, err = logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{3})
	if err != nil || len(rows) != 1 || rows[0].BlacklistedUntil != nil || rows[0].BlacklistReason != "" || rows[0].ManualClearUntil != nil {
		t.Fatalf("cleared state = %#v err=%v", rows, err)
	}
}

func TestCacheHealthIsolatedByGatewayGroup(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	now := time.Now().Truncate(time.Millisecond)
	rows := []*GatewayUsageLog{
		{GatewayGroupID: 11, GatewayProviderID: 7, RequestID: "group-11", Success: true, InputTokens: 100, CacheReadTokens: 0, CreatedAt: now},
		{GatewayGroupID: 22, GatewayProviderID: 7, RequestID: "group-22", Success: true, InputTokens: 0, CacheReadTokens: 100, CreatedAt: now},
	}
	for _, row := range rows {
		if err := logs.Create(row); err != nil {
			t.Fatalf("create usage row: %v", err)
		}
	}
	from := now.Add(-time.Minute)
	group11, err := logs.CacheHealthAggregateForGroup(GatewayRouteSourceProvider, 7, 11, from)
	if err != nil {
		t.Fatalf("group 11 aggregate: %v", err)
	}
	group22, err := logs.CacheHealthAggregateForGroup(GatewayRouteSourceProvider, 7, 22, from)
	if err != nil {
		t.Fatalf("group 22 aggregate: %v", err)
	}
	if group11.RequestCount != 1 || group11.HitRate != 0 {
		t.Fatalf("group 11 aggregate = %+v", group11)
	}
	if group22.RequestCount != 1 || group22.HitRate != 100 {
		t.Fatalf("group 22 aggregate = %+v", group22)
	}

	until := now.Add(time.Hour)
	if err := logs.UpsertCacheHealth(&GatewayChannelCacheHealth{
		GatewayGroupID: 11, SourceKind: GatewayRouteSourceProvider, SourceID: 7,
		HitRate: 0, RequestCount: 1, BlacklistedUntil: &until,
	}); err != nil {
		t.Fatalf("upsert group 11 state: %v", err)
	}
	if err := logs.UpsertCacheHealth(&GatewayChannelCacheHealth{
		GatewayGroupID: 22, SourceKind: GatewayRouteSourceProvider, SourceID: 7,
		HitRate: 100, RequestCount: 1,
	}); err != nil {
		t.Fatalf("upsert group 22 state: %v", err)
	}
	group11States, err := logs.CacheHealthStatesForGroup(GatewayRouteSourceProvider, []uint{7}, 11)
	if err != nil || len(group11States) != 1 || group11States[0].BlacklistedUntil == nil {
		t.Fatalf("group 11 state = %#v err=%v", group11States, err)
	}
	group22States, err := logs.CacheHealthStatesForGroup(GatewayRouteSourceProvider, []uint{7}, 22)
	if err != nil || len(group22States) != 1 || group22States[0].BlacklistedUntil != nil {
		t.Fatalf("group 22 state = %#v err=%v", group22States, err)
	}
	if err := logs.ClearCacheHealthBlacklistForGroup(GatewayRouteSourceProvider, 7, 11); err != nil {
		t.Fatalf("clear group 11 state: %v", err)
	}
	group11States, err = logs.CacheHealthStatesForGroup(GatewayRouteSourceProvider, []uint{7}, 11)
	if err != nil || len(group11States) != 1 || group11States[0].BlacklistedUntil != nil {
		t.Fatalf("cleared group 11 state = %#v err=%v", group11States, err)
	}
	group22States, err = logs.CacheHealthStatesForGroup(GatewayRouteSourceProvider, []uint{7}, 22)
	if err != nil || len(group22States) != 1 || group22States[0].BlacklistedUntil != nil {
		t.Fatalf("group 22 state changed after group 11 clear = %#v err=%v", group22States, err)
	}
}

func TestCacheHealthIsolatedByRouteWithinGatewayGroup(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	now := time.Now().Truncate(time.Millisecond)
	for i, routeID := range []uint{101, 202} {
		row := &GatewayUsageLog{
			GatewayGroupID: 9, RouteID: routeID, GatewayProviderID: 7,
			RequestID: "route-health-" + string(rune('a'+i)), Success: true,
			CreatedAt: now,
		}
		if routeID == 101 {
			row.InputTokens = 100
		} else {
			row.CacheReadTokens = 100
		}
		if err := logs.Create(row); err != nil {
			t.Fatalf("create route usage: %v", err)
		}
	}
	aggs, err := logs.CacheHealthAggregatesForGroup(GatewayRouteSourceProvider, []uint{7}, 9, now.Add(-time.Minute))
	if err != nil || len(aggs) != 2 {
		t.Fatalf("route aggregates = %#v err=%v", aggs, err)
	}
	byRoute := map[uint]GatewayCacheHealthAggregate{}
	for _, agg := range aggs {
		byRoute[agg.RouteID] = agg
	}
	if byRoute[101].HitRate != 0 || byRoute[202].HitRate != 100 {
		t.Fatalf("route hit rates = %#v", byRoute)
	}
	until := now.Add(time.Hour)
	for _, routeID := range []uint{101, 202} {
		if err := logs.UpsertCacheHealth(&GatewayChannelCacheHealth{
			GatewayGroupID: 9, RouteID: routeID, SourceKind: GatewayRouteSourceProvider, SourceID: 7,
			HitRate: float64(routeID), BlacklistedUntil: &until,
		}); err != nil {
			t.Fatalf("upsert route %d state: %v", routeID, err)
		}
	}
	if err := logs.ClearCacheHealthBlacklistForRoute(GatewayRouteSourceProvider, 7, 9, 101); err != nil {
		t.Fatalf("clear route state: %v", err)
	}
	states, err := logs.CacheHealthStatesForGroup(GatewayRouteSourceProvider, []uint{7}, 9)
	if err != nil || len(states) != 2 {
		t.Fatalf("route states = %#v err=%v", states, err)
	}
	for _, state := range states {
		if state.RouteID == 101 && state.BlacklistedUntil != nil {
			t.Fatalf("cleared route remains blocked: %+v", state)
		}
		if state.RouteID == 202 && state.BlacklistedUntil == nil {
			t.Fatalf("sibling route was cleared: %+v", state)
		}
	}
}

func TestClearCacheHealthBlacklistsBelowMinimum(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	until := time.Now().Add(time.Hour)
	for _, state := range []*GatewayChannelCacheHealth{
		{SourceKind: GatewayRouteSourceProvider, SourceID: 1, RequestCount: 1, BlacklistedUntil: &until, BlacklistReason: "legacy warmup"},
		{SourceKind: GatewayRouteSourceProvider, SourceID: 2, RequestCount: 10, BlacklistedUntil: &until, BlacklistReason: "enough samples"},
	} {
		if err := logs.UpsertCacheHealth(state); err != nil {
			t.Fatalf("seed state: %v", err)
		}
	}
	cleared, err := logs.ClearCacheHealthBlacklistsBelowMinimum(10)
	if err != nil || cleared != 1 {
		t.Fatalf("cleared=%d err=%v", cleared, err)
	}
	rows, err := logs.CacheHealthStates(GatewayRouteSourceProvider, []uint{1, 2})
	if err != nil || len(rows) != 2 {
		t.Fatalf("states=%#v err=%v", rows, err)
	}
	byID := map[uint]GatewayChannelCacheHealth{}
	for _, row := range rows {
		byID[row.SourceID] = row
	}
	if byID[1].BlacklistedUntil != nil || byID[1].BlacklistReason != "" {
		t.Fatalf("under-sampled state remained blocked: %+v", byID[1])
	}
	if byID[2].BlacklistedUntil == nil || byID[2].BlacklistReason != "enough samples" {
		t.Fatalf("qualified state was released: %+v", byID[2])
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
