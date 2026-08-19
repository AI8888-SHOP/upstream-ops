package gateway

import (
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestEvaluateCacheHealthBlacklistsProviderRoutesAndExpires(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	routes := storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	group := &storage.GatewayGroup{Name: "cache-health-group", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	provider := &storage.GatewayProvider{
		Name: "cache-health-provider", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	svc := NewService(groups, storage.NewGatewayKeys(db), routes, usage, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	svc.UpdateGatewayConfig(config.GatewayConfig{
		CacheHitRateWindowMinutes:    60,
		CacheHitRateThresholdPercent: 50,
		CacheHitRateBlacklistMinutes: 10,
		CacheHitRateMinimumRequests:  12,
	})
	saved, err := svc.SaveRoutes(group.ID, []RouteInput{{
		SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID,
		Enabled: true, Weight: 1,
	}})
	if err != nil || len(saved) != 1 {
		t.Fatalf("save route: %#v err=%v", saved, err)
	}
	now := time.Now().Truncate(time.Millisecond)
	for i := 0; i < 11; i++ {
		if err := usage.Create(&storage.GatewayUsageLog{
			GatewayGroupID: group.ID, RouteID: saved[0].ID, GatewayProviderID: provider.ID,
			RequestID: "cache-health-" + string(rune('a'+i)), Success: true,
			InputTokens: 90, CacheReadTokens: 10, CreatedAt: now.Add(-time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("create usage: %v", err)
		}
	}
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, provider.ID, now); err != nil {
		t.Fatalf("warmup evaluate: %v", err)
	}
	warmupStats, err := svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(warmupStats) != 1 || warmupStats[0].BlacklistedUntil != nil {
		t.Fatalf("source was blacklisted before the minimum sample count: %+v err=%v", warmupStats, err)
	}
	if err := usage.Create(&storage.GatewayUsageLog{
		GatewayGroupID: group.ID, RouteID: saved[0].ID, GatewayProviderID: provider.ID,
		RequestID: "cache-health-l", Success: true,
		InputTokens: 90, CacheReadTokens: 10, CreatedAt: now,
	}); err != nil {
		t.Fatalf("create tenth usage: %v", err)
	}
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, provider.ID, now); err != nil {
		t.Fatalf("evaluate: %v", err)
	}
	stats, err := svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(stats) != 1 {
		t.Fatalf("stats: %#v err=%v", stats, err)
	}
	if stats[0].HitRate >= 50 || stats[0].BlacklistedUntil == nil {
		t.Fatalf("expected active blacklist, stats=%+v", stats[0])
	}
	firstUntil := *stats[0].BlacklistedUntil
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, provider.ID, now.Add(time.Minute)); err != nil {
		t.Fatalf("re-evaluate: %v", err)
	}
	stats, err = svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(stats) != 1 || stats[0].BlacklistedUntil == nil || !stats[0].BlacklistedUntil.Equal(firstUntil) {
		t.Fatalf("blacklist was extended: %+v err=%v", stats, err)
	}
	loaded, err := routes.ListByGroupID(group.ID)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("load routes: %#v err=%v", loaded, err)
	}
	if IsRouteSchedulable(&loaded[0], now) {
		t.Fatal("blacklisted provider route remained schedulable")
	}
	if err := svc.ClearCacheHealthBlacklist(storage.GatewayRouteSourceProvider, provider.ID); err != nil {
		t.Fatalf("clear provider blacklist: %v", err)
	}
	loaded, err = routes.ListByGroupID(group.ID)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("reload after source clear: %#v err=%v", loaded, err)
	}
	if loaded[0].CacheHealthBlacklistedUntil != nil || !IsRouteSchedulable(&loaded[0], now) {
		t.Fatalf("source clear did not release route: %+v", loaded[0])
	}
	// A manual release keeps the existing rolling counters but suppresses the
	// evaluator for the configured window; otherwise the next async evaluation
	// would immediately restore the same blacklist.
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, provider.ID, now.Add(time.Minute)); err != nil {
		t.Fatalf("re-evaluate after source clear: %v", err)
	}
	stats, err = svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(stats) != 1 || stats[0].BlacklistedUntil != nil || stats[0].ManualClearUntil == nil {
		t.Fatalf("manual release was re-blacklisted: %+v err=%v", stats, err)
	}
	// Once the grace window has elapsed, the old rows have also rolled out of
	// the statistics window and the source remains schedulable normally.
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, provider.ID, now.Add(61*time.Minute)); err != nil {
		t.Fatalf("evaluate after grace window: %v", err)
	}
	stats, err = svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(stats) != 1 || stats[0].BlacklistedUntil != nil || stats[0].ManualClearUntil != nil {
		t.Fatalf("expired manual release state = %+v err=%v", stats, err)
	}
	// Disabling protection clears persisted automatic state immediately, even
	// before its original expiry.
	svc.UpdateGatewayConfig(config.GatewayConfig{})
	loaded, err = routes.ListByGroupID(group.ID)
	if err != nil || len(loaded) != 1 {
		t.Fatalf("reload after disable: %#v err=%v", loaded, err)
	}
	if loaded[0].CacheHealthBlacklistedUntil != nil || !IsRouteSchedulable(&loaded[0], now) {
		t.Fatalf("disabled protection left route blocked: %+v", loaded[0])
	}
}

func TestCacheHealthProtectionDisabledDoesNotBlacklist(t *testing.T) {
	db := openGatewayTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	routes := storage.NewGatewayRoutes(db)
	svc := NewService(nil, nil, routes, usage, nil, nil, nil, nil, nil)
	svc.UpdateGatewayConfig(config.GatewayConfig{})
	if err := svc.EvaluateCacheHealth(storage.GatewayRouteSourceProvider, 1, time.Now()); err != nil {
		t.Fatalf("disabled evaluation: %v", err)
	}
	if svc.CacheHealthEnabled() {
		t.Fatal("empty config unexpectedly enabled cache protection")
	}
}

func TestCacheHealthBlacklistIsolatedByGatewayGroup(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	routes := storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	groupA := &storage.GatewayGroup{Name: "cache-health-group-a", Status: storage.GatewayGroupStatusActive}
	groupB := &storage.GatewayGroup{Name: "cache-health-group-b", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(groupA); err != nil {
		t.Fatalf("create group A: %v", err)
	}
	if err := groups.Create(groupB); err != nil {
		t.Fatalf("create group B: %v", err)
	}
	provider := &storage.GatewayProvider{Name: "cache-health-shared-provider", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	svc := NewService(groups, storage.NewGatewayKeys(db), routes, usage, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	svc.UpdateGatewayConfig(config.GatewayConfig{
		CacheHitRateWindowMinutes:    60,
		CacheHitRateThresholdPercent: 50,
		CacheHitRateBlacklistMinutes: 10,
		CacheHitRateMinimumRequests:  10,
	})
	routeA, err := svc.SaveRoutes(groupA.ID, []RouteInput{{SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, Weight: 1}})
	if err != nil || len(routeA) != 1 {
		t.Fatalf("save group A route: %#v err=%v", routeA, err)
	}
	routeB, err := svc.SaveRoutes(groupB.ID, []RouteInput{{SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, Weight: 1}})
	if err != nil || len(routeB) != 1 {
		t.Fatalf("save group B route: %#v err=%v", routeB, err)
	}
	now := time.Now().Truncate(time.Millisecond)
	for i := 0; i < 10; i++ {
		if err := usage.Create(&storage.GatewayUsageLog{GatewayGroupID: groupA.ID, RouteID: routeA[0].ID, GatewayProviderID: provider.ID, RequestID: "group-a-" + string(rune('a'+i)), Success: true, InputTokens: 100, CreatedAt: now}); err != nil {
			t.Fatalf("create group A usage: %v", err)
		}
		if err := usage.Create(&storage.GatewayUsageLog{GatewayGroupID: groupB.ID, RouteID: routeB[0].ID, GatewayProviderID: provider.ID, RequestID: "group-b-" + string(rune('a'+i)), Success: true, CacheReadTokens: 100, CreatedAt: now}); err != nil {
			t.Fatalf("create group B usage: %v", err)
		}
	}
	if err := svc.EvaluateCacheHealthForGroup(storage.GatewayRouteSourceProvider, provider.ID, groupA.ID, now); err != nil {
		t.Fatalf("evaluate group A: %v", err)
	}
	if err := svc.EvaluateCacheHealthForGroup(storage.GatewayRouteSourceProvider, provider.ID, groupB.ID, now); err != nil {
		t.Fatalf("evaluate group B: %v", err)
	}
	statsA, err := svc.CacheHealthStatsForGroup(storage.GatewayRouteSourceProvider, []uint{provider.ID}, groupA.ID)
	if err != nil || len(statsA) != 1 || statsA[0].BlacklistedUntil == nil {
		t.Fatalf("group A stats = %#v err=%v", statsA, err)
	}
	statsB, err := svc.CacheHealthStatsForGroup(storage.GatewayRouteSourceProvider, []uint{provider.ID}, groupB.ID)
	if err != nil || len(statsB) != 1 || statsB[0].BlacklistedUntil != nil || statsB[0].HitRate != 100 {
		t.Fatalf("group B stats = %#v err=%v", statsB, err)
	}
	global, err := svc.CacheHealthStats(storage.GatewayRouteSourceProvider, []uint{provider.ID})
	if err != nil || len(global) != 1 || global[0].GatewayGroupID != 0 || global[0].HitRate != 50 || global[0].BlacklistedUntil == nil {
		t.Fatalf("global stats = %#v err=%v", global, err)
	}
	loadedA, err := routes.ListByGroupID(groupA.ID)
	if err != nil || len(loadedA) != 1 || loadedA[0].CacheHealthBlacklistedUntil == nil {
		t.Fatalf("group A routes = %#v err=%v", loadedA, err)
	}
	loadedB, err := routes.ListByGroupID(groupB.ID)
	if err != nil || len(loadedB) != 1 || loadedB[0].CacheHealthBlacklistedUntil != nil || loadedB[0].CacheHealthHitRate != 100 {
		t.Fatalf("group B routes = %#v err=%v", loadedB, err)
	}
}

func TestCacheHealthBlacklistIsolatedByRouteWithinGatewayGroup(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	routes := storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	usage := storage.NewGatewayUsageLogs(db)
	group := &storage.GatewayGroup{Name: "cache-health-route-group", Status: storage.GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	provider := &storage.GatewayProvider{Name: "cache-health-route-provider", BaseURL: "https://example.test", APIKeyCipher: "cipher", Enabled: true}
	if err := providers.Create(provider); err != nil {
		t.Fatalf("create provider: %v", err)
	}
	svc := NewService(groups, storage.NewGatewayKeys(db), routes, usage, nil, nil, nil, nil, nil)
	svc.SetProviders(providers)
	svc.UpdateGatewayConfig(config.GatewayConfig{
		CacheHitRateWindowMinutes: 60, CacheHitRateThresholdPercent: 50,
		CacheHitRateBlacklistMinutes: 10, CacheHitRateMinimumRequests: 10,
	})
	saved, err := svc.SaveRoutes(group.ID, []RouteInput{
		{SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, Weight: 1},
		{SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, Weight: 1},
	})
	if err != nil || len(saved) != 2 {
		t.Fatalf("save routes: %#v err=%v", saved, err)
	}
	now := time.Now().Truncate(time.Millisecond)
	for i := 0; i < 10; i++ {
		for routeIndex, route := range saved {
			row := &storage.GatewayUsageLog{
				GatewayGroupID: group.ID, RouteID: route.ID, GatewayProviderID: provider.ID,
				RequestID: "route-isolation-" + string(rune('a'+i)) + string(rune('0'+routeIndex)),
				Success:   true, CreatedAt: now,
			}
			if routeIndex == 0 {
				row.InputTokens = 100
			} else {
				row.CacheReadTokens = 100
			}
			if err := usage.Create(row); err != nil {
				t.Fatalf("create usage: %v", err)
			}
		}
	}
	if err := svc.EvaluateCacheHealthForGroup(storage.GatewayRouteSourceProvider, provider.ID, group.ID, now); err != nil {
		t.Fatalf("evaluate routes: %v", err)
	}
	loaded, err := routes.ListByGroupID(group.ID)
	if err != nil || len(loaded) != 2 {
		t.Fatalf("load routes: %#v err=%v", loaded, err)
	}
	if loaded[0].CacheHealthBlacklistedUntil == nil {
		t.Fatalf("low-hit route was not blacklisted: %+v", loaded[0])
	}
	if loaded[1].CacheHealthBlacklistedUntil != nil {
		t.Fatalf("healthy sibling route was blacklisted: %+v", loaded[1])
	}
	if err := svc.ClearCacheHealthBlacklistForRoute(storage.GatewayRouteSourceProvider, provider.ID, group.ID, loaded[0].ID); err != nil {
		t.Fatalf("clear route blacklist: %v", err)
	}
	loaded, err = routes.ListByGroupID(group.ID)
	if err != nil || len(loaded) != 2 || loaded[0].CacheHealthBlacklistedUntil != nil || loaded[1].CacheHealthBlacklistedUntil != nil {
		t.Fatalf("route clear affected unexpected state: %#v err=%v", loaded, err)
	}
}
