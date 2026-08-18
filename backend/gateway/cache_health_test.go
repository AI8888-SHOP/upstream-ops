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
