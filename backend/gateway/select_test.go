package gateway

import (
	"reflect"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestSortRoutes_RateThenWeight(t *testing.T) {
	now := time.Now()
	routes := []storage.GatewayRoute{
		{ID: 1, SourceChannelID: 1, Position: 0, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.5, SourceAPIKeyCipher: "x", BillingRateMultiplier: 1},
		{ID: 2, SourceChannelID: 2, Position: 1, Weight: 10, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.2, SourceAPIKeyCipher: "x", BillingRateMultiplier: 1},
		{ID: 3, SourceChannelID: 3, Position: 2, Weight: 5, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.2, SourceAPIKeyCipher: "x", BillingRateMultiplier: 1},
	}
	got := SortRoutes(routes, nil, "asc", now, nil)
	if len(got) != 3 {
		t.Fatalf("len=%d", len(got))
	}
	// rate 0.2 first; among 0.2 weight 10 before 5
	if got[0].Route.ID != 2 || got[1].Route.ID != 3 || got[2].Route.ID != 1 {
		t.Fatalf("order=%v %v %v", got[0].Route.ID, got[1].Route.ID, got[2].Route.ID)
	}
}

func TestOrderRoutesByRate_MatchesAttemptOrder(t *testing.T) {
	routes := []storage.GatewayRoute{
		{ID: 10, Position: 0, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 1, SourceAPIKeyCipher: "x"},
		{ID: 11, Position: 1, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.05, SourceAPIKeyCipher: "x"},
	}
	ordered := OrderRoutesByRate(routes, nil, "asc")
	if len(ordered) != 2 || ordered[0].ID != 11 || ordered[1].ID != 10 {
		t.Fatalf("asc order=%+v", ordered)
	}
	if ordered[0].Position != 0 || ordered[1].Position != 1 {
		t.Fatalf("positions=%d %d", ordered[0].Position, ordered[1].Position)
	}
	// 禁用路由也参与落库排序
	routes[0].Enabled = false
	ordered = OrderRoutesByRate(routes, nil, "asc")
	if ordered[0].ID != 11 || ordered[1].ID != 10 {
		t.Fatalf("disabled still sorted by rate: %+v", ordered)
	}
}

func TestSortRoutes_TempPauseAndExclude(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, SourceChannelID: 1, Position: 0, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.1, SourceAPIKeyCipher: "x", TempUnschedulableUntil: &until},
		{ID: 2, SourceChannelID: 2, Position: 1, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.2, SourceAPIKeyCipher: "x"},
		{ID: 3, SourceChannelID: 3, Position: 2, Weight: 1, Enabled: false, RateConvertMode: "custom", RateConvertValue: 0.05, SourceAPIKeyCipher: "x"},
	}
	got := SortRoutes(routes, nil, "asc", now, map[uint]struct{}{2: {}})
	if len(got) != 0 {
		t.Fatalf("expected empty, got %d", len(got))
	}
	got = SortRoutes(routes, nil, "asc", now, nil)
	if len(got) != 1 || got[0].Route.ID != 2 {
		t.Fatalf("got=%+v", got)
	}
}

func TestRateForRoute_GroupRatio(t *testing.T) {
	gid := int64(9)
	route := &storage.GatewayRoute{
		SourceGroupID:    &gid,
		RateConvertMode:  "multiply_100",
		RateConvertValue: 1,
	}
	groups := []connector.APIKeyGroup{
		{ID: &gid, Name: "default", Ratio: 0.05},
	}
	got := RateForRoute(route, groups)
	if got != 5 {
		t.Fatalf("got %v want 5", got)
	}
}

func TestSortRoutesForModel_CooldownIsolated(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{
			ID: 1, SourceChannelID: 1, Position: 0, Weight: 1, Enabled: true,
			RateConvertMode: "custom", RateConvertValue: 0.1, SourceAPIKeyCipher: "x",
			ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
				"model-a": {RouteID: 1, Model: "model-a", TempUnschedulableUntil: &until},
			},
		},
		{ID: 2, SourceChannelID: 2, Position: 1, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.2, SourceAPIKeyCipher: "x"},
	}

	got := SortRoutesForModel(routes, nil, "asc", now, nil, "model-a")
	if len(got) != 1 || got[0].Route.ID != 2 {
		t.Fatalf("model-a should skip only cooled route, got=%+v", got)
	}
	got = SortRoutesForModel(routes, nil, "asc", now, nil, "model-b")
	if len(got) != 2 || got[0].Route.ID != 1 {
		t.Fatalf("model-b should keep route 1 schedulable, got=%+v", got)
	}
}

func TestIsRouteSchedulableForModelExpiredProbeLeaseFailsOpen(t *testing.T) {
	now := time.Now()
	expired := now.Add(-time.Second)
	route := storage.GatewayRoute{
		ID: 1, SourceChannelID: 1, Enabled: true, SourceAPIKeyCipher: "x",
		ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"model-a": {
				RouteID: 1, Model: "model-a", ProbeStatus: storage.GatewayModelProbeStatusProbing,
				ProbeLeaseUntil: &expired, NextProbeAt: &expired,
			},
		},
	}
	if !IsRouteSchedulableForModel(&route, "model-a", now) {
		t.Fatal("expired probe lease must not permanently block the route")
	}
}

func TestRecoverWhenAllRoutesRestrictedWakesOldestDistinctUpstreams(t *testing.T) {
	now := time.Date(2026, time.August, 7, 12, 0, 0, 0, time.UTC)
	activeUntil := now.Add(5 * time.Minute)
	failedAt := func(minutesAgo int) *time.Time {
		value := now.Add(-time.Duration(minutesAgo) * time.Minute)
		return &value
	}
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 10, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(4), TempUnschedulableReason: "first"},
		}},
		// A second route for the same physical channel must not consume the
		// second emergency recovery slot.
		{ID: 2, Position: 1, SourceChannelID: 10, Enabled: true, SourceAPIKeyCipher: "b", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 2, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(3), TempUnschedulableReason: "second"},
		}},
		{ID: 3, Position: 2, SourceChannelID: 20, Enabled: true, SourceAPIKeyCipher: "c", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 3, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(2), TempUnschedulableReason: "third"},
		}},
		{ID: 4, Position: 3, SourceChannelID: 30, Enabled: true, SourceAPIKeyCipher: "d", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 4, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(1), TempUnschedulableReason: "fourth"},
		}},
	}

	original := append([]storage.GatewayRoute(nil), routes...)
	rt := (&Service{}).runtime()
	got := rt.recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if len(got) != len(routes) {
		t.Fatalf("route count=%d, want %d", len(got), len(routes))
	}
	if got[0].ModelCooldowns["m"].TempUnschedulableUntil != nil || got[2].ModelCooldowns["m"].TempUnschedulableUntil != nil {
		t.Fatalf("oldest routes were not woken: route1=%+v route3=%+v", got[0].ModelCooldowns["m"], got[2].ModelCooldowns["m"])
	}
	if got[1].ModelCooldowns["m"].TempUnschedulableUntil == nil || got[3].ModelCooldowns["m"].TempUnschedulableUntil == nil {
		t.Fatal("a second route on the first channel or a newer route was woken")
	}
	if got[0].ModelCooldowns["m"].TempUnschedulableReason != "first" || got[2].ModelCooldowns["m"].TempUnschedulableReason != "third" {
		t.Fatal("emergency recovery should retain failure diagnostics")
	}
	if !reflect.DeepEqual(original[0].ModelCooldowns["m"].TempUnschedulableUntil, routes[0].ModelCooldowns["m"].TempUnschedulableUntil) {
		t.Fatal("request-local recovery must not mutate the input route slice")
	}
	if candidates := SortRoutesForModel(got, nil, "asc", now, nil, "m"); len(candidates) != 2 || candidates[0].Route.ID != 1 || candidates[1].Route.ID != 3 {
		t.Fatalf("woken routes were not schedulable: %+v", candidates)
	}
}

func TestRecoverWhenAllRoutesRestrictedSeparatesSourceGroupsOnSameChannel(t *testing.T) {
	now := time.Date(2026, time.August, 19, 12, 0, 0, 0, time.UTC)
	activeUntil := now.Add(5 * time.Minute)
	failedAt := func(minutesAgo int) *time.Time {
		value := now.Add(-time.Duration(minutesAgo) * time.Minute)
		return &value
	}
	groupA, groupB := int64(201), int64(202)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 10, SourceGroupID: &groupA, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(4)},
		}},
		{ID: 2, Position: 1, SourceChannelID: 10, SourceGroupID: &groupB, Enabled: true, SourceAPIKeyCipher: "b", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 2, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(3)},
		}},
		{ID: 3, Position: 2, SourceChannelID: 20, Enabled: true, SourceAPIKeyCipher: "c", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 3, Model: "m", TempUnschedulableUntil: &activeUntil, TempUnschedulableAt: failedAt(2)},
		}},
	}

	got := (&Service{}).runtime().recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if got[0].ModelCooldowns["m"].TempUnschedulableUntil != nil || got[1].ModelCooldowns["m"].TempUnschedulableUntil != nil {
		t.Fatalf("same-channel source groups were incorrectly deduplicated: route1=%+v route2=%+v", got[0].ModelCooldowns["m"], got[1].ModelCooldowns["m"])
	}
}

func TestRecoverWhenAllRoutesRestrictedProbePendingUsesFailureAge(t *testing.T) {
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Second)
	failedAt := func(minutesAgo int) *time.Time {
		value := now.Add(-time.Duration(minutesAgo) * time.Minute)
		return &value
	}
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 1, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", NextProbeAt: &due, ProbeStatus: storage.GatewayModelProbeStatusPending, TempUnschedulableAt: failedAt(30)},
		}},
		{ID: 2, Position: 1, SourceChannelID: 2, Enabled: true, SourceAPIKeyCipher: "b", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 2, Model: "m", NextProbeAt: &due, ProbeStatus: storage.GatewayModelProbeStatusPending, TempUnschedulableAt: failedAt(20)},
		}},
		{ID: 3, Position: 2, SourceChannelID: 3, Enabled: true, SourceAPIKeyCipher: "c", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 3, Model: "m", NextProbeAt: &due, ProbeStatus: storage.GatewayModelProbeStatusPending, TempUnschedulableAt: failedAt(10)},
		}},
	}

	got := (&Service{}).runtime().recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	for _, id := range []uint{1, 2} {
		cooldown := got[id-1].ModelCooldowns["m"]
		if cooldown.ProbeStatus != storage.GatewayModelProbeStatusManual || cooldown.NextProbeAt != nil {
			t.Fatalf("oldest pending route %d was not selected: %+v", id, cooldown)
		}
	}
	if cooldown := got[2].ModelCooldowns["m"]; cooldown.ProbeStatus != storage.GatewayModelProbeStatusPending || cooldown.NextProbeAt == nil {
		t.Fatalf("newest pending route was selected unexpectedly: %+v", cooldown)
	}
}

func TestRecoverWhenAllRoutesRestrictedLeavesHealthyRouteUntouched(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 1, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", TempUnschedulableUntil: &until},
		}},
		{ID: 2, Position: 1, SourceChannelID: 2, Enabled: true, SourceAPIKeyCipher: "b"},
	}
	rt := (&Service{}).runtime()
	got := rt.recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if !reflect.DeepEqual(got, routes) {
		t.Fatalf("healthy route means no emergency reset; got=%+v want=%+v", got, routes)
	}
}

func TestRecoverWhenAllRoutesRestrictedClearsResolvedUpstreamModel(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 1, Enabled: true, SourceAPIKeyCipher: "a", ModelMappingJSON: `{"alias":"upstream"}`, ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"upstream": {RouteID: 1, Model: "upstream", TempUnschedulableUntil: &until},
		}},
		{ID: 2, Position: 1, SourceChannelID: 2, Enabled: true, SourceAPIKeyCipher: "b", ModelMappingJSON: `{"alias":"upstream"}`, ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"upstream": {RouteID: 2, Model: "upstream", TempUnschedulableUntil: &until},
		}},
	}
	rt := (&Service{}).runtime()
	got := rt.recoverWhenAllRoutesRestricted(routes, "alias", nil, now)
	for _, route := range got {
		if route.ModelCooldowns["upstream"].TempUnschedulableUntil != nil {
			t.Fatalf("resolved upstream cooldown was not cleared: %+v", route.ModelCooldowns)
		}
		if _, exists := route.ModelCooldowns["alias"]; exists {
			t.Fatal("request alias should not be persisted as a cooldown key")
		}
	}
}

func TestRecoverWhenAllRoutesRestrictedWakesHighestHitRateCacheRoutes(t *testing.T) {
	now := time.Date(2026, time.August, 17, 1, 0, 0, 0, time.UTC)
	until := now.Add(10 * time.Minute)
	evaluatedAt := func(minutesAgo int) *time.Time {
		value := now.Add(-time.Duration(minutesAgo) * time.Minute)
		return &value
	}
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 10, Enabled: true, SourceAPIKeyCipher: "a", CacheHealthHitRate: 10, CacheHealthBlacklistedUntil: &until, CacheHealthEvaluatedAt: evaluatedAt(3)},
		{ID: 2, Position: 1, SourceChannelID: 20, Enabled: true, SourceAPIKeyCipher: "b", CacheHealthHitRate: 80, CacheHealthBlacklistedUntil: &until, CacheHealthEvaluatedAt: evaluatedAt(2)},
		{ID: 3, Position: 2, SourceChannelID: 30, Enabled: true, SourceAPIKeyCipher: "c", CacheHealthHitRate: 60, CacheHealthBlacklistedUntil: &until, CacheHealthEvaluatedAt: evaluatedAt(1)},
	}

	rt := (&Service{}).runtime()
	got := rt.recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if got[1].CacheHealthBlacklistedUntil != nil || got[2].CacheHealthBlacklistedUntil != nil {
		t.Fatalf("highest-hit-rate cache restrictions were not bypassed: route2=%+v route3=%+v", got[1], got[2])
	}
	if got[0].CacheHealthBlacklistedUntil == nil {
		t.Fatal("the lowest-hit-rate route consumed a bounded recovery slot")
	}
	if routes[1].CacheHealthBlacklistedUntil == nil || routes[2].CacheHealthBlacklistedUntil == nil {
		t.Fatal("cache blacklist bypass must remain request-local")
	}
	if candidates := SortRoutesForModel(got, nil, "asc", now, nil, "m"); len(candidates) != 2 || candidates[0].Route.ID != 2 || candidates[1].Route.ID != 3 {
		t.Fatalf("cache recovery candidates=%+v", candidates)
	}
}

func TestRecoverWhenAllRoutesRestrictedPrefersCacheOnlyRoute(t *testing.T) {
	now := time.Date(2026, time.August, 17, 1, 0, 0, 0, time.UTC)
	until := now.Add(10 * time.Minute)
	failedAt := now.Add(-10 * time.Minute)
	evaluatedAt := now.Add(-time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 10, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", TempUnschedulableUntil: &until, TempUnschedulableAt: &failedAt},
		}},
		{ID: 2, Position: 1, SourceChannelID: 20, Enabled: true, SourceAPIKeyCipher: "b", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 2, Model: "m", TempUnschedulableUntil: &until, TempUnschedulableAt: &failedAt},
		}},
		{ID: 3, Position: 2, SourceChannelID: 30, Enabled: true, SourceAPIKeyCipher: "c", CacheHealthBlacklistedUntil: &until, CacheHealthEvaluatedAt: &evaluatedAt},
	}

	got := (&Service{}).runtime().recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if got[2].CacheHealthBlacklistedUntil != nil {
		t.Fatal("cache-only restriction should be preferred over a known failed model")
	}
	if got[0].ModelCooldowns["m"].TempUnschedulableUntil != nil && got[1].ModelCooldowns["m"].TempUnschedulableUntil != nil {
		t.Fatal("recovery should retain one model probe alongside the cache-only route")
	}
}

func TestRecoverWhenAllRoutesRestrictedDoesNotBypassHardRestrictions(t *testing.T) {
	now := time.Date(2026, time.August, 17, 1, 0, 0, 0, time.UTC)
	until := now.Add(10 * time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 10, Enabled: true, SourceAPIKeyCipher: "valid", CacheHealthBlacklistedUntil: &until},
		{ID: 2, Position: 1, SourceChannelID: 20, Enabled: false, SourceAPIKeyCipher: "disabled", CacheHealthBlacklistedUntil: &until},
		{ID: 3, Position: 2, SourceChannelID: 30, Enabled: true, CacheHealthBlacklistedUntil: &until},
		{ID: 4, Position: 3, SourceChannelID: 40, Enabled: true, RateLimitAutoDisabled: true, SourceAPIKeyCipher: "limited", CacheHealthBlacklistedUntil: &until},
	}

	got := (&Service{}).runtime().recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if got[0].CacheHealthBlacklistedUntil != nil {
		t.Fatal("the otherwise-valid route should be recovered")
	}
	for _, index := range []int{1, 2, 3} {
		if got[index].CacheHealthBlacklistedUntil == nil {
			t.Fatalf("hard-restricted route %d was incorrectly recovered", got[index].ID)
		}
	}
	if candidates := SortRoutesForModel(got, nil, "asc", now, nil, "m"); len(candidates) != 1 || candidates[0].Route.ID != 1 {
		t.Fatalf("hard restrictions must remain unschedulable: %+v", candidates)
	}
}

func TestBindModelCooldownAliases(t *testing.T) {
	now := time.Now()
	until := now.Add(time.Minute)
	routes := []storage.GatewayRoute{{
		ID: 1, Enabled: true, SourceAPIKeyCipher: "x",
		ModelMappingJSON: `{"alias":"upstream-model"}`,
		ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"upstream-model": {RouteID: 1, Model: "upstream-model", TempUnschedulableUntil: &until},
		},
	}}
	routes = bindModelCooldownAliases(routes, "alias", nil)
	if _, ok := routes[0].ModelCooldowns["alias"]; !ok {
		t.Fatal("resolved upstream cooldown was not aliased to requested model")
	}
	if got := SortRoutesForModel(routes, nil, "asc", now, nil, "alias"); len(got) != 0 {
		t.Fatalf("aliased cooldown should filter route, got=%+v", got)
	}
}

func TestRateForRoute_GroupNameFallbackTrimsWhitespace(t *testing.T) {
	route := &storage.GatewayRoute{
		SourceGroupName:  "Team/plus典韦 🪓",
		RateConvertMode:  "raw",
		RateConvertValue: 1,
	}
	groups := []connector.APIKeyGroup{
		{Name: "Team/plus典韦 🪓 ", Ratio: 0.063},
	}
	got := RateForRoute(route, groups)
	if got != 0.063 {
		t.Fatalf("got %v want 0.063", got)
	}
}

func TestRateForRoute_PrefersGroupIDOverName(t *testing.T) {
	wantedID, otherID := int64(54), int64(55)
	route := &storage.GatewayRoute{
		SourceGroupID:    &wantedID,
		SourceGroupName:  "Team/plus典韦 🪓",
		RateConvertMode:  "raw",
		RateConvertValue: 1,
	}
	groups := []connector.APIKeyGroup{
		{ID: &otherID, Name: "Team/plus典韦 🪓", Ratio: 1},
		{ID: &wantedID, Name: "renamed upstream group", Ratio: 0.063},
	}
	got := RateForRoute(route, groups)
	if got != 0.063 {
		t.Fatalf("got %v want 0.063", got)
	}
}

func TestRateForRoute_FallbackBillingMultiplier(t *testing.T) {
	// 列表显示 0.05 且已落库，但运行时拉不到源分组时不应变成 1
	route := &storage.GatewayRoute{
		SourceGroupName:       "grok",
		RateConvertMode:       "raw",
		BillingRateMultiplier: 0.05,
	}
	got := RateForRoute(route, nil)
	if got != 0.05 {
		t.Fatalf("got %v want 0.05", got)
	}
}

func TestRateForRoute_LegacyGroupIDPlaceholder(t *testing.T) {
	groupID := int64(63)
	route := &storage.GatewayRoute{
		SourceGroupName:       "id:63",
		RateConvertMode:       "raw",
		BillingRateMultiplier: 1,
	}
	groups := []connector.APIKeyGroup{{ID: &groupID, Name: "Team/plus典韦 🪓", Ratio: 0.063}}
	if got := RateForRoute(route, groups); got != 0.063 {
		t.Fatalf("legacy ID placeholder should use live group ratio, got %v", got)
	}
}

func TestOrderRoutesByRate_LegacyPlaceholderUsesLiveThreeDecimalRate(t *testing.T) {
	lowID, middleID, highID := int64(60), int64(63), int64(70)
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, SourceChannelID: 1, SourceGroupName: "id:70", Weight: 1, RateConvertMode: "raw"},
		{ID: 2, Position: 1, SourceChannelID: 1, SourceGroupName: "id:63", Weight: 1, RateConvertMode: "raw"},
		{ID: 3, Position: 2, SourceChannelID: 1, SourceGroupName: "id:60", Weight: 1, RateConvertMode: "raw"},
	}
	groups := map[uint][]connector.APIKeyGroup{1: {
		{ID: &highID, Name: "high", Ratio: 0.07},
		{ID: &middleID, Name: "Team/plus典韦 🪓", Ratio: 0.063},
		{ID: &lowID, Name: "low", Ratio: 0.06},
	}}
	ordered := OrderRoutesByRate(routes, groups, "asc")
	if len(ordered) != 3 || ordered[0].ID != 3 || ordered[1].ID != 2 || ordered[2].ID != 1 {
		t.Fatalf("live rates should order 0.06 < 0.063 < 0.07, got ids %d/%d/%d", ordered[0].ID, ordered[1].ID, ordered[2].ID)
	}
}

func TestOrderRoutesByRate_SameRateHigherWeightFirst(t *testing.T) {
	routes := []storage.GatewayRoute{
		{ID: 1, Position: 0, Weight: 1, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.05, SourceAPIKeyCipher: "x"},
		{ID: 2, Position: 1, Weight: 99, Enabled: true, RateConvertMode: "custom", RateConvertValue: 0.05, SourceAPIKeyCipher: "x"},
	}
	// 输入顺序：低权重在前；同倍率应按权重大优先
	ordered := OrderRoutesByRate(routes, nil, "asc")
	if ordered[0].ID != 2 || ordered[1].ID != 1 {
		t.Fatalf("want weight 99 first, got ids %d %d", ordered[0].ID, ordered[1].ID)
	}
}

func TestResolveModel(t *testing.T) {
	up, chain := ResolveModel("claude-opus", map[string]string{"claude-opus": "claude-sonnet"})
	if up != "claude-sonnet" || chain != "claude-opus->claude-sonnet" {
		t.Fatalf("up=%s chain=%s", up, chain)
	}
	up, chain = ResolveModel("foo", map[string]string{"*": "bar"})
	if up != "bar" || chain != "foo->bar" {
		t.Fatalf("up=%s chain=%s", up, chain)
	}
}

func TestCalculateCost(t *testing.T) {
	p := ModelPricing{InputPricePerToken: 0.001, OutputPricePerToken: 0.002}
	// 原值倍率 0.06：actual = base × 0.06（只乘一次账号计费倍率）
	c := CalculateCost(p, UsageTokens{InputTokens: 10, OutputTokens: 5}, 0.06, 0.06)
	// base = 10*0.001 + 5*0.002 = 0.02; actual = 0.0012
	if c.TotalCost != 0.02 || c.ActualCost != 0.0012 {
		t.Fatalf("%+v", c)
	}
	// billing 无效时回退 rate
	c2 := CalculateCost(p, UsageTokens{InputTokens: 10, OutputTokens: 5}, 2, 0)
	if c2.ActualCost != 0.04 {
		t.Fatalf("fallback actual=%v want 0.04", c2.ActualCost)
	}
}

func TestPricingCatalog_Grok45Fallback(t *testing.T) {
	cat := NewPricingCatalog(nil)
	p := cat.Resolve("grok-4.5")
	// sub2api: $2 / $6 per MTok
	if p.InputPricePerToken != 2e-6 || p.OutputPricePerToken != 6e-6 {
		t.Fatalf("grok-4.5 pricing = %+v", p)
	}
	cost := CalculateCost(p, UsageTokens{InputTokens: 2693, OutputTokens: 49}, 0.05, 0.05)
	if cost.TotalCost <= 0 || cost.ActualCost <= 0 {
		t.Fatalf("expected non-zero cost, got %+v", cost)
	}
}

func TestPricingCatalog_KnownLiteLLMModel(t *testing.T) {
	cat := NewPricingCatalog(nil)
	p := cat.Resolve("claude-sonnet-4-5")
	if !p.HasTokenPrice() {
		t.Fatalf("expected litellm price for claude-sonnet-4-5, got %+v", p)
	}
}

func TestPricingCatalog_DeepSeekModule(t *testing.T) {
	cat := NewPricingCatalog(nil)
	// 系统默认价应覆盖官方 DeepSeek 主型号
	for _, name := range []string{
		"deepseek-chat",
		"deepseek-reasoner",
		"deepseek-v3",
		"deepseek-v3.2",
		"deepseek-r1",
		"deepseek-coder",
		"deepseek-v4-flash",
		"deepseek-v4-pro",
		"deepseek/deepseek-chat",
	} {
		p := cat.Resolve(name)
		if !p.HasTokenPrice() {
			t.Fatalf("expected price for %s, got %+v", name, p)
		}
	}
	// 带前缀 / 变体后缀走家族回退
	p := cat.Resolve("deepseek/deepseek-v4-flash-experimental")
	if p.InputPricePerToken != 1.4e-7 || p.OutputPricePerToken != 2.8e-7 {
		t.Fatalf("v4-flash family fallback = %+v", p)
	}
	// 列表可见 deepseek 条目
	items := cat.ListDefaults("deepseek")
	if len(items) < 10 {
		t.Fatalf("ListDefaults(deepseek) too few: %d", len(items))
	}
}

func TestRateForRoute_RawIsSourceRatio(t *testing.T) {
	gid := int64(3)
	route := &storage.GatewayRoute{
		SourceGroupID:   &gid,
		RateConvertMode: "raw",
	}
	groups := []connector.APIKeyGroup{
		{ID: &gid, Name: "pro", Ratio: 0.06},
	}
	got := RateForRoute(route, groups)
	if got != 0.06 {
		t.Fatalf("raw mode should keep source ratio, got %v want 0.06", got)
	}
}
