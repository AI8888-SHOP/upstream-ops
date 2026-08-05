package gateway

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func loadBalanceTestCandidates(channelIDs ...uint) []ScoredRoute {
	candidates := make([]ScoredRoute, 0, len(channelIDs))
	for i, channelID := range channelIDs {
		candidates = append(candidates, ScoredRoute{Route: storage.GatewayRoute{
			ID:              uint(i + 1),
			SourceKind:      storage.GatewayRouteSourceMonitor,
			SourceChannelID: channelID,
		}})
	}
	return candidates
}

func loadBalanceTestRouteIDs(candidates []ScoredRoute) []uint {
	ids := make([]uint, len(candidates))
	for i := range candidates {
		ids[i] = candidates[i].Route.ID
	}
	return ids
}

func assertLoadBalanceTestRouteIDs(t *testing.T, got []ScoredRoute, want ...uint) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("candidate count = %d, want %d; ids=%v", len(got), len(want), loadBalanceTestRouteIDs(got))
	}
	for i := range want {
		if got[i].Route.ID != want[i] {
			t.Fatalf("candidate ids = %v, want %v", loadBalanceTestRouteIDs(got), want)
		}
	}
}

func TestOrderLoadBalancedCandidatesCountOnePreservesLegacyOrder(t *testing.T) {
	for _, count := range []int{-1, 0, 1} {
		t.Run(fmt.Sprintf("count_%d", count), func(t *testing.T) {
			rt := (&Service{}).runtime()
			group := &storage.GatewayGroup{ID: uint(100 + count + 1), LoadBalanceRouteCount: count}
			candidates := loadBalanceTestCandidates(11, 22, 33)

			for i := 0; i < 3; i++ {
				got := rt.orderLoadBalancedCandidates(candidates, group, nil)
				assertLoadBalanceTestRouteIDs(t, got, 1, 2, 3)
			}
		})
	}
}

func TestOrderLoadBalancedCandidatesRoundRobinAndFailoverOrder(t *testing.T) {
	tests := []struct {
		name      string
		groupID   uint
		poolSize  int
		wantOrder [][]uint
	}{
		{
			name:     "two upstreams",
			groupID:  201,
			poolSize: 2,
			wantOrder: [][]uint{
				{1, 2, 3, 4},
				{2, 1, 3, 4},
				{1, 2, 3, 4},
				{2, 1, 3, 4},
			},
		},
		{
			name:     "three upstreams",
			groupID:  202,
			poolSize: 3,
			wantOrder: [][]uint{
				{1, 2, 3, 4},
				{2, 1, 3, 4},
				{3, 1, 2, 4},
				{1, 2, 3, 4},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rt := (&Service{}).runtime()
			group := &storage.GatewayGroup{ID: tt.groupID, LoadBalanceRouteCount: tt.poolSize}
			candidates := loadBalanceTestCandidates(11, 22, 33, 44)

			for _, want := range tt.wantOrder {
				got := rt.orderLoadBalancedCandidates(candidates, group, nil)
				assertLoadBalanceTestRouteIDs(t, got, want...)
			}
		}
	}
}

func TestOrderLoadBalancedCandidatesDeduplicatesPhysicalUpstreams(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 301, LoadBalanceRouteCount: 2}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 10}},
		{Route: storage.GatewayRoute{ID: 2, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 10}},
		{Route: storage.GatewayRoute{ID: 3, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 20}},
		{Route: storage.GatewayRoute{ID: 4, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 30}},
	}

	firstRoutes := make([]uint, 4)
	for i := range firstRoutes {
		ordered := rt.orderLoadBalancedCandidates(candidates, group, nil)
		firstRoutes[i] = ordered[0].Route.ID
	}
	if want := []uint{1, 3, 1, 3}; fmt.Sprint(firstRoutes) != fmt.Sprint(want) {
		t.Fatalf("first routes = %v, want %v; duplicate route for channel 10 must not occupy a pool slot", firstRoutes, want)
	}
}

func TestOrderLoadBalancedCandidatesSeparatesMonitorAndProviderIDs(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 302, LoadBalanceRouteCount: 2}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 7}},
		{Route: storage.GatewayRoute{ID: 2, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: 7}},
		{Route: storage.GatewayRoute{ID: 3, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 8}},
	}

	for i, want := range []uint{1, 2, 1, 2} {
		got := rt.orderLoadBalancedCandidates(candidates, group, nil)
		if got[0].Route.ID != want {
			t.Fatalf("call %d first route = %d, want %d", i+1, got[0].Route.ID, want)
		}
	}
}

func TestOrderLoadBalancedCandidatesCountAboveAvailableUsesAllUpstreams(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 401, LoadBalanceRouteCount: 64}
	candidates := loadBalanceTestCandidates(11, 22, 33)

	for i, want := range []uint{1, 2, 3, 1, 2, 3} {
		got := rt.orderLoadBalancedCandidates(candidates, group, nil)
		if got[0].Route.ID != want {
			t.Fatalf("call %d first route = %d, want %d", i+1, got[0].Route.ID, want)
		}
	}
}

func TestOrderLoadBalancedCandidatesConcurrentRoundRobin(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 501, LoadBalanceRouteCount: 3}
	candidates := loadBalanceTestCandidates(11, 22, 33, 44)
	const requests = 300

	start := make(chan struct{})
	counts := make(map[uint]int)
	var countsMu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(requests)
	for i := 0; i < requests; i++ {
		go func() {
			defer wg.Done()
			<-start
			ordered := rt.orderLoadBalancedCandidates(candidates, group, nil)
			countsMu.Lock()
			counts[ordered[0].Route.ID]++
			countsMu.Unlock()
		}()
	}
	close(start)
	wg.Wait()

	for _, routeID := range []uint{1, 2, 3} {
		if counts[routeID] != requests/3 {
			t.Fatalf("route %d selected %d times, want %d; all counts=%v", routeID, counts[routeID], requests/3, counts)
		}
	}
	if counts[4] != 0 {
		t.Fatalf("route outside the first three physical upstreams was selected %d times", counts[4])
	}
}

func TestOrderLoadBalancedCandidatesPreservesHealthySessionAffinity(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 601, LoadBalanceRouteCount: 2}
	routes := []storage.GatewayRoute{
		{ID: 1, SourceChannelID: 11, Position: 0, Enabled: true, SourceAPIKeyCipher: "a", RateConvertMode: "custom", RateConvertValue: 0.01},
		{ID: 2, SourceChannelID: 22, Position: 1, Enabled: true, SourceAPIKeyCipher: "b", RateConvertMode: "custom", RateConvertValue: 0.02},
		{ID: 3, SourceChannelID: 33, Position: 2, Enabled: true, SourceAPIKeyCipher: "c", RateConvertMode: "custom", RateConvertValue: 0.03},
	}
	affinityKey := routeAffinityKey{GatewayKeyID: 1, GroupID: group.ID, Protocol: "openai_chat", Model: "m", Fingerprint: "session"}
	affinity := routeAffinityContext{
		Keys:             []routeAffinityKey{affinityKey},
		LookupKey:        affinityKey,
		PreferredRouteID: 2,
	}

	candidates := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &affinity, "m")
	assertLoadBalanceTestRouteIDs(t, candidates, 2, 1, 3)
	for i := 0; i < 2; i++ {
		ordered := rt.orderLoadBalancedCandidates(candidates, group, &affinity)
		assertLoadBalanceTestRouteIDs(t, ordered, 2, 1, 3)
	}

	// Affinity traffic must not consume the shared round-robin cursor. The
	// next new session therefore starts at the first route in the pool.
	unaffiliated := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, nil, "m")
	ordered := rt.orderLoadBalancedCandidates(unaffiliated, group, nil)
	assertLoadBalanceTestRouteIDs(t, ordered, 1, 2, 3)
}

func TestCreateAndUpdateGroupLoadBalanceRouteCountBounds(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := NewService(
		groups,
		storage.NewGatewayKeys(db),
		storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db),
		storage.NewModelPriceOverrides(db),
		storage.NewChannels(db),
		nil,
		nil,
		nil,
	)

	defaultGroup, err := svc.CreateGroup(CreateGroupInput{Name: "load-balance-default"})
	if err != nil {
		t.Fatalf("CreateGroup default: %v", err)
	}
	if defaultGroup.LoadBalanceRouteCount != 1 {
		t.Fatalf("default load balance route count = %d, want 1", defaultGroup.LoadBalanceRouteCount)
	}

	tooHigh := 999
	clampedGroup, err := svc.CreateGroup(CreateGroupInput{
		Name:                  "load-balance-clamped",
		LoadBalanceRouteCount: &tooHigh,
	})
	if err != nil {
		t.Fatalf("CreateGroup clamped: %v", err)
	}
	if clampedGroup.LoadBalanceRouteCount != maxLoadBalanceRouteCount {
		t.Fatalf("clamped load balance route count = %d, want %d", clampedGroup.LoadBalanceRouteCount, maxLoadBalanceRouteCount)
	}

	zero := 0
	updated, err := svc.UpdateGroup(clampedGroup.ID, UpdateGroupInput{LoadBalanceRouteCount: &zero})
	if err != nil {
		t.Fatalf("UpdateGroup zero: %v", err)
	}
	if updated.LoadBalanceRouteCount != 1 {
		t.Fatalf("updated load balance route count = %d, want 1", updated.LoadBalanceRouteCount)
	}
	persisted, err := groups.FindByID(clampedGroup.ID)
	if err != nil {
		t.Fatalf("FindByID updated group: %v", err)
	}
	if persisted.LoadBalanceRouteCount != 1 {
		t.Fatalf("persisted load balance route count = %d, want 1", persisted.LoadBalanceRouteCount)
	}
}

func TestOrderLoadBalancedCandidatesFillsPoolAfterModelAndEnabledFiltering(t *testing.T) {
	now := time.Now()
	cooldownUntil := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, SourceChannelID: 10, Position: 0, Enabled: false, SourceAPIKeyCipher: "disabled", RateConvertMode: "custom", RateConvertValue: 0.01},
		{
			ID: 2, SourceChannelID: 20, Position: 1, Enabled: true, SourceAPIKeyCipher: "cooled", RateConvertMode: "custom", RateConvertValue: 0.02,
			ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
				"m": {RouteID: 2, Model: "m", TempUnschedulableUntil: &cooldownUntil},
			},
		},
		{ID: 3, SourceChannelID: 30, Position: 2, Enabled: true, SourceAPIKeyCipher: "available-a", RateConvertMode: "custom", RateConvertValue: 0.03},
		{ID: 4, SourceChannelID: 30, Position: 3, Enabled: true, SourceAPIKeyCipher: "available-a-second-route", RateConvertMode: "custom", RateConvertValue: 0.04},
		{ID: 5, SourceChannelID: 40, Position: 4, Enabled: true, SourceAPIKeyCipher: "available-b", RateConvertMode: "custom", RateConvertValue: 0.05},
		{ID: 6, SourceChannelID: 50, Position: 5, Enabled: true, SourceAPIKeyCipher: "available-c", RateConvertMode: "custom", RateConvertValue: 0.06},
	}

	candidates := SortRoutesForModel(routes, nil, "asc", now, nil, "m")
	assertLoadBalanceTestRouteIDs(t, candidates, 3, 4, 5, 6)
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 701, LoadBalanceRouteCount: 2}

	first := rt.orderLoadBalancedCandidates(candidates, group, nil)
	assertLoadBalanceTestRouteIDs(t, first, 3, 4, 5, 6)
	second := rt.orderLoadBalancedCandidates(candidates, group, nil)
	assertLoadBalanceTestRouteIDs(t, second, 5, 3, 4, 6)
}
