package gateway

import (
	"context"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestExplicitRoutePrioritySurvivesRateSortAndPreservesBilling(t *testing.T) {
	routes := []storage.GatewayRoute{
		{ID: 1, Priority: 0, Enabled: true, SourceKind: "provider", GatewayProviderID: 1, BillingRateMultiplier: .01},
		{ID: 2, Priority: 2, Enabled: true, SourceKind: "provider", GatewayProviderID: 2, BillingRateMultiplier: .02},
		{ID: 3, Priority: 1, Enabled: true, SourceKind: "provider", GatewayProviderID: 3, BillingRateMultiplier: 2},
	}
	for _, direction := range []string{"asc", "desc"} {
		ordered := OrderRoutesByRate(routes, nil, direction)
		if ordered[0].ID != 3 || ordered[1].ID != 2 || ordered[2].ID != 1 {
			t.Fatalf("wrong saved order: %+v", ordered)
		}
		if ordered[0].BillingRateMultiplier != 2 {
			t.Fatal("strategy must not rewrite billing")
		}
		assertRouteOrder(t, SortRoutesForModel(routes, nil, direction, time.Now(), nil, "image"), []uint{3, 2, 1})
	}
	routes[2].Enabled = false
	assertRouteOrder(t, SortRoutesForModel(routes, nil, "asc", time.Now(), nil, "image"), []uint{2, 1})
}

func TestExplicitRoutePriorityKeepsTiesFallbackAndRecovery(t *testing.T) {
	candidates := cachePriorityCandidates(95, 20, 40, 99, 60)
	candidates[0].Route.Priority = 2
	candidates[1].Route.Priority = 1
	candidates[2].Route.Priority = 1
	candidates[3].Route.Priority = 1
	candidates[3].Route.FallbackOnly = true
	rt := (&Service{}).runtime()
	assertRouteOrder(t, rt.orderExplicitRoutePriorities(candidates, 0), []uint{2, 3, 1, 5, 4})
	assertRouteOrder(t, rt.orderExplicitRoutePriorities(candidates, 1), []uint{1, 2, 3, 5, 4})
	assertRouteOrder(t, candidates, []uint{1, 2, 3, 4, 5}) // input not mutated
	identity := loadBalanceIdentity(&candidates[1].Route)
	release, err := rt.upstreamConcurrencyRegistry().acquire(context.Background(), upstreamConcurrencyKey{Kind: identity.Kind, ID: identity.ID}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	assertRouteOrder(t, rt.orderExplicitRoutePriorities(candidates, 0), []uint{3, 1, 5, 2, 4})
	for i := range candidates {
		candidates[i].Route.Priority = 0
	}
	assertRouteOrder(t, rt.orderExplicitRoutePriorities(candidates, 0), []uint{1, 2, 3, 4, 5})
}
