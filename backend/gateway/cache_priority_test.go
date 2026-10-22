package gateway

import (
	"context"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func cachePriorityCandidates(rates ...float64) []ScoredRoute {
	out := make([]ScoredRoute, 0, len(rates))
	for i, rate := range rates {
		out = append(out, ScoredRoute{Route: storage.GatewayRoute{
			ID: uint(i + 1), Position: i, Enabled: true,
			SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: uint(i + 1),
			CacheHealthHitRate: rate, CacheHealthRequestCount: 200,
		}})
	}
	return out
}

func routeIDs(candidates []ScoredRoute) []uint {
	ids := make([]uint, 0, len(candidates))
	for _, candidate := range candidates {
		ids = append(ids, candidate.Route.ID)
	}
	return ids
}

func assertRouteOrder(t *testing.T, got []ScoredRoute, want []uint) {
	t.Helper()
	ids := routeIDs(got)
	if len(ids) != len(want) {
		t.Fatalf("order=%v want=%v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("order=%v want=%v", ids, want)
		}
	}
}

// 开启后按缓存命中率从高到低排序；关闭时保持原顺序。
func TestCacheHitRatePriorityOrdersCandidates(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{Name: "cache-priority", CacheHitRatePriorityEnabled: true}
	assertRouteOrder(t, rt.applyCacheHitRatePriority(cachePriorityCandidates(10, 90, 50), group, 0), []uint{2, 3, 1})

	off := &storage.GatewayGroup{Name: "cache-priority-off"}
	assertRouteOrder(t, rt.applyCacheHitRatePriority(cachePriorityCandidates(10, 90, 50), off, 0), []uint{1, 2, 3})
}

// 样本不足的渠道使用中性分：既不抢已验证的高命中渠道，也不被排到已知低命中之后。
func TestCacheHitRatePriorityKeepsColdRoutesNeutral(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{Name: "cache-priority", CacheHitRatePriorityEnabled: true}
	candidates := cachePriorityCandidates(10, 90)
	candidates[0].Route.CacheHealthRequestCount = 2 // 样本不足 → 中性 50
	assertRouteOrder(t, rt.applyCacheHitRatePriority(candidates, group, 0), []uint{2, 1})
}

// 兜底分区不受影响：命中率再高也不会排到普通渠道前面。
func TestCacheHitRatePriorityKeepsReserveLast(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{Name: "cache-priority", CacheHitRatePriorityEnabled: true}
	candidates := cachePriorityCandidates(10, 90, 20, 99)
	candidates[2].Route.FallbackOnly = true
	candidates[3].Route.FallbackOnly = true
	assertRouteOrder(t, rt.applyCacheHitRatePriority(candidates, group, 0), []uint{2, 1, 4, 3})
}

// 会话亲和锁定的路由保持普通分区首位。
func TestCacheHitRatePriorityKeepsAffinityFirst(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{Name: "cache-priority", CacheHitRatePriorityEnabled: true}
	assertRouteOrder(t, rt.applyCacheHitRatePriority(cachePriorityCandidates(10, 90, 60), group, 1), []uint{1, 2, 3})
}

// 并发打满的渠道即使命中率最高也要让位。
func TestCacheHitRatePriorityYieldsWhenSaturated(t *testing.T) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{Name: "cache-priority", CacheHitRatePriorityEnabled: true}
	candidates := cachePriorityCandidates(95, 40)
	registry := rt.upstreamConcurrencyRegistry()
	identity := loadBalanceIdentity(&candidates[0].Route)
	release, err := registry.acquire(context.Background(), upstreamConcurrencyKey{Kind: identity.Kind, ID: identity.ID}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	assertRouteOrder(t, rt.applyCacheHitRatePriority(candidates, group, 0), []uint{2, 1})
}

// 占比上限：优先渠道不会被独占，超出部分轮转到后面的渠道。
func TestCacheHitRatePriorityShareCapRotates(t *testing.T) {
	rt := (&Service{}).runtime()
	share := 60
	group := &storage.GatewayGroup{ID: 7, Name: "cache-priority", CacheHitRatePriorityEnabled: true, CacheHitRatePrioritySharePercent: &share}
	lead := map[uint]int{}
	for i := 0; i < 100; i++ {
		ordered := rt.applyCacheHitRatePriority(cachePriorityCandidates(90, 30), group, 0)
		lead[ordered[0].Route.ID]++
	}
	if lead[1] != 60 || lead[2] != 40 {
		t.Fatalf("lead distribution=%v want route1=60 route2=40", lead)
	}
}
