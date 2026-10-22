package gateway

import (
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
