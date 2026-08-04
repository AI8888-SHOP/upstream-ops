package gateway

import (
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/pkg/rateconvert"
	"github.com/bejix/upstream-ops/backend/storage"
)

// ScoredRoute 排序后的候选路由。
type ScoredRoute struct {
	Route         storage.GatewayRoute
	EffectiveRate float64
	BillingRate   float64
}

// parseSourceGroupIDRef keeps old routes sortable when only the display
// placeholder (for example, "id:63") was persisted and SourceGroupID is nil.
func parseSourceGroupIDRef(value string) (int64, bool) {
	compact := strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), "")
	prefixes := []string{"id:", "sourceid:", "源id:"}
	for _, prefix := range prefixes {
		if !strings.HasPrefix(compact, prefix) {
			continue
		}
		id, err := strconv.ParseInt(strings.TrimPrefix(compact, prefix), 10, 64)
		if err == nil && id > 0 {
			return id, true
		}
	}
	return 0, false
}

// RateForRoute 计算路由有效倍率（对齐同步账号 rateMultiplierForAccount）。
//
// 优先级：
//  1. custom → RateConvertValue
//  2. 能匹配到源分组 → 用分组 ratio 换算（实时）
//  3. 已保存的 BillingRateMultiplier（列表「账号计费倍率」）→ 避免拉分组失败时回落成 1，导致尝试顺序与列表不一致
//  4. 最后回落 Convert(1, mode, …)
func RateForRoute(route *storage.GatewayRoute, groups []connector.APIKeyGroup) float64 {
	if route == nil {
		return 1
	}
	mode := rateconvert.NormalizeMode(route.RateConvertMode)
	if mode == "custom" {
		return rateconvert.Convert(1, mode, route.RateConvertValue)
	}
	sourceGroupName := strings.TrimSpace(route.SourceGroupName)
	groupID := route.SourceGroupID
	if (groupID == nil || *groupID <= 0) && sourceGroupName != "" {
		if id, ok := parseSourceGroupIDRef(sourceGroupName); ok {
			groupID = &id
		}
	}
	if groupID == nil && sourceGroupName == "" {
		if route.BillingRateMultiplier > 0 {
			return route.BillingRateMultiplier
		}
		return rateconvert.Convert(1, mode, route.RateConvertValue)
	}
	if groupID != nil {
		for _, g := range groups {
			if g.ID != nil && *g.ID == *groupID {
				return rateconvert.Convert(g.Ratio, mode, route.RateConvertValue)
			}
		}
	}
	if sourceGroupName != "" {
		for _, g := range groups {
			if !strings.EqualFold(strings.TrimSpace(g.Name), sourceGroupName) {
				continue
			}
			return rateconvert.Convert(g.Ratio, mode, route.RateConvertValue)
		}
	}
	// 源分组未匹配到：用保存时的账号计费倍率，保证列表序 = 尝试序
	if route.BillingRateMultiplier > 0 {
		return route.BillingRateMultiplier
	}
	return rateconvert.Convert(1, mode, route.RateConvertValue)
}

// IsRouteSchedulable 是否可参与调度。
// 直连 provider 密钥在 GatewayProvider 上，路由本身可不存 SourceAPIKeyCipher。
func IsRouteSchedulable(route *storage.GatewayRoute, now time.Time) bool {
	if route == nil || !route.Enabled {
		return false
	}
	if route.TempUnschedulableUntil != nil && route.TempUnschedulableUntil.After(now) {
		return false
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		return route.GatewayProviderID > 0
	}
	if strings.TrimSpace(route.SourceAPIKeyCipher) == "" {
		return false
	}
	return true
}

// routeRateLess 比较两条路由优先级（与同步账号 sortAccountsForApply 一致）。
// direction: asc 低倍率优先；desc 高倍率优先。
// 同倍率：权重大优先；再比 position；再比 id。
func routeRateLess(a, b storage.GatewayRoute, rateA, rateB float64, desc bool) bool {
	finiteA := !math.IsNaN(rateA) && !math.IsInf(rateA, 0)
	finiteB := !math.IsNaN(rateB) && !math.IsInf(rateB, 0)
	if finiteA != finiteB {
		return finiteA
	}
	if !finiteA {
		// Keep malformed rates deterministic without allowing NaN to make the
		// comparator claim that neither route is less than the other.
		if a.Weight != b.Weight {
			return a.Weight > b.Weight
		}
		if a.Position != b.Position {
			return a.Position < b.Position
		}
		return a.ID < b.ID
	}
	if rateA != rateB {
		if desc {
			return rateA > rateB
		}
		return rateA < rateB
	}
	if a.Weight != b.Weight {
		return a.Weight > b.Weight
	}
	if a.Position != b.Position {
		return a.Position < b.Position
	}
	return a.ID < b.ID
}

// OrderRoutesByRate 按倍率对全部路由重排（含禁用），用于列表展示与保存落库。
// 对齐上游同步：列表顺序 = 排序结果 = 尝试顺序。
func OrderRoutesByRate(routes []storage.GatewayRoute, groupsByChannel map[uint][]connector.APIKeyGroup, direction string) []storage.GatewayRoute {
	if len(routes) <= 1 {
		return routes
	}
	type scored struct {
		route storage.GatewayRoute
		rate  float64
		idx   int
	}
	items := make([]scored, len(routes))
	for i, r := range routes {
		cp := r
		groups := groupsByChannel[r.SourceChannelID]
		items[i] = scored{route: cp, rate: RateForRoute(&cp, groups), idx: i}
	}
	desc := strings.EqualFold(strings.TrimSpace(direction), "desc")
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].rate != items[j].rate || items[i].route.Weight != items[j].route.Weight ||
			items[i].route.Position != items[j].route.Position {
			return routeRateLess(items[i].route, items[j].route, items[i].rate, items[j].rate, desc)
		}
		return items[i].idx < items[j].idx
	})
	out := make([]storage.GatewayRoute, len(items))
	for i := range items {
		out[i] = items[i].route
		out[i].Position = i
	}
	return out
}

// SortRoutes 按倍率方向 + 权重 + position 排序（仅可调度路由，用于运行时 failover）。
// direction: asc 低倍率优先；desc 高倍率优先。
//
// BillingRate 与上游同步「账号计费倍率」一致：即 RateForRoute 换算结果
// （原值 / ×100 / ÷100 / 自定义），不再使用独立字段默认 1，避免计费失真。
func SortRoutes(routes []storage.GatewayRoute, groupsByChannel map[uint][]connector.APIKeyGroup, direction string, now time.Time, exclude map[uint]struct{}) []ScoredRoute {
	out := make([]ScoredRoute, 0, len(routes))
	for _, r := range routes {
		if exclude != nil {
			if _, ok := exclude[r.ID]; ok {
				continue
			}
		}
		cp := r
		if !IsRouteSchedulable(&cp, now) {
			continue
		}
		groups := groupsByChannel[r.SourceChannelID]
		rate := RateForRoute(&cp, groups)
		out = append(out, ScoredRoute{Route: cp, EffectiveRate: rate, BillingRate: rate})
	}
	desc := strings.EqualFold(strings.TrimSpace(direction), "desc")
	sort.SliceStable(out, func(i, j int) bool {
		return routeRateLess(out[i].Route, out[j].Route, out[i].EffectiveRate, out[j].EffectiveRate, desc)
	})
	return out
}
