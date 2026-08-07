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
	return IsRouteSchedulableForModel(route, "", now)
}

// IsRouteSchedulableForModel applies both the legacy route-wide pause and the
// automatic cooldown for the requested model. An empty model intentionally
// ignores model-specific cooldowns (for example, while listing /v1/models).
func IsRouteSchedulableForModel(route *storage.GatewayRoute, model string, now time.Time) bool {
	if route == nil || !route.Enabled || route.RateLimitAutoDisabled {
		return false
	}
	key := storage.NormalizeGatewayModel(model)
	// Legacy route-wide cooldowns have no model identity. Keep them for
	// model-less management/list requests, but never let them block a
	// model-specific request after the per-model table is available.
	if key == "" && route.TempUnschedulableUntil != nil && route.TempUnschedulableUntil.After(now) {
		return false
	}
	if key != "" {
		if cooldown, ok := route.ModelCooldowns[key]; ok && cooldown.TempUnschedulableUntil != nil && cooldown.TempUnschedulableUntil.After(now) {
			return false
		}
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
	return sortRoutesForModel(routes, groupsByChannel, direction, now, exclude, "")
}

// SortRoutesForModel sorts routes while filtering only the requested model's
// cooldown. This keeps other models on the same channel schedulable.
func SortRoutesForModel(routes []storage.GatewayRoute, groupsByChannel map[uint][]connector.APIKeyGroup, direction string, now time.Time, exclude map[uint]struct{}, model string) []ScoredRoute {
	return sortRoutesForModel(routes, groupsByChannel, direction, now, exclude, model)
}

func sortRoutesForModel(routes []storage.GatewayRoute, groupsByChannel map[uint][]connector.APIKeyGroup, direction string, now time.Time, exclude map[uint]struct{}, model string) []ScoredRoute {
	out := make([]ScoredRoute, 0, len(routes))
	for _, r := range routes {
		if exclude != nil {
			if _, ok := exclude[r.ID]; ok {
				continue
			}
		}
		cp := r
		if !IsRouteSchedulableForModel(&cp, model, now) {
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

const emergencyCooldownRecoveryRouteCount = 2

type emergencyCooldownRecoveryCandidate struct {
	index         int
	model         string
	failedAt      time.Time
	cooldownAt    *time.Time
	cooldownUntil time.Time
	requestID     string
	position      int
	routeID       uint
	identity      upstreamConcurrencyKey
	identityOK    bool
}

// recoverWhenAllRoutesCooling clears the oldest model cooldowns when a group
// would otherwise have no schedulable route. It is intentionally limited to
// routes that are otherwise valid and enabled, and wakes at most one route per
// physical upstream so duplicate route rows cannot amplify a recovery probe.
// The persisted cooldown is cleared before the request-local copy is made
// schedulable, so a restart does not immediately put the route back into the
// same dead-end state.
func (rt *Runtime) recoverWhenAllRoutesCooling(
	routes []storage.GatewayRoute,
	requestedModel string,
	groupMapping map[string]string,
	now time.Time,
) []storage.GatewayRoute {
	if rt == nil || len(routes) == 0 || strings.TrimSpace(requestedModel) == "" {
		return routes
	}
	if now.IsZero() {
		now = time.Now()
	}

	candidates := make([]emergencyCooldownRecoveryCandidate, 0, len(routes))
	eligible := 0
	for index := range routes {
		route := routes[index]
		upstreamModel, _ := ResolveModel(
			requestedModel,
			ParseModelMapping(route.ModelMappingJSON),
			groupMapping,
		)
		if strings.TrimSpace(upstreamModel) == "" {
			upstreamModel = requestedModel
		}
		withoutCooldown := route
		withoutCooldown.TempUnschedulableUntil = nil
		withoutCooldown.ModelCooldowns = nil
		if !IsRouteSchedulableForModel(&withoutCooldown, requestedModel, now) {
			continue
		}
		eligible++

		modelKey := storage.NormalizeGatewayModel(upstreamModel)
		if modelKey == "" {
			continue
		}
		cooldown, ok := route.ModelCooldowns[modelKey]
		if !ok || cooldown.TempUnschedulableUntil == nil || !cooldown.TempUnschedulableUntil.After(now) {
			// A healthy route means this is not an all-cooling outage. Return
			// immediately so the recovery check stays off the normal hot path.
			return routes
		}
		failedAt := cooldown.TempUnschedulableAt
		if failedAt == nil || failedAt.IsZero() {
			fallback := *cooldown.TempUnschedulableUntil
			failedAt = &fallback
		}
		var cooldownAt *time.Time
		if cooldown.TempUnschedulableAt != nil {
			at := *cooldown.TempUnschedulableAt
			cooldownAt = &at
		}
		identity, identityOK := routeUpstreamConcurrencyKey(&route)
		candidates = append(candidates, emergencyCooldownRecoveryCandidate{
			index: index, model: modelKey, failedAt: *failedAt,
			cooldownAt: cooldownAt, cooldownUntil: *cooldown.TempUnschedulableUntil,
			requestID: cooldown.TempUnschedulableRequestID,
			position:  route.Position, routeID: route.ID,
			identity: identity, identityOK: identityOK,
		})
	}

	// If even one otherwise-valid route is not cooling, there is no outage to
	// recover from. This also avoids waking a single model while another route
	// can still serve it normally.
	if eligible == 0 || len(candidates) != eligible {
		return routes
	}
	sort.SliceStable(candidates, func(i, j int) bool {
		if !candidates[i].failedAt.Equal(candidates[j].failedAt) {
			return candidates[i].failedAt.Before(candidates[j].failedAt)
		}
		if candidates[i].position != candidates[j].position {
			return candidates[i].position < candidates[j].position
		}
		return candidates[i].routeID < candidates[j].routeID
	})

	selected := make([]emergencyCooldownRecoveryCandidate, 0, emergencyCooldownRecoveryRouteCount)
	seen := make(map[upstreamConcurrencyKey]struct{}, emergencyCooldownRecoveryRouteCount)
	for _, candidate := range candidates {
		if len(selected) >= emergencyCooldownRecoveryRouteCount {
			break
		}
		if candidate.identityOK {
			if _, exists := seen[candidate.identity]; exists {
				continue
			}
			seen[candidate.identity] = struct{}{}
		}
		selected = append(selected, candidate)
	}
	if len(selected) == 0 {
		return routes
	}

	recovered := make([]storage.GatewayRoute, len(routes))
	copy(recovered, routes)
	for _, candidate := range selected {
		if rt.Routes != nil {
			cleared, err := rt.Routes.ClearModelTempUnschedulableUntilIfMatch(
				candidate.routeID, candidate.model, candidate.cooldownUntil,
				candidate.cooldownAt, candidate.requestID,
			)
			if err != nil {
				if rt.Log != nil {
					rt.Log.Warn("failed to recover cooled gateway route", "route_id", candidate.routeID, "model", candidate.model, "err", err)
				}
				continue
			}
			if !cleared {
				if rt.Log != nil {
					rt.Log.Debug("cooled gateway route changed before recovery", "route_id", candidate.routeID, "model", candidate.model)
				}
				continue
			}
		}
		cooldowns := cloneModelCooldowns(recovered[candidate.index].ModelCooldowns)
		for key, cooldown := range cooldowns {
			// bindModelCooldownAliases adds request-local aliases that point to
			// the same persisted upstream cooldown. Clear those views as well,
			// otherwise the scheduler would immediately filter the recovered
			// route again for the requested model.
			if key != candidate.model &&
				(cooldown.RouteID != candidate.routeID || storage.NormalizeGatewayModel(cooldown.Model) != candidate.model) {
				continue
			}
			cooldown.TempUnschedulableUntil = nil
			cooldowns[key] = cooldown
		}
		recovered[candidate.index].ModelCooldowns = cooldowns
		if rt.Log != nil {
			rt.Log.Info("emergency gateway cooldown recovery", "route_id", candidate.routeID, "model", candidate.model)
		}
	}
	return recovered
}

func cloneModelCooldowns(source map[string]storage.GatewayRouteModelCooldown) map[string]storage.GatewayRouteModelCooldown {
	if len(source) == 0 {
		return nil
	}
	out := make(map[string]storage.GatewayRouteModelCooldown, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

// sortRoutesWithAffinity preserves the normal scheduler order for unrelated
// requests. A matching session may bypass a live cooldown exactly once so the
// upstream prompt cache can be reactivated; all other requests still see the
// cooled route as unschedulable.
func (rt *Runtime) sortRoutesWithAffinity(
	routes []storage.GatewayRoute,
	groupsByChannel map[uint][]connector.APIKeyGroup,
	direction string,
	now time.Time,
	exclude map[uint]struct{},
	affinity *routeAffinityContext,
	models ...string,
) []ScoredRoute {
	model := ""
	if len(models) > 0 {
		model = models[0]
	}
	normal := SortRoutesForModel(routes, groupsByChannel, direction, now, exclude, model)
	if rt == nil || affinity == nil || affinity.PreferredRouteID == 0 || affinity.LookupKey.Fingerprint == "" {
		return normal
	}
	if exclude != nil {
		if _, excluded := exclude[affinity.PreferredRouteID]; excluded {
			return normal
		}
	}
	for index := range normal {
		if normal[index].Route.ID != affinity.PreferredRouteID {
			continue
		}
		if index == 0 {
			return normal
		}
		prioritized := make([]ScoredRoute, 0, len(normal))
		prioritized = append(prioritized, normal[index])
		prioritized = append(prioritized, normal[:index]...)
		prioritized = append(prioritized, normal[index+1:]...)
		return prioritized
	}
	for _, route := range routes {
		if route.ID != affinity.PreferredRouteID {
			continue
		}
		cooldownUntil := routeModelCooldownUntil(&route, model, now)
		if cooldownUntil == nil {
			return normal
		}
		candidate := route
		candidate.TempUnschedulableUntil = nil
		if key := storage.NormalizeGatewayModel(model); key != "" && len(candidate.ModelCooldowns) > 0 {
			candidate.ModelCooldowns = cloneModelCooldowns(candidate.ModelCooldowns)
			if cooldown, ok := candidate.ModelCooldowns[key]; ok {
				// Keep the observed failure generation on the request-local
				// recovery copy. A later success uses it as a CAS guard so an
				// older probe cannot clear a newer concurrent cooldown.
				cooldown.TempUnschedulableUntil = nil
				candidate.ModelCooldowns[key] = cooldown
			}
		}
		if !IsRouteSchedulableForModel(&candidate, model, now) {
			return normal
		}
		affinity.PreservePreferred = true
		if !rt.claimRouteAffinityProbe(affinity.LookupKey, route.ID, now) {
			return normal
		}
		affinity.RecoveryCooldownUntil = *cooldownUntil
		affinity.RecoveryRouteID = route.ID
		affinity.Recovery = true
		groups := groupsByChannel[route.SourceChannelID]
		rate := RateForRoute(&candidate, groups)
		return append([]ScoredRoute{{Route: candidate, EffectiveRate: rate, BillingRate: rate}}, normal...)
	}
	return normal
}

func routeModelCooldownUntil(route *storage.GatewayRoute, model string, now time.Time) *time.Time {
	if route == nil {
		return nil
	}
	key := storage.NormalizeGatewayModel(model)
	if key == "" {
		if route.TempUnschedulableUntil != nil && route.TempUnschedulableUntil.After(now) {
			until := *route.TempUnschedulableUntil
			return &until
		}
		return nil
	}
	if cooldown, ok := route.ModelCooldowns[key]; ok && cooldown.TempUnschedulableUntil != nil && cooldown.TempUnschedulableUntil.After(now) {
		until := *cooldown.TempUnschedulableUntil
		return &until
	}
	return nil
}
