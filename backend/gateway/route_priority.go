package gateway

import "sort"

func routePriorityLess(a, b int) bool {
	if a == b {
		return false
	}
	if a <= 0 {
		return false
	}
	if b <= 0 {
		return true
	}
	return a < b
}

// Apply explicit cost tiers after adaptive/cache/affinity ordering. Eligibility,
// cooldowns and price ceilings are still enforced by the normal filters. Keep
// fallback-only routes behind normal routes; preserve scheduler order within a
// tier. With no explicit priority this is a no-op (including legacy affinity).
func (rt *Runtime) orderExplicitRoutePriorities(candidates []ScoredRoute, recoveryRouteID uint) []ScoredRoute {
	hasPriority := false
	for _, c := range candidates {
		if c.Route.Priority > 0 {
			hasPriority = true
			break
		}
	}
	if !hasPriority {
		return candidates
	}
	out := append([]ScoredRoute(nil), candidates...)
	// A claimed half-open recovery probe must remain first so the request's
	// probe lifecycle and single-attempt budget remain intact.
	start := 0
	if len(out) > 0 && recoveryRouteID != 0 && out[0].Route.ID == recoveryRouteID {
		start = 1
	}
	tail := out[start:]
	saturated := make(map[uint]bool, len(tail))
	for _, c := range tail {
		saturated[c.Route.ID] = rt.cachePrioritySaturated(c)
	}
	sort.SliceStable(tail, func(i, j int) bool {
		a, b := tail[i].Route, tail[j].Route
		if a.FallbackOnly != b.FallbackOnly {
			return !a.FallbackOnly
		}
		// Cost preference must not pile requests on a fully occupied source.
		if saturated[a.ID] != saturated[b.ID] {
			return !saturated[a.ID]
		}
		return routePriorityLess(a.Priority, b.Priority)
	})
	return out
}
