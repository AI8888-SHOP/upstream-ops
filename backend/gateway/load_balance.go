package gateway

import (
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

const (
	maxLoadBalanceRouteCount = 64
	maxLoadBalancePoolStates = 2048
	loadBalanceStateTTL      = 24 * time.Hour
)

type loadBalancePoolKey struct {
	GroupID   uint
	Signature string
}

type loadBalancePoolState struct {
	next     atomic.Uint64
	lastUsed atomic.Int64
}

func normalizeLoadBalanceRouteCount(count int) int {
	if count < 1 {
		return 1
	}
	if count > maxLoadBalanceRouteCount {
		return maxLoadBalanceRouteCount
	}
	return count
}

// routeUpstreamConcurrencyKey deliberately uses the same physical-upstream
// identity as the concurrency limiter. Multiple routes for one monitored
// channel therefore consume only one load-balancing pool slot.
func routeUpstreamConcurrencyKey(route *storage.GatewayRoute) (upstreamConcurrencyKey, bool) {
	if route == nil {
		return upstreamConcurrencyKey{}, false
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if route.GatewayProviderID == 0 {
			return upstreamConcurrencyKey{}, false
		}
		return upstreamConcurrencyKey{Kind: upstreamConcurrencyKindProvider, ID: route.GatewayProviderID}, true
	}
	if route.SourceChannelID == 0 {
		return upstreamConcurrencyKey{}, false
	}
	return upstreamConcurrencyKey{Kind: upstreamConcurrencyKindMonitor, ID: route.SourceChannelID}, true
}

func loadBalanceIdentity(route *storage.GatewayRoute) upstreamConcurrencyKey {
	if key, ok := routeUpstreamConcurrencyKey(route); ok {
		return key
	}
	if route == nil {
		return upstreamConcurrencyKey{}
	}
	// Invalid legacy routes still need deterministic, independent identities so
	// one malformed row cannot collapse every fallback into a single pool slot.
	return upstreamConcurrencyKey{Kind: "route", ID: route.ID}
}

// orderLoadBalancedCandidates chooses one request primary from the highest
// ranked N physical upstreams. The remaining candidates retain their original
// order, so retry, response validation, hedge, and failover budgets are unchanged.
func (rt *Runtime) orderLoadBalancedCandidates(
	candidates []ScoredRoute,
	group *storage.GatewayGroup,
	affinity *routeAffinityContext,
) []ScoredRoute {
	if len(candidates) < 2 || group == nil {
		return candidates
	}

	// sortRoutesWithAffinity places a healthy preferred route or a controlled
	// cooldown recovery probe first. Session continuity always wins over RR.
	if affinity != nil && affinity.PreferredRouteID > 0 && candidates[0].Route.ID == affinity.PreferredRouteID {
		return candidates
	}

	poolLimit := normalizeLoadBalanceRouteCount(group.LoadBalanceRouteCount)
	if poolLimit <= 1 {
		return candidates
	}

	poolIndexes := make([]int, 0, minInt(poolLimit, len(candidates)))
	poolKeys := make([]upstreamConcurrencyKey, 0, cap(poolIndexes))
	seen := make(map[upstreamConcurrencyKey]struct{}, cap(poolIndexes))
	for index := range candidates {
		key := loadBalanceIdentity(&candidates[index].Route)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		poolIndexes = append(poolIndexes, index)
		poolKeys = append(poolKeys, key)
		if len(poolIndexes) == poolLimit {
			break
		}
	}
	if len(poolIndexes) < 2 || rt == nil || rt.Service == nil {
		return candidates
	}

	selectedPoolIndex := rt.nextLoadBalancePoolIndex(group.ID, poolKeys)
	selectedCandidateIndex := poolIndexes[selectedPoolIndex]
	if selectedCandidateIndex == 0 {
		return candidates
	}

	ordered := make([]ScoredRoute, 0, len(candidates))
	ordered = append(ordered, candidates[selectedCandidateIndex])
	ordered = append(ordered, candidates[:selectedCandidateIndex]...)
	ordered = append(ordered, candidates[selectedCandidateIndex+1:]...)
	return ordered
}

func (s *Service) nextLoadBalancePoolIndex(groupID uint, pool []upstreamConcurrencyKey) int {
	if s == nil || len(pool) < 2 {
		return 0
	}
	var signature strings.Builder
	for _, key := range pool {
		signature.WriteString(key.Kind)
		signature.WriteByte(':')
		signature.WriteString(strconv.FormatUint(uint64(key.ID), 10))
		signature.WriteByte(';')
	}
	poolKey := loadBalancePoolKey{GroupID: groupID, Signature: signature.String()}

	s.loadBalanceMu.RLock()
	state := s.loadBalanceStates[poolKey]
	s.loadBalanceMu.RUnlock()
	if state == nil {
		s.loadBalanceMu.Lock()
		if s.loadBalanceStates == nil {
			s.loadBalanceStates = make(map[loadBalancePoolKey]*loadBalancePoolState)
		}
		state = s.loadBalanceStates[poolKey]
		if state == nil {
			if len(s.loadBalanceStates) >= maxLoadBalancePoolStates {
				s.cleanupLoadBalanceStatesLocked(time.Now())
			}
			state = &loadBalancePoolState{}
			s.loadBalanceStates[poolKey] = state
		}
		s.loadBalanceMu.Unlock()
	}
	state.lastUsed.Store(time.Now().UnixNano())
	return int((state.next.Add(1) - 1) % uint64(len(pool)))
}

func (s *Service) cleanupLoadBalanceStatesLocked(now time.Time) {
	for key, state := range s.loadBalanceStates {
		lastUsed := time.Unix(0, state.lastUsed.Load())
		if lastUsed.Add(loadBalanceStateTTL).Before(now) {
			delete(s.loadBalanceStates, key)
		}
	}
	if len(s.loadBalanceStates) < maxLoadBalancePoolStates {
		return
	}
	for key := range s.loadBalanceStates {
		delete(s.loadBalanceStates, key)
		if len(s.loadBalanceStates) < maxLoadBalancePoolStates/2 {
			break
		}
	}
}

func (s *Service) resetLoadBalanceGroup(groupID uint) {
	if s == nil || groupID == 0 {
		return
	}
	s.loadBalanceMu.Lock()
	defer s.loadBalanceMu.Unlock()
	for key := range s.loadBalanceStates {
		if key.GroupID == groupID {
			delete(s.loadBalanceStates, key)
		}
	}
}
