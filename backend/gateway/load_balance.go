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

// routeSelectionKey identifies one independently schedulable source. A
// monitored channel may expose multiple source groups, each with its own API
// key and gateway route. Those groups must occupy separate load-balancing and
// emergency-recovery slots even though the concurrency limiter remains shared
// by the physical channel.
type routeSelectionKey struct {
	Kind     string
	ID       uint
	GroupRef string
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

// routeSelectionIdentity returns the source-group-aware identity used by route
// ordering and bounded emergency recovery. It is intentionally separate from
// upstreamConcurrencyKey: all groups on one physical channel still share the
// channel's configured concurrency limit, while failures/recovery are scoped
// to one channel group.
func routeSelectionIdentity(route *storage.GatewayRoute) (routeSelectionKey, bool) {
	if route == nil {
		return routeSelectionKey{}, false
	}
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if route.GatewayProviderID == 0 {
			return routeSelectionKey{}, false
		}
		return routeSelectionKey{Kind: upstreamConcurrencyKindProvider, ID: route.GatewayProviderID}, true
	}
	if route.SourceChannelID == 0 {
		return routeSelectionKey{}, false
	}

	groupRef := "default"
	if route.SourceGroupID != nil && *route.SourceGroupID > 0 {
		groupRef = "id:" + strconv.FormatInt(*route.SourceGroupID, 10)
	} else if id, ok := parseSourceGroupIDRef(route.SourceGroupName); ok {
		groupRef = "id:" + strconv.FormatInt(id, 10)
	} else if name := strings.TrimSpace(route.SourceGroupName); name != "" {
		groupRef = "name:" + strings.ToLower(name)
	}
	return routeSelectionKey{Kind: upstreamConcurrencyKindMonitor, ID: route.SourceChannelID, GroupRef: groupRef}, true
}

func loadBalanceIdentity(route *storage.GatewayRoute) routeSelectionKey {
	if key, ok := routeSelectionIdentity(route); ok {
		return key
	}
	if route == nil {
		return routeSelectionKey{}
	}
	// Invalid legacy routes still need deterministic, independent identities so
	// one malformed row cannot collapse every fallback into a single pool slot.
	return routeSelectionKey{Kind: "route", ID: route.ID}
}

// orderLoadBalancedCandidates chooses one request primary from the highest
// ranked N independently schedulable source groups. The remaining candidates
// retain their original order, so retry, response validation, hedge, and
// failover budgets are unchanged.
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
	poolKeys := make([]routeSelectionKey, 0, cap(poolIndexes))
	seen := make(map[routeSelectionKey]struct{}, cap(poolIndexes))
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

func (s *Service) nextLoadBalancePoolIndex(groupID uint, pool []routeSelectionKey) int {
	if s == nil || len(pool) < 2 {
		return 0
	}
	var signature strings.Builder
	for _, key := range pool {
		signature.WriteString(key.Kind)
		signature.WriteByte(':')
		signature.WriteString(strconv.FormatUint(uint64(key.ID), 10))
		signature.WriteByte(':')
		signature.WriteString(key.GroupRef)
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
