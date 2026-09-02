package gateway

import (
	"fmt"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

// cacheHealthSourceKey identifies one independently schedulable route. The
// physical source and gateway group remain part of the key for validation and
// compatibility with legacy route_id=0 state.
type cacheHealthSourceKey struct {
	kind    string
	id      uint
	groupID uint
	routeID uint
}

type cacheHealthPolicy struct {
	WindowMinutes    int
	ThresholdPercent float64
	BlacklistMinutes int
	MinimumRequests  int
}

// CacheHealthStat is the admin-facing rolling cache summary plus persisted
// automatic blacklist state.
type CacheHealthStat struct {
	GatewayGroupID      uint       `json:"gateway_group_id"`
	RouteID             uint       `json:"route_id"`
	SourceKind          string     `json:"source_kind"`
	SourceID            uint       `json:"source_id"`
	HitRate             float64    `json:"hit_rate"`
	RequestCount        int64      `json:"request_count"`
	InputTokens         int64      `json:"input_tokens"`
	CacheReadTokens     int64      `json:"cache_read_tokens"`
	CacheCreationTokens int64      `json:"cache_creation_tokens"`
	WindowStart         time.Time  `json:"window_start"`
	EvaluatedAt         *time.Time `json:"evaluated_at,omitempty"`
	BlacklistedUntil    *time.Time `json:"blacklisted_until,omitempty"`
	BlacklistReason     string     `json:"blacklist_reason,omitempty"`
	ManualClearUntil    *time.Time `json:"manual_clear_until,omitempty"`
}

func normalizeCacheHealthKind(kind string) string {
	if strings.EqualFold(strings.TrimSpace(kind), storage.GatewayRouteSourceProvider) {
		return storage.GatewayRouteSourceProvider
	}
	return storage.GatewayRouteSourceMonitor
}

func resolveCacheHealthPolicy(cfg config.GatewayConfig, group *storage.GatewayGroup) cacheHealthPolicy {
	cfg = cfg.WithDefaults()
	policy := cacheHealthPolicy{
		WindowMinutes:    cfg.CacheHitRateWindowMinutes,
		ThresholdPercent: cfg.CacheHitRateThresholdPercent,
		BlacklistMinutes: cfg.CacheHitRateBlacklistMinutes,
		MinimumRequests:  cfg.CacheHitRateMinimumRequests,
	}
	if group != nil {
		if group.CacheHitRateWindowMinutes != nil {
			policy.WindowMinutes = *group.CacheHitRateWindowMinutes
		}
		if group.CacheHitRateThresholdPercent != nil {
			policy.ThresholdPercent = *group.CacheHitRateThresholdPercent
		}
		if group.CacheHitRateBlacklistMinutes != nil {
			policy.BlacklistMinutes = *group.CacheHitRateBlacklistMinutes
		}
		if group.CacheHitRateMinimumRequests != nil {
			policy.MinimumRequests = *group.CacheHitRateMinimumRequests
		}
	}
	if policy.WindowMinutes < 0 {
		policy.WindowMinutes = 0
	} else if policy.WindowMinutes > 7*24*60 {
		policy.WindowMinutes = 7 * 24 * 60
	}
	if policy.ThresholdPercent < 0 {
		policy.ThresholdPercent = 0
	} else if policy.ThresholdPercent > 100 {
		policy.ThresholdPercent = 100
	}
	if policy.BlacklistMinutes < 0 {
		policy.BlacklistMinutes = 0
	} else if policy.BlacklistMinutes > 30*24*60 {
		policy.BlacklistMinutes = 30 * 24 * 60
	}
	if policy.MinimumRequests < config.MinGatewayCacheHitRateMinimumRequests {
		policy.MinimumRequests = config.MinGatewayCacheHitRateMinimumRequests
	} else if policy.MinimumRequests > config.MaxGatewayCacheHitRateMinimumRequests {
		policy.MinimumRequests = config.MaxGatewayCacheHitRateMinimumRequests
	}
	return policy
}

func cacheHealthProtectionEnabled(policy cacheHealthPolicy) bool {
	return policy.WindowMinutes > 0 &&
		policy.ThresholdPercent > 0 &&
		policy.BlacklistMinutes > 0
}

func cacheHealthWindowMinutes(policy cacheHealthPolicy) int {
	if policy.WindowMinutes > 0 {
		return policy.WindowMinutes
	}
	// Statistics remain useful even when automatic protection is disabled.
	return 60
}

func (s *Service) cacheHealthPolicyForGroup(groupID uint) (cacheHealthPolicy, error) {
	if s == nil {
		return resolveCacheHealthPolicy(config.GatewayConfig{}, nil), nil
	}
	global := s.gatewayRuntime()
	if groupID == 0 || s.Groups == nil {
		return resolveCacheHealthPolicy(global, nil), nil
	}
	group, err := s.Groups.FindByID(groupID)
	if err != nil {
		return cacheHealthPolicy{}, err
	}
	return resolveCacheHealthPolicy(global, group), nil
}

func (s *Service) resetCacheHealthForGroup(groupID uint) {
	if s == nil {
		return
	}
	if s.Usage != nil {
		if err := s.Usage.ClearCacheHealthStateForGroup(groupID); err != nil && s.Log != nil {
			s.Log.Warn("reset gateway cache health state failed", "gateway_group_id", groupID, "err", err)
		}
	}
	if s.Routes != nil {
		s.Routes.InvalidateCacheHealthForGroup(groupID)
	}
}

func (s *Service) cacheHealthGroups() ([]storage.GatewayGroup, error) {
	if s == nil || s.Groups == nil {
		return nil, nil
	}
	return s.Groups.List()
}

func (s *Service) reconcileInitialCacheHealthPolicies(cfg config.GatewayConfig) {
	if s == nil || s.Usage == nil {
		return
	}
	groups, err := s.cacheHealthGroups()
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("load gateway groups for cache health reconciliation failed", "err", err)
		}
		return
	}
	s.reconcileInitialCacheHealthPolicy(0, resolveCacheHealthPolicy(cfg, nil))
	for i := range groups {
		s.reconcileInitialCacheHealthPolicy(groups[i].ID, resolveCacheHealthPolicy(cfg, &groups[i]))
	}
}

func (s *Service) reconcileInitialCacheHealthPolicy(groupID uint, policy cacheHealthPolicy) {
	if !cacheHealthProtectionEnabled(policy) {
		s.resetCacheHealthForGroup(groupID)
		return
	}
	cleared, err := s.Usage.ClearCacheHealthBlacklistsBelowMinimumForGroup(groupID, int64(policy.MinimumRequests))
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("reconcile gateway cache health warm-up failed", "gateway_group_id", groupID, "err", err)
		}
		return
	}
	if cleared > 0 && s.Routes != nil {
		s.Routes.InvalidateCacheHealthForGroup(groupID)
	}
}

func (s *Service) resetChangedCacheHealthPolicies(previous, next config.GatewayConfig) {
	if s == nil || s.Usage == nil {
		return
	}
	groups, err := s.cacheHealthGroups()
	if err != nil {
		if s.Log != nil {
			s.Log.Warn("load gateway groups for cache health update failed", "err", err)
		}
		return
	}
	if resolveCacheHealthPolicy(previous, nil) != resolveCacheHealthPolicy(next, nil) {
		s.resetCacheHealthForGroup(0)
	}
	for i := range groups {
		before := resolveCacheHealthPolicy(previous, &groups[i])
		after := resolveCacheHealthPolicy(next, &groups[i])
		if before != after {
			s.resetCacheHealthForGroup(groups[i].ID)
		}
	}
}

// CacheHealthStats returns current rolling statistics for one source kind.
// sourceIDs may be empty to include every source represented in logs/state.
func (s *Service) CacheHealthStats(sourceKind string, sourceIDs []uint) ([]CacheHealthStat, error) {
	return s.cacheHealthStats(sourceKind, sourceIDs, 0)
}

// CacheHealthStatsForGroup returns cache health scoped to one gateway group.
func (s *Service) CacheHealthStatsForGroup(sourceKind string, sourceIDs []uint, groupID uint) ([]CacheHealthStat, error) {
	return s.cacheHealthStats(sourceKind, sourceIDs, groupID)
}

// CacheHealthStatsForRoute returns the rolling state for one concrete route.
func (s *Service) CacheHealthStatsForRoute(sourceKind string, sourceID, groupID, routeID uint) ([]CacheHealthStat, error) {
	if s == nil || s.Usage == nil {
		return nil, fmt.Errorf("usage storage is unavailable")
	}
	if sourceID == 0 || groupID == 0 || routeID == 0 {
		return nil, fmt.Errorf("cache health source, gateway group, and route are required")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	policy, err := s.cacheHealthPolicyForGroup(groupID)
	if err != nil {
		return nil, err
	}
	from := time.Now().Add(-time.Duration(cacheHealthWindowMinutes(policy)) * time.Minute)
	agg, err := s.Usage.CacheHealthAggregateForRoute(kind, sourceID, groupID, routeID, from)
	if err != nil {
		return nil, err
	}
	states, err := s.Usage.CacheHealthStatesForRoute(kind, sourceID, groupID, routeID)
	if err != nil {
		return nil, err
	}
	item := CacheHealthStat{
		GatewayGroupID: groupID, RouteID: routeID, SourceKind: kind, SourceID: sourceID,
		HitRate: agg.HitRate, RequestCount: agg.RequestCount, InputTokens: agg.InputTokens,
		CacheReadTokens: agg.CacheReadTokens, CacheCreationTokens: agg.CacheCreationTokens,
		WindowStart: agg.WindowStart,
	}
	if len(states) > 0 {
		state := states[0]
		item.EvaluatedAt = cloneTimePointer(state.EvaluatedAt)
		item.BlacklistedUntil = cloneTimePointer(state.BlacklistedUntil)
		item.BlacklistReason = state.BlacklistReason
		item.ManualClearUntil = cloneTimePointer(state.ManualClearUntil)
		if state.WindowStart != nil && !state.WindowStart.IsZero() {
			item.WindowStart = *state.WindowStart
		}
	}
	return []CacheHealthStat{item}, nil
}

func (s *Service) cacheHealthStats(sourceKind string, sourceIDs []uint, groupID uint) ([]CacheHealthStat, error) {
	if s == nil || s.Usage == nil {
		return nil, fmt.Errorf("usage storage is unavailable")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	policy, err := s.cacheHealthPolicyForGroup(groupID)
	if err != nil {
		return nil, err
	}
	from := time.Now().Add(-time.Duration(cacheHealthWindowMinutes(policy)) * time.Minute)
	var aggs []storage.GatewayCacheHealthAggregate
	if groupID > 0 {
		aggs, err = s.Usage.CacheHealthAggregatesForGroup(kind, sourceIDs, groupID, from)
	} else {
		aggs, err = s.Usage.CacheHealthAggregates(kind, sourceIDs, from)
	}
	if err != nil {
		return nil, err
	}
	var states []storage.GatewayChannelCacheHealth
	if groupID > 0 {
		states, err = s.Usage.CacheHealthStatesForGroup(kind, sourceIDs, groupID)
	} else {
		states, err = s.Usage.CacheHealthStates(kind, sourceIDs)
	}
	if err != nil {
		return nil, err
	}
	type statKey struct {
		sourceID uint
		routeID  uint
	}
	keyFor := func(sourceID, routeID uint) statKey {
		if groupID == 0 {
			routeID = 0
		}
		return statKey{sourceID: sourceID, routeID: routeID}
	}
	byKey := make(map[statKey]CacheHealthStat, len(aggs)+len(states))
	for _, agg := range aggs {
		key := keyFor(agg.SourceID, agg.RouteID)
		byKey[key] = CacheHealthStat{
			GatewayGroupID: agg.GatewayGroupID, RouteID: key.routeID, SourceKind: agg.SourceKind, SourceID: agg.SourceID,
			HitRate: agg.HitRate, RequestCount: agg.RequestCount,
			InputTokens: agg.InputTokens, CacheReadTokens: agg.CacheReadTokens,
			CacheCreationTokens: agg.CacheCreationTokens, WindowStart: agg.WindowStart,
		}
	}
	for _, state := range states {
		key := keyFor(state.SourceID, state.RouteID)
		item, ok := byKey[key]
		if !ok {
			windowStart := from
			if state.WindowStart != nil && !state.WindowStart.IsZero() {
				windowStart = *state.WindowStart
			}
			item = CacheHealthStat{
				GatewayGroupID: state.GatewayGroupID, RouteID: key.routeID, SourceKind: kind, SourceID: state.SourceID,
				HitRate: state.HitRate, RequestCount: state.RequestCount,
				InputTokens: state.InputTokens, CacheReadTokens: state.CacheReadTokens,
				CacheCreationTokens: state.CacheCreationTokens,
				WindowStart:         windowStart,
			}
			if groupID == 0 {
				item.GatewayGroupID = 0
			}
		}
		if groupID > 0 {
			// The live aggregate uses this group's effective window. Persisted
			// evaluator rows contribute restriction metadata, and provide counters
			// only when no matching usage remains in the current window.
			item.GatewayGroupID = state.GatewayGroupID
			item.RouteID = state.RouteID
			item.EvaluatedAt = cloneTimePointer(state.EvaluatedAt)
			item.BlacklistedUntil = cloneTimePointer(state.BlacklistedUntil)
			item.BlacklistReason = state.BlacklistReason
			item.ManualClearUntil = cloneTimePointer(state.ManualClearUntil)
		} else {
			// The provider-management endpoint is intentionally global. Its
			// rolling counters come from the all-group aggregate above; when
			// several groups have state rows, merge only deterministic metadata
			// instead of letting database row order pick one group's counters.
			if state.EvaluatedAt != nil && (item.EvaluatedAt == nil || state.EvaluatedAt.After(*item.EvaluatedAt)) {
				item.EvaluatedAt = cloneTimePointer(state.EvaluatedAt)
				// Manual suppression belongs to the most recently evaluated
				// group; a newer evaluation with no suppression must clear an
				// older group's stale value from the global response.
				item.ManualClearUntil = cloneTimePointer(state.ManualClearUntil)
			}
			if state.BlacklistedUntil != nil && (item.BlacklistedUntil == nil || state.BlacklistedUntil.After(*item.BlacklistedUntil)) {
				item.BlacklistedUntil = cloneTimePointer(state.BlacklistedUntil)
				item.BlacklistReason = state.BlacklistReason
			}
			if state.EvaluatedAt == nil && state.ManualClearUntil != nil && (item.ManualClearUntil == nil || state.ManualClearUntil.After(*item.ManualClearUntil)) {
				item.ManualClearUntil = cloneTimePointer(state.ManualClearUntil)
			}
		}
		byKey[key] = item
	}
	if len(sourceIDs) > 0 {
		// Keep the response stable for provider/channel lists, including sources
		// that have no usage in the current window.
		for _, id := range sourceIDs {
			if id == 0 {
				continue
			}
			represented := false
			for key := range byKey {
				if key.sourceID == id {
					represented = true
					break
				}
			}
			if !represented {
				key := keyFor(id, 0)
				byKey[key] = CacheHealthStat{GatewayGroupID: groupID, SourceKind: kind, SourceID: id, WindowStart: from}
			}
		}
	}
	out := make([]CacheHealthStat, 0, len(byKey))
	for _, item := range byKey {
		out = append(out, item)
	}
	// Deterministic order keeps the API/UI stable and makes tests simple.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && (out[j].SourceID < out[j-1].SourceID ||
			(out[j].SourceID == out[j-1].SourceID && out[j].RouteID < out[j-1].RouteID)); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, nil
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

// scheduleCacheHealthEvaluation asynchronously evaluates a source after a
// usage write. A short debounce is enough to collapse bursts while keeping the
// rolling state fresh; the request itself never waits on aggregate SQL.
func (s *Service) scheduleCacheHealthEvaluation(sourceKind string, sourceID uint, group *storage.GatewayGroup, routeID uint) {
	if s == nil || sourceID == 0 || group == nil || s.Usage == nil || s.Routes == nil {
		return
	}
	policy := resolveCacheHealthPolicy(s.gatewayRuntime(), group)
	if !cacheHealthProtectionEnabled(policy) {
		return
	}
	key := cacheHealthSourceKey{kind: normalizeCacheHealthKind(sourceKind), id: sourceID, groupID: group.ID, routeID: routeID}
	now := time.Now()
	s.cacheHealthMu.Lock()
	if s.cacheHealthPending == nil {
		s.cacheHealthPending = make(map[cacheHealthSourceKey]time.Time)
	}
	const debounce = 15 * time.Second
	if previous, ok := s.cacheHealthPending[key]; ok && now.Sub(previous) < debounce {
		s.cacheHealthMu.Unlock()
		return
	}
	s.cacheHealthPending[key] = now
	s.cacheHealthMu.Unlock()
	scheduledAt := now
	time.AfterFunc(500*time.Millisecond, func() {
		if err := s.EvaluateCacheHealthForRoute(key.kind, key.id, key.groupID, key.routeID, time.Now()); err != nil && s.Log != nil {
			s.Log.Warn("evaluate gateway cache health failed", "source_kind", key.kind, "source_id", key.id, "gateway_group_id", key.groupID, "route_id", key.routeID, "err", err)
		}
		// Keep the timestamp for the full debounce interval. The old code
		// removed it after the 500ms timer, allowing every subsequent usage
		// write to run another aggregate query almost immediately.
		time.AfterFunc(debounce, func() {
			s.cacheHealthMu.Lock()
			if pending, ok := s.cacheHealthPending[key]; ok && pending.Equal(scheduledAt) {
				delete(s.cacheHealthPending, key)
			}
			s.cacheHealthMu.Unlock()
		})
	})
}

// EvaluateCacheHealth evaluates and persists one source immediately. It is
// exported for deterministic administration/tests; normal traffic uses the
// debounced scheduler above.
func (s *Service) EvaluateCacheHealth(sourceKind string, sourceID uint, now time.Time) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}
	// Discover groups across the longest supported policy window. Each group is
	// then evaluated with its own effective window and threshold.
	from := now.Add(-7 * 24 * time.Hour)
	groups, err := s.Usage.CacheHealthGroupIDs(sourceKind, sourceID, from)
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		if !cacheHealthProtectionEnabled(resolveCacheHealthPolicy(s.gatewayRuntime(), nil)) {
			return nil
		}
		return s.evaluateCacheHealthForRoute(sourceKind, sourceID, 0, 0, now)
	}
	for _, groupID := range groups {
		if err := s.EvaluateCacheHealthForGroup(sourceKind, sourceID, groupID, now); err != nil {
			return err
		}
	}
	return nil
}

// EvaluateCacheHealthForGroup evaluates every recent route for one source in
// a gateway group. It remains available for administration and old callers;
// runtime traffic uses the route-specific method below.
func (s *Service) EvaluateCacheHealthForGroup(sourceKind string, sourceID, groupID uint, now time.Time) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if now.IsZero() {
		now = time.Now()
	}
	policy, err := s.cacheHealthPolicyForGroup(groupID)
	if err != nil {
		return err
	}
	if !cacheHealthProtectionEnabled(policy) {
		return nil
	}
	from := now.Add(-time.Duration(cacheHealthWindowMinutes(policy)) * time.Minute)
	routeIDs, err := s.Usage.CacheHealthRouteIDsForGroup(sourceKind, sourceID, groupID, from)
	if err != nil {
		return err
	}
	if len(routeIDs) == 0 {
		return s.evaluateCacheHealthForRoute(sourceKind, sourceID, groupID, 0, now)
	}
	for _, routeID := range routeIDs {
		if err := s.evaluateCacheHealthForRoute(sourceKind, sourceID, groupID, routeID, now); err != nil {
			return err
		}
	}
	return nil
}

// EvaluateCacheHealthForRoute evaluates and persists one independently
// schedulable source group.
func (s *Service) EvaluateCacheHealthForRoute(sourceKind string, sourceID, groupID, routeID uint, now time.Time) error {
	return s.evaluateCacheHealthForRoute(sourceKind, sourceID, groupID, routeID, now)
}

func (s *Service) evaluateCacheHealthForRoute(sourceKind string, sourceID, groupID, routeID uint, now time.Time) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if sourceID == 0 {
		return fmt.Errorf("cache health source id is required")
	}
	if now.IsZero() {
		now = time.Now()
	}
	kind := normalizeCacheHealthKind(sourceKind)
	policy, err := s.cacheHealthPolicyForGroup(groupID)
	if err != nil {
		return err
	}
	if !cacheHealthProtectionEnabled(policy) {
		return nil
	}
	if s.Routes == nil {
		return fmt.Errorf("cache health route storage is unavailable")
	}
	from := now.Add(-time.Duration(policy.WindowMinutes) * time.Minute)
	agg, err := s.Usage.CacheHealthAggregateForRoute(kind, sourceID, groupID, routeID, from)
	if err != nil {
		return err
	}
	states, err := s.Usage.CacheHealthStatesForRoute(kind, sourceID, groupID, routeID)
	if err != nil {
		return err
	}
	state := storage.GatewayChannelCacheHealth{
		GatewayGroupID: groupID, RouteID: routeID, SourceKind: kind, SourceID: sourceID,
		HitRate: agg.HitRate, RequestCount: agg.RequestCount,
		InputTokens: agg.InputTokens, CacheReadTokens: agg.CacheReadTokens,
		CacheCreationTokens: agg.CacheCreationTokens,
		WindowStart:         &agg.WindowStart,
		EvaluatedAt:         &now,
	}
	if len(states) > 0 {
		previous := states[0]
		state.ID = previous.ID
		state.BlacklistedUntil = cloneTimePointer(previous.BlacklistedUntil)
		state.BlacklistReason = previous.BlacklistReason
		state.ManualClearUntil = cloneTimePointer(previous.ManualClearUntil)
	}
	denominator := agg.InputTokens + agg.CacheReadTokens + agg.CacheCreationTokens
	low := agg.RequestCount >= int64(policy.MinimumRequests) && denominator > 0 &&
		agg.HitRate < policy.ThresholdPercent
	active := state.BlacklistedUntil != nil && state.BlacklistedUntil.After(now)
	manualClearActive := state.ManualClearUntil != nil && state.ManualClearUntil.After(now)
	if manualClearActive {
		// A manual release is a deliberate warm-up period. Keep the rolling
		// counters for visibility, but never reapply the same restriction until
		// the grace period has elapsed.
		state.BlacklistedUntil = nil
		state.BlacklistReason = ""
	} else {
		// Do not retain an expired marker forever; otherwise an old manual
		// release would keep appearing in the admin state.
		state.ManualClearUntil = nil
	}
	if !manualClearActive && low && !active {
		until := now.Add(time.Duration(policy.BlacklistMinutes) * time.Minute)
		state.BlacklistedUntil = &until
		state.BlacklistReason = fmt.Sprintf(
			"缓存命中率 %.2f%% 低于阈值 %.2f%%（最近 %d 分钟）",
			agg.HitRate, policy.ThresholdPercent, policy.WindowMinutes,
		)
	} else if !manualClearActive && !active && !low {
		state.BlacklistedUntil = nil
		state.BlacklistReason = ""
	}
	if err := s.Usage.UpsertCacheHealth(&state); err != nil {
		return err
	}
	s.Routes.InvalidateCacheHealthRoute(kind, sourceID, groupID, routeID)
	return nil
}

// CacheHealthEnabled reports whether automatic protection is active globally
// or for at least one gateway group.
func (s *Service) CacheHealthEnabled() bool {
	if s == nil {
		return false
	}
	global := s.gatewayRuntime()
	if cacheHealthProtectionEnabled(resolveCacheHealthPolicy(global, nil)) {
		return true
	}
	if s.Groups == nil {
		return false
	}
	groups, err := s.Groups.List()
	if err != nil {
		return false
	}
	for i := range groups {
		if cacheHealthProtectionEnabled(resolveCacheHealthPolicy(global, &groups[i])) {
			return true
		}
	}
	return false
}

// ClearCacheHealthBlacklist releases one source immediately without changing
// its rolling cache statistics or the global protection configuration.
func (s *Service) ClearCacheHealthBlacklist(sourceKind string, sourceID uint) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if sourceID == 0 {
		return fmt.Errorf("cache health source id is required")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	var err error
	maxWindow := 0
	states, stateErr := s.Usage.CacheHealthStates(kind, []uint{sourceID})
	if stateErr != nil {
		return stateErr
	}
	seenGroups := make(map[uint]struct{}, len(states))
	for _, state := range states {
		if _, exists := seenGroups[state.GatewayGroupID]; exists {
			continue
		}
		seenGroups[state.GatewayGroupID] = struct{}{}
		policy, policyErr := s.cacheHealthPolicyForGroup(state.GatewayGroupID)
		if policyErr != nil {
			return policyErr
		}
		if cacheHealthProtectionEnabled(policy) && policy.WindowMinutes > maxWindow {
			maxWindow = policy.WindowMinutes
		}
	}
	if len(states) == 0 {
		policy := resolveCacheHealthPolicy(s.gatewayRuntime(), nil)
		if cacheHealthProtectionEnabled(policy) {
			maxWindow = policy.WindowMinutes
		}
	}
	if maxWindow > 0 {
		graceUntil := time.Now().Add(time.Duration(maxWindow) * time.Minute)
		err = s.Usage.ClearCacheHealthBlacklistWithSuppression(kind, sourceID, graceUntil)
	} else {
		err = s.Usage.ClearCacheHealthBlacklist(kind, sourceID)
	}
	if err != nil {
		return err
	}
	if s.Routes != nil {
		s.Routes.InvalidateCacheHealthSource(kind, sourceID)
	}
	return nil
}

// ClearCacheHealthBlacklistForGroup releases only the selected group's
// restriction. The rolling counters and other groups remain untouched.
func (s *Service) ClearCacheHealthBlacklistForGroup(sourceKind string, sourceID, groupID uint) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if sourceID == 0 || groupID == 0 {
		return fmt.Errorf("cache health source and gateway group are required")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	policy, policyErr := s.cacheHealthPolicyForGroup(groupID)
	if policyErr != nil {
		return policyErr
	}
	var err error
	if cacheHealthProtectionEnabled(policy) {
		graceUntil := time.Now().Add(time.Duration(policy.WindowMinutes) * time.Minute)
		err = s.Usage.ClearCacheHealthBlacklistWithSuppressionForGroup(kind, sourceID, groupID, graceUntil)
	} else {
		err = s.Usage.ClearCacheHealthBlacklistForGroup(kind, sourceID, groupID)
	}
	if err != nil {
		return err
	}
	if s.Routes != nil {
		s.Routes.InvalidateCacheHealthSourceForGroup(kind, sourceID, groupID)
	}
	return nil
}

// ClearCacheHealthBlacklistForRoute releases only one concrete source group.
// When the database contains only legacy route_id=0 state, it is copied into
// a route-specific override before release so sibling routes remain blocked.
func (s *Service) ClearCacheHealthBlacklistForRoute(sourceKind string, sourceID, groupID, routeID uint) error {
	if s == nil || s.Usage == nil {
		return fmt.Errorf("cache health dependencies are unavailable")
	}
	if sourceID == 0 || groupID == 0 || routeID == 0 {
		return fmt.Errorf("cache health source, gateway group, and route are required")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	states, err := s.Usage.CacheHealthStatesForRoute(kind, sourceID, groupID, routeID)
	if err != nil {
		return err
	}
	state := storage.GatewayChannelCacheHealth{
		GatewayGroupID: groupID,
		RouteID:        routeID,
		SourceKind:     kind,
		SourceID:       sourceID,
	}
	if len(states) > 0 {
		state = states[0]
	} else {
		groupStates, loadErr := s.Usage.CacheHealthStatesForGroup(kind, []uint{sourceID}, groupID)
		if loadErr != nil {
			return loadErr
		}
		for _, candidate := range groupStates {
			if candidate.RouteID == 0 {
				state = candidate
				state.ID = 0
				state.RouteID = routeID
				break
			}
		}
	}
	state.GatewayGroupID = groupID
	state.RouteID = routeID
	state.SourceKind = kind
	state.SourceID = sourceID
	state.BlacklistedUntil = nil
	state.BlacklistReason = ""
	state.ManualClearUntil = nil
	policy, policyErr := s.cacheHealthPolicyForGroup(groupID)
	if policyErr != nil {
		return policyErr
	}
	if cacheHealthProtectionEnabled(policy) {
		graceUntil := time.Now().Add(time.Duration(policy.WindowMinutes) * time.Minute)
		state.ManualClearUntil = &graceUntil
	}
	if err := s.Usage.UpsertCacheHealth(&state); err != nil {
		return err
	}
	if s.Routes != nil {
		s.Routes.InvalidateCacheHealthRoute(kind, sourceID, groupID, routeID)
	}
	return nil
}
