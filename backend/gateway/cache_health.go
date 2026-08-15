package gateway

import (
	"fmt"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/storage"
)

// cacheHealthSourceKey identifies one physical upstream source. A provider
// can be referenced by many routes, so the key is deliberately source-level.
type cacheHealthSourceKey struct {
	kind string
	id   uint
}

// CacheHealthStat is the admin-facing rolling cache summary plus persisted
// automatic blacklist state.
type CacheHealthStat struct {
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
}

func normalizeCacheHealthKind(kind string) string {
	if strings.EqualFold(strings.TrimSpace(kind), storage.GatewayRouteSourceProvider) {
		return storage.GatewayRouteSourceProvider
	}
	return storage.GatewayRouteSourceMonitor
}

func cacheHealthProtectionEnabled(cfg config.GatewayConfig) bool {
	return cfg.CacheHitRateWindowMinutes > 0 &&
		cfg.CacheHitRateThresholdPercent > 0 &&
		cfg.CacheHitRateBlacklistMinutes > 0
}

func cacheHealthWindowMinutes(cfg config.GatewayConfig) int {
	if cfg.CacheHitRateWindowMinutes > 0 {
		return cfg.CacheHitRateWindowMinutes
	}
	// Statistics remain useful even when automatic protection is disabled.
	return 60
}

// CacheHealthStats returns current rolling statistics for one source kind.
// sourceIDs may be empty to include every source represented in logs/state.
func (s *Service) CacheHealthStats(sourceKind string, sourceIDs []uint) ([]CacheHealthStat, error) {
	if s == nil || s.Usage == nil {
		return nil, fmt.Errorf("usage storage is unavailable")
	}
	kind := normalizeCacheHealthKind(sourceKind)
	cfg := s.gatewayRuntime()
	from := time.Now().Add(-time.Duration(cacheHealthWindowMinutes(cfg)) * time.Minute)
	aggs, err := s.Usage.CacheHealthAggregates(kind, sourceIDs, from)
	if err != nil {
		return nil, err
	}
	states, err := s.Usage.CacheHealthStates(kind, sourceIDs)
	if err != nil {
		return nil, err
	}
	byID := make(map[uint]CacheHealthStat, len(aggs)+len(states))
	for _, agg := range aggs {
		byID[agg.SourceID] = CacheHealthStat{
			SourceKind: agg.SourceKind, SourceID: agg.SourceID,
			HitRate: agg.HitRate, RequestCount: agg.RequestCount,
			InputTokens: agg.InputTokens, CacheReadTokens: agg.CacheReadTokens,
			CacheCreationTokens: agg.CacheCreationTokens, WindowStart: agg.WindowStart,
		}
	}
	for _, state := range states {
		item, ok := byID[state.SourceID]
		if !ok {
			windowStart := from
			if state.WindowStart != nil && !state.WindowStart.IsZero() {
				windowStart = *state.WindowStart
			}
			item = CacheHealthStat{
				SourceKind: kind, SourceID: state.SourceID,
				HitRate: state.HitRate, RequestCount: state.RequestCount,
				InputTokens: state.InputTokens, CacheReadTokens: state.CacheReadTokens,
				CacheCreationTokens: state.CacheCreationTokens,
				WindowStart:         windowStart,
			}
		}
		item.EvaluatedAt = cloneTimePointer(state.EvaluatedAt)
		item.BlacklistedUntil = cloneTimePointer(state.BlacklistedUntil)
		item.BlacklistReason = state.BlacklistReason
		byID[state.SourceID] = item
	}
	if len(sourceIDs) > 0 {
		// Keep the response stable for provider/channel lists, including sources
		// that have no usage in the current window.
		for _, id := range sourceIDs {
			if id == 0 {
				continue
			}
			if _, ok := byID[id]; !ok {
				byID[id] = CacheHealthStat{SourceKind: kind, SourceID: id, WindowStart: from}
			}
		}
	}
	out := make([]CacheHealthStat, 0, len(byID))
	for _, item := range byID {
		out = append(out, item)
	}
	// Deterministic order keeps the API/UI stable and makes tests simple.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].SourceID < out[j-1].SourceID; j-- {
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
func (s *Service) scheduleCacheHealthEvaluation(sourceKind string, sourceID uint) {
	if s == nil || sourceID == 0 || s.Usage == nil || s.Routes == nil {
		return
	}
	cfg := s.gatewayRuntime()
	if !cacheHealthProtectionEnabled(cfg) {
		return
	}
	key := cacheHealthSourceKey{kind: normalizeCacheHealthKind(sourceKind), id: sourceID}
	now := time.Now()
	s.cacheHealthMu.Lock()
	if s.cacheHealthPending == nil {
		s.cacheHealthPending = make(map[cacheHealthSourceKey]time.Time)
	}
	if previous, ok := s.cacheHealthPending[key]; ok && now.Sub(previous) < 15*time.Second {
		s.cacheHealthMu.Unlock()
		return
	}
	s.cacheHealthPending[key] = now
	s.cacheHealthMu.Unlock()
	time.AfterFunc(500*time.Millisecond, func() {
		defer func() {
			s.cacheHealthMu.Lock()
			delete(s.cacheHealthPending, key)
			s.cacheHealthMu.Unlock()
		}()
		if err := s.EvaluateCacheHealth(key.kind, key.id, time.Now()); err != nil && s.Log != nil {
			s.Log.Warn("evaluate gateway cache health failed", "source_kind", key.kind, "source_id", key.id, "err", err)
		}
	})
}

// EvaluateCacheHealth evaluates and persists one source immediately. It is
// exported for deterministic administration/tests; normal traffic uses the
// debounced scheduler above.
func (s *Service) EvaluateCacheHealth(sourceKind string, sourceID uint, now time.Time) error {
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
	cfg := s.gatewayRuntime()
	if !cacheHealthProtectionEnabled(cfg) {
		return nil
	}
	if s.Routes == nil {
		return fmt.Errorf("cache health route storage is unavailable")
	}
	from := now.Add(-time.Duration(cfg.CacheHitRateWindowMinutes) * time.Minute)
	agg, err := s.Usage.CacheHealthAggregate(kind, sourceID, from)
	if err != nil {
		return err
	}
	states, err := s.Usage.CacheHealthStates(kind, []uint{sourceID})
	if err != nil {
		return err
	}
	state := storage.GatewayChannelCacheHealth{
		SourceKind: kind, SourceID: sourceID,
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
	}
	denominator := agg.InputTokens + agg.CacheReadTokens + agg.CacheCreationTokens
	low := agg.RequestCount >= int64(cfg.CacheHitRateMinimumRequests) && denominator > 0 &&
		agg.HitRate < cfg.CacheHitRateThresholdPercent
	active := state.BlacklistedUntil != nil && state.BlacklistedUntil.After(now)
	if low && !active {
		until := now.Add(time.Duration(cfg.CacheHitRateBlacklistMinutes) * time.Minute)
		state.BlacklistedUntil = &until
		state.BlacklistReason = fmt.Sprintf(
			"缓存命中率 %.2f%% 低于阈值 %.2f%%（最近 %d 分钟）",
			agg.HitRate, cfg.CacheHitRateThresholdPercent, cfg.CacheHitRateWindowMinutes,
		)
	} else if !active && !low {
		state.BlacklistedUntil = nil
		state.BlacklistReason = ""
	}
	if err := s.Usage.UpsertCacheHealth(&state); err != nil {
		return err
	}
	s.Routes.InvalidateCacheHealthSource(kind, sourceID)
	return nil
}

// CacheHealthEnabled reports whether automatic protection is active under the
// currently applied gateway settings.
func (s *Service) CacheHealthEnabled() bool {
	if s == nil {
		return false
	}
	return cacheHealthProtectionEnabled(s.gatewayRuntime())
}
