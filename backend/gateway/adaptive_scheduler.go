package gateway

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"sort"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

type schedulingDecision struct {
	Mode           string  `json:"mode"`
	Reason         string  `json:"reason"`
	Rate           float64 `json:"rate"`
	Ceiling        float64 `json:"ceiling"`
	Samples        int     `json:"samples"`
	FirstSamples   int     `json:"first_samples"`
	Window         int     `json:"window_minutes"`
	EstimatedMS    float64 `json:"estimated_ms"`
	MeanMS         float64 `json:"mean_ms"`
	P90MS          float64 `json:"p90_ms"`
	FailurePercent float64 `json:"failure_percent"`
	Active         int     `json:"active"`
	Limit          int     `json:"limit"`
	Queued         int     `json:"queued"`
}

func (d *schedulingDecision) json() string {
	if d == nil {
		return ""
	}
	data, _ := json.Marshal(d)
	return string(data)
}

type adaptiveRequest struct {
	group                          *storage.GatewayGroup
	model, effort, path, requestID string
	thinking                       bool
	size                           uint8
	enabled, priced                bool
	ceiling                        float64
}

func newAdaptiveRequest(group *storage.GatewayGroup, model, effort, path, requestID string, thinking bool, size int, eligible bool) *adaptiveRequest {
	bucket := uint8(0)
	if size > 16*1024 {
		bucket = 1
	}
	if size > 64*1024 {
		bucket = 2
	}
	if size > 256*1024 {
		bucket = 3
	}
	return &adaptiveRequest{group: group, model: model, effort: effort, path: path, requestID: requestID, thinking: thinking, size: bucket,
		enabled: eligible && group != nil && (group.SchedulingMode == "balanced" || group.SchedulingMode == "latency")}
}

func (r *adaptiveRequest) key(route storage.GatewayRoute) schedulerStatsKey {
	model, _ := ResolveModel(r.model, ParseModelMapping(route.ModelMappingJSON), ParseModelMapping(r.group.ModelMappingJSON))
	if model == "" {
		model = r.model
	}
	credential := schedulerCredential(route.SourceAPIKeyCipher, "")
	if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
		if decoded, err := hex.DecodeString(route.SchedulerCredential); err == nil {
			copy(credential[:], decoded)
		}
	}
	return schedulerStatsKey{GroupID: r.group.ID, Source: loadBalanceIdentity(&route), Credential: credential,
		Model: model, Effort: ApplyThinkingEnabledEffortFallback(r.effort, r.thinking, model, r.model), Endpoint: r.path, Size: r.size}
}

func schedulerPositive(value, fallback, maximum int) int {
	if value <= 0 {
		return fallback
	}
	if value > maximum {
		return maximum
	}
	return value
}

// The first eligible pool establishes the ceiling for the entire request.
// Excluding a cheap route after a failure must never silently raise its budget.
func (r *adaptiveRequest) filter(candidates []ScoredRoute, affinity *routeAffinityContext) []ScoredRoute {
	if !r.enabled {
		return candidates
	}
	if !r.priced {
		baseline := math.Inf(1)
		for _, candidate := range candidates {
			if affinity != nil && affinity.Recovery && candidate.Route.ID == affinity.RecoveryRouteID && len(candidates) > 1 {
				continue
			}
			if validSchedulingRate(candidate.EffectiveRate) {
				baseline = math.Min(baseline, candidate.EffectiveRate)
			}
		}
		if math.IsInf(baseline, 1) {
			for _, candidate := range candidates {
				if validSchedulingRate(candidate.EffectiveRate) {
					baseline = math.Min(baseline, candidate.EffectiveRate)
				}
			}
		}
		premium := r.group.SchedulingPremiumPercent
		if math.IsNaN(premium) || math.IsInf(premium, 0) || premium < 0 {
			premium = 0
		}
		r.ceiling = baseline * (1 + math.Min(premium, 1000)/100)
		if math.IsInf(r.ceiling, 0) || math.IsNaN(r.ceiling) {
			r.ceiling = -1
		}
		if absolute := normalizeMaxBillingRateMultiplier(r.group.MaxBillingRateMultiplier); absolute > 0 {
			r.ceiling = math.Min(r.ceiling, absolute)
		}
		r.priced = true
	}
	out := make([]ScoredRoute, 0, len(candidates))
	for _, candidate := range candidates {
		if validSchedulingRate(candidate.EffectiveRate) && candidate.EffectiveRate <= r.ceiling+math.Abs(r.ceiling)*1e-12 {
			out = append(out, candidate)
		}
	}
	return out
}

func validSchedulingRate(rate float64) bool {
	return rate >= 0 && !math.IsNaN(rate) && !math.IsInf(rate, 0)
}

type adaptiveCandidate struct {
	candidate   ScoredRoute
	score       float64
	meets, full bool
	trained     bool
}

func (rt *Runtime) orderAdaptiveCandidates(candidates []ScoredRoute, r *adaptiveRequest, affinity *routeAffinityContext, initial bool, now time.Time) []ScoredRoute {
	if !r.enabled {
		if initial {
			return rt.orderLoadBalancedCandidates(candidates, r.group, affinity)
		}
		return candidates
	}
	candidates = r.filter(candidates, affinity)
	if initial && affinity != nil && affinity.Recovery {
		allowed := false
		for _, candidate := range candidates {
			if candidate.Route.ID == affinity.RecoveryRouteID {
				allowed = true
				break
			}
		}
		if !allowed {
			// sortRoutesWithAffinity claimed a probe before price filtering.
			// Release that claim when the hard budget forbids sending it.
			rt.finishRouteAffinityProbe(affinity, affinity.RecoveryRouteID, false, nil, now)
			affinity.Recovery = false
			affinity.RecoveryRouteID = 0
			affinity.PreferredRouteID = 0
			affinity.PreservePreferred = false
		}
	}
	if len(candidates) == 0 {
		return candidates
	}
	window := schedulerPositive(r.group.SchedulingWindowMinutes, 5, 60)
	minimum := schedulerPositive(r.group.SchedulingMinSamples, 10, 1000)
	targetMS := float64(schedulerPositive(r.group.SchedulingTargetTTFTSec, 10, 300)) * 1000
	stats := rt.schedulingStatistics()
	registry := rt.upstreamConcurrencyRegistry()
	ranked := make([]adaptiveCandidate, 0, len(candidates))
	for _, candidate := range candidates {
		key := r.key(candidate.Route)
		estimate := stats.estimate(key, now, window, minimum)
		confidence := math.Min(1, float64(estimate.FirstSamples)/float64(minimum))
		prediction := confidence*(estimate.MeanMS*0.7+estimate.P90MS*0.3) + (1-confidence)*targetMS*1.25
		failure := estimate.FailureRate
		if estimate.Samples == 0 {
			failure = 0.05
		}
		active, limit, queued := registry.snapshot(upstreamConcurrencyKey{Kind: key.Source.Kind, ID: key.Source.ID})
		if candidate.Route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider {
			limit = candidate.Route.SchedulerConcurrencyLimit
		}
		load := 1.0
		if limit > 0 {
			load += 0.5 * float64(active+queued) / float64(limit)
		} else {
			load += math.Min(1, float64(active)*0.03)
		}
		// Failed attempts have a cost even when they produced no token at all.
		score := (prediction + failure*math.Max(30000, targetMS*2)) / math.Max(0.05, 1-failure) * load
		decision := &schedulingDecision{Mode: r.group.SchedulingMode, Reason: "latency", Rate: candidate.EffectiveRate, Ceiling: r.ceiling,
			Samples: estimate.Samples, FirstSamples: estimate.FirstSamples, Window: estimate.Window, EstimatedMS: score, MeanMS: estimate.MeanMS,
			P90MS: estimate.P90MS, FailurePercent: failure * 100, Active: active, Limit: limit, Queued: queued}
		candidate.Decision = decision
		candidate.StatisticsKey = key
		meets := estimate.FirstSamples >= minimum && prediction*load <= targetMS && failure <= 0.1
		if r.group.SchedulingMode == "balanced" && meets {
			decision.Reason = "target_cost"
		}
		if estimate.FirstSamples < minimum {
			decision.Reason = "cold_start"
		}
		ranked = append(ranked, adaptiveCandidate{candidate: candidate, score: score, meets: meets, full: queued > 0 || (limit > 0 && active >= limit)})
		ranked[len(ranked)-1].trained = estimate.FirstSamples >= minimum && failure <= 0.1
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		a, b := ranked[i], ranked[j]
		if a.full != b.full {
			return !a.full
		}
		// Unknown speed is not evidence that a source beats a measured,
		// reliable one. Limited exploration supplies those missing samples.
		if a.trained != b.trained {
			return a.trained
		}
		if r.group.SchedulingMode == "balanced" {
			if a.meets != b.meets {
				return a.meets
			}
			if a.meets && a.candidate.EffectiveRate != b.candidate.EffectiveRate {
				return a.candidate.EffectiveRate < b.candidate.EffectiveRate
			}
		}
		if a.score != b.score {
			return a.score < b.score
		}
		return routeRateLess(a.candidate.Route, b.candidate.Route, a.candidate.EffectiveRate, b.candidate.EffectiveRate, false)
	})
	chosen := 0
	preferred := false
	if affinity != nil {
		for i, candidate := range ranked {
			if initial && affinity.Recovery && candidate.candidate.Route.ID == affinity.RecoveryRouteID {
				chosen = i
				preferred = true
				ranked[i].candidate.Decision.Reason = "recovery"
				break
			}
			if candidate.candidate.Route.ID == affinity.PreferredRouteID && !candidate.full && candidate.meets == ranked[0].meets && candidate.score <= ranked[0].score*1.15 {
				chosen = i
				preferred = true
				ranked[i].candidate.Decision.Reason = "affinity"
				break
			}
		}
	}
	// About 5% of new primaries sample a healthy, unsaturated under-observed
	// source within the same hard price envelope. Never multiply exploration
	// across retries or displace a competitive conversation binding.
	digest := sha256.Sum256([]byte(r.requestID))
	seed := binary.LittleEndian.Uint64(digest[:8])
	if initial && !preferred && seed%20 == 0 {
		cold := make([]int, 0, len(ranked))
		for i, candidate := range ranked {
			if !candidate.full && candidate.candidate.Decision.FirstSamples < minimum && candidate.candidate.Decision.FailurePercent <= 10 {
				cold = append(cold, i)
			}
		}
		if len(cold) > 0 {
			chosen = cold[(seed/20)%uint64(len(cold))]
			ranked[chosen].candidate.Decision.Reason = "exploration"
		}
	}
	out := make([]ScoredRoute, 0, len(ranked))
	out = append(out, ranked[chosen].candidate)
	for i := range ranked {
		if i != chosen {
			out = append(out, ranked[i].candidate)
		}
	}
	return out
}
