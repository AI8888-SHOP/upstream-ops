package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func adaptiveFixture() (*Runtime, *storage.GatewayGroup, *adaptiveRequest, []ScoredRoute) {
	rt := (&Service{}).runtime()
	group := &storage.GatewayGroup{ID: 7, GatewaySchedulerPolicy: storage.GatewaySchedulerPolicy{
		SchedulingMode: "balanced", SchedulingPremiumPercent: 20, SchedulingWindowMinutes: 5, SchedulingMinSamples: 10, SchedulingTargetTTFTSec: 10,
	}}
	req := newAdaptiveRequest(group, "m", "high", "/v1/chat/completions", "fixture", false, 1024, true)
	candidates := make([]ScoredRoute, 3)
	for i, rate := range []float64{0.05, 0.055, 0.08} {
		candidates[i] = ScoredRoute{Route: storage.GatewayRoute{ID: uint(i + 1), SourceChannelID: uint(i + 1), SourceAPIKeyCipher: "encrypted-key", Enabled: true}, EffectiveRate: rate, BillingRate: rate}
	}
	return rt, group, req, candidates
}

func seedAdaptive(rt *Runtime, req *adaptiveRequest, candidate ScoredRoute, now time.Time, ms float64, count, failures int) {
	key := req.key(candidate.Route)
	for i := 0; i < count; i++ {
		failed := i < failures
		var first *float64
		if !failed {
			first = &ms
		}
		rt.schedulingStatistics().record(key, now, first, &failed)
	}
}

func TestAdaptivePriceEnvelopeAndFailover(t *testing.T) {
	rt, group, req, candidates := adaptiveFixture()
	now := time.Now()
	seedAdaptive(rt, req, candidates[0], now, 20000, 100, 15)
	seedAdaptive(rt, req, candidates[1], now, 5000, 100, 1)
	seedAdaptive(rt, req, candidates[2], now, 2000, 100, 0)
	ordered := rt.orderAdaptiveCandidates(candidates, req, nil, false, now)
	if len(ordered) != 2 || ordered[0].Route.ID != 2 || math.Abs(req.ceiling-0.06) > 1e-9 {
		t.Fatalf("order=%+v ceiling=%f", ordered, req.ceiling)
	}
	// Even after both cheap sources fail, an expensive source cannot become
	// a new baseline. This same filtered list is used to build hedge plans.
	if got := req.filter(candidates[2:], nil); len(got) != 0 {
		t.Fatalf("failover escaped ceiling: %+v", got)
	}
	group.MaxBillingRateMultiplier = 0.052
	req.priced = false
	if got := rt.orderAdaptiveCandidates(candidates, req, nil, false, now); len(got) != 1 || got[0].Route.ID != 1 {
		t.Fatalf("absolute guard failed: %+v", got)
	}
	group.MaxBillingRateMultiplier = 0
	group.SchedulingPremiumPercent = 0
	req.priced = false
	if got := req.filter(candidates, nil); len(got) != 1 || got[0].Route.ID != 1 {
		t.Fatalf("zero premium failed: %+v", got)
	}
}

func TestAdaptiveModesLoadAndSoftAffinity(t *testing.T) {
	rt, group, req, candidates := adaptiveFixture()
	now := time.Now()
	seedAdaptive(rt, req, candidates[0], now, 6000, 40, 0)
	seedAdaptive(rt, req, candidates[1], now, 5000, 40, 0)
	if got := rt.orderAdaptiveCandidates(candidates, req, nil, false, now); got[0].Route.ID != 1 {
		t.Fatal("balanced should prefer cheaper source meeting target")
	}
	group.SchedulingMode = "latency"
	if got := rt.orderAdaptiveCandidates(candidates, req, nil, false, now); got[0].Route.ID != 2 {
		t.Fatal("latency should prefer faster source")
	}
	// Within 15%, conversation continuity is worth retaining.
	affinity := &routeAffinityContext{PreferredRouteID: 1}
	got := rt.orderAdaptiveCandidates(candidates, req, affinity, false, now)
	if got[0].Route.ID != 1 || got[0].Decision.Reason != "affinity" {
		t.Fatalf("soft affinity=%+v", got[0])
	}
	release, err := rt.upstreamConcurrencyRegistry().acquire(context.Background(), upstreamConcurrencyKey{Kind: "monitor", ID: 1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if got := rt.orderAdaptiveCandidates(candidates, req, affinity, false, now); got[0].Route.ID != 2 {
		t.Fatal("full affinity source was not bypassed")
	}
	// Distinct groups on one physical channel share its saturation.
	groupID := int64(99)
	candidates[1].Route.SourceChannelID = 1
	candidates[1].Route.SourceGroupID = &groupID
	got = rt.orderAdaptiveCandidates(candidates, req, nil, false, now)
	for _, candidate := range got {
		if candidate.Decision.Active != 1 || candidate.Decision.Limit != 1 {
			t.Fatal("load was incorrectly separated by source group")
		}
	}
}

func TestAdaptiveColdStartExplorationAndLegacyMode(t *testing.T) {
	rt, group, req, candidates := adaptiveFixture()
	now := time.Now()
	seedAdaptive(rt, req, candidates[0], now, 20000, 20, 0)
	explored := 0
	for i := 0; i < 1000; i++ {
		req.requestID = fmt.Sprintf("request-%d", i)
		got := rt.orderAdaptiveCandidates(candidates, req, nil, true, now)
		if got[0].Route.ID == 2 {
			explored++
			if got[0].Decision.Reason != "exploration" {
				t.Fatal("cold prior displaced a measured source outside exploration")
			}
		}
		for _, candidate := range got {
			if candidate.EffectiveRate > 0.06 {
				t.Fatal("exploration exceeded price limit")
			}
		}
	}
	if explored < 20 || explored > 80 {
		t.Fatalf("exploration count=%d, expected about 50/1000", explored)
	}
	group.SchedulingMode = "cost"
	legacy := newAdaptiveRequest(group, "m", "", "", "", false, 0, true)
	got := rt.orderAdaptiveCandidates(candidates, legacy, nil, false, now)
	if len(got) != 3 || got[0].Route.ID != candidates[0].Route.ID || got[0].Decision != nil {
		t.Fatal("legacy order changed")
	}
	group.SchedulingMode = "latency"
	media := newAdaptiveRequest(group, "m", "", "", "", false, 0, false)
	if got := rt.orderAdaptiveCandidates(candidates, media, nil, false, now); len(got) != 3 || got[0].Decision != nil {
		t.Fatal("ineligible request used text TTFT statistics")
	}
}

func TestAdaptiveIdentityIsolationAndWindowFallback(t *testing.T) {
	rt, _, req, candidates := adaptiveFixture()
	now := time.Now()
	original := req.key(candidates[0].Route)
	seedAdaptive(rt, req, candidates[0], now.Add(-10*time.Minute), 5000, 20, 0)
	stats := rt.schedulingStatistics()
	if got := stats.estimate(original, now, 5, 10); got.Window != 60 || got.FirstSamples != 20 {
		t.Fatalf("fallback=%+v", got)
	}
	if got := stats.estimate(original, now.Add(time.Hour), 5, 10); got.FirstSamples != 0 {
		t.Fatal("expired statistics survived")
	}
	for _, mutate := range []func(*adaptiveRequest, *storage.GatewayRoute){
		func(r *adaptiveRequest, route *storage.GatewayRoute) { r.model = "other" },
		func(r *adaptiveRequest, route *storage.GatewayRoute) { r.effort = "low" },
		func(r *adaptiveRequest, route *storage.GatewayRoute) { r.size++ },
		func(r *adaptiveRequest, route *storage.GatewayRoute) { r.path = "/v1/responses" },
		func(r *adaptiveRequest, route *storage.GatewayRoute) { route.SourceAPIKeyCipher = "replacement" },
		func(r *adaptiveRequest, route *storage.GatewayRoute) { route.SourceGroupName = "another" },
	} {
		copyReq, route := *req, candidates[0].Route
		mutate(&copyReq, &route)
		if key := copyReq.key(route); key == original || stats.estimate(key, now, 5, 10).FirstSamples != 0 {
			t.Fatal("unrelated request inherited measurements")
		}
	}
}

func TestSchedulerObservationsAreImmediateOnceAndExcludeLocalCancellation(t *testing.T) {
	rt, _, req, candidates := adaptiveFixture()
	candidate := candidates[0]
	candidate.StatisticsKey = req.key(candidate.Route)
	candidate.Decision = &schedulingDecision{}
	o := rt.newSchedulerObservation(candidate, time.Now().Add(-2*time.Second))
	o.markUpstreamStarted()
	state := &forwardRequestTiming{started: time.Now().Add(-20 * time.Second)}
	state.setFirstObserver(o.first)
	state.flushed()
	state.flushed()
	got := o.stats.estimate(o.key, time.Now(), 5, 1)
	if got.FirstSamples != 1 || got.Samples != 0 || got.MeanMS < 2000 || got.MeanMS > 10000 {
		t.Fatalf("not recorded at per-attempt visible flush: %+v", got)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); o.finish(context.Background(), true, "", "") }()
	}
	wg.Wait()
	if got := o.stats.estimate(o.key, time.Now(), 5, 1); got.Samples != 1 {
		t.Fatal("outcome counted more than once")
	}
	for _, errorType := range []string{"client", "canceled", "queue_timeout", "request_timeout", "config"} {
		o := rt.newSchedulerObservation(candidate, time.Now())
		o.markUpstreamStarted()
		o.finish(context.Background(), false, errorType, "")
	}
	loser := rt.newSchedulerObservation(candidate, time.Now())
	loser.markUpstreamStarted()
	loser.finish(context.Background(), false, "transport", "canceled")
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	loser.finish(canceled, false, "transport", "")
	for _, status := range []int{400, 413, 422} {
		badRequest := rt.newSchedulerObservation(candidate, time.Now())
		badRequest.markUpstreamStarted()
		badRequest.finish(context.Background(), false, "http", "", status)
	}
	if got := o.stats.estimate(o.key, time.Now(), 5, 1); got.Samples != 1 {
		t.Fatal("local cancellation penalized source")
	}
	failed := rt.newSchedulerObservation(candidate, time.Now())
	failed.markUpstreamStarted()
	failed.finish(context.Background(), false, "transport", "")
	if got := o.stats.estimate(o.key, time.Now(), 5, 1); got.Samples != 2 || got.FirstSamples != 1 || got.FailureRate <= 0.05 {
		t.Fatalf("no-token failure disappeared: %+v", got)
	}
}

func TestSchedulerStatisticsMemoryBoundAndInvalidRates(t *testing.T) {
	rt, _, req, candidates := adaptiveFixture()
	stats := rt.schedulingStatistics()
	now := time.Now()
	failed := true
	for i := 0; i <= schedulerMaxKeys; i++ {
		stats.record(schedulerStatsKey{GroupID: uint(i)}, now, nil, &failed)
	}
	if len(stats.keys) != schedulerMaxKeys || stats.keys[schedulerStatsKey{}] != nil {
		t.Fatal("statistics LRU not bounded")
	}
	for _, rate := range []float64{math.NaN(), math.Inf(1), -1} {
		candidates[0].EffectiveRate = rate
		req.priced = false
		if got := req.filter(candidates[:1], nil); len(got) != 0 {
			t.Fatalf("invalid rate admitted: %+v", got)
		}
	}
}

func TestSchedulingPolicyJSONPersistenceAndExplicitZero(t *testing.T) {
	db := openGatewayTestDB(t)
	svc := NewService(storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db), storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, nil, nil)
	var input CreateGroupInput
	if err := json.Unmarshal([]byte(`{"name":"adaptive","scheduling_mode":"balanced","scheduling_premium_percent":0,"scheduling_window_minutes":3,"scheduling_min_samples":15,"scheduling_target_ttft_sec":8}`), &input); err != nil {
		t.Fatal(err)
	}
	group, err := svc.CreateGroup(input)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := svc.Groups.FindByID(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.SchedulingMode != "balanced" || loaded.SchedulingPremiumPercent != 0 || loaded.SchedulingWindowMinutes != 3 || loaded.SchedulingMinSamples != 15 || loaded.SchedulingTargetTTFTSec != 8 {
		t.Fatalf("policy=%+v", loaded.GatewaySchedulerPolicy)
	}
	mode := "latency"
	premium := 20.0
	_, err = svc.UpdateGroup(group.ID, UpdateGroupInput{SchedulingPolicyInput: SchedulingPolicyInput{SchedulingMode: &mode, SchedulingPremiumPercent: &premium}})
	if err != nil {
		t.Fatal(err)
	}
	premium = 0
	updated, err := svc.UpdateGroup(group.ID, UpdateGroupInput{SchedulingPolicyInput: SchedulingPolicyInput{SchedulingPremiumPercent: &premium}})
	if err != nil {
		t.Fatal(err)
	}
	if updated.SchedulingMode != "latency" || updated.SchedulingPremiumPercent != 0 || updated.SchedulingMinSamples != 15 {
		t.Fatal("partial update lost policy")
	}
	mode = "invalid"
	if _, err := svc.UpdateGroup(group.ID, UpdateGroupInput{SchedulingPolicyInput: SchedulingPolicyInput{SchedulingMode: &mode}}); err == nil {
		t.Fatal("invalid mode accepted")
	}
}

func TestAdaptiveProviderCredentialRotationDoesNotMixInFlightSamples(t *testing.T) {
	rt, _, req, _ := adaptiveFixture()
	route := storage.GatewayRoute{GatewayProviderID: 91, SourceKind: storage.GatewayRouteSourceProvider,
		SchedulerCredential: fmt.Sprintf("%x", schedulerCredential("old-cipher", "https://upstream.test"))}
	oldKey := req.key(route)
	candidate := ScoredRoute{Route: route, StatisticsKey: oldKey, Decision: &schedulingDecision{}}
	// Provider changed between candidate filtering and target resolution.
	target := &upstreamTarget{Provider: &storage.GatewayProvider{APIKeyCipher: "new-cipher", BaseURL: "https://upstream.test"}}
	o := rt.newSchedulerObservation(candidate, time.Now(), target)
	o.markUpstreamStarted()
	o.first()
	o.finish(context.Background(), true, "", "")
	if oldKey == o.key || rt.schedulingStatistics().estimate(oldKey, time.Now(), 5, 1).Samples != 0 {
		t.Fatal("rotated credential contaminated the old identity")
	}
	if rt.schedulingStatistics().estimate(o.key, time.Now(), 5, 1).Samples != 1 {
		t.Fatal("actual target identity was not recorded")
	}
}

func TestAdaptivePriceGuardReleasesUnsentRecoveryProbe(t *testing.T) {
	rt, _, req, candidates := adaptiveFixture()
	now := time.Now()
	key := routeAffinityKey{GroupID: 7, Fingerprint: "test"}
	rt.routeAffinities = map[routeAffinityKey]routeAffinityEntry{key: {RouteID: 3, ExpiresAt: now.Add(time.Hour), ProbeUntil: now.Add(time.Minute)}}
	affinity := &routeAffinityContext{Keys: []routeAffinityKey{key}, LookupKey: key, PreferredRouteID: 3, RecoveryRouteID: 3, Recovery: true, PreservePreferred: true, RecoveryCooldownUntil: now.Add(time.Minute)}
	got := rt.orderAdaptiveCandidates(candidates, req, affinity, true, now)
	if len(got) != 2 || affinity.Recovery || !rt.routeAffinities[key].ProbeUntil.IsZero() {
		t.Fatal("budget-excluded recovery probe was left claimed")
	}
}
