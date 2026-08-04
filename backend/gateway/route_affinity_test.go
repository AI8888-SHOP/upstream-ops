package gateway

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/bejix/upstream-ops/backend/storage"
)

func TestRequestRouteAffinityMatchesConversationPrefix(t *testing.T) {
	first := []byte(`{"model":"m","messages":[{"role":"user","content":"one"}]}`)
	continued := []byte(`{"model":"m","messages":[{"role":"user","content":"one"},{"role":"assistant","content":"reply"},{"role":"user","content":"two"}]}`)
	firstFingerprints := requestRouteAffinityFingerprints(nil, first, "m")
	continuedFingerprints := requestRouteAffinityFingerprints(nil, continued, "m")
	if len(firstFingerprints) == 0 || len(continuedFingerprints) < 2 {
		t.Fatalf("fingerprints first=%d continued=%d", len(firstFingerprints), len(continuedFingerprints))
	}
	found := false
	for _, want := range firstFingerprints {
		for _, got := range continuedFingerprints {
			if want == got {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("continued conversation did not match the previous full history")
	}
}

func TestSortRoutesWithAffinityRecoversCooledRoute(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &Service{}
	rt := svc.runtime()
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"one"}]}`)
	rt.rememberRouteAffinity(
		rt.routeAffinityForRequest(nil, 1, 1, "openai_chat", "m", body).Keys,
		1, time.Now(),
	)
	affinity := rt.routeAffinityForRequest(nil, 1, 1, "openai_chat", "m", body)
	until := time.Now().Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, GatewayGroupID: 1, Position: 0, Enabled: true, SourceAPIKeyCipher: "a", TempUnschedulableUntil: &until, RateConvertMode: "custom", RateConvertValue: 0.01},
		{ID: 2, GatewayGroupID: 1, Position: 1, Enabled: true, SourceAPIKeyCipher: "b", RateConvertMode: "custom", RateConvertValue: 0.02},
	}
	got := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &affinity)
	if len(got) != 2 || got[0].Route.ID != 1 || !affinity.Recovery {
		t.Fatalf("recovery candidates=%+v affinity=%+v", got, affinity)
	}

	second := rt.routeAffinityForRequest(nil, 1, 1, "openai_chat", "m", body)
	got = rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &second)
	if len(got) != 1 || got[0].Route.ID != 2 {
		t.Fatalf("concurrent probe should fall back, got=%+v", got)
	}
}

func TestRouteAffinityProbeFailureBlocksUntilCooldown(t *testing.T) {
	svc := &Service{}
	rt := svc.runtime()
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"one"}]}`)
	keys := rt.routeAffinityForRequest(nil, 2, 1, "openai_chat", "m", body).Keys
	rt.rememberRouteAffinity(keys, 1, time.Now())
	affinity := rt.routeAffinityForRequest(nil, 2, 1, "openai_chat", "m", body)
	until := time.Now().Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, GatewayGroupID: 1, Position: 0, Enabled: true, SourceAPIKeyCipher: "a", TempUnschedulableUntil: &until},
		{ID: 2, GatewayGroupID: 1, Position: 1, Enabled: true, SourceAPIKeyCipher: "b"},
	}
	if got := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &affinity); len(got) != 2 || !affinity.Recovery {
		t.Fatalf("initial probe candidates=%+v", got)
	}
	// The scheduler passes a temporary route copy with its cooldown cleared to
	// the probe. The affinity context must preserve the original deadline.
	rt.finishRouteAffinityProbe(&affinity, 1, false, nil, time.Now())
	affinity = rt.routeAffinityForRequest(nil, 2, 1, "openai_chat", "m", body)
	got := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &affinity)
	if len(got) != 1 || got[0].Route.ID != 2 {
		t.Fatalf("failed probe should preserve cooldown, got=%+v", got)
	}
	if affinity.shouldRememberRoute(2) {
		t.Fatal("fallback route must not overwrite the cooled affinity")
	}
}

func TestRemoveRecoveryRouteRetries(t *testing.T) {
	plan := []coordinatedRoutePlan{
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}, TryOnRoute: 0},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}, TryOnRoute: 0},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}, TryOnRoute: 1},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}, TryOnRoute: 1},
	}
	got := removeRecoveryRouteRetries(plan, 1)
	if len(got) != 3 || got[0].Candidate.Route.ID != 1 || got[1].Candidate.Route.ID != 2 || got[2].Candidate.Route.ID != 2 {
		t.Fatalf("unexpected recovery plan: %+v", got)
	}
}
