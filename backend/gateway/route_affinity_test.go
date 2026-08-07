package gateway

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestRequestRouteAffinityIncrementalHashMatchesLegacy(t *testing.T) {
	items := []any{
		map[string]any{"role": "system", "content": "system"},
		map[string]any{"role": "developer", "content": "developer"},
		map[string]any{"role": "tool", "content": "tool"},
	}
	for index := 0; index < 80; index++ {
		role := "user"
		if index%2 == 1 {
			role = "assistant"
		}
		items = append(items, map[string]any{
			"role":    role,
			"content": "turn-" + string(rune('a'+index%26)),
		})
	}
	payload := map[string]any{
		"model":        "m",
		"system":       "system prompt",
		"instructions": "developer instruction",
		"tools":        []any{map[string]any{"name": "lookup"}},
		"messages":     items,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	want := legacyRouteAffinityFingerprints("messages", payload, items, "m")
	got := requestRouteAffinityFingerprints(nil, body, "m")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("incremental fingerprints differ from legacy algorithm:\n got: %v\nwant: %v", got, want)
	}
	if len(got) != maxRouteAffinityPrefixes {
		t.Fatalf("fingerprint count=%d, want bounded tail %d", len(got), maxRouteAffinityPrefixes)
	}
}

func legacyRouteAffinityFingerprints(field string, payload map[string]any, items []any, model string) []string {
	static := map[string]any{"model": model}
	for _, key := range []string{"system", "instructions", "tools"} {
		if value, ok := payload[key]; ok {
			static[key] = value
		}
	}
	staticJSON, err := json.Marshal(static)
	if err != nil {
		return nil
	}
	hashInput := make([]byte, 0, len(staticJSON)+len(field)+len(items)*32+16)
	hashInput = append(hashInput, ("body:" + field + "\x00")...)
	hashInput = append(hashInput, staticJSON...)
	conversationSeen := false
	prefixes := make([]string, 0, minInt(len(items), maxRouteAffinityPrefixes))
	for _, item := range items {
		itemJSON, err := json.Marshal(item)
		if err != nil {
			return nil
		}
		hashInput = append(hashInput, 0)
		hashInput = append(hashInput, itemJSON...)
		conversationSeen = conversationSeen || conversationItemHasTurn(item)
		if !conversationSeen {
			continue
		}
		sum := sha256.Sum256(hashInput)
		fingerprint := hex.EncodeToString(sum[:])
		if len(prefixes) == maxRouteAffinityPrefixes {
			copy(prefixes, prefixes[1:])
			prefixes[len(prefixes)-1] = fingerprint
		} else {
			prefixes = append(prefixes, fingerprint)
		}
	}
	result := make([]string, 0, len(prefixes))
	for index := len(prefixes) - 1; index >= 0; index-- {
		result = appendUniqueString(result, prefixes[index])
	}
	return result
}

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

func TestRequestRouteAffinityPrefersExplicitSessionHeader(t *testing.T) {
	c := &gin.Context{}
	// The body deliberately contains a conversation and a different body ID.
	// A stable session header is authoritative and must avoid the expensive
	// full-body affinity scan.
	c.Request = &http.Request{Header: make(http.Header)}
	c.Request.Header.Set("X-Session-ID", "session-id")

	got := requestRouteAffinityFingerprints(c, []byte(`{"conversation_id":"body-id","messages":[{"role":"user","content":"hello"}]}`), "m")
	want := []string{routeAffinityDigest("header:x-session-id\x00session-id")}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("header fingerprints=%v, want %v", got, want)
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

func TestRouteAffinityKeepsNewlyCooledPreferredRoute(t *testing.T) {
	svc := &Service{}
	rt := svc.runtime()
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"one"}]}`)
	keys := rt.routeAffinityForRequest(nil, 3, 1, "openai_chat", "m", body).Keys
	rt.rememberRouteAffinity(keys, 1, time.Now())

	affinity := rt.routeAffinityForRequest(nil, 3, 1, "openai_chat", "m", body)
	if !affinity.shouldRememberRoute(2) {
		t.Fatal("fallback route should be rememberable before the preferred route cools")
	}
	affinity.preservePreferredOnCooldown(1)
	if affinity.shouldRememberRoute(2) {
		t.Fatal("fallback route must not overwrite a newly cooled preferred route")
	}
	if affinity.shouldRememberRoute(0) {
		t.Fatal("an empty route must not replace the preserved preferred route")
	}
	if !affinity.shouldRememberRoute(1) {
		t.Fatal("the original preferred route must remain rememberable")
	}

	// Mirror fallback settlement: route 2 succeeds, but its winner state must
	// not replace route 1 in the shared affinity map.
	if affinity.shouldRememberRoute(2) {
		rt.rememberRouteAffinity(affinity.Keys, 2, time.Now())
	}
	next := rt.routeAffinityForRequest(nil, 3, 1, "openai_chat", "m", body)
	if next.PreferredRouteID != 1 {
		t.Fatalf("preferred route=%d, want 1 after fallback success", next.PreferredRouteID)
	}

	until := time.Now().Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, GatewayGroupID: 1, Position: 0, Enabled: true, SourceAPIKeyCipher: "a", ModelCooldowns: map[string]storage.GatewayRouteModelCooldown{
			"m": {RouteID: 1, Model: "m", TempUnschedulableUntil: &until},
		}},
		{ID: 2, GatewayGroupID: 1, Position: 1, Enabled: true, SourceAPIKeyCipher: "b"},
	}
	candidates := rt.sortRoutesWithAffinity(routes, nil, "asc", time.Now(), nil, &next, "m")
	if len(candidates) != 2 || candidates[0].Route.ID != 1 || !next.Recovery {
		t.Fatalf("next request did not probe cooled preferred route first: candidates=%+v affinity=%+v", candidates, next)
	}
	recoveryCooldown, ok := candidates[0].Route.ModelCooldowns["m"]
	if !ok || recoveryCooldown.TempUnschedulableUntil != nil {
		t.Fatalf("recovery route did not retain a schedulable cooldown snapshot: %+v", candidates[0].Route.ModelCooldowns)
	}
	if recoveryCooldown.Model != "m" {
		t.Fatalf("recovery route lost cooldown generation metadata: %+v", recoveryCooldown)
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
