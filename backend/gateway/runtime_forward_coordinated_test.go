package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestWriteCoordinatedFailureDoesNotReplayRejectedResponse(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	req := &coordinatedForwardRequest{
		c: context, kind: protocolOpenAI, requestID: "req-rejected",
	}
	rejection := validationResult{
		Decision: validationRejected, RuleID: 7, RuleName: "blocked output",
		Target: "assistant_text", Pattern: "blocked",
	}
	attempt := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{Number: 1}, Route: storage.GatewayRoute{ID: 1},
		Status: http.StatusOK, ClientBody: []byte(`{"choices":[{"message":{"content":"blocked"}}]}`),
		Validation: rejection,
	}
	result := hedgeRunResult[*coordinatedForwardAttempt]{
		Attempts: []hedgeAttemptResult[*coordinatedForwardAttempt]{{
			Info: attempt.Info, Value: attempt, Outcome: hedgeOutcomeRejected,
		}},
	}

	(&Runtime{Service: &Service{}}).writeCoordinatedFailure(req, result, errHedgeExhausted)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if strings.Contains(recorder.Body.String(), `"content":"blocked"`) {
		t.Fatalf("rejected response was replayed: %s", recorder.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode gateway error: %v", err)
	}
	if !strings.Contains(recorder.Body.String(), "blocked output") {
		t.Fatalf("validation rule missing from gateway error: %s", recorder.Body.String())
	}
}

func TestWriteCoordinatedFailureDoesNotReplayPostCommitMatch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	req := &coordinatedForwardRequest{
		c: context, kind: protocolOpenAI, requestID: "req-post-commit",
	}
	postCommit := validationResult{
		Decision: validationRejected, RuleID: 8, RuleName: "late blocked output",
		Target: "assistant_text", Pattern: "blocked", PostCommit: true,
	}
	attempt := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{Number: 1}, Route: storage.GatewayRoute{ID: 1},
		Status: http.StatusOK, ClientBody: []byte(`{"choices":[{"message":{"content":"blocked"}}]}`),
		Validation: postCommit,
	}
	result := hedgeRunResult[*coordinatedForwardAttempt]{
		Attempts: []hedgeAttemptResult[*coordinatedForwardAttempt]{{
			Info: attempt.Info, Value: attempt, Outcome: hedgeOutcomeRejected,
		}},
	}

	(&Runtime{Service: &Service{}}).writeCoordinatedFailure(req, result, errHedgeExhausted)

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status=%d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if strings.Contains(recorder.Body.String(), `"content":"blocked"`) {
		t.Fatalf("post-commit rejected response was replayed: %s", recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), "late blocked output") {
		t.Fatalf("validation rule missing from gateway error: %s", recorder.Body.String())
	}
}

func TestBuildCoordinatedRoutePlanValidationSwitchesWithoutFailoverToggle(t *testing.T) {
	group := &storage.GatewayGroup{
		RetryEnabled:     false,
		FailoverEnabled:  false,
		FailoverMax:      0,
	}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1}},
		{Route: storage.GatewayRoute{ID: 2}},
		{Route: storage.GatewayRoute{ID: 3}},
	}
	plan := buildCoordinatedRoutePlan(candidates, group, false, true)
	if len(plan) != len(candidates) {
		t.Fatalf("validation plan length=%d, want %d", len(plan), len(candidates))
	}
	for i, entry := range plan {
		if entry.Candidate.Route.ID != candidates[i].Route.ID {
			t.Fatalf("plan[%d] route=%d, want %d", i, entry.Candidate.Route.ID, candidates[i].Route.ID)
		}
	}
}

func TestBuildCoordinatedRoutePlanValidationIgnoresZeroTransportBudget(t *testing.T) {
	group := &storage.GatewayGroup{
		RetryEnabled:    true,
		RetryCount:      2,
		FailoverEnabled: true,
		FailoverMax:     0,
	}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1}},
		{Route: storage.GatewayRoute{ID: 2}},
	}
	plan := buildCoordinatedRoutePlan(candidates, group, false, true)
	if len(plan) != 6 {
		t.Fatalf("validation plan length=%d, want 6", len(plan))
	}
	wantRoutes := []uint{1, 1, 1, 2, 2, 2}
	for i, entry := range plan {
		if entry.Candidate.Route.ID != wantRoutes[i] || entry.TryOnRoute != i%3 || entry.MaxTries != 3 {
			t.Fatalf("plan[%d]=route %d try %d max %d, want route %d try %d max 3", i,
				entry.Candidate.Route.ID, entry.TryOnRoute, entry.MaxTries, wantRoutes[i], i%3)
		}
	}
}

func TestCoordinatedTransportFailurePolicyKeepsRetryIndependent(t *testing.T) {
	group := &storage.GatewayGroup{
		RetryEnabled:    true,
		RetryCount:      1,
		FailoverEnabled: false,
	}
	first := &coordinatedForwardAttempt{Plan: coordinatedRoutePlan{TryOnRoute: 0, MaxTries: 2}}
	last := &coordinatedForwardAttempt{Plan: coordinatedRoutePlan{TryOnRoute: 1, MaxTries: 2}}
	if coordinatedTransportFailoverEnabled(group) {
		t.Fatal("transport failover unexpectedly enabled")
	}
	if !coordinatedSameRouteRetryAllowed(group, first) {
		t.Fatal("same-route retry unexpectedly disabled")
	}
	if coordinatedSameRouteRetryAllowed(group, last) {
		t.Fatal("same-route retry unexpectedly allowed after retry budget")
	}
}

func TestCoordinatedAttemptFirstTokenTimeoutFollowsTransportRoutes(t *testing.T) {
	plan := []coordinatedRoutePlan{
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}, TryOnRoute: 0, MaxTries: 2},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}, TryOnRoute: 1, MaxTries: 2},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}, TryOnRoute: 0, MaxTries: 2},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}, TryOnRoute: 1, MaxTries: 2},
	}
	configured := 7 * time.Second
	transportGroup := &storage.GatewayGroup{RetryEnabled: true, FailoverEnabled: true, FailoverMax: 1}
	for _, number := range []int{1, 2} {
		if got := coordinatedAttemptFirstTokenTimeout(configured, transportGroup, false, plan, number, 1); got != configured {
			t.Fatalf("route 1 attempt %d timeout=%s, want %s", number, got, configured)
		}
	}
	for _, number := range []int{3, 4} {
		if got := coordinatedAttemptFirstTokenTimeout(configured, transportGroup, false, plan, number, 2); got != 0 {
			t.Fatalf("last route attempt %d timeout=%s, want disabled", number, got)
		}
	}
	noTransportFailover := &storage.GatewayGroup{RetryEnabled: true, FailoverEnabled: false, FailoverMax: 0}
	if got := coordinatedAttemptFirstTokenTimeout(configured, noTransportFailover, false, plan, 1, 1); got != 0 {
		t.Fatalf("validation-only timeout=%s, want disabled", got)
	}
	if got := coordinatedAttemptFirstTokenTimeout(configured, transportGroup, true, plan, 1, 1); got != configured {
		t.Fatalf("hedge primary timeout=%s, want %s", got, configured)
	}
	if got := coordinatedAttemptFirstTokenTimeout(configured, transportGroup, true, plan, len(plan), 2); got != 0 {
		t.Fatalf("last hedge timeout=%s, want disabled", got)
	}
}
