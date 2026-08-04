package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
