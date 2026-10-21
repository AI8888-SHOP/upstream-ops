package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestCoordinatedPolicyRejectionsSuppressSameRouteRetries(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		validation   validationResult
		wantSuppress bool
	}{
		{
			name:   "zero usage",
			status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "zero_usage", RuleID: 1,
				RuleName: "reject zero usage", MatchedOn: "input_tokens=0;output_tokens=0",
			},
		},
		{
			name:   "response model mismatch",
			status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "response_model", RuleID: 2,
				RuleName: "reject substituted model", MatchedOn: "mini",
			},
		},
		{
			name:   "permission error message over 403",
			status: http.StatusForbidden,
			validation: validationResult{
				Decision: validationRejected, Target: "error_message", RuleID: 3,
				RuleName: "reject denied group", MatchedOn: "无权访问 pro 分组",
			},
			wantSuppress: true,
		},
		{
			name:   "permission error message over 200 stream envelope",
			status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "error_message", RuleID: 4,
				RuleName: "reject denied group", MatchedOn: "permission denied for this group",
			},
			wantSuppress: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := &coordinatedForwardAttempt{
				Status: tc.status, Validation: tc.validation,
				ErrInfo: validationErrorInfo(tc.validation),
			}
			if !coordinatedPolicyRejection(tc.validation) {
				t.Fatal("expected a source-specific pre-commit policy rejection")
			}
			if !coordinatedRejectedUpstreamFailure(attempt) {
				t.Fatal("policy rejection was not classified as an upstream failure")
			}
			if got := coordinatedAttemptSuppressesSameRouteRetries(attempt); got != tc.wantSuppress {
				t.Fatalf("same-route suppression=%v, want %v", got, tc.wantSuppress)
			}
		})
	}
}

func TestCoordinatedPolicyRejectionPrefersReachableAlternative(t *testing.T) {
	group := &storage.GatewayGroup{RetryEnabled: true, RetryCount: 2, ResponseValidationRetryCount: 2}
	withAlternative := buildCoordinatedRoutePlan([]ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1}}, {Route: storage.GatewayRoute{ID: 2}},
	}, group, false, true)
	if !coordinatedPlanHasLaterAlternative(withAlternative, 1, 1) {
		t.Fatal("validation plan did not put another route before same-route retries")
	}
	withoutAlternative := buildCoordinatedRoutePlan([]ScoredRoute{{Route: storage.GatewayRoute{ID: 1}}}, group, false, true)
	if coordinatedPlanHasLaterAlternative(withoutAlternative, 1, 1) {
		t.Fatal("single-route plan exposed a false alternative")
	}
}

func TestCoordinatedPolicyRejectionDoesNotSuppressGenericContentOrPostCommit(t *testing.T) {
	for _, validation := range []validationResult{
		{
			Decision: validationRejected, Target: "error_message",
			RuleName: "custom error rule", MatchedOn: "temporary issue, please retry",
		},
		{
			Decision: validationRejected, Target: "response_model", PostCommit: true,
			RuleName: "late model audit", MatchedOn: "mini",
		},
		{
			Decision: validationRejected, Target: "zero_usage", PostCommit: true,
			RuleName: "late usage audit", MatchedOn: "input_tokens=0;output_tokens=0",
		},
	} {
		if coordinatedPolicyRejection(validation) {
			t.Fatalf("validation should not be a pre-commit policy rejection: %+v", validation)
		}
	}
	if coordinatedAttemptSuppressesSameRouteRetries(&coordinatedForwardAttempt{
		Status: http.StatusOK,
		Validation: validationResult{
			Decision: validationRejected, Target: "error_message",
			RuleName: "custom error rule", MatchedOn: "temporary issue, please retry",
		},
	}) {
		t.Fatal("generic error-message rule rejection unexpectedly suppressed the retry ladder")
	}
}

func TestCoordinatedRetrySuppressionSkipsOnlySameRouteRetrySlots(t *testing.T) {
	var suppressed sync.Map
	suppressed.Store(uint(11), struct{}{})
	for _, tc := range []struct {
		name string
		plan coordinatedRoutePlan
		want bool
	}{
		{name: "primary route can be first attempt", plan: coordinatedRoutePlan{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 11}}}},
		{name: "same route retry is skipped", plan: coordinatedRoutePlan{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 11}}, TryOnRoute: 1}, want: true},
		{name: "other route remains eligible", plan: coordinatedRoutePlan{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 12}}, TryOnRoute: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := coordinatedRetrySuppressed(&suppressed, tc.plan); got != tc.want {
				t.Fatalf("suppressed=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestCoordinatedFailureCooldownScope(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          int
		validation      validationResult
		failure         string
		body            string
		canceled        bool
		localAdapter    bool
		timeoutCooldown bool
		wantScope       string
	}{
		{
			name: "zero usage", status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "zero_usage", RuleName: "reject zero usage",
				MatchedOn: "input_tokens=0;output_tokens=0",
			},
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "response model mismatch", status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "response_model", RuleName: "reject substituted model",
				MatchedOn: "mini",
			},
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "permission rejection over 403", status: http.StatusForbidden,
			validation: validationResult{
				Decision: validationRejected, Target: "error_message", RuleName: "reject denied group",
				MatchedOn: "无权访问 pro 分组",
			},
			body:      `{"error":{"message":"无权访问 pro 分组"}}`,
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "permission rejection over 200", status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "error_message", RuleName: "reject denied group",
				MatchedOn: "permission denied for this group",
			},
			body:      `{"error":{"message":"permission denied for this group"}}`,
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "preconnect terminal identity failure", status: http.StatusOK,
			failure:   "Responses preconnect terminal output identity changed",
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "preconnect invalid terminal output", status: http.StatusOK,
			failure:   "invalid Responses preconnect terminal output",
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "generic bad gateway remains shared", status: http.StatusBadGateway,
			failure:   "temporary gateway failure",
			body:      `{"error":{"message":"temporary gateway failure"}}`,
			wantScope: storage.GatewayModelCooldownScopeShared,
		},
		{
			name: "upstream first token budget with cooldown disabled", status: http.StatusBadGateway,
			failure: "request first token budget exhausted",
			body:    `{"error":{"message":"request first token budget exhausted"}}`,
		},
		{
			name: "upstream first token budget with cooldown enabled", status: http.StatusBadGateway,
			failure: "request first token budget exhausted", timeoutCooldown: true,
			body:      `{"error":{"message":"request first token budget exhausted"}}`,
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "regex first token budget with cooldown disabled", status: http.StatusOK,
			validation: validationResult{Decision: validationRejected, Target: "error_message", RuleName: "timeout", MatchedOn: "request first token budget exhausted"},
			body:       "event: response.failed\ndata: {\"response\":{\"error\":{\"message\":\"request first token budget exhausted\"}}}\n\n",
		},
		{
			name: "regex first token budget with cooldown enabled", status: http.StatusOK,
			validation:      validationResult{Decision: validationRejected, Target: "error_message", RuleName: "timeout", MatchedOn: "request first token budget exhausted"},
			body:            "event: response.failed\ndata: {\"response\":{\"error\":{\"message\":\"request first token budget exhausted\"}}}\n\n",
			timeoutCooldown: true, wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "assistant content rejection", status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "assistant_text", RuleName: "content rule",
				MatchedOn: "permission denied",
			},
			body: `{"choices":[{"message":{"role":"assistant","content":"permission denied"}}]}`,
		},
		{
			name: "raw content rejection", status: http.StatusOK,
			validation: validationResult{
				Decision: validationRejected, Target: "raw_body", RuleName: "content rule",
				MatchedOn: "model not found",
			},
			body: `{"choices":[{"message":{"role":"assistant","content":"model not found"}}]}`,
		},
		{
			name: "canceled preconnect failure does not publish health", status: http.StatusOK,
			failure: "Responses preconnect terminal output identity changed", canceled: true,
		},
		{
			name: "local preconnect failure preserves health after adapter cancellation", status: http.StatusOK,
			failure: "Responses preconnect terminal output identity changed", canceled: true, localAdapter: true,
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
		{
			name: "local preconnect EOF preserves health after adapter cancellation", status: http.StatusOK,
			failure: io.ErrUnexpectedEOF.Error(), canceled: true, localAdapter: true,
			wantScope: storage.GatewayModelCooldownScopeGroup,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openGatewayTestDB(t)
			groups := []storage.GatewayGroup{
				{Name: "validation-group-a", RetryEnabled: true, CooldownSeconds: 60},
				{Name: "validation-group-b", RetryEnabled: true, CooldownSeconds: 60},
			}
			if err := db.Create(&groups).Error; err != nil {
				t.Fatal(err)
			}
			// Set explicitly after Create; GORM may populate boolean defaults.
			groups[0].FirstTokenTimeoutCooldownEnabled = tc.timeoutCooldown
			sourceGroupID := int64(77)
			routeRows := make([]storage.GatewayRoute, len(groups))
			for i := range groups {
				routeRows[i] = storage.GatewayRoute{
					GatewayGroupID: groups[i].ID, Enabled: true, SourceKind: storage.GatewayRouteSourceMonitor,
					SourceChannelID: 12, SourceGroupID: &sourceGroupID, SourceGroupName: "premium",
					SourceAPIKeyID: 501, SourceAPIKeyCipher: "test-cipher",
					UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses,
				}
			}
			if err := db.Create(&routeRows).Error; err != nil {
				t.Fatal(err)
			}
			scope := storage.GatewaySharedModelCooldownScope(&routeRows[0])
			if scope == "" || scope != storage.GatewaySharedModelCooldownScope(&routeRows[1]) {
				t.Fatal("fixture routes must share the same upstream credential and protocol")
			}
			routes := storage.NewGatewayRoutes(db)
			for _, row := range routeRows {
				stored, err := routes.FindByID(row.ID)
				if err != nil || !IsRouteSchedulableForModel(stored, "model-a", time.Now()) {
					t.Fatalf("fixture route %d was not schedulable: %v", row.ID, err)
				}
			}
			runtime := (&Service{Routes: routes}).runtime()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			if tc.canceled {
				requestContext, cancel := context.WithCancel(c.Request.Context())
				c.Request = c.Request.WithContext(requestContext)
				cancel()
			}
			attempt := &coordinatedForwardAttempt{
				Route: routeRows[0], UpstreamModel: "model-a", Status: tc.status,
				Validation: tc.validation, UpstreamBody: []byte(tc.body),
			}
			if tc.validation.IsRejected() {
				attempt.ErrInfo = validationErrorInfo(tc.validation)
			}
			if tc.failure != "" {
				attempt.Err = errors.New(tc.failure)
			}
			if tc.localAdapter {
				writer := &streamPreconnectWriter{protocolErr: attempt.Err}
				c.Request = c.Request.WithContext(context.WithValue(c.Request.Context(), streamPreconnectKey{}, writer))
			}
			request := &coordinatedForwardRequest{
				c: c, group: &groups[0], kind: protocol.KindOpenAIResponses,
				path: "/v1/responses", requestID: "cooldown-scope-test",
			}

			runtime.publishCoordinatedFailure(request, attempt, nil, 1)
			wantHealth := tc.wantScope != ""
			if attempt.healthRecorded != wantHealth || (attempt.UsageMeta.CooldownUntil != nil) != wantHealth {
				t.Fatalf("health recorded=%v, cooldown=%v, want published=%v", attempt.healthRecorded, attempt.UsageMeta.CooldownUntil, wantHealth)
			}
			var sharedCount int64
			if err := db.Model(&storage.GatewaySharedModelCooldown{}).Count(&sharedCount).Error; err != nil {
				t.Fatal(err)
			}
			wantSharedCount := int64(0)
			if tc.wantScope == storage.GatewayModelCooldownScopeShared {
				wantSharedCount = 1
			}
			if sharedCount != wantSharedCount {
				t.Fatalf("shared cooldown rows=%d, want %d", sharedCount, wantSharedCount)
			}
			var sharedCooldownID uint
			for i, row := range routeRows {
				stored, err := routes.FindByID(row.ID)
				if err != nil {
					t.Fatal(err)
				}
				wantBlocked := wantHealth && (i == 0 || tc.wantScope == storage.GatewayModelCooldownScopeShared)
				if got := IsRouteSchedulableForModel(stored, "model-a", time.Now()); got == wantBlocked {
					t.Fatalf("group %d model-a schedulable=%v, want %v", i, got, !wantBlocked)
				}
				if !IsRouteSchedulableForModel(stored, "other-model", time.Now()) {
					t.Fatalf("group %d cooldown leaked to another model", i)
				}
				cooldown, exists := stored.ModelCooldowns["model-a"]
				if exists != wantBlocked {
					t.Fatalf("group %d has model cooldown=%v, want %v", i, exists, wantBlocked)
				}
				if !wantBlocked {
					continue
				}
				if cooldown.CooldownScope != tc.wantScope {
					t.Fatalf("group %d scope=%q, want %q", i, cooldown.CooldownScope, tc.wantScope)
				}
				if tc.wantScope == storage.GatewayModelCooldownScopeGroup && cooldown.SharedCooldownID != 0 {
					t.Fatalf("group policy unexpectedly projected shared cooldown %d", cooldown.SharedCooldownID)
				}
				if tc.wantScope == storage.GatewayModelCooldownScopeShared {
					if cooldown.SharedCooldownID == 0 || (i > 0 && cooldown.SharedCooldownID != sharedCooldownID) {
						t.Fatalf("same credentials did not share one model cooldown: %d and %d", sharedCooldownID, cooldown.SharedCooldownID)
					}
					sharedCooldownID = cooldown.SharedCooldownID
				}
			}
		})
	}
}
