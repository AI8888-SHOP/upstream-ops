package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestSourceFailureClassificationUsesOnlyErrorEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name, body  string
		status      int
		unavailable bool
	}{
		{"model code", `{"error":{"code":"model_not_found","type":"invalid_request_error","message":"model missing"}}`, 400, true},
		{"model unavailable", `{"error":{"message":"The requested model is unavailable. Please select a supported model."}}`, 400, true},
		{"model access", `{"error":{"message":"The model 'gpt-test' does not exist or you do not have access to it."}}`, 400, true},
		{"model group", `{"error":{"message":"Model gpt-test is not supported by any configured account in this group"}}`, 404, true},
		{"balance string", `{"error":"Insufficient account balance"}`, 403, true},
		{"key", `{"error":{"code":"invalid_api_key"}}`, 401, true},
		{"nested model", `{"type":"response.failed","response":{"error":{"code":"model_not_found"}}}`, 400, true},
		{"SSE model", "event: response.failed\ndata: {\"response\":{\"error\":{\"code\":\"model_not_found\"}}}\n\n", 404, true},
		{"invalid content", `{"error":{"type":"invalid_request_error","message":"input_item_not_supported"}}`, 400, false},
		{"length", `{"error":{"code":"context_length_exceeded","message":"model input too long"}}`, 400, false},
		{"generic 404", `{"error":{"message":"Not found"}}`, 404, false},
		{"null error", `{"error":null}`, 400, false},
		{"assistant prose", `{"choices":[{"message":{"content":"model_not_found and insufficient balance"}}]}`, 400, false},
		{"tool arguments", `{"output":[{"type":"function_call","arguments":"{\"error\":{\"code\":\"model_not_found\"}}"}]}`, 400, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			if got := upstreamRouteUnavailable(body); got != tc.unavailable {
				t.Fatalf("unavailable=%v want=%v", got, tc.unavailable)
			}
			if got := (*Service)(nil).isFailoverResponse(tc.status, false, body); got != tc.unavailable {
				t.Fatalf("failover=%v want=%v", got, tc.unavailable)
			}
			info := (*Service)(nil).buildUpstreamErrorInfo(nil, tc.status, nil, body, "", http.MethodPost)
			if tc.unavailable && (info.Type != "upstream_error" || isSameRouteRetryableUpstreamFailure(tc.status, info) || !hasStructuredUpstreamError(body)) {
				t.Fatalf("source-local error was not classified consistently: %+v", info)
			}
		})
	}
}

func TestAvailabilityPlanVisitsAlternativesBeforeSameSourceRetries(t *testing.T) {
	group := &storage.GatewayGroup{RetryEnabled: true, RetryCount: 2, ResponseValidationRetryCount: 2, FailoverEnabled: true, FailoverMax: 8, RequestMaxAttempts: 2}
	candidates := []ScoredRoute{{Route: storage.GatewayRoute{ID: 1}}, {Route: storage.GatewayRoute{ID: 2}}, {Route: storage.GatewayRoute{ID: 3}}}
	for _, validation := range []bool{false, true} {
		plan := buildCoordinatedRoutePlan(candidates, group, false, validation)
		if len(plan) != 9 {
			t.Fatalf("plan size=%d", len(plan))
		}
		for i, entry := range plan {
			if entry.Candidate.Route.ID != uint(i%3+1) || entry.TryOnRoute != i/3 {
				t.Fatalf("plan[%d]=%+v; fresh routes must precede retries", i, entry)
			}
		}
		if !coordinatedPreferUntriedAlternative(group, plan, 1, 1) || !coordinatedPreferUntriedAlternative(group, plan, 2, 2) || coordinatedPreferUntriedAlternative(group, plan, 3, 3) {
			t.Fatal("only failures with an untried alternative should skip same-source retries")
		}
	}
}

func TestRemainingAttemptsNeverResetsOrExtendsBudget(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	finish := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocolOpenAI, true, time.Minute)
	defer finish()
	ctx := c.Request.Context()
	if requestAttemptsRemaining(ctx, 0) != -1 || requestAttemptsRemaining(ctx, 2) != 2 {
		t.Fatal("incorrect initial limit")
	}
	for remaining := 1; remaining >= 0; remaining-- {
		if err := claimRequestAttempt(ctx, 2); err != nil || requestAttemptsRemaining(ctx, 2) != remaining {
			t.Fatalf("remaining=%d err=%v", requestAttemptsRemaining(ctx, 2), err)
		}
	}
	if err := claimRequestAttempt(ctx, 2); !errors.Is(err, errRequestAttemptLimit) || requestAttemptsRemaining(ctx, 2) != 0 {
		t.Fatalf("budget exceeded: %v", err)
	}
}

func TestFirstTokenTimeoutIgnoresSuppressedAlternatives(t *testing.T) {
	group := &storage.GatewayGroup{RetryEnabled: true, FailoverEnabled: true, FailoverMax: 2}
	plan := []coordinatedRoutePlan{
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 1}}, TryOnRoute: 1},
		{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: 2}}, TryOnRoute: 1},
	}
	eligible := func(entry coordinatedRoutePlan) bool { return entry.Candidate.Route.ID != 1 }
	if got := coordinatedAttemptFirstTokenTimeout(time.Second, group, false, plan, 2, 2, eligible); got != 0 {
		t.Fatalf("suppressed route armed a pointless failover timer: %s", got)
	}
	// A retry on the last source is not an alternative transport route, but
	// remains an available attempt under the explicit hedge policy.
	if got := coordinatedAttemptFirstTokenTimeout(time.Second, group, true, plan, 2, 2, eligible); got != time.Second {
		t.Fatalf("eligible hedge retry lost its timer: %s", got)
	}
	if got := coordinatedAttemptFirstTokenTimeout(time.Second, group, true, plan, 2, 2, func(coordinatedRoutePlan) bool { return false }); got != 0 {
		t.Fatalf("exhausted hedge plan armed a pointless timer: %s", got)
	}
}

func TestAvailabilityPublishesHealthWithoutWaitingForUnusableRetries(t *testing.T) {
	for _, alternative := range []bool{false, true} {
		t.Run(fmt.Sprintf("alternative=%v", alternative), func(t *testing.T) {
			db := openGatewayTestDB(t)
			group := &storage.GatewayGroup{Name: "budget-health", RetryEnabled: true, FailoverEnabled: alternative, FailoverMax: 1, CooldownSeconds: 60}
			if err := db.Create(group).Error; err != nil {
				t.Fatal(err)
			}
			// GORM defaults may populate false booleans on insert.
			group.FailoverEnabled = alternative
			route := storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1, SourceAPIKeyCipher: "test-cipher"}
			if err := db.Create(&route).Error; err != nil {
				t.Fatal(err)
			}
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			finish := beginForwardRequest(c, time.Now(), group, protocol.KindOpenAIResponses, true, time.Minute)
			defer finish()
			if !alternative {
				group.RequestMaxAttempts = 1
				if err := claimRequestAttempt(c.Request.Context(), 1); err != nil {
					t.Fatal(err)
				}
			}
			routes := storage.NewGatewayRoutes(db)
			rt := (&Service{Routes: routes}).runtime()
			req := &coordinatedForwardRequest{c: c, group: group, kind: protocol.KindOpenAIResponses, path: "/v1/responses", requestID: "budget-health"}
			plan := []coordinatedRoutePlan{{Candidate: ScoredRoute{Route: route}}}
			if alternative {
				plan = append(plan, coordinatedRoutePlan{Candidate: ScoredRoute{Route: storage.GatewayRoute{ID: route.ID + 1}}})
			}
			plan = append(plan, coordinatedRoutePlan{Candidate: ScoredRoute{Route: route}, TryOnRoute: 1})
			attempt := &coordinatedForwardAttempt{Route: route, UpstreamModel: "m", Status: 502, Err: errors.New("bad gateway"), ErrInfo: usageErrorInfo{Type: "http", Summary: "bad gateway"}}
			rt.publishCoordinatedFailure(req, attempt, plan, 1)
			if !attempt.healthRecorded || attempt.UsageMeta.CooldownUntil == nil {
				t.Fatal("unusable same-route retries delayed health publication")
			}
			stored, err := routes.FindByID(route.ID)
			if err != nil || IsRouteSchedulableForModel(stored, "m", time.Now()) {
				t.Fatalf("failed model remained schedulable: %v", err)
			}
			callerError := &coordinatedForwardAttempt{Route: route, UpstreamModel: "other", Status: 400, Terminal: true}
			rt.publishCoordinatedFailure(req, callerError, nil, 1)
			if callerError.healthRecorded {
				t.Fatal("ordinary caller error cooled an unrelated model")
			}
		})
	}
}

func TestAdaptiveOutageDoesNotBorrowOldReliability(t *testing.T) {
	rt, _, req, candidates := adaptiveFixture()
	now := time.Now()
	seedAdaptive(rt, req, candidates[0], now.Add(-10*time.Minute), 1000, 1000, 0)
	seedAdaptive(rt, req, candidates[0], now, 1000, 30, 30)
	seedAdaptive(rt, req, candidates[1], now, 8000, 30, 0)
	estimate := rt.schedulingStatistics().estimate(req.key(candidates[0].Route), now, 5, 10)
	if estimate.Window != 60 || estimate.FailureWindow != 5 || estimate.Samples != 30 || estimate.FailureRate < 0.7 {
		t.Fatalf("old successes hid current outage: %+v", estimate)
	}
	ordered := rt.orderAdaptiveCandidates(candidates, req, nil, false, now)
	if ordered[0].Route.ID != candidates[1].Route.ID {
		t.Fatalf("outage source retained priority: %+v", ordered)
	}
}

func TestAdaptivePrefersReliableRouteOverFastFailingRoute(t *testing.T) {
	rt, group, req, candidates := adaptiveFixture()
	group.SchedulingMode = "latency"
	now := time.Now()
	seedAdaptive(rt, req, candidates[0], now, 3800, 37, 19)
	seedAdaptive(rt, req, candidates[1], now, 8800, 249, 4)
	ordered := rt.orderAdaptiveCandidates(candidates, req, nil, false, now)
	if ordered[0].Route.ID != candidates[1].Route.ID {
		t.Fatal("fast but unreliable source beat reliable source")
	}
}

func TestAdaptiveFailureWaitAndSource400AffectReliability(t *testing.T) {
	rt, group, req, candidates := adaptiveFixture()
	group.SchedulingMode = "latency"
	now := time.Now()
	for _, candidate := range candidates[:2] {
		seedAdaptive(rt, req, candidate, now, 5000, 100, 5)
	}
	failed := true
	rt.schedulingStatistics().record(req.key(candidates[0].Route), now, nil, &failed, 120000)
	rt.schedulingStatistics().record(req.key(candidates[1].Route), now, nil, &failed, 1000)
	// Six failures, five with no duration, still distinguish expensive failures
	// in the observed diagnostic. Seed more long failures for scoring.
	for i := 0; i < 5; i++ {
		rt.schedulingStatistics().record(req.key(candidates[0].Route), now, nil, &failed, 120000)
		rt.schedulingStatistics().record(req.key(candidates[1].Route), now, nil, &failed, 1000)
	}
	ordered := rt.orderAdaptiveCandidates(candidates, req, nil, false, now)
	if ordered[0].Route.ID != candidates[1].Route.ID || ordered[1].Decision.FailureWaitMS < 60000 {
		t.Fatalf("failure wait not penalized: %+v", ordered)
	}
	candidate := ordered[0]
	before := rt.schedulingStatistics().estimate(candidate.StatisticsKey, now, 5, 1).Samples
	o := rt.newSchedulerObservation(candidate, now.Add(-time.Second))
	o.markUpstreamStarted()
	o.finish(context.Background(), false, "upstream_error", "error", 400)
	after := rt.schedulingStatistics().estimate(candidate.StatisticsKey, time.Now(), 5, 1).Samples
	if after != before+1 {
		t.Fatal("model-unavailable 400 was ignored as caller input error")
	}
}

// Real forwarding paths must reach a healthy alternative within a two-attempt
// budget, without dropping validation, leaking an error prefix, or double billing.
func TestAvailabilityFailoverThroughBothGatewayPaths(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			for _, tc := range []struct {
				name   string
				status int
				body   string
			}{
				{"bad-gateway", 502, `{"error":{"message":"bad gateway"}}`},
				{"busy", 503, `{"error":{"message":"busy"}}`},
				{"model-400", 400, `{"error":{"code":"model_not_found","message":"The requested model is unavailable"}}`},
				{"model-404", 404, `{"error":{"message":"Model gpt-test is not supported by any configured account in this group"}}`},
				{"balance", 403, `{"error":{"message":"Insufficient account balance"}}`},
				{"invalid-input-terminal", 400, `{"error":{"type":"invalid_request_error","message":"input_item_not_supported"}}`},
				{"empty-input-terminal", 400, ""},
			} {
				t.Run(fmt.Sprintf("coordinated=%v/stream=%v/%s", coordinated, stream, tc.name), func(t *testing.T) {
					db := openGatewayTestDB(t)
					cipher, err := crypto.NewCipher("availability-regression")
					if err != nil {
						t.Fatal(err)
					}
					secret, err := cipher.Encrypt("test-key")
					if err != nil {
						t.Fatal(err)
					}
					groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
					group := &storage.GatewayGroup{Name: "availability", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
						RetryEnabled: true, RetryCount: 2, FailoverEnabled: true, FailoverMax: 8, RequestMaxAttempts: 2,
						ResponseValidationEnabled: coordinated, ResponseValidationRetryCount: 2, ResponseValidationStreamMode: "prefix", RequestFirstTokenTimeoutSec: 5, CooldownSeconds: 60}
					if err := groups.Create(group); err != nil {
						t.Fatal(err)
					}
					key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive}
					if err := keys.Create(key); err != nil {
						t.Fatal(err)
					}
					rules := storage.NewGatewayResponseRules(db)
					if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "unused-rule", Enabled: true, Target: "error_message", Pattern: "never-match-this-error"}); err != nil {
						t.Fatal(err)
					}
					providers := storage.NewGatewayProviders(db)
					var calls [2]atomic.Int32
					var inputs []storage.GatewayRoute
					for index := 0; index < 2; index++ {
						index := index
						upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
							calls[index].Add(1)
							if index == 0 {
								w.Header().Set("Content-Type", "application/json")
								w.WriteHeader(tc.status)
								_, _ = io.WriteString(w, tc.body)
								return
							}
							if stream {
								w.Header().Set("Content-Type", "text/event-stream")
							} else {
								w.Header().Set("Content-Type", "application/json")
							}
							body := zeroUsageTestBody(protocol.KindOpenAIResponses, stream, "fallback answer", 2, true, true)
							body = strings.ReplaceAll(body, `"input_tokens":0`, `"input_tokens":10`)
							_, _ = io.WriteString(w, body)
						}))
						t.Cleanup(upstream.Close)
						provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
						if err := providers.Create(provider); err != nil {
							t.Fatal(err)
						}
						rate := float64(index+1) / 10
						inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
					}
					if err := routes.SaveForGroup(group.ID, inputs); err != nil {
						t.Fatal(err)
					}
					svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
					svc.SetProviders(providers)
					svc.SetResponseRules(rules)
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"gpt-test","stream":%v,"input":"hi"}`, stream)))
					c.Request.Header.Set("Authorization", "Bearer client-key")
					svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
					if tc.name == "invalid-input-terminal" || tc.name == "empty-input-terminal" {
						if recorder.Code != 400 || calls[0].Load() != 1 || calls[1].Load() != 0 || (tc.body != "" && !strings.Contains(recorder.Body.String(), "input_item_not_supported")) {
							t.Fatalf("caller error was retried: status=%d calls=%d/%d body=%s", recorder.Code, calls[0].Load(), calls[1].Load(), recorder.Body.String())
						}
						var logs []storage.GatewayUsageLog
						if err := db.Find(&logs).Error; err != nil {
							t.Fatal(err)
						}
						if len(logs) != 1 || logs[0].Success || logs[0].Winner || logs[0].CooldownUntil != nil {
							t.Fatalf("caller error was successful or cooled the model: %+v", logs)
						}
						return
					}
					if recorder.Code != 200 || calls[0].Load() != 1 || calls[1].Load() != 1 || !strings.Contains(recorder.Body.String(), "fallback answer") {
						t.Fatalf("failed to reach alternative: status=%d calls=%d/%d body=%s", recorder.Code, calls[0].Load(), calls[1].Load(), recorder.Body.String())
					}
					var logs []storage.GatewayUsageLog
					if err := db.Order("attempt").Find(&logs).Error; err != nil {
						t.Fatal(err)
					}
					if len(logs) != 2 || logs[0].Winner || logs[0].Success || !logs[1].Winner || !logs[1].Success || logs[0].RouteID == logs[1].RouteID {
						t.Fatalf("bad attempt audit: %+v", logs)
					}
					if logs[0].CooldownUntil == nil {
						t.Fatal("failed source was not cooled")
					}
				})
			}
		}
	}
}
