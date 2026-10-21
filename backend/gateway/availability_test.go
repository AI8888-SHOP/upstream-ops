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

func TestSourceFailureClassificationRequiresErrorEnvelopeOrHTTPFailure(t *testing.T) {
	for _, tc := range []struct {
		name, body  string
		status      int
		unavailable bool
		structured  bool
	}{
		{"model code", `{"error":{"code":"model_not_found","type":"invalid_request_error","message":"model missing"}}`, 400, true, true},
		{"model unavailable", `{"error":{"message":"The requested model is unavailable. Please select a supported model."}}`, 400, true, true},
		{"model access", `{"error":{"message":"The model 'gpt-test' does not exist or you do not have access to it."}}`, 400, true, true},
		{"model group", `{"error":{"message":"Model gpt-test is not supported by any configured account in this group"}}`, 404, true, true},
		{"balance string", `{"error":"Insufficient account balance"}`, 403, true, true},
		{"media capability", `{"error":{"message":"Image generation is not enabled for this group"}}`, 403, true, true},
		{"Chinese JSON permission", `{"error":{"message":"无权访问 pro 分组"}}`, 403, true, true},
		{"Chinese group permission", `无权访问 pro 分组`, 403, true, false},
		{"deleted group", `API Key 所属分组已删除`, 403, true, false},
		{"Chinese missing model", `模型不存在`, 404, true, false},
		{"Chinese model not found", `找不到模型 gpt-test`, 404, true, false},
		{"numeric error code", `{"error":{"code":404,"message":"model not found"}}`, 404, true, true},
		{"numeric code and permission", `{"error":{"code":403,"message":"无权访问 pro 分组"}}`, 403, true, true},
		{"numeric code and invalid input", `{"error":{"code":400,"type":"invalid_request_error","message":"invalid input"}}`, 400, false, false},
		{"key", `{"error":{"code":"invalid_api_key"}}`, 401, true, true},
		{"nested model", `{"type":"response.failed","response":{"error":{"code":"model_not_found"}}}`, 400, true, true},
		{"SSE model", "event: response.failed\ndata: {\"response\":{\"error\":{\"code\":\"model_not_found\"}}}\n\n", 404, true, true},
		{"SSE id before error", "id: 1\nevent: error\ndata: {\"error\":{\"code\":\"model_not_found\"}}\n\n", 404, true, true},
		{"SSE retry before error", "retry: 1000\nevent: error\ndata: {\"error\":{\"code\":404,\"message\":\"model not found\"}}\n\n", 404, true, true},
		{"invalid content", `{"error":{"type":"invalid_request_error","message":"input_item_not_supported"}}`, 400, false, false},
		{"length", `{"error":{"code":"context_length_exceeded","message":"model input too long"}}`, 400, false, false},
		{"generic 404", `{"error":{"message":"Not found"}}`, 404, false, false},
		{"null error", `{"error":null}`, 400, false, false},
		{"assistant prose", `{"choices":[{"message":{"content":"model_not_found and insufficient balance"}}]}`, 400, false, false},
		{"tool arguments", `{"output":[{"type":"function_call","arguments":"{\"error\":{\"code\":\"model_not_found\"}}"}]}`, 400, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			if got := upstreamRouteUnavailable(body, tc.status >= http.StatusBadRequest); got != tc.unavailable {
				t.Fatalf("unavailable=%v want=%v", got, tc.unavailable)
			}
			if got := upstreamRouteUnavailable(body); got != tc.structured {
				t.Fatalf("envelope-only unavailable=%v want=%v", got, tc.structured)
			}
			if got := hasStructuredUpstreamError(body); got != tc.structured {
				t.Fatalf("structured=%v want=%v", got, tc.structured)
			}
			if got := (*Service)(nil).isFailoverResponse(tc.status, false, body); !got {
				t.Fatalf("upstream HTTP %d was not classified for route failover", tc.status)
			}
			info := (*Service)(nil).buildUpstreamErrorInfo(nil, tc.status, nil, body, "", http.MethodPost)
			wantType := "http"
			if tc.unavailable {
				wantType = "upstream_error"
			}
			if info.Type != wantType {
				t.Fatalf("error type=%q want=%q", info.Type, wantType)
			}
			if tc.unavailable && isSameRouteRetryableUpstreamFailure(tc.status, info) {
				t.Fatalf("source-local error was not classified consistently: %+v", info)
			}
		})
	}
}

func TestSourceFailureClassificationDoesNotScanSuccessfulContent(t *testing.T) {
	prose := "permission denied; 无权访问 pro 分组; 模型不存在; request first token budget exhausted; upstream stream idle for 2m0s; Responses preconnect terminal output identity changed"
	arguments := fmt.Sprintf(`{"error":{"message":%q}}`, prose)
	for _, tc := range []struct {
		name, body string
	}{
		{"assistant prose", fmt.Sprintf(`{"choices":[{"message":{"content":%q}}]}`, prose)},
		{"tool arguments", fmt.Sprintf(`{"output":[{"type":"function_call","arguments":%q}]}`, arguments)},
		{"SSE assistant prose", fmt.Sprintf("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", prose)},
		{"SSE id before assistant prose", fmt.Sprintf("id: 1\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", prose)},
		{"SSE retry before assistant prose", fmt.Sprintf("retry: 1000\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", prose)},
		{"SSE tool arguments", fmt.Sprintf("event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"delta\":%q}\n\n", arguments)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(tc.body)
			if upstreamRouteUnavailable(body) || hasStructuredUpstreamError(body) {
				t.Fatal("ordinary content was classified as a structured upstream failure")
			}
			for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
				if upstreamRouteUnavailable(body, status >= http.StatusBadRequest) {
					t.Fatalf("ordinary content was classified as a source-specific failure for HTTP %d", status)
				}
				if got := (*Service)(nil).isFailoverResponse(status, false, body); got != (status == http.StatusBadRequest) {
					t.Fatalf("HTTP %d failover=%v; failover should depend on status, not body text", status, got)
				}
				info := (*Service)(nil).buildUpstreamErrorInfo(nil, status, nil, body, "", http.MethodPost)
				if info.Type != "http" {
					t.Fatalf("ordinary content changed error type for HTTP %d: %+v", status, info)
				}
			}
		})
	}
	for _, body := range []string{"无权访问 pro 分组", "模型不存在", prose} {
		if upstreamRouteUnavailable([]byte(body)) || hasStructuredUpstreamError([]byte(body)) || (*Service)(nil).isFailoverResponse(http.StatusOK, false, []byte(body)) {
			t.Errorf("successful plain text was classified as an upstream failure: %q", body)
		}
		info := (*Service)(nil).buildUpstreamErrorInfo(nil, http.StatusOK, nil, []byte(body), "", http.MethodPost)
		if info.Type != "http" {
			t.Errorf("successful plain text changed error type: %+v", info)
		}
	}
}

func TestHTTP200ErrorEnvelopeIsFailoverEligibleWithoutScanningContent(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"root error", `{"error":{"type":"upstream_error","message":"temporarily unavailable"}}`, true},
		{"responses failure", `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"busy"}}}`, true},
		{"responses failed status", `{"type":"response.failed","response":{"status":"failed"}}`, true},
		{"sse failure", "event: response.failed\ndata: {\"response\":{\"status\":\"failed\"}}\n\n", true},
		{"normal answer mentioning error", `{"choices":[{"message":{"content":"the word error is harmless"}}]}`, false},
		{"tool argument error", `{"output":[{"type":"function_call","arguments":"{\"error\":\"inside tool input\"}"}]}`, false},
		{"null errors", `{"error":null,"response":{"status":"completed","error":null}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasExplicitUpstreamError([]byte(tc.body)); got != tc.want {
				t.Fatalf("explicit error=%v want=%v body=%s", got, tc.want, tc.body)
			}
			if got := (*Service)(nil).isFailoverResponse(http.StatusOK, false, []byte(tc.body)); got != tc.want {
				t.Fatalf("HTTP 200 failover=%v want=%v body=%s", got, tc.want, tc.body)
			}
		})
	}
}

func TestSameRouteRetrySuppressionIncludesChinesePermissionAndAdapterIntegrity(t *testing.T) {
	for _, tc := range []struct {
		status  int
		message string
	}{
		{http.StatusForbidden, "HTTP 403: 无权访问 pro 分组"},
		{http.StatusForbidden, "API Key 所属分组已删除"},
		{http.StatusBadGateway, "HTTP 502: permission denied for pro group"},
		{http.StatusBadGateway, "request first token budget exhausted"},
		{http.StatusBadGateway, "upstream stream idle for 2m0s"},
		{http.StatusOK, "Responses preconnect terminal output identity changed"},
	} {
		info := usageErrorInfo{Summary: tc.message}
		if isSameRouteRetryableUpstreamFailure(tc.status, info) {
			t.Errorf("same-route retry unexpectedly allowed for %q", tc.message)
		}
	}
	if isSameRouteRetryableUpstreamFailure(http.StatusBadGateway, usageErrorInfo{Summary: "temporary gateway failure"}) == false {
		t.Fatal("transient HTTP 502 should retain its same-route retry")
	}
}

func TestRetryOnlyFailuresDoNotMarkRouteUnavailable(t *testing.T) {
	for _, tc := range []struct {
		message    string
		structured bool
	}{
		{"request first token budget exhausted", true},
		{"upstream stream idle for 2m0s", true},
		{"Responses preconnect terminal output identity changed", false},
		{"Responses preconnect output item identity changed", false},
	} {
		t.Run(tc.message, func(t *testing.T) {
			if isDeterministicRouteFailure(tc.message) {
				t.Fatal("wait or adapter failure was classified as a route identity failure")
			}
			for _, body := range []string{
				fmt.Sprintf(`{"error":{"message":%q}}`, tc.message),
				fmt.Sprintf("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"status\":\"in_progress\"}}\n\nevent: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"error\":{\"message\":%q}}}\n\n", tc.message),
			} {
				if upstreamRouteUnavailable([]byte(body), true) {
					t.Fatal("wait or adapter failure was classified as a route identity failure")
				}
				if got := hasStructuredUpstreamError([]byte(body)); got != tc.structured {
					t.Fatalf("structured failure=%v want=%v; waits are upstream errors, adapter integrity failures are local", got, tc.structured)
				}
				info := usageErrorInfo{Summary: "response matched rejection rule", UpstreamBody: body}
				if isSameRouteRetryableUpstreamFailure(http.StatusBadGateway, info) {
					t.Fatal("wait or adapter error envelope did not suppress same-route retry")
				}
			}
		})
	}
}

func TestStructuredUpstreamFailurePreservesNumericCodeMessage(t *testing.T) {
	for _, prefix := range []string{"", "id: 1\n", "retry: 1000\n"} {
		body := `{"error":{"code":503,"message":"service unavailable"}}`
		if prefix != "" {
			body = prefix + "event: error\ndata: " + body + "\n\n"
		}
		if !hasStructuredUpstreamError([]byte(body)) {
			t.Errorf("numeric error code hid the upstream error message: %q", body)
		}
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
				{"timeout-524-reserve", 524, `<html>upstream timed out</html>`},
				{"model-400", 400, `{"error":{"code":"model_not_found","message":"The requested model is unavailable"}}`},
				{"model-404", 404, `{"error":{"message":"Model gpt-test is not supported by any configured account in this group"}}`},
				{"balance", 403, `{"error":{"message":"Insufficient account balance"}}`},
				{"unauthorized-401", 401, `{"error":{"type":"authentication_error","message":"invalid api key"}}`},
				{"permission-403", 403, `{"error":{"type":"permission_error","message":"permission denied"}}`},
				{"not-found-404", 404, `{"error":{"code":"model_not_found","message":"model not available"}}`},
				{"invalid-input-terminal", 400, `{"error":{"type":"invalid_request_error","message":"input_item_not_supported"}}`},
				{"empty-input-terminal", 400, ""},
				{"http-200-error-envelope", 200, `{"error":{"type":"upstream_error","message":"source returned an error"}}`},
				{"http-200-response-failed", 200, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"source returned an error\"}}}\n\n"},
				{"exhausted-401", 401, `{"error":{"type":"authentication_error","message":"invalid api key"}}`},
			} {
				t.Run(fmt.Sprintf("coordinated=%v/stream=%v/%s", coordinated, stream, tc.name), func(t *testing.T) {
					allFail := tc.name == "exhausted-401"
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
							if index == 0 || allFail {
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
						reserve := tc.name == "timeout-524-reserve" && index == 1
						if reserve {
							rate = 0.05
						}
						inputs = append(inputs, storage.GatewayRoute{FallbackOnly: reserve, GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
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
					if allFail {
						if recorder.Code != http.StatusBadGateway || calls[0].Load() != 1 || calls[1].Load() != 1 || strings.Contains(recorder.Body.String(), "invalid api key") || !strings.Contains(recorder.Body.String(), "all upstream routes failed") {
							t.Fatalf("exhausted upstream detail leaked: status=%d calls=%d/%d body=%s", recorder.Code, calls[0].Load(), calls[1].Load(), recorder.Body.String())
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
					if logs[0].CooldownUntil != nil && (tc.name == "invalid-input-terminal" || tc.name == "empty-input-terminal") {
						t.Fatalf("caller error cooled the source: %+v", logs[0])
					}
					if tc.name != "invalid-input-terminal" && tc.name != "empty-input-terminal" && logs[0].CooldownUntil == nil {
						t.Fatal("failed source was not cooled")
					}
				})
			}
		}
	}
}
