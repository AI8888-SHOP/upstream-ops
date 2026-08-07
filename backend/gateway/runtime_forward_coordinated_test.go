package gateway

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestBuildVirtualCacheSettlementPreservesAnthropicFreshInput(t *testing.T) {
	const model = "claude-3-7-sonnet-20250219"
	rt := &Runtime{Service: &Service{Pricing: NewPricingCatalog(nil)}}
	req := &coordinatedForwardRequest{
		group:          &storage.GatewayGroup{HedgeVirtualCacheEnabled: true},
		hedgeTriggered: true,
		requestedModel: model,
	}
	winner := &coordinatedForwardAttempt{
		UpstreamModel: model,
		UsageMeta:     usageRecordMeta{UpstreamProtocol: string(protocol.KindAnthropic)},
		Tokens:        UsageTokens{InputTokens: 100, CacheReadTokens: 40, OutputTokens: 1},
		Plan: coordinatedRoutePlan{Candidate: ScoredRoute{
			EffectiveRate: 1, BillingRate: 1,
		}},
	}
	settlement := rt.buildVirtualCacheSettlement(req, winner)
	if !settlement.VirtualCacheReadEnabled || settlement.VirtualCacheReadTokens != 100 {
		t.Fatalf("settlement=%+v, want all 100 fresh Anthropic input tokens virtualized", settlement)
	}
	if settlement.BilledCost <= 0 || settlement.BilledCost >= 100*3e-6+40*3e-7+1*15e-6 {
		t.Fatalf("billed_cost=%v, want discounted but positive", settlement.BilledCost)
	}
}

func TestBuildVirtualCacheSettlementDoesNotRequireLocalPricing(t *testing.T) {
	const model = "provider-private-model"
	rt := &Runtime{Service: &Service{}}
	req := &coordinatedForwardRequest{
		group:              &storage.GatewayGroup{HedgeVirtualCacheEnabled: true},
		virtualCacheReason: storage.GatewayVirtualCacheReasonHedge,
		requestedModel:     model,
	}
	winner := &coordinatedForwardAttempt{
		UpstreamModel: model,
		UsageMeta:     usageRecordMeta{UpstreamProtocol: string(protocol.KindAnthropic)},
		Tokens:        UsageTokens{InputTokens: 100, OutputTokens: 1},
		Plan: coordinatedRoutePlan{Candidate: ScoredRoute{
			EffectiveRate: 1, BillingRate: 1,
		}},
	}
	settlement := rt.buildVirtualCacheSettlement(req, winner)
	if !settlement.VirtualCacheReadEnabled || !settlement.BilledCostSet || settlement.VirtualCacheReadTokens != 100 {
		t.Fatalf("settlement=%+v, want a token settlement without local pricing", settlement)
	}
	if settlement.BilledCost != 0 || settlement.VirtualCacheReadCost != 0 {
		t.Fatalf("unknown local pricing must keep monetary estimates at zero: %+v", settlement)
	}
}

func TestCoordinatedAttemptSuppressesDeterministicSameRouteRetries(t *testing.T) {
	attempt := &coordinatedForwardAttempt{
		Status: http.StatusForbidden,
		Err:    errors.New("upstream status 403"),
		ErrInfo: usageErrorInfo{
			Summary: "HTTP 403: Image generation is not enabled for this group",
		},
	}
	if !coordinatedAttemptSuppressesSameRouteRetries(attempt) {
		t.Fatal("deterministic capability failure should suppress same-route retries")
	}
	attempt.Status = http.StatusServiceUnavailable
	attempt.Err = errors.New("upstream status 503")
	attempt.ErrInfo = usageErrorInfo{Summary: "HTTP 503: temporarily overloaded"}
	if coordinatedAttemptSuppressesSameRouteRetries(attempt) {
		t.Fatal("temporary 503 must retain configured same-route retries")
	}
}

func TestVirtualCacheReasonDoesNotRequireLocalPricing(t *testing.T) {
	rt := &Runtime{Service: &Service{}}
	req := &coordinatedForwardRequest{
		path: "/v1/chat/completions", requestedModel: "provider-private-model",
		group: &storage.GatewayGroup{HedgeVirtualCacheEnabled: true},
	}
	winner := &coordinatedForwardAttempt{
		Info:  hedgeAttemptInfo{Number: 1, Kind: attemptKindPrimary},
		Route: storage.GatewayRoute{ID: 1},
	}
	winner.markUpstreamStarted()
	hedge := &coordinatedForwardAttempt{
		Info:  hedgeAttemptInfo{Number: 2, Kind: attemptKindHedge, Concurrent: true},
		Route: storage.GatewayRoute{ID: 2},
	}
	hedge.markUpstreamStarted()
	var states sync.Map
	states.Store(1, winner)
	states.Store(2, hedge)
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonHedge {
		t.Fatalf("reason=%q, want hedge even when local pricing is unavailable", reason)
	}
}

func TestFinishCoordinatedNonStreamWritesVirtualCacheUsageDownstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req := &coordinatedForwardRequest{
		c: context, kind: protocol.KindOpenAIChat, requestedModel: "provider-private-model",
		group:              &storage.GatewayGroup{HedgeVirtualCacheEnabled: true},
		virtualCacheReason: storage.GatewayVirtualCacheReasonHedge,
	}
	winner := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{Number: 1}, Status: http.StatusOK,
		Headers:       http.Header{"Content-Length": []string{"999"}},
		ClientBody:    []byte(`{"choices":[{"message":{"content":"ok"}}],"usage":{"prompt_tokens":100,"completion_tokens":4,"prompt_tokens_details":{"cached_tokens":20},"cache_creation_input_tokens":10}}`),
		UpstreamModel: "provider-private-model",
		UsageMeta:     usageRecordMeta{UpstreamProtocol: string(protocol.KindOpenAIChat)},
		Tokens:        UsageTokens{InputTokens: 100, OutputTokens: 4, CacheReadTokens: 20, CacheCreationTokens: 10},
		Plan: coordinatedRoutePlan{Candidate: ScoredRoute{
			EffectiveRate: 1, BillingRate: 1,
		}},
	}

	(&Runtime{Service: &Service{}}).finishCoordinatedNonStream(req, winner, 0)

	tokens := NormalizeUsageBuckets(ParseOpenAIUsage(recorder.Body.Bytes()), protocol.KindOpenAIChat)
	if tokens.InputTokens != 0 || tokens.CacheReadTokens != 90 || tokens.CacheCreationTokens != 10 {
		t.Fatalf("downstream usage=%+v, want fresh=0 cache_read=90 cache_creation=10; body=%s", tokens, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Length"); got != "" {
		t.Fatalf("rewritten response retained Content-Length=%q", got)
	}
}

func TestCoordinatedHedgeCreditRequiresActualUpstreamStart(t *testing.T) {
	attempt := &coordinatedForwardAttempt{Info: hedgeAttemptInfo{
		Number: 2, Kind: attemptKindHedge, Concurrent: true,
	}}
	result := hedgeRunResult[*coordinatedForwardAttempt]{Attempts: []hedgeAttemptResult[*coordinatedForwardAttempt]{
		{Info: attempt.Info, Value: attempt},
	}}
	if coordinatedHedgeTriggered(result, nil) {
		t.Fatal("scheduled hedge without an upstream request should not trigger virtual cache")
	}
	attempt.markUpstreamStarted()
	if !coordinatedHedgeTriggered(result, nil) {
		t.Fatal("hedge that reached upstream should trigger virtual cache")
	}
	var states sync.Map
	states.Store(attempt.Info.Number, attempt)
	synthetic := hedgeRunResult[*coordinatedForwardAttempt]{Attempts: []hedgeAttemptResult[*coordinatedForwardAttempt]{
		{Info: attempt.Info, Outcome: hedgeOutcomeLost},
	}}
	if !coordinatedHedgeTriggered(synthetic, &states) {
		t.Fatal("cleanup-timeout hedge state should trigger virtual cache")
	}
	if got := coordinatedAttemptKind(attempt, true, 2); got != storage.GatewayAttemptKindHedge {
		t.Fatalf("attempt kind=%q, want hedge", got)
	}
	sequential := &coordinatedForwardAttempt{Info: hedgeAttemptInfo{
		Number: 2, Kind: attemptKindHedge, Concurrent: false,
	}}
	sequential.markUpstreamStarted()
	if got := coordinatedAttemptKind(sequential, true, 2); got == storage.GatewayAttemptKindHedge {
		t.Fatalf("sequential fallback was classified as hedge: %q", got)
	}
}

func TestVirtualCacheReasonForResponseRuleFailover(t *testing.T) {
	const model = "claude-3-7-sonnet-20250219"
	rt := &Runtime{Service: &Service{Pricing: NewPricingCatalog(nil)}}
	req := &coordinatedForwardRequest{
		path: "/v1/chat/completions", requestedModel: model,
		group: &storage.GatewayGroup{ResponseValidationVirtualCacheEnabled: true},
	}
	rejected := &coordinatedForwardAttempt{
		Info:       hedgeAttemptInfo{Number: 1, Kind: attemptKindPrimary},
		Route:      storage.GatewayRoute{ID: 1},
		Validation: validationResult{Decision: validationRejected, RuleID: 7},
	}
	rejected.markUpstreamStarted()
	winner := &coordinatedForwardAttempt{
		Info:  hedgeAttemptInfo{Number: 2, Kind: attemptKindFailover},
		Route: storage.GatewayRoute{ID: 2}, UpstreamModel: model,
	}
	winner.markUpstreamStarted()
	var states sync.Map
	states.Store(1, rejected)
	states.Store(2, winner)
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonResponseRuleFailover {
		t.Fatalf("reason=%q, want response-rule failover", reason)
	}
	rejected.Validation.PostCommit = true
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != "" {
		t.Fatalf("post-commit reason=%q, want empty", reason)
	}
	rejected.Validation.PostCommit = false
	req.group.HedgeVirtualCacheEnabled = true
	hedge := &coordinatedForwardAttempt{Info: hedgeAttemptInfo{Number: 3, Kind: attemptKindHedge, Concurrent: true}}
	hedge.markUpstreamStarted()
	states.Store(3, hedge)
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonHedge {
		t.Fatalf("hedge precedence reason=%q", reason)
	}
}

func TestVirtualCacheReasonForSequentialResponseRuleFailoverWithHedgeEnabled(t *testing.T) {
	const model = "claude-3-7-sonnet-20250219"
	rt := &Runtime{Service: &Service{Pricing: NewPricingCatalog(nil)}}
	req := &coordinatedForwardRequest{
		path: "/v1/chat/completions", requestedModel: model, hedgeActive: true,
		group: &storage.GatewayGroup{ResponseValidationVirtualCacheEnabled: true},
	}
	rejected := &coordinatedForwardAttempt{
		Info:       hedgeAttemptInfo{Number: 1, Kind: attemptKindPrimary},
		Route:      storage.GatewayRoute{ID: 1},
		Validation: validationResult{Decision: validationRejected, RuleID: 7},
	}
	rejected.markUpstreamStarted()
	winner := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{
			Number: 2, Kind: attemptKindHedge, Concurrent: false,
		},
		Route: storage.GatewayRoute{ID: 2}, UpstreamModel: model,
	}
	winner.markUpstreamStarted()
	var states sync.Map
	states.Store(1, rejected)
	states.Store(2, winner)
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonResponseRuleFailover {
		t.Fatalf("reason=%q, want response-rule failover", reason)
	}
}

func TestResponseRuleVirtualCacheRequiresItsOwnSwitch(t *testing.T) {
	req := &coordinatedForwardRequest{
		path: "/v1/chat/completions", requestedModel: "gpt-test",
		group: &storage.GatewayGroup{HedgeVirtualCacheEnabled: true},
	}
	rejected := &coordinatedForwardAttempt{
		Info:       hedgeAttemptInfo{Number: 1, Kind: attemptKindPrimary},
		Route:      storage.GatewayRoute{ID: 1},
		Validation: validationResult{Decision: validationRejected, RuleID: 8},
	}
	rejected.markUpstreamStarted()
	winner := &coordinatedForwardAttempt{
		Info:  hedgeAttemptInfo{Number: 2, Kind: attemptKindFailover},
		Route: storage.GatewayRoute{ID: 2}, UpstreamModel: "gpt-test",
	}
	winner.markUpstreamStarted()
	var states sync.Map
	states.Store(1, rejected)
	states.Store(2, winner)
	if reason := (&Runtime{Service: &Service{}}).virtualCacheReasonForWinner(req, winner, &states); reason != "" {
		t.Fatalf("hedge-only switch granted response-rule virtual read: %q", reason)
	}
}

func TestVirtualCacheReasonForConcurrentResponseRuleFailover(t *testing.T) {
	const model = "claude-3-7-sonnet-20250219"
	rt := &Runtime{Service: &Service{Pricing: NewPricingCatalog(nil)}}
	req := &coordinatedForwardRequest{
		path: "/v1/chat/completions", requestedModel: model, hedgeActive: true,
		group: &storage.GatewayGroup{ResponseValidationVirtualCacheEnabled: true},
	}
	rejected := &coordinatedForwardAttempt{
		Info:       hedgeAttemptInfo{Number: 1, Kind: attemptKindPrimary},
		Route:      storage.GatewayRoute{ID: 1},
		Validation: validationResult{Decision: validationRejected, RuleID: 7},
	}
	rejected.markUpstreamStarted()
	winner := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{
			Number: 2, Kind: attemptKindHedge, Concurrent: true,
		},
		Route: storage.GatewayRoute{ID: 2}, UpstreamModel: model,
	}
	winner.markUpstreamStarted()
	var states sync.Map
	states.Store(1, rejected)
	states.Store(2, winner)

	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonResponseRuleFailover {
		t.Fatalf("response-only reason=%q, want response-rule failover", reason)
	}
	req.group.HedgeVirtualCacheEnabled = true
	if reason := rt.virtualCacheReasonForWinner(req, winner, &states); reason != storage.GatewayVirtualCacheReasonHedge {
		t.Fatalf("both-enabled reason=%q, want hedge precedence", reason)
	}
}

func TestCoordinatedResponsesRegexRejectSwitchesBeforeClientCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	failedUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"failed\",\"status\":\"in_progress\"}}\n\n")
		_, _ = io.WriteString(w, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_failed\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n")
		_, _ = io.WriteString(w, "event: response.content_part.added\ndata: {\"type\":\"response.content_part.added\",\"output_index\":0,\"content_index\":0,\"part\":{\"type\":\"output_text\",\"text\":\"\"}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"failed\",\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer failedUpstream.Close()

	successUpstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"success\",\"status\":\"in_progress\"}}\n\n")
		_, _ = io.WriteString(w, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_success\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n")
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"fallback ok\"}\n\n")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"success\",\"status\":\"completed\"}}\n\n")
	}))
	defer successUpstream.Close()

	cases := []struct {
		name       string
		clientKind protocol.Kind
		path       string
		body       string
	}{
		{"responses", protocol.KindOpenAIResponses, "/v1/responses", `{"model":"m","stream":true,"input":"hi"}`},
		{"chat", protocol.KindOpenAIChat, "/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`},
		{"anthropic", protocol.KindAnthropic, "/v1/messages", `{"model":"m","stream":true,"messages":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
			context, _ := gin.CreateTestContext(recorder)
			context.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			validator := mustResponseValidator(t, 64, time.Second, responseRuleSpec{
				ID: 91, Name: "capacity", Enabled: true,
				Pattern: `(?i)(Selected model is at capacity\.|Our servers are currently overloaded\.)`,
				Target:  "error_message",
			})
			request := &coordinatedForwardRequest{
				c: context, kind: tc.clientKind, group: &storage.GatewayGroup{},
				requestedModel: "m", validator: validator,
			}
			newAttempt := func(number int, baseURL string) *coordinatedForwardAttempt {
				return &coordinatedForwardAttempt{
					Info: hedgeAttemptInfo{Number: number}, StartedAt: time.Now(),
					Target:       &upstreamTarget{BaseURL: baseURL, APIKey: "k"},
					UpstreamKind: protocol.KindOpenAIResponses, UpstreamModel: "m",
					UpstreamPath: "/v1/responses", UpstreamURL: baseURL + "/v1/responses",
					Converted:   tc.clientKind != protocol.KindOpenAIResponses,
					ForwardBody: []byte(tc.body),
				}
			}

			first := newAttempt(1, failedUpstream.URL)
			if _, err := (&Runtime{Service: &Service{}}).runCoordinatedStreamAttempt(
				context.Request.Context(), request, first, 0,
			); err != nil {
				t.Fatalf("failed route returned transport error: %v", err)
			}
			if !first.Validation.IsRejected() || first.Validation.PostCommit || first.Validation.RuleID != 91 {
				t.Fatalf("failed route validation=%+v, want pre-commit rule 91 rejection", first.Validation)
			}
			if recorder.Body.Len() != 0 {
				t.Fatalf("failed route reached client: %s", recorder.Body.String())
			}
			if first.Gate != nil {
				first.Gate.Lose()
			}

			second := newAttempt(2, successUpstream.URL)
			if _, err := (&Runtime{Service: &Service{}}).runCoordinatedStreamAttempt(
				context.Request.Context(), request, second, 0,
			); err != nil {
				t.Fatalf("fallback route failed: %v", err)
			}
			if second.Gate == nil {
				t.Fatal("fallback route did not create a stream gate")
			}
			if err := second.Gate.Win(); err != nil {
				t.Fatalf("commit fallback route: %v", err)
			}
			streamResult := second.awaitStreamResult()
			second.applyStreamResult(streamResult)
			if streamResult.Err != nil || !second.Gate.DownstreamCommitted() {
				t.Fatalf("fallback stream result=%+v committed=%v", streamResult, second.Gate.DownstreamCommitted())
			}
			body := recorder.Body.String()
			if !strings.Contains(body, "fallback ok") {
				t.Fatalf("fallback output missing: %s", body)
			}
			if strings.Contains(body, "currently overloaded") {
				t.Fatalf("rejected route leaked to client: %s", body)
			}
		})
	}
}

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
		RetryEnabled:    false,
		FailoverEnabled: false,
		FailoverMax:     0,
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
		RetryEnabled:                true,
		RetryCount:                  2,
		ResponseValidationRetryCount: -1,
		FailoverEnabled:             true,
		FailoverMax:                 0,
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

func TestBuildCoordinatedRoutePlanUsesIndependentResponseRetryBudget(t *testing.T) {
	group := &storage.GatewayGroup{
		RetryEnabled:                true,
		RetryCount:                  3,
		ResponseValidationRetryCount: 1,
		FailoverEnabled:             true,
		FailoverMax:                 0,
	}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1}},
		{Route: storage.GatewayRoute{ID: 2}},
	}
	plan := buildCoordinatedRoutePlan(candidates, group, false, true)
	if len(plan) != 8 {
		t.Fatalf("validation plan length=%d, want 8", len(plan))
	}
	for i, entry := range plan {
		if entry.MaxTries != 4 || entry.ResponseMaxTries != 2 {
			t.Fatalf("plan[%d] max tries=%d response max=%d, want 4/2", i, entry.MaxTries, entry.ResponseMaxTries)
		}
	}
	if got := effectiveResponseValidationRetryCount(group); got != 1 {
		t.Fatalf("effective response retries=%d, want 1", got)
	}
	group.ResponseValidationRetryCount = -1
	if got := effectiveResponseValidationRetryCount(group); got != 3 {
		t.Fatalf("inherited response retries=%d, want 3", got)
	}
	group.ResponseValidationRetryCount = 0
	if got := effectiveResponseValidationRetryCount(group); got != 0 {
		t.Fatalf("disabled response retries=%d, want 0", got)
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

func TestValidateCoordinatedAttemptSkipsExcludedPrimary(t *testing.T) {
	attempt := &coordinatedForwardAttempt{
		Route:  storage.GatewayRoute{ID: 7},
		Plan:   coordinatedRoutePlan{TryOnRoute: 0, MaxTries: 2},
		Status: http.StatusOK,
	}
	var excluded sync.Map
	excluded.Store(uint(7), struct{}{})
	accepted, err := validateCoordinatedAttempt(attempt, &excluded, nil)
	if accepted || !errors.Is(err, errSkippedNonRetryableRoute) {
		t.Fatalf("accepted=%v err=%v, want excluded primary", accepted, err)
	}
}

func TestValidateCoordinatedAttemptAllowsPlannedSameRouteRetryAfterRejection(t *testing.T) {
	attempt := &coordinatedForwardAttempt{
		Route:  storage.GatewayRoute{ID: 7},
		Plan:   coordinatedRoutePlan{TryOnRoute: 1, MaxTries: 2},
		Status: http.StatusOK,
	}
	var excluded sync.Map
	excluded.Store(uint(7), struct{}{})
	accepted, err := validateCoordinatedAttempt(attempt, nil, &excluded)
	if !accepted || err != nil {
		t.Fatalf("accepted=%v err=%v, want same-route retry to remain eligible", accepted, err)
	}
}

func TestValidateCoordinatedAttemptDoesNotRetryHardExcludedRoute(t *testing.T) {
	attempt := &coordinatedForwardAttempt{
		Route:  storage.GatewayRoute{ID: 7},
		Plan:   coordinatedRoutePlan{TryOnRoute: 1, MaxTries: 2},
		Status: http.StatusOK,
	}
	var excluded sync.Map
	excluded.Store(uint(7), struct{}{})
	accepted, err := validateCoordinatedAttempt(attempt, &excluded, nil)
	if accepted || !errors.Is(err, errSkippedNonRetryableRoute) {
		t.Fatalf("accepted=%v err=%v, want hard exclusion", accepted, err)
	}
}

func TestCoordinatedPlanSchedulerPromotesSameRouteRetry(t *testing.T) {
	group := &storage.GatewayGroup{
		RetryEnabled:                true,
		RetryCount:                  1,
		ResponseValidationRetryCount: -1,
		HedgeMaxAttempts:            3,
	}
	candidates := []ScoredRoute{
		{Route: storage.GatewayRoute{ID: 1}},
		{Route: storage.GatewayRoute{ID: 2}},
	}
	plan := buildCoordinatedRoutePlan(candidates, group, true, true)
	if len(plan) != 4 {
		t.Fatalf("plan length=%d, want full retry plan", len(plan))
	}
	scheduler := newCoordinatedPlanScheduler(plan, 3)
	first := scheduler.reserve(1)
	if first.Candidate.Route.ID != 1 || first.TryOnRoute != 0 {
		t.Fatalf("first=%+v, want route 1 primary", first)
	}
	scheduler.prioritizeRetry(&coordinatedForwardAttempt{Route: first.Candidate.Route, Plan: first})
	second := scheduler.reserve(2)
	if second.Candidate.Route.ID != 1 || second.TryOnRoute != 1 {
		t.Fatalf("second=%+v, want route 1 retry", second)
	}
	third := scheduler.reserve(3)
	if third.Candidate.Route.ID != 2 || third.TryOnRoute != 0 {
		t.Fatalf("third=%+v, want route 2 primary after retry", third)
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
