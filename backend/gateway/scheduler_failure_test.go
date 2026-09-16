package gateway

import (
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
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestRejectedOverloadSkipsRetriesButContentRulesRemainLocal(t *testing.T) {
	for _, tc := range []struct {
		name     string
		status   int
		body     string
		err      error
		suppress bool
	}{
		{"http overload", 503, `{"error":{"message":"busy"}}`, nil, true},
		{"http rate limit", 429, `{"error":{"message":"rate limit"}}`, nil, true},
		{"stream error", 200, "data: {\"type\":\"response.failed\",\"response\":{\"error\":{\"code\":\"server_error\"}}}\n\n", nil, true},
		{"ordinary rejected content", 200, "data: {\"choices\":[{\"delta\":{\"content\":\"Our servers are overloaded\"}}]}\n\n", nil, false},
		{"null error", 200, `{"error":null}`, nil, false},
		{"caller input error", 200, `{"error":{"type":"invalid_request_error","message":"invalid input"}}`, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := &coordinatedForwardAttempt{Status: tc.status, UpstreamBody: []byte(tc.body), Err: tc.err, Validation: validationResult{Decision: validationRejected}}
			if got := coordinatedAttemptSuppressesSameRouteRetries(a); got != tc.suppress {
				t.Fatalf("suppress=%v want=%v", got, tc.suppress)
			}
		})
	}
	for _, err := range []error{errFirstTokenTimeout, errUpstreamQueueTimeout} {
		if !coordinatedAttemptSuppressesSameRouteRetries(&coordinatedForwardAttempt{Status: 200, Err: err}) {
			t.Fatalf("timeout %v retried same route", err)
		}
	}
}

func TestCoordinatedOverloadFailsOverWithinBudgetAndCoolsBeforeWinnerCompletes(t *testing.T) {
	db := openGatewayTestDB(t)
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	cipher, err := crypto.NewCipher("scheduler-test-secret")
	if err != nil {
		t.Fatal(err)
	}
	keyCipher, err := cipher.Encrypt("scheduler-test-key")
	if err != nil {
		t.Fatal(err)
	}
	group := &storage.GatewayGroup{Name: "overload-failover", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc", RetryEnabled: true, RetryCount: 10, ResponseValidationRetryCount: 10, FailoverEnabled: true, FailoverMax: 2, CooldownSeconds: 60, RequestMaxAttempts: 2}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "test", KeyHash: HashAPIKey("scheduler-test-key"), KeyCipher: keyCipher, Status: storage.GatewayKeyStatusActive}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	failed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"message":"currently overloaded"}}`)
	}))
	defer failed.Close()
	var failedRouteID uint
	healthAtFallback := make(chan bool, 1)
	allowFinish := make(chan struct{}, 1)
	winner := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		row, findErr := routes.FindByID(failedRouteID)
		healthAtFallback <- findErr == nil && !IsRouteSchedulableForModel(row, "m", time.Now())
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-allowFinish:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
	}))
	defer winner.Close()
	defer close(allowFinish)
	inputs := make([]storage.GatewayRoute, 0, 2)
	for i, url := range []string{failed.URL, winner.URL} {
		provider := &storage.GatewayProvider{Name: fmt.Sprintf("scheduler-%d", i), BaseURL: url, APIKeyCipher: keyCipher, Enabled: true}
		if err := providers.Create(provider); err != nil {
			t.Fatal(err)
		}
		inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: i, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, RateConvertMode: "custom", RateConvertValue: float64(i + 1), BillingRateMultiplier: float64(i + 1)})
	}
	if err := routes.SaveForGroup(group.ID, inputs); err != nil {
		t.Fatal(err)
	}
	configured, err := routes.ListByGroupID(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	failedRouteID = configured[0].ID
	svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
	svc.SetProviders(providers)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	validator := mustResponseValidator(t, 64, 10*time.Millisecond, responseRuleSpec{ID: 1, Name: "overload", Enabled: true, Pattern: "currently overloaded", Target: "error_message"})
	req := coordinatedForwardRequest{c: c, path: "/v1/chat/completions", kind: protocolOpenAI, key: key, group: group, body: []byte(`{"model":"m","stream":true,"messages":[]}`), requestedModel: "m", stream: true, routes: configured, validator: validator, requestID: "scheduler-overload-test"}
	done := make(chan struct{})
	go func() { svc.runtime().handleForwardCoordinated(req); close(done) }()
	select {
	case cooled := <-healthAtFallback:
		if !cooled {
			t.Error("failure was not cooled before fallback started")
		}
	case <-done:
		t.Fatal("fallback was not reached within two real attempts")
	case <-time.After(5 * time.Second):
		t.Fatal("fallback never started")
	}
	// Let the winner finish without racing its writer or accessing the Gin
	// context until the handler has returned.
	allowFinish <- struct{}{}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("winner did not finish")
	}
	if calls.Load() != 1 {
		t.Fatalf("overloaded source called %d times", calls.Load())
	}
	var logs []storage.GatewayUsageLog
	if err := db.Where("request_id = ?", req.requestID).Order("attempt").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if len(logs) != 2 {
		t.Fatalf("attempt logs=%d want=2", len(logs))
	}
	if logs[0].CooldownUntil == nil || !strings.Contains(logs[0].ErrorType, "http") {
		t.Fatalf("failure log=%+v", logs[0])
	}
	if !logs[1].Winner || logs[1].RequestFirstTokenMS == nil || logs[1].RequestDurationMS == nil {
		t.Fatalf("missing winner timing: %+v", logs[1])
	}
}

func TestFailureHealthIsPublishedBeforeUsageAndNotExtendedByAudit(t *testing.T) {
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{Name: "early-health", RetryEnabled: true, CooldownSeconds: 60, FirstTokenTimeoutCooldownEnabled: true}
	if err := db.Create(group).Error; err != nil {
		t.Fatal(err)
	}
	route := storage.GatewayRoute{GatewayGroupID: group.ID, Enabled: true, SourceKind: storage.GatewayRouteSourceMonitor, SourceChannelID: 1, SourceAPIKeyCipher: "test-cipher"}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req := &coordinatedForwardRequest{c: c, group: group, kind: protocolOpenAI, path: "/v1/chat/completions", requestID: "early-health"}
	rt := (&Service{Routes: routes}).runtime()
	a := &coordinatedForwardAttempt{Route: route, UpstreamModel: "m", Status: 503, Validation: validationResult{Decision: validationRejected}, UpstreamBody: []byte(`{"error":{"message":"busy"}}`)}
	rt.publishCoordinatedFailure(req, a, nil, 1)
	if !a.healthRecorded || a.UsageMeta.CooldownUntil == nil {
		t.Fatal("failure not published immediately")
	}
	stored, err := routes.FindByID(route.ID)
	if err != nil {
		t.Fatal(err)
	}
	if IsRouteSchedulableForModel(stored, "m", time.Now()) {
		t.Fatal("failed model still schedulable before usage audit")
	}
	if !IsRouteSchedulableForModel(stored, "other-model", time.Now()) {
		t.Fatal("cooldown leaked to unrelated model")
	}
	until := *a.UsageMeta.CooldownUntil
	rt.publishCoordinatedFailure(req, a, nil, 1)
	if !a.UsageMeta.CooldownUntil.Equal(until) {
		t.Fatal("audit extended the cooldown")
	}
	for _, attemptErr := range []error{errUpstreamQueueTimeout, errRequestFirstTokenBudget} {
		local := &coordinatedForwardAttempt{Route: route, UpstreamModel: "healthy-model", Err: attemptErr}
		rt.publishCoordinatedFailure(req, local, nil, 1)
		if local.healthRecorded {
			t.Fatalf("local failure %v cooled upstream", attemptErr)
		}
	}
}

func TestLocalQueueTimeoutHasSeparateErrorType(t *testing.T) {
	rt := (&Service{}).runtime()
	target := &upstreamTarget{Channel: &storage.Channel{ID: 1, ConcurrencyLimit: 1}}
	ctx := httptest.NewRequest(http.MethodPost, "/", nil).Context()
	release, err := rt.acquireUpstreamConcurrency(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	_, err = rt.acquireUpstreamConcurrencyForAttempt(ctx, target, time.Now(), 10*time.Millisecond)
	if !errors.Is(err, errUpstreamQueueTimeout) || rt.isFirstTokenTimeout(err) {
		t.Fatalf("queue error=%v", err)
	}
	info := rt.buildUpstreamErrorInfo(err, 0, nil, nil, "", http.MethodPost)
	if info.Type != "queue_timeout" {
		t.Fatalf("error type=%s", info.Type)
	}
}

func TestCoordinatedCapturesProtocolErrorAfterMetadata(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"test\",\"role\":\"assistant\",\"content\":[]}}\n\n")
		_, _ = io.WriteString(w, "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"currently overloaded\"}}\n\n")
	}))
	defer upstream.Close()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	validator := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{ID: 1, Name: "overload", Enabled: true, Pattern: "currently overloaded", Target: "error_message"})
	req := &coordinatedForwardRequest{c: c, kind: protocolAnthropic, group: &storage.GatewayGroup{}, requestedModel: "m", validator: validator}
	a := &coordinatedForwardAttempt{StartedAt: time.Now(), Target: &upstreamTarget{BaseURL: upstream.URL, APIKey: "test"}, UpstreamKind: protocolAnthropic, UpstreamModel: "m", UpstreamPath: "/v1/messages", ForwardBody: []byte(`{"model":"m","stream":true,"messages":[]}`)}
	_, err := (&Service{}).runtime().runCoordinatedStreamAttempt(c.Request.Context(), req, a, time.Second)
	if a.Gate != nil {
		defer a.Gate.Lose()
	}
	if err != nil {
		t.Fatal(err)
	}
	if !a.Validation.IsRejected() || !coordinatedRejectedUpstreamFailure(a) {
		t.Fatalf("protocol error was lost after metadata: validation=%+v body=%s", a.Validation, a.UpstreamBody)
	}
}
