package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const coordinatedRetryRegressionModel = "retry-regression-model"

type coordinatedRetryRegressionFixture struct {
	runtime  *Runtime
	request  coordinatedForwardRequest
	db       *gorm.DB
	routes   *storage.GatewayRoutes
	sources  []storage.GatewayRoute
	recorder *httptest.ResponseRecorder
	calls    [2]atomic.Int32
	orderMu  sync.Mutex
	order    []int
}

func newCoordinatedRetryRegressionFixture(t *testing.T, stream bool, sourceCount int, group storage.GatewayGroup, serve func(int, int32, http.ResponseWriter, *http.Request)) *coordinatedRetryRegressionFixture {
	t.Helper()
	f := &coordinatedRetryRegressionFixture{db: openGatewayTestDB(t), recorder: httptest.NewRecorder()}
	groups, keys := storage.NewGatewayGroups(f.db), storage.NewGatewayKeys(f.db)
	f.routes = storage.NewGatewayRoutes(f.db)
	providers := storage.NewGatewayProviders(f.db)
	cipher, err := crypto.NewCipher("coordinated-retry-regression")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("coordinated-retry-test-key")
	if err != nil {
		t.Fatal(err)
	}
	group.Name = "coordinated-retry-regression"
	group.Status = storage.GatewayGroupStatusActive
	group.RateSortDirection = "asc"
	group.RequestFirstTokenTimeoutSec = 10
	group.ResponseValidationEnabled = true
	// Preserve explicit zero/false retry settings: GORM insert defaults may
	// populate them, while this fixture calls the coordinator with the policy.
	policy := group
	if err := groups.Create(&group); err != nil {
		t.Fatal(err)
	}
	policy.ID = group.ID
	group = policy
	key := &storage.GatewayKey{
		GroupID: group.ID, Name: "retry-client", KeyHash: HashAPIKey("coordinated-retry-test-key"),
		KeyCipher: secret, Status: storage.GatewayKeyStatusActive,
	}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	inputs := make([]storage.GatewayRoute, 0, sourceCount)
	for index := 0; index < sourceCount; index++ {
		index := index
		upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			call := f.calls[index].Add(1)
			f.orderMu.Lock()
			f.order = append(f.order, index)
			f.orderMu.Unlock()
			serve(index, call, w, r)
		}))
		t.Cleanup(upstream.Close)
		provider := &storage.GatewayProvider{
			Name: fmt.Sprintf("retry-source-%d", index), BaseURL: upstream.URL,
			APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll,
		}
		if err := providers.Create(provider); err != nil {
			t.Fatal(err)
		}
		rate := float64(index+1) / 10
		inputs = append(inputs, storage.GatewayRoute{
			GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider,
			GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIChat,
			RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate,
		})
	}
	if err := f.routes.SaveForGroup(group.ID, inputs); err != nil {
		t.Fatal(err)
	}
	f.sources, err = f.routes.ListByGroupID(group.ID)
	if err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, f.routes, storage.NewGatewayUsageLogs(f.db), storage.NewModelPriceOverrides(f.db), storage.NewChannels(f.db), nil, cipher, nil)
	svc.SetProviders(providers)
	f.runtime = svc.runtime()
	c, _ := gin.CreateTestContext(f.recorder)
	body := fmt.Sprintf(`{"model":%q,"stream":%v,"messages":[{"role":"user","content":"hi"}]}`, coordinatedRetryRegressionModel, stream)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	ctx, cancel := context.WithTimeout(c.Request.Context(), 10*time.Second)
	t.Cleanup(cancel)
	c.Request = c.Request.WithContext(ctx)
	validator := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 1, Name: "unrelated validation rule", Enabled: true,
		Target: "error_message", Pattern: "^coordinated-retry-never-match$",
	})
	f.request = coordinatedForwardRequest{
		c: c, path: "/v1/chat/completions", kind: protocolOpenAI, key: key, group: &group,
		body: []byte(body), requestedModel: coordinatedRetryRegressionModel, stream: stream,
		routes: f.sources, validator: validator, requestID: "coordinated-retry-regression",
		firstToken: 5 * time.Second, hedgeActive: group.HedgeEnabled,
	}
	return f
}

func writeCoordinatedRetryRegressionFailure(w http.ResponseWriter, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"error":{"type":"upstream_error","message":"retry regression upstream failure"}}`)
}

func writeCoordinatedRetryRegressionSuccess(w http.ResponseWriter, stream bool) {
	if !stream {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"retry-completion","object":"chat.completion","model":%q,"choices":[{"index":0,"message":{"role":"assistant","content":"recovered retry answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9}}`, coordinatedRetryRegressionModel)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = fmt.Fprintf(w, "data: {\"id\":\"retry-completion\",\"object\":\"chat.completion.chunk\",\"model\":%q,\"choices\":[{\"index\":0,\"delta\":{\"role\":\"assistant\",\"content\":\"recovered retry answer\"},\"finish_reason\":null}]}\n\n", coordinatedRetryRegressionModel)
	_, _ = io.WriteString(w, "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":2,\"total_tokens\":9}}\n\ndata: [DONE]\n\n")
}

func assertCoordinatedRetryRegressionResult(t *testing.T, f *coordinatedRetryRegressionFixture, wantOrder []int, success bool) []storage.GatewayUsageLog {
	t.Helper()
	f.orderMu.Lock()
	order := append([]int(nil), f.order...)
	f.orderMu.Unlock()
	if !reflect.DeepEqual(order, wantOrder) {
		t.Fatalf("source call order=%v, want=%v; status=%d body=%s", order, wantOrder, f.recorder.Code, f.recorder.Body.String())
	}
	if success {
		if f.recorder.Code != http.StatusOK || !strings.Contains(f.recorder.Body.String(), "recovered retry answer") || strings.Contains(f.recorder.Body.String(), "retry regression upstream failure") {
			t.Fatalf("retry did not deliver a clean success: status=%d body=%s", f.recorder.Code, f.recorder.Body.String())
		}
	} else if f.recorder.Code != http.StatusBadGateway || strings.Contains(f.recorder.Body.String(), "recovered retry answer") {
		t.Fatalf("exhausted transport budget returned status=%d body=%s", f.recorder.Code, f.recorder.Body.String())
	}
	var logs []storage.GatewayUsageLog
	if err := f.db.Where("request_id = ?", f.request.requestID).Order("attempt").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if len(logs) != len(wantOrder) {
		t.Fatalf("usage logs=%d, want one for each of %d real requests", len(logs), len(wantOrder))
	}
	for index, row := range logs {
		wantWinner := success && index == len(logs)-1
		if row.Winner != wantWinner || row.Success != wantWinner {
			t.Fatalf("attempt %d success=%v winner=%v, want=%v", row.Attempt, row.Success, row.Winner, wantWinner)
		}
	}
	return logs
}

func TestCoordinatedTransportRetryBudgetAndRouteIsolation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			name      string
			failover  bool
			retries   int
			sources   int
			recover   bool
			wantOrder []int
		}{
			{name: "transport-failover-disabled", retries: 1, sources: 2, recover: true, wantOrder: []int{0, 0}},
			{name: "alternative-fails-original-retry-recovers", failover: true, retries: 1, sources: 2, recover: true, wantOrder: []int{0, 1, 0}},
			{name: "response-budget-cannot-fund-transport-retries", failover: true, sources: 2, wantOrder: []int{0, 1}},
			{name: "single-source-response-budget-cannot-fund-transport-retries", failover: true, sources: 1, wantOrder: []int{0}},
		} {
			t.Run(fmt.Sprintf("stream=%v/%s", stream, tc.name), func(t *testing.T) {
				f := newCoordinatedRetryRegressionFixture(t, stream, tc.sources, storage.GatewayGroup{
					RetryEnabled: true, RetryCount: tc.retries, ResponseValidationRetryCount: 3,
					FailoverEnabled: tc.failover, FailoverMax: 1, CooldownSeconds: 60,
				}, func(source int, call int32, w http.ResponseWriter, _ *http.Request) {
					if tc.recover && (source == 0 && call > 1 || !tc.failover && source == 1) {
						writeCoordinatedRetryRegressionSuccess(w, stream)
						return
					}
					status := http.StatusBadGateway
					if tc.recover && source == 1 {
						status = http.StatusServiceUnavailable
					}
					writeCoordinatedRetryRegressionFailure(w, status)
				})
				f.runtime.handleForwardCoordinated(f.request)
				logs := assertCoordinatedRetryRegressionResult(t, f, tc.wantOrder, tc.recover)
				if tc.recover && tc.failover {
					if logs[0].CooldownUntil == nil {
						t.Fatal("fixture did not publish the original source failure before failover")
					}
					stored, err := f.routes.FindByID(f.sources[0].ID)
					if err != nil || !IsRouteSchedulableForModel(stored, coordinatedRetryRegressionModel, time.Now()) {
						t.Fatalf("successful fallback left its own earlier cooldown active: route=%+v err=%v", stored, err)
					}
				}
			})
		}
	}
}

func TestCoordinatedRetrySuccessPreservesNewerCooldownGeneration(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			var f *coordinatedRetryRegressionFixture
			generationCheck := make(chan error, 1)
			const newerRequestID = "newer-independent-request"
			f = newCoordinatedRetryRegressionFixture(t, stream, 2, storage.GatewayGroup{
				RetryEnabled: true, RetryCount: 1, ResponseValidationRetryCount: 3,
				FailoverEnabled: true, FailoverMax: 1, CooldownSeconds: 60,
			}, func(source int, call int32, w http.ResponseWriter, _ *http.Request) {
				if source == 0 && call > 1 {
					stored, err := f.routes.FindByID(f.sources[0].ID)
					if err == nil {
						cooldown, ok := routeModelCooldownSnapshot(stored, coordinatedRetryRegressionModel)
						if !ok || cooldown.TempUnschedulableRequestID != f.request.requestID || cooldown.TempUnschedulableUntil == nil {
							err = fmt.Errorf("original failure generation was not recorded: %+v", cooldown)
						}
					}
					if err == nil {
						now := time.Now()
						err = f.routes.SetModelTempUnschedulableWithProbeProtocol(f.sources[0].ID, coordinatedRetryRegressionModel, now.Add(5*time.Minute), "newer failure", now, newerRequestID, false, "openai_chat")
					}
					generationCheck <- err
					writeCoordinatedRetryRegressionSuccess(w, stream)
					return
				}
				status := http.StatusBadGateway
				if source == 1 {
					status = http.StatusServiceUnavailable
				}
				writeCoordinatedRetryRegressionFailure(w, status)
			})
			f.runtime.handleForwardCoordinated(f.request)
			assertCoordinatedRetryRegressionResult(t, f, []int{0, 1, 0}, true)
			select {
			case err := <-generationCheck:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("successful retry did not exercise the newer failure generation")
			}
			stored, err := f.routes.FindByID(f.sources[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			cooldown, ok := routeModelCooldownSnapshot(stored, coordinatedRetryRegressionModel)
			if !ok || cooldown.TempUnschedulableRequestID != newerRequestID || IsRouteSchedulableForModel(stored, coordinatedRetryRegressionModel, time.Now()) {
				t.Fatalf("older successful request cleared a newer cooldown: %+v", cooldown)
			}
		})
	}
}

func TestCoordinatedOutOfOrderHedgeFailuresPreserveOriginalRetrySlots(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			firstFailed, secondFailed := make(chan struct{}), make(chan struct{})
			var firstOnce, secondOnce sync.Once
			releaseFirst := func() { firstOnce.Do(func() { close(firstFailed) }) }
			releaseSecond := func() { secondOnce.Do(func() { close(secondFailed) }) }
			f := newCoordinatedRetryRegressionFixture(t, stream, 2, storage.GatewayGroup{
				RetryEnabled: true, RetryCount: 2, ResponseValidationRetryCount: 2,
				FailoverEnabled: true, FailoverMax: 1, CooldownSeconds: 60,
				HedgeEnabled: true, HedgeMaxAttempts: 6, HedgeMaxParallel: 2, HedgeDelaySeconds: 0.001,
			}, func(source int, call int32, w http.ResponseWriter, r *http.Request) {
				if source == 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusForbidden)
					_, _ = io.WriteString(w, `{"error":{"code":"invalid_api_key","message":"invalid api key"}}`)
					return
				}
				var ready <-chan struct{}
				switch call {
				case 1:
					// B must already have failed before A's slower primary fails.
					ready = secondFailed
				case 2:
					// An overlapping A retry waits for A0's failure publication, so
					// A0's old temporary suppression cannot be cleared by B later.
					ready = firstFailed
				default:
					writeCoordinatedRetryRegressionSuccess(w, stream)
					return
				}
				select {
				case <-ready:
					writeCoordinatedRetryRegressionFailure(w, http.StatusBadGateway)
				case <-r.Context().Done():
				}
			})
			t.Cleanup(releaseFirst)
			t.Cleanup(releaseSecond)
			// Health writes happen after source failure classification. Channels
			// synchronize those completions without relying on response sleeps.
			if err := f.db.Callback().Create().After("gorm:create").Register("retry-regression:failure-order", func(tx *gorm.DB) {
				cooldown, ok := tx.Statement.Dest.(*storage.GatewaySharedModelCooldown)
				if !ok || tx.Error != nil || cooldown.TempUnschedulableRequestID != f.request.requestID {
					return
				}
				if cooldown.PreferredRouteID == f.sources[0].ID {
					releaseFirst()
				}
				if cooldown.PreferredRouteID == f.sources[1].ID {
					releaseSecond()
				}
			}); err != nil {
				t.Fatal(err)
			}
			f.runtime.handleForwardCoordinated(f.request)
			assertCoordinatedRetryRegressionResult(t, f, []int{0, 1, 0, 0}, true)
			stored, err := f.routes.FindByID(f.sources[0].ID)
			if err != nil || !IsRouteSchedulableForModel(stored, coordinatedRetryRegressionModel, time.Now()) {
				t.Fatalf("hedge retry success retained its own cooldown: route=%+v err=%v", stored, err)
			}
		})
	}
}
