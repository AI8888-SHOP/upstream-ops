package gateway

import (
	"encoding/json"
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

// Exercise both production entry paths, including authentication, candidate
// filtering, target resolution, actual visible SSE delivery and usage audit.
func TestAdaptiveForwardRecordsBothPathsAndHonorsPriceEnvelope(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		t.Run(fmt.Sprintf("coordinated=%v", coordinated), func(t *testing.T) {
			db := openGatewayTestDB(t)
			cipher, err := crypto.NewCipher("adaptive-integration")
			if err != nil {
				t.Fatal(err)
			}
			ciphertext, err := cipher.Encrypt("test-secret")
			if err != nil {
				t.Fatal(err)
			}
			groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
			providers := storage.NewGatewayProviders(db)
			group := &storage.GatewayGroup{
				Name: "adaptive-forward", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
				GatewaySchedulerPolicy: storage.GatewaySchedulerPolicy{SchedulingMode: "balanced", SchedulingPremiumPercent: 20, SchedulingWindowMinutes: 5, SchedulingMinSamples: 10, SchedulingTargetTTFTSec: 10},
				RetryEnabled:           true, FailoverEnabled: true, FailoverMax: 3, RequestMaxAttempts: 3, RequestFirstTokenTimeoutSec: 5,
				HedgeEnabled: coordinated, HedgeDelaySeconds: 30, HedgeMaxParallel: 2, HedgeMaxAttempts: 3,
			}
			if err := groups.Create(group); err != nil {
				t.Fatal(err)
			}
			key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-secret"), KeyCipher: ciphertext, Status: storage.GatewayKeyStatusActive}
			if err := keys.Create(key); err != nil {
				t.Fatal(err)
			}
			var calls [3]atomic.Int32
			inputs := make([]storage.GatewayRoute, 0, 3)
			for index, rate := range []float64{0.05, 0.055, 0.08} {
				index := index
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[index].Add(1)
					if index == 0 {
						w.WriteHeader(http.StatusServiceUnavailable)
						_, _ = io.WriteString(w, `{"error":{"message":"overloaded"}}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
					w.(http.Flusher).Flush()
					_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
				}))
				t.Cleanup(upstream.Close)
				provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: ciphertext, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll, ConcurrencyLimit: 1}
				if err := providers.Create(provider); err != nil {
					t.Fatal(err)
				}
				inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
			}
			if err := routes.SaveForGroup(group.ID, inputs); err != nil {
				t.Fatal(err)
			}
			svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
			svc.SetProviders(providers)
			// Choose a deterministic non-exploration request ID for this test.
			requestID := "fixture"
			body := `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`
			response := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(response)
			c.Set(ctxKeyUpstreamOpsRequestID, requestID)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
			c.Request.Header.Set("Authorization", "Bearer client-secret")
			c.Request.Header.Set("X-Request-ID", requestID)
			svc.runtime().HandleForward(c, "/v1/chat/completions", protocolOpenAI)
			if !strings.Contains(response.Body.String(), "hello") || calls[0].Load() != 1 || calls[1].Load() != 1 || calls[2].Load() != 0 {
				t.Fatalf("calls=%d/%d/%d body=%s", calls[0].Load(), calls[1].Load(), calls[2].Load(), response.Body.String())
			}
			var logs []storage.GatewayUsageLog
			if err := db.Order("attempt").Find(&logs).Error; err != nil {
				t.Fatal(err)
			}
			if len(logs) != 2 {
				t.Fatalf("logs=%d", len(logs))
			}
			for _, log := range logs {
				var decision schedulingDecision
				if err := json.Unmarshal([]byte(log.SchedulingDecision), &decision); err != nil {
					t.Fatalf("missing decision: %v", err)
				}
				if decision.Ceiling > 0.060000001 {
					t.Fatalf("ceiling changed during failover: %f", decision.Ceiling)
				}
			}
			configured, err := routes.ListByGroupID(group.ID)
			if err != nil {
				t.Fatal(err)
			}
			configured, err = svc.runtime().filterRoutesForRequestedModel(configured, "m", nil)
			if err != nil {
				t.Fatal(err)
			}
			req := newAdaptiveRequest(group, "m", "", "/v1/chat/completions", requestID, false, len(body), true)
			for i := 0; i < 2; i++ {
				stats := svc.schedulingStatistics().estimate(req.key(configured[i]), time.Now(), 5, 1)
				if stats.Samples != 1 || stats.FirstSamples != i {
					t.Fatalf("source %d statistics=%+v", i, stats)
				}
			}
		})
	}
}
