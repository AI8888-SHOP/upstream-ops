package gateway

import (
	"context"
	"fmt"
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

func TestReservePoolKeepsOrdinaryTrafficAndAbsoluteBudget(t *testing.T) {
	for _, mode := range []string{"cost", "balanced", "latency"} {
		t.Run(mode, func(t *testing.T) {
			rt, group, _, candidates := adaptiveFixture()
			group.SchedulingMode, group.MaxBillingRateMultiplier, group.LoadBalanceRouteCount = mode, 0.08, 4
			candidates[2].Route.FallbackOnly = true
			req := newAdaptiveRequest(group, "m", "high", "/v1/responses", "reserve", false, 100, true)
			for i := 0; i < 10; i++ {
				got := rt.orderAdaptiveCandidates(candidates, req, nil, true, time.Now())
				if len(got) != 3 || got[0].Route.FallbackOnly || !got[2].Route.FallbackOnly {
					t.Fatalf("reserve entered primary traffic: %+v", got)
				}
			}
			// The reserve survives the ordinary 20% premium, but not the hard cap.
			got := rt.orderAdaptiveCandidates(candidates[2:], req, nil, false, time.Now())
			if len(got) != 1 {
				t.Fatal("explicit reserve filtered by relative premium")
			}
			group.MaxBillingRateMultiplier = 0.07
			if got := rt.orderAdaptiveCandidates(candidates[2:], req, nil, false, time.Now()); len(got) != 0 {
				t.Fatal("reserve escaped hard price cap")
			}
		})
	}
}

func TestCandyNetworkBackoffRecoveryDoesNotReleaseWrongAnswers(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Minute)
	routes := []storage.GatewayRoute{
		{ID: 1, Enabled: true, SourceChannelID: 1, SourceAPIKeyCipher: "cipher", CandyCheck: &storage.GatewayRouteCandyCheck{Active: true, Status: "error", BackoffUntil: &later}},
		{ID: 2, Enabled: true, SourceChannelID: 2, SourceAPIKeyCipher: "cipher", CandyCheck: &storage.GatewayRouteCandyCheck{Active: true, Status: "incorrect", CooldownUntil: &later}},
	}
	rt := (&Service{}).runtime()
	recovered := rt.recoverWhenAllRoutesRestricted(routes, "m", nil, now)
	if !IsRouteSchedulableForModel(&recovered[0], "m", now) || IsRouteSchedulableForModel(&recovered[1], "m", now) {
		t.Fatal("network recovery bypassed quality rejection")
	}
	if !routes[0].CandyCheck.BackingOff(now) {
		t.Fatal("request-local recovery mutated shared state")
	}
}

// Exercise the final launch budget as well as a single-candidate route pool.
// Before the fix both forwarding paths removed this per-attempt deadline.
func TestLastAttemptHonorsFirstTokenDeadline(t *testing.T) {
	for _, coordinated := range []bool{false, true} {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("coordinated=%v/stream=%v", coordinated, stream), func(t *testing.T) {
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("final-deadline-test")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("key")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				group := &storage.GatewayGroup{Name: "deadline", RetryEnabled: true, RetryCount: 10, FailoverEnabled: true, FailoverMax: 8, RequestMaxAttempts: 1, FirstTokenTimeoutSec: 1, RequestFirstTokenTimeoutSec: 5, ResponseValidationEnabled: coordinated, ResponseValidationStreamMode: "prefix"}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				if err := keys.Create(&storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive}); err != nil {
					t.Fatal(err)
				}
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					select {
					case <-r.Context().Done():
					case <-time.After(3 * time.Second):
					}
				}))
				t.Cleanup(server.Close)
				providers := storage.NewGatewayProviders(db)
				provider := &storage.GatewayProvider{Name: "stalled", Enabled: true, BaseURL: server.URL, APIKeyCipher: secret, ModelPolicy: "all"}
				if err := providers.Create(provider); err != nil {
					t.Fatal(err)
				}
				if err := routes.SaveForGroup(group.ID, []storage.GatewayRoute{{Enabled: true, SourceKind: "provider", GatewayProviderID: provider.ID, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses}}); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Enabled: true, Name: "unused", Target: "error_message", Pattern: "never-match"}); err != nil {
					t.Fatal(err)
				}
				svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
				svc.SetProviders(providers)
				svc.SetResponseRules(rules)
				w := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(w)
				ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
				defer cancel()
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"m","stream":%v,"input":"hello"}`, stream))).WithContext(ctx)
				c.Request.Header.Set("Authorization", "Bearer client")
				start := time.Now()
				svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
				if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
					t.Fatalf("final attempt ignored deadline: %s", elapsed)
				}
				if calls.Load() != 1 || !strings.Contains(w.Body.String(), "first token timeout") {
					t.Fatalf("calls=%d response=%s", calls.Load(), w.Body.String())
				}
				var logs []storage.GatewayUsageLog
				if err := db.Find(&logs).Error; err != nil || len(logs) != 1 || logs[0].Success || logs[0].Winner {
					t.Fatalf("timeout settled or audit missing: %+v %v", logs, err)
				}
			})
		}
	}
}

func TestSlowHTTPFailureIsNotRetriedOnSameSource(t *testing.T) {
	for _, status := range []int{408, 504, 524} {
		info := usageErrorInfo{Summary: "upstream timed out"}
		if !(*Service)(nil).isFailoverStatus(status, false) || isSameRouteRetryableUpstreamFailure(status, info) {
			t.Fatalf("status %d does not switch sources", status)
		}
		if !coordinatedAttemptSuppressesSameRouteRetries(&coordinatedForwardAttempt{Status: status, ErrInfo: info}) {
			t.Fatalf("coordinator retries %d", status)
		}
	}
}

func TestNoCandidateAuditIsVisibleWithoutUpstreamCharge(t *testing.T) {
	db := openGatewayTestDB(t)
	groups, keys := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db)
	group := &storage.GatewayGroup{Name: "empty-pool"}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: "no-candidate-test", Status: storage.GatewayKeyStatusActive}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, storage.NewGatewayRoutes(db), storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), nil, nil, nil, nil)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	svc.runtime().failNoRoutes(c, key, group, []storage.GatewayRoute{{ID: 99, SourceAPIKeyCipher: "DO-NOT-LOG-ME", Enabled: false}}, "m", "/v1/responses", protocol.KindOpenAIResponses, true, "no schedulable routes", nil)
	var logs []storage.GatewayUsageLog
	if err := db.Find(&logs).Error; err != nil || len(logs) != 1 {
		t.Fatalf("missing audit: %v %v", logs, err)
	}
	log := logs[0]
	if log.ErrorType != "routing" || log.RouteID != 0 || log.Success || log.BilledCost != 0 || !strings.Contains(log.ErrorDetail, "渠道路由已关闭") || strings.Contains(log.ErrorDetail, "DO-NOT-LOG-ME") {
		t.Fatalf("invalid routing audit: %+v", log)
	}
	if w.Code != 503 || w.Header().Get("Retry-After") == "" || !strings.Contains(w.Body.String(), "route_unavailable") {
		t.Fatalf("missing client hint: %s", w.Body.String())
	}
	var final storage.GatewayRequestFinalization
	if err := db.First(&final, "request_id = ?", log.RequestID).Error; err != nil || final.Delivered {
		t.Fatalf("failed request not finalized: %+v %v", final, err)
	}
}
