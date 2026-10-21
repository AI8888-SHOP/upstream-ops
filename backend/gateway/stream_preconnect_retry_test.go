package gateway

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// The first source waits until the real HTTP client receives preconnect. A
// rejected attempt must stay private even after that early HTTP 200 is sent.
func TestStreamPreconnectRetainsRejectedFailureRecovery(t *testing.T) {
	for _, mode := range []struct {
		name              string
		sub2api, multihop bool
	}{
		{"comment", false, false},
		{"sub2api", true, false},
		{"multihop", true, true},
	} {
		for _, tc := range []struct {
			name                 string
			status               int
			alternative, recover bool
			maxAttempts          int
			invalidKey           bool
			wantCalls            [2]int32
		}{
			{"502-retry-recovers", 502, false, true, 0, false, [2]int32{2, 0}},
			{"500-retry-recovers", 500, false, true, 0, false, [2]int32{2, 0}},
			{"502-fresh-route-first", 502, true, true, 0, false, [2]int32{1, 1}},
			{"503-fresh-route-first", 503, true, true, 0, false, [2]int32{1, 1}},
			{"http200-failed-stream-switches", 200, true, true, 0, false, [2]int32{1, 1}},
			{"502-bounded-retry-exhaustion", 502, false, false, 0, false, [2]int32{3, 0}},
			{"request-budget-bounds-retries", 502, false, false, 1, false, [2]int32{1, 0}},
			{"502-invalid-key-not-replayed", 502, false, false, 0, true, [2]int32{1, 0}},
		} {
			t.Run(mode.name+"/"+tc.name, func(t *testing.T) {
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("preconnect-retry-regression")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("preconnect-retry-upstream-key")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				group := &storage.GatewayGroup{
					Name: "preconnect-retry", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					RetryEnabled: true, RetryCount: 2, FailoverEnabled: true, FailoverMax: 1, RequestMaxAttempts: tc.maxAttempts,
					RequestFirstTokenTimeoutSec: 10, ResponseValidationEnabled: true, ResponseValidationRetryCount: 2,
					ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 8192,
					GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
						StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: mode.sub2api, StreamPreconnectMultihopEnabled: mode.multihop,
					},
				}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("preconnect-retry-client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
				if err := keys.Create(key); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "retryable-upstream-error", Enabled: true, Target: "error_message", Pattern: "upstream failed"}); err != nil {
					t.Fatal(err)
				}
				receivedPreconnect := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(receivedPreconnect) }) }
				t.Cleanup(release)
				providers := storage.NewGatewayProviders(db)
				var calls [2]atomic.Int32
				var inputs []storage.GatewayRoute
				sourceCount := 1
				if tc.alternative {
					sourceCount = 2
				}
				for index := 0; index < sourceCount; index++ {
					index := index
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						call := calls[index].Add(1)
						select {
						case <-receivedPreconnect:
						case <-r.Context().Done():
							return
						}
						if tc.recover && (index == 1 || call > 1) {
							w.Header().Set("Content-Type", "text/event-stream")
							_, _ = io.WriteString(w, preconnectForwardResponse("recovered answer"))
							return
						}
						w.Header().Set("Content-Type", "application/json")
						payload := "{\"error\":{\"type\":\"upstream_error\",\"message\":\"upstream failed\"}}"
						if tc.invalidKey {
							payload = "{\"error\":{\"code\":\"invalid_api_key\",\"message\":\"upstream failed\"}}"
						}
						if tc.status == http.StatusOK {
							w.Header().Set("Content-Type", "text/event-stream")
							payload = "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"type\":\"server_error\",\"message\":\"upstream failed\"}}}\n\n"
						}
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, payload)
					}))
					t.Cleanup(upstream.Close)
					provider := &storage.GatewayProvider{Name: fmt.Sprintf("retry-source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
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
				done := make(chan struct{})
				router := gin.New()
				router.POST("/v1/responses", func(c *gin.Context) {
					defer close(done)
					svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
				})
				server := httptest.NewServer(router)
				t.Cleanup(func() { release(); server.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader("{\"model\":\"gpt-4o\",\"stream\":true,\"store\":false,\"input\":\"hi\"}"))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "Bearer preconnect-retry-client-key")
				req.Header.Set("Content-Type", "application/json")
				resp, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				if resp.StatusCode != http.StatusOK || !strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream") {
					t.Fatalf("early preconnect missing: status=%d headers=%v", resp.StatusCode, resp.Header)
				}
				reader := bufio.NewReader(resp.Body)
				var prefix strings.Builder
				for {
					line, err := reader.ReadString('\n')
					if err != nil {
						t.Fatalf("preconnect before source reply: %v", err)
					}
					prefix.WriteString(line)
					if strings.TrimSpace(line) == "" && (!mode.multihop || strings.Contains(prefix.String(), "event: response.reasoning_summary_text.delta\n")) {
						break
					}
				}
				if mode.sub2api {
					if !strings.Contains(prefix.String(), "resp_uops_preconnect_") {
						t.Fatalf("synthetic lifecycle was delayed: %s", prefix.String())
					}
				} else if !strings.Contains(prefix.String(), ": upstream-ops keepalive") {
					t.Fatalf("transport preconnect was delayed: %s", prefix.String())
				}
				release()
				rest, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-done:
				case <-ctx.Done():
					t.Fatal("gateway did not finish within request deadline")
				}
				body := prefix.String() + string(rest)
				if got := [2]int32{calls[0].Load(), calls[1].Load()}; got != tc.wantCalls {
					t.Fatalf("source calls=%v want=%v body=%s", got, tc.wantCalls, body)
				}
				var logs []storage.GatewayUsageLog
				if err := db.Order("attempt").Find(&logs).Error; err != nil {
					t.Fatal(err)
				}
				if len(logs) != int(tc.wantCalls[0]+tc.wantCalls[1]) {
					t.Fatalf("attempt log count=%d calls=%v", len(logs), tc.wantCalls)
				}
				failedCount := len(logs)
				if tc.recover {
					failedCount--
					winner := logs[len(logs)-1]
					if !winner.Success || !winner.Winner || winner.InputTokens != 7 || winner.OutputTokens != 2 || winner.RequestFirstTokenMS == nil {
						t.Fatalf("recovered usage missing: %+v", winner)
					}
					if !strings.Contains(body, "recovered answer") || strings.Count(body, "event: response.completed\n") != 1 || strings.Contains(body, "response.failed") || strings.Contains(body, "upstream failed") {
						t.Fatalf("rejected source leaked into successful response: %s", body)
					}
				} else if strings.Count(body, "event: response.failed\n") != 1 || strings.Contains(body, "response.completed") {
					t.Fatalf("exhaustion must emit exactly one final failure: %s", body)
				}
				for _, log := range logs[:failedCount] {
					if log.Success || log.Winner || log.BilledCost != 0 || log.RequestFirstTokenMS != nil || log.ValidationPostCommit || log.AttemptStatus != storage.GatewayAttemptStatusRejected || log.ValidationRuleName != "retryable-upstream-error" {
						t.Fatalf("preconnect incorrectly committed or billed rejected attempt: %+v", log)
					}
				}
				var finalization storage.GatewayRequestFinalization
				if err := db.Where("request_id = ?", logs[0].RequestID).First(&finalization).Error; err != nil {
					t.Fatal(err)
				}
				if finalization.Delivered != tc.recover || (!tc.recover && finalization.BilledCost != 0) {
					t.Fatalf("invalid final settlement: %+v", finalization)
				}
			})
		}
	}
}
