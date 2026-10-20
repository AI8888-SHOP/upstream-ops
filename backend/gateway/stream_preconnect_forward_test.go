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

// The first upstream cannot respond until the real HTTP client has received
// a complete SSE comment. Retry ordering therefore needs no arbitrary sleeps.
func TestStreamPreconnectThroughForwardingAndSettlement(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, coordinated := range []bool{false, true} {
		for _, tc := range []struct {
			name                                     string
			enabled, stream, fallbackOK, rejectFirst bool
			upstreamError                            int
		}{
			{"keepalive-then-failover-success", true, true, true, false, http.StatusServiceUnavailable},
			{"keepalive-then-validation-failover", true, true, true, true, http.StatusServiceUnavailable},
			{"keepalive-then-all-failed", true, true, false, false, http.StatusServiceUnavailable},
			{"disabled-retains-503", false, true, false, false, http.StatusServiceUnavailable},
			{"disabled-retains-502", false, true, false, false, http.StatusBadGateway},
			{"nonstream-retains-503", true, false, false, false, http.StatusServiceUnavailable},
		} {
			if tc.rejectFirst && !coordinated {
				continue
			}
			t.Run(fmt.Sprintf("coordinated=%v/%s", coordinated, tc.name), func(t *testing.T) {
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("preconnect-forward-regression")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("upstream-test-key")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				group := &storage.GatewayGroup{
					GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: tc.enabled},
					Name:                          "preconnect-forward", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					RetryEnabled: true, RetryCount: 0, FailoverEnabled: true, FailoverMax: 1,
					RequestFirstTokenTimeoutSec: 10, ResponseValidationEnabled: coordinated,
					ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 8192,
				}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				if err := db.Model(group).Update("response_validation_retry_count", 0).Error; err != nil {
					t.Fatal(err)
				}
				key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("preconnect-client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
				if err := keys.Create(key); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if coordinated {
					// A real rule enables the coordinator, without concurrent hedging.
					if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "blocked-output", Enabled: true, Target: storage.GatewayResponseRuleTargetAssistantText, Pattern: "blocked-preconnect-answer"}); err != nil {
						t.Fatal(err)
					}
				}
				var calls [2]atomic.Int32
				commentReceived := make(chan struct{})
				var releaseOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(commentReceived) }) }
				wantPreconnect := tc.enabled && tc.stream
				providers := storage.NewGatewayProviders(db)
				var inputs []storage.GatewayRoute
				for index := 0; index < 2; index++ {
					index := index
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls[index].Add(1)
						if index == 0 && wantPreconnect {
							select {
							case <-commentReceived:
							case <-r.Context().Done():
								return
							}
						}
						if (index == 1 && tc.fallbackOK) || (index == 0 && tc.rejectFirst) {
							w.Header().Set("Content-Type", "text/event-stream")
							text := "fallback answer"
							if index == 0 {
								text = "blocked-preconnect-answer"
							}
							payload := zeroUsageTestBody(protocol.KindOpenAIResponses, true, text, 2, true, true)
							payload = strings.ReplaceAll(payload, "\"input_tokens\":0", "\"input_tokens\":7")
							_, _ = io.WriteString(w, payload)
							return
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(tc.upstreamError)
						_, _ = io.WriteString(w, "{\"error\":{\"type\":\"upstream_error\",\"message\":\"upstream busy for preconnect regression\"}}")
					}))
					t.Cleanup(upstream.Close)
					provider := &storage.GatewayProvider{Name: fmt.Sprintf("preconnect-source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
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
				validator, err := svc.runtime().responseValidatorForGroup(group)
				if err != nil {
					t.Fatal(err)
				}
				if usesCoordinator, _, _ := svc.runtime().shouldUseCoordinatedForward(group, validator, hedgeRequest{Path: "/v1/responses", Model: "gpt-4o", Stream: tc.stream}); usesCoordinator != coordinated {
					t.Fatalf("fixture coordinator=%v, want %v", usesCoordinator, coordinated)
				}
				handlerDone := make(chan struct{})
				var installedPreconnect bool // Read only after handlerDone closes.
				router := gin.New()
				router.POST("/v1/responses", func(c *gin.Context) {
					defer close(handlerDone)
					svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
					installedPreconnect = streamPreconnectFor(c) != nil
				})
				server := httptest.NewServer(router)
				t.Cleanup(func() { release(); server.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(fmt.Sprintf("{\"model\":\"gpt-4o\",\"stream\":%v,\"input\":\"hi\"}", tc.stream)))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Authorization", "Bearer preconnect-client-key")
				req.Header.Set("Content-Type", "application/json")
				response, err := server.Client().Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				reader := bufio.NewReader(response.Body)
				var prefix strings.Builder
				if wantPreconnect {
					if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
						t.Fatalf("early response status=%d headers=%v", response.StatusCode, response.Header)
					}
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							t.Fatalf("read complete keepalive: %v", err)
						}
						prefix.WriteString(line)
						if strings.TrimSpace(line) == "" {
							break
						}
						if !strings.HasPrefix(line, ":") {
							t.Fatalf("model output fabricated before upstream responded: %q", line)
						}
					}
					if !strings.Contains(prefix.String(), "keepalive") {
						t.Fatalf("missing SSE comment: %q", prefix.String())
					}
					release()
				}
				rest, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				body := prefix.String() + string(rest)
				select {
				case <-handlerDone:
				case <-ctx.Done():
					t.Fatal("forward handler did not finish")
				}
				if installedPreconnect != wantPreconnect {
					t.Fatalf("installed preconnect=%v, want %v", installedPreconnect, wantPreconnect)
				}
				if calls[0].Load() != 1 || calls[1].Load() != 1 {
					t.Fatalf("early 200 prevented failover: calls=%d/%d body=%s", calls[0].Load(), calls[1].Load(), body)
				}
				var logs []storage.GatewayUsageLog
				if err := db.Order("attempt").Find(&logs).Error; err != nil || len(logs) != 2 {
					t.Fatalf("usage rows=%+v err=%v", logs, err)
				}
				wantFirstStatus := storage.GatewayAttemptStatusError
				if tc.rejectFirst {
					wantFirstStatus = storage.GatewayAttemptStatusRejected
				}
				if logs[0].Success || logs[0].Winner || logs[0].BilledCost != 0 || logs[0].AttemptStatus != wantFirstStatus || logs[0].RequestFirstTokenMS != nil {
					t.Fatalf("keepalive recorded as successful model output: %+v", logs[0])
				}
				var finalization storage.GatewayRequestFinalization
				if err := db.Where("request_id = ?", logs[0].RequestID).First(&finalization).Error; err != nil {
					t.Fatal(err)
				}
				if tc.fallbackOK {
					if response.StatusCode != http.StatusOK || !strings.Contains(body, "fallback answer") || !strings.Contains(body, "response.completed") || strings.Contains(body, "response.failed") || strings.Contains(body, "blocked-preconnect-answer") {
						t.Fatalf("fallback body invalid: status=%d body=%s", response.StatusCode, body)
					}
					if !logs[1].Success || !logs[1].Winner || logs[1].InputTokens != 7 || logs[1].OutputTokens != 2 || logs[1].FirstTokenMS == nil || logs[1].RequestFirstTokenMS == nil || !finalization.Delivered {
						t.Fatalf("fallback delivery/usage lost: usage=%+v final=%+v", logs[1], finalization)
					}
				} else {
					if wantPreconnect {
						if response.StatusCode != http.StatusOK || !strings.Contains(body, "event: response.failed") || strings.Contains(body, "response.completed") {
							t.Fatalf("all-failed response not terminal SSE: status=%d body=%s", response.StatusCode, body)
						}
					} else if response.StatusCode != tc.upstreamError || strings.Contains(body, ": upstream-ops keepalive") || !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
						t.Fatalf("excluded request changed HTTP error: status=%d body=%s", response.StatusCode, body)
					}
					if logs[1].Success || logs[1].Winner || logs[1].BilledCost != 0 || logs[1].AttemptStatus != storage.GatewayAttemptStatusError || logs[1].RequestFirstTokenMS != nil || finalization.Delivered || finalization.BilledCost != 0 {
						t.Fatalf("failed stream falsely settled: usage=%+v final=%+v", logs[1], finalization)
					}
					var settlements int64
					if err := db.Model(&storage.GatewayWinnerSettlement{}).Count(&settlements).Error; err != nil || settlements != 0 {
						t.Fatalf("failed request has winner settlement: count=%d err=%v", settlements, err)
					}
					var persistedKey storage.GatewayKey
					if err := db.First(&persistedKey, key.ID).Error; err != nil || persistedKey.QuotaUsed != 0 {
						t.Fatalf("failed request charged quota: used=%v err=%v", persistedKey.QuotaUsed, err)
					}
				}
			})
		}
	}
}
