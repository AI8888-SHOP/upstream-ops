package gateway

import (
	"bufio"
	"context"
	"encoding/json"
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

func preconnectForwardResponse(text string) string {
	var wire strings.Builder
	sequence := 0
	emit := func(kind string, fields map[string]any) {
		fields["type"], fields["sequence_number"] = kind, sequence
		sequence++
		payload, _ := json.Marshal(fields)
		fmt.Fprintf(&wire, "event: %s\ndata: %s\n\n", kind, payload)
	}
	part := map[string]any{"type": "output_text", "text": text, "annotations": []any{}}
	item := map[string]any{"id": "msg_real", "type": "message", "role": "assistant", "status": "completed", "content": []any{part}}
	emit("response.created", map[string]any{"response": map[string]any{"id": "resp_real", "model": "m", "object": "response", "status": "in_progress", "output": []any{}}})
	emit("response.output_item.added", map[string]any{"output_index": 0, "item": map[string]any{"id": "msg_real", "type": "message", "role": "assistant", "status": "in_progress", "content": []any{}}})
	emit("response.content_part.added", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_real", "part": map[string]any{"type": "output_text", "text": "", "annotations": []any{}}})
	emit("response.output_text.delta", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_real", "delta": text})
	emit("response.output_text.done", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_real", "text": text})
	emit("response.content_part.done", map[string]any{"output_index": 0, "content_index": 0, "item_id": "msg_real", "part": part})
	emit("response.output_item.done", map[string]any{"output_index": 0, "item": item})
	emit("response.completed", map[string]any{"response": map[string]any{"id": "resp_real", "model": "m", "object": "response", "status": "completed", "output": []any{item}, "usage": map[string]int{"input_tokens": 7, "output_tokens": 2}}})
	return wire.String()
}

// The first upstream cannot respond until the real HTTP client has received
// a complete SSE comment or compatibility lifecycle event. Retry ordering
// therefore needs no arbitrary sleeps.
func TestStreamPreconnectThroughForwardingAndSettlement(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, coordinated := range []bool{false, true} {
		for _, tc := range []struct {
			name                                     string
			enabled, stream, fallbackOK, rejectFirst bool
			upstreamError                            int
			sub2api                                  bool
			corrupt                                  string
			multihop                                 bool
		}{
			{"keepalive-then-failover-success", true, true, true, false, http.StatusServiceUnavailable, false, "", false},
			{"keepalive-then-validation-failover", true, true, true, true, http.StatusServiceUnavailable, false, "", false},
			{"keepalive-then-all-failed", true, true, false, false, http.StatusServiceUnavailable, false, "", false},
			{"disabled-retains-503", false, true, false, false, http.StatusServiceUnavailable, false, "", false},
			{"disabled-retains-502", false, true, false, false, http.StatusBadGateway, false, "", false},
			{"nonstream-retains-503", true, false, false, false, http.StatusServiceUnavailable, false, "", false},
			{"sub2api-then-failover-success", true, true, true, false, http.StatusServiceUnavailable, true, "", false},
			{"sub2api-then-validation-failover", true, true, true, true, http.StatusServiceUnavailable, true, "", false},
			{"sub2api-then-all-failed", true, true, false, false, http.StatusServiceUnavailable, true, "", false},
			{"sub2api-parent-disabled-retains-503", false, true, false, false, http.StatusServiceUnavailable, true, "", false},
			{"sub2api-nonstream-retains-503", true, false, false, false, http.StatusServiceUnavailable, true, "", false},
			{"sub2api-postcommit-eof-not-billed", true, true, true, false, http.StatusServiceUnavailable, true, "eof", false},
			{"sub2api-postcommit-bad-terminal-not-billed", true, true, true, false, http.StatusServiceUnavailable, true, "identity", false},
			{"multihop-then-failover-success", true, true, true, false, http.StatusServiceUnavailable, true, "", true},
			{"multihop-then-validation-failover", true, true, true, true, http.StatusServiceUnavailable, true, "", true},
			{"multihop-then-all-failed", true, true, false, false, http.StatusServiceUnavailable, true, "", true},
			{"multihop-parent-disabled-retains-503", false, true, false, false, http.StatusServiceUnavailable, true, "", true},
			{"multihop-semantic-parent-disabled-comment-only", true, true, false, false, http.StatusServiceUnavailable, false, "", true},
			{"multihop-nonstream-retains-503", true, false, false, false, http.StatusServiceUnavailable, true, "", true},
			{"multihop-postcommit-eof-not-billed", true, true, true, false, http.StatusServiceUnavailable, true, "eof", true},
			{"multihop-postcommit-bad-terminal-not-billed", true, true, true, false, http.StatusServiceUnavailable, true, "identity", true},
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
					GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: tc.enabled, StreamPreconnectSub2apiEnabled: tc.sub2api, StreamPreconnectMultihopEnabled: tc.multihop},
					Name:                          "preconnect-forward", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					RetryEnabled: true, RetryCount: 0, FailoverEnabled: true, FailoverMax: 1,
					RequestFirstTokenTimeoutSec: 10, ResponseValidationEnabled: coordinated,
					ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 8192,
				}
				if tc.corrupt != "" {
					// Commit on the first eligible real output; the terminal is held
					// by the fake upstream until the downstream receives that output.
					group.ResponseValidationPrefixBytes = 1
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
				answerReceived := make(chan struct{})
				var releaseOnce sync.Once
				var answerOnce sync.Once
				release := func() { releaseOnce.Do(func() { close(commentReceived) }) }
				releaseAnswer := func() { answerOnce.Do(func() { close(answerReceived) }) }
				t.Cleanup(releaseAnswer)
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
							if tc.sub2api {
								payload = preconnectForwardResponse(text)
							}
							if index == 1 && tc.corrupt != "" {
								terminal := strings.Index(payload, "event: response.completed\n")
								if terminal < 0 {
									t.Error("corrupt fixture has no terminal event")
									return
								}
								_, _ = io.WriteString(w, payload[:terminal])
								w.(http.Flusher).Flush()
								select {
								case <-answerReceived:
								case <-r.Context().Done():
									return
								}
								if tc.corrupt == "identity" {
									_, _ = io.WriteString(w, strings.ReplaceAll(payload[terminal:], `"type":"message"`, `"type":"function_call"`))
								}
								return
							}
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
				t.Cleanup(func() { release(); releaseAnswer(); server.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(fmt.Sprintf("{\"model\":\"gpt-4o\",\"stream\":%v,\"store\":false,\"input\":\"hi\"}", tc.stream)))
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
							if !tc.multihop || !tc.sub2api || strings.Contains(prefix.String(), "event: response.reasoning_summary_text.delta\n") {
								break
							}
							continue
						}
						if !tc.sub2api && !strings.HasPrefix(line, ":") {
							t.Fatalf("model output fabricated before upstream responded: %q", line)
						}
					}
					if tc.sub2api {
						if !strings.Contains(prefix.String(), "event: response.created\n") || !strings.Contains(prefix.String(), "resp_uops_preconnect_") {
							t.Fatalf("missing compatible lifecycle: %q", prefix.String())
						}
					} else if !strings.Contains(prefix.String(), "keepalive") {
						t.Fatalf("missing SSE comment: %q", prefix.String())
					}
					if tc.multihop && tc.sub2api {
						events := parsePreconnectResponsesTestEvents(t, []byte(prefix.String()))
						if len(events) != 5 {
							t.Fatalf("multihop preamble event count=%d: %s", len(events), prefix.String())
						}
						last := events[len(events)-1]
						if last.name != "response.reasoning_summary_text.delta" || preconnectResponsesTestString(t, last.root["delta"]) != " " {
							t.Fatalf("multihop did not reach client before real upstream output: %s", prefix.String())
						}
					}
					release()
				}
				if tc.corrupt != "" {
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							t.Fatalf("read real output before corrupt terminal: %v", err)
						}
						prefix.WriteString(line)
						if strings.TrimSpace(line) == "" && strings.Contains(prefix.String(), "fallback answer") {
							releaseAnswer()
							break
						}
					}
				}
				rest, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				body := prefix.String() + string(rest)
				if wantPreconnect && tc.sub2api {
					if strings.Count(body, "event: response.created\n") != 1 || !strings.Contains(body, "rs_uops_preconnect_") {
						t.Fatalf("compatibility lifecycle was duplicated or lost: %s", body)
					}
				} else if strings.Contains(body, "uops_preconnect_") {
					t.Fatalf("excluded request acquired synthetic lifecycle: %s", body)
				}
				wantSyntheticDelta := 0
				if wantPreconnect && tc.sub2api && tc.multihop {
					wantSyntheticDelta = 1
				}
				if count := strings.Count(body, "event: response.reasoning_summary_text.delta\n"); count != wantSyntheticDelta {
					t.Fatalf("synthetic delta count=%d want=%d: %s", count, wantSyntheticDelta, body)
				}
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
				if !tc.rejectFirst && (logs[0].InputTokens != 0 || logs[0].OutputTokens != 0) {
					t.Fatalf("synthetic preamble manufactured usage before upstream output: %+v", logs[0])
				}
				var finalization storage.GatewayRequestFinalization
				if err := db.Where("request_id = ?", logs[0].RequestID).First(&finalization).Error; err != nil {
					t.Fatal(err)
				}
				if tc.fallbackOK && tc.corrupt == "" {
					if response.StatusCode != http.StatusOK || !strings.Contains(body, "fallback answer") || !strings.Contains(body, "response.completed") || strings.Contains(body, "response.failed") || strings.Contains(body, "blocked-preconnect-answer") {
						t.Fatalf("fallback body invalid: status=%d body=%s", response.StatusCode, body)
					}
					if !logs[1].Success || !logs[1].Winner || logs[1].InputTokens != 7 || logs[1].OutputTokens != 2 || logs[1].FirstTokenMS == nil || logs[1].RequestFirstTokenMS == nil || !finalization.Delivered {
						t.Fatalf("fallback delivery/usage lost: usage=%+v final=%+v", logs[1], finalization)
					}
				} else {
					if tc.corrupt != "" {
						if response.StatusCode != http.StatusOK || !strings.Contains(body, "fallback answer") || !strings.Contains(body, "event: response.failed") || strings.Contains(body, "event: response.completed") {
							t.Fatalf("corrupt committed response not terminal failure: status=%d body=%s", response.StatusCode, body)
						}
					} else if wantPreconnect {
						if response.StatusCode != http.StatusOK || !strings.Contains(body, "event: response.failed") || strings.Contains(body, "response.completed") {
							t.Fatalf("all-failed response not terminal SSE: status=%d body=%s", response.StatusCode, body)
						}
					} else if response.StatusCode != http.StatusBadGateway || strings.Contains(body, "upstream busy") || !strings.Contains(body, "all upstream routes failed") || strings.Contains(body, ": upstream-ops keepalive") || !strings.HasPrefix(response.Header.Get("Content-Type"), "application/json") {
						t.Fatalf("all-failed response was not a sanitized gateway error: status=%d body=%s", response.StatusCode, body)
					}
					if logs[1].Success || logs[1].Winner || logs[1].BilledCost != 0 || logs[1].AttemptStatus != storage.GatewayAttemptStatusError || (tc.corrupt == "" && logs[1].RequestFirstTokenMS != nil) || finalization.Delivered || finalization.BilledCost != 0 {
						t.Fatalf("failed stream falsely settled: usage=%+v final=%+v", logs[1], finalization)
					}
					if tc.corrupt != "" && logs[1].RequestFirstTokenMS == nil {
						t.Fatalf("real output before failure lost true TTFT: %+v", logs[1])
					}
					if tc.corrupt == "" && (logs[1].InputTokens != 0 || logs[1].OutputTokens != 0) {
						t.Fatalf("all-failed synthetic stream manufactured usage: %+v", logs[1])
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
