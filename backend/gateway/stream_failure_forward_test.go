package gateway

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestStreamFailuresThroughGatewayAuditAndSettlement(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		validate, output, late bool
		payload                string
		zeroRejected           bool
	}{
		{"failed-missing-usage", true, false, false, badStatusResponsesFailure, false},
		{"legacy-failed-missing-usage", false, false, false, overloadedResponsesFailure, false},
		{"failed-after-output-late-rule", true, true, true, overloadedResponsesFailure, false},
		{"legacy-failed-after-output", false, true, false, overloadedResponsesFailure, false},
		{"empty-missing-usage", true, false, false, `{"type":"response.completed","response":{"status":"completed","output":[]}}`, true},
		{"failed-with-real-usage", true, true, false, `{"type":"response.failed","response":{"status":"failed","error":{"message":"upstream busy"},"usage":{"input_tokens":7,"output_tokens":2}}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openGatewayTestDB(t)
			cipher, err := crypto.NewCipher("stream-failure-regression")
			if err != nil {
				t.Fatal(err)
			}
			secret, err := cipher.Encrypt("test-key")
			if err != nil {
				t.Fatal(err)
			}
			groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
			group := &storage.GatewayGroup{
				Name: "stream-failure", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
				RetryEnabled: true, FailoverEnabled: true, FailoverMax: 1, RequestMaxAttempts: 2,
				RequestFirstTokenTimeoutSec: 5, ResponseValidationEnabled: tc.validate,
				ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 1,
			}
			if err := groups.Create(group); err != nil {
				t.Fatal(err)
			}
			if err := db.Model(group).Update("response_validation_retry_count", 0).Error; err != nil {
				t.Fatal(err)
			}
			key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive}
			if err := keys.Create(key); err != nil {
				t.Fatal(err)
			}
			rules := storage.NewGatewayResponseRules(db)
			if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "zero", Enabled: true, Target: storage.GatewayResponseRuleTargetZeroUsage, Pattern: storage.GatewayResponseRuleZeroUsagePattern}); err != nil {
				t.Fatal(err)
			}
			if tc.late {
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "overloaded", Enabled: true, Target: "error_message", Pattern: "currently overloaded"}); err != nil {
					t.Fatal(err)
				}
			}
			providers := storage.NewGatewayProviders(db)
			var calls [2]atomic.Int32
			var inputs []storage.GatewayRoute
			for index := 0; index < 2; index++ {
				index := index
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[index].Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					if index == 1 {
						_, _ = io.WriteString(w, zeroUsageTestBody(protocol.KindOpenAIResponses, true, "fallback answer", 2, true, true))
						return
					}
					_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
					if tc.output {
						_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial answer\"}\n\n")
					}
					_, _ = io.WriteString(w, "data: "+tc.payload+"\n\n")
				}))
				t.Cleanup(upstream.Close)
				provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
				if err := providers.Create(provider); err != nil {
					t.Fatal(err)
				}
				rate := float64(index+1) / 10
				inputs = append(inputs, storage.GatewayRoute{
					GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider,
					GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses,
					RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate,
				})
			}
			if err := routes.SaveForGroup(group.ID, inputs); err != nil {
				t.Fatal(err)
			}
			svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
			svc.SetProviders(providers)
			svc.SetResponseRules(rules)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4o","stream":true,"input":"hi"}`))
			c.Request.Header.Set("Authorization", "Bearer client-key")
			svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
			wantLogs, wantFallback := 2, int32(1)
			if tc.output {
				wantLogs, wantFallback = 1, 0
			}
			if calls[0].Load() != 1 || calls[1].Load() != wantFallback {
				t.Fatalf("unexpected failover: calls=%d/%d body=%s", calls[0].Load(), calls[1].Load(), recorder.Body.String())
			}
			var logs []storage.GatewayUsageLog
			if err := db.Order("attempt").Find(&logs).Error; err != nil {
				t.Fatal(err)
			}
			if len(logs) != wantLogs {
				t.Fatalf("logs=%+v", logs)
			}
			failed := logs[0]
			wantStatus := storage.GatewayAttemptStatusError
			if tc.zeroRejected {
				wantStatus = storage.GatewayAttemptStatusRejected
			}
			if failed.Success || failed.Winner || failed.BilledCost != 0 || failed.AttemptStatus != wantStatus {
				t.Fatalf("failed response accepted or settled: %+v", failed)
			}
			if !tc.zeroRejected && (failed.ErrorType != "upstream_error" || !strings.Contains(failed.UpstreamErrorBody, "response.failed")) {
				t.Fatalf("failure details missing: %+v", failed)
			}
			if tc.late && (!failed.ValidationPostCommit || failed.ValidationRuleName != "overloaded") {
				t.Fatalf("late regex audit lost: %+v", failed)
			}
			if tc.name == "failed-with-real-usage" && (failed.InputTokens != 7 || failed.OutputTokens != 2) {
				t.Fatalf("failure usage lost: %+v", failed)
			}
			if tc.output {
				if !strings.Contains(recorder.Body.String(), "response.failed") || strings.Contains(recorder.Body.String(), "response.completed") || strings.Contains(recorder.Body.String(), "fallback answer") {
					t.Fatalf("failure masked or streams spliced: %s", recorder.Body.String())
				}
				var settlements int64
				if err := db.Model(&storage.GatewayWinnerSettlement{}).Count(&settlements).Error; err != nil || settlements != 0 {
					t.Fatalf("unexpected winner settlement: count=%d err=%v", settlements, err)
				}
			} else if !logs[1].Success || !logs[1].Winner || !strings.Contains(recorder.Body.String(), "fallback answer") || strings.Contains(recorder.Body.String(), "response.failed") {
				t.Fatalf("fallback not delivered/settled: logs=%+v body=%s", logs, recorder.Body.String())
			}
		})
	}
}
