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

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Exercise authentication, local mapping, Anthropic -> Responses conversion,
// regex failover and persisted per-attempt audit in both forwarding paths.
func TestResponseModelAuditAndRejectionThroughGateway(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, reject := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%v/reject=%v", stream, reject), func(t *testing.T) {
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("response-model-integration")
				if err != nil {
					t.Fatal(err)
				}
				ciphertext, err := cipher.Encrypt("test-secret")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				group := &storage.GatewayGroup{
					Name: "model-audit", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					ModelMappingJSON: `{"public-main":"sent-main"}`,
					RetryEnabled:     true, FailoverEnabled: true, FailoverMax: 3, RequestMaxAttempts: 3,
					ResponseValidationEnabled: reject, ResponseValidationRetryCount: 0,
					ResponseValidationStreamMode: "prefix", RequestFirstTokenTimeoutSec: 5,
				}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				// Persist zero explicitly rather than letting GORM's -1 default apply.
				if err := db.Model(group).Update("response_validation_retry_count", 0).Error; err != nil {
					t.Fatal(err)
				}
				key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-secret"), KeyCipher: ciphertext, Status: storage.GatewayKeyStatusActive}
				if err := keys.Create(key); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if err := rules.Create(&storage.GatewayResponseRule{
					GatewayGroupID: group.ID, Name: "deny-mini", Enabled: true,
					Target: storage.GatewayResponseRuleTargetResponseModel, Pattern: `^mini$`,
					ModelsJSON: `["public-main"]`, ProtocolsJSON: `["openai_responses"]`,
				}); err != nil {
					t.Fatal(err)
				}
				providers := storage.NewGatewayProviders(db)
				var calls [2]atomic.Int32
				var wrongSentModel atomic.Bool
				inputs := make([]storage.GatewayRoute, 0, 2)
				for index, declared := range []string{"mini", "sent-main"} {
					index, declared := index, declared
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls[index].Add(1)
						var body struct {
							Model string `json:"model"`
						}
						if json.NewDecoder(r.Body).Decode(&body) != nil || body.Model != "sent-main" {
							wrongSentModel.Store(true)
							w.WriteHeader(http.StatusBadRequest)
							return
						}
						if !stream {
							w.Header().Set("Content-Type", "application/json")
							_, _ = fmt.Fprintf(w, `{"id":"msg-1","type":"message","role":"assistant","model":%q,"content":[{"type":"text","text":%q}],"stop_reason":"end_turn","usage":{"input_tokens":10,"output_tokens":2}}`, declared, "answer-"+declared)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg-1\",\"type\":\"message\",\"role\":\"assistant\",\"model\":%q,\"content\":[],\"usage\":{\"input_tokens\":10,\"output_tokens\":0}}}\n\n", declared)
						_, _ = io.WriteString(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n")
						_, _ = fmt.Fprintf(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":%q}}\n\n", "answer-"+declared)
						_, _ = io.WriteString(w, "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
					}))
					t.Cleanup(upstream.Close)
					provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: ciphertext, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
					if err := providers.Create(provider); err != nil {
						t.Fatal(err)
					}
					rate := float64(index+1) / 10
					inputs = append(inputs, storage.GatewayRoute{
						GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider,
						GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolAnthropic,
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
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":"public-main","stream":%v,"input":"hi"}`, stream)))
				c.Request.Header.Set("Authorization", "Bearer client-secret")
				svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
				if wrongSentModel.Load() || calls[0].Load() != 1 {
					t.Fatalf("wrong sent model=%v, first calls=%d", wrongSentModel.Load(), calls[0].Load())
				}
				wantModel, wantLogs, wantFallback := "mini", 1, int32(0)
				if reject {
					wantModel, wantLogs, wantFallback = "sent-main", 2, 1
				}
				if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "answer-"+wantModel) || calls[1].Load() != wantFallback {
					t.Fatalf("status=%d calls=%d/%d body=%s", recorder.Code, calls[0].Load(), calls[1].Load(), recorder.Body.String())
				}
				if reject && strings.Contains(recorder.Body.String(), "answer-mini") {
					t.Fatalf("rejected output leaked: %s", recorder.Body.String())
				}
				var logs []storage.GatewayUsageLog
				if err := db.Order("attempt").Find(&logs).Error; err != nil {
					t.Fatal(err)
				}
				if len(logs) != wantLogs {
					t.Fatalf("logs=%+v", logs)
				}
				for i, log := range logs {
					if log.RequestedModel != "public-main" || log.UpstreamModel != "sent-main" || !log.ProtocolConverted || log.UpstreamModelMismatch == nil || *log.UpstreamModelMismatch != (i == 0) {
						t.Fatalf("audit %d = %+v", i, log)
					}
					if i == 0 && log.UpstreamResponseModel != "mini" || i == 1 && log.UpstreamResponseModel != "sent-main" {
						t.Fatalf("response model %d = %q", i, log.UpstreamResponseModel)
					}
				}
				if reject && (logs[0].Winner || logs[0].AttemptStatus != storage.GatewayAttemptStatusRejected || logs[0].ValidationRuleName != "deny-mini" || logs[0].ValidationPostCommit) {
					t.Fatalf("rejected audit = %+v", logs[0])
				}
			})
		}
	}
}
