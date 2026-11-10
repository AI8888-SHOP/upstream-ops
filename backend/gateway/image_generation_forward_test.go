package gateway

import (
	"bytes"
	"context"
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
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

const imageForwardModel = "canvas-custom-v2"
const imageForwardURL = "https://example.test/generated-image.png"
const imageForwardResponse = `{"created":1700000000,"data":[{"url":"https://example.test/generated-image.png"}]}`

// Keep client and provider credentials distinct so forwarding the client's
// Authorization header instead of decrypting the provider key is observable.
func newImageForwardService(t *testing.T, models string, validation, hedge bool, handler http.HandlerFunc) (*Service, *storage.GatewayGroup) {
	t.Helper()
	upstream := httptest.NewServer(handler)
	t.Cleanup(upstream.Close)
	db := openGatewayTestDB(t)
	cipher, err := crypto.NewCipher("image-generation-forward")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("image-upstream-secret")
	if err != nil {
		t.Fatal(err)
	}
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{
		Name: "image-forward", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
		ModelMappingJSON:          `{"public-canvas":"canvas-custom-v2"}`,
		ResponseValidationEnabled: validation, ResponseValidationStreamMode: "prefix",
		RequestFirstTokenTimeoutSec: 5,
		HedgeEnabled:                hedge, HedgeDelaySeconds: 0.001, HedgeMaxParallel: 2, HedgeMaxAttempts: 2,
		RetryEnabled: hedge, FailoverEnabled: hedge, FailoverMax: 1, RequestMaxAttempts: 2,
	}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := keys.Create(&storage.GatewayKey{
		GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("image-client-secret"),
		KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100,
	}); err != nil {
		t.Fatal(err)
	}
	providers := storage.NewGatewayProviders(db)
	provider := &storage.GatewayProvider{
		Name: "canvas-source", BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true,
		ModelPolicy: storage.GatewayProviderModelPolicyAll, ImageGenerationModelsJSON: models,
	}
	if err := providers.Create(provider); err != nil {
		t.Fatal(err)
	}
	inputs := []storage.GatewayRoute{{
		GatewayGroupID: group.ID, Position: 0, SourceKind: storage.GatewayRouteSourceProvider,
		GatewayProviderID: provider.ID, Enabled: true,
		RateConvertMode: "custom", RateConvertValue: 1, BillingRateMultiplier: 1,
	}}
	if hedge {
		// Give the alternative its own provider identity so source-level
		// deduplication cannot make the no-hedging assertion vacuous.
		alternative := *provider
		alternative.ID, alternative.Name = 0, "canvas-alternative"
		if err := providers.Create(&alternative); err != nil {
			t.Fatal(err)
		}
		second := inputs[0]
		second.GatewayProviderID = alternative.ID
		second.Position, second.RateConvertValue, second.BillingRateMultiplier = 1, 2, 2
		inputs = append(inputs, second)
	}
	if err := routes.SaveForGroup(group.ID, inputs); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
	svc.SetProviders(providers)
	if validation {
		// A group flag alone does not enable the coordinator: it also needs
		// an enabled rule. Do not require usage from an image-only response.
		rules := storage.NewGatewayResponseRules(db)
		if err := rules.Create(&storage.GatewayResponseRule{
			GatewayGroupID: group.ID, Name: "image-regression-sentinel", Enabled: true,
			Target: "raw_body", Pattern: "image-regression-reject-sentinel",
		}); err != nil {
			t.Fatal(err)
		}
		svc.SetResponseRules(rules)
	}
	return svc, group
}

func imageForwardRequest(svc *Service, path string, kind protocol.Kind, contentType string, body []byte) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	c.Request.Header.Set("Authorization", "Bearer image-client-secret")
	c.Request.Header.Set("Content-Type", contentType)
	svc.runtime().HandleForward(c, path, kind)
	return recorder
}

func assertImageForwardAuth(t *testing.T, r *http.Request) {
	t.Helper()
	if got := r.Header.Get("Authorization"); got != "Bearer image-upstream-secret" {
		t.Errorf("upstream Authorization=%q want provider credential", got)
	}
}

func assertImageForwardBody(t *testing.T, r *http.Request, promptParts ...string) {
	t.Helper()
	assertImageForwardAuth(t, r)
	if r.Method != http.MethodPost || r.URL.Path != "/v1/images/generations" {
		t.Errorf("upstream request=%s %s", r.Method, r.URL.Path)
	}
	var body map[string]json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Errorf("decode upstream body: %v", err)
		return
	}
	var model, prompt string
	if err := json.Unmarshal(body["model"], &model); err != nil || model != imageForwardModel {
		t.Errorf("upstream model=%q err=%v", model, err)
	}
	if err := json.Unmarshal(body["prompt"], &prompt); err != nil {
		t.Errorf("upstream prompt=%s err=%v", body["prompt"], err)
	}
	for _, part := range promptParts {
		if !strings.Contains(prompt, part) {
			t.Errorf("prompt=%q missing %q", prompt, part)
		}
	}
	for _, field := range []string{"messages", "input", "instructions", "system", "max_tokens", "max_output_tokens", "stream"} {
		if _, exists := body[field]; exists {
			t.Errorf("text protocol field %q leaked into image body: %s", field, body[field])
		}
	}
}

func TestImageGenerationMappedAliasForward(t *testing.T) {
	clients := []struct {
		name, path, body, object, streamMarker, streamEnd string
		kind                                              protocol.Kind
	}{
		{"chat", "/v1/chat/completions", `{"model":"public-canvas","messages":[{"role":"system","content":"paint softly"},{"role":"user","content":"draw a lighthouse"}],"max_tokens":64,"stream":%v,"size":"1024x1024"}`, "chat.completion", "chat.completion.chunk", "data: [DONE]", protocol.KindOpenAIChat},
		{"messages", "/v1/messages", `{"model":"public-canvas","system":"paint softly","messages":[{"role":"user","content":[{"type":"text","text":"draw a lighthouse"}]}],"max_tokens":64,"stream":%v,"size":"1024x1024"}`, "message", "event: content_block_delta", "event: message_stop", protocol.KindAnthropic},
		{"responses", "/v1/responses", `{"model":"public-canvas","instructions":"paint softly","input":"draw a lighthouse","max_output_tokens":64,"stream":%v,"size":"1024x1024"}`, "response", "event: response.output_text.delta", "event: response.completed", protocol.KindOpenAIResponses},
	}
	for _, client := range clients {
		for _, stream := range []bool{false, true} {
			for _, validation := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%v/validation=%v", client.name, stream, validation), func(t *testing.T) {
					var calls atomic.Int32
					svc, _ := newImageForwardService(t, `["canvas-custom-v2"]`, validation, true, func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						body, err := io.ReadAll(r.Body)
						if err != nil {
							t.Errorf("read image request: %v", err)
						}
						r.Body = io.NopCloser(bytes.NewReader(body))
						assertImageForwardBody(t, r, "paint softly", "draw a lighthouse")
						var imageRequest struct {
							Size string `json:"size"`
						}
						if err := json.Unmarshal(body, &imageRequest); err != nil || imageRequest.Size != "1024x1024" {
							t.Errorf("image options lost: body=%s err=%v", body, err)
						}
						// Exceed the hedge delay with an unfamiliar image model ID.
						// Two eligible routes exist, but generation must run only once.
						timer := time.NewTimer(30 * time.Millisecond)
						defer timer.Stop()
						select {
						case <-timer.C:
						case <-r.Context().Done():
							return
						}
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, imageForwardResponse)
					})
					recorder := imageForwardRequest(svc, client.path, client.kind, "application/json", []byte(fmt.Sprintf(client.body, stream)))
					body := recorder.Body.String()
					if recorder.Code != http.StatusOK || !strings.Contains(body, imageForwardURL) {
						t.Fatalf("status=%d body=%s", recorder.Code, body)
					}
					if calls.Load() != 1 {
						t.Errorf("upstream calls=%d want 1 (no concurrent image hedge)", calls.Load())
					}
					if stream {
						if !strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") || !strings.Contains(body, client.streamMarker) || !strings.Contains(body, client.streamEnd) {
							t.Fatalf("invalid %s SSE: headers=%v body=%s", client.name, recorder.Header(), body)
						}
						return
					}
					var response map[string]json.RawMessage
					if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
						t.Fatalf("invalid client JSON: %v body=%s", err, body)
					}
					field := "object"
					if client.kind == protocol.KindAnthropic {
						field = "type"
					}
					var object string
					if err := json.Unmarshal(response[field], &object); err != nil || object != client.object {
						t.Fatalf("client %s=%q err=%v body=%s", field, object, err, body)
					}
					contentField := "choices"
					switch client.kind {
					case protocol.KindAnthropic:
						contentField = "content"
					case protocol.KindOpenAIResponses:
						contentField = "output"
					}
					if !strings.Contains(string(response[contentField]), imageForwardURL) {
						t.Fatalf("image missing from client %s: %s", contentField, body)
					}
				})
			}
		}
	}
}

func TestImageGenerationConfigurationRequiresExactUpstreamModel(t *testing.T) {
	for _, models := range []string{"", `[]`, `["public-canvas"]`, `["canvas-custom"]`, `["canvas-custom-v2-extra"]`, `["CANVAS-CUSTOM-V2"]`} {
		t.Run("models="+models, func(t *testing.T) {
			var calls atomic.Int32
			svc, _ := newImageForwardService(t, models, false, false, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assertImageForwardAuth(t, r)
				if r.URL.Path != "/v1/chat/completions" {
					t.Errorf("unmatched model path=%s", r.URL.Path)
				}
				var body map[string]json.RawMessage
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Errorf("decode upstream: %v", err)
				}
				if string(body["model"]) != `"canvas-custom-v2"` || len(body["messages"]) == 0 || string(body["max_tokens"]) != "64" || len(body["prompt"]) != 0 {
					t.Errorf("unmatched configuration changed chat body: %v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"id":"chat-1","object":"chat.completion","model":"canvas-custom-v2","choices":[{"index":0,"message":{"role":"assistant","content":"ordinary chat"},"finish_reason":"stop"}]}`)
			})
			recorder := imageForwardRequest(svc, "/v1/chat/completions", protocol.KindOpenAIChat, "application/json", []byte(`{"model":"public-canvas","messages":[{"role":"user","content":"draw a lighthouse"}],"max_tokens":64,"stream":false}`))
			if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), "ordinary chat") || calls.Load() != 1 {
				t.Fatalf("status=%d calls=%d body=%s", recorder.Code, calls.Load(), recorder.Body.String())
			}
		})
	}
}

func TestImageGenerationNativeEndpointsRemainPassthrough(t *testing.T) {
	contentType, edits := multipartImageBody(t, imageForwardModel)
	for _, tc := range []struct {
		name, path, contentType string
		body                    []byte
	}{
		{"generations", "/v1/images/generations", "application/json", []byte(`{"model":"canvas-custom-v2","prompt":"draw a lighthouse","n":2,"size":"1024x1024","quality":"high","vendor_option":{"keep":true}}`)},
		{"edits", "/v1/images/edits", contentType, edits},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			svc, _ := newImageForwardService(t, `["canvas-custom-v2"]`, true, false, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				assertImageForwardAuth(t, r)
				if r.URL.Path != tc.path || r.Header.Get("Content-Type") != tc.contentType {
					t.Errorf("native request path=%s content-type=%s", r.URL.Path, r.Header.Get("Content-Type"))
				}
				body, err := io.ReadAll(r.Body)
				if err != nil || !bytes.Equal(body, tc.body) {
					t.Errorf("native body altered: err=%v got=%q want=%q", err, body, tc.body)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, imageForwardResponse)
			})
			recorder := imageForwardRequest(svc, tc.path, protocol.KindOpenAIChat, tc.contentType, tc.body)
			if recorder.Code != http.StatusOK || recorder.Body.String() != imageForwardResponse || calls.Load() != 1 {
				t.Fatalf("status=%d calls=%d body=%s", recorder.Code, calls.Load(), recorder.Body.String())
			}
		})
	}
}

// A 200 status from the image provider is not sufficient: malformed or error
// envelopes must fail before either JSON or synthesized SSE reaches the client.
func TestImageGenerationInvalidResponseFailsForward(t *testing.T) {
	for _, upstream := range []struct {
		name, body string
	}{
		{"empty-data", `{"created":1700000000,"data":[]}`},
		{"invalid-json", `{"created":1700000000,"data":`},
		{"error-envelope", `{"error":{"type":"upstream_error","message":"image generation failed"}}`},
	} {
		for _, stream := range []bool{false, true} {
			for _, validation := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/stream=%v/validation=%v", upstream.name, stream, validation), func(t *testing.T) {
					var calls atomic.Int32
					svc, _ := newImageForwardService(t, `["canvas-custom-v2"]`, validation, false, func(w http.ResponseWriter, r *http.Request) {
						calls.Add(1)
						assertImageForwardBody(t, r, "draw a lighthouse")
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						_, _ = io.WriteString(w, upstream.body)
					})
					request := []byte(fmt.Sprintf(`{"model":"public-canvas","messages":[{"role":"user","content":"draw a lighthouse"}],"max_tokens":64,"stream":%v}`, stream))
					recorder := imageForwardRequest(svc, "/v1/chat/completions", protocol.KindOpenAIChat, "application/json", request)
					body := recorder.Body.String()
					if recorder.Code != http.StatusBadGateway || calls.Load() != 1 {
						t.Fatalf("status=%d want 502 calls=%d want 1 body=%s", recorder.Code, calls.Load(), body)
					}
					var response map[string]json.RawMessage
					if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil || len(response["error"]) == 0 {
						t.Fatalf("expected JSON error envelope: err=%v body=%s", err, body)
					}
					if strings.Contains(recorder.Header().Get("Content-Type"), "text/event-stream") || strings.Contains(body, "chat.completion") || strings.Contains(body, "data: [DONE]") {
						t.Fatalf("invalid image response committed as success: headers=%v body=%s", recorder.Header(), body)
					}
				})
			}
		}
	}
}

func TestImageGenerationAdminModelProbe(t *testing.T) {
	var calls atomic.Int32
	svc, group := newImageForwardService(t, `["canvas-custom-v2"]`, false, false, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		assertImageForwardBody(t, r, "ping")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, imageForwardResponse)
	})
	results, err := svc.admin().TestGroupModel(context.Background(), group.ID, TestModelInput{Model: "public-canvas"})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("probe results=%+v", results)
	}
	result := results[0]
	if !result.OK || result.StatusCode != http.StatusOK || result.RequestedModel != "public-canvas" || result.UpstreamModel != imageForwardModel || result.UpstreamPath != "/v1/images/generations" || calls.Load() != 1 {
		t.Fatalf("probe result=%+v calls=%d", result, calls.Load())
	}
}
