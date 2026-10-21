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

// Upstream panels disagree on how a protocol failure is shaped. Every shape
// below is a route-local failure even when the transport status is 200, and
// none of the successful payloads may be reclassified as an error.
func TestRootLevelUpstreamErrorShapes(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want bool
	}{
		{"root error object", `{"error":{"type":"upstream_error","message":"temporarily unavailable"}}`, true},
		{"root code numeric", `{"code":500,"msg":"no available channel"}`, true},
		{"root code numeric string", `{"code":"500","message":"no available channel"}`, true},
		{"root code named", `{"code":"model_not_found","message":"model not found"}`, true},
		{"root errors array", `{"errors":[{"message":"upstream failed"}]}`, true},
		{"root code zero is success", `{"code":0,"msg":"ok","data":{"id":"x"}}`, false},
		{"root code 200 is success", `{"code":200,"message":"ok","choices":[{"delta":{}}]}`, false},
		{"named success", `{"code":"success","message":"ok","data":{}}`, false},
		{"assistant prose mentioning code", `{"choices":[{"message":{"content":"error 500 means nothing here"}}]}`, false},
		{"chat delta", `{"choices":[{"delta":{"content":"hi"}}]}`, false},
		{"null error with zero code", `{"error":null,"code":0}`, false},
		{"responses completed", `{"type":"response.completed","response":{"status":"completed"}}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := hasExplicitUpstreamError([]byte(tc.body)); got != tc.want {
				t.Fatalf("explicit error=%v want=%v body=%s", got, tc.want, tc.body)
			}
			if got := (*Service)(nil).isFailoverResponse(http.StatusOK, false, []byte(tc.body)); got != tc.want {
				t.Fatalf("HTTP 200 failover=%v want=%v body=%s", got, tc.want, tc.body)
			}
			if got := upstreamStreamFailure("", tc.body) != nil; got != tc.want {
				t.Fatalf("stream failure=%v want=%v body=%s", got, tc.want, tc.body)
			}
		})
	}
	for _, tc := range []struct {
		name string
		data string
		want bool
	}{
		{"plain overloaded", "上游负载已饱和", true},
		{"plain timeout", "upstream timeout", true},
		{"plain content", "recovered text", false},
		{"json envelope is not plain text", `{"error":{"message":"x"}}`, false},
		{"done marker", "[DONE]", false},
	} {
		t.Run("plain/"+tc.name, func(t *testing.T) {
			if got := plainTextStreamFailure(tc.data) != nil; got != tc.want {
				t.Fatalf("plain text failure=%v want=%v data=%q", got, tc.want, tc.data)
			}
		})
	}
}

// A stream request answered with HTTP 200 plus a protocol error document must
// be consumed inside the gateway: the client only ever sees the healthy source.
func TestStreamHTTP200ErrorShapesFailOverBeforeCommit(t *testing.T) {
	shapes := []struct {
		name        string
		contentType string
		body        string
		marker      string
	}{
		{"non-sse root error", "application/json", `{"error":{"type":"upstream_error","message":"upstream failed"}}`, "upstream failed"},
		{"non-sse code message", "application/json", `{"code":500,"msg":"no available channel"}`, "no available channel"},
		{"non-sse errors array", "application/json", `{"errors":[{"message":"upstream failed"}]}`, "upstream failed"},
		{"sse plain text", "text/event-stream", "data: 上游负载已饱和\n\n", "上游负载已饱和"},
		{"sse root code message", "text/event-stream", "data: {\"code\":500,\"msg\":\"no available channel\"}\n\n", "no available channel"},
	}
	for _, coordinated := range []bool{false, true} {
		for _, shape := range shapes {
			t.Run(fmt.Sprintf("coordinated=%v/%s", coordinated, shape.name), func(t *testing.T) {
				var calls [2]atomic.Int32
				upstreams := [2]*httptest.Server{}
				for index := 0; index < 2; index++ {
					index := index
					upstreams[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls[index].Add(1)
						if index == 0 {
							w.Header().Set("Content-Type", shape.contentType)
							w.WriteHeader(http.StatusOK)
							_, _ = io.WriteString(w, shape.body)
							return
						}
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"recovered\"}}]}\n\n")
						w.(http.Flusher).Flush()
						_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n")
					}))
					t.Cleanup(upstreams[index].Close)
				}
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("upstream-error-failover")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("upstream-key")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				providers := storage.NewGatewayProviders(db)
				group := &storage.GatewayGroup{
					Name: "upstream-error-failover", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					RetryEnabled: true, RetryCount: 1, FailoverEnabled: true, FailoverMax: 2,
					RequestFirstTokenTimeoutSec: 5, HedgeEnabled: coordinated, HedgeDelaySeconds: 30, HedgeMaxParallel: 2, HedgeMaxAttempts: 3,
				}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("upstream-error-client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
				if err := keys.Create(key); err != nil {
					t.Fatal(err)
				}
				inputs := make([]storage.GatewayRoute, 0, 2)
				for index, upstream := range upstreams {
					provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
					if err := providers.Create(provider); err != nil {
						t.Fatal(err)
					}
					rate := float64(index+1) / 10
					inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
				}
				if err := routes.SaveForGroup(group.ID, inputs); err != nil {
					t.Fatal(err)
				}
				svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
				svc.SetProviders(providers)
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
				c.Request.Header.Set("Authorization", "Bearer upstream-error-client-key")
				svc.runtime().HandleForward(c, "/v1/chat/completions", protocolOpenAI)
				body := recorder.Body.String()
				if got := [2]int32{calls[0].Load(), calls[1].Load()}; got != [2]int32{1, 1} {
					t.Fatalf("source calls=%v want [1 1] body=%s", got, body)
				}
				if !strings.Contains(body, "recovered") || strings.Contains(body, shape.marker) || strings.Contains(body, "upstream_error") {
					t.Fatalf("failed source leaked downstream: %s", body)
				}
				var logs []storage.GatewayUsageLog
				if err := db.Order("attempt").Find(&logs).Error; err != nil {
					t.Fatal(err)
				}
				if len(logs) != 2 {
					t.Fatalf("attempt logs=%d want 2: %+v", len(logs), logs)
				}
				if logs[0].Success || logs[0].Winner {
					t.Fatalf("failed source recorded as success: %+v", logs[0])
				}
				if !logs[1].Success || !logs[1].Winner {
					t.Fatalf("recovered source not recorded as winner: %+v", logs[1])
				}
			})
		}
	}
}

// The same envelope on a non-stream request must fail over instead of being
// forwarded as a 200 body.
func TestNonStreamHTTP200CodeMessageFailsOver(t *testing.T) {
	var calls [2]atomic.Int32
	upstreams := [2]*httptest.Server{}
	for index := 0; index < 2; index++ {
		index := index
		upstreams[index] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[index].Add(1)
			w.Header().Set("Content-Type", "application/json")
			if index == 0 {
				w.WriteHeader(http.StatusOK)
				_, _ = io.WriteString(w, `{"code":500,"msg":"no available channel"}`)
				return
			}
			_, _ = io.WriteString(w, `{"id":"chatcmpl-1","object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"recovered"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":1}}`)
		}))
		t.Cleanup(upstreams[index].Close)
	}
	db := openGatewayTestDB(t)
	cipher, err := crypto.NewCipher("upstream-error-nonstream")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := cipher.Encrypt("upstream-key")
	if err != nil {
		t.Fatal(err)
	}
	groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
	providers := storage.NewGatewayProviders(db)
	group := &storage.GatewayGroup{
		Name: "upstream-error-nonstream", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
		RetryEnabled: true, RetryCount: 1, FailoverEnabled: true, FailoverMax: 2, RequestFirstTokenTimeoutSec: 5,
	}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("upstream-error-nonstream-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
	if err := keys.Create(key); err != nil {
		t.Fatal(err)
	}
	inputs := make([]storage.GatewayRoute, 0, 2)
	for index, upstream := range upstreams {
		provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
		if err := providers.Create(provider); err != nil {
			t.Fatal(err)
		}
		rate := float64(index+1) / 10
		inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate})
	}
	if err := routes.SaveForGroup(group.ID, inputs); err != nil {
		t.Fatal(err)
	}
	svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
	svc.SetProviders(providers)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	c.Request.Header.Set("Authorization", "Bearer upstream-error-nonstream-key")
	svc.runtime().HandleForward(c, "/v1/chat/completions", protocolOpenAI)
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.Contains(body, "recovered") || strings.Contains(body, "no available channel") {
		t.Fatalf("status=%d body=%s", recorder.Code, body)
	}
	if got := [2]int32{calls[0].Load(), calls[1].Load()}; got != [2]int32{1, 1} {
		t.Fatalf("source calls=%v want [1 1]", got)
	}
}

// Fake first token already sent (HTTP 200 + keepalive/lifecycle) must not stop
// a rejected source from being replaced by a healthy one, and the client must
// only ever see the healthy stream.
func TestStreamPreconnectFailoverStaysInvisible(t *testing.T) {
	for _, mode := range []struct {
		name              string
		sub2api, multihop bool
	}{
		{"comment", false, false},
		{"sub2api", true, false},
		{"multihop", true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			db := openGatewayTestDB(t)
			cipher, err := crypto.NewCipher("preconnect-failover")
			if err != nil {
				t.Fatal(err)
			}
			secret, err := cipher.Encrypt("upstream-key")
			if err != nil {
				t.Fatal(err)
			}
			groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
			providers := storage.NewGatewayProviders(db)
			group := &storage.GatewayGroup{
				Name: "preconnect-failover", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
				RetryEnabled: true, RetryCount: 1, FailoverEnabled: true, FailoverMax: 2, RequestFirstTokenTimeoutSec: 10,
				GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
					StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: mode.sub2api, StreamPreconnectMultihopEnabled: mode.multihop,
					StreamPreconnectMinDelayMS: 1, StreamPreconnectMaxDelayMS: 1,
				},
			}
			if err := groups.Create(group); err != nil {
				t.Fatal(err)
			}
			key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("preconnect-failover-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
			if err := keys.Create(key); err != nil {
				t.Fatal(err)
			}
			receivedPreconnect := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(receivedPreconnect) }) }
			t.Cleanup(release)
			var calls [2]atomic.Int32
			inputs := make([]storage.GatewayRoute, 0, 2)
			for index := 0; index < 2; index++ {
				index := index
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[index].Add(1)
					if index == 0 {
						select {
						case <-receivedPreconnect:
						case <-r.Context().Done():
							return
						}
						w.Header().Set("Content-Type", "application/json")
						w.WriteHeader(http.StatusOK)
						_, _ = io.WriteString(w, `{"code":500,"msg":"no available channel"}`)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, preconnectForwardResponse("recovered answer"))
				}))
				t.Cleanup(upstream.Close)
				provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
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
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-4o","stream":true,"store":false,"input":"hi"}`))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer preconnect-failover-key")
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
			if got := [2]int32{calls[0].Load(), calls[1].Load()}; got != [2]int32{1, 1} {
				t.Fatalf("source calls=%v want [1 1] body=%s", got, body)
			}
			if !strings.Contains(body, "recovered answer") || strings.Contains(body, "no available channel") || strings.Contains(body, "response.failed") || strings.Contains(body, "upstream_error") {
				t.Fatalf("rejected source leaked into the fake-first-token stream: %s", body)
			}
			if got := strings.Count(body, "event: response.completed\n"); got != 1 {
				t.Fatalf("completed events=%d want 1: %s", got, body)
			}
			var logs []storage.GatewayUsageLog
			if err := db.Order("attempt").Find(&logs).Error; err != nil {
				t.Fatal(err)
			}
			if len(logs) != 2 {
				t.Fatalf("attempt logs=%d want 2: %+v", len(logs), logs)
			}
			if logs[0].Success || logs[0].Winner {
				t.Fatalf("rejected attempt billed as success: %+v", logs[0])
			}
			winner := logs[1]
			if !winner.Success || !winner.Winner || winner.RequestFirstTokenMS == nil {
				t.Fatalf("recovered attempt missing winner state: %+v", winner)
			}
		})
	}
}
