package gateway

import (
	"bufio"
	"context"
	"errors"
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

const preconnectAnthropicTestPing = "event: ping\ndata: {\"type\":\"ping\"}\n\n"

func TestAnthropicPreconnectCompatibilityEligibility(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		enabled, compat, multihop bool
		stream, text, wantPing    bool
		kind                      protocolKind
		path                      string
	}{
		{"native", true, true, false, true, true, true, protocol.KindAnthropic, "/v1/messages"},
		{"native-multihop", true, true, true, true, true, true, protocol.KindAnthropic, "/v1/messages"},
		{"transport-only", true, false, true, true, true, false, protocol.KindAnthropic, "/v1/messages"},
		{"disabled", false, true, true, true, true, false, protocol.KindAnthropic, "/v1/messages"},
		{"nonstream", true, true, true, false, true, false, protocol.KindAnthropic, "/v1/messages"},
		{"media", true, true, true, true, false, false, protocol.KindAnthropic, "/v1/messages"},
		{"wrong-kind", true, true, true, true, true, false, protocol.KindOpenAIChat, "/v1/messages"},
		{"wrong-path", true, true, true, true, true, false, protocol.KindAnthropic, "/v1/responses"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
				StreamPreconnectEnabled: tc.enabled, StreamPreconnectSub2apiEnabled: tc.compat,
				StreamPreconnectMultihopEnabled: tc.multihop, StreamPreconnectMinDelayMS: 60000, StreamPreconnectMaxDelayMS: 60000,
			}}
			finish := beginStreamPreconnect(c, group, tc.kind, tc.path, tc.stream, tc.text)
			defer finish()
			writer := streamPreconnectFor(c)
			if got := writer != nil && writer.anthropicPing; got != tc.wantPing {
				t.Fatalf("Anthropic compatibility=%v want=%v", got, tc.wantPing)
			}
			if writer == nil {
				return
			}
			if !writer.keepalive() || !writer.keepalive() {
				t.Fatal("preconnect stopped before real output")
			}
			want := ": upstream-ops keepalive\n\n"
			if tc.wantPing {
				want = preconnectAnthropicTestPing
			}
			if recorder.Body.String() != want+want || writer.DownstreamCommitted() {
				t.Fatalf("unexpected preconnect/body commitment: %q", recorder.Body.String())
			}
		})
	}
}

func TestAnthropicPreconnectKeepsRealTimingAndProtocolBytes(t *testing.T) {
	c, recorder, writer := preconnectTestWriter(t, protocol.KindAnthropic)
	writer.anthropicPing = true
	state := &forwardRequestTiming{started: time.Now().Add(-100 * time.Millisecond)}
	c.Writer = &requestTimingWriter{ResponseWriter: writer, state: state, kind: protocol.KindAnthropic}
	if !writer.keepalive() || streamWriterActuallyCommitted(c) {
		t.Fatal("ping failed or committed an upstream winner")
	}
	if first, _ := state.snapshot(); first != nil {
		t.Fatal("ping manufactured gateway TTFT")
	}
	if streamEventHasVisibleOutput(protocol.KindAnthropic, "ping", []byte(`{"type":"ping"}`)) {
		t.Fatal("ping classified as real output")
	}
	// Keeping the real message_start and usage verbatim avoids inventing a
	// model identity or losing input/cache usage reported only at stream start.
	real := strings.ReplaceAll(zeroUsageTestBody(protocol.KindAnthropic, true, "real answer", 2, true, true), `"input_tokens":0`, `"input_tokens":7`)
	if n, err := c.Writer.Write([]byte(real)); err != nil || n != len(real) {
		t.Fatalf("real stream write=%d err=%v", n, err)
	}
	c.Writer.Flush()
	if got := recorder.Body.String(); got != preconnectAnthropicTestPing+real {
		t.Fatalf("Anthropic lifecycle, indexes, model or usage changed: %s", got)
	}
	if !streamWriterActuallyCommitted(c) || writer.keepalive() {
		t.Fatal("real output did not take over")
	}
	if first, _ := state.snapshot(); first == nil || *first < 100 {
		t.Fatalf("real TTFT=%v", first)
	}
}

func TestAnthropicPreconnectKeepsFirstTokenBudgetAndTerminalFailure(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	finishTiming := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocol.KindAnthropic, true, 50*time.Millisecond)
	defer finishTiming()
	group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
		StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true,
		StreamPreconnectMinDelayMS: 60000, StreamPreconnectMaxDelayMS: 60000,
	}}
	finish := beginStreamPreconnect(c, group, protocol.KindAnthropic, "/v1/messages", true, true)
	defer finish()
	writer := streamPreconnectFor(c)
	if writer == nil || !writer.keepalive() {
		t.Fatal("missing Anthropic preconnect")
	}
	select {
	case <-c.Request.Context().Done():
		if !errors.Is(context.Cause(c.Request.Context()), errRequestFirstTokenBudget) {
			t.Fatalf("budget cause=%v", context.Cause(c.Request.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("ping stopped real first-token budget")
	}
	if first, _ := requestTiming(c.Request.Context()).snapshot(); first != nil || streamWriterActuallyCommitted(c) {
		t.Fatal("ping recorded real TTFT or winner commitment")
	}
	(&Service{}).writeGatewayError(c, protocol.KindAnthropic, http.StatusGatewayTimeout, "request_timeout", "budget exhausted")
	body := recorder.Body.String()
	if recorder.Code != http.StatusOK || !strings.HasPrefix(body, preconnectAnthropicTestPing) || !strings.Contains(body, "event: error\n") || !strings.Contains(body, "request_timeout") || strings.Contains(body, "message_stop") || strings.Contains(body, "message_start") {
		t.Fatalf("preconnected failure became successful or invalid lifecycle: %s", body)
	}
}

func TestAnthropicPreconnectCancelledClientStopsPing(t *testing.T) {
	_, recorder, writer := preconnectTestWriter(t, protocol.KindAnthropic)
	writer.anthropicPing = true
	if !writer.keepalive() {
		t.Fatal("initial ping failed")
	}
	before := recorder.Body.String()
	writer.cancel()
	if writer.keepalive() {
		t.Fatal("cancelled client received another ping")
	}
	writer.finish()
	if recorder.Body.String() != before || writer.DownstreamCommitted() {
		t.Fatalf("cancellation invented content or terminal success: %s", recorder.Body.String())
	}
}

// The relay matches the audited sub2api Messages paths: any nonempty data
// except [DONE] starts FirstTokenMs; each complete event is flushed onward.
// Source references: gateway_anthropic_passthrough.go,
// gateway_upstream_response.go and openai_gateway_messages_anthropic_native.go
// in ranxi2001/sub2api ab5619112. This is a behavioral fixture, not a deployment
// of sub2api itself; arbitrary protocol converters may intentionally drop ping.
func newAnthropicPreconnectRelay(t *testing.T, upstream string, observed chan<- string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, upstream+"/v1/messages", nil)
		if err != nil {
			return
		}
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return
		}
		defer response.Body.Close()
		w.Header().Set("Content-Type", "text/event-stream")
		reader := bufio.NewReader(response.Body)
		first := false
		for {
			frame, data, err := preconnectRelayReadFrame(reader)
			if err != nil {
				return
			}
			if _, err := w.Write(frame); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			if len(data) != 0 && string(data) != "[DONE]" && !first {
				first = true
				observed <- string(data)
			}
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestAnthropicPreconnectReachesTwoSub2apiStyleRelaysBeforeRealOutput(t *testing.T) {
	releaseReal := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseReal) }) }
	gin.SetMode(gin.TestMode)
	router := gin.New()
	real := zeroUsageTestBody(protocol.KindAnthropic, true, "real answer", 2, true, true)
	router.POST("/v1/messages", func(c *gin.Context) {
		group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
			StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true,
		}}
		finish := beginStreamPreconnect(c, group, protocol.KindAnthropic, "/v1/messages", true, true)
		defer finish()
		select {
		case <-releaseReal:
		case <-c.Request.Context().Done():
			return
		}
		_, _ = c.Writer.Write([]byte(real))
		c.Writer.Flush()
	})
	source := httptest.NewServer(router)
	t.Cleanup(source.Close)
	firstReceipt, secondReceipt := make(chan string, 1), make(chan string, 1)
	first := newAnthropicPreconnectRelay(t, source.URL, firstReceipt)
	second := newAnthropicPreconnectRelay(t, first.URL, secondReceipt)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Release pending handlers before httptest cleanup, even after an assertion.
	defer release()
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, second.URL+"/v1/messages", nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	frame, data, err := preconnectRelayReadFrame(reader)
	if err != nil || string(frame) != preconnectAnthropicTestPing || string(data) != `{"type":"ping"}` {
		t.Fatalf("final client did not receive early ping: frame=%q data=%q err=%v", frame, data, err)
	}
	for layer, receipt := range map[string]<-chan string{"first": firstReceipt, "second": secondReceipt} {
		select {
		case got := <-receipt:
			if got != `{"type":"ping"}` {
				t.Fatalf("%s layer timed a different event: %s", layer, got)
			}
		case <-ctx.Done():
			t.Fatalf("%s layer did not time early ping", layer)
		}
	}
	// Only now allow real message_start/content/usage to be produced.
	release()
	rest, err := io.ReadAll(reader)
	if err != nil || string(rest) != real {
		t.Fatalf("real stream changed across two relays: %q err=%v", rest, err)
	}
}

func TestAnthropicPreconnectRetainsFailoverValidationAndUsage(t *testing.T) {
	for _, upstreamKind := range []protocol.Kind{protocol.KindAnthropic, protocol.KindOpenAIChat, protocol.KindOpenAIResponses} {
		for _, rejectFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reject=%v", upstreamKind, rejectFirst), func(t *testing.T) {
				db := openGatewayTestDB(t)
				cipher, err := crypto.NewCipher("anthropic-preconnect-regression")
				if err != nil {
					t.Fatal(err)
				}
				secret, err := cipher.Encrypt("source-key")
				if err != nil {
					t.Fatal(err)
				}
				groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
				group := &storage.GatewayGroup{
					Name: "anthropic-preconnect", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
					RetryEnabled: true, FailoverEnabled: true, FailoverMax: 1, RequestFirstTokenTimeoutSec: 10,
					ResponseValidationEnabled: true, ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 8192,
					GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true, StreamPreconnectMultihopEnabled: true},
				}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				if err := db.Model(group).Update("response_validation_retry_count", 0).Error; err != nil {
					t.Fatal(err)
				}
				key := &storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("anthropic-client"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive, Quota: 100}
				if err := keys.Create(key); err != nil {
					t.Fatal(err)
				}
				rules := storage.NewGatewayResponseRules(db)
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "blocked", Enabled: true, Target: storage.GatewayResponseRuleTargetAssistantText, Pattern: "private-rejected-answer"}); err != nil {
					t.Fatal(err)
				}
				pingReceived := make(chan struct{})
				var once sync.Once
				release := func() { once.Do(func() { close(pingReceived) }) }
				defer release()
				var calls [2]atomic.Int32
				providers := storage.NewGatewayProviders(db)
				var inputs []storage.GatewayRoute
				for index := 0; index < 2; index++ {
					index := index
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						calls[index].Add(1)
						select {
						case <-pingReceived:
						case <-r.Context().Done():
							return
						}
						if index == 0 && !rejectFirst {
							w.WriteHeader(http.StatusServiceUnavailable)
							_, _ = io.WriteString(w, `{"error":{"message":"private-upstream-error"}}`)
							return
						}
						text := "real fallback answer"
						if index == 0 {
							text = "private-rejected-answer"
						}
						body := zeroUsageTestBody(upstreamKind, true, text, 2, true, true)
						body = strings.ReplaceAll(body, `"input_tokens":0`, `"input_tokens":7`)
						body = strings.ReplaceAll(body, `"prompt_tokens":0`, `"prompt_tokens":7`)
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = io.WriteString(w, body)
					}))
					t.Cleanup(upstream.Close)
					provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
					if err := providers.Create(provider); err != nil {
						t.Fatal(err)
					}
					inputs = append(inputs, storage.GatewayRoute{GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider, GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: string(upstreamKind), RateConvertMode: "custom", RateConvertValue: float64(index+1) / 10})
				}
				if err := routes.SaveForGroup(group.ID, inputs); err != nil {
					t.Fatal(err)
				}
				svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
				svc.SetProviders(providers)
				svc.SetResponseRules(rules)
				handlerDone := make(chan struct{})
				router := gin.New()
				router.POST("/v1/messages", func(c *gin.Context) {
					defer close(handlerDone)
					svc.runtime().HandleForward(c, "/v1/messages", protocol.KindAnthropic)
				})
				server := httptest.NewServer(router)
				t.Cleanup(func() { release(); server.Close() })
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				request, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/messages", strings.NewReader(`{"model":"m","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`))
				request.Header.Set("Authorization", "Bearer anthropic-client")
				request.Header.Set("Content-Type", "application/json")
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				reader := bufio.NewReader(response.Body)
				prefix, _, err := preconnectRelayReadFrame(reader)
				if err != nil || string(prefix) != preconnectAnthropicTestPing {
					t.Fatalf("missing ping before upstream response: %q err=%v", prefix, err)
				}
				release()
				body, err := io.ReadAll(reader)
				if err != nil || !strings.Contains(string(body), "real fallback answer") || !strings.Contains(string(body), `"message_stop"`) || strings.Contains(string(body), "private-") {
					t.Fatalf("failover leaked an error or lost output: %s err=%v", body, err)
				}
				select {
				case <-handlerDone:
				case <-ctx.Done():
					t.Fatal("handler did not finish")
				}
				if calls[0].Load() != 1 || calls[1].Load() != 1 {
					t.Fatalf("ping altered failover attempts: %d/%d", calls[0].Load(), calls[1].Load())
				}
				var logs []storage.GatewayUsageLog
				if err := db.Order("attempt").Find(&logs).Error; err != nil || len(logs) != 2 {
					t.Fatalf("usage rows=%+v err=%v", logs, err)
				}
				if logs[0].Success || logs[0].Winner || logs[0].RequestFirstTokenMS != nil || logs[0].BilledCost != 0 {
					t.Fatalf("early ping selected/settled first attempt: %+v", logs[0])
				}
				wantFirstStatus := storage.GatewayAttemptStatusError
				if rejectFirst {
					wantFirstStatus = storage.GatewayAttemptStatusRejected
				}
				if logs[0].AttemptStatus != wantFirstStatus {
					t.Fatalf("first attempt status=%s want=%s", logs[0].AttemptStatus, wantFirstStatus)
				}
				if !logs[1].Success || !logs[1].Winner || logs[1].InputTokens != 7 || logs[1].OutputTokens != 2 || logs[1].RequestFirstTokenMS == nil {
					t.Fatalf("real winner lost usage/timing: %+v", logs[1])
				}
			})
		}
	}
}
