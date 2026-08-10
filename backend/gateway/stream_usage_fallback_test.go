package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestPrepareAnthropicCountTokensBodyRemovesStreamControls(t *testing.T) {
	body, err := prepareAnthropicCountTokensBody([]byte(`{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("prepare count body: %v", err)
	}
	text := string(body)
	if strings.Contains(text, `"stream"`) || strings.Contains(text, `"stream_options"`) {
		t.Fatalf("count body retained stream controls: %s", text)
	}
	if !strings.Contains(text, `"messages"`) {
		t.Fatalf("count body lost messages: %s", text)
	}
}

func TestRecoverMissingStreamInputTokensUsesCountEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var gotPath string
	var gotBody string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"input_tokens":42}`)
	}))
	defer upstream.Close()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	got := (&Runtime{Service: &Service{}}).recoverMissingStreamInputTokens(
		c.Request.Context(), c,
		&upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
		[]byte(`{"model":"m","stream":true,"stream_options":{"include_usage":true},"messages":[{"role":"user","content":"hi"}]}`),
		protocol.KindAnthropic,
		UsageTokens{OutputTokens: 2},
	)
	if got.InputTokens != 42 || got.OutputTokens != 2 {
		t.Fatalf("tokens=%+v, want input=42 output=2", got)
	}
	if gotPath != "/v1/messages/count_tokens" {
		t.Fatalf("count path=%q", gotPath)
	}
	if strings.Contains(gotBody, `"stream"`) || strings.Contains(gotBody, `"stream_options"`) {
		t.Fatalf("count request retained stream controls: %s", gotBody)
	}
}

func TestRecoverMissingStreamInputTokensSubtractsKnownCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"input_tokens":50}`)
	}))
	defer upstream.Close()

	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	got := (&Runtime{Service: &Service{}}).recoverMissingStreamInputTokens(
		c.Request.Context(), c,
		&upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
		[]byte(`{"model":"m","stream":true,"messages":[]}`),
		protocol.KindAnthropic,
		UsageTokens{CacheReadTokens: 10, CacheCreationTokens: 5},
	)
	if got.InputTokens != 35 {
		t.Fatalf("tokens=%+v, want fresh input=35 after subtracting cache buckets", got)
	}
}

func TestRecoverMissingStreamInputTokensSkipsNonAnthropic(t *testing.T) {
	called := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer upstream.Close()

	got := (&Runtime{Service: &Service{}}).recoverMissingStreamInputTokens(
		nil, nil, &upstreamTarget{BaseURL: upstream.URL}, []byte(`{}`), protocol.KindOpenAIResponses,
		UsageTokens{OutputTokens: 1},
	)
	if got.InputTokens != 0 || called {
		t.Fatalf("non-Anthropic fallback made a request or changed tokens: %+v called=%v", got, called)
	}
}

func TestForwardStreamRecoversUsageBeforeResponsesVirtualCache(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var countCalls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/messages/count_tokens":
			countCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"input_tokens":120}`)
		case "/v1/messages":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			for _, event := range []string{
				"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"m\",\"usage\":{\"input_tokens\":0}}}\n\n",
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\n",
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
				"data: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"input_tokens\":0,\"output_tokens\":4,\"cache_read_input_tokens\":20}}\n\n",
				"data: {\"type\":\"message_stop\"}\n\n",
			} {
				_, _ = io.WriteString(w, event)
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		default:
			http.NotFound(w, r)
		}
	}))
	defer upstream.Close()

	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	target := &upstreamTarget{
		BaseURL: upstream.URL,
		APIKey:  "k",
		Provider: &storage.GatewayProvider{
			ID:               1,
			ConcurrencyLimit: 1,
		},
	}

	resultCh := make(chan streamAttemptResult, 1)
	go func() {
		resultCh <- (&Service{}).forwardStreamWithVirtualCache(
			c.Request.Context(), c, target, "/v1/messages", http.MethodPost,
			http.Header{"Content-Type": []string{"application/json"}},
			[]byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`),
			protocol.KindOpenAIResponses, protocol.KindAnthropic, "m", true, 0, 90,
		)
	}()

	var result streamAttemptResult
	select {
	case result = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stream input recovery deadlocked behind provider concurrency limit")
	}
	if result.Err != nil || result.StreamErr != nil {
		t.Fatalf("stream result error: %+v", result)
	}
	if !result.InputUsageRecoveryAttempted || !result.VirtualCacheApplied {
		t.Fatalf("recovery/cache flags: attempted=%v applied=%v", result.InputUsageRecoveryAttempted, result.VirtualCacheApplied)
	}
	if result.Tokens.InputTokens != 100 || result.Tokens.CacheReadTokens != 20 || result.Tokens.OutputTokens != 4 {
		t.Fatalf("raw stream tokens=%+v, want input=100 cache_read=20 output=4", result.Tokens)
	}
	if countCalls.Load() != 1 {
		t.Fatalf("count_tokens calls=%d, want 1", countCalls.Load())
	}
	downstream := NormalizeUsageBuckets(ParseOpenAISSEUsage(recorder.Body.Bytes()), protocol.KindOpenAIResponses)
	if downstream.InputTokens != 10 || downstream.CacheReadTokens != 110 || downstream.OutputTokens != 4 {
		t.Fatalf("downstream usage=%+v, want fresh=10 cache_read=110 output=4; body=%s", downstream, recorder.Body.String())
	}
}
