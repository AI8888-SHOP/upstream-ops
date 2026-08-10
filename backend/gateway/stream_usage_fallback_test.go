package gateway

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
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
