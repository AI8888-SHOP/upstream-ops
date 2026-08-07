package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// flushRecorder 实现 http.Flusher，便于观察流式写出。
type flushRecorder struct {
	*httptest.ResponseRecorder
	flushes  int
	flushCh  chan struct{}
}

func (f *flushRecorder) Flush() {
	f.flushes++
	if f.flushCh != nil {
		select {
		case f.flushCh <- struct{}{}:
		default:
		}
	}
}

func TestCommitSSEHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	if err := (&Runtime{Service: &Service{}}).commitSSEHeaders(c, http.Header{"X-Request-Id": []string{"u1"}}); err != nil {
		t.Fatal(err)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type=%q", ct)
	}
	if rec.Header().Get("X-Accel-Buffering") != "no" {
		t.Fatal("missing X-Accel-Buffering")
	}
	// 上游 X-Request-Id 原样保留；网关只写 X-Upstream-Ops-Request-Id
	if got := rec.Header().Get("X-Request-Id"); got != "u1" {
		t.Fatalf("X-Request-Id=%q, want upstream u1", got)
	}
	opsID := rec.Header().Get("X-Upstream-Ops-Request-Id")
	if !(*Service)(nil).isGatewayGeneratedRequestID(opsID) {
		t.Fatalf("want gateway ops request id, got %q", opsID)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestMayContainUsageFieldsCoversCacheAndDetailVariants(t *testing.T) {
	for _, payload := range []string{
		`{"prompt_cache_hit_tokens":12}`,
		`{"cache_hit_tokens":12}`,
		`{"cache_creation_5m_input_tokens":12}`,
		`{"output_tokens_details":{"reasoning_tokens":12}}`,
		`{"input_tokens_details":{"cached_tokens":12}}`,
		`{"us\u0061ge":{"prompt_\u0074okens":12}}`,
	} {
		if !mayContainUsageFields(payload, protocol.KindOpenAIChat) {
			t.Fatalf("usage prefilter rejected token payload %q", payload)
		}
	}
	if mayContainUsageFields(`{"choices":[{"delta":{"content":"tokens"}}]}`, protocol.KindOpenAIChat) {
		t.Fatal("usage prefilter matched ordinary content")
	}
}

func TestAppendSSEDataCaptureKeepsBoundedPrefixOfLargeLine(t *testing.T) {
	const limit = 128
	var capture bytes.Buffer
	appendSSEDataCapture(&capture, strings.Repeat("x", 1024), limit)
	if capture.Len() == 0 || capture.Len() > limit {
		t.Fatalf("capture length = %d, want 1..%d", capture.Len(), limit)
	}
	if !strings.HasPrefix(capture.String(), "data: xxx") {
		t.Fatalf("large data line was discarded: %q", capture.String())
	}
	if !strings.HasSuffix(capture.String(), "\n\n") {
		t.Fatalf("capture is not a complete SSE frame: %q", capture.String())
	}
}

func TestWriteStreamTerminalError_OpenAI(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	_ = (&Runtime{Service: &Service{}}).commitSSEHeaders(c, nil)
	if err := (&Runtime{Service: &Service{}}).writeStreamTerminalError(c, protocol.KindOpenAIChat, "api_error", "boom"); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"api_error"`) && !strings.Contains(body, `"message":"boom"`) {
		t.Fatalf("body=%s", body)
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatalf("missing DONE: %s", body)
	}
}

func TestWriteStreamTerminalError_Anthropic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	_ = (&Runtime{Service: &Service{}}).commitSSEHeaders(c, nil)
	if err := (&Runtime{Service: &Service{}}).writeStreamTerminalError(c, protocol.KindAnthropic, "stream_timeout", "idle"); err != nil {
		t.Fatal(err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") {
		t.Fatalf("body=%s", body)
	}
}

func TestForwardStream_PassthroughChunks(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 上游按块输出 SSE
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"A\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"B\"},\"finish_reason\":\"stop\"}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	body := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	res := svc.forwardStream(
		c.Request.Context(), c, target, "/v1/chat/completions", http.MethodPost,
		http.Header{"Content-Type": []string{"application/json"}},
		body, protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
	)
	if res.Err != nil {
		t.Fatalf("err=%v", res.Err)
	}
	if !res.Committed {
		t.Fatal("expected committed stream")
	}
	if res.StreamErr != nil {
		t.Fatalf("streamErr=%v", res.StreamErr)
	}
	out := rec.Body.String()
	if !strings.Contains(out, `"content":"A"`) || !strings.Contains(out, `"content":"B"`) {
		t.Fatalf("body=%s", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Fatalf("missing DONE: %s", out)
	}
	if rec.flushes == 0 {
		t.Fatal("expected at least one flush")
	}
}

func TestForwardStream_ResponsesLifecycleDoesNotStopFirstTokenTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(250 * time.Millisecond)
	}))
	defer upstream.Close()

	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	res := (&Service{}).forwardStream(
		c.Request.Context(), c, target, "/v1/responses", http.MethodPost, nil,
		[]byte(`{"model":"m","stream":true,"input":"hi"}`),
		protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, 100*time.Millisecond,
	)
	if !errors.Is(res.Err, errFirstTokenTimeout) {
		t.Fatalf("err=%v, want first token timeout", res.Err)
	}
	if res.Committed || rec.Body.Len() != 0 {
		t.Fatalf("lifecycle metadata reached client: committed=%v body=%q", res.Committed, rec.Body.String())
	}
}

func TestForwardStream_ResponsesLifecycleFlushesWithFirstOutput(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	res := (&Service{}).forwardStream(
		c.Request.Context(), c, target, "/v1/responses", http.MethodPost, nil,
		[]byte(`{"model":"m","stream":true,"input":"hi"}`),
		protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, 500*time.Millisecond,
	)
	if res.Err != nil || !res.Committed {
		t.Fatalf("result=%+v", res)
	}
	if res.FirstTokenMS == nil || *res.FirstTokenMS < 40 {
		t.Fatalf("first_token_ms=%v, lifecycle event was counted as output", res.FirstTokenMS)
	}
	body := rec.Body.String()
	if created, output := strings.Index(body, "response.created"), strings.Index(body, "response.output_text.delta"); created < 0 || output < 0 || created > output {
		t.Fatalf("buffered lifecycle/output order is invalid: %s", body)
	}
}

func TestForwardStream_ConvertedResponsesLifecycleDoesNotStopFirstTokenTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer upstream.Close()

	cases := []struct {
		name       string
		clientKind protocol.Kind
		path       string
		body       string
	}{
		{"chat", protocol.KindOpenAIChat, "/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`},
		{"anthropic", protocol.KindAnthropic, "/v1/messages", `{"model":"m","stream":true,"messages":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			res := (&Service{}).forwardStream(
				c.Request.Context(), c, &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
				"/v1/responses", http.MethodPost, nil, []byte(tc.body),
				tc.clientKind, protocol.KindOpenAIResponses, "m", true, 75*time.Millisecond,
			)
			if !errors.Is(res.Err, errFirstTokenTimeout) {
				t.Fatalf("err=%v, want first token timeout", res.Err)
			}
			if res.Committed || rec.Body.Len() != 0 {
				t.Fatalf("lifecycle metadata reached client: committed=%v body=%q", res.Committed, rec.Body.String())
			}
		})
	}
}

func TestForwardStream_ConvertedResponsesLifecycleEOFIsError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
	}))
	defer upstream.Close()

	cases := []struct {
		name       string
		clientKind protocol.Kind
		path       string
		body       string
	}{
		{"responses", protocol.KindOpenAIResponses, "/v1/responses", `{"model":"m","stream":true,"input":"hi"}`},
		{"chat", protocol.KindOpenAIChat, "/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`},
		{"anthropic", protocol.KindAnthropic, "/v1/messages", `{"model":"m","stream":true,"messages":[]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			res := (&Service{}).forwardStream(
				c.Request.Context(), c, &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
				"/v1/responses", http.MethodPost, nil, []byte(tc.body),
				tc.clientKind, protocol.KindOpenAIResponses, "m", true, 0,
			)
			if res.Err == nil || !strings.Contains(res.Err.Error(), "ended before output or terminal event") {
				t.Fatalf("err=%v, want lifecycle-only EOF error", res.Err)
			}
			if res.Committed || rec.Body.Len() != 0 {
				t.Fatalf("lifecycle-only stream reached client: committed=%v body=%q", res.Committed, rec.Body.String())
			}
		})
	}
}

func TestForwardStream_UpstreamErrorNotCommitted(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, `{"error":{"message":"busy","type":"server_error"}}`)
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	res := svc.forwardStream(
		c.Request.Context(), c, target, "/v1/chat/completions", http.MethodPost,
		nil, []byte(`{"model":"m","stream":true}`),
		protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
	)
	if res.Committed {
		t.Fatal("503 must not commit SSE")
	}
	if res.Status != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", res.Status)
	}
	if !strings.Contains(string(res.Body), "busy") {
		t.Fatalf("body=%s", res.Body)
	}
	// 客户端 recorder 应仍未写入 SSE
	if rec.Body.Len() > 0 {
		t.Fatalf("client body should be empty, got %s", rec.Body.String())
	}
}

func TestForwardStream_ClientDisconnectDrainsUsage(t *testing.T) {
	// 对齐 sub2api：客户端中途断开后，上游请求不取消，继续读到 usage 帧。
	// 注意：中途断开时故意不让 [DONE] 先写出到客户端（断开发生在 content 之后、DONE 之前），
	// 这样仍记 ClientDisconnected；若 DONE 已交付再关连接则见 TestForwardStream_DisconnectAfterDoneIsSuccess。
	gin.SetMode(gin.TestMode)
	started := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		close(started)
		// 模拟客户端断开后上游仍在继续生成，最后带 usage（但客户端已断，[DONE] 写不出去）
		time.Sleep(80 * time.Millisecond)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":7}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	clientCtx, cancelClient := context.WithCancel(context.Background())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(clientCtx)

	// 首包写出后取消客户端 context，模拟用户手动中断（此时尚未 DONE）
	go func() {
		<-started
		time.Sleep(20 * time.Millisecond)
		cancelClient()
	}()

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	body := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	res := svc.forwardStream(
		clientCtx, c, target, "/v1/chat/completions", http.MethodPost,
		http.Header{"Content-Type": []string{"application/json"}},
		body, protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
	)
	if !res.Committed {
		t.Fatal("expected committed before disconnect")
	}
	if res.DownstreamComplete {
		t.Fatal("mid-stream disconnect must not mark DownstreamComplete")
	}
	if !res.ClientDisconnected {
		t.Fatal("expected ClientDisconnected")
	}
	if res.StreamErr == nil || !errors.Is(res.StreamErr, errClientDisconnected) {
		t.Fatalf("streamErr=%v, want errClientDisconnected", res.StreamErr)
	}
	// 关键：断开后仍 drain 到 usage
	if res.Tokens.InputTokens != 11 || res.Tokens.OutputTokens != 7 {
		t.Fatalf("tokens=%+v, want input=11 output=7 (drain after disconnect)", res.Tokens)
	}
}

func TestForwardStream_DisconnectAfterDoneIsSuccess(t *testing.T) {
	// 客户端在收到 [DONE] 后立刻关连接（SDK 常见行为）应记普通成功，不标「客户端断开」。
	gin.SetMode(gin.TestMode)
	doneSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2}}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		close(doneSent)
		// 模拟上游尾包稍晚结束
		time.Sleep(40 * time.Millisecond)
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	clientCtx, cancelClient := context.WithCancel(context.Background())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(clientCtx)

	go func() {
		<-doneSent
		// 给网关一点时间把 [DONE] 写出后再取消
		time.Sleep(15 * time.Millisecond)
		cancelClient()
	}()

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	body := []byte(`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`)
	res := svc.forwardStream(
		clientCtx, c, target, "/v1/chat/completions", http.MethodPost,
		http.Header{"Content-Type": []string{"application/json"}},
		body, protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
	)
	if !res.Committed {
		t.Fatal("expected committed")
	}
	if !res.DownstreamComplete {
		t.Fatal("expected DownstreamComplete after [DONE]")
	}
	if res.ClientDisconnected {
		t.Fatal("post-terminal disconnect must not keep ClientDisconnected")
	}
	if res.StreamErr != nil {
		t.Fatalf("streamErr=%v, want nil", res.StreamErr)
	}
	if res.Tokens.InputTokens != 3 || res.Tokens.OutputTokens != 2 {
		t.Fatalf("tokens=%+v", res.Tokens)
	}
	if !strings.Contains(rec.Body.String(), "[DONE]") {
		t.Fatalf("client body missing [DONE]: %s", rec.Body.String())
	}
}

func TestForwardStream_DisconnectAfterResponsesCompletedIsSuccess(t *testing.T) {
	// /v1/responses 无 [DONE]，以 response.completed 收尾；客户端收完后关连接应记普通成功。
	gin.SetMode(gin.TestMode)
	doneSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"status\":\"in_progress\"}}\n\n")
		_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n")
		_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":5,\"output_tokens\":2}}}\n\n")
		if flusher != nil {
			flusher.Flush()
		}
		close(doneSent)
		time.Sleep(40 * time.Millisecond)
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	clientCtx, cancelClient := context.WithCancel(context.Background())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(clientCtx)

	go func() {
		<-doneSent
		time.Sleep(15 * time.Millisecond)
		cancelClient()
	}()

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	body := []byte(`{"model":"m","stream":true,"input":"hi"}`)
	res := svc.forwardStream(
		clientCtx, c, target, "/v1/responses", http.MethodPost,
		http.Header{"Content-Type": []string{"application/json"}},
		body, protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, 0,
	)
	if !res.Committed {
		t.Fatal("expected committed")
	}
	if !res.DownstreamComplete {
		t.Fatal("expected DownstreamComplete after response.completed")
	}
	if res.ClientDisconnected {
		t.Fatal("post-response.completed disconnect must not keep ClientDisconnected")
	}
	if res.StreamErr != nil {
		t.Fatalf("streamErr=%v, want nil", res.StreamErr)
	}
	if res.Tokens.InputTokens != 5 || res.Tokens.OutputTokens != 2 {
		t.Fatalf("tokens=%+v", res.Tokens)
	}
	if !strings.Contains(rec.Body.String(), "response.completed") {
		t.Fatalf("client body missing response.completed: %s", rec.Body.String())
	}
}

func TestSSEFrameIsTerminal(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"data: [DONE]\n\n", true},
		{"data:[DONE]\n\n", true},
		{"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", true},
		{"event: error\ndata: {\"type\":\"error\"}\n\n", true},
		// OpenAI Responses：透传/转换后均以 response.completed 收尾，无 [DONE]
		{"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n", true},
		{"event:response.completed\ndata: {\"type\":\"response.completed\"}\n\n", true},
		{"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\"}}\n\n", true},
		{"event: response.failed\ndata: {\"type\":\"response.failed\"}\n\n", true},
		{"event: response.incomplete\ndata: {\"type\":\"response.incomplete\"}\n\n", true},
		{"event: response.error\ndata: {\"type\":\"response.error\"}\n\n", true},
		{"event: response.canceled\ndata: {\"type\":\"response.canceled\"}\n\n", true},
		{"data: {\"type\":\"error\",\"message\":\"busy\"}\n\n", true},
		{"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"x\"}\n\n", false},
		{"data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n", false},
		{": ping\n\n", false},
		{"", false},
	}
	for _, tc := range cases {
		if got := (&Runtime{Service: &Service{}}).sseFrameIsTerminal([]byte(tc.in)); got != tc.want {
			t.Fatalf("(&Runtime{Service: &Service{}}).sseFrameIsTerminal(%q)=%v want %v", tc.in, got, tc.want)
		}
	}
}

func TestResponsesSSEEventIsLifecycle(t *testing.T) {
	cases := []struct {
		eventName string
		data      string
		want      bool
	}{
		{"response.created", `{"type":"response.created"}`, true},
		{"response.in_progress", `{"type":"response.in_progress"}`, true},
		{"response.queued", `{"type":"response.queued"}`, true},
		{"response.output_item.added", `{"type":"response.output_item.added"}`, true},
		{"response.content_part.added", `{"type":"response.content_part.added"}`, true},
		{"response.output_text.delta", `{"type":"response.output_text.delta","delta":"x"}`, false},
		{"response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","delta":"{}"}`, false},
		{"response.failed", `{"type":"response.failed"}`, false},
		{"error", `{"type":"error","message":"busy"}`, false},
		{"response.created", `{not-json`, true},
	}
	for _, tc := range cases {
		if got := responsesSSEEventIsLifecycle(tc.eventName, tc.data); got != tc.want {
			t.Fatalf("event=%q data=%q got=%v want=%v", tc.eventName, tc.data, got, tc.want)
		}
	}
}

func TestClassifyResponsesSSEEventAvoidsJSONDecodeForTerminalStatus(t *testing.T) {
	lifecycle, terminal := classifyResponsesSSEEvent(
		"response.created",
		`{"type":"response.created","response":{"status":"completed"}}`,
	)
	if !lifecycle || !terminal {
		t.Fatalf("classification = lifecycle=%v terminal=%v, want both true", lifecycle, terminal)
	}
	lifecycle, terminal = classifyResponsesSSEEvent(
		"response.in_progress",
		`{"type":"response.in_progress","response":{"status":"in_progress"}}`,
	)
	if !lifecycle || terminal {
		t.Fatalf("in-progress classification = lifecycle=%v terminal=%v, want lifecycle only", lifecycle, terminal)
	}
}

func TestFinalizeStreamClientDisconnect(t *testing.T) {
	// 中途断开：保留 client 标记并补 StreamErr
	mid := streamAttemptResult{ClientDisconnected: true}
	(&Runtime{Service: &Service{}}).finalizeStreamClientDisconnect(&mid)
	if !mid.ClientDisconnected || !errors.Is(mid.StreamErr, errClientDisconnected) {
		t.Fatalf("mid=%+v", mid)
	}

	// 已交付终端帧：清除 client 标签
	done := streamAttemptResult{
		ClientDisconnected: true,
		DownstreamComplete: true,
		StreamErr:          errClientDisconnected,
	}
	(&Runtime{Service: &Service{}}).finalizeStreamClientDisconnect(&done)
	if done.ClientDisconnected || done.StreamErr != nil {
		t.Fatalf("done=%+v", done)
	}

	// 无断开：不动
	ok := streamAttemptResult{DownstreamComplete: true}
	(&Runtime{Service: &Service{}}).finalizeStreamClientDisconnect(&ok)
	if ok.ClientDisconnected || ok.StreamErr != nil {
		t.Fatalf("ok=%+v", ok)
	}
}

func TestForwardStream_AnthropicToOpenAIConvert(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_x\",\"model\":\"claude\"}}\n\n")
		_, _ = io.WriteString(w, "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Z\"}}\n\n")
		_, _ = io.WriteString(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	}))
	defer upstream.Close()

	svc := &Service{}
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"}
	res := svc.forwardStream(
		c.Request.Context(), c, target, "/v1/messages", http.MethodPost,
		nil, []byte(`{"model":"claude","stream":true,"messages":[]}`),
		protocol.KindOpenAIChat, protocol.KindAnthropic, "claude", true, 0,
	)
	if res.Err != nil {
		t.Fatalf("err=%v", res.Err)
	}
	if !res.Committed {
		t.Fatal("expected committed")
	}
	out := rec.Body.String()
	if !strings.Contains(out, "chat.completion.chunk") {
		t.Fatalf("expected openai chunks: %s", out)
	}
	if !strings.Contains(out, `"content":"Z"`) {
		t.Fatalf("missing content: %s", out)
	}
	if got := strings.Count(out, "[DONE]"); got != 1 {
		t.Fatalf("expected one converted terminal frame, got %d: %s", got, out)
	}
}

func TestStreamKeepaliveFrame(t *testing.T) {
	if got := string((&Runtime{Service: &Service{}}).streamKeepaliveFrame(protocol.KindOpenAIChat)); got != ":\n\n" {
		t.Fatalf("openai keepalive=%q", got)
	}
	if got := string((&Runtime{Service: &Service{}}).streamKeepaliveFrame(protocol.KindAnthropic)); !strings.Contains(got, "ping") {
		t.Fatalf("anthropic keepalive=%q", got)
	}
}

func TestSSEEventHasPayload(t *testing.T) {
	if (&Runtime{Service: &Service{}}).sseEventHasPayload(nil) || (&Runtime{Service: &Service{}}).sseEventHasPayload([]string{": PING", ":"}) {
		t.Fatal("comment-only events must not count as payload")
	}
	if !(&Runtime{Service: &Service{}}).sseEventHasPayload([]string{"data: {\"x\":1}"}) {
		t.Fatal("data line should count as payload")
	}
	if !(&Runtime{Service: &Service{}}).sseEventHasPayload([]string{"event: response.created", "data: {}"}) {
		t.Fatal("event+data should count as payload")
	}
}

func TestStreamFlushStateFlushesOnIntervalOrSize(t *testing.T) {
	now := time.Unix(100, 0)
	var state streamFlushState
	state.add([][]byte{[]byte("token")})
	if !state.shouldFlush(now) {
		t.Fatal("first buffered frame should flush immediately")
	}
	state.reset(now)
	state.add([][]byte{[]byte("token")})
	if state.shouldFlush(now.Add(streamFlushInterval - time.Nanosecond)) {
		t.Fatal("small frame flushed before interval")
	}
	if !state.shouldFlush(now.Add(streamFlushInterval)) {
		t.Fatal("small frame did not flush at interval")
	}
	state.reset(now)
	state.add([][]byte{[]byte(strings.Repeat("x", streamFlushBytes))})
	if !state.shouldFlush(now) {
		t.Fatal("size threshold did not force a flush")
	}
	state.stop()
}

func TestStreamFlushStateTimerFiresWithoutAnotherEvent(t *testing.T) {
	var state streamFlushState
	state.reset(time.Now())
	state.add([][]byte{[]byte("token")})
	ch := state.channel()
	select {
	case <-ch:
	case <-time.After(250 * time.Millisecond):
		state.stop()
		t.Fatal("flush timer did not fire")
	}
	state.stop()
}

func TestForwardStreamPreservesMultilineSSEEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "event: custom\nid: event-1\ndata: first\ndata: second\nretry: 1000\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer upstream.Close()

	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	res := (&Service{}).forwardStream(
		c.Request.Context(), c, &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
		"/v1/chat/completions", http.MethodPost, nil,
		[]byte(`{"model":"m","stream":true}`),
		protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
	)
	if res.Err != nil {
		t.Fatalf("err=%v", res.Err)
	}
	out := rec.Body.String()
	for _, part := range []string{"event: custom\n", "id: event-1\n", "data: first\n", "data: second\n", "retry: 1000\n\n"} {
		if !strings.Contains(out, part) {
			t.Fatalf("output missing %q: %s", part, out)
		}
	}
	if strings.Index(out, "event: custom") > strings.Index(out, "data: first") || strings.Index(out, "data: first") > strings.Index(out, "data: second") {
		t.Fatalf("multiline event order changed: %s", out)
	}
}

func TestForwardStreamReturnsAfterTerminalWithoutWaitingForEOF(t *testing.T) {
	gin.SetMode(gin.TestMode)
	release := make(chan struct{})
	releaseUpstream := func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}
	terminalSent := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		close(terminalSent)
		<-release
	}))
	defer func() {
		releaseUpstream()
		upstream.Close()
	}()

	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resultCh := make(chan streamAttemptResult, 1)
	started := time.Now()
	go func() {
		resultCh <- (&Service{}).forwardStream(
			c.Request.Context(), c, &upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
			"/v1/chat/completions", http.MethodPost, nil,
			[]byte(`{"model":"m","stream":true}`),
			protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, 0,
		)
	}()
	select {
	case <-terminalSent:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("test upstream did not send terminal frame")
	}
	var result streamAttemptResult
	select {
	case result = <-resultCh:
	case <-time.After(250 * time.Millisecond):
		releaseUpstream()
		<-resultCh
		t.Fatal("forwarder waited for upstream EOF after terminal frame")
	}
	if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
		t.Fatalf("terminal return took %s", elapsed)
	}
	if result.Err != nil || !result.DownstreamComplete {
		t.Fatalf("result=%+v", result)
	}
}

type streamErrorBody struct {
	data   []byte
	offset int
	err    error
}

func (r *streamErrorBody) Read(p []byte) (int, error) {
	if r.offset < len(r.data) {
		n := copy(p, r.data[r.offset:])
		r.offset += n
		return n, nil
	}
	return 0, r.err
}

func (r *streamErrorBody) Close() error { return nil }

func TestForwardStreamScannerErrorDoesNotCommitIncompleteEvent(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body:       &streamErrorBody{data: []byte("data: {\"error\":{\"message\":\"partial\"}}"), err: errors.New("read boom")},
	}
	result := (&Runtime{Service: &Service{}}).forwardStreamIncremental(
		context.Background(), context.Background(), nil, c, resp, time.Now(), 0,
		protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, resp.Header, resp.StatusCode,
	)
	if result.Err == nil {
		t.Fatal("expected scanner error")
	}
	if result.Committed || rec.Body.Len() != 0 {
		t.Fatalf("incomplete event reached client: committed=%v body=%q", result.Committed, rec.Body.String())
	}
	if !strings.Contains(string(result.Body), "partial") {
		t.Fatalf("diagnostic body lost incomplete event: %q", result.Body)
	}
}

func TestForwardStreamScannerErrorKeepsCompletedEvents(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: &streamErrorBody{
			data: []byte("data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\ndata: {\"partial\":true}"),
			err:  errors.New("read boom"),
		},
	}
	result := (&Runtime{Service: &Service{}}).forwardStreamIncremental(
		context.Background(), context.Background(), nil, c, resp, time.Now(), 0,
		protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, resp.Header, resp.StatusCode,
	)
	if !result.Committed || result.StreamErr == nil {
		t.Fatalf("result=%+v, want committed stream error", result)
	}
	out := rec.Body.String()
	if !strings.Contains(out, `"content":"ok"`) {
		t.Fatalf("completed event was lost: %s", out)
	}
	if strings.Contains(out, "partial") {
		t.Fatalf("incomplete event was forwarded: %s", out)
	}
}
