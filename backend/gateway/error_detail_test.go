package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/connector"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestIsSourceGroupIDPlaceholder(t *testing.T) {
	cases := map[string]bool{
		"":          false,
		"GPT Plus":  false,
		"id:31":     true,
		"ID:31":     true,
		"id：31":     true,
		"源 ID: 31":  true,
		"源id:41":    true,
		"id:":       false,
		"something": false,
	}
	for in, want := range cases {
		if got := (*Service)(nil).isSourceGroupIDPlaceholder(in); got != want {
			t.Fatalf("%q: got %v want %v", in, got, want)
		}
	}
}

func TestIsFailoverStatus(t *testing.T) {
	// 默认：仅 0 / 429 / 5xx
	for _, code := range []int{0, 429, 500, 502, 503} {
		if !(*Service)(nil).isFailoverStatus(code, false) {
			t.Fatalf("default: status %d should failover", code)
		}
	}
	for _, code := range []int{200, 400, 401, 403, 404, 408, 422} {
		if (*Service)(nil).isFailoverStatus(code, false) {
			t.Fatalf("default: status %d should NOT failover", code)
		}
	}
	// 开启 4xx 顺延：全部 4xx 可 failover
	for _, code := range []int{400, 401, 403, 404, 408, 422, 429} {
		if !(*Service)(nil).isFailoverStatus(code, true) {
			t.Fatalf("failover_on_4xx: status %d should failover", code)
		}
	}
	if (*Service)(nil).isFailoverStatus(200, true) {
		t.Fatalf("failover_on_4xx: 200 should not failover")
	}
	if !(*Service)(nil).isFailoverStatus(503, true) {
		t.Fatalf("failover_on_4xx: 503 should still failover")
	}
}

func TestSameRouteRetryabilitySkipsDeterministicErrors(t *testing.T) {
	tests := []struct {
		name   string
		status int
		info   usageErrorInfo
		want   bool
	}{
		{name: "temporary 429", status: http.StatusTooManyRequests, want: true},
		{name: "temporary 503", status: http.StatusServiceUnavailable, want: true},
		{name: "media capability", status: http.StatusForbidden, info: usageErrorInfo{Summary: "HTTP 403: Image generation is not enabled for this group"}, want: false},
		{name: "balance", status: http.StatusForbidden, info: usageErrorInfo{UpstreamBody: `{"error":"Insufficient account balance"}`}, want: false},
		{name: "model missing", status: http.StatusNotFound, info: usageErrorInfo{Summary: "model_not_found: no available channel for model gpt-test"}, want: false},
		{name: "unknown 403", status: http.StatusForbidden, info: usageErrorInfo{Summary: "HTTP 403: temporary policy response"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSameRouteRetryableUpstreamFailure(tt.status, tt.info); got != tt.want {
				t.Fatalf("retryable=%v, want %v", got, tt.want)
			}
		})
	}
}

func TestEnrichRouteSourceGroupName(t *testing.T) {
	gid := int64(31)
	route := storage.GatewayRoute{SourceChannelID: 15, SourceGroupID: &gid, SourceGroupName: ""}
	groups := []connector.APIKeyGroup{
		{ID: &gid, Name: "测试123"},
	}
	(*Service)(nil).enrichRouteSourceGroupName(&route, groups)
	if route.SourceGroupName != "测试123" {
		t.Fatalf("name=%q", route.SourceGroupName)
	}
	// 已有真实名不覆盖
	route.SourceGroupName = "keep"
	(*Service)(nil).enrichRouteSourceGroupName(&route, groups)
	if route.SourceGroupName != "keep" {
		t.Fatalf("should keep real name, got %q", route.SourceGroupName)
	}
	// 占位 id:N 会被替换
	route.SourceGroupName = "id:31"
	(*Service)(nil).enrichRouteSourceGroupName(&route, groups)
	if route.SourceGroupName != "测试123" {
		t.Fatalf("placeholder not replaced: %q", route.SourceGroupName)
	}
}

func TestIsClientDisconnectAfterCommit(t *testing.T) {
	if (*Service)(nil).isClientDisconnectAfterCommit(false, nil) {
		t.Fatal("no disconnect should be false")
	}
	if !(*Service)(nil).isClientDisconnectAfterCommit(true, nil) {
		t.Fatal("disconnect with nil streamErr should count")
	}
	if !(*Service)(nil).isClientDisconnectAfterCommit(true, errClientDisconnected) {
		t.Fatal("disconnect with errClientDisconnected should count")
	}
	if !(*Service)(nil).isClientDisconnectAfterCommit(true, fmt.Errorf("wrap: %w", errClientDisconnected)) {
		t.Fatal("wrapped errClientDisconnected should count")
	}
	if (*Service)(nil).isClientDisconnectAfterCommit(true, errors.New("upstream stream idle")) {
		t.Fatal("real stream error must not count as only-client-disconnect")
	}
	if (*Service)(nil).isClientDisconnectAfterCommit(false, errClientDisconnected) {
		t.Fatal("stream err without disconnect flag should not count")
	}
}

func TestIsClientContextError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// 入站 context 已取消 + 包装后的 canceled 错误
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)

	wrapped := errors.New(`Post "https://e.e2.ink/v1/responses": context canceled`)
	if !(*Service)(nil).isClientContextError(wrapped, c) {
		t.Fatal("want client context error when request ctx canceled")
	}
	if !(*Service)(nil).isClientContextError(context.Canceled, c) {
		t.Fatal("want context.Canceled recognized")
	}

	// 入站 context 仍存活：裸 Canceled 不应算客户端取消（避免误伤）
	c2, _ := gin.CreateTestContext(httptest.NewRecorder())
	c2.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	if (*Service)(nil).isClientContextError(context.Canceled, c2) {
		t.Fatal("live request ctx should not treat bare Canceled as client cancel")
	}
	if (*Service)(nil).isClientContextError(wrapped, c2) {
		t.Fatal("live request ctx should not treat string-wrapped cancel as client cancel")
	}

	// 首字超时不是客户端取消
	if (*Service)(nil).isClientContextError(errFirstTokenTimeout, c) {
		t.Fatal("first token timeout is not client cancel")
	}
}

func TestAnnotateClientContextError(t *testing.T) {
	info := usageErrorInfo{
		Type:    "transport",
		Summary: `Post "https://x/v1/responses": context canceled`,
		Detail:  "transport error\n",
	}
	(*Service)(nil).annotateClientContextError(&info, nil, "https://x/v1/responses", "POST", context.Canceled)
	if info.Type != "client" {
		t.Fatalf("type=%s", info.Type)
	}
	if !strings.Contains(info.Summary, "客户端") {
		t.Fatalf("summary=%q", info.Summary)
	}
	if !strings.Contains(info.Detail, "stop retry/failover") {
		t.Fatalf("detail=%q", info.Detail)
	}
}

func TestEnsureGatewayRequestID_IgnoresClientHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	// Codex 风格 UUID + 客户端头：不得成为使用记录 request_id
	clientRID := "019f51b1-f52c-74c1-9370-db09bab43ffb"
	req.Header.Set("X-Request-Id", clientRID)
	req.Header.Set("X-Client-Request-Id", clientRID)
	req.Header.Set("X-Upstream-Ops-Request-Id", "client-forged-id")
	c.Request = req

	id1 := (*Service)(nil).ensureGatewayRequestID(c)
	if id1 == clientRID || id1 == "client-forged-id" {
		t.Fatalf("must not adopt client request id, got %q", id1)
	}
	if !(*Service)(nil).isGatewayGeneratedRequestID(id1) {
		t.Fatalf("want gateway 24-hex id, got %q", id1)
	}
	// 同请求再次 ensure 应复用
	id2 := (*Service)(nil).ensureGatewayRequestID(c)
	if id2 != id1 {
		t.Fatalf("same request should reuse id: %q vs %q", id1, id2)
	}
	if got := w.Header().Get("X-Upstream-Ops-Request-Id"); got != id1 {
		t.Fatalf("response header = %q, want %q", got, id1)
	}
	// 网关不得改写客户端/上游的 X-Request-Id（响应侧仅由 copyResponseHeaders 透传上游值）
	if got := w.Header().Get("X-Request-Id"); got != "" {
		t.Fatalf("must not set X-Request-Id on response, got %q", got)
	}
}

func TestCopyClientRequestIDHeaders_Passthrough(t *testing.T) {
	src := http.Header{}
	src.Set("X-Request-Id", "019f51b1-f52c-74c1-9370-db09bab43ffb")
	src.Set("X-Client-Request-Id", "client_xyz")
	src.Set("X-Upstream-Ops-Request-Id", "forged") // 不得转发网关专用头
	src.Set("Authorization", "Bearer secret")
	dst := http.Header{}
	(*Service)(nil).copyClientRequestIDHeaders(dst, src)
	if got := dst.Get("X-Request-Id"); got != "019f51b1-f52c-74c1-9370-db09bab43ffb" {
		t.Fatalf("X-Request-Id=%q", got)
	}
	if got := dst.Get("X-Client-Request-Id"); got != "client_xyz" {
		t.Fatalf("X-Client-Request-Id=%q", got)
	}
	if dst.Get("X-Upstream-Ops-Request-Id") != "" {
		t.Fatal("must not forward X-Upstream-Ops-Request-Id")
	}
	if dst.Get("Authorization") != "" {
		t.Fatal("must not forward Authorization via request-id helper")
	}
}

func TestCopyResponseHeaders_PreservesUpstreamRequestID(t *testing.T) {
	dst := http.Header{}
	// 模拟 ensure 已写入网关专用头
	dst.Set("X-Upstream-Ops-Request-Id", "5381a23b3f0cea2d7b8ff8df")
	src := http.Header{}
	src.Set("X-Request-Id", "upstream-req-abc")
	src.Set("X-Upstream-Ops-Request-Id", "should-not-copy")
	src.Set("Cf-Ray", "ray-1")
	(*Service)(nil).copyResponseHeaders(dst, src)
	if got := dst.Get("X-Request-Id"); got != "upstream-req-abc" {
		t.Fatalf("X-Request-Id=%q", got)
	}
	if got := dst.Get("X-Upstream-Ops-Request-Id"); got != "5381a23b3f0cea2d7b8ff8df" {
		t.Fatalf("ops id should stay gateway-owned, got %q", got)
	}
	if got := dst.Get("Cf-Ray"); got != "ray-1" {
		t.Fatalf("Cf-Ray=%q", got)
	}
}

func TestIsGatewayGeneratedRequestID(t *testing.T) {
	if !(*Service)(nil).isGatewayGeneratedRequestID("5381a23b3f0cea2d7b8ff8df") {
		t.Fatal("24 hex should pass")
	}
	if (*Service)(nil).isGatewayGeneratedRequestID("019f51b1-f52c-74c1-9370-db09bab43ffb") {
		t.Fatal("UUID should fail")
	}
	if (*Service)(nil).isGatewayGeneratedRequestID("short") {
		t.Fatal("short should fail")
	}
}

func TestBuildUpstreamErrorInfo_HTTP(t *testing.T) {
	body := []byte(`{"error":{"message":"model overloaded","type":"server_error","code":"overloaded"}}`)
	h := http.Header{}
	h.Set("Content-Type", "application/json")
	h.Set("x-request-id", "req_abc")
	h.Set("X-Client-Request-Id", "client_xyz")
	h.Set("Cf-Ray", "ray-123")
	h.Set("Server", "Caddy")
	h.Set("Authorization", "Bearer secret")

	info := (*Service)(nil).buildUpstreamErrorInfo(nil, 503, h, body, "https://api.example/v1/chat/completions", "POST")
	if info.Type != "http" {
		t.Fatalf("type=%s", info.Type)
	}
	if !strings.Contains(info.Summary, "503") || !strings.Contains(info.Summary, "model overloaded") {
		t.Fatalf("summary=%q", info.Summary)
	}
	if !strings.Contains(info.Detail, "upstream_request_id: req_abc") {
		t.Fatalf("detail missing request id: %s", info.Detail)
	}
	// Detail 应包含完整上游 header 文本块
	for _, want := range []string{
		"headers:",
		"Content-Type: application/json",
		"X-Request-Id: req_abc",
		"X-Client-Request-Id: client_xyz",
		"Cf-Ray: ray-123",
		"Server: Caddy",
		"Authorization: [redacted]",
	} {
		if !strings.Contains(info.Detail, want) {
			t.Fatalf("detail missing %q:\n%s", want, info.Detail)
		}
	}
	if !strings.Contains(info.UpstreamBody, "model overloaded") {
		t.Fatalf("body=%q", info.UpstreamBody)
	}
	if strings.Contains(info.UpstreamHeaders, "secret") {
		t.Fatalf("headers should redact secrets: %s", info.UpstreamHeaders)
	}
	if !strings.Contains(info.UpstreamHeaders, "[redacted]") {
		t.Fatalf("expected redacted auth header: %s", info.UpstreamHeaders)
	}
	// JSON 字段也应包含完整 header 名
	for _, want := range []string{"X-Request-Id", "Cf-Ray", "Server", "X-Client-Request-Id"} {
		if !strings.Contains(info.UpstreamHeaders, want) {
			t.Fatalf("UpstreamHeaders missing %q: %s", want, info.UpstreamHeaders)
		}
	}
}

func TestFormatHeadersPlain_Complete(t *testing.T) {
	h := http.Header{}
	h.Add("X-Custom", "a")
	h.Add("X-Custom", "b")
	h.Set("Set-Cookie", "session=secret")
	plain := (*Service)(nil).formatHeadersPlain(h, 0)
	if !strings.Contains(plain, "X-Custom: a") || !strings.Contains(plain, "X-Custom: b") {
		t.Fatalf("want multi-value headers, got:\n%s", plain)
	}
	if !strings.Contains(plain, "Set-Cookie: [redacted]") {
		t.Fatalf("want redacted cookie, got:\n%s", plain)
	}
	if strings.Contains(plain, "secret") {
		t.Fatalf("leaked secret:\n%s", plain)
	}
}

func TestBuildUpstreamErrorInfo_Transport(t *testing.T) {
	info := (*Service)(nil).buildUpstreamErrorInfo(errors.New("dial tcp: i/o timeout"), 0, nil, nil, "https://api.example/v1/messages", "POST")
	if info.Type != "transport" {
		t.Fatalf("type=%s", info.Type)
	}
	if !strings.Contains(info.Summary, "i/o timeout") {
		t.Fatalf("summary=%q", info.Summary)
	}
	if !strings.Contains(info.Detail, "https://api.example/v1/messages") {
		t.Fatalf("detail=%q", info.Detail)
	}
}

func TestExtractUpstreamErrorSnippet_OpenAI(t *testing.T) {
	s := (*Service)(nil).extractUpstreamErrorSnippet([]byte(`{"error":{"message":"rate limited","type":"rate_limit_error","code":"429"}}`))
	if !strings.Contains(s, "rate limited") {
		t.Fatalf("got %q", s)
	}
}

func TestTruncateBytesForLog(t *testing.T) {
	in := []byte(strings.Repeat("a", 100))
	out := (*Service)(nil).truncateBytesForLog(in, 10)
	if !strings.HasSuffix(out, "…(truncated)") {
		t.Fatalf("out=%q", out)
	}
}

func TestInjectUpstreamOpsRequestID(t *testing.T) {
	in := []byte(`{"error":{"message":"boom","type":"api_error"}}`)
	out := (*Service)(nil).injectUpstreamOpsRequestID(in, "req_test_123")
	if !strings.Contains(string(out), `"upstream_ops_request_id":"req_test_123"`) {
		t.Fatalf("%s", out)
	}
	if !strings.Contains(string(out), `"message":"boom"`) {
		t.Fatalf("lost original error: %s", out)
	}
	// non-json
	out2 := (*Service)(nil).injectUpstreamOpsRequestID([]byte("plain fail"), "rid2")
	if !strings.Contains(string(out2), `"upstream_ops_request_id":"rid2"`) {
		t.Fatalf("%s", out2)
	}
}

func TestReadFirstChunk_Timeout(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	// 不写数据，应超时
	_, err := (*Service)(nil).readFirstChunk(pr, 50*time.Millisecond)
	if !(*Service)(nil).isFirstTokenTimeout(err) {
		t.Fatalf("want first token timeout, got %v", err)
	}
	_ = pw.Close()
}

func TestReadFirstChunk_OK(t *testing.T) {
	r := strings.NewReader("hello")
	got, err := (*Service)(nil).readFirstChunk(r, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q", got)
	}
}

func TestRemainingFirstTokenWait(t *testing.T) {
	start := time.Now().Add(-1500 * time.Millisecond)
	left, timedOut := (*Service)(nil).remainingFirstTokenWait(start, 2*time.Second)
	if timedOut {
		t.Fatal("1.5s elapsed of 2s should not be timed out")
	}
	if left < 200*time.Millisecond || left > 700*time.Millisecond {
		t.Fatalf("left=%s, want ~500ms", left)
	}

	start2 := time.Now().Add(-3 * time.Second)
	_, timedOut = (*Service)(nil).remainingFirstTokenWait(start2, 2*time.Second)
	if !timedOut {
		t.Fatal("3s elapsed of 2s should be timed out")
	}

	_, timedOut = (*Service)(nil).remainingFirstTokenWait(time.Now(), 0)
	if timedOut {
		t.Fatal("configured 0 means disabled, not timed out")
	}
}

func TestDoHTTPWithFirstTokenDeadline_SlowHeaders(t *testing.T) {
	// 上游故意晚发响应头：应在首字预算内超时，而不是一直等到 body
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := (*Service)(nil).doHTTPWithFirstTokenDeadline(ctx, cancel, srv.Client(), req, start, 80*time.Millisecond)
	if resp != nil {
		_ = resp.Body.Close()
	}
	if !(*Service)(nil).isFirstTokenTimeout(err) {
		t.Fatalf("want first token timeout on slow headers, got resp=%v err=%v", resp, err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("should abort quickly, elapsed=%s", elapsed)
	}
}

func TestDoHTTPWithFirstTokenDeadline_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("hi"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := (*Service)(nil).doHTTPWithFirstTokenDeadline(ctx, cancel, srv.Client(), req, start, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	// 成功拿到头后不应再被 abort（cancel 是同一 cancel，测试里 defer 会 cancel；
	// 这里立即读 body 验证 Do 成功路径可用）
	b, _ := io.ReadAll(resp.Body)
	if string(b) != "hi" {
		t.Fatalf("body=%q", b)
	}
}

func TestClampFirstTokenTimeoutSec(t *testing.T) {
	if (*Service)(nil).clampFirstTokenTimeoutSec(0) != 0 {
		t.Fatal("0 should stay 0")
	}
	if (*Service)(nil).clampFirstTokenTimeoutSec(1) != 1 {
		t.Fatal("1 ok")
	}
	if (*Service)(nil).clampFirstTokenTimeoutSec(999) != 300 {
		t.Fatal("cap 300")
	}
}

func TestEffectiveFirstTokenTimeout(t *testing.T) {
	cfg := 30 * time.Second

	// 未配置：始终关闭
	if got := (*Service)(nil).effectiveFirstTokenTimeout(0, true, true, 0, 8, true); got != 0 {
		t.Fatalf("configured 0 => 0, got %s", got)
	}

	// 中间渠道且还能顺延：启用
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, true, true, 0, 8, true); got != cfg {
		t.Fatalf("mid route with failover => configured, got %s", got)
	}

	// 最后一条可试路由（无更多路由）：关闭
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, true, true, 0, 8, false); got != 0 {
		t.Fatalf("last route (no more) => 0, got %s", got)
	}

	// 顺延次数已用尽：关闭
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, true, true, 1, 1, true); got != 0 {
		t.Fatalf("failover max exhausted => 0, got %s", got)
	}

	// 顺延关闭：关闭
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, true, false, 0, 8, true); got != 0 {
		t.Fatalf("failover disabled => 0, got %s", got)
	}

	// 重试总开关关闭：关闭
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, false, true, 0, 8, true); got != 0 {
		t.Fatalf("retry disabled => 0, got %s", got)
	}

	// 首条失败后 failoversDone 仍为 0、且还有下家：仍启用
	if got := (*Service)(nil).effectiveFirstTokenTimeout(cfg, true, true, 0, 1, true); got != cfg {
		t.Fatalf("first of two with max=1 => configured, got %s", got)
	}
}
