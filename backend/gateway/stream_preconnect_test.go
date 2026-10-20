package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func preconnectTestWriter(t *testing.T, kind protocolKind) (*gin.Context, *httptest.ResponseRecorder, *streamPreconnectWriter) {
	t.Helper()
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	ctx, cancel := context.WithCancel(c.Request.Context())
	w := newStreamPreconnectWriter(ctx, c.Writer, kind, "preconnect-test", cancel)
	c.Request = c.Request.WithContext(context.WithValue(ctx, streamPreconnectKey{}, w))
	c.Writer = w
	w.start(time.Hour, defaultStreamKeepalive)
	t.Cleanup(func() { w.finish(); cancel() })
	return c, rec, w
}

func TestStreamPreconnectCommentDoesNotCommitModelOrTTFT(t *testing.T) {
	c, rec, w := preconnectTestWriter(t, protocol.KindOpenAIResponses)
	state := &forwardRequestTiming{started: time.Now().Add(-100 * time.Millisecond)}
	c.Writer = &requestTimingWriter{ResponseWriter: w, state: state, kind: protocol.KindOpenAIResponses}
	if !w.keepalive() || !w.Written() || w.Status() != http.StatusOK {
		t.Fatal("heartbeat did not establish HTTP 200")
	}
	if streamWriterActuallyCommitted(c) {
		t.Fatal("transport comment locked the upstream attempt")
	}
	if first, _ := state.snapshot(); first != nil {
		t.Fatal("comment manufactured TTFT")
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" || rec.Body.String() != ": upstream-ops keepalive\n\n" {
		t.Fatalf("headers=%v body=%s", rec.Header(), rec.Body.String())
	}
	// Normal header changes must never race the timer's frozen physical map.
	c.Header("Content-Type", "text/event-stream")
	_, err := c.Writer.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"answer\"}\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	c.Writer.Flush()
	if !streamWriterActuallyCommitted(c) || w.keepalive() {
		t.Fatal("real output did not take over keepalive")
	}
	if first, _ := state.snapshot(); first == nil || *first < 100 {
		t.Fatalf("real TTFT=%v", first)
	}
}

func TestStreamPreconnectDoesNotDisarmRealFirstTokenBudget(t *testing.T) {
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	finishTiming := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocol.KindOpenAIResponses, true, 50*time.Millisecond)
	defer finishTiming()
	group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
		StreamPreconnectEnabled: true, StreamPreconnectMinDelayMS: 60000, StreamPreconnectMaxDelayMS: 60000,
	}}
	finish := beginStreamPreconnect(c, group, protocol.KindOpenAIResponses, "/v1/responses", true, true)
	defer finish()
	w := streamPreconnectFor(c)
	if w == nil || !w.keepalive() {
		t.Fatal("missing early stream")
	}
	select {
	case <-c.Request.Context().Done():
		if !errors.Is(context.Cause(c.Request.Context()), errRequestFirstTokenBudget) {
			t.Fatalf("cause=%v", context.Cause(c.Request.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("comment stopped the first-token budget")
	}
	if first, _ := requestTiming(c.Request.Context()).snapshot(); first != nil {
		t.Fatal("timed-out request acquired synthetic TTFT")
	}
}

func TestStreamPreconnectErrorsAreProtocolCompatible(t *testing.T) {
	for _, kind := range []protocolKind{protocol.KindOpenAIChat, protocol.KindOpenAIResponses, protocol.KindAnthropic} {
		for _, early := range []bool{false, true} {
			t.Run(string(kind)+map[bool]string{false: "/json", true: "/sse"}[early], func(t *testing.T) {
				c, rec, w := preconnectTestWriter(t, kind)
				if early && !w.keepalive() {
					t.Fatal("heartbeat failed")
				}
				(&Service{}).writeGatewayError(c, kind, 504, "request_timeout", "budget exhausted")
				if !early {
					if rec.Code != 504 || !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") || strings.Contains(rec.Body.String(), "data:") {
						t.Fatalf("JSON failure=%d %s", rec.Code, rec.Body.String())
					}
					return
				}
				body := rec.Body.String()
				if rec.Code != 200 || !strings.HasPrefix(body, ": upstream-ops keepalive\n\n") || !strings.Contains(body, "budget exhausted") || !strings.Contains(body, "request_timeout") || !strings.Contains(body, "preconnect-test") {
					t.Fatalf("stream failure=%d %s", rec.Code, body)
				}
				switch kind {
				case protocol.KindAnthropic:
					if !strings.Contains(body, "event: error\n") {
						t.Fatal(body)
					}
				case protocol.KindOpenAIResponses:
					if !strings.Contains(body, "event: response.failed\n") {
						t.Fatal(body)
					}
				default:
					if !strings.HasSuffix(body, "data: [DONE]\n\n") {
						t.Fatal(body)
					}
				}
			})
		}
	}
}

func TestStreamPreconnectErrorAndHeartbeatRace(t *testing.T) {
	for i := 0; i < 40; i++ {
		_, rec, w := preconnectTestWriter(t, protocol.KindOpenAIResponses)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; w.keepalive() }()
		go func() {
			defer wg.Done()
			<-start
			if err := w.writeFinalError(503, []byte("{\"error\":{\"message\":\"busy\"}}")); err != nil {
				t.Error(err)
			}
		}()
		close(start)
		wg.Wait()
		w.finish()
		body := rec.Body.String()
		if rec.Code == 200 {
			if !strings.Contains(body, "event: response.failed\n") || strings.Contains(body, "\n\n{\"error\"") {
				t.Fatal(body)
			}
		} else if rec.Code != 503 || strings.Contains(body, "keepalive") || strings.Contains(body, "data:") {
			t.Fatalf("mixed response=%d %s", rec.Code, body)
		}
	}
}

func TestStreamPreconnectPreservesErrorIdentity(t *testing.T) {
	for _, kind := range []protocolKind{protocol.KindOpenAIChat, protocol.KindOpenAIResponses, protocol.KindAnthropic} {
		for _, tc := range []struct {
			name, body, errorType, code string
		}{
			{"root", "{\"error\":{\"message\":\"missing\",\"type\":\"invalid_request_error\",\"code\":\"model_not_found\"}}", "invalid_request_error", "\"code\":\"model_not_found\""},
			{"nested", "{\"response\":{\"error\":{\"message\":\"timeout\",\"type\":\"request_timeout\",\"code\":\"first_token_budget\"}}}", "request_timeout", "\"code\":\"first_token_budget\""},
			{"numeric-code", "{\"error\":{\"message\":\"busy\",\"type\":\"overloaded_error\",\"code\":503}}", "overloaded_error", "\"code\":503"},
			{"sse", "event: response.failed\ndata: {\"response\":{\"error\":{\"message\":\"timeout\",\"type\":\"request_timeout\",\"code\":\"first_token_budget\"}}}\n\n", "request_timeout", "\"code\":\"first_token_budget\""},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				_, rec, w := preconnectTestWriter(t, kind)
				if !w.keepalive() {
					t.Fatal("heartbeat failed")
				}
				if err := w.writeFinalError(503, []byte(tc.body)); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(rec.Body.String(), tc.errorType) || !strings.Contains(rec.Body.String(), tc.code) {
					t.Fatalf("error identity lost: %s", rec.Body.String())
				}
			})
		}
	}
}

type preconnectShortWriter struct {
	gin.ResponseWriter
	short bool
}

func (w *preconnectShortWriter) Write(payload []byte) (int, error) {
	if w.short {
		return 0, nil
	}
	return w.ResponseWriter.Write(payload)
}

func (w *preconnectShortWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func TestStreamPreconnectFinalErrorRejectsShortWrite(t *testing.T) {
	for _, early := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sse"}[early], func(t *testing.T) {
			_, _, w := preconnectTestWriter(t, protocol.KindOpenAIResponses)
			raw := &preconnectShortWriter{ResponseWriter: w.ResponseWriter}
			w.ResponseWriter = raw
			if early && !w.keepalive() {
				t.Fatal("heartbeat failed")
			}
			raw.short = true
			err := w.writeFinalError(503, []byte("{\"error\":{\"message\":\"busy\"}}"))
			if !errors.Is(err, io.ErrShortWrite) || !errors.Is(w.writeErr, io.ErrShortWrite) || w.ctx.Err() == nil {
				t.Fatalf("short write was ignored: err=%v stored=%v context=%v", err, w.writeErr, w.ctx.Err())
			}
			if w.DownstreamCommitted() || w.keepalive() {
				t.Fatal("failed write was committed or heartbeat continued")
			}
		})
	}
}

func TestStreamPreconnectFastOutputAndCancellationStopTimer(t *testing.T) {
	_, rec, w := preconnectTestWriter(t, protocol.KindOpenAIChat)
	w.Header().Set("Content-Type", "text/event-stream")
	if _, err := w.Write([]byte("data: {\"choices\":[]}\n\n")); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	w.finish()
	if w.keepalive() || strings.Contains(rec.Body.String(), "keepalive") {
		t.Fatal("fast output was delayed or prefixed")
	}
	_, _, canceled := preconnectTestWriter(t, protocol.KindOpenAIResponses)
	canceled.cancel()
	select {
	case <-canceled.done:
	case <-time.After(time.Second):
		t.Fatal("timer leaked after client cancellation")
	}
}

func TestStreamPreconnectDelayAndEligibility(t *testing.T) {
	for i := 0; i < 200; i++ {
		if delay := streamPreconnectDelay(123, 129); delay < 123*time.Millisecond || delay > 129*time.Millisecond {
			t.Fatal(delay)
		}
	}
	if streamPreconnectDelay(0, 0) != 0 || streamPreconnectDelay(42, 42) != 42*time.Millisecond {
		t.Fatal("fixed delay changed")
	}
	for _, tc := range []struct {
		enabled, stream, text bool
		path                  string
		min, max              int
	}{
		{false, true, true, "/v1/responses", 0, 0},
		{true, false, true, "/v1/responses", 0, 0},
		{true, true, false, "/v1/responses", 0, 0},
		{true, true, true, "/v1/images/generations", 0, 0},
		{true, true, true, "/v1/responses", 10, 1},
		{true, true, true, "/v1/responses", 0, 60001},
	} {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
		group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: tc.enabled, StreamPreconnectMinDelayMS: tc.min, StreamPreconnectMaxDelayMS: tc.max}}
		finish := beginStreamPreconnect(c, group, protocol.KindOpenAIResponses, tc.path, tc.stream, tc.text)
		finish()
		if streamPreconnectFor(c) != nil {
			t.Fatalf("unexpected preconnect: %+v", tc)
		}
	}
	for _, header := range []http.Header{
		{"Connection": {"Upgrade"}, "Upgrade": {"websocket"}},
		{"Sec-Websocket-Key": {"test-key"}},
	} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
		c.Request.Header = header
		group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: true}}
		finish := beginStreamPreconnect(c, group, protocol.KindOpenAIResponses, "/v1/responses", true, true)
		finish()
		if streamPreconnectFor(c) != nil {
			t.Fatalf("WebSocket upgrade gained SSE preconnect: %v", header)
		}
	}
}
