package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestRequestTTFTIncludesPriorWaitAndRequiresVisibleFlush(t *testing.T) {
	for _, tc := range []struct {
		name              string
		kind              protocolKind
		metadata, content string
	}{
		{"chat", protocol.KindOpenAIChat, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n", "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"},
		{"responses", protocol.KindOpenAIResponses, "event: response.created\ndata: {\"type\":\"response.created\"}\n\n", "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hi\"}\n\n"},
		{"anthropic", protocol.KindAnthropic, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"role\":\"assistant\"}}\n\n", "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			finish := beginForwardRequest(c, time.Now().Add(-2*time.Second), &storage.GatewayGroup{}, tc.kind, true, time.Minute)
			defer finish()
			state := requestTiming(c.Request.Context())
			_, _ = c.Writer.WriteString(tc.metadata)
			c.Writer.Flush()
			if first, _ := state.snapshot(); first != nil {
				t.Fatal("metadata started TTFT")
			}
			// Split every byte to exercise frame boundaries across writes.
			for _, b := range []byte(tc.content) {
				if _, err := c.Writer.Write([]byte{b}); err != nil {
					t.Fatal(err)
				}
			}
			if first, _ := state.snapshot(); first != nil {
				t.Fatal("unflushed content started TTFT")
			}
			c.Writer.Flush()
			first, duration := state.snapshot()
			if first == nil || *first < 2000 || *duration < *first {
				t.Fatalf("first=%v duration=%v", first, duration)
			}
			c.Writer.Flush()
			again, _ := state.snapshot()
			if *again != *first {
				t.Fatal("subsequent flush changed TTFT")
			}
		})
	}
}

func TestRequestBudgetStopsBeforeFirstContentButNotHealthyLongStream(t *testing.T) {
	for _, deliver := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
		finish := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocolOpenAI, true, 100*time.Millisecond)
		if deliver {
			_, _ = c.Writer.WriteString("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
			c.Writer.Flush()
		} else {
			// A heartbeat and several attempts cannot reset the request clock.
			for i := 0; i < 3; i++ {
				if err := claimRequestAttempt(c.Request.Context(), 0); err != nil {
					t.Fatal(err)
				}
			}
			_, _ = c.Writer.WriteString(": ping\n\n")
			c.Writer.Flush()
		}
		select {
		case <-c.Request.Context().Done():
			if deliver || !errors.Is(context.Cause(c.Request.Context()), errRequestFirstTokenBudget) {
				t.Fatalf("unexpected cancellation: %v", context.Cause(c.Request.Context()))
			}
		case <-time.After(300 * time.Millisecond):
			if !deliver {
				t.Fatal("budget did not cancel request")
			}
		}
		finish()
	}
}

func TestRequestBudgetCancelsMetadataOnlyUpstreamAndReleasesSlot(t *testing.T) {
	stopped := make(chan struct{})
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(stopped)
	}))
	defer upstream.Close()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	finish := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocolOpenAI, true, 200*time.Millisecond)
	defer finish()
	rt := (&Service{}).runtime()
	target := &upstreamTarget{BaseURL: upstream.URL, APIKey: "test", Channel: &storage.Channel{ID: 91, ConcurrencyLimit: 1}}
	done := make(chan streamAttemptResult, 1)
	go func() {
		done <- rt.forwardStream(c.Request.Context(), c, target, "/v1/chat/completions", http.MethodPost, nil, []byte(`{"model":"m","stream":true}`), protocolOpenAI, protocolOpenAI, "m", false, 0)
	}()
	select {
	case result := <-done:
		if !errors.Is(result.Err, errRequestFirstTokenBudget) || result.ClientDisconnected {
			t.Fatalf("result=%+v", result)
		}
		if result.Committed && !strings.Contains(recorder.Body.String(), "request_timeout") {
			t.Fatal("metadata-only stream ended without a terminal budget error")
		}
	case <-time.After(2 * time.Second):
		upstream.CloseClientConnections()
		t.Fatal("budget left upstream running")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("upstream was not canceled")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	release, err := rt.acquireUpstreamConcurrency(ctx, target)
	if err != nil {
		t.Fatal("request budget leaked concurrency slot:", err)
	}
	release()
}

func TestRequestAttemptLimitSharedByConcurrentAttempts(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	finish := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocolOpenAI, false, 0)
	defer finish()
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if claimRequestAttempt(c.Request.Context(), 3) == nil {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := admitted.Load(); got != 3 {
		t.Fatalf("admitted=%d, want 3", got)
	}
	if c.Request.Context().Err() != nil {
		t.Fatal("launch limit canceled active attempts")
	}
}

func TestRequestTimingIgnoresErrorsAndBoundsIncompleteFrames(t *testing.T) {
	state := &forwardRequestTiming{started: time.Now()}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	w := &requestTimingWriter{ResponseWriter: c.Writer, state: state, kind: protocolOpenAI}
	_, _ = w.WriteString("data: {\"error\":{\"message\":\"overloaded\"}}\n\n")
	w.Flush()
	if first, _ := state.snapshot(); first != nil {
		t.Fatal("error frame counted as first content")
	}
	for i := 0; i < 32; i++ {
		_, _ = w.WriteString(strings.Repeat("x", 64<<10))
	}
	if len(w.line)+len(w.data) > maxResponsesPreCommitBytes {
		t.Fatal("unbounded prefix buffer")
	}
}

func TestRequestTimingRecognizesToolAndMultimodalOutput(t *testing.T) {
	for _, payload := range []string{
		`{"choices":[{"delta":{"tool_calls":[{"function":{"name":"search","arguments":""}}]}}]}`,
		`{"choices":[{"delta":{"content":[{"type":"text","text":"hello"}]}}]}`,
		`{"choices":[{"delta":{"audio":{"data":"YQ=="}}}]}`,
		`{"choices":[{"delta":{"content":"hello"}}],"usage":{"completion_tokens":1}}`,
	} {
		if !streamEventHasVisibleOutput(protocolOpenAI, "", []byte(payload)) {
			t.Fatalf("visible output missed: %s", payload)
		}
	}
}
