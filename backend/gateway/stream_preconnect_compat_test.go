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
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func preconnectCompatTestWriter(t *testing.T) (*gin.Context, *httptest.ResponseRecorder, *streamPreconnectWriter) {
	t.Helper()
	c, recorder, writer := preconnectTestWriter(t, protocol.KindOpenAIResponses)
	responses, err := newPreconnectResponsesStream()
	if err != nil {
		t.Fatal(err)
	}
	writer.responses = responses
	return c, recorder, writer
}

func TestStreamPreconnectCompatPreambleDoesNotCommitOrRecordRealTTFT(t *testing.T) {
	c, recorder, writer := preconnectCompatTestWriter(t)
	state := &forwardRequestTiming{started: time.Now().Add(-100 * time.Millisecond)}
	c.Writer = &requestTimingWriter{ResponseWriter: writer, state: state, kind: protocol.KindOpenAIResponses}
	if !writer.keepalive() || !writer.keepalive() {
		t.Fatal("compatibility prefix or subsequent heartbeat failed")
	}
	if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("headers = %d %v", recorder.Code, recorder.Header())
	}
	events := parsePreconnectResponsesTestEvents(t, recorder.Body.Bytes())
	wantNames := []string{"response.created", "response.in_progress", "response.output_item.added"}
	if len(events) != len(wantNames) {
		t.Fatalf("prefix repeated: %s", recorder.Body.String())
	}
	for i, event := range events {
		if event.name != wantNames[i] {
			t.Fatalf("prefix event %d = %s", i, event.name)
		}
		if streamEventHasVisibleOutput(protocol.KindOpenAIResponses, event.name, event.data) {
			t.Fatalf("prefix contains real output: %s", event.data)
		}
	}
	item := preconnectResponsesTestObject(t, events[2].root["item"])
	if preconnectResponsesTestString(t, item["type"]) != "reasoning" || string(item["summary"]) != "[]" {
		t.Fatalf("prefix is not empty reasoning: %s", events[2].data)
	}
	if !strings.HasSuffix(recorder.Body.String(), ": upstream-ops keepalive\n\n") {
		t.Fatalf("subsequent keepalive was not a comment: %s", recorder.Body.String())
	}
	if streamWriterActuallyCommitted(c) {
		t.Fatal("synthetic prefix committed an upstream winner")
	}
	if first, _ := state.snapshot(); first != nil {
		t.Fatalf("synthetic prefix manufactured internal TTFT: %v", *first)
	}
	actual := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_real\",\"output_index\":0,\"content_index\":0,\"delta\":\"answer\"}\n\n")
	if n, err := c.Writer.Write(actual); err != nil || n != len(actual) {
		t.Fatalf("real delta write = %d, %v", n, err)
	}
	if first, _ := state.snapshot(); first != nil {
		t.Fatal("real TTFT recorded before downstream flush")
	}
	c.Writer.Flush()
	if !streamWriterActuallyCommitted(c) || writer.keepalive() {
		t.Fatal("real output did not commit and stop early keepalive")
	}
	if first, _ := state.snapshot(); first == nil || *first < 100 {
		t.Fatalf("real TTFT = %v", first)
	}
	events = parsePreconnectResponsesTestEvents(t, recorder.Body.Bytes())
	foundDelta := false
	for _, event := range events {
		if event.name != "response.output_text.delta" {
			continue
		}
		foundDelta = true
		if preconnectResponsesTestInt(t, event.root["output_index"]) != 1 || preconnectResponsesTestString(t, event.root["delta"]) != "answer" {
			t.Fatalf("real delta lost data or index shift: %s", event.data)
		}
	}
	if !foundDelta {
		t.Fatal("real delta was not forwarded")
	}
	assertPreconnectResponsesTestSequence(t, events)
	// The test intentionally stops after the first delta, not a response
	// terminal; cancellation avoids pretending that this fixture completed.
	writer.cancel()
}

func TestStreamPreconnectCompatKeepsRealFirstTokenBudget(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	finishTiming := beginForwardRequest(c, time.Now(), &storage.GatewayGroup{}, protocol.KindOpenAIResponses, true, 50*time.Millisecond)
	defer finishTiming()
	group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
		StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true,
		StreamPreconnectMinDelayMS: 60000, StreamPreconnectMaxDelayMS: 60000,
	}}
	finish := beginStreamPreconnect(c, group, protocol.KindOpenAIResponses, "/v1/responses", true, true, []byte(`{"store":false}`))
	defer finish()
	writer := streamPreconnectFor(c)
	if writer == nil || writer.responses == nil || !writer.keepalive() {
		t.Fatal("compatibility preamble not enabled")
	}
	select {
	case <-c.Request.Context().Done():
		if !errors.Is(context.Cause(c.Request.Context()), errRequestFirstTokenBudget) {
			t.Fatalf("budget cause = %v", context.Cause(c.Request.Context()))
		}
	case <-time.After(time.Second):
		t.Fatal("synthetic prefix disabled real first-token budget")
	}
	if first, _ := requestTiming(c.Request.Context()).snapshot(); first != nil {
		t.Fatalf("synthetic prefix recorded real TTFT: %v", *first)
	}
	if streamWriterActuallyCommitted(c) {
		t.Fatal("synthetic prefix committed before real output")
	}
}

func TestStreamPreconnectCompatEligibility(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		path                 string
		kind                 protocolKind
		body                 string
		enabled, compat      bool
		stream, text         bool
		wantWriter, wantMode bool
		websocket, omitBody  bool
	}{
		{name: "stateless Responses", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true, wantMode: true},
		{name: "compatibility disabled", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, stream: true, text: true, wantWriter: true},
		{name: "parent disabled", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, compat: true, stream: true, text: true},
		{name: "nonstream", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, text: true},
		{name: "media request", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, stream: true},
		{name: "image endpoint", path: "/v1/images/generations", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true},
		{name: "chat path", path: "/v1/chat/completions", kind: protocol.KindOpenAIChat, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "messages path", path: "/v1/messages", kind: protocol.KindAnthropic, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "Responses kind on chat path", path: "/v1/chat/completions", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "wrong protocol", path: "/v1/responses", kind: protocol.KindOpenAIChat, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "inherited store", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "stateful store", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":true}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "previous response", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false,"previous_response_id":"resp_prior"}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "conversation", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false,"conversation":"conv_prior"}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "background", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false,"background":true}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "missing body argument", path: "/v1/responses", kind: protocol.KindOpenAIResponses, enabled: true, compat: true, stream: true, text: true, wantWriter: true, omitBody: true},
		{name: "malformed body", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false,}`, enabled: true, compat: true, stream: true, text: true, wantWriter: true},
		{name: "websocket", path: "/v1/responses", kind: protocol.KindOpenAIResponses, body: `{"store":false}`, enabled: true, compat: true, stream: true, text: true, websocket: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, tc.path, nil)
			if tc.websocket {
				c.Request.Header.Set("Connection", "Upgrade")
				c.Request.Header.Set("Upgrade", "websocket")
			}
			group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
				StreamPreconnectEnabled: tc.enabled, StreamPreconnectSub2apiEnabled: tc.compat,
				StreamPreconnectMinDelayMS: 60000, StreamPreconnectMaxDelayMS: 60000,
			}}
			var bodies [][]byte
			if !tc.omitBody {
				bodies = append(bodies, []byte(tc.body))
			}
			finish := beginStreamPreconnect(c, group, tc.kind, tc.path, tc.stream, tc.text, bodies...)
			defer finish()
			writer := streamPreconnectFor(c)
			if (writer != nil) != tc.wantWriter {
				t.Fatalf("writer = %v, want present=%v", writer, tc.wantWriter)
			}
			if writer != nil && (writer.responses != nil) != tc.wantMode {
				t.Fatalf("compatibility enabled=%v, want %v", writer.responses != nil, tc.wantMode)
			}
			if recorder.Body.Len() != 0 {
				t.Fatalf("configured delay emitted immediately: %s", recorder.Body.String())
			}
		})
	}
}

func TestStreamPreconnectCompatFastUpstreamIsByteIdentical(t *testing.T) {
	_, recorder, writer := preconnectCompatTestWriter(t)
	wire := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_actual\"}}\n\nevent: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"delta\":\"answer\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_actual\",\"status\":\"completed\",\"output\":[]}}\n\n")
	writer.Header().Set("Content-Type", "text/event-stream")
	if n, err := writer.Write(wire); err != nil || n != len(wire) {
		t.Fatalf("fast upstream write = %d, %v", n, err)
	}
	writer.Flush()
	writer.finish()
	if !bytes.Equal(recorder.Body.Bytes(), wire) || writer.preconnected || writer.keepalive() {
		t.Fatalf("fast upstream was prefixed or rewritten: %s", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "uops_preconnect_") {
		t.Fatal("generated IDs appeared before the compatibility timer fired")
	}
}

func TestStreamPreconnectCompatFinalErrorPreservesIdentity(t *testing.T) {
	_, recorder, writer := preconnectCompatTestWriter(t)
	if !writer.keepalive() {
		t.Fatal("prefix failed")
	}
	prefix := parsePreconnectResponsesTestEvents(t, recorder.Body.Bytes())
	response := preconnectResponsesTestObject(t, prefix[0].root["response"])
	responseID := preconnectResponsesTestString(t, response["id"])
	body := []byte(`{"error":{"type":"request_timeout","code":"first_token_budget","message":"budget exhausted"}}`)
	if err := writer.writeFinalError(http.StatusGatewayTimeout, body); err != nil {
		t.Fatal(err)
	}
	writer.finish()
	events := parsePreconnectResponsesTestEvents(t, recorder.Body.Bytes())
	if len(events) != 4 || events[3].name != "response.failed" || recorder.Code != http.StatusOK {
		t.Fatalf("failure lifecycle = %d %s", recorder.Code, recorder.Body.String())
	}
	failed := preconnectResponsesTestObject(t, events[3].root["response"])
	identity := preconnectResponsesTestObject(t, failed["error"])
	if preconnectResponsesTestString(t, failed["id"]) != responseID || preconnectResponsesTestString(t, identity["type"]) != "request_timeout" || preconnectResponsesTestString(t, identity["code"]) != "first_token_budget" || preconnectResponsesTestString(t, identity["message"]) != "budget exhausted" {
		t.Fatalf("failure lost identity: %s", events[3].data)
	}
	if !strings.Contains(string(events[3].data), "preconnect-test") {
		t.Fatalf("failure lost request ID: %s", events[3].data)
	}
	if strings.Contains(recorder.Body.String(), "response.output_item.done") || strings.Contains(recorder.Body.String(), "response.completed") {
		t.Fatalf("failure emitted synthetic progress or success: %s", recorder.Body.String())
	}
	assertPreconnectResponsesTestSequence(t, events)
}

func TestStreamPreconnectCompatIncompleteFrameFailsOnFinish(t *testing.T) {
	_, recorder, writer := preconnectCompatTestWriter(t)
	if !writer.keepalive() {
		t.Fatal("prefix failed")
	}
	partial := []byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":")
	if n, err := writer.Write(partial); err != nil || n != len(partial) {
		t.Fatalf("partial write = %d, %v", n, err)
	}
	if writer.DownstreamCommitted() {
		t.Fatal("incomplete frame committed downstream output")
	}
	prefixBytes := recorder.Body.Len()
	writer.WriteHeaderNow()
	writer.Flush()
	if writer.DownstreamCommitted() || recorder.Body.Len() != prefixBytes {
		t.Fatal("flushing an incomplete frame exposed or committed partial output")
	}
	writer.finish()
	events := parsePreconnectResponsesTestEvents(t, recorder.Body.Bytes())
	if len(events) != 4 || events[3].name != "response.failed" || !errors.Is(writer.writeErr, io.ErrUnexpectedEOF) {
		t.Fatalf("incomplete response did not fail closed: error=%v body=%s", writer.writeErr, recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "response.output_item.done") || strings.Contains(recorder.Body.String(), "response.completed") {
		t.Fatalf("incomplete response manufactured success: %s", recorder.Body.String())
	}
}

func TestStreamPreconnectCompatCancellationDoesNotWriteFailure(t *testing.T) {
	_, recorder, writer := preconnectCompatTestWriter(t)
	if !writer.keepalive() {
		t.Fatal("prefix failed")
	}
	prefix := append([]byte(nil), recorder.Body.Bytes()...)
	writer.cancel()
	writer.finish()
	if !bytes.Equal(recorder.Body.Bytes(), prefix) || writer.keepalive() {
		t.Fatalf("canceled request performed another write: %s", recorder.Body.String())
	}
}

func TestStreamPreconnectCompatShortPreambleCancelsRequest(t *testing.T) {
	_, _, writer := preconnectCompatTestWriter(t)
	writer.ResponseWriter = &preconnectShortWriter{ResponseWriter: writer.ResponseWriter, short: true}
	if writer.keepalive() || !errors.Is(writer.writeErr, io.ErrShortWrite) || writer.ctx.Err() == nil || writer.DownstreamCommitted() {
		t.Fatalf("short preamble was accepted: error=%v canceled=%v committed=%v", writer.writeErr, writer.ctx.Err(), writer.DownstreamCommitted())
	}
	if writer.keepalive() {
		t.Fatal("keepalive continued after a short write")
	}
}
