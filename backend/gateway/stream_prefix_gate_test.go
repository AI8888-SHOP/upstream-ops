package gateway

import (
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

func TestStreamPrefixGateDoesNotWriteBeforeWinnerSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 4, time.Second, responseRuleSpec{
		ID: 1, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_chat", "gpt-test"))

	written := make(chan error, 1)
	go func() {
		_, err := gate.Write([]byte("safe"))
		written <- err
	}()
	select {
	case decision := <-gate.Ready():
		if !decision.IsAccepted() {
			t.Fatalf("decision=%+v", decision)
		}
	case <-time.After(time.Second):
		t.Fatal("gate did not announce a valid prefix")
	}
	if recorder.Body.Len() != 0 {
		t.Fatalf("body reached downstream before selection: %q", recorder.Body.String())
	}
	if err := gate.Win(); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if got := recorder.Body.String(); got != "safe" {
		t.Fatalf("body=%q", got)
	}
}

func TestStreamPrefixGateRewritesWinnerUsageForDownstreamBilling(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	gate := newStreamPrefixGateWriter(context.Writer, nil)
	content := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"ok\"}}]}\n\n")
	written := make(chan error, 1)
	go func() {
		_, err := gate.Write(content)
		written <- err
	}()
	select {
	case result := <-gate.Ready():
		if !result.IsAccepted() {
			t.Fatalf("prefix result=%+v, want accepted", result)
		}
	case <-time.After(time.Second):
		t.Fatal("gate did not become ready")
	}
	gate.EnableVirtualCache(protocol.KindOpenAIChat)
	if err := gate.Win(); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	usage := []byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":4,\"prompt_tokens_details\":{\"cached_tokens\":20},\"cache_creation_input_tokens\":10}}\n\n")
	if _, err := gate.Write(usage); err != nil {
		t.Fatal(err)
	}
	gate.Finish()

	tokens := NormalizeUsageBuckets(ParseOpenAISSEUsage(recorder.Body.Bytes()), protocol.KindOpenAIChat)
	if tokens.InputTokens != 0 || tokens.CacheReadTokens != 90 || tokens.CacheCreationTokens != 10 {
		t.Fatalf("downstream stream usage=%+v, want fresh=0 cache_read=90 cache_creation=10; body=%s", tokens, recorder.Body.String())
	}
	if !gate.VirtualCacheApplied() {
		t.Fatal("winner gate did not record the virtual-cache rewrite")
	}
}

func TestStreamPrefixGateRejectsWithoutDownstreamCommit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 4096, time.Second, responseRuleSpec{
		ID: 2, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_chat", "gpt-test"))
	if _, err := gate.Write([]byte("data: blocked\n\n")); err == nil {
		t.Fatal("Write() accepted a rejected prefix")
	}
	decision := <-gate.Ready()
	if !decision.IsRejected() || decision.RuleID != 2 {
		t.Fatalf("decision=%+v", decision)
	}
	if gate.DownstreamCommitted() || recorder.Body.Len() != 0 {
		t.Fatalf("rejected prefix was committed: %q", recorder.Body.String())
	}
}

func TestStreamPrefixGateTimeoutMakesShortPrefixSelectable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 4096, 10*time.Millisecond, responseRuleSpec{
		ID: 3, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_chat", "gpt-test"))
	if _, err := gate.Write([]byte("data: safe\n\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case decision := <-gate.Ready():
		if !decision.IsAccepted() {
			t.Fatalf("decision=%+v", decision)
		}
	case <-time.After(time.Second):
		t.Fatal("prefix timeout did not make the gate ready")
	}
}

func TestStreamPrefixGateResponsesMetadataTimeoutKeepsFailureRetryable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 4096, 10*time.Millisecond, responseRuleSpec{
		ID: 5, Name: "overloaded", Enabled: true,
		Pattern: `(?i)servers are currently overloaded`, Target: "error_message",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_responses", "gpt-test"))
	created := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n")
	if _, err := gate.Write(created); err != nil {
		t.Fatal(err)
	}
	select {
	case decision := <-gate.Ready():
		t.Fatalf("metadata-only prefix became ready: %+v", decision)
	case <-time.After(30 * time.Millisecond):
	}

	failed := []byte("event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}}\n\n")
	if _, err := gate.Write(failed); err == nil {
		t.Fatal("response.failed was not rejected")
	}
	decision := <-gate.Ready()
	if !decision.IsRejected() || decision.PostCommit || decision.RuleID != 5 {
		t.Fatalf("decision=%+v, want pre-commit rejection", decision)
	}
	if gate.DownstreamCommitted() || recorder.Body.Len() != 0 {
		t.Fatalf("failed stream reached downstream: %q", recorder.Body.String())
	}
}

func TestStreamPrefixGateResponsesPreCommitBufferHasHardLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 128, time.Hour, responseRuleSpec{
		ID: 6, Name: "blocked", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_responses", "gpt-test"))
	frame := []byte("event: response.created\ndata: {\"type\":\"response.created\",\"metadata\":\"" + strings.Repeat("x", maxResponsesClassifyBytes) + "\"}\n\n")
	for {
		gate.mu.Lock()
		fits := len(gate.buffer)+len(frame) <= maxResponsesPreCommitBytes
		gate.mu.Unlock()
		if !fits {
			break
		}
		if _, err := gate.Write(frame); err != nil {
			t.Fatal(err)
		}
	}
	gate.mu.Lock()
	buffered := len(gate.buffer)
	gate.mu.Unlock()
	if buffered > maxResponsesPreCommitBytes {
		t.Fatalf("pre-commit buffer=%d, limit=%d", buffered, maxResponsesPreCommitBytes)
	}

	lastWrite := make(chan error, 1)
	go func() {
		_, err := gate.Write(frame)
		lastWrite <- err
	}()
	if err := <-lastWrite; !errors.Is(err, errStreamGateBufferLimit) {
		t.Fatalf("buffer-limit error=%v, want %v", err, errStreamGateBufferLimit)
	}
	gate.mu.Lock()
	buffered = len(gate.buffer)
	gate.mu.Unlock()
	if buffered > maxResponsesPreCommitBytes {
		t.Fatalf("released pre-commit buffer=%d, limit=%d", buffered, maxResponsesPreCommitBytes)
	}
	if gate.DownstreamCommitted() || recorder.Body.Len() != 0 {
		t.Fatalf("overflowed prefix reached downstream: %q", recorder.Body.String())
	}
}

func TestStreamPrefixGatePostCommitAuditDoesNotRejectWrite(t *testing.T) {
	gin.SetMode(gin.TestMode)
	recorder := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	context, _ := gin.CreateTestContext(recorder)
	validator := mustResponseValidator(t, 4, time.Second, responseRuleSpec{
		ID: 4, Name: "late", Enabled: true, Pattern: `blocked`, Target: "raw_body",
	})
	gate := newStreamPrefixGateWriter(context.Writer, validator.NewStreamValidator("openai_chat", "gpt-test"))
	written := make(chan error, 1)
	go func() {
		_, err := gate.Write([]byte("safe"))
		written <- err
	}()
	select {
	case decision := <-gate.Ready():
		if !decision.IsAccepted() {
			t.Fatalf("decision=%+v", decision)
		}
	case <-time.After(time.Second):
		t.Fatal("prefix was not accepted")
	}
	if err := gate.Win(); err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Write([]byte(" data: blocked\n\n")); err != nil {
		t.Fatalf("post-commit write unexpectedly failed: %v", err)
	}
	if result := gate.Finish(); !result.IsRejected() || !result.PostCommit || result.RuleID != 4 {
		t.Fatalf("audit result=%+v", result)
	}
	if got := recorder.Body.String(); got != "safe data: blocked\n\n" {
		t.Fatalf("downstream body=%q", got)
	}
}

func TestCleanupCoordinatedStreamLosersDoesNotWaitForResult(t *testing.T) {
	loser := &coordinatedForwardAttempt{
		Info:       hedgeAttemptInfo{Number: 2},
		StartedAt:  time.Now(),
		streamDone: make(chan streamAttemptResult, 1),
	}
	var states sync.Map
	states.Store(2, loser)
	release := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		<-release
		loser.streamDone <- streamAttemptResult{Status: 499, Err: errStreamGateLost}
		close(sent)
	}()
	runResult := hedgeRunResult[*coordinatedForwardAttempt]{
		Winner: &hedgeAttemptResult[*coordinatedForwardAttempt]{Info: hedgeAttemptInfo{Number: 1}},
	}
	started := time.Now()
	(&Runtime{}).cleanupCoordinatedStreamLosers(runResult, &states)
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("loser cleanup blocked for %s", elapsed)
	}
	loser.streamMu.Lock()
	ready, status := loser.streamReady, loser.streamResult.Status
	loser.streamMu.Unlock()
	if ready || status != 0 || loser.Status != 0 {
		t.Fatalf("loser completed during non-blocking cleanup: ready=%v stream_status=%d attempt_status=%d", ready, status, loser.Status)
	}
	close(release)
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("loser result was not published")
	}
	(&Runtime{}).cleanupCoordinatedStreamLosers(runResult, &states)
	loser.streamMu.Lock()
	ready, status = loser.streamReady, loser.streamResult.Status
	loser.streamMu.Unlock()
	if !ready || status != 499 || loser.Status != 499 {
		t.Fatalf("ready loser was not collected: ready=%v stream_status=%d attempt_status=%d", ready, status, loser.Status)
	}
}
