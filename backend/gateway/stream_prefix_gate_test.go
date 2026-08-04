package gateway

import (
	"net/http/httptest"
	"sync"
	"testing"
	"time"

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

func TestCleanupCoordinatedStreamLosersWaitsForResult(t *testing.T) {
	originalCleanup := hedgeCleanupTimeout
	hedgeCleanupTimeout = time.Second
	t.Cleanup(func() { hedgeCleanupTimeout = originalCleanup })
	loser := &coordinatedForwardAttempt{
		Info: hedgeAttemptInfo{Number: 2},
		StartedAt: time.Now(),
		streamDone: make(chan streamAttemptResult, 1),
	}
	var states sync.Map
	states.Store(2, loser)
	go func() {
		time.Sleep(20 * time.Millisecond)
		loser.streamDone <- streamAttemptResult{Status: 499, Err: errStreamGateLost}
	}()
	runResult := hedgeRunResult[*coordinatedForwardAttempt]{
		Winner: &hedgeAttemptResult[*coordinatedForwardAttempt]{Info: hedgeAttemptInfo{Number: 1}},
	}
	(&Runtime{}).cleanupCoordinatedStreamLosers(runResult, &states)
	loser.streamMu.Lock()
	ready, status := loser.streamReady, loser.streamResult.Status
	loser.streamMu.Unlock()
	if !ready || status != 499 || loser.Status != 499 {
		t.Fatalf("loser ready=%v stream_status=%d attempt_status=%d", ready, status, loser.Status)
	}
}
