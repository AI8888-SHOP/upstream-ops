package gateway

import (
	"bufio"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

var (
	errStreamGateLost        = errors.New("stream attempt lost before downstream commit")
	errStreamGateBufferLimit = errors.New("stream pre-commit buffer limit exceeded")
)

func streamWriterActuallyCommitted(c *gin.Context) bool {
	if c == nil || c.Writer == nil {
		return false
	}
	if gate, ok := c.Writer.(*streamPrefixGateWriter); ok {
		return gate.DownstreamCommitted()
	}
	return c.Writer.Written()
}

// streamPrefixGateWriter is an attempt-local Gin writer. It retains converted
// client-side SSE until validation accepts the prefix and the coordinator
// selects this attempt. Only the selected gate ever touches downstream.
type streamPrefixGateWriter struct {
	downstream gin.ResponseWriter
	validator  *streamResponseValidator

	mu        sync.Mutex
	header    http.Header
	status    int
	size      int
	buffer    []byte
	ready     chan validationResult
	decision  chan struct{}
	decided   bool
	winner    bool
	committed bool
	commitErr error
	finished  bool
	rejection validationResult
	lateMatch validationResult
	timer     *timeTimer
	virtualCache *virtualCacheSSETransformer
}

// timeTimer keeps the production implementation testable without exposing a
// concrete *time.Timer through the gate API.
type timeTimer struct {
	stop func() bool
}

func newStreamPrefixGateWriter(downstream gin.ResponseWriter, validator *streamResponseValidator) *streamPrefixGateWriter {
	if validator == nil {
		validator = (&responseValidator{}).NewStreamValidator("", "")
	}
	return &streamPrefixGateWriter{
		downstream: downstream,
		validator:  validator,
		header:     make(http.Header),
		status:     http.StatusOK,
		size:       -1,
		ready:      make(chan validationResult, 1),
		decision:   make(chan struct{}),
	}
}

func (g *streamPrefixGateWriter) Header() http.Header { return g.header }

func (g *streamPrefixGateWriter) WriteHeader(code int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.size >= 0 || g.decided && g.committed {
		return
	}
	if code > 0 {
		g.status = code
	}
}

func (g *streamPrefixGateWriter) WriteHeaderNow() {
	g.mu.Lock()
	if g.size < 0 {
		g.size = 0
	}
	g.mu.Unlock()
}

func (g *streamPrefixGateWriter) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	g.mu.Lock()
	if g.decided && !g.winner {
		g.mu.Unlock()
		return 0, errStreamGateLost
	}
	if g.committed {
		if result := g.validator.Consume(payload); result.IsRejected() && result.PostCommit && !g.lateMatch.IsRejected() {
			g.lateMatch = result
		}
		out := payload
		if g.virtualCache != nil {
			out = g.virtualCache.Transform(payload, false)
		}
		downstream := g.downstream
		g.mu.Unlock()
		if len(out) > 0 {
			if _, err := downstream.Write(out); err != nil {
				return 0, err
			}
		}
		return len(payload), nil
	}

	if g.size < 0 {
		g.size = 0
	}
	g.size += len(payload)
	responsesPrefixHeld := g.validator.holdsResponsesPrefix()
	bufferWouldOverflow := responsesPrefixHeld && len(g.buffer)+len(payload) > maxResponsesPreCommitBytes
	if bufferWouldOverflow {
		g.stopTimerLocked()
		g.mu.Unlock()
		return 0, errStreamGateBufferLimit
	}
	result := g.validator.Consume(payload)
	if g.validator.responsesPreCommitOverflow() {
		g.stopTimerLocked()
		g.mu.Unlock()
		return 0, errStreamGateBufferLimit
	}
	if result.IsRejected() && !result.PostCommit {
		g.rejection = result
		g.stopTimerLocked()
		g.signalReadyLocked(result)
		g.mu.Unlock()
		return 0, &responseRejectedError{Result: result}
	}
	if result.IsRejected() && result.PostCommit && !g.lateMatch.IsRejected() {
		g.lateMatch = result
	}
	g.buffer = append(g.buffer, payload...)
	if result.IsAccepted() || g.validator.prefixReady {
		g.stopTimerLocked()
		g.signalReadyLocked(acceptedValidation())
		decision := g.decision
		g.mu.Unlock()
		<-decision
		g.mu.Lock()
		winner, commitErr := g.winner, g.commitErr
		g.mu.Unlock()
		if !winner {
			return 0, errStreamGateLost
		}
		if commitErr != nil {
			return 0, commitErr
		}
		return len(payload), nil
	}
	g.startTimerLocked()
	g.mu.Unlock()
	return len(payload), nil
}

func (g *streamPrefixGateWriter) WriteString(value string) (int, error) {
	return g.Write([]byte(value))
}

func (g *streamPrefixGateWriter) Flush() {
	g.mu.Lock()
	committed := g.committed
	downstream := g.downstream
	g.mu.Unlock()
	if committed {
		downstream.Flush()
	}
}

func (g *streamPrefixGateWriter) Status() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.status
}

func (g *streamPrefixGateWriter) Size() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.size
}

func (g *streamPrefixGateWriter) Written() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.size >= 0
}

func (g *streamPrefixGateWriter) CloseNotify() <-chan bool {
	return g.downstream.CloseNotify()
}

func (g *streamPrefixGateWriter) Pusher() http.Pusher {
	return g.downstream.Pusher()
}

func (g *streamPrefixGateWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return g.downstream.Hijack()
}

func (g *streamPrefixGateWriter) Unwrap() http.ResponseWriter {
	return g.downstream
}

func (g *streamPrefixGateWriter) Ready() <-chan validationResult { return g.ready }

func (g *streamPrefixGateWriter) EnableVirtualCache(kind protocol.Kind) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.committed || g.virtualCache != nil {
		return
	}
	g.virtualCache = newVirtualCacheSSETransformer(kind)
}

func (g *streamPrefixGateWriter) Win() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.decided {
		if g.winner {
			return g.commitErr
		}
		return errStreamGateLost
	}
	if g.rejection.IsRejected() {
		return &responseRejectedError{Result: g.rejection}
	}
	g.decided = true
	g.winner = true
	g.stopTimerLocked()
	g.validator.Commit()
	for key, values := range g.header {
		copied := append([]string(nil), values...)
		g.downstream.Header()[key] = copied
	}
	g.downstream.WriteHeader(g.status)
	buffer := g.buffer
	if g.virtualCache != nil {
		buffer = g.virtualCache.Transform(buffer, g.finished)
	}
	if len(buffer) > 0 {
		_, g.commitErr = g.downstream.Write(buffer)
	}
	if g.commitErr == nil {
		g.downstream.Flush()
	}
	g.buffer = nil
	g.committed = g.commitErr == nil
	close(g.decision)
	return g.commitErr
}

func (g *streamPrefixGateWriter) Lose() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.decided {
		return
	}
	g.decided = true
	g.winner = false
	g.stopTimerLocked()
	g.buffer = nil
	close(g.decision)
}

func (g *streamPrefixGateWriter) Finish() validationResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.finished = true
	if g.committed && g.virtualCache != nil && g.commitErr == nil {
		if tail := g.virtualCache.Transform(nil, true); len(tail) > 0 {
			_, g.commitErr = g.downstream.Write(tail)
			if g.commitErr == nil {
				g.downstream.Flush()
			}
		}
	}
	if g.rejection.IsRejected() {
		return g.rejection
	}
	if g.committed {
		if late := g.validator.AuditPostCommit(); late.IsRejected() {
			g.lateMatch = late
		}
		if g.lateMatch.IsRejected() {
			return g.lateMatch
		}
		return acceptedValidation()
	}
	result := g.validator.Finalize()
	if result.IsRejected() {
		g.rejection = result
	}
	if late := g.validator.AuditPostCommit(); late.IsRejected() {
		g.lateMatch = late
	}
	g.stopTimerLocked()
	g.signalReadyLocked(result)
	return result
}

func (g *streamPrefixGateWriter) DownstreamCommitted() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.committed
}

func (g *streamPrefixGateWriter) CommitError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.commitErr
}

func (g *streamPrefixGateWriter) VirtualCacheApplied() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.virtualCache != nil && g.virtualCache.Applied()
}

func (g *streamPrefixGateWriter) Rejection() validationResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.rejection
}

func (g *streamPrefixGateWriter) LateMatch() validationResult {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lateMatch
}

func (g *streamPrefixGateWriter) signalReadyLocked(result validationResult) {
	select {
	case g.ready <- result:
	default:
	}
}

func (g *streamPrefixGateWriter) startTimerLocked() {
	if g.timer != nil || g.validator == nil || g.validator.FirstContentAt().IsZero() {
		return
	}
	delay := time.Until(g.validator.FirstContentAt().Add(g.validator.validator.PrefixTimeout()))
	if delay < 0 {
		delay = 0
	}
	timer := time.AfterFunc(delay, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.decided || g.committed || g.rejection.IsRejected() {
			return
		}
		result := g.validator.Ready(time.Now())
		if result.IsRejected() {
			g.rejection = result
		}
		if !result.IsPending() {
			g.signalReadyLocked(result)
		}
	})
	g.timer = &timeTimer{stop: timer.Stop}
}

func (g *streamPrefixGateWriter) stopTimerLocked() {
	if g.timer != nil {
		g.timer.stop()
		g.timer = nil
	}
}

var _ gin.ResponseWriter = (*streamPrefixGateWriter)(nil)
var _ http.Flusher = (*streamPrefixGateWriter)(nil)
