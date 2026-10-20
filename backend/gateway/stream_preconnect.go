package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

const streamPreconnectWriteTimeout = 5 * time.Second

type streamPreconnectKey struct{}

// The raw writer is below requestTimingWriter: transport comments and the
// opt-in synthetic Responses preamble cannot mark our real TTFT, stop its budget,
// select a hedge winner or commit an unvalidated upstream attempt.
type streamPreconnectWriter struct {
	gin.ResponseWriter
	mu              sync.Mutex
	header          http.Header // only the normal handler/winning attempt touches this map
	earlyHeader     http.Header // immutable; safe for the timer goroutine
	status          int
	preconnected    bool
	outputCommitted bool
	writeErr        error
	protocolErr     error // adapter failure, never a downstream client disconnect
	stop            chan struct{}
	done            chan struct{}
	stopped         bool
	ctx             context.Context
	cancel          context.CancelFunc
	kind            protocolKind
	requestID       string
	responses       *preconnectResponsesStream // used only after the first early write
}

func streamPreconnectFor(c *gin.Context) *streamPreconnectWriter {
	if c == nil || c.Request == nil {
		return nil
	}
	w, _ := c.Request.Context().Value(streamPreconnectKey{}).(*streamPreconnectWriter)
	return w
}

// Gin's Copy reads its private writermem by value even when c.Writer has been
// replaced by a wrapper. Synchronize that snapshot with preconnect writes to
// the same raw writer; otherwise copying an attempt races its size/status.
func copyForwardContext(c *gin.Context) *gin.Context {
	if w := streamPreconnectFor(c); w != nil {
		w.mu.Lock()
		defer w.mu.Unlock()
	}
	return c.Copy()
}

func streamPreconnectDelay(minMS, maxMS int) time.Duration {
	if maxMS <= minMS {
		return time.Duration(minMS) * time.Millisecond
	}
	return time.Duration(minMS+rand.IntN(maxMS-minMS+1)) * time.Millisecond
}

func beginStreamPreconnect(c *gin.Context, group *storage.GatewayGroup, kind protocolKind, path string, stream, textRequest bool, requestBody ...[]byte) func() {
	noop := func() {}
	if c == nil || c.Request == nil || group == nil || !group.StreamPreconnectEnabled || !stream || !textRequest {
		return noop
	}
	if isWebSocketRequest(c.Request.Header) {
		return noop
	}
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/messages":
	default:
		return noop
	}
	minMS, maxMS := group.StreamPreconnectMinDelayMS, group.StreamPreconnectMaxDelayMS
	// Fail closed for malformed persisted configuration, even if it bypassed
	// the admin API. Never allocate unbounded timers or panic in rand.IntN.
	if minMS < 0 || maxMS < minMS || maxMS > 60000 {
		return noop
	}
	raw := c.Writer
	timing, hasTiming := raw.(*requestTimingWriter)
	if hasTiming {
		raw = timing.ResponseWriter
	}
	ctx, cancel := context.WithCancel(c.Request.Context())
	w := newStreamPreconnectWriter(ctx, raw, kind, raw.Header().Get(headerUpstreamOpsRequestID), cancel)
	if group.StreamPreconnectSub2apiEnabled && path == "/v1/responses" && protocol.NormalizeKind(kind) == protocol.KindOpenAIResponses && len(requestBody) == 1 && streamPreconnectStatelessEligible(requestBody[0]) {
		// On entropy failure retain the ordinary transport-only mode. Never
		// substitute a predictable response ID or alter stateful requests.
		w.responses, _ = newPreconnectResponsesStream(group.StreamPreconnectMultihopEnabled)
	}
	c.Request = c.Request.WithContext(context.WithValue(ctx, streamPreconnectKey{}, w))
	if hasTiming {
		timing.ResponseWriter = w
	} else {
		c.Writer = w
	}
	w.start(streamPreconnectDelay(minMS, maxMS), defaultStreamKeepalive)
	return func() {
		w.finish()
		cancel()
	}
}

func newStreamPreconnectWriter(ctx context.Context, raw gin.ResponseWriter, kind protocolKind, requestID string, cancel context.CancelFunc) *streamPreconnectWriter {
	header := raw.Header().Clone()
	early := header.Clone()
	setPreconnectHeaders(early)
	return &streamPreconnectWriter{ResponseWriter: raw, header: header, earlyHeader: early,
		status: http.StatusOK, stop: make(chan struct{}), done: make(chan struct{}),
		ctx: ctx, cancel: cancel, kind: kind, requestID: requestID}
}

func setPreconnectHeaders(header http.Header) {
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	header.Set("Connection", "keep-alive")
	header.Del("Content-Length")
	header.Del("Content-Encoding")
}

func (w *streamPreconnectWriter) start(delay, interval time.Duration) {
	go func() {
		defer close(w.done)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-w.ctx.Done():
				return
			case <-timer.C:
				if !w.keepalive() {
					return
				}
				timer.Reset(interval)
			}
		}
	}()
}

func (w *streamPreconnectWriter) stopLocked() {
	if !w.stopped {
		w.stopped = true
		close(w.stop)
	}
}

func (w *streamPreconnectWriter) finish() {
	w.mu.Lock()
	w.stopLocked()
	w.mu.Unlock()
	// Never wait for the timer while holding its write mutex. A slow socket
	// write is bounded by the heartbeat-only deadline below.
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.responses != nil && w.preconnected && !w.responses.done() && w.writeErr == nil && w.ctx.Err() == nil {
		// An incomplete synthetic lifecycle must not look like a successful
		// response. This bypasses real-output timing just like writeFinalError.
		w.failResponsesLocked(io.ErrUnexpectedEOF)
	}
}

func replaceResponseHeaders(dst, src http.Header) {
	for key := range dst {
		delete(dst, key)
	}
	for key, values := range src {
		dst[key] = append([]string(nil), values...)
	}
}

// Gin's Flush has no error return. Reach the actual net/http writer through
// Unwrap so its FlushError and write deadlines can detect a disconnected peer.
func preconnectController(raw http.ResponseWriter) *http.ResponseController {
	for {
		unwrapper, ok := raw.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return http.NewResponseController(raw)
		}
		raw = unwrapper.Unwrap()
	}
}

func (w *streamPreconnectWriter) keepalive() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped || w.ctx.Err() != nil || w.outputCommitted || w.writeErr != nil {
		return false
	}
	controller := preconnectController(w.ResponseWriter)
	if err := controller.SetWriteDeadline(time.Now().Add(streamPreconnectWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.failLocked(err)
		return false
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	if !w.ResponseWriter.Written() {
		replaceResponseHeaders(w.ResponseWriter.Header(), w.earlyHeader)
		w.ResponseWriter.WriteHeader(http.StatusOK)
	}
	// Ordinary mode remains transport-only. Responses compatibility adds a
	// coherent lifecycle; multihop mode adds a single synthetic summary space.
	// Both bypass our real TTFT/commit tracking and never invent model usage.
	payload := []byte(": upstream-ops keepalive\n\n")
	if w.responses != nil && !w.preconnected {
		payload = w.responses.preamble()
	}
	n, err := w.ResponseWriter.Write(payload)
	w.preconnected = true
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = controller.Flush()
	}
	if err != nil {
		w.failLocked(err)
		return false
	}
	return true
}

func (w *streamPreconnectWriter) failLocked(err error) {
	w.writeErr = err
	w.stopLocked()
	if w.cancel != nil {
		w.cancel()
	}
}

func (w *streamPreconnectWriter) Header() http.Header { return w.header }

func (w *streamPreconnectWriter) WriteHeader(status int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if status > 0 && !w.outputCommitted {
		w.status = status
	}
}

func (w *streamPreconnectWriter) prepareWriteLocked() {
	w.stopLocked()
	if !w.ResponseWriter.Written() {
		replaceResponseHeaders(w.ResponseWriter.Header(), w.header)
		w.ResponseWriter.WriteHeader(w.status)
	}
}

func (w *streamPreconnectWriter) Write(payload []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	if len(payload) == 0 {
		return 0, nil
	}
	w.prepareWriteLocked()
	if w.responses != nil && w.preconnected {
		err := w.responses.feed(payload, w.writeResponsesFrameLocked)
		if err != nil {
			if w.writeErr == nil {
				w.failResponsesLocked(err)
			}
			return 0, err
		}
		return len(payload), nil
	}
	n, err := w.ResponseWriter.Write(payload)
	if n > 0 {
		w.outputCommitted = true
	}
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.failLocked(err)
	}
	return n, err
}

func (w *streamPreconnectWriter) writeResponsesFrameLocked(frame []byte) error {
	n, err := w.ResponseWriter.Write(frame)
	if n > 0 {
		w.outputCommitted = true
	}
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.failLocked(err)
	}
	return err
}

// Fail closed on malformed/oversized transformed SSE instead of mixing raw
// upstream IDs and indexes into an already-started synthetic lifecycle.
func (w *streamPreconnectWriter) failResponsesLocked(cause error) {
	w.protocolErr = cause
	controller := preconnectController(w.ResponseWriter)
	if err := controller.SetWriteDeadline(time.Now().Add(streamPreconnectWriteTimeout)); err == nil || errors.Is(err, http.ErrNotSupported) {
		if !w.responses.done() {
			frame := w.responses.failure("upstream response stream could not be completed")
			if err := w.writeResponsesFrameLocked(frame); err == nil {
				_ = controller.Flush()
			}
		}
		_ = controller.SetWriteDeadline(time.Time{})
	}
	w.failLocked(cause)
}

// Check the selected output before either forwarding path records success or
// settles a winner. Attempt-local buffered/losing streams must not call this
// while another attempt is still running against the same downstream writer.
func finalizeStreamPreconnect(c *gin.Context, result *streamAttemptResult) {
	w := streamPreconnectFor(c)
	if w == nil || result == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.responses == nil || !w.preconnected {
		return
	}
	if w.protocolErr == nil && !w.responses.done() && w.writeErr == nil && w.ctx.Err() == nil &&
		(result.Committed || w.outputCommitted) && !result.ClientDisconnected && result.Err == nil && result.StreamErr == nil {
		w.failResponsesLocked(io.ErrUnexpectedEOF)
	}
	if w.protocolErr != nil {
		result.Err = w.protocolErr
		result.StreamErr = w.protocolErr
		result.ClientDisconnected = false
		result.DownstreamComplete = false
		result.Committed = w.outputCommitted
	}
}

func (w *streamPreconnectWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}

func (w *streamPreconnectWriter) WriteHeaderNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prepareWriteLocked()
	w.ResponseWriter.WriteHeaderNow()
	if w.responses == nil || !w.preconnected {
		w.outputCommitted = true
	}
}

func (w *streamPreconnectWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return
	}
	w.prepareWriteLocked()
	w.ResponseWriter.Flush()
	if w.responses == nil || !w.preconnected {
		w.outputCommitted = true
	}
}

func (w *streamPreconnectWriter) Status() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.ResponseWriter.Written() {
		return w.ResponseWriter.Status()
	}
	return w.status
}

func (w *streamPreconnectWriter) Size() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ResponseWriter.Size()
}

func (w *streamPreconnectWriter) Written() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.ResponseWriter.Written()
}

func (w *streamPreconnectWriter) DownstreamCommitted() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.outputCommitted
}

func (w *streamPreconnectWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Stop the timer and choose JSON vs SSE under ONE lock. A separate "already
// sent?" check followed by c.JSON can race the first heartbeat and corrupt SSE.
func (w *streamPreconnectWriter) writeFinalError(status int, body []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.stopLocked()
	if w.writeErr != nil {
		return w.writeErr
	}
	controller := preconnectController(w.ResponseWriter)
	if err := controller.SetWriteDeadline(time.Now().Add(streamPreconnectWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.failLocked(err)
		return err
	}
	defer func() { _ = controller.SetWriteDeadline(time.Time{}) }()
	payload := body
	if !w.preconnected {
		w.status = status
		w.header.Set("Content-Type", "application/json; charset=utf-8")
		w.header.Del("Content-Length")
		w.prepareWriteLocked()
	} else {
		message := extractResponseErrorMessage(body, nil)
		if strings.TrimSpace(message) == "" {
			message = http.StatusText(status)
		}
		type errorIdentity struct {
			Type string          `json:"type"`
			Code json.RawMessage `json:"code"`
		}
		identityPayloads := [][]byte{body}
		if looksLikeSSEBody(body) {
			identityPayloads = sseDataPayloads(body)
		}
		var identity errorIdentity
		for _, candidate := range identityPayloads {
			var envelope struct {
				Error    errorIdentity `json:"error"`
				Response struct {
					Error errorIdentity `json:"error"`
				} `json:"response"`
			}
			_ = json.Unmarshal(candidate, &envelope)
			identity = envelope.Error
			if strings.TrimSpace(identity.Type) == "" && len(identity.Code) == 0 {
				identity = envelope.Response.Error
			}
			if strings.TrimSpace(identity.Type) != "" || len(identity.Code) != 0 {
				break
			}
		}
		errType := strings.TrimSpace(identity.Type)
		if errType == "" {
			errType = "upstream_error"
		}
		var errCode any
		if len(identity.Code) > 0 {
			errCode = identity.Code
		}
		payload = streamTerminalErrorFrameWithCode(protocol.NormalizeKind(w.kind), errType, errCode, message, w.requestID)
	}
	if w.responses != nil && w.preconnected {
		// Discard any incomplete frame before appending the authoritative
		// gateway failure. Legacy mode leaves empty reasoning open to preserve
		// downstream failover; multihop closes only its already-delivered space.
		w.responses.discardPending()
		err := w.responses.feed(payload, w.writeResponsesFrameLocked)
		if err == nil {
			err = controller.Flush()
		}
		if err != nil {
			w.failResponsesLocked(err)
		}
		return err
	}
	n, err := w.ResponseWriter.Write(payload)
	if n > 0 {
		w.outputCommitted = true
	}
	if err == nil && n != len(payload) {
		err = io.ErrShortWrite
	}
	if err == nil && w.preconnected {
		err = controller.Flush()
	}
	if err != nil {
		w.failLocked(err)
	}
	return err
}

var _ gin.ResponseWriter = (*streamPreconnectWriter)(nil)
