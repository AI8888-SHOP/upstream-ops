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

// This is transport keepalive, not synthetic model output. The raw writer is
// below requestTimingWriter: comments cannot mark TTFT, stop its budget, select
// a hedge winner or make an unvalidated upstream prefix committed.
type streamPreconnectWriter struct {
	gin.ResponseWriter
	mu              sync.Mutex
	header          http.Header // only the normal handler/winning attempt touches this map
	earlyHeader     http.Header // immutable; safe for the timer goroutine
	status          int
	preconnected    bool
	outputCommitted bool
	writeErr        error
	stop            chan struct{}
	done            chan struct{}
	stopped         bool
	ctx             context.Context
	cancel          context.CancelFunc
	kind            protocolKind
	requestID       string
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

func beginStreamPreconnect(c *gin.Context, group *storage.GatewayGroup, kind protocolKind, path string, stream, textRequest bool) func() {
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
	// An SSE comment is ignored by model parsers; never emit a fabricated
	// content delta, response.created, usage record or success terminal frame.
	payload := []byte(": upstream-ops keepalive\n\n")
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

func (w *streamPreconnectWriter) WriteString(value string) (int, error) {
	return w.Write([]byte(value))
}

func (w *streamPreconnectWriter) WriteHeaderNow() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prepareWriteLocked()
	w.ResponseWriter.WriteHeaderNow()
	w.outputCommitted = true
}

func (w *streamPreconnectWriter) Flush() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.writeErr != nil {
		return
	}
	w.prepareWriteLocked()
	w.ResponseWriter.Flush()
	w.outputCommitted = true
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
