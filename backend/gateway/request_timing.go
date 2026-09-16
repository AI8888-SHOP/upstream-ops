package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

var errRequestFirstTokenBudget = errors.New("request first token budget exhausted")
var errRequestAttemptLimit = errors.New("request attempt limit exhausted")

type requestTimingKey struct{}

// One clock and one attempt counter per inbound request, shared by all hedges
// and retries. The timer is disarmed after visible output is flushed, so it
// never becomes a completion deadline for a long, healthy response.
type forwardRequestTiming struct {
	started  time.Time
	mu       sync.Mutex
	first    *int64
	onFirst  func()
	timer    *time.Timer
	expired  bool
	attempts atomic.Int32
}

func requestTiming(ctx context.Context) *forwardRequestTiming {
	state, _ := ctx.Value(requestTimingKey{}).(*forwardRequestTiming)
	return state
}

func beginForwardRequest(c *gin.Context, started time.Time, group *storage.GatewayGroup, kind protocolKind, stream bool, fallback time.Duration) func() {
	state := &forwardRequestTiming{started: started}
	ctx, cancel := context.WithCancelCause(c.Request.Context())
	c.Request = c.Request.WithContext(context.WithValue(ctx, requestTimingKey{}, state))
	if stream {
		budget := fallback
		if group.RequestFirstTokenTimeoutSec > 0 {
			budget = time.Duration(clampRequestFirstTokenTimeout(group.RequestFirstTokenTimeoutSec)) * time.Second
		}
		if budget > 0 {
			state.timer = time.AfterFunc(time.Until(started.Add(budget)), func() {
				state.mu.Lock()
				defer state.mu.Unlock()
				if state.first == nil {
					state.expired = true
					cancel(errRequestFirstTokenBudget)
				}
			})
		}
		c.Writer = &requestTimingWriter{ResponseWriter: c.Writer, state: state, kind: kind}
	}
	return func() {
		state.mu.Lock()
		if state.timer != nil {
			state.timer.Stop()
		}
		state.mu.Unlock()
		cancel(context.Canceled)
	}
}

func claimRequestAttempt(ctx context.Context, max int) error {
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	state := requestTiming(ctx)
	if state == nil || max <= 0 {
		return nil
	}
	if state.attempts.Add(1) > int32(clampRequestMaxAttempts(max)) {
		return errRequestAttemptLimit
	}
	return nil
}

func (s *forwardRequestTiming) flushed() {
	s.mu.Lock()
	var callback func()
	if s.first == nil && !s.expired {
		ms := time.Since(s.started).Milliseconds()
		s.first = &ms
		if s.timer != nil {
			s.timer.Stop()
		}
		callback = s.onFirst
		s.onFirst = nil
	}
	s.mu.Unlock()
	if callback != nil {
		callback()
	}
}

func (s *forwardRequestTiming) setFirstObserver(callback func()) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.first == nil && !s.expired {
		s.onFirst = callback
	}
}

func (s *forwardRequestTiming) snapshot() (*int64, *int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var first *int64
	if s.first != nil {
		value := *s.first
		first = &value
	}
	duration := time.Since(s.started).Milliseconds()
	return first, &duration
}

// Only the selected attempt writes to this wrapper. Scan until the first
// visible frame, then stop parsing for the remainder of the stream. The
// prefix is bounded and each byte is visited once, including split SSE lines.
type requestTimingWriter struct {
	gin.ResponseWriter
	state      *forwardRequestTiming
	kind       protocolKind
	line, data []byte
	event      string
	overflow   bool
	visible    bool
	failed     bool
	flushed    bool
}

func (w *requestTimingWriter) Write(payload []byte) (int, error) {
	n, err := w.ResponseWriter.Write(payload)
	if err != nil || n != len(payload) {
		w.failed = true
		if err == nil {
			err = io.ErrShortWrite
		}
	}
	if !w.visible && !w.failed && w.Status() >= 200 && w.Status() < 300 {
		w.observe(payload[:n])
	}
	return n, err
}

func (w *requestTimingWriter) WriteString(value string) (int, error) { return w.Write([]byte(value)) }
func (w *requestTimingWriter) Unwrap() http.ResponseWriter           { return w.ResponseWriter }

func (w *requestTimingWriter) Flush() {
	w.ResponseWriter.Flush()
	if w.visible && !w.failed && !w.flushed {
		w.flushed = true
		w.state.flushed()
	}
}

func (w *requestTimingWriter) observe(payload []byte) {
	for len(payload) > 0 && !w.visible {
		index := bytes.IndexByte(payload, '\n')
		length := len(payload)
		if index >= 0 {
			length = index
		}
		if len(w.line)+len(w.data)+length <= maxResponsesPreCommitBytes {
			w.line = append(w.line, payload[:length]...)
		} else {
			w.overflow = true
		}
		if index < 0 {
			return
		}
		payload = payload[index+1:]
		line := bytes.TrimSuffix(w.line, []byte{'\r'})
		if len(line) == 0 {
			if !w.overflow {
				w.visible = streamEventHasVisibleOutput(w.kind, w.event, w.data)
			}
			w.data = w.data[:0]
			w.event = ""
			w.overflow = false
		} else if bytes.HasPrefix(line, []byte("event:")) {
			w.event = strings.TrimSpace(string(line[len("event:"):]))
		} else if bytes.HasPrefix(line, []byte("data:")) {
			if len(w.data) > 0 {
				w.data = append(w.data, '\n')
			}
			w.data = append(w.data, bytes.TrimSpace(line[len("data:"):])...)
		}
		w.line = w.line[:0]
	}
	if w.visible {
		w.line = nil
		w.data = nil
	}
}

func streamEventHasVisibleOutput(kind protocolKind, event string, payload []byte) bool {
	if kind == protocol.KindOpenAIResponses {
		return responsesSSEEventStartsVisibleOutputBytes(event, payload, nil)
	}
	// Read only output-bearing fields. Role declarations, lifecycle events,
	// usage, errors and ping frames are deliberately excluded.
	if kind == protocolAnthropic {
		if delta, ok := partialJSONRootMember(payload, "delta"); ok {
			for _, key := range []string{"text", "thinking", "partial_json"} {
				if responsesJSONRootStringNonEmpty(delta, key) {
					return true
				}
			}
		}
		if block, ok := partialJSONRootMember(payload, "content_block"); ok {
			return responsesJSONRootStringNonEmpty(block, "text") || responsesJSONRootStringNonEmpty(block, "thinking") || responsesJSONRootStringNonEmpty(block, "name")
		}
		return false
	}
	choices, ok := partialJSONRootMember(payload, "choices")
	if !ok {
		return false
	}
	// Existing bounded partial-JSON extraction handles a large first delta
	// without decoding the full response or retaining later stream output.
	return chatChoicesHaveVisibleOutput(choices)
}

func chatChoicesHaveVisibleOutput(choices []byte) bool {
	var values []struct {
		Text  string `json:"text"`
		Delta struct {
			Content       json.RawMessage `json:"content"`
			Reasoning     string          `json:"reasoning_content"`
			ReasoningText string          `json:"reasoning"`
			Refusal       string          `json:"refusal"`
			Audio         struct {
				Data       string `json:"data"`
				Transcript string `json:"transcript"`
			} `json:"audio"`
			ToolCalls []struct {
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
			FunctionCall struct {
				Name      string `json:"name"`
				Arguments string `json:"arguments"`
			} `json:"function_call"`
		} `json:"delta"`
	}
	// partialJSONRootMember returns the suffix starting at the value, which
	// also contains the outer closing brace and any following root fields.
	// Decode one array from that suffix instead of requiring it to be the
	// entire JSON document.
	if json.NewDecoder(bytes.NewReader(choices)).Decode(&values) != nil {
		return false
	}
	for _, value := range values {
		d := value.Delta
		if value.Text != "" || chatContentHasVisibleOutput(d.Content) || d.Audio.Data != "" || d.Audio.Transcript != "" || d.Reasoning != "" || d.ReasoningText != "" || d.Refusal != "" || d.FunctionCall.Name != "" || d.FunctionCall.Arguments != "" {
			return true
		}
		for _, tool := range d.ToolCalls {
			if tool.Function.Name != "" || tool.Function.Arguments != "" {
				return true
			}
		}
	}
	return false
}

func chatContentHasVisibleOutput(content json.RawMessage) bool {
	var text string
	if json.Unmarshal(content, &text) == nil {
		return text != ""
	}
	var parts []struct {
		Text     string `json:"text"`
		ImageURL struct {
			URL string `json:"url"`
		} `json:"image_url"`
	}
	if json.Unmarshal(content, &parts) != nil {
		return false
	}
	for _, part := range parts {
		if part.Text != "" || part.ImageURL.URL != "" {
			return true
		}
	}
	return false
}
