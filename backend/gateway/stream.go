// 流式转发结果类型与 Service→Runtime 委托入口。
package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	defaultStreamKeepalive   = 15 * time.Second
	defaultStreamIdleTimeout = 120 * time.Second
	maxSSELineSize           = 8 << 20 // 8 MiB
	// Keep the first useful event and terminal frames immediate, while
	// coalescing ordinary token frames in a small bounded window. This avoids
	// a syscall and scheduler wakeup for every token on long-lived streams.
	streamFlushInterval = 8 * time.Millisecond
	streamFlushBytes    = 16 << 10
)

type streamFlushState struct {
	bytes     int
	lastFlush time.Time
	timer     *time.Timer
	armed     bool
}

func (s *streamFlushState) add(frames [][]byte) {
	if s == nil {
		return
	}
	for _, frame := range frames {
		s.bytes += len(frame)
	}
}

func (s *streamFlushState) shouldFlush(now time.Time) bool {
	if s == nil || s.bytes <= 0 {
		return false
	}
	if s.bytes >= streamFlushBytes || s.lastFlush.IsZero() {
		return true
	}
	return now.Sub(s.lastFlush) >= streamFlushInterval
}

func (s *streamFlushState) channel() <-chan time.Time {
	if s == nil || s.bytes <= 0 {
		return nil
	}
	delay := streamFlushInterval
	if !s.lastFlush.IsZero() {
		delay = time.Until(s.lastFlush.Add(streamFlushInterval))
		if delay < 0 {
			delay = 0
		}
	}
	if s.timer == nil {
		s.timer = time.NewTimer(delay)
	} else if !s.armed {
		s.timer.Reset(delay)
	}
	s.armed = true
	return s.timer.C
}

func (s *streamFlushState) reset(now time.Time) {
	if s == nil {
		return
	}
	s.bytes = 0
	s.lastFlush = now
	if s.timer != nil {
		if !s.timer.Stop() {
			select {
			case <-s.timer.C:
			default:
			}
		}
		s.armed = false
	}
}

func (s *streamFlushState) stop() {
	if s == nil || s.timer == nil {
		return
	}
	if !s.timer.Stop() {
		select {
		case <-s.timer.C:
		default:
		}
	}
	s.armed = false
	s.timer = nil
}

// streamAttemptResult 单次流式转发结果。
// Committed=true 表示已向客户端写出有效 SSE，禁止 retry/failover。
// DownstreamComplete=true 表示已成功向客户端写出流式终端帧（[DONE] / message_stop 等）；
// 此后客户端关连接属于正常收尾，不应记 error_type=client。
type streamAttemptResult struct {
	Status             int
	Headers            http.Header
	Body               []byte
	FirstTokenMS       *int64
	Tokens             UsageTokens
	Committed          bool
	ClientDisconnected bool
	DownstreamComplete bool
	StreamErr          error
	Err                error
	// ValidationRejection is a pre-commit response-rule match. It is a direct
	// route switch signal and must not consume same-route retry budget.
	ValidationRejection validationResult
	// PostCommitValidation is informational audit data; it can never trigger a
	// route switch after bytes were exposed to the client.
	PostCommitValidation validationResult
}

type preserveAttemptCancelContextKey struct{}

func withPreservedAttemptCancel(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, preserveAttemptCancelContextKey{}, true)
}

func preserveAttemptCancel(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	value, _ := ctx.Value(preserveAttemptCancelContextKey{}).(bool)
	return value
}

// buildUpstreamHTTPRequest 构建上游 HTTP 请求（forwardOnce / forwardStream 共用）。
func (s *Service) buildUpstreamHTTPRequest(ctx context.Context, target *upstreamTarget, path string, method string, inHeader http.Header, body []byte, kind protocolKind, stream bool) (*http.Request, error) {
	return s.runtime().buildUpstreamHTTPRequest(ctx, target, path, method, inHeader, body, kind, stream)
}

func (s *Service) forwardStream(ctx context.Context, c *gin.Context, target *upstreamTarget, path string, method string, inHeader http.Header, body []byte, inboundKind protocolKind, upstreamKind protocolKind, model string, converted bool, firstTokenTimeout time.Duration) streamAttemptResult {
	return s.runtime().forwardStream(ctx, c, target, path, method, inHeader, body, inboundKind, upstreamKind, model, converted, firstTokenTimeout)
}

func (s *Service) forwardStreamBuffered(c *gin.Context, resp *http.Response, start time.Time, firstTokenTimeout time.Duration, inbound, upstream protocolKind, model string, converted bool, headers http.Header, status int) streamAttemptResult {
	return s.runtime().forwardStreamBuffered(c, resp, start, firstTokenTimeout, inbound, upstream, model, converted, headers, status)
}

func (s *Service) forwardStreamIncremental(upCtx context.Context, clientCtx context.Context, abortReq context.CancelFunc, c *gin.Context, resp *http.Response, start time.Time, firstTokenTimeout time.Duration, inbound, upstream protocolKind, model string, converted bool, headers http.Header, status int) streamAttemptResult {
	return s.runtime().forwardStreamIncremental(upCtx, clientCtx, abortReq, c, resp, start, firstTokenTimeout, inbound, upstream, model, converted, headers, status)
}
