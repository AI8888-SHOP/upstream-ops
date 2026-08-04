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
)

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
