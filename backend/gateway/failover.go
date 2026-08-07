// 故障转移状态判定与客户端断开语义。
package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func (svc *Service) isFailoverStatus(code int, failoverOn4xx bool) bool {
	if code == 0 {
		return true
	}
	if code == http.StatusTooManyRequests {
		return true
	}
	if code >= 500 {
		return true
	}
	if failoverOn4xx && code >= 400 && code < 500 {
		return true
	}
	return false
}

// isSameRouteRetryableUpstreamFailure distinguishes transient failures from
// responses that cannot succeed by sending the same request to the same route
// again. The caller may still fail over to another route when its policy allows
// it. Keeping this list narrow preserves configured retries for ordinary 4xx
// responses and all transport/5xx/429 failures.
func isSameRouteRetryableUpstreamFailure(status int, info usageErrorInfo) bool {
	if status == 0 || status == http.StatusTooManyRequests || status >= 500 {
		return true
	}
	if status < 400 || status >= 500 {
		return true
	}
	text := strings.ToLower(strings.TrimSpace(info.Summary))
	if body := strings.ToLower(strings.TrimSpace(info.UpstreamBody)); body != "" {
		if text != "" {
			text += "\n"
		}
		text += body
	}
	for _, marker := range []string{
		"image generation is not enabled",
		"video generation is not enabled",
		"not enabled for this group",
		"insufficient account balance",
		"model_not_found",
		"model not found",
		"no available channel for model",
		"invalid api key",
		"invalid_api_key",
		"permission denied",
		"not authorized",
	} {
		if strings.Contains(text, marker) {
			return false
		}
	}
	return true
}

// isClientDisconnectAfterCommit 流已向客户端提交后，仅因客户端中途断开/取消结束。
// 此时上游通常已正常响应（甚至完整计费），应记成功而非失败。
// 注意：若下游已写出终端帧（DownstreamComplete），forwardStream 会先清掉 ClientDisconnected，
// 不会走到此处——收完再关连接记普通成功。
func (svc *Service) isClientDisconnectAfterCommit(clientDisconnected bool, streamErr error) bool {
	if !clientDisconnected {
		return false
	}
	if streamErr == nil {
		return true
	}
	return errors.Is(streamErr, errClientDisconnected)
}

// isClientContextError 判断失败是否由「客户端断开 / 入站请求 context 取消」引起。
// 典型表现：Post "...": context canceled。若继续用同一 c.Request.Context() 重试/顺延，
// 后续 attempt 会全部被父 context 污染成同样的 canceled，并误冷却路由。
func (svc *Service) isClientContextError(err error, c *gin.Context) bool {
	if err == nil {
		return false
	}
	// 首字超时是网关主动断开，不是客户端取消
	if svc.isFirstTokenTimeout(err) {
		return false
	}
	reqCanceled := false
	if c != nil && c.Request != nil {
		if ce := c.Request.Context().Err(); ce != nil {
			reqCanceled = true
		}
	}
	if errors.Is(err, context.Canceled) {
		// 仅当入站请求 context 也已取消时视为客户端侧；避免误伤其它 Canceled
		return reqCanceled || c == nil
	}
	if errors.Is(err, context.DeadlineExceeded) && reqCanceled {
		return true
	}
	// net/http 常包装为：Post "url": context canceled
	if reqCanceled {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "context canceled") ||
			strings.Contains(msg, "context deadline exceeded") ||
			strings.Contains(msg, "client disconnected") ||
			strings.Contains(msg, "request canceled") {
			return true
		}
	}
	return false
}

// annotateClientContextError 把 transport 的 context canceled 改写为可区分的 client 错误。
func (svc *Service) annotateClientContextError(info *usageErrorInfo, c *gin.Context, upstreamURL, method string, fwdErr error) {
	if info == nil {
		return
	}
	info.Type = "client"
	info.Summary = "客户端已断开或取消请求（context canceled）"
	var b strings.Builder
	fmt.Fprintf(&b, "client context canceled — stop retry/failover\n")
	fmt.Fprintf(&b, "method: %s\nurl: %s\n", method, upstreamURL)
	if fwdErr != nil {
		fmt.Fprintf(&b, "error: %s\n", fwdErr.Error())
	}
	if c != nil && c.Request != nil {
		if ce := c.Request.Context().Err(); ce != nil {
			fmt.Fprintf(&b, "request_context: %s\n", ce.Error())
		}
	}
	b.WriteString("note: 入站请求 context 已取消，后续重试/顺延会全部失败且污染统计，已停止\n")
	// 保留已有 headers 段（若 buildUpstreamErrorInfo 写过）
	if strings.TrimSpace(info.Detail) != "" && strings.Contains(info.Detail, "headers:") {
		if idx := strings.Index(info.Detail, "headers:"); idx >= 0 {
			b.WriteString(info.Detail[idx:])
		}
	} else if strings.TrimSpace(info.UpstreamHeaders) != "" {
		// JSON headers 转不上 plain 时至少提示字段有值
		b.WriteString("headers: (see upstream_error_headers)\n")
	}
	info.Detail = b.String()
}
