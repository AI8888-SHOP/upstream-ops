// 上游错误详情构建、header 脱敏与日志截断。
package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/bejix/upstream-ops/backend/config"
)

type usageRecordMeta struct {
	InboundEndpoint   string
	UpstreamEndpoint  string
	InboundProtocol   string
	UpstreamProtocol  string
	ProtocolConverted bool
	ServiceTier       string
	ReasoningEffort   string
	UpstreamURL       string
	Attempt           int
	AttemptKind       string
	AttemptStatus     string
	Winner            bool
	DeferSettlement   bool
	Validation        validationResult
	CooldownUntil     *time.Time
}

// usageErrorInfo 失败请求的结构化错误信息（摘要 + 上游原文）。
type usageErrorInfo struct {
	Type            string // transport|http|config|internal
	Summary         string // 短摘要，写入 error_message
	Detail          string // 人类可读详情（含 status/url/header 要点）
	UpstreamBody    string // 上游原始错误 body（截断）
	UpstreamHeaders string // 关键响应头 JSON（脱敏截断）
}

func (svc *Service) buildUpstreamErrorInfo(
	fwdErr error,
	status int,
	respHeaders http.Header,
	respBody []byte,
	upstreamURL, method string,
) usageErrorInfo {
	return svc.buildUpstreamErrorInfoCfg(config.GatewayConfig{}.WithDefaults(), fwdErr, status, respHeaders, respBody, upstreamURL, method)
}

// buildUpstreamErrorInfoCfg 同上，截断上限取自网关运行时配置。
func (svc *Service) buildUpstreamErrorInfoCfg(
	cfg config.GatewayConfig,
	fwdErr error,
	status int,
	respHeaders http.Header,
	respBody []byte,
	upstreamURL, method string,
) usageErrorInfo {
	cfg = cfg.WithDefaults()
	info := usageErrorInfo{}
	headerJSON := svc.formatDebugHeaders(respHeaders, cfg.UsageErrorHeadersJSONBytes, cfg.UsageErrorHeaderValueRunes)
	headerPlain := svc.formatHeadersPlain(respHeaders, cfg.UsageErrorHeaderValueRunes)
	bodyText := svc.truncateBytesForLog(respBody, cfg.UsageErrorBodyBytes)
	bodySnippet := svc.extractUpstreamErrorSnippet(respBody)

	if fwdErr != nil {
		info.Type = "transport"
		info.Summary = fwdErr.Error()
		var b strings.Builder
		fmt.Fprintf(&b, "transport error\nmethod: %s\nurl: %s\nerror: %s\n", method, upstreamURL, fwdErr.Error())
		if status > 0 {
			fmt.Fprintf(&b, "partial_status: %d\n", status)
		}
		svc.appendUpstreamHeadersToDetail(&b, headerPlain)
		if bodyText != "" {
			fmt.Fprintf(&b, "partial_body:\n%s\n", bodyText)
		}
		info.Detail = b.String()
		info.UpstreamBody = bodyText
		info.UpstreamHeaders = headerJSON
		return info
	}

	info.Type = "http"
	if bodySnippet != "" {
		info.Summary = fmt.Sprintf("HTTP %d: %s", status, bodySnippet)
	} else {
		info.Summary = fmt.Sprintf("HTTP %d", status)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "upstream HTTP error\nmethod: %s\nurl: %s\nstatus: %d\n", method, upstreamURL, status)
	if ct := respHeaders.Get("Content-Type"); ct != "" {
		fmt.Fprintf(&b, "content_type: %s\n", ct)
	}
	if rid := svc.firstHeader(respHeaders, "x-request-id", "x-openai-request-id", "cf-ray", "x-trace-id", "x-client-request-id"); rid != "" {
		fmt.Fprintf(&b, "upstream_request_id: %s\n", rid)
	}
	svc.appendUpstreamHeadersToDetail(&b, headerPlain)
	if bodyText != "" {
		fmt.Fprintf(&b, "body:\n%s\n", bodyText)
	} else {
		b.WriteString("body: (empty)\n")
	}
	info.Detail = b.String()
	info.UpstreamBody = bodyText
	info.UpstreamHeaders = headerJSON
	return info
}

func (svc *Service) appendUpstreamHeadersToDetail(b *strings.Builder, headerPlain string) {
	if b == nil {
		return
	}
	if strings.TrimSpace(headerPlain) == "" {
		b.WriteString("headers: (none)\n")
		return
	}
	b.WriteString("headers:\n")
	b.WriteString(headerPlain)
	if !strings.HasSuffix(headerPlain, "\n") {
		b.WriteByte('\n')
	}
}

func (svc *Service) firstHeader(h http.Header, keys ...string) string {
	if h == nil {
		return ""
	}
	for _, k := range keys {
		if v := h.Get(k); v != "" {
			return v
		}
	}
	return ""
}

// formatDebugHeaders 序列化完整上游响应头为 JSON（敏感头脱敏；尽量不截断）。
func (svc *Service) formatDebugHeaders(h http.Header, maxJSONBytes, maxValueRunes int) string {
	if maxJSONBytes <= 0 {
		maxJSONBytes = config.DefaultGatewayUsageErrorHeadersJSONBytes
	}
	if maxValueRunes <= 0 {
		maxValueRunes = config.DefaultGatewayUsageErrorHeaderValueRunes
	}
	pairs := svc.collectDebugHeaderPairs(h, maxValueRunes)
	if len(pairs) == 0 {
		return ""
	}
	b, err := json.Marshal(pairs)
	if err != nil {
		return ""
	}
	out := string(b)
	if len(out) > maxJSONBytes {
		return out[:maxJSONBytes] + "…(truncated)"
	}
	return out
}

// formatHeadersPlain 将完整上游响应头格式化为 Name: value 文本（敏感头脱敏）。
func (svc *Service) formatHeadersPlain(h http.Header, maxValueRunes int) string {
	if maxValueRunes <= 0 {
		maxValueRunes = config.DefaultGatewayUsageErrorHeaderValueRunes
	}
	pairs := svc.collectDebugHeaderPairs(h, maxValueRunes)
	if len(pairs) == 0 {
		return ""
	}
	var b strings.Builder
	for _, p := range pairs {
		if len(p.Values) == 0 {
			fmt.Fprintf(&b, "%s:\n", p.Name)
			continue
		}
		for _, v := range p.Values {
			fmt.Fprintf(&b, "%s: %s\n", p.Name, v)
		}
	}
	return b.String()
}

type debugHeaderPair struct {
	Name   string   `json:"name"`
	Values []string `json:"values"`
}

func (svc *Service) collectDebugHeaderPairs(h http.Header, maxValueRunes int) []debugHeaderPair {
	if h == nil || len(h) == 0 {
		return nil
	}
	if maxValueRunes <= 0 {
		maxValueRunes = config.DefaultGatewayUsageErrorHeaderValueRunes
	}
	out := make([]debugHeaderPair, 0, len(h))
	for k, vals := range h {
		ck := http.CanonicalHeaderKey(k)
		redacted := make([]string, len(vals))
		for i, v := range vals {
			if svc.isSensitiveHeader(ck) {
				redacted[i] = "[redacted]"
			} else {
				// 完整保留响应头值；仅极端长值截断防止撑爆库
				redacted[i] = svc.truncateRunes(v, maxValueRunes)
			}
		}
		out = append(out, debugHeaderPair{Name: ck, Values: redacted})
	}
	// 稳定顺序便于 diff / 阅读
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (svc *Service) isSensitiveHeader(canonical string) bool {
	switch strings.ToLower(canonical) {
	case "authorization", "cookie", "set-cookie", "x-api-key", "api-key", "proxy-authorization":
		return true
	default:
		return false
	}
}

// extractUpstreamErrorSnippet 从 JSON 错误体抽出 message/error 字段作摘要。
func (svc *Service) extractUpstreamErrorSnippet(body []byte) string {
	body = bytes.TrimSpace(body)
	if len(body) == 0 {
		return ""
	}
	// SSE 错误：取第一段 data
	if bytes.Contains(body, []byte("data:")) {
		for _, line := range bytes.Split(body, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if bytes.HasPrefix(line, []byte("data:")) {
				payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
				if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
					continue
				}
				if s := svc.extractUpstreamErrorSnippet(payload); s != "" {
					return s
				}
			}
		}
	}
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		// 非 JSON：截一段纯文本
		return svc.truncateRunes(string(body), 200)
	}
	if s := svc.walkErrorMessage(raw, 0); s != "" {
		return svc.truncateRunes(s, 200)
	}
	return svc.truncateRunes(string(body), 200)
}

func (svc *Service) walkErrorMessage(v any, depth int) string {
	if depth > 6 || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t)
	case map[string]any:
		for _, key := range []string{"message", "error", "msg", "detail", "error_description"} {
			if child, ok := t[key]; ok {
				if s := svc.walkErrorMessage(child, depth+1); s != "" {
					return s
				}
			}
		}
		// OpenAI: error: { message, type, code }
		if errObj, ok := t["error"].(map[string]any); ok {
			parts := make([]string, 0, 3)
			if m, _ := errObj["message"].(string); strings.TrimSpace(m) != "" {
				parts = append(parts, strings.TrimSpace(m))
			}
			if typ, _ := errObj["type"].(string); strings.TrimSpace(typ) != "" {
				parts = append(parts, "type="+strings.TrimSpace(typ))
			}
			if code := errObj["code"]; code != nil {
				parts = append(parts, fmt.Sprintf("code=%v", code))
			}
			if len(parts) > 0 {
				return strings.Join(parts, " · ")
			}
		}
	case []any:
		for _, item := range t {
			if msg := svc.walkErrorMessage(item, depth+1); msg != "" {
				return msg
			}
		}
	}
	return ""
}

func (svc *Service) truncateBytesForLog(b []byte, max int) string {
	if max <= 0 || len(b) == 0 {
		return ""
	}
	// 尽量按 UTF-8 截断
	if len(b) <= max {
		return string(b)
	}
	cut := b[:max]
	// 回退到合法 rune 边界
	for len(cut) > 0 && !utf8.Valid(cut) {
		cut = cut[:len(cut)-1]
	}
	return string(cut) + "…(truncated)"
}

func (svc *Service) truncateRunes(str string, max int) string {
	str = strings.TrimSpace(str)
	if max <= 0 || str == "" {
		return str
	}
	r := []rune(str)
	if len(r) <= max {
		return str
	}
	return string(r[:max]) + "…"
}
