// Package protocol 实现 OpenAI Chat Completions / OpenAI Responses / Anthropic Messages 的协议解析与互转。
package protocol

import "strings"

// Kind 协议种类。
type Kind string

const (
	KindOpenAI          Kind = "openai"           // Chat Completions（兼容旧值）
	KindOpenAIChat      Kind = "openai_chat"      // /v1/chat/completions · messages
	KindOpenAIResponses Kind = "openai_responses" // /v1/responses · input
	KindOpenAIImages    Kind = "openai_images"    // /v1/images/generations · prompt
	KindAnthropic       Kind = "anthropic"        // /v1/messages
	KindAuto            Kind = "auto"
)

// IsOpenAIFamily 是否属于 OpenAI 协议族（Chat 或 Responses）。
func IsOpenAIFamily(k Kind) bool {
	switch NormalizeKind(k) {
	case KindOpenAI, KindOpenAIChat, KindOpenAIResponses:
		return true
	default:
		return false
	}
}

// NormalizeKind 归一化协议：openai → openai_chat。
func NormalizeKind(k Kind) Kind {
	switch Kind(strings.ToLower(strings.TrimSpace(string(k)))) {
	case KindOpenAI, KindOpenAIChat, "chat", "chat_completions":
		return KindOpenAIChat
	case KindOpenAIResponses, "responses":
		return KindOpenAIResponses
	case KindOpenAIImages:
		return KindOpenAIImages
	case KindAnthropic:
		return KindAnthropic
	case KindAuto, "":
		return KindAuto
	default:
		return Kind(strings.ToLower(strings.TrimSpace(string(k))))
	}
}

// PathFor 返回协议默认上游 path。
// inboundPath 非空时：对「透传类」端点（embeddings/images/videos 等）原样保留路径。
func PathFor(kind Kind, inboundPath string) string {
	ip := strings.TrimSpace(inboundPath)
	// 透传类：不因协议族改写 path
	if isPassthroughPath(ip) {
		if !strings.HasPrefix(ip, "/") {
			ip = "/" + ip
		}
		return ip
	}
	switch NormalizeKind(kind) {
	case KindOpenAIImages:
		return "/v1/images/generations"
	case KindAnthropic:
		return "/v1/messages"
	case KindOpenAIResponses:
		// 保留 /v1/responses/* 子路径
		if strings.HasPrefix(ip, "/v1/responses") {
			return ip
		}
		if strings.HasPrefix(ip, "/responses") {
			return "/v1" + ip
		}
		return "/v1/responses"
	case KindOpenAIChat, KindOpenAI:
		if strings.Contains(ip, "/completions") && !strings.Contains(ip, "/chat/") {
			return "/v1/completions"
		}
		if strings.HasPrefix(ip, "/v1/chat/completions") || ip == "/chat/completions" {
			return "/v1/chat/completions"
		}
		return "/v1/chat/completions"
	default:
		if ip == "" {
			return "/v1/chat/completions"
		}
		if !strings.HasPrefix(ip, "/") {
			return "/" + ip
		}
		return ip
	}
}

func isPassthroughPath(p string) bool {
	p = strings.ToLower(strings.TrimSpace(p))
	if p == "" {
		return false
	}
	// 标准化
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	pass := []string{
		"/v1/embeddings", "/embeddings",
		"/v1/images/", "/images/",
		"/v1/videos/", "/videos/",
		"/v1/alpha/", "/alpha/",
	}
	for _, pref := range pass {
		if p == strings.TrimSuffix(pref, "/") || strings.HasPrefix(p, pref) {
			return true
		}
	}
	// exact generations/edits
	switch p {
	case "/v1/images/generations", "/images/generations",
		"/v1/images/edits", "/images/edits",
		"/v1/videos/generations", "/videos/generations",
		"/v1/videos/edits", "/videos/edits",
		"/v1/videos/extensions", "/videos/extensions",
		"/v1/alpha/search", "/alpha/search":
		return true
	}
	return false
}

// ResolveUpstream 根据路由配置与模型名解析实际上游协议。
//
// routeProtocol: auto | openai | openai_chat | openai_responses | anthropic
// inbound: 客户端协议（当前仅 chat 或 anthropic）
// model: 映射后的上游模型名
//
// auto 规则：
//   - Claude 系模型 → anthropic
//   - 否则跟随入站协议（chat 入站 → openai_chat，不会静默升到 responses）
func ResolveUpstream(routeProtocol string, inbound Kind, model string) Kind {
	p := strings.ToLower(strings.TrimSpace(routeProtocol))
	switch p {
	case string(KindOpenAI), string(KindOpenAIChat), "chat", "chat_completions":
		return KindOpenAIChat
	case string(KindOpenAIResponses), "responses":
		return KindOpenAIResponses
	case string(KindAnthropic):
		return KindAnthropic
	}
	// auto
	if LooksLikeClaudeModel(model) {
		return KindAnthropic
	}
	in := NormalizeKind(inbound)
	if in == KindOpenAIResponses {
		return KindOpenAIResponses
	}
	if IsOpenAIFamily(in) || in == KindAuto {
		return KindOpenAIChat
	}
	return in
}

// NeedsBodyConvert 入站与上游协议是否需要 body 转换。
func NeedsBodyConvert(inbound, upstream Kind) bool {
	a, b := NormalizeKind(inbound), NormalizeKind(upstream)
	if a == b {
		return false
	}
	// openai / openai_chat 视为同一形态
	if (a == KindOpenAIChat || a == KindOpenAI) && (b == KindOpenAIChat || b == KindOpenAI) {
		return false
	}
	return true
}

// IsPassthroughEndpoint 是否为不走 chat/messages 转换的透传端点。
func IsPassthroughEndpoint(path string) bool {
	return isPassthroughPath(path)
}

// LooksLikeClaudeModel 启发式判断 Claude 系模型。
func LooksLikeClaudeModel(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	if m == "" {
		return false
	}
	if strings.HasPrefix(m, "claude") {
		return true
	}
	if strings.Contains(m, "claude-") {
		return true
	}
	if strings.HasPrefix(m, "anthropic/") {
		return true
	}
	return false
}
