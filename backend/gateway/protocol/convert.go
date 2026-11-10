// 协议转换统一入口（请求 / 非流式响应 / 流式响应）。
// 内部调用各方向具体实现；gateway 侧通过 protocol_bridge 使用本文件。
package protocol

// ConvertRequest 按入站/上游协议方向转换请求 body。
// 无对应转换器时返回原 body 与 converted=false（调用方可仍改 path）。
// Responses→Anthropic 经 Chat 中间态。
func ConvertRequest(inbound, upstream Kind, body []byte, model string, stream bool) (out []byte, converted bool, err error) {
	in := NormalizeKind(inbound)
	up := NormalizeKind(upstream)
	out = body

	switch {
	case up == KindOpenAIImages:
		out, err = ImageGenerationRequest(in, out, model)
		converted = true
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindAnthropic:
		out, err = OpenAIToAnthropicRequest(out, model, stream)
		converted = true
	case in == KindAnthropic && (up == KindOpenAIChat || up == KindOpenAI):
		out, err = AnthropicToOpenAIRequest(out, model, stream)
		converted = true
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindOpenAIResponses:
		out, err = OpenAIChatToResponsesRequest(out, model, stream)
		converted = true
	case in == KindAnthropic && up == KindOpenAIResponses:
		out, err = AnthropicToResponsesRequest(out, model, stream)
		converted = true
	case in == KindOpenAIResponses && (up == KindOpenAIChat || up == KindOpenAI):
		out, err = ResponsesToOpenAIChatRequest(out, model, stream)
		converted = true
	case in == KindOpenAIResponses && up == KindAnthropic:
		chat, e2 := ResponsesToOpenAIChatRequest(out, model, stream)
		if e2 != nil {
			return nil, false, e2
		}
		out, err = OpenAIToAnthropicRequest(chat, model, stream)
		converted = true
	case in == KindOpenAIResponses && up == KindOpenAIResponses:
		converted = false
	default:
		converted = NeedsBodyConvert(in, up)
	}
	if err != nil {
		return nil, false, err
	}
	return out, converted, nil
}

// ConvertResponse 将上游非流式响应转回客户端协议。
// 失败或无转换器时返回原 body。
func ConvertResponse(inbound, upstream Kind, body []byte, model string) []byte {
	if NormalizeKind(upstream) == KindOpenAIImages {
		return imageGenerationResponse(inbound, body, model, false)
	}
	if len(body) == 0 {
		return body
	}
	in := NormalizeKind(inbound)
	up := NormalizeKind(upstream)

	var out []byte
	var err error
	switch {
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindAnthropic:
		out, err = AnthropicToOpenAIResponse(body, model)
	case in == KindAnthropic && IsOpenAIFamily(up) && up != KindOpenAIResponses:
		out, err = OpenAIToAnthropicResponse(body, model)
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindOpenAIResponses:
		out, err = ResponsesToOpenAIChatResponse(body, model)
	case in == KindAnthropic && up == KindOpenAIResponses:
		out, err = ResponsesToAnthropicResponse(body, model)
	case in == KindOpenAIResponses && (up == KindOpenAIChat || up == KindOpenAI):
		out, err = OpenAIChatToResponsesResponse(body, model)
	case in == KindOpenAIResponses && up == KindAnthropic:
		chat, e2 := AnthropicToOpenAIResponse(body, model)
		if e2 != nil {
			return body
		}
		out, err = OpenAIChatToResponsesResponse(chat, model)
	default:
		return body
	}
	if err != nil || len(out) == 0 {
		return body
	}
	return out
}

// ConvertStreamResponse 将上游流式响应（SSE/缓冲）转回客户端协议。
// wrapChatAsOpenAISSE 用于把完整 chat JSON 包成 OpenAI SSE；可为 nil 则原样返回 chat JSON。
// tryExtractJSON 从可能的 SSE 缓冲取 JSON 对象；nil 时用 body 本身。
func ConvertStreamResponse(
	inbound, upstream Kind,
	body []byte,
	model string,
	wrapChatAsOpenAISSE func([]byte) []byte,
	tryExtractJSON func([]byte) []byte,
) []byte {
	if NormalizeKind(upstream) == KindOpenAIImages {
		return imageGenerationResponse(inbound, body, model, true)
	}
	if len(body) == 0 {
		return body
	}
	in := NormalizeKind(inbound)
	up := NormalizeKind(upstream)
	if wrapChatAsOpenAISSE == nil {
		wrapChatAsOpenAISSE = func(b []byte) []byte { return b }
	}
	if tryExtractJSON == nil {
		tryExtractJSON = func(b []byte) []byte { return b }
	}

	switch {
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindAnthropic:
		return AnthropicSSEToOpenAISSE(body, model)
	case in == KindAnthropic && IsOpenAIFamily(up) && up != KindOpenAIResponses:
		return OpenAISSEToAnthropicSSE(body, model)
	case IsOpenAIFamily(in) && in != KindOpenAIResponses && up == KindOpenAIResponses:
		if chat, err := ResponsesStreamOrJSONToOpenAIChat(body, model); err == nil {
			return wrapChatAsOpenAISSE(chat)
		}
		return body
	case in == KindAnthropic && up == KindOpenAIResponses:
		if sse, err := ResponsesStreamOrJSONToAnthropicSSE(body, model); err == nil && len(sse) > 0 {
			return sse
		}
		return body
	case in == KindOpenAIResponses && up == KindOpenAIChat:
		if chat := tryExtractJSON(body); len(chat) > 0 {
			if out, err := OpenAIChatToResponsesResponse(chat, model); err == nil {
				return out
			}
		}
		return body
	case in == KindOpenAIResponses && up == KindAnthropic:
		chatSSE := AnthropicSSEToOpenAISSE(body, model)
		if chat := tryExtractJSON(chatSSE); len(chat) > 0 {
			if out, err := OpenAIChatToResponsesResponse(chat, model); err == nil {
				return out
			}
		}
		return body
	default:
		return body
	}
}
