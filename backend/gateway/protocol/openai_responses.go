package protocol

import (
	"encoding/json"
	"fmt"
	"strings"
)

// OpenAIChatToResponsesRequest 将 OpenAI chat/completions 请求转为 /v1/responses。
// 对齐 sub2api ChatCompletionsToResponses 的 input/tools 形态：
//   - messages[].content 多模态 parts：text → input_text，image_url → input_image
//   - assistant.tool_calls → 独立 function_call 项（禁止把 tool_calls 塞进 message）
//   - role=tool → function_call_output
//   - tools: {type:function,function:{name,parameters}} → {type:function,name,parameters}
//
// 错误形态会触发上游 422：
//
//	Failed to deserialize ... untagged enum ModelInput
func OpenAIChatToResponsesRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid openai chat request: %w", err)
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	out := map[string]any{
		"model":  model,
		"stream": stream,
	}

	// system / developer → instructions（与 chat 的 system 语义对齐）
	var instructions []string
	var input []any
	rawMsgs, _ := in["messages"].([]any)
	for _, raw := range rawMsgs {
		msg, _ := raw.(map[string]any)
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		switch role {
		case "system", "developer":
			if t := contentToPlainTextForResponses(msg["content"]); t != "" {
				instructions = append(instructions, t)
			}
		case "user":
			item, err := chatUserMessageToResponsesInput(msg)
			if err != nil {
				return nil, err
			}
			input = append(input, item)
		case "assistant":
			items, err := chatAssistantMessageToResponsesInput(msg)
			if err != nil {
				return nil, err
			}
			input = append(input, items...)
		case "tool", "function":
			item, err := chatToolMessageToResponsesInput(msg)
			if err != nil {
				return nil, err
			}
			input = append(input, item)
		default:
			// 未知角色按 user 处理，并规范化 content
			item, err := chatUserMessageToResponsesInput(map[string]any{
				"role":    "user",
				"content": msg["content"],
			})
			if err != nil {
				return nil, err
			}
			input = append(input, item)
		}
	}
	if len(instructions) > 0 {
		out["instructions"] = strings.Join(instructions, "\n\n")
	}
	if len(input) == 0 {
		// 空对话时给一个占位，避免上游 400
		input = []any{map[string]any{"role": "user", "content": ""}}
	}
	out["input"] = input

	// max_tokens → max_output_tokens
	if v, ok := asInt(in["max_tokens"]); ok && v > 0 {
		out["max_output_tokens"] = v
	} else if v, ok := asInt(in["max_completion_tokens"]); ok && v > 0 {
		out["max_output_tokens"] = v
	}
	if v, ok := asFloat(in["temperature"]); ok {
		out["temperature"] = v
	}
	if v, ok := asFloat(in["top_p"]); ok {
		out["top_p"] = v
	}
	// reasoning
	if v, ok := in["reasoning_effort"].(string); ok && strings.TrimSpace(v) != "" {
		out["reasoning"] = map[string]any{"effort": v}
	}
	// tools：chat 嵌套 function → responses 扁平结构
	if tools, ok := in["tools"]; ok {
		if converted := convertChatToolsToResponses(tools); converted != nil {
			out["tools"] = converted
		}
	}
	if tc, ok := in["tool_choice"]; ok {
		out["tool_choice"] = convertChatToolChoiceToResponses(tc)
	}
	if rf, ok := in["response_format"]; ok {
		out["text"] = map[string]any{"format": rf}
	}
	if v, ok := in["service_tier"].(string); ok && strings.TrimSpace(v) != "" {
		out["service_tier"] = v
	}
	if v, ok := in["parallel_tool_calls"]; ok {
		out["parallel_tool_calls"] = v
	}

	return json.Marshal(out)
}

// chatUserMessageToResponsesInput user → EasyInputMessage（string 或 input_* parts）。
func chatUserMessageToResponsesInput(msg map[string]any) (map[string]any, error) {
	content, err := convertChatContentToResponsesInput(msg["content"], false)
	if err != nil {
		return nil, err
	}
	return map[string]any{
		"role":    "user",
		"content": content,
	}, nil
}

// chatAssistantMessageToResponsesInput assistant → output_text message + function_call 项。
func chatAssistantMessageToResponsesInput(msg map[string]any) ([]any, error) {
	var items []any
	text := contentToPlainTextForResponses(msg["content"])
	if rc, ok := msg["reasoning_content"].(string); ok && strings.TrimSpace(rc) != "" {
		if text != "" {
			text = "<thinking>" + rc + "</thinking>\n" + text
		} else {
			text = "<thinking>" + rc + "</thinking>"
		}
	}
	if strings.TrimSpace(text) != "" {
		// assistant 历史用 output_text parts（上游对 role=assistant 的 text parts 更严格）
		items = append(items, map[string]any{
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "output_text", "text": text},
			},
		})
	}
	if tcs, ok := msg["tool_calls"].([]any); ok {
		for _, raw := range tcs {
			tc, _ := raw.(map[string]any)
			if tc == nil {
				continue
			}
			fn, _ := tc["function"].(map[string]any)
			name, _ := fn["name"].(string)
			args, _ := fn["arguments"].(string)
			if strings.TrimSpace(args) == "" {
				args = "{}"
			}
			id, _ := tc["id"].(string)
			if name == "" && id == "" {
				continue
			}
			items = append(items, map[string]any{
				"type":      "function_call",
				"call_id":   id,
				"name":      name,
				"arguments": args,
			})
		}
	}
	// 无文本也无 tool_calls：仍给空 assistant，避免丢角色
	if len(items) == 0 {
		items = append(items, map[string]any{"role": "assistant", "content": ""})
	}
	return items, nil
}

// chatToolMessageToResponsesInput role=tool/function → function_call_output。
func chatToolMessageToResponsesInput(msg map[string]any) (map[string]any, error) {
	output := contentToPlainTextForResponses(msg["content"])
	if output == "" {
		output = "(empty)"
	}
	callID, _ := msg["tool_call_id"].(string)
	if callID == "" {
		// legacy function role 用 name 当 call_id
		callID, _ = msg["name"].(string)
	}
	return map[string]any{
		"type":    "function_call_output",
		"call_id": callID,
		"output":  output,
	}, nil
}

// convertChatContentToResponsesInput 将 chat content 转为 Responses EasyInput content。
// assistantHistory=false：text parts → input_text；true 时用 output_text。
func convertChatContentToResponsesInput(content any, assistantHistory bool) (any, error) {
	switch c := content.(type) {
	case nil:
		return "", nil
	case string:
		return c, nil
	case []any:
		textType := "input_text"
		if assistantHistory {
			textType = "output_text"
		}
		var parts []any
		for _, raw := range c {
			pm, _ := raw.(map[string]any)
			if pm == nil {
				if s, ok := raw.(string); ok && s != "" {
					parts = append(parts, map[string]any{"type": textType, "text": s})
				}
				continue
			}
			typ, _ := pm["type"].(string)
			switch typ {
			case "text", "input_text", "output_text":
				if t, ok := pm["text"].(string); ok && t != "" {
					parts = append(parts, map[string]any{"type": textType, "text": t})
				}
			case "image_url", "input_image":
				if url := extractChatImageURL(pm); url != "" {
					// Responses: image_url 直接是 URL 字符串
					parts = append(parts, map[string]any{"type": "input_image", "image_url": url})
				}
			default:
				// 未知 part：尽量抽 text
				if t, ok := pm["text"].(string); ok && t != "" {
					parts = append(parts, map[string]any{"type": textType, "text": t})
				}
			}
		}
		if len(parts) == 0 {
			return "", nil
		}
		return parts, nil
	default:
		// 其它类型压成字符串
		if b, err := json.Marshal(c); err == nil {
			return string(b), nil
		}
		return fmt.Sprint(c), nil
	}
}

func extractChatImageURL(pm map[string]any) string {
	if pm == nil {
		return ""
	}
	// Responses 原生
	if s, ok := pm["image_url"].(string); ok && strings.TrimSpace(s) != "" {
		return s
	}
	// Chat Completions: image_url: { url: "..." }
	if img, ok := pm["image_url"].(map[string]any); ok {
		if s, ok := img["url"].(string); ok {
			return s
		}
	}
	if s, ok := pm["url"].(string); ok {
		return s
	}
	return ""
}

// convertChatToolsToResponses chat tools → responses tools（扁平 name/parameters）。
func convertChatToolsToResponses(tools any) []any {
	arr, ok := tools.([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	var out []any
	for _, raw := range arr {
		t, _ := raw.(map[string]any)
		if t == nil {
			continue
		}
		typ, _ := t["type"].(string)
		if typ == "" {
			typ = "function"
		}
		// Anthropic / OpenAI server tools：web_search_20250305 → web_search
		if strings.HasPrefix(typ, "web_search") {
			out = append(out, map[string]any{"type": "web_search"})
			continue
		}
		// 已是 responses 扁平形态
		if name, ok := t["name"].(string); ok && name != "" && t["function"] == nil {
			// Anthropic 原生 tool：name + input_schema（无 type/function）
			params := t["parameters"]
			if params == nil {
				if schema, ok := t["input_schema"]; ok {
					params = schema
				}
			}
			item := map[string]any{
				"type":       "function",
				"name":       name,
				"parameters": normalizeToolParameters(params),
				"strict":     false,
			}
			if d, ok := t["description"]; ok {
				item["description"] = d
			}
			if s, ok := t["strict"]; ok {
				item["strict"] = s
			}
			out = append(out, item)
			continue
		}
		// chat: {type:function, function:{name,description,parameters,strict}}
		fn, _ := t["function"].(map[string]any)
		if fn == nil {
			// 非 function 工具原样透传（如 web_search）
			out = append(out, t)
			continue
		}
		name, _ := fn["name"].(string)
		if name == "" {
			continue
		}
		item := map[string]any{
			"type":       "function",
			"name":       name,
			"parameters": normalizeToolParameters(fn["parameters"]),
			"strict":     false,
		}
		if d, ok := fn["description"]; ok {
			item["description"] = d
		}
		if s, ok := fn["strict"]; ok {
			item["strict"] = s
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// normalizeToolParameters 保证 Responses function.parameters 合法。
// object schema 缺 properties 时补 {}，nil/空 → {"type":"object","properties":{}}。
func normalizeToolParameters(schema any) any {
	if schema == nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	m, ok := schema.(map[string]any)
	if !ok {
		// 可能是 json.RawMessage / 已序列化字符串
		switch v := schema.(type) {
		case string:
			if strings.TrimSpace(v) == "" || v == "null" {
				return map[string]any{"type": "object", "properties": map[string]any{}}
			}
			var parsed any
			if err := json.Unmarshal([]byte(v), &parsed); err == nil {
				return normalizeToolParameters(parsed)
			}
			return schema
		default:
			return schema
		}
	}
	typ, _ := m["type"].(string)
	if typ == "object" {
		if _, ok := m["properties"]; !ok {
			// 浅拷贝后补 properties，避免改写调用方 map
			cp := make(map[string]any, len(m)+1)
			for k, v := range m {
				cp[k] = v
			}
			cp["properties"] = map[string]any{}
			return cp
		}
	}
	if typ == "" && m["properties"] == nil && len(m) == 0 {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	return m
}

// convertChatToolChoiceToResponses 规范化 tool_choice。
// chat: {"type":"function","function":{"name":"x"}} → responses: {"type":"function","name":"x"}
func convertChatToolChoiceToResponses(tc any) any {
	m, ok := tc.(map[string]any)
	if !ok {
		return tc
	}
	typ, _ := m["type"].(string)
	if typ != "function" {
		return tc
	}
	if name, ok := m["name"].(string); ok && name != "" {
		return m
	}
	if fn, ok := m["function"].(map[string]any); ok {
		name, _ := fn["name"].(string)
		if name == "" {
			return tc
		}
		return map[string]any{"type": "function", "name": name}
	}
	return tc
}

// AnthropicToResponsesRequest Anthropic /v1/messages → OpenAI /v1/responses。
// 直接转换（对齐 sub2api AnthropicToResponses 核心形态），避免 chat 中转丢失 tool/schema。
//
// 关键点：
//   - system → instructions（兼容多数 OpenAI 兼容网关；developer 角色部分代理会 422）
//   - tool_use / tool_result → function_call / function_call_output
//   - tools.input_schema → 扁平 function.parameters，并 normalize properties
//   - thinking/output_config → reasoning.effort
func AnthropicToResponsesRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid anthropic request: %w", err)
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}

	input, err := anthropicMessagesToResponsesInput(in)
	if err != nil {
		return nil, err
	}
	if len(input) == 0 {
		input = []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": []any{map[string]any{"type": "input_text", "text": ""}},
		}}
	}

	out := map[string]any{
		"model":  model,
		"stream": stream,
		"input":  input,
		"store":  false,
	}
	// system → instructions（字符串），不塞 developer 角色，降低第三方网关拒识风险
	if sys, ok := in["system"]; ok && sys != nil {
		if sysText := anthropicSystemToText(sys); sysText != "" && !strings.HasPrefix(sysText, "x-anthropic-billing-header:") {
			out["instructions"] = sysText
		}
	}

	// max_tokens → max_output_tokens（Responses 过小 max 常被上游拒绝，设下限 16）
	if v, ok := asInt(in["max_tokens"]); ok && v > 0 {
		if v < 16 {
			v = 16
		}
		out["max_output_tokens"] = v
	}

	// 采样参数：部分推理模型（gpt-5*）不接受 temperature/top_p
	if !isReasoningModelName(model) {
		if v, ok := asFloat(in["temperature"]); ok {
			out["temperature"] = v
		}
		if v, ok := asFloat(in["top_p"]); ok {
			out["top_p"] = v
		}
	}

	// tools
	if tools, ok := in["tools"].([]any); ok && len(tools) > 0 {
		if converted := convertAnthropicToolsToResponses(tools); len(converted) > 0 {
			out["tools"] = converted
			out["parallel_tool_calls"] = true
		}
	}
	if tc, ok := in["tool_choice"]; ok {
		out["tool_choice"] = convertAnthropicToolChoiceToResponses(tc)
	}

	// thinking / output_config.effort → reasoning
	effort := ""
	if oc, ok := in["output_config"].(map[string]any); ok {
		if e, ok := oc["effort"].(string); ok && strings.TrimSpace(e) != "" {
			effort = e
		}
	}
	if effort == "" {
		if thinking, ok := in["thinking"].(map[string]any); ok {
			if typ, _ := thinking["type"].(string); typ == "enabled" {
				budget, _ := asInt(thinking["budget_tokens"])
				effort = budgetToEffort(budget)
			}
		}
	}
	if effort != "" {
		if effort == "max" {
			effort = "xhigh"
		}
		out["reasoning"] = map[string]any{"effort": effort, "summary": "auto"}
	}

	return json.Marshal(out)
}

// anthropicMessagesToResponsesInput messages → Responses input items（system 由调用方写 instructions）。
func anthropicMessagesToResponsesInput(in map[string]any) ([]any, error) {
	var input []any
	rawMsgs, _ := in["messages"].([]any)
	for _, raw := range rawMsgs {
		msg, _ := raw.(map[string]any)
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		switch role {
		case "assistant":
			items, err := anthropicAssistantToResponsesInput(msg["content"])
			if err != nil {
				return nil, err
			}
			input = append(input, items...)
		default: // user / 其它
			items, err := anthropicUserToResponsesInput(msg["content"])
			if err != nil {
				return nil, err
			}
			input = append(input, items...)
		}
	}
	return input, nil
}

// anthropicUserToResponsesInput user content → function_call_output* + optional user message。
func anthropicUserToResponsesInput(content any) ([]any, error) {
	switch c := content.(type) {
	case string:
		return []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": []any{map[string]any{"type": "input_text", "text": c}},
		}}, nil
	case []any:
		var out []any
		var parts []any
		for _, raw := range c {
			pm, _ := raw.(map[string]any)
			if pm == nil {
				continue
			}
			switch pm["type"] {
			case "tool_result":
				id, _ := pm["tool_use_id"].(string)
				text, imgParts := anthropicToolResultToOutput(pm["content"])
				out = append(out, map[string]any{
					"type":    "function_call_output",
					"call_id": id,
					"output":  text,
				})
				parts = append(parts, imgParts...)
			case "text":
				if s, ok := pm["text"].(string); ok && s != "" {
					parts = append(parts, map[string]any{"type": "input_text", "text": s})
				}
			case "image":
				if url := anthropicImageSourceToURL(pm["source"]); url != "" {
					parts = append(parts, map[string]any{"type": "input_image", "image_url": url})
				}
			}
		}
		if len(parts) > 0 {
			out = append(out, map[string]any{
				"type":    "message",
				"role":    "user",
				"content": parts,
			})
		}
		// 纯 tool_result 轮次：只回 function_call_output
		if len(out) == 0 {
			out = append(out, map[string]any{
				"type":    "message",
				"role":    "user",
				"content": []any{map[string]any{"type": "input_text", "text": ""}},
			})
		}
		return out, nil
	default:
		return []any{map[string]any{
			"type":    "message",
			"role":    "user",
			"content": []any{map[string]any{"type": "input_text", "text": fmt.Sprint(content)}},
		}}, nil
	}
}

// anthropicAssistantToResponsesInput assistant → output_text message + function_call items。
// thinking 块忽略（Responses input 不接受）。
func anthropicAssistantToResponsesInput(content any) ([]any, error) {
	switch c := content.(type) {
	case string:
		if strings.TrimSpace(c) == "" {
			return nil, nil
		}
		return []any{map[string]any{
			"type": "message",
			"role": "assistant",
			"content": []any{
				map[string]any{"type": "output_text", "text": c},
			},
		}}, nil
	case []any:
		var items []any
		var texts []string
		for _, raw := range c {
			pm, _ := raw.(map[string]any)
			if pm == nil {
				continue
			}
			switch pm["type"] {
			case "text":
				if s, ok := pm["text"].(string); ok && s != "" {
					texts = append(texts, s)
				}
			case "tool_use":
				// 先落文本，再落 function_call（保持与 sub2api 一致：text message 在前）
				if len(texts) > 0 {
					items = append(items, map[string]any{
						"type": "message",
						"role": "assistant",
						"content": []any{
							map[string]any{"type": "output_text", "text": strings.Join(texts, "\n\n")},
						},
					})
					texts = nil
				}
				id, _ := pm["id"].(string)
				name, _ := pm["name"].(string)
				args := "{}"
				if input := pm["input"]; input != nil {
					if b, err := json.Marshal(input); err == nil {
						args = string(b)
					}
				}
				items = append(items, map[string]any{
					"type":      "function_call",
					"call_id":   id,
					"name":      name,
					"arguments": args,
				})
			}
		}
		if len(texts) > 0 {
			items = append(items, map[string]any{
				"type": "message",
				"role": "assistant",
				"content": []any{
					map[string]any{"type": "output_text", "text": strings.Join(texts, "\n\n")},
				},
			})
		}
		return items, nil
	default:
		return nil, nil
	}
}

func anthropicToolResultToOutput(content any) (text string, imageParts []any) {
	switch c := content.(type) {
	case string:
		if c == "" {
			return "(empty)", nil
		}
		return c, nil
	case []any:
		var texts []string
		for _, raw := range c {
			pm, _ := raw.(map[string]any)
			if pm == nil {
				continue
			}
			switch pm["type"] {
			case "text":
				if s, ok := pm["text"].(string); ok && s != "" {
					texts = append(texts, s)
				}
			case "image":
				if url := anthropicImageSourceToURL(pm["source"]); url != "" {
					imageParts = append(imageParts, map[string]any{"type": "input_image", "image_url": url})
				}
			}
		}
		text = strings.Join(texts, "\n\n")
		if text == "" {
			text = "(empty)"
		}
		return text, imageParts
	case nil:
		return "(empty)", nil
	default:
		if b, err := json.Marshal(c); err == nil {
			return string(b), nil
		}
		return fmt.Sprint(c), nil
	}
}

func anthropicImageSourceToURL(src any) string {
	m, _ := src.(map[string]any)
	if m == nil {
		return ""
	}
	st, _ := m["type"].(string)
	switch st {
	case "base64":
		mt, _ := m["media_type"].(string)
		if mt == "" {
			mt = "image/png"
		}
		data, _ := m["data"].(string)
		if data == "" {
			return ""
		}
		return "data:" + mt + ";base64," + data
	case "url":
		url, _ := m["url"].(string)
		return url
	default:
		return ""
	}
}

func convertAnthropicToolsToResponses(tools []any) []any {
	// 复用扁平化逻辑：把 Anthropic {name,input_schema} 伪装成已扁平/chat 形态
	var asChat []any
	for _, raw := range tools {
		t, _ := raw.(map[string]any)
		if t == nil {
			continue
		}
		typ, _ := t["type"].(string)
		if strings.HasPrefix(typ, "web_search") {
			asChat = append(asChat, map[string]any{"type": "web_search"})
			continue
		}
		name, _ := t["name"].(string)
		if name == "" {
			continue
		}
		item := map[string]any{
			"type":        "function",
			"name":        name,
			"description": t["description"],
			"parameters":  normalizeToolParameters(t["input_schema"]),
			"strict":      false,
		}
		asChat = append(asChat, item)
	}
	return asChat
}

func convertAnthropicToolChoiceToResponses(tc any) any {
	m, ok := tc.(map[string]any)
	if !ok {
		if s, ok := tc.(string); ok {
			return s
		}
		return "auto"
	}
	switch m["type"] {
	case "auto":
		return "auto"
	case "none":
		return "none"
	case "any":
		return "required"
	case "tool":
		name, _ := m["name"].(string)
		return map[string]any{"type": "function", "name": name}
	default:
		return "auto"
	}
}

func isReasoningModelName(model string) bool {
	m := strings.ToLower(strings.TrimSpace(model))
	return strings.HasPrefix(m, "gpt-5") || strings.HasPrefix(m, "o1") || strings.HasPrefix(m, "o3") || strings.HasPrefix(m, "o4")
}

// ResponsesToOpenAIChatRequest 将入站 /v1/responses 请求转为 chat/completions。
// 对齐 sub2api ResponsesToChatCompletionsRequest 核心字段。
// ResponsesToOpenAIChatRequest 将 /v1/responses 请求转为 chat/completions。
// 对齐 sub2api Responses→Chat 核心：
//   - instructions / developer / system → system
//   - message(user|assistant) content：input_text/output_text → 纯文本或 chat parts
//   - type=function_call → assistant.tool_calls（禁止丢弃）
//   - type=function_call_output → role=tool
//   - tools：Responses 扁平 {name,parameters} → Chat 嵌套 {type,function:{name,parameters}}
func ResponsesToOpenAIChatRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid responses request: %w", err)
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	var messages []any
	// instructions → system
	if inst, ok := in["instructions"].(string); ok && strings.TrimSpace(inst) != "" {
		messages = append(messages, map[string]any{"role": "system", "content": inst})
	}
	// input: string | array
	switch inp := in["input"].(type) {
	case string:
		messages = append(messages, map[string]any{"role": "user", "content": inp})
	case []any:
		messages = append(messages, responsesInputArrayToChatMessages(inp)...)
	default:
		if inp != nil {
			messages = append(messages, map[string]any{"role": "user", "content": fmt.Sprint(inp)})
		}
	}
	if len(messages) == 0 {
		messages = []any{map[string]any{"role": "user", "content": ""}}
	}
	out := map[string]any{
		"model":    model,
		"messages": messages,
		"stream":   stream,
	}
	if v, ok := asInt(in["max_output_tokens"]); ok && v > 0 {
		out["max_tokens"] = v
	}
	if v, ok := asFloat(in["temperature"]); ok {
		out["temperature"] = v
	}
	if v, ok := asFloat(in["top_p"]); ok {
		out["top_p"] = v
	}
	if tools, ok := in["tools"]; ok {
		if converted := convertResponsesToolsToChat(tools); converted != nil {
			out["tools"] = converted
		}
	}
	if tc, ok := in["tool_choice"]; ok {
		out["tool_choice"] = convertResponsesToolChoiceToChat(tc)
	}
	if r, ok := in["reasoning"].(map[string]any); ok {
		if e, ok := r["effort"].(string); ok && e != "" {
			out["reasoning_effort"] = e
		}
	}
	if st, ok := in["service_tier"].(string); ok && st != "" {
		out["service_tier"] = st
	}
	if text, ok := in["text"].(map[string]any); ok {
		if f, ok := text["format"]; ok {
			out["response_format"] = f
		}
	}
	if v, ok := in["parallel_tool_calls"]; ok {
		out["parallel_tool_calls"] = v
	}
	return json.Marshal(out)
}

// responsesInputArrayToChatMessages 将 Responses input 数组转为 chat messages。
// 连续 function_call 合并为一条 assistant.tool_calls（对齐 Chat 多工具一轮形态）。
func responsesInputArrayToChatMessages(inp []any) []any {
	var messages []any
	var pendingToolCalls []any

	flushToolCalls := func() {
		if len(pendingToolCalls) == 0 {
			return
		}
		messages = append(messages, map[string]any{
			"role":       "assistant",
			"content":    nil,
			"tool_calls": pendingToolCalls,
		})
		pendingToolCalls = nil
	}

	for _, raw := range inp {
		switch item := raw.(type) {
		case string:
			flushToolCalls()
			messages = append(messages, map[string]any{"role": "user", "content": item})
		case map[string]any:
			typ, _ := item["type"].(string)
			switch typ {
			case "function_call", "custom_tool_call", "tool_call":
				if tc := responsesFunctionCallToChatToolCall(item); tc != nil {
					pendingToolCalls = append(pendingToolCalls, tc)
				}
			case "function_call_output", "tool_result", "custom_tool_call_output":
				flushToolCalls()
				callID, _ := item["call_id"].(string)
				if callID == "" {
					callID, _ = item["tool_use_id"].(string)
				}
				output := item["output"]
				if output == nil {
					output = item["content"]
				}
				messages = append(messages, map[string]any{
					"role":         "tool",
					"tool_call_id": callID,
					"content":      responsesOutputToChatToolContent(output),
				})
			case "reasoning":
				// skip
			default:
				flushToolCalls()
				if msgs := responsesMessageItemToChatMessages(item); len(msgs) > 0 {
					messages = append(messages, msgs...)
				}
			}
		}
	}
	flushToolCalls()
	return messages
}

func responsesFunctionCallToChatToolCall(item map[string]any) map[string]any {
	callID, _ := item["call_id"].(string)
	if callID == "" {
		callID, _ = item["id"].(string)
	}
	name, _ := item["name"].(string)
	args, _ := item["arguments"].(string)
	if strings.TrimSpace(args) == "" {
		args = "{}"
	}
	if name == "" && callID == "" {
		return nil
	}
	return map[string]any{
		"id":   callID,
		"type": "function",
		"function": map[string]any{
			"name":      name,
			"arguments": args,
		},
	}
}

// responsesMessageItemToChatMessages type=message / EasyInputMessage。
func responsesMessageItemToChatMessages(item map[string]any) []any {
	if item == nil {
		return nil
	}
	typ, _ := item["type"].(string)
	role, _ := item["role"].(string)

	// 非 message 且无 role：忽略
	if typ != "" && typ != "message" && role == "" {
		return nil
	}
	if role == "" {
		role = "user"
	}
	if role == "developer" || role == "system" {
		text := contentToPlainTextForResponses(item["content"])
		if text == "" {
			return nil
		}
		return []any{map[string]any{"role": "system", "content": text}}
	}

	content := convertResponsesContentToChat(item["content"], role == "assistant")
	msg := map[string]any{"role": role, "content": content}
	if tcs, ok := item["tool_calls"].([]any); ok && len(tcs) > 0 {
		msg["tool_calls"] = tcs
	}
	if tid, ok := item["tool_call_id"]; ok {
		msg["tool_call_id"] = tid
	}
	if n, ok := item["name"]; ok {
		msg["name"] = n
	}
	return []any{msg}
}

// convertResponsesContentToChat 将 Responses content（string | parts）转为 chat content。
func convertResponsesContentToChat(content any, assistant bool) any {
	switch c := content.(type) {
	case nil:
		if assistant {
			return nil
		}
		return ""
	case string:
		return c
	case []any:
		var texts []string
		var parts []any
		hasNonText := false
		for _, raw := range c {
			pm, _ := raw.(map[string]any)
			if pm == nil {
				if s, ok := raw.(string); ok && s != "" {
					texts = append(texts, s)
				}
				continue
			}
			typ, _ := pm["type"].(string)
			switch typ {
			case "input_text", "output_text", "text":
				if t, ok := pm["text"].(string); ok {
					texts = append(texts, t)
					parts = append(parts, map[string]any{"type": "text", "text": t})
				}
			case "input_image", "image_url":
				hasNonText = true
				url := extractChatImageURL(pm)
				if url == "" {
					continue
				}
				parts = append(parts, map[string]any{
					"type":      "image_url",
					"image_url": map[string]any{"url": url},
				})
			default:
				if t, ok := pm["text"].(string); ok && t != "" {
					texts = append(texts, t)
					parts = append(parts, map[string]any{"type": "text", "text": t})
				}
			}
		}
		if hasNonText && len(parts) > 0 {
			return parts
		}
		return strings.Join(texts, "")
	default:
		return contentToPlainTextForResponses(c)
	}
}

func responsesOutputToChatToolContent(output any) string {
	switch o := output.(type) {
	case nil:
		return "(empty)"
	case string:
		if o == "" {
			return "(empty)"
		}
		return o
	default:
		t := contentToPlainTextForResponses(o)
		if t == "" {
			if b, err := json.Marshal(o); err == nil {
				return string(b)
			}
			return "(empty)"
		}
		return t
	}
}

// convertResponsesToolsToChat Responses 扁平 tools → Chat 嵌套 function tools。
func convertResponsesToolsToChat(tools any) []any {
	arr, ok := tools.([]any)
	if !ok || len(arr) == 0 {
		return nil
	}
	var out []any
	for _, raw := range arr {
		t, _ := raw.(map[string]any)
		if t == nil {
			continue
		}
		// 已是 chat 嵌套形态
		if fn, ok := t["function"].(map[string]any); ok && fn != nil {
			out = append(out, t)
			continue
		}
		typ, _ := t["type"].(string)
		if typ == "" {
			typ = "function"
		}
		if strings.HasPrefix(typ, "web_search") {
			// chat 侧通常无原生 web_search；跳过或透传
			continue
		}
		name, _ := t["name"].(string)
		if name == "" {
			continue
		}
		params := t["parameters"]
		if params == nil {
			params = t["input_schema"]
		}
		if params == nil {
			params = map[string]any{"type": "object", "properties": map[string]any{}}
		}
		fn := map[string]any{
			"name":       name,
			"parameters": params,
		}
		if d, ok := t["description"]; ok {
			fn["description"] = d
		}
		item := map[string]any{
			"type":     "function",
			"function": fn,
		}
		if s, ok := t["strict"]; ok {
			item["strict"] = s
		}
		out = append(out, item)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// convertResponsesToolChoiceToChat Responses tool_choice → Chat tool_choice。
func convertResponsesToolChoiceToChat(tc any) any {
	switch v := tc.(type) {
	case string:
		return v // auto / none / required
	case map[string]any:
		typ, _ := v["type"].(string)
		switch typ {
		case "function":
			name, _ := v["name"].(string)
			if name == "" {
				if fn, ok := v["function"].(map[string]any); ok {
					name, _ = fn["name"].(string)
				}
			}
			if name == "" {
				return "auto"
			}
			return map[string]any{
				"type":     "function",
				"function": map[string]any{"name": name},
			}
		case "auto", "none", "required":
			return typ
		default:
			return v
		}
	default:
		return tc
	}
}

// OpenAIChatToResponsesResponse 将 chat/completions 非流式响应转为 Responses。
func OpenAIChatToResponsesResponse(body []byte, model string) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid chat response: %w", err)
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	id, _ := in["id"].(string)
	text := ""
	var toolCalls []any
	if choices, ok := in["choices"].([]any); ok && len(choices) > 0 {
		if ch, ok := choices[0].(map[string]any); ok {
			if msg, ok := ch["message"].(map[string]any); ok {
				switch c := msg["content"].(type) {
				case string:
					text = c
				case nil:
				default:
					if b, err := json.Marshal(c); err == nil {
						text = string(b)
					}
				}
				if tc, ok := msg["tool_calls"].([]any); ok {
					toolCalls = tc
				}
			}
		}
	}
	content := []any{}
	if text != "" {
		content = append(content, map[string]any{"type": "output_text", "text": text})
	}
	output := []any{
		map[string]any{"type": "message", "role": "assistant", "content": content, "status": "completed"},
	}
	for _, tc := range toolCalls {
		tcm, _ := tc.(map[string]any)
		if tcm == nil {
			continue
		}
		fn, _ := tcm["function"].(map[string]any)
		name, _ := fn["name"].(string)
		args, _ := fn["arguments"].(string)
		cid, _ := tcm["id"].(string)
		output = append(output, map[string]any{
			"type":      "function_call",
			"name":      name,
			"arguments": args,
			"call_id":   cid,
			"id":        cid,
		})
	}
	usage := map[string]any{}
	if u, ok := in["usage"].(map[string]any); ok {
		usage = openAIUsageToResponses(u)
		if d, ok := u["completion_tokens_details"]; ok {
			usage["output_tokens_details"] = d
		}
	}
	out := map[string]any{
		"id":     id,
		"object": "response",
		"model":  model,
		"status": "completed",
		"output": output,
	}
	if text != "" {
		out["output_text"] = text
	}
	if len(usage) > 0 {
		out["usage"] = usage
	}
	return json.Marshal(out)
}

// ResponsesStreamOrJSONToOpenAIChat 将 Responses 非流式 JSON 或 SSE 缓冲转为 chat.completion。
// 流式场景上游常返回 response.* 事件流；末行往往是 response.completed，真正 payload 在
// event.response 里。若只取最后一行当 Response 对象，会丢掉 output/function_call，
// 客户端表现为「请求成功但工具调用无输出」。
func ResponsesStreamOrJSONToOpenAIChat(body []byte, model string) ([]byte, error) {
	resp := extractResponsesObject(body)
	if resp == nil {
		return nil, fmt.Errorf("no responses object in body")
	}
	return responsesObjectToOpenAIChat(resp, model)
}

// ResponsesToOpenAIChatResponse 将 /v1/responses 非流式响应转为 chat/completions。
// 也接受 response.completed 事件包装或 SSE 缓冲（委托 extractResponsesObject）。
func ResponsesToOpenAIChatResponse(body []byte, model string) ([]byte, error) {
	return ResponsesStreamOrJSONToOpenAIChat(body, model)
}

// extractResponsesObject 从 JSON / SSE / response.* 事件中抽出 Response 对象。
func extractResponsesObject(body []byte) map[string]any {
	trim := bytesTrimSpace(body)
	if len(trim) == 0 {
		return nil
	}
	// 纯 JSON
	if trim[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal(trim, &m); err == nil {
			if unwrapped := unwrapResponsesEvent(m); unwrapped != nil {
				return unwrapped
			}
		}
	}
	// SSE：优先 response.completed / response.done，否则带 output 的最后一帧
	var (
		bestCompleted map[string]any
		bestWithOut   map[string]any
		assembled     = map[string]any{}
		outputItems   []any
		sawAny        bool
	)
	for _, ev := range parseSSEEvents(string(body)) {
		data := strings.TrimSpace(ev.Data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(data), &m); err != nil {
			continue
		}
		sawAny = true
		typ, _ := m["type"].(string)
		if typ == "" {
			typ = strings.TrimSpace(ev.Event)
		}
		switch typ {
		case "response.completed", "response.done", "response.failed", "response.incomplete":
			if unwrapped := unwrapResponsesEvent(m); unwrapped != nil {
				bestCompleted = unwrapped
			}
		case "response.created", "response.in_progress":
			if unwrapped := unwrapResponsesEvent(m); unwrapped != nil {
				// 仅作 id/model 种子
				if assembled["id"] == nil {
					if id, ok := unwrapped["id"]; ok {
						assembled["id"] = id
					}
				}
				if assembled["model"] == nil {
					if mod, ok := unwrapped["model"]; ok {
						assembled["model"] = mod
					}
				}
			}
		case "response.output_item.done", "response.output_item.added":
			if item, ok := m["item"].(map[string]any); ok && item != nil {
				// done 覆盖同 call_id/id 的 added
				outputItems = upsertResponsesOutputItem(outputItems, item)
			}
		default:
			if unwrapped := unwrapResponsesEvent(m); unwrapped != nil {
				if _, has := unwrapped["output"]; has {
					bestWithOut = unwrapped
				}
			} else if _, has := m["output"]; has {
				// 裸 Response 对象
				bestWithOut = m
			}
		}
	}
	if bestCompleted != nil {
		return bestCompleted
	}
	if bestWithOut != nil {
		return bestWithOut
	}
	if len(outputItems) > 0 {
		assembled["object"] = "response"
		assembled["status"] = "completed"
		assembled["output"] = outputItems
		return assembled
	}
	if sawAny {
		// 最后一搏：末行 JSON
		for i := len(parseSSEEvents(string(body))) - 1; i >= 0; i-- {
			// already walked; fallthrough
			break
		}
	}
	// 非 SSE 失败时再试整包
	var m map[string]any
	if err := json.Unmarshal(trim, &m); err == nil {
		return unwrapResponsesEvent(m)
	}
	return nil
}

// unwrapResponsesEvent 若是 response.* 事件则取 .response；否则若本身像 Response 则返回自身。
// 故意不把 chat.completion / 任意 JSON 当成 Response，否则 messages→responses 会
// 在上游误回 chat 形态时抽出空 content（表现为成功但无输出）。
func unwrapResponsesEvent(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	if resp, ok := m["response"].(map[string]any); ok && resp != nil {
		// 事件外壳：type=response.completed 等
		return resp
	}
	// 明确排除 chat 形态
	if obj, _ := m["object"].(string); obj == "chat.completion" || obj == "chat.completion.chunk" {
		return nil
	}
	if _, hasChoices := m["choices"]; hasChoices {
		return nil
	}
	// 已是 Response：有 output / output_text / object=response
	if _, ok := m["output"]; ok {
		return m
	}
	if _, ok := m["output_text"]; ok {
		return m
	}
	if obj, _ := m["object"].(string); obj == "response" {
		return m
	}
	// type 是事件名且无 response 字段：无效
	if typ, _ := m["type"].(string); strings.HasPrefix(typ, "response.") {
		return nil
	}
	return nil
}

func upsertResponsesOutputItem(items []any, item map[string]any) []any {
	id, _ := item["id"].(string)
	callID, _ := item["call_id"].(string)
	for i, raw := range items {
		ex, _ := raw.(map[string]any)
		if ex == nil {
			continue
		}
		exID, _ := ex["id"].(string)
		exCall, _ := ex["call_id"].(string)
		if (id != "" && id == exID) || (callID != "" && callID == exCall) {
			items[i] = item
			return items
		}
	}
	return append(items, item)
}

func bytesTrimSpace(b []byte) []byte {
	return []byte(strings.TrimSpace(string(b)))
}

func responsesObjectToOpenAIChat(in map[string]any, model string) ([]byte, error) {
	if in == nil {
		return nil, fmt.Errorf("nil responses object")
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	id, _ := in["id"].(string)
	text, toolCalls := extractResponsesOutput(in)

	msg := map[string]any{
		"role":    "assistant",
		"content": text,
	}
	if len(toolCalls) > 0 {
		msg["tool_calls"] = toolCalls
		if text == "" {
			msg["content"] = nil
		}
	}

	usage := map[string]any{}
	if u, ok := in["usage"].(map[string]any); ok {
		usage = openAIUsageToChat(u)
		// 透传输出细节。
		if d, ok := u["output_tokens_details"]; ok {
			usage["completion_tokens_details"] = d
		}
	}

	finish := responsesFinishReason(in, len(toolCalls) > 0)
	out := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"model":   model,
		"choices": []any{map[string]any{"index": 0, "message": msg, "finish_reason": finish}},
	}
	if len(usage) > 0 {
		out["usage"] = usage
	}
	if created, ok := asInt(in["created_at"]); ok {
		out["created"] = created
	}
	return json.Marshal(out)
}

// copyOpenAICacheUsageFields keeps cache buckets intact when converting
// between Chat Completions and Responses. Providers use several compatible
// spellings for cache reads and cache creation; preserving all fields that are
// present prevents a later usage normalizer from silently reclassifying them.
func copyOpenAICacheUsageFields(dst, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	for _, key := range []string{
		"cache_read_input_tokens", "cache_read_tokens", "cached_tokens",
		"prompt_cache_hit_tokens", "cache_hit_tokens",
		"cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens", "cache_write_input_tokens",
		"cache_creation_5m_input_tokens", "cache_creation_5m_tokens",
		"cache_creation_1h_input_tokens", "cache_creation_1h_tokens", "cache_creation",
	} {
		if value, exists := src[key]; exists {
			// A converter may already have synthesized a canonical value from
			// duration buckets. Do not replace that value with a stale alias.
			if _, canonical := dst[key]; !canonical {
				dst[key] = value
			}
		}
	}
}

// ResponsesToAnthropicResponse Responses → Anthropic messages（非流式 JSON）。
// body 可为 Response JSON、response.completed 包装或 SSE 缓冲。
// 优先直接从 Response.output 组装（保留 reasoning→thinking），失败再走 chat 中转；
// 若上游实际返回 chat.completion 形态（部分网关兼容口），再尝试 chat→anthropic。
func ResponsesToAnthropicResponse(body []byte, model string) ([]byte, error) {
	if resp := extractResponsesObject(body); resp != nil {
		if out, err := responsesObjectToAnthropic(resp, model); err == nil && len(out) > 0 {
			return out, nil
		}
	}
	// chat.completion JSON / 末包
	if chat, err := tryParseAsChatCompletion(body); err == nil && chat != nil {
		return OpenAIToAnthropicResponse(mustJSON(chat), model)
	}
	// 最后：旧路径 Responses→chat→anthropic
	chat, err := ResponsesStreamOrJSONToOpenAIChat(body, model)
	if err != nil {
		// chat SSE 缓冲
		if anth := OpenAISSEToAnthropicSSE(body, model); len(anth) > 0 && strings.Contains(string(anth), "message_start") {
			// 调用方若要 JSON，从 SSE 不方便；这里仍返回 error 让 SSE 入口处理
			return nil, err
		}
		return nil, err
	}
	return OpenAIToAnthropicResponse(chat, model)
}

// ResponsesStreamOrJSONToAnthropicSSE 将 Responses SSE/JSON 转为 Anthropic Messages SSE。
// 供入站 /v1/messages + 上游 /v1/responses 的流式路径使用。
// 旧逻辑只回非流式 Anthropic JSON，Claude 客户端无法解析，表现为转换异常/无输出。
func ResponsesStreamOrJSONToAnthropicSSE(body []byte, model string) ([]byte, error) {
	// 1) 标准 Responses 对象
	if resp := extractResponsesObject(body); resp != nil {
		if anth, err := responsesObjectToAnthropic(resp, model); err == nil && len(anth) > 0 {
			return AnthropicMessageToSSE(anth), nil
		}
	}
	// 2) 上游误回 chat.completion / chat SSE（兼容网关）
	if chat, err := tryParseAsChatCompletion(body); err == nil && chat != nil {
		if anth, err2 := OpenAIToAnthropicResponse(mustJSON(chat), model); err2 == nil {
			return AnthropicMessageToSSE(anth), nil
		}
	}
	// chat SSE → Anthropic SSE 增量组装
	if looksLikeOpenAIChatSSE(body) {
		out := OpenAISSEToAnthropicSSE(body, model)
		if len(out) > 0 && strings.Contains(string(out), "message_start") {
			return out, nil
		}
	}
	// 3) 旧路径
	anth, err := ResponsesToAnthropicResponse(body, model)
	if err != nil {
		return nil, err
	}
	return AnthropicMessageToSSE(anth), nil
}

// responsesObjectToAnthropic 直接从 Response.output 组装 Anthropic message。
// 比 chat 中转多保留 reasoning→thinking。
func responsesObjectToAnthropic(in map[string]any, model string) ([]byte, error) {
	if in == nil {
		return nil, fmt.Errorf("nil responses object")
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	id, _ := in["id"].(string)
	if id == "" {
		id = "msg_converted"
	}
	// Anthropic 客户端习惯 msg_ 前缀；resp_ 多数也能接受，这里保留原 id

	var blocks []any
	outputs, _ := in["output"].([]any)
	for _, raw := range outputs {
		item, _ := raw.(map[string]any)
		if item == nil {
			continue
		}
		switch item["type"] {
		case "reasoning":
			var thinking string
			if summary, ok := item["summary"].([]any); ok {
				var parts []string
				for _, s := range summary {
					sm, _ := s.(map[string]any)
					if sm == nil {
						continue
					}
					if t, _ := sm["type"].(string); t == "summary_text" || t == "" {
						if txt, ok := sm["text"].(string); ok && txt != "" {
							parts = append(parts, txt)
						}
					}
				}
				thinking = strings.Join(parts, "")
			}
			if thinking == "" {
				if t, ok := item["content"].(string); ok {
					thinking = t
				}
			}
			if thinking != "" {
				blocks = append(blocks, map[string]any{"type": "thinking", "thinking": thinking})
			}
		case "message":
			if c, ok := item["content"].([]any); ok {
				for _, p := range c {
					pm, _ := p.(map[string]any)
					if pm == nil {
						continue
					}
					pt, _ := pm["type"].(string)
					if pt == "output_text" || pt == "text" {
						if s, ok := pm["text"].(string); ok && s != "" {
							blocks = append(blocks, map[string]any{"type": "text", "text": s})
						}
					}
				}
			} else if s, ok := item["content"].(string); ok && s != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": s})
			}
		case "function_call", "tool_call":
			name, _ := item["name"].(string)
			args := item["arguments"]
			var input any = map[string]any{}
			switch a := args.(type) {
			case string:
				if strings.TrimSpace(a) != "" {
					_ = json.Unmarshal([]byte(a), &input)
				}
			case map[string]any:
				input = a
			default:
				if a != nil {
					if b, err := json.Marshal(a); err == nil {
						_ = json.Unmarshal(b, &input)
					}
				}
			}
			callID, _ := item["call_id"].(string)
			if callID == "" {
				callID, _ = item["id"].(string)
			}
			callID = fromResponsesCallID(callID)
			blocks = append(blocks, map[string]any{
				"type":  "tool_use",
				"id":    callID,
				"name":  name,
				"input": input,
			})
		}
	}
	// 便捷字段 output_text（无 message 块时兜底）
	if len(blocks) == 0 {
		if t, ok := in["output_text"].(string); ok && t != "" {
			blocks = append(blocks, map[string]any{"type": "text", "text": t})
		}
	}
	if len(blocks) == 0 {
		blocks = []any{map[string]any{"type": "text", "text": ""}}
	}

	stopReason := "end_turn"
	hasTool := false
	for _, b := range blocks {
		if bm, _ := b.(map[string]any); bm != nil && bm["type"] == "tool_use" {
			hasTool = true
			break
		}
	}
	if hasTool {
		stopReason = "tool_use"
	} else if st, ok := in["status"].(string); ok && st == "incomplete" {
		stopReason = "max_tokens"
	}

	usage := map[string]any{}
	if u, ok := in["usage"].(map[string]any); ok {
		usage = openAIUsageToAnthropic(u)
	}

	out := map[string]any{
		"id":          id,
		"type":        "message",
		"role":        "assistant",
		"model":       model,
		"content":     blocks,
		"stop_reason": stopReason,
		"usage":       usage,
	}
	return json.Marshal(out)
}

// fromResponsesCallID 兼容旧 fc_toolu_ / fc_call_ 前缀，当前 id 原样返回。
func fromResponsesCallID(id string) string {
	if after, ok := strings.CutPrefix(id, "fc_"); ok {
		if strings.HasPrefix(after, "toolu_") || strings.HasPrefix(after, "call_") {
			return after
		}
	}
	return id
}

func tryParseAsChatCompletion(body []byte) (map[string]any, error) {
	trim := bytesTrimSpace(body)
	if len(trim) == 0 {
		return nil, fmt.Errorf("empty")
	}
	// 纯 JSON chat.completion
	if trim[0] == '{' {
		var m map[string]any
		if err := json.Unmarshal(trim, &m); err == nil {
			if obj, _ := m["object"].(string); obj == "chat.completion" || m["choices"] != nil {
				if _, hasErr := m["error"]; !hasErr {
					return m, nil
				}
			}
		}
	}
	// 从 SSE 组装：优先带 usage 的最后一包，或聚合 delta
	// 简化：若有完整 chat.completion 行则用它；否则用已有 OpenAI SSE→非流式不可得时返回 error
	// 这里用 ResponsesStreamOrJSON 同类思路：找 choices.message
	var best map[string]any
	for _, ev := range parseSSEEvents(string(body)) {
		data := strings.TrimSpace(ev.Data)
		if data == "" || data == "[DONE]" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(data), &m); err != nil {
			continue
		}
		if obj, _ := m["object"].(string); obj == "chat.completion" {
			best = m
			continue
		}
		// chat.completion.chunk 无法单帧当完整 message，跳过（交给 OpenAISSEToAnthropicSSE）
		if _, ok := m["choices"].([]any); ok {
			if obj, _ := m["object"].(string); obj == "chat.completion.chunk" {
				continue
			}
			// 有些网关省略 object
			if ch, ok := m["choices"].([]any); ok && len(ch) > 0 {
				if c0, _ := ch[0].(map[string]any); c0 != nil {
					if _, hasMsg := c0["message"]; hasMsg {
						best = m
					}
				}
			}
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, fmt.Errorf("not chat completion")
}

func looksLikeOpenAIChatSSE(body []byte) bool {
	s := string(body)
	return strings.Contains(s, "chat.completion.chunk") || strings.Contains(s, `"object":"chat.completion"`)
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

// AnthropicMessageToSSE 将非流式 Anthropic message JSON 拆成标准 SSE 事件流。
// 支持 text + tool_use 多 content block。
func AnthropicMessageToSSE(messageJSON []byte) []byte {
	var msg map[string]any
	if err := json.Unmarshal(messageJSON, &msg); err != nil {
		return messageJSON
	}
	// 已是 error 事件包装则原样
	if t, _ := msg["type"].(string); t == "error" {
		return encodeSSEFrame("error", msg)
	}

	id, _ := msg["id"].(string)
	if id == "" {
		id = "msg_converted"
	}
	model, _ := msg["model"].(string)
	stopReason, _ := msg["stop_reason"].(string)
	if stopReason == "" {
		stopReason = "end_turn"
	}
	usageIn, usageOut := 0, 0
	if u, ok := msg["usage"].(map[string]any); ok {
		if v, ok := asInt(u["input_tokens"]); ok {
			usageIn = v
		}
		if v, ok := asInt(u["output_tokens"]); ok {
			usageOut = v
		}
	}

	var frames [][]byte
	frames = append(frames, encodeSSEFrame("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            id,
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage":         map[string]any{"input_tokens": usageIn, "output_tokens": 0},
		},
	}))

	blocks, _ := msg["content"].([]any)
	if len(blocks) == 0 {
		// 至少开一个空 text block，兼容只返回 usage 的空完成
		blocks = []any{map[string]any{"type": "text", "text": ""}}
	}

	for i, raw := range blocks {
		bm, _ := raw.(map[string]any)
		if bm == nil {
			continue
		}
		typ, _ := bm["type"].(string)
		switch typ {
		case "tool_use":
			id, _ := bm["id"].(string)
			name, _ := bm["name"].(string)
			input := bm["input"]
			if input == nil {
				input = map[string]any{}
			}
			frames = append(frames, encodeSSEFrame("content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": i,
				"content_block": map[string]any{
					"type":  "tool_use",
					"id":    id,
					"name":  name,
					"input": map[string]any{},
				},
			}))
			partial, _ := json.Marshal(input)
			frames = append(frames, encodeSSEFrame("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": i,
				"delta": map[string]any{
					"type":         "input_json_delta",
					"partial_json": string(partial),
				},
			}))
			frames = append(frames, encodeSSEFrame("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": i,
			}))
		default: // text / thinking 等按 text 处理
			text, _ := bm["text"].(string)
			if text == "" && typ != "text" {
				if t, ok := bm["thinking"].(string); ok {
					text = t
				}
			}
			frames = append(frames, encodeSSEFrame("content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": i,
				"content_block": map[string]any{
					"type": "text",
					"text": "",
				},
			}))
			if text != "" {
				frames = append(frames, encodeSSEFrame("content_block_delta", map[string]any{
					"type":  "content_block_delta",
					"index": i,
					"delta": map[string]any{
						"type": "text_delta",
						"text": text,
					},
				}))
			}
			frames = append(frames, encodeSSEFrame("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": i,
			}))
		}
	}

	frames = append(frames, encodeSSEFrame("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   stopReason,
			"stop_sequence": nil,
		},
		"usage": map[string]any{"output_tokens": usageOut},
	}))
	frames = append(frames, encodeSSEFrame("message_stop", map[string]any{"type": "message_stop"}))
	return JoinSSEFrames(frames)
}

func extractResponsesOutput(in map[string]any) (text string, toolCalls []any) {
	// 优先 output_text 便捷字段
	if t, ok := in["output_text"].(string); ok && strings.TrimSpace(t) != "" {
		text = t
	}
	outputs, _ := in["output"].([]any)
	var parts []string
	var thinkingParts []string
	for _, raw := range outputs {
		item, _ := raw.(map[string]any)
		if item == nil {
			continue
		}
		typ, _ := item["type"].(string)
		switch typ {
		case "reasoning":
			// chat 路径无原生 thinking 字段时，把 summary 拼进 content 前缀（可选可见）
			if summary, ok := item["summary"].([]any); ok {
				for _, s := range summary {
					sm, _ := s.(map[string]any)
					if sm == nil {
						continue
					}
					if txt, ok := sm["text"].(string); ok && txt != "" {
						thinkingParts = append(thinkingParts, txt)
					}
				}
			}
		case "message":
			if text == "" {
				if c, ok := item["content"].([]any); ok {
					for _, p := range c {
						pm, _ := p.(map[string]any)
						if pm == nil {
							continue
						}
						pt, _ := pm["type"].(string)
						if pt == "output_text" || pt == "text" {
							if s, ok := pm["text"].(string); ok {
								parts = append(parts, s)
							}
						}
					}
				} else if s, ok := item["content"].(string); ok {
					parts = append(parts, s)
				}
			}
		case "function_call", "tool_call":
			name, _ := item["name"].(string)
			args := item["arguments"]
			argStr := ""
			switch a := args.(type) {
			case string:
				argStr = a
			default:
				if b, err := json.Marshal(a); err == nil {
					argStr = string(b)
				}
			}
			id, _ := item["call_id"].(string)
			if id == "" {
				id, _ = item["id"].(string)
			}
			id = fromResponsesCallID(id)
			toolCalls = append(toolCalls, map[string]any{
				"id":   id,
				"type": "function",
				"function": map[string]any{
					"name":      name,
					"arguments": argStr,
				},
			})
		}
	}
	if text == "" && len(parts) > 0 {
		text = strings.Join(parts, "")
	}
	// 仅有 reasoning 无 message 时，把 summary 作为可见文本，避免空 content
	if text == "" && len(thinkingParts) > 0 {
		text = strings.Join(thinkingParts, "")
	}
	return text, toolCalls
}

func responsesFinishReason(in map[string]any, hasToolCalls bool) string {
	// 工具调用优先：否则客户端拿到 finish_reason=stop + 空 content，表现为无工具输出
	if hasToolCalls {
		return "tool_calls"
	}
	if s, ok := in["status"].(string); ok {
		switch s {
		case "completed":
			return "stop"
		case "incomplete":
			return "length"
		case "failed":
			return "stop"
		}
	}
	return "stop"
}

func contentToPlainTextForResponses(content any) string {
	switch c := content.(type) {
	case string:
		return c
	case []any:
		var parts []string
		for _, p := range c {
			pm, _ := p.(map[string]any)
			if pm == nil {
				continue
			}
			if t, ok := pm["text"].(string); ok {
				parts = append(parts, t)
			} else if t, ok := pm["content"].(string); ok {
				parts = append(parts, t)
			}
		}
		return strings.Join(parts, "\n")
	default:
		return ""
	}
}
