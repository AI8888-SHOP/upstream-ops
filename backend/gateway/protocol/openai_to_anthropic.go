package protocol

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
)

// OpenAIToAnthropicRequest 将 OpenAI chat/completions 请求转为 Anthropic /v1/messages。
func OpenAIToAnthropicRequest(body []byte, model string, stream bool) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid openai request: %w", err)
	}
	out := map[string]any{}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	out["model"] = model
	out["stream"] = stream

	// max_tokens: Claude 必填
	maxTokens := 4096
	if v, ok := asInt(in["max_tokens"]); ok && v > 0 {
		maxTokens = v
	} else if v, ok := asInt(in["max_completion_tokens"]); ok && v > 0 {
		maxTokens = v
	}
	out["max_tokens"] = maxTokens

	if v, ok := asFloat(in["temperature"]); ok {
		out["temperature"] = v
	}
	if v, ok := asFloat(in["top_p"]); ok {
		out["top_p"] = v
	}
	if v, ok := in["stop"]; ok {
		out["stop_sequences"] = normalizeStop(v)
	}

	// system + messages
	var systemParts []any
	var messages []any
	rawMsgs, _ := in["messages"].([]any)
	for _, raw := range rawMsgs {
		msg, _ := raw.(map[string]any)
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		switch role {
		case "system", "developer":
			systemParts = append(systemParts, contentToAnthropicBlocks(msg["content"])...)
		case "user":
			messages = append(messages, map[string]any{
				"role":    "user",
				"content": contentToAnthropicBlocks(msg["content"]),
			})
		case "assistant":
			blocks := contentToAnthropicBlocks(msg["content"])
			// tool_calls → tool_use blocks
			if tcs, ok := msg["tool_calls"].([]any); ok {
				for _, tc := range tcs {
					tcm, _ := tc.(map[string]any)
					if tcm == nil {
						continue
					}
					id, _ := tcm["id"].(string)
					fn, _ := tcm["function"].(map[string]any)
					name, _ := fn["name"].(string)
					argsStr, _ := fn["arguments"].(string)
					var input any = map[string]any{}
					if strings.TrimSpace(argsStr) != "" {
						_ = json.Unmarshal([]byte(argsStr), &input)
					}
					blocks = append(blocks, map[string]any{
						"type":  "tool_use",
						"id":    id,
						"name":  name,
						"input": input,
					})
				}
			}
			if len(blocks) == 0 {
				blocks = []any{map[string]any{"type": "text", "text": ""}}
			}
			messages = append(messages, map[string]any{
				"role":    "assistant",
				"content": blocks,
			})
		case "tool":
			toolCallID, _ := msg["tool_call_id"].(string)
			content := contentToPlainText(msg["content"])
			// Anthropic 要求 tool_result 放在 user 消息 content 里
			messages = append(messages, map[string]any{
				"role": "user",
				"content": []any{
					map[string]any{
						"type":        "tool_result",
						"tool_use_id": toolCallID,
						"content":     content,
					},
				},
			})
		}
	}
	if len(systemParts) > 0 {
		// 简化：拼成 text 或 blocks
		if len(systemParts) == 1 {
			if b, ok := systemParts[0].(map[string]any); ok && b["type"] == "text" {
				out["system"] = b["text"]
			} else {
				out["system"] = systemParts
			}
		} else {
			out["system"] = systemParts
		}
	}
	// Anthropic 要求 user/assistant 严格交替；并行 tool 结果会变成连续 user。
	out["messages"] = mergeConsecutiveAnthropicMessages(messages)

	// tools
	if tools, ok := in["tools"].([]any); ok && len(tools) > 0 {
		aTools := make([]any, 0, len(tools))
		for _, t := range tools {
			tm, _ := t.(map[string]any)
			if tm == nil {
				continue
			}
			// OpenAI: {type:function, function:{name,description,parameters}}
			if fn, ok := tm["function"].(map[string]any); ok {
				aTools = append(aTools, map[string]any{
					"name":         fn["name"],
					"description":  fn["description"],
					"input_schema": normalizeSchemaForAnthropic(fn["parameters"]),
				})
				continue
			}
			// 已是 Anthropic / Responses 扁平：name + parameters|input_schema
			if name, ok := tm["name"].(string); ok && name != "" {
				params := tm["input_schema"]
				if params == nil {
					params = tm["parameters"]
				}
				aTools = append(aTools, map[string]any{
					"name":         name,
					"description":  tm["description"],
					"input_schema": normalizeSchemaForAnthropic(params),
				})
			}
		}
		if len(aTools) > 0 {
			out["tools"] = aTools
		}
	}
	if tc, ok := in["tool_choice"]; ok {
		out["tool_choice"] = convertToolChoiceOpenAIToAnthropic(tc)
	}

	// reasoning_effort → thinking (best effort)
	if effort, ok := in["reasoning_effort"].(string); ok && effort != "" {
		out["thinking"] = map[string]any{
			"type":          "enabled",
			"budget_tokens": effortToBudget(effort),
		}
	}

	return json.Marshal(out)
}

// AnthropicToOpenAIResponse 将 Anthropic 非流式响应转为 OpenAI chat.completion。
func AnthropicToOpenAIResponse(body []byte, model string) ([]byte, error) {
	var in map[string]any
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, err
	}
	// 若已是 error 形态
	if t, _ := in["type"].(string); t == "error" {
		return openaiErrorFromAnthropic(in)
	}
	id, _ := in["id"].(string)
	if id == "" {
		id = "chatcmpl-converted"
	}
	stopReason, _ := in["stop_reason"].(string)
	finish := mapStopReasonToOpenAI(stopReason)

	content, toolCalls := anthropicContentToOpenAI(in["content"])
	msg := map[string]any{
		"role":    "assistant",
		"content": content,
	}
	if len(toolCalls) > 0 {
		msg["tool_calls"] = toolCalls
		if content == "" {
			msg["content"] = nil
		}
	}
	usage := map[string]any{}
	if u, ok := in["usage"].(map[string]any); ok {
		usage = anthropicUsageToOpenAI(u)
	}
	if model == "" {
		if m, ok := in["model"].(string); ok {
			model = m
		}
	}
	out := map[string]any{
		"id":      id,
		"object":  "chat.completion",
		"created": 0,
		"model":   model,
		"choices": []any{
			map[string]any{
				"index":         0,
				"message":       msg,
				"finish_reason": finish,
			},
		},
		"usage": usage,
	}
	return json.Marshal(out)
}

func anthropicUsageToOpenAI(usage map[string]any) map[string]any {
	fresh, _ := asInt(usage["input_tokens"])
	output, _ := asInt(usage["output_tokens"])
	cacheRead := firstPositiveUsageInt(usage, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens")
	cacheCreation := firstPositiveUsageInt(usage, "cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens")
	fiveMinute, oneHour := anthropicCacheCreationBreakdown(usage)
	if cacheCreation == 0 {
		cacheCreation = fiveMinute + oneHour
	}
	totalInput := fresh + cacheRead + cacheCreation
	out := map[string]any{
		"prompt_tokens":     totalInput,
		"completion_tokens": output,
		"total_tokens":      totalInput + output,
	}
	copyOpenAICacheUsageFields(out, usage)
	if cacheRead > 0 {
		out["cache_read_input_tokens"] = cacheRead
		out["prompt_tokens_details"] = map[string]any{"cached_tokens": cacheRead}
	}
	if cacheCreation > 0 {
		out["cache_creation_input_tokens"] = cacheCreation
	}
	if fiveMinute > 0 {
		out["cache_creation_5m_input_tokens"] = fiveMinute
	}
	if oneHour > 0 {
		out["cache_creation_1h_input_tokens"] = oneHour
	}
	return out
}

func anthropicCacheCreationBreakdown(usage map[string]any) (fiveMinute, oneHour int) {
	if breakdown, ok := usage["cache_creation"].(map[string]any); ok {
		fiveMinute = firstPositiveUsageInt(breakdown, "ephemeral_5m_input_tokens")
		oneHour = firstPositiveUsageInt(breakdown, "ephemeral_1h_input_tokens")
	}
	if fiveMinute == 0 {
		fiveMinute = firstPositiveUsageInt(usage, "cache_creation_5m_input_tokens", "cache_creation_5m_tokens")
	}
	if oneHour == 0 {
		oneHour = firstPositiveUsageInt(usage, "cache_creation_1h_input_tokens", "cache_creation_1h_tokens")
	}
	return fiveMinute, oneHour
}

// AnthropicSSEToOpenAISSE 将缓冲的 Anthropic SSE 转为 OpenAI chat.completion.chunk SSE。
func AnthropicSSEToOpenAISSE(sse []byte, model string) []byte {
	conv := NewAnthropicToOpenAIStream(model)
	var frames [][]byte
	for _, ev := range parseSSEEvents(string(sse)) {
		frames = append(frames, conv.Feed(ev.Event, ev.Data)...)
	}
	frames = append(frames, conv.Close()...)
	return JoinSSEFrames(frames)
}

func openaiChunk(id, model string, delta map[string]any, finish any) []byte {
	m := map[string]any{
		"id":      id,
		"object":  "chat.completion.chunk",
		"created": 0,
		"model":   model,
		"choices": []any{
			map[string]any{
				"index":         0,
				"delta":         delta,
				"finish_reason": finish,
			},
		},
	}
	raw, _ := json.Marshal(m)
	return raw
}

func openaiErrorFromAnthropic(in map[string]any) ([]byte, error) {
	errObj, _ := in["error"].(map[string]any)
	msg := "upstream error"
	typ := "api_error"
	if errObj != nil {
		if m, ok := errObj["message"].(string); ok {
			msg = m
		}
		if t, ok := errObj["type"].(string); ok {
			typ = t
		}
	}
	return json.Marshal(map[string]any{
		"error": map[string]any{
			"message": msg,
			"type":    typ,
		},
	})
}

func contentToAnthropicBlocks(content any) []any {
	switch v := content.(type) {
	case nil:
		return []any{map[string]any{"type": "text", "text": ""}}
	case string:
		return []any{map[string]any{"type": "text", "text": v}}
	case []any:
		blocks := make([]any, 0, len(v))
		for _, part := range v {
			pm, _ := part.(map[string]any)
			if pm == nil {
				continue
			}
			typ, _ := pm["type"].(string)
			switch typ {
			case "text":
				text, _ := pm["text"].(string)
				blocks = append(blocks, map[string]any{"type": "text", "text": text})
			case "image_url":
				img, _ := pm["image_url"].(map[string]any)
				url, _ := img["url"].(string)
				mediaType, data, ok := parseDataURL(url)
				if ok {
					blocks = append(blocks, map[string]any{
						"type": "image",
						"source": map[string]any{
							"type":       "base64",
							"media_type": mediaType,
							"data":       data,
						},
					})
				} else if url != "" {
					blocks = append(blocks, map[string]any{
						"type": "image",
						"source": map[string]any{
							"type": "url",
							"url":  url,
						},
					})
				}
			default:
				// ignore
			}
		}
		if len(blocks) == 0 {
			return []any{map[string]any{"type": "text", "text": ""}}
		}
		return blocks
	default:
		return []any{map[string]any{"type": "text", "text": fmt.Sprint(v)}}
	}
}

func contentToPlainText(content any) string {
	switch v := content.(type) {
	case string:
		return v
	case []any:
		var b strings.Builder
		for _, part := range v {
			pm, _ := part.(map[string]any)
			if pm == nil {
				continue
			}
			if t, _ := pm["type"].(string); t == "text" {
				if s, ok := pm["text"].(string); ok {
					b.WriteString(s)
				}
			}
		}
		return b.String()
	default:
		if v == nil {
			return ""
		}
		return fmt.Sprint(v)
	}
}

func anthropicContentToOpenAI(content any) (text string, toolCalls []any) {
	arr, ok := content.([]any)
	if !ok {
		if s, ok := content.(string); ok {
			return s, nil
		}
		return "", nil
	}
	var texts []string
	for _, part := range arr {
		pm, _ := part.(map[string]any)
		if pm == nil {
			continue
		}
		switch pm["type"] {
		case "text":
			if s, ok := pm["text"].(string); ok {
				texts = append(texts, s)
			}
		case "tool_use":
			id, _ := pm["id"].(string)
			name, _ := pm["name"].(string)
			input := pm["input"]
			args, _ := json.Marshal(input)
			toolCalls = append(toolCalls, map[string]any{
				"id":   id,
				"type": "function",
				"function": map[string]any{
					"name":      name,
					"arguments": string(args),
				},
			})
		}
	}
	return strings.Join(texts, ""), toolCalls
}

func mapStopReasonToOpenAI(sr string) any {
	switch sr {
	case "end_turn", "stop_sequence":
		return "stop"
	case "max_tokens":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "":
		return nil
	default:
		return "stop"
	}
}

func convertToolChoiceOpenAIToAnthropic(tc any) any {
	switch v := tc.(type) {
	case string:
		switch v {
		case "auto":
			return map[string]any{"type": "auto"}
		case "none":
			return map[string]any{"type": "none"}
		case "required":
			return map[string]any{"type": "any"}
		default:
			return map[string]any{"type": "auto"}
		}
	case map[string]any:
		if fn, ok := v["function"].(map[string]any); ok {
			return map[string]any{"type": "tool", "name": fn["name"]}
		}
	}
	return map[string]any{"type": "auto"}
}

func effortToBudget(effort string) int {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "low", "minimal":
		return 1024
	case "medium":
		return 8192
	case "high", "xhigh", "max":
		return 32000
	default:
		return 8192
	}
}

func normalizeStop(v any) []string {
	switch s := v.(type) {
	case string:
		if s == "" {
			return nil
		}
		return []string{s}
	case []any:
		out := make([]string, 0, len(s))
		for _, x := range s {
			if str, ok := x.(string); ok && str != "" {
				out = append(out, str)
			}
		}
		return out
	default:
		return nil
	}
}

func parseDataURL(url string) (mediaType, data string, ok bool) {
	if !strings.HasPrefix(url, "data:") {
		return "", "", false
	}
	// data:image/png;base64,xxxx
	rest := strings.TrimPrefix(url, "data:")
	parts := strings.SplitN(rest, ",", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	meta := parts[0]
	data = parts[1]
	if !strings.Contains(meta, ";base64") {
		// try encode
		data = base64.StdEncoding.EncodeToString([]byte(data))
	}
	mediaType = strings.Split(meta, ";")[0]
	if mediaType == "" {
		mediaType = "image/png"
	}
	return mediaType, data, true
}

// mergeConsecutiveAnthropicMessages 合并连续同角色消息（content 数组拼接）。
// 对齐 sub2api mergeConsecutiveMessages，满足 Anthropic 角色交替约束。
func mergeConsecutiveAnthropicMessages(messages []any) []any {
	if len(messages) <= 1 {
		return messages
	}
	var out []any
	for _, raw := range messages {
		msg, _ := raw.(map[string]any)
		if msg == nil {
			continue
		}
		role, _ := msg["role"].(string)
		if len(out) == 0 {
			out = append(out, msg)
			continue
		}
		prev, _ := out[len(out)-1].(map[string]any)
		if prev == nil {
			out = append(out, msg)
			continue
		}
		prevRole, _ := prev["role"].(string)
		if prevRole != role {
			out = append(out, msg)
			continue
		}
		// 合并 content 为数组
		prevBlocks := anthropicContentToBlockSlice(prev["content"])
		curBlocks := anthropicContentToBlockSlice(msg["content"])
		merged := append(prevBlocks, curBlocks...)
		if len(merged) == 0 {
			merged = []any{map[string]any{"type": "text", "text": ""}}
		}
		prev["content"] = merged
	}
	return out
}

func anthropicContentToBlockSlice(content any) []any {
	switch c := content.(type) {
	case string:
		if c == "" {
			return nil
		}
		return []any{map[string]any{"type": "text", "text": c}}
	case []any:
		return c
	case nil:
		return nil
	default:
		if b, err := json.Marshal(c); err == nil {
			return []any{map[string]any{"type": "text", "text": string(b)}}
		}
		return nil
	}
}

// normalizeSchemaForAnthropic 保证 input_schema 至少是 object schema。
func normalizeSchemaForAnthropic(params any) any {
	if params == nil {
		return map[string]any{"type": "object", "properties": map[string]any{}}
	}
	if m, ok := params.(map[string]any); ok {
		if _, ok := m["type"]; !ok {
			m["type"] = "object"
		}
		if _, ok := m["properties"]; !ok && m["type"] == "object" {
			m["properties"] = map[string]any{}
		}
		return m
	}
	return params
}

func asInt(v any) (int, bool) {
	switch n := v.(type) {
	case float64:
		return int(n), true
	case int:
		return n, true
	case int64:
		return int(n), true
	case json.Number:
		i, err := n.Int64()
		return int(i), err == nil
	default:
		return 0, false
	}
}

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	default:
		return 0, false
	}
}

type sseEvent struct {
	Event string
	Data  string
}

func parseSSEEvents(raw string) []sseEvent {
	var events []sseEvent
	blocks := strings.Split(raw, "\n\n")
	for _, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		var ev sseEvent
		for _, line := range strings.Split(block, "\n") {
			line = strings.TrimRight(line, "\r")
			if strings.HasPrefix(line, "event:") {
				ev.Event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
			} else if strings.HasPrefix(line, "data:") {
				d := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
				if ev.Data != "" {
					ev.Data += "\n"
				}
				ev.Data += d
			}
		}
		if ev.Data != "" && ev.Data != "[DONE]" {
			events = append(events, ev)
		}
	}
	return events
}
