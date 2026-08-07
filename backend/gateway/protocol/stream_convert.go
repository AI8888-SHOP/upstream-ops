package protocol

import (
	"encoding/json"
	"strings"
)

// SupportsIncrementalStream 是否支持事件级增量 SSE 转换（或透传）。
// 三协议矩阵（Chat / Responses / Anthropic）互转均走真流，禁止假流缓冲。
func SupportsIncrementalStream(inbound, upstream Kind, converted bool) bool {
	if !converted {
		return true
	}
	in, up := NormalizeKind(inbound), NormalizeKind(upstream)
	// 任意 OpenAI Chat / Responses / Anthropic 互转均可增量
	okIn := in == KindOpenAIChat || in == KindOpenAI || in == KindOpenAIResponses || in == KindAnthropic
	okUp := up == KindOpenAIChat || up == KindOpenAI || up == KindOpenAIResponses || up == KindAnthropic
	return okIn && okUp
}

// AnthropicToOpenAIStream 将 Anthropic SSE 事件增量转为 OpenAI chat.completion.chunk SSE。
type AnthropicToOpenAIStream struct {
	Model    string
	MsgID    string
	RoleSent bool
	done     bool
	// content block index → tool_calls index（仅 tool_use）
	toolIndex map[int]int
	nextTool  int
	finish    any
	usage     map[string]any
}

func NewAnthropicToOpenAIStream(model string) *AnthropicToOpenAIStream {
	return &AnthropicToOpenAIStream{
		Model:     model,
		MsgID:     "chatcmpl-stream",
		toolIndex: make(map[int]int),
	}
}

// Feed 处理一个完整 SSE 事件的 data 载荷（及可选 event 名）。
func (s *AnthropicToOpenAIStream) Feed(eventName, data string) [][]byte {
	if s == nil || s.done {
		return nil
	}
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return nil
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return nil
	}
	typ, _ := payload["type"].(string)
	if typ == "" && eventName != "" {
		typ = eventName
	}
	var out [][]byte
	switch typ {
	case "message_start":
		if msg, ok := payload["message"].(map[string]any); ok {
			if id, ok := msg["id"].(string); ok && id != "" {
				s.MsgID = id
			}
			if m, ok := msg["model"].(string); ok && m != "" {
				s.Model = m
			}
			if usage, ok := msg["usage"].(map[string]any); ok {
				s.usage = usage
			}
		}
		if !s.RoleSent {
			s.RoleSent = true
			out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
				"role":    "assistant",
				"content": "",
			}, nil)))
		}
	case "content_block_start":
		block, _ := payload["content_block"].(map[string]any)
		if block == nil {
			return nil
		}
		bt, _ := block["type"].(string)
		blockIdx, _ := asInt(payload["index"])
		switch bt {
		case "tool_use":
			id, _ := block["id"].(string)
			name, _ := block["name"].(string)
			ti := s.nextTool
			s.toolIndex[blockIdx] = ti
			s.nextTool++
			if !s.RoleSent {
				s.RoleSent = true
				out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
					"role": "assistant",
				}, nil)))
			}
			out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
				"tool_calls": []any{
					map[string]any{
						"index": ti,
						"id":    id,
						"type":  "function",
						"function": map[string]any{
							"name":      name,
							"arguments": "",
						},
					},
				},
			}, nil)))
		}
	case "content_block_delta":
		delta, _ := payload["delta"].(map[string]any)
		if delta == nil {
			return nil
		}
		dt, _ := delta["type"].(string)
		blockIdx, _ := asInt(payload["index"])
		switch dt {
		case "text_delta":
			text, _ := delta["text"].(string)
			if text == "" {
				return nil
			}
			if !s.RoleSent {
				s.RoleSent = true
				out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
					"role":    "assistant",
					"content": "",
				}, nil)))
			}
			out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{"content": text}, nil)))
		case "input_json_delta":
			partial, _ := delta["partial_json"].(string)
			if partial == "" {
				return nil
			}
			ti, ok := s.toolIndex[blockIdx]
			if !ok {
				ti = 0
			}
			out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
				"tool_calls": []any{
					map[string]any{
						"index": ti,
						"function": map[string]any{
							"arguments": partial,
						},
					},
				},
			}, nil)))
		case "thinking_delta":
			// chat 侧无标准 thinking 字段时，可选映射 reasoning_content
			text, _ := delta["thinking"].(string)
			if text == "" {
				return nil
			}
			out = append(out, openAISSEFrame(openaiChunk(s.MsgID, s.Model, map[string]any{
				"reasoning_content": text,
			}, nil)))
		}
	case "message_delta":
		delta, _ := payload["delta"].(map[string]any)
		var finish any
		if delta != nil {
			if sr, ok := delta["stop_reason"].(string); ok {
				finish = mapStopReasonToOpenAI(sr)
				s.finish = finish
			}
		}
		if s.usage == nil {
			s.usage = make(map[string]any)
		}
		if u, ok := payload["usage"].(map[string]any); ok {
			for key, value := range u {
				s.usage[key] = value
			}
		}
		usageDelta := anthropicUsageToOpenAI(s.usage)
		chunkMap := map[string]any{
			"id":      s.MsgID,
			"object":  "chat.completion.chunk",
			"created": 0,
			"model":   s.Model,
			"choices": []any{
				map[string]any{
					"index":         0,
					"delta":         map[string]any{},
					"finish_reason": finish,
				},
			},
		}
		if len(usageDelta) > 0 {
			chunkMap["usage"] = usageDelta
		}
		raw, _ := json.Marshal(chunkMap)
		out = append(out, openAISSEFrame(raw))
	case "message_stop", "error":
		// message_stop: Close 负责 [DONE]
	}
	return out
}

// Close 结束流并输出 [DONE]（仅一次）。
func (s *AnthropicToOpenAIStream) Close() [][]byte {
	if s == nil || s.done {
		return nil
	}
	s.done = true
	return [][]byte{[]byte("data: [DONE]\n\n")}
}

// OpenAIToAnthropicStream 将 OpenAI chat SSE 增量转为 Anthropic SSE。
// 支持 text + tool_calls（真流，不预开 text block）。
type OpenAIToAnthropicStream struct {
	Model   string
	MsgID   string
	Finish  string
	Started bool
	done    bool

	// 当前内容块
	textOpen     bool
	textIndex    int
	nextBlockIdx int
	// chat tool index → anthropic content block index
	toolBlockIdx map[int]int
	toolOpened   map[int]bool
	usage        map[string]any
}

func NewOpenAIToAnthropicStream(model string) *OpenAIToAnthropicStream {
	return &OpenAIToAnthropicStream{
		Model:        model,
		MsgID:        "msg_stream",
		toolBlockIdx: make(map[int]int),
		toolOpened:   make(map[int]bool),
	}
}

// EnsureStarted 只写 message_start（不再预开 text block，避免纯 tool 流多出空 text）。
func (s *OpenAIToAnthropicStream) EnsureStarted() [][]byte {
	if s == nil || s.Started || s.done {
		return nil
	}
	s.Started = true
	return [][]byte{encodeSSEFrame("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":            s.MsgID,
			"type":          "message",
			"role":          "assistant",
			"model":         s.Model,
			"content":       []any{},
			"stop_reason":   nil,
			"stop_sequence": nil,
			"usage":         map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	})}
}

// FeedData 处理 OpenAI SSE 的 data 载荷（不含 "data: " 前缀）。
func (s *OpenAIToAnthropicStream) FeedData(data string) [][]byte {
	if s == nil || s.done {
		return nil
	}
	data = strings.TrimSpace(data)
	if data == "" || data == "[DONE]" {
		return nil
	}
	var out [][]byte
	if !s.Started {
		out = append(out, s.EnsureStarted()...)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return out
	}
	if id, ok := payload["id"].(string); ok && id != "" {
		s.MsgID = id
	}
	if m, ok := payload["model"].(string); ok && m != "" {
		s.Model = m
	}
	if usage, ok := payload["usage"].(map[string]any); ok {
		s.usage = usage
	}
	choices, _ := payload["choices"].([]any)
	if len(choices) == 0 {
		return out
	}
	ch, _ := choices[0].(map[string]any)
	if ch == nil {
		return out
	}
	if fr, ok := ch["finish_reason"].(string); ok && fr != "" {
		s.Finish = fr
	}
	delta, _ := ch["delta"].(map[string]any)
	if delta == nil {
		return out
	}
	// text
	if text, ok := delta["content"].(string); ok && text != "" {
		if !s.textOpen {
			s.textIndex = s.nextBlockIdx
			s.nextBlockIdx++
			s.textOpen = true
			out = append(out, encodeSSEFrame("content_block_start", map[string]any{
				"type":  "content_block_start",
				"index": s.textIndex,
				"content_block": map[string]any{
					"type": "text",
					"text": "",
				},
			}))
		}
		out = append(out, encodeSSEFrame("content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": s.textIndex,
			"delta": map[string]any{
				"type": "text_delta",
				"text": text,
			},
		}))
	}
	// tool_calls
	if tcs, ok := delta["tool_calls"].([]any); ok {
		// 新 tool 前关闭 text block
		for _, raw := range tcs {
			tc, _ := raw.(map[string]any)
			if tc == nil {
				continue
			}
			tIdx, _ := asInt(tc["index"])
			if !s.toolOpened[tIdx] {
				if s.textOpen {
					out = append(out, encodeSSEFrame("content_block_stop", map[string]any{
						"type":  "content_block_stop",
						"index": s.textIndex,
					}))
					s.textOpen = false
				}
				bIdx := s.nextBlockIdx
				s.nextBlockIdx++
				s.toolBlockIdx[tIdx] = bIdx
				s.toolOpened[tIdx] = true
				id, _ := tc["id"].(string)
				name := ""
				if fn, ok := tc["function"].(map[string]any); ok {
					name, _ = fn["name"].(string)
				}
				out = append(out, encodeSSEFrame("content_block_start", map[string]any{
					"type":  "content_block_start",
					"index": bIdx,
					"content_block": map[string]any{
						"type":  "tool_use",
						"id":    id,
						"name":  name,
						"input": map[string]any{},
					},
				}))
			}
			bIdx := s.toolBlockIdx[tIdx]
			if fn, ok := tc["function"].(map[string]any); ok {
				// 迟到的 name：Anthropic 无单独 name delta，忽略
				if args, ok := fn["arguments"].(string); ok && args != "" {
					out = append(out, encodeSSEFrame("content_block_delta", map[string]any{
						"type":  "content_block_delta",
						"index": bIdx,
						"delta": map[string]any{
							"type":         "input_json_delta",
							"partial_json": args,
						},
					}))
				}
			}
		}
	}
	return out
}

// Close 写出 stop 系列事件。
func (s *OpenAIToAnthropicStream) Close() [][]byte {
	if s == nil || s.done {
		return nil
	}
	var out [][]byte
	if !s.Started {
		out = append(out, s.EnsureStarted()...)
	}
	s.done = true
	if s.textOpen {
		out = append(out, encodeSSEFrame("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": s.textIndex,
		}))
		s.textOpen = false
	}
	for tIdx, bIdx := range s.toolBlockIdx {
		if s.toolOpened[tIdx] {
			out = append(out, encodeSSEFrame("content_block_stop", map[string]any{
				"type":  "content_block_stop",
				"index": bIdx,
			}))
			s.toolOpened[tIdx] = false
		}
	}
	// 若全程无内容，补一个空 text block（Anthropic 要求 content 非空列表更稳）
	if s.nextBlockIdx == 0 {
		out = append(out, encodeSSEFrame("content_block_start", map[string]any{
			"type":          "content_block_start",
			"index":         0,
			"content_block": map[string]any{"type": "text", "text": ""},
		}))
		out = append(out, encodeSSEFrame("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": 0,
		}))
	}
	usage := map[string]any{"output_tokens": 0}
	if s.usage != nil {
		usage = openAIUsageToAnthropic(s.usage)
	}
	out = append(out, encodeSSEFrame("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   mapFinishReasonToAnthropic(s.Finish),
			"stop_sequence": nil,
		},
		"usage": usage,
	}))
	out = append(out, encodeSSEFrame("message_stop", map[string]any{"type": "message_stop"}))
	return out
}

func openAIUsageToAnthropic(usage map[string]any) map[string]any {
	totalInput := openAIInputTokens(usage)
	output := openAIOutputTokens(usage)
	cacheRead := openAICachedTokens(usage)
	cacheCreation := openAICacheCreationTokens(usage)
	if cacheCreation == 0 {
		fiveMinute, oneHour := anthropicCacheCreationBreakdown(usage)
		cacheCreation = fiveMinute + oneHour
	}
	fresh := totalInput - cacheRead - cacheCreation
	if fresh < 0 {
		fresh = 0
	}
	out := map[string]any{
		"input_tokens":                fresh,
		"output_tokens":               output,
		"cache_read_input_tokens":     cacheRead,
		"cache_creation_input_tokens": cacheCreation,
	}
	copyAnthropicCacheCreationBreakdown(out, usage)
	return out
}

// OpenAI-compatible providers use both Chat and Responses aliases. Match the
// downstream Sub2API parser: input_tokens/output_tokens win when positive,
// with prompt_tokens/completion_tokens as compatibility fallbacks.
func openAIInputTokens(usage map[string]any) int {
	if value, ok := asInt(usage["input_tokens"]); ok && value > 0 {
		return value
	}
	value, _ := asInt(usage["prompt_tokens"])
	return value
}

func openAIOutputTokens(usage map[string]any) int {
	if value, ok := asInt(usage["output_tokens"]); ok && value > 0 {
		return value
	}
	value, _ := asInt(usage["completion_tokens"])
	return value
}

func openAICachedTokens(usage map[string]any) int {
	for _, key := range []string{"input_tokens_details", "prompt_tokens_details"} {
		if details, ok := usage[key].(map[string]any); ok {
			if value, ok := asInt(details["cached_tokens"]); ok && value > 0 {
				return value
			}
		}
	}
	for _, key := range []string{"cache_read_input_tokens", "cache_read_tokens", "cached_tokens"} {
		if value, ok := asInt(usage[key]); ok && value > 0 {
			return value
		}
	}
	return 0
}

func openAICacheCreationTokens(usage map[string]any) int {
	for _, nested := range [][2]string{
		{"input_tokens_details", "cache_write_tokens"},
		{"prompt_tokens_details", "cache_write_tokens"},
		{"input_tokens_details", "cache_creation_tokens"},
		{"prompt_tokens_details", "cache_creation_tokens"},
	} {
		if details, ok := usage[nested[0]].(map[string]any); ok {
			if value, exists := details[nested[1]]; exists {
				if parsed, ok := asInt(value); ok && parsed > 0 {
					return parsed
				}
			}
		}
	}
	for _, key := range []string{"cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens", "cache_write_input_tokens"} {
		if value, ok := asInt(usage[key]); ok && value > 0 {
			return value
		}
	}
	return 0
}

func copyUsageDetails(dst map[string]any, target string, usage map[string]any) {
	for _, key := range []string{"input_tokens_details", "prompt_tokens_details"} {
		if details, ok := usage[key].(map[string]any); ok {
			copied := make(map[string]any, len(details))
			for field, value := range details {
				copied[field] = value
			}
			if cached := openAICachedTokens(usage); cached > 0 {
				copied["cached_tokens"] = cached
			}
			if created := openAICacheCreationTokens(usage); created > 0 {
				// Sub2API checks nested cache_write_tokens before flat
				// compatibility aliases, including an explicit stale zero.
				copied["cache_write_tokens"] = created
			}
			dst[target] = copied
			return
		}
	}
	details := make(map[string]any, 2)
	if cached := openAICachedTokens(usage); cached > 0 {
		details["cached_tokens"] = cached
	}
	if created := openAICacheCreationTokens(usage); created > 0 {
		details["cache_write_tokens"] = created
	}
	if len(details) > 0 {
		dst[target] = details
	}
}

func openAIUsageToResponses(usage map[string]any) map[string]any {
	if usage == nil {
		return nil
	}
	input := openAIInputTokens(usage)
	output := openAIOutputTokens(usage)
	out := map[string]any{
		"input_tokens":  input,
		"output_tokens": output,
		"total_tokens":  input + output,
	}
	copyUsageDetails(out, "input_tokens_details", usage)
	copyOpenAICacheUsageFields(out, usage)
	return out
}

func openAIUsageToChat(usage map[string]any) map[string]any {
	if usage == nil {
		return nil
	}
	input := openAIInputTokens(usage)
	output := openAIOutputTokens(usage)
	out := map[string]any{
		"prompt_tokens":     input,
		"completion_tokens": output,
		"total_tokens":      input + output,
	}
	copyUsageDetails(out, "prompt_tokens_details", usage)
	copyOpenAICacheUsageFields(out, usage)
	return out
}

func copyAnthropicCacheCreationBreakdown(dst, src map[string]any) {
	if dst == nil || src == nil {
		return
	}
	if breakdown, ok := src["cache_creation"].(map[string]any); ok {
		copied := make(map[string]any, len(breakdown))
		for key, value := range breakdown {
			copied[key] = value
		}
		dst["cache_creation"] = copied
		return
	}
	fiveMinute := firstPositiveUsageInt(src, "cache_creation_5m_input_tokens", "cache_creation_5m_tokens")
	oneHour := firstPositiveUsageInt(src, "cache_creation_1h_input_tokens", "cache_creation_1h_tokens")
	if fiveMinute > 0 || oneHour > 0 {
		dst["cache_creation"] = map[string]any{
			"ephemeral_5m_input_tokens": fiveMinute,
			"ephemeral_1h_input_tokens": oneHour,
		}
	}
}

func firstPositiveUsageInt(usage map[string]any, keys ...string) int {
	for _, key := range keys {
		if value, ok := asInt(usage[key]); ok && value > 0 {
			return value
		}
	}
	return 0
}

func openAISSEFrame(chunkJSON []byte) []byte {
	var b strings.Builder
	b.WriteString("data: ")
	b.Write(chunkJSON)
	b.WriteString("\n\n")
	return []byte(b.String())
}

func encodeSSEFrame(event string, payload any) []byte {
	var b strings.Builder
	writeSSE(&b, event, payload)
	return []byte(b.String())
}

// JoinSSEFrames 拼接多个完整 SSE frame。
func JoinSSEFrames(frames [][]byte) []byte {
	if len(frames) == 0 {
		return nil
	}
	var n int
	for _, f := range frames {
		n += len(f)
	}
	out := make([]byte, 0, n)
	for _, f := range frames {
		out = append(out, f...)
	}
	return out
}
