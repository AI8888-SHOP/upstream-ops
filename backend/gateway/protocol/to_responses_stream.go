package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strings"
)

// ---------------------------------------------------------------------------
// Anthropic SSE → Responses SSE（真流）
// 对齐 sub2api wire：message 必须 content:[]；function_call 必须 arguments；
// response.completed.output 必须带完整 items（OpenAI JS 会对 content/output 做 .map）。
// ---------------------------------------------------------------------------

// AnthropicToResponsesStream 将 Anthropic Messages SSE 增量转为 Responses SSE。
type AnthropicToResponsesStream struct {
	Model  string
	RespID string

	createdSent   bool
	completedSent bool
	done          bool
	seq           int

	outputIndex     int
	currentItemID   string
	currentItemType string // message | function_call | reasoning
	currentCallID   string
	currentName     string
	currentText     string // 当前 message / reasoning 累积
	currentArgs     string // 当前 function_call 累积
	textPartOpen    bool
	contentIndex    int

	// 已完成的 output items（completed 事件用）
	finished []any

	inputTokens   int
	outputTokens  int
	cacheRead     int
	cacheCreate   int
	cacheCreate5m int
	cacheCreate1h int
	stopReason    string
}

func NewAnthropicToResponsesStream(model string) *AnthropicToResponsesStream {
	return &AnthropicToResponsesStream{
		Model:  model,
		RespID: generateResponsesStreamID("resp_"),
	}
}

func (s *AnthropicToResponsesStream) Feed(eventName, data string) [][]byte {
	if s == nil || s.done || s.completedSent {
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
	if typ == "" {
		typ = strings.TrimSpace(eventName)
	}
	switch typ {
	case "message_start":
		return s.handleMessageStart(payload)
	case "content_block_start":
		return s.handleBlockStart(payload)
	case "content_block_delta":
		return s.handleBlockDelta(payload)
	case "content_block_stop":
		return s.handleBlockStop()
	case "message_delta":
		return s.handleMessageDelta(payload)
	case "message_stop":
		return s.handleMessageStop()
	default:
		return nil
	}
}

func (s *AnthropicToResponsesStream) Close() [][]byte {
	if s == nil || s.done {
		return nil
	}
	s.done = true
	if s.completedSent {
		return nil
	}
	var frames [][]byte
	frames = append(frames, s.ensureCreated()...)
	frames = append(frames, s.closeItem()...)
	frames = append(frames, s.emitCompleted("completed")...)
	return frames
}

func (s *AnthropicToResponsesStream) handleMessageStart(payload map[string]any) [][]byte {
	if msg, ok := payload["message"].(map[string]any); ok && msg != nil {
		if id, ok := msg["id"].(string); ok && id != "" {
			s.RespID = id
		}
		if m, ok := msg["model"].(string); ok && m != "" {
			s.Model = m
		}
		if u, ok := msg["usage"].(map[string]any); ok {
			s.ingestUsage(u)
		}
	}
	return s.ensureCreated()
}

func (s *AnthropicToResponsesStream) ensureCreated() [][]byte {
	if s.createdSent {
		return nil
	}
	s.createdSent = true
	return [][]byte{s.respEvent("response.created", map[string]any{
		"response": map[string]any{
			"id":     s.RespID,
			"object": "response",
			"model":  s.Model,
			"status": "in_progress",
			"output": []any{},
		},
	})}
}

func (s *AnthropicToResponsesStream) handleBlockStart(payload map[string]any) [][]byte {
	block, _ := payload["content_block"].(map[string]any)
	if block == nil {
		return nil
	}
	var frames [][]byte
	frames = append(frames, s.ensureCreated()...)
	bt, _ := block["type"].(string)
	switch bt {
	case "thinking":
		frames = append(frames, s.closeItem()...)
		s.currentItemID = generateResponsesStreamID("rs_")
		s.currentItemType = "reasoning"
		s.currentText = ""
		frames = append(frames, s.respEvent("response.output_item.added", map[string]any{
			"output_index": s.outputIndex,
			"item": map[string]any{
				"type":    "reasoning",
				"id":      s.currentItemID,
				"summary": []any{},
			},
		}))
	case "text":
		if s.currentItemType != "message" {
			frames = append(frames, s.closeItem()...)
			s.currentItemID = generateResponsesStreamID("msg_")
			s.currentItemType = "message"
			s.contentIndex = 0
			s.currentText = ""
			s.textPartOpen = false
			// 严格 wire：message 必须带 content:[]，否则 OpenAI JS .map 崩溃
			frames = append(frames, s.respEvent("response.output_item.added", map[string]any{
				"output_index": s.outputIndex,
				"item": map[string]any{
					"type":    "message",
					"id":      s.currentItemID,
					"role":    "assistant",
					"status":  "in_progress",
					"content": []any{},
				},
			}))
		}
	case "tool_use":
		frames = append(frames, s.closeItem()...)
		s.currentItemID = generateResponsesStreamID("fc_")
		s.currentItemType = "function_call"
		s.currentCallID, _ = block["id"].(string)
		s.currentName, _ = block["name"].(string)
		s.currentArgs = ""
		// 严格 wire：function_call 必须带 arguments（可空串）
		frames = append(frames, s.respEvent("response.output_item.added", map[string]any{
			"output_index": s.outputIndex,
			"item": map[string]any{
				"type":      "function_call",
				"id":        s.currentItemID,
				"call_id":   s.currentCallID,
				"name":      s.currentName,
				"arguments": "",
				"status":    "in_progress",
			},
		}))
	}
	return frames
}

func (s *AnthropicToResponsesStream) ensureTextPart() [][]byte {
	if s.textPartOpen || s.currentItemType != "message" {
		return nil
	}
	s.textPartOpen = true
	return [][]byte{s.respEvent("response.content_part.added", map[string]any{
		"output_index":  s.outputIndex,
		"content_index": s.contentIndex,
		"item_id":       s.currentItemID,
		"part": map[string]any{
			"type":        "output_text",
			"text":        "",
			"annotations": []any{},
			"logprobs":    []any{},
		},
	})}
}

func (s *AnthropicToResponsesStream) handleBlockDelta(payload map[string]any) [][]byte {
	delta, _ := payload["delta"].(map[string]any)
	if delta == nil {
		return nil
	}
	var frames [][]byte
	frames = append(frames, s.ensureCreated()...)
	dt, _ := delta["type"].(string)
	switch dt {
	case "text_delta":
		text, _ := delta["text"].(string)
		if text == "" {
			return frames
		}
		if s.currentItemType != "message" {
			fake := map[string]any{"content_block": map[string]any{"type": "text"}}
			frames = append(frames, s.handleBlockStart(fake)...)
		}
		frames = append(frames, s.ensureTextPart()...)
		s.currentText += text
		frames = append(frames, s.respEvent("response.output_text.delta", map[string]any{
			"output_index":  s.outputIndex,
			"content_index": s.contentIndex,
			"item_id":       s.currentItemID,
			"delta":         text,
		}))
	case "thinking_delta":
		text, _ := delta["thinking"].(string)
		if text == "" {
			return frames
		}
		if s.currentItemType != "reasoning" {
			fake := map[string]any{"content_block": map[string]any{"type": "thinking"}}
			frames = append(frames, s.handleBlockStart(fake)...)
		}
		s.currentText += text
		frames = append(frames, s.respEvent("response.reasoning_summary_text.delta", map[string]any{
			"output_index":  s.outputIndex,
			"summary_index": 0,
			"item_id":       s.currentItemID,
			"delta":         text,
		}))
	case "input_json_delta":
		partial, _ := delta["partial_json"].(string)
		if partial == "" {
			return frames
		}
		s.currentArgs += partial
		frames = append(frames, s.respEvent("response.function_call_arguments.delta", map[string]any{
			"output_index": s.outputIndex,
			"item_id":      s.currentItemID,
			"call_id":      s.currentCallID,
			"name":         s.currentName,
			"delta":        partial,
		}))
	}
	return frames
}

func (s *AnthropicToResponsesStream) handleBlockStop() [][]byte {
	switch s.currentItemType {
	case "reasoning":
		var frames [][]byte
		frames = append(frames, s.respEvent("response.reasoning_summary_text.done", map[string]any{
			"output_index":  s.outputIndex,
			"summary_index": 0,
			"item_id":       s.currentItemID,
			"text":          s.currentText,
		}))
		frames = append(frames, s.closeItem()...)
		return frames
	case "function_call":
		var frames [][]byte
		args := s.currentArgs
		if args == "" {
			args = "{}"
		}
		frames = append(frames, s.respEvent("response.function_call_arguments.done", map[string]any{
			"output_index": s.outputIndex,
			"item_id":      s.currentItemID,
			"call_id":      s.currentCallID,
			"name":         s.currentName,
			"arguments":    args,
		}))
		frames = append(frames, s.closeItem()...)
		return frames
	case "message":
		// text done 在 closeItem 里与 content_part.done 一起发
		return nil
	default:
		return nil
	}
}

func (s *AnthropicToResponsesStream) handleMessageDelta(payload map[string]any) [][]byte {
	if u, ok := payload["usage"].(map[string]any); ok {
		s.ingestUsage(u)
	}
	if d, ok := payload["delta"].(map[string]any); ok {
		if sr, ok := d["stop_reason"].(string); ok {
			s.stopReason = sr
		}
	}
	return nil
}

func (s *AnthropicToResponsesStream) ingestUsage(usage map[string]any) {
	if s == nil || usage == nil {
		return
	}
	// Anthropic-compatible providers sometimes repeat omitted counters as zero
	// in message_delta. Treat stream usage as a partial update so those zero
	// placeholders cannot erase the values captured from message_start.
	if value, ok := asInt(usage["input_tokens"]); ok && value > 0 {
		s.inputTokens = value
	}
	if value, ok := asInt(usage["output_tokens"]); ok && value > 0 {
		s.outputTokens = value
	}
	if hasUsageField(usage, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens") {
		if value := firstPositiveUsageInt(usage, "cache_read_input_tokens", "cache_read_tokens", "cached_tokens"); value > 0 {
			s.cacheRead = value
		}
	}
	fiveMinute, oneHour := anthropicCacheCreationBreakdown(usage)
	if hasUsageField(usage, "cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens") || fiveMinute > 0 || oneHour > 0 {
		if value := firstPositiveUsageInt(usage, "cache_creation_input_tokens", "cache_creation_tokens", "cache_write_tokens"); value > 0 {
			s.cacheCreate = value
		}
	}
	if fiveMinute > 0 {
		s.cacheCreate5m = fiveMinute
	}
	if oneHour > 0 {
		s.cacheCreate1h = oneHour
	}
	if breakdownTotal := s.cacheCreate5m + s.cacheCreate1h; breakdownTotal > s.cacheCreate {
		s.cacheCreate = breakdownTotal
	}
}

func hasUsageField(usage map[string]any, keys ...string) bool {
	for _, key := range keys {
		if _, exists := usage[key]; exists {
			return true
		}
	}
	return false
}

func (s *AnthropicToResponsesStream) handleMessageStop() [][]byte {
	if s.completedSent {
		return nil
	}
	status := "completed"
	if s.stopReason == "max_tokens" {
		status = "incomplete"
	}
	var frames [][]byte
	frames = append(frames, s.closeItem()...)
	frames = append(frames, s.emitCompleted(status)...)
	return frames
}

func (s *AnthropicToResponsesStream) closeItem() [][]byte {
	if s.currentItemType == "" {
		return nil
	}
	idx := s.outputIndex
	itemType := s.currentItemType
	itemID := s.currentItemID
	callID := s.currentCallID
	name := s.currentName
	text := s.currentText
	args := s.currentArgs
	textPartOpen := s.textPartOpen

	s.currentItemType = ""
	s.currentItemID = ""
	s.currentCallID = ""
	s.currentName = ""
	s.currentText = ""
	s.currentArgs = ""
	s.textPartOpen = false
	s.outputIndex++
	s.contentIndex = 0

	var frames [][]byte
	item := map[string]any{
		"type":   itemType,
		"id":     itemID,
		"status": "completed",
	}
	switch itemType {
	case "function_call":
		if args == "" {
			args = "{}"
		}
		item["call_id"] = callID
		item["name"] = name
		item["arguments"] = args
	case "message":
		item["role"] = "assistant"
		// 严格 wire：content 永远是数组
		content := []any{}
		if textPartOpen || text != "" {
			if textPartOpen {
				frames = append(frames, s.respEvent("response.output_text.done", map[string]any{
					"output_index":  idx,
					"content_index": 0,
					"item_id":       itemID,
					"text":          text,
				}))
				frames = append(frames, s.respEvent("response.content_part.done", map[string]any{
					"output_index":  idx,
					"content_index": 0,
					"item_id":       itemID,
					"part": map[string]any{
						"type":        "output_text",
						"text":        text,
						"annotations": []any{},
						"logprobs":    []any{},
					},
				}))
			}
			content = []any{
				map[string]any{
					"type":        "output_text",
					"text":        text,
					"annotations": []any{},
					"logprobs":    []any{},
				},
			}
		}
		item["content"] = content
	case "reasoning":
		summary := []any{}
		if text != "" {
			summary = []any{map[string]any{"type": "summary_text", "text": text}}
		}
		item["summary"] = summary
	}
	s.finished = append(s.finished, item)
	frames = append(frames, s.respEvent("response.output_item.done", map[string]any{
		"output_index": idx,
		"item":         item,
	}))
	return frames
}

func (s *AnthropicToResponsesStream) emitCompleted(status string) [][]byte {
	if s.completedSent {
		return nil
	}
	s.completedSent = true
	s.done = true
	anthropicUsage := map[string]any{
		"input_tokens": s.inputTokens, "output_tokens": s.outputTokens,
		"cache_read_input_tokens": s.cacheRead, "cache_creation_input_tokens": s.cacheCreate,
	}
	if s.cacheCreate5m > 0 || s.cacheCreate1h > 0 {
		anthropicUsage["cache_creation"] = map[string]any{
			"ephemeral_5m_input_tokens": s.cacheCreate5m,
			"ephemeral_1h_input_tokens": s.cacheCreate1h,
		}
	}
	usage := openAIUsageToResponses(anthropicUsageToOpenAI(anthropicUsage))
	output := s.finished
	if output == nil {
		output = []any{}
	}
	resp := map[string]any{
		"id":     s.RespID,
		"object": "response",
		"model":  s.Model,
		"status": status,
		"output": output, // 必须是完整 items，不能空数组（SDK 会 .map content）
		"usage":  usage,
	}
	if status == "incomplete" {
		resp["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
		return [][]byte{s.respEvent("response.incomplete", map[string]any{"response": resp})}
	}
	return [][]byte{s.respEvent("response.completed", map[string]any{"response": resp})}
}

func (s *AnthropicToResponsesStream) respEvent(typ string, extra map[string]any) []byte {
	payload := map[string]any{
		"type":            typ,
		"sequence_number": s.seq,
	}
	s.seq++
	for k, v := range extra {
		payload[k] = v
	}
	return encodeSSEFrame(typ, payload)
}

// ---------------------------------------------------------------------------
// Chat Completions SSE → Responses SSE（真流）
// ---------------------------------------------------------------------------

// ChatToResponsesStream 将 chat.completion.chunk SSE 增量转为 Responses SSE。
type ChatToResponsesStream struct {
	Model  string
	RespID string

	createdSent   bool
	completedSent bool
	done          bool
	seq           int

	nextOutIdx int

	// message text
	msgItemID    string
	msgOutIdx    int
	msgOpen      bool
	textPartOpen bool
	textBuf      string

	// reasoning
	rsItemID string
	rsOutIdx int
	rsOpen   bool
	rsText   string

	// tools: chat index → state
	tools map[int]*chatToolStreamState

	// 已关闭的 output items（completed 用）
	finished []any

	finish string
	usage  map[string]any
}

type chatToolStreamState struct {
	itemID    string
	outIdx    int
	callID    string
	name      string
	args      string
	announced bool
	closed    bool
}

func NewChatToResponsesStream(model string) *ChatToResponsesStream {
	return &ChatToResponsesStream{
		Model:  model,
		RespID: generateResponsesStreamID("resp_"),
		tools:  make(map[int]*chatToolStreamState),
	}
}

func (s *ChatToResponsesStream) FeedData(data string) [][]byte {
	if s == nil || s.done || s.completedSent {
		return nil
	}
	data = strings.TrimSpace(data)
	if data == "" {
		return nil
	}
	if data == "[DONE]" {
		return s.Close()
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return nil
	}
	if id, ok := payload["id"].(string); ok && id != "" {
		s.RespID = id
	}
	if m, ok := payload["model"].(string); ok && m != "" {
		s.Model = m
	}
	if u, ok := payload["usage"].(map[string]any); ok {
		s.usage = u
	}

	var frames [][]byte
	frames = append(frames, s.ensureCreated()...)

	choices, _ := payload["choices"].([]any)
	for _, raw := range choices {
		ch, _ := raw.(map[string]any)
		if ch == nil {
			continue
		}
		if fr, ok := ch["finish_reason"].(string); ok && fr != "" {
			s.finish = fr
		}
		delta, _ := ch["delta"].(map[string]any)
		if delta == nil {
			continue
		}
		if rc, ok := delta["reasoning_content"].(string); ok && rc != "" {
			frames = append(frames, s.ensureReasoning()...)
			s.rsText += rc
			frames = append(frames, s.respEvent("response.reasoning_summary_text.delta", map[string]any{
				"output_index":  s.rsOutIdx,
				"summary_index": 0,
				"item_id":       s.rsItemID,
				"delta":         rc,
			}))
		}
		if text, ok := delta["content"].(string); ok && text != "" {
			frames = append(frames, s.closeReasoning()...)
			frames = append(frames, s.ensureMessage()...)
			s.textBuf += text
			frames = append(frames, s.respEvent("response.output_text.delta", map[string]any{
				"output_index":  s.msgOutIdx,
				"content_index": 0,
				"item_id":       s.msgItemID,
				"delta":         text,
			}))
		}
		if tcs, ok := delta["tool_calls"].([]any); ok {
			for _, tr := range tcs {
				tc, _ := tr.(map[string]any)
				if tc == nil {
					continue
				}
				idx, _ := asInt(tc["index"])
				st, ok := s.tools[idx]
				if !ok {
					frames = append(frames, s.closeReasoning()...)
					frames = append(frames, s.closeMessage()...)
					st = &chatToolStreamState{
						itemID: generateResponsesStreamID("fc_"),
						outIdx: s.allocOut(),
					}
					s.tools[idx] = st
				}
				if id, ok := tc["id"].(string); ok && id != "" {
					st.callID = id
				}
				if fn, ok := tc["function"].(map[string]any); ok {
					if n, ok := fn["name"].(string); ok && n != "" {
						st.name = n
					}
					if a, ok := fn["arguments"].(string); ok && a != "" {
						if !st.announced {
							frames = append(frames, s.announceTool(st)...)
						}
						st.args += a
						if st.announced {
							frames = append(frames, s.respEvent("response.function_call_arguments.delta", map[string]any{
								"output_index": st.outIdx,
								"item_id":      st.itemID,
								"call_id":      st.callID,
								"name":         st.name,
								"delta":        a,
							}))
						}
					} else if st.name != "" && !st.announced {
						frames = append(frames, s.announceTool(st)...)
					}
				}
			}
		}
	}
	return frames
}

func (s *ChatToResponsesStream) closeMessage() [][]byte {
	if !s.msgOpen {
		return nil
	}
	var frames [][]byte
	text := s.textBuf
	if s.textPartOpen {
		frames = append(frames, s.respEvent("response.output_text.done", map[string]any{
			"output_index":  s.msgOutIdx,
			"content_index": 0,
			"item_id":       s.msgItemID,
			"text":          text,
		}))
		frames = append(frames, s.respEvent("response.content_part.done", map[string]any{
			"output_index":  s.msgOutIdx,
			"content_index": 0,
			"item_id":       s.msgItemID,
			"part": map[string]any{
				"type":        "output_text",
				"text":        text,
				"annotations": []any{},
				"logprobs":    []any{},
			},
		}))
		s.textPartOpen = false
	}
	content := []any{}
	if text != "" {
		content = []any{
			map[string]any{
				"type":        "output_text",
				"text":        text,
				"annotations": []any{},
				"logprobs":    []any{},
			},
		}
	}
	item := map[string]any{
		"type":    "message",
		"id":      s.msgItemID,
		"role":    "assistant",
		"status":  "completed",
		"content": content,
	}
	s.finished = append(s.finished, item)
	frames = append(frames, s.respEvent("response.output_item.done", map[string]any{
		"output_index": s.msgOutIdx,
		"item":         item,
	}))
	s.msgOpen = false
	s.textBuf = ""
	return frames
}

func (s *ChatToResponsesStream) Close() [][]byte {
	if s == nil || s.done {
		return nil
	}
	s.done = true
	if s.completedSent {
		return nil
	}
	var frames [][]byte
	frames = append(frames, s.ensureCreated()...)
	frames = append(frames, s.closeReasoning()...)
	frames = append(frames, s.closeMessage()...)
	idxs := make([]int, 0, len(s.tools))
	for idx := range s.tools {
		idxs = append(idxs, idx)
	}
	sort.Ints(idxs)
	for _, idx := range idxs {
		st := s.tools[idx]
		if st == nil || st.closed {
			continue
		}
		if !st.announced {
			frames = append(frames, s.announceTool(st)...)
			if st.args != "" {
				frames = append(frames, s.respEvent("response.function_call_arguments.delta", map[string]any{
					"output_index": st.outIdx,
					"item_id":      st.itemID,
					"call_id":      st.callID,
					"name":         st.name,
					"delta":        st.args,
				}))
			}
		}
		args := st.args
		if args == "" {
			args = "{}"
		}
		frames = append(frames, s.respEvent("response.function_call_arguments.done", map[string]any{
			"output_index": st.outIdx,
			"item_id":      st.itemID,
			"call_id":      st.callID,
			"name":         st.name,
			"arguments":    args,
		}))
		item := map[string]any{
			"type":      "function_call",
			"id":        st.itemID,
			"call_id":   st.callID,
			"name":      st.name,
			"arguments": args,
			"status":    "completed",
		}
		s.finished = append(s.finished, item)
		frames = append(frames, s.respEvent("response.output_item.done", map[string]any{
			"output_index": st.outIdx,
			"item":         item,
		}))
		st.closed = true
	}
	status := "completed"
	if s.finish == "length" {
		status = "incomplete"
	}
	frames = append(frames, s.emitCompleted(status)...)
	return frames
}

func (s *ChatToResponsesStream) ensureCreated() [][]byte {
	if s.createdSent {
		return nil
	}
	s.createdSent = true
	return [][]byte{s.respEvent("response.created", map[string]any{
		"response": map[string]any{
			"id":     s.RespID,
			"object": "response",
			"model":  s.Model,
			"status": "in_progress",
			"output": []any{},
		},
	})}
}

func (s *ChatToResponsesStream) ensureReasoning() [][]byte {
	if s.rsOpen {
		return nil
	}
	s.rsOpen = true
	s.rsItemID = generateResponsesStreamID("rs_")
	s.rsOutIdx = s.allocOut()
	s.rsText = ""
	return [][]byte{s.respEvent("response.output_item.added", map[string]any{
		"output_index": s.rsOutIdx,
		"item": map[string]any{
			"type":    "reasoning",
			"id":      s.rsItemID,
			"summary": []any{},
		},
	})}
}

func (s *ChatToResponsesStream) closeReasoning() [][]byte {
	if !s.rsOpen {
		return nil
	}
	s.rsOpen = false
	var frames [][]byte
	frames = append(frames, s.respEvent("response.reasoning_summary_text.done", map[string]any{
		"output_index":  s.rsOutIdx,
		"summary_index": 0,
		"item_id":       s.rsItemID,
		"text":          s.rsText,
	}))
	summary := []any{}
	if s.rsText != "" {
		summary = []any{map[string]any{"type": "summary_text", "text": s.rsText}}
	}
	item := map[string]any{
		"type":    "reasoning",
		"id":      s.rsItemID,
		"status":  "completed",
		"summary": summary,
	}
	s.finished = append(s.finished, item)
	frames = append(frames, s.respEvent("response.output_item.done", map[string]any{
		"output_index": s.rsOutIdx,
		"item":         item,
	}))
	s.rsText = ""
	return frames
}

func (s *ChatToResponsesStream) ensureMessage() [][]byte {
	var frames [][]byte
	if !s.msgOpen {
		s.msgOpen = true
		s.msgItemID = generateResponsesStreamID("msg_")
		s.msgOutIdx = s.allocOut()
		s.textBuf = ""
		frames = append(frames, s.respEvent("response.output_item.added", map[string]any{
			"output_index": s.msgOutIdx,
			"item": map[string]any{
				"type":    "message",
				"id":      s.msgItemID,
				"role":    "assistant",
				"status":  "in_progress",
				"content": []any{}, // 严格 wire
			},
		}))
	}
	if !s.textPartOpen {
		s.textPartOpen = true
		frames = append(frames, s.respEvent("response.content_part.added", map[string]any{
			"output_index":  s.msgOutIdx,
			"content_index": 0,
			"item_id":       s.msgItemID,
			"part": map[string]any{
				"type":        "output_text",
				"text":        "",
				"annotations": []any{},
				"logprobs":    []any{},
			},
		}))
	}
	return frames
}

func (s *ChatToResponsesStream) announceTool(st *chatToolStreamState) [][]byte {
	if st.announced {
		return nil
	}
	st.announced = true
	if st.callID == "" {
		st.callID = st.itemID
	}
	return [][]byte{s.respEvent("response.output_item.added", map[string]any{
		"output_index": st.outIdx,
		"item": map[string]any{
			"type":      "function_call",
			"id":        st.itemID,
			"call_id":   st.callID,
			"name":      st.name,
			"arguments": "", // 严格 wire
			"status":    "in_progress",
		},
	})}
}

func (s *ChatToResponsesStream) allocOut() int {
	i := s.nextOutIdx
	s.nextOutIdx++
	return i
}

func (s *ChatToResponsesStream) emitCompleted(status string) [][]byte {
	if s.completedSent {
		return nil
	}
	s.completedSent = true
	usage := map[string]any{}
	if s.usage != nil {
		usage = openAIUsageToResponses(s.usage)
	}
	output := s.finished
	if output == nil {
		output = []any{}
	}
	resp := map[string]any{
		"id":     s.RespID,
		"object": "response",
		"model":  s.Model,
		"status": status,
		"output": output,
	}
	if len(usage) > 0 {
		resp["usage"] = usage
	}
	if status == "incomplete" {
		resp["incomplete_details"] = map[string]any{"reason": "max_output_tokens"}
		return [][]byte{s.respEvent("response.incomplete", map[string]any{"response": resp})}
	}
	return [][]byte{s.respEvent("response.completed", map[string]any{"response": resp})}
}

func (s *ChatToResponsesStream) respEvent(typ string, extra map[string]any) []byte {
	payload := map[string]any{
		"type":            typ,
		"sequence_number": s.seq,
	}
	s.seq++
	for k, v := range extra {
		payload[k] = v
	}
	return encodeSSEFrame(typ, payload)
}

func generateResponsesStreamID(prefix string) string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return prefix + hex.EncodeToString(b)
}
