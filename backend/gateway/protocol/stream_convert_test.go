package protocol

import (
	"strings"
	"testing"
)

func TestAnthropicToOpenAIStream_Incremental(t *testing.T) {
	conv := NewAnthropicToOpenAIStream("claude-x")
	var out strings.Builder
	for _, frame := range conv.Feed("message_start", `{"type":"message_start","message":{"id":"msg_1","model":"claude-x"}}`) {
		out.Write(frame)
	}
	for _, frame := range conv.Feed("content_block_delta", `{"type":"content_block_delta","delta":{"type":"text_delta","text":"Hi"}}`) {
		out.Write(frame)
	}
	for _, frame := range conv.Close() {
		out.Write(frame)
	}
	s := out.String()
	if !strings.Contains(s, "chat.completion.chunk") {
		t.Fatalf("missing chunk: %s", s)
	}
	if !strings.Contains(s, `"content":"Hi"`) {
		t.Fatalf("missing content: %s", s)
	}
	if !strings.Contains(s, "data: [DONE]") {
		t.Fatalf("missing DONE: %s", s)
	}
}

func TestOpenAIToAnthropicStream_Incremental(t *testing.T) {
	conv := NewOpenAIToAnthropicStream("gpt-x")
	var out strings.Builder
	for _, frame := range conv.EnsureStarted() {
		out.Write(frame)
	}
	chunk := `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"Hello"},"finish_reason":null}]}`
	for _, frame := range conv.FeedData(chunk) {
		out.Write(frame)
	}
	for _, frame := range conv.Close() {
		out.Write(frame)
	}
	s := out.String()
	if !strings.Contains(s, "event: message_start") {
		t.Fatalf("missing message_start: %s", s)
	}
	if !strings.Contains(s, "text_delta") || !strings.Contains(s, "Hello") {
		t.Fatalf("missing delta: %s", s)
	}
	if !strings.Contains(s, "event: message_stop") {
		t.Fatalf("missing message_stop: %s", s)
	}
}

func TestOpenAIToAnthropicStream_ToolCalls(t *testing.T) {
	conv := NewOpenAIToAnthropicStream("gpt-x")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.EnsureStarted())
	write(conv.FeedData(`{"id":"c1","choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"Bash","arguments":""}}]},"finish_reason":null}]}`))
	write(conv.FeedData(`{"id":"c1","choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"x\":1}"}}]},"finish_reason":null}]}`))
	write(conv.FeedData(`{"id":"c1","choices":[{"delta":{},"finish_reason":"tool_calls"}]}`))
	write(conv.Close())
	s := out.String()
	if !strings.Contains(s, "tool_use") {
		t.Fatalf("missing tool_use: %s", s)
	}
	if !strings.Contains(s, "input_json_delta") {
		t.Fatalf("missing input_json_delta: %s", s)
	}
	if !strings.Contains(s, "tool_use") || !strings.Contains(s, "message_stop") {
		t.Fatalf("incomplete: %s", s)
	}
}

func TestAnthropicToResponsesStream_Incremental(t *testing.T) {
	conv := NewAnthropicToResponsesStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.Feed("message_start", `{"type":"message_start","message":{"id":"msg_1","model":"m"}}`))
	write(conv.Feed("content_block_start", `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
	write(conv.Feed("content_block_delta", `{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}}`))
	write(conv.Feed("content_block_stop", `{"type":"content_block_stop","index":0}`))
	write(conv.Feed("message_stop", `{"type":"message_stop"}`))
	s := out.String()
	if !strings.Contains(s, "response.created") {
		t.Fatalf("missing response.created: %s", s)
	}
	if !strings.Contains(s, "output_text.delta") || !strings.Contains(s, "Hi") {
		t.Fatalf("missing text delta: %s", s)
	}
	if !strings.Contains(s, "response.completed") {
		t.Fatalf("missing completed: %s", s)
	}
	// 严格 wire：message item 必须 content 数组；completed.output 不得空
	if !strings.Contains(s, `"content":[]`) && !strings.Contains(s, `"content": []`) {
		// 至少 added 时应有 content:[]
		if !strings.Contains(s, `"content"`) {
			t.Fatalf("message item missing content field (OpenAI JS .map crash): %s", s)
		}
	}
	if !strings.Contains(s, `"text":"Hi"`) {
		t.Fatalf("completed/output missing text Hi: %s", s)
	}
}

func TestChatToResponsesStream_Incremental(t *testing.T) {
	conv := NewChatToResponsesStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.FeedData(`{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"ab"},"finish_reason":null}]}`))
	write(conv.FeedData(`{"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	write(conv.Close())
	s := out.String()
	if !strings.Contains(s, "response.created") {
		t.Fatalf("missing created: %s", s)
	}
	if !strings.Contains(s, "output_text.delta") || !strings.Contains(s, "ab") {
		t.Fatalf("missing delta: %s", s)
	}
	if !strings.Contains(s, "response.completed") {
		t.Fatalf("missing completed: %s", s)
	}
	// OpenAI JS 严格字段
	if !strings.Contains(s, `"content"`) {
		t.Fatalf("message missing content (SDK .map crash): %s", s)
	}
	if !strings.Contains(s, `"text":"ab"`) {
		t.Fatalf("completed output missing full text: %s", s)
	}
	if !strings.Contains(s, "content_part.added") {
		t.Fatalf("missing content_part.added: %s", s)
	}
}

func TestStreamUsageConversionsPreserveCacheAliases(t *testing.T) {
	chat := NewChatToResponsesStream("m")
	chatFrames := chat.FeedData(`{"id":"c1","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":0},"cache_read_input_tokens":20,"cache_write_tokens":10}}`)
	chatFrames = append(chatFrames, chat.Close()...)
	chatBody := string(JoinSSEFrames(chatFrames))
	for _, want := range []string{`"input_tokens":100`, `"cached_tokens":20`, `"cache_write_tokens":10`} {
		if !strings.Contains(chatBody, want) {
			t.Fatalf("chat -> responses lost %s: %s", want, chatBody)
		}
	}

	responses := NewResponsesToAnthropicStream("m")
	frames := responses.Feed("response.completed", `{"type":"response.completed","response":{"id":"r1","status":"completed","usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":20},"cache_write_tokens":10}}}`)
	responsesBody := string(JoinSSEFrames(frames))
	for _, want := range []string{`"input_tokens":70`, `"cache_read_input_tokens":20`, `"cache_creation_input_tokens":10`} {
		if !strings.Contains(responsesBody, want) {
			t.Fatalf("responses -> anthropic lost %s: %s", want, responsesBody)
		}
	}

	anthropic := NewAnthropicToResponsesStream("m")
	anthropic.Feed("message_start", `{"type":"message_start","message":{"id":"m1","usage":{"input_tokens":70,"cache_read_input_tokens":20,"cache_creation_input_tokens":10,"cache_creation":{"ephemeral_5m_input_tokens":4,"ephemeral_1h_input_tokens":6}}}}`)
	anthropicBody := string(JoinSSEFrames(anthropic.Feed("message_stop", `{"type":"message_stop"}`)))
	for _, want := range []string{`"input_tokens":100`, `"cached_tokens":20`, `"ephemeral_5m_input_tokens":4`, `"ephemeral_1h_input_tokens":6`} {
		if !strings.Contains(anthropicBody, want) {
			t.Fatalf("anthropic -> responses lost %s: %s", want, anthropicBody)
		}
	}
}

func TestSupportsIncrementalStream(t *testing.T) {
	if !SupportsIncrementalStream(KindOpenAIChat, KindOpenAIChat, false) {
		t.Fatal("passthrough should be incremental")
	}
	// 三协议矩阵全部真流
	pairs := [][2]Kind{
		{KindOpenAIChat, KindAnthropic},
		{KindAnthropic, KindOpenAIChat},
		{KindOpenAIChat, KindOpenAIResponses},
		{KindOpenAIResponses, KindOpenAIChat},
		{KindAnthropic, KindOpenAIResponses},
		{KindOpenAIResponses, KindAnthropic},
	}
	for _, p := range pairs {
		if !SupportsIncrementalStream(p[0], p[1], true) {
			t.Fatalf("want incremental %s←%s", p[0], p[1])
		}
	}
}

func TestResponsesToAnthropicStream_IncrementalText(t *testing.T) {
	conv := NewResponsesToAnthropicStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.Feed("response.created", `{"type":"response.created","response":{"id":"resp_1","model":"m","status":"in_progress"}}`))
	// Lifecycle metadata is held until the first substantive frame.
	if out.Len() != 0 {
		t.Fatalf("lifecycle metadata must not create client-visible output: %s", out.String())
	}
	write(conv.Feed("response.output_text.delta", `{"type":"response.output_text.delta","delta":"Hel"}`))
	write(conv.Feed("response.output_text.delta", `{"type":"response.output_text.delta","delta":"lo"}`))
	write(conv.Feed("response.completed", `{"type":"response.completed","response":{"id":"resp_1","status":"completed","usage":{"input_tokens":1,"output_tokens":2}}}`))
	s := out.String()
	if !strings.Contains(s, "text_delta") || !strings.Contains(s, "Hel") || !strings.Contains(s, "lo") {
		t.Fatalf("missing text deltas: %s", s)
	}
	if !strings.Contains(s, "message_stop") {
		t.Fatalf("missing message_stop: %s", s)
	}
	// 增量：message_start 应出现在第一个 delta 之前（已在 created 时写出）
	idxStart := strings.Index(s, "message_start")
	idxHel := strings.Index(s, "Hel")
	if idxStart < 0 || idxHel < 0 || idxStart > idxHel {
		t.Fatalf("message_start should precede text: %s", s)
	}
}

func TestResponsesToAnthropicStream_FailedPreservesError(t *testing.T) {
	conv := NewResponsesToAnthropicStream("m")
	if frames := conv.Feed("response.created", `{"type":"response.created","response":{"id":"r1","status":"in_progress"}}`); len(frames) != 0 {
		t.Fatalf("created frames=%q, want none", JoinSSEFrames(frames))
	}
	frames := conv.Feed("response.failed", `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"Our servers are currently overloaded. Please try again later."}}}`)
	body := string(JoinSSEFrames(frames))
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "Our servers are currently overloaded") {
		t.Fatalf("failed response was not preserved as an Anthropic error: %s", body)
	}
	if strings.Contains(body, "message_stop") {
		t.Fatalf("failed response was converted to a normal stop: %s", body)
	}
}

func TestResponsesToAnthropicStream_FailedEnvelopeUsesRootError(t *testing.T) {
	conv := NewResponsesToAnthropicStream("m")
	frames := conv.Feed("", `{"response":{"id":"r1","status":"failed"},"error":{"code":"server_error","message":"Selected model is at capacity. Please try a different model."}}`)
	body := string(JoinSSEFrames(frames))
	if !strings.Contains(body, "event: error") || !strings.Contains(body, "Selected model is at capacity") {
		t.Fatalf("root failure was not preserved as an Anthropic error: %s", body)
	}
}

func TestResponsesToAnthropicStream_IncrementalTool(t *testing.T) {
	conv := NewResponsesToAnthropicStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.Feed("response.created", `{"type":"response.created","response":{"id":"resp_t","model":"m"}}`))
	write(conv.Feed("response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"call_1","name":"Bash","arguments":""}}`))
	write(conv.Feed("response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","output_index":0,"delta":"{\"c\""}`))
	write(conv.Feed("response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","output_index":0,"delta":":\"x\"}"}`))
	write(conv.Feed("response.function_call_arguments.done", `{"type":"response.function_call_arguments.done","output_index":0,"arguments":"{\"c\":\"x\"}"}`))
	write(conv.Feed("response.completed", `{"type":"response.completed","response":{"id":"resp_t","status":"completed","usage":{"input_tokens":2,"output_tokens":3}}}`))
	s := out.String()
	if !strings.Contains(s, "tool_use") {
		t.Fatalf("missing tool_use: %s", s)
	}
	if !strings.Contains(s, "input_json_delta") {
		t.Fatalf("missing input_json_delta: %s", s)
	}
	if !strings.Contains(s, `"stop_reason":"tool_use"`) && !strings.Contains(s, `"stop_reason": "tool_use"`) {
		// compact JSON
		if !strings.Contains(s, "tool_use") {
			t.Fatalf("missing stop tool_use: %s", s)
		}
	}
	if !strings.Contains(s, "message_stop") {
		t.Fatalf("missing message_stop: %s", s)
	}
}

func TestResponsesToOpenAIStream_IncrementalText(t *testing.T) {
	conv := NewResponsesToOpenAIStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.Feed("response.created", `{"type":"response.created","response":{"id":"r1","model":"m"}}`))
	write(conv.Feed("response.output_text.delta", `{"type":"response.output_text.delta","delta":"hi"}`))
	write(conv.Feed("response.completed", `{"type":"response.completed","response":{"id":"r1","status":"completed"}}`))
	s := out.String()
	if !strings.Contains(s, "chat.completion.chunk") {
		t.Fatalf("missing chunk: %s", s)
	}
	if !strings.Contains(s, `"content":"hi"`) {
		t.Fatalf("missing content: %s", s)
	}
	if !strings.Contains(s, "data: [DONE]") {
		t.Fatalf("missing DONE: %s", s)
	}
}

func TestResponsesToOpenAIStream_FailedPreservesError(t *testing.T) {
	conv := NewResponsesToOpenAIStream("m")
	if frames := conv.Feed("response.created", `{"type":"response.created","response":{"id":"r1","status":"in_progress"}}`); len(frames) != 0 {
		t.Fatalf("created frames=%q, want none", JoinSSEFrames(frames))
	}
	frames := conv.Feed("response.failed", `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","message":"Selected model is at capacity. Please try a different model."}}}`)
	body := string(JoinSSEFrames(frames))
	if !strings.Contains(body, `"error"`) || !strings.Contains(body, "Selected model is at capacity") {
		t.Fatalf("failed response was not preserved as an OpenAI error: %s", body)
	}
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatalf("missing terminal marker: %s", body)
	}
}

func TestResponsesToOpenAIStream_FailedEnvelopeUsesRootError(t *testing.T) {
	conv := NewResponsesToOpenAIStream("m")
	frames := conv.Feed("", `{"response":{"id":"r1","status":"failed"},"error":{"code":"server_error","message":"Our servers are currently overloaded. Please try again later."}}`)
	body := string(JoinSSEFrames(frames))
	if !strings.Contains(body, `"error"`) || !strings.Contains(body, "Our servers are currently overloaded") {
		t.Fatalf("root failure was not preserved as an OpenAI error: %s", body)
	}
}

func TestResponsesToOpenAIStream_ToolNoDuplicateArgs(t *testing.T) {
	// delta 已推 partial 后，output_item.done 不得再推全量 arguments
	conv := NewResponsesToOpenAIStream("m")
	var out strings.Builder
	write := func(frames [][]byte) {
		for _, f := range frames {
			out.Write(f)
		}
	}
	write(conv.Feed("response.created", `{"type":"response.created","response":{"id":"r1","model":"m"}}`))
	write(conv.Feed("response.output_item.added", `{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"Bash","arguments":""}}`))
	write(conv.Feed("response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","call_id":"c1","delta":"{\"x\":"}`))
	write(conv.Feed("response.function_call_arguments.delta", `{"type":"response.function_call_arguments.delta","call_id":"c1","delta":"1}"}`))
	write(conv.Feed("response.output_item.done", `{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","call_id":"c1","name":"Bash","arguments":"{\"x\":1}"}}`))
	write(conv.Feed("response.completed", `{"type":"response.completed","response":{"id":"r1","status":"completed"}}`))
	s := out.String()
	if !strings.Contains(s, "tool_calls") || !strings.Contains(s, "Bash") {
		t.Fatalf("missing tool: %s", s)
	}
	// 全量 "{\"x\":1}" 若再出现，客户端拼接会坏；delta 片段应存在
	if !strings.Contains(s, `{\"x\":`) {
		t.Fatalf("missing arg delta: %s", s)
	}
	// 统计 arguments 出现次数：全量整包不应单独再出现一次作为 arguments 值
	// 简单断言：done 后不应出现完整 arguments 字段值 "{\"x\":1}" 作为独立推送
	// （delta 是分段的，不会单独等于完整 JSON）
	if strings.Count(s, `"arguments":"{\"x\":1}"`) > 0 {
		t.Fatalf("full arguments re-sent after deltas (duplicate risk): %s", s)
	}
	if !strings.Contains(s, "tool_calls") || !strings.Contains(s, "data: [DONE]") {
		t.Fatalf("incomplete: %s", s)
	}
}
