package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestOpenAIToAnthropicRequestBasic(t *testing.T) {
	in := []byte(`{
		"model":"gpt-x",
		"messages":[
			{"role":"system","content":"You are helpful"},
			{"role":"user","content":"hi"}
		],
		"max_tokens":100,
		"temperature":0.5
	}`)
	out, err := OpenAIToAnthropicRequest(in, "claude-sonnet", false)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["model"] != "claude-sonnet" {
		t.Fatalf("model=%v", m["model"])
	}
	if m["system"] != "You are helpful" {
		t.Fatalf("system=%v", m["system"])
	}
	msgs, _ := m["messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("messages len=%d", len(msgs))
	}
}

func TestNonStreamUsageConversionsPreserveCacheCreation(t *testing.T) {
	chat := []byte(`{"id":"c1","model":"gpt-x","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":100,"completion_tokens":4,"total_tokens":104,"prompt_tokens_details":{"cached_tokens":20},"cache_creation_tokens":10}}`)
	responses, err := OpenAIChatToResponsesResponse(chat, "gpt-x")
	if err != nil {
		t.Fatal(err)
	}
	var response map[string]any
	if err := json.Unmarshal(responses, &response); err != nil {
		t.Fatal(err)
	}
	responseUsage, _ := response["usage"].(map[string]any)
	if got, _ := asInt(responseUsage["cache_creation_tokens"]); got != 10 {
		t.Fatalf("chat -> responses lost cache creation: %#v", responseUsage)
	}
	back, err := ResponsesToOpenAIChatResponse(responses, "gpt-x")
	if err != nil {
		t.Fatal(err)
	}
	var chatBack map[string]any
	if err := json.Unmarshal(back, &chatBack); err != nil {
		t.Fatal(err)
	}
	backUsage, _ := chatBack["usage"].(map[string]any)
	if got, _ := asInt(backUsage["cache_creation_tokens"]); got != 10 {
		t.Fatalf("responses -> chat lost cache creation: %#v", backUsage)
	}

	anthropic, err := ResponsesToAnthropicResponse(responses, "claude-x")
	if err != nil {
		t.Fatal(err)
	}
	var message map[string]any
	if err := json.Unmarshal(anthropic, &message); err != nil {
		t.Fatal(err)
	}
	anthropicUsage, _ := message["usage"].(map[string]any)
	if got, _ := asInt(anthropicUsage["input_tokens"]); got != 70 {
		t.Fatalf("responses -> anthropic fresh input=%d, want 70: %#v", got, anthropicUsage)
	}
	if got, _ := asInt(anthropicUsage["cache_read_input_tokens"]); got != 20 {
		t.Fatalf("responses -> anthropic cache read=%d, want 20: %#v", got, anthropicUsage)
	}
	if got, _ := asInt(anthropicUsage["cache_creation_input_tokens"]); got != 10 {
		t.Fatalf("responses -> anthropic cache creation=%d, want 10: %#v", got, anthropicUsage)
	}

	openAIAnthropic, err := OpenAIToAnthropicResponse(chat, "claude-x")
	if err != nil {
		t.Fatal(err)
	}
	var openAIMessage map[string]any
	if err := json.Unmarshal(openAIAnthropic, &openAIMessage); err != nil {
		t.Fatal(err)
	}
	openAIUsage, _ := openAIMessage["usage"].(map[string]any)
	if got, _ := asInt(openAIUsage["input_tokens"]); got != 70 {
		t.Fatalf("chat -> anthropic fresh input=%d, want 70: %#v", got, openAIUsage)
	}
}

func TestNonStreamUsageConversionsPreserveCompatibilityAliases(t *testing.T) {
	chat := []byte(`{"id":"c1","model":"m","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"input_tokens":100,"output_tokens":4,"input_tokens_details":{"cached_tokens":0},"cache_read_tokens":20,"cache_write_tokens":10,"cache_creation_5m_tokens":4,"cache_creation_1h_tokens":6}}`)
	responses, err := OpenAIChatToResponsesResponse(chat, "m")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(responses, &payload); err != nil {
		t.Fatal(err)
	}
	usage, _ := payload["usage"].(map[string]any)
	if got := openAIInputTokens(usage); got != 100 {
		t.Fatalf("input tokens=%d, want 100: %#v", got, usage)
	}
	if got := openAICachedTokens(usage); got != 20 {
		t.Fatalf("cache read=%d, want 20: %#v", got, usage)
	}
	if got := openAICacheCreationTokens(usage); got != 10 {
		t.Fatalf("cache creation=%d, want 10: %#v", got, usage)
	}
	for _, key := range []string{"cache_read_tokens", "cache_write_tokens", "cache_creation_5m_tokens", "cache_creation_1h_tokens"} {
		if _, exists := usage[key]; !exists {
			t.Fatalf("lost %s: %#v", key, usage)
		}
	}
}

func TestCacheCreationConversionsUseSub2APIPriorityAndPreserveDurationTotal(t *testing.T) {
	conflicting := map[string]any{
		"input_tokens_details":  map[string]any{"cache_creation_tokens": 5},
		"prompt_tokens_details": map[string]any{"cache_write_tokens": 7},
	}
	if got := openAICacheCreationTokens(conflicting); got != 7 {
		t.Fatalf("cache creation=%d, want higher-priority prompt cache_write_tokens=7", got)
	}

	anthropic := map[string]any{
		"input_tokens": 70, "output_tokens": 4, "cache_creation_input_tokens": 0,
		"cache_creation": map[string]any{
			"ephemeral_5m_input_tokens": 4,
			"ephemeral_1h_input_tokens": 6,
		},
	}
	converted := anthropicUsageToOpenAI(anthropic)
	if got, _ := asInt(converted["prompt_tokens"]); got != 80 {
		t.Fatalf("total input=%d, want 80: %#v", got, converted)
	}
	if got, _ := asInt(converted["cache_creation_input_tokens"]); got != 10 {
		t.Fatalf("canonical cache creation=%d, want 10: %#v", got, converted)
	}
}

func TestUsageConversionCanonicalizesConflictingNestedCacheAliases(t *testing.T) {
	usage := map[string]any{
		"input_tokens": 100, "output_tokens": 4,
		"input_tokens_details":  map[string]any{"cached_tokens": 0, "cache_write_tokens": 0},
		"prompt_tokens_details": map[string]any{"cached_tokens": 20, "cache_write_tokens": 10},
	}
	converted := openAIUsageToResponses(usage)
	details, _ := converted["input_tokens_details"].(map[string]any)
	if cached, _ := asInt(details["cached_tokens"]); cached != 20 {
		t.Fatalf("canonical cached tokens=%d, want 20: %#v", cached, converted)
	}
	if created, _ := asInt(details["cache_write_tokens"]); created != 10 {
		t.Fatalf("canonical cache creation=%d, want 10: %#v", created, converted)
	}
}

func TestAnthropicUsageConversionKeepsSynthesizedCreationTotal(t *testing.T) {
	converted := anthropicUsageToOpenAI(map[string]any{
		"input_tokens": 70, "output_tokens": 4, "cache_creation_input_tokens": 0,
		"cache_creation": map[string]any{
			"ephemeral_5m_input_tokens": 4,
			"ephemeral_1h_input_tokens": 6,
		},
	})
	if created, _ := asInt(converted["cache_creation_input_tokens"]); created != 10 {
		t.Fatalf("synthesized cache creation=%d, want 10: %#v", created, converted)
	}
}

func TestAnthropicToOpenAIRequestBasic(t *testing.T) {
	in := []byte(`{
		"model":"claude-x",
		"max_tokens":256,
		"system":"sys",
		"messages":[{"role":"user","content":"hello"}]
	}`)
	out, err := AnthropicToOpenAIRequest(in, "gpt-4o", true)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["model"] != "gpt-4o" {
		t.Fatalf("model=%v", m["model"])
	}
	if m["stream"] != true {
		t.Fatalf("stream=%v", m["stream"])
	}
	msgs, _ := m["messages"].([]any)
	if len(msgs) < 2 {
		t.Fatalf("messages=%v", msgs)
	}
}

func TestAnthropicToOpenAIResponse(t *testing.T) {
	in := []byte(`{
		"id":"msg_1",
		"type":"message",
		"role":"assistant",
		"model":"claude",
		"content":[{"type":"text","text":"hello"}],
		"stop_reason":"end_turn",
		"usage":{"input_tokens":3,"output_tokens":1}
	}`)
	out, err := AnthropicToOpenAIResponse(in, "claude")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["object"] != "chat.completion" {
		t.Fatalf("object=%v", m["object"])
	}
	choices, _ := m["choices"].([]any)
	ch, _ := choices[0].(map[string]any)
	msg, _ := ch["message"].(map[string]any)
	if msg["content"] != "hello" {
		t.Fatalf("content=%v", msg["content"])
	}
}

func TestOpenAIToAnthropicResponse(t *testing.T) {
	in := []byte(`{
		"id":"chatcmpl_1",
		"object":"chat.completion",
		"model":"gpt",
		"choices":[{"index":0,"message":{"role":"assistant","content":"yo"},"finish_reason":"stop"}],
		"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}
	}`)
	out, err := OpenAIToAnthropicResponse(in, "gpt")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["type"] != "message" {
		t.Fatalf("type=%v", m["type"])
	}
	if m["stop_reason"] != "end_turn" {
		t.Fatalf("stop=%v", m["stop_reason"])
	}
}

func TestResolveUpstream(t *testing.T) {
	if got := ResolveUpstream("auto", KindOpenAI, "claude-sonnet-4"); got != KindAnthropic {
		t.Fatalf("got %s", got)
	}
	if got := ResolveUpstream("auto", KindOpenAI, "gpt-4o"); got != KindOpenAIChat {
		t.Fatalf("got %s want openai_chat", got)
	}
	if got := ResolveUpstream("openai", KindAnthropic, "claude-x"); got != KindOpenAIChat {
		t.Fatalf("got %s want openai_chat", got)
	}
	if got := ResolveUpstream("openai_responses", KindOpenAI, "gpt-4o"); got != KindOpenAIResponses {
		t.Fatalf("got %s", got)
	}
	if PathFor(KindOpenAIResponses, "/v1/chat/completions") != "/v1/responses" {
		t.Fatal("responses path")
	}
}

func TestOpenAIChatToResponsesRequest(t *testing.T) {
	in := []byte(`{"model":"gpt-4o","messages":[{"role":"system","content":"hi"},{"role":"user","content":"yo"}],"max_tokens":10}`)
	out, err := OpenAIChatToResponsesRequest(in, "gpt-4o", false)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["model"] != "gpt-4o" {
		t.Fatalf("model=%v", m["model"])
	}
	if m["instructions"] != "hi" {
		t.Fatalf("instructions=%v", m["instructions"])
	}
	input, _ := m["input"].([]any)
	if len(input) != 1 {
		t.Fatalf("input len=%d", len(input))
	}
}

func TestOpenAIChatToResponsesRequest_MultimodalAndTools(t *testing.T) {
	// 复现上游 422 ModelInput：chat 的 text parts / tool_calls / nested tools 必须规范化
	in := []byte(`{
		"model":"gpt-4o",
		"messages":[
			{"role":"user","content":[{"type":"text","text":"see"},{"type":"image_url","image_url":{"url":"https://x/a.png"}}]},
			{"role":"assistant","content":null,"tool_calls":[{"id":"call_1","type":"function","function":{"name":"ping","arguments":"{\"h\":\"a\"}"}}]},
			{"role":"tool","tool_call_id":"call_1","content":"pong"}
		],
		"tools":[{"type":"function","function":{"name":"ping","description":"d","parameters":{"type":"object"}}}],
		"tool_choice":{"type":"function","function":{"name":"ping"}}
	}`)
	out, err := OpenAIChatToResponsesRequest(in, "gpt-4o", true)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	input, _ := m["input"].([]any)
	if len(input) != 3 {
		t.Fatalf("input len=%d body=%s", len(input), string(out))
	}
	// user: text → input_text, image_url → input_image
	user, _ := input[0].(map[string]any)
	parts, _ := user["content"].([]any)
	if len(parts) != 2 {
		t.Fatalf("user parts=%v", user["content"])
	}
	p0, _ := parts[0].(map[string]any)
	if p0["type"] != "input_text" || p0["text"] != "see" {
		t.Fatalf("part0=%v", p0)
	}
	p1, _ := parts[1].(map[string]any)
	if p1["type"] != "input_image" || p1["image_url"] != "https://x/a.png" {
		t.Fatalf("part1=%v", p1)
	}
	// assistant tool_calls → function_call item（不得残留 tool_calls）
	fc, _ := input[1].(map[string]any)
	if fc["type"] != "function_call" || fc["call_id"] != "call_1" || fc["name"] != "ping" {
		t.Fatalf("function_call=%v", fc)
	}
	if _, has := fc["tool_calls"]; has {
		t.Fatal("must not keep chat tool_calls on responses input")
	}
	// tool → function_call_output
	fo, _ := input[2].(map[string]any)
	if fo["type"] != "function_call_output" || fo["call_id"] != "call_1" || fo["output"] != "pong" {
		t.Fatalf("function_call_output=%v", fo)
	}
	// tools 扁平化
	tools, _ := m["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools=%v", m["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["name"] != "ping" || tool["type"] != "function" {
		t.Fatalf("tool=%v", tool)
	}
	if _, has := tool["function"]; has {
		t.Fatal("responses tools must not nest function")
	}
	// tool_choice 扁平化
	tc, _ := m["tool_choice"].(map[string]any)
	if tc["type"] != "function" || tc["name"] != "ping" {
		t.Fatalf("tool_choice=%v", tc)
	}
	if _, has := tc["function"]; has {
		t.Fatal("tool_choice must not nest function")
	}
}

func TestResponsesToOpenAIChatRequest(t *testing.T) {
	in := []byte(`{"model":"gpt-4o","instructions":"sys","input":[{"role":"user","content":"hi"}],"max_output_tokens":8}`)
	out, err := ResponsesToOpenAIChatRequest(in, "gpt-4o", false)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	msgs, _ := m["messages"].([]any)
	if len(msgs) < 2 {
		t.Fatalf("messages=%v", m["messages"])
	}
}

func TestResponsesToOpenAIChatRequest_FunctionCallHistoryAndTools(t *testing.T) {
	// 复现：多轮 function_call / function_call_output 不得丢失；扁平 tools 须转 chat 嵌套
	in := []byte(`{
		"model":"gpt-x",
		"input":[
			{"type":"message","role":"user","content":[{"type":"input_text","text":"run"}]},
			{"type":"function_call","call_id":"c0","name":"Bash","arguments":"{\"cmd\":\"ls\"}"},
			{"type":"function_call","call_id":"c1","name":"Read","arguments":"{\"path\":\"a\"}"},
			{"type":"function_call_output","call_id":"c0","output":"a.txt"},
			{"type":"function_call_output","call_id":"c1","output":"ok"}
		],
		"tools":[
			{"type":"function","name":"Bash","description":"shell","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}},
			{"type":"function","name":"Read","parameters":{"type":"object","properties":{"path":{"type":"string"}}}}
		],
		"tool_choice":{"type":"function","name":"Bash"}
	}`)
	out, err := ResponsesToOpenAIChatRequest(in, "gpt-x", true)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["stream"] != true {
		t.Fatalf("stream=%v", m["stream"])
	}
	msgs, _ := m["messages"].([]any)
	// system? no; user + 1 assistant(with 2 tool_calls) + 2 tool
	if len(msgs) < 4 {
		t.Fatalf("want >=4 messages got %d: %#v", len(msgs), msgs)
	}
	// find assistant with tool_calls
	var foundAssistant, foundTool0, foundTool1 bool
	for _, raw := range msgs {
		msg, _ := raw.(map[string]any)
		if msg == nil {
			continue
		}
		switch msg["role"] {
		case "assistant":
			tcs, _ := msg["tool_calls"].([]any)
			if len(tcs) != 2 {
				t.Fatalf("assistant tool_calls len=%d want 2: %#v", len(tcs), msg)
			}
			foundAssistant = true
		case "tool":
			if msg["tool_call_id"] == "c0" && msg["content"] == "a.txt" {
				foundTool0 = true
			}
			if msg["tool_call_id"] == "c1" && msg["content"] == "ok" {
				foundTool1 = true
			}
		case "user":
			if msg["content"] != "run" {
				// input_text should flatten to plain string
				if s, ok := msg["content"].(string); !ok || s != "run" {
					t.Fatalf("user content=%#v want run", msg["content"])
				}
			}
		}
	}
	if !foundAssistant || !foundTool0 || !foundTool1 {
		t.Fatalf("history incomplete assistant=%v tool0=%v tool1=%v msgs=%#v", foundAssistant, foundTool0, foundTool1, msgs)
	}
	tools, _ := m["tools"].([]any)
	if len(tools) != 2 {
		t.Fatalf("tools=%#v", m["tools"])
	}
	t0, _ := tools[0].(map[string]any)
	fn, _ := t0["function"].(map[string]any)
	if fn["name"] != "Bash" {
		t.Fatalf("tool nested function=%#v", t0)
	}
	tc, _ := m["tool_choice"].(map[string]any)
	if tc["type"] != "function" {
		t.Fatalf("tool_choice=%#v", m["tool_choice"])
	}
}

func TestResponsesToAnthropicViaChat_ToolHistory(t *testing.T) {
	// Responses → Chat → Anthropic 两跳后仍应保留 tool_use/tool_result
	in := []byte(`{
		"model":"gpt-x",
		"input":[
			{"role":"user","content":"hi"},
			{"type":"function_call","call_id":"call_1","name":"ping","arguments":"{}"},
			{"type":"function_call_output","call_id":"call_1","output":"pong"}
		],
		"tools":[{"type":"function","name":"ping","parameters":{"type":"object","properties":{}}}],
		"max_output_tokens":64
	}`)
	chat, err := ResponsesToOpenAIChatRequest(in, "gpt-x", false)
	if err != nil {
		t.Fatal(err)
	}
	anth, err := OpenAIToAnthropicRequest(chat, "claude-x", false)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(anth, &m); err != nil {
		t.Fatal(err)
	}
	msgs, _ := m["messages"].([]any)
	if len(msgs) < 3 {
		t.Fatalf("messages=%#v", msgs)
	}
	// 期望 assistant tool_use + user tool_result
	raw, _ := json.Marshal(msgs)
	s := string(raw)
	if !strings.Contains(s, "tool_use") || !strings.Contains(s, "tool_result") {
		t.Fatalf("missing tool blocks: %s", s)
	}
	tools, _ := m["tools"].([]any)
	if len(tools) == 0 {
		t.Fatalf("tools dropped: %s", string(anth))
	}
}

func TestResponsesStreamOrJSONToOpenAIChat_FunctionCallFromCompletedEvent(t *testing.T) {
	// 复现：流式缓冲末行是 response.completed，payload 在 .response 内
	sse := "" +
		"event: response.created\n" +
		"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\",\"status\":\"in_progress\"}}\n\n" +
		"event: response.output_item.done\n" +
		"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"ping\",\"arguments\":\"{\\\"x\\\":1}\"}}\n\n" +
		"event: response.completed\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\",\"status\":\"completed\",\"output\":[{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"ping\",\"arguments\":\"{\\\"x\\\":1}\"}],\"usage\":{\"input_tokens\":10,\"output_tokens\":5}}}\n\n"

	out, err := ResponsesStreamOrJSONToOpenAIChat([]byte(sse), "m")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	choices, _ := m["choices"].([]any)
	if len(choices) == 0 {
		t.Fatalf("no choices: %s", string(out))
	}
	ch := choices[0].(map[string]any)
	if ch["finish_reason"] != "tool_calls" {
		t.Fatalf("finish_reason=%v want tool_calls body=%s", ch["finish_reason"], string(out))
	}
	msg := ch["message"].(map[string]any)
	tcs, _ := msg["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("tool_calls=%v body=%s", msg["tool_calls"], string(out))
	}
	tc := tcs[0].(map[string]any)
	fn := tc["function"].(map[string]any)
	if tc["id"] != "call_1" || fn["name"] != "ping" {
		t.Fatalf("tc=%v", tc)
	}
	// 旧逻辑只取最后一行当 Response 会得到空 tool_calls
	if msg["content"] != nil && msg["content"] != "" {
		// tool-only 应为 null
		t.Fatalf("content should be null for tool-only, got %v", msg["content"])
	}
}

func TestResponsesStreamOrJSONToOpenAIChat_AssembleFromOutputItemDone(t *testing.T) {
	// 无 response.completed 时，从 output_item.done 组装
	sse := "" +
		"data: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"c9\",\"name\":\"lookup\",\"arguments\":\"{}\"}}\n\n"
	out, err := ResponsesStreamOrJSONToOpenAIChat([]byte(sse), "m")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	msg := m["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	tcs := msg["tool_calls"].([]any)
	if len(tcs) != 1 {
		t.Fatalf("tool_calls=%v", msg["tool_calls"])
	}
}

func TestResponsesToOpenAIChatResponse_UnwrapCompletedEnvelope(t *testing.T) {
	in := []byte(`{"type":"response.completed","response":{"id":"r1","model":"m","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hi"}]}]}}`)
	out, err := ResponsesToOpenAIChatResponse(in, "m")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	msg := m["choices"].([]any)[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "hi" {
		t.Fatalf("content=%v", msg["content"])
	}
}

func TestPathFor_Passthrough(t *testing.T) {
	if PathFor(KindOpenAIChat, "/v1/embeddings") != "/v1/embeddings" {
		t.Fatal("embeddings path")
	}
	if PathFor(KindOpenAIResponses, "/v1/responses/compact") != "/v1/responses/compact" {
		t.Fatal("responses subpath")
	}
}

func TestAnthropicSSEToOpenAISSE(t *testing.T) {
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"model\":\"c\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"hi\"}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
	out := string(AnthropicSSEToOpenAISSE([]byte(sse), "c"))
	if !strings.Contains(out, "chat.completion.chunk") {
		t.Fatalf("out=%s", out)
	}
	if !strings.Contains(out, "[DONE]") {
		t.Fatalf("missing DONE")
	}
}

func TestAnthropicToResponsesRequest_ToolsAndHistory(t *testing.T) {
	in := []byte(`{
		"model":"claude-x",
		"max_tokens":256,
		"stream":true,
		"system":"sys",
		"messages":[
			{"role":"user","content":"ping"},
			{"role":"assistant","content":[
				{"type":"text","text":"ok"},
				{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"command":"echo hi"}}
			]},
			{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"toolu_1","content":"hi"}
			]}
		],
		"tools":[{"name":"Bash","description":"shell","input_schema":{"type":"object","properties":{"command":{"type":"string"}}}}],
		"tool_choice":{"type":"auto"}
	}`)
	out, err := AnthropicToResponsesRequest(in, "grok-4.5", true)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["stream"] != true {
		t.Fatalf("stream=%v", m["stream"])
	}
	if m["model"] != "grok-4.5" {
		t.Fatalf("model=%v", m["model"])
	}
	if m["instructions"] != "sys" {
		t.Fatalf("instructions=%v", m["instructions"])
	}
	input, _ := m["input"].([]any)
	// user + assistant text + function_call + function_call_output
	if len(input) < 3 {
		t.Fatalf("input len=%d body=%s", len(input), string(out))
	}
	// last should be function_call_output
	last, _ := input[len(input)-1].(map[string]any)
	if last["type"] != "function_call_output" || last["call_id"] != "toolu_1" {
		t.Fatalf("last=%v", last)
	}
	tools, _ := m["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("tools=%v", m["tools"])
	}
	tool, _ := tools[0].(map[string]any)
	if tool["name"] != "Bash" {
		t.Fatalf("tool=%v", tool)
	}
	if _, has := tool["function"]; has {
		t.Fatal("must not nest function")
	}
	params, _ := tool["parameters"].(map[string]any)
	if params == nil {
		t.Fatalf("params nil tool=%v", tool)
	}
	if _, ok := params["properties"]; !ok {
		t.Fatalf("params missing properties: %v", params)
	}
	if m["tool_choice"] != "auto" {
		t.Fatalf("tool_choice=%v", m["tool_choice"])
	}
}

func TestResponsesStreamOrJSONToAnthropicSSE_ToolUse(t *testing.T) {
	sse := "event: response.completed\n" +
		`data: {"type":"response.completed","response":{"id":"resp_9","model":"m","status":"completed","output":[{"type":"function_call","call_id":"call_1","name":"Bash","arguments":"{\"command\":\"ls\"}"}],"usage":{"input_tokens":3,"output_tokens":2}}}` + "\n\n"
	out, err := ResponsesStreamOrJSONToAnthropicSSE([]byte(sse), "m")
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "event: message_start") {
		t.Fatalf("missing message_start: %s", s)
	}
	if !strings.Contains(s, `"type":"tool_use"`) && !strings.Contains(s, `"type": "tool_use"`) {
		// JSON compact
		if !strings.Contains(s, "tool_use") {
			t.Fatalf("missing tool_use: %s", s)
		}
	}
	if !strings.Contains(s, "tool_use") {
		t.Fatalf("missing stop tool_use: %s", s)
	}
	if !strings.Contains(s, "message_stop") {
		t.Fatalf("missing message_stop: %s", s)
	}
}

func TestResponsesStreamOrJSONToAnthropicSSE_ChatFallback(t *testing.T) {
	// 上游 /v1/responses 实际回了 chat.completion
	body := []byte(`{"id":"c1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":1}}`)
	out, err := ResponsesStreamOrJSONToAnthropicSSE(body, "m")
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if !strings.Contains(s, "message_start") || !strings.Contains(s, "hello") {
		t.Fatalf("out=%s", s)
	}
}

func TestResponsesObjectToAnthropic_ReasoningAndTool(t *testing.T) {
	body := []byte(`{"id":"resp_1","model":"m","status":"completed","output":[
		{"type":"reasoning","summary":[{"type":"summary_text","text":"think"}]},
		{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ans"}]},
		{"type":"function_call","call_id":"call_x","name":"ping","arguments":"{}"}
	],"usage":{"input_tokens":10,"output_tokens":5}}`)
	out, err := ResponsesToAnthropicResponse(body, "m")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatal(err)
	}
	if m["stop_reason"] != "tool_use" {
		t.Fatalf("stop=%v body=%s", m["stop_reason"], string(out))
	}
	blocks, _ := m["content"].([]any)
	if len(blocks) < 3 {
		t.Fatalf("blocks=%v", m["content"])
	}
	b0 := blocks[0].(map[string]any)
	if b0["type"] != "thinking" {
		t.Fatalf("b0=%v", b0)
	}
}
