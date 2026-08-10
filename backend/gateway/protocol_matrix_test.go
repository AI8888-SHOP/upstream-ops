package gateway

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// 六向协议互转集成测试：mock 上游 + forwardStream（真流路径）。
// 覆盖：
//  1. /v1/messages → chat
//  2. /v1/messages → responses
//  3. /v1/chat/completions → messages
//  4. /v1/chat/completions → responses
//  5. /v1/responses → chat
//  6. /v1/responses → messages

func TestProtocolMatrix_RequestConversion(t *testing.T) {
	type caseReq struct {
		name    string
		in      protocol.Kind
		up      protocol.Kind
		inPath  string
		body    string
		checkUp func(t *testing.T, path string, raw []byte)
	}
	cases := []caseReq{
		{
			name:   "messages→chat",
			in:     protocol.KindAnthropic,
			up:     protocol.KindOpenAIChat,
			inPath: "/v1/messages",
			body: `{
				"model":"m","max_tokens":64,"stream":true,
				"messages":[{"role":"user","content":"hi"}],
				"tools":[{"name":"Bash","description":"sh","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/chat/completions" {
					t.Fatalf("path=%s", path)
				}
				var m map[string]any
				if err := json.Unmarshal(raw, &m); err != nil {
					t.Fatal(err)
				}
				if m["stream"] != true {
					t.Fatalf("stream=%v", m["stream"])
				}
				msgs, _ := m["messages"].([]any)
				if len(msgs) == 0 {
					t.Fatalf("no messages: %s", raw)
				}
				tools, _ := m["tools"].([]any)
				if len(tools) == 0 {
					t.Fatalf("tools dropped: %s", raw)
				}
				// chat 嵌套 function
				t0, _ := tools[0].(map[string]any)
				if _, ok := t0["function"].(map[string]any); !ok {
					t.Fatalf("want nested function tool: %v", t0)
				}
			},
		},
		{
			name:   "messages→responses",
			in:     protocol.KindAnthropic,
			up:     protocol.KindOpenAIResponses,
			inPath: "/v1/messages",
			body: `{
				"model":"m","max_tokens":64,"stream":true,
				"messages":[
					{"role":"user","content":"run"},
					{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"Bash","input":{"cmd":"ls"}}]},
					{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"a.txt"}]}
				],
				"tools":[{"name":"Bash","input_schema":{"type":"object","properties":{"cmd":{"type":"string"}}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/responses" {
					t.Fatalf("path=%s", path)
				}
				var m map[string]any
				if err := json.Unmarshal(raw, &m); err != nil {
					t.Fatal(err)
				}
				input, _ := m["input"].([]any)
				if len(input) < 2 {
					t.Fatalf("input too short: %s", raw)
				}
				// 必须有 function_call + function_call_output
				s := string(raw)
				if !strings.Contains(s, `"type":"function_call"`) && !strings.Contains(s, `"type": "function_call"`) {
					t.Fatalf("missing function_call in input: %s", raw)
				}
				if !strings.Contains(s, "function_call_output") {
					t.Fatalf("missing function_call_output: %s", raw)
				}
				tools, _ := m["tools"].([]any)
				if len(tools) == 0 {
					t.Fatalf("tools dropped")
				}
			},
		},
		{
			name:   "chat→messages",
			in:     protocol.KindOpenAIChat,
			up:     protocol.KindAnthropic,
			inPath: "/v1/chat/completions",
			body: `{
				"model":"m","stream":true,"max_tokens":64,
				"messages":[
					{"role":"user","content":"hi"},
					{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"ping","arguments":"{}"}}]},
					{"role":"tool","tool_call_id":"c1","content":"pong"}
				],
				"tools":[{"type":"function","function":{"name":"ping","parameters":{"type":"object","properties":{}}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/messages" {
					t.Fatalf("path=%s", path)
				}
				s := string(raw)
				if !strings.Contains(s, "tool_use") || !strings.Contains(s, "tool_result") {
					t.Fatalf("missing tool blocks: %s", raw)
				}
				if !strings.Contains(s, "input_schema") && !strings.Contains(s, `"name":"ping"`) {
					t.Fatalf("tools bad: %s", raw)
				}
			},
		},
		{
			name:   "chat→responses",
			in:     protocol.KindOpenAIChat,
			up:     protocol.KindOpenAIResponses,
			inPath: "/v1/chat/completions",
			body: `{
				"model":"m","stream":true,
				"messages":[
					{"role":"user","content":"hi"},
					{"role":"assistant","content":null,"tool_calls":[{"id":"c1","type":"function","function":{"name":"ping","arguments":"{}"}}]},
					{"role":"tool","tool_call_id":"c1","content":"pong"}
				],
				"tools":[{"type":"function","function":{"name":"ping","parameters":{"type":"object","properties":{}}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/responses" {
					t.Fatalf("path=%s", path)
				}
				s := string(raw)
				if !strings.Contains(s, "function_call") || !strings.Contains(s, "function_call_output") {
					t.Fatalf("history lost: %s", raw)
				}
				// 禁止 tool_calls 残留在 message 里
				if strings.Contains(s, `"tool_calls"`) {
					t.Fatalf("tool_calls must not appear in responses input: %s", raw)
				}
			},
		},
		{
			name:   "responses→chat",
			in:     protocol.KindOpenAIResponses,
			up:     protocol.KindOpenAIChat,
			inPath: "/v1/responses",
			body: `{
				"model":"m","stream":true,
				"input":[
					{"type":"message","role":"user","content":[{"type":"input_text","text":"run"}]},
					{"type":"function_call","call_id":"c0","name":"Bash","arguments":"{\"cmd\":\"ls\"}"},
					{"type":"function_call_output","call_id":"c0","output":"a.txt"}
				],
				"tools":[{"type":"function","name":"Bash","parameters":{"type":"object","properties":{"cmd":{"type":"string"}}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/chat/completions" {
					t.Fatalf("path=%s", path)
				}
				var m map[string]any
				if err := json.Unmarshal(raw, &m); err != nil {
					t.Fatal(err)
				}
				msgs, _ := m["messages"].([]any)
				var hasAssistantTools, hasTool bool
				for _, r := range msgs {
					msg, _ := r.(map[string]any)
					if msg == nil {
						continue
					}
					if msg["role"] == "assistant" {
						if tcs, _ := msg["tool_calls"].([]any); len(tcs) > 0 {
							hasAssistantTools = true
						}
					}
					if msg["role"] == "tool" {
						hasTool = true
					}
				}
				if !hasAssistantTools || !hasTool {
					t.Fatalf("function_call history lost: %s", raw)
				}
				tools, _ := m["tools"].([]any)
				if len(tools) == 0 {
					t.Fatal("tools dropped")
				}
				t0, _ := tools[0].(map[string]any)
				if _, ok := t0["function"].(map[string]any); !ok {
					t.Fatalf("want nested tools: %v", t0)
				}
			},
		},
		{
			name:   "responses→messages",
			in:     protocol.KindOpenAIResponses,
			up:     protocol.KindAnthropic,
			inPath: "/v1/responses",
			body: `{
				"model":"m","stream":true,"max_output_tokens":64,
				"input":[
					{"role":"user","content":"hi"},
					{"type":"function_call","call_id":"c1","name":"ping","arguments":"{}"},
					{"type":"function_call_output","call_id":"c1","output":"pong"}
				],
				"tools":[{"type":"function","name":"ping","parameters":{"type":"object","properties":{}}}]
			}`,
			checkUp: func(t *testing.T, path string, raw []byte) {
				if path != "/v1/messages" {
					t.Fatalf("path=%s", path)
				}
				s := string(raw)
				if !strings.Contains(s, "tool_use") || !strings.Contains(s, "tool_result") {
					t.Fatalf("tool history lost: %s", raw)
				}
				if !strings.Contains(s, `"name":"ping"`) {
					t.Fatalf("tools lost: %s", raw)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd, path, converted, err := (*Service)(nil).prepareUpstreamRequest(
				[]byte(tc.body), tc.in, tc.up, "m", true, tc.inPath,
			)
			if err != nil {
				t.Fatalf("prepare err: %v", err)
			}
			if !converted {
				t.Fatal("expected converted=true")
			}
			tc.checkUp(t, path, fwd)
		})
	}
}

func TestProtocolMatrix_TrueStreamAllDirections(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type caseStream struct {
		name      string
		inbound   protocol.Kind
		upstream  protocol.Kind
		upPath    string
		clientReq string
		// mock 上游返回的原生 SSE（分块 + sleep）
		upstreamChunks []string
		// 客户端 body 必须包含
		wantClient []string
		// 客户端 body 禁止出现（防假流残留 / 协议串味）
		forbidClient []string
	}

	// 上游延迟：用于证明是增量写出而不是整包假流
	const chunkDelay = 25 * time.Millisecond

	cases := []caseStream{
		{
			name:      "messages←chat 真流",
			inbound:   protocol.KindAnthropic,
			upstream:  protocol.KindOpenAIChat,
			upPath:    "/v1/chat/completions",
			clientReq: `{"model":"m","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"hi"}]}`,
			upstreamChunks: []string{
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"role":"assistant","content":""},"finish_reason":null}]}` + "\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Hel"},"finish_reason":null}]}` + "\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"lo"},"finish_reason":null}]}` + "\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}` + "\n\n",
				`data: [DONE]` + "\n\n",
			},
			wantClient:   []string{"event: message_start", "text_delta", "Hel", "lo", "event: message_stop"},
			forbidClient: []string{"chat.completion.chunk", "[DONE]"},
		},
		{
			name:      "messages←responses 真流+tool",
			inbound:   protocol.KindAnthropic,
			upstream:  protocol.KindOpenAIResponses,
			upPath:    "/v1/responses",
			clientReq: `{"model":"m","stream":true,"max_tokens":32,"messages":[{"role":"user","content":"run"}],"tools":[{"name":"Bash","input_schema":{"type":"object","properties":{}}}]}`,
			upstreamChunks: []string{
				"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"model\":\"m\",\"status\":\"in_progress\"}}\n\n",
				"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"call_1\",\"name\":\"Bash\",\"arguments\":\"\"}}\n\n",
				"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_1\",\"delta\":\"{\\\"c\\\"\"}\n\n",
				"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"output_index\":0,\"call_id\":\"call_1\",\"delta\":\":\\\"x\\\"}\"}\n\n",
				"event: response.function_call_arguments.done\ndata: {\"type\":\"response.function_call_arguments.done\",\"output_index\":0,\"call_id\":\"call_1\",\"arguments\":\"{\\\"c\\\":\\\"x\\\"}\"}\n\n",
				"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\",\"usage\":{\"input_tokens\":2,\"output_tokens\":3}}}\n\n",
			},
			wantClient:   []string{"event: message_start", "tool_use", "input_json_delta", "event: message_stop"},
			forbidClient: []string{"response.created", "chat.completion"},
		},
		{
			name:      "chat←messages 真流",
			inbound:   protocol.KindOpenAIChat,
			upstream:  protocol.KindAnthropic,
			upPath:    "/v1/messages",
			clientReq: `{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`,
			upstreamChunks: []string{
				"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_1\",\"model\":\"m\",\"role\":\"assistant\",\"content\":[]}}\n\n",
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"AB\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"CD\"}}\n\n",
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			},
			wantClient:   []string{"chat.completion.chunk", `"content":"AB"`, `"content":"CD"`, "[DONE]"},
			forbidClient: []string{"event: message_start", "text_delta"},
		},
		{
			name:      "chat←responses 真流+tool无重复args",
			inbound:   protocol.KindOpenAIChat,
			upstream:  protocol.KindOpenAIResponses,
			upPath:    "/v1/responses",
			clientReq: `{"model":"m","stream":true,"messages":[{"role":"user","content":"x"}],"tools":[{"type":"function","function":{"name":"Bash","parameters":{"type":"object"}}}]}`,
			upstreamChunks: []string{
				"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"model\":\"m\"}}\n\n",
				"event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"Bash\",\"arguments\":\"\"}}\n\n",
				"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"c1\",\"delta\":\"{\\\"x\\\":\"}\n\n",
				"event: response.function_call_arguments.delta\ndata: {\"type\":\"response.function_call_arguments.delta\",\"call_id\":\"c1\",\"delta\":\"1}\"}\n\n",
				"event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"item\":{\"type\":\"function_call\",\"call_id\":\"c1\",\"name\":\"Bash\",\"arguments\":\"{\\\"x\\\":1}\"}}\n\n",
				"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"r1\",\"status\":\"completed\"}}\n\n",
			},
			wantClient:   []string{"chat.completion.chunk", "tool_calls", "Bash", "[DONE]"},
			forbidClient: []string{`"arguments":"{\"x\":1}"`, "event: response."}, // 全量 args 不得在 delta 后再推
		},
		{
			name:      "responses←chat 真流",
			inbound:   protocol.KindOpenAIResponses,
			upstream:  protocol.KindOpenAIChat,
			upPath:    "/v1/chat/completions",
			clientReq: `{"model":"m","stream":true,"input":[{"role":"user","content":"hi"}]}`,
			upstreamChunks: []string{
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"X"},"finish_reason":null}]}` + "\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{"content":"Y"},"finish_reason":null}]}` + "\n\n",
				`data: {"id":"c1","object":"chat.completion.chunk","choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":1,"completion_tokens":2}}` + "\n\n",
				`data: [DONE]` + "\n\n",
			},
			wantClient:   []string{"response.created", "output_text.delta", "X", "Y", "response.completed"},
			forbidClient: []string{"chat.completion.chunk", "[DONE]"},
		},
		{
			name:      "responses←messages 真流",
			inbound:   protocol.KindOpenAIResponses,
			upstream:  protocol.KindAnthropic,
			upPath:    "/v1/messages",
			clientReq: `{"model":"m","stream":true,"input":"hi"}`,
			upstreamChunks: []string{
				"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_9\",\"model\":\"m\"}}\n\n",
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"P\"}}\n\n",
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"Q\"}}\n\n",
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n",
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
			},
			wantClient:   []string{"response.created", "output_text.delta", "P", "Q", "response.completed"},
			forbidClient: []string{"event: message_start", "text_delta"},
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var (
				mu         sync.Mutex
				gotPath    string
				gotBody    []byte
				flushTimes []time.Time
			)

			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/messages/count_tokens" {
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"input_tokens":1}`)
					return
				}
				mu.Lock()
				gotPath = r.URL.Path
				gotBody, _ = io.ReadAll(r.Body)
				mu.Unlock()

				w.Header().Set("Content-Type", "text/event-stream")
				w.WriteHeader(http.StatusOK)
				flusher, _ := w.(http.Flusher)
				for i, chunk := range tc.upstreamChunks {
					_, _ = io.WriteString(w, chunk)
					if flusher != nil {
						flusher.Flush()
					}
					if i < len(tc.upstreamChunks)-1 {
						time.Sleep(chunkDelay)
					}
				}
			}))
			defer upstream.Close()

			// 客户端侧：带时间戳的 flush 记录
			rec := &timedFlushRecorder{ResponseRecorder: httptest.NewRecorder(), times: &flushTimes}
			c, _ := gin.CreateTestContext(rec)
			c.Request = httptest.NewRequest(http.MethodPost, "/test", nil)

			// 请求先做协议转换（与线上一致）
			fwd, upPath, converted, err := (*Service)(nil).prepareUpstreamRequest(
				[]byte(tc.clientReq), tc.inbound, tc.upstream, "m", true, protocol.PathFor(tc.inbound, ""),
			)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if !converted {
				t.Fatal("want converted")
			}
			if upPath != tc.upPath {
				t.Fatalf("upPath=%s want %s", upPath, tc.upPath)
			}

			svc := &Service{}
			start := time.Now()
			res := svc.forwardStream(
				c.Request.Context(), c,
				&upstreamTarget{BaseURL: upstream.URL, APIKey: "k"},
				upPath, http.MethodPost,
				http.Header{"Content-Type": []string{"application/json"}},
				fwd, tc.inbound, tc.upstream, "m", true, 0,
			)
			elapsed := time.Since(start)

			if res.Err != nil {
				t.Fatalf("forwardStream err: %v", res.Err)
			}
			if !res.Committed {
				t.Fatal("expected committed")
			}
			if res.StreamErr != nil {
				t.Fatalf("streamErr: %v", res.StreamErr)
			}

			out := rec.Body.String()
			for _, w := range tc.wantClient {
				if !strings.Contains(out, w) {
					t.Fatalf("client missing %q\nbody=\n%s", w, out)
				}
			}
			for _, bad := range tc.forbidClient {
				if strings.Contains(out, bad) {
					t.Fatalf("client forbid %q\nbody=\n%s", bad, out)
				}
			}

			// 真流判定：多次 flush + 总耗时至少接近 chunk 间隔之和
			mu.Lock()
			path := gotPath
			upBody := string(gotBody)
			mu.Unlock()
			if path != tc.upPath {
				t.Fatalf("upstream path=%s want %s body=%s", path, tc.upPath, upBody)
			}
			if rec.flushes < 2 {
				t.Fatalf("want >=2 flushes for true stream, got %d (fake stream?)", rec.flushes)
			}
			minExpected := chunkDelay * time.Duration(len(tc.upstreamChunks)-2)
			if minExpected > 0 && elapsed < minExpected/2 {
				// 若几乎瞬间结束，说明可能整包缓冲（假流）
				t.Fatalf("elapsed=%s too fast for %d delayed chunks (suspect buffered/fake stream)", elapsed, len(tc.upstreamChunks))
			}
			t.Logf("OK %s: flushes=%d elapsed=%s body_len=%d", tc.name, rec.flushes, elapsed, len(out))
		})
	}
}

// timedFlushRecorder 记录每次 Flush 时刻，便于判断真流。
type timedFlushRecorder struct {
	*httptest.ResponseRecorder
	flushes int
	times   *[]time.Time
}

func (f *timedFlushRecorder) Flush() {
	f.flushes++
	if f.times != nil {
		*f.times = append(*f.times, time.Now())
	}
}

func TestProtocolMatrix_SupportsIncrementalAllPairs(t *testing.T) {
	pairs := [][2]protocol.Kind{
		{protocol.KindAnthropic, protocol.KindOpenAIChat},
		{protocol.KindAnthropic, protocol.KindOpenAIResponses},
		{protocol.KindOpenAIChat, protocol.KindAnthropic},
		{protocol.KindOpenAIChat, protocol.KindOpenAIResponses},
		{protocol.KindOpenAIResponses, protocol.KindOpenAIChat},
		{protocol.KindOpenAIResponses, protocol.KindAnthropic},
	}
	for _, p := range pairs {
		if !protocol.SupportsIncrementalStream(p[0], p[1], true) {
			t.Fatalf("NOT incremental (would fake-stream): %s←%s", p[0], p[1])
		}
	}
}
