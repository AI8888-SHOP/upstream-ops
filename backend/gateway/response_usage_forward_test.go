package gateway

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func zeroUsageTestBody(kind protocol.Kind, stream bool, text string, output int, includeUsage, terminal bool) string {
	usage := ""
	if includeUsage {
		usage = fmt.Sprintf(`,"usage":{"input_tokens":0,"output_tokens":%d}`, output)
		if kind == protocol.KindOpenAIChat {
			usage = fmt.Sprintf(`,"usage":{"prompt_tokens":0,"completion_tokens":%d}`, output)
		}
	}
	if !stream {
		switch kind {
		case protocol.KindAnthropic:
			return fmt.Sprintf(`{"id":"msg-1","type":"message","role":"assistant","model":"m","content":[{"type":"text","text":%q}],"stop_reason":"end_turn"%s}`, text, usage)
		case protocol.KindOpenAIResponses:
			return fmt.Sprintf(`{"id":"resp-1","object":"response","model":"m","status":"completed","output":[{"id":"msg-1","type":"message","role":"assistant","content":[{"type":"output_text","text":%q}]}]%s}`, text, usage)
		default:
			return fmt.Sprintf(`{"id":"chat-1","object":"chat.completion","model":"m","choices":[{"index":0,"message":{"role":"assistant","content":%q},"finish_reason":"stop"}]%s}`, text, usage)
		}
	}
	sse := func(payload string) string { return "data: " + payload + "\n\n" }
	initialUsage := ""
	if includeUsage {
		initialUsage = `,"usage":{"input_tokens":0,"output_tokens":0}`
	}
	var body string
	switch kind {
	case protocol.KindAnthropic:
		body = sse(`{"type":"message_start","message":{"id":"msg-1","type":"message","role":"assistant","model":"m","content":[]` + initialUsage + `}}`)
		body += sse(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		if text != "" {
			body += sse(fmt.Sprintf(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":%q}}`, text))
		}
		body += sse(`{"type":"content_block_stop","index":0}`)
		// Anthropic final usage normally supplies only output_tokens.
		if includeUsage {
			usage = fmt.Sprintf(`,"usage":{"output_tokens":%d}`, output)
		}
		body += sse(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}` + usage + `}`)
		if terminal {
			body += sse(`{"type":"message_stop"}`)
		}
	case protocol.KindOpenAIResponses:
		body = sse(`{"type":"response.created","response":{"id":"resp-1","model":"m","status":"in_progress"` + initialUsage + `}}`)
		if text != "" {
			body += sse(fmt.Sprintf(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":%q}`, text))
		}
		if terminal {
			body += sse(`{"type":"response.completed","response":{"id":"resp-1","model":"m","status":"completed","output":[]` + usage + `}}`)
		}
	default:
		body = sse(`{"id":"chat-1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":""}}]}`)
		if text != "" {
			body += sse(fmt.Sprintf(`{"id":"chat-1","choices":[{"index":0,"delta":{"content":%q}}]}`, text))
		}
		body += sse(`{"id":"chat-1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
		body += sse(`{"id":"chat-1","choices":[]` + usage + `}`)
		if terminal {
			body += sse(`[DONE]`)
		}
	}
	return body
}

func TestZeroUsageRejectionAndFallbackAcrossProtocols(t *testing.T) {
	kinds := []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses}
	for _, stream := range []bool{false, true} {
		for _, upstream := range kinds {
			for _, inbound := range kinds {
				t.Run(fmt.Sprintf("stream=%v/%s-to-%s", stream, upstream, inbound), func(t *testing.T) {
					var calls atomic.Int32
					var countCalls atomic.Int32
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						if r.URL.Path == "/v1/messages/count_tokens" {
							countCalls.Add(1)
							w.Header().Set("Content-Type", "application/json")
							_, _ = io.WriteString(w, `{"input_tokens":10}`)
							return
						}
						if stream {
							w.Header().Set("Content-Type", "text/event-stream")
						} else {
							w.Header().Set("Content-Type", "application/json")
						}
						text, output := "", 0
						if calls.Add(1) > 1 {
							text, output = "fallback ok", 2
						}
						_, _ = io.WriteString(w, zeroUsageTestBody(upstream, stream, text, output, true, true))
					}))
					t.Cleanup(server.Close)
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
					validator := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{ID: 9, Name: "zero", Enabled: true, Target: "zero_usage"})
					req := &coordinatedForwardRequest{c: c, kind: inbound, group: &storage.GatewayGroup{}, requestedModel: "m", validator: validator}
					rt := &Runtime{Service: &Service{}}
					for number := 1; number <= 2; number++ {
						attempt := &coordinatedForwardAttempt{
							Info: hedgeAttemptInfo{Number: number}, StartedAt: time.Now(),
							Target:       &upstreamTarget{BaseURL: server.URL, APIKey: "k"},
							UpstreamKind: upstream, UpstreamModel: "m", UpstreamPath: "/v1/chat/completions",
							UpstreamURL: server.URL + "/v1/chat/completions", Converted: inbound != upstream,
							ForwardBody: []byte(fmt.Sprintf(`{"model":"m","stream":%v,"messages":[]}`, stream)),
						}
						var err error
						if stream {
							_, err = rt.runCoordinatedStreamAttempt(ctx, req, attempt, 2*time.Second)
						} else {
							_, err = rt.runCoordinatedNonStreamAttempt(ctx, req, attempt, 2*time.Second)
						}
						if err != nil {
							t.Fatal(err)
						}
						if number == 1 {
							if !attempt.Validation.IsRejected() || attempt.Validation.PostCommit || attempt.Validation.RuleID != 9 || recorder.Body.Len() != 0 {
								t.Fatalf("validation=%+v body=%s", attempt.Validation, recorder.Body.String())
							}
							if countCalls.Load() != 0 {
								t.Fatal("zero usage was recovered before rejection")
							}
							if stream {
								attempt.Gate.Lose()
							}
							continue
						}
						if attempt.Validation.IsRejected() {
							t.Fatalf("normal response rejected: %+v", attempt.Validation)
						}
						body := string(attempt.ClientBody)
						if stream {
							if err := attempt.Gate.Win(); err != nil {
								t.Fatal(err)
							}
							result := attempt.awaitStreamResult()
							if result.Err != nil || attempt.Gate.LateMatch().IsRejected() {
								t.Fatalf("result=%+v late=%+v", result, attempt.Gate.LateMatch())
							}
							body = recorder.Body.String()
						}
						if !strings.Contains(body, "fallback ok") {
							t.Fatalf("fallback output missing: %s", body)
						}
					}
					if calls.Load() != 2 {
						t.Fatalf("upstream calls=%d", calls.Load())
					}
				})
			}
		}
	}
}

func TestZeroUsageStreamEOFAndLateAudit(t *testing.T) {
	for _, kind := range []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses} {
		for _, tc := range []struct {
			name, text        string
			usage, terminal   bool
			reject, lateAudit bool
		}{
			{"zero-at-eof", "", true, false, true, false},
			{"late-zero", "already visible", true, true, false, true},
			{"missing-usage", "", false, true, false, false},
		} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
				v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{ID: 9, Enabled: true, Target: "zero_usage"})
				gate := newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator(string(kind), "m"))
				c.Writer = gate
				defer gate.Lose()
				resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(zeroUsageTestBody(kind, true, tc.text, 0, tc.usage, tc.terminal)))}
				defer resp.Body.Close()
				resultCh := make(chan streamAttemptResult, 1)
				var recoveryCalls atomic.Int32
				go func() {
					result := (&Runtime{Service: &Service{}}).forwardStreamIncrementalWithRecovery(
						context.Background(), context.Background(), nil, c, resp, time.Now(), time.Second,
						kind, kind, "m", false, resp.Header, resp.StatusCode, 100,
						func(tokens UsageTokens) UsageTokens {
							recoveryCalls.Add(1)
							tokens.InputTokens = 50
							return tokens
						},
					)
					gate.Finish()
					resultCh <- result
				}()
				select {
				case decision := <-gate.Ready():
					if decision.IsRejected() != tc.reject {
						t.Fatalf("decision=%+v, want rejection=%v", decision, tc.reject)
					}
					if !tc.reject {
						if err := gate.Win(); err != nil {
							t.Fatal(err)
						}
					}
				case <-time.After(3 * time.Second):
					t.Fatal("stream did not reach a decision")
				}
				select {
				case result := <-resultCh:
					if result.ValidationRejection.IsRejected() != tc.reject || gate.LateMatch().IsRejected() != tc.lateAudit {
						t.Fatalf("result=%+v late=%+v", result, gate.LateMatch())
					}
					if tc.reject && (recorder.Body.Len() != 0 || recoveryCalls.Load() != 0) {
						t.Fatalf("empty response leaked or was recovered: body=%s recovery=%d", recorder.Body.String(), recoveryCalls.Load())
					}
					if !tc.reject && result.Err != nil {
						t.Fatal(result.Err)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("stream did not finish")
				}
			})
		}
	}
}

func TestZeroUsageStreamReleasesContentBeforeFinalUsage(t *testing.T) {
	allowTail := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\"}}],\"usage\":{\"prompt_tokens\":0,\"completion_tokens\":0}}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"visible now\"}}]}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-allowTail:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n")
	}))
	t.Cleanup(server.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).WithContext(ctx)
	v := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{Enabled: true, Target: "zero_usage"})
	req := &coordinatedForwardRequest{c: c, kind: protocol.KindOpenAIChat, group: &storage.GatewayGroup{}, requestedModel: "m", validator: v}
	attempt := &coordinatedForwardAttempt{
		StartedAt: time.Now(), Target: &upstreamTarget{BaseURL: server.URL, APIKey: "k"},
		UpstreamKind: protocol.KindOpenAIChat, UpstreamModel: "m", UpstreamPath: "/v1/chat/completions",
		ForwardBody: []byte(`{"model":"m","stream":true,"messages":[]}`),
	}
	if _, err := (&Runtime{Service: &Service{}}).runCoordinatedStreamAttempt(ctx, req, attempt, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	defer attempt.Gate.Lose()
	if err := attempt.Gate.Win(); err != nil {
		t.Fatal(err)
	}
	// The server cannot send its final usage until this assertion succeeds.
	if !strings.Contains(recorder.Body.String(), "visible now") {
		t.Fatalf("first content was held for final usage: %s", recorder.Body.String())
	}
	close(allowTail)
	result := attempt.awaitStreamResult()
	if result.Err != nil || attempt.Gate.LateMatch().IsRejected() {
		t.Fatalf("result=%+v late=%+v", result, attempt.Gate.LateMatch())
	}
}

func TestZeroUsageMetadataPrefixIsBounded(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{Enabled: true, Target: "zero_usage"})
	gate := newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator("openai_chat", "m"))
	c.Writer = gate
	defer gate.Lose()
	frame := "data: {\"id\":\"" + strings.Repeat("x", maxResponsesLifecycleBufferBytes/2) + "\",\"choices\":[{\"delta\":{\"role\":\"assistant\"}}]}\n\n"
	resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(frame + frame))}
	defer resp.Body.Close()
	result := (&Runtime{Service: &Service{}}).forwardStreamIncremental(
		context.Background(), context.Background(), nil, c, resp, time.Now(), time.Second,
		protocol.KindOpenAIChat, protocol.KindOpenAIChat, "m", false, nil, http.StatusOK,
	)
	if result.Err == nil || !strings.Contains(result.Err.Error(), "prefix exceeded") || recorder.Body.Len() != 0 {
		t.Fatalf("result=%+v body length=%d", result, recorder.Body.Len())
	}
}
