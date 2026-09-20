package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/gin-gonic/gin"
)

// Production failure envelopes had no usage, despite HTTP 200.
const overloadedResponsesFailure = `{"response":{"error":{"code":"server_error","message":"Our servers are currently overloaded. Please try again later.","status":500,"type":"service_unavailable_error"},"id":"resp_0","object":"response","status":"failed"},"sequence_number":6,"type":"response.failed"}`
const badStatusResponsesFailure = `{"response":{"error":{"code":"bad_response_status_code","message":"openai_error (request id: regression)","param":"","type":"bad_response_status_code"},"status":"failed"},"sequence_number":5,"type":"response.failed"}`

func TestUpstreamStreamFailureOnlyInspectsEnvelope(t *testing.T) {
	for _, tc := range []struct {
		name, event, data string
		failed            bool
	}{
		{"production-overloaded", "", overloadedResponsesFailure, true},
		{"production-bad-status", "", badStatusResponsesFailure, true},
		{"named-error", "error", `{}`, true},
		{"anthropic", "", `{"type":"error","error":{"type":"overloaded_error","message":"busy"}}`, true},
		{"chat", "", `{"error":{"message":"busy"}}`, true},
		{"nested-status", "response.created", `{"response":{"status":"failed"}}`, true},
		{"canceled", "response.cancelled", `{}`, true},
		{"null-error", "response.completed", `{"response":{"status":"completed","error":null},"error":null}`, false},
		{"length-stop", "response.incomplete", `{"response":{"status":"incomplete","error":null,"incomplete_details":{"reason":"max_output_tokens"}}}`, false},
		{"incomplete-with-error", "response.incomplete", `{"response":{"status":"incomplete","error":{"message":"busy"}}}`, true},
		{"dialogue", "", `{"choices":[{"delta":{"content":"response.failed: error"}}]}`, false},
		{"tool-json", "", `{"choices":[{"delta":{"tool_calls":[{"function":{"arguments":"{\"error\":\"busy\"}"}}]}}]}`, false},
		{"tool-object", "", `{"item":{"error":{"message":"busy"},"status":"failed"}}`, false},
		{"escaped-key", "", `{"er\u0072or":{"message":"busy"}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := upstreamStreamFailure(tc.event, tc.data)
			if errors.Is(err, errUpstreamStreamFailure) != tc.failed {
				t.Fatalf("failure=%v, want failed=%v", err, tc.failed)
			}
		})
	}
}

func TestResponsesErrorEnvelopeTerminalClassification(t *testing.T) {
	for _, tc := range []struct {
		data     string
		terminal bool
	}{
		{`{"type":"response.created","error":null,"response":{"status":"in_progress"}}`, false},
		{`{"response":{"error":{"message":"busy"}}}`, true},
		{`{"response":{"error":null,"status":"in_progress"}}`, false},
		{`{"er\u0072or":{"message":"busy"}}`, true},
	} {
		_, terminal := classifyResponsesSSEEvent("", tc.data)
		if terminal != tc.terminal {
			t.Fatalf("%s: terminal=%v", tc.data, terminal)
		}
	}
}

func TestStreamFailureBeforeAndAfterOutputAcrossProtocols(t *testing.T) {
	kinds := []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses}
	for _, upstream := range kinds {
		for _, inbound := range kinds {
			for _, output := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s-to-%s/output=%v", upstream, inbound, output), func(t *testing.T) {
					body := ""
					failure := `{"error":{"message":"upstream busy"},"usage":{"prompt_tokens":7,"completion_tokens":2}}`
					switch upstream {
					case protocol.KindOpenAIResponses:
						body = "data: {\"type\":\"response.created\",\"response\":{\"id\":\"r1\",\"status\":\"in_progress\"}}\n\n"
						if output {
							body += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"visible answer\"}\n\n"
						}
						failure = `{"type":"response.failed","response":{"status":"failed","error":{"message":"upstream busy"},"usage":{"input_tokens":7,"output_tokens":2}}}`
					case protocol.KindAnthropic:
						if output {
							body = "data: {\"type\":\"message_start\",\"message\":{\"id\":\"m1\",\"role\":\"assistant\",\"content\":[]}}\n\n"
							body += "data: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n"
							body += "data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"visible answer\"}}\n\n"
						}
						failure = `{"type":"error","error":{"type":"overloaded_error","message":"upstream busy"},"usage":{"input_tokens":7,"output_tokens":2}}`
					default:
						if output {
							body = "data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"visible answer\"}}]}\n\n"
						}
					}
					body += "data: " + failure + "\n\n"
					recorder := httptest.NewRecorder()
					c, _ := gin.CreateTestContext(recorder)
					c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
					resp := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body))}
					defer resp.Body.Close()
					var recoveryCalls int
					result := (&Runtime{Service: &Service{}}).forwardStreamIncrementalWithRecovery(
						context.Background(), context.Background(), nil, c, resp, time.Now(), time.Second,
						inbound, upstream, "m", inbound != upstream, nil, http.StatusOK, 100,
						func(tokens UsageTokens) UsageTokens { recoveryCalls++; return tokens },
					)
					if !errors.Is(result.Err, errUpstreamStreamFailure) || result.Committed != output || recoveryCalls != 0 {
						t.Fatalf("result=%+v recovery=%d", result, recoveryCalls)
					}
					if result.Tokens.InputTokens != 7 || result.Tokens.OutputTokens != 2 || !strings.Contains(string(result.Body), "upstream busy") {
						t.Fatalf("lost failure usage/body: %+v", result)
					}
					if !output {
						if recorder.Body.Len() != 0 || result.FirstTokenMS != nil {
							t.Fatalf("failed prefix leaked: result=%+v body=%s", result, recorder.Body.String())
						}
						return
					}
					if !errors.Is(result.StreamErr, errUpstreamStreamFailure) || !strings.Contains(recorder.Body.String(), "upstream busy") || !strings.Contains(recorder.Body.String(), "visible answer") {
						t.Fatalf("result=%+v body=%s", result, recorder.Body.String())
					}
					if strings.Contains(recorder.Body.String(), "response.completed") || strings.Contains(recorder.Body.String(), "message_stop") {
						t.Fatalf("fabricated success after failure: %s", recorder.Body.String())
					}
				})
			}
		}
	}
}

func TestZeroUsagePreservesNonTextOutputWithoutUsage(t *testing.T) {
	for _, tc := range []struct {
		name, payload string
	}{
		{"refusal", `{"type":"response.completed","response":{"status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"Cannot comply"}]}]}}`},
		{"tool", `{"type":"response.completed","response":{"status":"completed","output":[{"type":"function_call","name":"clock","arguments":"{}"}]}}`},
		{"length-stop", `{"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[{"type":"message","content":[{"type":"output_text","text":"partial answer"}]}]}}`},
		{"real-cache", `{"type":"response.completed","response":{"status":"completed","output":[],"usage":{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":8}}}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
			v := mustResponseValidator(t, 8192, time.Second, responseRuleSpec{Enabled: true, Target: "zero_usage"})
			gate := newStreamPrefixGateWriter(c.Writer, v.NewStreamValidator("openai_responses", "m"))
			c.Writer = gate
			defer gate.Lose()
			resp := &http.Response{Body: io.NopCloser(strings.NewReader("data: " + tc.payload + "\n\n"))}
			defer resp.Body.Close()
			finished := make(chan streamAttemptResult, 1)
			go func() {
				result := (&Runtime{Service: &Service{}}).forwardStreamIncremental(context.Background(), context.Background(), nil, c, resp, time.Now(), time.Second,
					protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, nil, http.StatusOK)
				gate.Finish()
				finished <- result
			}()
			select {
			case decision := <-gate.Ready():
				if decision.IsRejected() {
					t.Fatalf("meaningful response rejected: %+v", decision)
				}
				if err := gate.Win(); err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("stream did not become ready")
			}
			select {
			case result := <-finished:
				if result.Err != nil || gate.LateMatch().IsRejected() {
					t.Fatalf("result=%+v late=%+v", result, gate.LateMatch())
				}
			case <-time.After(3 * time.Second):
				t.Fatal("stream did not finish")
			}
		})
	}
}

type cancelAfterStreamWrite struct {
	gin.ResponseWriter
	cancel context.CancelFunc
}

func (w *cancelAfterStreamWrite) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	w.cancel()
	return n, err
}

func TestStreamFailureWhileDrainingClientDisconnectIsNotSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
	c.Writer = &cancelAfterStreamWrite{ResponseWriter: c.Writer, cancel: cancel}
	body := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial answer\"}\n\ndata: " + overloadedResponsesFailure + "\n\n"
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(body))}
	defer resp.Body.Close()
	rt := &Runtime{Service: &Service{}}
	result := rt.forwardStreamIncremental(context.Background(), ctx, nil, c, resp, time.Now(), time.Second,
		protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, nil, http.StatusOK)
	if !result.Committed || !result.ClientDisconnected || result.DownstreamComplete || !errors.Is(result.StreamErr, errUpstreamStreamFailure) {
		t.Fatalf("result=%+v", result)
	}
	if rt.isClientDisconnectAfterCommit(result.ClientDisconnected, result.StreamErr) {
		t.Fatal("upstream failure was classified as a successful client disconnect")
	}
}
