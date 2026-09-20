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

	"github.com/bejix/upstream-ops/backend/crypto"
	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

// Keep usage in the final frame: receiving content must neither imply that
// usage exists nor postpone delivery until accounting metadata arrives.
func strictUsageStreamParts(kind protocol.Kind, usage string) (string, string) {
	field := ""
	if usage != "" {
		field = `,"usage":` + usage
	}
	frame := func(payload string) string { return "data: " + payload + "\n\n" }
	switch kind {
	case protocol.KindAnthropic:
		prefix := frame(`{"type":"message_start","message":{"id":"m1","role":"assistant","model":"m","content":[]}}`)
		prefix += frame(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`)
		prefix += frame(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"visible answer"}}`)
		tail := frame(`{"type":"content_block_stop","index":0}`)
		tail += frame(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}` + field + `}`)
		return prefix, tail + frame(`{"type":"message_stop"}`)
	case protocol.KindOpenAIResponses:
		prefix := frame(`{"type":"response.created","response":{"id":"r1","model":"m","status":"in_progress"}}`)
		prefix += frame(`{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"visible answer"}`)
		return prefix, frame(`{"type":"response.completed","response":{"id":"r1","model":"m","status":"completed","output":[]` + field + `}}`)
	default:
		prefix := frame(`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","content":"visible answer"}}]}`)
		tail := frame(`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`)
		tail += frame(`{"id":"c1","choices":[]` + field + `}`)
		return prefix, tail + frame(`[DONE]`)
	}
}

func runStrictUsageStream(t *testing.T, upstream, inbound protocol.Kind, body string, validator *responseValidator) (streamAttemptResult, validationResult, string) {
	t.Helper()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil)
	gate := newStreamPrefixGateWriter(c.Writer, validator.NewStreamValidator(string(inbound), "m"))
	c.Writer = gate
	defer gate.Lose()
	resp := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(body))}
	defer resp.Body.Close()
	finished := make(chan streamAttemptResult, 1)
	go func() {
		result := (&Runtime{Service: &Service{}}).forwardStreamIncremental(context.Background(), context.Background(), nil, c, resp, time.Now(), time.Second,
			inbound, upstream, "m", inbound != upstream, resp.Header, http.StatusOK)
		gate.Finish()
		finished <- result
	}()
	select {
	case decision := <-gate.Ready():
		if !decision.IsRejected() {
			if err := gate.Win(); err != nil {
				t.Fatal(err)
			}
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not reach a routing decision")
	}
	select {
	case result := <-finished:
		return result, gate.LateMatch(), recorder.Body.String()
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not finish")
		return streamAttemptResult{}, validationResult{}, ""
	}
}

func assertStrictUsageFailureTerminal(t *testing.T, inbound protocol.Kind, body string) {
	t.Helper()
	if !strings.Contains(body, `"error"`) {
		t.Fatalf("client did not receive an error: %s", body)
	}
	if strings.Contains(body, "response.completed") || strings.Contains(body, "message_stop") {
		t.Fatalf("a successful terminal event escaped usage validation: %s", body)
	}
	if inbound == protocol.KindOpenAIResponses && !strings.Contains(body, "response.failed") {
		t.Fatalf("Responses client did not receive response.failed: %s", body)
	}
}

func TestStrictZeroUsageAfterContentAcrossProtocols(t *testing.T) {
	kinds := []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses}
	for _, upstream := range kinds {
		for _, inbound := range kinds {
			for _, tc := range []struct {
				name, usage string
				eof         bool
			}{
				{name: "missing"},
				{name: "zero", usage: `{"input_tokens":0,"output_tokens":0}`},
				{name: "null", usage: `null`},
				{name: "invalid", usage: `{"input_tokens":"unknown","output_tokens":null}`},
				{name: "partial-zero", usage: `{"output_tokens":0}`},
				{name: "missing-at-eof", eof: true},
			} {
				t.Run(fmt.Sprintf("%s-to-%s/%s", upstream, inbound, tc.name), func(t *testing.T) {
					prefix, tail := strictUsageStreamParts(upstream, tc.usage)
					if tc.eof {
						tail = ""
					}
					validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{ID: 17, Name: "strict usage", Enabled: true, Target: "zero_usage"})
					result, late, body := runStrictUsageStream(t, upstream, inbound, prefix+tail, validator)
					if !result.Committed || result.StreamErr == nil || !late.IsRejected() || !late.PostCommit || late.RuleID != 17 {
						t.Fatalf("missing/zero usage was accepted after content: result=%+v late=%+v body=%s", result, late, body)
					}
					if !strings.Contains(body, "visible answer") {
						t.Fatalf("content already sent to the client was lost: %s", body)
					}
					assertStrictUsageFailureTerminal(t, inbound, body)
				})
			}
		}
	}
}

func TestStrictZeroUsageAcceptsRealUpstreamCounters(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		placeholder bool
	}{
		{name: "input", usage: `{"input_tokens":7,"output_tokens":0}`},
		{name: "output", usage: `{"input_tokens":0,"output_tokens":2}`},
		{name: "cache", usage: `{"input_tokens":0,"output_tokens":0,"input_tokens_details":{"cached_tokens":7}}`},
		{name: "reasoning", usage: `{"input_tokens":0,"output_tokens":0,"output_tokens_details":{"reasoning_tokens":2}}`},
		{name: "positive-after-null-placeholder", usage: `{"input_tokens":7,"output_tokens":2}`, placeholder: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prefix, tail := strictUsageStreamParts(protocol.KindOpenAIResponses, tc.usage)
			if tc.placeholder {
				prefix = strings.Replace(prefix, `"status":"in_progress"`, `"status":"in_progress","usage":{"input_tokens":null,"output_tokens":null}`, 1)
			}
			validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{Enabled: true, Target: "zero_usage"})
			result, late, body := runStrictUsageStream(t, protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, prefix+tail, validator)
			if result.Err != nil || result.StreamErr != nil || late.IsRejected() || !strings.Contains(body, "response.completed") {
				t.Fatalf("real upstream usage was rejected: result=%+v late=%+v body=%s", result, late, body)
			}
		})
	}
}

func TestStrictZeroUsageRespectsRuleScope(t *testing.T) {
	for _, tc := range []struct {
		name, streamMode string
		disabled         bool
		rule             responseRuleSpec
	}{
		{name: "validation-disabled", disabled: true, rule: responseRuleSpec{Enabled: true, Target: "zero_usage"}},
		{name: "stream-disabled", streamMode: "disabled", rule: responseRuleSpec{Enabled: true, Target: "zero_usage"}},
		{name: "rule-disabled", rule: responseRuleSpec{Enabled: false, Target: "zero_usage"}},
		{name: "different-model", rule: responseRuleSpec{Enabled: true, Target: "zero_usage", Models: []string{"other-model"}}},
		{name: "different-protocol", rule: responseRuleSpec{Enabled: true, Target: "zero_usage", Protocols: []string{"anthropic"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validator, err := newResponseValidator(responseValidationConfig{Enabled: !tc.disabled, StreamMode: tc.streamMode, PrefixBytes: 8192, PrefixTimeout: time.Second}, []responseRuleSpec{tc.rule})
			if err != nil {
				t.Fatal(err)
			}
			prefix, tail := strictUsageStreamParts(protocol.KindOpenAIResponses, "")
			result, late, body := runStrictUsageStream(t, protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, prefix+tail, validator)
			if result.Err != nil || result.StreamErr != nil || late.IsRejected() || !strings.Contains(body, "response.completed") {
				t.Fatalf("nonmatching rule changed stream behavior: result=%+v late=%+v body=%s", result, late, body)
			}
		})
	}
}

func TestStrictZeroUsageReleasesContentBeforeFinalUsageAcrossProtocols(t *testing.T) {
	kinds := []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses}
	for _, upstream := range kinds {
		for _, inbound := range kinds {
			t.Run(fmt.Sprintf("%s-to-%s", upstream, inbound), func(t *testing.T) {
				prefix, tail := strictUsageStreamParts(upstream, `{"input_tokens":7,"output_tokens":2}`)
				allowTail := make(chan struct{})
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, prefix)
					w.(http.Flusher).Flush()
					select {
					case <-allowTail:
					case <-r.Context().Done():
						return
					}
					_, _ = io.WriteString(w, tail)
				}))
				t.Cleanup(server.Close)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				recorder := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(recorder)
				c.Request = httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
				validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{Enabled: true, Target: "zero_usage"})
				req := &coordinatedForwardRequest{c: c, kind: inbound, group: &storage.GatewayGroup{}, requestedModel: "m", validator: validator}
				attempt := &coordinatedForwardAttempt{
					StartedAt: time.Now(), Target: &upstreamTarget{BaseURL: server.URL, APIKey: "k"},
					UpstreamKind: upstream, UpstreamModel: "m", UpstreamPath: "/v1/chat/completions", Converted: inbound != upstream,
					ForwardBody: []byte(`{"model":"m","stream":true,"messages":[]}`),
				}
				if _, err := (&Runtime{Service: &Service{}}).runCoordinatedStreamAttempt(ctx, req, attempt, 2*time.Second); err != nil {
					t.Fatal(err)
				}
				defer attempt.Gate.Lose()
				defer attempt.Cancel()
				if err := attempt.Gate.Win(); err != nil {
					t.Fatal(err)
				}
				if !strings.Contains(recorder.Body.String(), "visible answer") {
					t.Fatalf("first content waited for final usage: %s", recorder.Body.String())
				}
				close(allowTail)
				result := attempt.awaitStreamResult()
				if result.Err != nil || result.StreamErr != nil || attempt.Gate.LateMatch().IsRejected() {
					t.Fatalf("late real usage was rejected: result=%+v late=%+v", result, attempt.Gate.LateMatch())
				}
			})
		}
	}
}

func TestStrictZeroUsageGatewayAuditFailoverAndSettlement(t *testing.T) {
	for _, tc := range []struct {
		name, usage string
		output      bool
		lateText    bool
	}{
		{name: "production-completed-with-content-missing-usage", output: true},
		{name: "completed-with-content-zero-usage", output: true, usage: `{"input_tokens":0,"output_tokens":0}`},
		{name: "empty-completed-missing-usage"},
		{name: "late-text-audit-must-not-hide-usage-failure", output: true, lateText: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := openGatewayTestDB(t)
			cipher, err := crypto.NewCipher("strict-usage-regression")
			if err != nil {
				t.Fatal(err)
			}
			secret, err := cipher.Encrypt("test-key")
			if err != nil {
				t.Fatal(err)
			}
			groups, keys, routes := storage.NewGatewayGroups(db), storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db)
			group := &storage.GatewayGroup{
				Name: "strict-usage", Status: storage.GatewayGroupStatusActive, RateSortDirection: "asc",
				RetryEnabled: true, FailoverEnabled: true, FailoverMax: 1, RequestMaxAttempts: 2,
				RequestFirstTokenTimeoutSec: 5, ResponseValidationEnabled: true,
				ResponseValidationStreamMode: "prefix", ResponseValidationPrefixBytes: 1,
			}
			if err := groups.Create(group); err != nil {
				t.Fatal(err)
			}
			if err := db.Model(group).Update("response_validation_retry_count", 0).Error; err != nil {
				t.Fatal(err)
			}
			if err := keys.Create(&storage.GatewayKey{GroupID: group.ID, Name: "client", KeyHash: HashAPIKey("client-key"), KeyCipher: secret, Status: storage.GatewayKeyStatusActive}); err != nil {
				t.Fatal(err)
			}
			rules := storage.NewGatewayResponseRules(db)
			rule := &storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "strict usage", Enabled: true, Target: storage.GatewayResponseRuleTargetZeroUsage, Pattern: storage.GatewayResponseRuleZeroUsagePattern}
			if err := rules.Create(rule); err != nil {
				t.Fatal(err)
			}
			if tc.lateText {
				if err := rules.Create(&storage.GatewayResponseRule{GatewayGroupID: group.ID, Name: "late text audit", Enabled: true, Target: "assistant_text", Pattern: "late-audit-marker"}); err != nil {
					t.Fatal(err)
				}
			}
			providers := storage.NewGatewayProviders(db)
			var calls [2]atomic.Int32
			var inputs []storage.GatewayRoute
			for index := 0; index < 2; index++ {
				index := index
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls[index].Add(1)
					w.Header().Set("Content-Type", "text/event-stream")
					if index == 1 {
						_, _ = io.WriteString(w, zeroUsageTestBody(protocol.KindOpenAIResponses, true, "fallback answer", 2, true, true))
						return
					}
					prefix, tail := strictUsageStreamParts(protocol.KindOpenAIResponses, tc.usage)
					if !tc.output {
						prefix = ""
					}
					if tc.lateText {
						prefix += "data: {\"type\":\"response.output_text.delta\",\"delta\":\"late-audit-marker\"}\n\n"
					}
					_, _ = io.WriteString(w, prefix+tail)
				}))
				t.Cleanup(upstream.Close)
				provider := &storage.GatewayProvider{Name: fmt.Sprintf("source-%d", index), BaseURL: upstream.URL, APIKeyCipher: secret, Enabled: true, ModelPolicy: storage.GatewayProviderModelPolicyAll}
				if err := providers.Create(provider); err != nil {
					t.Fatal(err)
				}
				rate := float64(index+1) / 10
				inputs = append(inputs, storage.GatewayRoute{
					GatewayGroupID: group.ID, Position: index, SourceKind: storage.GatewayRouteSourceProvider,
					GatewayProviderID: provider.ID, Enabled: true, UpstreamProtocol: storage.GatewayUpstreamProtocolOpenAIResponses,
					RateConvertMode: "custom", RateConvertValue: rate, BillingRateMultiplier: rate,
				})
			}
			if err := routes.SaveForGroup(group.ID, inputs); err != nil {
				t.Fatal(err)
			}
			svc := NewService(groups, keys, routes, storage.NewGatewayUsageLogs(db), storage.NewModelPriceOverrides(db), storage.NewChannels(db), nil, cipher, nil)
			svc.SetProviders(providers)
			svc.SetResponseRules(rules)
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-4o","stream":true,"input":"hi"}`))
			c.Request.Header.Set("Authorization", "Bearer client-key")
			svc.runtime().HandleForward(c, "/v1/responses", protocol.KindOpenAIResponses)
			wantLogs, wantFallback := 2, int32(1)
			if tc.output {
				wantLogs, wantFallback = 1, 0
			}
			if calls[0].Load() != 1 || calls[1].Load() != wantFallback {
				t.Fatalf("unexpected failover: calls=%d/%d body=%s", calls[0].Load(), calls[1].Load(), recorder.Body.String())
			}
			var logs []storage.GatewayUsageLog
			if err := db.Order("attempt").Find(&logs).Error; err != nil {
				t.Fatal(err)
			}
			if len(logs) != wantLogs {
				t.Fatalf("logs=%+v", logs)
			}
			failed := logs[0]
			if failed.Success || failed.Winner || failed.BilledCost != 0 || failed.AttemptStatus == storage.GatewayAttemptStatusAccepted {
				t.Fatalf("missing/zero usage accepted or settled: %+v", failed)
			}
			if failed.ValidationRuleName != rule.Name || failed.ValidationPostCommit != tc.output {
				t.Fatalf("usage rejection audit missing: %+v", failed)
			}
			if failed.ErrorType != "validation" {
				t.Fatalf("usage rejection misclassified as transport failure: %+v", failed)
			}
			if tc.output {
				assertStrictUsageFailureTerminal(t, protocol.KindOpenAIResponses, recorder.Body.String())
				var settlements int64
				if err := db.Model(&storage.GatewayWinnerSettlement{}).Count(&settlements).Error; err != nil || settlements != 0 {
					t.Fatalf("failed stream was settled: count=%d err=%v", settlements, err)
				}
			} else if !logs[1].Success || !logs[1].Winner || !strings.Contains(recorder.Body.String(), "fallback answer") || strings.Contains(recorder.Body.String(), "response.failed") {
				t.Fatalf("pre-output rejection did not deliver fallback: logs=%+v body=%s", logs, recorder.Body.String())
			}
		})
	}
}

func TestStrictZeroUsageRejectsBufferedContentBeforeCommit(t *testing.T) {
	for _, kind := range []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses} {
		t.Run(string(kind), func(t *testing.T) {
			// Another rule may still be holding a short prefix when final usage
			// arrives. Only actual downstream commit makes the rejection late.
			validator := mustResponseValidator(t, 8192, time.Hour,
				responseRuleSpec{ID: 17, Enabled: true, Target: "zero_usage"},
				responseRuleSpec{ID: 18, Enabled: true, Target: "assistant_text", Pattern: "never-match"},
			)
			prefix, tail := strictUsageStreamParts(kind, "")
			result, late, body := runStrictUsageStream(t, kind, kind, prefix+tail, validator)
			if result.Err == nil || result.Committed || !result.ValidationRejection.IsRejected() || result.ValidationRejection.PostCommit || late.IsRejected() || body != "" {
				t.Fatalf("buffered content was committed or marked late: result=%+v late=%+v body=%s", result, late, body)
			}
		})
	}
}

func TestStrictZeroUsageBufferedFallbackRejectsMissingUsage(t *testing.T) {
	for _, kind := range []protocol.Kind{protocol.KindOpenAIChat, protocol.KindAnthropic, protocol.KindOpenAIResponses} {
		t.Run(string(kind), func(t *testing.T) {
			recorder := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(recorder)
			validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{ID: 17, Enabled: true, Target: "zero_usage"})
			gate := newStreamPrefixGateWriter(c.Writer, validator.NewStreamValidator("openai_responses", "m"))
			c.Writer = gate
			defer gate.Lose()
			prefix, tail := strictUsageStreamParts(kind, "")
			resp := &http.Response{Body: io.NopCloser(strings.NewReader(prefix + tail))}
			defer resp.Body.Close()
			result := (&Runtime{Service: &Service{}}).forwardStreamBuffered(c, resp, time.Now(), time.Second,
				protocol.KindOpenAIResponses, kind, "m", kind != protocol.KindOpenAIResponses, nil, http.StatusOK, 100)
			if result.Err == nil || result.Committed || result.ValidationRejection.RuleID != 17 || recorder.Body.Len() != 0 {
				t.Fatalf("buffered fallback bypassed validation: result=%+v body=%s", result, recorder.Body.String())
			}
		})
	}
}

func TestStrictZeroUsageNonStreamMissingUsageAndHTTPErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		status     int
		reject     bool
	}{
		{"missing", `{"id":"r1","model":"m","status":"completed","output":[]}`, http.StatusOK, true},
		{"empty", "", http.StatusOK, true},
		{"invalid", `{"usage":{"input_tokens":null,"output_tokens":-1}}`, http.StatusOK, true},
		{"positive", `{"usage":{"input_tokens":7,"output_tokens":2}}`, http.StatusOK, false},
		{"http-error", `{"error":{"message":"upstream busy"}}`, http.StatusServiceUnavailable, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
			validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{ID: 17, Enabled: true, Target: "zero_usage"})
			req := &coordinatedForwardRequest{c: c, kind: protocol.KindOpenAIResponses, group: &storage.GatewayGroup{}, requestedModel: "m", validator: validator}
			attempt := &coordinatedForwardAttempt{
				StartedAt: time.Now(), Target: &upstreamTarget{BaseURL: server.URL, APIKey: "k"},
				UpstreamKind: protocol.KindOpenAIResponses, UpstreamModel: "m", UpstreamPath: "/v1/responses",
				ForwardBody: []byte(`{"model":"m","input":"hi"}`),
			}
			_, err := (&Runtime{Service: &Service{}}).runCoordinatedNonStreamAttempt(context.Background(), req, attempt, time.Second)
			if attempt.Validation.IsRejected() != tc.reject || (err != nil) != (tc.status >= 400) {
				t.Fatalf("validation=%+v err=%v", attempt.Validation, err)
			}
			if tc.status >= 400 && attempt.ErrInfo.Type != "http" {
				t.Fatalf("HTTP failure was replaced by a usage rejection: %+v", attempt.ErrInfo)
			}
		})
	}
}

func TestStrictZeroUsageWhileDrainingClientDisconnectIsFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/", nil).WithContext(ctx)
	c.Writer = &cancelAfterStreamWrite{ResponseWriter: c.Writer, cancel: cancel}
	validator := mustResponseValidator(t, 8192, time.Hour, responseRuleSpec{ID: 17, Enabled: true, Target: "zero_usage"})
	gate := newStreamPrefixGateWriter(c.Writer, validator.NewStreamValidator("openai_responses", "m"))
	c.Writer = gate
	defer gate.Lose()
	prefix, tail := strictUsageStreamParts(protocol.KindOpenAIResponses, "")
	resp := &http.Response{Body: io.NopCloser(strings.NewReader(prefix + tail))}
	defer resp.Body.Close()
	rt := &Runtime{Service: &Service{}}
	finished := make(chan streamAttemptResult, 1)
	go func() {
		finished <- rt.forwardStreamIncremental(context.Background(), ctx, nil, c, resp, time.Now(), time.Second,
			protocol.KindOpenAIResponses, protocol.KindOpenAIResponses, "m", false, nil, http.StatusOK)
	}()
	select {
	case <-gate.Ready():
		if err := gate.Win(); err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not become ready")
	}
	select {
	case result := <-finished:
		if !result.Committed || !result.ClientDisconnected || result.Err == nil || result.StreamErr == nil || result.PostCommitValidation.RuleID != 17 || result.DownstreamComplete {
			t.Fatalf("missing usage was treated as a successful client disconnect: %+v", result)
		}
		if rt.isClientDisconnectAfterCommit(result.ClientDisconnected, result.StreamErr) {
			t.Fatal("usage rejection was classified as a successful client disconnect")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not finish")
	}
}
