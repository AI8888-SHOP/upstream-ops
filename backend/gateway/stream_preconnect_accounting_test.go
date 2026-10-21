package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestFinalizeStreamPreconnectRejectsTruncatedSuccessBeforeSettlement(t *testing.T) {
	forEachPreconnectCompatMode(t, func(t *testing.T, multihop bool) {
		c, recorder, writer := preconnectCompatTestWriter(t, multihop)
		if !writer.keepalive() {
			t.Fatal("start compatibility stream failed")
		}
		wire := preconnectForwardResponse("partial real answer")
		terminal := strings.Index(wire, "event: response.completed\n")
		if terminal < 0 {
			t.Fatal("fixture has no terminal event")
		}
		if _, err := writer.Write([]byte(wire[:terminal])); err != nil {
			t.Fatal(err)
		}
		result := streamAttemptResult{Status: http.StatusOK, Committed: true, DownstreamComplete: true}
		result.Tokens.InputTokens, result.Tokens.OutputTokens = 7, 2
		finalizeStreamPreconnect(c, &result)
		if !errors.Is(result.Err, io.ErrUnexpectedEOF) || !errors.Is(result.StreamErr, io.ErrUnexpectedEOF) || result.ClientDisconnected || result.DownstreamComplete || !result.Committed {
			t.Fatalf("truncated real output still looks successfully delivered: %+v", result)
		}
		if result.Tokens.InputTokens != 7 || result.Tokens.OutputTokens != 2 {
			t.Fatalf("upstream attempted usage lost during failure finalization: %+v", result.Tokens)
		}
		body := recorder.Body.String()
		if !strings.Contains(body, "partial real answer") || strings.Count(body, "event: response.failed\n") != 1 || strings.Contains(body, "event: response.completed\n") {
			t.Fatalf("truncated stream did not end with one honest failure: %s", body)
		}
		before := recorder.Body.Len()
		finalizeStreamPreconnect(c, &result)
		if recorder.Body.Len() != before {
			t.Fatal("repeated accounting finalization emitted another terminal")
		}
	})
}

func TestFinalizeStreamPreconnectProtocolErrorIsNotClientDisconnect(t *testing.T) {
	forEachPreconnectCompatMode(t, func(t *testing.T, multihop bool) {
		c, recorder, writer := preconnectCompatTestWriter(t, multihop)
		if !writer.keepalive() {
			t.Fatal("start compatibility stream failed")
		}
		wire := preconnectForwardResponse("real answer before broken terminal")
		terminal := strings.Index(wire, "event: response.completed\n")
		if _, err := writer.Write([]byte(wire[:terminal])); err != nil {
			t.Fatal(err)
		}
		badTerminal := strings.ReplaceAll(wire[terminal:], `"type":"message"`, `"type":"function_call"`)
		if _, err := writer.Write([]byte(badTerminal)); err == nil || writer.protocolErr == nil {
			t.Fatal("inconsistent terminal item type was not rejected")
		}
		// Lower layers can report the adapter-triggered context cancellation as a
		// client disconnect. Finalization must retain the upstream protocol cause.
		result := streamAttemptResult{Status: http.StatusOK, Committed: true, DownstreamComplete: true, ClientDisconnected: true, Err: context.Canceled, StreamErr: context.Canceled}
		finalizeStreamPreconnect(c, &result)
		if result.Err != writer.protocolErr || result.StreamErr != writer.protocolErr || result.ClientDisconnected || result.DownstreamComplete || errors.Is(result.Err, context.Canceled) {
			t.Fatalf("protocol failure misclassified as a chargeable disconnect: %+v", result)
		}
		if !strings.Contains(recorder.Body.String(), "real answer before broken terminal") || strings.Contains(recorder.Body.String(), "event: response.completed\n") || strings.Count(recorder.Body.String(), "event: response.failed\n") != 1 {
			t.Fatalf("malformed terminal leaked successful completion: %s", recorder.Body.String())
		}
	})
}

func TestFinalizeStreamPreconnectPreservesValidDelivery(t *testing.T) {
	forEachPreconnectCompatMode(t, func(t *testing.T, multihop bool) {
		c, recorder, writer := preconnectCompatTestWriter(t, multihop)
		if !writer.keepalive() {
			t.Fatal("start compatibility stream failed")
		}
		if _, err := writer.Write([]byte(preconnectForwardResponse("real complete answer"))); err != nil {
			t.Fatal(err)
		}
		result := streamAttemptResult{Status: http.StatusOK, Committed: true, DownstreamComplete: true}
		result.Tokens.InputTokens, result.Tokens.OutputTokens = 7, 2
		before := recorder.Body.Len()
		finalizeStreamPreconnect(c, &result)
		if result.Err != nil || result.StreamErr != nil || result.ClientDisconnected || !result.DownstreamComplete || !result.Committed || result.Tokens.InputTokens != 7 || result.Tokens.OutputTokens != 2 || recorder.Body.Len() != before {
			t.Fatalf("valid delivery was changed by accounting finalization: %+v", result)
		}
	})
}
