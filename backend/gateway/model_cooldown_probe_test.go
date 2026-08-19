package gateway

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

func TestModelProbeInboundRequestFollowsProtocol(t *testing.T) {
	tests := []struct {
		name     string
		kind     protocol.Kind
		path     string
		contains string
	}{
		{name: "chat", kind: protocol.KindOpenAIChat, path: "/v1/chat/completions", contains: "messages"},
		{name: "responses", kind: protocol.KindOpenAIResponses, path: "/v1/responses", contains: "input"},
		{name: "anthropic", kind: protocol.KindAnthropic, path: "/v1/messages", contains: "messages"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, path := modelProbeInboundRequest(tt.kind, "probe-model")
			if path != tt.path {
				t.Fatalf("path=%q, want %q", path, tt.path)
			}
			var payload map[string]any
			if err := json.Unmarshal(body, &payload); err != nil {
				t.Fatalf("invalid probe body: %v", err)
			}
			if _, ok := payload[tt.contains]; !ok {
				t.Fatalf("probe body missing %q: %s", tt.contains, body)
			}
			if payload["model"] != "probe-model" {
				t.Fatalf("model=%v", payload["model"])
			}
		})
	}
}

func TestModelProbeInboundRequestEscapesModelAsJSON(t *testing.T) {
	model := "model\"with\ncontrols"
	body, _ := modelProbeInboundRequest(protocol.KindOpenAIChat, model)
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("probe body is not valid JSON: %v", err)
	}
	if payload["model"] != model {
		t.Fatalf("model was not preserved: %#v", payload["model"])
	}
}

func TestParseProbeRetryAfterHTTPDateUsesProvidedClock(t *testing.T) {
	now := time.Date(2026, time.August, 18, 12, 0, 0, 0, time.UTC)
	want := 90 * time.Second
	value := now.Add(want).Format(http.TimeFormat)
	if got := parseProbeRetryAfter(value, now); got != want {
		t.Fatalf("retry-after=%s, want %s", got, want)
	}
}

func TestModelCooldownProbeSupportedRequest(t *testing.T) {
	tests := []struct {
		path string
		kind protocol.Kind
		want bool
	}{
		{path: "/v1/chat/completions", kind: protocol.KindOpenAIChat, want: true},
		{path: "/v1/responses", kind: protocol.KindOpenAIResponses, want: true},
		{path: "/v1/messages", kind: protocol.KindAnthropic, want: true},
		{path: "/v1/embeddings", kind: protocol.KindOpenAIChat, want: false},
		{path: "/v1/images/generations", kind: protocol.KindOpenAIChat, want: false},
		{path: "/v1/completions", kind: protocol.KindOpenAIChat, want: false},
	}
	for _, tt := range tests {
		if got := modelCooldownProbeSupportedRequest(tt.path, tt.kind); got != tt.want {
			t.Errorf("supported(%q, %q)=%v, want %v", tt.path, tt.kind, got, tt.want)
		}
	}
}

func TestNormalizeProbeInboundKindDefaultsToChat(t *testing.T) {
	if got := normalizeProbeInboundKind(""); got != protocol.KindOpenAIChat {
		t.Fatalf("empty protocol=%q, want chat", got)
	}
	if got := normalizeProbeInboundKind("responses"); got != protocol.KindOpenAIResponses {
		t.Fatalf("responses protocol=%q", got)
	}
	if got := normalizeProbeInboundKind("anthropic"); got != protocol.KindAnthropic {
		t.Fatalf("anthropic protocol=%q", got)
	}
}
