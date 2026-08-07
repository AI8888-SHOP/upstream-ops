package gateway

import (
	"bytes"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
)

func TestUpstreamRequestPrepareCacheReusesPreparedBody(t *testing.T) {
	body := []byte(`{"model":"requested","messages":[{"role":"user","content":"hello"}]}`)
	var cache upstreamRequestPrepareCache
	svc := &Service{}

	first, firstPath, firstConverted, firstErr := cache.prepare(
		svc, body, protocol.KindOpenAIChat, protocol.KindOpenAIChat,
		"requested", "upstream", false, "/v1/chat/completions",
	)
	second, secondPath, secondConverted, secondErr := cache.prepare(
		svc, body, protocol.KindOpenAIChat, protocol.KindOpenAIChat,
		"requested", "upstream", false, "/v1/chat/completions",
	)
	if firstErr != nil || secondErr != nil {
		t.Fatalf("prepare errors: first=%v second=%v", firstErr, secondErr)
	}
	if firstPath != secondPath || firstConverted != secondConverted || !bytes.Equal(first, second) {
		t.Fatalf("cached result changed: first=%s second=%s", first, second)
	}
	if len(first) == 0 || &first[0] != &second[0] {
		t.Fatal("identical preparation did not reuse the cached body")
	}

	third, _, _, err := cache.prepare(
		svc, body, protocol.KindOpenAIChat, protocol.KindOpenAIChat,
		"requested", "another-upstream", false, "/v1/chat/completions",
	)
	if err != nil {
		t.Fatalf("prepare distinct model: %v", err)
	}
	if bytes.Equal(first, third) {
		t.Fatalf("distinct upstream model reused stale body: %s", third)
	}
}
