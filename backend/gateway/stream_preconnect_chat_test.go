package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func TestChatPreconnectPreambleIsProtocolValid(t *testing.T) {
	stream, err := newPreconnectChatStream("deepseek-v4.1-flash", false)
	if err != nil {
		t.Fatal(err)
	}
	raw := strings.TrimSpace(string(stream.bytes()))
	if !strings.HasPrefix(raw, "data: ") || !strings.HasSuffix(raw, "}") {
		t.Fatalf("frame=%q", raw)
	}
	var chunk struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Model   string `json:"model"`
		Choices []struct {
			Index int `json:"index"`
			Delta struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"delta"`
			Finish *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal([]byte(strings.TrimPrefix(raw, "data: ")), &chunk); err != nil {
		t.Fatalf("invalid chunk: %v", err)
	}
	if chunk.Object != "chat.completion.chunk" || chunk.Model != "deepseek-v4.1-flash" {
		t.Fatalf("chunk=%+v", chunk)
	}
	if len(chunk.Choices) != 1 || chunk.Choices[0].Delta.Role != "assistant" ||
		chunk.Choices[0].Delta.Content != "" || chunk.Choices[0].Finish != nil {
		t.Fatalf("choices=%+v", chunk.Choices)
	}
}

func TestChatPreconnectMultihopCarriesOneSpace(t *testing.T) {
	stream, err := newPreconnectChatStream("m", true)
	if err != nil {
		t.Fatal(err)
	}
	raw := string(stream.bytes())
	if !strings.Contains(raw, `"content":" "`) {
		t.Fatalf("frame=%s", raw)
	}
}

func TestChatPreconnectAdoptsFirstRealChunkIdentity(t *testing.T) {
	stream, err := newPreconnectChatStream("m", false)
	if err != nil {
		t.Fatal(err)
	}
	upstream := []byte("data: {\"id\":\"chatcmpl-upstream\",\"object\":\"chat.completion.chunk\",\"model\":\"m\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hi\"},\"finish_reason\":null}]}\n\n")
	first := stream.rewriteFirst(upstream)
	if !strings.Contains(string(first), stream.id) || strings.Contains(string(first), "chatcmpl-upstream") {
		t.Fatalf("first=%s", first)
	}
	if got := stream.rewriteFirst(upstream); string(got) != string(upstream) {
		t.Fatalf("later chunks must pass through, got %s", got)
	}
}

func TestChatPreconnectEmitsSyntheticEventBeforeUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	group := &storage.GatewayGroup{GatewayStreamPreconnectPolicy: storage.GatewayStreamPreconnectPolicy{
		StreamPreconnectEnabled:        true,
		StreamPreconnectSub2apiEnabled: true,
		StreamPreconnectMinDelayMS:     60000,
		StreamPreconnectMaxDelayMS:     60000,
	}}
	finish := beginStreamPreconnect(c, group, protocol.KindOpenAIChat, "/v1/chat/completions", true, true,
		[]byte(`{"model":"deepseek-v4.1-flash","stream":true}`))
	defer finish()
	w := streamPreconnectFor(c)
	if w == nil || !w.keepalive() {
		t.Fatal("missing early stream")
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "data: {") || !strings.Contains(body, `"chat.completion.chunk"`) {
		t.Fatalf("body=%q", body)
	}
	if rec.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("headers=%v", rec.Header())
	}
	// 后续心跳只能是注释，不得重复合成首事件。
	w.keepalive()
	if strings.Count(rec.Body.String(), "chat.completion.chunk") != 1 {
		t.Fatalf("duplicate synthetic chunk: %q", rec.Body.String())
	}
}
