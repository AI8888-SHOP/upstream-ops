package gateway

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestCandyCheckHTTPErrorDetail(t *testing.T) {
	svc := &Service{}
	const prefix = "检测上游返回 HTTP 413"
	const message = "同步请求内容超过允许的最大字符数，请缩短输入后重试"
	body := []byte(`{"error":{"message":"` + message + `","type":"content_policy_violation"}}`)
	if got := svc.candyCheckHTTPError(413, body, nil, nil); got != prefix+": "+message {
		t.Fatalf("lost upstream explanation: %s", got)
	}
	for _, raw := range []string{"<html>private error page</html>", "invalid JSON", `{"request":{"secret":"private"}}`, strings.Repeat("x", candyCheckBodyLimit+1)} {
		if got := svc.candyCheckHTTPError(413, []byte(raw), nil, nil); got != prefix {
			t.Fatalf("raw error body leaked: %.100s", got)
		}
	}
	target := &upstreamTarget{APIKey: "sk-probe-secret", Provider: &storage.GatewayProvider{ExtraHeadersJSON: `{"X-Custom-Token":"custom-secret"}`}}
	headers := http.Header{"Authorization": {"Bearer bearer-secret"}, "Cookie": {"session=cookie-secret; other=second-secret"}, "X-Api-Key": {"header-secret"}}
	messageWithSecrets := message + " sk-probe-secret Bearer bearer-secret cookie-secret second-secret custom-secret header-secret\n\t" + strings.Repeat("界", 500)
	body, err := json.Marshal(map[string]any{"error": map[string]string{"message": messageWithSecrets}})
	if err != nil {
		t.Fatal(err)
	}
	got := svc.candyCheckHTTPError(413, body, target, headers)
	for _, secret := range []string{"sk-probe-secret", "bearer-secret", "cookie-secret", "second-secret", "custom-secret", "header-secret", "\n", "\t"} {
		if strings.Contains(got, secret) {
			t.Fatalf("credential or control character leaked: %s", got)
		}
	}
	if !strings.Contains(got, "[redacted]") || !utf8.ValidString(got) || len([]rune(got)) > len([]rune(prefix))+2+241 {
		t.Fatalf("unsafe/unbounded reason: %s", got)
	}
}
