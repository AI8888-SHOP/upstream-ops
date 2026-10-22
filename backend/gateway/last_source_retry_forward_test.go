package gateway

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

// 单路由 + HTTP 200 流内 overloaded：必须按配置重试同一条路由，
// 而不是把规则名直接甩给下游（线上 a80e8cd2 那次就是这个形态）。
func TestSingleSourceOverloadRetriesInsteadOfFailingImmediately(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprintf("stream=%v", stream), func(t *testing.T) {
			f := newCoordinatedRetryRegressionFixture(t, stream, 1, storage.GatewayGroup{
				RetryEnabled: true, RetryCount: 2, ResponseValidationRetryCount: 2,
				FailoverEnabled: true, FailoverMax: 1, CooldownSeconds: 60,
			}, func(_ int, _ int32, w http.ResponseWriter, _ *http.Request) {
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					_, _ = io.WriteString(w, "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"status\":\"failed\",\"error\":{\"code\":\"server_is_overloaded\",\"message\":\"server_is_overloaded\"}}}\n\n")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"server_is_overloaded"}}}`)
			})
			f.request.validator = mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
				ID: 84, Name: "(server_is_overloaded", Enabled: true,
				Target: "error_message", Pattern: "(?i)(server_is_overloaded|overloaded_error)",
			})
			f.runtime.handleForwardCoordinated(f.request)

			if calls := f.calls[0].Load(); calls != 3 {
				t.Fatalf("upstream calls=%d, want 3 (initial + 2 configured retries); status=%d body=%s",
					calls, f.recorder.Code, f.recorder.Body.String())
			}
			if f.recorder.Code != http.StatusBadGateway {
				t.Fatalf("status=%d, want %d", f.recorder.Code, http.StatusBadGateway)
			}
			body := f.recorder.Body.String()
			if !strings.Contains(body, "upstream response rejected") {
				t.Fatalf("gateway did not return the generic rejection text: %s", body)
			}
			if strings.Contains(body, "server_is_overloaded") || strings.Contains(body, "rejection rule") {
				t.Fatalf("internal rule identity leaked downstream: %s", body)
			}
		})
	}
}

// 换源优先级不能被放宽逻辑破坏：还有可用路由时依然先换源。
func TestAlternativeRouteStillPreferredOverSameRouteRetry(t *testing.T) {
	f := newCoordinatedRetryRegressionFixture(t, false, 2, storage.GatewayGroup{
		RetryEnabled: true, RetryCount: 2, ResponseValidationRetryCount: 2,
		FailoverEnabled: true, FailoverMax: 1, CooldownSeconds: 60,
	}, func(source int, call int32, w http.ResponseWriter, _ *http.Request) {
		if source == 1 {
			writeCoordinatedRetryRegressionSuccess(w, false)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"server_is_overloaded"}}}`)
	})
	f.request.validator = mustResponseValidator(t, 8192, time.Second, responseRuleSpec{
		ID: 84, Name: "(server_is_overloaded", Enabled: true,
		Target: "error_message", Pattern: "(?i)(server_is_overloaded|overloaded_error)",
	})
	f.runtime.handleForwardCoordinated(f.request)

	if got := []int32{f.calls[0].Load(), f.calls[1].Load()}; got[0] != 1 || got[1] != 1 {
		t.Fatalf("source calls=%v, want one attempt per source", got)
	}
	if f.recorder.Code != http.StatusOK || !strings.Contains(f.recorder.Body.String(), "recovered retry answer") {
		t.Fatalf("failover did not deliver the healthy source: status=%d body=%s", f.recorder.Code, f.recorder.Body.String())
	}
}
