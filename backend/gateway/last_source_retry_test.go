package gateway

import (
	"net/http"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

const overloadedStreamBody = `{"type":"response.failed","response":{"status":"failed","error":{"code":"server_is_overloaded","message":"server_is_overloaded"}}}`

func lastSourceAttempt(routeID uint, number int, validation validationResult, status int) *coordinatedForwardAttempt {
	return &coordinatedForwardAttempt{
		Info:         hedgeAttemptInfo{Number: number},
		Route:        storage.GatewayRoute{ID: routeID},
		Status:       status,
		UpstreamBody: []byte(overloadedStreamBody),
		Validation:   validation,
	}
}

// 只有一条可用路由时，HTTP 200 流里的瞬时过载必须保留重试阶梯，
// 不能把配置好的重试次数直接丢掉。
func TestLastSourceKeepsRetriesForTransientStreamFailure(t *testing.T) {
	rejection := validationResult{
		Decision: validationRejected, RuleID: 84, RuleName: "(server_is_overloaded",
		Target: "error_message", Pattern: "(?i)(server_is_overloaded", MatchedOn: "server_is_overloaded",
	}
	attempt := lastSourceAttempt(517, 1, rejection, http.StatusOK)
	plan := buildCoordinatedRoutePlan([]ScoredRoute{{Route: storage.GatewayRoute{ID: 517}}},
		&storage.GatewayGroup{RetryEnabled: true, RetryCount: 10, ResponseValidationRetryCount: 10, FailoverMax: 8},
		false, true)
	if len(plan) < 2 {
		t.Fatalf("plan must contain same-route retry slots, got %d", len(plan))
	}
	if coordinatedShouldSuppressSameRouteRetry(attempt, plan) {
		t.Fatal("single-source overload must keep its retry ladder")
	}
}

// 还有别的路由没试过时，仍然优先换源，不在原路线上重放。
func TestAlternativeSourceStillSuppressesSameRouteRetry(t *testing.T) {
	rejection := validationResult{
		Decision: validationRejected, RuleID: 84, RuleName: "(server_is_overloaded",
		Target: "error_message", Pattern: "(?i)(server_is_overloaded", MatchedOn: "server_is_overloaded",
	}
	attempt := lastSourceAttempt(477, 1, rejection, http.StatusOK)
	plan := buildCoordinatedRoutePlan([]ScoredRoute{
		{Route: storage.GatewayRoute{ID: 477}},
		{Route: storage.GatewayRoute{ID: 517}},
	}, &storage.GatewayGroup{RetryEnabled: true, RetryCount: 10, ResponseValidationRetryCount: 10, FailoverMax: 8}, false, true)
	if !coordinatedShouldSuppressSameRouteRetry(attempt, plan) {
		t.Fatal("an untried alternative must win over same-route replay")
	}
}

// 内容类拒绝（模型输出被规则拦下）换源才有意义，最后一路也不重放。
func TestLastSourceDoesNotReplayContentRejection(t *testing.T) {
	rejection := validationResult{
		Decision: validationRejected, RuleID: 12, RuleName: "copyright refusal",
		Target: "assistant_text", Pattern: "(?i)cannot help with that", MatchedOn: "cannot help with that",
	}
	attempt := lastSourceAttempt(517, 1, rejection, http.StatusOK)
	plan := buildCoordinatedRoutePlan([]ScoredRoute{{Route: storage.GatewayRoute{ID: 517}}},
		&storage.GatewayGroup{RetryEnabled: true, RetryCount: 10, ResponseValidationRetryCount: 10, FailoverMax: 8},
		false, true)
	if !coordinatedShouldSuppressSameRouteRetry(attempt, plan) {
		t.Fatal("content rejection must not replay on the last source")
	}
}

// 确定性的策略拒绝（鉴权/分组/模型不支持）重放没有意义，即使只剩一条路由也不重试。
func TestLastSourceStillSuppressesDeterministicRejection(t *testing.T) {
	for _, matched := range []string{"invalid_api_key", "insufficient_quota", "model_not_found"} {
		rejection := validationResult{
			Decision: validationRejected, RuleID: 9, RuleName: "auth",
			Target: "error_message", Pattern: ".*", MatchedOn: matched,
		}
		attempt := lastSourceAttempt(517, 1, rejection, http.StatusOK)
		plan := buildCoordinatedRoutePlan([]ScoredRoute{{Route: storage.GatewayRoute{ID: 517}}},
			&storage.GatewayGroup{RetryEnabled: true, RetryCount: 10, ResponseValidationRetryCount: 10, FailoverMax: 8},
			false, true)
		if !coordinatedShouldSuppressSameRouteRetry(attempt, plan) {
			t.Fatalf("deterministic rejection %q must not be replayed", matched)
		}
	}
}
