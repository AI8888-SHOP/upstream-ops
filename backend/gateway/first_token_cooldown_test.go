package gateway

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func newFirstTokenCooldownFixture(t *testing.T, exempt bool) (*Runtime, *storage.GatewayRoutes, storage.GatewayGroup, storage.GatewayRoute) {
	t.Helper()
	db := openGatewayTestDB(t)
	routes := storage.NewGatewayRoutes(db)
	group := &storage.GatewayGroup{Name: "first-token-whitelist", RetryEnabled: true, CooldownSeconds: 60, FirstTokenTimeoutCooldownEnabled: true}
	if err := db.Create(group).Error; err != nil {
		t.Fatal(err)
	}
	route := storage.GatewayRoute{
		GatewayGroupID: group.ID, Enabled: true, SourceKind: storage.GatewayRouteSourceMonitor,
		SourceChannelID: 1, SourceAPIKeyCipher: "test-cipher", FirstTokenCooldownExempt: exempt,
	}
	if err := db.Create(&route).Error; err != nil {
		t.Fatal(err)
	}
	return (&Service{Routes: routes}).runtime(), routes, *group, route
}

// 白名单里的渠道即使首字超时也不进冷却，其它错误照常冷却。
func TestFirstTokenWhitelistSkipsTimeoutCooldown(t *testing.T) {
	rt, routes, group, route := newFirstTokenCooldownFixture(t, true)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req := &coordinatedForwardRequest{c: c, group: &group, kind: protocolOpenAI, path: "/v1/chat/completions", requestID: "whitelist"}
	attempt := &coordinatedForwardAttempt{Route: route, UpstreamModel: "gpt-6-sol", Err: errFirstTokenTimeout}
	rt.publishCoordinatedFailure(req, attempt, nil, 1)
	if attempt.healthRecorded || attempt.UsageMeta.CooldownUntil != nil {
		t.Fatalf("whitelisted route cooled on first-token timeout: %+v", attempt.UsageMeta.CooldownUntil)
	}
	stored, err := routes.FindByID(route.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !IsRouteSchedulableForModel(stored, "gpt-6-sol", time.Now()) {
		t.Fatal("whitelisted route must stay schedulable for the model")
	}

	// 同一个白名单渠道遇到普通失败仍要冷却，白名单只针对首字超时。
	hard := &coordinatedForwardAttempt{Route: route, UpstreamModel: "gpt-6-sol", Status: 503, UpstreamBody: []byte(`{"error":{"message":"upstream overloaded"}}`)}
	rt.publishCoordinatedFailure(req, hard, nil, 1)
	if !hard.healthRecorded {
		t.Fatal("non-timeout failure must still cool the route")
	}
}

// 没勾选白名单的渠道保持原行为。
func TestFirstTokenCooldownStillAppliesWithoutWhitelist(t *testing.T) {
	rt, routes, group, route := newFirstTokenCooldownFixture(t, false)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req := &coordinatedForwardRequest{c: c, group: &group, kind: protocolOpenAI, path: "/v1/chat/completions", requestID: "no-whitelist"}
	attempt := &coordinatedForwardAttempt{Route: route, UpstreamModel: "gpt-6-sol", Err: errFirstTokenTimeout}
	rt.publishCoordinatedFailure(req, attempt, nil, 1)
	if !attempt.healthRecorded || attempt.UsageMeta.CooldownUntil == nil {
		t.Fatal("route without the whitelist must still cool on first-token timeout")
	}
	stored, err := routes.FindByID(route.ID)
	if err != nil {
		t.Fatal(err)
	}
	if IsRouteSchedulableForModel(stored, "gpt-6-sol", time.Now()) {
		t.Fatal("cooled route must not be schedulable for the model")
	}
}

// 组级总开关关闭时，白名单也必须服从（不再冷却由组开关决定）。
func TestFirstTokenWhitelistRespectsGroupSwitch(t *testing.T) {
	rt, _, group, route := newFirstTokenCooldownFixture(t, true)
	group.FirstTokenTimeoutCooldownEnabled = false
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req := &coordinatedForwardRequest{c: c, group: &group, kind: protocolOpenAI, path: "/v1/chat/completions", requestID: "group-switch"}
	attempt := &coordinatedForwardAttempt{Route: route, UpstreamModel: "gpt-6-sol", Err: errFirstTokenTimeout}
	rt.publishCoordinatedFailure(req, attempt, nil, 1)
	if attempt.healthRecorded {
		t.Fatal("group switch off must keep first-token timeouts out of cooldown")
	}
}
