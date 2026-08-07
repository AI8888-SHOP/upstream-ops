package gateway

import (
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestRouteNeedsModelSuccessUpdate(t *testing.T) {
	route := &storage.GatewayRoute{ID: 7}
	if routeNeedsModelSuccessUpdate(route, "model-a") {
		t.Fatal("healthy route should not issue a recovery UPDATE")
	}
	now := time.Now()
	route.ModelCooldowns = map[string]storage.GatewayRouteModelCooldown{
		"model-a": {RouteID: route.ID, Model: "model-a", TempUnschedulableAt: &now},
	}
	if !routeNeedsModelSuccessUpdate(route, " model-a ") {
		t.Fatal("model cooldown snapshot should issue a recovery UPDATE")
	}
	if routeNeedsModelSuccessUpdate(route, "model-b") {
		t.Fatal("cooldown for another model must not cause a recovery UPDATE")
	}
}
