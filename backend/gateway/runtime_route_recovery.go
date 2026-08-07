package gateway

import "github.com/bejix/upstream-ops/backend/storage"

func routeModelCooldownSnapshot(route *storage.GatewayRoute, model string) (storage.GatewayRouteModelCooldown, bool) {
	if route == nil || route.ID == 0 {
		return storage.GatewayRouteModelCooldown{}, false
	}
	model = storage.NormalizeGatewayModel(model)
	if model == "" {
		return storage.GatewayRouteModelCooldown{}, false
	}
	cooldown, ok := route.ModelCooldowns[model]
	if ok {
		return cooldown, true
	}
	// Request-local alias maps retain the persisted upstream model on the
	// cooldown value. Fall back to it when the direct alias is absent.
	for _, candidate := range route.ModelCooldowns {
		if storage.NormalizeGatewayModel(candidate.Model) == model {
			return candidate, true
		}
	}
	return storage.GatewayRouteModelCooldown{}, false
}

func routeNeedsModelSuccessUpdate(route *storage.GatewayRoute, model string) bool {
	cooldown, ok := routeModelCooldownSnapshot(route, model)
	if !ok {
		return false
	}
	return cooldown.TempUnschedulableUntil != nil || cooldown.TempUnschedulableAt != nil ||
		cooldown.TempUnschedulableReason != "" || cooldown.TempUnschedulableRequestID != "" ||
		cooldown.RecoverSuccessStreak > 0
}

func (rt *Runtime) noteRouteModelSuccess(route *storage.GatewayRoute, model string) {
	if rt == nil || rt.Routes == nil || route == nil || route.ID == 0 {
		return
	}
	// Healthy traffic has no cooldown snapshot and needs no database write.
	// The retained failure generation also prevents this success from clearing
	// a newer cooldown written after the route snapshot was loaded.
	cooldown, ok := routeModelCooldownSnapshot(route, model)
	if !ok || !routeNeedsModelSuccessUpdate(route, model) {
		return
	}
	if err := rt.Routes.NoteSuccessForModelPauseError(
		route.ID, model, cooldown.TempUnschedulableAt, cooldown.TempUnschedulableRequestID,
	); err != nil && rt.Log != nil {
		rt.Log.Warn("failed to record gateway route model recovery", "route_id", route.ID, "model", model, "err", err)
	}
}
