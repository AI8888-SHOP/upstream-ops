package gateway

import (
	"errors"

	"github.com/bejix/upstream-ops/backend/storage"
)

// RouteAllowsUpstreamModel uses the same exact, case-sensitive upstream model
// IDs as direct providers. An empty legacy policy means all; an explicitly
// selected but empty allowlist denies every model. Provider policies, when
// present, are an additional constraint and cannot be widened by a route.
func RouteAllowsUpstreamModel(route *storage.GatewayRoute, upstreamModel string) (bool, error) {
	if route == nil {
		return false, errors.New("route is nil")
	}
	return ProviderAllowsUpstreamModel(&storage.GatewayProvider{
		ModelPolicy:       route.ModelPolicy,
		AllowedModelsJSON: route.AllowedModelsJSON,
	}, upstreamModel)
}

// FilterRouteModels filters upstream IDs before model aliases are exposed.
func FilterRouteModels(route *storage.GatewayRoute, models []string) ([]string, error) {
	if route == nil {
		return nil, errors.New("route is nil")
	}
	return FilterProviderModels(&storage.GatewayProvider{
		ModelPolicy:       route.ModelPolicy,
		AllowedModelsJSON: route.AllowedModelsJSON,
	}, models)
}
