package gateway

import (
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

// filterRoutesForRequestedModel removes direct providers that cannot serve the
// final upstream model after group and route mappings. The filtered slice is
// shared by ordinary failover and coordinated hedge planning.
func (rt *Runtime) filterRoutesForRequestedModel(
	routes []storage.GatewayRoute,
	requestedModel string,
	groupMapping map[string]string,
) ([]storage.GatewayRoute, error) {
	requestedModel = strings.TrimSpace(requestedModel)
	if requestedModel == "" || len(routes) == 0 {
		return routes, nil
	}
	filtered := make([]storage.GatewayRoute, 0, len(routes))
	providerCache := make(map[uint]*storage.GatewayProvider)
	for _, route := range routes {
		if route.NormalizeSourceKind() != storage.GatewayRouteSourceProvider {
			filtered = append(filtered, route)
			continue
		}
		if rt.Providers == nil || route.GatewayProviderID == 0 {
			continue
		}
		provider, ok := providerCache[route.GatewayProviderID]
		if !ok {
			loaded, err := rt.Providers.FindByID(route.GatewayProviderID)
			if err != nil {
				// The normal resolver will report missing/disabled providers when
				// no other route is available; do not turn it into a model error.
				providerCache[route.GatewayProviderID] = nil
				continue
			}
			provider = loaded
			providerCache[route.GatewayProviderID] = provider
		}
		if provider == nil {
			continue
		}
		upstreamModel, _ := ResolveModel(
			requestedModel,
			ParseModelMapping(route.ModelMappingJSON),
			groupMapping,
		)
		if upstreamModel == "" {
			upstreamModel = requestedModel
		}
		allowed, err := ProviderAllowsUpstreamModel(provider, upstreamModel)
		if err != nil {
			return nil, fmt.Errorf("provider %q model policy: %w", provider.Name, err)
		}
		if allowed {
			filtered = append(filtered, route)
		}
	}
	return filtered, nil
}

