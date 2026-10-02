package gateway

import (
	"fmt"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

// filterRoutesForRequestedModel applies per-route model policies and removes
// unavailable providers after resolving group and route model mappings. The
// filtered slice is shared by ordinary failover and coordinated hedge
// planning. Provider enabled state is checked here, before route sorting, so a
// disabled direct provider cannot consume a scheduler attempt.
func (rt *Runtime) filterRoutesForRequestedModel(
	routes []storage.GatewayRoute,
	requestedModel string,
	groupMapping map[string]string,
) ([]storage.GatewayRoute, error) {
	requestedModel = strings.TrimSpace(requestedModel)
	if len(routes) == 0 {
		return routes, nil
	}
	filtered := make([]storage.GatewayRoute, 0, len(routes))
	providerCache := make(map[uint]*storage.GatewayProvider)
	for _, route := range routes {
		upstreamModel := requestedModel
		// Legacy/all-model monitor routes need no mapping parse on this hot path.
		if requestedModel != "" && (route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider ||
			(route.ModelPolicy != "" && route.ModelPolicy != storage.GatewayProviderModelPolicyAll)) {
			if resolved, _ := ResolveModel(requestedModel, ParseModelMapping(route.ModelMappingJSON), groupMapping); resolved != "" {
				upstreamModel = resolved
			}
		}
		// Model discovery and model-less resource requests have no model to
		// evaluate. Generation requests are filtered before sorting, recovery,
		// retries or hedging, so none of those paths can bypass an allowlist.
		if requestedModel != "" {
			allowed, err := RouteAllowsUpstreamModel(&route, upstreamModel)
			if err != nil {
				return nil, fmt.Errorf("route %d model policy: %w", route.ID, err)
			}
			if !allowed {
				continue
			}
		}
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
		if !provider.Enabled {
			continue
		}
		route.SchedulerCredential = fmt.Sprintf("%x", schedulerCredential(provider.APIKeyCipher, provider.BaseURL))
		route.SchedulerConcurrencyLimit = provider.ConcurrencyLimit
		// Model-less requests still need the provider enabled check, but have no
		// model policy to evaluate.
		if requestedModel == "" {
			filtered = append(filtered, route)
			continue
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

// bindModelCooldownAliases exposes a route's resolved upstream-model cooldown
// under the requested model as well. Cooldown records use the final upstream
// model so aliases that map to the same upstream model share one failure state;
// the alias map is request-local and is never persisted.
func bindModelCooldownAliases(routes []storage.GatewayRoute, requestedModel string, groupMapping map[string]string) []storage.GatewayRoute {
	requestedKey := storage.NormalizeGatewayModel(requestedModel)
	if requestedKey == "" || len(routes) == 0 {
		return routes
	}
	for i := range routes {
		routeMapping := ParseModelMapping(routes[i].ModelMappingJSON)
		upstreamModel, _ := ResolveModel(requestedModel, routeMapping, groupMapping)
		upstreamKey := storage.NormalizeGatewayModel(upstreamModel)
		if upstreamKey == "" || upstreamKey == requestedKey || len(routes[i].ModelCooldowns) == 0 {
			continue
		}
		cooldown, ok := routes[i].ModelCooldowns[upstreamKey]
		if !ok {
			continue
		}
		aliases := make(map[string]storage.GatewayRouteModelCooldown, len(routes[i].ModelCooldowns)+1)
		for key, value := range routes[i].ModelCooldowns {
			aliases[key] = value
		}
		aliases[requestedKey] = cooldown
		routes[i].ModelCooldowns = aliases
	}
	return routes
}
