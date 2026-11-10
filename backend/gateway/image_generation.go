package gateway

import (
	"strings"

	"github.com/bejix/upstream-ops/backend/gateway/protocol"
	"github.com/bejix/upstream-ops/backend/storage"
)

// Explicit provider model IDs apply only to text-generation entry points.
// Native image generation/edit and all other passthrough endpoints retain their path.
func providerUsesImageGeneration(provider *storage.GatewayProvider, model, path string) bool {
	if provider == nil {
		return false
	}
	switch strings.TrimSpace(path) {
	case "/v1/chat/completions", "/chat/completions", "/v1/responses", "/responses", "/v1/messages", "/messages":
	default:
		return false
	}
	models, err := ParseProviderAllowedModelsJSON(provider.ImageGenerationModelsJSON)
	if err != nil {
		return false
	}
	model = strings.TrimSpace(model)
	for _, candidate := range models {
		if candidate == model {
			return true
		}
	}
	return false
}

func imageGenerationUpstreamKind(provider *storage.GatewayProvider, model, path string, fallback protocol.Kind) protocol.Kind {
	if providerUsesImageGeneration(provider, model, path) {
		return protocol.KindOpenAIImages
	}
	return fallback
}

// Validate before committing headers or recording success, including HTTP 200
// responses that contain an upstream error or omit generated images.
func imageGenerationResponseError(body []byte) []byte {
	converted := protocol.ConvertResponse(protocol.KindOpenAIChat, protocol.KindOpenAIImages, body, "")
	if hasExplicitUpstreamError(converted) {
		return converted
	}
	return nil
}

// Custom image model names need the same no-hedging treatment as known media IDs:
// a delayed image response must never trigger a second billed generation in parallel.
func (rt *Runtime) routesContainConfiguredImageModel(routes []storage.GatewayRoute, requested string, mapping ModelMap, path string) bool {
	if rt.Providers == nil {
		return false
	}
	providers := make(map[uint]*storage.GatewayProvider)
	for _, route := range routes {
		if route.NormalizeSourceKind() != storage.GatewayRouteSourceProvider {
			continue
		}
		provider, exists := providers[route.GatewayProviderID]
		if !exists {
			loaded, err := rt.Providers.FindByID(route.GatewayProviderID)
			if err != nil {
				continue
			}
			provider = loaded
			providers[route.GatewayProviderID] = provider
		}
		model, _ := ResolveModel(requested, ParseModelMapping(route.ModelMappingJSON), mapping)
		if model == "" {
			model = requested
		}
		if providerUsesImageGeneration(provider, model, path) {
			return true
		}
	}
	return false
}
