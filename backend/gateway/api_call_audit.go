package gateway

import (
	"context"
	"math"

	"github.com/bejix/upstream-ops/backend/apiaudit"
	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/gin-gonic/gin"
)

func routeAPIAuditMetadata(route *storage.GatewayRoute) storage.GatewayAPICallLog {
	if route == nil {
		return storage.GatewayAPICallLog{}
	}
	return storage.GatewayAPICallLog{GatewayGroupID: route.GatewayGroupID, RouteID: route.ID, ChannelID: route.SourceChannelID, GatewayProviderID: route.GatewayProviderID, SourceGroupID: route.SourceGroupID, SourceAPIKeyID: route.SourceAPIKeyID, SourceAPIKeyName: route.SourceAPIKeyName}
}

func apiCallPurpose(ctx context.Context, purpose string) context.Context {
	return apiaudit.WithMetadata(ctx, storage.GatewayAPICallLog{Purpose: purpose})
}

// The API audit is an operator ledger only. It never debits a gateway key or
// runs winner settlement. Missing usage/pricing remains unknown, not zero.
func (rt *Runtime) finishAPICallCost(item *storage.GatewayAPICallLog) {
	if rt == nil || rt.Pricing == nil || item.Model == "" {
		return
	}
	price := rt.Pricing.Resolve(item.Model)
	if !price.HasTokenPrice() && !price.IsPerRequest() {
		return
	}
	if !item.UsageKnown && !price.IsPerRequest() {
		return
	}
	if price.IsPerRequest() && (price.PerRequestPrice == nil || item.StatusCode < 200 || item.StatusCode >= 300) {
		return
	}
	rate := float64(0)
	if item.RouteID != 0 && rt.Routes != nil {
		if route, err := rt.Routes.FindByID(item.RouteID); err == nil {
			rate = route.BillingRateMultiplier
			if route.RateConvertMode == "custom" {
				rate = route.RateConvertValue
			}
		}
	}
	if rate <= 0 && item.GatewayProviderID != 0 && rt.Providers != nil {
		if provider, err := rt.Providers.FindByID(item.GatewayProviderID); err == nil {
			rate = provider.DefaultBillingRate
		}
	}
	if rate <= 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return
	}
	tokens := UsageTokens{InputTokens: item.InputTokens, OutputTokens: item.OutputTokens, CacheReadTokens: item.CacheReadTokens, CacheCreationTokens: item.CacheCreationTokens, CacheCreation5mTokens: item.CacheCreation5mTokens, CacheCreation1hTokens: item.CacheCreation1hTokens, ImageOutputTokens: item.ImageOutputTokens, ReasoningTokens: item.ReasoningTokens}
	value := CalculateCost(price, tokens, rate, rate).ActualCost
	if value >= 0 && !math.IsNaN(value) && !math.IsInf(value, 0) {
		item.EstimatedCost = &value
	}
}

func gatewayAPICallContext(ctx context.Context, c *gin.Context) context.Context {
	if c != nil {
		if v, ok := c.Get(ctxKeyUpstreamOpsRequestID); ok {
			if id, ok := v.(string); ok && id != "" {
				return apiaudit.WithMetadata(ctx, storage.GatewayAPICallLog{GatewayRequestID: id})
			}
		}
	}
	return ctx
}
