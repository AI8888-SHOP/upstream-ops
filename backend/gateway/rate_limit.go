package gateway

import (
	"context"
	"fmt"
	"math"

	"github.com/bejix/upstream-ops/backend/storage"
)

// normalizeMaxBillingRateMultiplier treats zero and invalid values as disabled.
// The admin API uses the same behavior for omitted and non-positive values.
func normalizeMaxBillingRateMultiplier(value float64) float64 {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return 0
	}
	return value
}

func rateLimitState(rate, limit float64) (bool, string) {
	if limit <= 0 {
		return false, ""
	}
	if math.IsNaN(rate) || math.IsInf(rate, 0) {
		return true, fmt.Sprintf("effective billing multiplier is invalid (%v)", rate)
	}
	if rate > limit {
		return true, fmt.Sprintf("effective billing multiplier %.6g exceeds group limit %.6g", rate, limit)
	}
	return false, ""
}

// applyRateLimitForGroup refreshes the derived route state after a rate or
// group-policy change. It updates only the two derived columns so concurrent
// cooldown and credential changes are not overwritten.
func (a *AdminService) applyRateLimitForGroup(groupID uint) error {
	if a == nil || a.Groups == nil || a.Routes == nil {
		return nil
	}
	group, err := a.Groups.FindByID(groupID)
	if err != nil {
		return err
	}
	routes, err := a.Routes.ListByGroupID(groupID)
	if err != nil {
		return err
	}
	if len(routes) == 0 {
		return nil
	}
	groupsByChannel := a.loadGroupsByChannel(context.Background(), routes)
	limit := normalizeMaxBillingRateMultiplier(group.MaxBillingRateMultiplier)
	for i := range routes {
		rate := RateForRoute(&routes[i], groupsByChannel[routes[i].SourceChannelID])
		disabled, reason := rateLimitState(rate, limit)
		if routes[i].RateLimitAutoDisabled == disabled && routes[i].RateLimitAutoDisabledReason == reason {
			continue
		}
		if err := a.Routes.SetRateLimitAutoDisabled(routes[i].ID, disabled, reason); err != nil {
			return fmt.Errorf("update route %d rate limit state: %w", routes[i].ID, err)
		}
	}
	return nil
}

func (a *AdminService) applyRateLimitForProvider(providerID uint) error {
	if a == nil || a.Groups == nil || a.Routes == nil || providerID == 0 {
		return nil
	}
	if a.Providers == nil {
		return nil
	}
	provider, err := a.Providers.FindByID(providerID)
	if err != nil {
		return err
	}
	groups, err := a.Groups.List()
	if err != nil {
		return err
	}
	for _, group := range groups {
		routes, err := a.Routes.ListByGroupID(group.ID)
		if err != nil {
			return err
		}
		matched := false
		for _, route := range routes {
			if route.NormalizeSourceKind() == storage.GatewayRouteSourceProvider && route.GatewayProviderID == providerID {
				matched = true
				refreshed := route
				a.applyProviderRouteBilling(&refreshed, provider)
				if refreshed.BillingRateMultiplier != route.BillingRateMultiplier ||
					refreshed.RateConvertValue != route.RateConvertValue {
					if err := a.Routes.SetProviderBillingSnapshot(route.ID, refreshed.BillingRateMultiplier, refreshed.RateConvertValue); err != nil {
						return fmt.Errorf("update provider route %d billing rate: %w", route.ID, err)
					}
				}
			}
		}
		if matched {
			if err := a.applyRateLimitForGroup(group.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
