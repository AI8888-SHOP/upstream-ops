package gateway

import (
	"errors"
	"math"
)

// Match the existing billing-rate guard ceiling and avoid unbounded products
// in revenue aggregates when a malformed-but-finite value is submitted.
const maxGroupSellRateMultiplier = 1_000_000

// groupSellRateMultiplier keeps unset distinct from an explicitly free rate.
// It is a reporting input only: key settlement and routing never use this rate.
func groupSellRateMultiplier(value *float64) (*float64, error) {
	if value == nil {
		return nil, nil
	}
	if math.IsNaN(*value) || math.IsInf(*value, 0) || *value < 0 || *value > maxGroupSellRateMultiplier {
		return nil, errors.New("sell_rate_multiplier must be a finite number between 0 and 1000000")
	}
	rate := *value
	return &rate, nil
}
