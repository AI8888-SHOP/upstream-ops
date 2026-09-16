package gateway

import (
	"errors"
	"math"
	"strings"

	"github.com/bejix/upstream-ops/backend/storage"
)

type SchedulingPolicyInput struct {
	SchedulingMode           *string  `json:"scheduling_mode"`
	SchedulingPremiumPercent *float64 `json:"scheduling_premium_percent"`
	SchedulingWindowMinutes  *int     `json:"scheduling_window_minutes"`
	SchedulingMinSamples     *int     `json:"scheduling_min_samples"`
	SchedulingTargetTTFTSec  *int     `json:"scheduling_target_ttft_sec"`
}

func applySchedulingPolicy(policy *storage.GatewaySchedulerPolicy, in SchedulingPolicyInput) error {
	if in.SchedulingMode != nil {
		mode := strings.ToLower(strings.TrimSpace(*in.SchedulingMode))
		if mode != "cost" && mode != "balanced" && mode != "latency" {
			return errors.New("scheduling_mode must be cost, balanced or latency")
		}
		policy.SchedulingMode = mode
	}
	if in.SchedulingPremiumPercent != nil {
		value := *in.SchedulingPremiumPercent
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 1000 {
			return errors.New("scheduling_premium_percent must be between 0 and 1000")
		}
		policy.SchedulingPremiumPercent = value
	}
	for _, field := range []struct {
		input *int
		dest  *int
		max   int
	}{
		{in.SchedulingWindowMinutes, &policy.SchedulingWindowMinutes, 60},
		{in.SchedulingMinSamples, &policy.SchedulingMinSamples, 1000},
		{in.SchedulingTargetTTFTSec, &policy.SchedulingTargetTTFTSec, 300},
	} {
		if field.input != nil {
			if *field.input < 1 || *field.input > field.max {
				return errors.New("invalid scheduling window (1–60), minimum samples (1–1000) or TTFT target (1–300)")
			}
			*field.dest = *field.input
		}
	}
	return nil
}
