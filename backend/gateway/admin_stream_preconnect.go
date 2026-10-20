package gateway

import (
	"errors"

	"github.com/bejix/upstream-ops/backend/storage"
)

// StreamPreconnectPolicyInput uses pointers so omitted fields preserve the
// existing group policy, including when an older admin client saves a group.
type StreamPreconnectPolicyInput struct {
	StreamPreconnectEnabled    *bool `json:"stream_preconnect_enabled"`
	StreamPreconnectMinDelayMS *int  `json:"stream_preconnect_min_delay_ms"`
	StreamPreconnectMaxDelayMS *int  `json:"stream_preconnect_max_delay_ms"`
}

func applyStreamPreconnectPolicy(policy *storage.GatewayStreamPreconnectPolicy, in StreamPreconnectPolicyInput) error {
	next := *policy
	if in.StreamPreconnectEnabled != nil {
		next.StreamPreconnectEnabled = *in.StreamPreconnectEnabled
	}
	if in.StreamPreconnectMinDelayMS != nil {
		next.StreamPreconnectMinDelayMS = *in.StreamPreconnectMinDelayMS
	}
	if in.StreamPreconnectMaxDelayMS != nil {
		next.StreamPreconnectMaxDelayMS = *in.StreamPreconnectMaxDelayMS
	}
	if next.StreamPreconnectMinDelayMS < 0 || next.StreamPreconnectMaxDelayMS > 60000 || next.StreamPreconnectMinDelayMS > next.StreamPreconnectMaxDelayMS {
		return errors.New("stream preconnect delay must satisfy 0 <= min <= max <= 60000 milliseconds")
	}
	*policy = next
	return nil
}
