package gateway

import (
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestApplyStreamPreconnectPolicyValidatesRangeAtomically(t *testing.T) {
	enabled := true
	for _, tc := range []struct {
		name  string
		min   int
		max   int
		valid bool
	}{
		{"immediate", 0, 0, true},
		{"random", 50, 250, true},
		{"fixed boundary", 60000, 60000, true},
		{"negative minimum", -1, 250, false},
		{"negative maximum", 0, -1, false},
		{"reversed", 251, 250, false},
		{"over maximum", 0, 60001, false},
		{"both over maximum", 60001, 60001, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			policy := storage.GatewayStreamPreconnectPolicy{StreamPreconnectMinDelayMS: 20, StreamPreconnectMaxDelayMS: 40}
			original := policy
			err := applyStreamPreconnectPolicy(&policy, StreamPreconnectPolicyInput{
				StreamPreconnectEnabled: &enabled, StreamPreconnectMinDelayMS: &tc.min, StreamPreconnectMaxDelayMS: &tc.max,
			})
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v error=%v", tc.valid, err)
			}
			if !tc.valid && policy != original {
				t.Fatalf("invalid input mutated policy: %+v", policy)
			}
			if tc.valid && (!policy.StreamPreconnectEnabled || policy.StreamPreconnectMinDelayMS != tc.min || policy.StreamPreconnectMaxDelayMS != tc.max) {
				t.Fatalf("policy = %+v", policy)
			}
		})
	}
}

func TestApplyStreamPreconnectPolicyPartialUpdate(t *testing.T) {
	policy := storage.GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: true, StreamPreconnectMinDelayMS: 50, StreamPreconnectMaxDelayMS: 100}
	original := policy
	if err := applyStreamPreconnectPolicy(&policy, StreamPreconnectPolicyInput{}); err != nil || policy != original {
		t.Fatalf("omitted fields changed policy: %+v, %v", policy, err)
	}
	maxDelay := 49
	if err := applyStreamPreconnectPolicy(&policy, StreamPreconnectPolicyInput{StreamPreconnectMaxDelayMS: &maxDelay}); err == nil || policy != original {
		t.Fatalf("partial update broke existing range: %+v, %v", policy, err)
	}
	disabled := false
	if err := applyStreamPreconnectPolicy(&policy, StreamPreconnectPolicyInput{StreamPreconnectEnabled: &disabled}); err != nil || policy.StreamPreconnectEnabled || policy.StreamPreconnectMinDelayMS != 50 || policy.StreamPreconnectMaxDelayMS != 100 {
		t.Fatalf("disable did not preserve range: %+v, %v", policy, err)
	}
}
