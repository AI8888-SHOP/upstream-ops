package connector

import "testing"

func TestApplyGroupMultiplierMatchesRechargeConversion(t *testing.T) {
	divide := 4.0
	multiply := 2.0

	tests := []struct {
		name       string
		value      float64
		multiplier *float64
		mode       string
		want       float64
	}{
		{name: "nil keeps upstream value", value: 1.234567, want: 1.234567},
		{name: "zero keeps upstream value", value: 1.234567, multiplier: ptrFloat(0), want: 1.234567},
		{name: "divide", value: 10, multiplier: &divide, mode: RechargeMultiplierModeDivide, want: 2.5},
		{name: "multiply", value: 1.23456, multiplier: &multiply, mode: RechargeMultiplierModeMultiply, want: 2.4691},
		{name: "invalid mode defaults to divide", value: 10, multiplier: &divide, mode: "invalid", want: 2.5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotGroup := ApplyGroupMultiplier(tt.value, tt.multiplier, tt.mode)
			gotRecharge := ApplyRechargeMultiplier(tt.value, tt.multiplier, tt.mode)
			if gotGroup != tt.want {
				t.Fatalf("group conversion = %v, want %v", gotGroup, tt.want)
			}
			if gotGroup != gotRecharge {
				t.Fatalf("group conversion = %v, recharge conversion = %v", gotGroup, gotRecharge)
			}
		})
	}
}

func TestNormalizeGroupMultiplierMode(t *testing.T) {
	if got := NormalizeGroupMultiplierMode(RechargeMultiplierModeMultiply); got != RechargeMultiplierModeMultiply {
		t.Fatalf("mode = %q, want multiply", got)
	}
	if got := NormalizeGroupMultiplierMode(" "); got != RechargeMultiplierModeDivide {
		t.Fatalf("mode = %q, want divide", got)
	}
}

func ptrFloat(v float64) *float64 {
	return &v
}
