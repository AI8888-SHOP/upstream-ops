package gateway

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/bejix/upstream-ops/backend/storage"
)

func TestGroupSellRateMultiplierValidation(t *testing.T) {
	if value, err := groupSellRateMultiplier(nil); err != nil || value != nil {
		t.Fatalf("unset rate = %v, %v", value, err)
	}
	for _, value := range []float64{0, 0.08, 1, 1_000_000} {
		rate, err := groupSellRateMultiplier(&value)
		if err != nil || rate == nil || *rate != value || rate == &value {
			t.Fatalf("rate %v = %v, %v", value, rate, err)
		}
	}
	for _, value := range []float64{-1, 1_000_001, 1e308, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := groupSellRateMultiplier(&value); err == nil {
			t.Errorf("invalid rate %v accepted", value)
		}
	}
}

func TestGatewayGroupSellRateCreateUpdateAndClear(t *testing.T) {
	db := openGatewayTestDB(t)
	groups := storage.NewGatewayGroups(db)
	svc := NewService(
		groups, storage.NewGatewayKeys(db), storage.NewGatewayRoutes(db),
		storage.NewGatewayUsageLogs(db), nil, nil, nil, nil, nil,
	)
	unset, err := svc.CreateGroup(CreateGroupInput{Name: "sell-rate-unset"})
	if err != nil || unset.SellRateMultiplier != nil {
		t.Fatalf("unset create = %+v, %v", unset, err)
	}
	initial := 0.15
	group, err := svc.CreateGroup(CreateGroupInput{Name: "sell-rate-configured", SellRateMultiplier: &initial})
	if err != nil || group.SellRateMultiplier == nil || *group.SellRateMultiplier != initial {
		t.Fatalf("configured create = %+v, %v", group, err)
	}
	for _, tc := range []struct {
		name    string
		payload string
		want    *float64
		wantErr bool
	}{
		{name: "omitted preserves", payload: `{"description":"unrelated change"}`, want: floatPointer(0.15)},
		{name: "negative rejected atomically", payload: `{"sell_rate_multiplier":-1}`, want: floatPointer(0.15), wantErr: true},
		{name: "zero is free", payload: `{"sell_rate_multiplier":0}`, want: floatPointer(0)},
		{name: "omitted preserves zero", payload: `{}`, want: floatPointer(0)},
		{name: "null clears", payload: `{"sell_rate_multiplier":null}`},
		{name: "omitted preserves unset", payload: `{}`},
		{name: "positive restores", payload: `{"sell_rate_multiplier":0.2}`, want: floatPointer(0.2)},
		{name: "upper bound accepted", payload: `{"sell_rate_multiplier":1000000}`, want: floatPointer(1_000_000)},
		{name: "above bound rejected atomically", payload: `{"sell_rate_multiplier":1000001}`, want: floatPointer(1_000_000), wantErr: true},
		{name: "extreme finite rejected atomically", payload: `{"sell_rate_multiplier":1e308}`, want: floatPointer(1_000_000), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input UpdateGroupInput
			if err := json.Unmarshal([]byte(tc.payload), &input); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.UpdateGroup(group.ID, input); (err != nil) != tc.wantErr {
				t.Fatalf("update error = %v, want error %v", err, tc.wantErr)
			}
			var saved storage.GatewayGroup
			if err := db.First(&saved, group.ID).Error; err != nil {
				t.Fatal(err)
			}
			if (saved.SellRateMultiplier == nil) != (tc.want == nil) ||
				(tc.want != nil && *saved.SellRateMultiplier != *tc.want) {
				t.Fatalf("persisted rate = %v, want %v", saved.SellRateMultiplier, tc.want)
			}
		})
	}
	for _, value := range []float64{-1, 1_000_001, 1e308, math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := svc.CreateGroup(CreateGroupInput{Name: "sell-rate-invalid", SellRateMultiplier: &value}); err == nil {
			t.Fatalf("create accepted invalid rate %v", value)
		}
		if _, err := svc.UpdateGroup(group.ID, UpdateGroupInput{SellRateMultiplier: optionalFloat64{Set: true, Value: &value}}); err == nil {
			t.Fatalf("update accepted invalid rate %v", value)
		}
	}
	free := 0.0
	createdFree, err := svc.CreateGroup(CreateGroupInput{Name: "sell-rate-free", SellRateMultiplier: &free})
	if err != nil || createdFree.SellRateMultiplier == nil || *createdFree.SellRateMultiplier != 0 {
		t.Fatalf("free create = %+v, %v", createdFree, err)
	}
	maximum := 1_000_000.0
	createdMax, err := svc.CreateGroup(CreateGroupInput{Name: "sell-rate-max", SellRateMultiplier: &maximum})
	if err != nil || createdMax.SellRateMultiplier == nil || *createdMax.SellRateMultiplier != maximum {
		t.Fatalf("maximum create = %+v, %v", createdMax, err)
	}
}
