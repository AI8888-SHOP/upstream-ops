package storage

import "testing"

func TestGatewaySellRateMigrationCloneAndCacheIsolation(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	legacy := &GatewayGroup{Name: "legacy-sell-rate"}
	if err := groups.Create(legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropColumn(&GatewayGroup{}, "sell_rate_multiplier"); err != nil {
		t.Fatal(err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	var restored GatewayGroup
	if err := db.First(&restored, legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if restored.SellRateMultiplier != nil {
		t.Fatalf("migration invented selling rate %v", restored.SellRateMultiplier)
	}
	unsetClone, err := groups.Clone(restored.ID, "cloned-unset-sell-rate", nil)
	if err != nil || unsetClone.Group.SellRateMultiplier != nil {
		t.Fatalf("unset clone = %+v, %v", unsetClone, err)
	}
	rate := 0.25
	restored.SellRateMultiplier = &rate
	if err := groups.Update(&restored); err != nil {
		t.Fatal(err)
	}
	cloned, err := groups.Clone(restored.ID, "cloned-sell-rate", nil)
	if err != nil || cloned.Group.SellRateMultiplier == nil || *cloned.Group.SellRateMultiplier != rate {
		t.Fatalf("configured clone = %+v, %v", cloned, err)
	}
	firstRead, err := groups.FindByID(restored.ID)
	if err != nil {
		t.Fatal(err)
	}
	*firstRead.SellRateMultiplier = 100
	secondRead, err := groups.FindByID(restored.ID)
	if err != nil || secondRead.SellRateMultiplier == nil || *secondRead.SellRateMultiplier != rate {
		t.Fatalf("caller mutated cached rate: %+v, %v", secondRead, err)
	}
	free := 0.0
	for _, next := range []*float64{&free, nil} {
		cloned.Group.SellRateMultiplier = next
		if err := groups.Update(&cloned.Group); err != nil {
			t.Fatal(err)
		}
		saved, err := groups.FindByID(cloned.Group.ID)
		if err != nil || (saved.SellRateMultiplier == nil) != (next == nil) ||
			(next != nil && *saved.SellRateMultiplier != *next) {
			t.Fatalf("zero/null update = %+v, %v", saved, err)
		}
	}
	source, err := groups.FindByID(restored.ID)
	if err != nil || source.SellRateMultiplier == nil || *source.SellRateMultiplier != rate {
		t.Fatalf("clone update changed source: %+v, %v", source, err)
	}
}
