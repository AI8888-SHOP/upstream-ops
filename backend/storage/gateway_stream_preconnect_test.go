package storage

import "testing"

func TestGatewayStreamPreconnectSub2apiMigrationKeepsExistingGroupOptedOut(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	legacy := &GatewayGroup{
		Name: "legacy-enabled-stream-preconnect",
		GatewayStreamPreconnectPolicy: GatewayStreamPreconnectPolicy{
			StreamPreconnectEnabled: true, StreamPreconnectMinDelayMS: 30, StreamPreconnectMaxDelayMS: 90,
		},
	}
	if err := groups.Create(legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropColumn(&GatewayGroup{}, "stream_preconnect_sub2api_enabled"); err != nil {
		t.Fatal(err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	var restored GatewayGroup
	if err := db.First(&restored, legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if restored.GatewayStreamPreconnectPolicy != legacy.GatewayStreamPreconnectPolicy {
		t.Fatalf("migration changed existing policy or opted group in: %+v", restored.GatewayStreamPreconnectPolicy)
	}
}

func TestGatewayStreamPreconnectMultihopMigrationKeepsExistingGroupOptedOut(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	legacy := &GatewayGroup{
		Name: "legacy-semantic-stream-preconnect",
		GatewayStreamPreconnectPolicy: GatewayStreamPreconnectPolicy{
			StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true,
			StreamPreconnectMinDelayMS: 30, StreamPreconnectMaxDelayMS: 90,
		},
	}
	if err := groups.Create(legacy); err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropColumn(&GatewayGroup{}, "stream_preconnect_multihop_enabled"); err != nil {
		t.Fatal(err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	var restored GatewayGroup
	if err := db.First(&restored, legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if restored.GatewayStreamPreconnectPolicy != legacy.GatewayStreamPreconnectPolicy {
		t.Fatalf("migration changed semantic policy or opted group into multihop: %+v", restored.GatewayStreamPreconnectPolicy)
	}
}

func TestGatewayStreamPreconnectMigrationDefaultAndClone(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	legacy := &GatewayGroup{Name: "legacy-stream-preconnect"}
	if err := groups.Create(legacy); err != nil {
		t.Fatal(err)
	}
	for _, column := range []string{"stream_preconnect_enabled", "stream_preconnect_sub2api_enabled", "stream_preconnect_multihop_enabled", "stream_preconnect_min_delay_ms", "stream_preconnect_max_delay_ms"} {
		if err := db.Migrator().DropColumn(&GatewayGroup{}, column); err != nil {
			t.Fatalf("drop %s: %v", column, err)
		}
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatal(err)
	}
	var restored GatewayGroup
	if err := db.First(&restored, legacy.ID).Error; err != nil {
		t.Fatal(err)
	}
	if restored.GatewayStreamPreconnectPolicy != (GatewayStreamPreconnectPolicy{}) {
		t.Fatalf("legacy group should remain disabled with zero delays: %+v", restored.GatewayStreamPreconnectPolicy)
	}
	restored.GatewayStreamPreconnectPolicy = GatewayStreamPreconnectPolicy{StreamPreconnectEnabled: true, StreamPreconnectSub2apiEnabled: true, StreamPreconnectMultihopEnabled: true, StreamPreconnectMinDelayMS: 30, StreamPreconnectMaxDelayMS: 90}
	if err := groups.Update(&restored); err != nil {
		t.Fatal(err)
	}
	clone, err := groups.Clone(restored.ID, "cloned-stream-preconnect", nil)
	if err != nil {
		t.Fatal(err)
	}
	if clone.Group.GatewayStreamPreconnectPolicy != restored.GatewayStreamPreconnectPolicy {
		t.Fatalf("clone policy = %+v", clone.Group.GatewayStreamPreconnectPolicy)
	}
	clone.Group.GatewayStreamPreconnectPolicy = GatewayStreamPreconnectPolicy{}
	if err := groups.Update(&clone.Group); err != nil {
		t.Fatal(err)
	}
	saved, err := groups.FindByID(clone.Group.ID)
	if err != nil || saved.GatewayStreamPreconnectPolicy != (GatewayStreamPreconnectPolicy{}) {
		t.Fatalf("explicit false/zero not persisted: %+v, %v", saved, err)
	}
	source, err := groups.FindByID(restored.ID)
	if err != nil || source.GatewayStreamPreconnectPolicy != restored.GatewayStreamPreconnectPolicy {
		t.Fatalf("clone update changed source: %+v, %v", source, err)
	}
}
