package storage

import (
	"testing"
	"time"
)

func TestSharedModelCooldownFollowsSameUpstreamCredentialAcrossGatewayGroups(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	groupID := int64(77)
	inputs := []GatewayRoute{{
		SourceChannelID:  12,
		SourceGroupID:    &groupID,
		SourceGroupName:  "premium",
		UpstreamProtocol: GatewayUpstreamProtocolOpenAIChat,
		Enabled:          true,
	}}
	if err := routes.SaveForGroup(9101, inputs); err != nil {
		t.Fatalf("save first gateway group: %v", err)
	}
	if err := routes.SaveForGroup(9102, inputs); err != nil {
		t.Fatalf("save second gateway group: %v", err)
	}
	first, err := routes.ListByGroupID(9101)
	if err != nil || len(first) != 1 {
		t.Fatalf("list first gateway group: err=%v count=%d", err, len(first))
	}
	second, err := routes.ListByGroupID(9102)
	if err != nil || len(second) != 1 {
		t.Fatalf("list second gateway group: err=%v count=%d", err, len(second))
	}
	if err := routes.UpdateSourceKey(first[0].ID, 501, "uops-ch12-sg77", "cipher-a"); err != nil {
		t.Fatalf("bind first source key: %v", err)
	}
	if err := routes.UpdateSourceKey(second[0].ID, 501, "uops-ch12-sg77", "cipher-b"); err != nil {
		t.Fatalf("bind second source key: %v", err)
	}

	until := time.Now().Add(-time.Second)
	if err := routes.SetModelTempUnschedulableWithProbeProtocol(
		first[0].ID, "upstream-model", until, "upstream failed", time.Now().Add(-time.Minute), "failure-a", true, "responses",
	); err != nil {
		t.Fatalf("set shared cooldown: %v", err)
	}
	first, err = routes.ListByGroupID(9101)
	if err != nil {
		t.Fatalf("reload first route: %v", err)
	}
	second, err = routes.ListByGroupID(9102)
	if err != nil {
		t.Fatalf("reload second route: %v", err)
	}
	left := first[0].ModelCooldowns["upstream-model"]
	right := second[0].ModelCooldowns["upstream-model"]
	if left.SharedCooldownID == 0 || left.SharedCooldownID != right.SharedCooldownID {
		t.Fatalf("cooldown was not projected from one shared record: left=%+v right=%+v", left, right)
	}
	if left.TempUnschedulableUntil == nil || right.TempUnschedulableUntil == nil {
		t.Fatalf("shared cooldown did not block both routes: left=%+v right=%+v", left, right)
	}

	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 4, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim one shared probe: err=%v claims=%d", err, len(claims))
	}
	if claims[0].SharedCooldownID == 0 || claims[0].ProbeInboundProtocol != "openai_responses" {
		t.Fatalf("unexpected shared probe claim: %+v", claims[0])
	}
	more, err := routes.ClaimDueModelCooldownProbes(time.Now(), 4, time.Minute)
	if err != nil || len(more) != 0 {
		t.Fatalf("same shared credential received duplicate probe: err=%v claims=%d", err, len(more))
	}
	if updated, err := routes.MarkModelProbeSuccess(claims[0], time.Now(), 200); err != nil || !updated {
		t.Fatalf("recover shared probe: updated=%v err=%v", updated, err)
	}
	second, err = routes.ListByGroupID(9102)
	if err != nil {
		t.Fatalf("reload recovered second route: %v", err)
	}
	if cooldown := second[0].ModelCooldowns["upstream-model"]; cooldown.TempUnschedulableUntil != nil || cooldown.ProbeStatus != GatewayModelProbeStatusHealthy {
		t.Fatalf("probe recovery did not follow to second group: %+v", cooldown)
	}

	if err := routes.SetModelTempUnschedulable(first[0].ID, "upstream-model", time.Now().Add(time.Minute), "failed again", time.Now(), "failure-b"); err != nil {
		t.Fatalf("set second shared cooldown: %v", err)
	}
	if err := routes.ClearTempUnschedulable(second[0].ID); err != nil {
		t.Fatalf("manual clear from second group: %v", err)
	}
	first, err = routes.ListByGroupID(9101)
	if err != nil {
		t.Fatalf("reload manually cleared first route: %v", err)
	}
	if cooldown := first[0].ModelCooldowns["upstream-model"]; cooldown.TempUnschedulableUntil != nil {
		t.Fatalf("manual clear did not follow to first group: %+v", cooldown)
	}
}

func TestGroupModelCooldownDoesNotCrossGatewayGroups(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	remoteGroupID := int64(88)
	input := GatewayRoute{
		SourceChannelID:  18,
		SourceGroupID:    &remoteGroupID,
		SourceGroupName:  "premium",
		UpstreamProtocol: GatewayUpstreamProtocolOpenAIChat,
		Enabled:          true,
	}
	if err := routes.SaveForGroup(9111, []GatewayRoute{input}); err != nil {
		t.Fatalf("save first gateway group: %v", err)
	}
	if err := routes.SaveForGroup(9112, []GatewayRoute{input}); err != nil {
		t.Fatalf("save second gateway group: %v", err)
	}
	first, err := routes.ListByGroupID(9111)
	if err != nil || len(first) != 1 {
		t.Fatalf("list first gateway group: err=%v count=%d", err, len(first))
	}
	second, err := routes.ListByGroupID(9112)
	if err != nil || len(second) != 1 {
		t.Fatalf("list second gateway group: err=%v count=%d", err, len(second))
	}
	until := time.Now().Add(-time.Second)
	if err := routes.SetGroupModelTempUnschedulableWithProbeProtocol(
		first[0].ID, "upstream-model", until, "first token timeout", time.Now(), "group-timeout", true, "responses",
	); err != nil {
		t.Fatalf("set group cooldown: %v", err)
	}
	first, err = routes.ListByGroupID(9111)
	if err != nil {
		t.Fatalf("reload first route: %v", err)
	}
	second, err = routes.ListByGroupID(9112)
	if err != nil {
		t.Fatalf("reload second route: %v", err)
	}
	left := first[0].ModelCooldowns["upstream-model"]
	if left.CooldownScope != GatewayModelCooldownScopeGroup || left.SharedCooldownID != 0 || left.TempUnschedulableUntil == nil {
		t.Fatalf("group cooldown was not kept route-local: %+v", left)
	}
	if _, exists := second[0].ModelCooldowns["upstream-model"]; exists {
		t.Fatalf("group cooldown crossed into another gateway group: %+v", second[0].ModelCooldowns)
	}
	var sharedCount int64
	if err := db.Model(&GatewaySharedModelCooldown{}).Count(&sharedCount).Error; err != nil {
		t.Fatalf("count shared cooldowns: %v", err)
	}
	if sharedCount != 0 {
		t.Fatalf("group cooldown unexpectedly created shared rows: %d", sharedCount)
	}

	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 2, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim group probe: err=%v claims=%d", err, len(claims))
	}
	if claims[0].CooldownScope != GatewayModelCooldownScopeGroup || claims[0].RouteID != first[0].ID {
		t.Fatalf("unexpected group probe claim: %+v", claims[0])
	}
	if updated, err := routes.MarkModelProbeSuccess(claims[0], time.Now(), 200); err != nil || !updated {
		t.Fatalf("recover group probe: updated=%v err=%v", updated, err)
	}
	second, err = routes.ListByGroupID(9112)
	if err != nil {
		t.Fatalf("reload second route after recovery: %v", err)
	}
	if _, exists := second[0].ModelCooldowns["upstream-model"]; exists {
		t.Fatalf("group recovery leaked a cooldown into another gateway group: %+v", second[0].ModelCooldowns)
	}
}

func TestSharedModelCooldownDoesNotJoinDifferentCredentialOrProtocol(t *testing.T) {
	groupID := int64(9)
	base := GatewayRoute{
		SourceChannelID:  8,
		SourceGroupID:    &groupID,
		SourceGroupName:  "vip",
		SourceAPIKeyID:   100,
		UpstreamProtocol: GatewayUpstreamProtocolOpenAIChat,
	}
	otherKey := base
	otherKey.SourceAPIKeyID = 101
	responses := base
	responses.UpstreamProtocol = GatewayUpstreamProtocolOpenAIResponses
	otherGroupID := int64(10)
	otherGroup := base
	otherGroup.SourceGroupID = &otherGroupID

	baseScope := GatewaySharedModelCooldownScope(&base)
	if baseScope == "" {
		t.Fatal("base route did not produce a shared scope")
	}
	if baseScope == GatewaySharedModelCooldownScope(&otherKey) {
		t.Fatal("different upstream API keys must not share model cooldowns")
	}
	if baseScope == GatewaySharedModelCooldownScope(&responses) {
		t.Fatal("different route protocols must not share model cooldowns")
	}
	if baseScope == GatewaySharedModelCooldownScope(&otherGroup) {
		t.Fatal("different source groups must not share model cooldowns")
	}
}

func TestFindActiveRouteForSharedModelCooldownSkipsDisabledGatewayGroups(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	groups := NewGatewayGroups(db)
	disabled := &GatewayGroup{Name: "shared-cooldown-disabled", Status: GatewayGroupStatusDisabled}
	enabled := &GatewayGroup{Name: "shared-cooldown-enabled", Status: GatewayGroupStatusActive}
	if err := groups.Create(disabled); err != nil {
		t.Fatalf("create disabled group: %v", err)
	}
	if err := groups.Create(enabled); err != nil {
		t.Fatalf("create enabled group: %v", err)
	}
	remoteGroupID := int64(24)
	routeInput := []GatewayRoute{{
		SourceChannelID: 31, SourceGroupID: &remoteGroupID, SourceGroupName: "shared",
		Enabled: true,
	}}
	if err := routes.SaveForGroup(disabled.ID, routeInput); err != nil {
		t.Fatalf("save disabled route: %v", err)
	}
	if err := routes.SaveForGroup(enabled.ID, routeInput); err != nil {
		t.Fatalf("save enabled route: %v", err)
	}
	disabledRoutes, err := routes.ListByGroupID(disabled.ID)
	if err != nil || len(disabledRoutes) != 1 {
		t.Fatalf("list disabled route: err=%v count=%d", err, len(disabledRoutes))
	}
	enabledRoutes, err := routes.ListByGroupID(enabled.ID)
	if err != nil || len(enabledRoutes) != 1 {
		t.Fatalf("list enabled route: err=%v count=%d", err, len(enabledRoutes))
	}
	if err := routes.UpdateSourceKey(disabledRoutes[0].ID, 909, "uops-ch31-sg24", "cipher-disabled"); err != nil {
		t.Fatalf("bind disabled source key: %v", err)
	}
	if err := routes.UpdateSourceKey(enabledRoutes[0].ID, 909, "uops-ch31-sg24", "cipher-enabled"); err != nil {
		t.Fatalf("bind enabled source key: %v", err)
	}
	disabledRoutes, err = routes.ListByGroupID(disabled.ID)
	if err != nil || len(disabledRoutes) != 1 {
		t.Fatalf("reload disabled route: err=%v count=%d", err, len(disabledRoutes))
	}
	scope := GatewaySharedModelCooldownScope(&disabledRoutes[0])
	selected, err := routes.FindActiveRouteForSharedModelCooldown(scope, disabledRoutes[0].ID)
	if err != nil {
		t.Fatalf("find active shared route: %v", err)
	}
	if selected == nil || selected.ID != enabledRoutes[0].ID {
		t.Fatalf("selected route=%+v, want enabled route %d", selected, enabledRoutes[0].ID)
	}
}

func TestAutoMigratePromotesLegacyModelCooldownsToSharedState(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	remoteGroupID := int64(37)
	if err := routes.SaveForGroup(9301, []GatewayRoute{{
		SourceChannelID: 17, SourceGroupID: &remoteGroupID, SourceGroupName: "legacy", Enabled: true,
	}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(9301)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v count=%d", err, len(items))
	}
	if err := routes.UpdateSourceKey(items[0].ID, 1701, "uops-ch17-sg37", "cipher"); err != nil {
		t.Fatalf("bind source key: %v", err)
	}
	if err := db.Migrator().DropTable(&GatewaySharedModelCooldown{}); err != nil {
		t.Fatalf("drop shared table to simulate pre-release schema: %v", err)
	}
	until := time.Now().Add(time.Minute)
	if err := db.Create(&GatewayRouteModelCooldown{
		RouteID: items[0].ID, Model: "legacy-model", TempUnschedulableUntil: &until,
		TempUnschedulableReason: "legacy failure",
	}).Error; err != nil {
		t.Fatalf("create legacy cooldown: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("upgrade migration: %v", err)
	}
	var localCount, sharedCount int64
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", items[0].ID).Count(&localCount).Error; err != nil {
		t.Fatalf("count legacy cooldowns: %v", err)
	}
	if err := db.Model(&GatewaySharedModelCooldown{}).Count(&sharedCount).Error; err != nil {
		t.Fatalf("count shared cooldowns: %v", err)
	}
	if localCount != 0 || sharedCount != 1 {
		t.Fatalf("migration counts local=%d shared=%d, want 0 and 1", localCount, sharedCount)
	}
}

func TestSourceKeyBindingPromotesLegacyModelCooldownToSharedState(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	groupID := int64(12)
	if err := routes.SaveForGroup(9201, []GatewayRoute{{
		SourceChannelID: 4, SourceGroupID: &groupID, SourceGroupName: "legacy", Enabled: true,
	}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(9201)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v count=%d", err, len(items))
	}
	routeID := items[0].ID
	if err := routes.SetModelTempUnschedulable(routeID, "legacy-model", time.Now().Add(time.Minute), "legacy failure", time.Now(), "legacy-request"); err != nil {
		t.Fatalf("set legacy cooldown: %v", err)
	}
	var localCount int64
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", routeID).Count(&localCount).Error; err != nil || localCount != 1 {
		t.Fatalf("expected one legacy row before key binding: count=%d err=%v", localCount, err)
	}
	if err := routes.UpdateSourceKey(routeID, 404, "uops-ch4-sg12", "cipher"); err != nil {
		t.Fatalf("bind source key and promote cooldown: %v", err)
	}
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", routeID).Count(&localCount).Error; err != nil || localCount != 0 {
		t.Fatalf("legacy cooldown remained after source key binding: count=%d err=%v", localCount, err)
	}
	items, err = routes.ListByGroupID(9201)
	if err != nil {
		t.Fatalf("reload promoted route: %v", err)
	}
	cooldown := items[0].ModelCooldowns["legacy-model"]
	if cooldown.SharedCooldownID == 0 || cooldown.TempUnschedulableUntil == nil {
		t.Fatalf("legacy cooldown was not promoted into shared state: %+v", cooldown)
	}
}
