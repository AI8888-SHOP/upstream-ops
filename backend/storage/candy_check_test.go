package storage

import (
	"testing"
	"time"
)

func TestCandyCheckLeaseManualClearAndConfigChange(t *testing.T) {
	db := openTestDB(t)
	groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
	group := &GatewayGroup{Name: "candy", Status: GatewayGroupStatusActive, GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "model-a", CandyCheckIntervalMinutes: 3, CandyCheckCooldownMinutes: 7, CandyCheckReasoningEffort: "medium"}}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1}}); err != nil {
		t.Fatal(err)
	}
	list, err := routes.ListByGroupID(group.ID)
	if err != nil || len(list) != 1 {
		t.Fatalf("routes: %v", err)
	}
	route := list[0]
	key := GatewayCandyCheckConfigKey(group, &route, nil)
	now := time.Now().Truncate(time.Microsecond)
	claim, err := routes.ClaimCandyCheck(route.ID, key, "model-a", now, time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("claim: %v", err)
	}
	if second, err := routes.ClaimCandyCheck(route.ID, key, "model-a", now, time.Minute); err != nil || second != nil {
		t.Fatalf("duplicate lease: %v %v", second, err)
	}
	ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "incorrect", AnswerPreview: "22"}, now)
	if err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	loaded, err := routes.FindByID(route.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.CandyCheck.Blocks(now) || !loaded.CandyCheck.NextCheckAt.Equal(now.Add(7*time.Minute)) {
		t.Fatalf("cooldown: %+v", loaded.CandyCheck)
	}
	if second, err := routes.ClaimCandyCheck(route.ID, key, "model-a", now.Add(4*time.Minute), time.Minute); err != nil || second != nil {
		t.Fatalf("reprobed while cooling: %v %v", second, err)
	}
	// Returned snapshots must not mutate the cache shared by other requests.
	loaded.CandyCheck.Active = false
	loadedAgain, _ := routes.FindByID(route.ID)
	if !loadedAgain.CandyCheck.Blocks(now) {
		t.Fatal("snapshot mutation leaked")
	}
	if err := routes.ClearCandyCheck(route.ID, now); err != nil {
		t.Fatal(err)
	}
	if ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "error"}, now); err != nil || ok {
		t.Fatalf("stale result overwrote manual release: %v %v", ok, err)
	}
	if second, err := routes.ClaimCandyCheck(route.ID, key, "model-a", now.Add(time.Second), time.Minute); err != nil || second != nil {
		t.Fatalf("manual grace missing: %v %v", second, err)
	}
	claim, err = routes.ClaimCandyCheck(route.ID, key, "model-a", now.Add(3*time.Minute), time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("next cycle: %v", err)
	}
	group.CandyCheckEnabled = false
	if err := groups.Update(group); err != nil {
		t.Fatal(err)
	}
	if ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "incorrect"}, now.Add(3*time.Minute)); err != nil || ok {
		t.Fatalf("disabled policy accepted result: %v %v", ok, err)
	}
	loaded, err = routes.FindByID(route.ID)
	if err != nil || loaded.CandyCheck.Active || loaded.CandyCheck.Blocks(now) {
		t.Fatalf("disabled still blocks: %+v %v", loaded, err)
	}
}

func TestCandyCheckExpiredLeaseAndSuccess(t *testing.T) {
	db := openTestDB(t)
	groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
	group := &GatewayGroup{Name: "candy-restart", GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "model-a", CandyCheckIntervalMinutes: 1, CandyCheckCooldownMinutes: 1}}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1}}); err != nil {
		t.Fatal(err)
	}
	list, _ := routes.ListByGroupID(group.ID)
	key := GatewayCandyCheckConfigKey(group, &list[0], nil)
	now := time.Now()
	old, err := routes.ClaimCandyCheck(list[0].ID, key, "model-a", now, time.Second)
	if err != nil || old == nil {
		t.Fatalf("initial claim: %v", err)
	}
	claim, err := NewGatewayRoutes(db).ClaimCandyCheck(list[0].ID, key, "model-a", now.Add(2*time.Second), time.Minute)
	if err != nil || claim == nil || claim.LeaseToken == old.LeaseToken {
		t.Fatalf("lease recovery: %+v %v", claim, err)
	}
	if ok, err := routes.FinishCandyCheck(*old, GatewayRouteCandyCheck{Status: "error"}, now); err != nil || ok {
		t.Fatalf("old lease completed: %v %v", ok, err)
	}
	if ok, err := routes.FinishCandyCheck(*claim, GatewayRouteCandyCheck{Status: "correct", AnswerPreview: "21"}, now); err != nil || !ok {
		t.Fatalf("success: %v %v", ok, err)
	}
	loaded, err := routes.FindByID(list[0].ID)
	if err != nil || loaded.CandyCheck.Status != "correct" || loaded.CandyCheck.CooldownUntil != nil {
		t.Fatalf("success state: %+v %v", loaded, err)
	}
	if err := groups.Delete(group.ID); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&GatewayRouteCandyCheck{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("orphan state: %d %v", count, err)
	}
}
