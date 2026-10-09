package storage

import (
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestCandyProbeErrorsPreserveQualityVerdictAndHistory(t *testing.T) {
	db := openTestDB(t)
	groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
	group := &GatewayGroup{Name: "probe-errors", GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "m", CandyCheckIntervalMinutes: 5, CandyCheckCooldownMinutes: 10}}
	if err := groups.Create(group); err != nil {
		t.Fatal(err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1, FallbackOnly: true}}); err != nil {
		t.Fatal(err)
	}
	list, err := routes.ListByGroupID(group.ID)
	if err != nil || len(list) != 1 || !list[0].FallbackOnly {
		t.Fatalf("route: %+v %v", list, err)
	}
	route := list[0]
	key := GatewayCandyCheckConfigKey(group, &route, nil)
	now := time.Now().Truncate(time.Microsecond)
	finish := func(at time.Time, status string) *GatewayRouteCandyCheck {
		t.Helper()
		claim, err := routes.ClaimManualCandyCheck(route.ID, key, "m", at, time.Minute)
		if err != nil || claim == nil {
			t.Fatalf("claim: %v", err)
		}
		ok, err := routes.FinishManualCandyCheck(*claim, GatewayRouteCandyCheck{Status: status}, at)
		if err != nil || !ok {
			t.Fatalf("finish: %v", err)
		}
		loaded, err := routes.FindByID(route.ID)
		if err != nil {
			t.Fatal(err)
		}
		return loaded.CandyCheck
	}
	for i, delay := range []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, time.Minute} {
		at := now.Add(time.Duration(i) * time.Minute)
		state := finish(at, "error")
		if state.Blocks(at) || !state.BackingOff(at) || !state.NextCheckAt.Equal(at.Add(delay)) {
			t.Fatalf("network error became quality cooldown: %+v", state)
		}
	}
	at := now.Add(5 * time.Minute)
	wrong := finish(at, "incorrect")
	if !wrong.Blocks(at) || wrong.BackingOff(at) {
		t.Fatalf("wrong verdict: %+v", wrong)
	}
	state := finish(at.Add(time.Second), "error")
	if !state.Blocks(at) || !state.CooldownUntil.Equal(*wrong.CooldownUntil) {
		t.Fatal("probe error cleared or extended wrong-answer cooldown")
	}
	if state := finish(at.Add(2*time.Second), "correct"); state.Blocks(at) || state.BackingOff(at) || state.ErrorStreak != 0 {
		t.Fatal("correct answer did not recover")
	}
	if err := routes.ClearCandyCheck(route.ID, at.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	events, err := routes.CandyCheckHistory(route.ID)
	if err != nil || len(events) != 8 || events[0].Status != "manual" {
		t.Fatalf("history: %+v %v", events, err)
	}
	// Configuration edits invalidate probe leases but retain the evidence.
	group.CandyCheckModel = "m2"
	if err := groups.Update(group); err != nil {
		t.Fatal(err)
	}
	events, err = routes.CandyCheckHistory(route.ID)
	if err != nil || len(events) != 8 {
		t.Fatal("settings erased outage history")
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		for i := 0; i < 105; i++ {
			if err := appendCandyCheckEvent(tx, GatewayCandyCheckEvent{RouteID: route.ID, Status: "correct", CreatedAt: at}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&GatewayCandyCheckEvent{}).Where("route_id = ?", route.ID).Count(&count).Error; err != nil || count != 100 {
		t.Fatalf("unbounded history: %d %v", count, err)
	}
	if err := routes.SaveForGroup(group.ID, nil); err != nil {
		t.Fatal(err)
	}
	events, err = routes.CandyCheckHistory(route.ID)
	if err != nil || len(events) != 0 {
		t.Fatal("deleted route left orphaned history")
	}
}
