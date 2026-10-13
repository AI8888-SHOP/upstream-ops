package storage

import (
	"fmt"
	"testing"
	"time"
)

func TestCandyCheckRequestRejectionDoesNotRestrictRoute(t *testing.T) {
	for _, manual := range []bool{false, true} {
		for _, code := range []int{400, 404, 405, 413, 415, 422} {
			t.Run(fmt.Sprintf("manual=%t/status=%d", manual, code), func(t *testing.T) {
				db := openTestDB(t)
				groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
				group := &GatewayGroup{Name: "request-rejected", GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "m", CandyCheckIntervalMinutes: 5, CandyCheckCooldownMinutes: 10}}
				if err := groups.Create(group); err != nil {
					t.Fatal(err)
				}
				if err := routes.SaveForGroup(group.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1}}); err != nil {
					t.Fatal(err)
				}
				list, err := routes.ListByGroupID(group.ID)
				if err != nil || len(list) != 1 {
					t.Fatalf("routes: %+v %v", list, err)
				}
				route := list[0]
				key := GatewayCandyCheckConfigKey(group, &route, nil)
				now := time.Now().Truncate(time.Microsecond)
				finish := func(at time.Time, status string, code int, force bool) *GatewayRouteCandyCheck {
					t.Helper()
					claimFn, finishFn := routes.ClaimCandyCheck, routes.FinishCandyCheck
					if manual || force {
						claimFn, finishFn = routes.ClaimManualCandyCheck, routes.FinishManualCandyCheck
					}
					claim, err := claimFn(route.ID, key, "m", at, time.Minute)
					if err != nil || claim == nil {
						t.Fatalf("claim: %v", err)
					}
					ok, err := finishFn(*claim, GatewayRouteCandyCheck{Status: status, StatusCode: code}, at)
					if err != nil || !ok {
						t.Fatalf("finish: %v %v", ok, err)
					}
					loaded, err := routes.FindByID(route.ID)
					if err != nil || loaded.CandyCheck == nil {
						t.Fatalf("load: %v", err)
					}
					return loaded.CandyCheck
				}
				state := finish(now, "error", code, false)
				if state.Status != "error" || state.StatusCode != code || state.Blocks(now) || state.BackingOff(now) || state.ErrorStreak != 0 || state.NextCheckAt == nil || !state.NextCheckAt.Equal(now.Add(5*time.Minute)) {
					t.Fatalf("request rejection restricted route or rescheduled early: %+v", state)
				}
				if claim, err := routes.ClaimCandyCheck(route.ID, key, "m", now.Add(time.Minute), time.Minute); err != nil || claim != nil {
					t.Fatalf("repeated incompatible probe before interval: %+v %v", claim, err)
				}
				events, err := routes.CandyCheckHistory(route.ID)
				if err != nil || len(events) != 1 || events[0].BackoffUntil != nil || events[0].Manual != manual || events[0].StatusCode != code {
					t.Fatalf("history: %+v %v", events, err)
				}
				// A manual retest can remove a previous transport backoff, but
				// cannot erase or extend a real wrong-answer quality cooldown.
				at := now.Add(5 * time.Minute)
				wrong := finish(at, "incorrect", 200, false)
				failed := finish(at.Add(time.Second), "error", 503, true)
				if !failed.BackingOff(at.Add(time.Second)) {
					t.Fatal("server error lost transport backoff")
				}
				state = finish(at.Add(2*time.Second), "error", code, true)
				if !state.Blocks(at) || state.CooldownUntil == nil || !state.CooldownUntil.Equal(*wrong.CooldownUntil) || state.BackoffUntil != nil || state.ErrorStreak != 0 || !state.NextCheckAt.Equal(*wrong.CooldownUntil) {
					t.Fatalf("request error changed quality verdict or retained backoff: %+v", state)
				}
			})
		}
	}
}

func TestCandyCheckTransientErrorsRetainBackoff(t *testing.T) {
	for _, code := range []int{0, 200, 401, 403, 408, 429, 500, 502, 503, 504} {
		if candyCheckRequestRejected(code) {
			t.Errorf("HTTP %d must retain transport backoff", code)
		}
	}
}
