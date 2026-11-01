package storage

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCandySharedLeasePersistsAndFencesWorkers(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	start := make(chan struct{})
	var wg sync.WaitGroup
	leases := make(chan *GatewayCandyCheckSharedLease, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			a, err := NewGatewayRoutes(db).AcquireSharedCandyCheck("source", now, time.Minute, 5*time.Minute, true)
			if err != nil {
				t.Error(err)
				return
			}
			if a.Lease != nil {
				leases <- a.Lease
			}
		}()
	}
	close(start)
	wg.Wait()
	close(leases)
	if len(leases) != 1 {
		t.Fatalf("leases=%d want one", len(leases))
	}
	old := <-leases
	// A restart/new repository sees the same active lease, then takes over only
	// after expiry. The crashed worker's completion cannot replace the new one.
	restarted := NewGatewayRoutes(db)
	if a, err := restarted.AcquireSharedCandyCheck("source", now.Add(time.Second), time.Minute, 5*time.Minute, false); err != nil || !a.InFlight {
		t.Fatalf("restart active lease: %+v %v", a, err)
	}
	a, err := restarted.AcquireSharedCandyCheck("source", now.Add(time.Minute), time.Minute, 5*time.Minute, false)
	if err != nil || a.Lease == nil {
		t.Fatalf("expired lease: %+v %v", a, err)
	}
	if ok, err := routes.FinishSharedCandyCheck(*old, GatewayRouteCandyCheck{Status: "correct"}, now.Add(time.Minute)); err != nil || ok {
		t.Fatalf("stale worker published: %v %v", ok, err)
	}
	observed := now.Add(time.Minute)
	if ok, err := restarted.FinishSharedCandyCheck(*a.Lease, GatewayRouteCandyCheck{Status: "incorrect", Model: "m", SourceRouteID: 7, SourceRequestID: "real-request", ObservedAt: &observed}, observed); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	a, err = NewGatewayRoutes(db).AcquireSharedCandyCheck("source", observed.Add(time.Second), time.Minute, 5*time.Minute, false)
	if err != nil || a.Cached == nil || a.Cached.SourceRequestID != "real-request" || a.Cached.Status != "incorrect" {
		t.Fatalf("persistent result: %+v %v", a, err)
	}
}

func TestCandySharedFreshnessUsesConsumerInterval(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	a, err := routes.AcquireSharedCandyCheck("freshness", now, time.Minute, 5*time.Minute, false)
	if err != nil || a.Lease == nil {
		t.Fatalf("claim: %+v %v", a, err)
	}
	if ok, err := routes.FinishSharedCandyCheck(*a.Lease, GatewayRouteCandyCheck{Status: "correct", ObservedAt: &now, SourceRequestID: "once"}, now); err != nil || !ok {
		t.Fatalf("finish: %v %v", ok, err)
	}
	for _, age := range []time.Duration{90 * time.Second, 3 * time.Minute, 6 * time.Minute, 11 * time.Minute} {
		for _, interval := range []time.Duration{2 * time.Minute, 5 * time.Minute, 10 * time.Minute} {
			t.Run(fmt.Sprintf("age-%s-interval-%s", age, interval), func(t *testing.T) {
				a, err := NewGatewayRoutes(db).AcquireSharedCandyCheck("freshness", now.Add(age), time.Minute, interval, false)
				if err != nil {
					t.Fatal(err)
				}
				if age < interval {
					if a.Cached == nil || !a.Cached.ObservedAt.Equal(now) {
						t.Fatalf("lost original observation: %+v", a)
					}
				} else {
					if a.Cached != nil || a.Lease == nil {
						t.Fatalf("expired cache reused: %+v", a)
					}
					if _, err := routes.ReleaseSharedCandyCheck(*a.Lease); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
	// Force ignores a completed fresh cache but merges with any active lease.
	a, err = routes.AcquireSharedCandyCheck("freshness", now.Add(time.Second), time.Minute, 10*time.Minute, true)
	if err != nil || a.Lease == nil || a.Cached != nil {
		t.Fatalf("manual refresh: %+v %v", a, err)
	}
	if b, err := routes.AcquireSharedCandyCheck("freshness", now.Add(2*time.Second), time.Minute, 10*time.Minute, true); err != nil || !b.InFlight {
		t.Fatalf("manual in-flight merge: %+v %v", b, err)
	}
}

func TestCandySharedReleaseAndErrorsNeverBecomeQualityCache(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	now := time.Now().UTC().Truncate(time.Microsecond)
	a, err := routes.AcquireSharedCandyCheck("released", now, time.Minute, time.Minute, true)
	if err != nil || a.Lease == nil {
		t.Fatalf("claim: %+v %v", a, err)
	}
	if _, err := routes.ReleaseSharedCandyCheck(*a.Lease); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	row, err := routes.WaitForSharedCandyCheck(ctx, "released", time.Millisecond)
	if err != nil || candySharedResultUsable(row, now, time.Minute) {
		t.Fatalf("pending accepted: %+v %v", row, err)
	}
	a, err = routes.AcquireSharedCandyCheck("released", now, time.Minute, time.Minute, false)
	if err != nil || a.Lease == nil {
		t.Fatalf("released pending not retried: %+v %v", a, err)
	}
	if ok, err := routes.FinishSharedCandyCheck(*a.Lease, GatewayRouteCandyCheck{Status: "error", SourceRequestID: "transport-error"}, now); err != nil || !ok {
		t.Fatalf("error finish: %v %v", ok, err)
	}
	if a, err := routes.AcquireSharedCandyCheck("released", now.Add(time.Second), time.Minute, time.Minute, false); err != nil || a.Cached == nil || a.Cached.Status != "error" {
		t.Fatalf("short error suppression: %+v %v", a, err)
	}
	if a, err := routes.AcquireSharedCandyCheck("released", now.Add(15*time.Second), time.Minute, time.Minute, false); err != nil || a.Cached != nil || a.Lease == nil {
		t.Fatalf("error was cached as quality: %+v %v", a, err)
	}
}

func TestCandyReusedResultKeepsObservationAndGroupCooldown(t *testing.T) {
	db := openTestDB(t)
	groups, routes := NewGatewayGroups(db), NewGatewayRoutes(db)
	observed := time.Now().UTC().Truncate(time.Microsecond)
	applied := observed.Add(90 * time.Second)
	for _, interval := range []int{2, 5, 10} {
		g := &GatewayGroup{Name: fmt.Sprintf("fresh-%d", interval), GatewayCandyCheckPolicy: GatewayCandyCheckPolicy{CandyCheckEnabled: true, CandyCheckModel: "m", CandyCheckIntervalMinutes: interval, CandyCheckCooldownMinutes: interval + 1}}
		if err := groups.Create(g); err != nil {
			t.Fatal(err)
		}
		if err := routes.SaveForGroup(g.ID, []GatewayRoute{{Enabled: true, SourceChannelID: 1}}); err != nil {
			t.Fatal(err)
		}
		list, err := routes.ListByGroupID(g.ID)
		if err != nil {
			t.Fatal(err)
		}
		key := GatewayCandyCheckConfigKey(g, &list[0], nil)
		claim, err := routes.ClaimCandyCheck(list[0].ID, key, "m", applied, time.Minute)
		if err != nil || claim == nil {
			t.Fatalf("claim: %v", err)
		}
		result := GatewayRouteCandyCheck{Status: "incorrect", Reused: true, SourceRouteID: 99, SourceRequestID: "upstream-once", ObservedAt: &observed}
		if ok, err := routes.FinishCandyCheck(*claim, result, applied); err != nil || !ok {
			t.Fatalf("finish: %v %v", ok, err)
		}
		got, err := routes.FindByID(list[0].ID)
		if err != nil {
			t.Fatal(err)
		}
		state := got.CandyCheck
		if !state.Reused || !state.ObservedAt.Equal(observed) || !state.CheckedAt.Equal(applied) || !state.CooldownUntil.Equal(observed.Add(time.Duration(interval+1)*time.Minute)) || !state.NextCheckAt.Equal(*state.CooldownUntil) {
			t.Fatalf("observation/cooldown renewed: %+v", state)
		}
		history, err := routes.CandyCheckHistory(list[0].ID)
		if err != nil || len(history) != 1 || !history[0].Reused || history[0].SourceRequestID != "upstream-once" {
			t.Fatalf("reuse history: %+v %v", history, err)
		}
	}
}
