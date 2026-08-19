package storage

import (
	"sync"
	"testing"
	"time"
)

func TestModelCooldownProbeClaimAndSuccess(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(701, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(701)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	failedAt := time.Now().Add(-time.Minute)
	until := time.Now().Add(-time.Second)
	if err := routes.SetModelTempUnschedulableWithProbeProtocol(routeID, "probe-model", until, "failed", failedAt, "request-1", true, "responses"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}

	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 2, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: err=%v claims=%d", err, len(claims))
	}
	claim := claims[0]
	if claim.ProbeStatus != GatewayModelProbeStatusProbing || claim.ProbeRequestID == "" || claim.ProbeLeaseUntil == nil || claim.ProbeInboundProtocol != "openai_responses" {
		t.Fatalf("invalid claim: %+v", claim)
	}
	second, err := routes.ClaimDueModelCooldownProbes(time.Now(), 2, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if len(second) != 0 {
		t.Fatalf("lease was not exclusive: %#v", second)
	}

	updated, err := routes.MarkModelProbeSuccess(claim, time.Now(), 200)
	if err != nil || !updated {
		t.Fatalf("mark success: updated=%v err=%v", updated, err)
	}
	loaded, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load route: %v", err)
	}
	cooldown := loaded.ModelCooldowns["probe-model"]
	if cooldown.TempUnschedulableUntil != nil || cooldown.ProbeStatus != GatewayModelProbeStatusHealthy || cooldown.LastProbeAt == nil || cooldown.ProbeLastStatusCode != 200 {
		t.Fatalf("success state not persisted: %+v", cooldown)
	}
}

func TestModelCooldownProbeClaimIsAtomic(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(702, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(702)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	if err := routes.SetModelTempUnschedulable(items[0].ID, "probe-model", time.Now().Add(-time.Second), "failed", time.Now().Add(-time.Minute), "request-2"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	claimed := 0
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, claimErr := routes.ClaimDueModelCooldownProbes(time.Now(), 1, time.Minute)
			if claimErr != nil {
				t.Errorf("claim: %v", claimErr)
				return
			}
			if len(got) > 0 {
				mu.Lock()
				claimed += len(got)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if claimed != 1 {
		t.Fatalf("claimed=%d, want exactly one", claimed)
	}
}

func TestModelCooldownProbeFailureKeepsRouteBlocked(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(703, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(703)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	if err := routes.SetModelTempUnschedulable(routeID, "probe-model", time.Now().Add(-time.Second), "failed", time.Now().Add(-time.Minute), "request-3"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim: err=%v claims=%d", err, len(claims))
	}
	next := time.Now().Add(3 * time.Minute)
	updated, err := routes.MarkModelProbeFailure(claims[0], time.Now(), next, 503, "upstream unavailable", false)
	if err != nil || !updated {
		t.Fatalf("mark failure: updated=%v err=%v", updated, err)
	}
	loaded, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load route: %v", err)
	}
	cooldown := loaded.ModelCooldowns["probe-model"]
	if cooldown.TempUnschedulableUntil == nil || !cooldown.TempUnschedulableUntil.Equal(next) || cooldown.NextProbeAt == nil || cooldown.ProbeStatus != GatewayModelProbeStatusTransient || cooldown.ProbeFailureCount != 1 {
		t.Fatalf("failure state not persisted: %+v", cooldown)
	}
}

func TestManualModelCooldownProbeFailureDoesNotArmWorker(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(704, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(704)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	until := time.Now().Add(time.Minute)
	if err := routes.SetModelTempUnschedulableWithProbe(routeID, "probe-model", until, "failed", time.Now(), "request-4", false); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	claim, err := routes.ClaimModelCooldownProbe(routeID, "probe-model", time.Now(), time.Minute)
	if err != nil || claim == nil {
		t.Fatalf("claim manual probe: claim=%+v err=%v", claim, err)
	}
	updated, err := routes.MarkManualModelProbeFailure(*claim, time.Now(), 503, "still unavailable")
	if err != nil || !updated {
		t.Fatalf("mark manual failure: updated=%v err=%v", updated, err)
	}
	loaded, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load route: %v", err)
	}
	cooldown := loaded.ModelCooldowns["probe-model"]
	if cooldown.TempUnschedulableUntil == nil || cooldown.NextProbeAt != nil || cooldown.ProbeLeaseUntil != nil || cooldown.ProbeStatus != GatewayModelProbeStatusManual || cooldown.ProbeFailureCount != 1 {
		t.Fatalf("manual failure armed automatic probing: %+v", cooldown)
	}
}

func TestManualModelCooldownProbeRejectsConcurrentClaim(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(706, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(706)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	if err := routes.SetModelTempUnschedulable(routeID, "probe-model", time.Now().Add(time.Minute), "failed", time.Now(), "request-6"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	if claim, err := routes.ClaimModelCooldownProbe(routeID, "probe-model", time.Now(), time.Minute); err != nil || claim == nil {
		t.Fatalf("first claim: claim=%+v err=%v", claim, err)
	}
	if claim, err := routes.ClaimModelCooldownProbe(routeID, "probe-model", time.Now(), time.Minute); err != nil || claim != nil {
		t.Fatalf("concurrent claim: claim=%+v err=%v", claim, err)
	}
}

func TestConfigureModelCooldownProbesTogglesSchedulingState(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(705, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(705)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	if err := routes.SetModelTempUnschedulable(routeID, "probe-model", time.Now().Add(-time.Second), "failed", time.Now(), "request-5"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	if _, err := routes.ClaimDueModelCooldownProbes(time.Now(), 1, time.Minute); err != nil {
		t.Fatalf("claim probe: %v", err)
	}
	loaded, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load probing state: %v", err)
	}
	probing := loaded.ModelCooldowns["probe-model"]
	if probing.ProbeStatus != GatewayModelProbeStatusProbing || probing.ProbeLeaseUntil == nil || probing.ProbeRequestID == "" {
		t.Fatalf("claim did not create probing state: %+v", probing)
	}
	if err := routes.ConfigureModelCooldownProbes(true, time.Now()); err != nil {
		t.Fatalf("reapply enabled probes: %v", err)
	}
	loaded, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load reapplied state: %v", err)
	}
	reapplied := loaded.ModelCooldowns["probe-model"]
	if reapplied.ProbeStatus != GatewayModelProbeStatusProbing || reapplied.ProbeLeaseUntil == nil || reapplied.ProbeRequestID != probing.ProbeRequestID {
		t.Fatalf("enabled reapply disturbed live probe: before=%+v after=%+v", probing, reapplied)
	}
	if err := routes.ConfigureModelCooldownProbes(false, time.Now()); err != nil {
		t.Fatalf("disable probes: %v", err)
	}
	loaded, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load disabled state: %v", err)
	}
	cooldown := loaded.ModelCooldowns["probe-model"]
	if cooldown.ProbeStatus != GatewayModelProbeStatusProbing || cooldown.NextProbeAt == nil || cooldown.ProbeLeaseUntil == nil || cooldown.ProbeRequestID == "" {
		t.Fatalf("disable interrupted a live probe lease: %+v", cooldown)
	}
	if err := routes.ConfigureModelCooldownProbes(false, cooldown.ProbeLeaseUntil.Add(time.Second)); err != nil {
		t.Fatalf("disable after lease: %v", err)
	}
	loaded, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("reload disabled state: %v", err)
	}
	cooldown = loaded.ModelCooldowns["probe-model"]
	if cooldown.ProbeStatus != GatewayModelProbeStatusManual || cooldown.NextProbeAt != nil || cooldown.ProbeLeaseUntil != nil || cooldown.ProbeRequestID != "" {
		t.Fatalf("disable did not clear expired scheduling state: %+v", cooldown)
	}
	if err := routes.ConfigureModelCooldownProbes(true, time.Now()); err != nil {
		t.Fatalf("enable probes: %v", err)
	}
	loaded, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load enabled state: %v", err)
	}
	cooldown = loaded.ModelCooldowns["probe-model"]
	if cooldown.ProbeStatus != GatewayModelProbeStatusPending || cooldown.NextProbeAt == nil {
		t.Fatalf("enable did not rearm scheduling state: %+v", cooldown)
	}
}

func TestConfigureModelCooldownProbesPreservesLiveLease(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(707, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(707)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	routeID := items[0].ID
	if err := routes.SetModelTempUnschedulableWithProbe(routeID, "probe-model", time.Now().Add(-time.Second), "failed", time.Now(), "request-live", true); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 1, time.Minute)
	if err != nil || len(claims) != 1 {
		t.Fatalf("claim probe: err=%v claims=%d", err, len(claims))
	}
	claim := claims[0]
	if err := routes.ConfigureModelCooldownProbes(false, time.Now()); err != nil {
		t.Fatalf("disable probes during lease: %v", err)
	}
	loaded, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load live lease: %v", err)
	}
	live := loaded.ModelCooldowns["probe-model"]
	if live.ProbeStatus != GatewayModelProbeStatusProbing || live.ProbeRequestID != claim.ProbeRequestID || live.ProbeLeaseUntil == nil {
		t.Fatalf("live lease was overwritten: before=%+v after=%+v", claim, live)
	}
	if err := routes.ConfigureModelCooldownProbes(false, claim.ProbeLeaseUntil.Add(time.Second)); err != nil {
		t.Fatalf("disable probes after lease: %v", err)
	}
	loaded, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load expired lease: %v", err)
	}
	expired := loaded.ModelCooldowns["probe-model"]
	if expired.ProbeStatus != GatewayModelProbeStatusManual || expired.ProbeLeaseUntil != nil || expired.ProbeRequestID != "" || expired.NextProbeAt != nil {
		t.Fatalf("expired lease was not normalized: %+v", expired)
	}
}

func TestClaimDueModelCooldownProbesDoesNotProbeFarFutureLegacyRows(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(708, []GatewayRoute{{SourceChannelID: 1, Enabled: true}}); err != nil {
		t.Fatalf("save route: %v", err)
	}
	items, err := routes.ListByGroupID(708)
	if err != nil || len(items) != 1 {
		t.Fatalf("list route: err=%v items=%d", err, len(items))
	}
	until := time.Now().Add(time.Hour)
	if err := db.Create(&GatewayRouteModelCooldown{
		RouteID: items[0].ID, Model: "legacy-model", TempUnschedulableUntil: &until,
		TempUnschedulableReason: "legacy", ProbeStatus: "",
	}).Error; err != nil {
		t.Fatalf("create legacy cooldown: %v", err)
	}
	claims, err := routes.ClaimDueModelCooldownProbes(time.Now(), 1, time.Minute)
	if err != nil {
		t.Fatalf("claim future legacy row: %v", err)
	}
	if len(claims) != 0 {
		t.Fatalf("far-future legacy row was probed: %+v", claims)
	}
}
