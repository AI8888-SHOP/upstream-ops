package storage

import (
	"testing"
	"time"
)

func TestGatewayUsageStatsPreservesZeroSettlementAfterRepricing(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "zero-settlement", KeyHash: "zero-settlement-hash", KeyPrefix: "sk-zero-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatal(err)
	}
	logs := NewGatewayUsageLogs(db)
	for i, originalCost := range []float64{0, 1} {
		requestID := []string{"zero-settlement", "nonzero-settlement"}[i]
		usage := &GatewayUsageLog{
			GatewayKeyID: key.ID, RequestID: requestID, Attempt: 1,
			AttemptKind: GatewayAttemptKindPrimary, AttemptStatus: GatewayAttemptStatusAccepted,
			ActualCost: originalCost, Success: true, CreatedAt: time.Now().UTC(),
		}
		if err := logs.Create(usage); err != nil {
			t.Fatal(err)
		}
		if usage.Winner {
			t.Fatal("unfinalized attempt must not be a settled winner")
		}
		if _, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
			RequestID: requestID, GatewayKeyID: key.ID, Delivered: true,
			WinnerAttempt: 1, WinnerUsageLogID: usage.ID,
		}); err != nil {
			t.Fatal(err)
		}
		// Repricing only changes mutable reference costs, not the settled debit.
		if err := db.Model(&GatewayUsageLog{}).Where("id = ?", usage.ID).Update("actual_cost", float64(2*(i+1))).Error; err != nil {
			t.Fatal(err)
		}
		var settlement GatewayWinnerSettlement
		if err := db.Where("request_id = ?", requestID).First(&settlement).Error; err != nil {
			t.Fatal(err)
		}
		if settlement.BilledCost != originalCost || settlement.ActualCost != originalCost {
			t.Fatalf("immutable settlement changed: %+v", settlement)
		}
	}
	// Retain the old fallback for pre-settlement-format rows and ensure an
	// accepted but unfinalized attempt never contributes to collected revenue.
	for _, usage := range []GatewayUsageLog{
		{RequestID: "legacy-no-snapshot", Winner: true, Success: true, ActualCost: 3},
		{RequestID: "legacy-with-snapshot", Winner: true, Success: true, ActualCost: 1, BilledCost: 0.75},
		{RequestID: "pending-settlement", AttemptStatus: GatewayAttemptStatusAccepted, Success: true, ActualCost: 5},
	} {
		usage.GatewayKeyID, usage.CreatedAt = key.ID, time.Now().UTC()
		if err := logs.Create(&usage); err != nil {
			t.Fatal(err)
		}
	}
	stats, err := logs.Stats(GatewayUsageQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if stats.WinnerCost != 4.75 || stats.TotalUpstreamCost != 15 {
		t.Fatalf("settled=%g reference=%g, want 4.75/15", stats.WinnerCost, stats.TotalUpstreamCost)
	}
	var storedKey GatewayKey
	if err := db.First(&storedKey, key.ID).Error; err != nil {
		t.Fatal(err)
	}
	if storedKey.QuotaUsed != 1 {
		t.Fatalf("quota changed after reference repricing: %g", storedKey.QuotaUsed)
	}
}
