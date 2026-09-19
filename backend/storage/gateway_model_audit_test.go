package storage

import (
	"testing"
	"time"
)

func TestGatewayUsageModelMismatchFilterPreservesScopeAndUnknowns(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayUsageLogs(db)
	matched, mismatched := false, true
	now := time.Now().UTC().Truncate(time.Second)
	rows := []GatewayUsageLog{
		{GatewayGroupID: 7, RequestID: "old-unknown", CreatedAt: now},
		{GatewayGroupID: 7, RequestID: "matched", UpstreamResponseModel: "main", UpstreamModelMismatch: &matched, CreatedAt: now},
		{GatewayGroupID: 7, RequestID: "mismatched", UpstreamResponseModel: "mini", UpstreamModelMismatch: &mismatched, CreatedAt: now},
		{GatewayGroupID: 7, RequestID: "conflicting", UpstreamResponseModel: "main", UpstreamModelMismatch: &matched, UpstreamModelConflict: true, CreatedAt: now},
		{GatewayGroupID: 8, RequestID: "other-group-conflicting", UpstreamModelConflict: true, CreatedAt: now},
		{GatewayGroupID: 7, RequestID: "expired", UpstreamModelMismatch: &mismatched, CreatedAt: now.Add(-2 * time.Hour)},
	}
	for i := range rows {
		if err := logs.Create(&rows[i]); err != nil {
			t.Fatal(err)
		}
	}
	from, to := now.Add(-time.Minute), now.Add(time.Minute)
	q := GatewayUsageQuery{GatewayGroupID: 7, ResultMode: "model_mismatch", From: &from, To: &to}
	page, err := logs.List(q)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 2 {
		t.Fatalf("mismatch filter total = %d, want 2", page.Total)
	}
	for _, row := range page.Items {
		if row.RequestID != "mismatched" && row.RequestID != "conflicting" {
			t.Fatalf("unexpected audit row: %+v", row)
		}
	}
	stats, err := logs.Stats(q)
	if err != nil || stats.TotalRequests != 2 {
		t.Fatalf("filtered stats = %+v, err = %v", stats, err)
	}
	points, err := logs.Timeline(q)
	if err != nil || len(points) != 1 || points[0].Requests != 2 {
		t.Fatalf("filtered timeline = %+v, err = %v", points, err)
	}
	var unknown GatewayUsageLog
	if err := db.First(&unknown, rows[0].ID).Error; err != nil {
		t.Fatal(err)
	}
	if unknown.UpstreamModelMismatch != nil || unknown.UpstreamResponseModel != "" {
		t.Fatalf("unknown audit was turned into a match: %+v", unknown)
	}
}
