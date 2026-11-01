package storage

import (
	"testing"
	"time"
)

func TestGatewayAPICallLogsCRUDPaginationAndStats(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayAPICallLogs(db)
	started := time.Unix(1_700_000_000, 0).UTC()
	knownCost := 0.125
	channelID := uint(7)
	groupID := int64(42)
	pending := &GatewayAPICallLog{
		RequestID: "audit-pending", GatewayRequestID: "gateway-request-1", Purpose: GatewayAPICallPurposeGateway,
		Method: "POST", Host: "upstream.example", Endpoint: "/v1/chat/completions?token=redacted",
		Model: "model-a", ChannelID: channelID, SourceGroupID: &groupID,
		SourceAPIKeyName: "key-a", Status: GatewayAPICallStatusPending, StartedAt: started,
	}
	if err := logs.Create(pending); err != nil {
		t.Fatalf("create pending: %v", err)
	}
	if pending.Endpoint != "/v1/chat/completions" {
		t.Fatalf("endpoint retained query: %q", pending.Endpoint)
	}
	var persisted GatewayAPICallLog
	if err := db.Where("request_id = ?", pending.RequestID).First(&persisted).Error; err != nil {
		t.Fatalf("reload pending row: %v", err)
	}
	if persisted.GatewayRequestID != "gateway-request-1" {
		t.Fatalf("gateway_request_id = %q", persisted.GatewayRequestID)
	}
	finished := started.Add(250 * time.Millisecond)
	if err := logs.SaveFinish(&GatewayAPICallLog{
		RequestID: pending.RequestID, Status: GatewayAPICallStatusSuccess,
		StatusCode: 200, UsageKnown: true, InputTokens: 10, OutputTokens: 20,
		CacheReadTokens: 3, CacheCreationTokens: 4, CacheCreation5mTokens: 1, CacheCreation1hTokens: 3,
		ImageOutputTokens: 5, ReasoningTokens: 6, EstimatedCost: &knownCost,
		FinishedAt: &finished, DurationMS: 250,
	}); err != nil {
		t.Fatalf("finish pending: %v", err)
	}
	if err := logs.Create(&GatewayAPICallLog{
		RequestID: "audit-error", Purpose: GatewayAPICallPurposeCandyCheck,
		Method: "POST", Host: "upstream.example", Endpoint: "/v1/messages",
		Model: "model-b", ChannelID: channelID, Status: GatewayAPICallStatusError,
		StartedAt: started.Add(time.Hour), ErrorType: "transport",
	}); err != nil {
		t.Fatalf("create error: %v", err)
	}
	if err := logs.Create(&GatewayAPICallLog{
		RequestID: "audit-pending-2", Purpose: GatewayAPICallPurposeGateway,
		Model: "model-a", ChannelID: channelID, Status: GatewayAPICallStatusPending,
		StartedAt: started.Add(2 * time.Hour),
	}); err != nil {
		t.Fatalf("create second pending: %v", err)
	}

	page, err := logs.List(GatewayAPICallLogQuery{Purpose: GatewayAPICallPurposeGateway, ChannelID: channelID, Model: "model-a", Page: 1, PageSize: 1})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if page.Total != 2 || len(page.Items) != 1 || page.PageSize != 1 {
		t.Fatalf("page = %+v", page)
	}
	if err := db.Where("request_id = ?", pending.RequestID).First(&persisted).Error; err != nil {
		t.Fatalf("reload finished row: %v", err)
	}
	if persisted.CacheCreation5mTokens != 1 || persisted.CacheCreation1hTokens != 3 || persisted.ImageOutputTokens != 5 || persisted.ReasoningTokens != 6 {
		t.Fatalf("finished token details = %+v", persisted)
	}
	stats, err := logs.Stats(GatewayAPICallLogQuery{ChannelID: channelID})
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats.TotalCalls != 3 || stats.SuccessCalls != 1 || stats.ErrorCalls != 1 || stats.PendingCalls != 1 {
		t.Fatalf("status stats = %+v", stats)
	}
	if stats.TotalInputTokens != 10 || stats.TotalOutputTokens != 20 || stats.TotalCacheReadTokens != 3 || stats.TotalCacheCreationTokens != 4 {
		t.Fatalf("token stats = %+v", stats)
	}
	if stats.TotalEstimatedCost == nil || *stats.TotalEstimatedCost != knownCost || stats.UnknownCostCalls != 2 {
		t.Fatalf("cost stats = %+v", stats)
	}
	if stats.UnknownUsageCallsForTest() != 2 {
		t.Fatalf("unknown usage stats = %+v", stats)
	}
}

func (s *GatewayAPICallLogStats) UnknownUsageCallsForTest() int64 {
	return s.UnknownUsageCount
}

func TestGatewayAPICallLogsDeleteBeforeRetainsAtLeast90Days(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayAPICallLogs(db)
	now := time.Now().UTC()
	old := now.Add(-100 * 24 * time.Hour)
	recent := now.Add(-30 * 24 * time.Hour)
	for _, item := range []*GatewayAPICallLog{
		{RequestID: "audit-retention-old", Purpose: GatewayAPICallPurposeGateway, StartedAt: old, CreatedAt: old},
		{RequestID: "audit-retention-recent", Purpose: GatewayAPICallPurposeGateway, StartedAt: recent, CreatedAt: recent},
	} {
		if err := logs.Create(item); err != nil {
			t.Fatalf("create retention row: %v", err)
		}
	}
	deleted, err := logs.DeleteBefore(now)
	if err != nil {
		t.Fatalf("delete before: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}
	var count int64
	if err := db.Model(&GatewayAPICallLog{}).Where("request_id = ?", "audit-retention-recent").Count(&count).Error; err != nil {
		t.Fatalf("count recent row: %v", err)
	}
	if count != 1 {
		t.Fatalf("recent row count = %d, want 1", count)
	}
}

func TestGatewayAPICallLogsUpdateRejectsUnknownFields(t *testing.T) {
	db := openTestDB(t)
	logs := NewGatewayAPICallLogs(db)
	if err := logs.Create(&GatewayAPICallLog{RequestID: "audit-update", Purpose: GatewayAPICallPurposeGateway}); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := logs.UpdateByRequestID("audit-update", map[string]any{"request_id": "other"}); err == nil {
		t.Fatal("update accepted immutable request_id")
	}
	if err := logs.UpdateByRequestID("audit-update", map[string]any{"reasoning_tokens": -1}); err == nil {
		t.Fatal("update accepted negative token detail")
	}
	if err := logs.Create(&GatewayAPICallLog{RequestID: "negative-detail", Purpose: GatewayAPICallPurposeGateway, ImageOutputTokens: -1}); err == nil {
		t.Fatal("create accepted negative token detail")
	}
}
