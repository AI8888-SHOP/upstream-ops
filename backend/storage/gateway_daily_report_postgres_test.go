package storage

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestGatewayDailyReportPostgres(t *testing.T) {
	dsn := os.Getenv("UPSTREAM_OPS_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("PostgreSQL integration DSN not configured")
	}
	admin, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	adminSQL, err := admin.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer adminSQL.Close()
	schema := fmt.Sprintf("daily_report_test_%d", time.Now().UnixNano())
	if err := admin.Exec("CREATE SCHEMA " + schema).Error; err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := admin.Exec("DROP SCHEMA " + schema + " CASCADE").Error; err != nil {
			t.Error(err)
		}
	}()
	var scopedDSN string
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		if err != nil {
			t.Fatal(err)
		}
		q := u.Query()
		q.Set("search_path", schema)
		u.RawQuery = q.Encode()
		scopedDSN = u.String()
	} else {
		scopedDSN = dsn + " search_path=" + schema
	}
	db, err := gorm.Open(postgres.Open(scopedDSN), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer sqlDB.Close()
	if err := db.AutoMigrate(&GatewayUsageLog{}, &Channel{}, &GatewayProvider{}); err != nil {
		t.Fatal(err)
	}
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	today := time.Date(2026, 10, 21, 0, 0, 0, 0, loc)
	group := int64(41)
	rows := []GatewayUsageLog{
		{ChannelID: 11, SourceGroupID: &group, SourceGroupName: "before", RequestID: "previous", RequestedModel: "model", ActualCost: 9, CreatedAt: today.Add(-time.Second).UTC()},
		{ChannelID: 11, SourceGroupID: &group, SourceGroupName: "before", RequestID: "retry", RequestedModel: "model", ActualCost: 2, InputTokens: 10, CacheReadTokens: 90, CreatedAt: today.UTC()},
		{ChannelID: 11, SourceGroupID: &group, SourceGroupName: "renamed", RequestID: "retry", RequestedModel: "model", ActualCost: 3, Winner: true, Success: true, InputTokens: 50, CacheReadTokens: 50, VirtualCacheReadTokens: 50, OutputTokens: 20, CreatedAt: today.Add(time.Second).UTC()},
		{ChannelID: 12, SourceGroupID: &group, SourceGroupName: "renamed", RequestID: "other", RequestedModel: "model", ActualCost: 1, Winner: true, Success: true, InputTokens: 100, CreatedAt: today.Add(2 * time.Second).UTC()},
		{ChannelID: 11, SourceGroupID: &group, SourceGroupName: "future", RequestID: "future", RequestedModel: "model", ActualCost: 100, CreatedAt: today.AddDate(0, 0, 1).UTC()},
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := NewGatewayUsageLogs(db).DailyReport(context.Background(), today)
	if err != nil {
		t.Fatal(err)
	}
	if report.Today.Cost != 6 || report.Today.Requests != 2 || report.Today.Attempts != 3 || report.Today.Tokens != 320 || report.Today.SuccessfulRequests != 2 {
		t.Fatalf("today=%+v", report.Today)
	}
	if report.Today.CacheHitRate == nil || *report.Today.CacheHitRate != 25 || report.Yesterday.Cost != 9 {
		t.Fatalf("cache/yesterday=%+v", report)
	}
	if len(report.Today.Sources) != 2 || len(report.Today.Sources[0].Groups) != 1 || report.Today.Sources[0].Groups[0].SourceGroupName != "renamed" {
		t.Fatalf("source identity=%+v", report.Today.Sources)
	}
}
