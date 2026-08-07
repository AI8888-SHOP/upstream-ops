package scheduler

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/bejix/upstream-ops/backend/config"
	"github.com/bejix/upstream-ops/backend/monitor"
	"github.com/bejix/upstream-ops/backend/storage"
	"gorm.io/gorm"
)

type fakeUpstreamSync struct {
	called int
}

func (f *fakeUpstreamSync) SyncAllOnRateScan(ctx context.Context) {
	f.called++
}

type fakeGatewayResort struct {
	called int
}

func (f *fakeGatewayResort) ResortRoutesOnRateScan(ctx context.Context) {
	f.called++
}

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := storage.Open(storage.DBConfig{
		Driver: storage.DBDriverSQLite,
		Path:   filepath.Join(t.TempDir(), "scheduler-test.db"),
	})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := storage.AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	return db
}

func TestRunRetentionDeletesAnnouncements(t *testing.T) {
	db := openTestDB(t)
	announcements := storage.NewUpstreamAnnouncements(db)
	notifies := storage.NewNotifications(db)
	monLogs := storage.NewMonitorLogs(db)
	rates := storage.NewRates(db)
	syncLogs := storage.NewUpstreamSyncLogs(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	if _, err := announcements.Sync(1, []storage.UpstreamAnnouncement{{
		ChannelID:   1,
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcement: %v", err)
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				AnnouncementsDays: 1,
			},
		},
		&monitor.Service{},
		monLogs,
		nil,
		syncLogs,
		rates,
		notifies,
		announcements,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list announcements: %v", err)
	}
	if total != 0 || len(list) != 0 {
		t.Fatalf("announcements not cleaned: total=%d list=%#v", total, list)
	}
}

func TestRunRetentionDeletesUpstreamSyncLogsWithMonitorLogDays(t *testing.T) {
	db := openTestDB(t)
	monLogs := storage.NewMonitorLogs(db)
	syncLogs := storage.NewUpstreamSyncLogs(db)
	rates := storage.NewRates(db)
	notifies := storage.NewNotifications(db)

	if err := syncLogs.Append(&storage.UpstreamSyncLog{
		SyncGroupID: 1,
		TargetID:    1,
		Action:      "apply",
		Success:     true,
		Message:     "old",
		CreatedAt:   time.Now().AddDate(0, 0, -10),
	}); err != nil {
		t.Fatalf("append old sync log: %v", err)
	}
	if err := syncLogs.Append(&storage.UpstreamSyncLog{
		SyncGroupID: 1,
		TargetID:    1,
		Action:      "apply",
		Success:     true,
		Message:     "new",
		CreatedAt:   time.Now(),
	}); err != nil {
		t.Fatalf("append new sync log: %v", err)
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				MonitorLogsDays: 1,
			},
		},
		&monitor.Service{},
		monLogs,
		nil,
		syncLogs,
		rates,
		notifies,
		nil,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	list, total, err := syncLogs.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list sync logs: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].Message != "new" {
		t.Fatalf("sync logs not cleaned: total=%d list=%#v", total, list)
	}
}

func TestRunRetentionDeletesGatewayUsageLogs(t *testing.T) {
	db := openTestDB(t)
	usage := storage.NewGatewayUsageLogs(db)
	oldCreatedAt := time.Now().AddDate(0, 0, -10)
	newCreatedAt := time.Now()
	for _, item := range []*storage.GatewayUsageLog{
		{RequestID: "old-request", CreatedAt: oldCreatedAt},
		{RequestID: "new-request", CreatedAt: newCreatedAt},
	} {
		if err := usage.Create(item); err != nil {
			t.Fatalf("create usage log: %v", err)
		}
	}

	s := New(
		config.SchedulerConfig{
			Retention: config.RetentionConfig{
				GatewayUsageLogsDays: 1,
			},
		},
		nil,
		nil,
		usage,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		config.ProxyConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRetention()

	page, err := usage.List(storage.GatewayUsageQuery{Page: 1, PageSize: 10})
	if err != nil {
		t.Fatalf("list usage logs: %v", err)
	}
	if page.Total != 1 || len(page.Items) != 1 || page.Items[0].RequestID != "new-request" {
		t.Fatalf("usage logs not cleaned: total=%d items=%#v", page.Total, page.Items)
	}
}

func TestRunRatesTriggersUpstreamSync(t *testing.T) {
	syncSvc := &fakeUpstreamSync{}
	gatewayResort := &fakeGatewayResort{}
	s := New(
		config.SchedulerConfig{},
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		syncSvc,
		gatewayResort,
		config.ProxyConfig{},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)

	s.runRates()

	if syncSvc.called != 1 {
		t.Fatalf("sync calls = %d, want 1", syncSvc.called)
	}
	if gatewayResort.called != 1 {
		t.Fatalf("gateway resort calls = %d, want 1", gatewayResort.called)
	}
}
