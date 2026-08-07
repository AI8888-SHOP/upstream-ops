package storage

import (
	"fmt"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestPostgresDSNNormalizesIPv6AndSetsTimeout(t *testing.T) {
	dsn := (DBConfig{
		Driver: DBDriverPostgres, Host: "[::1]", User: "user", Password: "p@ss",
		Name: "upstreamops", ConnectTimeoutSeconds: 7,
	}).PostgresDSN()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse postgres dsn: %v", err)
	}
	if parsed.Host != "[::1]:5432" {
		t.Fatalf("postgres host = %q, want [::1]:5432", parsed.Host)
	}
	if got := parsed.Query().Get("connect_timeout"); got != "7" {
		t.Fatalf("connect_timeout = %q, want 7", got)
	}
	if got, _ := parsed.User.Password(); got != "p@ss" {
		t.Fatalf("password = %q, want p@ss", got)
	}
}

func TestSQLiteReadOnlyWindowsDriveURI(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows URI syntax")
	}
	dsn := (DBConfig{Driver: DBDriverSQLite, Path: `C:\data\upstream-ops.db`, ReadOnly: true}).SQLiteDSN()
	if !strings.HasPrefix(dsn, "file:///C:/") {
		t.Fatalf("Windows SQLite DSN = %q, want file:///C:/ prefix", dsn)
	}
}

func openTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := Open(DBConfig{
		Driver:       DBDriverSQLite,
		Path:         filepath.Join(t.TempDir(), "test.db"),
		MaxOpenConns: 20,
		MaxIdleConns: 5,
	})
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get sql.DB: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })

	return db
}

func TestOpenSQLiteReadOnlyDoesNotWriteDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "readonly.db")
	db, err := Open(DBConfig{Driver: DBDriverSQLite, Path: path})
	if err != nil {
		t.Fatalf("open writable db: %v", err)
	}
	if err := db.Exec("CREATE TABLE readonly_probe (id INTEGER PRIMARY KEY, value TEXT)").Error; err != nil {
		t.Fatalf("create probe: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("get writable sql db: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("close writable db: %v", err)
	}

	readOnly, err := Open(DBConfig{Driver: DBDriverSQLite, Path: path, ReadOnly: true})
	if err != nil {
		t.Fatalf("open read-only db: %v", err)
	}
	readOnlySQL, err := readOnly.DB()
	if err != nil {
		t.Fatalf("get read-only sql db: %v", err)
	}
	t.Cleanup(func() { _ = readOnlySQL.Close() })
	var count int64
	if err := readOnly.Table("readonly_probe").Count(&count).Error; err != nil {
		t.Fatalf("query read-only db: %v", err)
	}
	if count != 0 {
		t.Fatalf("probe count = %d, want 0", count)
	}
	if err := readOnly.Exec("CREATE TABLE should_fail (id INTEGER PRIMARY KEY)").Error; err == nil {
		t.Fatal("read-only SQLite unexpectedly accepted a schema write")
	}
}

func TestGatewayStatsCacheKeyKeepsUserFiltersDistinct(t *testing.T) {
	first := gatewayStatsCacheKey(GatewayUsageQuery{Model: "model|request", RequestID: "id"})
	second := gatewayStatsCacheKey(GatewayUsageQuery{Model: "model", RequestID: "request|id"})
	if first == second {
		t.Fatalf("cache keys collided: %#v", first)
	}
	from := time.Unix(0, 0)
	withoutFrom := gatewayStatsCacheKey(GatewayUsageQuery{})
	withFrom := gatewayStatsCacheKey(GatewayUsageQuery{From: &from})
	if withoutFrom == withFrom {
		t.Fatalf("nil and epoch From filters collided: %#v", withFrom)
	}
}

func TestAggregateBalanceTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day2.Add(12 * time.Hour)},
		{ChannelID: 2, Balance: 5, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, Balance: 7, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, Balance: 3, SampledAt: day1.Add(18 * time.Hour)},
		{ChannelID: 2, Balance: 9, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, Balance: 11, SampledAt: day0.Add(22 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: 25},
		{Day: day1, Balance: 10},
		{Day: day0, Balance: 11},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Balance != want[i].Balance {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestChannelProxyEnabledPersists(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)
	ch := &Channel{
		Name:           "proxy-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		ProxyEnabled:   true,
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	got, err := channels.FindByID(ch.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if !got.ProxyEnabled {
		t.Fatal("proxy_enabled = false, want true")
	}
}

func TestUpstreamSyncAccountUpdateKeepsCreatedAt(t *testing.T) {
	db := openTestDB(t)
	accounts := NewUpstreamSyncAccounts(db)
	sourceGroupID := int64(21)
	if err := accounts.SaveForGroup(2, []UpstreamSyncAccount{{
		SourceChannelID:  3,
		SourceGroupID:    &sourceGroupID,
		Concurrency:      10,
		Weight:           1,
		RateConvertMode:  "raw",
		RateConvertValue: 1,
		Enabled:          true,
	}}); err != nil {
		t.Fatalf("create sync account: %v", err)
	}
	created, err := accounts.ListBySyncGroupID(2)
	if err != nil {
		t.Fatalf("list sync accounts: %v", err)
	}
	if len(created) != 1 || created[0].CreatedAt.IsZero() {
		t.Fatalf("created account = %#v", created)
	}

	account := UpstreamSyncAccount{
		ID:               created[0].ID,
		SourceChannelID:  3,
		SourceGroupID:    &sourceGroupID,
		Concurrency:      20,
		Weight:           2,
		RateConvertMode:  "raw",
		RateConvertValue: 1,
		Enabled:          true,
		TestEnabled:      true,
		TestModel:        "gpt-b",
	}
	if err := accounts.SaveForGroup(2, []UpstreamSyncAccount{account}); err != nil {
		t.Fatalf("update sync account: %v", err)
	}
	updated, err := accounts.ListBySyncGroupID(2)
	if err != nil {
		t.Fatalf("list updated sync accounts: %v", err)
	}
	if len(updated) != 1 || !updated[0].CreatedAt.Equal(created[0].CreatedAt) {
		t.Fatalf("created_at changed: before=%s after=%s", created[0].CreatedAt, updated[0].CreatedAt)
	}
	if !updated[0].TestEnabled {
		t.Fatalf("test_enabled = false, want true")
	}
	if updated[0].TestModel != "gpt-b" {
		t.Fatalf("test_model = %q, want gpt-b", updated[0].TestModel)
	}
}

func TestUpstreamSyncTargetGroupUpsertKeepsCreatedAt(t *testing.T) {
	db := openTestDB(t)
	groups := NewUpstreamSyncTargetGroups(db)
	lastSync := time.Now()
	if err := groups.Upsert(&UpstreamSyncTargetGroup{
		TargetID:      1,
		RemoteGroupID: 101,
		Name:          "old",
		Platform:      "openai",
		Ratio:         0.06,
		Status:        "active",
		LastSyncAt:    &lastSync,
	}); err != nil {
		t.Fatalf("create target group: %v", err)
	}
	created, err := groups.FindByTargetAndRemote(1, 101)
	if err != nil {
		t.Fatalf("find target group: %v", err)
	}

	if err := groups.Upsert(&UpstreamSyncTargetGroup{
		TargetID:      1,
		RemoteGroupID: 101,
		Name:          "new",
		Platform:      "openai",
		Ratio:         0.065,
		Status:        "active",
		LastSyncAt:    &lastSync,
	}); err != nil {
		t.Fatalf("update target group: %v", err)
	}
	updated, err := groups.FindByTargetAndRemote(1, 101)
	if err != nil {
		t.Fatalf("find updated target group: %v", err)
	}
	if !updated.CreatedAt.Equal(created.CreatedAt) || updated.Name != "new" || updated.Ratio != 0.065 {
		t.Fatalf("updated target group = %#v, created_at before=%s", updated, created.CreatedAt)
	}
}

func TestUpstreamSyncManagedAccountUpsertKeepsCreatedAt(t *testing.T) {
	db := openTestDB(t)
	managed := NewUpstreamSyncManagedAccounts(db)
	appliedAt := time.Now()
	if err := managed.Upsert(&UpstreamSyncManagedAccount{
		SyncGroupID:        1,
		SyncAccountID:      2,
		SourceAPIKeyID:     10,
		SourceAPIKeyName:   "key",
		TargetAccountID:    20,
		TargetAccountName:  "old",
		TargetGroupIDsJSON: "[1]",
		LastAppliedAt:      &appliedAt,
	}); err != nil {
		t.Fatalf("create managed account: %v", err)
	}
	created, err := managed.FindByAccountID(2)
	if err != nil {
		t.Fatalf("find managed account: %v", err)
	}

	if err := managed.Upsert(&UpstreamSyncManagedAccount{
		SyncGroupID:        1,
		SyncAccountID:      2,
		SourceAPIKeyID:     0,
		SourceAPIKeyName:   "",
		TargetAccountID:    21,
		TargetAccountName:  "new",
		TargetGroupIDsJSON: "[2]",
		LastAppliedAt:      &appliedAt,
	}); err != nil {
		t.Fatalf("update managed account: %v", err)
	}
	updated, err := managed.FindByAccountID(2)
	if err != nil {
		t.Fatalf("find updated managed account: %v", err)
	}
	if !updated.CreatedAt.Equal(created.CreatedAt) || updated.SourceAPIKeyID != 0 || updated.TargetAccountName != "new" {
		t.Fatalf("updated managed account = %#v, created_at before=%s", updated, created.CreatedAt)
	}
}

func TestProxyEnabledPersistsForCaptchaAndNotification(t *testing.T) {
	db := openTestDB(t)

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver-proxy",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	gotCaptcha, err := captchas.FindByID(cfg.ID)
	if err != nil {
		t.Fatalf("find captcha: %v", err)
	}
	if !gotCaptcha.ProxyEnabled {
		t.Fatal("captcha proxy_enabled = false, want true")
	}

	notifies := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "notify-proxy",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
		ProxyEnabled: true,
	}
	if err := notifies.CreateChannel(notify); err != nil {
		t.Fatalf("create notification: %v", err)
	}
	gotNotify, err := notifies.FindChannel(notify.ID)
	if err != nil {
		t.Fatalf("find notification: %v", err)
	}
	if !gotNotify.ProxyEnabled {
		t.Fatal("notification proxy_enabled = false, want true")
	}
}

func TestAggregateBalanceTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, Balance: 20, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	got, err := rates.AggregateBalanceTrend(3)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyAggregate{
		{Day: day2, Balance: 10},
		{Day: day1, Balance: 0},
		{Day: day0, Balance: 20},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Balance != want[i].Balance {
			t.Fatalf("balance %d mismatch: got %v want %v", i, got[i].Balance, want[i].Balance)
		}
	}
}

func TestAggregateCostTrend(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.1, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.2, SampledAt: day2.Add(18 * time.Hour)},
		{ChannelID: 2, TodayCost: 0.8, SampledAt: day2.Add(10 * time.Hour)},
		{ChannelID: 1, TodayCost: 3.5, SampledAt: day1.Add(11 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.2, SampledAt: day1.Add(13 * time.Hour)},
		{ChannelID: 2, TodayCost: 1.8, SampledAt: day1.Add(22 * time.Hour)},
		{ChannelID: 1, TodayCost: 4.0, SampledAt: day0.Add(8 * time.Hour)},
		{ChannelID: 2, TodayCost: 2.5, SampledAt: day0.Add(21 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: 3.0},
		{Day: day1, Cost: 5.3},
		{Day: day0, Cost: 6.5},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Cost != want[i].Cost {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestAggregateCostTrendFillsMissingDays(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now().In(trendLocation)
	day0 := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)
	day2 := day0.AddDate(0, 0, -2)

	snapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: day2.Add(9 * time.Hour)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: day0.Add(12 * time.Hour)},
	}
	for _, snapshot := range snapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	got, err := rates.AggregateCostTrend(3)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 days, got %d", len(got))
	}

	want := []DailyCostAggregate{
		{Day: day2, Cost: 1.5},
		{Day: day1, Cost: 0},
		{Day: day0, Cost: 2.5},
	}
	for i := range want {
		if !got[i].Day.Equal(want[i].Day) {
			t.Fatalf("day %d mismatch: got %s want %s", i, got[i].Day, want[i].Day)
		}
		if got[i].Cost != want[i].Cost {
			t.Fatalf("cost %d mismatch: got %v want %v", i, got[i].Cost, want[i].Cost)
		}
	}
}

func TestAggregateTrendUsesShanghaiDayBoundary(t *testing.T) {
	oldNow := trendNow
	trendNow = func() time.Time {
		return time.Date(2026, 6, 19, 16, 30, 0, 0, time.UTC)
	}
	t.Cleanup(func() { trendNow = oldNow })

	db := openTestDB(t)
	rates := NewRates(db)

	day0 := time.Date(2026, 6, 20, 0, 0, 0, 0, trendLocation)
	day1 := day0.AddDate(0, 0, -1)

	balanceSnapshots := []BalanceSnapshot{
		{ChannelID: 1, Balance: 10, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, Balance: 20, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range balanceSnapshots {
		snapshot := snapshot
		if err := rates.AppendBalance(&snapshot); err != nil {
			t.Fatalf("append balance: %v", err)
		}
	}

	costSnapshots := []CostSnapshot{
		{ChannelID: 1, TodayCost: 1.5, SampledAt: time.Date(2026, 6, 19, 15, 59, 0, 0, time.UTC)},
		{ChannelID: 1, TodayCost: 2.5, SampledAt: time.Date(2026, 6, 19, 16, 1, 0, 0, time.UTC)},
	}
	for _, snapshot := range costSnapshots {
		snapshot := snapshot
		if err := rates.AppendCost(&snapshot); err != nil {
			t.Fatalf("append cost: %v", err)
		}
	}

	balances, err := rates.AggregateBalanceTrend(2)
	if err != nil {
		t.Fatalf("aggregate balance trend: %v", err)
	}
	if len(balances) != 2 {
		t.Fatalf("balance days = %d, want 2", len(balances))
	}
	if !balances[0].Day.Equal(day1) || balances[0].Balance != 10 {
		t.Fatalf("previous shanghai day = %#v, want day %s balance 10", balances[0], day1)
	}
	if !balances[1].Day.Equal(day0) || balances[1].Balance != 20 {
		t.Fatalf("current shanghai day = %#v, want day %s balance 20", balances[1], day0)
	}

	costs, err := rates.AggregateCostTrend(2)
	if err != nil {
		t.Fatalf("aggregate cost trend: %v", err)
	}
	if len(costs) != 2 {
		t.Fatalf("cost days = %d, want 2", len(costs))
	}
	if !costs[0].Day.Equal(day1) || costs[0].Cost != 1.5 {
		t.Fatalf("previous shanghai day cost = %#v, want day %s cost 1.5", costs[0], day1)
	}
	if !costs[1].Day.Equal(day0) || costs[1].Cost != 2.5 {
		t.Fatalf("current shanghai day cost = %#v, want day %s cost 2.5", costs[1], day0)
	}
}

func TestDeleteCostSnapshotsBefore(t *testing.T) {
	db := openTestDB(t)
	rates := NewRates(db)

	now := time.Now()
	oldSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 1.2, SampledAt: now.AddDate(0, 0, -10)}
	newSnapshot := CostSnapshot{ChannelID: 1, TodayCost: 2.3, SampledAt: now.AddDate(0, 0, -2)}
	if err := rates.AppendCost(&oldSnapshot); err != nil {
		t.Fatalf("append old cost: %v", err)
	}
	if err := rates.AppendCost(&newSnapshot); err != nil {
		t.Fatalf("append new cost: %v", err)
	}

	deleted, err := rates.DeleteCostSnapshotsBefore(now.AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete cost snapshots: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1", deleted)
	}

	var count int64
	if err := db.Model(&CostSnapshot{}).Count(&count).Error; err != nil {
		t.Fatalf("count cost snapshots: %v", err)
	}
	if count != 1 {
		t.Fatalf("remaining count = %d, want 1", count)
	}
}

func TestTryClaimCooldown(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	ok, err := notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if !ok {
		t.Fatal("first claim should succeed")
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if ok {
		t.Fatal("second claim should be blocked by cooldown")
	}

	oldTime := time.Now().Add(-2 * time.Minute)
	if err := db.Model(&NotificationCooldown{}).
		Where("channel_id = ? AND event = ?", 1, EventBalanceLow).
		Updates(map[string]any{
			"last_sent_at": oldTime,
			"updated_at":   oldTime,
		}).Error; err != nil {
		t.Fatalf("age cooldown: %v", err)
	}

	ok, err = notifications.TryClaimCooldown(1, EventBalanceLow, time.Minute)
	if err != nil {
		t.Fatalf("third claim: %v", err)
	}
	if !ok {
		t.Fatal("third claim should succeed after cooldown expires")
	}
}

func TestTryClaimCooldownConcurrent(t *testing.T) {
	db := openTestDB(t)
	notifications := NewNotifications(db)

	var claimed int32
	var wg sync.WaitGroup

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ok, err := notifications.TryClaimCooldown(2, EventBalanceLow, time.Minute)
			if err != nil {
				t.Errorf("concurrent claim: %v", err)
				return
			}
			if ok {
				atomic.AddInt32(&claimed, 1)
			}
		}()
	}
	wg.Wait()

	if claimed != 1 {
		t.Fatalf("expected exactly one successful claim, got %d", claimed)
	}
}

func TestUpstreamAnnouncementsSyncDedupes(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	items := []UpstreamAnnouncement{
		{SourceKey: "a", Title: "A", Content: "one", FirstSeenAt: now},
		{SourceKey: "a", Title: "A2", Content: "dup", FirstSeenAt: now.Add(time.Second)},
	}
	newItems, err := announcements.Sync(1, items)
	if err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if len(newItems) != 1 {
		t.Fatalf("new items = %d, want 1", len(newItems))
	}

	exists, err := announcements.Exists(1, "a")
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if !exists {
		t.Fatal("expected announcement to exist")
	}
}

func TestUpstreamAnnouncementsListLatest(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	publishedOld := now.Add(-3 * time.Hour)
	publishedNew := now.Add(-1 * time.Hour)
	items := []UpstreamAnnouncement{
		{ChannelID: 1, SourceKey: "a", Content: "body", PublishedAt: &publishedOld, FirstSeenAt: now.Add(3 * time.Minute)},
		{ChannelID: 1, SourceKey: "b", Content: "body", PublishedAt: &publishedNew, FirstSeenAt: now.Add(1 * time.Minute)},
		{ChannelID: 1, SourceKey: "c", Content: "body", FirstSeenAt: now.Add(4 * time.Minute)},
	}
	for _, item := range items {
		item := item
		if err := db.Create(&item).Error; err != nil {
			t.Fatalf("create announcement: %v", err)
		}
	}

	list, err := announcements.ListLatest(2)
	if err != nil {
		t.Fatalf("list latest: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("list len = %d, want 2", len(list))
	}
	if list[0].SourceKey != "b" || list[1].SourceKey != "a" {
		t.Fatalf("unexpected order: %#v", list)
	}
}

func TestUpstreamAnnouncementsDeleteByChannel(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	now := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "a",
		Content:     "one",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(2, []UpstreamAnnouncement{{
		SourceKey:   "b",
		Content:     "two",
		FirstSeenAt: now,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteByChannel(1)
	if err != nil {
		t.Fatalf("delete by channel: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ChannelID != 2 {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpstreamAnnouncementsDeleteBefore(t *testing.T) {
	db := openTestDB(t)
	announcements := NewUpstreamAnnouncements(db)

	oldTime := time.Now().AddDate(0, 0, -10)
	newTime := time.Now()
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "old",
		Content:     "old",
		FirstSeenAt: oldTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}
	if _, err := announcements.Sync(1, []UpstreamAnnouncement{{
		SourceKey:   "new",
		Content:     "new",
		FirstSeenAt: newTime,
	}}); err != nil {
		t.Fatalf("sync announcements: %v", err)
	}

	rows, err := announcements.DeleteBefore(time.Now().AddDate(0, 0, -5))
	if err != nil {
		t.Fatalf("delete before: %v", err)
	}
	if rows != 1 {
		t.Fatalf("rows = %d, want 1", rows)
	}
	list, total, err := announcements.ListPage(1, 10)
	if err != nil {
		t.Fatalf("list page: %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].SourceKey != "new" {
		t.Fatalf("unexpected remaining announcements: total=%d list=%#v", total, list)
	}
}

func TestUpdateCosts(t *testing.T) {
	db := openTestDB(t)
	channels := NewChannels(db)

	c := &Channel{
		Name:           "test",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(c); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	if err := channels.UpdateCosts(c.ID, 1.23, 9.87); err != nil {
		t.Fatalf("update costs: %v", err)
	}

	got, err := channels.FindByID(c.ID)
	if err != nil {
		t.Fatalf("find channel: %v", err)
	}
	if got.TodayCost == nil || *got.TodayCost != 1.23 {
		t.Fatalf("today cost mismatch: %#v", got.TodayCost)
	}
	if got.TotalCost == nil || *got.TotalCost != 9.87 {
		t.Fatalf("total cost mismatch: %#v", got.TotalCost)
	}
}

func TestHardDeleteAllowsReusingNames(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := channels.Delete(ch.ID); err != nil {
		t.Fatalf("delete channel: %v", err)
	}
	ch = &Channel{
		Name:           "demo",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("recreate channel: %v", err)
	}

	captchas := NewCaptchas(db)
	cfg := &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("create captcha: %v", err)
	}
	if err := captchas.Delete(cfg.ID); err != nil {
		t.Fatalf("delete captcha: %v", err)
	}
	cfg = &CaptchaConfig{
		Name:         "solver",
		Type:         CaptchaCapSolver,
		APIKeyCipher: "x",
		Enabled:      true,
	}
	if err := captchas.Create(cfg); err != nil {
		t.Fatalf("recreate captcha: %v", err)
	}

	notifications := NewNotifications(db)
	notify := &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("create notification channel: %v", err)
	}
	if err := notifications.DeleteChannel(notify.ID); err != nil {
		t.Fatalf("delete notification channel: %v", err)
	}
	notify = &NotificationChannel{
		Name:         "telegram",
		Type:         NotifyTelegram,
		ConfigCipher: "x",
		Enabled:      true,
	}
	if err := notifications.CreateChannel(notify); err != nil {
		t.Fatalf("recreate notification channel: %v", err)
	}
}

func TestDeleteChannelCleansScopedState(t *testing.T) {
	db := openTestDB(t)

	channels := NewChannels(db)
	ch := &Channel{
		Name:           "demo",
		Type:           ChannelTypeSub2API,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := channels.Create(ch); err != nil {
		t.Fatalf("create channel: %v", err)
	}

	now := time.Now()
	if err := db.Create(&AuthSession{ChannelID: ch.ID}).Error; err != nil {
		t.Fatalf("create auth session: %v", err)
	}
	if err := db.Create(&RateSnapshot{ChannelID: ch.ID, ModelName: "old", Ratio: 1, LastSeenAt: now}).Error; err != nil {
		t.Fatalf("create rate snapshot: %v", err)
	}
	if err := db.Create(&RateChangeLog{ChannelID: ch.ID, ModelName: "old", NewRatio: 1, ChangedAt: now}).Error; err != nil {
		t.Fatalf("create rate change: %v", err)
	}
	if err := db.Create(&BalanceSnapshot{ChannelID: ch.ID, Balance: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create balance snapshot: %v", err)
	}
	if err := db.Create(&CostSnapshot{ChannelID: ch.ID, TodayCost: 1, SampledAt: now}).Error; err != nil {
		t.Fatalf("create cost snapshot: %v", err)
	}
	if err := db.Create(&MonitorLog{ChannelID: ch.ID, Job: MonitorJobBalance, Success: true, StartedAt: now, FinishedAt: now}).Error; err != nil {
		t.Fatalf("create monitor log: %v", err)
	}
	if err := db.Create(&NotificationCooldown{ChannelID: ch.ID, Event: EventBalanceLow, LastSentAt: now}).Error; err != nil {
		t.Fatalf("create cooldown: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, UpstreamChannelID: ch.ID, Event: EventBalanceLow, Subject: "alert", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create notification log: %v", err)
	}
	if err := db.Create(&NotificationLog{ChannelID: 99, Event: EventBalanceLow, Subject: "demo 余额低于阈值", Success: true, SentAt: now}).Error; err != nil {
		t.Fatalf("create legacy notification log: %v", err)
	}
	if err := db.Create(&UpstreamAnnouncement{ChannelID: ch.ID, SourceKey: "a", Content: "deleted", FirstSeenAt: now}).Error; err != nil {
		t.Fatalf("create announcement: %v", err)
	}

	if err := channels.Delete(ch.ID); err != nil {
		t.Fatalf("delete channel: %v", err)
	}

	for _, tt := range []struct {
		name  string
		model any
	}{
		{"auth_sessions", &AuthSession{}},
		{"rate_snapshots", &RateSnapshot{}},
		{"rate_change_logs", &RateChangeLog{}},
		{"balance_snapshots", &BalanceSnapshot{}},
		{"cost_snapshots", &CostSnapshot{}},
		{"monitor_logs", &MonitorLog{}},
		{"notification_cooldowns", &NotificationCooldown{}},
		{"upstream_announcements", &UpstreamAnnouncement{}},
		{"notification_logs", &NotificationLog{}},
	} {
		var count int64
		q := db.Model(tt.model).Where("channel_id = ?", ch.ID)
		if tt.name == "notification_logs" {
			q = db.Model(tt.model).Where("upstream_channel_id = ? OR subject LIKE ?", ch.ID, "%"+ch.Name+"%")
		}
		if err := q.Count(&count).Error; err != nil {
			t.Fatalf("count %s: %v", tt.name, err)
		}
		if count != 0 {
			t.Fatalf("%s count = %d, want 0", tt.name, count)
		}
	}
}

func TestAutoMigrateDropsDeletedAtColumns(t *testing.T) {
	db := openTestDB(t)

	for _, ddl := range []string{
		"ALTER TABLE channels ADD COLUMN deleted_at datetime",
		"ALTER TABLE captcha_configs ADD COLUMN deleted_at datetime",
		"ALTER TABLE notification_channels ADD COLUMN deleted_at datetime",
		"CREATE INDEX idx_channels_deleted_at ON channels(deleted_at)",
		"CREATE INDEX idx_captcha_configs_deleted_at ON captcha_configs(deleted_at)",
		"CREATE INDEX idx_notification_channels_deleted_at ON notification_channels(deleted_at)",
	} {
		if err := db.Exec(ddl).Error; err != nil {
			t.Fatalf("exec %q: %v", ddl, err)
		}
	}

	now := time.Now()
	activeChannel := &Channel{
		Name:           "active-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	deletedChannel := &Channel{
		Name:           "deleted-channel",
		Type:           ChannelTypeNewAPI,
		SiteURL:        "https://example.com",
		Username:       "u",
		PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := db.Create(activeChannel).Error; err != nil {
		t.Fatalf("create active channel: %v", err)
	}
	if err := db.Create(deletedChannel).Error; err != nil {
		t.Fatalf("create deleted channel: %v", err)
	}
	if err := db.Table("channels").Where("id = ?", deletedChannel.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted channel: %v", err)
	}

	activeCaptcha := &CaptchaConfig{Name: "active-captcha", Type: CaptchaCapSolver, APIKeyCipher: "x", Enabled: true}
	deletedCaptcha := &CaptchaConfig{Name: "deleted-captcha", Type: CaptchaCapSolver, APIKeyCipher: "x", Enabled: true}
	if err := db.Create(activeCaptcha).Error; err != nil {
		t.Fatalf("create active captcha: %v", err)
	}
	if err := db.Create(deletedCaptcha).Error; err != nil {
		t.Fatalf("create deleted captcha: %v", err)
	}
	if err := db.Table("captcha_configs").Where("id = ?", deletedCaptcha.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted captcha: %v", err)
	}

	activeNotify := &NotificationChannel{Name: "active-notify", Type: NotifyTelegram, ConfigCipher: "x", Enabled: true}
	deletedNotify := &NotificationChannel{Name: "deleted-notify", Type: NotifyTelegram, ConfigCipher: "x", Enabled: true}
	if err := db.Create(activeNotify).Error; err != nil {
		t.Fatalf("create active notification channel: %v", err)
	}
	if err := db.Create(deletedNotify).Error; err != nil {
		t.Fatalf("create deleted notification channel: %v", err)
	}
	if err := db.Table("notification_channels").Where("id = ?", deletedNotify.ID).Update("deleted_at", now).Error; err != nil {
		t.Fatalf("mark deleted notification channel: %v", err)
	}

	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate: %v", err)
	}

	for _, table := range []string{"channels", "captcha_configs", "notification_channels"} {
		hasColumn, err := tableHasColumn(db, table, "deleted_at")
		if err != nil {
			t.Fatalf("inspect %s.deleted_at: %v", table, err)
		}
		if hasColumn {
			t.Fatalf("%s.deleted_at still exists", table)
		}
	}

	var count int64
	if err := db.Model(&Channel{}).Count(&count).Error; err != nil {
		t.Fatalf("count channels: %v", err)
	}
	if count != 1 {
		t.Fatalf("channel count = %d, want 1", count)
	}
	if err := db.Model(&CaptchaConfig{}).Count(&count).Error; err != nil {
		t.Fatalf("count captchas: %v", err)
	}
	if count != 1 {
		t.Fatalf("captcha count = %d, want 1", count)
	}
	if err := db.Model(&NotificationChannel{}).Count(&count).Error; err != nil {
		t.Fatalf("count notification channels: %v", err)
	}
	if count != 1 {
		t.Fatalf("notification channel count = %d, want 1", count)
	}
}

func TestGatewayGroupsReorderAndListOrder(t *testing.T) {
	db := openTestDB(t)
	repo := NewGatewayGroups(db)

	a := &GatewayGroup{Name: "a", Status: GatewayGroupStatusActive}
	b := &GatewayGroup{Name: "b", Status: GatewayGroupStatusActive}
	c := &GatewayGroup{Name: "c", Status: GatewayGroupStatusActive}
	for _, g := range []*GatewayGroup{a, b, c} {
		pos, err := repo.NextPosition()
		if err != nil {
			t.Fatalf("next pos: %v", err)
		}
		g.Position = pos
		if err := repo.Create(g); err != nil {
			t.Fatalf("create %s: %v", g.Name, err)
		}
	}

	// 创建顺序 a,b,c → position 0,1,2；列表应为 a,b,c
	list, err := repo.List()
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 3 || list[0].Name != "a" || list[1].Name != "b" || list[2].Name != "c" {
		t.Fatalf("initial order = %v %v %v", list[0].Name, list[1].Name, list[2].Name)
	}

	// 重排为 c, a, b
	if err := repo.Reorder([]uint{c.ID, a.ID, b.ID}); err != nil {
		t.Fatalf("reorder: %v", err)
	}
	list, err = repo.List()
	if err != nil {
		t.Fatalf("list after reorder: %v", err)
	}
	if list[0].Name != "c" || list[1].Name != "a" || list[2].Name != "b" {
		t.Fatalf("reordered = %v %v %v", list[0].Name, list[1].Name, list[2].Name)
	}
	if list[0].Position != 0 || list[1].Position != 1 || list[2].Position != 2 {
		t.Fatalf("positions = %d %d %d", list[0].Position, list[1].Position, list[2].Position)
	}
}

func TestGatewayResponseRulesValidateCompileAndScope(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	first := &GatewayGroup{Name: "rules-first", Status: GatewayGroupStatusActive}
	second := &GatewayGroup{Name: "rules-second", Status: GatewayGroupStatusActive}
	if err := groups.Create(first); err != nil {
		t.Fatalf("create first group: %v", err)
	}
	if err := groups.Create(second); err != nil {
		t.Fatalf("create second group: %v", err)
	}
	rules := NewGatewayResponseRules(db)
	item := &GatewayResponseRule{
		GatewayGroupID: first.ID,
		Name:           "bad-content",
		Enabled:        true,
		Priority:       2,
		Pattern:        `(?i)declined`,
		Target:         GatewayResponseRuleTargetAssistantText,
		ModelsJSON:     `[" model-a ","model-a"]`,
		ProtocolsJSON:  `["OpenAI","openai"]`,
	}
	if err := rules.Create(item); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	if item.ModelsJSON != `["model-a"]` || item.ProtocolsJSON != `["openai"]` {
		t.Fatalf("normalized lists = %s / %s", item.ModelsJSON, item.ProtocolsJSON)
	}
	list, err := rules.ListEnabledByGroupID(first.ID)
	if err != nil || len(list) != 1 || list[0].Name != item.Name {
		t.Fatalf("enabled rules = %#v, err=%v", list, err)
	}
	if other, err := rules.ListByGroupID(second.ID); err != nil || len(other) != 0 {
		t.Fatalf("rule scope leaked: %#v, err=%v", other, err)
	}
	duplicate := *item
	duplicate.ID = 0
	duplicate.Name = "BAD-CONTENT"
	if err := rules.Create(&duplicate); err == nil {
		t.Fatal("case-insensitive duplicate rule name was accepted")
	}
	invalid := *item
	invalid.ID = 0
	invalid.Name = "invalid"
	invalid.Pattern = "["
	if err := rules.Create(&invalid); err == nil {
		t.Fatal("invalid regular expression was accepted")
	}
}

func TestGatewayResponseRulesImportStrategies(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	group := &GatewayGroup{Name: "rules-import", Status: GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	rules := NewGatewayResponseRules(db)
	if err := rules.Create(&GatewayResponseRule{
		GatewayGroupID: group.ID,
		Name:           "capacity",
		Enabled:        true,
		Priority:       1,
		Pattern:        "old",
		Target:         GatewayResponseRuleTargetAssistantText,
		ModelsJSON:     "[]",
		ProtocolsJSON:  "[]",
	}); err != nil {
		t.Fatalf("create existing rule: %v", err)
	}
	enabled := false
	bundle := GatewayResponseRuleBundle{
		Kind:    GatewayResponseRuleBundleKind,
		Version: GatewayResponseRuleBundleVersion,
		Rules: []GatewayResponseRuleBundleRule{
			{Name: "capacity", Enabled: &enabled, Priority: 5, Pattern: "new", Target: GatewayResponseRuleTargetRawBody},
			{Name: "other", Priority: 8, Pattern: "other", Target: GatewayResponseRuleTargetErrorMessage},
		},
	}
	result, err := rules.Import(group.ID, bundle, GatewayResponseRuleImportReplace)
	if err != nil {
		t.Fatalf("replace import: %v", err)
	}
	if result.Replaced != 1 || result.Created != 1 || result.Skipped != 0 {
		t.Fatalf("replace result = %#v", result)
	}
	updated, err := rules.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list after replace: %v", err)
	}
	if len(updated) != 2 || updated[0].Pattern != "new" || updated[0].Enabled {
		t.Fatalf("updated rules = %#v", updated)
	}

	renameBundle := GatewayResponseRuleBundle{
		Kind:    GatewayResponseRuleBundleKind,
		Version: GatewayResponseRuleBundleVersion,
		Rules:   []GatewayResponseRuleBundleRule{{Name: "capacity", Pattern: "third", Target: GatewayResponseRuleTargetAssistantText}},
	}
	rename, err := rules.Import(group.ID, renameBundle, GatewayResponseRuleImportRename)
	if err != nil {
		t.Fatalf("rename import: %v", err)
	}
	if rename.Renamed != 1 || rename.Created != 1 || len(rename.Items) != 1 || rename.Items[0].Name != "capacity (2)" {
		t.Fatalf("rename result = %#v", rename)
	}

	invalid := GatewayResponseRuleBundle{
		Kind:    GatewayResponseRuleBundleKind,
		Version: GatewayResponseRuleBundleVersion,
		Rules: []GatewayResponseRuleBundleRule{
			{Name: "valid-before-invalid", Pattern: "ok", Target: GatewayResponseRuleTargetAssistantText},
			{Name: "bad", Pattern: "[", Target: GatewayResponseRuleTargetAssistantText},
		},
	}
	if _, err := rules.Import(group.ID, invalid, GatewayResponseRuleImportSkip); err == nil {
		t.Fatal("invalid bundle was accepted")
	}
	updated, err = rules.ListByGroupID(group.ID)
	if err != nil {
		t.Fatalf("list after invalid import: %v", err)
	}
	for _, item := range updated {
		if item.Name == "valid-before-invalid" {
			t.Fatal("invalid bundle partially committed")
		}
	}
}

func TestGatewayGroupsDeleteRemovesResponseRules(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	group := &GatewayGroup{Name: "rules-delete", Status: GatewayGroupStatusActive}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	rules := NewGatewayResponseRules(db)
	rule := &GatewayResponseRule{
		GatewayGroupID: group.ID,
		Name:           "rule",
		Pattern:        "x",
		ModelsJSON:     "[]",
		ProtocolsJSON:  "[]",
	}
	if err := rules.Create(rule); err != nil {
		t.Fatalf("create rule: %v", err)
	}
	if err := groups.Delete(group.ID); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	if _, err := rules.FindByID(rule.ID); err == nil {
		t.Fatal("response rule survived group deletion")
	}
}

func TestGatewayUsageFinalizeRequestChargesWinnerOnce(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "settle-key", KeyHash: "settle-hash", KeyPrefix: "sk-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID,
		RequestID:    "settle-request",
		Attempt:      2,
		AttemptKind:  GatewayAttemptKindHedge,
		ActualCost:   1.25,
		Success:      true,
		CreatedAt:    time.Now().UTC(),
	}
	logs := NewGatewayUsageLogs(db)
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID:        usage.RequestID,
		GatewayKeyID:     key.ID,
		Delivered:        true,
		WinnerAttempt:    usage.Attempt,
		WinnerUsageLogID: usage.ID,
	})
	if err != nil || !first {
		t.Fatalf("first finalize = %v, err=%v", first, err)
	}
	second, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID:     usage.RequestID,
		GatewayKeyID:  key.ID,
		Delivered:     true,
		WinnerAttempt: usage.Attempt,
	})
	if err != nil || second {
		t.Fatalf("replayed finalize = %v, err=%v", second, err)
	}
	var gotKey GatewayKey
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("load key: %v", err)
	}
	if gotKey.QuotaUsed != 1.25 {
		t.Fatalf("quota_used = %v, want 1.25", gotKey.QuotaUsed)
	}
	var winnerCount int64
	if err := db.Model(&GatewayUsageLog{}).Where("request_id = ? AND winner = ?", usage.RequestID, true).Count(&winnerCount).Error; err != nil {
		t.Fatalf("count winner: %v", err)
	}
	if winnerCount != 1 {
		t.Fatalf("winner count = %d, want 1", winnerCount)
	}
	var extraCost float64
	if err := db.Model(&GatewayUsageLog{}).Where("id = ?", usage.ID).Select("estimated_extra_cost").Scan(&extraCost).Error; err != nil {
		t.Fatalf("load winner extra cost: %v", err)
	}
	if extraCost != 0 {
		t.Fatalf("winner estimated_extra_cost = %v, want 0", extraCost)
	}
	var settlementCount int64
	if err := db.Model(&GatewayWinnerSettlement{}).Where("request_id = ?", usage.RequestID).Count(&settlementCount).Error; err != nil {
		t.Fatalf("count settlements: %v", err)
	}
	if settlementCount != 1 {
		t.Fatalf("settlement count = %d, want 1", settlementCount)
	}
}

func TestGatewayUsageFinalizeVirtualCacheKeepsRawCostAndChargesBilledCost(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "virtual-cache-key", KeyHash: "virtual-cache-hash", KeyPrefix: "sk-vc-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID,
		RequestID:    "virtual-cache-request",
		Attempt:      1,
		AttemptKind:  GatewayAttemptKindPrimary,
		BillingMode:  "token",
		InputTokens:  100,
		ActualCost:   1.25,
		Success:      true,
		CreatedAt:    time.Now().UTC(),
	}
	logs := NewGatewayUsageLogs(db)
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	// The primary winner is eligible only because a concurrent hedge was also
	// recorded for this request. The loser can retain its raw cost and status.
	hedge := &GatewayUsageLog{
		GatewayKeyID:  key.ID,
		RequestID:     usage.RequestID,
		Attempt:       2,
		AttemptKind:   GatewayAttemptKindHedge,
		AttemptStatus: GatewayAttemptStatusCanceled,
		BillingMode:   "token",
		InputTokens:   100,
		ActualCost:    1.25,
		Success:       false,
		CreatedAt:     time.Now().UTC(),
	}
	if err := logs.Create(hedge); err != nil {
		t.Fatalf("create hedge usage: %v", err)
	}
	first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID:               usage.RequestID,
		GatewayKeyID:            key.ID,
		Delivered:               true,
		WinnerAttempt:           usage.Attempt,
		WinnerUsageLogID:        usage.ID,
		BilledCost:              0.50,
		BilledCostSet:           true,
		HedgeTriggered:          true,
		VirtualCacheReadEnabled: true,
		VirtualCacheReadTokens:  100,
		VirtualCacheReadCost:    0.01,
	})
	if err != nil || !first {
		t.Fatalf("first virtual finalize = %v, err=%v", first, err)
	}
	var gotKey GatewayKey
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("load key: %v", err)
	}
	if gotKey.QuotaUsed != 0.50 {
		t.Fatalf("quota_used = %v, want 0.50", gotKey.QuotaUsed)
	}
	var gotUsage GatewayUsageLog
	if err := db.First(&gotUsage, usage.ID).Error; err != nil {
		t.Fatalf("load usage: %v", err)
	}
	if gotUsage.ActualCost != 1.25 {
		t.Fatalf("actual_cost = %v, want raw 1.25", gotUsage.ActualCost)
	}
	if gotUsage.BilledCost != 0.50 || gotUsage.VirtualCacheReadTokens != 100 || gotUsage.VirtualCacheReadCost != 0.01 {
		t.Fatalf("virtual usage settlement = billed=%v tokens=%d cost=%v", gotUsage.BilledCost, gotUsage.VirtualCacheReadTokens, gotUsage.VirtualCacheReadCost)
	}
	var settlement GatewayWinnerSettlement
	if err := db.First(&settlement, "request_id = ?", usage.RequestID).Error; err != nil {
		t.Fatalf("load settlement: %v", err)
	}
	if settlement.ActualCost != 1.25 || settlement.BilledCost != 0.50 || settlement.VirtualCacheReadTokens != 100 {
		t.Fatalf("settlement = %#v", settlement)
	}
	var finalization GatewayRequestFinalization
	if err := db.First(&finalization, "request_id = ?", usage.RequestID).Error; err != nil {
		t.Fatalf("load finalization: %v", err)
	}
	if finalization.ActualCost != 1.25 || finalization.BilledCost != 0.50 || finalization.VirtualCacheReadTokens != 100 {
		t.Fatalf("finalization = %#v", finalization)
	}

	// A replay cannot overwrite the original settlement or charge the key a
	// second time, even if a caller supplies different virtual values.
	second, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID:               usage.RequestID,
		GatewayKeyID:            key.ID,
		Delivered:               true,
		WinnerAttempt:           usage.Attempt,
		BilledCost:              0.01,
		BilledCostSet:           true,
		HedgeTriggered:          true,
		VirtualCacheReadEnabled: true,
		VirtualCacheReadTokens:  1,
		VirtualCacheReadCost:    0.001,
	})
	if err != nil || second {
		t.Fatalf("replayed virtual finalize = %v, err=%v", second, err)
	}
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("reload key: %v", err)
	}
	if gotKey.QuotaUsed != 0.50 {
		t.Fatalf("replayed quota_used = %v, want 0.50", gotKey.QuotaUsed)
	}
}

func TestGatewayUsageFinalizeVirtualCacheRejectsIneligibleWinner(t *testing.T) {
	cases := []struct {
		name    string
		kind    string
		billing string
		image   int
		tokens  int
		fresh   int
	}{
		{name: "without hedge", kind: GatewayAttemptKindPrimary, billing: "token", tokens: 1, fresh: 1},
		{name: "retry winner", kind: GatewayAttemptKindRetry, billing: "token", tokens: 1, fresh: 1},
		{name: "media output", kind: GatewayAttemptKindHedge, billing: "image", image: 1, tokens: 1, fresh: 1},
		{name: "token cap", kind: GatewayAttemptKindHedge, billing: "token", tokens: 2, fresh: 1},
		{name: "zero virtual tokens", kind: GatewayAttemptKindHedge, billing: "token", tokens: 0, fresh: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			key := &GatewayKey{Name: "vc-reject-" + tc.name, KeyHash: "vc-reject-hash-" + tc.name, KeyPrefix: "sk-vcr-", KeyCipher: "cipher"}
			if err := db.Create(key).Error; err != nil {
				t.Fatalf("create key: %v", err)
			}
			usage := &GatewayUsageLog{
				GatewayKeyID: key.ID, RequestID: "vc-reject-request-" + tc.name,
				Attempt: 1, AttemptKind: tc.kind, BillingMode: tc.billing,
				ImageOutputTokens: tc.image, InputTokens: tc.fresh, ActualCost: 1,
				Success: true, CreatedAt: time.Now().UTC(),
			}
			logs := NewGatewayUsageLogs(db)
			if err := logs.Create(usage); err != nil {
				t.Fatalf("create usage: %v", err)
			}
			_, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
				RequestID: usage.RequestID, GatewayKeyID: key.ID, Delivered: true,
				WinnerAttempt: 1, WinnerUsageLogID: usage.ID, BilledCost: 0.5,
				BilledCostSet: true, HedgeTriggered: tc.kind == GatewayAttemptKindHedge,
				VirtualCacheReadEnabled: true, VirtualCacheReadTokens: tc.tokens,
			})
			if err == nil {
				t.Fatal("ineligible virtual cache settlement unexpectedly succeeded")
			}
			var gotKey GatewayKey
			if err := db.First(&gotKey, key.ID).Error; err != nil {
				t.Fatalf("load key: %v", err)
			}
			if gotKey.QuotaUsed != 0 {
				t.Fatalf("rejected settlement charged quota_used=%v", gotKey.QuotaUsed)
			}
		})
	}
}

func TestGatewayUsageFinalizeVirtualCacheRequiresRecordedHedgeAndSuccessfulWinner(t *testing.T) {
	cases := []struct {
		name      string
		success   bool
		companion bool
		billing   string
		endpoint  string
	}{
		{name: "missing companion", success: true},
		{name: "failed winner", success: false, companion: true},
		{name: "video billing mode", success: true, companion: true, billing: "video"},
		{name: "video endpoint", success: true, companion: true, endpoint: "/v1/videos/generations"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			key := &GatewayKey{Name: "vc-guard-" + tc.name, KeyHash: "vc-guard-hash-" + tc.name, KeyPrefix: "sk-vcg-", KeyCipher: "cipher"}
			if err := db.Create(key).Error; err != nil {
				t.Fatalf("create key: %v", err)
			}
			logs := NewGatewayUsageLogs(db)
			usage := &GatewayUsageLog{
				GatewayKeyID: key.ID, RequestID: "vc-guard-request-" + tc.name,
				Attempt: 1, AttemptKind: GatewayAttemptKindPrimary, AttemptStatus: GatewayAttemptStatusAccepted,
				BillingMode: tc.billing, InboundEndpoint: tc.endpoint, InputTokens: 10, ActualCost: 1,
				Success: tc.success, CreatedAt: time.Now().UTC(),
			}
			if usage.BillingMode == "" {
				usage.BillingMode = "token"
			}
			if err := logs.Create(usage); err != nil {
				t.Fatalf("create winner usage: %v", err)
			}
			if tc.companion {
				hedge := &GatewayUsageLog{
					GatewayKeyID: key.ID, RequestID: usage.RequestID,
					Attempt: 2, AttemptKind: GatewayAttemptKindHedge, AttemptStatus: GatewayAttemptStatusCanceled,
					BillingMode: "token", InputTokens: 10, ActualCost: 1,
					CreatedAt: time.Now().UTC(),
				}
				if err := logs.Create(hedge); err != nil {
					t.Fatalf("create hedge usage: %v", err)
				}
			}
			_, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
				RequestID: usage.RequestID, GatewayKeyID: key.ID, Delivered: true,
				WinnerAttempt: 1, WinnerUsageLogID: usage.ID, BilledCost: 0.5,
				BilledCostSet: true, HedgeTriggered: true,
				VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 5,
			})
			if err == nil {
				t.Fatal("ineligible virtual cache settlement unexpectedly succeeded")
			}
			var gotKey GatewayKey
			if err := db.First(&gotKey, key.ID).Error; err != nil {
				t.Fatalf("load key: %v", err)
			}
			if gotKey.QuotaUsed != 0 {
				t.Fatalf("rejected settlement charged quota_used=%v", gotKey.QuotaUsed)
			}
		})
	}
}

func TestGatewayUsageFinalizeVirtualCacheAcceptsRegexRejectedConcurrentCompanion(t *testing.T) {
	cases := []struct {
		name        string
		winnerKind  string
		companionID int
	}{
		{name: "primary winner", winnerKind: GatewayAttemptKindPrimary, companionID: 2},
		{name: "hedge winner", winnerKind: GatewayAttemptKindHedge, companionID: 1},
	}
	for index, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := openTestDB(t)
			key := &GatewayKey{
				Name:      "vc-regex-companion-key-" + fmt.Sprint(index),
				KeyHash:   "vc-regex-companion-hash-" + fmt.Sprint(index),
				KeyPrefix: "sk-vcrx-",
				KeyCipher: "cipher",
			}
			if err := db.Create(key).Error; err != nil {
				t.Fatalf("create key: %v", err)
			}
			logs := NewGatewayUsageLogs(db)
			winner := &GatewayUsageLog{
				GatewayKeyID: key.ID, RequestID: "vc-regex-companion-request-" + fmt.Sprint(index),
				Attempt: 1, AttemptKind: tc.winnerKind, AttemptStatus: GatewayAttemptStatusAccepted,
				BillingMode: "token", InputTokens: 10, ActualCost: 1, Success: true,
				CreatedAt: time.Now().UTC(),
			}
			if tc.winnerKind == GatewayAttemptKindHedge {
				winner.Attempt = 2
			}
			if err := logs.Create(winner); err != nil {
				t.Fatalf("create winner usage: %v", err)
			}
			companion := &GatewayUsageLog{
				GatewayKeyID: key.ID, RequestID: winner.RequestID,
				Attempt: tc.companionID, AttemptKind: GatewayAttemptKindRegexReject,
				AttemptStatus: GatewayAttemptStatusRejected, BillingMode: "token",
				InputTokens: 10, ActualCost: 1, Success: false, CreatedAt: time.Now().UTC(),
			}
			if err := logs.Create(companion); err != nil {
				t.Fatalf("create regex companion usage: %v", err)
			}
			first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
				RequestID: winner.RequestID, GatewayKeyID: key.ID, Delivered: true,
				WinnerAttempt: winner.Attempt, WinnerUsageLogID: winner.ID,
				BilledCost: 0.5, BilledCostSet: true, HedgeTriggered: true,
				VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 5,
			})
			if err != nil || !first {
				t.Fatalf("regex companion virtual finalize = %v, err=%v", first, err)
			}
		})
	}
}

func TestGatewayUsageFinalizeResponseRuleVirtualCache(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "vc-response-rule-key", KeyHash: "vc-response-rule-hash", KeyPrefix: "sk-vcrr-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	rejected := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: "vc-response-rule-request", RouteID: 11,
		Attempt: 1, AttemptKind: GatewayAttemptKindRegexReject, AttemptStatus: GatewayAttemptStatusRejected,
		ValidationPostCommit: false, BillingMode: "token", InputTokens: 50, ActualCost: 1,
		CreatedAt: time.Now().UTC(),
	}
	winner := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: rejected.RequestID, RouteID: 22,
		Attempt: 2, AttemptKind: GatewayAttemptKindFailover, AttemptStatus: GatewayAttemptStatusAccepted,
		BillingMode: "token", InputTokens: 50, ActualCost: 1, Success: true,
		CreatedAt: time.Now().UTC(),
	}
	if err := logs.Create(rejected); err != nil {
		t.Fatalf("create rejected usage: %v", err)
	}
	if err := logs.Create(winner); err != nil {
		t.Fatalf("create winner usage: %v", err)
	}
	first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: winner.RequestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: winner.Attempt, WinnerUsageLogID: winner.ID,
		BilledCost: 0.25, BilledCostSet: true,
		VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 50, VirtualCacheReadCost: 0.01,
		VirtualCacheReason: GatewayVirtualCacheReasonResponseRuleFailover,
	})
	if err != nil || !first {
		t.Fatalf("response-rule virtual finalize = %v, err=%v", first, err)
	}
	var gotUsage GatewayUsageLog
	if err := db.First(&gotUsage, winner.ID).Error; err != nil {
		t.Fatalf("load winner usage: %v", err)
	}
	var settlement GatewayWinnerSettlement
	if err := db.First(&settlement, "request_id = ?", winner.RequestID).Error; err != nil {
		t.Fatalf("load settlement: %v", err)
	}
	var finalization GatewayRequestFinalization
	if err := db.First(&finalization, "request_id = ?", winner.RequestID).Error; err != nil {
		t.Fatalf("load finalization: %v", err)
	}
	if gotUsage.VirtualCacheReason != GatewayVirtualCacheReasonResponseRuleFailover ||
		settlement.VirtualCacheReason != GatewayVirtualCacheReasonResponseRuleFailover ||
		finalization.VirtualCacheReason != GatewayVirtualCacheReasonResponseRuleFailover {
		t.Fatalf("virtual cache reason was not persisted: usage=%q settlement=%q finalization=%q",
			gotUsage.VirtualCacheReason, settlement.VirtualCacheReason, finalization.VirtualCacheReason)
	}
}

func TestGatewayUsageFinalizeResponseRuleVirtualCacheAllowsHedgeWinner(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "vc-response-rule-hedge-key", KeyHash: "vc-response-rule-hedge-hash", KeyPrefix: "sk-vcrh-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	requestID := "vc-response-rule-hedge-request"
	rejected := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: requestID, RouteID: 11,
		Attempt: 1, AttemptKind: GatewayAttemptKindRegexReject, AttemptStatus: GatewayAttemptStatusRejected,
		ValidationPostCommit: false, BillingMode: "token", InputTokens: 50, ActualCost: 1,
		CreatedAt: time.Now().UTC(),
	}
	winner := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: requestID, RouteID: 22,
		Attempt: 2, AttemptKind: GatewayAttemptKindHedge, AttemptStatus: GatewayAttemptStatusAccepted,
		BillingMode: "token", InputTokens: 50, ActualCost: 1, Success: true,
		CreatedAt: time.Now().UTC(),
	}
	if err := logs.Create(rejected); err != nil {
		t.Fatalf("create rejected usage: %v", err)
	}
	if err := logs.Create(winner); err != nil {
		t.Fatalf("create winner usage: %v", err)
	}
	first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: requestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: winner.Attempt, WinnerUsageLogID: winner.ID,
		BilledCost: 0.25, BilledCostSet: true, HedgeTriggered: true,
		VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 50, VirtualCacheReadCost: 0.01,
		VirtualCacheReason: GatewayVirtualCacheReasonResponseRuleFailover,
	})
	if err != nil || !first {
		t.Fatalf("response-rule hedge virtual finalize = %v, err=%v", first, err)
	}
	var gotUsage GatewayUsageLog
	if err := db.First(&gotUsage, winner.ID).Error; err != nil {
		t.Fatalf("load winner usage: %v", err)
	}
	if gotUsage.VirtualCacheReason != GatewayVirtualCacheReasonResponseRuleFailover {
		t.Fatalf("virtual cache reason=%q, want response-rule failover", gotUsage.VirtualCacheReason)
	}
}

func TestGatewayUsageFinalizeResponseRuleVirtualCacheRejectsSameRoute(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "vc-response-rule-same-key", KeyHash: "vc-response-rule-same-hash", KeyPrefix: "sk-vcrs-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	requestID := "vc-response-rule-same-request"
	for _, usage := range []*GatewayUsageLog{
		{GatewayKeyID: key.ID, RequestID: requestID, RouteID: 11, Attempt: 1, AttemptKind: GatewayAttemptKindRegexReject, AttemptStatus: GatewayAttemptStatusRejected, BillingMode: "token", InputTokens: 10, ActualCost: 1, CreatedAt: time.Now().UTC()},
		{GatewayKeyID: key.ID, RequestID: requestID, RouteID: 11, Attempt: 2, AttemptKind: GatewayAttemptKindFailover, AttemptStatus: GatewayAttemptStatusAccepted, BillingMode: "token", InputTokens: 10, ActualCost: 1, Success: true, CreatedAt: time.Now().UTC()},
	} {
		if err := logs.Create(usage); err != nil {
			t.Fatalf("create usage: %v", err)
		}
	}
	_, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: requestID, GatewayKeyID: key.ID, Delivered: true, WinnerAttempt: 2,
		BilledCost: 0.5, BilledCostSet: true, VirtualCacheReadEnabled: true,
		VirtualCacheReadTokens: 5, VirtualCacheReason: GatewayVirtualCacheReasonResponseRuleFailover,
	})
	if err == nil {
		t.Fatal("same-route response-rule virtual settlement unexpectedly succeeded")
	}
}

func TestGatewayUsageFinalizeProviderGlobalVirtualCache(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "vc-provider-key", KeyHash: "vc-provider-hash", KeyPrefix: "sk-vcp-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID, GatewayProviderID: 77, RequestID: "vc-provider-request", RouteID: 7,
		Attempt: 1, AttemptKind: GatewayAttemptKindPrimary, AttemptStatus: GatewayAttemptStatusAccepted,
		BillingMode: "token", InputTokens: 100, ActualCost: 1, Success: true,
		CreatedAt: time.Now().UTC(),
	}
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create provider usage: %v", err)
	}
	first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: usage.RequestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: 1, WinnerUsageLogID: usage.ID,
		BilledCost: 0.7, BilledCostSet: true,
		VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 30,
		VirtualCacheReadCost: 0.01, VirtualCacheReason: GatewayVirtualCacheReasonProviderGlobal,
	})
	if err != nil || !first {
		t.Fatalf("provider virtual finalize = %v, err=%v", first, err)
	}
	var got GatewayUsageLog
	if err := db.First(&got, usage.ID).Error; err != nil {
		t.Fatalf("load provider usage: %v", err)
	}
	if got.VirtualCacheReason != GatewayVirtualCacheReasonProviderGlobal || got.VirtualCacheReadTokens != 30 || !got.Winner {
		t.Fatalf("provider virtual usage=%+v", got)
	}

	bad := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: "vc-provider-missing-id", RouteID: 8,
		Attempt: 1, AttemptKind: GatewayAttemptKindPrimary, AttemptStatus: GatewayAttemptStatusAccepted,
		BillingMode: "token", InputTokens: 10, ActualCost: 1, Success: true,
		CreatedAt: time.Now().UTC(),
	}
	if err := logs.Create(bad); err != nil {
		t.Fatalf("create invalid provider usage: %v", err)
	}
	if _, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: bad.RequestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: 1, WinnerUsageLogID: bad.ID,
		BilledCost: 0.5, BilledCostSet: true,
		VirtualCacheReadEnabled: true, VirtualCacheReadTokens: 5,
		VirtualCacheReason: GatewayVirtualCacheReasonProviderGlobal,
	}); err == nil {
		t.Fatal("provider virtual settlement without provider id unexpectedly succeeded")
	}
}

func TestGatewayUsageFinalizeRejectsUnboundBilledCostOverride(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "billed-override-key", KeyHash: "billed-override-hash", KeyPrefix: "sk-bco-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: "billed-override-request", Attempt: 1,
		AttemptKind: GatewayAttemptKindPrimary, BillingMode: "token", InputTokens: 1,
		ActualCost: 1, Success: true, CreatedAt: time.Now().UTC(),
	}
	logs := NewGatewayUsageLogs(db)
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	_, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: usage.RequestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: 1, WinnerUsageLogID: usage.ID, BilledCost: 0.01, BilledCostSet: true,
	})
	if err == nil {
		t.Fatal("unbound billed cost override unexpectedly succeeded")
	}
	var gotKey GatewayKey
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("load key: %v", err)
	}
	if gotKey.QuotaUsed != 0 {
		t.Fatalf("rejected override charged quota_used=%v", gotKey.QuotaUsed)
	}
}

func TestGatewayUsageStatsReclassifiesVirtualCacheWithoutAddingTokens(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "virtual-cache-stats-key", KeyHash: "virtual-cache-stats-hash", KeyPrefix: "sk-vcs-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: "virtual-cache-stats-request", Attempt: 1,
		AttemptKind: GatewayAttemptKindPrimary, Winner: true, Success: true,
		InputTokens: 100, CacheReadTokens: 10, CacheCreationTokens: 5, OutputTokens: 20,
		VirtualCacheReadTokens: 40, ActualCost: 1.25, BilledCost: 0.75,
		CreatedAt: time.Now().UTC(),
	}
	if err := NewGatewayUsageLogs(db).Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	loser := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: usage.RequestID, Attempt: 2,
		AttemptKind: GatewayAttemptKindHedge, AttemptStatus: GatewayAttemptStatusCanceled,
		ActualCost: 0.25, EstimatedExtraCost: 0.25, CreatedAt: time.Now().UTC(),
	}
	if err := NewGatewayUsageLogs(db).Create(loser); err != nil {
		t.Fatalf("create loser usage: %v", err)
	}
	stats, err := NewGatewayUsageLogs(db).Stats(GatewayUsageQuery{})
	if err != nil {
		t.Fatalf("usage stats: %v", err)
	}
	if stats.TotalInputTokens != 60 || stats.TotalCacheReadTokens != 50 || stats.TotalTokens != 135 {
		t.Fatalf("token stats = input=%d read=%d total=%d, want 60/50/135", stats.TotalInputTokens, stats.TotalCacheReadTokens, stats.TotalTokens)
	}
	if stats.WinnerCost != 0.75 || stats.TotalUpstreamCost != 1.50 {
		t.Fatalf("cost stats = winner=%v upstream=%v, want 0.75/1.50", stats.WinnerCost, stats.TotalUpstreamCost)
	}
	if stats.VirtualCacheSubsidyCost != 0.50 || stats.ExtraAttemptCost != 0.75 {
		t.Fatalf("extra cost stats = virtual_subsidy=%v extra=%v, want 0.50/0.75", stats.VirtualCacheSubsidyCost, stats.ExtraAttemptCost)
	}
}

func TestGatewayUsageCleanupRemovesOrphanSettlements(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "cleanup-key", KeyHash: "cleanup-hash", KeyPrefix: "ck-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID, RequestID: "cleanup-request", Attempt: 1,
		ActualCost: 0.5, Winner: true, Success: true, CreatedAt: time.Now().Add(-time.Hour),
	}
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	if _, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID: usage.RequestID, GatewayKeyID: key.ID, Delivered: true,
		WinnerAttempt: 1, WinnerUsageLogID: usage.ID,
	}); err != nil {
		t.Fatalf("finalize usage: %v", err)
	}
	deleted, err := logs.DeleteBefore(time.Now().Add(-time.Minute))
	if err != nil || deleted != 1 {
		t.Fatalf("delete before = %d, err=%v", deleted, err)
	}
	var finalizationCount, settlementCount int64
	if err := db.Model(&GatewayRequestFinalization{}).Where("request_id = ?", usage.RequestID).Count(&finalizationCount).Error; err != nil {
		t.Fatalf("count finalizations: %v", err)
	}
	if err := db.Model(&GatewayWinnerSettlement{}).Where("request_id = ?", usage.RequestID).Count(&settlementCount).Error; err != nil {
		t.Fatalf("count settlements: %v", err)
	}
	if finalizationCount != 0 || settlementCount != 0 {
		t.Fatalf("cleanup left finalization=%d settlement=%d", finalizationCount, settlementCount)
	}
}

func TestGatewayUsageFinalizeFailedRequestDoesNotCharge(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "failed-settle-key", KeyHash: "failed-settle-hash", KeyPrefix: "sk-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	first, err := logs.FinalizeFailedRequest("failed-request", key.ID)
	if err != nil || !first {
		t.Fatalf("first failed finalize = %v, err=%v", first, err)
	}
	second, err := logs.FinalizeFailedRequest("failed-request", key.ID)
	if err != nil || second {
		t.Fatalf("replayed failed finalize = %v, err=%v", second, err)
	}
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID,
		RequestID:    "failed-request",
		Attempt:      2,
		ActualCost:   9,
		Success:      true,
		CreatedAt:    time.Now().UTC(),
	}
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create replay usage: %v", err)
	}
	lateWinner, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
		RequestID:        usage.RequestID,
		GatewayKeyID:     key.ID,
		Delivered:        true,
		WinnerAttempt:    usage.Attempt,
		WinnerUsageLogID: usage.ID,
	})
	if err != nil || lateWinner {
		t.Fatalf("late winner after terminal failure = %v, err=%v", lateWinner, err)
	}
	var gotKey GatewayKey
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("load key: %v", err)
	}
	if gotKey.QuotaUsed != 0 {
		t.Fatalf("failed request charged %v", gotKey.QuotaUsed)
	}
	var settlements int64
	if err := db.Model(&GatewayWinnerSettlement{}).Where("request_id = ?", "failed-request").Count(&settlements).Error; err != nil {
		t.Fatalf("count settlements: %v", err)
	}
	if settlements != 0 {
		t.Fatalf("failed request created %d settlements", settlements)
	}
}

func TestGatewayUsageFinalizeRequestConcurrentClaim(t *testing.T) {
	db := openTestDB(t)
	key := &GatewayKey{Name: "race-settle-key", KeyHash: "race-settle-hash", KeyPrefix: "sk-", KeyCipher: "cipher"}
	if err := db.Create(key).Error; err != nil {
		t.Fatalf("create key: %v", err)
	}
	logs := NewGatewayUsageLogs(db)
	usage := &GatewayUsageLog{
		GatewayKeyID: key.ID,
		RequestID:    "race-request",
		Attempt:      1,
		ActualCost:   0.75,
		Success:      true,
		CreatedAt:    time.Now().UTC(),
	}
	if err := logs.Create(usage); err != nil {
		t.Fatalf("create usage: %v", err)
	}
	const callers = 8
	var wg sync.WaitGroup
	var claimed atomic.Int64
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			first, err := logs.FinalizeRequest(GatewayFinalizeRequestInput{
				RequestID:        usage.RequestID,
				GatewayKeyID:     key.ID,
				Delivered:        true,
				WinnerAttempt:    usage.Attempt,
				WinnerUsageLogID: usage.ID,
			})
			if err != nil {
				errs <- err
				return
			}
			if first {
				claimed.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent finalize: %v", err)
	}
	if claimed.Load() != 1 {
		t.Fatalf("successful claims = %d, want 1", claimed.Load())
	}
	var gotKey GatewayKey
	if err := db.First(&gotKey, key.ID).Error; err != nil {
		t.Fatalf("load key: %v", err)
	}
	if gotKey.QuotaUsed != 0.75 {
		t.Fatalf("quota_used = %v, want 0.75", gotKey.QuotaUsed)
	}
}

func TestGatewayRoutesSaveForGroupPreservesID(t *testing.T) {
	db, err := Open(DBConfig{
		Driver:       DBDriverSQLite,
		Path:         filepath.Join(t.TempDir(), "gw-route-preserve.db"),
		MaxOpenConns: 5,
		MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("sql db: %v", err)
	}
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(7, []GatewayRoute{
		{SourceChannelID: 1, Weight: 1, Enabled: true},
		{SourceChannelID: 2, Weight: 2, Enabled: true},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	first, err := routes.ListByGroupID(7)
	if err != nil || len(first) != 2 {
		t.Fatalf("list first: %v len=%d", err, len(first))
	}
	id0, id1 := first[0].ID, first[1].ID
	if id0 == 0 || id1 == 0 {
		t.Fatalf("ids zero: %+v", first)
	}
	// 模拟 ensure-keys 写入上游密钥
	if err := routes.UpdateSourceKey(id0, 41, "upstream-ops-gw-g7-r1", "cipher-a"); err != nil {
		t.Fatalf("key0: %v", err)
	}
	if err := routes.UpdateSourceKey(id1, 42, "k2", "cipher-b"); err != nil {
		t.Fatalf("key1: %v", err)
	}

	// 换序 + 改权重，id 应保持；密钥字段由服务端从旧行回填
	if err := routes.SaveForGroup(7, []GatewayRoute{
		{ID: id1, SourceChannelID: 2, Weight: 9, Enabled: true},
		{ID: id0, SourceChannelID: 1, Weight: 3, Enabled: true},
	}); err != nil {
		t.Fatalf("update reorder: %v", err)
	}
	second, err := routes.ListByGroupID(7)
	if err != nil || len(second) != 2 {
		t.Fatalf("list second: %v len=%d", err, len(second))
	}
	if second[0].ID != id1 || second[1].ID != id0 {
		t.Fatalf("ids not preserved: got %d,%d want %d,%d", second[0].ID, second[1].ID, id1, id0)
	}
	if second[0].Weight != 9 || second[1].Weight != 3 {
		t.Fatalf("weights: %+v", second)
	}
	if second[0].SourceAPIKeyName != "k2" || second[1].SourceAPIKeyName != "upstream-ops-gw-g7-r1" {
		t.Fatalf("keys not preserved: %+v / %+v", second[0], second[1])
	}

	// 删除一条
	if err := routes.SaveForGroup(7, []GatewayRoute{
		{ID: id0, SourceChannelID: 1, Weight: 1, Enabled: true},
	}); err != nil {
		t.Fatalf("delete one: %v", err)
	}
	third, err := routes.ListByGroupID(7)
	if err != nil || len(third) != 1 || third[0].ID != id0 {
		t.Fatalf("after delete: %+v err=%v", third, err)
	}
}

func TestNoteSuccessForPauseErrorClearsAfterStreak(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(9, []GatewayRoute{
		{SourceChannelID: 11, Weight: 1, Enabled: true},
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	list, err := routes.ListByGroupID(9)
	if err != nil || len(list) != 1 {
		t.Fatalf("list: %v len=%d", err, len(list))
	}
	id := list[0].ID

	until := time.Now().Add(5 * time.Minute)
	if err := routes.SetTempUnschedulable(id, until, "upstream HTTP error\nstatus: 503", time.Now(), "req_pause_1"); err != nil {
		t.Fatalf("set pause: %v", err)
	}

	// 无残留路由：调用应 noop
	if err := routes.NoteSuccessForPauseError(0); err != nil {
		t.Fatalf("id0: %v", err)
	}

	// 第 1、2 次成功：解除冷却，但保留错误信息
	for i := 1; i <= 2; i++ {
		if err := routes.NoteSuccessForPauseError(id); err != nil {
			t.Fatalf("success %d: %v", i, err)
		}
		got, err := routes.FindByID(id)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		if got.TempUnschedulableUntil != nil {
			t.Fatalf("success %d: until should be cleared", i)
		}
		if got.TempUnschedulableReason == "" {
			t.Fatalf("success %d: reason should remain for UI", i)
		}
		if got.RecoverSuccessStreak != i {
			t.Fatalf("success %d: streak=%d", i, got.RecoverSuccessStreak)
		}
	}

	// 第 3 次成功：清空「已恢复/错误/清除」相关字段
	if err := routes.NoteSuccessForPauseError(id); err != nil {
		t.Fatalf("success 3: %v", err)
	}
	got, err := routes.FindByID(id)
	if err != nil {
		t.Fatalf("get final: %v", err)
	}
	if got.TempUnschedulableUntil != nil ||
		got.TempUnschedulableReason != "" ||
		got.TempUnschedulableAt != nil ||
		got.TempUnschedulableRequestID != "" ||
		got.RecoverSuccessStreak != 0 {
		t.Fatalf("expected full clear, got %+v", got)
	}

	// 无残留后再成功：不累计 streak
	if err := routes.NoteSuccessForPauseError(id); err != nil {
		t.Fatalf("success after clear: %v", err)
	}
	got, err = routes.FindByID(id)
	if err != nil {
		t.Fatalf("get after clear: %v", err)
	}
	if got.RecoverSuccessStreak != 0 {
		t.Fatalf("streak should stay 0, got %d", got.RecoverSuccessStreak)
	}
}

func TestGatewayRouteModelCooldownIsolated(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(12, []GatewayRoute{{SourceChannelID: 21, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	list, err := routes.ListByGroupID(12)
	if err != nil || len(list) != 1 {
		t.Fatalf("list route: %v len=%d", err, len(list))
	}
	routeID := list[0].ID
	until := time.Now().Add(5 * time.Minute)
	if err := routes.SetModelTempUnschedulable(routeID, "model-a", until, "a failed", time.Now(), "req-a"); err != nil {
		t.Fatalf("set model-a cooldown: %v", err)
	}
	if err := routes.SetModelTempUnschedulable(routeID, "model-b", until, "b failed", time.Now(), "req-b"); err != nil {
		t.Fatalf("set model-b cooldown: %v", err)
	}
	if err := routes.SetModelTempUnschedulable(routeID, "model-a", until.Add(time.Minute), "a failed again", time.Now(), "req-a2"); err != nil {
		t.Fatalf("upsert model-a cooldown: %v", err)
	}

	got, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("find route: %v", err)
	}
	if len(got.ModelCooldowns) != 2 {
		t.Fatalf("model cooldown count=%d, want 2: %+v", len(got.ModelCooldowns), got.ModelCooldowns)
	}
	if got.ModelCooldowns["model-a"].TempUnschedulableReason != "a failed again" {
		t.Fatalf("model-a was not upserted: %+v", got.ModelCooldowns["model-a"])
	}

	modelACooldown := got.ModelCooldowns["model-a"]
	if err := routes.NoteSuccessForModelPauseError(
		routeID, "model-a", modelACooldown.TempUnschedulableAt, modelACooldown.TempUnschedulableRequestID,
	); err != nil {
		t.Fatalf("model-a success: %v", err)
	}
	got, err = routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("find after model-a success: %v", err)
	}
	if got.ModelCooldowns["model-a"].TempUnschedulableUntil != nil {
		t.Fatal("model-a cooldown should clear immediately after success")
	}
	if got.ModelCooldowns["model-b"].TempUnschedulableUntil == nil {
		t.Fatal("model-b cooldown must remain after model-a success")
	}

	if err := routes.ClearTempUnschedulable(routeID); err != nil {
		t.Fatalf("clear route cooldowns: %v", err)
	}
	var count int64
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", routeID).Count(&count).Error; err != nil {
		t.Fatalf("count model cooldowns: %v", err)
	}
	if count != 0 {
		t.Fatalf("route clear left %d model cooldown rows", count)
	}
}

func TestNoteSuccessForModelPauseErrorDoesNotClearNewerFailure(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(15, []GatewayRoute{{SourceChannelID: 24, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	list, err := routes.ListByGroupID(15)
	if err != nil || len(list) != 1 {
		t.Fatalf("list route: %v len=%d", err, len(list))
	}
	routeID := list[0].ID
	oldAt := time.Now().Add(-2 * time.Minute)
	if err := routes.SetModelTempUnschedulable(
		routeID, "model-a", time.Now().Add(time.Minute), "old", oldAt, "old-request",
	); err != nil {
		t.Fatalf("set old cooldown: %v", err)
	}
	oldRoute, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load old cooldown: %v", err)
	}
	observed := oldRoute.ModelCooldowns["model-a"]

	newAt := time.Now().Add(-time.Second)
	newUntil := time.Now().Add(5 * time.Minute)
	if err := routes.SetModelTempUnschedulable(
		routeID, "model-a", newUntil, "new", newAt, "new-request",
	); err != nil {
		t.Fatalf("set new cooldown: %v", err)
	}
	if err := routes.NoteSuccessForModelPauseError(
		routeID, "model-a", observed.TempUnschedulableAt, observed.TempUnschedulableRequestID,
	); err != nil {
		t.Fatalf("record stale success: %v", err)
	}
	got, err := routes.FindByID(routeID)
	if err != nil {
		t.Fatalf("load current cooldown: %v", err)
	}
	cooldown := got.ModelCooldowns["model-a"]
	if cooldown.TempUnschedulableUntil == nil || !cooldown.TempUnschedulableUntil.Equal(newUntil) ||
		cooldown.TempUnschedulableRequestID != "new-request" || cooldown.RecoverSuccessStreak != 0 {
		t.Fatalf("stale success changed newer cooldown: %+v", cooldown)
	}
}

func TestClearModelTempUnschedulableUntilRetainsDiagnostics(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(13, []GatewayRoute{{SourceChannelID: 22, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	list, err := routes.ListByGroupID(13)
	if err != nil || len(list) != 1 {
		t.Fatalf("list route: %v len=%d", err, len(list))
	}
	failedAt := time.Now().Add(-time.Minute)
	until := time.Now().Add(time.Minute)
	if err := routes.SetModelTempUnschedulable(list[0].ID, "model-a", until, "capacity", failedAt, "request-a"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	if err := routes.ClearModelTempUnschedulableUntil(list[0].ID, "model-a"); err != nil {
		t.Fatalf("clear cooldown until: %v", err)
	}
	got, err := routes.FindByID(list[0].ID)
	if err != nil {
		t.Fatalf("find route: %v", err)
	}
	cooldown := got.ModelCooldowns["model-a"]
	if cooldown.TempUnschedulableUntil != nil {
		t.Fatalf("cooldown until=%v, want nil", cooldown.TempUnschedulableUntil)
	}
	if cooldown.TempUnschedulableReason != "capacity" || cooldown.TempUnschedulableRequestID != "request-a" || cooldown.TempUnschedulableAt == nil {
		t.Fatalf("diagnostics were not retained: %+v", cooldown)
	}
}

func TestClearModelTempUnschedulableUntilIfMatchDoesNotClearNewerCooldown(t *testing.T) {
	db := openTestDB(t)
	routes := NewGatewayRoutes(db)
	if err := routes.SaveForGroup(14, []GatewayRoute{{SourceChannelID: 23, Weight: 1, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	list, err := routes.ListByGroupID(14)
	if err != nil || len(list) != 1 {
		t.Fatalf("list route: %v len=%d", err, len(list))
	}
	oldAt := time.Now().Add(-2 * time.Minute)
	oldUntil := time.Now().Add(time.Minute)
	if err := routes.SetModelTempUnschedulable(list[0].ID, "model-a", oldUntil, "old", oldAt, "old-request"); err != nil {
		t.Fatalf("set old cooldown: %v", err)
	}
	oldSnapshot, err := routes.FindByID(list[0].ID)
	if err != nil {
		t.Fatalf("load old cooldown: %v", err)
	}
	oldCooldown := oldSnapshot.ModelCooldowns["model-a"]
	newAt := time.Now().Add(-time.Second)
	newUntil := time.Now().Add(5 * time.Minute)
	if err := routes.SetModelTempUnschedulable(list[0].ID, "model-a", newUntil, "new", newAt, "new-request"); err != nil {
		t.Fatalf("set new cooldown: %v", err)
	}
	cleared, err := routes.ClearModelTempUnschedulableUntilIfMatch(
		list[0].ID, "model-a", *oldCooldown.TempUnschedulableUntil,
		oldCooldown.TempUnschedulableAt, oldCooldown.TempUnschedulableRequestID,
	)
	if err != nil {
		t.Fatalf("stale clear: %v", err)
	}
	if cleared {
		t.Fatal("stale recovery probe cleared a newer cooldown")
	}
	got, err := routes.FindByID(list[0].ID)
	if err != nil {
		t.Fatalf("find after stale clear: %v", err)
	}
	cooldown := got.ModelCooldowns["model-a"]
	if cooldown.TempUnschedulableUntil == nil || cooldown.TempUnschedulableRequestID != "new-request" {
		t.Fatalf("new cooldown was lost: %+v", cooldown)
	}
	cleared, err = routes.ClearModelTempUnschedulableUntilIfMatch(
		list[0].ID, "model-a", *cooldown.TempUnschedulableUntil,
		cooldown.TempUnschedulableAt, cooldown.TempUnschedulableRequestID,
	)
	if err != nil || !cleared {
		t.Fatalf("current clear: cleared=%v err=%v", cleared, err)
	}
}

func TestGatewayGroupDeleteRemovesModelCooldowns(t *testing.T) {
	db := openTestDB(t)
	groups := NewGatewayGroups(db)
	routes := NewGatewayRoutes(db)
	group := &GatewayGroup{Name: "model-cooldown-delete"}
	if err := groups.Create(group); err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := routes.SaveForGroup(group.ID, []GatewayRoute{{SourceChannelID: 31, Enabled: true}}); err != nil {
		t.Fatalf("create route: %v", err)
	}
	list, err := routes.ListByGroupID(group.ID)
	if err != nil || len(list) != 1 {
		t.Fatalf("list route: %v len=%d", err, len(list))
	}
	if err := routes.SetModelTempUnschedulable(list[0].ID, "model-a", time.Now().Add(time.Minute), "failed", time.Now(), "req"); err != nil {
		t.Fatalf("set cooldown: %v", err)
	}
	if err := groups.Delete(group.ID); err != nil {
		t.Fatalf("delete group: %v", err)
	}
	var count int64
	if err := db.Model(&GatewayRouteModelCooldown{}).Where("route_id = ?", list[0].ID).Count(&count).Error; err != nil {
		t.Fatalf("count orphan cooldowns: %v", err)
	}
	if count != 0 {
		t.Fatalf("group delete left %d model cooldown rows", count)
	}
}

func TestGatewayUsageListModels(t *testing.T) {
	db := openTestDB(t)
	usage := NewGatewayUsageLogs(db)
	now := time.Now()
	rows := []GatewayUsageLog{
		{RequestID: "r1", RequestedModel: "grok-4", UpstreamModel: "grok-4", GatewayGroupID: 1, GatewayKeyID: 1, Success: true, CreatedAt: now},
		{RequestID: "r2", RequestedModel: "grok-4", UpstreamModel: "grok-4", GatewayGroupID: 1, GatewayKeyID: 1, Success: true, CreatedAt: now},
		{RequestID: "r3", RequestedModel: "claude-sonnet", UpstreamModel: "claude-sonnet", GatewayGroupID: 1, GatewayKeyID: 2, Success: true, CreatedAt: now},
		{RequestID: "r4", RequestedModel: "gpt-4o", UpstreamModel: "gpt-4o", GatewayGroupID: 2, GatewayKeyID: 3, Success: true, CreatedAt: now},
		{RequestID: "r5", RequestedModel: "", UpstreamModel: "ignored", GatewayGroupID: 1, GatewayKeyID: 1, Success: true, CreatedAt: now},
	}
	for i := range rows {
		if err := usage.Create(&rows[i]); err != nil {
			t.Fatalf("create %d: %v", i, err)
		}
	}

	all, err := usage.ListModels(GatewayUsageQuery{})
	if err != nil {
		t.Fatalf("list all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("want 3 models, got %d %+v", len(all), all)
	}
	if all[0].Model != "grok-4" || all[0].Count != 2 {
		t.Fatalf("first should be grok-4 x2, got %+v", all[0])
	}

	g1, err := usage.ListModels(GatewayUsageQuery{GatewayGroupID: 1})
	if err != nil {
		t.Fatalf("list g1: %v", err)
	}
	if len(g1) != 2 {
		t.Fatalf("group1 want 2 models, got %d %+v", len(g1), g1)
	}

	// model 筛选不应影响下拉聚合
	withModel, err := usage.ListModels(GatewayUsageQuery{Model: "gpt-4o"})
	if err != nil {
		t.Fatalf("list with model filter ignored: %v", err)
	}
	if len(withModel) != 3 {
		t.Fatalf("model filter should be ignored, got %d", len(withModel))
	}
}
