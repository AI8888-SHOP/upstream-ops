package storage

import (
	"path/filepath"
	"testing"
	"time"
)

func TestMigrateDatabaseChecksMissingSourceTableTargetRowsBeforeCopy(t *testing.T) {
	source := openTestDB(t)
	if err := source.Migrator().DropTable(&ModelPriceOverride{}); err != nil {
		t.Fatalf("drop old-version table: %v", err)
	}
	group := &GatewayGroup{Name: "preflight-group", Status: GatewayGroupStatusActive}
	if err := source.Create(group).Error; err != nil {
		t.Fatalf("seed source group: %v", err)
	}

	target, err := Open(DBConfig{Driver: DBDriverSQLite, Path: filepath.Join(t.TempDir(), "target.db")})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	targetSQL, err := target.DB()
	if err != nil {
		t.Fatalf("target sql db: %v", err)
	}
	t.Cleanup(func() { _ = targetSQL.Close() })
	if err := AutoMigrate(target); err != nil {
		t.Fatalf("migrate target schema: %v", err)
	}
	if err := target.Create(&ModelPriceOverride{ModelName: "already-there"}).Error; err != nil {
		t.Fatalf("seed target row: %v", err)
	}

	if _, err := MigrateDatabase(source, target, MigrationOptions{SkipMissingTbl: true}); err == nil {
		t.Fatal("migration with a populated target table unexpectedly succeeded")
	}
	var copied GatewayGroup
	if err := target.Where("name = ?", group.Name).First(&copied).Error; err == nil {
		t.Fatal("preflight failure copied source rows before returning")
	}
}

func TestNormalizeMigrationValuePostgresBoolean(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want bool
	}{
		{name: "integer zero", in: int64(0), want: false},
		{name: "integer one", in: int64(1), want: true},
		{name: "text false", in: []byte("false"), want: false},
		{name: "text true", in: "true", want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeMigrationValue(tt.in, "boolean", "postgres")
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			value, ok := got.(bool)
			if !ok || value != tt.want {
				t.Fatalf("normalized value = %#v, want bool(%v)", got, tt.want)
			}
		})
	}
}

func TestAutoMigrateAddsVirtualCacheFlagWithSafeDefault(t *testing.T) {
	db := openTestDB(t)
	group := &GatewayGroup{Name: "legacy-virtual-cache", Status: GatewayGroupStatusActive}
	if err := db.Create(group).Error; err != nil {
		t.Fatalf("create group: %v", err)
	}
	if err := db.Migrator().DropColumn(&GatewayGroup{}, "hedge_virtual_cache_enabled"); err != nil {
		t.Fatalf("drop virtual cache column: %v", err)
	}
	if err := db.Migrator().DropColumn(&GatewayGroup{}, "response_validation_virtual_cache_enabled"); err != nil {
		t.Fatalf("drop response validation virtual cache column: %v", err)
	}
	if db.Migrator().HasColumn(&GatewayGroup{}, "hedge_virtual_cache_enabled") {
		t.Fatal("virtual cache column was not removed from legacy schema")
	}
	if err := AutoMigrate(db); err != nil {
		t.Fatalf("auto migrate legacy schema: %v", err)
	}
	if !db.Migrator().HasColumn(&GatewayGroup{}, "hedge_virtual_cache_enabled") {
		t.Fatal("auto migrate did not restore virtual cache column")
	}
	if !db.Migrator().HasColumn(&GatewayGroup{}, "response_validation_virtual_cache_enabled") {
		t.Fatal("auto migrate did not restore response validation virtual cache column")
	}
	var restored GatewayGroup
	if err := db.First(&restored, group.ID).Error; err != nil {
		t.Fatalf("load migrated group: %v", err)
	}
	if restored.HedgeVirtualCacheEnabled || restored.ResponseValidationVirtualCacheEnabled {
		t.Fatalf("legacy group virtual cache flag = true, want safe default false")
	}
}

func TestMigrateDatabaseDoesNotRestoreSoftDeletedRows(t *testing.T) {
	source := openTestDB(t)
	if err := source.Exec("ALTER TABLE channels ADD COLUMN deleted_at datetime").Error; err != nil {
		t.Fatalf("add legacy deleted_at: %v", err)
	}
	active := &Channel{
		Name: "migration-active-channel", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
		MonitorEnabled: true,
	}
	deleted := &Channel{
		Name: "migration-deleted-channel", Type: ChannelTypeNewAPI,
		SiteURL: "https://example.com", Username: "u", PasswordCipher: "x",
		MonitorEnabled: true,
	}
	if err := source.Create(active).Error; err != nil {
		t.Fatalf("create active channel: %v", err)
	}
	if err := source.Create(deleted).Error; err != nil {
		t.Fatalf("create deleted channel: %v", err)
	}
	if err := source.Table("channels").Where("id = ?", deleted.ID).Update("deleted_at", time.Now()).Error; err != nil {
		t.Fatalf("mark deleted channel: %v", err)
	}

	target, err := Open(DBConfig{Driver: DBDriverSQLite, Path: filepath.Join(t.TempDir(), "target.db")})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	targetSQL, err := target.DB()
	if err != nil {
		t.Fatalf("target sql db: %v", err)
	}
	t.Cleanup(func() { _ = targetSQL.Close() })
	if _, err := MigrateDatabase(source, target, MigrationOptions{SkipMissingTbl: true}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var count int64
	if err := target.Model(&Channel{}).Count(&count).Error; err != nil {
		t.Fatalf("count migrated channels: %v", err)
	}
	if count != 1 {
		t.Fatalf("migrated channel count = %d, want 1", count)
	}
	var restored Channel
	if err := target.Where("name = ?", deleted.Name).First(&restored).Error; err == nil {
		t.Fatal("soft-deleted channel was restored by migration")
	}
}

func TestMigrateDatabaseCopiesKnownTablesAndRejectsNonEmptyTarget(t *testing.T) {
	source := openTestDB(t)
	group := &GatewayGroup{Name: "migration-group", Status: GatewayGroupStatusActive}
	if err := source.Create(group).Error; err != nil {
		t.Fatalf("seed group: %v", err)
	}
	key := &GatewayKey{
		GroupID: group.ID, Name: "migration-key", KeyHash: "migration-hash",
		KeyPrefix: "sk-", KeyCipher: "cipher", Status: GatewayKeyStatusActive,
	}
	if err := source.Create(key).Error; err != nil {
		t.Fatalf("seed key: %v", err)
	}
	usage := &GatewayUsageLog{
		GatewayGroupID: group.ID, GatewayKeyID: key.ID, RequestID: "migration-request",
		Attempt: 1, Success: true, Winner: true, ActualCost: 0.25,
	}
	if err := source.Create(usage).Error; err != nil {
		t.Fatalf("seed usage: %v", err)
	}

	target, err := Open(DBConfig{Driver: DBDriverSQLite, Path: filepath.Join(t.TempDir(), "target.db")})
	if err != nil {
		t.Fatalf("open target: %v", err)
	}
	targetSQL, err := target.DB()
	if err != nil {
		t.Fatalf("target sql db: %v", err)
	}
	t.Cleanup(func() { _ = targetSQL.Close() })
	report, err := MigrateDatabase(source, target, MigrationOptions{SkipMissingTbl: true, BatchSize: 2})
	if err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if len(report.Tables) == 0 {
		t.Fatal("migration report is empty")
	}
	var gotGroup GatewayGroup
	if err := target.Where("name = ?", group.Name).First(&gotGroup).Error; err != nil {
		t.Fatalf("load migrated group: %v", err)
	}
	var gotUsage GatewayUsageLog
	if err := target.Where("request_id = ?", usage.RequestID).First(&gotUsage).Error; err != nil {
		t.Fatalf("load migrated usage: %v", err)
	}
	if gotUsage.ActualCost != usage.ActualCost || !gotUsage.Winner {
		t.Fatalf("migrated usage = %+v, want cost/winner preserved", gotUsage)
	}
	if _, err := MigrateDatabase(source, target, MigrationOptions{SkipMissingTbl: true}); err == nil {
		t.Fatal("second migration into non-empty target unexpectedly succeeded")
	}
}
