package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"gorm.io/gorm"
)

type UpgradeAssessment struct {
	Driver         string `json:"driver"`
	DatabasePath   string `json:"database_path,omitempty"`
	UsageRows      int64  `json:"usage_rows"`
	DatabaseBytes  int64  `json:"database_bytes"`
	Recommended    bool   `json:"recommended"`
	Recommendation string `json:"recommendation,omitempty"`
}

const (
	upgradeUsageRowThreshold   = 50000
	upgradeDatabaseMBThreshold = 128
)

// AssessUpgrade is a read-only startup check. It never changes the database;
// it only gives operators an actionable hint when SQLite's single-writer
// queue is likely to become the latency bottleneck.
func AssessUpgrade(cfg DBConfig, db *gorm.DB) UpgradeAssessment {
	assessment := UpgradeAssessment{Driver: string(cfg.Driver), DatabasePath: cfg.SQLitePath()}
	if db == nil || !strings.EqualFold(string(cfg.Driver), string(DBDriverSQLite)) {
		return assessment
	}
	_ = db.Model(&GatewayUsageLog{}).Count(&assessment.UsageRows).Error
	if info, err := os.Stat(filepath.Clean(cfg.SQLitePath())); err == nil {
		assessment.DatabaseBytes = info.Size()
	}
	assessment.Recommended = assessment.UsageRows >= upgradeUsageRowThreshold ||
		assessment.DatabaseBytes >= upgradeDatabaseMBThreshold*1024*1024
	if assessment.Recommended {
		assessment.Recommendation = "run scripts/upgrade-to-postgres.sh (or .ps1) after setting DATABASE_*; the script creates a backup and refuses a non-empty target"
	}
	return assessment
}

// MigrationOptions controls a one-time database copy. The target must be
// empty by default; this is deliberate because silently merging encrypted
// gateway keys or usage rows makes rollback and accounting ambiguous.
type MigrationOptions struct {
	BatchSize      int
	AllowExisting  bool
	SkipMissingTbl bool
}

type MigrationTableReport struct {
	Table      string `json:"table"`
	SourceRows int64  `json:"source_rows"`
	TargetRows int64  `json:"target_rows"`
	Copied     int64  `json:"copied"`
}

type MigrationReport struct {
	Tables []MigrationTableReport `json:"tables"`
}

type migrationModelSpec struct {
	table string
	model any
}

// gatewayMigrationModels is intentionally explicit and follows AutoMigrate's
// model list. Keeping the list here makes the migration auditable and avoids
// copying SQLite's internal tables or unrelated future tables by accident.
var gatewayMigrationModels = []migrationModelSpec{
	{table: "channels", model: &Channel{}},
	{table: "auth_sessions", model: &AuthSession{}},
	{table: "captcha_configs", model: &CaptchaConfig{}},
	{table: "rate_snapshots", model: &RateSnapshot{}},
	{table: "rate_change_logs", model: &RateChangeLog{}},
	{table: "upstream_announcements", model: &UpstreamAnnouncement{}},
	{table: "balance_snapshots", model: &BalanceSnapshot{}},
	{table: "cost_snapshots", model: &CostSnapshot{}},
	{table: "notification_channels", model: &NotificationChannel{}},
	{table: "notification_logs", model: &NotificationLog{}},
	{table: "notification_cooldowns", model: &NotificationCooldown{}},
	{table: "monitor_logs", model: &MonitorLog{}},
	{table: "upstream_sync_targets", model: &UpstreamSyncTarget{}},
	{table: "upstream_sync_target_groups", model: &UpstreamSyncTargetGroup{}},
	{table: "upstream_sync_groups", model: &UpstreamSyncGroup{}},
	{table: "upstream_sync_accounts", model: &UpstreamSyncAccount{}},
	{table: "upstream_sync_managed_accounts", model: &UpstreamSyncManagedAccount{}},
	{table: "upstream_sync_logs", model: &UpstreamSyncLog{}},
	{table: "gateway_groups", model: &GatewayGroup{}},
	{table: "gateway_keys", model: &GatewayKey{}},
	{table: "gateway_routes", model: &GatewayRoute{}},
	{table: "gateway_route_model_cooldowns", model: &GatewayRouteModelCooldown{}},
	{table: "gateway_providers", model: &GatewayProvider{}},
	{table: "gateway_response_rules", model: &GatewayResponseRule{}},
	{table: "gateway_usage_logs", model: &GatewayUsageLog{}},
	{table: "gateway_request_finalizations", model: &GatewayRequestFinalization{}},
	{table: "gateway_winner_settlements", model: &GatewayWinnerSettlement{}},
	{table: "model_price_overrides", model: &ModelPriceOverride{}},
}

var migrationSoftDeleteTables = map[string]struct{}{
	"channels":              {},
	"captcha_configs":       {},
	"notification_channels": {},
}

// MigrateDatabase copies all known application tables from source to target.
// It is suitable for SQLite -> PostgreSQL and also supports SQLite -> SQLite
// in tests. The source is never modified.
func MigrateDatabase(source, target *gorm.DB, options MigrationOptions) (*MigrationReport, error) {
	if source == nil || target == nil {
		return nil, fmt.Errorf("source and target databases are required")
	}
	if options.BatchSize <= 0 {
		options.BatchSize = 500
	}
	if !options.AllowExisting {
		if err := rejectUnexpectedTargetRows(target); err != nil {
			return nil, err
		}
		// Reject populated application tables before AutoMigrate changes the
		// target schema.  This keeps a failed upgrade non-destructive and gives
		// operators the same result even when an older target lacks some tables.
		for _, spec := range gatewayMigrationModels {
			hasTarget := target.Migrator().HasTable(spec.model)
			if !hasTarget {
				continue
			}
			var count int64
			if err := target.Table(spec.table).Count(&count).Error; err != nil {
				return nil, fmt.Errorf("count target table %s: %w", spec.table, err)
			}
			if count > 0 {
				return nil, fmt.Errorf("target table %s is not empty (%d rows); use an empty database or explicitly allow existing data", spec.table, count)
			}
		}
	}
	if err := AutoMigrate(target); err != nil {
		return nil, fmt.Errorf("migrate target schema: %w", err)
	}

	// Build the complete plan before inserting anything.  Older SQLite files
	// may not have every table, while an existing target must still be checked
	// even when its corresponding source table is absent.  Doing this in a
	// separate pass prevents a late non-empty-table error from leaving a
	// partially migrated target behind.
	plan := make([]migrationPlanItem, 0, len(gatewayMigrationModels))
	for _, spec := range gatewayMigrationModels {
		hasSource := source.Migrator().HasTable(spec.model)
		hasTarget := target.Migrator().HasTable(spec.model)
		if !hasTarget {
			return nil, fmt.Errorf("target table %s is missing after migration", spec.table)
		}
		var sourceCount int64
		if hasSource {
			sourceQuery, err := migrationSourceQuery(source, spec.table)
			if err != nil {
				return nil, err
			}
			if err := sourceQuery.Count(&sourceCount).Error; err != nil {
				return nil, fmt.Errorf("count source table %s: %w", spec.table, err)
			}
		} else if !options.SkipMissingTbl {
			return nil, fmt.Errorf("source table %s is missing", spec.table)
		}
		var targetCount int64
		if err := target.Table(spec.table).Count(&targetCount).Error; err != nil {
			return nil, fmt.Errorf("count target table %s: %w", spec.table, err)
		}
		if targetCount > 0 && !options.AllowExisting {
			return nil, fmt.Errorf("target table %s is not empty (%d rows); use an empty database or explicitly allow existing data", spec.table, targetCount)
		}
		plan = append(plan, migrationPlanItem{
			spec: spec, hasSource: hasSource, sourceRows: sourceCount, targetRows: targetCount,
		})
	}

	report := &MigrationReport{Tables: make([]MigrationTableReport, 0, len(plan))}
	if err := target.Transaction(func(tx *gorm.DB) error {
		for _, item := range plan {
			if !item.hasSource {
				// The target schema is already current; there is no source data
				// to copy for a table introduced after the old SQLite version.
				report.Tables = append(report.Tables, MigrationTableReport{
					Table: item.spec.table, SourceRows: 0, TargetRows: item.targetRows, Copied: 0,
				})
				continue
			}
			copied, err := copyTableRows(source, tx, item.spec.table, options.BatchSize)
			if err != nil {
				return err
			}
			var finalCount int64
			if err := tx.Table(item.spec.table).Count(&finalCount).Error; err != nil {
				return fmt.Errorf("count migrated table %s: %w", item.spec.table, err)
			}
			if finalCount != item.sourceRows+item.targetRows {
				return fmt.Errorf("row count mismatch for %s: source=%d existing=%d target=%d", item.spec.table, item.sourceRows, item.targetRows, finalCount)
			}
			report.Tables = append(report.Tables, MigrationTableReport{
				Table: item.spec.table, SourceRows: item.sourceRows, TargetRows: finalCount, Copied: copied,
			})
		}
		return resetPostgresSequences(tx, report)
	}); err != nil {
		return nil, fmt.Errorf("copy migration transaction: %w", err)
	}
	return report, nil
}

// rejectUnexpectedTargetRows protects the "new empty database" contract from
// silently leaving unrelated data behind.  SQLite's internal sqlite_* tables
// are metadata and are intentionally ignored.  An empty auxiliary table is
// harmless; only rows cause a refusal.
func rejectUnexpectedTargetRows(db *gorm.DB) error {
	tables, err := db.Migrator().GetTables()
	if err != nil {
		return fmt.Errorf("inspect target tables: %w", err)
	}
	known := make(map[string]struct{}, len(gatewayMigrationModels))
	for _, spec := range gatewayMigrationModels {
		known[strings.ToLower(spec.table)] = struct{}{}
	}
	for _, table := range tables {
		lower := strings.ToLower(strings.TrimSpace(table))
		if strings.HasPrefix(lower, "sqlite_") {
			continue
		}
		if _, ok := known[lower]; ok {
			continue
		}
		var marker int
		result := db.Table(table).Select("1").Limit(1).Scan(&marker)
		if result.Error != nil {
			return fmt.Errorf("inspect target table %s: %w", table, result.Error)
		}
		if result.RowsAffected > 0 {
			return fmt.Errorf("target table %s contains data; use a dedicated empty database or explicitly allow existing data", table)
		}
	}
	return nil
}

type migrationPlanItem struct {
	spec       migrationModelSpec
	hasSource  bool
	sourceRows int64
	targetRows int64
}

func copyTableRows(source, target *gorm.DB, table string, batchSize int) (int64, error) {
	sourceQuery, err := migrationSourceQuery(source, table)
	if err != nil {
		return 0, err
	}
	rows, err := sourceQuery.Rows()
	if err != nil {
		return 0, fmt.Errorf("read source table %s: %w", table, err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return 0, fmt.Errorf("read columns for %s: %w", table, err)
	}
	targetColumns, err := targetColumnInfo(target, table)
	if err != nil {
		return 0, fmt.Errorf("inspect target columns for %s: %w", table, err)
	}
	if len(targetColumns) == 0 {
		return 0, fmt.Errorf("target table %s has no columns", table)
	}
	batch := make([]map[string]any, 0, batchSize)
	var copied int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := target.Table(table).CreateInBatches(batch, len(batch)).Error; err != nil {
			return fmt.Errorf("insert table %s: %w", table, err)
		}
		copied += int64(len(batch))
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return copied, fmt.Errorf("scan table %s: %w", table, err)
		}
		record := make(map[string]any, len(columns))
		for i, column := range columns {
			info, ok := targetColumns[strings.ToLower(column)]
			if !ok {
				continue
			}
			value, err := normalizeMigrationValue(values[i], info.databaseType, target.Dialector.Name())
			if err != nil {
				return copied, fmt.Errorf("normalize %s.%s: %w", table, info.name, err)
			}
			record[info.name] = value
		}
		if len(record) == 0 {
			return copied, fmt.Errorf("source table %s has no columns compatible with target", table)
		}
		batch = append(batch, record)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return copied, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return copied, fmt.Errorf("iterate table %s: %w", table, err)
	}
	if err := flush(); err != nil {
		return copied, err
	}
	return copied, nil
}

type migrationColumnInfo struct {
	name         string
	databaseType string
}

func migrationSourceQuery(db *gorm.DB, table string) (*gorm.DB, error) {
	if db == nil {
		return nil, fmt.Errorf("source database is required")
	}
	query := db.Table(table)
	if _, ok := migrationSoftDeleteTables[strings.ToLower(strings.TrimSpace(table))]; !ok {
		return query, nil
	}
	hasDeletedAt, err := tableHasColumn(db, table, "deleted_at")
	if err != nil {
		return nil, fmt.Errorf("inspect %s.deleted_at: %w", table, err)
	}
	if hasDeletedAt {
		query = query.Where("deleted_at IS NULL")
	}
	return query, nil
}

func targetColumnInfo(db *gorm.DB, table string) (map[string]migrationColumnInfo, error) {
	columns, err := db.Migrator().ColumnTypes(table)
	if err != nil {
		return nil, err
	}
	result := make(map[string]migrationColumnInfo, len(columns))
	for _, column := range columns {
		name := column.Name()
		result[strings.ToLower(name)] = migrationColumnInfo{
			name: name, databaseType: strings.ToLower(strings.TrimSpace(column.DatabaseTypeName())),
		}
	}
	return result, nil
}

// SQLite stores boolean fields as INTEGER values.  PostgreSQL's bool codec
// does not accept an int64 parameter, so normalize that one cross-driver type
// explicitly while leaving all other values to the database driver.
func normalizeMigrationValue(value any, databaseType, driver string) (any, error) {
	if !strings.EqualFold(driver, string(DBDriverPostgres)) && !strings.EqualFold(driver, "postgresql") {
		return value, nil
	}
	typeName := strings.ToLower(strings.TrimSpace(databaseType))
	if typeName != "bool" && typeName != "boolean" {
		return value, nil
	}
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return typed, nil
	case int:
		return typed != 0, nil
	case int8:
		return typed != 0, nil
	case int16:
		return typed != 0, nil
	case int32:
		return typed != 0, nil
	case int64:
		return typed != 0, nil
	case uint:
		return typed != 0, nil
	case uint8:
		return typed != 0, nil
	case uint16:
		return typed != 0, nil
	case uint32:
		return typed != 0, nil
	case uint64:
		return typed != 0, nil
	case float32:
		return typed != 0, nil
	case float64:
		return typed != 0, nil
	case []byte:
		return parseMigrationBool(string(typed))
	case string:
		return parseMigrationBool(typed)
	default:
		return nil, fmt.Errorf("unsupported boolean value type %T", value)
	}
}

func parseMigrationBool(raw string) (bool, error) {
	raw = strings.TrimSpace(raw)
	if raw == "0" {
		return false, nil
	}
	if raw == "1" {
		return true, nil
	}
	parsed, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("invalid boolean value %q", raw)
	}
	return parsed, nil
}

func resetPostgresSequences(db *gorm.DB, report *MigrationReport) error {
	if db == nil || !strings.EqualFold(db.Dialector.Name(), "postgres") {
		return nil
	}
	for _, item := range report.Tables {
		// HasColumn(string, ...) is not implemented consistently by every
		// GORM dialect (notably PostgreSQL). The migration target schema is
		// already known here, so use the portable ColumnTypes path instead.
		columns, err := targetColumnInfo(db, item.Table)
		if err != nil {
			return fmt.Errorf("inspect target columns for %s: %w", item.Table, err)
		}
		if _, ok := columns["id"]; !ok {
			continue
		}
		var sequence string
		if err := db.Raw(
			"SELECT COALESCE(pg_get_serial_sequence(?, 'id'), '')",
			item.Table,
		).Scan(&sequence).Error; err != nil {
			return fmt.Errorf("find sequence for %s: %w", item.Table, err)
		}
		if strings.TrimSpace(sequence) == "" {
			continue
		}

		// The table names come from the fixed allow-list above.  The sequence
		// name is returned by PostgreSQL itself and is passed as a parameter.
		table := `"` + strings.ReplaceAll(item.Table, `"`, `""`) + `"`
		query := fmt.Sprintf(
			"SELECT setval(?::regclass, COALESCE(MAX(\"id\"), 1), COUNT(*) > 0) FROM %s",
			table,
		)
		if err := db.Exec(query, sequence).Error; err != nil {
			return fmt.Errorf("reset sequence for %s: %w", item.Table, err)
		}
	}
	return nil
}
