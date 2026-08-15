// Package storage 提供 GORM 仓储与领域模型持久化。
package storage

import (
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	sqliteDriver "github.com/glebarez/sqlite"
	mysqlDriver "gorm.io/driver/mysql"
	postgresDriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type DBDriver string

const (
	DBDriverSQLite   DBDriver = "sqlite"
	DBDriverMySQL    DBDriver = "mysql"
	DBDriverPostgres DBDriver = "postgres"
)

type DBConfig struct {
	Driver                DBDriver
	Path                  string
	Host                  string
	Port                  int
	User                  string
	Password              string
	Name                  string
	SSLMode               string
	ConnectTimeoutSeconds int
	ReadOnly              bool
	MaxOpenConns          int
	MaxIdleConns          int
}

func (c DBConfig) SQLitePath() string {
	if strings.TrimSpace(c.Path) != "" {
		return c.Path
	}
	return "./data/upstream-ops.db"
}

// SQLiteDSN returns the path used by the SQLite dialector. Read-only opens use
// SQLite URI mode so opening a source snapshot never creates a journal or
// attempts to change journal_mode. This is used by the migration helper while
// the live data directory is mounted read-only.
func (c DBConfig) SQLiteDSN() string {
	path := c.SQLitePath()
	if !c.ReadOnly {
		return path
	}
	absPath, err := filepath.Abs(path)
	if err == nil {
		path = absPath
	}
	path = filepath.ToSlash(path)
	// file: URIs require an extra slash before a Windows drive letter. Without
	// it, SQLite treats the drive (`C:`) as the URI authority and rejects the
	// read-only open. UNC paths already begin with two slashes and are kept as
	// network paths.
	if len(path) >= 2 && path[1] == ':' && !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	u := url.URL{Scheme: "file", Path: path}
	query := url.Values{}
	query.Set("mode", "ro")
	u.RawQuery = query.Encode()
	return u.String()
}

func (c DBConfig) MySQLDSN() string {
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		c.User, c.Password, c.Host, c.Port, c.Name,
	)
}

// PostgresDSN returns a URL-form PostgreSQL DSN. URL encoding is important
// here because generated passwords commonly contain characters meaningful to
// libpq (for example @, #, or /).
func (c DBConfig) PostgresDSN() string {
	host := strings.TrimSpace(c.Host)
	if host == "" {
		host = "localhost"
	}
	port := c.Port
	if port <= 0 {
		port = 5432
	}
	database := strings.TrimSpace(c.Name)
	if database == "" {
		database = "upstreamops"
	}
	sslMode := strings.TrimSpace(c.SSLMode)
	if sslMode == "" {
		sslMode = "disable"
	}
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	}
	u := &url.URL{
		Scheme: "postgres",
		Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		Path:   "/" + database,
	}
	if strings.TrimSpace(c.User) != "" {
		u.User = url.UserPassword(c.User, c.Password)
	}
	query := url.Values{}
	query.Set("sslmode", sslMode)
	query.Set("TimeZone", "UTC")
	connectTimeout := c.ConnectTimeoutSeconds
	if connectTimeout <= 0 {
		connectTimeout = 15
	}
	query.Set("connect_timeout", strconv.Itoa(connectTimeout))
	u.RawQuery = query.Encode()
	return u.String()
}

// newGormLogger 关掉 GORM 默认 logger 对 ErrRecordNotFound 的告警噪音。
//
// 业务代码（如 Rates.Upsert）显式处理了"找不到就插入"，这种情况下 GORM 默认仍会
// 把 record not found 当 Warn 打出来，造成日志看起来满是错误其实没问题。
// IgnoreRecordNotFoundError = true 可以静默这类预期内的"未找到"。
func newGormLogger() logger.Interface {
	slowThreshold := time.Second
	if raw := strings.TrimSpace(os.Getenv("UPSTREAM_OPS_SLOW_SQL_MS")); raw != "" {
		if ms, err := strconv.Atoi(raw); err == nil && ms > 0 {
			slowThreshold = time.Duration(ms) * time.Millisecond
		}
	}
	return logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             slowThreshold,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
			ParameterizedQueries:      true,
			Colorful:                  true,
		},
	)
}

func Open(cfg DBConfig) (*gorm.DB, error) {
	driver := DBDriver(strings.ToLower(string(cfg.Driver)))
	if driver == "" {
		driver = DBDriverSQLite
	}

	var dialector gorm.Dialector
	switch driver {
	case DBDriverSQLite:
		path := cfg.SQLitePath()
		if cfg.ReadOnly {
			if _, err := os.Stat(filepath.Dir(path)); err != nil {
				return nil, fmt.Errorf("inspect sqlite directory: %w", err)
			}
		} else if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("create sqlite dir: %w", err)
		}
		dialector = sqliteDriver.Open(cfg.SQLiteDSN())
	case DBDriverMySQL:
		dialector = mysqlDriver.Open(cfg.MySQLDSN())
	case DBDriverPostgres, DBDriver("postgresql"):
		dialector = postgresDriver.Open(cfg.PostgresDSN())
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", cfg.Driver)
	}

	db, err := gorm.Open(dialector, &gorm.Config{
		Logger: newGormLogger(),
	})
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("get sql.DB: %w", err)
	}

	switch driver {
	case DBDriverSQLite:
		if !cfg.ReadOnly {
			if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
				return nil, fmt.Errorf("set sqlite journal mode: %w", err)
			}
		}
		if err := db.Exec("PRAGMA busy_timeout=5000").Error; err != nil {
			return nil, fmt.Errorf("set sqlite busy timeout: %w", err)
		}
		sqlDB.SetMaxOpenConns(1)
		sqlDB.SetMaxIdleConns(1)
	default:
		if cfg.MaxOpenConns <= 0 {
			cfg.MaxOpenConns = 20
		}
		if cfg.MaxIdleConns <= 0 {
			cfg.MaxIdleConns = 5
		}
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	return db, nil
}

// AutoMigrate 启动时自动同步表结构。
func AutoMigrate(db *gorm.DB) error {
	if err := dropDeletedAtColumns(db); err != nil {
		return err
	}
	// 仅 AutoMigrate 当前模型（网关未发布，不做「密钥→组」等历史数据迁移）
	if err := db.AutoMigrate(
		&Channel{},
		&AuthSession{},
		&CaptchaConfig{},
		&RateSnapshot{},
		&RateChangeLog{},
		&UpstreamAnnouncement{},
		&BalanceSnapshot{},
		&CostSnapshot{},
		&NotificationChannel{},
		&NotificationLog{},
		&NotificationCooldown{},
		&MonitorLog{},
		&UpstreamSyncTarget{},
		&UpstreamSyncTargetGroup{},
		&UpstreamSyncGroup{},
		&UpstreamSyncAccount{},
		&UpstreamSyncManagedAccount{},
		&UpstreamSyncLog{},
		&GatewayGroup{},
		&GatewayKey{},
		&GatewayRoute{},
		&GatewayRouteModelCooldown{},
		&GatewayChannelCacheHealth{},
		&GatewayProvider{},
		&GatewayResponseRule{},
		&GatewayUsageLog{},
		&GatewayRequestFinalization{},
		&GatewayWinnerSettlement{},
		&ModelPriceOverride{},
	); err != nil {
		return err
	}
	return ensurePerformanceIndexes(db)
}

// ensurePerformanceIndexes adds the indexes that cannot be expressed by a
// portable GORM tag. In particular, SQLite stores timestamps as RFC3339 text
// while the usage filters compare Unix seconds; the expression index must
// match the WHERE expression exactly for SQLite to use it.
func ensurePerformanceIndexes(db *gorm.DB) error {
	if db == nil {
		return fmt.Errorf("database is nil")
	}
	definitions := []struct {
		name    string
		columns string
	}{
		{name: "idx_gateway_usage_request_key_attempt", columns: "request_id, gateway_key_id, attempt"},
		{name: "idx_gateway_usage_request_key_winner", columns: "request_id, gateway_key_id, winner"},
		{name: "idx_gateway_usage_key_created", columns: "gateway_key_id, created_at"},
		{name: "idx_gateway_usage_group_created", columns: "gateway_group_id, created_at"},
		{name: "idx_gateway_usage_provider_created", columns: "gateway_provider_id, created_at"},
		{name: "idx_gateway_usage_channel_created", columns: "channel_id, created_at"},
	}
	for _, definition := range definitions {
		if err := ensureGatewayUsageIndex(db, definition.name, definition.columns); err != nil {
			return err
		}
	}
	if isSQLite(db) {
		if err := ensureGatewayUsageIndex(db, "idx_gateway_usage_created_unix", "CAST(strftime('%s', created_at) AS INTEGER)"); err != nil {
			return err
		}
		// PRAGMA optimize is bounded and safe on every startup; unlike a full
		// ANALYZE it does not force a large table scan on small deployments.
		if err := db.Exec("PRAGMA optimize").Error; err != nil {
			return fmt.Errorf("optimize sqlite statistics: %w", err)
		}
	}
	return nil
}

// ensureGatewayUsageIndex keeps index creation idempotent across all three
// supported SQL dialects. MySQL does not accept PostgreSQL/SQLite's
// CREATE INDEX IF NOT EXISTS syntax, so check the catalog first there.
func ensureGatewayUsageIndex(db *gorm.DB, name, columns string) error {
	if db.Migrator().HasIndex(&GatewayUsageLog{}, name) {
		return nil
	}
	create := "CREATE INDEX " + name + " ON gateway_usage_logs(" + columns + ")"
	if !strings.EqualFold(db.Dialector.Name(), "mysql") {
		create = "CREATE INDEX IF NOT EXISTS " + name + " ON gateway_usage_logs(" + columns + ")"
	}
	if err := db.Exec(create).Error; err != nil {
		return fmt.Errorf("ensure performance index %s: %w", name, err)
	}
	return nil
}

func dropDeletedAtColumns(db *gorm.DB) error {
	targets := []struct {
		table string
		model any
	}{
		{table: "channels", model: &Channel{}},
		{table: "captcha_configs", model: &CaptchaConfig{}},
		{table: "notification_channels", model: &NotificationChannel{}},
	}

	for _, target := range targets {
		if !db.Migrator().HasTable(target.model) {
			continue
		}
		hasColumn, err := tableHasColumn(db, target.table, "deleted_at")
		if err != nil {
			return fmt.Errorf("inspect %s.deleted_at: %w", target.table, err)
		}
		if !hasColumn {
			continue
		}
		if err := db.Exec("DELETE FROM " + target.table + " WHERE deleted_at IS NOT NULL").Error; err != nil {
			return fmt.Errorf("delete soft-deleted rows from %s: %w", target.table, err)
		}
		if db.Migrator().HasIndex(target.model, "idx_"+target.table+"_deleted_at") {
			if err := db.Migrator().DropIndex(target.model, "idx_"+target.table+"_deleted_at"); err != nil {
				return fmt.Errorf("drop %s deleted_at index: %w", target.table, err)
			}
		}
		if err := db.Migrator().DropColumn(target.model, "deleted_at"); err != nil {
			return fmt.Errorf("drop %s.deleted_at: %w", target.table, err)
		}
		hasColumn, err = tableHasColumn(db, target.table, "deleted_at")
		if err != nil {
			return fmt.Errorf("inspect %s.deleted_at after drop: %w", target.table, err)
		}
		if hasColumn && db.Dialector.Name() == "sqlite" {
			if err := db.Exec("ALTER TABLE " + target.table + " DROP COLUMN deleted_at").Error; err != nil {
				return fmt.Errorf("drop sqlite %s.deleted_at: %w", target.table, err)
			}
		}
	}
	return nil
}

func tableHasColumn(db *gorm.DB, table, column string) (bool, error) {
	columns, err := db.Migrator().ColumnTypes(table)
	if err != nil {
		return false, err
	}
	for _, c := range columns {
		if strings.EqualFold(c.Name(), column) {
			return true, nil
		}
	}
	return false, nil
}
