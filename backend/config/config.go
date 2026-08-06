// Package config 定义应用配置结构体与默认值（含 gateway 运行时参数）。
package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/bejix/upstream-ops/backend/storage"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

type Config struct {
	App           AppConfig           `mapstructure:"app" yaml:"app" json:"app"`
	Server        ServerConfig        `mapstructure:"server" yaml:"server" json:"server"`
	Database      DatabaseConfig      `mapstructure:"database" yaml:"database" json:"database"`
	Security      SecurityConfig      `mapstructure:"security" yaml:"security" json:"security"`
	Auth          AuthConfig          `mapstructure:"auth" yaml:"auth" json:"auth"`
	Scheduler     SchedulerConfig     `mapstructure:"scheduler" yaml:"scheduler" json:"scheduler"`
	Notifications NotificationsConfig `mapstructure:"notifications" yaml:"notifications" json:"notifications"`
	Proxy         ProxyConfig         `mapstructure:"proxy" yaml:"proxy" json:"proxy"`
	Upstream      UpstreamConfig      `mapstructure:"upstream" yaml:"upstream" json:"upstream"`
	Gateway       GatewayConfig       `mapstructure:"gateway" yaml:"gateway" json:"gateway"`
	Log           LogConfig           `mapstructure:"log" yaml:"log" json:"log"`
}

type AppConfig struct {
	Title              string `mapstructure:"title" yaml:"title" json:"title"`
	NotificationPrefix string `mapstructure:"notificationPrefix" yaml:"notificationPrefix" json:"notificationPrefix"`
}

type ServerConfig struct {
	Port           int      `mapstructure:"port" yaml:"port" json:"port"`
	Mode           string   `mapstructure:"mode" yaml:"mode" json:"mode"`
	TrustedProxies []string `mapstructure:"trustedProxies" yaml:"trustedProxies" json:"trustedProxies"`
	BaseURL        string   `mapstructure:"baseURL" yaml:"baseURL" json:"baseURL"`
}

type DatabaseConfig struct {
	Driver       string `mapstructure:"driver" yaml:"driver" json:"driver"`
	Path         string `mapstructure:"path" yaml:"path" json:"path"`
	Host         string `mapstructure:"host" yaml:"host" json:"host"`
	Port         int    `mapstructure:"port" yaml:"port" json:"port"`
	User         string `mapstructure:"user" yaml:"user" json:"user"`
	Password     string `mapstructure:"password" yaml:"password" json:"password"`
	Name         string `mapstructure:"name" yaml:"name" json:"name"`
	SSLMode      string `mapstructure:"sslMode" yaml:"sslMode" json:"sslMode"`
	MaxOpenConns int    `mapstructure:"maxOpenConns" yaml:"maxOpenConns" json:"maxOpenConns"`
	MaxIdleConns int    `mapstructure:"maxIdleConns" yaml:"maxIdleConns" json:"maxIdleConns"`
}

func (d DatabaseConfig) ToStorageConfig() storage.DBConfig {
	port := d.Port
	if port <= 0 {
		if strings.EqualFold(strings.TrimSpace(d.Driver), string(storage.DBDriverPostgres)) ||
			strings.EqualFold(strings.TrimSpace(d.Driver), "postgresql") {
			port = 5432
		} else {
			port = 3306
		}
	}
	return storage.DBConfig{
		Driver:       storage.DBDriver(d.Driver),
		Path:         d.Path,
		Host:         d.Host,
		Port:         port,
		User:         d.User,
		Password:     d.Password,
		Name:         d.Name,
		SSLMode:      d.SSLMode,
		MaxOpenConns: d.MaxOpenConns,
		MaxIdleConns: d.MaxIdleConns,
	}
}

type SecurityConfig struct {
	// AppSecret 主密钥，用于 AES-GCM。优先从 APP_SECRET 环境变量读取。
	AppSecret string `mapstructure:"appSecret" yaml:"appSecret" json:"appSecret"`
}

// AuthConfig 后台单用户登录配置。
//
// Enabled = false（默认）时整套鉴权被关掉：/api/* 全部免 token，前端检测后跳过登录页。
// 适合纯内网 / 反代后面的部署。需要公网暴露时必须显式 Enabled=true 并设强密码。
//
// Enabled=true 时 Username/Password 是写死的管理员凭据，TokenSecret 用于签发 HMAC token。
// 如果 TokenSecret 为空，会回退使用 Security.AppSecret，保证有合理默认。
type AuthConfig struct {
	Enabled         bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	Username        string `mapstructure:"username" yaml:"username" json:"username"`
	Password        string `mapstructure:"password" yaml:"password" json:"password"`
	TokenSecret     string `mapstructure:"tokenSecret" yaml:"tokenSecret" json:"tokenSecret"`
	SessionTTLHours int    `mapstructure:"sessionTTLHours" yaml:"sessionTTLHours" json:"sessionTTLHours"`
}

type SchedulerConfig struct {
	BalanceCron string          `mapstructure:"balanceCron" yaml:"balanceCron" json:"balanceCron"`
	RateCron    string          `mapstructure:"rateCron" yaml:"rateCron" json:"rateCron"`
	Concurrency int             `mapstructure:"concurrency" yaml:"concurrency" json:"concurrency"`
	Retention   RetentionConfig `mapstructure:"retention" yaml:"retention" json:"retention"`
}

// RetentionConfig 历史数据保留策略。
//
// 字段为 0 表示该表不清理，永久保留（默认 rate_change_logs 永远保留，是核心业务数据）。
// Cron 为空时不启动清理任务。
type RetentionConfig struct {
	Cron                 string `mapstructure:"cron" yaml:"cron" json:"cron"`
	MonitorLogsDays      int    `mapstructure:"monitorLogsDays" yaml:"monitorLogsDays" json:"monitorLogsDays"`
	BalanceSnapshotsDays int    `mapstructure:"balanceSnapshotsDays" yaml:"balanceSnapshotsDays" json:"balanceSnapshotsDays"`
	NotificationLogsDays int    `mapstructure:"notificationLogsDays" yaml:"notificationLogsDays" json:"notificationLogsDays"`
	AnnouncementsDays    int    `mapstructure:"announcementsDays" yaml:"announcementsDays" json:"announcementsDays"`
}

// NotificationsConfig 通知去抖策略。所有字段都是"少烦我"取向，默认不丢消息只合并。
//
//   - BatchRateChanges：同次扫描中将多个分组的变化合并成 1 条消息，避免上游一次大调价
//     瞬间发出 30+ 条通知刷屏。默认 true。
//   - MinChangePct：涨跌幅 < X% 的 rate_changed 跳过推送（仍会写入 rate_change_logs）。
//     0 = 全发，对应原始行为。
//   - BalanceLowCooldownMinutes：同一渠道的 balance_low 在 X 分钟内不重复推送。
//     0 = 不冷却（每次扫描发现仍 < 阈值都发）。冷却状态持久化在数据库的
//     notification_cooldowns 表，跨重启生效。
//   - SendMaxAttempts：单条通知发送失败时最多尝试次数（含首次）。
//     1 = 不重试。重试采用指数退避：1s / 2s / 4s …，上限 30s。
type NotificationsConfig struct {
	BatchRateChanges                         bool    `mapstructure:"batchRateChanges" yaml:"batchRateChanges" json:"batchRateChanges"`
	MinChangePct                             float64 `mapstructure:"minChangePct" yaml:"minChangePct" json:"minChangePct"`
	BalanceLowCooldownMinutes                int     `mapstructure:"balanceLowCooldownMinutes" yaml:"balanceLowCooldownMinutes" json:"balanceLowCooldownMinutes"`
	SubscriptionDailyRemainingThresholdPct   float64 `mapstructure:"subscriptionDailyRemainingThresholdPct" yaml:"subscriptionDailyRemainingThresholdPct" json:"subscriptionDailyRemainingThresholdPct"`
	SubscriptionWeeklyRemainingThresholdPct  float64 `mapstructure:"subscriptionWeeklyRemainingThresholdPct" yaml:"subscriptionWeeklyRemainingThresholdPct" json:"subscriptionWeeklyRemainingThresholdPct"`
	SubscriptionMonthlyRemainingThresholdPct float64 `mapstructure:"subscriptionMonthlyRemainingThresholdPct" yaml:"subscriptionMonthlyRemainingThresholdPct" json:"subscriptionMonthlyRemainingThresholdPct"`
	SubscriptionExpiryThresholdHours         int     `mapstructure:"subscriptionExpiryThresholdHours" yaml:"subscriptionExpiryThresholdHours" json:"subscriptionExpiryThresholdHours"`
	SubscriptionAlertCooldownMinutes         int     `mapstructure:"subscriptionAlertCooldownMinutes" yaml:"subscriptionAlertCooldownMinutes" json:"subscriptionAlertCooldownMinutes"`
	SendMaxAttempts                          int     `mapstructure:"sendMaxAttempts" yaml:"sendMaxAttempts" json:"sendMaxAttempts"`
}

type ProxyConfig struct {
	Enabled             bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	VersionCheckEnabled bool   `mapstructure:"versionCheckEnabled" yaml:"versionCheckEnabled" json:"versionCheckEnabled"`
	Protocol            string `mapstructure:"protocol" yaml:"protocol" json:"protocol"`
	Host                string `mapstructure:"host" yaml:"host" json:"host"`
	Port                int    `mapstructure:"port" yaml:"port" json:"port"`
	Username            string `mapstructure:"username" yaml:"username" json:"username"`
	Password            string `mapstructure:"password" yaml:"password" json:"password"`
}

const (
	DefaultUpstreamTimeoutSeconds = 30
	DefaultUpstreamUserAgent      = "upstream-ops/0.1"

	// 网关默认值（设置页可改；0/空表示使用下列默认）。
	DefaultGatewayTempPauseSeconds            = 30
	DefaultGatewayForwardTimeoutSeconds       = 600 // 10 分钟
	DefaultGatewayModelsCacheTTLSeconds       = 60
	DefaultGatewayMaxFailoverSwitches         = 8
	DefaultGatewayRouteBatchConcurrency       = 8
	DefaultGatewayUsageErrorBodyBytes         = 32 * 1024
	DefaultGatewayUsageErrorMsgRunes          = 500
	DefaultGatewayUsageErrorHeaderValueRunes  = 8 * 1024
	DefaultGatewayUsageErrorHeadersJSONBytes  = 64 * 1024
	DefaultGatewayHedgeDelaySeconds           = 10.0
	DefaultGatewayHedgeMaxParallel            = 2
	DefaultGatewayHedgeMaxAttempts            = 4
	MaxGatewayHedgeParallel                   = 32
	MaxGatewayHedgeAttempts                   = 64
	DefaultGatewayResponseValidationMode      = "prefix"
	DefaultGatewayResponseValidationBytes     = 8 * 1024
	DefaultGatewayResponseValidationTimeoutMS = 2 * 1000
)

type UpstreamConfig struct {
	TimeoutSeconds int    `mapstructure:"timeoutSeconds" yaml:"timeoutSeconds" json:"timeoutSeconds"`
	UserAgent      string `mapstructure:"userAgent" yaml:"userAgent" json:"userAgent"`
}

func (u UpstreamConfig) WithDefaults() UpstreamConfig {
	if u.TimeoutSeconds <= 0 {
		u.TimeoutSeconds = DefaultUpstreamTimeoutSeconds
	}
	if strings.TrimSpace(u.UserAgent) == "" {
		u.UserAgent = DefaultUpstreamUserAgent
	}
	return u
}

// GatewayConfig 网关运行时参数（转发超时、批量运维并发、用量错误落库截断等）。
// 可在设置页保存并「应用配置」后立即生效；字段 ≤0 时回退默认值。
type GatewayConfig struct {
	// TempPauseSeconds 新建组默认临时暂停时长（秒），对应路由冷却。
	TempPauseSeconds int `mapstructure:"tempPauseSeconds" yaml:"tempPauseSeconds" json:"tempPauseSeconds"`
	// ForwardTimeoutSeconds 单次上游转发/流式 drain 超时（秒）。
	ForwardTimeoutSeconds int `mapstructure:"forwardTimeoutSeconds" yaml:"forwardTimeoutSeconds" json:"forwardTimeoutSeconds"`
	// ModelsCacheTTLSeconds 公开 /v1/models 列表缓存 TTL（秒）。
	ModelsCacheTTLSeconds int `mapstructure:"modelsCacheTTLSeconds" yaml:"modelsCacheTTLSeconds" json:"modelsCacheTTLSeconds"`
	// MaxFailoverSwitches 新建组默认最大顺延切换次数。
	MaxFailoverSwitches int `mapstructure:"maxFailoverSwitches" yaml:"maxFailoverSwitches" json:"maxFailoverSwitches"`
	// RouteBatchConcurrency 批量运维并发（探测模型 / ensure 密钥 / 同步模型 / 拉源分组）。
	RouteBatchConcurrency int `mapstructure:"routeBatchConcurrency" yaml:"routeBatchConcurrency" json:"routeBatchConcurrency"`
	// UsageError* 用量错误明细落库截断上限（字节或 rune）。
	UsageErrorBodyBytes        int `mapstructure:"usageErrorBodyBytes" yaml:"usageErrorBodyBytes" json:"usageErrorBodyBytes"`
	UsageErrorMsgRunes         int `mapstructure:"usageErrorMsgRunes" yaml:"usageErrorMsgRunes" json:"usageErrorMsgRunes"`
	UsageErrorHeaderValueRunes int `mapstructure:"usageErrorHeaderValueRunes" yaml:"usageErrorHeaderValueRunes" json:"usageErrorHeaderValueRunes"`
	UsageErrorHeadersJSONBytes int `mapstructure:"usageErrorHeadersJSONBytes" yaml:"usageErrorHeadersJSONBytes" json:"usageErrorHeadersJSONBytes"`
	// Hedge / ResponseValidation 是新建 GatewayGroup 的默认策略；组内保存后独立生效。
	Hedge              GatewayHedgeConfig              `mapstructure:"hedge" yaml:"hedge" json:"hedge"`
	ResponseValidation GatewayResponseValidationConfig `mapstructure:"responseValidation" yaml:"responseValidation" json:"responseValidation"`
}

type GatewayHedgeConfig struct {
	Enabled      bool    `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	DelaySeconds float64 `mapstructure:"delaySeconds" yaml:"delaySeconds" json:"delaySeconds"`
	MaxParallel  int     `mapstructure:"maxParallel" yaml:"maxParallel" json:"maxParallel"`
	MaxAttempts  int     `mapstructure:"maxAttempts" yaml:"maxAttempts" json:"maxAttempts"`
}

func (h GatewayHedgeConfig) WithDefaults() GatewayHedgeConfig {
	if math.IsNaN(h.DelaySeconds) || math.IsInf(h.DelaySeconds, 0) || h.DelaySeconds <= 0 {
		h.DelaySeconds = DefaultGatewayHedgeDelaySeconds
	}
	if h.DelaySeconds < 0.1 {
		h.DelaySeconds = 0.1
	}
	if h.DelaySeconds > 300 {
		h.DelaySeconds = 300
	}
	if h.MaxParallel <= 0 {
		h.MaxParallel = DefaultGatewayHedgeMaxParallel
	}
	if h.MaxParallel > MaxGatewayHedgeParallel {
		h.MaxParallel = MaxGatewayHedgeParallel
	}
	if h.MaxAttempts <= 0 {
		h.MaxAttempts = DefaultGatewayHedgeMaxAttempts
	}
	if h.MaxAttempts > MaxGatewayHedgeAttempts {
		h.MaxAttempts = MaxGatewayHedgeAttempts
	}
	if h.MaxAttempts < h.MaxParallel {
		h.MaxAttempts = h.MaxParallel
	}
	return h
}

func (h GatewayHedgeConfig) Delay() time.Duration {
	h = h.WithDefaults()
	return time.Duration(h.DelaySeconds * float64(time.Second))
}

type GatewayResponseValidationConfig struct {
	Enabled         bool   `mapstructure:"enabled" yaml:"enabled" json:"enabled"`
	StreamMode      string `mapstructure:"streamMode" yaml:"streamMode" json:"streamMode"`
	PrefixBytes     int    `mapstructure:"prefixBytes" yaml:"prefixBytes" json:"prefixBytes"`
	PrefixTimeoutMS int    `mapstructure:"prefixTimeoutMs" yaml:"prefixTimeoutMs" json:"prefixTimeoutMs"`
}

func (v GatewayResponseValidationConfig) WithDefaults() GatewayResponseValidationConfig {
	v.StreamMode = strings.ToLower(strings.TrimSpace(v.StreamMode))
	if v.StreamMode != DefaultGatewayResponseValidationMode {
		v.StreamMode = DefaultGatewayResponseValidationMode
	}
	if v.PrefixBytes <= 0 {
		v.PrefixBytes = DefaultGatewayResponseValidationBytes
	}
	if v.PrefixBytes < 1024 {
		v.PrefixBytes = 1024
	}
	if v.PrefixBytes > 1024*1024 {
		v.PrefixBytes = 1024 * 1024
	}
	if v.PrefixTimeoutMS <= 0 {
		v.PrefixTimeoutMS = DefaultGatewayResponseValidationTimeoutMS
	}
	if v.PrefixTimeoutMS < 100 {
		v.PrefixTimeoutMS = 100
	}
	if v.PrefixTimeoutMS > 30000 {
		v.PrefixTimeoutMS = 30000
	}
	return v
}

func (v GatewayResponseValidationConfig) PrefixTimeout() time.Duration {
	v = v.WithDefaults()
	return time.Duration(v.PrefixTimeoutMS) * time.Millisecond
}

// Validate 检查显式配置边界；Load 会在 Viper 默认值展开后调用。
func (g GatewayConfig) Validate() error {
	var joined error
	if math.IsNaN(g.Hedge.DelaySeconds) || math.IsInf(g.Hedge.DelaySeconds, 0) ||
		g.Hedge.DelaySeconds < 0.1 || g.Hedge.DelaySeconds > 300 {
		joined = errors.Join(joined, fmt.Errorf("gateway.hedge.delaySeconds must be between 0.1 and 300"))
	}
	if g.Hedge.MaxParallel < 1 || g.Hedge.MaxParallel > MaxGatewayHedgeParallel {
		joined = errors.Join(joined, fmt.Errorf("gateway.hedge.maxParallel must be between 1 and %d", MaxGatewayHedgeParallel))
	}
	if g.Hedge.MaxAttempts < 1 || g.Hedge.MaxAttempts > MaxGatewayHedgeAttempts {
		joined = errors.Join(joined, fmt.Errorf("gateway.hedge.maxAttempts must be between 1 and %d", MaxGatewayHedgeAttempts))
	}
	if g.Hedge.MaxAttempts < g.Hedge.MaxParallel {
		joined = errors.Join(joined, fmt.Errorf("gateway.hedge.maxAttempts must be greater than or equal to maxParallel"))
	}
	if strings.ToLower(strings.TrimSpace(g.ResponseValidation.StreamMode)) != DefaultGatewayResponseValidationMode {
		joined = errors.Join(joined, fmt.Errorf("gateway.responseValidation.streamMode must be prefix"))
	}
	if g.ResponseValidation.PrefixBytes < 1024 || g.ResponseValidation.PrefixBytes > 1024*1024 {
		joined = errors.Join(joined, fmt.Errorf("gateway.responseValidation.prefixBytes must be between 1024 and 1048576"))
	}
	if g.ResponseValidation.PrefixTimeoutMS < 100 || g.ResponseValidation.PrefixTimeoutMS > 30000 {
		joined = errors.Join(joined, fmt.Errorf("gateway.responseValidation.prefixTimeoutMs must be between 100 and 30000"))
	}
	return joined
}

func (g GatewayConfig) WithDefaults() GatewayConfig {
	if g.TempPauseSeconds <= 0 {
		g.TempPauseSeconds = DefaultGatewayTempPauseSeconds
	}
	if g.ForwardTimeoutSeconds <= 0 {
		g.ForwardTimeoutSeconds = DefaultGatewayForwardTimeoutSeconds
	}
	if g.ModelsCacheTTLSeconds <= 0 {
		g.ModelsCacheTTLSeconds = DefaultGatewayModelsCacheTTLSeconds
	}
	if g.MaxFailoverSwitches <= 0 {
		g.MaxFailoverSwitches = DefaultGatewayMaxFailoverSwitches
	}
	if g.RouteBatchConcurrency <= 0 {
		g.RouteBatchConcurrency = DefaultGatewayRouteBatchConcurrency
	}
	if g.RouteBatchConcurrency > 64 {
		g.RouteBatchConcurrency = 64
	}
	if g.UsageErrorBodyBytes <= 0 {
		g.UsageErrorBodyBytes = DefaultGatewayUsageErrorBodyBytes
	}
	if g.UsageErrorMsgRunes <= 0 {
		g.UsageErrorMsgRunes = DefaultGatewayUsageErrorMsgRunes
	}
	if g.UsageErrorHeaderValueRunes <= 0 {
		g.UsageErrorHeaderValueRunes = DefaultGatewayUsageErrorHeaderValueRunes
	}
	if g.UsageErrorHeadersJSONBytes <= 0 {
		g.UsageErrorHeadersJSONBytes = DefaultGatewayUsageErrorHeadersJSONBytes
	}
	g.Hedge = g.Hedge.WithDefaults()
	g.ResponseValidation = g.ResponseValidation.WithDefaults()
	return g
}

func (g GatewayConfig) TempPause() time.Duration {
	g = g.WithDefaults()
	return time.Duration(g.TempPauseSeconds) * time.Second
}

func (g GatewayConfig) ForwardTimeout() time.Duration {
	g = g.WithDefaults()
	return time.Duration(g.ForwardTimeoutSeconds) * time.Second
}

func (g GatewayConfig) ModelsCacheTTL() time.Duration {
	g = g.WithDefaults()
	return time.Duration(g.ModelsCacheTTLSeconds) * time.Second
}

type LogConfig struct {
	Level  string `mapstructure:"level" yaml:"level" json:"level"`
	Format string `mapstructure:"format" yaml:"format" json:"format"`
}

// Load 读取 config.yaml（可选）+ APP_SECRET / * 环境变量覆盖。
//
// 关键映射：
//
//	APP_SECRET                       -> security.appSecret
//	DATABASE_DRIVER      -> database.driver
//	DATABASE_PATH        -> database.path
//	DATABASE_HOST        -> database.host
//	SERVER_PORT          -> server.port
//	SCHEDULER_BALANCECRON-> scheduler.balanceCron
func Load(path string) (*Config, error) {
	cfg, _, err := load(path, true)
	return cfg, err
}

func LoadWithPath(path string) (*Config, string, error) {
	return load(path, true)
}

func LoadFile(path string) (*Config, error) {
	cfg, _, err := load(path, false)
	return cfg, err
}

func load(path string, withEnv bool) (*Config, string, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	if path != "" {
		v.SetConfigFile(path)
	} else {
		v.SetConfigName("config")
		for _, p := range configSearchPaths() {
			v.AddConfigPath(p)
		}
		v.AddConfigPath("/etc/upstream-ops")
	}

	setDefaults(v)

	if withEnv {
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		v.AutomaticEnv()
		// APP_SECRET / ADMIN_USERNAME / ADMIN_PASSWORD / AUTH_ENABLED 是独立约定的环境变量名，不带前缀。
		_ = v.BindEnv("security.appSecret", "APP_SECRET")
		_ = v.BindEnv("auth.enabled", "AUTH_ENABLED")
		_ = v.BindEnv("auth.username", "ADMIN_USERNAME")
		_ = v.BindEnv("auth.password", "ADMIN_PASSWORD")
		_ = v.BindEnv("auth.tokenSecret", "AUTH_TOKEN_SECRET")
		// Viper 坑：AutomaticEnv 只对已通过 SetDefault / BindEnv / 配置文件注册过的 key 生效；
		// 数据库的 user/password 没有合理的默认值（拒绝写"change-me"作默认），
		// 因此显式 BindEnv 以确保从环境变量读取。
		_ = v.BindEnv("database.driver", "DATABASE_DRIVER")
		_ = v.BindEnv("database.path", "DATABASE_PATH")
		_ = v.BindEnv("database.host", "DATABASE_HOST")
		_ = v.BindEnv("database.port", "DATABASE_PORT")
		_ = v.BindEnv("database.user", "DATABASE_USER")
		_ = v.BindEnv("database.password", "DATABASE_PASSWORD")
		_ = v.BindEnv("database.name", "DATABASE_NAME")
		_ = v.BindEnv("database.sslMode", "DATABASE_SSL_MODE")
		_ = v.BindEnv("database.maxOpenConns", "DATABASE_MAX_OPEN_CONNS")
		_ = v.BindEnv("database.maxIdleConns", "DATABASE_MAX_IDLE_CONNS")
		_ = v.BindEnv("server.port", "SERVER_PORT")
		_ = v.BindEnv("server.mode", "SERVER_MODE")
		_ = v.BindEnv("log.level", "LOG_LEVEL")
	}

	if err := v.ReadInConfig(); err != nil {
		if path != "" {
			if !os.IsNotExist(err) {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		} else {
			if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
				return nil, "", fmt.Errorf("read config: %w", err)
			}
		}
	}

	cfg := &Config{}
	if err := v.Unmarshal(cfg); err != nil {
		return nil, "", fmt.Errorf("unmarshal config: %w", err)
	}
	if err := cfg.Gateway.Validate(); err != nil {
		return nil, "", err
	}
	cfg.Upstream = cfg.Upstream.WithDefaults()
	cfg.Gateway = cfg.Gateway.WithDefaults()
	return cfg, v.ConfigFileUsed(), nil
}

func Save(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("config path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir config dir: %w", err)
	}
	if cfg == nil {
		return fmt.Errorf("config is nil")
	}
	// Persist normalized runtime defaults so a config created by an older
	// caller remains loadable after new hedge/validation fields are added.
	normalized := *cfg
	normalized.Upstream = normalized.Upstream.WithDefaults()
	normalized.Gateway = normalized.Gateway.WithDefaults()
	body, err := yaml.Marshal(&normalized)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	if err := os.WriteFile(path, body, 0o644); err != nil {
		return fmt.Errorf("write config: %w", err)
	}
	return nil
}

func ResolvePath(requested, used string) string {
	if requested != "" {
		return requested
	}
	if used != "" {
		return used
	}
	for _, candidate := range configSearchPaths() {
		candidate = filepath.Join(candidate, "config.yaml")
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return "../config.yaml"
	}
	return "config.yaml"
}

func configSearchPaths() []string {
	if wd, err := os.Getwd(); err == nil && filepath.Base(wd) == "backend" {
		return []string{"..", "."}
	}
	return []string{"."}
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("app.title", "UpstreamOps")
	v.SetDefault("app.notificationPrefix", "[AI 聚合监控] ")

	v.SetDefault("server.port", 8418)
	v.SetDefault("server.mode", "debug")
	v.SetDefault("server.baseURL", "http://localhost:8418")

	v.SetDefault("database.driver", "sqlite")
	v.SetDefault("database.path", "./data/upstream-ops.db")
	v.SetDefault("database.host", "localhost")
	// Resolve the default by driver in ToStorageConfig: MySQL uses 3306,
	// PostgreSQL uses 5432. A single static Viper default caused a PostgreSQL
	// config that omitted port to try MySQL's port.
	v.SetDefault("database.port", 0)
	v.SetDefault("database.name", "upstreamops")
	v.SetDefault("database.sslMode", "disable")
	v.SetDefault("database.maxOpenConns", 20)
	v.SetDefault("database.maxIdleConns", 5)

	// CLAUDE.md 默认建议：余额 15 分钟，倍率 30 分钟。
	v.SetDefault("scheduler.balanceCron", "37 */15 * * * *")
	v.SetDefault("scheduler.rateCron", "13 */30 * * * *")
	v.SetDefault("scheduler.concurrency", 4)

	// 历史清理：每天凌晨 3:17 跑一次（6 字段 cron 含秒），
	// monitor 30 天 / balance 90 天 / notify 90 天。rate_change_logs 不清理（业务核心数据）。
	v.SetDefault("scheduler.retention.cron", "0 17 3 * * *")
	v.SetDefault("scheduler.retention.monitorLogsDays", 30)
	v.SetDefault("scheduler.retention.balanceSnapshotsDays", 90)
	v.SetDefault("scheduler.retention.notificationLogsDays", 90)
	v.SetDefault("scheduler.retention.announcementsDays", 90)

	v.SetDefault("auth.enabled", false)
	v.SetDefault("auth.username", "admin")
	v.SetDefault("auth.sessionTTLHours", 168) // 7 天

	// 通知去抖：默认开合并、不过滤涨跌幅、balance_low 1h 内不重复、失败重试 3 次。
	// 即"默认行为是合并刷屏 + 不重复 balance_low + 抗短时网络抖动"，不丢任何 rate_changed 事件。
	v.SetDefault("notifications.batchRateChanges", true)
	v.SetDefault("notifications.minChangePct", 0)
	v.SetDefault("notifications.balanceLowCooldownMinutes", 60)
	v.SetDefault("notifications.subscriptionDailyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionWeeklyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionMonthlyRemainingThresholdPct", 0)
	v.SetDefault("notifications.subscriptionExpiryThresholdHours", 0)
	v.SetDefault("notifications.subscriptionAlertCooldownMinutes", 1440)
	v.SetDefault("notifications.sendMaxAttempts", 3)

	v.SetDefault("proxy.protocol", "http")
	v.SetDefault("proxy.port", 0)
	v.SetDefault("proxy.enabled", false)
	v.SetDefault("proxy.versionCheckEnabled", false)

	v.SetDefault("upstream.timeoutSeconds", DefaultUpstreamTimeoutSeconds)
	v.SetDefault("upstream.userAgent", DefaultUpstreamUserAgent)

	v.SetDefault("gateway.tempPauseSeconds", DefaultGatewayTempPauseSeconds)
	v.SetDefault("gateway.forwardTimeoutSeconds", DefaultGatewayForwardTimeoutSeconds)
	v.SetDefault("gateway.modelsCacheTTLSeconds", DefaultGatewayModelsCacheTTLSeconds)
	v.SetDefault("gateway.maxFailoverSwitches", DefaultGatewayMaxFailoverSwitches)
	v.SetDefault("gateway.routeBatchConcurrency", DefaultGatewayRouteBatchConcurrency)
	v.SetDefault("gateway.usageErrorBodyBytes", DefaultGatewayUsageErrorBodyBytes)
	v.SetDefault("gateway.usageErrorMsgRunes", DefaultGatewayUsageErrorMsgRunes)
	v.SetDefault("gateway.usageErrorHeaderValueRunes", DefaultGatewayUsageErrorHeaderValueRunes)
	v.SetDefault("gateway.usageErrorHeadersJSONBytes", DefaultGatewayUsageErrorHeadersJSONBytes)
	v.SetDefault("gateway.hedge.enabled", false)
	v.SetDefault("gateway.hedge.delaySeconds", DefaultGatewayHedgeDelaySeconds)
	v.SetDefault("gateway.hedge.maxParallel", DefaultGatewayHedgeMaxParallel)
	v.SetDefault("gateway.hedge.maxAttempts", DefaultGatewayHedgeMaxAttempts)
	v.SetDefault("gateway.responseValidation.enabled", false)
	v.SetDefault("gateway.responseValidation.streamMode", DefaultGatewayResponseValidationMode)
	v.SetDefault("gateway.responseValidation.prefixBytes", DefaultGatewayResponseValidationBytes)
	v.SetDefault("gateway.responseValidation.prefixTimeoutMs", DefaultGatewayResponseValidationTimeoutMS)

	v.SetDefault("log.level", "info")
	v.SetDefault("log.format", "text")
}
