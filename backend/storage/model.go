package storage

import (
	"strings"
	"time"
)

// ChannelType 上游渠道类型。
type ChannelType string

const (
	ChannelTypeNewAPI  ChannelType = "newapi"
	ChannelTypeSub2API ChannelType = "sub2api"
)

// CredentialMode 渠道凭据模式：
//   - password: 经典模式，存账号 + 密码，由 Connector 走完整登录流程
//   - token:    跳过登录，存用户已有的 cookie / access_token，直接构造 AuthSession
//
// token 模式不依赖打码 / 不会自动续期，token 失效时表现为 last_error 显示鉴权失败。
type CredentialMode string

const (
	CredentialModePassword CredentialMode = "password"
	CredentialModeToken    CredentialMode = "token"
)

// Channel 上游渠道账号。Password / Turnstile API key 等敏感字段都加密保存。
//
// 注意：会话凭据（access_token / refresh_token / cookie / csrf）单独存放在 AuthSession 表。
//
// CredentialMode + PasswordCipher 的语义重载：
//   - password 模式（默认）：Username + PasswordCipher 存账号密码，由 Connector.Login 用
//   - token    模式：PasswordCipher 存 JSON blob（NewAPI: {cookie,user_id} / Sub2API: {access_token,refresh_token}），
//     channel.Service 解析后直接构造 AuthSession，跳过 Login。Username 字段在 token 模式下保留
//     用户填写的备注（一般是邮箱），仅做展示。
//
// 复用 PasswordCipher 而不新增 TokenCipher 是为了让现有的 GORM 行 / 加密路径 / 迁移流程零变动。
type Channel struct {
	ID                     uint           `gorm:"primaryKey" json:"id"`
	Name                   string         `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type                   ChannelType    `gorm:"size:32;not null;index" json:"type"`
	SiteURL                string         `gorm:"size:512;not null" json:"site_url"`
	Username               string         `gorm:"size:256;not null" json:"username"`
	SortOrder              int            `gorm:"not null;default:1" json:"sort_order"`
	PasswordCipher         string         `gorm:"size:4096;not null" json:"-"`
	CredentialMode         CredentialMode `gorm:"size:16;not null;default:'password'" json:"credential_mode"`
	LoginExtraParams       string         `gorm:"type:text" json:"login_extra_params"`
	TurnstileEnabled       bool           `gorm:"default:false" json:"turnstile_enabled"`
	IgnoreAnnouncements    bool           `gorm:"default:false" json:"ignore_announcements"`
	SubscriptionEnabled    bool           `gorm:"default:false" json:"subscription_enabled"`
	ProxyEnabled           bool           `gorm:"default:false" json:"proxy_enabled"`
	CaptchaConfigID        *uint          `json:"captcha_config_id,omitempty"`
	BalanceThreshold       float64        `gorm:"default:0" json:"balance_threshold"`
	RechargeMultiplier     *float64       `json:"recharge_multiplier,omitempty"`
	RechargeMultiplierMode string         `gorm:"size:16;not null;default:'divide'" json:"recharge_multiplier_mode"`
	GroupMultiplier        *float64       `json:"group_multiplier,omitempty"`
	GroupMultiplierMode    string         `gorm:"size:16;not null;default:'divide'" json:"group_multiplier_mode"`
	MonitorEnabled         bool           `gorm:"default:true" json:"monitor_enabled"`
	// ConcurrencyLimit limits all gateway attempts that resolve to this monitor channel.
	// Zero preserves the legacy unlimited behavior.
	ConcurrencyLimit int `gorm:"not null;default:0" json:"concurrency_limit"`
	// OnlyCreatedKeyGroupsEnabled 开启后仅展示与监听"已创建密钥的分组"。
	// 影响：渠道卡片 / 分组对话框 / 全局倍率面板的分组列表，以及倍率变动通知。
	// 不影响 RateSnapshot 本地存储（始终保留全部分组），网关、上游同步、通知设置
	// 等仍可拿到原始全部分组。
	OnlyCreatedKeyGroupsEnabled bool `gorm:"default:false" json:"only_created_key_groups_enabled"`

	// 最近一次采集结果（聚合视图，便于列表页直接展示）
	LastBalance   *float64   `json:"last_balance,omitempty"`
	LastBalanceAt *time.Time `json:"last_balance_at,omitempty"`
	TodayCost     *float64   `json:"today_cost,omitempty"`
	TotalCost     *float64   `json:"total_cost,omitempty"`
	LastError     string     `gorm:"type:text" json:"last_error,omitempty"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (Channel) TableName() string { return "channels" }

// AuthSession 渠道登录后保存的凭据，按 ChannelID 一对一关联。
// *Cipher 字段都用 AES-GCM 加密；UserID 是上游账号 ID 字符串（非敏感），明文存放。
type AuthSession struct {
	ChannelID          uint       `gorm:"primaryKey" json:"channel_id"`
	UserID             string     `gorm:"size:64" json:"user_id,omitempty"`
	AccessTokenCipher  string     `gorm:"type:text" json:"-"`
	RefreshTokenCipher string     `gorm:"type:text" json:"-"`
	CookieCipher       string     `gorm:"type:text" json:"-"`
	CSRFTokenCipher    string     `gorm:"size:1024" json:"-"`
	ExpiresAt          *time.Time `json:"expires_at,omitempty"`
	LastLoginAt        *time.Time `json:"last_login_at,omitempty"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (AuthSession) TableName() string { return "auth_sessions" }

// CaptchaProviderType 打码平台类型。
type CaptchaProviderType string

const (
	CaptchaCapSolver   CaptchaProviderType = "capsolver"
	CaptchaTwoCaptcha  CaptchaProviderType = "2captcha"
	CaptchaAntiCaptcha CaptchaProviderType = "anticaptcha"
	CaptchaYesCaptcha  CaptchaProviderType = "yescaptcha"
)

// CaptchaConfig 打码平台配置。APIKeyCipher 加密保存，Extra 存放各平台差异化 JSON。
type CaptchaConfig struct {
	ID           uint                `gorm:"primaryKey" json:"id"`
	Name         string              `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type         CaptchaProviderType `gorm:"size:32;not null;index" json:"type"`
	APIKeyCipher string              `gorm:"size:1024" json:"-"`
	Endpoint     string              `gorm:"size:512" json:"endpoint,omitempty"`
	Extra        string              `gorm:"type:text" json:"extra,omitempty"`
	Enabled      bool                `gorm:"default:true" json:"enabled"`
	ProxyEnabled bool                `gorm:"default:false" json:"proxy_enabled"`
	LastBalance  *float64            `json:"last_balance,omitempty"`
	BalanceUnit  string              `gorm:"size:32" json:"balance_unit,omitempty"`
	BalanceAt    *time.Time          `json:"balance_at,omitempty"`
	BalanceError string              `gorm:"type:text" json:"balance_error,omitempty"`
	CreatedAt    time.Time           `json:"created_at"`
	UpdatedAt    time.Time           `json:"updated_at"`
}

func (CaptchaConfig) TableName() string { return "captcha_configs" }

// RateSnapshot 渠道当前观察到的模型 / 分组倍率快照。upsert per (channel_id, model_name)。
// 实际的"变化历史"在 RateChangeLog；此表只保存当前状态。
type RateSnapshot struct {
	ID              uint    `gorm:"primaryKey" json:"id"`
	ChannelID       uint    `gorm:"not null;uniqueIndex:idx_rate_chan_model" json:"channel_id"`
	RemoteGroupID   *int64  `json:"remote_group_id,omitempty"`
	ModelName       string  `gorm:"size:256;not null;uniqueIndex:idx_rate_chan_model" json:"model_name"`
	Description     string  `gorm:"size:512" json:"description,omitempty"`
	Ratio           float64 `gorm:"not null" json:"ratio"`
	CompletionRatio float64 `json:"completion_ratio"`

	FirstSeenAt time.Time `json:"first_seen_at"`
	LastSeenAt  time.Time `json:"last_seen_at"`
}

func (RateSnapshot) TableName() string { return "rate_snapshots" }

// RateChangeLog 倍率变化历史。每次扫描发现差异时写入一行。
type RateChangeLog struct {
	ID                 uint      `gorm:"primaryKey" json:"id"`
	ChannelID          uint      `gorm:"not null;index" json:"channel_id"`
	ModelName          string    `gorm:"size:256;not null;index" json:"model_name"`
	OldRatio           *float64  `json:"old_ratio,omitempty"`
	NewRatio           float64   `gorm:"not null" json:"new_ratio"`
	OldCompletionRatio *float64  `json:"old_completion_ratio,omitempty"`
	NewCompletionRatio float64   `json:"new_completion_ratio"`
	ChangedAt          time.Time `gorm:"not null;index" json:"changed_at"`
}

func (RateChangeLog) TableName() string { return "rate_change_logs" }

// UpstreamAnnouncement 保存从上游渠道同步到的公告。
type UpstreamAnnouncement struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	ChannelID       uint       `gorm:"not null;uniqueIndex:idx_announcement_chan_source;index" json:"channel_id"`
	SourceKey       string     `gorm:"size:512;not null;uniqueIndex:idx_announcement_chan_source" json:"source_key"`
	Title           string     `gorm:"size:512" json:"title,omitempty"`
	Content         string     `gorm:"type:text;not null" json:"content"`
	Type            string     `gorm:"size:64" json:"type,omitempty"`
	Link            string     `gorm:"size:512" json:"link,omitempty"`
	PublishedAt     *time.Time `json:"published_at,omitempty"`
	SourceUpdatedAt *time.Time `json:"source_updated_at,omitempty"`
	FirstSeenAt     time.Time  `gorm:"not null;index" json:"first_seen_at"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (UpstreamAnnouncement) TableName() string { return "upstream_announcements" }

// BalanceSnapshot 周期性余额采样，用于图表展示。
type BalanceSnapshot struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	ChannelID uint      `gorm:"not null;index" json:"channel_id"`
	Balance   float64   `gorm:"not null" json:"balance"`
	SampledAt time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (BalanceSnapshot) TableName() string { return "balance_snapshots" }

// CostSnapshot 周期性消费采样，用于图表展示。
type CostSnapshot struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	ChannelID uint      `gorm:"not null;index" json:"channel_id"`
	TodayCost float64   `gorm:"not null" json:"today_cost"`
	SampledAt time.Time `gorm:"not null;index" json:"sampled_at"`
}

func (CostSnapshot) TableName() string { return "cost_snapshots" }

// NotificationChannelType 通知渠道类型。第一版至少 telegram，其它预留。
type NotificationChannelType string

const (
	NotifyTelegram    NotificationChannelType = "telegram"
	NotifyWebhook     NotificationChannelType = "webhook"
	NotifyEmail       NotificationChannelType = "email"
	NotifyWecom       NotificationChannelType = "wecom"
	NotifyDingTalk    NotificationChannelType = "dingtalk"
	NotifyFeishu      NotificationChannelType = "feishu"
	NotifyServerChan3 NotificationChannelType = "serverchan3"
)

// NotificationChannel 通知渠道配置。ConfigCipher 加密保存 JSON 配置（含 token / webhook url / 密码等）。
//
// Subscriptions 是 JSON 数组，记录该渠道关心的上游、事件和分组过滤；为空 / "[]" 表示订阅一切。
// 非敏感数据，明文保存，方便 Dispatcher 直接读取过滤而不解密。
type NotificationChannel struct {
	ID            uint                    `gorm:"primaryKey" json:"id"`
	Name          string                  `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Type          NotificationChannelType `gorm:"size:32;not null;index" json:"type"`
	ConfigCipher  string                  `gorm:"type:text;not null" json:"-"`
	Subscriptions string                  `gorm:"size:4096;not null;default:'[]'" json:"subscriptions"`
	Enabled       bool                    `gorm:"default:true" json:"enabled"`
	ProxyEnabled  bool                    `gorm:"default:false" json:"proxy_enabled"`
	CreatedAt     time.Time               `json:"created_at"`
	UpdatedAt     time.Time               `json:"updated_at"`
}

func (NotificationChannel) TableName() string { return "notification_channels" }

// NotificationEvent 系统内部触发的通知事件类型。
type NotificationEvent string

const (
	EventBalanceLow               NotificationEvent = "balance_low"
	EventRateChanged              NotificationEvent = "rate_changed"
	EventRateStructureChanged     NotificationEvent = "rate_structure_changed"
	EventRateAdded                NotificationEvent = "rate_added"
	EventRateRemoved              NotificationEvent = "rate_removed"
	EventAnnouncement             NotificationEvent = "announcement"
	EventLoginFailed              NotificationEvent = "login_failed"
	EventCaptchaFailed            NotificationEvent = "captcha_failed"
	EventMonitorFailed            NotificationEvent = "monitor_failed"
	EventSubscriptionDailyLow     NotificationEvent = "subscription_daily_remaining_low"
	EventSubscriptionWeeklyLow    NotificationEvent = "subscription_weekly_remaining_low"
	EventSubscriptionMonthlyLow   NotificationEvent = "subscription_monthly_remaining_low"
	EventSubscriptionExpiring     NotificationEvent = "subscription_expiring"
	EventUpstreamSyncGroupChanged NotificationEvent = "upstream_sync_group_changed"
)

// NotificationLog 通知发送记录。
type NotificationLog struct {
	ID                uint              `gorm:"primaryKey" json:"id"`
	ChannelID         uint              `gorm:"not null;index" json:"channel_id"`
	UpstreamChannelID uint              `gorm:"not null;default:0;index" json:"upstream_channel_id,omitempty"`
	Event             NotificationEvent `gorm:"size:64;not null;index" json:"event"`
	Subject           string            `gorm:"size:512;not null" json:"subject"`
	Body              string            `gorm:"type:text" json:"body"`
	Success           bool              `gorm:"not null" json:"success"`
	ErrorMessage      string            `gorm:"type:text" json:"error_message,omitempty"`
	SentAt            time.Time         `gorm:"not null;index" json:"sent_at"`
}

func (NotificationLog) TableName() string { return "notification_logs" }

// NotificationCooldown 跨重启持久化的通知冷却记录。
//
// 业务键 (ChannelID, Event)：标记某渠道某类事件最近一次发送时间。
// Dispatcher 在发送 cooldown-aware 事件（如 balance_low）前查这张表，
// 命中且未过 cooldown 就跳过。
//
// 不和 NotificationLog 合并是因为：
//   - NotificationLog 是审计/历史日志（用户可见、可清理）
//   - NotificationCooldown 是去抖控制平面（仅最新一条、原子 upsert）
//
// ChannelID 这里指的是**上游渠道**（storage.Channel），不是通知渠道。
type NotificationCooldown struct {
	ChannelID  uint              `gorm:"primaryKey" json:"channel_id"`
	Event      NotificationEvent `gorm:"primaryKey;size:64" json:"event"`
	LastSentAt time.Time         `gorm:"not null" json:"last_sent_at"`
	UpdatedAt  time.Time         `json:"updated_at"`
}

func (NotificationCooldown) TableName() string { return "notification_cooldowns" }

// MonitorJob 监控任务类型。
type MonitorJob string

const (
	MonitorJobLogin   MonitorJob = "login"
	MonitorJobBalance MonitorJob = "balance"
	MonitorJobRates   MonitorJob = "rates"
)

// MonitorLog 每次扫描 / 登录尝试的结果，便于诊断失败。
type MonitorLog struct {
	ID           uint       `gorm:"primaryKey" json:"id"`
	ChannelID    uint       `gorm:"not null;index" json:"channel_id"`
	Job          MonitorJob `gorm:"size:32;not null;index" json:"job"`
	Success      bool       `gorm:"not null" json:"success"`
	ErrorMessage string     `gorm:"type:text" json:"error_message,omitempty"`
	DurationMS   int64      `json:"duration_ms"`
	StartedAt    time.Time  `gorm:"not null;index" json:"started_at"`
	FinishedAt   time.Time  `json:"finished_at"`
}

func (MonitorLog) TableName() string { return "monitor_logs" }

// UpstreamSyncTarget 目标 Sub2API 站点配置。
//
// 管理员 API Key 单独加密保存，检测结果只作为状态缓存，不影响已保存的同步分组。
type UpstreamSyncTarget struct {
	ID                uint       `gorm:"primaryKey" json:"id"`
	Name              string     `gorm:"size:128;not null;uniqueIndex" json:"name"`
	BaseURL           string     `gorm:"size:512;not null" json:"base_url"`
	AdminAPIKeyCipher string     `gorm:"type:text;not null" json:"-"`
	Enabled           bool       `gorm:"default:true" json:"enabled"`
	LastCheckStatus   string     `gorm:"size:32" json:"last_check_status,omitempty"`
	LastCheckAt       *time.Time `json:"last_check_at,omitempty"`
	LastCheckError    string     `gorm:"type:text" json:"last_check_error,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func (UpstreamSyncTarget) TableName() string { return "upstream_sync_targets" }

// UpstreamSyncTargetGroup 是目标 Sub2API 分组缓存。
//
// 同一个目标站点内按 (target_id, remote_group_id) upsert
type UpstreamSyncTargetGroup struct {
	ID            uint       `gorm:"primaryKey" json:"id"`
	TargetID      uint       `gorm:"not null;uniqueIndex:idx_upstream_sync_target_group" json:"target_id"`
	RemoteGroupID int64      `gorm:"not null;uniqueIndex:idx_upstream_sync_target_group" json:"remote_group_id"`
	Name          string     `gorm:"size:256;not null" json:"name"`
	Platform      string     `gorm:"size:64" json:"platform,omitempty"`
	Ratio         float64    `gorm:"not null" json:"ratio"`
	Status        string     `gorm:"size:32;index" json:"status"`
	Sort          int        `json:"sort"`
	Description   string     `gorm:"type:text" json:"description,omitempty"`
	LastSyncAt    *time.Time `json:"last_sync_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

func (UpstreamSyncTargetGroup) TableName() string { return "upstream_sync_target_groups" }

// UpstreamSyncGroup 保存一组目标分组同步配置。
//
// 分组名称和名称模板创建后固定，不允许二次修改，避免远端对象命名漂移。
type UpstreamSyncGroup struct {
	ID                       uint       `gorm:"primaryKey" json:"id"`
	DisplayName              string     `gorm:"size:256;not null;default:''" json:"display_name"`
	NameTemplate             string     `gorm:"size:256;not null" json:"name_template"`
	Name                     string     `gorm:"size:256;not null;uniqueIndex" json:"name"`
	TargetID                 uint       `gorm:"not null;index" json:"target_id"`
	TargetGroupIDsJSON       string     `gorm:"type:text;not null" json:"target_group_ids"`
	Platform                 string     `gorm:"size:64;not null" json:"platform"`
	ModelLimitsMode          string     `gorm:"size:32;not null;default:'sync_upstream'" json:"model_limits_mode"`
	ModelLimitsText          string     `gorm:"type:text" json:"model_limits,omitempty"`
	PoolModeEnabled          bool       `gorm:"default:false" json:"pool_mode_enabled"`
	PoolModeRetryCount       int        `gorm:"default:10" json:"pool_mode_retry_count"`
	PoolModeRetryStatusCodes string     `gorm:"type:text" json:"pool_mode_retry_status_codes,omitempty"`
	CustomErrorCodesEnabled  bool       `gorm:"default:false" json:"custom_error_codes_enabled"`
	CustomErrorCodes         string     `gorm:"type:text" json:"custom_error_codes,omitempty"`
	RateSortDirection        string     `gorm:"size:16;not null;default:'asc'" json:"rate_sort_direction"`
	Enabled                  bool       `gorm:"default:true" json:"enabled"`
	ApplyStatus              string     `gorm:"size:64" json:"apply_status,omitempty"`
	ApplyError               string     `gorm:"type:text" json:"apply_error,omitempty"`
	LastAppliedAt            *time.Time `json:"last_applied_at,omitempty"`
	CreatedAt                time.Time  `json:"created_at"`
	UpdatedAt                time.Time  `json:"updated_at"`
}

func (UpstreamSyncGroup) TableName() string { return "upstream_sync_groups" }

// UpstreamSyncAccount 是同步分组下的一条账号同步配置。
type UpstreamSyncAccount struct {
	ID               uint      `gorm:"primaryKey" json:"id"`
	SyncGroupID      uint      `gorm:"not null;index" json:"sync_group_id"`
	Position         int       `gorm:"not null;default:0" json:"position"`
	SourceChannelID  uint      `gorm:"not null;index" json:"source_channel_id"`
	SourceGroupID    *int64    `json:"source_group_id,omitempty"`
	SourceGroupName  string    `gorm:"size:256;not null;default:''" json:"source_group_name,omitempty"`
	ProxyID          *int64    `json:"proxy_id,omitempty"`
	Concurrency      int       `gorm:"default:10" json:"concurrency"`
	Weight           int       `gorm:"default:1" json:"weight"`
	RateConvertMode  string    `gorm:"size:32;not null;default:'raw'" json:"rate_convert_mode"`
	RateConvertValue float64   `gorm:"default:1" json:"rate_convert_value"`
	Enabled          bool      `gorm:"default:true" json:"enabled"`
	TestEnabled      bool      `gorm:"default:false" json:"test_enabled"`
	TestModel        string    `gorm:"size:256;not null;default:''" json:"test_model,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (UpstreamSyncAccount) TableName() string { return "upstream_sync_accounts" }

// UpstreamSyncManagedAccount 记录同步账号在远端生成的两个对象映射，便于幂等更新和删除确认。
type UpstreamSyncManagedAccount struct {
	ID                 uint       `gorm:"primaryKey" json:"id"`
	SyncGroupID        uint       `gorm:"not null;index" json:"sync_group_id"`
	SyncAccountID      uint       `gorm:"not null;uniqueIndex" json:"sync_account_id"`
	SourceAPIKeyID     int64      `gorm:"not null" json:"source_api_key_id"`
	SourceAPIKeyName   string     `gorm:"size:256;not null" json:"source_api_key_name"`
	TargetAccountID    int64      `gorm:"not null" json:"target_account_id"`
	TargetAccountName  string     `gorm:"size:256;not null" json:"target_account_name"`
	TargetGroupIDsJSON string     `gorm:"type:text;not null" json:"target_group_ids"`
	LastAppliedAt      *time.Time `json:"last_applied_at,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func (UpstreamSyncManagedAccount) TableName() string { return "upstream_sync_managed_accounts" }

// UpstreamSyncLog 记录同步分组的执行结果。
type UpstreamSyncLog struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	SyncGroupID uint      `gorm:"not null;index" json:"sync_group_id"`
	TargetID    uint      `gorm:"not null;index" json:"target_id"`
	Action      string    `gorm:"size:64;not null;index" json:"action"`
	Success     bool      `gorm:"not null" json:"success"`
	Message     string    `gorm:"type:text" json:"message,omitempty"`
	Detail      string    `gorm:"type:text" json:"detail,omitempty"`
	CreatedAt   time.Time `gorm:"not null;index" json:"created_at"`
}

func (UpstreamSyncLog) TableName() string { return "upstream_sync_logs" }

// ---------- 请求转发网关 ----------

const (
	GatewayKeyStatusActive   = "active"
	GatewayKeyStatusDisabled = "disabled"

	GatewayGroupStatusActive   = "active"
	GatewayGroupStatusDisabled = "disabled"

	GatewayModelsModeAuto   = "auto"
	GatewayModelsModeManual = "manual"
	GatewayModelsModeHybrid = "hybrid"

	// 直连渠道模型策略。旧数据 model_policy 为空时按 all 兼容；allowlist
	// 仅允许 allowed_models_json 中列出的最终上游模型 ID。
	GatewayProviderModelPolicyAll       = "all"
	GatewayProviderModelPolicyAllowlist = "allowlist"

	// 上游协议（路由 / 直连渠道）
	//   auto | openai_chat | openai_responses | anthropic
	//   openai 为 openai_chat 的历史别名，读写时仍接受
	GatewayUpstreamProtocolAuto            = "auto"
	GatewayUpstreamProtocolOpenAI          = "openai"           // 兼容：等同 openai_chat
	GatewayUpstreamProtocolOpenAIChat      = "openai_chat"      // /v1/chat/completions · messages
	GatewayUpstreamProtocolOpenAIResponses = "openai_responses" // /v1/responses · input
	GatewayUpstreamProtocolAnthropic       = "anthropic"        // /v1/messages

	// GatewayRoute 上游来源：监控渠道 vs 直连提供商（base+key）
	GatewayRouteSourceMonitor  = "monitor"
	GatewayRouteSourceProvider = "provider"

	GatewayProviderAuthBearer  = "bearer"
	GatewayProviderAuthXAPIKey = "x-api-key"
	GatewayProviderAuthBoth    = "both"

	// 路由 User-Agent 策略（组级统一 UA + 路由三选一）：
	//   passthrough — 透传客户端 UA（默认；模型测试/拉模型无客户端时不设 UA）
	//   group       — 使用组 UserAgent（组为空则等同透传）
	//   custom      — 使用路由 UserAgentCustom（自定义为空则等同透传）
	GatewayUserAgentModePassthrough = "passthrough"
	GatewayUserAgentModeGroup       = "group"
	GatewayUserAgentModeCustom      = "custom"

	GatewayRequestTypeUnknown = 0
	GatewayRequestTypeSync    = 1
	GatewayRequestTypeStream  = 2

	GatewayResponseRuleTargetAssistantText = "assistant_text"
	GatewayResponseRuleTargetRawBody       = "raw_body"
	GatewayResponseRuleTargetErrorMessage  = "error_message"

	GatewayAttemptKindPrimary     = "primary"
	GatewayAttemptKindRetry       = "retry"
	GatewayAttemptKindFailover    = "failover"
	GatewayAttemptKindHedge       = "hedge"
	GatewayAttemptKindRecovery    = "recovery"
	GatewayAttemptKindRegexReject = "regex_reject"

	GatewayVirtualCacheReasonHedge                = "hedge"
	GatewayVirtualCacheReasonResponseRuleFailover = "response_rule_failover"
	GatewayVirtualCacheReasonProviderGlobal      = "provider_global"

	GatewayAttemptStatusAccepted = "accepted"
	GatewayAttemptStatusRejected = "rejected"
	GatewayAttemptStatusError    = "error"
	GatewayAttemptStatusCanceled = "canceled"
)

// GatewayProvider 直连上游（Base URL + API Key），不登录、不监控余额。
// 对齐 CLI Proxy openai-compatibility / api-key 形态，可与监控渠道在网关组内混用。
// User-Agent 不在直连渠道配置：由网关组 + 路由 UA 策略统一决定。
type GatewayProvider struct {
	ID                uint   `gorm:"primaryKey" json:"id"`
	Name              string `gorm:"size:128;not null;uniqueIndex" json:"name"`
	BaseURL           string `gorm:"size:512;not null" json:"base_url"`
	APIKeyCipher      string `gorm:"type:text;not null" json:"-"`
	APIKeyHint        string `gorm:"size:64;not null;default:''" json:"api_key_hint"`
	UpstreamProtocol  string `gorm:"size:16;not null;default:'auto'" json:"upstream_protocol"`
	ModelPolicy       string `gorm:"size:16;not null;default:'all'" json:"model_policy"`
	AllowedModelsJSON string `gorm:"type:text;not null;default:'[]'" json:"allowed_models_json"`
	// ConcurrencyLimit is shared by every gateway route that references this provider.
	// Zero preserves the legacy unlimited behavior.
	ConcurrencyLimit   int     `gorm:"not null;default:0" json:"concurrency_limit"`
	// Provider-level virtual cache is an opt-in downstream accounting hint for
	// text models. The percentage is applied to fresh input tokens only.
	VirtualCacheEnabled    bool   `gorm:"not null;default:false" json:"virtual_cache_enabled"`
	VirtualCacheModelsJSON  string `gorm:"type:text;not null;default:'[]'" json:"virtual_cache_models_json"`
	VirtualCachePercent     int    `gorm:"not null;default:0" json:"virtual_cache_percent"`
	DefaultBillingRate float64 `gorm:"not null;default:1" json:"default_billing_rate"`
	AuthStyle          string  `gorm:"size:16;not null;default:'both'" json:"auth_style"`
	Enabled            bool    `gorm:"not null;default:true;index" json:"enabled"`
	// ProxyEnabled 与监控渠道一致：全局代理开启且本开关打开时，转发走系统代理配置。
	ProxyEnabled     bool      `gorm:"not null;default:false" json:"proxy_enabled"`
	ExtraHeadersJSON string    `gorm:"type:text" json:"extra_headers,omitempty"`
	Notes            string    `gorm:"size:512;not null;default:''" json:"notes,omitempty"`
	// Cache health fields are populated by the admin usage view and are not
	// persisted on the provider itself.
	CacheHitRate                float64    `gorm:"-" json:"cache_hit_rate,omitempty"`
	CacheHealthRequestCount     int64      `gorm:"-" json:"cache_health_request_count,omitempty"`
	CacheHealthInputTokens      int64      `gorm:"-" json:"cache_health_input_tokens,omitempty"`
	CacheHealthReadTokens       int64      `gorm:"-" json:"cache_health_read_tokens,omitempty"`
	CacheHealthCreationTokens   int64      `gorm:"-" json:"cache_health_creation_tokens,omitempty"`
	CacheHealthEvaluatedAt      *time.Time `gorm:"-" json:"cache_health_evaluated_at,omitempty"`
	CacheHealthBlacklistedUntil *time.Time `gorm:"-" json:"cache_health_blacklisted_until,omitempty"`
	CacheHealthBlacklistReason  string     `gorm:"-" json:"cache_health_blacklist_reason,omitempty"`
	CacheHealthManualClearUntil *time.Time `gorm:"-" json:"cache_health_manual_clear_until,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

func (GatewayProvider) TableName() string { return "gateway_providers" }

// GatewayGroup 是网关配置单元：路由、模型映射、模型列表归属组；组内可有多把密钥。
type GatewayGroup struct {
	ID          uint   `gorm:"primaryKey" json:"id"`
	Name        string `gorm:"size:128;not null;uniqueIndex" json:"name"`
	Description string `gorm:"size:512;not null;default:''" json:"description,omitempty"`
	// Position 侧栏展示与管理端列表顺序（越小越靠前）；新建组追加到末尾。
	Position          int    `gorm:"not null;default:0;index" json:"position"`
	Status            string `gorm:"size:16;not null;default:'active';index" json:"status"`
	RateSortDirection string `gorm:"size:16;not null;default:'asc'" json:"rate_sort_direction"`
	// RateResortEnabled 渠道分组价格倍率重排：开启后，倍率扫描结束时按源分组实时倍率
	// 重写路由 position 与 billing_rate_multiplier（对齐上游同步账号 Apply 逻辑）。
	// 关闭时仅在保存路由 / 改排序方向时落库顺序；运行时仍按实时倍率 SortRoutes。
	RateResortEnabled        bool    `gorm:"not null;default:false" json:"rate_resort_enabled"`
	MaxBillingRateMultiplier float64 `gorm:"not null;default:0" json:"max_billing_rate_multiplier"`
	// LoadBalanceRouteCount controls how many highest-priority physical upstreams
	// may receive normal requests. It does not change per-request hedge parallelism.
	LoadBalanceRouteCount int    `gorm:"not null;default:1" json:"load_balance_route_count"`
	ModelMappingJSON      string `gorm:"type:text" json:"model_mapping,omitempty"`
	ModelsJSON            string `gorm:"type:text" json:"models_json,omitempty"`
	ModelsMode            string `gorm:"size:16;not null;default:'auto'" json:"models_mode"`
	// 重试 / 顺延 / 冷却（组级策略）
	// RetryEnabled=false：上游失败直接回显，不重试、不顺延
	RetryEnabled bool `gorm:"not null;default:true" json:"retry_enabled"`
	// 同一路由额外重试次数（不含首次；0=每条路由只打一次）
	RetryCount int `gorm:"not null;default:0" json:"retry_count"`
	// 是否顺延到下一条路由
	FailoverEnabled bool `gorm:"not null;default:true" json:"failover_enabled"`
	// 顺延次数：在首条路由耗尽后，最多再换几条路由
	FailoverMax int `gorm:"not null;default:8" json:"failover_max"`
	// FailoverOn4xx：是否将上游 4xx（含 400/401/403/404 等；429 始终可顺延）纳入重试/顺延。
	// 默认 false：仅网络错误、429、5xx 会重试/顺延；其它 4xx 直接回显。
	FailoverOn4xx bool `gorm:"not null;default:false" json:"failover_on_4xx"`
	// 失败后临时冷却秒数（0=不冷却）；恢复后可再参与调度
	CooldownSeconds int `gorm:"not null;default:30" json:"cooldown_seconds"`
	// 首字/首字节超时（秒）：0=关闭；>0 时等待首字节超过该时间则主动断开并走重试/顺延。
	// 可能造成上游已计费但客户端未收完，从而重复请求增加费用。
	FirstTokenTimeoutSec int `gorm:"not null;default:0" json:"first_token_timeout_sec"`
	// HedgeMaxParallel 包含主请求；HedgeMaxAttempts 是请求允许启动的 attempt 总数。
	HedgeEnabled      bool    `gorm:"not null;default:false" json:"hedge_enabled"`
	HedgeDelaySeconds float64 `gorm:"not null;default:10" json:"hedge_delay_seconds"`
	HedgeMaxParallel  int     `gorm:"not null;default:2" json:"hedge_max_parallel"`
	HedgeMaxAttempts  int     `gorm:"not null;default:4" json:"hedge_max_attempts"`
	// HedgeVirtualCacheEnabled grants the winner a virtual cache-read credit
	// when this request actually used concurrent hedge attempts. Raw upstream
	// usage and attempt costs remain unchanged for audit/accounting.
	HedgeVirtualCacheEnabled bool `gorm:"not null;default:false" json:"hedge_virtual_cache_enabled"`
	// 响应校验按组启用；流式响应只在提交客户端前检查 prefix。
	ResponseValidationEnabled             bool   `gorm:"not null;default:false" json:"response_validation_enabled"`
	ResponseValidationVirtualCacheEnabled bool   `gorm:"not null;default:false" json:"response_validation_virtual_cache_enabled"`
	// ResponseValidationRetryCount controls same-route retries after a
	// pre-commit response-rule rejection. -1 inherits RetryCount; 0 disables
	// response-rule retries while leaving transport retries unchanged.
	ResponseValidationRetryCount       int    `gorm:"not null;default:-1" json:"response_validation_retry_count"`
	ResponseValidationStreamMode      string `gorm:"size:16;not null;default:'prefix'" json:"response_validation_stream_mode"`
	ResponseValidationPrefixBytes     int    `gorm:"not null;default:8192" json:"response_validation_prefix_bytes"`
	ResponseValidationPrefixTimeoutMS int    `gorm:"not null;default:2000" json:"response_validation_prefix_timeout_ms"`
	// UserAgent 组级统一 User-Agent。路由 mode=group 时使用；留空表示组未配置。
	// 不用 omitempty：空串也要返回，前端编辑回填才能区分「未配置」与「字段缺失」。
	UserAgent string    `gorm:"size:512;not null;default:''" json:"user_agent"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (GatewayGroup) TableName() string { return "gateway_groups" }

// GatewayKey 是客户端调用本服务 /v1/* 时使用的请求密钥（归属某个组）。
type GatewayKey struct {
	ID              uint       `gorm:"primaryKey" json:"id"`
	GroupID         uint       `gorm:"not null;index;default:0" json:"group_id"`
	Name            string     `gorm:"size:128;not null;uniqueIndex" json:"name"`
	KeyHash         string     `gorm:"size:64;not null;uniqueIndex" json:"-"`
	KeyPrefix       string     `gorm:"size:32;not null" json:"key_prefix"`
	KeyCipher       string     `gorm:"type:text;not null" json:"-"`
	Status          string     `gorm:"size:16;not null;default:'active';index" json:"status"`
	Quota           float64    `gorm:"not null;default:0" json:"quota"`
	QuotaUsed       float64    `gorm:"not null;default:0" json:"quota_used"`
	IPWhitelistJSON string     `gorm:"type:text" json:"ip_whitelist,omitempty"`
	IPBlacklistJSON string     `gorm:"type:text" json:"ip_blacklist,omitempty"`
	LastUsedAt      *time.Time `json:"last_used_at,omitempty"`
	CreatedAt       time.Time  `json:"created_at"`
	UpdatedAt       time.Time  `json:"updated_at"`
}

func (GatewayKey) TableName() string { return "gateway_keys" }

// GatewayRoute 是网关组绑定的一条上游路由（监控渠道或直连 Provider）。
type GatewayRoute struct {
	ID             uint `gorm:"primaryKey" json:"id"`
	GatewayGroupID uint `gorm:"not null;index;uniqueIndex:idx_gateway_route_group_pos" json:"gateway_group_id"`
	Position       int  `gorm:"not null;default:0;uniqueIndex:idx_gateway_route_group_pos" json:"position"`
	// SourceKind: monitor | provider；空视为 monitor（兼容旧数据）
	SourceKind                  string  `gorm:"size:16;not null;default:'monitor';index" json:"source_kind"`
	SourceChannelID             uint    `gorm:"not null;index;default:0" json:"source_channel_id"`
	GatewayProviderID           uint    `gorm:"not null;index;default:0" json:"gateway_provider_id"`
	SourceGroupID               *int64  `json:"source_group_id,omitempty"`
	SourceGroupName             string  `gorm:"size:256;not null;default:''" json:"source_group_name,omitempty"`
	Weight                      int     `gorm:"default:1" json:"weight"`
	RateConvertMode             string  `gorm:"size:32;not null;default:'raw'" json:"rate_convert_mode"`
	RateConvertValue            float64 `gorm:"default:1" json:"rate_convert_value"`
	BillingRateMultiplier       float64 `gorm:"not null;default:1" json:"billing_rate_multiplier"`
	Enabled                     bool    `gorm:"default:true" json:"enabled"`
	RateLimitAutoDisabled       bool    `gorm:"not null;default:false" json:"rate_limit_auto_disabled"`
	RateLimitAutoDisabledReason string  `gorm:"size:512;not null;default:''" json:"rate_limit_auto_disabled_reason,omitempty"`
	ModelMappingJSON            string  `gorm:"type:text" json:"model_mapping,omitempty"`
	UpstreamProtocol            string  `gorm:"size:16;not null;default:'auto'" json:"upstream_protocol"`
	Concurrency                 int     `gorm:"default:10" json:"concurrency"`
	// UserAgentMode: passthrough | group | custom（见 GatewayUserAgentMode*）
	UserAgentMode string `gorm:"size:16;not null;default:'passthrough'" json:"user_agent_mode"`
	// UserAgentCustom 仅 mode=custom 时生效；转发/模型测试/拉模型共用。
	// 不用 omitempty：空串也要返回，前端编辑回填才能区分。
	UserAgentCustom         string     `gorm:"size:512;not null;default:''" json:"user_agent_custom"`
	SourceAPIKeyID          int64      `gorm:"not null;default:0" json:"source_api_key_id"`
	SourceAPIKeyName        string     `gorm:"size:256;not null;default:''" json:"source_api_key_name"`
	SourceAPIKeyCipher      string     `gorm:"type:text" json:"-"`
	TempUnschedulableUntil  *time.Time `json:"temp_unschedulable_until,omitempty"`
	TempUnschedulableReason string     `gorm:"type:text" json:"temp_unschedulable_reason,omitempty"`
	// TempUnschedulableAt / TempUnschedulableRequestID：最近一次触发暂停的失败请求时间与网关 request_id
	// （保留至手动清除、连续成功自动清除，或下次失败覆盖）
	TempUnschedulableAt        *time.Time `json:"temp_unschedulable_at,omitempty"`
	TempUnschedulableRequestID string     `gorm:"size:64;not null;default:''" json:"temp_unschedulable_request_id,omitempty"`
	// RecoverSuccessStreak：失败残留信息存在期间的连续成功次数；达到阈值后自动清空错误展示
	RecoverSuccessStreak int `gorm:"not null;default:0" json:"recover_success_streak,omitempty"`
	// CacheHealth* are loaded from gateway_channel_cache_health for scheduling;
	// they are intentionally not duplicated into gateway_routes.
	CacheHealthHitRate          float64    `gorm:"-" json:"cache_health_hit_rate,omitempty"`
	CacheHealthRequestCount     int64      `gorm:"-" json:"cache_health_request_count,omitempty"`
	CacheHealthEvaluatedAt      *time.Time `gorm:"-" json:"cache_health_evaluated_at,omitempty"`
	CacheHealthBlacklistedUntil *time.Time `gorm:"-" json:"cache_health_blacklisted_until,omitempty"`
	CacheHealthBlacklistReason  string     `gorm:"-" json:"cache_health_blacklist_reason,omitempty"`
	CacheHealthManualClearUntil *time.Time `gorm:"-" json:"cache_health_manual_clear_until,omitempty"`
	// ModelCooldowns contains automatic cooldowns keyed by normalized model.
	// It is loaded with routes and is not persisted as part of gateway_routes.
	ModelCooldowns map[string]GatewayRouteModelCooldown `gorm:"-" json:"model_cooldowns,omitempty"`
	CreatedAt      time.Time                            `json:"created_at"`
	UpdatedAt      time.Time                            `json:"updated_at"`
}

// RouteRecoverSuccessClearStreak 连续成功多少次后自动清除「已恢复/错误」残留展示。
const RouteRecoverSuccessClearStreak = 3

// NormalizeSourceKind 返回规范化来源类型。
func (r *GatewayRoute) NormalizeSourceKind() string {
	if r == nil {
		return GatewayRouteSourceMonitor
	}
	k := strings.ToLower(strings.TrimSpace(r.SourceKind))
	if k == GatewayRouteSourceProvider {
		return GatewayRouteSourceProvider
	}
	// 兼容：未写 kind 但填了 provider id
	if k == "" && r.GatewayProviderID > 0 && r.SourceChannelID == 0 {
		return GatewayRouteSourceProvider
	}
	return GatewayRouteSourceMonitor
}

func (GatewayRoute) TableName() string { return "gateway_routes" }

// GatewayRouteModelCooldown pauses one model on one route while allowing the
// same route to continue serving other models.
type GatewayRouteModelCooldown struct {
	ID                         uint       `gorm:"primaryKey" json:"id"`
	RouteID                    uint       `gorm:"not null;index;uniqueIndex:idx_gateway_route_model_cooldown" json:"route_id"`
	Model                      string     `gorm:"size:256;not null;uniqueIndex:idx_gateway_route_model_cooldown" json:"model"`
	TempUnschedulableUntil     *time.Time `json:"temp_unschedulable_until,omitempty"`
	TempUnschedulableReason    string     `gorm:"type:text" json:"temp_unschedulable_reason,omitempty"`
	TempUnschedulableAt        *time.Time `json:"temp_unschedulable_at,omitempty"`
	TempUnschedulableRequestID string     `gorm:"size:64;not null;default:''" json:"temp_unschedulable_request_id,omitempty"`
	RecoverSuccessStreak       int        `gorm:"not null;default:0" json:"recover_success_streak,omitempty"`
	CreatedAt                  time.Time  `json:"created_at"`
	UpdatedAt                  time.Time  `json:"updated_at"`
}

func (GatewayRouteModelCooldown) TableName() string { return "gateway_route_model_cooldowns" }

// GatewayChannelCacheHealth stores rolling cache statistics and the optional
// automatic blacklist for one upstream source. SourceKind/SourceID identify a
// monitored channel or a direct provider without changing its manual Enabled
// flag. The row is also used to hydrate route scheduling snapshots.
type GatewayChannelCacheHealth struct {
	ID                  uint       `gorm:"primaryKey" json:"id"`
	SourceKind          string     `gorm:"size:16;not null;uniqueIndex:idx_gateway_cache_health_source" json:"source_kind"`
	SourceID            uint       `gorm:"not null;uniqueIndex:idx_gateway_cache_health_source" json:"source_id"`
	HitRate             float64    `gorm:"not null;default:0" json:"hit_rate"`
	RequestCount        int64      `gorm:"not null;default:0" json:"request_count"`
	InputTokens         int64      `gorm:"not null;default:0" json:"input_tokens"`
	CacheReadTokens     int64      `gorm:"not null;default:0" json:"cache_read_tokens"`
	CacheCreationTokens int64      `gorm:"not null;default:0" json:"cache_creation_tokens"`
	WindowStart         *time.Time `json:"window_start,omitempty"`
	EvaluatedAt         *time.Time `json:"evaluated_at,omitempty"`
	BlacklistedUntil    *time.Time `json:"blacklisted_until,omitempty"`
	BlacklistReason     string     `gorm:"size:512;not null;default:''" json:"blacklist_reason,omitempty"`
	// ManualClearUntil is a persisted grace period after an administrator
	// releases the source. It prevents stale rolling data from re-blacklisting
	// the source on the next asynchronous evaluation.
	ManualClearUntil    *time.Time `json:"manual_clear_until,omitempty"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
}

func (GatewayChannelCacheHealth) TableName() string { return "gateway_channel_cache_health" }

// NormalizeGatewayModel returns the canonical scheduler key for a model. Model
// identifiers are trimmed but remain case-sensitive, matching model mapping
// and provider allowlist semantics.
func NormalizeGatewayModel(model string) string {
	return strings.TrimSpace(model)
}

// GatewayResponseRule 是组级响应内容拒绝规则。ModelsJSON / ProtocolsJSON
// 保存 JSON 字符串数组；空数组表示不限制。
type GatewayResponseRule struct {
	ID             uint      `gorm:"primaryKey" json:"id"`
	GatewayGroupID uint      `gorm:"not null;index;uniqueIndex:idx_gateway_response_rule_group_name" json:"gateway_group_id"`
	Name           string    `gorm:"size:128;not null;uniqueIndex:idx_gateway_response_rule_group_name" json:"name"`
	Enabled        bool      `gorm:"not null;default:false;index" json:"enabled"`
	Priority       int       `gorm:"not null;default:0;index" json:"priority"`
	Pattern        string    `gorm:"type:text;not null" json:"pattern"`
	Target         string    `gorm:"size:32;not null;default:'assistant_text'" json:"target"`
	ModelsJSON     string    `gorm:"type:text;not null" json:"models_json"`
	ProtocolsJSON  string    `gorm:"type:text;not null" json:"protocols_json"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func (GatewayResponseRule) TableName() string { return "gateway_response_rules" }

// GatewayUsageLog 记录每一次网关转发请求的用量与费用参考。
// Source* 字段为请求当时的路由快照：路由保存会换 id 时，历史记录仍可展示上游密钥/源分组。
type GatewayUsageLog struct {
	ID                uint `gorm:"primaryKey" json:"id"`
	GatewayGroupID    uint `gorm:"not null;index;default:0" json:"gateway_group_id"`
	GatewayKeyID      uint `gorm:"not null;index" json:"gateway_key_id"`
	RouteID           uint `gorm:"not null;index" json:"route_id"`
	ChannelID         uint `gorm:"not null;index;default:0" json:"channel_id"`
	GatewayProviderID uint `gorm:"not null;index;default:0" json:"gateway_provider_id"`
	// 路由快照（写入时固化，不依赖 route 表存活）
	ProviderName     string `gorm:"size:128;not null;default:''" json:"provider_name,omitempty"`
	SourceAPIKeyID   int64  `gorm:"not null;default:0" json:"source_api_key_id,omitempty"`
	SourceAPIKeyName string `gorm:"size:256;not null;default:''" json:"source_api_key_name,omitempty"`
	SourceGroupID    *int64 `json:"source_group_id,omitempty"`
	SourceGroupName  string `gorm:"size:256;not null;default:''" json:"source_group_name,omitempty"`
	RequestID        string `gorm:"size:64;not null;index" json:"request_id"`
	// 同一 RequestID 下的尝试序号（从 1 起）；用于使用记录关联
	Attempt int `gorm:"not null;default:1;index" json:"attempt"`
	// primary | retry | failover | hedge | recovery | regex_reject
	AttemptKind string `gorm:"size:16;not null;default:'primary'" json:"attempt_kind,omitempty"`
	// accepted | rejected | error | canceled；保留 Success 兼容原版查询。
	AttemptStatus        string `gorm:"size:16;not null;default:'';index" json:"attempt_status,omitempty"`
	Winner               bool   `gorm:"not null;default:false;index" json:"winner"`
	ValidationRuleID     *uint  `gorm:"index" json:"validation_rule_id,omitempty"`
	ValidationRuleName   string `gorm:"size:128;not null;default:''" json:"validation_rule_name,omitempty"`
	ValidationReason     string `gorm:"type:text" json:"validation_reason,omitempty"`
	ValidationPostCommit bool   `gorm:"not null;default:false" json:"validation_post_commit"`
	// 本条失败后写入的冷却截止（若有），便于日志展示
	CooldownUntil     *time.Time `json:"cooldown_until,omitempty"`
	RequestedModel    string     `gorm:"size:256;not null;index" json:"requested_model"`
	UpstreamModel     string     `gorm:"size:256" json:"upstream_model,omitempty"`
	ModelMappingChain string     `gorm:"size:512" json:"model_mapping_chain,omitempty"`
	InboundEndpoint   string     `gorm:"size:128" json:"inbound_endpoint,omitempty"`
	UpstreamEndpoint  string     `gorm:"size:128" json:"upstream_endpoint,omitempty"`
	InboundProtocol   string     `gorm:"size:16" json:"inbound_protocol,omitempty"`
	UpstreamProtocol  string     `gorm:"size:16" json:"upstream_protocol,omitempty"`
	ProtocolConverted bool       `gorm:"not null;default:false" json:"protocol_converted"`
	RequestType       int        `gorm:"not null;default:0;index" json:"request_type"`
	ServiceTier       string     `gorm:"size:64" json:"service_tier,omitempty"`
	ReasoningEffort   string     `gorm:"size:32" json:"reasoning_effort,omitempty"`
	BillingMode       string     `gorm:"size:32;not null;default:'token'" json:"billing_mode"`
	// Token 互斥桶（对齐 sub2api）：
	// InputTokens = 不含缓存的「新鲜输入」；CacheRead/Creation 单独计
	InputTokens           int `gorm:"not null;default:0" json:"input_tokens"`
	OutputTokens          int `gorm:"not null;default:0" json:"output_tokens"`
	CacheCreationTokens   int `gorm:"not null;default:0" json:"cache_creation_tokens"`
	CacheReadTokens       int `gorm:"not null;default:0" json:"cache_read_tokens"`
	CacheCreation5mTokens int `gorm:"not null;default:0" json:"cache_creation_5m_tokens"`
	CacheCreation1hTokens int `gorm:"not null;default:0" json:"cache_creation_1h_tokens"`
	ImageOutputTokens     int `gorm:"not null;default:0" json:"image_output_tokens"`
	// 推理 token（completion_tokens_details.reasoning_tokens），展示用，费用含在 output 单价内
	ReasoningTokens   int     `gorm:"not null;default:0" json:"reasoning_tokens"`
	InputCost         float64 `gorm:"not null;default:0" json:"input_cost"`
	OutputCost        float64 `gorm:"not null;default:0" json:"output_cost"`
	CacheCreationCost float64 `gorm:"not null;default:0" json:"cache_creation_cost"`
	CacheReadCost     float64 `gorm:"not null;default:0" json:"cache_read_cost"`
	// VirtualCacheReadTokens/Cost are settlement-only credits granted by the
	// gateway when a real concurrent hedge was used. They remain separate from
	// the raw upstream usage buckets above.
	VirtualCacheReadTokens int     `gorm:"not null;default:0" json:"virtual_cache_read_tokens"`
	VirtualCacheReadCost   float64 `gorm:"not null;default:0" json:"virtual_cache_read_cost"`
	VirtualCacheReason     string  `gorm:"size:32;not null;default:'';index" json:"virtual_cache_reason,omitempty"`
	ImageOutputCost        float64 `gorm:"not null;default:0" json:"image_output_cost"`
	TotalCost              float64 `gorm:"not null;default:0" json:"total_cost"`
	ActualCost             float64 `gorm:"not null;default:0" json:"actual_cost"`
	// BilledCost is the amount charged to the gateway key. It is populated at
	// finalization and can differ from ActualCost when virtual cache is used.
	BilledCost float64 `gorm:"not null;default:0" json:"billed_cost"`
	// EstimatedCost 是该 attempt 的估算上游成本；EstimatedExtraCost 仅用于
	// loser / rejected attempt 的额外成本核算，不参与网关 Key 扣费。
	EstimatedCost         float64 `gorm:"not null;default:0" json:"estimated_cost"`
	EstimatedExtraCost    float64 `gorm:"not null;default:0" json:"estimated_extra_cost"`
	AccountStatsCost      float64 `gorm:"not null;default:0" json:"account_stats_cost"`
	RateMultiplier        float64 `gorm:"not null;default:1" json:"rate_multiplier"`
	BillingRateMultiplier float64 `gorm:"not null;default:1" json:"billing_rate_multiplier"`
	AccountRateMultiplier float64 `gorm:"not null;default:1" json:"account_rate_multiplier"`
	Stream                bool    `gorm:"not null;default:false" json:"stream"`
	StatusCode            int     `gorm:"not null;default:0" json:"status_code"`
	Success               bool    `gorm:"not null;default:false;index" json:"success"`
	// ErrorMessage 短摘要（列表/告警）；ErrorDetail / UpstreamErrorBody 供 debug 追踪
	ErrorMessage         string    `gorm:"type:text" json:"error_message,omitempty"`
	ErrorType            string    `gorm:"size:32;not null;default:'';index" json:"error_type,omitempty"` // transport|http|config|internal
	ErrorDetail          string    `gorm:"type:text" json:"error_detail,omitempty"`
	UpstreamURL          string    `gorm:"size:512" json:"upstream_url,omitempty"`
	UpstreamErrorBody    string    `gorm:"type:text" json:"upstream_error_body,omitempty"`
	UpstreamErrorHeaders string    `gorm:"type:text" json:"upstream_error_headers,omitempty"`
	DurationMS           int64     `gorm:"not null;default:0" json:"duration_ms"`
	FirstTokenMS         *int64    `json:"first_token_ms,omitempty"`
	IPAddress            string    `gorm:"size:64" json:"ip_address,omitempty"`
	UserAgent            string    `gorm:"size:512" json:"user_agent,omitempty"`
	CreatedAt            time.Time `gorm:"not null;index" json:"created_at"`
}

func (GatewayUsageLog) TableName() string { return "gateway_usage_logs" }

// GatewayRequestFinalization 是请求级不可变终态标记。成功交付和终端失败都写入，
// 以 request_id 主键保证并发/重放只会完成一次结算。
type GatewayRequestFinalization struct {
	RequestID              string    `gorm:"size:64;primaryKey" json:"request_id"`
	GatewayKeyID           uint      `gorm:"not null;index" json:"gateway_key_id"`
	Delivered              bool      `gorm:"not null;default:false;index" json:"delivered"`
	WinnerAttempt          int       `gorm:"not null;default:0" json:"winner_attempt"`
	WinnerUsageLogID       uint      `gorm:"not null;default:0;index" json:"winner_usage_log_id"`
	ActualCost             float64   `gorm:"not null;default:0" json:"actual_cost"`
	BilledCost             float64   `gorm:"not null;default:0" json:"billed_cost"`
	VirtualCacheReadTokens int       `gorm:"not null;default:0" json:"virtual_cache_read_tokens"`
	VirtualCacheReadCost   float64   `gorm:"not null;default:0" json:"virtual_cache_read_cost"`
	VirtualCacheReason     string    `gorm:"size:32;not null;default:'';index" json:"virtual_cache_reason,omitempty"`
	CreatedAt              time.Time `gorm:"not null" json:"created_at"`
}

func (GatewayRequestFinalization) TableName() string { return "gateway_request_finalizations" }

// GatewayWinnerSettlement 保存已成功交付的唯一 winner 结算快照。
type GatewayWinnerSettlement struct {
	RequestID              string    `gorm:"size:64;primaryKey" json:"request_id"`
	GatewayKeyID           uint      `gorm:"not null;index" json:"gateway_key_id"`
	RouteID                uint      `gorm:"not null;default:0;index" json:"route_id"`
	WinnerAttempt          int       `gorm:"not null" json:"winner_attempt"`
	GatewayUsageLogID      uint      `gorm:"not null;default:0;index" json:"gateway_usage_log_id"`
	ActualCost             float64   `gorm:"not null;default:0" json:"actual_cost"`
	BilledCost             float64   `gorm:"not null;default:0" json:"billed_cost"`
	VirtualCacheReadTokens int       `gorm:"not null;default:0" json:"virtual_cache_read_tokens"`
	VirtualCacheReadCost   float64   `gorm:"not null;default:0" json:"virtual_cache_read_cost"`
	VirtualCacheReason     string    `gorm:"size:32;not null;default:'';index" json:"virtual_cache_reason,omitempty"`
	CreatedAt              time.Time `gorm:"not null" json:"created_at"`
}

func (GatewayWinnerSettlement) TableName() string { return "gateway_winner_settlements" }

// ModelPriceOverride 覆盖内置模型单价（per-token，USD）。
type ModelPriceOverride struct {
	ID                         uint      `gorm:"primaryKey" json:"id"`
	ModelName                  string    `gorm:"size:256;not null;uniqueIndex" json:"model_name"`
	InputPricePerToken         float64   `gorm:"not null;default:0" json:"input_price_per_token"`
	OutputPricePerToken        float64   `gorm:"not null;default:0" json:"output_price_per_token"`
	CacheCreationPricePerToken float64   `gorm:"not null;default:0" json:"cache_creation_price_per_token"`
	CacheReadPricePerToken     float64   `gorm:"not null;default:0" json:"cache_read_price_per_token"`
	Enabled                    bool      `gorm:"default:true" json:"enabled"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
}

func (ModelPriceOverride) TableName() string { return "model_price_overrides" }
