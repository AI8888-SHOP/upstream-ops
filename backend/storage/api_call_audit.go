package storage

import (
	"fmt"
	"math"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"gorm.io/gorm"
)

// GatewayAPICallLog records each real upstream HTTP call independently from
// gateway usage and billing rows. A request is inserted as pending before the
// upstream call and completed by UpdateByRequestID/SaveFinish afterwards.
type GatewayAPICallLog struct {
	ID                    uint       `gorm:"primaryKey" json:"id"`
	RequestID             string     `gorm:"size:128;not null;uniqueIndex" json:"request_id"`
	GatewayRequestID      string     `gorm:"size:128;not null;default:'';index" json:"gateway_request_id"`
	Purpose               string     `gorm:"size:32;not null;index:idx_gateway_api_call_purpose_created,priority:1" json:"purpose"`
	Method                string     `gorm:"size:16;not null;default:'POST'" json:"method"`
	Host                  string     `gorm:"size:255;not null;default:''" json:"host"`
	Endpoint              string     `gorm:"size:512;not null;default:''" json:"endpoint"`
	Model                 string     `gorm:"size:256;not null;default:'';index" json:"model"`
	GatewayGroupID        uint       `gorm:"not null;default:0;index" json:"gateway_group_id"`
	RouteID               uint       `gorm:"not null;default:0;index" json:"route_id"`
	ChannelID             uint       `gorm:"not null;default:0;index:idx_gateway_api_call_channel_created,priority:1" json:"channel_id"`
	GatewayProviderID     uint       `gorm:"not null;default:0;index:idx_gateway_api_call_provider_created,priority:1" json:"gateway_provider_id"`
	SourceGroupID         *int64     `gorm:"index" json:"source_group_id,omitempty"`
	SourceAPIKeyID        int64      `gorm:"not null;default:0;index" json:"source_api_key_id"`
	SourceAPIKeyName      string     `gorm:"size:256;not null;default:'';index" json:"source_api_key_name"`
	UpstreamRequestID     string     `gorm:"size:256;not null;default:''" json:"upstream_request_id"`
	StatusCode            int        `gorm:"not null;default:0" json:"status_code"`
	Status                string     `gorm:"size:16;not null;default:'pending';index" json:"status"`
	ErrorType             string     `gorm:"size:32;not null;default:'';index" json:"error_type"`
	ErrorMessage          string     `gorm:"type:text" json:"error_message,omitempty"`
	Stream                bool       `gorm:"not null;default:false" json:"stream"`
	UsageKnown            bool       `gorm:"not null;default:false;index" json:"usage_known"`
	InputTokens           int        `gorm:"not null;default:0" json:"input_tokens"`
	OutputTokens          int        `gorm:"not null;default:0" json:"output_tokens"`
	CacheReadTokens       int        `gorm:"not null;default:0" json:"cache_read_tokens"`
	CacheCreationTokens   int        `gorm:"not null;default:0" json:"cache_creation_tokens"`
	CacheCreation5mTokens int        `gorm:"column:cache_creation_5m_tokens;not null;default:0" json:"cache_creation_5m_tokens"`
	CacheCreation1hTokens int        `gorm:"column:cache_creation_1h_tokens;not null;default:0" json:"cache_creation_1h_tokens"`
	ImageOutputTokens     int        `gorm:"not null;default:0" json:"image_output_tokens"`
	ReasoningTokens       int        `gorm:"not null;default:0" json:"reasoning_tokens"`
	EstimatedCost         *float64   `json:"estimated_cost,omitempty"`
	StartedAt             time.Time  `gorm:"not null;index" json:"started_at"`
	FinishedAt            *time.Time `json:"finished_at,omitempty"`
	DurationMS            int64      `gorm:"not null;default:0" json:"duration_ms"`
	FirstTokenMS          *int64     `json:"first_token_ms,omitempty"`
	CreatedAt             time.Time  `gorm:"not null;index:idx_gateway_api_call_purpose_created,priority:2;index:idx_gateway_api_call_channel_created,priority:2;index:idx_gateway_api_call_provider_created,priority:2;index:idx_gateway_api_call_created" json:"created_at"`
}

func (GatewayAPICallLog) TableName() string { return "gateway_api_call_logs" }

const (
	GatewayAPICallPurposeGateway          = "gateway"
	GatewayAPICallPurposeCandyCheck       = "candy_check"
	GatewayAPICallPurposeModelProbe       = "model_probe"
	GatewayAPICallPurposeAdminModelTest   = "admin_model_test"
	GatewayAPICallPurposeOptimizerExplain = "optimizer_explain"
	GatewayAPICallPurposeChannelAPI       = "channel_api"
	GatewayAPICallPurposeSyncAPI          = "sync_api"

	GatewayAPICallStatusPending    = "pending"
	GatewayAPICallStatusSuccess    = "success"
	GatewayAPICallStatusError      = "error"
	GatewayAPICallStatusIncomplete = "incomplete"
)

// GatewayAPICallLogQuery controls the bounded admin list and aggregate scope.
type GatewayAPICallLogQuery struct {
	Purpose           string
	ChannelID         uint
	GatewayProviderID uint
	SourceGroupID     *int64
	SourceAPIKeyName  string
	Model             string
	Status            string
	Since             *time.Time
	Until             *time.Time
	Page              int
	PageSize          int
}

type GatewayAPICallLogPage struct {
	Items    []GatewayAPICallLog `json:"items"`
	Total    int64               `json:"total"`
	Page     int                 `json:"page"`
	PageSize int                 `json:"page_size"`
}

// GatewayAPICallLogStats is intentionally independent from billing usage.
// EstimatedCost is nullable per row; TotalEstimatedCost sums only known costs.
type GatewayAPICallLogStats struct {
	TotalCalls               int64    `json:"total_calls"`
	Total                    int64    `json:"total"`
	TotalRealCalls           int64    `json:"total_real_calls"`
	SuccessCalls             int64    `json:"success_calls"`
	SuccessCount             int64    `json:"success_count"`
	ErrorCalls               int64    `json:"error_calls"`
	ErrorCount               int64    `json:"error_count"`
	Failed                   int64    `json:"failed"`
	PendingCalls             int64    `json:"pending_calls"`
	Pending                  int64    `json:"pending"`
	InProgress               int64    `json:"in_progress"`
	IncompleteCalls          int64    `json:"incomplete_calls"`
	TotalInputTokens         int64    `json:"total_input_tokens"`
	TotalOutputTokens        int64    `json:"total_output_tokens"`
	TotalCacheReadTokens     int64    `json:"total_cache_read_tokens"`
	TotalCacheCreationTokens int64    `json:"total_cache_creation_tokens"`
	TotalTokens              int64    `json:"total_tokens"`
	TotalEstimatedCost       *float64 `json:"total_estimated_cost"`
	KnownEstimatedCost       *float64 `json:"known_estimated_cost"`
	UnknownCostCalls         int64    `json:"unknown_cost_calls"`
	UnknownCostCount         int64    `json:"unknown_cost_count"`
	UsageKnownCalls          int64    `json:"usage_known_calls"`
	UnknownUsage             int64    `json:"unknown_usage"`
	UnknownUsageCount        int64    `json:"unknown_usage_count"`
}

// GatewayAPICallLogs is the independent upstream-call audit repository.
type GatewayAPICallLogs struct{ db *gorm.DB }

func NewGatewayAPICallLogs(db *gorm.DB) *GatewayAPICallLogs { return &GatewayAPICallLogs{db: db} }

// APICalls exposes the independent audit repository without changing existing
// usage and billing behavior.
func (r *GatewayUsageLogs) APICalls() *GatewayAPICallLogs {
	if r == nil {
		return nil
	}
	return NewGatewayAPICallLogs(r.db)
}

func (r *GatewayAPICallLogs) Create(item *GatewayAPICallLog) error {
	if r == nil || r.db == nil || item == nil {
		return fmt.Errorf("api call audit storage is unavailable")
	}
	item.RequestID = strings.TrimSpace(item.RequestID)
	if item.RequestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if item.StartedAt.IsZero() {
		item.StartedAt = time.Now().UTC()
	}
	if item.CreatedAt.IsZero() {
		item.CreatedAt = item.StartedAt
	}
	if item.Status == "" {
		item.Status = GatewayAPICallStatusPending
	}
	if err := normalizeGatewayAPICallLog(item); err != nil {
		return err
	}
	return r.db.Create(item).Error
}

// UpdateByRequestID updates the supplied fields, including explicit zero/nil
// values. Initial identity and attribution fields remain immutable.
func (r *GatewayAPICallLogs) UpdateByRequestID(requestID string, updates map[string]any) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("api call audit storage is unavailable")
	}
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return fmt.Errorf("request_id is required")
	}
	if len(updates) == 0 {
		return nil
	}
	allowed := map[string]struct{}{"status": {}, "status_code": {}, "error_type": {}, "error_message": {}, "upstream_request_id": {}, "usage_known": {}, "input_tokens": {}, "output_tokens": {}, "cache_read_tokens": {}, "cache_creation_tokens": {}, "cache_creation_5m_tokens": {}, "cache_creation_1h_tokens": {}, "image_output_tokens": {}, "reasoning_tokens": {}, "estimated_cost": {}, "finished_at": {}, "duration_ms": {}, "first_token_ms": {}, "model": {}, "endpoint": {}, "host": {}, "method": {}, "stream": {}}
	clean := make(map[string]any, len(updates))
	for key, value := range updates {
		if _, ok := allowed[key]; !ok {
			return fmt.Errorf("field %q cannot be updated", key)
		}
		if isGatewayAPICallTokenField(key) {
			n, ok := value.(int)
			if !ok || n < 0 {
				return fmt.Errorf("%s must be a non-negative integer", key)
			}
		}
		clean[key] = value
	}
	if status, ok := clean["status"].(string); ok && status != "" {
		if err := validateGatewayAPICallStatus(status); err != nil {
			return err
		}
	}
	if cost, ok := clean["estimated_cost"].(*float64); ok && cost != nil && (math.IsNaN(*cost) || math.IsInf(*cost, 0) || *cost < 0) {
		return fmt.Errorf("estimated_cost must be finite and non-negative")
	}
	result := r.db.Model(&GatewayAPICallLog{}).Where("request_id = ?", requestID).Updates(clean)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		// MySQL reports changed rows rather than matched rows by default, so a
		// repeated identical finish is a valid no-op when the call still exists.
		var count int64
		if err := r.db.Model(&GatewayAPICallLog{}).Where("request_id = ?", requestID).Count(&count).Error; err != nil {
			return err
		}
		if count == 0 {
			return gorm.ErrRecordNotFound
		}
	}
	return nil
}

// SaveFinish updates the terminal fields for an existing call.
func (r *GatewayAPICallLogs) SaveFinish(item *GatewayAPICallLog) error {
	if item == nil {
		return fmt.Errorf("api call log is required")
	}
	if strings.TrimSpace(item.RequestID) == "" {
		return fmt.Errorf("request_id is required")
	}
	if item.Status == "" || item.Status == GatewayAPICallStatusPending {
		return fmt.Errorf("finish status must be success, error, or incomplete")
	}
	if err := validateGatewayAPICallStatus(item.Status); err != nil {
		return err
	}
	updates := map[string]any{
		"status": item.Status, "status_code": item.StatusCode, "error_type": item.ErrorType,
		"error_message": item.ErrorMessage, "upstream_request_id": item.UpstreamRequestID,
		"usage_known": item.UsageKnown, "input_tokens": item.InputTokens, "output_tokens": item.OutputTokens,
		"cache_read_tokens": item.CacheReadTokens, "cache_creation_tokens": item.CacheCreationTokens,
		"cache_creation_5m_tokens": item.CacheCreation5mTokens, "cache_creation_1h_tokens": item.CacheCreation1hTokens,
		"image_output_tokens": item.ImageOutputTokens, "reasoning_tokens": item.ReasoningTokens,
		"estimated_cost": item.EstimatedCost, "finished_at": item.FinishedAt, "duration_ms": item.DurationMS,
		"first_token_ms": item.FirstTokenMS,
	}
	if item.FinishedAt == nil {
		now := time.Now().UTC()
		updates["finished_at"] = &now
	}
	return r.UpdateByRequestID(item.RequestID, updates)
}

func (r *GatewayAPICallLogs) List(q GatewayAPICallLogQuery) (*GatewayAPICallLogPage, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("api call audit storage is unavailable")
	}
	q.normalize()
	db := applyGatewayAPICallLogFilters(r.db.Model(&GatewayAPICallLog{}), q)
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, err
	}
	items := make([]GatewayAPICallLog, 0, q.PageSize)
	offset := (q.Page - 1) * q.PageSize
	if err := db.Order("created_at DESC, id DESC").Offset(offset).Limit(q.PageSize).Find(&items).Error; err != nil {
		return nil, err
	}
	return &GatewayAPICallLogPage{Items: items, Total: total, Page: q.Page, PageSize: q.PageSize}, nil
}

func (r *GatewayAPICallLogs) Stats(q GatewayAPICallLogQuery) (*GatewayAPICallLogStats, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("api call audit storage is unavailable")
	}
	q.normalize()
	db := applyGatewayAPICallLogFilters(r.db.Model(&GatewayAPICallLog{}), q)
	var rows []GatewayAPICallLogStats
	selectExpr := "COUNT(*) AS total_calls, " +
		"COALESCE(SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END), 0) AS success_calls, " +
		"COALESCE(SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END), 0) AS error_calls, " +
		"COALESCE(SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END), 0) AS pending_calls, " +
		"COALESCE(SUM(CASE WHEN status = 'incomplete' THEN 1 ELSE 0 END), 0) AS incomplete_calls, " +
		"COALESCE(SUM(input_tokens), 0) AS total_input_tokens, COALESCE(SUM(output_tokens), 0) AS total_output_tokens, " +
		"COALESCE(SUM(cache_read_tokens), 0) AS total_cache_read_tokens, COALESCE(SUM(cache_creation_tokens), 0) AS total_cache_creation_tokens, " +
		"SUM(CASE WHEN estimated_cost IS NOT NULL THEN estimated_cost ELSE NULL END) AS total_estimated_cost, " +
		"COALESCE(SUM(CASE WHEN estimated_cost IS NULL THEN 1 ELSE 0 END), 0) AS unknown_cost_calls, " +
		"COALESCE(SUM(CASE WHEN usage_known = " + boolLiteral(r.db, true) + " THEN 1 ELSE 0 END), 0) AS usage_known_calls"
	if err := db.Select(selectExpr).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return &GatewayAPICallLogStats{}, nil
	}
	stats := rows[0]
	stats.Total = stats.TotalCalls
	stats.TotalRealCalls = stats.TotalCalls
	stats.SuccessCount = stats.SuccessCalls
	stats.ErrorCount = stats.ErrorCalls
	stats.Failed = stats.ErrorCalls
	stats.Pending = stats.PendingCalls
	stats.InProgress = stats.PendingCalls
	stats.TotalTokens = stats.TotalInputTokens + stats.TotalOutputTokens + stats.TotalCacheReadTokens + stats.TotalCacheCreationTokens
	stats.KnownEstimatedCost = stats.TotalEstimatedCost
	stats.UnknownCostCount = stats.UnknownCostCalls
	stats.UnknownUsage = stats.TotalCalls - stats.UsageKnownCalls
	if stats.UnknownUsage < 0 {
		stats.UnknownUsage = 0
	}
	stats.UnknownUsageCount = stats.UnknownUsage
	return &stats, nil
}

const gatewayAPICallMinimumRetention = 90 * 24 * time.Hour

// DeleteBefore deletes only rows older than the requested cutoff while always
// retaining the most recent 90 days. Scheduler wiring deliberately remains
// separate so operators can choose when to invoke this maintenance method.
func (r *GatewayAPICallLogs) DeleteBefore(before time.Time) (int64, error) {
	if r == nil || r.db == nil {
		return 0, fmt.Errorf("api call audit storage is unavailable")
	}
	if before.IsZero() {
		return 0, fmt.Errorf("retention cutoff is required")
	}
	minimumCutoff := time.Now().UTC().Add(-gatewayAPICallMinimumRetention)
	if before.After(minimumCutoff) {
		before = minimumCutoff
	}
	result := r.db.Where("created_at < ?", before).Delete(&GatewayAPICallLog{})
	return result.RowsAffected, result.Error
}

func (q *GatewayAPICallLogQuery) normalize() {
	if q.Page <= 0 {
		q.Page = 1
	}
	if q.Page > 100000 {
		q.Page = 100000
	}
	if q.PageSize <= 0 {
		q.PageSize = 50
	}
	if q.PageSize > 200 {
		q.PageSize = 200
	}
	q.Purpose = strings.TrimSpace(q.Purpose)
	q.SourceAPIKeyName = strings.TrimSpace(q.SourceAPIKeyName)
	q.Model = strings.TrimSpace(q.Model)
	q.Status = strings.TrimSpace(q.Status)
}

func applyGatewayAPICallLogFilters(db *gorm.DB, q GatewayAPICallLogQuery) *gorm.DB {
	if q.Purpose != "" {
		db = db.Where("purpose = ?", q.Purpose)
	}
	if q.ChannelID > 0 {
		db = db.Where("channel_id = ?", q.ChannelID)
	}
	if q.GatewayProviderID > 0 {
		db = db.Where("gateway_provider_id = ?", q.GatewayProviderID)
	}
	if q.SourceGroupID != nil {
		db = db.Where("source_group_id = ?", *q.SourceGroupID)
	}
	if q.SourceAPIKeyName != "" {
		db = db.Where("source_api_key_name = ?", q.SourceAPIKeyName)
	}
	if q.Model != "" {
		db = db.Where("model = ?", q.Model)
	}
	if q.Status != "" {
		db = db.Where("status = ?", q.Status)
	}
	if q.Since != nil {
		db = apiCallTimeWhere(db, "created_at >= ?", *q.Since)
	}
	if q.Until != nil {
		db = apiCallTimeWhere(db, "created_at <= ?", *q.Until)
	}
	return db
}

func apiCallTimeWhere(db *gorm.DB, clause string, t time.Time) *gorm.DB {
	if isSQLite(db) {
		op := ">="
		if strings.Contains(clause, "<=") {
			op = "<="
		}
		return db.Where("CAST(strftime('%s', created_at) AS INTEGER) "+op+" ?", t.Unix())
	}
	return db.Where(clause, t)
}

func boolLiteral(db *gorm.DB, value bool) string {
	if strings.EqualFold(db.Dialector.Name(), "postgres") || strings.EqualFold(db.Dialector.Name(), "mysql") {
		if value {
			return "TRUE"
		}
		return "FALSE"
	}
	if value {
		return "1"
	}
	return "0"
}

func validateGatewayAPICallStatus(status string) error {
	switch status {
	case GatewayAPICallStatusPending, GatewayAPICallStatusSuccess, GatewayAPICallStatusError, GatewayAPICallStatusIncomplete:
		return nil
	}
	return fmt.Errorf("invalid api call status %q", status)
}

func normalizeGatewayAPICallLog(item *GatewayAPICallLog) error {
	for field, value := range map[string]int{
		"input_tokens": item.InputTokens, "output_tokens": item.OutputTokens,
		"cache_read_tokens": item.CacheReadTokens, "cache_creation_tokens": item.CacheCreationTokens,
		"cache_creation_5m_tokens": item.CacheCreation5mTokens, "cache_creation_1h_tokens": item.CacheCreation1hTokens,
		"image_output_tokens": item.ImageOutputTokens, "reasoning_tokens": item.ReasoningTokens,
	} {
		if value < 0 {
			return fmt.Errorf("%s must be a non-negative integer", field)
		}
	}
	if err := validateGatewayAPICallStatus(item.Status); err != nil {
		return err
	}
	if err := validateGatewayAPICallPurpose(item.Purpose); err != nil {
		return err
	}
	for name, value := range map[string]*string{"request_id": &item.RequestID, "purpose": &item.Purpose, "method": &item.Method, "host": &item.Host, "endpoint": &item.Endpoint, "model": &item.Model, "source_api_key_name": &item.SourceAPIKeyName, "upstream_request_id": &item.UpstreamRequestID, "error_type": &item.ErrorType} {
		*value = strings.TrimSpace(*value)
		if name == "endpoint" && *value != "" {
			parsed, err := url.Parse(*value)
			if err != nil {
				return fmt.Errorf("invalid endpoint: %w", err)
			}
			parsed.RawQuery = ""
			parsed.Fragment = ""
			*value = parsed.EscapedPath()
			if *value == "" {
				*value = "/"
			}
		}
		if !utf8.ValidString(*value) {
			return fmt.Errorf("%s must be valid utf-8", name)
		}
	}
	if item.Method == "" {
		item.Method = "POST"
	}
	if item.Status == GatewayAPICallStatusPending && item.FinishedAt != nil {
		return fmt.Errorf("pending api call cannot have finished_at")
	}
	if item.EstimatedCost != nil && (math.IsNaN(*item.EstimatedCost) || math.IsInf(*item.EstimatedCost, 0) || *item.EstimatedCost < 0) {
		return fmt.Errorf("estimated_cost must be finite and non-negative")
	}
	return nil
}

func isGatewayAPICallTokenField(field string) bool {
	switch field {
	case "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens", "cache_creation_5m_tokens", "cache_creation_1h_tokens", "image_output_tokens", "reasoning_tokens":
		return true
	default:
		return false
	}
}

// ensureGatewayAPICallIndexes matches SQLite's Unix-second filter expression.
// Other dialects use the portable created_at indexes declared on the model.
func ensureGatewayAPICallIndexes(db *gorm.DB) error {
	if !isSQLite(db) {
		return nil
	}
	return db.Exec("CREATE INDEX IF NOT EXISTS idx_gateway_api_call_created_unix ON gateway_api_call_logs(CAST(strftime('%s', created_at) AS INTEGER))").Error
}

func validateGatewayAPICallPurpose(purpose string) error {
	switch purpose {
	case GatewayAPICallPurposeGateway, GatewayAPICallPurposeCandyCheck, GatewayAPICallPurposeModelProbe, GatewayAPICallPurposeAdminModelTest, GatewayAPICallPurposeOptimizerExplain, GatewayAPICallPurposeChannelAPI, GatewayAPICallPurposeSyncAPI:
		return nil
	default:
		return fmt.Errorf("invalid api call purpose %q", purpose)
	}
}
