package storage

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// GatewayOptimizerSample 是优化器分析所需的一行最小用量快照。
// 只携带诊断规则真正会用到的列，避免把整张宽表拉进内存。
type GatewayOptimizerSample struct {
	GatewayGroupID uint
	RouteID        uint
	Model          string
	AttemptStatus  string
	Success        bool
	FirstTokenMS   *int64
	ErrorMessage   string
	CreatedAt      time.Time
}

// GatewayOptimizerSamples 读取窗口内的最小用量样本，供一键优化规则诊断使用。
// 分位与成功率一律在 Go 侧计算：报表跑在 SQLite 与 PostgreSQL 两种方言上，
// percentile_cont 在测试库不存在，硬编码会让分析器只能在生产跑通。
func (r *GatewayUsageLogs) GatewayOptimizerSamples(from, to time.Time, limit int) ([]GatewayOptimizerSample, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("usage storage is unavailable")
	}
	if limit <= 0 {
		limit = 200000
	}
	if to.IsZero() {
		to = time.Now()
	}
	type row struct {
		GatewayGroupID uint
		RouteID        uint
		UpstreamModel  string
		RequestedModel string
		AttemptStatus  string
		Success        bool
		FirstTokenMS   *int64
		ErrorMessage   string
		CreatedAt      time.Time
	}
	var rows []row
	err := r.db.Model(&GatewayUsageLog{}).
		Select("gateway_group_id, route_id, upstream_model, requested_model, attempt_status, success, first_token_ms, error_message, created_at").
		Where("created_at >= ? AND created_at < ?", from, to).
		Order("id DESC").
		Limit(limit).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	samples := make([]GatewayOptimizerSample, 0, len(rows))
	for _, item := range rows {
		model := strings.TrimSpace(item.UpstreamModel)
		if model == "" {
			model = strings.TrimSpace(item.RequestedModel)
		}
		samples = append(samples, GatewayOptimizerSample{
			GatewayGroupID: item.GatewayGroupID,
			RouteID:        item.RouteID,
			Model:          model,
			AttemptStatus:  item.AttemptStatus,
			Success:        item.Success,
			FirstTokenMS:   item.FirstTokenMS,
			ErrorMessage:   item.ErrorMessage,
			CreatedAt:      item.CreatedAt,
		})
	}
	return samples, nil
}

// GatewayOptimizerRun 记录一次已应用的一键优化：既作审计，也作护栏来源——
// 同一个网关组在短时间内不应被反复抬高首字超时，否则规则会自我叠加。
type GatewayOptimizerRun struct {
	ID          uint      `gorm:"primaryKey" json:"id"`
	CreatedAt   time.Time `gorm:"not null;index" json:"created_at"`
	WindowHours int       `gorm:"not null;default:24" json:"window_hours"`
	GroupIDs    string    `gorm:"type:text;not null;default:''" json:"group_ids"`
	ChangesJSON string    `gorm:"type:text;not null;default:''" json:"changes_json"`
}

func (GatewayOptimizerRun) TableName() string { return "gateway_optimizer_runs" }

// RecordOptimizerRun 落一条应用记录。
func (r *GatewayUsageLogs) RecordOptimizerRun(windowHours int, groupIDs []uint, changesJSON string) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("usage storage is unavailable")
	}
	encoded, _ := json.Marshal(groupIDs)
	return r.db.Create(&GatewayOptimizerRun{
		CreatedAt: time.Now(), WindowHours: windowHours,
		GroupIDs: string(encoded), ChangesJSON: changesJSON,
	}).Error
}

// RecentOptimizerGroupIDs 返回窗口内已被优化过的网关组，用于抑制规则叠加。
func (r *GatewayUsageLogs) RecentOptimizerGroupIDs(from time.Time) (map[uint]struct{}, error) {
	out := map[uint]struct{}{}
	if r == nil || r.db == nil {
		return out, nil
	}
	var runs []GatewayOptimizerRun
	if err := r.db.Where("created_at >= ?", from).Find(&runs).Error; err != nil {
		return out, err
	}
	for _, run := range runs {
		var ids []uint
		if err := json.Unmarshal([]byte(run.GroupIDs), &ids); err != nil {
			continue
		}
		for _, id := range ids {
			out[id] = struct{}{}
		}
	}
	return out, nil
}
