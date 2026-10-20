package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ModelPriceContextTier replaces prices for the entire request above the input
// context threshold (fresh input + real cache read + cache creation), not only
// the excess tokens. Nil prices inherit the preceding/base tier; zero is free.
type ModelPriceContextTier struct {
	AboveInputTokens             int      `json:"above_input_tokens"`
	Inclusive                    bool     `json:"inclusive,omitempty"`
	InputPricePerToken           *float64 `json:"input_price_per_token,omitempty"`
	OutputPricePerToken          *float64 `json:"output_price_per_token,omitempty"`
	CacheCreationPricePerToken   *float64 `json:"cache_creation_price_per_token,omitempty"`
	CacheCreation1hPricePerToken *float64 `json:"cache_creation_1h_price_per_token,omitempty"`
	CacheReadPricePerToken       *float64 `json:"cache_read_price_per_token,omitempty"`
}

func ValidateModelPrice(item *ModelPriceOverride) error {
	item.ModelName = strings.TrimSpace(item.ModelName)
	if item.ModelName == "" || len(item.ModelName) > 256 {
		return fmt.Errorf("模型名不能为空且不可超过 256 字节")
	}
	valid := func(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 1000 }
	for _, v := range []float64{item.InputPricePerToken, item.OutputPricePerToken, item.CacheCreationPricePerToken, item.CacheReadPricePerToken} {
		if !valid(v) {
			return fmt.Errorf("单价必须是 0 到 1000 之间的有限 per-token 数值")
		}
	}
	if item.CacheCreation1hPricePerToken != nil && !valid(*item.CacheCreation1hPricePerToken) {
		return fmt.Errorf("1h 缓存写入价格必须是有效非负数")
	}
	if strings.TrimSpace(item.ContextTiersJSON) == "" {
		item.ContextTiersJSON = "[]"
	}
	if len(item.ContextTiersJSON) > 64*1024 {
		return fmt.Errorf("上下文阶梯价格数据过长")
	}
	var tiers []ModelPriceContextTier
	if err := json.Unmarshal([]byte(item.ContextTiersJSON), &tiers); err != nil {
		return fmt.Errorf("上下文阶梯价格 JSON 无效: %w", err)
	}
	if len(tiers) > 32 {
		return fmt.Errorf("最多设置 32 个上下文档位")
	}
	sort.Slice(tiers, func(i, j int) bool { return tiers[i].AboveInputTokens < tiers[j].AboveInputTokens })
	for i, t := range tiers {
		if t.AboveInputTokens <= 0 || (i > 0 && t.AboveInputTokens == tiers[i-1].AboveInputTokens) {
			return fmt.Errorf("上下文阈值必须是互不重复的正整数")
		}
		for _, v := range []*float64{t.InputPricePerToken, t.OutputPricePerToken, t.CacheCreationPricePerToken, t.CacheCreation1hPricePerToken, t.CacheReadPricePerToken} {
			if v != nil && !valid(*v) {
				return fmt.Errorf("上下文阶梯单价必须是有效非负数")
			}
		}
	}
	if tiers == nil {
		tiers = []ModelPriceContextTier{}
	}
	encoded, _ := json.Marshal(tiers)
	item.ContextTiersJSON = string(encoded)
	return nil
}

// NormalizePriceCacheCreation preserves aggregate token counts despite missing
// or contradictory TTL detail. Unknown remainder is priced at the 5m/base rate.
func NormalizePriceCacheCreation(total, fiveMinute, oneHour int) (int, int) {
	total, fiveMinute, oneHour = max(0, total), max(0, fiveMinute), max(0, oneHour)
	if total == 0 {
		return fiveMinute, oneHour
	}
	if oneHour > total || fiveMinute > total-oneHour {
		oneHour = int(math.Round(float64(total) * float64(oneHour) / (float64(fiveMinute) + float64(oneHour))))
	}
	return total - oneHour, oneHour
}

type ModelPriceRepricingJob struct {
	ID          uint       `gorm:"primaryKey" json:"id"`
	Generation  uint64     `gorm:"not null;default:0" json:"generation"`
	Status      string     `gorm:"size:16;not null;default:'idle'" json:"status"`
	LastID      uint       `gorm:"not null;default:0" json:"last_id"`
	UpperID     uint       `gorm:"not null;default:0" json:"upper_id"`
	UpdatedRows int64      `gorm:"not null;default:0" json:"updated_rows"`
	SkippedRows int64      `gorm:"not null;default:0" json:"skipped_rows"`
	Error       string     `gorm:"type:text" json:"error,omitempty"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (ModelPriceRepricingJob) TableName() string { return "model_price_repricing_jobs" }

// ApplyWithRepricing updates prices and starts/restarts a durable high-watermark
// scan in the same transaction. Existing manual overrides survive source sync.
func (r *ModelPriceOverrides) ApplyWithRepricing(ctx context.Context, items []ModelPriceOverride, deleteID uint, synced bool) (int, *ModelPriceRepricingJob, error) {
	for i := range items {
		if err := ValidateModelPrice(&items[i]); err != nil {
			return 0, nil, err
		}
	}
	changed := 0
	var job ModelPriceRepricingJob
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&ModelPriceRepricingJob{ID: 1}).Error; err != nil {
			return err
		}
		if err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&job, 1).Error; err != nil {
			return err
		}
		if deleteID > 0 {
			if err := tx.Delete(&ModelPriceOverride{}, deleteID).Error; err != nil {
				return err
			}
			changed++
		}
		if synced {
			var manual []ModelPriceOverride
			if err := tx.Select("model_name").Where("source <> ?", "sub2api").Find(&manual).Error; err != nil {
				return err
			}
			protected := map[string]bool{}
			for _, item := range manual {
				protected[strings.ToLower(item.ModelName)] = true
			}
			filtered := make([]ModelPriceOverride, 0, len(items))
			for _, item := range items {
				if !protected[strings.ToLower(item.ModelName)] {
					filtered = append(filtered, item)
				}
			}
			items = filtered
			for i := range items {
				items[i].Source = "sub2api"
			}
			if len(items) > 0 {
				conflict, err := modelPriceSyncConflict(tx)
				if err != nil {
					return err
				}
				result := tx.Clauses(conflict).CreateInBatches(items, 100)
				if result.Error != nil {
					return result.Error
				}
				changed += int(result.RowsAffected)
			}
		} else {
			for i := range items {
				items[i].Source = "manual"
				items[i].SourceURL = ""
				items[i].ID = 0
				var existing ModelPriceOverride
				err := tx.Where("LOWER(model_name) = ?", strings.ToLower(items[i].ModelName)).First(&existing).Error
				if err != nil && err != gorm.ErrRecordNotFound {
					return err
				}
				if err == nil {
					items[i].ID, items[i].CreatedAt, items[i].ModelName = existing.ID, existing.CreatedAt, existing.ModelName
				}
				enabled := items[i].Enabled
				if err := tx.Save(&items[i]).Error; err != nil {
					return err
				}
				if !enabled {
					if err := tx.Model(&items[i]).Update("enabled", false).Error; err != nil {
						return err
					}
				}
				changed++
			}
		}
		var upper uint
		if err := tx.Model(&GatewayUsageLog{}).Select("COALESCE(MAX(id),0)").Scan(&upper).Error; err != nil {
			return err
		}
		now := time.Now()
		job.Generation++
		job.Status = "pending"
		job.LastID = 0
		job.UpperID = upper
		job.UpdatedRows = 0
		job.SkippedRows = 0
		job.Error = ""
		job.StartedAt = &now
		job.CompletedAt = nil
		return tx.Save(&job).Error
	})
	if err == nil {
		r.invalidateCache()
	}
	return changed, &job, err
}

// AssignmentColumns uses literal database identifiers, unlike Select. Resolve
// them from the schema instead of JSON names: GORM keeps the numeral in
// CacheCreation1hPricePerToken attached to creation (cache_creation1h_...).
func modelPriceSyncConflict(db *gorm.DB) (clause.OnConflict, error) {
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(&ModelPriceOverride{}); err != nil {
		return clause.OnConflict{}, err
	}
	fields := []string{"ModelName", "InputPricePerToken", "OutputPricePerToken", "CacheCreationPricePerToken", "CacheCreation1hPricePerToken", "CacheReadPricePerToken", "ContextTiersJSON", "Source", "SourceURL", "Enabled", "UpdatedAt"}
	columns := make([]string, len(fields))
	for i, name := range fields {
		field := stmt.Schema.LookUpField(name)
		if field == nil || field.DBName == "" {
			return clause.OnConflict{}, fmt.Errorf("missing model price database field %s", name)
		}
		columns[i] = field.DBName
	}
	return clause.OnConflict{
		Columns:   []clause.Column{{Name: columns[0]}},
		DoUpdates: clause.AssignmentColumns(columns[1:]),
		Where: clause.Where{Exprs: []clause.Expression{clause.Eq{
			Column: clause.Column{Table: stmt.Schema.Table, Name: stmt.Schema.LookUpField("Source").DBName}, Value: "sub2api",
		}}},
	}, nil
}

func (r *ModelPriceOverrides) RepricingStatus(ctx context.Context) (*ModelPriceRepricingJob, error) {
	var job ModelPriceRepricingJob
	err := r.db.WithContext(ctx).First(&job, 1).Error
	if err == gorm.ErrRecordNotFound {
		return &ModelPriceRepricingJob{Status: "idle"}, nil
	}
	return &job, err
}

// RepriceBatch locks only the job and one small batch. Each committed cursor is
// restart-safe; newer price edits serialize and reset the cursor atomically.
// The callback returns only mutable reference-cost columns, never settlements.
func (r *ModelPriceOverrides) RepriceBatch(ctx context.Context, calculator func([]ModelPriceOverride) func(GatewayUsageLog) map[string]any) (bool, error) {
	done := false
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var job ModelPriceRepricingJob
		err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).First(&job, 1).Error
		if err == gorm.ErrRecordNotFound {
			done = true
			return nil
		}
		if err != nil {
			return err
		}
		if job.Status != "pending" && job.Status != "running" {
			done = true
			return nil
		}
		var prices []ModelPriceOverride
		if err := tx.Find(&prices).Error; err != nil {
			return err
		}
		calculate := calculator(prices)
		var rows []GatewayUsageLog
		// Select resolves Go field names through the model schema; JSON names
		// such as cache_creation_5m_tokens are not database column names.
		if err := tx.Select("ID", "RequestID", "AttemptStatus", "Winner", "BillingMode", "UpstreamModel", "RequestedModel", "InputTokens", "OutputTokens", "CacheReadTokens", "CacheCreationTokens", "CacheCreation5mTokens", "CacheCreation1hTokens", "ImageOutputTokens", "RateMultiplier", "BillingRateMultiplier").Where("id > ? AND id <= ?", job.LastID, job.UpperID).Order("id ASC").Limit(200).Find(&rows).Error; err != nil {
			return err
		}
		requestIDs := make([]string, 0, len(rows))
		for _, row := range rows {
			if row.AttemptStatus != "" {
				requestIDs = append(requestIDs, row.RequestID)
			}
		}
		finalized := map[string]bool{}
		if len(requestIDs) > 0 {
			var ids []string
			if err := tx.Model(&GatewayRequestFinalization{}).Where("request_id IN ?", requestIDs).Pluck("request_id", &ids).Error; err != nil {
				return err
			}
			for _, id := range ids {
				finalized[id] = true
			}
		}
		// Finalization can commit after the page read. Reload classifications in
		// one batch after observing terminal markers, avoiding per-row queries.
		finalizedRowIDs := make([]uint, 0, len(rows))
		for _, row := range rows {
			if row.AttemptStatus != "" && finalized[row.RequestID] {
				finalizedRowIDs = append(finalizedRowIDs, row.ID)
			}
		}
		winners := make(map[uint]bool, len(finalizedRowIDs))
		if len(finalizedRowIDs) > 0 {
			var states []struct {
				ID     uint
				Winner bool
			}
			if err := tx.Model(&GatewayUsageLog{}).Select("id", "winner").Where("id IN ?", finalizedRowIDs).Find(&states).Error; err != nil {
				return err
			}
			for _, state := range states {
				winners[state.ID] = state.Winner
			}
		}
		for _, row := range rows {
			// FinalizeRequest still reads actual_cost when debiting the key. Never
			// reprice a new-format attempt until its immutable finalization exists.
			if row.AttemptStatus != "" && !finalized[row.RequestID] {
				job.SkippedRows++
				job.LastID = row.ID
				continue
			}
			if row.AttemptStatus != "" {
				row.Winner = winners[row.ID]
			}
			values := calculate(row)
			if values == nil {
				job.SkippedRows++
			} else {
				// Allowlist protects immutable billed_cost and raw usage even if a caller
				// accidentally returns extra fields.
				allowed := map[string]any{}
				for _, key := range []string{"input_cost", "output_cost", "cache_creation_cost", "cache_read_cost", "image_output_cost", "total_cost", "actual_cost", "estimated_cost", "estimated_extra_cost", "account_stats_cost"} {
					if v, ok := values[key]; ok {
						allowed[key] = v
					}
				}
				if err := tx.Model(&GatewayUsageLog{}).Where("id = ?", row.ID).Updates(allowed).Error; err != nil {
					return err
				}
				job.UpdatedRows++
			}
			job.LastID = row.ID
		}
		job.Status = "running"
		job.Error = ""
		if len(rows) < 200 || job.LastID >= job.UpperID {
			now := time.Now()
			job.Status = "completed"
			job.CompletedAt = &now
			done = true
		}
		return tx.Save(&job).Error
	})
	return done, err
}

func (r *ModelPriceOverrides) MarkRepricingRetry(ctx context.Context) {
	_ = r.db.WithContext(ctx).Model(&ModelPriceRepricingJob{}).Where("id = ? AND status IN ?", 1, []string{"pending", "running"}).Updates(map[string]any{"error": "本批次重算失败，将在 30 秒后自动重试；已完成进度已保存", "updated_at": time.Now()}).Error
}

func (r *GatewayUsageLogs) InvalidatePriceStats() {
	r.statsMu.Lock()
	r.statsEpoch++
	r.statsCache = make(map[gatewayStatsQueryKey]gatewayStatsCacheEntry)
	r.statsMu.Unlock()
	r.markStatsChanged()
}
