package storage

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

// GatewayDailyReport summarizes usage-log reference costs, not upstream site
// invoices. All attempts (including failed/rejected/hedged attempts) contribute
// their recorded actual_cost. It never changes gateway-key settlement/balances.
type GatewayDailyReport struct {
	Date        string                `json:"date"`
	Timezone    string                `json:"timezone"`
	GeneratedAt time.Time             `json:"generated_at"`
	Today       GatewayDailyReportDay `json:"today"`
	Yesterday   GatewayDailyReportDay `json:"yesterday"`
}

type GatewayDailyReportMetrics struct {
	Requests           int64    `json:"requests"`
	Attempts           int64    `json:"attempts"`
	SuccessfulRequests int64    `json:"successful_requests"`
	Cost               float64  `json:"cost"`
	Revenue            *float64 `json:"revenue"`
	Profit             *float64 `json:"profit"`
	ProfitMargin       *float64 `json:"profit_margin"`
	UnpricedRequests   int64    `json:"unpriced_requests"`
	ZeroUsageRequests  int64    `json:"zero_usage_requests"`
	Tokens             int64    `json:"tokens"`
	CacheHitRate       *float64 `json:"cache_hit_rate"`
}

type GatewayDailyReportDay struct {
	GatewayDailyReportMetrics
	Date    string                     `json:"date"`
	Sources []GatewayDailyReportSource `json:"sources"`
}

type GatewayDailyReportSource struct {
	GatewayDailyReportMetrics
	SourceKind      string                    `json:"source_kind"`
	SourceID        uint                      `json:"source_id"`
	ChannelName     string                    `json:"channel_name"`
	TopCostModel    *GatewayDailyReportModel  `json:"top_cost_model,omitempty"`
	TopRequestModel *GatewayDailyReportModel  `json:"top_request_model,omitempty"`
	Groups          []GatewayDailyReportGroup `json:"groups"`
}

type GatewayDailyReportGroup struct {
	GatewayDailyReportMetrics
	SourceGroupID   *int64                    `json:"source_group_id,omitempty"`
	SourceGroupName string                    `json:"source_group_name"`
	TopCostModel    *GatewayDailyReportModel  `json:"top_cost_model,omitempty"`
	TopRequestModel *GatewayDailyReportModel  `json:"top_request_model,omitempty"`
	Models          []GatewayDailyReportModel `json:"models"`
}

type GatewayDailyReportModel struct {
	GatewayDailyReportMetrics
	Model string `json:"model"`
}

type gatewayDailyReportRow struct {
	Level, DayIndex                                int
	SourceKind                                     string
	SourceID                                       uint
	SourceGroupID                                  *int64
	LegacyGroupName, RequestedModel                string
	LatestID                                       uint
	Requests, Attempts, SuccessfulRequests, Tokens int64
	Cost                                           float64
	Revenue                                        float64
	UnpricedRequests                               int64
	ZeroUsageRequests                              int64
	CacheInput, CacheRead, CacheCreation           int64
}

func (row gatewayDailyReportRow) metrics() GatewayDailyReportMetrics {
	var hitRate *float64
	if row.CacheInput+row.CacheRead+row.CacheCreation > 0 {
		value := cacheHitRatePercent(row.CacheInput, row.CacheRead, row.CacheCreation)
		hitRate = &value
	}
	metrics := GatewayDailyReportMetrics{
		Requests: row.Requests, Attempts: row.Attempts, SuccessfulRequests: row.SuccessfulRequests,
		Cost: row.Cost, Tokens: row.Tokens, CacheHitRate: hitRate,
		UnpricedRequests: row.UnpricedRequests, ZeroUsageRequests: row.ZeroUsageRequests,
	}
	// Unknown selling prices are not free sales. Withhold the total rather
	// than presenting a partial sum as the channel's complete gross profit.
	if row.UnpricedRequests == 0 {
		revenue, profit := row.Revenue, row.Revenue-row.Cost
		if math.IsNaN(revenue) || math.IsInf(revenue, 0) || math.IsNaN(profit) || math.IsInf(profit, 0) {
			metrics.UnpricedRequests = row.SuccessfulRequests
			return metrics
		}
		metrics.Revenue, metrics.Profit = &revenue, &profit
		if revenue > 0 {
			margin := profit / revenue * 100
			if !math.IsNaN(margin) && !math.IsInf(margin, 0) {
				metrics.ProfitMargin = &margin
			}
		}
	}
	return metrics
}

// DailyReport uses a bounded two-day CTE and five grouped rollups. Neither raw
// usage rows nor request-ID sets leave the database; the number of enrichment
// queries is fixed, regardless of channel/group/model cardinality. The caller
// supplies a date in the desired reporting timezone (normally Asia/Shanghai).
func (r *GatewayUsageLogs) DailyReport(ctx context.Context, date time.Time) (*GatewayDailyReport, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("usage storage is unavailable")
	}
	if date.IsZero() {
		date = time.Now().In(time.FixedZone("Asia/Shanghai", 8*60*60))
	}
	loc := date.Location()
	today := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, loc)
	yesterday, tomorrow := today.AddDate(0, 0, -1), today.AddDate(0, 0, 1)
	// Serialize with the existing dashboard aggregate refreshes. These are
	// operator reads, not queries that should contend with request hot paths.
	r.statsQueryMu.Lock()
	defer r.statsQueryMu.Unlock()

	var result *GatewayDailyReport
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var rows []gatewayDailyReportRow
		if err := tx.Raw(gatewayDailyReportSQL, today.UTC(), yesterday.UTC(), tomorrow.UTC()).Scan(&rows).Error; err != nil {
			return err
		}
		channelIDs, providerIDs, latestIDs := map[uint]struct{}{}, map[uint]struct{}{}, map[uint]struct{}{}
		for _, row := range rows {
			if row.Level == 1 {
				if row.SourceKind == GatewayRouteSourceProvider {
					providerIDs[row.SourceID] = struct{}{}
				} else {
					channelIDs[row.SourceID] = struct{}{}
				}
			}
			if row.Level == 1 || row.Level == 3 {
				latestIDs[row.LatestID] = struct{}{}
			}
		}
		channelNames, providerNames := map[uint]string{}, map[uint]string{}
		if len(channelIDs) > 0 {
			var items []Channel
			if err := tx.Select("id", "name").Where("id IN ?", uintKeys(channelIDs)).Find(&items).Error; err != nil {
				return err
			}
			for _, item := range items {
				channelNames[item.ID] = item.Name
			}
		}
		if len(providerIDs) > 0 {
			var items []GatewayProvider
			if err := tx.Select("id", "name").Where("id IN ?", uintKeys(providerIDs)).Find(&items).Error; err != nil {
				return err
			}
			for _, item := range items {
				providerNames[item.ID] = item.Name
			}
		}
		snapshots := map[uint]GatewayUsageLog{}
		if len(latestIDs) > 0 {
			var items []GatewayUsageLog
			if err := tx.Select("id", "provider_name", "source_group_name").Where("id IN ?", uintKeys(latestIDs)).Find(&items).Error; err != nil {
				return err
			}
			for _, item := range items {
				snapshots[item.ID] = item
			}
		}
		result = buildGatewayDailyReport(today, rows, channelNames, providerNames, snapshots)
		return nil
	})
	return result, err
}

// Only token-priced attempts can use the zero-usage convention. Image/request/
// duration billing may have valid non-token charges. Check each usage bucket so
// cache-only, reasoning-only and image usage cannot be mistaken for zero usage.
const gatewayDailyReportZeroUsageSQL = `COALESCE(u.billing_mode,'') IN ('','token')
	AND COALESCE(u.input_tokens,0) = 0 AND COALESCE(u.output_tokens,0) = 0
	AND COALESCE(u.cache_creation_tokens,0) = 0 AND COALESCE(u.cache_read_tokens,0) = 0
	AND COALESCE(u.cache_creation5m_tokens,0) = 0 AND COALESCE(u.cache_creation1h_tokens,0) = 0
	AND COALESCE(u.image_output_tokens,0) = 0 AND COALESCE(u.reasoning_tokens,0) = 0`

// A shared CTE keeps the rollups consistent while usage is being appended.
// Explicit UTC boundaries and source-group IDs preserve existing identities.
const gatewayDailyReportSQL = `WITH report_base AS (
	SELECT CASE WHEN u.created_at >= ? THEN 1 ELSE 0 END AS day_index,
		CASE WHEN u.gateway_provider_id > 0 THEN 'provider' ELSE 'monitor' END AS source_kind,
		CASE WHEN u.gateway_provider_id > 0 THEN u.gateway_provider_id ELSE u.channel_id END AS source_id,
		CASE WHEN u.source_group_id > 0 THEN u.source_group_id ELSE NULL END AS source_group_id,
		CASE WHEN u.source_group_id > 0 THEN '' ELSE COALESCE(u.source_group_name,'') END AS legacy_group_name,
		COALESCE(u.requested_model,'') AS requested_model, u.id, u.request_id, u.winner, u.success,
		u.actual_cost, u.input_tokens, u.output_tokens, u.cache_creation_tokens, u.cache_read_tokens,
		g.sell_rate_multiplier,
		(` + gatewayDailyReportZeroUsageSQL + `) AS zero_usage,
		(COALESCE(u.total_cost,0) = 0 AND COALESCE(u.actual_cost,0) = 0 AND COALESCE(u.billed_cost,0) = 0
			AND COALESCE(f.actual_cost,0) = 0 AND COALESCE(f.billed_cost,0) = 0) AS zero_recorded_cost,
		CASE
			-- 只有真正未知的计费模式才放弃定价。per_request（按次收费）同样会
			-- 写入 billed_cost/actual_cost，必须参与收入，否则整张日报的利润
			-- 会因为一条按次记录而变成「无法计算」。
			WHEN COALESCE(u.billing_mode,'') NOT IN ('','token','image','per_request') THEN NULL
			WHEN u.reference_pricing_version > 0 THEN u.reference_sales_base
			WHEN f.request_id IS NOT NULL THEN
				CASE WHEN f.billed_cost > 0 OR (f.billed_cost = 0 AND f.actual_cost > 0 AND f.virtual_cache_read_tokens > 0)
					THEN f.billed_cost / (` + gatewayDailyReportOriginalRateSQL + `) ELSE NULL END
			WHEN u.billed_cost > 0 THEN u.billed_cost / (` + gatewayDailyReportOriginalRateSQL + `)
			WHEN COALESCE(u.attempt_status,'') = '' AND u.virtual_cache_read_tokens = 0 AND COALESCE(u.virtual_cache_reason,'') = '' THEN
				CASE WHEN u.total_cost > 0 THEN u.total_cost
					WHEN u.actual_cost > 0 THEN u.actual_cost / (` + gatewayDailyReportOriginalRateSQL + `) ELSE NULL END
			ELSE NULL
		END AS sales_base
	FROM gateway_usage_logs u
	LEFT JOIN gateway_groups g ON g.id = u.gateway_group_id
	LEFT JOIN gateway_request_finalizations f ON u.winner AND f.request_id = u.request_id
		AND f.gateway_key_id = u.gateway_key_id AND f.winner_usage_log_id = u.id AND f.delivered
	WHERE u.created_at >= ? AND u.created_at < ?
), report_usage AS (
	SELECT report_base.*,
		CASE WHEN NOT winner THEN 0
			WHEN zero_usage AND zero_recorded_cost THEN 0
			WHEN sell_rate_multiplier = 0 THEN 0
			WHEN sell_rate_multiplier > 0 AND sales_base >= 0 THEN sales_base * sell_rate_multiplier
			ELSE NULL END AS sales_revenue
	FROM report_base
), report_rollups AS (
	SELECT 0 AS level, day_index, '' AS source_kind, 0 AS source_id,
		CAST(NULL AS BIGINT) AS source_group_id, '' AS legacy_group_name, '' AS requested_model,
		` + gatewayDailyReportAggregateSQL + ` FROM report_usage GROUP BY day_index
	UNION ALL
	SELECT 1 AS level, day_index, source_kind, source_id, CAST(NULL AS BIGINT) AS source_group_id,
		'' AS legacy_group_name, '' AS requested_model,
		` + gatewayDailyReportAggregateSQL + ` FROM report_usage GROUP BY day_index, source_kind, source_id
	UNION ALL
	SELECT 2 AS level, day_index, source_kind, source_id, CAST(NULL AS BIGINT) AS source_group_id,
		'' AS legacy_group_name, requested_model,
		` + gatewayDailyReportAggregateSQL + ` FROM report_usage GROUP BY day_index, source_kind, source_id, requested_model
	UNION ALL
	SELECT 3 AS level, day_index, source_kind, source_id, source_group_id, legacy_group_name, '' AS requested_model,
		` + gatewayDailyReportAggregateSQL + ` FROM report_usage GROUP BY day_index, source_kind, source_id, source_group_id, legacy_group_name
	UNION ALL
	SELECT 4 AS level, day_index, source_kind, source_id, source_group_id, legacy_group_name, requested_model,
		` + gatewayDailyReportAggregateSQL + ` FROM report_usage GROUP BY day_index, source_kind, source_id, source_group_id, legacy_group_name, requested_model
)
SELECT * FROM report_rollups ORDER BY level, day_index, source_kind, source_id, source_group_id, legacy_group_name, requested_model`

// CalculateCost applies exactly one upstream rate. Log rates are immutable
// even after reference repricing; never divide settlement by actual/total_cost.
// Finalization has a request_id primary key, so joining it cannot multiply rows.
// Repricing writes a separate sales basis using current model prices and the
// original virtual-cache token counts. It takes precedence over old settlements,
// including zero snapshots. A processed but unknown price remains NULL rather
// than falling back to stale charges. Unprocessed rows retain the legacy basis
// while the bounded background backfill catches up.
const gatewayDailyReportOriginalRateSQL = `CASE
	WHEN u.billing_rate_multiplier > 0 THEN u.billing_rate_multiplier
	WHEN u.rate_multiplier > 0 THEN u.rate_multiplier ELSE 1.0 END`

const gatewayDailyReportAggregateSQL = `
	MAX(id) AS latest_id,
	COUNT(DISTINCT request_id) AS requests,
	COUNT(*) AS attempts,
	COUNT(DISTINCT CASE WHEN winner THEN request_id END) AS successful_requests,
	COALESCE(SUM(actual_cost),0) AS cost,
	COALESCE(SUM(sales_revenue),0) AS revenue,
	COUNT(DISTINCT CASE WHEN winner AND sales_revenue IS NULL THEN request_id END) AS unpriced_requests,
	COUNT(DISTINCT CASE WHEN winner AND zero_usage THEN request_id END) AS zero_usage_requests,
	COALESCE(SUM(input_tokens + output_tokens + cache_creation_tokens + cache_read_tokens),0) AS tokens,
	COALESCE(SUM(CASE WHEN success THEN input_tokens ELSE 0 END),0) AS cache_input,
	COALESCE(SUM(CASE WHEN success THEN cache_read_tokens ELSE 0 END),0) AS cache_read,
	COALESCE(SUM(CASE WHEN success THEN cache_creation_tokens ELSE 0 END),0) AS cache_creation`

type gatewayReportSourceKey struct {
	day  int
	kind string
	id   uint
}
type gatewayReportGroupKey struct {
	source gatewayReportSourceKey
	id     int64
	name   string
}

func buildGatewayDailyReport(today time.Time, rows []gatewayDailyReportRow, channelNames, providerNames map[uint]string, snapshots map[uint]GatewayUsageLog) *GatewayDailyReport {
	result := &GatewayDailyReport{
		Date: today.Format("2006-01-02"), Timezone: today.Location().String(), GeneratedAt: time.Now().UTC(),
		Today:     GatewayDailyReportDay{GatewayDailyReportMetrics: (gatewayDailyReportRow{}).metrics(), Date: today.Format("2006-01-02"), Sources: []GatewayDailyReportSource{}},
		Yesterday: GatewayDailyReportDay{GatewayDailyReportMetrics: (gatewayDailyReportRow{}).metrics(), Date: today.AddDate(0, 0, -1).Format("2006-01-02"), Sources: []GatewayDailyReportSource{}},
	}
	sources := map[gatewayReportSourceKey]*GatewayDailyReportSource{}
	groups := map[gatewayReportGroupKey]*GatewayDailyReportGroup{}
	for _, row := range rows {
		day := &result.Yesterday
		if row.DayIndex == 1 {
			day = &result.Today
		}
		sk := gatewayReportSourceKey{row.DayIndex, row.SourceKind, row.SourceID}
		gk := gatewayReportGroupKey{source: sk, name: row.LegacyGroupName}
		if row.SourceGroupID != nil {
			gk.id = *row.SourceGroupID
		}
		switch row.Level {
		case 0:
			day.GatewayDailyReportMetrics = row.metrics()
		case 1:
			name := strings.TrimSpace(channelNames[row.SourceID])
			if row.SourceKind == GatewayRouteSourceProvider {
				name = strings.TrimSpace(providerNames[row.SourceID])
				if name == "" {
					name = strings.TrimSpace(snapshots[row.LatestID].ProviderName)
				}
			}
			if name == "" {
				if row.SourceKind == GatewayRouteSourceProvider {
					name = fmt.Sprintf("直连 #%d（已删除或未命名）", row.SourceID)
				} else {
					name = fmt.Sprintf("渠道 #%d（已删除或未命名）", row.SourceID)
				}
			}
			sources[sk] = &GatewayDailyReportSource{GatewayDailyReportMetrics: row.metrics(), SourceKind: row.SourceKind, SourceID: row.SourceID, ChannelName: name, Groups: []GatewayDailyReportGroup{}}
		case 2:
			if source := sources[sk]; source != nil {
				model := reportModel(row)
				reportModelLeaders(&source.TopCostModel, &source.TopRequestModel, model)
			}
		case 3:
			name := strings.TrimSpace(snapshots[row.LatestID].SourceGroupName)
			if name == "" {
				name = row.LegacyGroupName
			}
			if name == "" {
				if row.SourceGroupID != nil {
					name = fmt.Sprintf("源分组 #%d", *row.SourceGroupID)
				} else {
					name = "未记录源分组 / 默认"
				}
			}
			groups[gk] = &GatewayDailyReportGroup{GatewayDailyReportMetrics: row.metrics(), SourceGroupID: row.SourceGroupID, SourceGroupName: name, Models: []GatewayDailyReportModel{}}
		case 4:
			if group := groups[gk]; group != nil {
				model := reportModel(row)
				group.Models = append(group.Models, model)
				reportModelLeaders(&group.TopCostModel, &group.TopRequestModel, model)
			}
		}
	}
	for key, group := range groups {
		sort.Slice(group.Models, func(i, j int) bool {
			return reportCostLess(group.Models[i].Cost, group.Models[j].Cost, group.Models[i].Model, group.Models[j].Model)
		})
		if source := sources[key.source]; source != nil {
			source.Groups = append(source.Groups, *group)
		}
	}
	for key, source := range sources {
		sort.Slice(source.Groups, func(i, j int) bool {
			a, b := source.Groups[i], source.Groups[j]
			if a.Cost != b.Cost {
				return a.Cost > b.Cost
			}
			if a.SourceGroupName != b.SourceGroupName {
				return a.SourceGroupName < b.SourceGroupName
			}
			var ai, bi int64
			if a.SourceGroupID != nil {
				ai = *a.SourceGroupID
			}
			if b.SourceGroupID != nil {
				bi = *b.SourceGroupID
			}
			return ai < bi
		})
		if key.day == 1 {
			result.Today.Sources = append(result.Today.Sources, *source)
		} else {
			result.Yesterday.Sources = append(result.Yesterday.Sources, *source)
		}
	}
	for _, day := range []*GatewayDailyReportDay{&result.Today, &result.Yesterday} {
		sort.Slice(day.Sources, func(i, j int) bool {
			a, b := day.Sources[i], day.Sources[j]
			if a.Cost != b.Cost {
				return a.Cost > b.Cost
			}
			if a.SourceKind != b.SourceKind {
				return a.SourceKind < b.SourceKind
			}
			return a.SourceID < b.SourceID
		})
	}
	return result
}

func reportModel(row gatewayDailyReportRow) GatewayDailyReportModel {
	name := strings.TrimSpace(row.RequestedModel)
	if name == "" {
		name = "未知模型"
	}
	return GatewayDailyReportModel{GatewayDailyReportMetrics: row.metrics(), Model: name}
}

func reportModelLeaders(cost, requests **GatewayDailyReportModel, model GatewayDailyReportModel) {
	if *cost == nil || reportCostLess(model.Cost, (*cost).Cost, model.Model, (*cost).Model) {
		copy := model
		*cost = &copy
	}
	if *requests == nil || model.Requests > (*requests).Requests || (model.Requests == (*requests).Requests && model.Model < (*requests).Model) {
		copy := model
		*requests = &copy
	}
}

func reportCostLess(a, b float64, aName, bName string) bool {
	if a != b {
		return a > b
	}
	return aName < bName
}
