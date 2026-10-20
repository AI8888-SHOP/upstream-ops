import type { GatewayRouteSourceKind } from "./api-types"

export interface DailyReportMetrics {
  requests: number
  attempts: number
  successful_requests: number
  cost: number
  revenue: number | null
  profit: number | null
  profit_margin: number | null
  unpriced_requests: number
  zero_usage_requests: number
  tokens: number
  cache_hit_rate: number | null
}

export interface DailyReportModel extends DailyReportMetrics {
  model: string
}

export interface DailyReportGroup extends DailyReportMetrics {
  source_group_id?: number | null
  source_group_name: string
  top_cost_model?: DailyReportModel | null
  top_request_model?: DailyReportModel | null
  models: DailyReportModel[]
}

export interface DailyReportSource extends DailyReportMetrics {
  source_kind: GatewayRouteSourceKind
  source_id: number
  channel_name: string
  top_cost_model?: DailyReportModel | null
  top_request_model?: DailyReportModel | null
  groups: DailyReportGroup[]
}

export interface DailyReportDay extends DailyReportMetrics {
  date: string
  sources: DailyReportSource[]
}

export interface GatewayDailyReport {
  date: string
  timezone: string
  generated_at: string
  today: DailyReportDay
  yesterday: DailyReportDay
}
