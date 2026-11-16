import type { GatewayRoute } from "./api-types"

export interface ComparisonConfig {
  endpoint: string
  model: string
  has_api_key: boolean
}

export interface ComparisonItem {
  candidate: {
    id: number
    channel_id: number
    channel_name: string
    group_id: number | null
    group_name: string
    description: string
  }
  quote: { price: number | null; basis: string; evidence: string; reason: string }
  cost: number | null
  priority: number
  calculation: string
  warning: string
}

export interface ComparisonStrategy {
  id: string
  search: string
  specification: string
  currency: string
  unit: string
  requirements: string
  analysis: string
  saved_at: string | null
  expires_at: string
  items: ComparisonItem[]
}

// Preserve existing credentials, model policy, disabled state and all unrelated
// routes. Reapplying the same strategy is idempotent. Name matching supports
// older routes whose numeric source group ID was not saved yet.
export function mergeComparisonRoutes(
  drafts: Partial<GatewayRoute>[],
  strategy: ComparisonStrategy,
  createRoute: () => Partial<GatewayRoute>,
): Partial<GatewayRoute>[] {
  const next = drafts.map((route) => ({ ...route, priority: 0 }))
  for (const item of strategy.items) {
    const source = item.candidate
    const matches = next.filter((route) => {
      if (route.source_kind === "provider" || route.gateway_provider_id || route.source_channel_id !== source.channel_id) return false
      if ((route.source_group_id ?? 0) > 0 && (source.group_id ?? 0) > 0) return route.source_group_id === source.group_id
      const name = (route.source_group_name ?? "").trim().toLowerCase()
      const idMatch = /^(?:id|sourceid|源id):([0-9]+)$/.exec(name.replace(/\s+/g, ""))
      return name === source.group_name.trim().toLowerCase() || (source.group_id != null && idMatch != null && Number(idMatch[1]) === source.group_id)
    })
    if (matches.length) {
      for (const route of matches) route.priority = item.priority
    } else {
      next.push({
        ...createRoute(), source_kind: "monitor", source_channel_id: source.channel_id,
        gateway_provider_id: 0, source_group_id: source.group_id,
        source_group_name: source.group_name, priority: item.priority,
      })
    }
  }
  return next
}

export function comparisonExpiry(value: string) {
  return new Date(value).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false })
}
