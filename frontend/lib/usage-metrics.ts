/** New-format winners are marked atomically with settlement, including $0.
 * Only legacy rows without attempt_status may lack a billed_cost snapshot. */
export function usageDisplayedCost(usage: {
  winner?: boolean
  attempt_status?: string
  billed_cost?: number | null
  actual_cost?: number | null
  virtual_cache_read_tokens?: number | null
}): number {
  const billed = usage.billed_cost ?? 0
  const settled = usage.winner && (Boolean(usage.attempt_status) || billed > 0 || (usage.virtual_cache_read_tokens ?? 0) > 0)
  return settled ? billed : usage.actual_cost ?? 0
}

/** Estimated output throughput for one upstream attempt, excluding its TTFT. */
export function usageOutputTPS(usage: {
  stream: boolean
  request_type?: number
  billing_mode?: string
  image_output_tokens?: number
  inbound_endpoint?: string
  upstream_endpoint?: string
  output_tokens: number
  duration_ms: number
  first_token_ms?: number | null
}): number | null {
  if (!usage.stream && usage.request_type !== 2) return null
  if (usage.billing_mode === "image" || usage.billing_mode === "video") return null
  if ((usage.image_output_tokens ?? 0) > 0) return null
  if ([usage.inbound_endpoint, usage.upstream_endpoint].some((path) => {
    const parts = (path ?? "").split("?")[0].split("/")
    return parts.includes("images") || parts.includes("videos")
  })) return null
  const { output_tokens: tokens, duration_ms: duration, first_token_ms: first } = usage
  if (first == null || !Number.isFinite(first) || first < 0) return null
  if (!Number.isFinite(tokens) || tokens <= 0 || !Number.isFinite(duration) || duration <= first) return null
  const rate = tokens * 1000 / (duration - first)
  return Number.isFinite(rate) ? rate : null
}
