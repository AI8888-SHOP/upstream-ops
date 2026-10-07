"use client"

import { useState } from "react"
import { Unlock } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { GatewayRouteCandyCheck } from "@/lib/api-types"

const labels: Record<GatewayRouteCandyCheck["status"], string> = {
  pending: "等待检测", correct: "通过", incorrect: "答题未通过", error: "检测失败", skipped: "已跳过", manual: "已人工解除",
}

export function RouteCandyCheckStatus({ enabled, state, onClear }: { enabled: boolean; state?: GatewayRouteCandyCheck; onClear: () => Promise<void> }) {
  const [busy, setBusy] = useState(false)
  if (!enabled && !state) return null
  const cooling = !!(state?.active && state.cooldown_until && new Date(state.cooldown_until).getTime() > Date.now())
  const running = !!(enabled && state?.lease_until && new Date(state.lease_until).getTime() > Date.now())
  const status = !enabled ? "已停用" : running ? "检测中" : !state?.active ? "等待检测" : labels[state.status]
  const format = (value: string) => new Date(value).toLocaleString("zh-CN")
  return <div className="mt-2 space-y-1 text-[11px]">
    <div className="flex flex-wrap items-center gap-1">
      <Badge className="px-1.5 text-[10px]" variant={cooling ? "destructive" : "secondary"}>糖果题 · {status}{cooling ? " · 本组全部模型冷却" : ""}</Badge>
      {(cooling || running) && <Button type="button" size="sm" variant="outline" className="h-6 gap-1 px-1.5 text-xs" disabled={busy} onClick={async () => {
        setBusy(true)
        try { await onClear() } finally { setBusy(false) }
      }}><Unlock className="size-3" />解除检测冷却</Button>}
    </div>
    {state && <details className="text-muted-foreground">
      <summary className="cursor-pointer">检测详情{state.checked_at ? ` · ${format(state.checked_at)}` : ""}</summary>
      <div className="mt-1 space-y-0.5 break-all">
        <div>检测上游模型：{state.model || "—"} · 耗时 {(state.latency_ms / 1000).toFixed(2)}s</div>
        {state.reason && <div>{state.reason}</div>}
        {state.answer_preview && <div className="whitespace-pre-wrap">最终答复：{state.answer_preview}</div>}
        {cooling && state.cooldown_until && <div>冷却至：{format(state.cooldown_until)}</div>}
        {enabled && state.next_check_at && <div>下次检测：{format(state.next_check_at)}</div>}
      </div>
    </details>}
  </div>
}
