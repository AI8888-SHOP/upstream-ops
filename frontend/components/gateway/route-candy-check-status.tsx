"use client"

import { useState } from "react"
import { Brain, Loader2, Unlock } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { apiFetch } from "@/lib/api"
import type { GatewayRouteCandyCheck, GatewayCandyCheckEvent } from "@/lib/api-types"

const labels: Record<GatewayRouteCandyCheck["status"], string> = {
  pending: "等待检测", correct: "通过", incorrect: "答题未通过", error: "检测失败", skipped: "已跳过", manual: "已人工解除",
}

export function RouteCandyCheckStatus({ routeID, enabled, state, disabledReason, onRun, onClear }: {
  routeID?: number
  enabled: boolean
  state?: GatewayRouteCandyCheck
  disabledReason?: string
  onRun: () => Promise<void>
  onClear: () => Promise<void>
}) {
  const [busy, setBusy] = useState(false)
  const [testing, setTesting] = useState(false)
  const [history, setHistory] = useState<GatewayCandyCheckEvent[] | null>(null)
  const [historyBusy, setHistoryBusy] = useState(false)
  const [historyError, setHistoryError] = useState("")
  const cooling = !!(state?.active && state.cooldown_until && new Date(state.cooldown_until).getTime() > Date.now())
  const running = testing || !!(state?.lease_until && new Date(state.lease_until).getTime() > Date.now())
  const backingOff = !!(state?.active && state.backoff_until && new Date(state.backoff_until).getTime() > Date.now())
  const status = running ? "检测中" : state?.active ? labels[state.status] : enabled ? "等待检测" : "定时检测关闭"
  const format = (value: string) => new Date(value).toLocaleString("zh-CN")
  return <div className="mt-2 space-y-1 text-[11px]">
    <div className="flex flex-wrap items-center gap-1">
      <Button type="button" size="sm" variant="outline" className="h-6 gap-1 px-1.5 text-xs"
        disabled={!!disabledReason || running || busy}
        title={disabledReason || (enabled ? "立即检测；通过解除冷却，答错按配置冷却，网络失败短暂退避后复测" : "使用已保存的配置立即检测；定时检测关闭时仅记录结果，不自动冷却")}
        onClick={async () => {
          setTesting(true)
          try { await onRun() } finally { setTesting(false) }
        }}>
        {running ? <Loader2 className="size-3 animate-spin" /> : <Brain className="size-3" />}
        {running ? "智商测试中…" : "智商测试"}
      </Button>
      {(enabled || state) && <Badge className="px-1.5 text-[10px]" variant={cooling ? "destructive" : "secondary"}>糖果题 · {status}{cooling ? " · 本组全部模型冷却" : ""}</Badge>}
      {backingOff && <Badge variant="secondary">检测网络异常 · 短暂退避</Badge>}
      {(cooling || (enabled && running)) && <Button type="button" size="sm" variant="outline" className="h-6 gap-1 px-1.5 text-xs" disabled={busy} onClick={async () => {
        setBusy(true)
        try { await onClear() } finally { setBusy(false) }
      }}><Unlock className="size-3" />解除检测冷却</Button>}
    </div>
    {disabledReason && <div className="text-muted-foreground">{disabledReason}</div>}
    {state && <details className="text-muted-foreground">
      <summary className="cursor-pointer">检测详情{state.checked_at ? ` · ${format(state.checked_at)}` : ""}</summary>
      <div className="mt-1 space-y-0.5 break-all">
        <div>检测上游模型：{state.model || "—"} · 耗时 {(state.latency_ms / 1000).toFixed(2)}s</div>
        {state.reason && <div>{state.reason}</div>}
        {state.answer_preview && <div className="whitespace-pre-wrap">最终答复：{state.answer_preview}</div>}
        {cooling && state.cooldown_until && <div>冷却至：{format(state.cooldown_until)}</div>}
        {backingOff && state.backoff_until && <div>网络退避至：{format(state.backoff_until)} · 连续异常 {state.error_streak ?? 1} 次</div>}
        {enabled && state.next_check_at && <div>下次检测：{format(state.next_check_at)}</div>}
      </div>
    </details>}
    {routeID && <div>
      <Button type="button" size="sm" variant="ghost" className="h-6 px-1 text-xs" disabled={historyBusy}
        onClick={async () => {
          setHistoryBusy(true)
          setHistoryError("")
          try {
            const response = await apiFetch<{ items: GatewayCandyCheckEvent[] | null }>(`/gateway/routes/${routeID}/candy-check/history`)
            setHistory(response.items ?? [])
          } catch (e) { setHistoryError(e instanceof Error ? e.message : "读取检测历史失败") }
          finally { setHistoryBusy(false) }
        }}>{historyBusy ? "读取中…" : "检测与解除历史"}</Button>
      {historyError && <div className="text-destructive">{historyError}</div>}
      {history && <div className="max-h-48 space-y-1 overflow-y-auto rounded border p-2">
        {history.length === 0 && <div>暂无历史记录</div>}
        {history.map((event) => <div key={event.id} className="border-b pb-1 last:border-0">
          <div>{format(event.created_at)} · {event.manual ? "人工" : "定时"} · {labels[event.status as GatewayRouteCandyCheck["status"]] || event.status}</div>
          {event.reason && <div className="break-all text-muted-foreground">{event.reason}</div>}
          {event.cooldown_until && <div>当时冷却截止：{format(event.cooldown_until)}</div>}
          {event.backoff_until && <div>当时网络退避截止：{format(event.backoff_until)}</div>}
        </div>)}
      </div>}
    </div>}
  </div>
}
