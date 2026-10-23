"use client"

import { useCallback, useMemo, useState } from "react"
import { Sparkles, Loader2, AlertTriangle } from "lucide-react"
import { toast } from "sonner"

import { apiFetch } from "@/lib/api"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"

/** 单条待确认的配置调整。 */
export type OptimizerChange = {
  id: string
  kind: "first_token_timeout" | "route_fallback_only" | "route_disable"
  gateway_group_id: number
  gateway_group_name: string
  route_id?: number
  route_label?: string
  model?: string
  field: string
  before: string
  after: string
  reason: string
  confidence: "high" | "medium" | string
  evidence: {
    attempts: number
    successes: number
    failures: number
    first_token_timeouts: number
    timeout_rate: number
    success_rate: number
    first_token_p50_ms: number
    first_token_p95_ms: number
    first_token_p99_ms: number
  }
}

/** 一次分析的完整产出。 */
export type OptimizerPlan = {
  generated_at: string
  window_hours: number
  sample_count: number
  changes: OptimizerChange[]
  notes: string[]
}

const KIND_LABEL: Record<string, string> = {
  first_token_timeout: "首字超时",
  route_fallback_only: "路由降权",
  route_disable: "路由停用",
}

const WINDOW_OPTIONS = [6, 24, 72, 168] as const

function seconds(ms: number): string {
  if (!ms) return "—"
  return `${(ms / 1000).toFixed(1)}s`
}

function percent(value: number): string {
  return `${(value * 100).toFixed(1)}%`
}

/**
 * 一键优化：分析真实流量 → 出方案 → 人工勾选确认 → 才写配置。
 * 面板本身不落任何配置，纯展示与确认。
 */
export function OptimizerDialog({
  open,
  onOpenChange,
  onApplied,
}: {
  open: boolean
  onOpenChange: (next: boolean) => void
  onApplied?: () => void
}) {
  const [windowHours, setWindowHours] = useState<number>(24)
  const [plan, setPlan] = useState<OptimizerPlan | null>(null)
  const [selected, setSelected] = useState<Record<string, boolean>>({})
  const [loading, setLoading] = useState(false)
  const [applying, setApplying] = useState(false)
  const [explanation, setExplanation] = useState("")
  const [explainSource, setExplainSource] = useState("")

  const analyze = useCallback(
    async (hours: number) => {
      setLoading(true)
      setExplanation("")
      setExplainSource("")
      try {
        const res = await apiFetch<OptimizerPlan>(
          `/gateway/optimizer/plan?window_hours=${hours}`,
        )
        setPlan(res)
        // 默认全选：运营只需要取消不想改的项。
        const next: Record<string, boolean> = {}
        for (const change of res.changes) next[change.id] = true
        setSelected(next)
      } catch (e) {
        toast.error(e instanceof Error ? e.message : "分析失败")
      } finally {
        setLoading(false)
      }
    },
    [],
  )

  const chosen = useMemo(
    () => (plan ? plan.changes.filter((c) => selected[c.id]) : []),
    [plan, selected],
  )

  const apply = async () => {
    if (chosen.length === 0) {
      toast.error("请至少选择一项调整")
      return
    }
    setApplying(true)
    try {
      const res = await apiFetch<{ applied: OptimizerChange[] }>(
        "/gateway/optimizer/apply",
        {
          method: "POST",
          body: JSON.stringify({
            change_ids: chosen.map((c) => c.id),
            window_hours: windowHours,
          }),
        },
      )
      toast.success(`已应用 ${res.applied.length} 项调整`)
      onApplied?.()
      await analyze(windowHours)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "应用失败")
    } finally {
      setApplying(false)
    }
  }

  const explain = async () => {
    try {
      const res = await apiFetch<{ explanation: string; source: string }>(
        "/gateway/optimizer/explain",
        {
          method: "POST",
          body: JSON.stringify({
            window_hours: windowHours,
            change_ids: chosen.map((c) => c.id),
          }),
        },
      )
      setExplanation(res.explanation)
      setExplainSource(res.source)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "生成说明失败")
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Sparkles className="size-4" /> 一键优化
          </DialogTitle>
          <DialogDescription>
            用真实流量分析首字超时与路由健康度，生成方案后由你确认才落配置；只调超时和路由优先级，不增删渠道与密钥。
          </DialogDescription>
        </DialogHeader>

        <div className="flex flex-wrap items-center gap-2">
          <span className="text-xs text-muted-foreground">分析窗口</span>
          {WINDOW_OPTIONS.map((hours) => (
            <Button
              key={hours}
              size="sm"
              variant={windowHours === hours ? "default" : "outline"}
              onClick={() => {
                setWindowHours(hours)
                void analyze(hours)
              }}
            >
              {hours >= 24 ? `${hours / 24} 天` : `${hours} 小时`}
            </Button>
          ))}
          <Button size="sm" variant="outline" onClick={() => void analyze(windowHours)} disabled={loading}>
            {loading ? <Loader2 className="size-3.5 animate-spin" /> : <Sparkles className="size-3.5" />}
            重新分析
          </Button>
        </div>

        <div className="max-h-[46vh] space-y-2 overflow-y-auto pr-1">
          {!plan && !loading ? (
            <p className="rounded-md border border-dashed border-border px-3 py-6 text-center text-sm text-muted-foreground">
              选一个时间窗口开始分析。
            </p>
          ) : null}

          {plan?.notes.length ? (
            <ul className="space-y-1 rounded-md border border-border bg-muted/40 px-3 py-2 text-xs leading-5 text-muted-foreground">
              {plan.notes.map((note) => (
                <li key={note}>{note}</li>
              ))}
            </ul>
          ) : null}

          {plan?.changes.length === 0 ? (
            <p className="rounded-md border border-emerald-200 bg-emerald-50 px-3 py-4 text-sm text-emerald-800 dark:border-emerald-900/50 dark:bg-emerald-950/30 dark:text-emerald-300">
              当前配置与实测流量吻合，没有需要调整的项。
            </p>
          ) : null}

          {plan?.changes.map((change) => (
            <label
              key={change.id}
              className="flex cursor-pointer gap-3 rounded-md border border-border px-3 py-2.5 hover:bg-muted/40"
            >
              <Checkbox
                checked={!!selected[change.id]}
                onCheckedChange={(value) =>
                  setSelected((prev) => ({ ...prev, [change.id]: value === true }))
                }
                className="mt-0.5"
              />
              <div className="min-w-0 flex-1 space-y-1">
                <div className="flex flex-wrap items-center gap-1.5">
                  <Badge variant="outline" className="text-[10px]">
                    {KIND_LABEL[change.kind] ?? change.kind}
                  </Badge>
                  {change.confidence === "high" ? (
                    <Badge variant="outline" className="text-[10px] text-amber-700 dark:text-amber-400">
                      高置信
                    </Badge>
                  ) : null}
                  <span className="text-sm font-medium">
                    {change.route_id
                      ? `${change.gateway_group_name} / ${change.route_label ?? `路由 #${change.route_id}`}`
                      : `网关组「${change.gateway_group_name}」`}
                  </span>
                  <span className="text-xs text-muted-foreground">
                    {change.before} → <span className="font-medium text-foreground">{change.after}</span>
                  </span>
                </div>
                <p className="text-xs leading-5 text-muted-foreground">{change.reason}</p>
                <p className="text-[11px] leading-4 text-muted-foreground/80">
                  尝试 {change.evidence.attempts} · 成功 {change.evidence.successes} · 成功率{" "}
                  {percent(change.evidence.success_rate)} · 首字超时 {change.evidence.first_token_timeouts}（
                  {percent(change.evidence.timeout_rate)}）· 首字 p50 {seconds(change.evidence.first_token_p50_ms)} /
                  p95 {seconds(change.evidence.first_token_p95_ms)} / p99 {seconds(change.evidence.first_token_p99_ms)}
                </p>
              </div>
            </label>
          ))}

          {explanation ? (
            <div className="rounded-md border border-border bg-muted/30 px-3 py-2 text-xs leading-6 whitespace-pre-wrap">
              {explanation}
              {explainSource === "rule" ? (
                <span className="ml-1 text-[10px] text-muted-foreground">（规则生成）</span>
              ) : null}
            </div>
          ) : null}
        </div>

        {plan && plan.changes.length > 0 ? (
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <AlertTriangle className="size-3.5" />
            已选 {chosen.length} / {plan.changes.length} 项，确认后才会写入配置。
          </div>
        ) : null}

        <DialogFooter className="gap-2">
          <Button variant="outline" size="sm" onClick={() => void explain()} disabled={!plan}>
            生成说明
          </Button>
          <Button
            size="sm"
            onClick={() => void apply()}
            disabled={applying || chosen.length === 0}
          >
            {applying ? <Loader2 className="size-3.5 animate-spin" /> : null}
            确认应用
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
