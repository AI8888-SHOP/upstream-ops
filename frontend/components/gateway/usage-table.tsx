import { Fragment, useCallback, useEffect, useMemo, useState } from "react"
import {
  ArrowDown,
  ArrowUp,
  ChevronDown,
  ChevronRight,
  Columns3,
  Copy,
  Info,
  Loader2,
  Trash2,
} from "lucide-react"
import { toast } from "sonner"
import { copyText as copyToClipboard } from "@/lib/clipboard"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip"
import {
  DropdownMenu,
  DropdownMenuCheckboxItem,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { fullDateTime, formatDurationMS, formatTokens } from "@/lib/format"
import { apiFetch } from "@/lib/api"
import type { GatewayUsageLog } from "@/lib/api-types"
import { cn } from "@/lib/utils"
import { formatSourceGroupDisplay } from "./gateway-utils"

/** datetime-local 字符串（本地）→ ISO RFC3339 */
function localDatetimeToISO(raw: string): string | null {
  const s = raw.trim()
  if (!s) return null
  const m = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(s)
  let d: Date
  if (m) {
    const [, y, mo, day, h, mi, sec] = m
    d = new Date(
      Number(y),
      Number(mo) - 1,
      Number(day),
      Number(h),
      Number(mi),
      Number(sec ?? 0),
      0,
    )
  } else {
    d = new Date(s)
  }
  if (Number.isNaN(d.getTime())) return null
  return d.toISOString()
}

function toLocalDatetimeValue(d: Date): string {
  const pad = (n: number) => String(n).padStart(2, "0")
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`
}

function daysAgoLocal(days: number): string {
  const d = new Date()
  d.setDate(d.getDate() - days)
  d.setHours(0, 0, 0, 0)
  return toLocalDatetimeValue(d)
}

function UsageCleanupDialog({
  open,
  onOpenChange,
  onCleaned,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  onCleaned?: () => void
}) {
  const [mode, setMode] = useState<"before" | "all">("before")
  const [beforeLocal, setBeforeLocal] = useState(() => daysAgoLocal(30))
  const [ack, setAck] = useState(false)
  const [matched, setMatched] = useState<number | null>(null)
  const [previewing, setPreviewing] = useState(false)
  const [submitting, setSubmitting] = useState(false)

  // 打开时重置
  useEffect(() => {
    if (!open) return
    setMode("before")
    setBeforeLocal(daysAgoLocal(30))
    setAck(false)
    setMatched(null)
  }, [open])

  // 条件变化时清空预览数
  useEffect(() => {
    setMatched(null)
  }, [mode, beforeLocal])

  const canSubmit =
    ack && !submitting && (mode === "all" || !!beforeLocal.trim())

  const runPreview = async () => {
    setPreviewing(true)
    try {
      const body =
        mode === "all"
          ? { all: true, dry_run: true }
          : {
              all: false,
              dry_run: true,
              before: localDatetimeToISO(beforeLocal) || beforeLocal,
            }
      if (mode === "before" && !localDatetimeToISO(beforeLocal)) {
        toast.error("请选择有效的截止时间")
        return
      }
      const res = await apiFetch<{ matched: number }>("/gateway/usage/cleanup", {
        method: "POST",
        body: JSON.stringify(body),
      })
      setMatched(res.matched ?? 0)
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "预览失败")
    } finally {
      setPreviewing(false)
    }
  }

  const runCleanup = async () => {
    if (!canSubmit) return
    setSubmitting(true)
    try {
      const body =
        mode === "all"
          ? { all: true, confirm: true }
          : {
              all: false,
              confirm: true,
              before: localDatetimeToISO(beforeLocal) || beforeLocal,
            }
      if (mode === "before" && !localDatetimeToISO(beforeLocal)) {
        toast.error("请选择有效的截止时间")
        return
      }
      const res = await apiFetch<{ deleted: number }>("/gateway/usage/cleanup", {
        method: "POST",
        body: JSON.stringify(body),
      })
      const n = res.deleted ?? 0
      toast.success(n > 0 ? `已清理 ${n} 条访问日志` : "没有可清理的记录")
      onOpenChange(false)
      onCleaned?.()
    } catch (e) {
      toast.error(e instanceof Error ? e.message : "清理失败")
    } finally {
      setSubmitting(false)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>清理访问日志</DialogTitle>
          <DialogDescription>
            删除网关使用记录（gateway_usage_logs）。此操作不可恢复，统计与费用明细将一并丢失。
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 text-sm">
          <div className="rounded-md border border-amber-200 bg-amber-50/80 px-3 py-2 text-xs text-amber-900 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-100">
            <p className="font-medium">清理说明</p>
            <ul className="mt-1 list-disc space-y-0.5 pl-4 text-amber-800/90 dark:text-amber-100/80">
              <li>仅删除访问 / 用量日志，不影响网关组、密钥、路由配置</li>
              <li>按时间清理：删除「截止时间之前」的全部记录（不含该时刻之后）</li>
              <li>清理全部：清空所有历史调用记录，无法找回</li>
              <li>建议先点「预览将删除条数」确认范围</li>
            </ul>
          </div>

          <div className="space-y-2">
            <Label>清理范围</Label>
            <div className="flex flex-col gap-2">
              <label className="flex cursor-pointer items-start gap-2 rounded-md border p-2.5 has-[:checked]:border-primary has-[:checked]:bg-primary/5">
                <input
                  type="radio"
                  name="usage-cleanup-mode"
                  className="mt-0.5"
                  checked={mode === "before"}
                  onChange={() => setMode("before")}
                />
                <span className="min-w-0 flex-1">
                  <span className="font-medium">按时间清理</span>
                  <span className="mt-0.5 block text-xs text-muted-foreground">
                    删除选定时间之前的日志
                  </span>
                </span>
              </label>
              <label className="flex cursor-pointer items-start gap-2 rounded-md border border-red-200/80 p-2.5 has-[:checked]:border-red-500 has-[:checked]:bg-red-50/50 dark:border-red-900/40 dark:has-[:checked]:bg-red-950/20">
                <input
                  type="radio"
                  name="usage-cleanup-mode"
                  className="mt-0.5"
                  checked={mode === "all"}
                  onChange={() => setMode("all")}
                />
                <span className="min-w-0 flex-1">
                  <span className="font-medium text-red-700 dark:text-red-300">
                    清理全部数据
                  </span>
                  <span className="mt-0.5 block text-xs text-muted-foreground">
                    删除所有访问日志，高风险
                  </span>
                </span>
              </label>
            </div>
          </div>

          {mode === "before" ? (
            <div className="space-y-2">
              <Label htmlFor="usage-cleanup-before">截止时间</Label>
              <Input
                id="usage-cleanup-before"
                type="datetime-local"
                value={beforeLocal}
                onChange={(e) => setBeforeLocal(e.target.value)}
              />
              <div className="flex flex-wrap gap-1.5">
                {[
                  { label: "7 天前", days: 7 },
                  { label: "30 天前", days: 30 },
                  { label: "90 天前", days: 90 },
                ].map((p) => (
                  <Button
                    key={p.days}
                    type="button"
                    size="sm"
                    variant="outline"
                    className="h-7 text-xs"
                    onClick={() => setBeforeLocal(daysAgoLocal(p.days))}
                  >
                    {p.label}
                  </Button>
                ))}
              </div>
              <p className="text-xs text-muted-foreground">
                将删除该时间点<strong className="text-foreground">之前</strong>
                的全部记录。
              </p>
            </div>
          ) : (
            <p className="text-xs text-red-700/90 dark:text-red-300/90">
              将删除全部访问日志。请确认下方复选框后提交。
            </p>
          )}

          <div className="flex items-center justify-between gap-2">
            <Button
              type="button"
              size="sm"
              variant="secondary"
              className="h-8"
              disabled={previewing || (mode === "before" && !beforeLocal.trim())}
              onClick={() => void runPreview()}
            >
              {previewing ? (
                <Loader2 className="size-3.5 animate-spin" />
              ) : null}
              预览将删除条数
            </Button>
            {matched != null ? (
              <span className="text-xs tabular-nums text-muted-foreground">
                匹配{" "}
                <span className="font-medium text-foreground">{matched}</span> 条
              </span>
            ) : null}
          </div>

          <label className="flex items-start gap-2 rounded-md border p-2.5">
            <Checkbox
              checked={ack}
              onCheckedChange={(v) => setAck(v === true)}
              className="mt-0.5"
            />
            <span className="text-xs leading-relaxed text-muted-foreground">
              我已了解：清理后数据
              <strong className="text-foreground">不可恢复</strong>
              ，且可能影响历史统计与对账。
            </span>
          </label>
        </div>

        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={submitting}
            onClick={() => onOpenChange(false)}
          >
            取消
          </Button>
          <Button
            type="button"
            variant="destructive"
            disabled={!canSubmit}
            onClick={() => void runCleanup()}
          >
            {submitting ? (
              <Loader2 className="size-3.5 animate-spin" />
            ) : (
              <Trash2 className="size-3.5" />
            )}
            {mode === "all" ? "清理全部" : "确认清理"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

/** 访问日志可切换列（展开箭头列固定显示，不在此列） */
type UsageColId =
  | "status"
  | "key"
  | "account"
  | "model"
  | "reasoning"
  | "endpoint"
  | "group"
  | "type"
  | "billing"
  | "token"
  | "cost"
  | "latency"
  | "time"
  | "ip"
  | "userAgent"

const USAGE_COLS_STORAGE_KEY = "gateway-usage-columns-v1"

const USAGE_COLUMNS: {
  id: UsageColId
  label: string
  defaultVisible: boolean
}[] = [
  { id: "status", label: "状态", defaultVisible: true },
  { id: "key", label: "API 密钥", defaultVisible: true },
  { id: "account", label: "账户", defaultVisible: true },
  { id: "model", label: "模型", defaultVisible: true },
  { id: "reasoning", label: "推理强度", defaultVisible: true },
  { id: "endpoint", label: "端点", defaultVisible: true },
  { id: "group", label: "分组", defaultVisible: true },
  { id: "type", label: "类型", defaultVisible: true },
  { id: "billing", label: "计费模式", defaultVisible: true },
  { id: "token", label: "Token", defaultVisible: true },
  { id: "cost", label: "费用", defaultVisible: true },
  { id: "latency", label: "延迟", defaultVisible: true },
  { id: "userAgent", label: "User-Agent", defaultVisible: false },
  { id: "ip", label: "IP", defaultVisible: true },
  { id: "time", label: "时间", defaultVisible: true },
]

type UsageColVisibility = Record<UsageColId, boolean>

function defaultUsageColVisibility(): UsageColVisibility {
  return Object.fromEntries(
    USAGE_COLUMNS.map((c) => [c.id, c.defaultVisible]),
  ) as UsageColVisibility
}

function loadUsageColVisibility(): UsageColVisibility {
  const defaults = defaultUsageColVisibility()
  if (typeof window === "undefined") return defaults
  try {
    const raw = window.localStorage.getItem(USAGE_COLS_STORAGE_KEY)
    if (!raw) return defaults
    const parsed = JSON.parse(raw) as Partial<Record<string, boolean>>
    const next = { ...defaults }
    for (const col of USAGE_COLUMNS) {
      if (typeof parsed[col.id] === "boolean") {
        next[col.id] = parsed[col.id]!
      }
    }
    // 至少保留一列，避免整表空白
    if (!USAGE_COLUMNS.some((c) => next[c.id])) {
      return defaults
    }
    return next
  } catch {
    return defaults
  }
}

function saveUsageColVisibility(v: UsageColVisibility) {
  try {
    window.localStorage.setItem(USAGE_COLS_STORAGE_KEY, JSON.stringify(v))
  } catch {
    /* ignore quota / private mode */
  }
}

/** 单元格文本：不截断、不限宽，由整表横向滚动查看完整内容 */
function CellText({
  text,
  className,
  empty = "—",
}: {
  text?: string | null
  className?: string
  empty?: string
}) {
  const display = (text ?? "").trim() || empty
  return (
    <span className={cn("whitespace-nowrap", className)}>{display}</span>
  )
}



type LatencySeverity = "good" | "warn" | "slow" | "critical"

function firstTokenSeverity(ms: number): LatencySeverity {
  if (ms >= 60_000) return "critical"
  if (ms >= 30_000) return "slow"
  if (ms >= 10_000) return "warn"
  return "good"
}

function durationSeverity(ms: number): LatencySeverity {
  if (ms >= 300_000) return "critical"
  if (ms >= 180_000) return "slow"
  if (ms >= 60_000) return "warn"
  return "good"
}

const LATENCY_TEXT: Record<LatencySeverity, string> = {
  good: "text-emerald-600 dark:text-emerald-400",
  warn: "text-amber-600 dark:text-amber-400",
  slow: "text-orange-600 dark:text-orange-400",
  critical: "text-red-600 dark:text-red-400",
}

const LATENCY_BAR: Record<LatencySeverity, string> = {
  good: "bg-emerald-500",
  warn: "bg-amber-400",
  slow: "bg-orange-500",
  critical: "bg-red-500",
}

const LATENCY_FROM: Record<LatencySeverity, string> = {
  good: "from-emerald-500",
  warn: "from-amber-400",
  slow: "from-orange-500",
  critical: "from-red-500",
}

const LATENCY_TO: Record<LatencySeverity, string> = {
  good: "to-emerald-500",
  warn: "to-amber-400",
  slow: "to-orange-500",
  critical: "to-red-500",
}

function money6(v?: number | null) {
  if (v == null || !Number.isFinite(v)) return "$0.000000"
  return `$${v.toFixed(6)}`
}

function formatReasoningEffort(v?: string | null) {
  if (!v?.trim()) return "—"
  const s = v.trim()
  return s.charAt(0).toUpperCase() + s.slice(1).toLowerCase()
}

function billingModeLabel(mode?: string | null) {
  switch (mode) {
    case "image":
      return "图像"
    case "per_request":
      return "按次"
    case "video":
      return "视频"
    default:
      return "按量"
  }
}

function billingModeClass(mode?: string | null) {
  switch (mode) {
    case "image":
      return "bg-pink-100 text-pink-700 dark:bg-pink-900/30 dark:text-pink-300"
    case "per_request":
      return "bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-300"
    case "video":
      return "bg-amber-100 text-amber-700 dark:bg-amber-900/30 dark:text-amber-300"
    default:
      return "bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
  }
}

function requestTypeLabel(u: GatewayUsageLog) {
  if (u.stream || u.request_type === 2) return "流式"
  if (u.request_type === 1) return "同步"
  return "同步"
}

function requestTypeClass(u: GatewayUsageLog) {
  if (u.stream || u.request_type === 2) {
    return "bg-cyan-100 text-cyan-800 dark:bg-cyan-900/40 dark:text-cyan-200"
  }
  return "bg-slate-100 text-slate-700 dark:bg-slate-800 dark:text-slate-200"
}

function mappingSteps(u: GatewayUsageLog): string[] {
  if (u.model_mapping_chain?.includes("→")) {
    return u.model_mapping_chain
      .split("→")
      .map((s) => s.trim())
      .filter(Boolean)
  }
  if (u.upstream_model && u.upstream_model !== u.requested_model) {
    return [u.requested_model, u.upstream_model]
  }
  return [u.requested_model || "—"]
}

function errorTypeLabel(t?: string) {
  switch (t) {
    case "transport":
      return "传输"
    case "http":
      return "HTTP 错误"
    case "config":
      return "配置"
    case "internal":
      return "内部"
    case "client":
      return "客户端断开"
    default:
      return t || "错误"
  }
}

/**
 * error_type=client：客户端主动关连接。
 * 新逻辑 success=true 仍带 client 标记；旧数据可能 success=false。
 * 展示一律为「客户端断开」，不进红字失败。
 */
function isClientDisconnectResult(u: GatewayUsageLog) {
  return u.error_type === "client"
}

function ResultBadge({ u }: { u: GatewayUsageLog }) {
  if (isClientDisconnectResult(u)) {
    return (
      <span
        className="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium bg-amber-100 text-amber-900 dark:bg-amber-900/40 dark:text-amber-200"
        title="客户端在流式响应过程中关闭连接；上游可能已正常计费，已同步用量"
      >
        客户端断开
      </span>
    )
  }
  if (u.attempt_status === "rejected") {
    return (
      <span className="inline-flex items-center rounded bg-rose-100 px-2 py-0.5 text-xs font-medium text-rose-800 dark:bg-rose-900/40 dark:text-rose-200">
        已拒绝
      </span>
    )
  }
  if (u.attempt_status === "canceled") {
    return (
      <span className="inline-flex items-center rounded bg-zinc-100 px-2 py-0.5 text-xs font-medium text-zinc-700 dark:bg-zinc-800 dark:text-zinc-200">
        已取消
      </span>
    )
  }
  if (u.attempt_status === "accepted" && !u.winner) {
    return (
      <span className="inline-flex items-center rounded bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-700 dark:bg-slate-800 dark:text-slate-200">
        未获胜
      </span>
    )
  }
  if (u.winner) {
    return (
      <span className="inline-flex items-center rounded bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200">
        已获胜
      </span>
    )
  }
  if (u.success) {
    return (
      <span className="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium bg-emerald-100 text-emerald-800 dark:bg-emerald-900/40 dark:text-emerald-200">
        成功
      </span>
    )
  }
  return (
    <span className="inline-flex items-center rounded px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-200">
      失败
    </span>
  )
}

function ResultText({ u }: { u: GatewayUsageLog }) {
  if (isClientDisconnectResult(u)) {
    return (
      <span
        className="text-amber-700 dark:text-amber-400"
        title="客户端在流式响应过程中关闭连接；上游可能已正常计费，已同步用量"
      >
        客户端断开
      </span>
    )
  }
  if (u.attempt_status === "rejected") {
    return <span className="text-rose-600 dark:text-rose-400">已拒绝</span>
  }
  if (u.attempt_status === "canceled") {
    return <span className="text-zinc-600 dark:text-zinc-300">已取消</span>
  }
  if (u.attempt_status === "accepted" && !u.winner) {
    return <span className="text-slate-600 dark:text-slate-300">未获胜</span>
  }
  if (u.winner) {
    return <span className="text-emerald-600 dark:text-emerald-400">已获胜</span>
  }
  if (u.success) {
    return <span className="text-emerald-600 dark:text-emerald-400">成功</span>
  }
  return <span className="text-red-600 dark:text-red-400">失败</span>
}

function hasErrorDetail(u: GatewayUsageLog) {
  return !!(
    u.error_message ||
    u.error_detail ||
    u.upstream_error_body ||
    u.upstream_error_headers ||
    u.upstream_url
  )
}

function hasValidationDetail(u: GatewayUsageLog) {
  return !!(
    u.validation_rule_id ||
    u.validation_rule_name ||
    u.validation_reason ||
    u.validation_post_commit
  )
}

function canShowAttemptDetail(u: GatewayUsageLog) {
  return (
    hasValidationDetail(u) ||
    (hasErrorDetail(u) && (!u.success || isClientDisconnectResult(u)))
  )
}

function detailToggleLabel(u: GatewayUsageLog) {
  if (isClientDisconnectResult(u)) return "断开详情"
  if (hasValidationDetail(u)) return "校验详情"
  return "错误详情"
}

function tryPrettyJSON(raw: string): string {
  const s = raw.trim()
  if (!s) return raw
  try {
    return JSON.stringify(JSON.parse(s), null, 2)
  } catch {
    return raw
  }
}

/** 将落库的上游 header JSON（[{name,values}]）格式化为完整 Name: value 文本。 */
function formatUpstreamHeaders(raw?: string): string {
  const s = (raw || "").trim()
  if (!s) return ""
  try {
    const parsed = JSON.parse(s) as unknown
    if (Array.isArray(parsed)) {
      const lines: string[] = []
      for (const item of parsed) {
        if (!item || typeof item !== "object") continue
        const name = String((item as { name?: unknown }).name ?? "").trim()
        const values = (item as { values?: unknown }).values
        if (!name) continue
        if (Array.isArray(values) && values.length > 0) {
          for (const v of values) {
            lines.push(`${name}: ${String(v)}`)
          }
        } else {
          lines.push(`${name}:`)
        }
      }
      if (lines.length > 0) return lines.join("\n")
    }
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      const lines: string[] = []
      for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
        if (Array.isArray(v)) {
          for (const item of v) lines.push(`${k}: ${String(item)}`)
        } else {
          lines.push(`${k}: ${String(v)}`)
        }
      }
      if (lines.length > 0) return lines.join("\n")
    }
  } catch {
    // fall through
  }
  return tryPrettyJSON(s)
}

async function copyText(label: string, text: string) {
  try {
    await copyToClipboard(text)
    toast.success(`已复制${label}`)
  } catch {
    toast.error("复制失败")
  }
}

function attemptKindLabel(kind?: string) {
  switch (kind) {
    case "recovery":
      return "冷却恢复"
    case "retry":
      return "重试"
    case "failover":
      return "顺延"
    case "hedge":
      return "并发兜底"
    case "regex_reject":
      return "正则拒绝"
    case "primary":
      return "首次"
    default:
      return kind || ""
  }
}

/** 仅在有重试/顺延时展示尝试标签，避免单次成功也刷 #1·首次 */
function AttemptBadge({
  u,
  chainTotal,
  chainIndex,
}: {
  u: GatewayUsageLog
  chainTotal?: number
  chainIndex?: number
}) {
  const attempt = u.attempt && u.attempt > 0 ? u.attempt : 1
  const kind = (u.attempt_kind || "").trim()
  const multi = (chainTotal ?? 0) > 1
  const isRetryish =
    kind === "retry" ||
    kind === "failover" ||
    kind === "hedge" ||
    kind === "recovery" ||
    kind === "regex_reject" ||
    attempt > 1
  if (!multi && !isRetryish) return null

  const label =
    multi && chainIndex != null
      ? `${chainIndex}/${chainTotal} · ${attemptKindLabel(kind || "primary") || "尝试"}`
      : `#${attempt}${kind ? ` · ${attemptKindLabel(kind)}` : ""}`

  return (
    <span
      className={cn(
        "inline-flex items-center rounded px-1.5 py-0.5 text-[10px] font-medium tabular-nums",
        kind === "regex_reject"
          ? "bg-rose-100 text-rose-800 dark:bg-rose-900/40 dark:text-rose-200"
          : kind === "hedge"
            ? "bg-amber-100 text-amber-900 dark:bg-amber-900/40 dark:text-amber-200"
            : kind === "failover"
          ? "bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-200"
          : kind === "retry"
            ? "bg-sky-100 text-sky-800 dark:bg-sky-900/40 dark:text-sky-200"
            : "bg-muted text-muted-foreground",
      )}
    >
      {label}
    </span>
  )
}

function StatusCell({
  u,
  chainTotal,
  chainIndex,
}: {
  u: GatewayUsageLog
  chainTotal?: number
  chainIndex?: number
}) {
  const reqID = (u.request_id || "").trim()
  return (
    <div className="space-y-0.5">
      <div className="flex flex-wrap items-center gap-1">
        <ResultBadge u={u} />
        <AttemptBadge u={u} chainTotal={chainTotal} chainIndex={chainIndex} />
        {u.status_code > 0 ? (
          <span className="text-[11px] tabular-nums text-muted-foreground">
            {u.status_code}
          </span>
        ) : null}
      </div>
      {reqID ? (
        <button
          type="button"
          className="group/rid flex items-center gap-1 text-left"
          title="点击复制 Request ID"
          onClick={() => void copyText("Request ID", reqID)}
        >
          <span className="shrink-0 text-[10px] text-muted-foreground">ID</span>
          <CellText
            text={reqID}
            className="font-mono text-[10px] text-muted-foreground group-hover/rid:text-foreground"
          />
        </button>
      ) : null}
      {!u.success && u.error_type && u.error_type !== "client" ? (
        <div className="text-[10px] text-muted-foreground">
          {errorTypeLabel(u.error_type)}
        </div>
      ) : null}
      {u.cooldown_until ? (
        <div className="text-[10px] text-amber-700 dark:text-amber-400">
          冷却至 {fullDateTime(u.cooldown_until)}
        </div>
      ) : null}
    </div>
  )
}

function ChainTimeline({
  rows,
  errorOpen,
  onToggleError,
}: {
  rows: GatewayUsageLog[]
  errorOpen: Record<number, boolean>
  onToggleError: (id: number) => void
}) {
  return (
    <div className="space-y-2 rounded-lg border border-violet-200/70 bg-violet-50/40 p-3 dark:border-violet-900/40 dark:bg-violet-950/20">
      <div className="min-w-0 text-xs font-medium text-violet-900 dark:text-violet-200">
        同请求链路 · {rows.length} 次尝试
        <span className="ml-2 break-all font-mono text-[10px] font-normal text-muted-foreground">
          {rows[0]?.request_id}
        </span>
      </div>
      <ol className="space-y-2">
        {rows.map((r, i) => {
          const account =
            r.provider_name ||
            r.channel_name ||
            (r.gateway_provider_id
              ? `直连 #${r.gateway_provider_id}`
              : r.channel_id
                ? `#${r.channel_id}`
                : "—")
          const canShowDetail = canShowAttemptDetail(r)
          const errOpen = canShowDetail && !!errorOpen[r.id]
          return (
            <li
              key={r.id}
              className="min-w-0 rounded-md border bg-background/80 px-2.5 py-2 text-xs"
            >
              <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
                <span className="w-6 tabular-nums text-muted-foreground">
                  {i + 1}.
                </span>
                <AttemptBadge u={r} chainTotal={rows.length} chainIndex={i + 1} />
                <ResultText u={r} />
                {r.status_code > 0 ? (
                  <span className="tabular-nums text-muted-foreground">{r.status_code}</span>
                ) : null}
                <span className="max-w-[10rem] truncate font-medium" title={account}>
                  {account}
                </span>
                {r.source_api_key_name ? (
                  <span className="max-w-[8rem] truncate text-muted-foreground">
                    {r.source_api_key_name}
                  </span>
                ) : null}
                <span className="tabular-nums text-muted-foreground">
                  {formatDurationMS(r.duration_ms)}
                </span>
                {hasValidationDetail(r) ? (
                  <span
                    className={cn(
                      "max-w-[14rem] truncate text-[11px]",
                      r.validation_post_commit
                        ? "text-amber-700 dark:text-amber-300"
                        : "text-rose-700 dark:text-rose-300",
                    )}
                    title={r.validation_reason || r.validation_rule_name || "响应校验命中"}
                  >
                    {r.validation_rule_name ||
                      (r.validation_rule_id ? `规则 #${r.validation_rule_id}` : "响应规则")}
                    {r.validation_post_commit ? " · 提交后命中" : " · 已拒绝"}
                  </span>
                ) : null}
                {(r.estimated_extra_cost || 0) > 0 ? (
                  <span
                    className="text-[11px] tabular-nums text-amber-700 dark:text-amber-300"
                    title="该未获胜或被拒绝 attempt 可能产生的额外上游成本"
                  >
                    额外 {money6(r.estimated_extra_cost || 0)}
                  </span>
                ) : null}
                {canShowDetail ? (
                  <button
                    type="button"
                    className="text-[11px] text-violet-700 underline-offset-2 hover:underline dark:text-violet-300"
                    onClick={() => onToggleError(r.id)}
                  >
                    {errOpen
                      ? "收起详情"
                      : detailToggleLabel(r)}
                  </button>
                ) : null}
              </div>
              {errOpen ? (
                <div className="mt-2 min-w-0 border-t border-border/60 pt-2">
                  <ErrorDetailPanel u={r} />
                </div>
              ) : null}
            </li>
          )
        })}
      </ol>
    </div>
  )
}

function ErrorDetailPanel({ u }: { u: GatewayUsageLog }) {
  const bodyPretty = u.upstream_error_body ? tryPrettyJSON(u.upstream_error_body) : ""
  const headersPretty = formatUpstreamHeaders(u.upstream_error_headers)
  const auditOnly = !!u.validation_post_commit && u.success
  const fullDebug = [
    `request_id: ${u.request_id || "—"}`,
    `attempt: ${u.attempt ?? 1}`,
    `attempt_kind: ${u.attempt_kind || "—"}`,
    `route_id: ${u.route_id || "—"}`,
    `channel: ${u.channel_name || u.channel_id || "—"}`,
    `status: ${u.status_code || 0}`,
    `type: ${u.error_type || "—"}`,
    `attempt_status: ${u.attempt_status || "—"}`,
    `winner: ${u.winner ? "true" : "false"}`,
    `validation_rule: ${u.validation_rule_name || u.validation_rule_id || "—"}`,
    `validation_reason: ${u.validation_reason || "—"}`,
    `validation_post_commit: ${u.validation_post_commit ? "true" : "false"}`,
    `estimated_extra_cost: ${money6(u.estimated_extra_cost || 0)}`,
    `url: ${u.upstream_url || "—"}`,
    `cooldown_until: ${u.cooldown_until || "—"}`,
    "",
    "--- summary ---",
    u.error_message || "(none)",
    "",
    "--- detail ---",
    u.error_detail || "(none)",
    "",
    "--- upstream body ---",
    bodyPretty || "(none)",
    "",
    "--- upstream headers (full) ---",
    headersPretty || "(none)",
  ].join("\n")

  return (
    <div
      className={cn(
        "space-y-3 rounded-lg border p-3 text-xs",
        auditOnly
          ? "border-amber-200/80 bg-amber-50/50 dark:border-amber-900/50 dark:bg-amber-950/20"
          : "border-red-200/80 bg-red-50/50 dark:border-red-900/50 dark:bg-red-950/20",
      )}
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div
          className={cn(
            "font-medium",
            auditOnly
              ? "text-amber-900 dark:text-amber-200"
              : "text-red-800 dark:text-red-200",
          )}
        >
          {auditOnly ? "响应校验审计" : "上游错误详情"}
        </div>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="h-7 gap-1 text-xs"
          onClick={() => void copyText(auditOnly ? "校验审计" : "错误详情", fullDebug)}
        >
          <Copy className="size-3" /> 复制全部
        </Button>
      </div>
      <div className="grid gap-2 sm:grid-cols-2">
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Request ID（同请求关联键）
          </div>
          <div className="font-mono break-all">{u.request_id || "—"}</div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
            尝试
          </div>
          <div>
            #{u.attempt && u.attempt > 0 ? u.attempt : 1}
            {u.attempt_kind ? ` · ${attemptKindLabel(u.attempt_kind)}` : ""}
            {u.winner ? " · winner" : ""}
            {u.cooldown_until ? ` · 冷却至 ${fullDateTime(u.cooldown_until)}` : ""}
          </div>
        </div>
        {hasValidationDetail(u) ? (
          <div>
            <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
              响应校验
            </div>
            <div className="space-y-0.5 break-all">
              <div>
                {u.validation_rule_name ||
                  (u.validation_rule_id ? `规则 #${u.validation_rule_id}` : "响应规则")}
                {u.validation_post_commit ? " · 提交后命中（仅审计）" : " · 已拒绝并切换"}
              </div>
              {u.validation_reason ? (
                <div className="text-muted-foreground">{u.validation_reason}</div>
              ) : null}
            </div>
          </div>
        ) : null}
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">Upstream URL</div>
          <div className="font-mono break-all">{u.upstream_url || "—"}</div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">Route / Channel</div>
          <div className="break-all">
            route #{u.route_id}
            {" · "}
            {u.channel_name || `channel #${u.channel_id}`}
            {u.source_api_key_name ? ` · ${u.source_api_key_name}` : ""}
          </div>
        </div>
        <div>
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">协议</div>
          <div>
            {u.inbound_protocol || "—"} → {u.upstream_protocol || "—"}
            {u.protocol_converted ? "（已转换）" : ""}
          </div>
        </div>
      </div>
      {u.error_detail ? (
        <div>
          <div className="mb-1 flex items-center justify-between">
            <div className="text-[10px] uppercase tracking-wide text-muted-foreground">Detail</div>
            <button
              type="button"
              className="text-[10px] text-muted-foreground hover:text-foreground"
              onClick={() => void copyText("Detail", u.error_detail || "")}
            >
              复制
            </button>
          </div>
          <pre className="max-h-40 overflow-auto whitespace-pre-wrap break-all rounded-md border bg-background/80 p-2 font-mono text-[11px] leading-4">
            {u.error_detail}
          </pre>
        </div>
      ) : null}
      {bodyPretty ? (
        <div>
          <div className="mb-1 flex items-center justify-between">
            <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
              Upstream Response Body
            </div>
            <button
              type="button"
              className="text-[10px] text-muted-foreground hover:text-foreground"
              onClick={() => void copyText("Body", bodyPretty)}
            >
              复制
            </button>
          </div>
          <pre className="max-h-56 overflow-auto whitespace-pre-wrap break-all rounded-md border bg-background/80 p-2 font-mono text-[11px] leading-4">
            {bodyPretty}
          </pre>
        </div>
      ) : null}
      <div>
        <div className="mb-1 flex items-center justify-between">
          <div className="text-[10px] uppercase tracking-wide text-muted-foreground">
            Upstream Response Headers（完整）
          </div>
          {headersPretty ? (
            <button
              type="button"
              className="text-[10px] text-muted-foreground hover:text-foreground"
              onClick={() => void copyText("Headers", headersPretty)}
            >
              复制
            </button>
          ) : null}
        </div>
        <pre className="max-h-64 overflow-auto whitespace-pre-wrap break-all rounded-md border bg-background/80 p-2 font-mono text-[11px] leading-4 [overflow-wrap:anywhere]">
          {headersPretty || "(none)"}
        </pre>
      </div>
    </div>
  )
}

function totalTokens(u: GatewayUsageLog) {
  const virtualCacheRead = u.virtual_cache_read_tokens ?? 0
  const inputTokens = Math.max(0, (u.input_tokens || 0) - virtualCacheRead)
  return (
    inputTokens +
    (u.output_tokens || 0) +
    (u.cache_creation_tokens || 0) +
    (u.cache_read_tokens || 0) +
    virtualCacheRead
  )
}

function TokenCell({ u }: { u: GatewayUsageLog }) {
  const reasoning = u.reasoning_tokens ?? 0
  const virtualCacheRead = u.virtual_cache_read_tokens ?? 0
  const virtualCacheLabel =
    u.virtual_cache_reason === "response_rule_failover"
      ? "正则顺延 virtual read"
      : u.virtual_cache_reason === "provider_global"
        ? "渠道级 virtual read"
        : "并发兜底 virtual read"
  const inputTokens = Math.max(0, (u.input_tokens || 0) - virtualCacheRead)
  return (
    <div className="flex items-start gap-1.5">
      <div className="space-y-1 text-sm">
        <div className="flex items-center gap-2">
          <div className="inline-flex items-center gap-1" title="输入（不含缓存）">
            <ArrowDown className="size-3.5 text-emerald-500" />
            <span className="font-medium tabular-nums">{formatTokens(inputTokens)}</span>
          </div>
          <div className="inline-flex items-center gap-1" title="输出">
            <ArrowUp className="size-3.5 text-violet-500" />
            <span className="font-medium tabular-nums">{formatTokens(u.output_tokens)}</span>
          </div>
        </div>
        {(u.cache_read_tokens > 0 || u.cache_creation_tokens > 0 || virtualCacheRead > 0) && (
          <div className="flex flex-wrap items-center gap-2 text-xs">
            {u.cache_read_tokens > 0 && (
              <span className="font-medium text-sky-600 dark:text-sky-400 tabular-nums">
                读 {formatTokens(u.cache_read_tokens)}
              </span>
            )}
            {virtualCacheRead > 0 && (
              <span className="font-medium text-cyan-600 dark:text-cyan-400 tabular-nums" title={virtualCacheLabel}>
                virtual read {formatTokens(virtualCacheRead)}
              </span>
            )}
            {u.cache_creation_tokens > 0 && (
              <span className="font-medium text-amber-600 dark:text-amber-400 tabular-nums">
                写 {formatTokens(u.cache_creation_tokens)}
                {(u.cache_creation_1h_tokens ?? 0) > 0 && (
                  <span className="ml-1 rounded bg-orange-100 px-1 text-[10px] text-orange-600 ring-1 ring-inset ring-orange-200 dark:bg-orange-500/20 dark:text-orange-400">
                    1h
                  </span>
                )}
              </span>
            )}
          </div>
        )}
        {(u.image_output_tokens ?? 0) > 0 && (
          <div className="text-xs font-medium text-pink-600 dark:text-pink-400 tabular-nums">
            图 {formatTokens(u.image_output_tokens)}
          </div>
        )}
      </div>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            className="mt-0.5 flex size-4 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground hover:bg-blue-100 hover:text-blue-600 dark:hover:bg-blue-900/50"
          >
            <Info className="size-3" />
          </button>
        </TooltipTrigger>
        <TooltipContent side="left" className="max-w-xs space-y-1 p-3 text-xs">
          <div className="mb-1 font-semibold">Token 明细</div>
          <div className="flex justify-between gap-6">
            <span className="text-muted-foreground">输入（不含缓存）</span>
            <span>{formatTokens(inputTokens)}</span>
          </div>
          {u.cache_read_tokens > 0 && (
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">缓存读取</span>
              <span>{formatTokens(u.cache_read_tokens)}</span>
            </div>
          )}
          {virtualCacheRead > 0 && (
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">{virtualCacheLabel}</span>
              <span>{formatTokens(virtualCacheRead)}</span>
            </div>
          )}
          {u.cache_creation_tokens > 0 && (
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">缓存创建</span>
              <span>{formatTokens(u.cache_creation_tokens)}</span>
            </div>
          )}
          <div className="flex justify-between gap-6">
            <span className="text-muted-foreground">输出</span>
            <span>{formatTokens(u.output_tokens)}</span>
          </div>
          {reasoning > 0 && (
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">其中推理</span>
              <span>{formatTokens(reasoning)}</span>
            </div>
          )}
          <div className="flex justify-between gap-6 border-t pt-1 font-semibold">
            <span className="text-muted-foreground">合计</span>
            <span className="text-blue-500">{formatTokens(totalTokens(u))}</span>
          </div>
        </TooltipContent>
      </Tooltip>
    </div>
  )
}

function CostCell({ u }: { u: GatewayUsageLog }) {
  const standard = u.total_cost || 0
  const actual = u.actual_cost || 0
  const virtualCacheRead = u.virtual_cache_read_tokens ?? 0
  const virtualCacheLabel =
    u.virtual_cache_reason === "response_rule_failover"
      ? "正则顺延 virtual read"
      : u.virtual_cache_reason === "provider_global"
        ? "渠道级 virtual read"
        : "并发兜底 virtual read"
  const billed = u.billed_cost ?? 0
  const displayed = u.winner && (billed > 0 || virtualCacheRead > 0) ? billed : actual
  const virtualCacheSubsidy =
    u.winner && virtualCacheRead > 0 ? Math.max(0, actual - billed) : 0
  const extra = Math.max(0, (u.estimated_extra_cost || 0) + virtualCacheSubsidy)
  return (
    <div className="text-sm">
      <div className="flex items-center gap-1.5">
        <span
          className="font-medium tabular-nums text-green-600 dark:text-green-400"
          title={virtualCacheRead > 0 ? `${virtualCacheLabel} 后用户实收` : "实收 = 标准费用 × 账号计费倍率"}
        >
          {money6(displayed)}
        </span>
        <Tooltip>
          <TooltipTrigger asChild>
            <button
              type="button"
              className="flex size-4 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground hover:bg-blue-100 hover:text-blue-600"
            >
              <Info className="size-3" />
            </button>
          </TooltipTrigger>
          <TooltipContent side="left" className="max-w-xs space-y-1 p-3 text-xs">
            <div className="mb-1 font-semibold">费用明细</div>
            {u.input_cost > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">输入</span>
                <span>{money6(u.input_cost)}</span>
              </div>
            )}
            {u.output_cost > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">输出</span>
                <span>{money6(u.output_cost)}</span>
              </div>
            )}
            {u.cache_creation_cost > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">缓存创建</span>
                <span>{money6(u.cache_creation_cost)}</span>
              </div>
            )}
            {u.cache_read_cost > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">缓存读取</span>
                <span>{money6(u.cache_read_cost)}</span>
              </div>
            )}
            {virtualCacheRead > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">{virtualCacheLabel}</span>
                <span>{money6(u.virtual_cache_read_cost ?? 0)}</span>
              </div>
            )}
            {virtualCacheSubsidy > 0 && (
              <div className="flex justify-between gap-6 text-amber-700 dark:text-amber-300">
                <span>虚拟缓存补贴</span>
                <span>{money6(virtualCacheSubsidy)}</span>
              </div>
            )}
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">倍率</span>
              <span className="text-blue-400">{(u.rate_multiplier || 1).toFixed(4)}x</span>
            </div>
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">计费倍率</span>
              <span className="text-blue-400">
                {(u.billing_rate_multiplier || 1).toFixed(4)}x
              </span>
            </div>
            <div className="flex justify-between gap-6">
              <span className="text-muted-foreground">标准费用</span>
              <span>{money6(standard)}</span>
            </div>
            <div className="flex justify-between gap-6 font-semibold">
              <span className="text-muted-foreground">实收</span>
              <span className="text-green-400">{money6(displayed)}</span>
            </div>
            {virtualCacheRead > 0 && (
              <div className="flex justify-between gap-6">
                <span className="text-muted-foreground">upstream actual</span>
                <span>{money6(actual)}</span>
              </div>
            )}
            {extra > 0 ? (
              <div className="flex justify-between gap-6 text-amber-700 dark:text-amber-300">
                <span>额外成本（attempt + 补贴）</span>
                <span>{money6(extra)}</span>
              </div>
            ) : null}
          </TooltipContent>
        </Tooltip>
      </div>
      {extra > 0 ? (
        <div className="mt-0.5 text-[11px] tabular-nums text-amber-600 dark:text-amber-400" title="loser/rejected attempt 成本 + 虚拟缓存补贴，不计入网关 Key 配额">
          额外 {money6(extra)}
        </div>
      ) : null}
      {/* 原样式：橙色副行；内容改为标准费用（未乘倍率），避免与实收重复 */}
      <div
        className="mt-0.5 text-[11px] text-orange-500 dark:text-orange-400 tabular-nums"
        title="标准费用 = tokens × 模型单价（未乘倍率）"
      >
        A {money6(standard)}
      </div>
    </div>
  )
}

function LatencyCell({ u }: { u: GatewayUsageLog }) {
  const dSev = durationSeverity(u.duration_ms ?? 0)
  const hasFT = u.first_token_ms != null
  const fSev = hasFT ? firstTokenSeverity(u.first_token_ms!) : dSev
  return (
    <div className="flex items-stretch gap-2">
      <span
        className={cn(
          "w-1 shrink-0 rounded-full",
          hasFT
            ? cn("bg-gradient-to-b from-40% to-60%", LATENCY_FROM[fSev], LATENCY_TO[dSev])
            : LATENCY_BAR[dSev],
        )}
        aria-hidden
      />
      <div className="grid grid-cols-[max-content_max-content] items-baseline gap-x-2 gap-y-0.5 text-xs">
        <span className="text-muted-foreground">首字</span>
        {hasFT ? (
          <span className={cn("font-medium tabular-nums", LATENCY_TEXT[fSev])}>
            {formatDurationMS(u.first_token_ms)}
          </span>
        ) : (
          <span className="text-muted-foreground">—</span>
        )}
        <span className="text-muted-foreground">总耗时</span>
        <span className={cn("font-medium tabular-nums", LATENCY_TEXT[dSev])}>
          {formatDurationMS(u.duration_ms)}
        </span>
      </div>
    </div>
  )
}

export function GatewayUsageTable({
  items,
  emptyText = "暂无使用记录",
  onCleaned,
}: {
  items: GatewayUsageLog[]
  emptyText?: string
  /** 清理成功后回调（刷新列表 / 统计） */
  onCleaned?: () => void
}) {
  const [errorOpen, setErrorOpen] = useState<Record<number, boolean>>({})
  const [chainOpen, setChainOpen] = useState<Record<string, boolean>>({})
  const [cleanupOpen, setCleanupOpen] = useState(false)
  const [colVisible, setColVisible] = useState<UsageColVisibility>(() =>
    loadUsageColVisibility(),
  )

  const show = useCallback(
    (id: UsageColId) => colVisible[id] !== false,
    [colVisible],
  )

  const visibleCount = useMemo(
    () => USAGE_COLUMNS.filter((c) => colVisible[c.id]).length,
    [colVisible],
  )
  // 展开箭头列 + 可见数据列
  const colSpan = 1 + visibleCount

  const toggleCol = useCallback((id: UsageColId, next: boolean) => {
    setColVisible((prev) => {
      if (!next) {
        const others = USAGE_COLUMNS.filter((c) => c.id !== id && prev[c.id])
        if (others.length === 0) {
          toast.message("至少保留一列")
          return prev
        }
      }
      const updated = { ...prev, [id]: next }
      saveUsageColVisibility(updated)
      return updated
    })
  }, [])

  const resetCols = useCallback(() => {
    const defaults = defaultUsageColVisibility()
    saveUsageColVisibility(defaults)
    setColVisible(defaults)
  }, [])

  // request_id → 本页内全部尝试（按 attempt / id 排序）
  const chainByReq = (() => {
    const m = new Map<string, GatewayUsageLog[]>()
    for (const u of items) {
      const rid = (u.request_id || "").trim()
      if (!rid) continue
      const list = m.get(rid) ?? []
      list.push(u)
      m.set(rid, list)
    }
    for (const [k, list] of m) {
      list.sort((a, b) => (a.attempt || 1) - (b.attempt || 1) || a.id - b.id)
      m.set(k, list)
    }
    return m
  })()

  // 主行列表：保持接口返回顺序，但同 request_id 只出一行（最终结果）
  // 避免「折叠时只显示成功、展开又插一堆完整行」把表格打乱
  const mainRows: GatewayUsageLog[] = []
  const seenReq = new Set<string>()
  for (const u of items) {
    const rid = (u.request_id || "").trim()
    if (rid && chainByReq.has(rid)) {
      if (seenReq.has(rid)) continue
      seenReq.add(rid)
      const chain = chainByReq.get(rid)!
      // 主行优先使用实际交付并结算的 winner；兼容旧记录时再回退到最终成功。
      const final =
        chain.find((r) => r.winner) ||
        [...chain].reverse().find((r) => r.success) ||
        chain[chain.length - 1]
      mainRows.push(final)
    } else {
      mainRows.push(u)
    }
  }

  return (
    <TooltipProvider delayDuration={200}>
      <div className="space-y-2">
        <div className="flex items-center justify-end gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            className="h-8 gap-1.5 text-muted-foreground hover:text-destructive"
            onClick={() => setCleanupOpen(true)}
          >
            <Trash2 className="size-3.5" />
            清理
          </Button>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button type="button" size="sm" variant="outline" className="h-8 gap-1.5">
                <Columns3 className="size-3.5" />
                列显示
                <span className="tabular-nums text-muted-foreground">
                  {visibleCount}/{USAGE_COLUMNS.length}
                </span>
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end" className="w-48">
              <DropdownMenuLabel>选择展示列</DropdownMenuLabel>
              <DropdownMenuSeparator />
              {USAGE_COLUMNS.map((col) => (
                <DropdownMenuCheckboxItem
                  key={col.id}
                  checked={colVisible[col.id]}
                  onCheckedChange={(checked) =>
                    toggleCol(col.id, checked === true)
                  }
                  onSelect={(e) => e.preventDefault()}
                >
                  {col.label}
                </DropdownMenuCheckboxItem>
              ))}
              <DropdownMenuSeparator />
              <DropdownMenuItem onSelect={resetCols}>恢复默认</DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>

        <UsageCleanupDialog
          open={cleanupOpen}
          onOpenChange={setCleanupOpen}
          onCleaned={onCleaned}
        />

        {/* 整表横向滚动：列按内容撑开，不在单元格内滚动 */}
        <div className="overflow-x-auto rounded-md border">
          <Table className="w-max min-w-full">
            <TableHeader>
              <TableRow>
                <TableHead className="w-10 whitespace-nowrap" />
                {show("status") && (
                  <TableHead className="whitespace-nowrap">状态</TableHead>
                )}
                {show("key") && (
                  <TableHead className="whitespace-nowrap">API 密钥</TableHead>
                )}
                {show("account") && (
                  <TableHead className="whitespace-nowrap">账户</TableHead>
                )}
                {show("model") && (
                  <TableHead className="whitespace-nowrap">模型</TableHead>
                )}
                {show("reasoning") && (
                  <TableHead className="whitespace-nowrap">推理强度</TableHead>
                )}
                {show("endpoint") && (
                  <TableHead className="whitespace-nowrap">端点</TableHead>
                )}
                {show("group") && (
                  <TableHead className="whitespace-nowrap">分组</TableHead>
                )}
                {show("type") && (
                  <TableHead className="whitespace-nowrap">类型</TableHead>
                )}
                {show("billing") && (
                  <TableHead className="whitespace-nowrap">计费模式</TableHead>
                )}
                {show("token") && (
                  <TableHead className="whitespace-nowrap">Token</TableHead>
                )}
                {show("cost") && (
                  <TableHead className="whitespace-nowrap">费用</TableHead>
                )}
                {show("latency") && (
                  <TableHead className="whitespace-nowrap">延迟</TableHead>
                )}
                {show("userAgent") && (
                  <TableHead className="whitespace-nowrap">User-Agent</TableHead>
                )}
                {show("ip") && (
                  <TableHead className="whitespace-nowrap">IP</TableHead>
                )}
                {show("time") && (
                  <TableHead className="whitespace-nowrap">时间</TableHead>
                )}
              </TableRow>
            </TableHeader>
            <TableBody>
              {mainRows.map((u) => {
                const steps = mappingSteps(u)
                const rid = (u.request_id || "").trim()
                const chain = rid ? chainByReq.get(rid) : undefined
                const chainTotal = chain?.length ?? 1
                const chainIndex =
                  chain && chainTotal > 1
                    ? chain.findIndex((r) => r.id === u.id) + 1
                    : undefined
                const hasChain = chainTotal > 1
                const chainExpanded = hasChain && !!chainOpen[rid]
                const canDetail = canShowAttemptDetail(u)
                const errExpanded = !!errorOpen[u.id]
                // 单箭头：有链路优先展开链路；无链路时展开错误或校验详情
                const showToggle = hasChain || canDetail
                const rowOpen = hasChain ? chainExpanded : errExpanded

                return (
                  <Fragment key={u.id}>
                    <TableRow
                      className={cn(
                        !u.success &&
                          !isClientDisconnectResult(u) &&
                          "bg-red-50/40 dark:bg-red-950/10",
                        isClientDisconnectResult(u) &&
                          "bg-amber-50/40 dark:bg-amber-950/10",
                        hasChain && "border-l-2 border-l-violet-400/80",
                      )}
                    >
                      <TableCell className="w-10 px-1">
                        {showToggle ? (
                          <button
                            type="button"
                            className={cn(
                              "inline-flex size-7 items-center justify-center rounded",
                              hasChain
                                ? "text-violet-600 hover:bg-violet-100 dark:text-violet-300 dark:hover:bg-violet-950/40"
                                : "text-muted-foreground hover:bg-muted hover:text-foreground",
                            )}
                            title={
                              hasChain
                                ? rowOpen
                                  ? "收起请求链路"
                                  : `展开请求链路（${chainTotal} 次）`
                                : rowOpen
                                  ? "收起详情"
                                  : `展开${detailToggleLabel(u)}`
                            }
                            onClick={() => {
                              if (hasChain) {
                                setChainOpen((prev) => ({
                                  ...prev,
                                  [rid]: !prev[rid],
                                }))
                              } else {
                                setErrorOpen((prev) => ({
                                  ...prev,
                                  [u.id]: !prev[u.id],
                                }))
                              }
                            }}
                          >
                            {rowOpen ? (
                              <ChevronDown className="size-4" />
                            ) : (
                              <ChevronRight className="size-4" />
                            )}
                          </button>
                        ) : (
                          <span className="inline-block size-7" />
                        )}
                      </TableCell>

                      {show("status") && (
                        <TableCell className="align-top">
                          <StatusCell
                            u={u}
                            chainTotal={hasChain ? chainTotal : undefined}
                            chainIndex={
                              hasChain ? chainIndex || chainTotal : undefined
                            }
                          />
                          {hasChain && !chainExpanded ? (
                            <button
                              type="button"
                              className="mt-1 text-[11px] text-violet-700 hover:underline dark:text-violet-300"
                              onClick={() =>
                                setChainOpen((prev) => ({ ...prev, [rid]: true }))
                              }
                            >
                              查看全部 {chainTotal} 次尝试
                            </button>
                          ) : null}
                        </TableCell>
                      )}

                      {show("key") && (
                        <TableCell className="text-sm">
                          <CellText
                            text={
                              u.gateway_key_name ||
                              (u.gateway_key_id ? `#${u.gateway_key_id}` : "—")
                            }
                            className="font-medium"
                          />
                        </TableCell>
                      )}

                      {show("account") && (
                        <TableCell className="text-sm">
                          {/* 两行：渠道/直连名 + 源密钥名（或「直连」） */}
                          {(() => {
                            const primary =
                              u.provider_name ||
                              u.channel_name ||
                              (u.gateway_provider_id
                                ? `直连 #${u.gateway_provider_id}`
                                : u.channel_id
                                  ? `#${u.channel_id}`
                                  : "—")
                            const isDirect = !!(
                              u.provider_name || u.gateway_provider_id
                            )
                            const secondary =
                              (u.source_api_key_name || "").trim() ||
                              (isDirect ? "直连" : "")
                            return (
                              <div className="flex flex-col gap-0.5 leading-tight">
                                <span className="whitespace-nowrap font-medium">
                                  {primary}
                                </span>
                                {secondary ? (
                                  <span className="whitespace-nowrap text-[11px] text-muted-foreground">
                                    {secondary}
                                  </span>
                                ) : null}
                              </div>
                            )
                          })()}
                        </TableCell>
                      )}

                      {show("model") && (
                        <TableCell className="text-xs">
                          {steps.length > 1 ? (
                            <div className="space-y-0.5">
                              {steps.map((step, i) => (
                                <div
                                  key={`${u.id}-m-${i}`}
                                  style={
                                    i > 0
                                      ? { paddingLeft: `${i * 0.75}rem` }
                                      : undefined
                                  }
                                >
                                  <CellText
                                    text={i > 0 ? `↳ ${step}` : step}
                                    className={cn(
                                      i === 0
                                        ? "font-medium text-foreground"
                                        : "text-muted-foreground",
                                    )}
                                  />
                                </div>
                              ))}
                            </div>
                          ) : (
                            <CellText text={steps[0]} className="font-medium" />
                          )}
                        </TableCell>
                      )}

                      {show("reasoning") && (
                        <TableCell className="text-sm">
                          <CellText
                            text={formatReasoningEffort(u.reasoning_effort)}
                          />
                        </TableCell>
                      )}

                      {show("endpoint") && (
                        <TableCell className="text-xs">
                          <div className="space-y-1">
                            <div className="flex items-baseline gap-1">
                              <span className="shrink-0 font-medium text-muted-foreground">
                                入站:
                              </span>
                              <CellText text={u.inbound_endpoint?.trim() || "—"} />
                            </div>
                            <div className="flex items-baseline gap-1">
                              <span className="shrink-0 font-medium text-muted-foreground">
                                上游:
                              </span>
                              <CellText text={u.upstream_endpoint?.trim() || "—"} />
                            </div>
                            {u.protocol_converted && (
                              <span className="inline-flex rounded px-1.5 py-0.5 text-[10px] font-medium bg-violet-100 text-violet-800 dark:bg-violet-900/40 dark:text-violet-200">
                                协议转换
                              </span>
                            )}
                          </div>
                        </TableCell>
                      )}

                      {show("group") && (
                        <TableCell className="text-xs">
                          {(() => {
                            const groupName = (u.gateway_group_name || "").trim()
                            const sg = formatSourceGroupDisplay(u.source_group_name)
                            const sourceTip = sg.tip
                              ? sg.tip
                              : sg.label
                                ? `源 ${sg.label}`
                                : undefined
                            const tagClass =
                              "inline-flex whitespace-nowrap rounded px-2 py-0.5 text-xs font-medium bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-200"

                            if (!groupName) {
                              return (
                                <span className="text-muted-foreground">—</span>
                              )
                            }

                            if (sourceTip) {
                              return (
                                <Tooltip>
                                  <TooltipTrigger asChild>
                                    <span
                                      className={cn(tagClass, "cursor-default")}
                                    >
                                      {groupName}
                                    </span>
                                  </TooltipTrigger>
                                  <TooltipContent
                                    side="top"
                                    className="space-y-0.5 text-xs"
                                  >
                                    <div className="font-medium">{groupName}</div>
                                    <div className="text-muted-foreground">
                                      {sourceTip}
                                    </div>
                                  </TooltipContent>
                                </Tooltip>
                              )
                            }

                            return (
                              <span className={tagClass}>{groupName}</span>
                            )
                          })()}
                        </TableCell>
                      )}

                      {show("type") && (
                        <TableCell>
                          <span
                            className={cn(
                              "inline-flex items-center rounded px-2 py-0.5 text-xs font-medium",
                              requestTypeClass(u),
                            )}
                          >
                            {requestTypeLabel(u)}
                          </span>
                        </TableCell>
                      )}

                      {show("billing") && (
                        <TableCell>
                          <span
                            className={cn(
                              "inline-flex items-center rounded px-2 py-0.5 text-xs font-medium",
                              billingModeClass(u.billing_mode),
                            )}
                          >
                            {billingModeLabel(u.billing_mode)}
                          </span>
                        </TableCell>
                      )}

                      {show("token") && (
                        <TableCell>
                          <TokenCell u={u} />
                        </TableCell>
                      )}

                      {show("cost") && (
                        <TableCell>
                          <CostCell u={u} />
                        </TableCell>
                      )}

                      {show("latency") && (
                        <TableCell>
                          <LatencyCell u={u} />
                        </TableCell>
                      )}

                      {show("userAgent") && (
                        <TableCell className="text-xs text-muted-foreground">
                          <CellText text={u.user_agent?.trim() || "—"} />
                        </TableCell>
                      )}

                      {show("ip") && (
                        <TableCell className="font-mono text-sm text-muted-foreground">
                          <CellText text={u.ip_address || "—"} />
                        </TableCell>
                      )}

                      {show("time") && (
                        <TableCell className="text-sm text-muted-foreground">
                          <CellText text={fullDateTime(u.created_at)} />
                        </TableCell>
                      )}
                    </TableRow>
                    {hasChain && chainExpanded ? (
                      <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={colSpan} className="bg-muted/15 p-3">
                          <ChainTimeline
                            rows={chain!}
                            errorOpen={errorOpen}
                            onToggleError={(id) =>
                              setErrorOpen((prev) => ({
                                ...prev,
                                [id]: !prev[id],
                              }))
                            }
                          />
                        </TableCell>
                      </TableRow>
                    ) : null}
                    {!hasChain && canDetail && errExpanded ? (
                      <TableRow className="hover:bg-transparent">
                        <TableCell colSpan={colSpan} className="bg-muted/20 p-3">
                          <ErrorDetailPanel u={u} />
                        </TableCell>
                      </TableRow>
                    ) : null}
                  </Fragment>
                )
              })}
              {mainRows.length === 0 && (
                <TableRow>
                  <TableCell
                    colSpan={colSpan}
                    className="h-24 text-center text-muted-foreground"
                  >
                    {emptyText}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </div>
      </div>
    </TooltipProvider>
  )
}
