import { useCallback, useEffect, useMemo, useState } from "react"
import {
  Activity,
  CheckCircle2,
  CircleAlert,
  Clock3,
  Copy,
  Filter,
  Loader2,
  RefreshCw,
  Search,
  XCircle,
} from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  fetchGatewayApiCallStats,
  fetchGatewayApiCalls,
} from "@/lib/api"
import type {
  GatewayApiCall,
  GatewayApiCallFilters,
  GatewayApiCallPage,
  GatewayApiCallStats,
  GatewayApiCallStatus,
} from "@/lib/api-types"
import { cn } from "@/lib/utils"

const PAGE_SIZE = 50
const ALL = "all"

const purposeLabels: Record<string, string> = {
  gateway: "业务转发",
  candy_check: "验糖",
  model_probe: "模型恢复",
  admin_model_test: "管理测试",
  optimizer_explain: "优化器解释",
  channel_api: "渠道管理",
  sync_api: "同步",
}

const statusLabels: Record<GatewayApiCallStatus, string> = {
  pending: "进行中",
  success: "成功",
  error: "失败",
  incomplete: "未完成",
}

const statusClassNames: Record<GatewayApiCallStatus, string> = {
  pending: "border-amber-200 bg-amber-50 text-amber-800 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-300",
  success: "border-emerald-200 bg-emerald-50 text-emerald-700 dark:border-emerald-900/50 dark:bg-emerald-950/30 dark:text-emerald-300",
  error: "border-red-200 bg-red-50 text-red-700 dark:border-red-900/50 dark:bg-red-950/30 dark:text-red-300",
  incomplete: "border-slate-200 bg-slate-50 text-slate-700 dark:border-slate-800 dark:bg-slate-900/40 dark:text-slate-300",
}

type FilterForm = {
  since: string
  until: string
  purpose: string
  model: string
  sourceApiKeyName: string
  channelID: string
  providerID: string
  sourceGroupID: string
  status: string
}

const emptyFilters: FilterForm = {
  since: "",
  until: "",
  purpose: ALL,
  model: "",
  sourceApiKeyName: "",
  channelID: "",
  providerID: "",
  sourceGroupID: "",
  status: ALL,
}

function unixSeconds(raw: string): number | undefined {
  if (!raw.trim()) return undefined
  const date = new Date(raw)
  const time = date.getTime()
  return Number.isFinite(time) ? Math.floor(time / 1000) : undefined
}

export function gatewayApiCallFilters(form: FilterForm, page: number): GatewayApiCallFilters {
  const filters: GatewayApiCallFilters = {
    purpose: form.purpose === ALL ? undefined : form.purpose,
    model: form.model.trim() || undefined,
    source_api_key_name: form.sourceApiKeyName.trim() || undefined,
    channel_id: form.channelID.trim() || undefined,
    provider_id: form.providerID.trim() || undefined,
    source_group_id: form.sourceGroupID.trim() || undefined,
    status: form.status === ALL ? undefined : form.status,
    since: unixSeconds(form.since),
    until: unixSeconds(form.until),
    page,
    page_size: PAGE_SIZE,
  }
  return filters
}

function timestamp(value: string | number | null | undefined): string {
  if (value == null || value === "") return "—"
  const numeric = typeof value === "number" ? value : /^\d+(?:\.\d+)?$/.test(value) ? Number(value) : NaN
  const milliseconds = Number.isFinite(numeric) ? (numeric < 10_000_000_000 ? numeric * 1000 : numeric) : Date.parse(String(value))
  if (!Number.isFinite(milliseconds)) return String(value)
  return new Date(milliseconds).toLocaleString("zh-CN")
}

function numberValue(value: unknown): number | undefined {
  return typeof value === "number" && Number.isFinite(value) ? value : undefined
}

function statValue(stats: GatewayApiCallStats | null, keys: string[]): number {
  for (const key of keys) {
    const value = numberValue(stats?.[key])
    if (value != null) return value
  }
  return 0
}

function costLabel(value: number | null | undefined, hasCalls: boolean): string {
  if (!hasCalls) return "暂无"
  return value == null || !Number.isFinite(value) ? "未知" : `$${value.toFixed(6)}`
}

function purposeLabel(purpose: string): string {
  return purposeLabels[purpose] ?? (purpose || "未知用途")
}

function StatusBadge({ status }: { status: GatewayApiCallStatus }) {
  const Icon = status === "success" ? CheckCircle2 : status === "error" ? XCircle : status === "pending" ? Clock3 : CircleAlert
  return (
    <Badge variant="outline" className={cn("gap-1 px-1.5 text-[10px]", statusClassNames[status] ?? statusClassNames.incomplete)}>
      <Icon className="size-3" />
      {statusLabels[status] ?? status}
    </Badge>
  )
}

function SummaryCard({ label, value, tone }: { label: string; value: string; tone?: string }) {
  return (
    <Card className="border-border shadow-none">
      <CardContent className="space-y-1 p-3">
        <div className="text-[11px] text-muted-foreground">{label}</div>
        <div className={cn("text-lg font-semibold tabular-nums", tone)}>{value}</div>
      </CardContent>
    </Card>
  )
}

function copyRequestID(requestID: string) {
  if (!requestID || typeof navigator === "undefined" || !navigator.clipboard) return
  void navigator.clipboard.writeText(requestID).then(() => toast.success("请求 ID 已复制"))
}

function CallRow({ item }: { item: GatewayApiCall }) {
  const status = item.status as GatewayApiCallStatus
  return (
    <details className="group border-b border-border/60 last:border-0">
      <summary className="grid cursor-pointer list-none gap-2 px-3 py-3 text-xs hover:bg-muted/30 sm:grid-cols-[minmax(190px,1.45fr)_minmax(130px,1fr)_minmax(160px,1.2fr)_90px_100px] sm:items-center">
        <div className="min-w-0 space-y-1">
          <div className="flex flex-wrap items-center gap-1.5">
            <StatusBadge status={status} />
            <Badge variant="secondary" className="px-1.5 text-[10px]">{purposeLabel(item.purpose)}</Badge>
            {item.stream ? <Badge variant="outline" className="px-1.5 text-[10px]">stream</Badge> : null}
          </div>
          <div className="truncate font-mono text-[11px] text-muted-foreground" title={item.request_id}>
            {item.request_id || "无 request_id"}
          </div>
        </div>
        <div className="min-w-0">
          <div className="truncate font-medium" title={`${item.method} ${item.endpoint}`}>
            <span className="mr-1 font-mono text-[11px] text-muted-foreground">{item.method}</span>{item.endpoint || "—"}
          </div>
          <div className="truncate text-[11px] text-muted-foreground" title={item.host}>{item.host || "—"}</div>
        </div>
        <div className="min-w-0 space-y-0.5">
          <div className="truncate" title={item.model || undefined}>{item.model || "未标记模型"}</div>
          <div className="truncate text-[11px] text-muted-foreground">
            {item.source_api_key_name || "未知上游 Key"}{item.channel_id ? ` · channel#${item.channel_id}` : item.gateway_provider_id ? ` · provider#${item.gateway_provider_id}` : ""}
          </div>
        </div>
        <div className="text-left tabular-nums text-muted-foreground sm:text-right">
          {item.status_code ?? "—"}<div className="text-[11px]">{item.duration_ms > 0 ? `${item.duration_ms} ms` : "—"}</div>
        </div>
        <div className="text-left tabular-nums sm:text-right">{costLabel(item.estimated_cost, true)}</div>
      </summary>
      <div className="grid gap-x-6 gap-y-2 border-t border-border/50 bg-muted/15 px-3 py-3 text-xs text-muted-foreground sm:grid-cols-2 lg:grid-cols-4">
        <div><span>开始：</span>{timestamp(item.started_at)}</div>
        <div><span>完成：</span>{timestamp(item.finished_at)}</div>
        <div><span>首 token：</span>{item.first_token_ms == null ? "—" : `${item.first_token_ms} ms`}</div>
        <div><span>网关组：</span>{item.gateway_group_id == null ? "—" : `#${item.gateway_group_id}`}</div>
        <div><span>路由：</span>{item.route_id == null ? "—" : `#${item.route_id}`}</div>
        <div className="sm:col-span-2"><span>网关逻辑请求 ID：</span><span className="break-all font-mono">{item.gateway_request_id || "—"}</span></div>
        <div><span>源分组：</span>{item.source_group_id == null ? "—" : `#${item.source_group_id}`}</div>
        <div><span>上游请求 ID：</span><span className="break-all font-mono">{item.upstream_request_id || "—"}</span></div>
        <div><span>来源 Key ID：</span>{item.source_api_key_id == null ? "—" : `#${item.source_api_key_id}`}</div>
        <div className="lg:col-span-2"><span>Token：</span>输入 {item.input_tokens.toLocaleString()} · 输出 {item.output_tokens.toLocaleString()} · 缓存读取 {item.cache_read_tokens.toLocaleString()} · 缓存创建 {item.cache_creation_tokens.toLocaleString()} · {item.usage_known ? "usage 已知" : "usage 未知"}</div>
        {item.error_type || item.error_message ? <div className="break-words text-destructive lg:col-span-2">{item.error_type ? `${item.error_type}：` : ""}{item.error_message || "请求失败"}</div> : null}
        <div className="flex items-center gap-1 lg:col-span-4"><span>真实请求 ID：</span><code className="break-all rounded bg-background px-1 py-0.5 font-mono">{item.request_id || "—"}</code>{item.request_id ? <Button type="button" variant="ghost" size="icon" className="size-6" title="复制请求 ID" onClick={() => copyRequestID(item.request_id)}><Copy className="size-3" /></Button> : null}</div>
      </div>
    </details>
  )
}

export function ApiCallsPanel() {
  const [form, setForm] = useState<FilterForm>(emptyFilters)
  const [applied, setApplied] = useState<FilterForm>(emptyFilters)
  const [page, setPage] = useState<GatewayApiCallPage | null>(null)
  const [stats, setStats] = useState<GatewayApiCallStats | null>(null)
  const [pageNumber, setPageNumber] = useState(1)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState("")

  const filters = useMemo(() => gatewayApiCallFilters(applied, pageNumber), [applied, pageNumber])
  const load = useCallback(async () => {
    setLoading(true)
    setError("")
    try {
      const statsFilters = { ...filters, page: undefined, page_size: undefined }
      const [nextPage, nextStats] = await Promise.all([
        fetchGatewayApiCalls(filters),
        fetchGatewayApiCallStats(statsFilters),
      ])
      setPage(nextPage)
      setStats(nextStats)
    } catch (e) {
      setError(e instanceof Error ? e.message : "加载 API 调用记录失败")
    } finally {
      setLoading(false)
    }
  }, [filters])

  useEffect(() => { void load() }, [load])

  function update<K extends keyof FilterForm>(key: K, value: FilterForm[K]) {
    setForm((current) => ({ ...current, [key]: value }))
  }

  function applyFilters() {
    setPageNumber(1)
    setApplied(form)
  }

  function clearFilters() {
    setForm(emptyFilters)
    setApplied(emptyFilters)
    setPageNumber(1)
  }

  const totalValue = statValue(stats, ["total_real_calls", "total_calls", "total"])
  const hasCalls = totalValue > 0
  const total = hasCalls ? totalValue.toLocaleString() : stats ? "暂无" : "—"
  const inProgress = stats ? statValue(stats, ["in_progress", "pending", "pending_calls"]).toLocaleString() : "—"
  const failed = stats ? statValue(stats, ["failed", "error_count", "error_calls"]).toLocaleString() : "—"
  const unknownUsage = stats ? statValue(stats, ["unknown_usage", "unknown_usage_count"]).toLocaleString() : "—"
  const knownCost = stats?.known_estimated_cost ?? stats?.total_estimated_cost ?? stats?.estimated_cost
  const unknownCost = stats ? statValue(stats, ["unknown_cost_count", "unknown_cost_calls", "unknown_costs"]).toLocaleString() : "—"
  const totalPages = Math.max(1, Math.ceil((page?.total ?? 0) / (page?.page_size || PAGE_SIZE)))

  return (
    <div className="space-y-4">
      <Card className="border-border shadow-none">
        <CardContent className="space-y-4 p-4 sm:p-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <div className="flex items-center gap-2"><Activity className="size-4 text-primary" /><h2 className="text-sm font-semibold">API调用记录</h2></div>
              <p className="max-w-3xl text-xs leading-5 text-muted-foreground">记录程序发起的所有真实出站调用：业务、验糖、模型恢复、管理测试、优化器解释、渠道管理与同步。此处与客户计费使用量隔离；不保存敏感 header 或正文。</p>
            </div>
            <Button type="button" variant="outline" size="sm" onClick={() => void load()} disabled={loading} title="刷新调用记录"><RefreshCw className={cn("size-3.5", loading && "animate-spin")} />刷新</Button>
          </div>
          <div className="grid gap-3 md:grid-cols-4 lg:grid-cols-8">
            <div className="space-y-1 lg:col-span-2"><Label htmlFor="api-calls-since">开始时间</Label><Input id="api-calls-since" type="datetime-local" value={form.since} onChange={(e) => update("since", e.target.value)} /></div>
            <div className="space-y-1 lg:col-span-2"><Label htmlFor="api-calls-until">结束时间</Label><Input id="api-calls-until" type="datetime-local" value={form.until} onChange={(e) => update("until", e.target.value)} /></div>
            <div className="space-y-1"><Label>用途</Label><Select value={form.purpose} onValueChange={(value) => update("purpose", value)}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value={ALL}>全部用途</SelectItem>{Object.entries(purposeLabels).map(([value, label]) => <SelectItem key={value} value={value}>{label}</SelectItem>)}</SelectContent></Select></div>
            <div className="space-y-1"><Label>状态</Label><Select value={form.status} onValueChange={(value) => update("status", value)}><SelectTrigger><SelectValue /></SelectTrigger><SelectContent><SelectItem value={ALL}>全部状态</SelectItem><SelectItem value="pending">进行中</SelectItem><SelectItem value="success">成功</SelectItem><SelectItem value="error">失败</SelectItem><SelectItem value="incomplete">未完成</SelectItem></SelectContent></Select></div>
            <div className="space-y-1 lg:col-span-2"><Label htmlFor="api-calls-model">模型</Label><Input id="api-calls-model" value={form.model} placeholder="手填模型 ID" onChange={(e) => update("model", e.target.value)} /></div>
            <div className="space-y-1 lg:col-span-2"><Label htmlFor="api-calls-key">上游 Key 名称</Label><Input id="api-calls-key" value={form.sourceApiKeyName} placeholder="手填 Key 名称" onChange={(e) => update("sourceApiKeyName", e.target.value)} /></div>
            <div className="space-y-1"><Label htmlFor="api-calls-channel">渠道 ID</Label><Input id="api-calls-channel" inputMode="numeric" value={form.channelID} placeholder="channel_id" onChange={(e) => update("channelID", e.target.value)} /></div>
            <div className="space-y-1"><Label htmlFor="api-calls-provider">Provider ID</Label><Input id="api-calls-provider" inputMode="numeric" value={form.providerID} placeholder="provider_id" onChange={(e) => update("providerID", e.target.value)} /></div>
            <div className="space-y-1"><Label htmlFor="api-calls-source-group">源分组 ID</Label><Input id="api-calls-source-group" inputMode="numeric" value={form.sourceGroupID} placeholder="source_group_id" onChange={(e) => update("sourceGroupID", e.target.value)} /></div>
            <div className="flex items-end gap-2 lg:col-span-2"><Button type="button" size="sm" onClick={applyFilters}><Filter className="size-3.5" />应用筛选</Button><Button type="button" variant="ghost" size="sm" onClick={clearFilters}><Search className="size-3.5" />清空</Button></div>
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-6">
        <SummaryCard label="总真实调用" value={total} />
        <SummaryCard label="进行中" value={hasCalls ? inProgress : stats ? "暂无" : "—"} tone="text-amber-600 dark:text-amber-400" />
        <SummaryCard label="失败" value={hasCalls ? failed : stats ? "暂无" : "—"} tone="text-red-600 dark:text-red-400" />
        <SummaryCard label="未知 usage" value={hasCalls ? unknownUsage : stats ? "暂无" : "—"} tone="text-slate-600 dark:text-slate-300" />
        <SummaryCard label="已知估算成本" value={costLabel(knownCost, hasCalls)} tone="text-emerald-700 dark:text-emerald-400" />
        <SummaryCard label="未知成本数量" value={hasCalls ? unknownCost : stats ? "暂无" : "—"} tone="text-slate-600 dark:text-slate-300" />
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>未知成本 {hasCalls ? `${unknownCost} 条` : stats ? "暂无记录" : "—"}</span>
        <span>估算成本是前端展示的估算值，不是上游已核账费用。</span>
      </div>

      <Card className="overflow-hidden border-border shadow-none">
        <CardContent className="p-0">
          <div className="hidden border-b border-border/60 bg-muted/30 px-3 py-2 text-[11px] text-muted-foreground sm:grid sm:grid-cols-[minmax(190px,1.45fr)_minmax(130px,1fr)_minmax(160px,1.2fr)_90px_100px] sm:gap-2"><span>状态 / 请求 ID</span><span>出站目标</span><span>模型 / 上游凭据</span><span className="text-right">响应 / 耗时</span><span className="text-right">估算成本</span></div>
          {loading && !page ? <div className="flex items-center justify-center gap-2 py-14 text-sm text-muted-foreground"><Loader2 className="size-4 animate-spin" />加载调用记录…</div> : error ? <div className="space-y-2 py-14 text-center text-sm text-destructive"><div>{error}</div><Button type="button" variant="outline" size="sm" onClick={() => void load()}>重试</Button></div> : page?.items?.length ? page.items.map((item) => <CallRow key={item.id} item={item} />) : <div className="py-14 text-center text-sm text-muted-foreground">当前筛选范围暂无程序出站调用</div>}
        </CardContent>
      </Card>

      <div className="flex flex-wrap items-center justify-between gap-2 text-xs text-muted-foreground">
        <span>共 {page?.total?.toLocaleString() ?? "0"} 条 · 第 {page?.page ?? pageNumber} / {totalPages} 页</span>
        <div className="flex items-center gap-2"><Button type="button" variant="outline" size="sm" disabled={loading || pageNumber <= 1} onClick={() => setPageNumber((current) => Math.max(1, current - 1))}>上一页</Button><Button type="button" variant="outline" size="sm" disabled={loading || pageNumber >= totalPages} onClick={() => setPageNumber((current) => Math.min(totalPages, current + 1))}>下一页</Button></div>
      </div>
    </div>
  )
}
