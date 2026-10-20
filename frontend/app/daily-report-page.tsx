import { Fragment, useEffect, useMemo, useState } from "react"
import { BarChart3, ChevronDown, ChevronRight, Loader2, RefreshCw } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { apiFetch } from "@/lib/api"
import { formatTokens, money } from "@/lib/format"
import type { DailyReportDay, DailyReportGroup, DailyReportModel, DailyReportSource, GatewayDailyReport } from "@/lib/daily-report-types"

const REPORT_TIMEZONE = "Asia/Shanghai"

function dateInTimezone(date: Date, timezone: string) {
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: timezone, year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date)
  const part = (type: string) => parts.find((entry) => entry.type === type)?.value ?? ""
  return [part("year"), part("month"), part("day")].join("-")
}

function cacheRate(value: number | null) {
  return value == null ? "—" : value.toFixed(2) + "%"
}

function sourceKey(source: DailyReportSource) {
  return source.source_kind + ":" + source.source_id
}

function ModelLeader({ model, byRequests = false }: { model?: DailyReportModel | null; byRequests?: boolean }) {
  if (!model) return <span className="text-muted-foreground">—</span>
  return <div className="min-w-0"><div className="max-w-64 truncate text-xs font-medium" title={model.model}>{model.model}</div><div className="mt-0.5 text-[11px] tabular-nums text-muted-foreground">{byRequests ? model.requests.toLocaleString() + " 次" : money(model.cost, { precise: true })}</div></div>
}

function DailySummary({ title, day }: { title: string; day: DailyReportDay }) {
  const top = day.sources[0]
  const group = top?.groups[0]
  return <Card className="gap-0 py-4"><CardContent className="space-y-3 px-4">
    <div className="flex items-center justify-between"><h2 className="text-sm font-semibold">{title}</h2><span className="text-xs text-muted-foreground">{day.date}</span></div>
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
      <div><div className="text-xs text-muted-foreground">上游参考消费</div><div className="mt-1 text-xl font-semibold tabular-nums">{money(day.cost, { precise: true })}</div></div>
      <div><div className="text-xs text-muted-foreground">请求 / 成功交付</div><div className="mt-1 text-sm font-medium tabular-nums">{day.requests.toLocaleString()} / {day.successful_requests.toLocaleString()}</div><div className="text-[11px] text-muted-foreground">{day.attempts.toLocaleString()} 次上游尝试</div></div>
      <div><div className="text-xs text-muted-foreground">总 Token</div><div className="mt-1 text-sm font-medium tabular-nums">{formatTokens(day.tokens)}</div></div>
      <div><div className="text-xs text-muted-foreground">真实缓存命中率</div><div className="mt-1 text-sm font-medium tabular-nums">{cacheRate(day.cache_hit_rate)}</div></div>
    </div>
    <div className="rounded-md bg-muted/50 px-3 py-2 text-xs leading-5">
      {top ? <><span className="text-muted-foreground">消费最高渠道：</span><span className="font-medium">{top.channel_name}</span><span className="ml-2 font-medium tabular-nums">{money(top.cost, { precise: true })}</span><br /><span className="text-muted-foreground">该渠道最高消费分组：</span><span className="font-medium">{group?.source_group_name ?? "—"}</span>{group ? <span className="ml-2 tabular-nums">{money(group.cost, { precise: true })}</span> : null}<br /><span className="text-muted-foreground">该渠道最高消费模型：</span><span className="font-medium">{top.top_cost_model?.model ?? "—"}</span></> : <span className="text-muted-foreground">本日暂无使用记录</span>}
    </div>
  </CardContent></Card>
}

function GroupDetail({ group }: { group: DailyReportGroup }) {
  return <details className="rounded-md border bg-background">
    <summary className="cursor-pointer rounded-md px-3 py-2 text-xs hover:bg-muted/40">
      <span className="font-medium">{group.source_group_name}</span>
      {group.source_group_id != null ? <span className="ml-1 text-muted-foreground">#{group.source_group_id}</span> : null}
      <span className="ml-4 tabular-nums">{money(group.cost, { precise: true })}</span>
      <span className="ml-4 text-muted-foreground">{group.requests.toLocaleString()} 次请求</span>
      <span className="ml-4 text-muted-foreground">缓存 {cacheRate(group.cache_hit_rate)}</span>
      <span className="ml-4 text-muted-foreground">消费最多：{group.top_cost_model?.model ?? "—"}</span>
      <span className="ml-4 text-muted-foreground">请求最多：{group.top_request_model?.model ?? "—"}</span>
    </summary>
    <Table className="min-w-[670px] text-xs"><TableHeader><TableRow><TableHead>请求模型</TableHead><TableHead className="text-right">参考消费</TableHead><TableHead className="text-right">请求数</TableHead><TableHead className="text-right">成功交付</TableHead><TableHead className="text-right">上游尝试</TableHead><TableHead className="text-right">Token</TableHead><TableHead className="text-right">真实缓存命中率</TableHead></TableRow></TableHeader>
      <TableBody>{group.models.map((model) => <TableRow key={model.model}><TableCell className="font-medium">{model.model}</TableCell><TableCell className="text-right tabular-nums">{money(model.cost, { precise: true })}</TableCell><TableCell className="text-right tabular-nums">{model.requests.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{model.successful_requests.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{model.attempts.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{formatTokens(model.tokens)}</TableCell><TableCell className="text-right tabular-nums">{cacheRate(model.cache_hit_rate)}</TableCell></TableRow>)}</TableBody>
    </Table>
  </details>
}

export default function DailyReportPage() {
  const [timezone, setTimezone] = useState(REPORT_TIMEZONE)
  const [date, setDate] = useState(() => dateInTimezone(new Date(), REPORT_TIMEZONE))
  const [tab, setTab] = useState("today")
  const [sortBy, setSortBy] = useState("cost")
  const [search, setSearch] = useState("")
  const [refresh, setRefresh] = useState(0)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [report, setReport] = useState<GatewayDailyReport | null>(null)
  const [expanded, setExpanded] = useState<Set<string>>(() => new Set())
  const browserTimezone = useMemo(() => Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC", [])
  const timezones = useMemo(() => [...new Set([REPORT_TIMEZONE, "UTC", browserTimezone])], [browserTimezone])

  useEffect(() => {
    const controller = new AbortController()
    setError(null)
    setLoading(true)
    setReport(null)
    if (!date) { setError("请选择报表日期"); setLoading(false); return () => controller.abort() }
    const query = new URLSearchParams({ date, timezone })
    apiFetch<GatewayDailyReport>("/gateway/usage/daily-report?" + query.toString(), { signal: controller.signal })
      .then((value) => { if (!controller.signal.aborted) setReport(value) })
      .catch((err: unknown) => { if (!controller.signal.aborted) setError(err instanceof Error ? err.message : "加载日报失败") })
      .finally(() => { if (!controller.signal.aborted) setLoading(false) })
    return () => controller.abort()
  }, [date, timezone, refresh])

  const day = report ? (tab === "yesterday" ? report.yesterday : report.today) : null
  const sources = useMemo(() => {
    const needle = search.trim().toLowerCase()
    return (day?.sources ?? []).filter((source) => !needle || source.channel_name.toLowerCase().includes(needle) || source.groups.some((group) => group.source_group_name.toLowerCase().includes(needle) || group.models.some((model) => model.model.toLowerCase().includes(needle))))
      .slice().sort((a,b) => sortBy === "requests" ? b.requests-a.requests || b.cost-a.cost : b.cost-a.cost || b.requests-a.requests)
  }, [day, search, sortBy])
  const isCurrentDate = date === dateInTimezone(new Date(), timezone)
  const todayLabel = isCurrentDate ? "今日" : "所选日"
  const yesterdayLabel = isCurrentDate ? "昨日" : "前一日"
  const toggleSource = (key: string) => setExpanded((old) => { const next = new Set(old); if (next.has(key)) next.delete(key); else next.add(key); return next })

  return <section className="space-y-4">
    <header className="flex flex-wrap items-start justify-between gap-3">
      <div><h1 className="flex items-center gap-2 text-lg font-semibold"><BarChart3 className="size-5" />每日报表</h1><p className="mt-1 text-xs text-muted-foreground">按渠道、源分组、请求模型查看消费，快速确定需要补货的上游。</p></div>
      <div className="flex flex-wrap items-end gap-2">
        <div className="space-y-1"><Label htmlFor="report-date" className="text-xs">报表日期</Label><Input id="report-date" type="date" value={date} onChange={(event) => setDate(event.target.value)} className="h-8 w-40" /></div>
        <div className="space-y-1"><Label htmlFor="report-timezone" className="text-xs">自然日时区</Label><Select value={timezone} onValueChange={setTimezone}><SelectTrigger id="report-timezone" className="h-8 w-44"><SelectValue /></SelectTrigger><SelectContent>{timezones.map((zone) => <SelectItem key={zone} value={zone}>{zone}</SelectItem>)}</SelectContent></Select></div>
        <Button size="sm" variant="outline" onClick={() => setDate(dateInTimezone(new Date(), timezone))}>回到今天</Button>
        <Button size="sm" variant="outline" disabled={loading} onClick={() => setRefresh((n) => n+1)}><RefreshCw className={loading ? "size-3.5 animate-spin" : "size-3.5"} />刷新</Button>
      </div>
    </header>
    <div className="rounded-md border bg-muted/30 px-3 py-2 text-xs leading-5 text-muted-foreground">费用为使用日志的上游参考成本（USD），包含失败、重试及并发尝试；应用价格覆盖后随历史重算更新，不代表上游站点实际扣账，也不改动网关 Key 余额。请求数按 Request ID 去重，跨渠道/分组重试会分别出现在对应行，因此分项请求数不能直接相加。真实缓存命中率只统计成功 attempt 的真实缓存读取，不含虚拟缓存；“—”表示无有效输入样本。</div>
    {error ? <div role="alert" className="rounded-md border border-destructive/40 p-4 text-sm text-destructive">{error}</div> : null}
    {loading ? <div className="flex items-center justify-center gap-2 py-16 text-sm text-muted-foreground"><Loader2 className="size-4 animate-spin" />汇总两日使用记录...</div> : null}
    {report ? <>
      <div className="grid gap-3 xl:grid-cols-2"><DailySummary title={todayLabel} day={report.today} /><DailySummary title={yesterdayLabel} day={report.yesterday} /></div>
      <Card className="gap-0 overflow-hidden py-0">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b p-3">
          <Tabs value={tab} onValueChange={setTab}><TabsList><TabsTrigger value="today">{todayLabel}渠道排行</TabsTrigger><TabsTrigger value="yesterday">{yesterdayLabel}渠道排行</TabsTrigger></TabsList></Tabs>
          <div className="flex flex-wrap gap-2"><Input aria-label="搜索渠道分组或模型" placeholder="搜索渠道 / 源分组 / 模型" value={search} onChange={(event) => setSearch(event.target.value)} className="h-8 w-60" /><Select value={sortBy} onValueChange={setSortBy}><SelectTrigger aria-label="排行排序" className="h-8 w-36"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="cost">消费从高到低</SelectItem><SelectItem value="requests">请求从多到少</SelectItem></SelectContent></Select></div>
        </div>
        <Table className="min-w-[1100px] text-xs"><TableHeader><TableRow><TableHead className="w-72">渠道 / 展开源分组</TableHead><TableHead className="text-right">参考消费</TableHead><TableHead className="text-right">请求数</TableHead><TableHead className="text-right">成功交付</TableHead><TableHead className="text-right">上游尝试</TableHead><TableHead className="text-right">Token</TableHead><TableHead className="text-right">真实缓存命中率</TableHead><TableHead>消费最多的模型</TableHead><TableHead>请求最多的模型</TableHead></TableRow></TableHeader>
          <TableBody>{sources.length === 0 ? <TableRow><TableCell colSpan={9} className="py-12 text-center text-muted-foreground">{search ? "没有匹配的渠道、源分组或模型" : "本日暂无使用记录"}</TableCell></TableRow> : sources.map((source, index) => { const key = sourceKey(source); const open = expanded.has(key); return <Fragment key={key}>
            <TableRow><TableCell><button type="button" onClick={() => toggleSource(key)} aria-expanded={open} className="flex w-full items-center gap-2 text-left"><span className="w-5 shrink-0 text-muted-foreground">{index+1}</span>{open ? <ChevronDown className="size-3.5 shrink-0" /> : <ChevronRight className="size-3.5 shrink-0" />}<div className="min-w-0"><div className="max-w-56 truncate font-medium" title={source.channel_name}>{source.channel_name}</div><div className="mt-1 flex items-center gap-1.5"><Badge variant="outline" className="px-1 py-0 text-[10px]">{source.source_kind === "provider" ? "直连" : "监控渠道"}</Badge><span className="text-[11px] text-muted-foreground">#{source.source_id} · {source.groups.length} 个源分组</span></div></div></button></TableCell><TableCell className="text-right font-semibold tabular-nums">{money(source.cost, { precise: true })}</TableCell><TableCell className="text-right tabular-nums">{source.requests.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{source.successful_requests.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{source.attempts.toLocaleString()}</TableCell><TableCell className="text-right tabular-nums">{formatTokens(source.tokens)}</TableCell><TableCell className="text-right tabular-nums">{cacheRate(source.cache_hit_rate)}</TableCell><TableCell><ModelLeader model={source.top_cost_model} /></TableCell><TableCell><ModelLeader model={source.top_request_model} byRequests /></TableCell></TableRow>
            {open ? <TableRow className="hover:bg-transparent"><TableCell colSpan={9} className="bg-muted/25 p-3"><div className="space-y-2">{source.groups.map((group) => <GroupDetail key={group.source_group_id != null ? "id:"+group.source_group_id : "name:"+group.source_group_name} group={group} />)}</div></TableCell></TableRow> : null}
          </Fragment> })}</TableBody>
        </Table>
        <div className="border-t px-3 py-2 text-[11px] text-muted-foreground">{day?.date} · {report.timezone} · {sources.length} 个匹配渠道 · 生成于 {new Date(report.generated_at).toLocaleString("zh-CN", { timeZone: report.timezone })} · 点击刷新获取最新汇总</div>
      </Card>
    </> : null}
  </section>
}
