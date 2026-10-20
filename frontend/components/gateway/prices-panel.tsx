import { useEffect, useState } from "react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table"
import { apiFetch } from "@/lib/api"
import type { ModelPriceOverride, ModelPriceRepricingJob } from "@/lib/api-types"
import { emptyPriceForm, emptyPriceTier, perTokenToMTok, priceTiersFromJSON, type PriceFormState, type PriceTierForm } from "./gateway-utils"

type PricesPanelProps = {
  busy: boolean
  prices: ModelPriceOverride[]
  priceForm: PriceFormState
  setPriceForm: (form: PriceFormState) => void
  onSavePrice: () => void
  onOpenDefaults: () => void
  onFillFromOverride: (p: ModelPriceOverride) => void
  onLoadPrices: () => Promise<void>
}

export function PricesPanel({
  busy,
  prices,
  priceForm,
  setPriceForm,
  onSavePrice,
  onOpenDefaults,
  onFillFromOverride,
  onLoadPrices,
}: PricesPanelProps) {
  const [syncing, setSyncing] = useState(false)
  const [job, setJob] = useState<ModelPriceRepricingJob | null>(null)
  const [query, setQuery] = useState("")
  const [page, setPage] = useState(1)
  const [progressUnavailable, setProgressUnavailable] = useState(false)
  useEffect(() => {
    const controller = new AbortController()
    let timer: number | undefined
    const load = async () => {
      let delay = 10000
      try {
        const result = await apiFetch<ModelPriceRepricingJob>("/gateway/prices/repricing", { signal: controller.signal })
        if (controller.signal.aborted) return
        setJob(result); setProgressUnavailable(false)
        if (result.status === "pending" || result.status === "running") delay = 3000
      } catch { if (!controller.signal.aborted) setProgressUnavailable(true) }
      if (!controller.signal.aborted) timer = window.setTimeout(() => void load(), delay)
    }
    void load()
    return () => { controller.abort(); if (timer !== undefined) window.clearTimeout(timer) }
  }, [])
  async function syncPrices() {
    setSyncing(true)
    try {
      const result = await apiFetch<{ updated: number; repricing: ModelPriceRepricingJob }>("/gateway/prices/sync", { method: "POST" })
      setJob(result.repricing)
      toast.success(`已同步 ${result.updated} 个模型，保留手动覆盖，历史成本、收入和利润后台重算中`)
      await onLoadPrices()
    } catch (error) { toast.error(error instanceof Error ? error.message : "价格同步失败") }
    finally { setSyncing(false) }
  }
  async function recalculate() {
    setSyncing(true)
    try {
      setJob(await apiFetch<ModelPriceRepricingJob>("/gateway/prices/repricing", { method: "POST" }))
      toast.success("已启动历史成本、收入和利润重算")
    } catch (error) { toast.error(error instanceof Error ? error.message : "启动失败") }
    finally { setSyncing(false) }
  }
  const visiblePrices = prices.filter((p) => p.model_name.toLowerCase().includes(query.trim().toLowerCase()))
  const pages = Math.max(1, Math.ceil(visiblePrices.length / 50))
  const currentPage = Math.min(page, pages)
  const pagePrices = visiblePrices.slice((currentPage - 1) * 50, currentPage * 50)
  const tierFields = [["input_mtok", "输入"], ["output_mtok", "输出"], ["cache_create_mtok", "5m 缓存写"], ["cache_create_1h_mtok", "1h 缓存写"], ["cache_read_mtok", "缓存读"]] as const
  function updateTier(index: number, patch: Partial<PriceTierForm>) {
    setPriceForm({ ...priceForm, context_tiers: priceForm.context_tiers.map((tier, i) => i === index ? { ...tier, ...patch } : tier) })
  }
  return (
    <div className="space-y-4">
<Card className="overflow-hidden border-border shadow-none">
  <CardContent className="space-y-4 p-4 sm:p-5">
    <p className="text-sm leading-6 text-muted-foreground">
      覆盖表优先于系统内置价。输入单位为 <strong>$/MTok</strong>
      （每百万 token 美元），保存时自动换算为 per-token 存储。同步读取 sub2api 官方仓库最新价目，不覆盖手动价格。保存、删除或同步后自动重算历史成本、收入和利润，包含过去为 0 的计价记录；不重扣 Key 余额、不改实际扣款凭据。
    </p>
          <div className="flex flex-wrap items-center gap-2">
            <Button disabled={busy || syncing} onClick={() => void syncPrices()}>{syncing ? "处理中…" : "一键同步 sub2api 最新价格"}</Button>
            <Button variant="outline" disabled={busy || syncing || job?.status === "pending" || job?.status === "running"} onClick={() => void recalculate()}>重算历史费用与利润</Button>
            <a className="text-xs text-primary underline" href="https://github.com/Wei-Shaw/sub2api/blob/main/backend/resources/model-pricing/model_prices_and_context_window.json" target="_blank" rel="noreferrer">查看价格源</a>
          </div>
          {progressUnavailable && <p className="text-xs text-destructive" role="status">重算进度暂不可用，正在重试；下方为上次读取结果。</p>}
          {job && job.status !== "idle" && <p className="rounded-md bg-muted px-3 py-2 text-xs" role="status">
            历史重算：{job.status === "completed" ? "完成" : job.status === "pending" ? "等待后台处理" : "进行中"} · 已更新 {job.updated_rows} 条 · 跳过 {job.skipped_rows} 条 · 游标 {job.last_id}/{job.upper_id}
            {job.error && <span className="ml-2 text-destructive">{job.error}</span>}
            {job.skipped_rows > 0 && <span className="ml-2 text-muted-foreground">跳过未知价格、非 token 计费或尚未终态记录；请求结束后可再次重算。</span>}
          </p>}
          <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3">
            <div className="space-y-1">
              <Label className="text-xs">模型名</Label>
              <Input
                placeholder="claude-sonnet-4"
                value={priceForm.model_name}
                onChange={(e) =>
                  setPriceForm({ ...priceForm, model_name: e.target.value })
                }
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Input $/MTok</Label>
              <Input
                type="number"
                step="0.01"
                value={priceForm.input_mtok}
                onChange={(e) =>
                  setPriceForm({ ...priceForm, input_mtok: e.target.value })
                }
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Output $/MTok</Label>
              <Input
                type="number"
                step="0.01"
                value={priceForm.output_mtok}
                onChange={(e) =>
                  setPriceForm({ ...priceForm, output_mtok: e.target.value })
                }
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">缓存写入（5m / 默认）$/MTok</Label>
              <Input
                type="number"
                step="0.01"
                value={priceForm.cache_create_mtok}
                onChange={(e) =>
                  setPriceForm({ ...priceForm, cache_create_mtok: e.target.value })
                }
              />
            </div>
            <div className="space-y-1">
              <Label className="text-xs">缓存写入（1h）$/MTok</Label>
              <Input type="number" min="0" step="0.01" placeholder="沿用默认写入价" value={priceForm.cache_create_1h_mtok} onChange={(e) => setPriceForm({ ...priceForm, cache_create_1h_mtok: e.target.value })} />
              <p className="text-[11px] text-muted-foreground">留空沿用默认写入价，0 表示免费；缓存读取不按保存时长区分。</p>
            </div>
            <div className="space-y-1">
              <Label className="text-xs">Cache read $/MTok</Label>
              <Input
                type="number"
                step="0.01"
                value={priceForm.cache_read_mtok}
                onChange={(e) =>
                  setPriceForm({ ...priceForm, cache_read_mtok: e.target.value })
                }
              />
            </div>
            <div className="flex items-end gap-2 pb-1">
              <div className="flex items-center gap-2">
                <Switch
                  checked={priceForm.enabled}
                  onCheckedChange={(v) => setPriceForm({ ...priceForm, enabled: v })}
                />
                <Label className="text-sm">启用覆盖</Label>
              </div>
            </div>
          </div>
          <div className="space-y-3 rounded-md border p-3">
            <div className="flex items-center justify-between gap-2"><Label>上下文分档价格（$/MTok）</Label><Button size="sm" variant="outline" disabled={priceForm.context_tiers.length >= 32} onClick={() => setPriceForm({ ...priceForm, context_tiers: [...priceForm.context_tiers, emptyPriceTier()] })}>添加档位</Button></div>
            <p className="text-xs text-muted-foreground">以新鲜输入 + 真实缓存读 + 缓存写的总量选档，应用于整次请求，而非只计算超出部分。空价格沿用上一档；0 表示该项免费。</p>
            {priceForm.context_tiers.map((tier, index) => <div key={index} className="grid gap-2 rounded border p-2 sm:grid-cols-3 lg:grid-cols-4">
              <div><Label className="text-xs">上下文阈值（token）</Label><Input type="number" min="1" step="1" value={tier.above_input_tokens} onChange={(e) => updateTier(index, { above_input_tokens: e.target.value })} /></div>
              {tierFields.map(([key, label]) => <div key={key}><Label className="text-xs">{label} $/MTok</Label><Input type="number" min="0" step="any" placeholder="沿用上一档" value={tier[key]} onChange={(e) => updateTier(index, { [key]: e.target.value })} /></div>)}
              <label className="flex items-center gap-2 text-xs"><Switch checked={tier.inclusive} onCheckedChange={(inclusive) => updateTier(index, { inclusive })} />达到阈值即用此档</label>
              <Button size="sm" variant="ghost" onClick={() => setPriceForm({ ...priceForm, context_tiers: priceForm.context_tiers.filter((_, i) => i !== index) })}>删除档位</Button>
            </div>)}
          </div>
          <div className="flex flex-wrap gap-2">
            <Button onClick={() => void onSavePrice()} disabled={busy}>
              保存价格覆盖
            </Button>
            <Button
              variant="outline"
              onClick={() => void onOpenDefaults()}
              disabled={busy}
            >
              查看系统默认价
            </Button>
            <Button
              variant="ghost"
              onClick={() => setPriceForm(emptyPriceForm())}
              disabled={busy}
            >
              清空表单
            </Button>
          </div>
          <Input value={query} onChange={(event) => { setQuery(event.target.value); setPage(1) }} placeholder="筛选模型价格…" aria-label="筛选模型价格" />
          <Table>
            <TableHeader>
              <TableRow>
                <TableHead>模型</TableHead>
                <TableHead>Input $/MTok</TableHead>
                <TableHead>Output $/MTok</TableHead>
                <TableHead>缓存写 5m / 1h · 读</TableHead>
                <TableHead>来源 / 上下文档位</TableHead>
                <TableHead>状态</TableHead>
                <TableHead />
              </TableRow>
            </TableHeader>
            <TableBody>
              {pagePrices.map((p) => (
                <TableRow
                  key={p.id}
                  className="cursor-pointer"
                  onClick={() => onFillFromOverride(p)}
                >
                  <TableCell>{p.model_name}</TableCell>
                  <TableCell className="text-xs">
                    {perTokenToMTok(p.input_price_per_token)}
                  </TableCell>
                  <TableCell className="text-xs">
                    {perTokenToMTok(p.output_price_per_token)}
                  </TableCell>
                  <TableCell className="text-xs">
                    {perTokenToMTok(p.cache_creation_price_per_token)} /{" "}
                    {perTokenToMTok(p.cache_creation_1h_price_per_token ?? p.cache_creation_price_per_token)} ·{" "}
                    {perTokenToMTok(p.cache_read_price_per_token)}
                  </TableCell>
                  <TableCell className="text-xs">{p.source === "sub2api" ? "sub2api 同步" : "手动覆盖"} · {priceTiersFromJSON(p.context_tiers_json).length} 档</TableCell>
                  <TableCell>
                    <Badge variant={p.enabled ? "default" : "secondary"}>
                      {p.enabled ? "启用" : "禁用"}
                    </Badge>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={async (e) => {
                        e.stopPropagation()
                        try {
                          await apiFetch(`/gateway/prices/${p.id}`, {
                            method: "DELETE",
                          })
                          toast.success("已删除覆盖，历史参考费用后台重算中")
                          await onLoadPrices()
                        } catch (err) {
                          toast.error(
                            err instanceof Error ? err.message : "删除失败",
                          )
                        }
                      }}
                    >
                      删除
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
              {visiblePrices.length === 0 && (
                <TableRow>
                  <TableCell colSpan={7} className="text-center text-muted-foreground">
                    {prices.length === 0 ? "暂无价格覆盖，计费使用系统内置默认价" : "没有匹配的模型"}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
          <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground"><span>共 {visiblePrices.length} 个模型 · 第 {currentPage}/{pages} 页</span><div className="flex gap-2"><Button size="sm" variant="outline" disabled={currentPage <= 1} onClick={() => setPage(currentPage - 1)}>上一页</Button><Button size="sm" variant="outline" disabled={currentPage >= pages} onClick={() => setPage(currentPage + 1)}>下一页</Button></div></div>
        </CardContent>
      </Card>

    </div>
  )
}
