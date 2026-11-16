import { useEffect, useRef, useState, type Dispatch, type SetStateAction } from "react"
import { Sparkles } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import type { GatewayRoute } from "@/lib/api-types"
import { comparisonExpiry, mergeComparisonRoutes, type ComparisonStrategy } from "@/lib/price-comparison"
import { ComparisonResult } from "@/components/monitor/price-comparison-dialog"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { emptyRoute } from "./gateway-utils"

export function ApplyComparisonStrategy({ disabled, setRoutes, loadGroups }: {
  disabled: boolean
  setRoutes: Dispatch<SetStateAction<Partial<GatewayRoute>[]>>
  loadGroups: (id: number) => void
}) {
  const [open, setOpen] = useState(false)
  const [busy, setBusy] = useState(false)
  const [items, setItems] = useState<ComparisonStrategy[]>([])
  const [error, setError] = useState("")
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => controller.current?.abort(), [])

  async function show() {
    setOpen(true); setBusy(true); setError(""); setItems([])
    try { setItems((await apiFetch<{ items: ComparisonStrategy[] }>("/price-comparison/strategies")).items) }
    catch (e) { setError(e instanceof Error ? e.message : "读取策略失败") }
    finally { setBusy(false) }
  }

  async function apply(id: string) {
    setBusy(true); setError("")
    const abort = new AbortController()
    controller.current = abort
    try {
      const strategy = await apiFetch<ComparisonStrategy>(`/price-comparison/strategies/${id}/apply`, { method: "POST", signal: abort.signal })
      if (abort.signal.aborted) return
      setRoutes((drafts) => mergeComparisonRoutes(drafts, strategy, emptyRoute))
      for (const channelID of new Set(strategy.items.map((item) => item.candidate.channel_id))) loadGroups(channelID)
      toast.success(`已合并 ${strategy.items.length} 个源分组并设置优先级，请保存路由`)
      setOpen(false)
    } catch (e) { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "应用失败") }
    finally { setBusy(false); controller.current = null }
  }

  return <>
    <Button size="sm" variant="outline" className="h-9" disabled={disabled || busy} onClick={() => void show()}><Sparkles className="size-3.5" />应用比价策略</Button>
    <Dialog open={open} onOpenChange={(value) => { setOpen(value); if (!value) controller.current?.abort() }}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-5xl">
        <DialogHeader><DialogTitle>应用比价策略</DialogTitle><DialogDescription>一键添加策略中的全部源分组并更新优先级，保留已有路由的设置、禁用状态和其他渠道。将替换本组原有比价优先级；保存路由后生效，新路由还需“确保上游密钥”。策略按指定规格比较价格，应用后影响本组路由顺序，不按请求分辨率自动切换策略。</DialogDescription></DialogHeader>
        {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
        {busy && <p className="text-sm text-muted-foreground">处理中…</p>}
        {!busy && !items.length && !error && <p className="py-6 text-center text-sm text-muted-foreground">暂无有效策略。请在首页搜索旁打开“智能比价”，分析并保存；策略 30 分钟后过期。</p>}
        {items.map((strategy) => <details key={strategy.id} className="rounded-md border p-3">
          <summary className="cursor-pointer text-sm font-medium">{strategy.search} · {strategy.specification} · {strategy.items.length} 个分组</summary>
          <div className="mt-3 space-y-3">
            <p className="text-xs text-muted-foreground">有效期至 {comparisonExpiry(strategy.expires_at)}（北京时间）</p>
            <ComparisonResult strategy={strategy} />
            <Button size="sm" disabled={busy || disabled || Date.parse(strategy.expires_at) <= Date.now()} onClick={() => void apply(strategy.id)}>一键添加全部并排序</Button>
          </div>
        </details>)}
      </DialogContent>
    </Dialog>
  </>
}
