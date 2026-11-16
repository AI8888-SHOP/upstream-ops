import { useEffect, useRef, useState } from "react"
import { Loader2, Sparkles } from "lucide-react"
import { toast } from "sonner"
import { apiFetch } from "@/lib/api"
import { comparisonExpiry, type ComparisonConfig, type ComparisonStrategy } from "@/lib/price-comparison"
import { Button } from "@/components/ui/button"
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Textarea } from "@/components/ui/textarea"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"

export function ComparisonResult({ strategy }: { strategy: ComparisonStrategy }) {
  return <div className="space-y-3">
    <p className="text-sm font-medium">{strategy.specification} · {strategy.currency} / {strategy.unit}</p>
    <p className="whitespace-pre-wrap text-sm text-muted-foreground">{strategy.analysis}</p>
    <p className="text-xs text-muted-foreground">{strategy.items.length} 个分组 · 同价并列；价格待核实的排在最后。应用后优先级持续生效，30 分钟仅限制策略的应用窗口。</p>
    <div className="overflow-x-auto rounded-md border">
      <table className="w-full text-left text-xs">
        <thead className="bg-muted"><tr><th className="p-2">优先级</th><th className="p-2">渠道 / 源分组</th><th className="p-2">参考实际成本</th><th className="p-2">依据 / 换算</th></tr></thead>
        <tbody>{strategy.items.map((item) => <tr key={item.candidate.id} className="border-t align-top">
          <td className="p-2">{item.priority}</td>
          <td className="min-w-32 p-2"><p>{item.candidate.channel_name}</p><p className="text-muted-foreground">{item.candidate.group_name}</p></td>
          <td className="min-w-28 p-2">{item.cost == null ? "待核实" : `${Number(item.cost.toPrecision(8))} ${strategy.currency}/${strategy.unit}`}</td>
          <td className="min-w-56 max-w-lg space-y-1 break-words p-2">
            {item.quote.evidence && <p>原文：{item.quote.evidence}</p>}
            {item.calculation && <p>{item.calculation}</p>}
            {item.warning && <p className="text-amber-600 dark:text-amber-400">{item.warning}</p>}
            <p className="text-muted-foreground">{item.quote.reason}</p>
            <details><summary className="cursor-pointer text-muted-foreground">完整分组简介</summary><p className="whitespace-pre-wrap">{item.candidate.description || "无简介"}</p></details>
          </td>
        </tr>)}</tbody>
      </table>
    </div>
  </div>
}

export function PriceComparisonDialog({ search }: { search: string }) {
  const [open, setOpen] = useState(false)
  const [term, setTerm] = useState("")
  const [specification, setSpecification] = useState("")
  const [currency, setCurrency] = useState("CNY")
  const [unit, setUnit] = useState("张")
  const [requirements, setRequirements] = useState("")
  const [config, setConfig] = useState<ComparisonConfig | null>(null)
  const [apiKey, setAPIKey] = useState("")
  const [busy, setBusy] = useState(false)
  const [configBusy, setConfigBusy] = useState(false)
  const [error, setError] = useState("")
  const [strategy, setStrategy] = useState<ComparisonStrategy | null>(null)
  const controller = useRef<AbortController | null>(null)
  useEffect(() => () => controller.current?.abort(), [])

  async function show() {
    setOpen(true)
    setTerm(search.trim())
    setError("")
    setConfigBusy(true)
    try { setConfig(await apiFetch<ComparisonConfig>("/price-comparison/config")) }
    catch (e) { setError(e instanceof Error ? e.message : "读取配置失败") }
    finally { setConfigBusy(false) }
  }

  async function saveConfig() {
    if (!config) return
    setConfigBusy(true)
    try {
      setConfig(await apiFetch<ComparisonConfig>("/price-comparison/config", { method: "PUT", body: JSON.stringify({ endpoint: config.endpoint, model: config.model, api_key: apiKey }) }))
      setAPIKey("")
      toast.success("比价模型配置已保存")
    } catch (e) { toast.error(e instanceof Error ? e.message : "保存配置失败") }
    finally { setConfigBusy(false) }
  }

  async function analyze() {
    setBusy(true)
    setError("")
    setStrategy(null)
    const abort = new AbortController()
    controller.current = abort
    try {
      setStrategy(await apiFetch<ComparisonStrategy>("/price-comparison/analyze", {
        method: "POST", signal: abort.signal,
        body: JSON.stringify({ search: term.trim(), specification, currency, unit, requirements }),
      }))
    } catch (e) { if (!abort.signal.aborted) setError(e instanceof Error ? e.message : "分析失败") }
    finally { setBusy(false); controller.current = null }
  }

  async function save() {
    if (!strategy) return
    setBusy(true)
    try {
      setStrategy(await apiFetch<ComparisonStrategy>(`/price-comparison/strategies/${strategy.id}/save`, { method: "POST" }))
      toast.success("策略已保存，30 分钟内可在网关组的渠道路由中应用")
    } catch (e) { setError(e instanceof Error ? e.message : "保存失败") }
    finally { setBusy(false) }
  }

  return <>
    <Button variant="outline" size="sm" className="h-9 shrink-0" onClick={() => void show()}><Sparkles className="size-4" />智能比价</Button>
    <Dialog open={open} onOpenChange={(value) => { setOpen(value); if (!value) controller.current?.abort() }}>
      <DialogContent className="max-h-[90vh] overflow-y-auto sm:max-w-5xl">
        <DialogHeader><DialogTitle>智能比价</DialogTitle><DialogDescription>读取所有匹配分组的简介，按同规格实际成本生成策略。渠道名称、分组简介及倍率将发给你配置的大模型，不包含渠道账号和密钥。请核对价格依据后保存。</DialogDescription></DialogHeader>
        <details className="rounded-md border p-3" open={config != null && !config.has_api_key}>
          <summary className="cursor-pointer text-sm font-medium">比价大模型配置 {configBusy ? "（读取或保存中…）" : ""}</summary>
          {config && <div className="mt-3 grid gap-3 sm:grid-cols-2">
            <div className="space-y-1 sm:col-span-2"><Label htmlFor="comparison-endpoint">完整 Chat Completions 接口地址</Label><Input id="comparison-endpoint" placeholder="https://你的接口/v1/chat/completions" value={config.endpoint} onChange={(e) => setConfig({ ...config, endpoint: e.target.value })} /></div>
            <div className="space-y-1"><Label htmlFor="comparison-model">模型</Label><Input id="comparison-model" value={config.model} onChange={(e) => setConfig({ ...config, model: e.target.value })} /></div>
            <div className="space-y-1"><Label htmlFor="comparison-key">API Key</Label><Input id="comparison-key" type="password" autoComplete="new-password" placeholder={config.has_api_key ? "已保存，留空保持；更换地址需重填" : "填写 API Key"} value={apiKey} onChange={(e) => setAPIKey(e.target.value)} /></div>
            <Button className="w-fit" size="sm" variant="secondary" disabled={busy || configBusy} onClick={() => void saveConfig()}>保存模型配置</Button>
          </div>}
        </details>
        <fieldset disabled={busy} className="grid gap-3 sm:grid-cols-2">
          <div className="space-y-1"><Label htmlFor="comparison-search">搜索分组名称或简介</Label><Input id="comparison-search" value={term} onChange={(e) => setTerm(e.target.value)} placeholder="例如：图片模型名称" maxLength={256} /></div>
          <div className="space-y-1"><Label htmlFor="comparison-spec">完整规格（模型、分辨率、时长等）</Label><Input id="comparison-spec" value={specification} onChange={(e) => setSpecification(e.target.value)} placeholder="例如：指定图片模型，2K，单张" maxLength={1024} /></div>
          <div className="space-y-1"><Label>比较货币</Label><Select value={currency} onValueChange={setCurrency}><SelectTrigger aria-label="比较货币"><SelectValue /></SelectTrigger><SelectContent><SelectItem value="CNY">CNY 人民币</SelectItem><SelectItem value="USD">USD 美元</SelectItem></SelectContent></Select></div>
          <div className="space-y-1"><Label htmlFor="comparison-unit">每份计价单位</Label><Input id="comparison-unit" value={unit} onChange={(e) => setUnit(e.target.value)} placeholder="张 / 秒 / 段 / 百万 token" maxLength={64} /></div>
          <div className="space-y-1 sm:col-span-2"><Label htmlFor="comparison-requirements">补充分析要求（可选）</Label><Textarea id="comparison-requirements" value={requirements} onChange={(e) => setRequirements(e.target.value)} placeholder="例如关注速度与稳定性；只采用简介提供的证据，价格仍优先" maxLength={2048} /></div>
        </fieldset>
        <p className="text-xs text-muted-foreground">余额扣费需配置渠道充值换算值；最终实付价不重复乘倍率。不同币种不自动换汇。一次最多分析 100 个分组，超出时请缩小搜索范围。</p>
        <div className="flex flex-wrap gap-2">
          <Button disabled={busy || configBusy || !config?.has_api_key || !term.trim() || !specification.trim() || !unit.trim()} onClick={() => void analyze()}>{busy && !strategy ? <Loader2 className="size-4 animate-spin" /> : <Sparkles className="size-4" />}分析并生成策略</Button>
          {busy && !strategy && <Button variant="outline" onClick={() => controller.current?.abort()}>取消分析</Button>}
          {strategy && <Button variant="secondary" disabled={busy || !!strategy.saved_at} onClick={() => void save()}>{strategy.saved_at ? "已保存策略" : "保存策略（有效 30 分钟）"}</Button>}
        </div>
        {error && <p role="alert" className="text-sm text-destructive">{error}</p>}
        {strategy && <>
          <p className="text-xs text-muted-foreground">{strategy.saved_at ? "应用有效期至" : "未保存，草稿有效期至"} {comparisonExpiry(strategy.expires_at)}（北京时间）。已生成结果对应下方规格，修改输入后请重新分析。</p>
          <ComparisonResult strategy={strategy} />
        </>}
      </DialogContent>
    </Dialog>
  </>
}
