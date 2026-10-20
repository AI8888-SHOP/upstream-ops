import { useEffect, useId, useRef, useState } from "react"
import { ChevronDown, RefreshCw } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Textarea } from "@/components/ui/textarea"
import { apiFetch } from "@/lib/api"
import type { GatewayProviderModelPolicy, GatewayRoute } from "@/lib/api-types"
import { cn } from "@/lib/utils"
import { routeSourceKind } from "./gateway-utils"
import {
  ModelMappingEditor,
  parseMappingJSON,
  serializeMappingJSON,
  type MappingRow,
} from "./model-mapping-editor"

function modelIDs(text: string): string[] {
  return Array.from(new Set(text.split(/\r?\n|,/).map((id) => id.trim()).filter(Boolean)))
}

function savedModelIDs(raw?: string): string[] {
  try {
    const values: unknown = JSON.parse(raw || "[]")
    return Array.isArray(values)
      ? Array.from(new Set(values.filter((v): v is string => typeof v === "string").map((v) => v.trim()).filter(Boolean)))
      : []
  } catch {
    return []
  }
}

// Source IDs take precedence over a display name that can be refreshed by the
// server. Keying the editor by this identity also discards stale model pulls.
export function routeModelSourceKey(route: Partial<GatewayRoute>): string {
  if (routeSourceKind(route) === "provider") return `provider:${route.gateway_provider_id ?? 0}`
  const group = (route.source_group_id ?? 0) > 0
    ? `id:${route.source_group_id}`
    : `name:${route.source_group_name?.trim() || ""}`
  return JSON.stringify(["monitor", route.source_channel_id ?? 0, group])
}

type RouteModelsSelectProps = {
  groupID: number
  route: Partial<GatewayRoute>
  disabled: boolean
  onChange: (policy: GatewayProviderModelPolicy, modelsJSON: string, modelMapping: string) => void
}

export function RouteModelsSelect({ groupID, route, disabled, onChange }: RouteModelsSelectProps) {
  const [open, setOpen] = useState(false)
  const [policy, setPolicy] = useState<GatewayProviderModelPolicy>("all")
  const [text, setText] = useState("")
  const [available, setAvailable] = useState<string[]>([])
  const [query, setQuery] = useState("")
  const [mappingRows, setMappingRows] = useState<MappingRow[]>([])
  const [loading, setLoading] = useState(false)
  const pullRef = useRef<AbortController | null>(null)
  const mappingDatalistID = `route-model-suggestions-${useId().replace(/:/g, "")}`

  useEffect(() => () => pullRef.current?.abort(), [])

  const selected = modelIDs(text)
  const candidates = Array.from(new Set([...available, ...selected]))
    .filter((model) => model.toLowerCase().includes(query.trim().toLowerCase()))
  const saved = savedModelIDs(route.allowed_models_json)
  const mappingCount = parseMappingJSON(route.model_mapping).length
  const summary = route.model_policy === "allowlist"
    ? `仅 ${saved.length} 个模型${mappingCount ? ` · 映射 ${mappingCount}` : ""}`
    : mappingCount ? `全部模型 · 映射 ${mappingCount}` : "全部模型"

  function changeOpen(value: boolean) {
    if (value) {
      setPolicy(route.model_policy || "all")
      setText(saved.join("\n"))
      setMappingRows(parseMappingJSON(route.model_mapping))
      setQuery("")
      setAvailable([])
    } else {
      pullRef.current?.abort()
      setLoading(false)
    }
    setOpen(value)
  }

  async function refreshModels() {
    if (!route.id) return
    pullRef.current?.abort()
    const controller = new AbortController()
    pullRef.current = controller
    setLoading(true)
    try {
      const preview = await apiFetch<{ route: GatewayRoute; available: string[] }>(
        `/gateway/groups/${groupID}/routes/${route.id}/models/preview`,
        { signal: controller.signal },
      )
      if (controller.signal.aborted) return
      if (routeModelSourceKey(preview.route) !== routeModelSourceKey(route)) {
        toast.error("来源或源分组已更改，请先保存路由并确保上游密钥，再拉取模型")
        return
      }
      setAvailable(preview.available ?? [])
      toast.success(`已获取 ${preview.available.length} 个模型，已选模型保持不变`)
    } catch (error) {
      if (!controller.signal.aborted) {
        toast.error(error instanceof Error ? error.message : "拉取路由模型失败")
      }
    } finally {
      if (!controller.signal.aborted) setLoading(false)
    }
  }

  function applyModels() {
    const sources = new Set<string>()
    for (const [index, row] of mappingRows.entries()) {
      const from = row.from.trim()
      const to = row.to.trim()
      if (!from && !to) continue
      if (!from || !to) {
        toast.error(`第 ${index + 1} 条模型映射需要同时填写客户端模型和上游模型`)
        return
      }
      if (sources.has(from)) {
        toast.error(`客户端模型 ${from} 重复，请为每个客户端模型只保留一条映射`)
        return
      }
      sources.add(from)
    }
    onChange(policy, JSON.stringify(selected), serializeMappingJSON(mappingRows))
    changeOpen(false)
  }

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogTrigger asChild>
        <Button type="button" variant="outline" className="w-40 justify-between" disabled={disabled} title={summary}>
          <span className="truncate">{summary}</span>
          <ChevronDown className="size-3.5 shrink-0 opacity-50" />
        </Button>
      </DialogTrigger>
      <DialogContent className="max-h-[85vh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>路由支持模型与映射</DialogTitle>
          <DialogDescription>
            支持范围和映射仅作用于当前路由。请求模型先应用本路由映射，再应用网关组映射；支持模型按最终上游模型 ID 匹配。
          </DialogDescription>
        </DialogHeader>
        <div className="space-y-4">
          <div className="flex items-center justify-between gap-2">
            <Select value={policy} onValueChange={(value) => setPolicy(value as GatewayProviderModelPolicy)}>
              <SelectTrigger className="w-40" aria-label="支持模型策略"><SelectValue /></SelectTrigger>
              <SelectContent>
                <SelectItem value="all">全部模型</SelectItem>
                <SelectItem value="allowlist">仅选中模型</SelectItem>
              </SelectContent>
            </Select>
            <Button type="button" variant="outline" size="sm" disabled={!route.id || loading} onClick={() => void refreshModels()}>
              <RefreshCw className={cn("size-3.5", loading && "animate-spin")} /> 拉取上游模型
            </Button>
          </div>
          <p className="text-xs text-muted-foreground">拉取使用已保存的路由配置；监控渠道需先确保上游密钥。也可直接填写模型 ID。</p>
          {policy === "allowlist" ? (
            <div className="space-y-3">
              <Input aria-label="搜索路由模型" placeholder="搜索已拉取或已选模型" value={query} onChange={(event) => setQuery(event.target.value)} />
              <div className="grid max-h-52 gap-1 overflow-y-auto rounded-md border p-2 sm:grid-cols-2">
                {candidates.map((model) => (
                  <label key={model} className="flex min-w-0 items-center gap-2 rounded p-1 text-xs hover:bg-muted">
                    <Checkbox checked={selected.includes(model)} onCheckedChange={(checked) => {
                      setText((previous) => {
                        const ids = modelIDs(previous)
                        return (checked === true ? Array.from(new Set([...ids, model])) : ids.filter((id) => id !== model)).join("\n")
                      })
                    }} />
                    <span className="truncate" title={model}>{model}</span>
                  </label>
                ))}
                {candidates.length === 0 ? <p className="col-span-full py-2 text-xs text-muted-foreground">暂无匹配模型，可拉取上游模型或在下方手动填写。</p> : null}
              </div>
              <Label htmlFor="route-supported-models">已选模型（{selected.length}）</Label>
              <Textarea id="route-supported-models" rows={4} value={text} onChange={(event) => setText(event.target.value)} placeholder="每行一个上游模型 ID，也支持逗号分隔" />
              <p className={cn("text-xs", selected.length === 0 ? "text-destructive" : "text-muted-foreground")}>
                {selected.length === 0 ? "当前未选择任何模型，保存后此路由不会参与有模型请求的调度。" : "区分大小写，不支持通配符；未选中的模型会直接跳过这条路由。"}
              </p>
            </div>
          ) : <p className="text-sm text-muted-foreground">不额外限制这条路由的模型；实际可用范围仍由上游决定。</p>}
          <div className="border-t pt-4">
            <ModelMappingEditor
              rows={mappingRows}
              onChange={setMappingRows}
              modelSuggestions={Array.from(new Set([...available, ...selected]))}
              datalistID={mappingDatalistID}
            />
            <p className="mt-2 text-xs text-muted-foreground">
              例如把客户端请求的 model-a 映射到此渠道的 upstream-model。留空则不添加渠道级映射。
            </p>
          </div>
        </div>
        <DialogFooter>
          <Button type="button" variant="outline" onClick={() => changeOpen(false)}>取消</Button>
          <Button type="button" disabled={disabled} onClick={applyModels}>应用到路由</Button>
        </DialogFooter>
        <p className="text-xs text-muted-foreground">应用后，点击渠道路由页面的“保存路由”生效。</p>
      </DialogContent>
    </Dialog>
  )
}
