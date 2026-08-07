import { useCallback, useEffect, useRef, useState } from "react"
import {
  Download,
  Pencil,
  Plus,
  RefreshCw,
  ShieldCheck,
  Trash2,
  Upload,
} from "lucide-react"
import { toast } from "sonner"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
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
import { Switch } from "@/components/ui/switch"
import { Textarea } from "@/components/ui/textarea"
import { useConfirm } from "@/components/ui/confirm-dialog"
import { apiFetch } from "@/lib/api"
import type {
  GatewayResponseRule,
  GatewayResponseValidationTarget,
} from "@/lib/api-types"

type RuleForm = {
  name: string
  enabled: boolean
  priority: string
  pattern: string
  target: GatewayResponseValidationTarget
  models: string
  protocols: string
}

type ResponseRuleExportItem = {
  name: string
  enabled: boolean
  priority: number
  pattern: string
  target: GatewayResponseValidationTarget
  models?: string[]
  protocols?: string[]
}

type ResponseRuleExportPackage = {
  kind: string
  version: number
  rules: ResponseRuleExportItem[]
}

type ImportStrategy = "skip" | "replace" | "rename"

const emptyRule = (): RuleForm => ({
  name: "",
  enabled: true,
  priority: "100",
  pattern: "",
  target: "assistant_text",
  models: "",
  protocols: "",
})

function parseList(raw?: string) {
  if (!raw) return ""
  try {
    const values = JSON.parse(raw)
    return Array.isArray(values) ? values.join("\n") : ""
  } catch {
    return ""
  }
}

function listJSON(raw: string) {
  const values = raw
    .split(/[\n,]/)
    .map((value) => value.trim())
    .filter(Boolean)
  return JSON.stringify([...new Set(values)])
}

function targetLabel(target: GatewayResponseValidationTarget) {
  if (target === "raw_body") return "原始响应"
  if (target === "error_message") return "错误信息"
  return "助手文本"
}

export function ResponseRulesPanel({
  groupID,
  enabled,
}: {
  groupID: number
  enabled: boolean
}) {
  const { confirm, dialog: confirmDialog } = useConfirm()
  const [rules, setRules] = useState<GatewayResponseRule[]>([])
  const [loading, setLoading] = useState(false)
  const [busy, setBusy] = useState(false)
  const [open, setOpen] = useState(false)
  const [editing, setEditing] = useState<GatewayResponseRule | null>(null)
  const [form, setForm] = useState<RuleForm>(emptyRule)
  const [importStrategy, setImportStrategy] = useState<ImportStrategy>("skip")
  const importInputRef = useRef<HTMLInputElement>(null)
  const loadSeqRef = useRef(0)

  const load = useCallback(async () => {
    const seq = ++loadSeqRef.current
    setLoading(true)
    try {
      const result = await apiFetch<{ items: GatewayResponseRule[] }>(
        `/gateway/groups/${groupID}/response-rules`,
      )
      if (seq !== loadSeqRef.current) return
      setRules(result.items ?? [])
    } catch (error) {
      if (seq !== loadSeqRef.current) return
      toast.error(error instanceof Error ? error.message : "加载响应规则失败")
    } finally {
      if (seq === loadSeqRef.current) setLoading(false)
    }
  }, [groupID])

  useEffect(() => {
    setRules([])
    setOpen(false)
    setEditing(null)
    void load()
    return () => {
      loadSeqRef.current += 1
    }
  }, [load])

  function startCreate() {
    setEditing(null)
    setForm(emptyRule())
    setOpen(true)
  }

  function startEdit(rule: GatewayResponseRule) {
    setEditing(rule)
    setForm({
      name: rule.name,
      enabled: rule.enabled,
      priority: String(rule.priority),
      pattern: rule.pattern,
      target: rule.target,
      models: parseList(rule.models_json),
      protocols: parseList(rule.protocols_json),
    })
    setOpen(true)
  }

  async function save() {
    if (!form.name.trim() || !form.pattern.trim()) {
      toast.error("请填写规则名称和正则表达式")
      return
    }
    const payload = {
      name: form.name.trim(),
      enabled: form.enabled,
      priority: Math.max(0, Math.min(100000, Number(form.priority) || 0)),
      pattern: form.pattern,
      target: form.target,
      models_json: listJSON(form.models),
      protocols_json: listJSON(form.protocols),
    }
    setBusy(true)
    try {
      if (editing) {
        await apiFetch(`/gateway/response-rules/${editing.id}`, {
          method: "PUT",
          body: JSON.stringify(payload),
        })
      } else {
        await apiFetch(`/gateway/groups/${groupID}/response-rules`, {
          method: "POST",
          body: JSON.stringify(payload),
        })
      }
      setOpen(false)
      await load()
      toast.success(editing ? "规则已更新" : "规则已创建")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "保存响应规则失败")
    } finally {
      setBusy(false)
    }
  }

  async function remove(rule: GatewayResponseRule) {
    const accepted = await confirm({
      title: "删除响应规则",
      description: `将删除“${rule.name}”，且不可恢复。`,
    })
    if (!accepted) return
    setBusy(true)
    try {
      await apiFetch(`/gateway/response-rules/${rule.id}`, { method: "DELETE" })
      await load()
      toast.success("规则已删除")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "删除响应规则失败")
    } finally {
      setBusy(false)
    }
  }

  async function exportRules() {
    setBusy(true)
    try {
      const payload = await apiFetch<ResponseRuleExportPackage>(
        `/gateway/groups/${groupID}/response-rules/export`,
      )
      const blob = new Blob([JSON.stringify(payload, null, 2)], {
        type: "application/json;charset=utf-8",
      })
      const url = URL.createObjectURL(blob)
      const link = document.createElement("a")
      link.href = url
      link.download = `upstream-ops-response-rules-${groupID}.json`
      document.body.appendChild(link)
      link.click()
      link.remove()
      URL.revokeObjectURL(url)
      toast.success("响应规则已导出")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "导出响应规则失败")
    } finally {
      setBusy(false)
    }
  }

  async function importRules(file: File) {
    try {
      const text = await file.text()
      const payload = JSON.parse(text) as Partial<ResponseRuleExportPackage>
      if (!payload || !Array.isArray(payload.rules)) {
        throw new Error("文件不是有效的响应规则包")
      }
      if (payload.rules.length === 0) {
        throw new Error("响应规则包为空")
      }
      if (payload.rules.length > 512) {
        throw new Error("一次最多导入 512 条规则")
      }
      setBusy(true)
      const result = await apiFetch<{
        created?: number
        replaced?: number
        updated?: number
        skipped?: number
      }>(`/gateway/groups/${groupID}/response-rules/import`, {
        method: "POST",
        body: JSON.stringify({ ...payload, strategy: importStrategy }),
      })
      await load()
      const replaced = result.replaced ?? result.updated ?? 0
      const parts = [
        `新增 ${result.created ?? 0}`,
        `更新 ${replaced}`,
      ]
      if ((result.skipped ?? 0) > 0) parts.push(`跳过 ${result.skipped}`)
      toast.success(`响应规则导入完成：${parts.join("，")}`)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "导入响应规则失败")
    } finally {
      setBusy(false)
      if (importInputRef.current) importInputRef.current.value = ""
    }
  }

  function chooseImportFile() {
    importInputRef.current?.click()
  }

  return (
    <>
      <Card className="overflow-hidden border-border shadow-none">
        <CardContent className="space-y-4 p-4 sm:p-5">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1">
              <div className="flex items-center gap-2 text-sm font-semibold">
                <ShieldCheck className="size-4 text-emerald-600" />
                响应正则规则
              </div>
              <p className="text-xs leading-5 text-muted-foreground">
                按优先级检查响应。命中后拒绝当前 attempt，先按组内重试次数重试当前路由，再切换其它路由。
              </p>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button size="icon-sm" variant="outline" onClick={() => void load()} disabled={loading || busy} title="刷新">
                <RefreshCw className={loading ? "size-4 animate-spin" : "size-4"} />
              </Button>
              <Button size="sm" variant="outline" onClick={() => void exportRules()} disabled={loading || busy}>
                <Download className="size-3.5" /> 导出
              </Button>
              <Select
                value={importStrategy}
                onValueChange={(value) => setImportStrategy(value as ImportStrategy)}
                disabled={loading || busy}
              >
                <SelectTrigger className="h-8 w-[7.25rem] text-xs" title="导入同名规则的处理方式">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="skip">同名跳过</SelectItem>
                  <SelectItem value="replace">同名覆盖</SelectItem>
                  <SelectItem value="rename">同名改名</SelectItem>
                </SelectContent>
              </Select>
              <Button size="sm" variant="outline" onClick={chooseImportFile} disabled={loading || busy}>
                <Upload className="size-3.5" /> 导入
              </Button>
              <input
                ref={importInputRef}
                className="hidden"
                type="file"
                accept=".json,application/json"
                onChange={(event) => {
                  const file = event.target.files?.[0]
                  if (file) void importRules(file)
                }}
              />
              <Button size="sm" onClick={startCreate} disabled={loading || busy}>
                <Plus className="size-3.5" /> 新建规则
              </Button>
            </div>
          </div>

          {!enabled ? (
            <p className="rounded-md border border-amber-200 bg-amber-50 px-3 py-2 text-xs text-amber-900 dark:border-amber-900/50 dark:bg-amber-950/30 dark:text-amber-200">
              当前组尚未启用正则响应校验。规则可先配置，启用开关位于“编辑组”。
            </p>
          ) : null}

          {loading && rules.length === 0 ? (
            <p className="py-8 text-center text-sm text-muted-foreground">加载中…</p>
          ) : rules.length === 0 ? (
            <div className="rounded-md border border-dashed border-border px-4 py-8 text-center text-sm text-muted-foreground">
              还没有响应规则
            </div>
          ) : (
            <div className="space-y-2">
              {rules.map((rule) => (
                <div key={rule.id} className="grid gap-3 rounded-md border border-border p-3 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-center">
                  <div className="min-w-0 space-y-1.5">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="truncate text-sm font-medium">{rule.name}</span>
                      <Badge variant={rule.enabled ? "default" : "secondary"}>{rule.enabled ? "启用" : "停用"}</Badge>
                      <Badge variant="outline">优先级 {rule.priority}</Badge>
                      <Badge variant="outline">{targetLabel(rule.target)}</Badge>
                    </div>
                    <code className="block max-w-full overflow-x-auto whitespace-pre rounded bg-muted px-2 py-1 text-[11px]">{rule.pattern}</code>
                    <p className="text-[11px] text-muted-foreground">
                      模型：{parseList(rule.models_json).replaceAll("\n", ", ") || "全部"} · 协议：{parseList(rule.protocols_json).replaceAll("\n", ", ") || "全部"}
                    </p>
                  </div>
                  <div className="flex gap-1 sm:justify-end">
                    <Button size="icon-sm" variant="ghost" disabled={loading || busy} onClick={() => startEdit(rule)} title="编辑">
                      <Pencil className="size-4" />
                    </Button>
                    <Button size="icon-sm" variant="ghost" className="text-destructive" disabled={busy} onClick={() => void remove(rule)} title="删除">
                      <Trash2 className="size-4" />
                    </Button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>

      <Dialog open={open} onOpenChange={(nextOpen) => !busy && setOpen(nextOpen)}>
        <DialogContent className="max-h-[90dvh] w-[calc(100vw-1.5rem)] max-w-xl overflow-y-auto">
          <DialogHeader>
            <DialogTitle>{editing ? "编辑响应规则" : "新建响应规则"}</DialogTitle>
            <DialogDescription>正则使用 Go RE2 语法。默认检查助手可见文本。</DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid gap-3 sm:grid-cols-[minmax(0,1fr)_120px]">
              <div className="space-y-1">
                <Label>名称</Label>
                <Input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} />
              </div>
              <div className="space-y-1">
                <Label>优先级</Label>
                <Input type="number" min={0} value={form.priority} onChange={(event) => setForm({ ...form, priority: event.target.value })} />
              </div>
            </div>
            <div className="flex items-center justify-between rounded-md border border-border px-3 py-2">
              <Label>启用规则</Label>
              <Switch checked={form.enabled} onCheckedChange={(value) => setForm({ ...form, enabled: value })} />
            </div>
            <div className="space-y-1">
              <Label>检查目标</Label>
              <Select value={form.target} onValueChange={(value) => setForm({ ...form, target: value as GatewayResponseValidationTarget })}>
                <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
                <SelectContent>
                  <SelectItem value="assistant_text">助手文本</SelectItem>
                  <SelectItem value="raw_body">原始响应</SelectItem>
                  <SelectItem value="error_message">错误信息</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-1">
              <Label>正则表达式</Label>
              <Textarea className="font-mono" rows={4} value={form.pattern} onChange={(event) => setForm({ ...form, pattern: event.target.value })} placeholder="(?i)temporarily unavailable|please retry" />
            </div>
            <div className="grid gap-3 sm:grid-cols-2">
              <div className="space-y-1">
                <Label>模型过滤</Label>
                <Textarea rows={3} value={form.models} onChange={(event) => setForm({ ...form, models: event.target.value })} placeholder="每行一个；留空表示全部" />
              </div>
              <div className="space-y-1">
                <Label>协议过滤</Label>
                <Textarea rows={3} value={form.protocols} onChange={(event) => setForm({ ...form, protocols: event.target.value })} placeholder="openai_chat\nanthropic" />
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" disabled={busy} onClick={() => setOpen(false)}>取消</Button>
            <Button disabled={busy} onClick={() => void save()}>保存</Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
      {confirmDialog}
    </>
  )
}
