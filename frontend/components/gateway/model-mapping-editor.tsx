import { Plus } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"

export type MappingRow = { from: string; to: string }

export function parseMappingJSON(raw?: string): MappingRow[] {
  if (!raw?.trim()) return []
  try {
    const obj = JSON.parse(raw) as Record<string, unknown>
    return Object.entries(obj)
      .filter(([, v]) => typeof v === "string" && String(v).trim())
      .map(([from, to]) => ({ from, to: String(to) }))
  } catch {
    return []
  }
}

export function serializeMappingJSON(rows: MappingRow[]): string {
  const obj: Record<string, string> = {}
  for (const r of rows) {
    const from = r.from.trim()
    const to = r.to.trim()
    if (!from || !to) continue
    obj[from] = to
  }
  return Object.keys(obj).length ? JSON.stringify(obj) : ""
}

export function ModelMappingEditor({
  rows,
  onChange,
  modelSuggestions = [],
  datalistID = "gw-model-suggestions",
}: {
  rows: MappingRow[]
  onChange: (rows: MappingRow[]) => void
  modelSuggestions?: string[]
  datalistID?: string
}) {
  return (
    <div className="space-y-3">
      <div className="flex items-center justify-between">
        <Label>模型映射（客户端 → 上游）</Label>
        <Button
          type="button"
          size="sm"
          variant="outline"
          onClick={() => onChange([...rows, { from: "", to: "" }])}
        >
          <Plus className="size-3.5" />
          添加
        </Button>
      </div>
      {rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          无映射。可添加 A→B，或使用 <code className="rounded bg-muted px-1">*</code> 通配。
        </p>
      ) : (
        <div className="space-y-2">
          {rows.map((row, i) => (
            <div key={i} className="flex items-center gap-2">
              <Input
                list={datalistID}
                placeholder="客户端模型"
                value={row.from}
                onChange={(e) => {
                  const next = [...rows]
                  next[i] = { ...next[i], from: e.target.value }
                  onChange(next)
                }}
              />
              <span className="text-muted-foreground shrink-0">→</span>
              <Input
                list={datalistID}
                placeholder="上游模型"
                value={row.to}
                onChange={(e) => {
                  const next = [...rows]
                  next[i] = { ...next[i], to: e.target.value }
                  onChange(next)
                }}
              />
              <Button
                type="button"
                size="sm"
                variant="outline"
                onClick={() => onChange(rows.filter((_, j) => j !== i))}
              >
                删除
              </Button>
            </div>
          ))}
        </div>
      )}
      {modelSuggestions.length > 0 && (
        <datalist id={datalistID}>
          {modelSuggestions.map((m) => (
            <option key={m} value={m} />
          ))}
        </datalist>
      )}
    </div>
  )
}
