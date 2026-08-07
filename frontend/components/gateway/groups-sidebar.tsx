import { useRef, useState } from "react"
import { Copy, GripVertical, Loader2, Pencil, Plus, Trash2 } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Card, CardContent } from "@/components/ui/card"
import { cn } from "@/lib/utils"
import type { GatewayGroup } from "@/lib/api-types"

type GroupsSidebarProps = {
  groups: GatewayGroup[]
  selectedGroupID: number | null
  groupLoading: boolean
  busy: boolean
  onSelect: (id: number) => void
  onCreate: () => void
  onClone: (id: number) => void | Promise<void>
  onEdit: (g: GatewayGroup) => void
  onDelete: (id: number) => void
  /** 拖拽重排后按新顺序提交 ids */
  onReorder: (orderedIDs: number[]) => void | Promise<void>
}

export function GroupsSidebar({
  groups,
  selectedGroupID,
  groupLoading,
  busy,
  onSelect,
  onCreate,
  onClone,
  onEdit,
  onDelete,
  onReorder,
}: GroupsSidebarProps) {
  const dragIDRef = useRef<number | null>(null)
  const [dragOverID, setDragOverID] = useState<number | null>(null)
  const [draggingID, setDraggingID] = useState<number | null>(null)

  function moveGroup(fromID: number, toID: number) {
    if (fromID === toID) return
    const fromIdx = groups.findIndex((g) => g.id === fromID)
    const toIdx = groups.findIndex((g) => g.id === toID)
    if (fromIdx < 0 || toIdx < 0) return
    const next = [...groups]
    const [item] = next.splice(fromIdx, 1)
    next.splice(toIdx, 0, item)
    void onReorder(next.map((g) => g.id))
  }

  return (
    <Card className="h-fit overflow-hidden border-border shadow-none">
      <CardContent className="space-y-3 p-3 sm:p-4">
        <div className="flex items-center justify-between gap-2">
          <div className="text-sm font-medium">网关组</div>
          <Button size="sm" variant="outline" className="h-7 px-2" onClick={onCreate} disabled={busy}>
            <Plus className="size-3.5" /> 新建
          </Button>
        </div>
        <div className="max-h-[min(70vh,560px)] space-y-2 overflow-auto">
          {groups.map((g) => {
            const selected = selectedGroupID === g.id
            const isDragging = draggingID === g.id
            const isOver = dragOverID === g.id && draggingID !== g.id
            return (
              <div
                key={g.id}
                role="button"
                tabIndex={0}
                onClick={() => {
                  if (selectedGroupID !== g.id) onSelect(g.id)
                }}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault()
                    if (selectedGroupID !== g.id) onSelect(g.id)
                  }
                }}
                onDragOver={(e) => {
                  e.preventDefault()
                  e.dataTransfer.dropEffect = "move"
                  if (dragOverID !== g.id) setDragOverID(g.id)
                }}
                onDragLeave={() => {
                  if (dragOverID === g.id) setDragOverID(null)
                }}
                onDrop={(e) => {
                  e.preventDefault()
                  const raw = e.dataTransfer.getData("text/plain")
                  const from = dragIDRef.current ?? (Number(raw) || 0)
                  setDragOverID(null)
                  setDraggingID(null)
                  dragIDRef.current = null
                  if (from > 0) moveGroup(from, g.id)
                }}
                className={cn(
                  "group cursor-pointer space-y-1.5 rounded-lg border px-2.5 py-2 text-left outline-none transition-all duration-150",
                  "focus-visible:ring-2 focus-visible:ring-ring/40 focus-visible:ring-offset-1",
                  selected
                    ? "border-primary/70 bg-primary/8 shadow-sm ring-1 ring-primary/20"
                    : "border-border/70 bg-background hover:border-border hover:bg-muted/35 hover:shadow-sm",
                  isDragging && "opacity-50",
                  isOver && "border-primary/50 ring-1 ring-primary/30",
                )}
              >
                {/* 第 1 行：标题 + 状态 tag */}
                <div className="flex min-w-0 items-center gap-1.5">
                  <span
                    className={cn(
                      "truncate text-sm font-medium leading-5",
                      selected
                        ? "text-foreground"
                        : "text-foreground/85",
                    )}
                  >
                    {g.name}
                  </span>
                  {selected && groupLoading ? (
                    <Loader2 className="size-3.5 shrink-0 animate-spin text-primary" />
                  ) : (
                    <Badge
                      variant={
                        g.status === "active" ? "default" : "secondary"
                      }
                      className={cn(
                        "h-5 shrink-0 px-1.5 text-[10px]",
                        selected && g.status === "active" && "shadow-none",
                        !selected &&
                          g.status === "active" &&
                          "bg-primary/80",
                      )}
                    >
                      {g.status === "active" ? "启用" : "禁用"}
                    </Badge>
                  )}
                </div>
                {/* 第 2 行：描述 */}
                <div
                  className={cn(
                    "line-clamp-2 min-h-4 text-xs leading-4",
                    selected
                      ? "text-muted-foreground"
                      : "text-muted-foreground/80",
                  )}
                >
                  {g.description?.trim() || (
                    <span className="text-muted-foreground/45">无描述</span>
                  )}
                </div>
                {/* 第 3 行：拖动排序 / 编辑 / 删除（右对齐） */}
                <div
                  className={cn(
                    "flex items-center justify-end gap-0.5 transition-opacity",
                    selected
                      ? "opacity-90"
                      : "opacity-55 group-hover:opacity-100",
                  )}
                  onClick={(e) => e.stopPropagation()}
                  onKeyDown={(e) => e.stopPropagation()}
                >
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="size-7"
                    title="复制网关组"
                    disabled={busy}
                    onClick={() => void onClone(g.id)}
                  >
                    <Copy className="size-3.5" />
                  </Button>
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="size-7 cursor-grab touch-none active:cursor-grabbing"
                    title="拖动排序"
                    draggable={!busy}
                    disabled={busy || groups.length < 2}
                    onDragStart={(e) => {
                      e.stopPropagation()
                      dragIDRef.current = g.id
                      setDraggingID(g.id)
                      e.dataTransfer.effectAllowed = "move"
                      e.dataTransfer.setData("text/plain", String(g.id))
                    }}
                    onDragEnd={() => {
                      dragIDRef.current = null
                      setDraggingID(null)
                      setDragOverID(null)
                    }}
                  >
                    <GripVertical className="size-3.5 text-muted-foreground" />
                  </Button>
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="size-7"
                    title="编辑"
                    onClick={() => onEdit(g)}
                  >
                    <Pencil className="size-3.5" />
                  </Button>
                  <Button
                    type="button"
                    size="icon"
                    variant="ghost"
                    className="size-7 text-muted-foreground hover:text-destructive"
                    title="删除"
                    onClick={() => void onDelete(g.id)}
                  >
                    <Trash2 className="size-3.5" />
                  </Button>
                </div>
              </div>
            )
          })}
          {groups.length === 0 && (
            <p className="py-8 text-center text-sm text-muted-foreground">
              暂无网关组
            </p>
          )}
        </div>
      </CardContent>
    </Card>
  )
}
