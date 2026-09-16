import { useCallback, useEffect, useRef, useState } from "react"
import { Download, RefreshCw, RotateCcw } from "lucide-react"
import { toast } from "sonner"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { apiFetch, type ApiError } from "@/lib/api"
import { useAppVersion } from "@/lib/queries"
import type { AppVersion } from "@/lib/api-types"
import { cn } from "@/lib/utils"

type UpdateJob = {
  id: string
  phase: string
  target_version: string
  previous_version: string
  message: string
  error?: string
}
type UpdateStatus = { available: boolean; mode?: string; reason?: string; busy: boolean; job?: UpdateJob }
type Release = { tag_name: string; html_url: string; published_at: string }
type Catalog = { current_version: string; update_available: boolean; latest: Release | null; rollback: Release[] }

const progressLabels: Record<string, string> = {
  queued: "等待执行", preparing: "下载与校验", applying: "切换版本", validating: "健康检查",
  rolling_back: "正在自动恢复", succeeded: "已完成", failed: "未切换版本", rolled_back: "已自动恢复", rollback_failed: "恢复异常",
}
const terminal = new Set(["succeeded", "failed", "rolled_back", "rollback_failed"])

export function ApplicationUpdater() {
  const version = useAppVersion()
  const [status, setStatus] = useState<UpdateStatus | null>(null)
  const [catalog, setCatalog] = useState<Catalog | null>(null)
  const [rollback, setRollback] = useState("")
  const [checking, setChecking] = useState(false)
  const [submitting, setSubmitting] = useState(false)
  const [reconnecting, setReconnecting] = useState(false)
  const [connectionMessage, setConnectionMessage] = useState("")
  const [catalogError, setCatalogError] = useState("")
  const pending = useRef<UpdateJob | null>(null)
  const legacy = useRef(false)
  const [legacyConnected, setLegacyConnected] = useState(false)
  const [connectedVersion, setConnectedVersion] = useState("")
  const alive = useRef(true)

  const loadStatus = useCallback(async () => {
    try {
      const result = await apiFetch<UpdateStatus>("/updates/status", { signal: AbortSignal.timeout(8000) })
      if (!alive.current) return
      setStatus(result)
      legacy.current = false
      setLegacyConnected(false)
      if (result.busy && result.job) pending.current = result.job
      if (result.available) {
        setConnectionMessage("")
        setReconnecting(false)
      }
      if (result.job && terminal.has(result.job.phase) && pending.current?.id === result.job.id) {
        pending.current = null
        if (result.job.phase === "succeeded") toast.success(`已切换至 ${result.job.target_version}`)
        else if (result.job.phase === "rolled_back") toast.warning(`更新失败，已恢复 ${result.job.previous_version}`)
        else toast.error(result.job.message)
      }
    } catch (error) {
      if (!alive.current) return
      if ((error as ApiError).status === 404 && legacy.current) return
      if ((error as ApiError).status === 404 && pending.current) {
        // Releases predating this feature have no /updates endpoint. Do not
        // misreport their missing API as an application startup failure.
        try {
          const info = await apiFetch<AppVersion>("/version", { signal: AbortSignal.timeout(8000) })
          if (!alive.current) return
          setConnectionMessage(`已连接版本 ${info.version}。该旧版本没有更新进度接口，独立更新服务仍会完成健康检查；请刷新页面进入该版本。`)
          setConnectedVersion(info.version)
          setStatus({ available: false, busy: false, reason: "该版本不提供网页升级入口" })
          setReconnecting(false)
          legacy.current = true
          setLegacyConnected(true)
          pending.current = null
          return
        } catch { /* Application may still be restarting; keep polling. */ }
      }
      setReconnecting(Boolean(pending.current))
      setConnectionMessage(pending.current ? "应用正在重启，等待重新连接；更新任务由独立进程继续执行。" : "暂时无法读取更新状态，请稍后检查。")
    }
  }, [])

  useEffect(() => {
    alive.current = true
    let cancelled = false
    let timer: ReturnType<typeof setTimeout>
    const poll = async () => {
      await loadStatus()
      if (!cancelled) timer = setTimeout(poll, 3000)
    }
    void poll()
    return () => { cancelled = true; alive.current = false; clearTimeout(timer) }
  }, [loadStatus])

  const loadCatalog = useCallback(async (force = false) => {
    const result = await apiFetch<Catalog>(`/updates/releases${force ? "?force=1" : ""}`, { signal: AbortSignal.timeout(40000) })
    if (alive.current) {
      setCatalog(result)
      setConnectedVersion(result.current_version)
      setRollback((old) => result.rollback.some((item) => item.tag_name === old) ? old : result.rollback[0]?.tag_name ?? "")
      setCatalogError("")
    }
    return result
  }, [])

  const completedJob = status?.job && terminal.has(status.job.phase) ? `${status.job.id}:${status.job.phase}` : ""
  useEffect(() => {
    if (status?.available) void loadCatalog().catch((error: Error) => {
      if (alive.current) setCatalogError(error.message)
    })
  }, [status?.available, completedJob, loadCatalog])

  async function check() {
    setChecking(true)
    const checks = await Promise.allSettled([
      apiFetch<AppVersion>("/version?force=1", { signal: AbortSignal.timeout(10000) }),
      status?.available ? loadCatalog(true) : Promise.resolve(null),
    ])
    if (!alive.current) return
    if (checks[0].status === "fulfilled") version.setData(checks[0].value)
    if (checks[1].status === "fulfilled" && checks[1].value) {
      const result = checks[1].value
      toast.message(result.update_available ? `发现新版本 ${result.latest?.tag_name}` : "当前已是最新版本")
    } else if (checks[0].status === "fulfilled") {
      if (checks[0].value.update_error) toast.error(checks[0].value.update_error)
      else toast.message(checks[0].value.update_available ? `发现新版本 ${checks[0].value.latest_version}` : "当前已是最新版本")
    } else toast.error("版本检测失败，请稍后重试")
    if (checks[1].status === "rejected") setCatalogError(checks[1].reason instanceof Error ? checks[1].reason.message : "读取发版列表失败")
    setChecking(false)
    void loadStatus()
  }

  async function start(action: "upgrade" | "rollback", target: string) {
    if (!target) return
    setSubmitting(true)
    try {
      const result = await apiFetch<UpdateStatus>("/updates/start", { method: "POST", body: JSON.stringify({ action, version: target }), signal: AbortSignal.timeout(45000) })
      pending.current = result.job ?? null
      setStatus(result)
      toast.message(action === "upgrade" ? "已开始自动升级" : "已开始版本回退")
    } catch (error) {
      toast.error(error instanceof Error ? error.message : "提交更新任务失败")
      // A lost POST response does not mean the agent failed to accept it.
      await loadStatus()
    } finally { setSubmitting(false) }
  }

  const busy = submitting || Boolean(status?.busy) || reconnecting
  const recoveryBlocked = status?.job?.phase === "rollback_failed"
  const latest = catalog?.latest?.tag_name ?? version.data?.latest_version
  const updateAvailable = catalog?.update_available ?? version.data?.update_available
  const canUpgrade = Boolean(status?.available && catalog?.latest && catalog.update_available && !catalogError)
  const failed = status?.job && ["failed", "rolled_back", "rollback_failed"].includes(status.job.phase)

  return (
    <div id="application-update" className="mb-5 space-y-3 rounded-xl border border-border bg-muted/20 p-3">
      <div className="flex flex-wrap items-center gap-2">
        <Badge variant="outline">当前版本 {connectedVersion || version.data?.version || "加载中"}</Badge>
        {status?.available && <Badge variant="outline">{status.mode === "docker" ? "Docker" : "原生服务"}更新服务已连接</Badge>}
        <Button size="sm" variant="outline" onClick={check} disabled={checking || busy}>
          <RefreshCw className={cn("size-3.5", checking && "animate-spin")} />{checking ? "检查中…" : "检查更新"}
        </Button>
        {updateAvailable && latest && (
          <Button size="sm" onClick={() => start("upgrade", latest)} disabled={!canUpgrade || busy || checking || recoveryBlocked}>
            <Download className="size-3.5" />升级到 {latest}
          </Button>
        )}
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <Select value={rollback} onValueChange={setRollback} disabled={!status?.available || busy || checking || !catalog?.rollback.length}>
          <SelectTrigger className="w-60"><SelectValue placeholder="最近三个旧版本" /></SelectTrigger>
          <SelectContent>
            {catalog?.rollback.map((release) => (
              <SelectItem key={release.tag_name} value={release.tag_name}>{release.tag_name} · {new Date(release.published_at).toLocaleDateString()}</SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Button size="sm" variant="outline" onClick={() => start("rollback", rollback)} disabled={!rollback || !status?.available || busy || checking || Boolean(catalogError) || recoveryBlocked}>
          <RotateCcw className="size-3.5" />回退到所选版本
        </Button>
      </div>
      {status?.job && (
        <div role="status" className={cn("space-y-1 rounded-lg bg-background p-2 text-xs", failed && "text-amber-700 dark:text-amber-400")}>
          <p>{progressLabels[status.job.phase] ?? status.job.phase} · {status.job.previous_version} → {status.job.target_version}</p>
          <p>{status.job.message}</p>
          {status.job.error && <details><summary className="cursor-pointer">失败详情</summary><p className="mt-1 max-h-48 overflow-auto whitespace-pre-wrap break-all">{status.job.error}</p></details>}
          {status.job.phase === "succeeded" && <Button size="sm" variant="outline" onClick={() => window.location.reload()}>刷新进入新版本</Button>}
        </div>
      )}
      {connectionMessage && <p role="status" className="text-xs text-muted-foreground">{connectionMessage}</p>}
      {legacyConnected && <Button size="sm" variant="outline" onClick={() => window.location.reload()}>刷新进入已连接版本</Button>}
      {catalogError && <p role="alert" className="text-xs text-amber-700 dark:text-amber-400">{catalogError}</p>}
      {status && !status.available && <p className="text-xs text-muted-foreground">{status.reason} <a className="underline" href="https://github.com/AI8888-SHOP/upstream-ops/blob/main/docs/AUTO_UPDATE.md" target="_blank" rel="noreferrer">启用说明</a></p>}
      <p className="text-xs leading-5 text-muted-foreground">切换会短暂重启应用，数据库数据保留。新版本启动或健康检查失败时自动恢复原版本。</p>
    </div>
  )
}
