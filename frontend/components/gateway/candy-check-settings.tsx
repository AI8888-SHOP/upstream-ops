"use client"

import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Switch } from "@/components/ui/switch"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import type { GroupFormState } from "./gateway-utils"

export function CandyCheckSettings({ form, onChange }: { form: GroupFormState; onChange: (form: GroupFormState) => void }) {
  return (
    <div className="space-y-3 rounded-lg border border-border bg-muted/20 p-3">
      <div className="flex items-center justify-between gap-3">
        <Label htmlFor="candy-check-enabled">不降智检测（糖果题）</Label>
        <Switch id="candy-check-enabled" checked={form.candy_check_enabled} onCheckedChange={(v) => onChange({ ...form, candy_check_enabled: v })} />
      </div>
      <p className="text-xs leading-5 text-muted-foreground">
        对本组每条启用的渠道路由分别发送糖果题。答错、空答、请求失败或超时，会冷却该路由在本组的全部模型；其他网关组不受影响。
      </p>
      {form.candy_check_enabled && <>
        <div className="space-y-1">
          <Label htmlFor="candy-check-model">检测模型</Label>
          <Input id="candy-check-model" value={form.candy_check_model} placeholder="填写客户端请求的文本模型 ID" onChange={(e) => onChange({ ...form, candy_check_model: e.target.value })} />
          <p className="text-xs text-muted-foreground">遵守模型映射和渠道支持范围；不支持此模型的路由会跳过，并显示原因。</p>
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div className="space-y-1">
            <Label htmlFor="candy-check-interval">每隔多少分钟检测</Label>
            <Input id="candy-check-interval" type="number" min={1} max={1440} step={1} value={form.candy_check_interval_minutes} onChange={(e) => onChange({ ...form, candy_check_interval_minutes: e.target.value })} />
          </div>
          <div className="space-y-1">
            <Label htmlFor="candy-check-cooldown">失败冷却（分钟）</Label>
            <Input id="candy-check-cooldown" type="number" min={1} max={43200} step={1} value={form.candy_check_cooldown_minutes} onChange={(e) => onChange({ ...form, candy_check_cooldown_minutes: e.target.value })} />
          </div>
        </div>
        <div className="space-y-1">
          <Label>检测推理强度</Label>
          <Select value={form.candy_check_reasoning_effort} onValueChange={(v) => onChange({ ...form, candy_check_reasoning_effort: v as GroupFormState["candy_check_reasoning_effort"] })}>
            <SelectTrigger className="w-full"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="default">上游默认</SelectItem><SelectItem value="low">低</SelectItem><SelectItem value="medium">中</SelectItem><SelectItem value="high">高</SelectItem>
            </SelectContent>
          </Select>
          <p className="text-xs text-muted-foreground">仅 OpenAI Chat / Responses 发送此参数；不支持推理强度的模型请选择“上游默认”。</p>
        </div>
        <p className="text-xs leading-5 text-muted-foreground">
          检测会消耗上游 Token，不计入客户使用量。单次最长 90 秒；冷却期间不重复检测，到期自动恢复，人工解除后等待一个检测周期再测。全部路由都检测失败时将暂无可用渠道，不会自动绕过此限制。测试通过只代表本题回答正确。
        </p>
      </>}
    </div>
  )
}
