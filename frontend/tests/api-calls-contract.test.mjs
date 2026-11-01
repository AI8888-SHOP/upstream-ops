import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { dirname, resolve } from "node:path"
import { fileURLToPath } from "node:url"
import test from "node:test"

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..")

async function source(relativePath) {
  return readFile(resolve(root, relativePath), "utf8")
}

test("API call audit keeps the backend query contract visible", async () => {
  const api = await source("lib/api.ts")
  const types = await source("lib/api-types.ts")
  const panel = await source("components/gateway/api-calls-panel.tsx")

  assert.match(api, /\/gateway\/api-calls\$\{gatewayApiCallQuery\(filters\)\}/)
  assert.match(api, /\/gateway\/api-calls\/stats\$\{gatewayApiCallQuery\(filters\)\}/)
  for (const filter of [
    "purpose",
    "channel_id",
    "provider_id",
    "source_group_id",
    "source_api_key_name",
    "model",
    "status",
    "since",
    "until",
    "page",
    "page_size",
  ]) {
    assert.match(types, new RegExp(`\\b${filter}\\b`), `missing filter type ${filter}`)
    assert.match(panel, new RegExp(`\\b${filter}\\b`), `missing filter wiring ${filter}`)
  }

  assert.match(panel, /API调用记录/)
  assert.match(types, /gateway_request_id: string/)
  assert.match(panel, /网关逻辑请求 ID/)
  for (const stat of ["success_calls", "error_calls", "pending_calls", "incomplete_calls", "total_estimated_cost", "unknown_cost_calls", "usage_known_calls"]) {
    assert.match(types, new RegExp(`\\b${stat}\\??:`), `missing storage stats field ${stat}`)
  }
  assert.match(panel, /未知成本数量/)
  assert.match(panel, /costLabel\(knownCost, hasCalls\)/)
  assert.match(panel, /const hasCalls = totalValue > 0/)
  assert.match(panel, /估算成本是前端展示的估算值，不是上游已核账费用/)
  assert.match(panel, /不保存敏感 header 或正文/)
})

test("candy check reuse is displayed as an observation, not a fresh request", async () => {
  const types = await source("lib/api-types.ts")
  const status = await source("components/gateway/route-candy-check-status.tsx")
  const settings = await source("components/gateway/candy-check-settings.tsx")

  for (const field of ["reused", "source_route_id", "source_request_id", "observed_at"]) {
    assert.match(types, new RegExp(`\\b${field}\\b`), `missing reuse field ${field}`)
    assert.match(status, new RegExp(`\\b${field}\\b`), `missing reuse display ${field}`)
  }
  assert.match(status, /复用 route#/)
  assert.match(status, /观察于/)
  assert.match(settings, /相同源站、凭据、模型、协议、推理强度、题目、请求头与代理/)
  assert.match(settings, /各组按自己的检测间隔判断新鲜度/)
  assert.match(settings, /网络失败最多短暂抑制 15 秒重复请求/)
  assert.match(settings, /手动复测会强制刷新；已有同源检测进行时会合并等待/)
  assert.match(settings, /检测与人工解除历史保留 90 天/)
})
