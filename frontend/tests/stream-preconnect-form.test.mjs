import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"
import ts from "typescript"

// Structural regressions run in CI alongside the full frontend typecheck.
const fields = ["stream_preconnect_enabled", "stream_preconnect_min_delay_ms", "stream_preconnect_max_delay_ms"]

test("group preconnect policy survives type, form, defaults, load and save wiring", async () => {
  const [types, utils, page, dialog] = await Promise.all([
    readFile(new URL("../lib/api-types.ts", import.meta.url), "utf8"),
    readFile(new URL("../components/gateway/gateway-utils.ts", import.meta.url), "utf8"),
    readFile(new URL("../components/gateway/gateway-page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../components/gateway/gateway-dialogs.tsx", import.meta.url), "utf8"),
  ])
  for (const source of [types, utils, page, dialog]) {
    for (const field of fields) assert.ok(source.includes(field), `missing ${field}`)
  }
  assert.ok(utils.includes("stream_preconnect_enabled: false"))
  assert.ok(utils.includes('stream_preconnect_min_delay_ms: "0"'))
  assert.ok(utils.includes('stream_preconnect_max_delay_ms: "0"'))
  for (const field of fields) {
    assert.ok(page.includes(`g.${field}`), `missing load ${field}`)
    assert.ok(page.includes(`groupForm.${field}`), `missing save ${field}`)
  }
  assert.ok(page.includes("preconnectMinDelay > preconnectMaxDelay"))
  assert.ok(page.includes("preconnectMaxDelay > 60000"))
  assert.ok(page.includes("Number.isInteger(preconnectMinDelay)"))
  assert.ok(page.includes("Number.isInteger(preconnectMaxDelay)"))
  assert.ok(dialog.includes("提前建立流并保活（非真实首字）"))
  assert.ok(dialog.includes("不代表请求成功"))
  assert.ok(dialog.includes("不会延长首字超时或请求总预算"))
  for (const [name, source] of [["gateway-page.tsx", page], ["gateway-dialogs.tsx", dialog]]) {
    const parsed = ts.createSourceFile(name, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
    assert.deepEqual(parsed.parseDiagnostics.map((d) => ts.flattenDiagnosticMessageText(d.messageText, " ").trim()), [], name)
  }
})
