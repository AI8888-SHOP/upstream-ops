import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"
import ts from "typescript"

// CI only: exercise the merge helper without mounting the application.
const source = await readFile(new URL("../lib/price-comparison.ts", import.meta.url), "utf8")
const compiled = ts.transpileModule(source, {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
}).outputText
const { mergeComparisonRoutes } = await import("data:text/javascript;base64," + Buffer.from(compiled).toString("base64"))

const createRoute = () => ({ enabled: true, model_policy: "all", concurrency: 10, rate_convert_mode: "raw" })
const strategy = { items: [
  { candidate: { channel_id: 1, group_id: 10, group_name: "images" }, priority: 1 },
  { candidate: { channel_id: 1, group_id: 20, group_name: "video" }, priority: 2 },
  { candidate: { channel_id: 2, group_id: null, group_name: "cheap" }, priority: 3 },
] }

test("comparison application merges groups without replacing credentials, policy or disabled state", () => {
  const drafts = [
    { id: 11, source_channel_id: 1, source_group_id: 10, enabled: false, source_api_key_name: "existing-key", model_policy: "allow", allowed_models_json: '["image-model"]', rate_convert_mode: "custom", rate_convert_value: .8, priority: 9 },
    { id: 12, source_channel_id: 1, source_group_name: "video", enabled: true },
    { id: 13, source_kind: "provider", gateway_provider_id: 7, enabled: true, priority: 1 },
  ]
  const merged = mergeComparisonRoutes(drafts, strategy, createRoute)
  assert.equal(merged.length, 4)
  assert.deepEqual(merged[0], { ...drafts[0], priority: 1 })
  assert.deepEqual(merged[1], { ...drafts[1], priority: 2 })
  assert.deepEqual(merged[2], { ...drafts[2], priority: 0 })
  assert.equal(merged[3].source_channel_id, 2)
  assert.equal(merged[3].source_group_name, "cheap")
  assert.equal(merged[3].enabled, true)
  assert.equal(merged[3].rate_convert_mode, "raw")
  assert.equal(drafts[0].priority, 9, "must not mutate existing draft objects")
  assert.deepEqual(mergeComparisonRoutes(merged, strategy, createRoute), merged, "repeated application must be idempotent")
})

test("same source group IDs on different channels stay independent", () => {
  const drafts = [{ id: 11, source_channel_id: 5, source_group_id: 10, priority: 3, enabled: false }]
  const merged = mergeComparisonRoutes(drafts, strategy, createRoute)
  assert.equal(merged.length, 4)
  assert.equal(merged[0].source_channel_id, 5)
  assert.equal(merged[0].enabled, false)
  assert.equal(merged[1].source_channel_id, 1)
  assert.equal(merged[1].source_group_id, 10)
})

test("legacy id placeholders are reused, not duplicated", () => {
  for (const name of ["id:10", "source id: 10", "源 ID: 10", "images"]) {
    const merged = mergeComparisonRoutes([{ source_channel_id: 1, source_group_id: 0, source_group_name: name, id: 99 }], strategy, createRoute)
    assert.equal(merged.length, 3)
    assert.equal(merged[0].id, 99)
    assert.equal(merged[0].priority, 1)
  }
})
