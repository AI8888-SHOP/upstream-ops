import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"
import ts from "typescript"

// Executed in GitHub CI; no additional test framework or browser dependency.
const metrics = await readFile(new URL("../lib/usage-metrics.ts", import.meta.url), "utf8")
const compiled = ts.transpileModule(metrics, { compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 } }).outputText
const { usageDisplayedCost, usageOutputTPS } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`)
const row = { stream: true, request_type: 2, output_tokens: 100, duration_ms: 5000, first_token_ms: 1000 }

test("historical repricing preserves settled charges, including zero", () => {
  const settled = { winner: true, attempt_status: "accepted", billed_cost: 0, actual_cost: 2 }
  assert.equal(usageDisplayedCost(settled), 0)
  assert.equal(usageDisplayedCost({ ...settled, billed_cost: 1, actual_cost: 4 }), 1)
  assert.equal(usageDisplayedCost({ ...settled, winner: false }), 2)
  assert.equal(usageDisplayedCost({ ...settled, attempt_status: "" }), 2)
  assert.equal(usageDisplayedCost({ ...settled, attempt_status: undefined }), 2)
  assert.equal(usageDisplayedCost({ ...settled, attempt_status: "", billed_cost: 0.75 }), 0.75)
  assert.equal(usageDisplayedCost({ ...settled, attempt_status: "", virtual_cache_read_tokens: 10 }), 0)
  assert.equal(usageDisplayedCost({ winner: false }), 0)
})

test("TPS uses attempt generation duration, not request retry timing", () => {
  assert.equal(usageOutputTPS(row), 25)
  assert.equal(usageOutputTPS({ ...row, request_duration_ms: 15000, request_first_token_ms: 11000 }), 25)
  assert.equal(usageOutputTPS({ ...row, request_duration_ms: 50000, request_first_token_ms: null }), 25)
  assert.equal(usageOutputTPS({ ...row, stream: false }), 25)
  assert.equal(usageOutputTPS({ ...row, first_token_ms: 0 }), 20)
})

test("TPS never manufactures a rate for missing or invalid measurements", () => {
  for (const patch of [
    { stream: false, request_type: 1 }, { billing_mode: "image" }, { billing_mode: "video" },
    { image_output_tokens: 50 }, { inbound_endpoint: "/v1/images/generations" },
    { upstream_endpoint: "/v1/videos?stream=true" },
    { first_token_ms: null }, { first_token_ms: undefined }, { first_token_ms: -1 },
    { first_token_ms: NaN }, { first_token_ms: Infinity }, { duration_ms: 1000 },
    { duration_ms: 0 }, { duration_ms: Infinity }, { duration_ms: NaN },
    { output_tokens: 0 }, { output_tokens: -1 }, { output_tokens: NaN }, { output_tokens: Infinity },
  ]) {
    assert.equal(usageOutputTPS({ ...row, ...patch }), null, JSON.stringify(patch))
  }
})

test("usage header, body and column toggle order stay aligned", async () => {
  const source = await readFile(new URL("../components/gateway/usage-table.tsx", import.meta.url), "utf8")
  const file = ts.createSourceFile("usage-table.tsx", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX)
  let config
  let header
  let body
  function visit(node) {
    if (ts.isVariableDeclaration(node) && node.name.getText(file) === "USAGE_COLUMNS") config = node.initializer
    if (ts.isJsxElement(node)) {
      const tag = node.openingElement.tagName.getText(file)
      if (tag === "TableHeader") header = node
      if (tag === "TableBody") body = node
    }
    ts.forEachChild(node, visit)
  }
  visit(file)
  assert.ok(config && ts.isArrayLiteralExpression(config))
  assert.ok(header && body)
  const ids = config.elements.map((element) => element.properties.find((property) => property.name?.getText(file) === "id").initializer.text)
  const labels = config.elements.map((element) => element.properties.find((property) => property.name?.getText(file) === "label").initializer.text)
  assert.equal(new Set(ids).size, ids.length)
  assert.equal(labels[ids.indexOf("channelRate")], "渠道分组倍率")
  assert.ok(header.getText(file).includes("USAGE_COLUMNS.filter((col) => show(col.id)).map"))
  const rendered = []
  function collect(node) {
    if (ts.isCallExpression(node) && node.expression.getText(file) === "show") {
      assert.equal(node.arguments.length, 1)
      rendered.push(node.arguments[0].text)
    }
    ts.forEachChild(node, collect)
  }
  collect(body)
  assert.deepEqual(rendered, ids)
  assert.deepEqual(ids.slice(ids.indexOf("channelRate"), ids.indexOf("latency") + 1), ["channelRate", "type", "billing", "token", "cost", "latency"])
})
