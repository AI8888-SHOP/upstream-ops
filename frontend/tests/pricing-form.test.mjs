import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"
import ts from "typescript"

// Exercise the real pure helpers in CI without loading unrelated UI imports.
const source = await readFile(new URL("../components/gateway/gateway-utils.ts", import.meta.url), "utf8")
const file = ts.createSourceFile("gateway-utils.ts", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS)
const names = new Set(["emptyPriceTier", "tierPriceFields", "priceTiersFromJSON", "priceTiersToJSON", "MTOK", "perTokenToMTok", "mTokToPerToken"])
const declarations = file.statements.filter((node) => {
  if (ts.isFunctionDeclaration(node)) return names.has(node.name?.text)
  return ts.isVariableStatement(node) && node.declarationList.declarations.some((declaration) => names.has(declaration.name.getText(file)))
})
assert.equal(declarations.length, names.size, "pricing helper declarations must remain discoverable")
const compiled = ts.transpileModule(declarations.map((node) => node.getText(file)).join("\n"), {
  compilerOptions: { target: ts.ScriptTarget.ES2022, module: ts.ModuleKind.ES2022 },
}).outputText
const { emptyPriceTier, priceTiersFromJSON, priceTiersToJSON, perTokenToMTok, mTokToPerToken } = await import("data:text/javascript;base64," + Buffer.from(compiled).toString("base64"))

test("price units preserve dollars per million and explicit zero", () => {
  for (const value of [0, 0.0000001, 0.00000125, 0.000015, 0.005]) {
    assert.ok(Math.abs(mTokToPerToken(perTokenToMTok(value)) - value) <= Math.max(1e-20, value * 1e-12))
  }
  assert.equal(perTokenToMTok(0.00000125), "1.25")
  assert.equal(mTokToPerToken("1.25"), 0.00000125)
  assert.equal(perTokenToMTok(0), "0")
})

test("context tier round trip preserves TTL, inheritance and threshold semantics", () => {
  const tiers = [
    { above_input_tokens: 128000, inclusive: true, input_price_per_token: 0.000003, cache_read_price_per_token: 0, cache_creation_1h_price_per_token: 0.000006 },
    { above_input_tokens: 200000, inclusive: false, output_price_per_token: 0.00003, cache_creation_price_per_token: 0.0000075 },
  ]
  const form = priceTiersFromJSON(JSON.stringify(tiers))
  assert.equal(form[0].input_mtok, "3")
  assert.equal(form[0].cache_create_1h_mtok, "6")
  assert.equal(form[0].cache_read_mtok, "0")
  assert.equal(form[0].output_mtok, "")
  const saved = JSON.parse(priceTiersToJSON(form))
  assert.deepEqual(saved, tiers)
  assert.equal(Object.hasOwn(saved[0], "output_price_per_token"), false)
  assert.equal(Object.hasOwn(saved[1], "cache_creation_1h_price_per_token"), false)
  const freeHour = JSON.parse(priceTiersToJSON([{ ...emptyPriceTier(), above_input_tokens: "100", cache_create_1h_mtok: "0" }]))[0]
  assert.equal(freeHour.cache_creation_1h_price_per_token, 0)
})

test("context tier input rejects ambiguous thresholds and non-finite prices", () => {
  for (const threshold of ["", "0", "-1", "1.5", "NaN", "Infinity", "9007199254740992"]) {
    assert.throws(() => priceTiersToJSON([{ ...emptyPriceTier(), above_input_tokens: threshold }]))
  }
  assert.throws(() => priceTiersToJSON([
    { ...emptyPriceTier(), above_input_tokens: "200000" },
    { ...emptyPriceTier(), above_input_tokens: "200000" },
  ]))
  for (const value of ["-0.1", "NaN", "Infinity", "free"]) {
    assert.throws(() => priceTiersToJSON([{ ...emptyPriceTier(), above_input_tokens: "100", input_mtok: value }]))
  }
})
