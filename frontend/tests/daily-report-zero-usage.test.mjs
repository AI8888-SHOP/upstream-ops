import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import test from "node:test"

test("daily report exposes zero usage separately from unknown pricing at each level", async () => {
  const [page, types] = await Promise.all([
    readFile(new URL("../app/daily-report-page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../lib/daily-report-types.ts", import.meta.url), "utf8"),
  ])
  assert.ok(types.includes("zero_usage_requests: number"))
  assert.ok(page.includes("metrics.zero_usage_requests > 0"))
  assert.ok(page.includes("metrics.unpriced_requests > 0"))
  assert.ok(page.includes("零用量异常"))
  assert.ok(page.includes("不代表上游实际没有扣款"))
  assert.ok(page.includes("其他尝试和已有费用仍正常计入"))
  assert.ok(page.includes("有 Token 但缺价格的记录仍待计价"))
  for (const level of ["day", "source", "group", "model"]) {
    assert.ok(page.includes(`<ProfitValue metrics={${level}}`), `missing ${level} anomaly display`)
  }
})

test("historical repricing updates revenue as well as costs without rebilling", async () => {
  const [page, prices, report, storage] = await Promise.all([
    readFile(new URL("../app/daily-report-page.tsx", import.meta.url), "utf8"),
    readFile(new URL("../components/gateway/prices-panel.tsx", import.meta.url), "utf8"),
    readFile(new URL("../../backend/storage/gateway_daily_report.go", import.meta.url), "utf8"),
    readFile(new URL("../../backend/storage/model_pricing.go", import.meta.url), "utf8"),
  ])
  assert.ok(page.includes("历史成本、收入和利润会一起重算"))
  assert.ok(page.includes("实际 Key 扣款、余额和结算凭据不变"))
  assert.ok(prices.includes("重算历史费用与利润"))
  assert.ok(!page.includes("优先使用历史结算快照"))
  assert.ok(report.indexOf("WHEN u.reference_pricing_version > 0 THEN u.reference_sales_base") < report.indexOf("WHEN f.request_id IS NOT NULL THEN"))
  assert.ok(storage.includes("EnsureReferenceSalesRepricing"))
  assert.ok(storage.includes("job.ReferencePricingVersion >= ReferencePricingVersion"))
})
