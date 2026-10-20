# Model Pricing Data

与参考项目 sub2api 对齐的模型单价数据。

## 来源

- 主表：`model_prices_and_context_window.json`（LiteLLM 格式），同步自
  `tmp/sub2api/backend/resources/model-pricing/model_prices_and_context_window.json`
- 上游：https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json
- 硬编码回退：`pricing.go` 中 `seedFallbackPrices` / `fallbackForModel`，对齐
  `tmp/sub2api/backend/internal/service/billing_service.go`（如 `grok-4.5` 不在 LiteLLM 表中）

## 解析顺序

1. DB 价格覆盖（`model_price_overrides`）
2. 本目录 JSON 精确/模糊匹配
3. 硬编码家族回退（Grok 等）

## DeepSeek

官方 `litellm_provider=deepseek` 条目（含 `deepseek/…` 前缀）与常见裸名别名
（`deepseek-v3`、`deepseek-r1`、`deepseek-v4-flash` 等）已写入本表。
无精确表项时，`pricing.go` 的 DeepSeek 家族前缀回退会落到对应主型号。

## 更新

管理端现在支持一键从 sub2api 官方仓库同步价格，并自动重算历史参考成本。
同步策略、上下文档位、5m/1h 缓存写入和不可变结算边界见
[模型价格同步与历史重算](../../../docs/model-pricing.md)。下列方式仅用于更新发行包的内置兜底表。

```bash
# 从参考项目同步（精选子集，DeepSeek 较少）
cp tmp/sub2api/backend/resources/model-pricing/model_prices_and_context_window.json \
  backend/gateway/pricing/default_prices.json

# 再从 LiteLLM 主表补全 litellm_provider=deepseek 条目与常见别名
# （见仓库内补价脚本或手工合并 /tmp litellm json）
```
