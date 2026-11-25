# Gemini 模型费用核验（2026-11-01）

官方来源：[Gemini Developer API 定价页](https://ai.google.dev/gemini-api/docs/pricing)，核对日期 2026-11-01。以下为 **Gemini Developer API / Standard / Paid** 的公开标价，币种 USD，单位为每 100 万 token；不代表本账号已开通 Paid 层级、实际配额或最终账单。

| 模型 | 输入 | 输出（含 thinking token） | 生效区间 |
|---|---:|---:|---|
| `gemini-3.5-flash-lite` | $0.30 | $2.50 | 2026-10-29 页面现价；页面未列切换日期 |
| `gemini-3.8-flash` | $0.75 | $3.75 | 截至 2026-12-31 |
| `gemini-3.8-flash` | $1.50 | $7.50 | 自 2027-01-01 起 |
| `gemini-3.1-flash-lite-image` | $0.25 输入 | $1.50 文本输出 + $30 图像输出 | 1K 图像约 $0.0336/张 |

`gemini-3.5-flash-lite` 的定价页将输入费率列为 $0.30/百万 token，覆盖文本、图片、视频和音频；官方[模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite)确认其支持图片输入与 function calling。因此主 Agent 看图仍使用同一个主模型，无需再设视觉模型。当前预算器对含图请求按该次整次模型费用记入读图子额度，避免错误声称可从 API 用量中精准拆出像素费用。

上述价格不包括可能单独计费的 Search grounding、缓存存储、批处理、Flex、Priority 和其他服务；启用这些能力前需另核。3.8 Flash 的标准价格将在 2027-01-01 调整，当前账本按日期选取费率。

两个模型的官方[3.5 Flash-Lite 模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite)和[3.8 Flash 模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)均列出稳定 API ID。**账号可用性、地区、配额、计费层级仍 UNVERIFIED**；本轮未调用 `models.list`、未读取或保存 Key，也未产生模型请求。真实接入前按官方 API 列表复核本账号可用模型，并用账本预留费用。
