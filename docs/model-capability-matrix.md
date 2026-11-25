# 模型与 ADK 初筛（2026-10-28）

| 目标 | 官方公开资料 | 本机/账号状态 |
|---|---|---|
| Gemini 3.5 Flash-Lite | [模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.5-flash-lite)列出 `gemini-3.5-flash-lite`，输入文本/图片/视频/音频/PDF，输出文本，支持工具调用和 Search grounding | ID 已有官方文档证据；账号可用性/地区/配额 **UNVERIFIED** |
| Gemini 3.8 Flash | [模型页](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)列出 `gemini-3.8-flash`，输入文本/图片/视频/音频/PDF，输出文本，支持工具调用和 Search grounding | ID 已有官方文档证据；账号可用性/地区/配额 **UNVERIFIED** |
| Google GenAI SDK | [官方文档](https://ai.google.dev/gemini-api/docs/libraries) | `google-genai==2.33.0` 已装入项目虚拟环境；假客户端文本适配器测试通过，真实账号/API 未验证 |
| Google ADK | [官方文档](https://google.github.io/adk-docs/) | `google-adk==2.13.0` 已装入项目虚拟环境；真实 ADK Runner + FakeLLM 的离线会话隔离测试通过，真实模型未接入 |

两个目标模型均不原生输出音频或生成图片；ASR/TTS/生图须另选经官方核实的模型。实际 API `models.list`、工具共用限制和费用需在接入前再验证。
