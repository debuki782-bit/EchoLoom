# ADR 0006：官方 SDK 的受控文本适配器

状态：2026-10-29 **离线实现，真实 API 未验收**。

按 Google [官方 Python SDK 文档](https://ai.google.dev/gemini-api/docs/libraries)将 `google-genai==2.33.0` 安装在项目 `.venv`，锁定于 `requirements.lock`。`GeminiProvider` 只允许官方已核对的两个文本模型 ID，默认 `enabled=false`，主程序仍使用 Mock Provider；本模块不会自行创建客户端、读取 Key 或联系 Google。

调用时把单条已接收事件转换为含会话和候选发件人的结构化用户角色数据，限制输入字节与输出 token，禁用工具和应用层重试。调用前用[费用账本](0005-usage-reservations.md)预留预算；仅新预留成功时调用 SDK 一次。SDK 异常或用量缺失时保留预留并报告结果不明，不能以同一请求 ID 重试。响应含明确的 prompt/候选安全阻断证据时结算已知用量并抛出独立 `SafetyBlocked`，供后续上下文隔离流程处理；普通空回复或超时不触发隔离。

当前仅用假 SDK 客户端测试了内容、限额、去重、未知结果及安全阻断；**没有真实 API 请求**。仍需本账号 `models.list`、实际价格/配额核对、可信 Key 注入、ADK Runner、真实返回结构兼容测试、预算管理入口以及安全阻断后的用户通知和历史隔离。不能把此模块当作完整 M4。
