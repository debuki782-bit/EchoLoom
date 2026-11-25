# ADR 0007：ADK 离线 Runner 与会话分区

状态：2026-10-29 **离线 Mock 切片**。

将官方 `google-adk==2.13.0` 锁定在项目环境。`MockAdkRunner` 使用真实 ADK `LlmAgent`、`Runner`、`InMemorySessionService`，只注入 `DeterministicLlm`；没有凭据、网络模型、工具或微信发送。上下文以用户角色的结构化数据传入，固定指令与用户消息保持独立。会话键由账号 ID 与会话 ID 的哈希组合生成：同一私聊历史可续接，不同账号、私聊和群聊互不串读。

另以 `SQLAlchemy[asyncio]==2.1.6` 和 SQLite 启用 ADK `DatabaseSessionService`；FakeLLM 测试确认 Runner 重建后同一会话可读取先前消息，数据仅落在测试目录。当前主程序尚未启用该持久会话，也没有费用预留插件接入 ADK 真实模型调用。后续需预算与每次 LLM 调用的硬门禁、工具白名单、会话删除/保留期和真实模型小范围验收。

Windows 沙箱内 `asyncio.run` 的本地唤醒 socket 无法建立，测试需在本机授权环境运行；这不是云端访问。实测项目测试 83 passed，ruff、mypy、`pip check` 通过。
