# 模型提示词占位模板（开源版）

这里放置运行所需的提示词模板、运行时文本配置与工具说明。开源仓库中的 `.md` 文件均为**精简占位版本**；部署到个人环境时，推荐放入集中私有金库目录 `.local/prompts/`（或兼容目录 `prompts.local/`，均已在 `.gitignore` 中忽略，放入同名文件即可优先覆盖 `prompts/`，不会污染 Git 版本库）后再按需要填写 `persona.md`、`main_agent.md` 等完整人设与策略。

- `persona.md`：助手人格设定占位模板。
- `main_agent.md`：主聊天助手行为说明占位模板。
- `session_summary.md`、`session_summary_system.md`：会话摘要模板（需保留 `{{transcript}}` 占位符）与摘要系统指令。
- `idle_agent.md`、`idle_finalize.md`、`idle_fallback.md`：后台空闲活动相关提示词占位模板。
- `cross_session_task.md`：跨会话内部委托模板（需保留 `{{task_text}}` 占位符）。
- `jev_strategy.md`：决策模型回复策略分类提示模板。
- `runtime_context.json`：上下文组装器使用的动态区块文本与占位键（`runtime_context` 契约字段请保持键名一致）。
- `tool_descriptions.json`：ADK FunctionTool 使用的标准化工具说明。
