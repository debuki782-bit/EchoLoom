# Jev 回复策略

普通入站消息先在队列中按 2 秒静默窗口合并；当前批次交给模型前，会为其中每条入站消息并发请求一次决策模型，最多等待 4 秒。请求遵循进程的 `HTTP_PROXY` / `HTTPS_PROXY` 环境配置，以支持本机代理访问。等待期间新到的消息继续持久化排队，不会改写已领取的当前批次；当前批次处理完成后，队列再将它们合并成后续批次。届时已返回的判断会随请求上下文交给主模型；超时、缺少 API Key 或接口出错时，静默跳过并直接由主大模型自行判断回复，无需额外降级分支。

Jev 每条消息输出三项互斥概率：`reply`、`concise`、`no_reply`，显示值总和为 100%。它还独立判断是否需要拆成多条连续气泡；`multi_turn` 概率达到 50% 才显示。Jev 结果仅作参考，不代替人物设定，也不直接决定是否发送。

## 快速更换决策模型与端点

默认使用 `gemini-decision-1`（`https://generativelanguage.googleapis.com/v1beta/decisions`）。可在不改代码的情况下通过以下任一方式快速替换：

1. **在 `.local/models.toml`（或 `config/models.toml`）覆盖 `[roles]`**：
   ```toml
   [roles]
   decision_model = "gemini-decision-1"
   decision_endpoint = "https://generativelanguage.googleapis.com/v1beta/decisions"
   ```
2. **通过环境变量覆盖**（优先级高于配置文件）：
   ```bash
   export ECHOLOOM_DECISION_MODEL="gemini-decision-1"
   export ECHOLOOM_DECISION_ENDPOINT="https://generativelanguage.googleapis.com/v1beta/decisions"
   ```

## 配置 API Key

复用 Gemini API Key（环境变量 `GEMINI_API_KEY` 或系统凭据 `--service model`）。未配置 Key 时日志记录 `jev_disabled`，Agent 会跳过前置判断直接由主模型处理。

## Agent CLI 管理提示词

Agent 可用 `list_project_resources` / `read_project_resource` 查看 `prompts/jev_strategy.md`，并用 `update_own_prompt` 做精确文本修改（带 SHA-256 校验与 `.prompt-revisions/` 本地快照回滚）。
