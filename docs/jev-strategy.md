# Jev 回复策略

普通入站消息先在队列中按 2 秒静默窗口合并；当前批次交给模型前，会为其中每条入站消息并发请求一次 Jev，最多等待 4 秒。Jev 请求遵循进程的 `HTTP_PROXY` / `HTTPS_PROXY` 环境配置，以支持本机代理访问。等待 Jev 期间新到的消息继续持久化排队，不会改写已领取的当前批次；当前批次处理完成后，队列再将它们合并成后续批次。届时已返回的判断会随请求上下文交给 Gemini；超时、缺少 API Key 或接口出错时，正常继续原有回复流程。

Jev 每条消息输出三项互斥概率：`reply`、`concise`、`no_reply`，显示值总和为 100%。玩笑语境应体现在是否展开或简短回应中，不再单独输出玩梗类别。它还独立判断是否确实需要拆成多条微信消息；`multi_turn` 概率达到 50% 才显示，多轮字段不会被 `no_reply` 判断吞掉。Jev 结果是参考，不代替人物设定，也不直接决定是否发送。

输入包括精简人物设定、聊天类型、联系人关系与 Tag、词典、最新目标消息之前最多五条历史、相关跨聊召回、事件背景和当前时间。每条消息的上下文单独聚焦，明确标出最新目标消息；目标消息之后的消息不加入输入。以上被选中的上下文会发送到 TypeSafe Jev API。

## 配置 API Key

可通过环境变量 `TYPESAFE_API_KEY` 提供 Key，也可存入当前 Windows 用户的凭据管理器。后者不会把 Key 写进项目文件：

```powershell
.\.venv\Scripts\python.exe -m wechat_agent.credentials store --service jev
.\.venv\Scripts\python.exe -m wechat_agent.credentials status --service jev
```

删除凭据时执行 `delete --service jev`。未配置 Key 时日志会记录 `jev_disabled`，Agent 不会因此失败。

## Agent CLI 管理提示词

Agent 可在任意聊天或空闲活动中用 `list_project_resources` / `read_project_resource` 查看 `prompts/jev_strategy.md`，然后用 `update_own_prompt` 做精确文本修改。修改前需要读取 SHA-256，修改后自动保存本地快照。`list_prompt_revisions` 查看最近 20 个快照；`rollback_own_prompt` 默认恢复最近快照，也可以指定列表里的 `revision`。每次回滚也会先保存当前版本，因此可再次回滚。快照放在本地 `.prompt-revisions/`，不依赖 Git。
