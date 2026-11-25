# 代码 Wiki

本页给出“在哪个文件、做什么、谁调用、结果交给谁”。类内部算法不在此复述；以源码为准。入口说明见[架构图](./current-architecture.md)，顺序说明见[调用拓扑](./call-topology.md)。

## 实时 Pilot 与 Hook

| 文件 | 主要类/方法 | 做什么与输入 → 输出 | 调用方 → 下游 |
|---|---|---|---|
| `scripts/run-group-pilot.py` | `main()` | 校验 Hook/账号/路由，构造所有依赖，启动 8080 Pilot。配置输入 → FastAPI 服务。 | `scripts/live.ps1` → `create_pilot_app()`。 |
| `pilot.py` | `Pilot`；`offer(item)`、`worker()`、`process(item)`、登录/维护/空闲 worker、`status()` | 接收 `HookObservation` 并写队列；worker 组批、调用上下文和模型；处理发送、恢复、计时与状态查询。 | HTTP callback/生命周期 → Scheduler、Assembler、Runner、Sender。 |
| `pilot.py` | `create_pilot_app(pilot)` | 注册 lifespan、`/callback`、`/health`、`/status`。HTTP JSON → Pilot 处理结果。 | `run-group-pilot.py` → Uvicorn。 |
| `hook_contract.py` | `HookObservation`、`parse_callback(payload)` | 将 Hook 原始回调解析成统一观察事件；原始 JSON → 结构化消息/拒绝异常。 | Pilot callback → `Pilot.offer()`。 |
| `rich_message.py` | `normalize_message()`、引用/图片引用解析 | 把 Hook 的文本、图片、表情、语音和引用载荷归一化；原始观察 → 标准化观察/引用信息。 | `Pilot.offer()`、Assembler → `MemoryStore`/Context。 |
| `conversation_scheduler.py` | `ConversationScheduler`；`enqueue()`、`claim_ready_batch()`、`mark_model_seen()`、重试方法 | 持久化和聚合同一聊天的入站；事件 → receipt/batch/state。 | `Pilot.offer/worker` → Pilot.process。 |
| `hook_outbound.py` | `HookTextSender`；`submit(draft)` | 调用 Hook 发送接口并规范化 HTTP/Hook 回执；`ReplyDraft` → `HookSendResult`。 | Pilot/WeChatTools → Hook HTTP。 |
| `hook_status_recovery.py` | `observation_from_hook_status()` | 将 Hook 状态中可恢复的最新消息转换为候选观察。 | 启动恢复流程 → Pilot 队列。 |
| `contacts.py` | `HookContactResolver.resolve(user_id)` | 按 Hook 联系人接口解析 ID、微信昵称/备注等。ID → `ContactIdentity`。 | Assembler、CrossSession → 名称展示/资料关联。 |

## 消息、上下文与持久资料

| 文件 | 主要类/方法 | 做什么与输入 → 输出 | 调用方 → 下游 |
|---|---|---|---|
| `context_assembler.py` | `MessageContextAssembler.assemble(observations)` | 入站批次 → 归档消息、媒体/引用/历史/跨聊资料、联系人身份和 `ContextWindow` 的 `AssembledBatch`。 | Pilot worker/跨 Session 委托 → Runner。 |
| `context.py` | `ContextTurn`、`ContextWindow`；`model_user_content()`、`model_history_records()`、`fit_context_window()` | 结构化上下文 → 紧凑自然语言文本和裁剪后的模型输入；统一处理消息正文、身份、引用、备注、动态尾块。 | Assembler、Runner、Idle handler → ADK。 |
| `memory_store.py` | `MemoryStore`；归档、搜索、增强、备注、词典、发送人资料、图片/语音方法 | SQLite 消息库/资料库入口；ID/消息/更新 → 记录、搜索结果或逐条写入状态。 | Assembler、CrossSession、WeChatTools、EventBook、TimeScheduler → Context/Agent tools。 |
| `temporal_context.py` | `message_time_context()`、`DailyEnvironmentProvider`、`ShenzhenWeatherProvider` | Hook tick 或当前时间/天气数据 → 消息时间特征/环境尾块。 | Hook contract、Pilot → ContextWindow/系统动态上下文。 |
| `media.py` | `WeChatImageIngestor.ingest()`、`WeChatVoiceIngestor.ingest()`、`image_for_model()` | Hook 图片/语音载荷 → 本地资产和处理状态；GIF 读图时采样最多 4 帧生成 PNG 联系表；异常写为明确错误码。 | Pilot/Assembler 与读图工具 → MemoryStore、本地文件及 ADK 多模态结果。 |
| `models.py` | `ConversationKey`、`InboundEvent`、`ReplyDraft`、`ProcessResult`、状态类型 | 跨模块共享的领域数据结构。字段值 → 类型化对象。 | Gateway/Pilot/Hook adapter → 各层边界。 |

## Agent、模型预算与工具

| 文件 | 主要类/方法 | 做什么与输入 → 输出 | 调用方 → 下游 |
|---|---|---|---|
| `adk_runner.py` | `ScopedAdkRunner.reply(window)`、`summarize_idle_sessions()`、`register_conversation()` | 以账号/聊天为路由管理 ADK Session，加载 `prompts/persona.md`，提供系统指令和动态尾块，运行模型/工具、摘要与 Session 轮换。ContextWindow → 回复文本/工具结果副作用。 | Pilot、跨 Session、空闲 handler → ADK Runner。原名 `adk_mock.py`，已按实时职责更名。 |
| `adk_budget.py` | `BudgetedAdkModel.generate_content_async()` | 核 token、预算全局和图片子额度；图片预算不足时只剥离当前请求副本中的像素；按响应的缓存 token 结算。ADK 请求 → ADK 响应或明确预算/模型异常。 | `ScopedAdkRunner` → Gemini SDK。 |
| `usage.py` | `UsageLedger.reserve/settle/release()` | 按北京时间自然日持久化账号总预算与文本、读图、生图子预算；同一模型请求只写一条账目，预留和已结算金额都计入当日上限。模型/费用数据 → 预留或结算状态。 | Budgeted model、WeChatTools → SQLite usage DB。 |
| `providers.py` | `GeminiProvider.reply(event)` | 非 ADK Provider 接口实现；消息事件 → 回复结果，按费用账本调用 Gemini。 | `LLMProvider` 路径；实时 Pilot 当前由 ADK Gemini + BudgetedAdkModel 驱动。 |
| `agent_tools.py` | `ReadOnlyToolset`、`AgentToolStatus` | 提供 Gateway 只读状态查询和 Agent 能力状态。 | ADK/Gateway 构造 → 模型只读工具。 |
| `wechat_tools.py` | `WeChatTools`；`commit_agent_turn()`、发送/引用、备注、词典、人物资料、图片库/读图、等待等 | 当前聊天内 Agent 操作入口；自然语言意图及持久 ID → Hook 发送、逐项资料更新、图片库列表、多模态 FunctionResponse 或状态结果；`image_notes` 按 image_id 保存主模型生成的描述。 | `ScopedAdkRunner` 工具注册 → Hook sender、MemoryStore、EventBook、TimeScheduler。 |
| `tool_timing.py` | `with_turn_elapsed()` | 为工具结果附加本轮处理耗时。工具函数 → 包含耗时的结果。 | Agent tool wrapper → ADK 工具返回。 |
| `credentials.py` | `load_key()` / `store_key()` / `delete_key()` | 本机凭据存取。凭据操作 → Key 或完成状态。 | `run-group-pilot.py`、模型烟测脚本 → Gemini SDK；不把 Key 交给日志。 |

## 跨聊天、事件簿与调度

| 文件 | 主要类/方法 | 做什么与输入 → 输出 | 调用方 → 下游 |
|---|---|---|---|
| `cross_session.py` | `CrossSessionService`；`list_conversations()`、`query_messages()`、`dispatch()`、`query_sql()` | 会话发现、按 ID/窗口/关键词/时间范围检索、只读 SQL 和目标 Session 内部委托。查询/任务 → 有来源、按字节预算的结果或目标回复。 | `CrossSessionToolset` → MemoryStore、联系人解析、Pilot dispatch closure。 |
| `cross_session_tools.py` | `CrossSessionToolset.adk_tools()` | 将跨聊天能力包装为自然语言可调用 ADK tools。模型参数 → CrossSessionService 结果。 | `run-group-pilot.py` 注册到主/空闲 Runner。 |
| `event_book.py` | `EventBook.list/render/add/update/delete()` | 维护跨 Session 高优和普通事件；账户范围 → 当前事件簿文本/写入结果。 | Runner 动态尾块、Agent tools → MemoryStore。 |
| `event_book_tools.py` | `EventBookToolset.adk_tools()` | 为模型提供事件簿读取和编辑工具。模型操作 → 每项状态。 | `WeChatTools`/Runner 工具注册 → EventBook。 |
| `time_scheduler.py` | `TimeScheduler.schedule/claim_due/finish/record_inbound()` 等 | 保存可恢复的定时任务和空闲事件记录。时间/作用域/任务 → 到期 Job/状态。 | Pilot workers、WeChatTools → Session maintenance、follow-up、idle handler。 |
| `idle_tools.py` | `IdleActivityToolset.record_idle_activity()` | 强制将空闲轮次产出写成活动摘要。模型总结 → 活动记录状态。 | Idle Runner → TimeScheduler。 |

## Mock Gateway、命令原型与辅助模块

| 文件 | 主要类/方法 | 做什么与输入 → 输出 | 调用方 → 下游 |
|---|---|---|---|
| `app.py` | `create_app()` | 创建 Mock FastAPI Gateway。HTTP health/status → Mock 状态；不调用实时 Hook/Gemini。 | `scripts/start.ps1`/Uvicorn → Gateway。 |
| `service.py` | `Gateway.ingest()`、`dispatch_mock()` | Mock 事件处理、隐私状态门和草稿分发。事件 → `ProcessResult`。 | app/replay/demo → Repository 和 Ports。 |
| `storage.py` | `Repository` | Mock Gateway 的 SQLite 入站、Outbox、隐私状态和健康统计。领域对象 → 持久状态。 | Gateway/Mock ledger → SQLite。 |
| `inbound_queue.py` | `InboundQueue` | 早期 Gateway 的持久队列/批量领取接口。入站事件 → batch claim。 | Gateway/独立测试；不是 Pilot 的 `ConversationScheduler`。 |
| `outbox.py` | `OutboxLedger` | 早期 Gateway 的审批、领取、settle 和异常恢复状态机。Draft/event → 投递状态。 | Gateway/独立测试；实时 Pilot 使用 HookTextSender。 |
| `commands.py` | `CommandRouter`、`parse_command()` | 解析/授权结构化管理命令。文本+证据 → command 或 Agent fallback。 | Mock command application/tests；真实 Pilot 接入情况见运行配置，不因模块存在就代表命令全部上线。 |
| `command_mock.py` | `MockCommandApplication`、`MockCommandExecutor` | 离线管理命令执行器原型。命令 → Mock 结果。 | Command response path/tests。 |
| `command_presentation.py` | `CommandResultProjector`、`ResponseComposer` | 把命令结果筛选/组织成可展示内容。结果 → presentation draft。 | `command_response.py` → Gateway draft。 |
| `command_response.py` | `MockCommandResponder` | 串接命令解析、Mock 执行和展示。事件 → 回复草稿/回退结果。 | Gateway 可选端口。 |
| `ports.py` | `MessageTransport`、`LLMProvider`、`Clock`、`CommandResponder` | 模块间协议。标准输入对象 → 协议定义的响应。 | Mock Gateway 与测试替身。 |
| `mock.py` | `MockProvider`、`MockTransport`、`SystemClock` | Gateway 本地模型、发送和时钟替身。事件 → Mock 回复/发送结果。 | `service.py`、`app.py`。 |
| `hook_ingress.py` | `create_hook_observe_app()` | 独立 Hook 观察接收器。回调 JSON → 计数/状态。 | 探针和契约测试，不等同实时 Pilot callback。 |
| `wechatpyapi_adapter.py` | `parse_message_push()` | 备选 WeChatPYAPI 消息载荷转换。消息 JSON → 标准观察对象。 | 适配器测试；实时脚本当前走 `hook_contract.parse_callback()`。 |
| `replay.py`、`demo.py` | `run()`、`main()` | Mock fixture 回放与本地演示。固定样本 → Gateway 处理结果。 | 开发脚本。 |

## 配置、测试与辅助文件

- `config/models.toml`、`config/agent_profiles.toml`、`config/runtime.example.toml`、`config/sources.example.toml`、`.env.example`、`pyproject.toml`：模型目录、Profile、运行与消息源配置样例及依赖定义；实际账号/Key 由 `.local/` 或系统密钥存储读取。
- `tests/test_*.py`：与同名领域模块对应的自动化测试；测试通过不等于 Hook 对端送达。
- `scripts/hook-smoke-test.ps1`、`scripts/group-probe-report.ps1`、`scripts/send-image-probe.ps1`：诊断/验证脚本；其中发信/媒体探针按脚本用途需谨慎运行。
- `prompts/`：模型角色文本；稳定规则和动态数据还分别由 `adk_runner.py`、`ContextWindow`、`EventBook` 在请求时组装。

### 定时等待工具

`WeChatTools.schedule_followup(task, wait_seconds=30)` 接受 1–60 秒；经 `Pilot._schedule_followup()` 持久化定时任务。若等待期间同一聊天来了新消息，Pilot 会等待自首条新消息起的 60 秒合并窗，再把新消息批次和原任务一起交给 Agent。返回值当前为 `cancel_on_new_message=false`。`wechat_tools.py` 中该方法的简短 docstring 仍写“新消息取消”，与实际返回/实现不一致；理解行为应以 `Pilot._run_scheduled_followup()` 为准。
