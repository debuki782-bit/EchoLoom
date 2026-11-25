# 主要方法调用拓扑

方法级概览。标识符、函数和返回值以源码为准；该图描述静态调用顺序，不代表某次运行一定完成或对端一定收到。

## A. 真实入站消息到 Agent

```mermaid
sequenceDiagram
    participant WX as 微信/Hook
    participant API as Pilot /callback
    participant P as Pilot.offer
    participant Q as ConversationScheduler
    participant W as Pilot.worker
    participant A as MessageContextAssembler
    participant M as MemoryStore/媒体/联系人
    participant R as ScopedAdkRunner
    participant G as Gemini + ADK Tools

    WX->>API: POST /callback (Hook JSON)
    API->>API: parse_callback(payload)
    API->>P: offer(HookObservation)
    P->>Q: enqueue(observation)
    Q-->>P: queued/duplicate/backpressure + receipt
    P-->>API: callback disposition
    W->>Q: claim_ready_batch()
    Q-->>W: MessageBatch
    W->>A: assemble(batch.observations)
    A->>M: archive, media ingest, quote lookup, history, dynamic enrichment
    M-->>A: stored IDs, media status, 50 same-chat + up to 20 other-chat records
    A-->>W: AssembledBatch(ContextWindow, message IDs, quote context)
    W->>P: process(synthetic batch marker)
    P->>R: reply(ContextWindow)
    R->>G: ADK Runner + budgeted Gemini request
    G-->>R: final text or tool events
    R-->>P: reply result / tool side effect completion
```

`ConversationScheduler` 持久化的是 Pilot 入站 receipt/batch。`MessageContextAssembler` 同时写消息归档并组装模型上下文；当批次处理正常完成后，Pilot 更新模型已看状态。失败/超时会交给现有重试状态处理，不应把 Hook 回调 HTTP 成功等同于模型完成。

### 按需读图（同一主模型）

```mermaid
sequenceDiagram
    participant G as ADK 主模型
    participant L as WeChatTools.list_wechat_images
    participant T as WeChatTools.inspect_wechat_image
    participant M as MemoryStore / 本地媒体
    participant B as BudgetedAdkModel
    participant U as UsageLedger
    participant API as Gemini

    G->>L: 当前聊天最近 10 张；需要时显式展开账号目录
    L->>M: 按已持久化的 image_id 分页查询
    M-->>L: ID、来源消息/聊天和已有描述（不读像素）
    L-->>G: 图片目录；约 15.5 KB 上限
    G->>T: 精确 image_id + 观察问题
    T->>M: 稳定 image_id 按账号映射；Hook 引用按当前聊天定位；核对归档状态、长度与 SHA-256
    M-->>T: 原图；GIF 原文件
    T->>T: GIF 抽样最多 4 帧并生成联系表
    T-->>G: ADK FunctionResponsePart（像素）+ image_id
    G->>B: 本轮请求（包括像素与同 Session 历史）
    B->>U: 预留账号总预算 + 图片子预算
    alt 图片预算可用
        B->>API: 发送多模态请求
    else 图片预算耗尽
        B->>B: 只从请求副本移除像素并加入说明
        B->>API: 发送剩余文本请求
    end
    API-->>B: 回复与 token 用量（可能包含缓存 token）
    B->>U: 结算实际用量
    G->>T: commit_agent_turn.image_notes（可选）
    T->>M: 按精确 image_id 保存可复用描述
```

像素会保留在 ADK Session Event 中，以便后续请求继续使用；预算器对副本作的剥离不修改存档 Event。图片描述是模型读图后的可选数据更新，不再由工具里的第二个模型生成。

## B. 模型发消息与批量更新

```mermaid
sequenceDiagram
    participant G as ADK Model
    participant T as WeChatTools
    participant M as MemoryStore/EventBook
    participant S as HookTextSender
    participant H as Hook HTTP
    participant P as Pilot/Context

    G->>T: commit_agent_turn(reply, notes, dictionary, sender, tags, events...)
    T->>M: 按项执行备注/词典/人物资料/标签/事件更新
    T->>S: 如有文本/引用，构造当前会话 ReplyDraft
    S->>H: POST Hook send API
    H-->>S: queued / accepted / rejected / unknown
    S-->>T: HookSendResult
    T-->>G: 每个子操作结果 + 发送状态/原因/耗时
    T->>P: 归档 Agent 出站记录/更新时间/失败提醒状态
```

`commit_agent_turn` 是合并写操作工具：合法项目可成功，单项错误按项返回；`queued` 表示 Hook 排队，不等于微信对端确认。模型也可能直接返回普通文本，由 Pilot 的普通回复处理路径发送并归档。

## C. 跨聊天检索和委托

```mermaid
sequenceDiagram
    participant A as 发起 Session Agent
    participant T as CrossSessionToolset
    participant S as CrossSessionService
    participant DB as MemoryStore
    participant D as Pilot dispatch closure
    participant B as 目标 Session Agent

    A->>T: list/search/read/delegate(...)
    T->>S: 结构化工具参数
    S->>DB: 会话列表、LIKE 消息查询、富化上下文/资料
    DB-->>S: 带会话/发送人/时间/ID 的记录
    alt delegate
        S->>D: dispatch(target window, task)
        D->>Pilot: 领取目标聊天未处理 batch（若有）并组装
        D->>B: Runner.reply(目标上下文+任务)
        B-->>D: 目标 Session 结果
        D-->>S: 返回目标结果/状态
    end
    S-->>T: 有界、标注来源的结果
    T-->>A: ADK tool response
```

会话搜索的“读到其他聊天”与“向当前聊天转述”是两个决定。目标 Session 的工具上下文路由到目标聊天；实际微信发送仍是该目标 Session 自己调用发送工具的副作用。

## D. 启动恢复、Session 维护和空闲活动

```mermaid
flowchart TD
    APP[create_pilot_app lifespan] --> W[worker]
    APP --> LOGIN[login_watch_worker]
    APP --> MAINT[session_maintenance_worker]
    APP --> IDLE[idle_event_worker]
    LOGIN -->|Hook 登录/账号检查通过| INIT[initialize_after_login]
    INIT --> BACKLOG[recover_archived_backlog / recover_failed_after_login / Hook status recovery]
    BACKLOG --> SCHED[ConversationScheduler]
    W -->|当前队列优先| BATCH[消息批次处理]
    MAINT -->|8h、已登录、无待处理消息| SUMMARY[summary_delegate / summarize_idle_sessions]
    SUMMARY --> NEWSESSION[ADK Session generation 轮换 + 最近历史组装]
    IDLE -->|30–60 分钟随机到期且就绪| IDLECB[run_idle_event]
    IDLECB --> RECENT[最近 100 条消息 + 最近 20 条空闲总结]
    RECENT --> IRUN[隔离 idle Runner / 3.5 Flash-Lite]
    IRUN --> REC[record_idle_activity 强制活动记录]
    BATCH -.新入站优先/中断.-> MAINT
    BATCH -.新入站优先/中断.-> IDLE
```

`TimeScheduler` 保存任务时点和状态，Pilot worker 执行到期处理。会话总结不在未登录或消息待处理时运行；空闲任务是独立 Session，并要求写入活动记录。

## E. Agent 主动等待与同聊新消息合并

```mermaid
sequenceDiagram
    participant A as 当前 Session Agent
    participant T as WeChatTools.schedule_followup
    participant P as Pilot._schedule_followup / _run_scheduled_followup
    participant TS as TimeScheduler
    participant Q as ConversationScheduler
    participant R as ScopedAdkRunner

    A->>T: schedule_followup(task, wait_seconds=30)
    T->>P: 默认 30 秒，允许 1–60 秒
    P->>TS: 持久化 followup due time
    P-->>A: scheduled
    Note over P: 等待到期；如果同聊新消息先到，启动 60 秒合并窗
    Q-->>P: 目标聊天的新消息批次
    P->>Q: claim_conversation_batch(account, conversation)
    Q-->>P: 按时间排序的消息 batch
    P->>R: 原任务 + 合并消息重新激活当前聊天 Session
    R-->>A: 本轮模型结果/工具操作
```

按当前实现，等待期间新消息不直接取消任务；在首条新消息后给该聊天 60 秒聚合，再合并唤醒。返回字段明确为 `cancel_on_new_message=false`。工具方法 docstring 仍称新消息会取消，代码与注释不一致，维护时应以 `_run_scheduled_followup()` 和实际返回对象为准。

## F. 消息处理失败与发送失败

```text
模型调用前预算不足 ──> 不调用模型，批次记为预算失败/状态可见
模型异常/超时       ──> Pilot 释放或重试当前 batch（有上限/退避）
发送明确拒绝        ──> 原因返回工具/当前处理逻辑，按已配置上限停止
发送 unknown        ──> 标记结果未知；避免把未知当成未发送而无限重发
发送 queued         ──> 记录 Hook 已排队；不宣称对端送达
连续发送终止失败    ──> tool-receipts.sqlite3 写待办提醒；下一条真实消息再给 Agent
```

具体状态枚举和次数上限见 `pilot.py`、`wechat_tools.py`、`hook_outbound.py`；此处只列责任流向，不复述其状态机细节。
