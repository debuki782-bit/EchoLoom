[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

面向真实聊天渠道的本地无人值守、事件驱动 Agent 运行时。

把大模型 API 接到聊天 Webhook 只需要几十行代码；但要让一个 Agent 在私聊和群聊里连续运行几周——不在网络抖动后发重、不在对方补话后发出过时草稿、不被群里改昵称套话带偏、不让长期记忆越攒越乱、也不在半夜把 API 额度跑穿——真正麻烦的是这些工程细节。EchoLoom 就是围绕这些问题写出来的一套运行时。

所有对话记录、媒体文件、长期记忆、审计日志与本地密钥都落在磁盘上的 SQLite 和已加入 `.gitignore` 的 `.local/` 目录里，不依赖托管云端存储。

---

## 真实聊天里的边界问题与处理方式

| 常见场景 | EchoLoom 的处理机制 |
|---|---|
| 对方连发多条短消息，或在模型推理中途又补了新信息 | SQLite WAL 持久化收件箱 + 静默窗口合批；新鲜度守卫在推理途中检测到新真人消息时压住陈旧自动回复，避免答非所问。 |
| 群聊里不是每句话都需要插嘴 | 可选前置决策模型（`reply` / `concise` / `no_reply` / `multi_turn`）作为回复策略参考，配合多气泡分段发送与短时跟进唤醒。 |
| 网络波动、超时或进程意外重启 | 崩溃可恢复的收件箱队列、带 `1+3` 有界重试的出站副作用账本，以及同事务内的工具调用幂等去重（`agent_tool_mutations`）。 |
| 群友修改自设昵称诱导身份混淆或注入 | 人工维护的备注（`sender_profiles`）对 Agent 只读；未备注联系人的敏感称呼昵称自动掩码脱敏；所有记忆条目均携带 `untrusted_*` 信任标签。 |
| 长期记忆膨胀、旧待办永不过期、后台整理编造结果 | 按用途分表的结构化 SQLite 记忆，外键强锚定 `source_messages`；Top-40 `fact_key` 目录索引按需检索；带 72 小时过时标记的跨会话事件簿；可选的 `EpisodeMemory` 向量召回与离线合并剪枝（Dreaming）。 |
| 深夜无新消息时定时巡检空转烧钱 | 阶梯式空闲退避（`30m` → `90m` → `3h` → `6h`，24 小时上限 10 次）+ 确定性 SQL 增量盘点；无新增消息且无碎片预警时直接跳过退出，收到新消息立即中断空闲任务让位。 |
| 无人值守下的调用成本失控 | 微美元级每日预算账本，按账号总预算、文本模型、读图与生图分别限额，配合会话自动轮转与 Prompt Cache 回收。 |

---

## 记忆分层设计

EchoLoom 没有把所有聊天记录塞进单一向量库，而是按“怎么召回、多久失效、谁能修改”拆成几类互不混用的本地存储：

- **原始消息档案（`source_messages`）**：唯一事实源，关联图片描述、语音转写、文件摘要，并通过平台消息 ID 精确反查引用原句。
- **人物档案与标签（`sender_profiles`、`sender_memories`、`sender_tags`）**：将人工维护的备注与 Agent 沉淀的人物资料严格隔离，标签通过追加式 diff 支持按历史时间点还原。
- **词典（`entity_terms`）**：针对群内黑话、别名、项目代号做确定性词边界正则全量匹配，补足向量检索对专有简称不稳的短板。
- **账号级长期事实（`user_facts`）**：按 `fact_key` 版本化管理并强绑定 `source_message_id`。上下文尾块只注入最近更新的 40 个 `fact_key` 作为目录，正文由模型按需调用 `search_user_memory` 查取。
- **跨会话事件簿（`event_book`）**：跨私聊与群聊共享的有界工作记忆（高优限 4 条、普通事件必填退出条件，超 72 小时未更新自动标为待复核）。
- **可选向量事件记忆（`episode-memory.sqlite3`）**：通过 `ECHOLOOM_EPISODE_RAG=1` 开启，用于补足近 50 条滑窗之外的模糊历史回忆；召回时自动剔除已在当前滑窗内出现的来源消息，并在空闲周期对条目 `>= 12` 的会话执行分阶段离线合并剪枝。

---

## 本地工作台（`echoloom dashboard`）

内置无前端构建依赖的本地管理界面（仅限 `127.0.0.1` / `localhost` 访问，开启严格 CSP 与同源写校验），无需手动连 SQLite 就能排查和纠偏：

- **会话记录与工具调用**：只读查看每次合批输入、完整模型请求快照、工具参数/回执与 Session 轮转边界，支持导出 JSONL。
- **工具管理**：实时开关主聊天与空闲 Agent 的已注册工具。
- **人物档案**：维护人工 `Remark`，查看单人资料碎片预警（`>= 8` 条），展开后直接增删联系人标签与人物资料。
- **记忆库**：集中浏览、搜索、溯源原消息正文、原地编辑或删除 `user_facts`（直观标识是否在 Top-40 索引内）、`event_book`（含高优槽位与 72h 预警）、`entity_terms`、`annotations` 与 `episodes`。结构化记忆修改通过 `MemoryStore` / `EventBook` 领域层落库以保留修订或审计记录；删除事件记忆则会直接从 `episode-memory.sqlite3` 中移除。

---

## 架构与多渠道解耦

核心运行层不依赖任何聊天平台 SDK。所有渠道均实现在 [`ChannelConnector`](src/echoloom/connectors/base.py) 协议之后，整个代码库的 6 层单向依赖由 [`tests/test_architecture_boundaries.py`](tests/test_architecture_boundaries.py) 中的 AST 边界测试强制校验。

| 渠道 | Factory | 能力说明 |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | 通用 HTTP JSON 入站、Bearer / HMAC-SHA256 验签、SSRF 防护、主动推送 |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Secret-Token 验签、文本/图片/语音/引用解析、Bot API 发送 |
| 飞书 / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event 2.0、Challenge 与签名校验、租户 Token 缓存与媒体收发 |
| Discord | `echoloom.connectors.discord:DiscordConnector` | 附件与引用消息解析、Bot REST 发送 |
| 微信 Hook（可选） | `echoloom.connectors.wechat_hook:WeChatHookConnector` | Windows 桌面微信适配层，仅在显式启用时加载 |

---

## 快速开始

需要 Python 3.12+。

```bash
# 1. 可编辑模式安装
pip install -e .

# 2. 初始化 gitignored 本地配置目录 (.local/runtime.toml, sources.toml, models.toml)
echoloom init

# 3. 配置模型密钥（Windows 下也可使用 `echoloom credentials store`）
export GOOGLE_API_KEY="your-key"    # 或使用 OPENAI_API_KEY

# 4. 启动运行时（本机 HTTP Ingress + Worker）
echoloom serve

# 5. 在另一终端发送一条测试事件
echoloom send --text "你好 EchoLoom"
```

常用运维命令：

```bash
echoloom serve --role ingress   # 将 HTTP 接入与模型 Worker 拆分为独立进程
echoloom serve --role worker
echoloom dashboard              # 启动本地 Trace 与记忆管理工作台
echoloom vault audit            # 检查 .local/ 隔离状态，确保密钥未进入 git 追踪
```

对话模型、记忆整理模型、多模态理解模型及可选决策模型均可在 `.local/models.toml` 或环境变量中直接替换，无需改代码。

---

## 更多文档

- [多源连接器与 HTTP 入站](docs/connectors-and-sources.md)
- [模型角色与 Profile 配置](docs/agent-profile-config.md)
- [决策模型配置](docs/jev-strategy.md)
- [计费与预算审计](docs/pricing-audit.md)
- [命令与运维手册](docs/commands.md)

## 许可协议

[0BSD](LICENSE)
