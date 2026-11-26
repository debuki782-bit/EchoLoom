[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

**面向真实聊天场景的长期无人值守多渠道 Agent 运行时。**

EchoLoom 将 Gemini 或兼容 OpenAI 接口的大模型接入私聊、群聊、定时跟进与空闲整理循环，把消息防丢、幂等防重发、预算控制与持久记忆这些决定“能不能长期挂机”的工程细节全部做在核心里。

## 它能用来做什么

- **7×24 个人助手 / 团队协作 Bot**：跨单聊、群聊响应消息，自动管理跟进提醒、图片语音解析与长短期记忆。
- **长期无人值守运行**：专为网络波动、进程重启、推理途中突发新消息等真实环境设计，重启可自动恢复现场。
- **数据与密钥完全本地掌控**：消息、媒体、知识库全部落在本地 SQLite，`.local/` 配置与密钥与代码仓库完全隔离。

## 核心特点

- **消息不丢、发送不重**：基于 SQLite WAL 的持久化收件箱 + 静默窗口合并，配合出站副作用账本与 `1+3` 有界重试，崩溃重启也不丢不重。
- **新鲜度守卫 & 防身份欺骗**：Agent 推理期间若用户又发来新消息，自动挂起过时回复避免答非所问；群聊严格隔离发送者真实身份，防注入假冒。
- **可审计记忆与成本控制**：结构化拆分消息、媒体与知识库，配合每日额度限制、Prompt Cache 生命周期管理与会话自动轮转，避免账单失控。
- **严格解耦的多渠道架构**：所有平台实现在 [`ChannelConnector`](src/echoloom/connectors/base.py) 协议之后，由自动化 AST 分层边界测试守护，核心层零第三方聊天客户端耦合。
- **配置与模型快速热替换**：通过 `.local/models.toml` 或环境变量即可快速切换对话模型、记忆模型以及可选决策模型（`decision_model` / `ECHOLOOM_DECISION_MODEL`）。

## 开箱即用的渠道连接器

| 渠道 | Factory | 能力 |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | 通用 HTTP JSON + Bearer/HMAC-SHA256 验签 + SSRF 防护 |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Bot API Webhook 解析、Secret-Token 验签、图文/语音与引用回复 |
| 飞书 / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event 2.0、Challenge 校验、签名/Token 验证、租户 Token 缓存与传图 |
| Discord | `echoloom.connectors.discord:DiscordConnector` | Gateway/Relay 消息与附件解析、Bot REST 发送与引用 |
| 微信 Hook（可选） | `echoloom.connectors.wechat_hook:WeChatHookConnector` | 仅在启用时加载的 Windows 桌面微信适配层 |

## 快速开始

```bash
# 1. 初始化 gitignored 本地配置仓库 (.local/)
echoloom init

# 2. 配置模型密钥（macOS/Linux 用环境变量；Windows 可用 echoloom credentials store）
export GOOGLE_API_KEY="your-key"    # 或使用 OPENAI_API_KEY

# 3. 启动运行时（Ingress + Worker）
echoloom serve

# 4. 在另一终端发送测试事件
echoloom send --text "你好 EchoLoom"
```

常用运维命令：

- `echoloom serve --role ingress` / `echoloom serve --role worker` — 将 HTTP 接入与模型 Worker 拆分为独立进程。
- `echoloom dashboard` — 启动只读本地 Trace 与记忆观测面板。
- `echoloom vault audit` — 审计仓库，确保无敏感配置或密钥落入 git 追踪范围。

## 更多文档

- [多源连接器与 HTTP 入站](docs/connectors-and-sources.md)
- [模型角色与 Profile 配置](docs/agent-profile-config.md)
- [决策模型配置](docs/jev-strategy.md)
- [计费与预算审计](docs/pricing-audit.md)
- [命令与运维手册](docs/commands.md)

## 许可协议

[0BSD](LICENSE)
