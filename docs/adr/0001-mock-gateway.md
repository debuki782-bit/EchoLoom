# ADR 0001：模块化单体与 Mock 边界

日期：2026-10-28。状态：M2 初版。

## 决定

- 一个 Python 进程，FastAPI 仅提供 `127.0.0.1` 上的只读健康/状态路由；真实入站接入前没有 HTTP 写入接口。
- `models`/`ports` 定义传输无关数据与端口；`service.Gateway` 负责入站策略、持久化、Provider、草稿和 Mock 发送；`storage.Repository` 实现 SQLite；`mock` 提供唯一可用的传输和 Provider。当前没有 Hook Adapter 或 ADK 集成。
- SQLite 使用 WAL、外键、版本化迁移、`BEGIN IMMEDIATE` 短事务，锁等待上限 0.5 秒。schema v2 出站账本与 v3 入站队列见 [ADR 0003](0003-durable-outbox-scaffold.md)和 [ADR 0004](0004-durable-inbound-queue.md)。Provider 在事务外调用，回写前重查隐私 epoch；Mock 发送前也重查。控制事件只保存必要元数据，不保存暂停期间聊天正文。
- 默认 `observe`；Mock 演示中的 `mock_sent` 仅表示假传输记录，绝不代表真实微信送达。
- 入站、Provider、Outbox 在未来接入真实组件时保持端口隔离；未知发送状态不能自动重复发送。
- 本切片故意只提供串行进程内 Mock；有界工作队列、会话级隔离调度、异常分类和预算扣减留待扩充。锁冲突会报错并保留原入站供显式重试；重启后未发草稿保持待处理，不自动重发。

## 信任边界

```text
合成事件 -> Gateway 校验/隐私闸门 -> SQLite Inbox -> Mock Provider
                                               -> ReplyDraft -> Mock Transport
FastAPI 127.0.0.1 -> 只读健康/状态
```

外部聊天正文始终是不可信输入；M2 不把它解释为命令执行或文件路径。`/ai off` 与授权 `/ai on` 是本地确定性隐私控制，不交给 Provider。权限与持久化完整门禁仍需后续扩充测试。

2026-10-29 增补：群事件必须携带由内部适配层构造的 `GroupTrust` 证据，分别证明来源已认证、发送者来自原生字段、成员身份已核实、来源事件 ID 稳定；任一项缺失都在 Inbox 之前失败关闭。Mock fixture 显式提供合成可信证据，真实 Hook 当前的正文前缀群 sender 与 `msgsvrid=0` 不能通过该闸门。紧急隐私指令只识别整条原生文本的精确小写形式，拒绝换行、拼接与同形字符。
