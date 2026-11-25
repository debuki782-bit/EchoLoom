# ADR 0003：SQLite 出站账本第一步

状态：2026-10-29 采用；**仅离线状态机，不接真实微信发送**。

## 决定

- schema v2 新增 `outbox`，与现有 `drafts` 一对一。v1 迁移前通过 SQLite backup API 在同目录生成 `.pre-v2-*.bak`；旧草稿映射为待审批，旧 Mock 发送保持 `mock_sent`，旧丢弃映射为取消。
- 新草稿先进入 `awaiting_approval`。内部 `OutboxLedger.approve` 需要显式调用和目标会话白名单，再转 `ready`。`claim` 只把一条 `ready` 草稿持久化为 `sending` 并返回草稿；它不调用 Hook。
- 只有外部确认结果才能将 `sending` 记为 `sent`。结果不明记 `unknown`；独占启动恢复时将遗留的 `sending` 也记为 `unknown`，不自动重发。
- 群暂停取消本群未发送的草稿及 `ready` 项；已进入 `sending` 的项记 `unknown`，因为无法断言外部发送是否发生。发送前再次检查群状态和 `privacy_epoch`。
- 原 M2 `dispatch_mock` 仅能处理尚未进入真实发送状态的 Mock 草稿，成功后账本记为 `mock_sent`。

## 验证与边界

项目测试 41 passed，ruff 与 mypy PASS。测试覆盖 v1 备份和数据保留、无白名单不可领取、重启后 `unknown` 不可再次领取、群暂停前后取消与状态不确定性。

尚缺真实 Hook Adapter、身份稳定 ID、回调快速入队、审批 API 的本地鉴权、跨进程独占/租约、实际送达证据、会话队列与背压。当前 `approve` 是内部方法，不能作为对外授权接口。`sent` 只表示调用方提交的确认结果，真实实现必须定义可核验的成功证据。M3 未通过。
