# ADR 0004：入站队列和复合去重键

状态：2026-10-29 采用为 **离线 M3 切片**，未接真实回调或模型。

schema v3 增加 `inbound_queue`，唯一键为 `(transport, account_id, source_id)`。从 v2 升级前用 SQLite backup API 备份；v1 经 v2、v3 两步迁移，每步有独立备份。队列持久化文本、会话和隐私 epoch，容量默认 1024 条，最多 4 个不同会话同时处于 `running`，同一账号与会话只领取一条。重复相同事件返回 `duplicate`，同键不同内容返回 `source_id_conflict`，队列满返回 `backpressure`；这些结果不调用 Provider。

只有内部确认的来源认证和稳定来源 ID 才可入队。群事件还须有完整 `GroupTrust`；精确隐私控制必须走更早的隐私闸门，不进入普通队列。群暂停在同一事务中取消该群尚未完成的队列项；领取时再次检查状态及 epoch。暂停期间正文不落入队列表。

`claim_ready_batch` 使用可持久化的 `received_at` 计算会话窗口：默认最后一条后静默 2 秒即可领取，最早一条等待 10 秒后强制释放；达到 20 条也立即释放。窗口用所选事件的最早/最晚时间，避免乱序时间导致提前放行。同一会话只运行一个批次，跨会话最多 4 个批次。领取和批量完成均在 SQLite 事务中；群暂停后，已领取批次不能完成。Fake Clock 测试验证静默窗口、最长等待与并行隔离。`claim_next` 仅是离线低层即时领取入口，不是未来生产 Worker 的聚合入口。

同一进程的 Repository 使用可重入锁统一保护 SQLite 事务和状态读取，避免多线程回调在共享连接上同时执行 `BEGIN`。20 个并发合成回调的入队测试通过；这不替代跨进程队列/租约验证。

旧 Mock `Gateway` 的 v1 `inbox` 仍以单一 `event_id` 为主键；现在遇到相同 ID 但账号、会话、发件人或内容不同会返回 `event_id_conflict`，不再静默当作重复。新 `inbound_queue` 的复合键才允许不同账号/传输复用来源 ID；真实 Hook 尚未接入任一路径。

当前 Hook 回调无来源认证，`msgsvrid=0`，故**不能调用该队列的可信入站路径**。队列领取只负责调度，不会重建群授权证据；从数据库取出的群事件 `group_trust=None`，不能直接交给 `Gateway.ingest` 或命令路由。`running` 项的异常重启租约/恢复、回调 HTTP 503 映射、真实 Hook Adapter 和可取消的处理 Worker 仍未实现。不能把此切片当作真实 M3 通过。

验证：项目测试 63 passed，ruff/mypy PASS；含 v1→v2→v3 备份、跨账号/传输去重、冲突、背压、会话串行、聚合窗口、乱序时间、重启存留和群暂停不保存正文。
