# ADR 0008：真实 Hook 形状的观察入口

状态：2026-10-29 **离线测试通过，尚未接入运行中的 Hook 回调**。

新增独立 FastAPI 应用 `wechat_agent.hook_ingress:app`。`POST /hook/callback` 只接收回环来源、限定 128 KiB 请求体，使用 `parse_callback` 检查入站帧，最近 16 条仅保留账号/会话/发件人摘要、来源标签、类型和长度于内存。`GET /status` 明确返回 `trusted_ingress=false`；不保存正文或完整 wxid，不调用 SQLite Inbox、Agent、模型或微信发送。该应用与主 Mock 服务、目前运行的 8080 探针分开，避免在用户上班期间中断现有回调。

群文本若 `sender` 仍等于群 ID，表示未拆出成员候选，解析器拒绝；以 `@chatroom` 开头的入站也不能因缺少 `room` 字段被当成私聊。合成回调测试覆盖两位候选发件人、跨来源拒绝、正文脱敏和大报文拒绝。

要接真实 Hook，需在可控时段将 Hook 回调目标指向该应用并做实机增量验证。目前未改 `wx.ini`、未重启微信或回调接收器。即使真实接入，此入口仍不提供来源认证、稳定事件 ID、原生成员身份和 mention；因此不能放行自动回复或管理命令。
