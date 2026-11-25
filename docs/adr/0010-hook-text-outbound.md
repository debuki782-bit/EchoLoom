# ADR 0010：文本出站适配器与未知回执

2026-10-29，仅离线测试，主程序尚未启用。

`HookTextSender` 按固定源码的 POST /SendTextMsg 契约构造目标与文本字段。默认 disabled，要求账号+会话白名单，限定文本长度并拒绝 NUL。端点固定回环地址；HTTP 客户端不读环境代理、不跟随重定向、没有请求重试。

该适配器刻意不实现旧的 bool MessageTransport，因为 bool 无法表达 queued 与送达的差异。结果为 rejected、queued、unknown。仅客户端拒绝和源码中在原生调用前返回的 ret=-1/-2 映射账本 failed；queued、原生返回-3、超时、无效响应等一律映射 unknown，绝不写成 sent。真实送达必须另有独立证据。

后续 Worker 必须先核对精确微信版本及当前登录账号，再执行持久 Outbox 的 approve/claim，发送前检查群 privacy_epoch 并解决暂停竞态，最后 settle。崩溃在途必须恢复为 unknown；本适配器本身不提供幂等性。当前不能把它直接连到 Gateway.approve_send，也不能因 allowlist 存在就跳过审批、稳定事件ID或来源校验。

这批只有 httpx.MockTransport 请求，无真实微信发送。实机文本证据沿用先前用户确认；图片仍以独立探针凭据为准。
