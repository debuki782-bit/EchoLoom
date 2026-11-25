# Hook 回调字段与身份解析契约

`src/wechat_agent/hook_contract.py` 将 Hook JSON 转成内存观察对象；它不调用可靠 Inbox、模型或微信发送。回调本身是 loopback HTTP，当前没有来源认证，`msgsvrid=0` 不能作为幂等键。

## 发件人身份

固定源码 `mosheng20205/WechatHook4.1.10.27` 的 `CaptureValidatedAddMsgItem` 从已校验的 SyncBatch `AddMsg` 记录中读取群消息正文，并拆出形如 `<member wxid>:\n<body>` 的成员前缀；`PostValidatedReceivedMessage` 同时发送该成员 ID 到 JSON `sender`，并保留原始 `msg`。适配器仅接受 `wxid_`/`gh_` 稳定联系人 ID，且要求原始正文以完全相同的 `sender + ":\n"` 开头。该一致性检查能阻止字段之间自相矛盾，但 sender 仍是 Hook 从正文解析出的值，不是独立的群成员认证证明。

`src/wechat_agent/contacts.py` 通过 `POST http://127.0.0.1:30001/GetContact` 按精确 wxid 查询本机联系人。只有响应 `contact_found=true` 且 `UserName` 与请求 ID 完全相同时才接受联系人；优先使用非空 `Remark`，否则使用 `NickName`。超时、未命中、ID 不匹配或不稳定 ID 均不生成名字；缓存仅在内存中、具有时限。当前源码未发现专用群成员名册查询接口。

当前模型上下文直接提供账号、群/会话、发件人和引用作者的原始 Hook ID。精确 `/GetContact` 匹配后同时提供 `UserName`、`NickName`、`Remark` 和可读展示名；存在备注时展示名格式为“昵称（备注）”。模型得到稳定 ID 和可读名称，便于跨消息区分对象；联系人匹配不代表权限，`authorization_verified` 和 `sender_verified` 仍为 `false`。管理命令、群授权及自动回复仍需其他可信证据。

## 引用消息

对 `appmsg/refermsg`，`fromusr` 表示引用来源会话；群引用中它通常是群 ID。`chatusr` 表示被引用消息作者，优先按该稳定联系人 ID 查询联系人。`displayname` 只作为未验证字段提示保留在内部归一化结果，不用于身份匹配，也不会进入模型上下文。上下文分别携带引用作者原始 ID 和可读名称，来源会话也携带原始 ID 与可用的群名。

## 验证边界

离线契约测试覆盖稳定 ID、群正文前缀一致性、`chatusr` 作者优先级、联系人响应精确匹配、原始标识进入模型上下文及 Pilot 并行解析。固定 Hook 的群 sender 仍需独立认证，事件 ID、原生 @ 与群成员授权仍未验证。
