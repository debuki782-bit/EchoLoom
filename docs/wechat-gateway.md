# 微信交互适配器

2026-11-07 起，生产入口通过 `WeChatGateway` 访问微信。Agent 工具、会话、消息队列、记忆、预算、发送账本、媒体归档和读图模型仍属于业务层；具体微信通信、回调格式、账号查询、联系人、图片缓存/解密/下载属于适配器。

## 当前实现

- `src/wechat_agent/wechat_gateway.py`：项目事件 `MessageEvent`、账号状态 `AccountState`、文本提交结果 `SendResult`、语音输入 `VoicePayload`、配置 `GatewayConfig` 和接口协议。
- `src/wechat_agent/adapters/hook.py`：当前真实使用的 `HookGateway`，继续调用原有本机 Hook，保留现有发送回执含义。
- `src/wechat_agent/adapters/hook_events.py`、`hook_contacts.py`、`hook_text.py`、`hook_recovery.py`、`hook_media.py`、`hook_native_images.py`：当前 Hook 的具体实现。
- `src/wechat_agent/adapters/recording.py`：不连接微信的本地记录适配器，用不同的事件格式验证替换接口。它不是第二套已经完成真实微信联调的后端。

默认配置不变：真实 Hook 使用本机 30001，图片下载桥接使用 30002。微信版本、注入和二进制指纹检查仍在当前 Hook 的启动/原生扩展范围内；新接口没有放宽这些检查。

## 替换方式

第三方包提供一个 `factory(config: GatewayConfig) -> WeChatGateway`，或提供接收该配置的类。配置中传入工作目录、账号、已允许的群会话、发送开关等；适配器自己的凭据由适配器管理，不从聊天消息中取得。

设置进程环境变量后启动：

```powershell
$env:WECHAT_GATEWAY_FACTORY = 'my_wechat_backend:build_gateway'
.\scripts\start-services.ps1
```

仅沿用当前 HTTP 协议、更换端口时，也可设置 `WECHAT_HOOK_URL`。修改后需重启 Agent；已运行进程不会热切换。自定义工厂或非默认 Hook 地址下，一键脚本不再启动本机固定版本的 Hook、注入 DLL 或启动专用原图桥接；外部适配器负责其连接与启动。默认 Hook 的启动行为保持原样。

也可以在自己的启动程序中构造 gateway，然后注入：

- `WeChatTools(..., gateway=gateway)`
- `WeChatImageIngestor(workspace, gateway=gateway)`
- `WeChatVoiceIngestor(workspace, gateway=gateway)`
- `MessageContextAssembler(..., resolve_contact=gateway.resolve_contact, archive_envelope=gateway.archive_envelope)`
- `Pilot(..., send=gateway.send_text, parse_events=gateway.parse_events, archive_envelope=gateway.archive_envelope)`
- `create_trace_dashboard(..., gateway=gateway)`

生产启动程序已完成这些连接，并在关闭时关闭 gateway。新增后端不需要修改上述业务模块。工厂会检查是否提供完整协议；不支持的具体能力应在 `capabilities` 中省略，并由相应方法明确返回不支持/不可用，不得伪造成功。

## 接口约定

| 接口 | 责任 |
| --- | --- |
| `parse_events(payload)` | 把该后端的回调变为 `MessageEvent`；验证路由、发送者和来源字段，不把正文里的身份声明当作真实发件人 |
| `archive_envelope(event, preserve_raw=...)` | 提供可归档的来源记录；应可由同一适配器重新解析，供旧消息补看；不能放模型凭据 |
| `account_state(account_id, conversation_id, timeout_seconds=...)` | 返回登录状态、是否为预期账号、可选存储根目录和最新可恢复事件；业务层不读取数据库路径或供应方字段 |
| `resolve_contact(user_id)` | 返回稳定 ID 精确匹配的 `ContactIdentity`，或 `None` |
| `allow_conversation(key)` | 接收业务层已批准的会话，不负责自行扩张权限 |
| `send_text(draft)` | 一次提交，返回 `SendResult`；不在适配器内偷偷重试 |
| `send_image(conversation_id, path)` / `send_quote(conversation_id, text, quote)` | 一次发送并返回状态、原因；引用元组顺序为作者 ID、作者显示名、来源会话 ID、正文 |
| `fetch_original_image(event, references, destination)` | 将已验证属于该资源的完整图片写入指定目标文件，返回来源 SHA-256 和 MIME；资源归属检查由适配器完成 |
| `fetch_thumbnail(...)` | 获取有明确来源的缩略图；业务层保留低清标记和后续升级流程 |
| `decode_image(source, destination)` | 兼容旧图片库中的加密本地图片；无需解密的后端可以明确不支持 |
| `voice_payload(event)` | 返回待解码的 SILK 数据与时长，或现有明确错误码；业务层保留有界解码、归档和转录 |
| `close()` | 释放该适配器持有的连接等资源 |

这些 I/O 方法是同步、有界调用；异步业务通过工作线程调用阻塞操作。`MessageEvent` 目前保留微信消息类型、既有发送者来源标签和原始私有 JSON 字段，以兼容已有消息库；不是任意聊天平台的通用消息协议。

`source_msg_id` 有真实稳定 ID 时填写正整数，没有时填 `None`。缺少稳定 ID 时，原始单条消息 JSON 必须在重放时保持稳定，因为去重使用它的哈希。业务队列保存的字段及收据算法没有改变。归档可以直接使用事件的稳定消息 ID，不要求第三方后端伪造 Hook 的 `msgsvrid` 字段。

`queued` 只表示排队，`accepted_by_server` 只表示服务端接受，`unknown` 表示结果不能确认；均不能标记为微信已送达。当前业务保留原有这些状态和 `delivered=False`，未新增已确认送达能力。适配器遇到结果不确定时不能自动重发；暂停、权限、重试次数和去重仍由业务层控制。

## 兼容与验证

原来的 `HookObservation`、`HookSendResult`、`HookTextSender`、`HookContactResolver`、原生图片下载入口及状态恢复导入路径保留为兼容入口。旧消息数据库不清空，不修改旧收据身份。`media.py` 的本地缓存私有访问器只为旧集成保留，新适配器无需实现 Hook 的缓存目录。

本次验证结果：

- 解耦前完整测试：321 passed / 3 failed。
- 解耦后完整测试：329 passed / 同样的 3 failed，无新增失败。原有失败是两个 Luna 输入上限用例和一个旧图片生成用例。
- 新增 8 项测试验证不同回调格式、完整队列/上下文/媒体/发送路径、稳定消息 ID、暂停和去重、归档补看、语音输入、原有 Hook 请求及接口替换。
- 对修改前快照作 68 项 AST 比较，图片获取核心逻辑、归档与升级、队列所有方法、模型可见工具的参数和说明均一致。
- 使用真实历史图片资源，在独立空缓存和归档目录通过新 gateway 获取原生下载结果，再构建图片模型输入：JPEG 712,880 字节、1080×1919，SHA-256 `88c84fa7cc5c43bdd799851fb1c340ecfc8fc5c639feb668263e795004f46530`。复用原有确定任务收据，没有重提原生下载、打开图片、调用付费模型或向微信发测试消息。
- Python F/I 检查、启动脚本解析和 diff 空白检查通过。

源码快照、测试 XML、兼容性结果与实图验证证据位于忽略目录 `.tmp/gateway-decoupling-20261107/`。

## 本机加载记录

后续 B 站长分享卡片与拍一拍接收扩展仍置于 Hook 适配器侧，沿用同一事件和回调接口，不增加业务层对 Frida 或原生地址的依赖。默认启动脚本负责加载该扩展，自定义网关跳过它。具体接收边界、测试及恢复记录见 [B 站分享与拍一拍接收修复](bilibili-and-pat-receive.md)。

完成回归后，在队列为空且没有在途任务时保存 Pilot 收据快照，重启本项目 Agent 与管理页加载新入口。启动日志确认 `factory=wechat_agent.adapters.hook:HookGateway`、账号校验通过、`native_image_download_configured enabled=True`。微信和 Hook 未重启，原生下载服务继续连接同一微信进程。加载后已处理状态保持 `model_seen=118`，`paused=false`、队列 0、无在途任务；原图服务 `callback_ready=true`，微信进程正常响应。没有由验收程序发送真实测试消息或调用付费模型。
