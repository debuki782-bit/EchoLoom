# 多信息源连接器与 Hook 解耦架构说明

EchoLoom 通过 [`src/echoloom/connectors/`](../src/echoloom/connectors/) 将**外部消息/事件通信协议**与**Agent 调度、记忆、上下文和 ADK 推理主体**完全解耦。核心业务模块禁止导入具体 Hook 适配器模块或嵌入 Hook HTTP 路径（由 [`tests/test_multisource_vault.py`](../tests/test_multisource_vault.py) 中的 AST 守卫测试持续校验）。

---

## 1. 连接器边界与核心组件

| 模块 | 职责 |
|---|---|
| [`connectors/base.py`](../src/echoloom/connectors/base.py) | 定义 `ChannelConnector` / `WeChatGateway` 协议、`MessageEvent`、`SendResult`、`AccountState`、`VoicePayload`、`ConnectorConfig` 及工厂加载器 `create_gateway()`。 |
| [`connectors/registry.py`](../src/echoloom/connectors/registry.py) | `SourceRegistry` 从 `.local/sources.toml`（或环境变量）同时加载一个或多个连接器；`RouterGateway` 代理按 `source_id` / 会话将入站解析、联系人查询、媒体获取与出站回复精准路由到对应来源。 |
| [`connectors/webhook.py`](../src/echoloom/connectors/webhook.py) | 通用 HTTP/JSON 源连接器 `WebhookConnector`，支持 `Bearer` Token / `X-Signature-256` HMAC-SHA256 校验、文本、图片、WAV 音频、链接卡片、引用及可选出站 JSON 推送。 |
| [`connectors/telegram.py`](../src/echoloom/connectors/telegram.py) | Telegram Bot API 连接器 `TelegramConnector`，支持 `X-Telegram-Bot-Api-Secret-Token` 验签、Bot Webhook 文本/图片/语音/回复解析及 `sendMessage`/`sendPhoto`/文件拉取。 |
| [`connectors/feishu.py`](../src/echoloom/connectors/feishu.py) | 飞书/Lark 开放平台连接器 `FeishuConnector`，支持 Event 2.0 `im.message.receive_v1`、`url_verification`、`X-Lark-Signature` / verification token、`tenant_access_token` 缓存与图片资源上传下载。 |
| [`connectors/discord.py`](../src/echoloom/connectors/discord.py) | Discord Bot REST 连接器 `DiscordConnector`，支持 Gateway/Relay 消息与附件/回复解析及 `/channels/{id}/messages` 文本、引用与图片发送。 |
| [`connectors/wechat_hook.py`](../src/echoloom/connectors/wechat_hook.py) | 桌面微信 Hook 插件入口 `WeChatHookConnector`（兼容导出自 `adapters/hook.py`）。全部 Hook HTTP 路径、WeChat XML 归一化、`wxid_` / `@chatroom` 强格式校验、SILK 解码与本地 `.dat` 解密均收敛在适配器内部。 |
| [`connectors/recording.py`](../src/echoloom/connectors/recording.py) | 离线录制/回放连接器 `RecordingConnector`，用于无外网、无桌面客户端的端到端测试。 |

---

## 2. 多源 HTTP 入站接口

独立 `Ingress` 服务（[`src/echoloom/ingress.py`](../src/echoloom/ingress.py)）默认提供：

- `GET /sources`：列出当前已激活的信息源 `source_id`、`channel` 与声明能力。
- `POST /sources/{source_id}/events`：按路由参数将请求直接交给对应连接器校验与解析：
  - 若该连接器配置了 `auth_token` 或 `hmac_secret`，强制校验 `Authorization: Bearer <token>` 或 `X-Signature-256: sha256=<hex>`；
  - 若未配置密钥，则强制限制仅允许 loopback（`127.0.0.1` / `::1`）调用。
- `POST /callback`：保留向后兼容入口，自动路由到 `default_source`。

---

## 3. Webhook 标准 JSON 事件格式

向 `POST /sources/webhook/events` 发送单个事件对象或 `{"events": [...]}`（每次最多 64 条）：

```json
{
  "events": [
    {
      "account_id": "echo_bot",
      "conversation_id": "room:eng-ops",
      "conversation_name": "Engineering Ops",
      "sender_id": "tg:42",
      "sender_name": "Alice",
      "is_group": true,
      "kind": "text",
      "text": "当前构建状态如何？",
      "message_id": 1001
    }
  ]
}
```

`kind` 支持：

- `text`：普通文本消息（`text` 字段）。
- `image` / `sticker`：支持 `image_base64`（可选 `mime_type`）或 `image_url`。
- `voice`：支持 `audio_base64`（支持 `audio_format: "wav"` 或 `"silk"`）与可选 `duration_ms`。
- `link`：链接卡片（`title`、`url`、`description`）。
- `quote`：引用消息（`text` 与 `quote` 对象）。
- `pat`：互动事件（`target_id`、`suffix`）。

---

## 4. 更换桌面 Hook 或关闭本地微信依赖

1. **不使用桌面微信**：在 `.local/sources.toml` 中将 `default_source` 设为 `"webhook"`（或 `"telegram"` / `"feishu"` / `"discord"`）并禁用 `[sources.wechat]`（或设置环境变量 `ECHOLOOM_GATEWAY_FACTORY=echoloom.connectors.webhook:WebhookConnector`）。[`scripts/start-services.ps1`](../scripts/start-services.ps1) 与 [`scripts/watch-services.py`](../scripts/watch-services.py) 会自动跳过微信进程与 `30001/30002/30003` 探测，只启动并守护核心服务。
2. **更换微信 Hook 实现**：实现 `ChannelConnector` 协议并修改 `.local/sources.toml` 的 `factory` 指向新的 `module:Class`（例如 `my_wechat_adapter:CustomHookConnector`）；所有本地路径、端口与二进制指纹统一托管在 `.local/runtime.toml`，不再写死在核心启动脚本中。
3. **复现 WeChat PC 4.1.10.27 Hook 栈（含一键编译脚本）**：参见 [`hooks/wechat-4.1.10.27/README.md`](../hooks/wechat-4.1.10.27/README.md) 与 [`build-wechat-hook.ps1`](../hooks/wechat-4.1.10.27/build-wechat-hook.ps1)。
