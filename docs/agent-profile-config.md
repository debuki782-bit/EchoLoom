# Agent 聊天档案配置

启动脚本从 `config/agent_profiles.toml` 读取 Agent 配置；若存在集中私有仓库文件 `.local/agent_profiles.toml`（或旧版同目录下的 `config/agent_profiles.local.toml`，均已被 `.gitignore` 忽略），会自动优先加载本地文件以隔离个人 ID 与绑定。提示词同理按 `.local/prompts/<name>` → `prompts.local/<name>` → `prompts/<name>` 顺序解析。聊天通过精确的
`account_id + conversation_id` 绑定到档案；没有匹配项时使用 `default_profile`。
提示词、运行时尾部模板和工具说明在启动时预载入内存。每次读取提示词都使用内存快照；
主人在聊天中发送“刷新提示词”会重新读取并校验所有文件，只有完整成功才切换快照，失败则保留旧版本。
模型、聊天档案绑定和工具组修改仍需重启 Agent。

## 可配置项

- `model`：`gemini-3.5-flash-lite`、`gemini-3.8-flash` 或 `gemini-4-argon`。
- 顶层 `memory_model`：当前为 `gpt-6-luna`，供会话摘要、后台空闲整理和事件记忆整理使用。
  聊天仍由 Google ADK 承载；摘要通过独立的 OpenAI 后端执行。生产 ADK 关闭 SDK 自带的
  Gemini 摘要，由应用现有的会话轮换流程调用 Luna，避免产生额外的 Gemini 整理调用。
- `persona_prompt`、`main_prompt`、`summary_prompt`、`summary_system_prompt`：引用
  `prompts/` 中已登记的提示词文件名，不带扩展名。提示词正文仍按文件独立维护；每轮运行时
  读取所选文件，因此改正文无需重启，切换档案引用则需重启。
- `toolsets`：`wechat`、`cross_session`、`web`、`project`、`status`。专属档案覆盖
  `toolsets` 时，该列表会完整替换继承值。
- `daily_budget_usd`：此档案的每日额度池；该档案绑定的聊天共同使用。
- `text_budget_usd`、`image_read_budget_usd`、`image_generation_budget_usd`：档案内
  相应功能的每日子额度。
- 顶层 `account_daily_budget_usd`：账号级硬上限，所有档案共用，防止各档案额度相加后
  超出总额。
- 顶层 `account_text_budget_usd`、`account_image_read_budget_usd`、
  `account_image_generation_budget_usd`：账号级功能上限，跨所有聊天/档案共用。
- `command_owner_ids`：允许使用特殊模型/刷新命令的精确微信发送人 ID。

档案可用 `extends = "default"` 继承默认配置，只覆盖有差异的字段。若要让某个聊天拥有
独立额度池，给它建立独立档案并绑定；多个聊天绑定同一档案时，会共享该档案额度。

## 绑定示例

```toml
[profiles.private_lab]
extends = "default"
model = "gemini-3.8-flash"
toolsets = ["wechat", "web", "status"]
daily_budget_usd = "0.40"
text_budget_usd = "0.30"
image_read_budget_usd = "0.05"
image_generation_budget_usd = "0.05"

[[bindings]]
account_id = "wxid_..."
conversation_id = "123456@chatroom"
profile = "private_lab"
```

当前默认档案使用 `gemini-4-hydrogen`。示例绑定展示了将指定 `account_id + conversation_id`
绑定到独立档案（`custom_contact`），其他未绑定聊天继续使用默认档案；账号 ID 和聊天 ID 必须同时匹配，不依据昵称匹配。管理员可在聊天里发送
“模型列表”“切换到 Gemini 3.5 Flash Lite”“切换到 Gemini 3.8 Flash”“切换到 Gemini 4 Hydrogen”“切换到 Gemini 4 Argon 一小时”
或“切回默认模型”。Argon 覆盖严格一小时，到期自动恢复该聊天档案的模型；其余选择持续到再次切换。

账号级额度会统计所有档案的用量；档案额度和功能子额度按档案 ID 分账。现有历史用量
归入 `default` 档案；新增的非默认档案从其生效时开始单独累计。

目前账号日额度为 $4：文字模型 $2、读图 $1、生图 $1。模型分工变化不改变这些预算。
网页检索与阅读由独立的 Gemini 3.8 Flash 执行，分别启用 Google Search 和 URL Context，
不接收微信聊天历史。B 站视频继续走专用的信息、字幕与评论读取。读图服务保留现有实现，
生图继续使用专用的 `gemini-3.1-flash-lite-image` 模型。

模型能力依据 [Gemini 3.8 Flash 文档](https://ai.google.dev/gemini-api/docs/models/gemini-3.8-flash)
和 [图片生成模型文档](https://ai.google.dev/gemini-api/docs/image-generation)。
