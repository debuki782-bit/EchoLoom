# 凭据与密钥本机管理（Credentials & Secrets）

本项目严格禁止将任何 API Key、Webhook Token、HMAC Secret 或真实账号凭据写入 Git 跟踪文件。生产与日常开发推荐将秘密与私有配置分层管理：

1. **系统凭据管理器 / 环境变量（高敏密钥）**
   - [`src/wechat_agent/credentials.py`](../../src/wechat_agent/credentials.py) 在 Windows 上通过 Windows Credential Manager 存储通用凭据：
     - `--service model` -> `WeChatAIAgent/ModelAPI`（Gemini API Key）
     - `--service openai` -> `WeChatAIAgent/OpenAIAPI`（OpenAI API Key）
     - `--service webhook` -> `EchoLoom/WebhookSecret`（Webhook 共享密钥，也可设置 `ECHOLOOM_WEBHOOK_SECRET`）
     - 也支持环境变量 `GEMINI_API_KEY` 与 `OPENAI_API_KEY`。
2. **集中私有配置金库 `.local/`（被 `.gitignore` 忽略）**
   - 账号路由、联系人和绑定、个人提示词与心境锚点、本地城市坐标与 Hook 二进制路径统一存放在 `.local/`（详见 [`config/runtime.example.toml`](../../config/runtime.example.toml) 与 [`config/sources.example.toml`](../../config/sources.example.toml)）。

## 常用命令行操作

在交互式 PowerShell 中隐藏输入保存 Key（不要把 Key 直接写在命令行参数或 `.env` 后提交）：

```powershell
.\.venv\Scripts\python.exe -m echoloom.credentials store --service model
.\.venv\Scripts\python.exe -m echoloom.credentials store --service openai
```

查询保存状态或删除凭据（`status` 只返回 `stored` / `missing`，不会输出明文）：

```powershell
.\.venv\Scripts\python.exe -m echoloom.credentials status --service model
.\.venv\Scripts\python.exe -m echoloom.credentials delete --service model
```

核验 `.local/` 状态并扫描仓库受跟踪文件是否存在敏感泄漏：

```powershell
.\.venv\Scripts\python.exe -m echoloom.local_config status
.\.venv\Scripts\python.exe -m echoloom.local_config audit
```
