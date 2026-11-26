[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

**An unattended, reliability-hardened AI agent runtime for messaging channels.**

EchoLoom turns Gemini or OpenAI-compatible LLMs into a persistent assistant that runs 24/7 across DMs, group chats, follow-up timers, and background idle tasks—without dropping messages, double-sending, or leaking secrets.

## What It's For

- **24/7 Personal or Team Copilot**: Handle questions, summarize discussions, track commitments, and follow up proactively across chats.
- **Unattended Reliability**: Built for headless servers and long-running desktops where network blips, restarts, and mid-turn new messages happen constantly.
- **Self-Hosted Control**: All conversations, media, memory, and overrides stay on disk in local SQLite databases; your credentials never enter git.

## Why EchoLoom

- **Zero Dropped / Duplicate Replies**: Durable SQLite inbox queues inbound events with quiet-window batching, crash recovery, and an outbound side-effect ledger (`1+3` bounded retries).
- **Freshness & Group Safety**: Holds stale auto-replies if a newer human message arrives mid-inference; isolates sender identities to block group-chat prompt spoofing.
- **Auditable Memory & Cost Control**: Split `messages`, `media`, and `knowledge` stores with daily model budgets, session rollover, and prompt-cache efficiency.
- **Strict Channel Isolation**: Every platform sits behind the [`ChannelConnector`](src/echoloom/connectors/base.py) protocol, enforced by AST boundary tests—no core logic depends on any chat SDK.
- **Hot-Swappable Models**: Swap chat, memory, media, or decision models (`decision_model` / `ECHOLOOM_DECISION_MODEL`) cleanly via `.local/models.toml`.

## Built-In Connectors

| Channel | Factory | Highlights |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | Bearer / HMAC-SHA256 webhook bridge, SSRF checks, outbound push |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Secret-token verify, text/photo/voice/reply parsing, Bot API send |
| Feishu / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event v2, challenge & signature check, tenant token cache, media |
| Discord | `echoloom.connectors.discord:DiscordConnector` | Bot REST send, attachment/reply parsing |
| WeChat Hook | `echoloom.connectors.wechat_hook:WeChatHookConnector` | Optional Windows desktop hook adapter (loaded only when enabled) |

## Quickstart

```bash
# 1. Initialize your gitignored local vault (.local/runtime.toml, sources.toml, models.toml)
echoloom init

# 2. Set a model API key (macOS/Linux; on Windows you can use `echoloom credentials store`)
export GOOGLE_API_KEY="your-key"    # or OPENAI_API_KEY

# 3. Start the runtime (loopback HTTP Ingress + Worker)
echoloom serve

# 4. Send a test event from another terminal
echoloom send --text "Hello EchoLoom"
```

Useful commands:

- `echoloom serve --role ingress` / `echoloom serve --role worker` — run HTTP ingestion and the LLM worker in separate processes.
- `echoloom dashboard` — launch the read-only local trace & memory dashboard.
- `echoloom vault audit` — check `.local/` isolation and ensure no secrets are tracked in git.

## Documentation

- [Connectors & Multi-Source Ingress](docs/connectors-and-sources.md)
- [Model Roles & Profiles](docs/agent-profile-config.md)
- [Decision Model Config](docs/jev-strategy.md)
- [Pricing & Budget Audit](docs/pricing-audit.md)
- [CLI & Operations](docs/commands.md)

## License

[0BSD](LICENSE)
