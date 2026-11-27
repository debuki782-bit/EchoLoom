[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

A self-hosted, event-driven runtime for running LLM agents unattended across real messaging channels.

Wiring a model API to a chat webhook takes a few dozen lines of code. Keeping that agent running in DMs and group chats for weeks—without replying twice after a network hiccup, sending stale drafts after a follow-up message arrives, getting misled by a renamed group nickname, or letting memory and API spend drift—is mostly systems work. EchoLoom is built around those constraints.

All state (messages, media, memory, audit logs, and credentials) stays on local disk in SQLite and a gitignored `.local/` vault.

---

## How It Handles Real Messaging Edge Cases

| Situation | Mechanism in EchoLoom |
|---|---|
| User sends multiple short messages in a row, or adds context while the model is still thinking | Persisted SQLite WAL inbox with quiet-window batching; a freshness gate holds stale auto-replies if newer human input arrives mid-inference. |
| Group chats where replying to every line feels noisy | Optional pre-turn decision classifier (`reply` / `concise` / `no_reply` / `multi_turn`) plus multi-bubble delivery with natural pacing and short follow-up timers. |
| Network drops, timeouts, or process restarts | Crash-recoverable inbox, outbound receipt ledger with `1+3` bounded retries, and transactional tool-mutation deduplication (`agent_tool_mutations`). |
| Untrusted group participants changing display names to spoof identity | Human-managed remarks (`sender_profiles`) are read-only to the agent; self-selected nicknames matching sensitive relationship terms are masked automatically; stored memories carry explicit `untrusted_*` labels. |
| Memory growth, stale reminders, and hallucinated self-maintenance | Layered SQLite memory with foreign-key provenance to `source_messages`, bounded working memory (`event_book`), Top-40 `fact_key` index for on-demand lookup, and optional `EpisodeMemory` vector recall + offline consolidation ("Dreaming"). |
| Idle background tasks wasting tokens when nothing happened | Stepped idle backoff (`30m` → `90m` → `3h` → `6h`, capped at 10 runs / 24h) backed by a deterministic SQL delta snapshot; quiet windows exit without extra queries, and new inbound messages preempt idle work immediately. |
| Unattended API cost spikes | Daily micro-USD budget ledger split by account, text model, image reading, and image generation, alongside session rollover and prompt-cache lifecycle management. |

---

## Memory Architecture

Rather than pushing every chat line into a single vector store, EchoLoom splits memory by retrieval pattern and retention lifecycle:

- **Message archive (`source_messages`)**: Durable source of truth with media metadata (images, voice transcripts, file summaries) and platform message IDs for exact quote resolution.
- **Contact profiles & tags (`sender_profiles`, `sender_memories`, `sender_tags`)**: Separates human-owned remarks from agent-gathered notes and timestamped tag diffs.
- **Entity dictionary (`entity_terms`)**: Deterministic word-boundary regex matching for group slang, aliases, and project codenames where embeddings frequently miss.
- **Long-term facts (`user_facts`)**: Versioned key-value facts anchored to `source_messages`. Only the 40 most recently updated `fact_key` names are injected as a compact directory; full bodies are fetched on demand via `search_user_memory`.
- **Cross-chat event book (`event_book`)**: Bounded working memory shared across conversations (4 high-priority slots, mandatory exit conditions, automatic 72-hour staleness flags for ordinary items).
- **Optional episodic vector memory (`episode-memory.sqlite3`)**: Enabled via `ECHOLOOM_EPISODE_RAG=1` for fuzzy recall beyond the recent message window, automatically excluding episodes whose source messages are already visible in context and consolidating bloated conversations (`>= 12` entries) offline during idle cycles.

---

## Local Workbench (`echoloom dashboard`)

EchoLoom includes a zero-build, loopback-only web workbench (`127.0.0.1` / `localhost`, strict CSP, same-origin mutation guard) for inspecting and correcting runtime state without touching SQLite manually:

- **Conversations & Traces**: Read-only timeline of batched inputs, model requests, tool calls, and delivery receipts, with JSONL export.
- **Tool Controls**: Enable or disable registered main-chat and idle tools live.
- **Contact Profiles**: Edit human-owned `Remark` names, inspect fragmentation warnings (`>= 8` notes), and add or remove tags and `sender_memories`.
- **Memory Library**: Browse, search, trace back to original source messages, edit inline, or delete `user_facts` (with Top-40 index visibility), `event_book` items (with slot quotas and 72h flags), `entity_terms`, `annotations`, and `episodes`. Structured memory edits go through `MemoryStore` / `EventBook` to preserve revision and audit history, while deleting an episode removes it from `episode-memory.sqlite3`.

---

## Architecture & Channel Isolation

Core runtime logic never imports a platform SDK. Every messaging integration sits behind [`ChannelConnector`](src/echoloom/connectors/base.py), and downward module dependencies across 6 explicit layers are enforced by AST tests in [`tests/test_architecture_boundaries.py`](tests/test_architecture_boundaries.py).

| Channel | Factory | Capabilities |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | HTTP JSON ingress, Bearer / HMAC-SHA256 verification, SSRF checks, outbound push |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Secret-token verification, text/photo/voice/reply parsing, Bot API delivery |
| Feishu / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event v2, challenge & signature validation, tenant token cache, media support |
| Discord | `echoloom.connectors.discord:DiscordConnector` | Attachment & reply parsing, Bot REST delivery |
| WeChat Hook (optional) | `echoloom.connectors.wechat_hook:WeChatHookConnector` | Windows desktop hook adapter, loaded only when explicitly enabled |

---

## Quickstart

Requires Python 3.12+.

```bash
# 1. Install in editable mode
pip install -e .

# 2. Initialize the gitignored local vault (.local/runtime.toml, sources.toml, models.toml)
echoloom init

# 3. Configure a model API key (on Windows you can also use `echoloom credentials store`)
export GOOGLE_API_KEY="your-key"    # or OPENAI_API_KEY

# 4. Start the runtime (loopback HTTP Ingress + Worker)
echoloom serve

# 5. Send a test event from another terminal
echoloom send --text "Hello EchoLoom"
```

Common operations:

```bash
echoloom serve --role ingress   # Run HTTP ingress and LLM worker as separate processes
echoloom serve --role worker
echoloom dashboard              # Start the local trace & memory workbench
echoloom vault audit            # Verify .local/ isolation and confirm no secrets are tracked in git
```

Models for chat, memory extraction, media understanding, and optional decision gating can be swapped without code changes in `.local/models.toml` or via environment variables.

---

## Documentation

- [Connectors & Multi-Source Ingress](docs/connectors-and-sources.md)
- [Model Roles & Profiles](docs/agent-profile-config.md)
- [Decision Model Config](docs/jev-strategy.md)
- [Pricing & Budget Audit](docs/pricing-audit.md)
- [CLI & Operations](docs/commands.md)

## License

[0BSD](LICENSE)
