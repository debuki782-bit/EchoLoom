[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

**メッセージング環境向けに堅牢化された、無人常駐型 AI エージェントランタイム。**

EchoLoom は Gemini や OpenAI 互換 LLM を 24 時間稼働するアシスタントへ変えます。個人チャット、グループ会話、予約フォローアップ、バックグラウンド整理までを、メッセージ欠損・二重送信・シークレット漏洩なしで処理します。

## 主な用途

- **24時間稼働の個人・チーム向けコパイロット**: 質問応答、会話要約、タスク追跡、自律的なフォローアップを複数チャネルで実行。
- **無人運用前提の信頼性**: 通信断、プロセス再起動、応答生成中の割り込みメッセージが起きても安全に状態復旧。
- **完全なセルフホスト管理**: 会話履歴・メディア・ナレッジ・設定オーバーライドはすべてローカル SQLite に保存され、認証情報は git に残りません。

## 特長

- **欠損・重複送信ゼロ**: SQLite WAL の永続受信キューとデバウンスバッチ処理、さらに冪等な送信台帳（`1+3` 回までの有界リトライ）を実装。
- **鮮度ガード & なりすまし防止**: 推論中に新しい人間メッセージが届くと古い返信を自動保留し、グループ内の送信者 ID を厳密に検証。
- **監査可能な記憶とコスト制御**: メッセージ／メディア／知識ストアの分離に加え、日次バジェット・セッションローテーション・Prompt Cache 最適化を内蔵。
- **厳格なチャネル分離**: 全プラットフォームは [`ChannelConnector`](src/echoloom/connectors/base.py) プロトコル配下に隔離され、AST 境界テストで依存ルールを自動検証。
- **モデルと設定の高速差し替え**: `.local/models.toml` や環境変数で、応答・記憶・判定モデル（`decision_model` / `ECHOLOOM_DECISION_MODEL`）を柔軟に切替可能。

## 標準コネクタ

| チャネル | Factory | 主な機能 |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | Bearer / HMAC-SHA256 署名検証、SSRF 対策、HTTP 送信 |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Secret-Token 検証、テキスト/画像/音声/返信パース、Bot API 送信 |
| Feishu / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event v2、`url_verification`、署名検証、Token キャッシュ、画像送受信 |
| Discord | `echoloom.connectors.discord:DiscordConnector` | Bot REST 送信、添付ファイル・返信メッセージパース |
| WeChat Hook (任意) | `echoloom.connectors.wechat_hook:WeChatHookConnector` | 有効化時のみ読み込まれる Windows デスクトップ向けアダプタ |

## クイックスタート

```bash
# 1. gitignored なローカル設定ボールト (.local/) を初期化
echoloom init

# 2. モデル API キーを設定（Windows では `echoloom credentials store` も利用可能）
export GOOGLE_API_KEY="your-key"    # または OPENAI_API_KEY

# 3. ランタイム起動（HTTP Ingress + Worker）
echoloom serve

# 4. 別ターミナルからテストイベント送信
echoloom send --text "Hello EchoLoom"
```

よく使うコマンド:

- `echoloom serve --role ingress` / `echoloom serve --role worker` — Ingress と Worker を別プロセスで分離起動。
- `echoloom dashboard` — ローカル専用の Trace・メモリ確認ダッシュボードを起動。
- `echoloom vault audit` — `.local/` の隔離状態と git 未追跡シークレットを監査。

## ドキュメント

- [コネクタとマルチソース Ingress](docs/connectors-and-sources.md)
- [モデルロール & プロファイル設定](docs/agent-profile-config.md)
- [判定モデル設定](docs/jev-strategy.md)
- [コスト・バジェット監査](docs/pricing-audit.md)
- [CLI・運用ガイド](docs/commands.md)

## ライセンス

[0BSD](LICENSE)
