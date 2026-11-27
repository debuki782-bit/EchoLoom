[English](README.md) · [简体中文](README.zh-CN.md) · [日本語](README.ja.md)

# EchoLoom

メッセージング環境での無人常駐を前提に設計された、セルフホスト型・イベント駆動の LLM エージェントランタイム。

LLM API をチャット Webhook につなぐだけなら数十行で書けますが、個人チャットやグループ会話で数週間運用し続けると、通信断による二重送信、応答生成中の追加入力に対する時代遅れの返信、ニックネーム変更によるなりすまし、記憶の肥大化や API コストの暴走といった実務上の問題が必ず出てきます。EchoLoom はこれらの運用課題をランタイム側で処理するために作られています。

会話ログ・メディア・記憶・監査履歴・認証情報はすべてローカルの SQLite と git 管理外の `.local/` ボールトに保存されます。

---

## 実運用でのエッジケースと対処方針

| 起きやすい状況 | EchoLoom の仕組み |
|---|---|
| 短時間に連続投稿される、または応答生成中に新しい発言が来る | SQLite WAL の永続受信キューとデバウンスによるバッチ処理。推論中に新しい人間メッセージが届いた場合、鮮度ガードが古い自動返信を保留します。 |
| グループ会話で全メッセージに反応すると不自然になる | 任意の事前判定モデル（`reply` / `concise` / `no_reply` / `multi_turn`）を参考にし、複数バブルへの分割送信や短時間のフォローアップ予約に対応します。 |
| ネットワーク瞬断・タイムアウト・プロセス再起動 | クラッシュ復旧可能な受信キュー、最大 `1+3` 回リトライの送信台帳、トランザクション内のツール実行冪等キャッシュ（`agent_tool_mutations`）。 |
| グループメンバーがニックネームを変更して権限や関係性を装う | 人間が管理する備考（`sender_profiles`）はエージェントから読み取り専用。未備考ユーザーのセンシティブな関係性ワードを含むニックネームは自動でマスクされ、記憶レコードには `untrusted_*` ラベルが付与されます。 |
| 記憶の肥大化・古いタスクの滞留・空振りな自己整理 | `source_messages` への外部キー参照を持つ階層化 SQLite 記憶、上位 40 件の `fact_key` インデックスによるオンデマンド参照、72 時間経過フラグ付きの共有イベント帳、任意のベクトル記憶（`EpisodeMemory`）とオフライン統合（Dreaming）。 |
| 静かな時間帯にバックグラウンド整理がトークンを浪費する | 段階的バックオフ（`30m` → `90m` → `3h` → `6h`、24 時間あたり上限 10 回）と決定論的な SQL 差分スナップショット。新規メッセージも断片化警告もない場合は即終了し、新規着信があれば即座に割り込み中断します。 |
| 無人稼働時の API 費用超過 | アカウント全体・テキスト・画像認識・画像生成ごとに分けた日次マイクロ USD バジェット台帳、セッションローテーション、Prompt Cache ライフサイクル管理。 |

---

## 記憶の階層設計

すべての発言を単一のベクトル DB に入れるのではなく、参照パターンと寿命に応じて役割を分けています。

- **メッセージアーカイブ（`source_messages`）**: 画像要約・音声文字起こし・ファイル要約を紐づけた原本ストア。プラットフォーム側メッセージ ID による厳密な引用元参照にも対応します。
- **人物プロファイルとタグ（`sender_profiles`, `sender_memories`, `sender_tags`）**: 人間管理の備考とエージェントが記録した人物メモを分離し、タグは追記型 diff により過去時点の復元が可能です。
- **用語辞書（`entity_terms`）**: ベクトル検索で取りこぼしやすい内輪の略語・あだ名・プロジェクト名を、単語境界正規表現で決定論的にマッチします。
- **アカウント横断の長期事実（`user_facts`）**: `fact_key` 単位のバージョニングと `source_message_id` 参照を保持。コンテキストには最近更新された 40 件のキー一覧のみを載せ、本文は必要時に `search_user_memory` で取得します。
- **共有イベント帳（`event_book`）**: 会話を跨いで参照する有界ワーキングメモリ（高優先 4 枠、通常項目は終了条件必須、72 時間未更新で再確認フラグ）。
- **任意のエピソード記憶（`episode-memory.sqlite3`）**: `ECHOLOOM_EPISODE_RAG=1` で有効化。直近ウィンドウ外の曖昧な過去イベントを補完検索し、すでにコンテキスト内に表示済みの出典は自動除外します。12 件以上に増えた会話はアイドル時にオフライン統合します。

---

## ローカルワークベンチ（`echoloom dashboard`）

ビルド不要のローカル専用 Web UI（`127.0.0.1` / `localhost` 限定、厳格な CSP、同一オリジン変更ガード）を同梱しており、SQLite を直接操作せずに状態確認と修正ができます。

- **会話ログ & ツール呼び出し**: バッチ入力・モデルリクエスト・ツール引数と結果・セッション切替を時系列で確認し、JSONL 形式でエクスポート可能。
- **ツール管理**: メインチャットおよびアイドルエージェント向けツールの有効／無効を即時切替。
- **人物アーカイブ**: 人間管理の `Remark` 編集、断片化警告（8 件以上）の確認、タグや `sender_memories` の追加・削除。
- **記憶ライブラリ**: `user_facts`（Top-40 インデックス状況付き）、`event_book`（枠使用状況と 72 時間経過表示）、`entity_terms`、`annotations`、`episodes` の閲覧・検索・元メッセージ参照・インライン編集・削除。構造化記憶の編集は `MemoryStore` / `EventBook` 経由で記録して改訂・監査履歴を保持し、エピソード削除は `episode-memory.sqlite3` から該当レコードを削除します。

---

## アーキテクチャとチャネル分離

コアランタイムは特定のチャット SDK に依存しません。各プラットフォームは [`ChannelConnector`](src/echoloom/connectors/base.py) プロトコル背後に配置され、6 層の一方向依存ルールは [`tests/test_architecture_boundaries.py`](tests/test_architecture_boundaries.py) の AST テストで検証されます。

| チャネル | Factory | 主な機能 |
|---|---|---|
| Webhook | `echoloom.connectors.webhook:WebhookConnector` | HTTP JSON 受信、Bearer / HMAC-SHA256 検証、SSRF 対策、送信プッシュ |
| Telegram | `echoloom.connectors.telegram:TelegramConnector` | Secret-Token 検証、テキスト/画像/音声/返信パース、Bot API 送信 |
| Feishu / Lark | `echoloom.connectors.feishu:FeishuConnector` | Event v2、Challenge・署名検証、テナント Token キャッシュ、メディア送受信 |
| Discord | `echoloom.connectors.discord:DiscordConnector` | 添付ファイル・返信パース、Bot REST 送信 |
| WeChat Hook（任意） | `echoloom.connectors.wechat_hook:WeChatHookConnector` | 明示的に有効化したときのみ読み込まれる Windows デスクトップ向けアダプタ |

---

## クイックスタート

Python 3.12+ が必要です。

```bash
# 1. 開発モードでインストール
pip install -e .

# 2. gitignored なローカル設定ボールト (.local/) を初期化
echoloom init

# 3. モデル API キーを設定（Windows では `echoloom credentials store` も利用可能）
export GOOGLE_API_KEY="your-key"    # または OPENAI_API_KEY

# 4. ランタイムを起動（ローカル HTTP Ingress + Worker）
echoloom serve

# 5. 別ターミナルからテストイベントを送信
echoloom send --text "Hello EchoLoom"
```

主な運用コマンド:

```bash
echoloom serve --role ingress   # HTTP 受信と LLM Worker を別プロセスで起動
echoloom serve --role worker
echoloom dashboard              # ローカル Trace・記憶ワークベンチを起動
echoloom vault audit            # .local/ の隔離状態と git 未追跡シークレットを監査
```

応答・記憶整理・メディア解析・事前判定用の各モデルは、コード変更なしで `.local/models.toml` または環境変数から差し替え可能です。

---

## ドキュメント

- [コネクタとマルチソース Ingress](docs/connectors-and-sources.md)
- [モデルロール & プロファイル設定](docs/agent-profile-config.md)
- [判定モデル設定](docs/jev-strategy.md)
- [コスト・バジェット監査](docs/pricing-audit.md)
- [CLI・運用ガイド](docs/commands.md)

## ライセンス

[0BSD](LICENSE)
