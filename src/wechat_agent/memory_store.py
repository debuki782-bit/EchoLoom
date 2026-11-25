"""Auditable project memory records; ADK remains the session/search adapter layer."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from .repos.knowledge_repo import (
    MAX_ENTITY_NOTE_CHARS,
    MAX_ENTITY_TERM_CHARS,
    MAX_FACT_CHARS,
    MAX_NOTE_CHARS,
    MAX_QUERY_CHARS,
    MAX_SENDER_MEMORY_CHARS,
    KnowledgeRepo,
    _like_escape,
)
from .repos.media_repo import MediaRepo
from .repos.message_repo import (
    DEFAULT_RETENTION_DAYS,
    MessageRepo,
    StoredMessage,
    _bounded_text,
    _now,
    _source_platform_message_id,
)

__all__ = [
    "DEFAULT_RETENTION_DAYS",
    "MAX_ENTITY_NOTE_CHARS",
    "MAX_ENTITY_TERM_CHARS",
    "MAX_FACT_CHARS",
    "MAX_NOTE_CHARS",
    "MAX_QUERY_CHARS",
    "MAX_SENDER_MEMORY_CHARS",
    "MemoryStore",
    "StoredMessage",
    "_bounded_text",
    "_gap_minutes",
    "_like_escape",
    "_now",
    "_source_platform_message_id",
]


def _gap_minutes(current_time: datetime, previous_at: str | None) -> int | None:
    if previous_at is None:
        return None
    previous_time = datetime.fromisoformat(previous_at)
    seconds = max(0, int((current_time - previous_time).total_seconds()))
    return seconds // 60


class MemoryStore:
    """Facade over MessageRepo, MediaRepo, and KnowledgeRepo sharing one SQLite connection."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._media_root = (path.parent / "received-images").resolve()
        self._audio_root = (path.parent / "received-audio").resolve()
        self.path = Path(path).resolve()
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=1, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS source_messages (
              message_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              conversation_id TEXT NOT NULL, sender_id TEXT NOT NULL,
              platform_message_id TEXT,
              captured_at TEXT NOT NULL, raw_json TEXT NOT NULL,
              raw_sha256 TEXT NOT NULL, body_text TEXT NOT NULL,
              expires_at TEXT NOT NULL, deleted_at TEXT,
              source_fingerprint TEXT, event_time_context_json TEXT,
              direction TEXT NOT NULL DEFAULT 'inbound', delivery_state TEXT
            );
            CREATE INDEX IF NOT EXISTS source_scope_time
              ON source_messages(account_id, conversation_id, captured_at);
            CREATE INDEX IF NOT EXISTS source_sender_time
              ON source_messages(account_id, conversation_id, sender_id, captured_at, message_id);
            CREATE TABLE IF NOT EXISTS annotations (
              note_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              message_id TEXT NOT NULL REFERENCES source_messages(message_id),
              author_kind TEXT NOT NULL CHECK(author_kind IN ('agent','user')),
              author_id TEXT NOT NULL, trust_label TEXT NOT NULL,
              content TEXT NOT NULL, source TEXT NOT NULL,
              version INTEGER NOT NULL, state TEXT NOT NULL
                CHECK(state IN ('active','deleted','superseded')),
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS annotation_current
              ON annotations(account_id, message_id, state);
            CREATE TABLE IF NOT EXISTS message_jev_labels (
              account_id TEXT NOT NULL,
              message_id TEXT NOT NULL REFERENCES source_messages(message_id),
              reply_pct INTEGER NOT NULL CHECK(reply_pct BETWEEN 0 AND 100),
              concise_pct INTEGER NOT NULL CHECK(concise_pct BETWEEN 0 AND 100),
              no_reply_pct INTEGER NOT NULL CHECK(no_reply_pct BETWEEN 0 AND 100),
              multi_turn_pct INTEGER NOT NULL CHECK(multi_turn_pct BETWEEN 0 AND 100),
              model TEXT NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL,
              PRIMARY KEY(account_id,message_id),
              CHECK(reply_pct + concise_pct + no_reply_pct = 100)
            );
            CREATE INDEX IF NOT EXISTS message_jev_order
              ON message_jev_labels(account_id,updated_at);
            CREATE TABLE IF NOT EXISTS annotation_revisions (
              revision_id TEXT PRIMARY KEY, note_id TEXT NOT NULL,
              version INTEGER NOT NULL, action TEXT NOT NULL,
              author_kind TEXT NOT NULL, author_id TEXT NOT NULL,
              trust_label TEXT NOT NULL, content TEXT NOT NULL,
              source TEXT NOT NULL, created_at TEXT NOT NULL,
              UNIQUE(note_id, version)
            );
            CREATE TABLE IF NOT EXISTS user_facts (
              fact_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              fact_key TEXT NOT NULL, content TEXT NOT NULL,
              source_message_id TEXT NOT NULL REFERENCES source_messages(message_id),
              author_id TEXT NOT NULL, trust_label TEXT NOT NULL,
              version INTEGER NOT NULL, state TEXT NOT NULL
                CHECK(state IN ('active','superseded','deleted')),
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
              UNIQUE(account_id, fact_key, version)
            );
            CREATE TABLE IF NOT EXISTS entity_terms (
              entity_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              term TEXT NOT NULL, explanation TEXT NOT NULL,
              author_id TEXT NOT NULL, trust_label TEXT NOT NULL,
              version INTEGER NOT NULL, state TEXT NOT NULL
                CHECK(state IN ('active','deleted')),
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
              UNIQUE(account_id, term, version)
            );
            CREATE TABLE IF NOT EXISTS sender_memories (
              memory_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              sender_id TEXT NOT NULL, topic TEXT NOT NULL, content TEXT NOT NULL,
              trust_label TEXT NOT NULL DEFAULT 'untrusted_reference_data',
              state TEXT NOT NULL CHECK(state IN ('active','deleted')),
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sender_profiles (
              account_id TEXT NOT NULL,sender_id TEXT NOT NULL,
              remark TEXT NOT NULL,version INTEGER NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('active','deleted')),
              updated_by TEXT NOT NULL,updated_at TEXT NOT NULL,
              PRIMARY KEY(account_id,sender_id)
            );
            CREATE TABLE IF NOT EXISTS sender_profile_revisions (
              revision_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,
              sender_id TEXT NOT NULL,version INTEGER NOT NULL,
              remark TEXT NOT NULL,action TEXT NOT NULL,
              actor TEXT NOT NULL,at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS sender_tags (
              account_id TEXT NOT NULL,sender_id TEXT NOT NULL,tag TEXT NOT NULL,
              created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
              PRIMARY KEY(account_id,sender_id,tag)
            );
            CREATE TABLE IF NOT EXISTS sender_tag_diffs (
              diff_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,sender_id TEXT NOT NULL,
              action TEXT NOT NULL CHECK(action IN ('add','remove','replace_add','replace_remove')),
              tag TEXT NOT NULL,source_message_id TEXT,source_task_id TEXT,at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS sender_tag_diff_order
              ON sender_tag_diffs(account_id,sender_id,at,diff_id);
            CREATE TABLE IF NOT EXISTS memory_audit (
              audit_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              actor_kind TEXT NOT NULL, actor_id TEXT NOT NULL,
              action TEXT NOT NULL, object_type TEXT NOT NULL,
              object_id TEXT NOT NULL, version INTEGER NOT NULL,
              at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS agent_tool_mutations (
              operation_id TEXT NOT NULL,item_key TEXT NOT NULL,result_json TEXT NOT NULL,
              created_at TEXT NOT NULL,PRIMARY KEY(operation_id,item_key)
            );
            CREATE TABLE IF NOT EXISTS message_media (
              image_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              conversation_id TEXT NOT NULL,
              message_id TEXT NOT NULL UNIQUE REFERENCES source_messages(message_id),
              original_references_json TEXT NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('pending','ready','failed','deleted')),
              artifact_path TEXT, mime_type TEXT, byte_count INTEGER,
              sha256 TEXT, source_sha256 TEXT, error_code TEXT,
              vision_summary TEXT, vision_model_id TEXT, vision_inspected_at TEXT,
              library_kind TEXT NOT NULL DEFAULT 'image',
              library_prompt TEXT NOT NULL DEFAULT '',
              quick_send INTEGER NOT NULL DEFAULT 0,
              display_order INTEGER NOT NULL DEFAULT 0,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS media_scope_time
              ON message_media(account_id,conversation_id,created_at);
            CREATE TABLE IF NOT EXISTS image_library_revisions (
              revision_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              image_id TEXT NOT NULL, actor TEXT NOT NULL, action TEXT NOT NULL,
              library_kind TEXT NOT NULL, library_prompt TEXT NOT NULL,
              quick_send INTEGER NOT NULL, display_order INTEGER NOT NULL,
              at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS message_audio (
              audio_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
              conversation_id TEXT NOT NULL,
              message_id TEXT NOT NULL UNIQUE REFERENCES source_messages(message_id),
              state TEXT NOT NULL CHECK(state IN ('pending','ready','failed','deleted')),
              artifact_path TEXT, mime_type TEXT, byte_count INTEGER,
              sha256 TEXT, duration_ms INTEGER, error_code TEXT,
              transcript TEXT, transcript_model_id TEXT, transcript_state TEXT,
              created_at TEXT NOT NULL, updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS audio_scope_time
              ON message_audio(account_id,conversation_id,created_at);
            """
        )
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(source_messages)")}
        media_columns = {row[1] for row in self.db.execute("PRAGMA table_info(message_media)")}
        needs_library_backfill = "library_kind" not in media_columns
        if "source_kind" not in media_columns:
            self.db.execute(
                "ALTER TABLE message_media ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'original'"
            )
        if "source_fingerprint" not in columns:
            self.db.execute("ALTER TABLE source_messages ADD COLUMN source_fingerprint TEXT")
        if "event_time_context_json" not in columns:
            self.db.execute("ALTER TABLE source_messages ADD COLUMN event_time_context_json TEXT")
        if "direction" not in columns:
            self.db.execute(
                "ALTER TABLE source_messages ADD COLUMN direction TEXT NOT NULL DEFAULT 'inbound'"
            )
        if "delivery_state" not in columns:
            self.db.execute("ALTER TABLE source_messages ADD COLUMN delivery_state TEXT")
        needs_platform_id_backfill = "platform_message_id" not in columns
        if needs_platform_id_backfill:
            self.db.execute("ALTER TABLE source_messages ADD COLUMN platform_message_id TEXT")
        media_columns = {row[1] for row in self.db.execute("PRAGMA table_info(message_media)")}
        for name, kind in (
            ("vision_summary", "TEXT"),
            ("vision_model_id", "TEXT"),
            ("vision_inspected_at", "TEXT"),
            ("library_kind", "TEXT NOT NULL DEFAULT 'image'"),
            ("library_prompt", "TEXT NOT NULL DEFAULT ''"),
            ("quick_send", "INTEGER NOT NULL DEFAULT 0"),
            ("display_order", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if name not in media_columns:
                self.db.execute(f"ALTER TABLE message_media ADD COLUMN {name} {kind}")
        if needs_library_backfill:
            self.db.execute(
                "UPDATE message_media SET library_kind='image', "
                "quick_send=CASE WHEN EXISTS (SELECT 1 FROM source_messages s "
                "WHERE s.message_id=message_media.message_id AND s.direction='outbound') "
                "THEN 1 ELSE 0 END"
            )
            self.db.execute(
                "UPDATE message_media SET library_prompt=COALESCE((SELECT a.content "
                "FROM annotations a WHERE a.message_id=message_media.message_id "
                "AND a.state='active' ORDER BY a.updated_at DESC LIMIT 1),'') "
                "WHERE library_kind='image' AND library_prompt=''"
            )
        self.db.execute("UPDATE message_media SET library_kind='image' WHERE library_kind!='image'")
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS media_library_order "
            "ON message_media(account_id,quick_send,display_order,created_at)"
        )
        self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS source_replay_fingerprint "
            "ON source_messages(account_id,source_fingerprint) "
            "WHERE source_fingerprint IS NOT NULL"
        )
        if needs_platform_id_backfill:
            self._backfill_platform_message_ids()
        self.db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS source_platform_message "
            "ON source_messages(account_id,conversation_id,platform_message_id) "
            "WHERE platform_message_id IS NOT NULL"
        )
        self.db.commit()
        self._migrate_sender_memories_to_multi_entry()

        self.knowledge = KnowledgeRepo(self.db, self._lock, self.transaction)
        self.messages = MessageRepo(
            self.db,
            self._lock,
            self.transaction,
            ensure_note=self.knowledge.ensure_note,
        )
        self.media = MediaRepo(
            self.db,
            self._lock,
            self.transaction,
            media_root=self._media_root,
            audio_root=self._audio_root,
            messages=self.messages,
            knowledge=self.knowledge,
        )

    @property
    def media_root(self) -> Path:
        return self._media_root

    @media_root.setter
    def media_root(self, value: Path) -> None:
        self._media_root = value
        if hasattr(self, "media"):
            self.media.media_root = value

    @property
    def audio_root(self) -> Path:
        return self._audio_root

    @audio_root.setter
    def audio_root(self, value: Path) -> None:
        self._audio_root = value
        if hasattr(self, "media"):
            self.media.audio_root = value

    @contextmanager
    def transaction(self):
        """Nested writes share the caller's transaction instead of committing it early."""
        with self._lock:
            nested = self.db.in_transaction
            savepoint = "memory_" + uuid4().hex
            self.db.execute(f"SAVEPOINT {savepoint}" if nested else "BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                if nested:
                    self.db.execute(f"ROLLBACK TO {savepoint}")
                    self.db.execute(f"RELEASE {savepoint}")
                else:
                    self.db.rollback()
                raise
            else:
                if nested:
                    self.db.execute(f"RELEASE {savepoint}")
                else:
                    self.db.commit()

    def _migrate_sender_memories_to_multi_entry(self) -> None:
        """Remove the legacy one-row-per-topic constraint without losing records."""
        for index in self.db.execute("PRAGMA index_list(sender_memories)").fetchall():
            if not index[2]:
                continue
            columns = tuple(
                row[2]
                for row in self.db.execute(
                    f"PRAGMA index_info('{str(index[1]).replace(chr(39), chr(39) * 2)}')"
                ).fetchall()
            )
            if columns != ("account_id", "sender_id", "topic"):
                continue
            with self._lock:
                self.db.execute("BEGIN IMMEDIATE")
                self.db.execute(
                    "CREATE TABLE sender_memories_multi_entry ("
                    "memory_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, "
                    "sender_id TEXT NOT NULL, topic TEXT NOT NULL, content TEXT NOT NULL, "
                    "trust_label TEXT NOT NULL DEFAULT 'untrusted_reference_data', "
                    "state TEXT NOT NULL CHECK(state IN ('active','deleted')), "
                    "created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
                )
                self.db.execute(
                    "INSERT INTO sender_memories_multi_entry "
                    "SELECT memory_id,account_id,sender_id,topic,content,trust_label,"
                    "state,created_at,updated_at "
                    "FROM sender_memories"
                )
                self.db.execute("DROP TABLE sender_memories")
                self.db.execute(
                    "ALTER TABLE sender_memories_multi_entry RENAME TO sender_memories"
                )
                self.db.execute(
                    "CREATE INDEX IF NOT EXISTS sender_memories_lookup "
                    "ON sender_memories(account_id,sender_id,state,updated_at)"
                )
                self.db.commit()
            break
        with self.db:
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS sender_memories_lookup "
                "ON sender_memories(account_id,sender_id,state,updated_at)"
            )

    def _backfill_platform_message_ids(self) -> None:
        """Backfill IDs only when an archived callback item matches its stored fingerprint."""
        rows = self.db.execute(
            "SELECT message_id,account_id,conversation_id,sender_id,source_fingerprint,raw_json "
            "FROM source_messages WHERE platform_message_id IS NULL "
            "AND direction='inbound' AND source_fingerprint IS NOT NULL"
        ).fetchall()
        with self.transaction():
            for row in rows:
                try:
                    envelope = json.loads(row["raw_json"])
                except (TypeError, json.JSONDecodeError):
                    continue
                messages = envelope.get("msglist") if isinstance(envelope, dict) else None
                if not isinstance(messages, list):
                    continue
                match_id = None
                for source in messages:
                    if not isinstance(source, dict):
                        continue
                    value = source.get("msgsvrid")
                    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                        continue
                    canonical = json.dumps(
                        source, ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    )
                    fingerprint = hashlib.sha256(
                        f"{row['account_id']}\0{row['conversation_id']}\0"
                        f"{row['sender_id']}\0{canonical}".encode()
                    ).hexdigest()
                    if fingerprint == row["source_fingerprint"]:
                        match_id = str(value)
                        break
                if match_id is None:
                    continue
                used = self.db.execute(
                    "SELECT 1 FROM source_messages WHERE account_id=? AND conversation_id=? "
                    "AND platform_message_id=? LIMIT 1",
                    (row["account_id"], row["conversation_id"], match_id),
                ).fetchone()
                if used is None:
                    self.db.execute(
                        "UPDATE source_messages SET platform_message_id=? WHERE message_id=?",
                        (match_id, row["message_id"]),
                    )

    # ------------------------------------------------------------------
    # Cross-repo orchestrators
    # ------------------------------------------------------------------

    def enrich_context_messages(
        self,
        *,
        account_id: str,
        messages: Sequence[Mapping[str, object]],
        entity_limit: int = 30,
    ) -> list[dict[str, object]]:
        """Attach one fresh, batched snapshot of notes, terms, and media to records.

        Call once for the current message, quote target, same-chat history, and
        cross-chat history together. This keeps all displayed records on the same
        retrieval path while loading active dictionary entries only once per turn.
        """
        if not 1 <= entity_limit <= 50 or len(messages) > 120:
            raise ValueError("context enrichment limits out of range")
        message_ids = list(
            dict.fromkeys(
                message_id
                for item in messages
                if isinstance((message_id := item.get("message_id")), str) and message_id
            )
        )
        sender_ids = list(
            dict.fromkeys(
                sender_id
                for item in messages
                if isinstance((sender_id := item.get("sender_id")), str) and sender_id
            )
        )
        sender_memories_by_id = self.knowledge.sender_memories(
            account_id=account_id,
            sender_ids=sender_ids,
        )
        tag_requests = [
            (
                str(item["message_id"]),
                str(item.get("sender_id", "")),
                str(item.get("captured_at", "9999-12-31T23:59:59+00:00")),
            )
            for item in messages
            if isinstance(item.get("message_id"), str)
        ]
        tags_by_message = self.knowledge.sender_tags_many_at(
            account_id=account_id,
            requests=tag_requests,
        )
        notes_by_message: dict[str, list[dict[str, object]]] = {}
        jev_by_message: dict[str, dict[str, object]] = {}
        images_by_message: dict[str, dict[str, object]] = {}
        audio_by_message: dict[str, dict[str, object]] = {}
        with self._lock:
            if message_ids:
                notes_by_message = self.knowledge.notes_for_messages(
                    account_id=account_id,
                    message_ids=message_ids,
                )
                jev_by_message = self.messages.jev_labels_for_messages(
                    account_id=account_id,
                    message_ids=message_ids,
                )
                images_by_message = self.media.image_metadata_for_messages(
                    account_id=account_id,
                    message_ids=message_ids,
                )
                audio_by_message = self.media.audio_metadata_for_messages(
                    account_id=account_id,
                    message_ids=message_ids,
                )
        search_texts = []
        for item in messages:
            message_id = item.get("message_id")
            key = message_id if isinstance(message_id, str) else ""
            base_image = item.get("image")
            base_audio = item.get("audio")
            image = {
                **(dict(base_image) if isinstance(base_image, Mapping) else {}),
                **images_by_message.get(key, {}),
            }
            audio = {
                **(dict(base_audio) if isinstance(base_audio, Mapping) else {}),
                **audio_by_message.get(key, {}),
            }
            search_texts.append(
                "\n".join(
                    str(value)
                    for value in (
                        item.get("entity_search_text"),
                        item.get("body_text"),
                        (image or {}).get("vision_summary"),
                        (audio or {}).get("transcript"),
                    )
                    if value
                )[:12000]
            )
        entity_matches = self.knowledge.match_entities_many(
            account_id=account_id,
            texts=search_texts,
            limit=entity_limit,
        )
        gap_requests = []
        for item in messages:
            message_id = item.get("message_id")
            captured_at = item.get("captured_at")
            conversation_id = item.get("conversation_id")
            sender_id = item.get("sender_id")
            if (
                isinstance(message_id, str)
                and isinstance(captured_at, str)
                and isinstance(conversation_id, str)
                and isinstance(sender_id, str)
                and message_id
            ):
                gap_requests.append((message_id, conversation_id, sender_id, captured_at))
        gaps_by_message = self.messages._previous_message_times_many(
            account_id=account_id,
            requests=gap_requests,
        )
        enriched: list[dict[str, object]] = []
        computed: dict[str, dict[str, object]] = {}
        for index, item in enumerate(messages):
            message_id = item.get("message_id")
            cache_key = message_id if isinstance(message_id, str) else None
            if cache_key and cache_key in computed:
                data = dict(item)
                data.update(computed[cache_key])
                enriched.append(data)
                continue
            data = dict(item)
            captured_at = item.get("captured_at")
            if isinstance(cache_key, str) and cache_key in gaps_by_message:
                prior_chat, prior_sender = gaps_by_message[cache_key]
                current_time = datetime.fromisoformat(str(captured_at))
                data["conversation_gap_minutes"] = _gap_minutes(current_time, prior_chat)
                data["sender_gap_minutes"] = _gap_minutes(current_time, prior_sender)
            else:
                data["conversation_gap_minutes"] = None
                data["sender_gap_minutes"] = None
            data.update(
                {
                    "annotations": notes_by_message.get(cache_key or "", []),
                    "jev_label": jev_by_message.get(cache_key or ""),
                    "entities": entity_matches[index],
                    "sender_memories": sender_memories_by_id.get(
                        str(item.get("sender_id", "")),
                        [],
                    ),
                    "sender_tags": tags_by_message.get(cache_key or "", []),
                    "image": {
                        **(dict(item["image"]) if isinstance(item.get("image"), Mapping) else {}),
                        **images_by_message.get(cache_key or "", {}),
                    }
                    or None,
                    "audio": {
                        **(dict(item["audio"]) if isinstance(item.get("audio"), Mapping) else {}),
                        **audio_by_message.get(cache_key or "", {}),
                    }
                    or None,
                }
            )
            if cache_key:
                computed[cache_key] = {
                    key: data[key]
                    for key in (
                        "annotations",
                        "jev_label",
                        "entities",
                        "sender_memories",
                        "sender_tags",
                        "conversation_gap_minutes",
                        "sender_gap_minutes",
                        "image",
                        "audio",
                    )
                }
            enriched.append(data)
        return enriched

    def purge_expired(self, *, now: str | None = None) -> int:
        now = now or _now()
        with self.transaction():
            message_ids = self.messages.soft_delete_expired(now=now)
            for message_id in message_ids:
                self.media.purge_for_message(message_id=message_id, now=now)
                self.knowledge.purge_for_message(message_id=message_id, now=now)
            return len(message_ids)

    # ------------------------------------------------------------------
    # MessageRepo delegates
    # ------------------------------------------------------------------

    _message_dict = staticmethod(MessageRepo._message_dict)

    def archive_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        sender_id: str,
        raw_envelope: dict,
        body_text: str,
        source_record: dict | None = None,
        event_time_context: Mapping[str, object] | None = None,
        direction: str = "inbound",
        delivery_state: str | None = None,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        source_message_id: int | None = None,
    ) -> StoredMessage:
        return self.messages.archive_message(
            account_id=account_id,
            conversation_id=conversation_id,
            sender_id=sender_id,
            raw_envelope=raw_envelope,
            body_text=body_text,
            source_record=source_record,
            event_time_context=event_time_context,
            direction=direction,
            delivery_state=delivery_state,
            retention_days=retention_days,
            source_message_id=source_message_id,
        )

    def archive_agent_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        text: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
    ) -> StoredMessage:
        return self.messages.archive_agent_message(
            account_id=account_id,
            conversation_id=conversation_id,
            text=text,
            operation_id=operation_id,
            delivery_state=delivery_state,
            note=note,
            library_prompt=library_prompt,
        )

    def get_event_time_context(
        self, *, message_id: str, account_id: str
    ) -> dict[str, object] | None:
        return self.messages.get_event_time_context(
            message_id=message_id,
            account_id=account_id,
        )

    def recent_other_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 20,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        return self.messages.recent_other_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            now=now,
        )

    def recent_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 50,
        exclude_message_id: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        return self.messages.recent_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            exclude_message_id=exclude_message_id,
            now=now,
        )

    def recent_unanswered_inbound(
        self,
        *,
        account_id: str,
        since: datetime,
        limit: int = 2048,
    ) -> list[dict[str, object]]:
        return self.messages.recent_unanswered_inbound(
            account_id=account_id,
            since=since,
            limit=limit,
        )

    def list_conversations(self, *, account_id: str, limit: int = 100) -> list[dict[str, object]]:
        return self.messages.list_conversations(account_id=account_id, limit=limit)

    def search_messages(
        self,
        *,
        account_id: str,
        query: str,
        conversation_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        return self.messages.search_messages(
            account_id=account_id,
            query=query,
            conversation_id=conversation_id,
            limit=limit,
        )

    def messages_for_idle_event(
        self, *, account_id: str, limit: int = 100
    ) -> list[dict[str, object]]:
        return self.messages.messages_for_idle_event(account_id=account_id, limit=limit)

    def reconstruct_message(self, message_id: str, account_id: str) -> dict:
        return self.messages.reconstruct_message(message_id, account_id)

    def find_message_by_platform_id(
        self, *, account_id: str, conversation_id: str, platform_message_id: str
    ) -> dict | None:
        return self.messages.find_message_by_platform_id(
            account_id=account_id,
            conversation_id=conversation_id,
            platform_message_id=platform_message_id,
        )

    def _previous_message_times_many(
        self,
        *,
        account_id: str,
        requests: Sequence[tuple[str, str, str, str]],
    ) -> dict[str, tuple[str | None, str | None]]:
        return self.messages._previous_message_times_many(
            account_id=account_id,
            requests=requests,
        )

    def minutes_since_last_human_message(
        self, *, account_id: str, conversation_id: str
    ) -> int | None:
        return self.messages.minutes_since_last_human_message(
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def count_effective_messages(
        self, *, account_id: str, conversation_id: str, since: datetime
    ) -> int:
        return self.messages.count_effective_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            since=since,
        )

    def message_belongs_to(
        self, *, message_id: str, account_id: str, conversation_id: str
    ) -> bool:
        return self.messages.message_belongs_to(
            message_id=message_id,
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def message_exists_for_sender(self, account_id: str, sender_id: str) -> bool:
        return self.messages.message_exists_for_sender(account_id, sender_id)

    def save_jev_labels(
        self, *, account_id: str, labels: Sequence[Mapping[str, object]]
    ) -> int:
        return self.messages.save_jev_labels(account_id=account_id, labels=labels)

    def _message_in_scope(self, message_id: str, account_id: str) -> bool:
        return self.messages._message_in_scope(message_id, account_id)

    def _conversation_for_message(self, message_id: str, account_id: str) -> str:
        return self.messages._conversation_for_message(message_id, account_id)

    # ------------------------------------------------------------------
    # MediaRepo delegates
    # ------------------------------------------------------------------

    def archive_agent_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        artifact_path: Path,
        mime_type: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
        quick_send: bool = True,
    ) -> dict[str, str]:
        return self.media.archive_agent_image(
            account_id=account_id,
            conversation_id=conversation_id,
            artifact_path=artifact_path,
            mime_type=mime_type,
            operation_id=operation_id,
            delivery_state=delivery_state,
            note=note,
            library_prompt=library_prompt,
            quick_send=quick_send,
        )

    def import_library_image(
        self,
        *,
        account_id: str,
        source_path: Path,
        description: str = "",
        display_order: int = 0,
        quick_send: bool = True,
    ) -> dict[str, object]:
        return self.media.import_library_image(
            account_id=account_id,
            source_path=source_path,
            description=description,
            display_order=display_order,
            quick_send=quick_send,
        )

    def begin_image_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        original_references: Sequence[dict[str, str]],
    ) -> dict[str, object]:
        return self.media.begin_image_ingest(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            original_references=original_references,
        )

    def complete_image_ingest(
        self,
        *,
        image_id: str,
        account_id: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
        source_kind: str = "original",
    ) -> None:
        self.media.complete_image_ingest(
            image_id=image_id,
            account_id=account_id,
            artifact_path=artifact_path,
            mime_type=mime_type,
            byte_count=byte_count,
            sha256=sha256,
            source_sha256=source_sha256,
            source_kind=source_kind,
        )

    def promote_inferred_image(
        self,
        *,
        image_id: str,
        account_id: str,
        previous_sha256: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
    ) -> bool:
        return self.media.promote_inferred_image(
            image_id=image_id,
            account_id=account_id,
            previous_sha256=previous_sha256,
            artifact_path=artifact_path,
            mime_type=mime_type,
            byte_count=byte_count,
            sha256=sha256,
            source_sha256=source_sha256,
        )

    def fail_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> None:
        self.media.fail_image_ingest(
            image_id=image_id,
            account_id=account_id,
            error_code=error_code,
        )

    def retry_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> bool:
        return self.media.retry_image_ingest(
            image_id=image_id,
            account_id=account_id,
            error_code=error_code,
        )

    def image_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.image_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        )

    def save_image_description(
        self, *, account_id: str, image_id: str, description: str, model_id: str
    ) -> None:
        self.media.save_image_description(
            account_id=account_id,
            image_id=image_id,
            description=description,
            model_id=model_id,
        )

    def get_image_by_id(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        image_id: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_image_by_id(
            account_id=account_id,
            conversation_id=conversation_id,
            image_id=image_id,
            include_unready=include_unready,
        )

    def list_images(
        self,
        *,
        account_id: str,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        return self.media.list_images(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            offset=offset,
        )

    def quick_send_images(self, *, account_id: str, limit: int = 10) -> list[dict[str, object]]:
        return self.media.quick_send_images(account_id=account_id, limit=limit)

    def manage_image_library(
        self,
        *,
        account_id: str,
        image_id: str,
        actor: str = "agent",
        description: str | None = None,
        quick_send: bool | None = None,
        display_order: int | None = None,
    ) -> dict[str, object]:
        return self.media.manage_image_library(
            account_id=account_id,
            image_id=image_id,
            actor=actor,
            description=description,
            quick_send=quick_send,
            display_order=display_order,
        )

    def get_image_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.get_image_for_message(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def get_latest_ready_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_latest_ready_image(
            account_id=account_id,
            conversation_id=conversation_id,
            before_message_id=before_message_id,
            include_unready=include_unready,
        )

    def get_image_by_reference(
        self,
        *,
        account_id: str,
        conversation_id: str,
        reference: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_image_by_reference(
            account_id=account_id,
            conversation_id=conversation_id,
            reference=reference,
            include_unready=include_unready,
        )

    def begin_audio_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        duration_ms: int | None,
    ) -> dict[str, object]:
        return self.media.begin_audio_ingest(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            duration_ms=duration_ms,
        )

    def fail_audio_ingest_for_message(
        self, *, account_id: str, message_id: str, error_code: str
    ) -> dict[str, object]:
        return self.media.fail_audio_ingest_for_message(
            account_id=account_id,
            message_id=message_id,
            error_code=error_code,
        )

    def complete_audio_ingest(
        self,
        *,
        audio_id: str,
        account_id: str,
        artifact_path: str,
        byte_count: int,
        sha256: str,
        duration_ms: int,
    ) -> None:
        self.media.complete_audio_ingest(
            audio_id=audio_id,
            account_id=account_id,
            artifact_path=artifact_path,
            byte_count=byte_count,
            sha256=sha256,
            duration_ms=duration_ms,
        )

    def audio_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.audio_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        )

    def get_audio_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.get_audio_for_message(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def get_latest_ready_audio(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
    ) -> dict[str, object] | None:
        return self.media.get_latest_ready_audio(
            account_id=account_id,
            conversation_id=conversation_id,
            before_message_id=before_message_id,
        )

    def claim_audio_transcription(self, *, audio_id: str, account_id: str) -> str:
        return self.media.claim_audio_transcription(audio_id=audio_id, account_id=account_id)

    def save_audio_transcript(
        self, *, audio_id: str, account_id: str, transcript: str, model_id: str
    ) -> None:
        self.media.save_audio_transcript(
            audio_id=audio_id,
            account_id=account_id,
            transcript=transcript,
            model_id=model_id,
        )

    def finish_audio_transcription(self, *, audio_id: str, account_id: str, state: str) -> None:
        self.media.finish_audio_transcription(
            audio_id=audio_id,
            account_id=account_id,
            state=state,
        )

    # ------------------------------------------------------------------
    # KnowledgeRepo delegates
    # ------------------------------------------------------------------

    _tag_width = staticmethod(KnowledgeRepo._tag_width)

    def sender_tags_at(self, *, account_id: str, sender_id: str, at: str) -> list[str]:
        return self.knowledge.sender_tags_at(account_id=account_id, sender_id=sender_id, at=at)

    def sender_tags_many_at(
        self, *, account_id: str, requests: Sequence[tuple[str, str, str]]
    ) -> dict[str, list[str]]:
        return self.knowledge.sender_tags_many_at(account_id=account_id, requests=requests)

    def update_sender_tags(
        self,
        *,
        account_id: str,
        sender_id: str,
        action: str,
        tags: Sequence[str],
        source_message_id: str | None = None,
        source_task_id: str | None = None,
    ) -> dict[str, object]:
        return self.knowledge.update_sender_tags(
            account_id=account_id,
            sender_id=sender_id,
            action=action,
            tags=tags,
            source_message_id=source_message_id,
            source_task_id=source_task_id,
        )

    def note_belongs_to(self, *, note_id: str, account_id: str, conversation_id: str) -> bool:
        return self.knowledge.note_belongs_to(
            note_id=note_id,
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def note_conversation(self, *, note_id: str, account_id: str) -> str | None:
        return self.knowledge.note_conversation(note_id=note_id, account_id=account_id)

    def add_note(
        self,
        *,
        account_id: str,
        message_id: str,
        content: str,
        author_id: str = "agent",
        source: str = "agent_observation",
    ) -> str:
        return self.knowledge.add_note(
            account_id=account_id,
            message_id=message_id,
            content=content,
            author_id=author_id,
            source=source,
        )

    def revise_note(
        self, *, account_id: str, note_id: str, content: str | None, actor_id: str = "agent"
    ) -> int:
        return self.knowledge.revise_note(
            account_id=account_id,
            note_id=note_id,
            content=content,
            actor_id=actor_id,
        )

    def notes_for_message(self, *, account_id: str, message_id: str) -> list[dict[str, str]]:
        return self.knowledge.notes_for_message(account_id=account_id, message_id=message_id)

    def search_notes(
        self, *, account_id: str, conversation_id: str, query: str, limit: int = 8
    ) -> list[dict[str, str]]:
        return self.knowledge.search_notes(
            account_id=account_id,
            conversation_id=conversation_id,
            query=query,
            limit=limit,
        )

    def remember_fact(
        self,
        *,
        account_id: str,
        fact_key: str,
        content: str,
        source_message_id: str,
        actor_id: str = "agent",
    ) -> str:
        return self.knowledge.remember_fact(
            account_id=account_id,
            fact_key=fact_key,
            content=content,
            source_message_id=source_message_id,
            actor_id=actor_id,
        )

    def search_facts(self, *, account_id: str, query: str, limit: int = 8) -> list[dict]:
        return self.knowledge.search_facts(account_id=account_id, query=query, limit=limit)

    def delete_fact(self, *, account_id: str, fact_id: str, actor_id: str = "agent") -> None:
        self.knowledge.delete_fact(account_id=account_id, fact_id=fact_id, actor_id=actor_id)

    def upsert_entity(
        self, *, account_id: str, term: str, explanation: str, actor_id: str = "agent"
    ) -> str:
        return self.knowledge.upsert_entity(
            account_id=account_id,
            term=term,
            explanation=explanation,
            actor_id=actor_id,
        )

    def upsert_sender_memory(
        self, *, account_id: str, sender_id: str, topic: str, content: str
    ) -> tuple[str, bool]:
        return self.knowledge.upsert_sender_memory(
            account_id=account_id,
            sender_id=sender_id,
            topic=topic,
            content=content,
        )

    def sender_memories(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, list[dict[str, object]]]:
        return self.knowledge.sender_memories(account_id=account_id, sender_ids=sender_ids)

    def sender_remarks(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, str]:
        return self.knowledge.sender_remarks(account_id=account_id, sender_ids=sender_ids)

    def set_human_sender_remark(
        self,
        *,
        account_id: str,
        sender_id: str,
        remark: str,
        actor: str = "dashboard-user",
    ) -> dict[str, object]:
        return self.knowledge.set_human_sender_remark(
            account_id=account_id,
            sender_id=sender_id,
            remark=remark,
            actor=actor,
        )

    def priority_sender_profiles(
        self,
        *,
        account_id: str,
        tag: str = "唯一主人",
        current_conversation_id: str | None = None,
        exclude_message_ids: Sequence[str] = (),
    ) -> list[dict[str, object]]:
        return self.knowledge.priority_sender_profiles(
            account_id=account_id,
            tag=tag,
            current_conversation_id=current_conversation_id,
            exclude_message_ids=exclude_message_ids,
        )

    def delete_sender_memory(self, *, account_id: str, memory_id: str) -> bool:
        return self.knowledge.delete_sender_memory(account_id=account_id, memory_id=memory_id)

    def delete_entity(self, *, account_id: str, entity_id: str, actor_id: str = "agent") -> None:
        self.knowledge.delete_entity(account_id=account_id, entity_id=entity_id, actor_id=actor_id)

    def match_entities(self, *, account_id: str, text: str, limit: int = 30) -> list[dict]:
        return self.knowledge.match_entities(account_id=account_id, text=text, limit=limit)

    def match_entities_many(
        self, *, account_id: str, texts: Sequence[str], limit: int = 30
    ) -> list[list[dict]]:
        return self.knowledge.match_entities_many(account_id=account_id, texts=texts, limit=limit)

    def _note_revision(
        self,
        note_id: str,
        version: int,
        action: str,
        actor: str,
        content: str,
        source: str,
        now: str,
    ) -> None:
        self.knowledge._note_revision(note_id, version, action, actor, content, source, now)

    def _audit(
        self,
        scope: str,
        actor_kind: str,
        actor: str,
        action: str,
        object_type: str,
        object_id: str,
        version: int,
        now: str,
    ) -> None:
        self.knowledge._audit(scope, actor_kind, actor, action, object_type, object_id, version, now)

    def close(self) -> None:
        self.db.close()
