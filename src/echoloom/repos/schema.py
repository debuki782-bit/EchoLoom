"""SQLite schema initialization, migrations, and backfill for MemoryStore."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable
from contextlib import AbstractContextManager

_SCHEMA_SCRIPT = """
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
CREATE TABLE IF NOT EXISTS message_files (
  file_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
  conversation_id TEXT NOT NULL,
  message_id TEXT NOT NULL UNIQUE REFERENCES source_messages(message_id),
  original_references_json TEXT NOT NULL DEFAULT '[]',
  filename TEXT NOT NULL, fileext TEXT NOT NULL DEFAULT '',
  expected_bytes INTEGER, expected_md5 TEXT,
  state TEXT NOT NULL CHECK(state IN ('pending','ready','failed','deleted')),
  artifact_path TEXT, mime_type TEXT, byte_count INTEGER,
  sha256 TEXT, error_code TEXT,
  summary TEXT, summary_model_id TEXT, summarized_at TEXT,
  created_at TEXT NOT NULL, updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS file_scope_time
  ON message_files(account_id,conversation_id,created_at);
"""


def migrate_sender_memories_to_multi_entry(
    db: sqlite3.Connection,
    lock: threading.RLock,
) -> None:
    """Remove the legacy one-row-per-topic constraint without losing records."""
    for index in db.execute("PRAGMA index_list(sender_memories)").fetchall():
        if not index[2]:
            continue
        columns = tuple(
            row[2]
            for row in db.execute(
                f"PRAGMA index_info('{str(index[1]).replace(chr(39), chr(39) * 2)}')"
            ).fetchall()
        )
        if columns != ("account_id", "sender_id", "topic"):
            continue
        with lock:
            db.execute("BEGIN IMMEDIATE")
            db.execute(
                "CREATE TABLE sender_memories_multi_entry ("
                "memory_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, "
                "sender_id TEXT NOT NULL, topic TEXT NOT NULL, content TEXT NOT NULL, "
                "trust_label TEXT NOT NULL DEFAULT 'untrusted_reference_data', "
                "state TEXT NOT NULL CHECK(state IN ('active','deleted')), "
                "created_at TEXT NOT NULL, updated_at TEXT NOT NULL)"
            )
            db.execute(
                "INSERT INTO sender_memories_multi_entry "
                "SELECT memory_id,account_id,sender_id,topic,content,trust_label,"
                "state,created_at,updated_at "
                "FROM sender_memories"
            )
            db.execute("DROP TABLE sender_memories")
            db.execute(
                "ALTER TABLE sender_memories_multi_entry RENAME TO sender_memories"
            )
            db.execute(
                "CREATE INDEX IF NOT EXISTS sender_memories_lookup "
                "ON sender_memories(account_id,sender_id,state,updated_at)"
            )
            db.commit()
        break
    with db:
        db.execute(
            "CREATE INDEX IF NOT EXISTS sender_memories_lookup "
            "ON sender_memories(account_id,sender_id,state,updated_at)"
        )


def backfill_platform_message_ids(
    db: sqlite3.Connection,
    transaction_fn: Callable[[], AbstractContextManager[None]],
) -> None:
    """Backfill IDs only when an archived callback item matches its stored fingerprint."""
    rows = db.execute(
        "SELECT message_id,account_id,conversation_id,sender_id,source_fingerprint,raw_json "
        "FROM source_messages WHERE platform_message_id IS NULL "
        "AND direction='inbound' AND source_fingerprint IS NOT NULL"
    ).fetchall()
    with transaction_fn():
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
            used = db.execute(
                "SELECT 1 FROM source_messages WHERE account_id=? AND conversation_id=? "
                "AND platform_message_id=? LIMIT 1",
                (row["account_id"], row["conversation_id"], match_id),
            ).fetchone()
            if used is None:
                db.execute(
                    "UPDATE source_messages SET platform_message_id=? WHERE message_id=?",
                    (match_id, row["message_id"]),
                )


def init_memory_schema(
    db: sqlite3.Connection,
    lock: threading.RLock,
    transaction_fn: Callable[[], AbstractContextManager[None]],
) -> None:
    """Create tables and run additive compatibility migrations on ``agent-memory.sqlite3``."""
    db.executescript(_SCHEMA_SCRIPT)
    columns = {row[1] for row in db.execute("PRAGMA table_info(source_messages)")}
    media_columns = {row[1] for row in db.execute("PRAGMA table_info(message_media)")}
    needs_library_backfill = "library_kind" not in media_columns
    if "source_kind" not in media_columns:
        db.execute(
            "ALTER TABLE message_media ADD COLUMN source_kind TEXT NOT NULL DEFAULT 'original'"
        )
    for col, ddl in (
        ("source_fingerprint", "TEXT"),
        ("event_time_context_json", "TEXT"),
        ("direction", "TEXT NOT NULL DEFAULT 'inbound'"),
        ("delivery_state", "TEXT"),
    ):
        if col not in columns:
            db.execute(f"ALTER TABLE source_messages ADD COLUMN {col} {ddl}")
    needs_platform_id_backfill = "platform_message_id" not in columns
    if needs_platform_id_backfill:
        db.execute("ALTER TABLE source_messages ADD COLUMN platform_message_id TEXT")

    media_columns = {row[1] for row in db.execute("PRAGMA table_info(message_media)")}
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
            db.execute(f"ALTER TABLE message_media ADD COLUMN {name} {kind}")
    if needs_library_backfill:
        db.execute(
            "UPDATE message_media SET library_kind='image', "
            "quick_send=CASE WHEN EXISTS (SELECT 1 FROM source_messages s "
            "WHERE s.message_id=message_media.message_id AND s.direction='outbound') "
            "THEN 1 ELSE 0 END"
        )
        db.execute(
            "UPDATE message_media SET library_prompt=COALESCE((SELECT a.content "
            "FROM annotations a WHERE a.message_id=message_media.message_id "
            "AND a.state='active' ORDER BY a.updated_at DESC LIMIT 1),'') "
            "WHERE library_kind='image' AND library_prompt=''"
        )
    db.execute("UPDATE message_media SET library_kind='image' WHERE library_kind!='image'")
    db.execute(
        "CREATE INDEX IF NOT EXISTS media_library_order "
        "ON message_media(account_id,quick_send,display_order,created_at)"
    )
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS source_replay_fingerprint "
        "ON source_messages(account_id,source_fingerprint) "
        "WHERE source_fingerprint IS NOT NULL"
    )
    if needs_platform_id_backfill:
        backfill_platform_message_ids(db, transaction_fn)
    db.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS source_platform_message "
        "ON source_messages(account_id,conversation_id,platform_message_id) "
        "WHERE platform_message_id IS NOT NULL"
    )
    db.commit()
    migrate_sender_memories_to_multi_entry(db, lock)
