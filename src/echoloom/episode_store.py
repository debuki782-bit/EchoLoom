"""Pure SQLite persistence and vector scoring for conversation-scoped episode memory."""

from __future__ import annotations

import json
import sqlite3
import struct
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .context import ContextTurn, ContextWindow
from .model_catalog import load_model_catalog

_ROLES = load_model_catalog().roles
EMBEDDING_MODEL = _ROLES.embedding_model
DIMENSIONS = _ROLES.embedding_dimensions
VECTOR = struct.Struct(f"<{DIMENSIONS}f")
SEARCH_TOOL = "search_episode_memory"


@dataclass
class RecallTurn:
    window: ContextWindow
    seen: set[str] = field(default_factory=set)
    automatic: list[dict[str, str]] = field(default_factory=list)
    task: Any = None
    eligible: bool = False
    searched: bool = False

    @property
    def scope(self) -> tuple[str, str]:
        return self.window.account_id, self.window.conversation_id


recall_turn: ContextVar[RecallTurn | None] = ContextVar("episode_recall_turn", default=None)


class EpisodeStore:
    """SQLite-backed episode repository and cosine-similarity ranker (no LLM SDK dependencies)."""

    def __init__(self, path: Path, *, score_floor: float = 0.50) -> None:
        self.path = Path(path)
        self.score_floor = score_floor
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.path) as db:
            db.executescript("""
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS episodes (
                    id TEXT PRIMARY KEY, account_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                    summary TEXT NOT NULL, updated_at TEXT NOT NULL,
                    model TEXT NOT NULL, vector BLOB NOT NULL,
                    UNIQUE(account_id, conversation_id, summary)
                );
                CREATE INDEX IF NOT EXISTS episode_scope
                    ON episodes(account_id, conversation_id, updated_at);
                CREATE TABLE IF NOT EXISTS episode_sources (
                    episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE,
                    message_id TEXT NOT NULL, PRIMARY KEY (episode_id, message_id)
                );
                CREATE TABLE IF NOT EXISTS processed_batches (
                    account_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                    source_ids TEXT NOT NULL, PRIMARY KEY(account_id, conversation_id, source_ids)
                );
            """)

    @staticmethod
    def semantic_text(item: ContextTurn, *, include_media_notes: bool = False) -> str:
        raw = item.text.removeprefix(f"{item.sender_id}:\n")
        parts: list[str] = []
        parsed = None
        if raw.startswith("{") and raw.endswith("}"):
            try:
                candidate = json.loads(raw)
                if isinstance(candidate, dict) and "kind" in candidate:
                    parsed = candidate
            except (json.JSONDecodeError, TypeError):
                parsed = None
        if parsed is not None:
            kind = parsed.get("kind")
            if kind == "quoted_message":
                body = str(parsed.get("text") or "").strip()
                if body:
                    parts.append(body)
                quote = parsed.get("quote")
                if include_media_notes and isinstance(quote, dict):
                    quoted_body = str(quote.get("content") or "").strip()
                    if quoted_body:
                        parts.append(quoted_body[:200])
            elif kind == "app_message":
                for key in ("text", "description"):
                    val = str(parsed.get(key) or "").strip()
                    if val:
                        parts.append(val)
            elif kind == "file_message":
                name = str(parsed.get("filename") or parsed.get("text") or "").strip()
                if name:
                    parts.append(name)
            elif kind == "pat_message":
                suffix = str(parsed.get("suffix") or "").strip()
                if suffix:
                    parts.append(suffix)
        else:
            stripped = raw.strip()
            if stripped:
                parts.append(stripped)
        if include_media_notes:
            if item.image_metadata:
                vision = str(item.image_metadata.get("vision_summary") or "").strip()
                if vision:
                    parts.append(vision[:250])
            if item.audio_metadata:
                transcript = str(item.audio_metadata.get("transcript") or "").strip()
                if transcript:
                    parts.append(transcript[:250])
            if item.file_metadata:
                summary = str(item.file_metadata.get("summary") or "").strip()
                if summary:
                    parts.append(summary[:250])
        return "\n".join(parts).strip()

    @staticmethod
    def current_inbound(turn: RecallTurn) -> tuple[ContextTurn, ...]:
        return tuple(
            item
            for item in turn.window.turns
            if item.direction == "inbound"
            and item.message_id
            and item.text.strip()
            and item.source_conversation_id in (None, turn.scope[1])
            and item.sender_id != turn.scope[0]
        )

    @classmethod
    def recent_history(cls, turn: RecallTurn) -> list[str]:
        lines: list[str] = []
        for item in turn.window.history_turns[-3:]:
            if item.source_conversation_id in (None, turn.scope[1]):
                cleaned = cls.semantic_text(item, include_media_notes=True)[:250]
                if cleaned:
                    lines.append(cleaned)
        return lines

    def has_episodes(self, scope: tuple[str, str]) -> bool:
        with sqlite3.connect(self.path) as db:
            return (
                db.execute(
                    "SELECT 1 FROM episodes WHERE account_id=? AND conversation_id=? AND model=? LIMIT 1",
                    (*scope, EMBEDDING_MODEL),
                ).fetchone()
                is not None
            )

    def rank(
        self,
        scope: tuple[str, str],
        vector: tuple[float, ...],
        exclude_ids: set[str],
        visible_sources: set[str],
        limit: int,
    ) -> list[dict[str, str]]:
        with sqlite3.connect(self.path) as db:
            rows = db.execute(
                "SELECT id,summary,updated_at,vector FROM episodes "
                "WHERE account_id=? AND conversation_id=? AND model=?",
                (*scope, EMBEDDING_MODEL),
            ).fetchall()
            sources = db.execute(
                "SELECT episode_id,message_id FROM episode_sources JOIN episodes ON episodes.id=episode_id "
                "WHERE account_id=? AND conversation_id=?",
                scope,
            ).fetchall()
        excluded = exclude_ids | {eid for eid, mid in sources if mid in visible_sources}
        ranked = [
            (sum(a * b for a, b in zip(vector, VECTOR.unpack(blob))), stamp, eid, summary)
            for eid, summary, stamp, blob in rows
            if eid not in excluded
        ]
        return [
            {"id": eid, "summary": summary, "updated_at": stamp}
            for score, stamp, eid, summary in sorted(ranked, reverse=True)[:limit]
            if score >= self.score_floor
        ]

    def existing_for_batch(
        self, scope: tuple[str, str], source_ids_json: str
    ) -> dict[str, str] | None:
        with sqlite3.connect(self.path) as db:
            if db.execute(
                "SELECT 1 FROM processed_batches WHERE account_id=? "
                "AND conversation_id=? AND source_ids=?",
                (*scope, source_ids_json),
            ).fetchone():
                return None
            return dict(
                db.execute(
                    "SELECT id,summary FROM episodes WHERE account_id=? AND conversation_id=? "
                    "ORDER BY updated_at DESC LIMIT 12",
                    scope,
                )
            )

    def bloated_conversations(
        self, *, account_id: str, threshold: int = 12, limit: int = 3
    ) -> list[dict[str, object]]:
        """List conversations whose active episode count reached the offline Dreaming threshold."""
        if threshold < 2 or not 1 <= limit <= 20:
            raise ValueError("episode bloat thresholds out of range")
        with sqlite3.connect(self.path) as db:
            rows = db.execute(
                "SELECT conversation_id, COUNT(*) AS cnt, "
                "MIN(updated_at) AS oldest_at, MAX(updated_at) AS newest_at "
                "FROM episodes WHERE account_id=? AND model=? "
                "GROUP BY conversation_id HAVING COUNT(*) >= ? "
                "ORDER BY cnt DESC, oldest_at ASC LIMIT ?",
                (account_id, EMBEDDING_MODEL, threshold, limit),
            ).fetchall()
        return [
            {
                "conversation_id": str(row[0]),
                "count": int(row[1]),
                "oldest_at": str(row[2]),
                "newest_at": str(row[3]),
            }
            for row in rows
        ]

    def load_consolidation_rows(
        self, *, account_id: str, conversation_id: str, limit: int
    ) -> list[tuple[str, str, str, bytes]]:
        with sqlite3.connect(self.path) as db:
            rows = db.execute(
                "SELECT id, summary, updated_at, vector FROM episodes "
                "WHERE account_id=? AND conversation_id=? AND model=? "
                "ORDER BY updated_at ASC, id ASC LIMIT ?",
                (account_id, conversation_id, EMBEDDING_MODEL, limit),
            ).fetchall()
        return [(str(r[0]), str(r[1]), str(r[2]), bytes(r[3])) for r in rows]

    def apply_consolidation(
        self,
        *,
        account_id: str,
        conversation_id: str,
        staged: list[tuple[str | None, str, bytes, list[str]]],
        ids_to_remove: set[str],
        preserved_ids: set[str],
    ) -> int:
        stamp = datetime.now(UTC).isoformat()
        scope = (account_id, conversation_id)
        with sqlite3.connect(self.path) as db:
            db.execute("PRAGMA foreign_keys=ON")
            source_map: dict[str, set[str]] = {}
            all_ref = list(ids_to_remove | preserved_ids)
            if all_ref:
                placeholders = ",".join("?" for _ in all_ref)
                for eid, mid in db.execute(
                    f"SELECT episode_id, message_id FROM episode_sources "
                    f"WHERE episode_id IN ({placeholders})",
                    all_ref,
                ).fetchall():
                    source_map.setdefault(str(eid), set()).add(str(mid))

            resolved_records: list[tuple[str | None, str, bytes, set[str]]] = []
            for existing_id, summary, blob, src_ids in staged:
                merged_msgs: set[str] = set()
                for sid in src_ids:
                    merged_msgs.update(source_map.get(sid, ()))
                resolved_records.append((existing_id, summary, blob, merged_msgs))

            delete_targets = [eid for eid in ids_to_remove if eid not in preserved_ids]
            if delete_targets:
                placeholders = ",".join("?" for _ in delete_targets)
                db.execute(
                    f"DELETE FROM episodes WHERE account_id=? AND conversation_id=? "
                    f"AND id IN ({placeholders})",
                    (*scope, *delete_targets),
                )

            for existing_id, summary, blob, merged_msgs in resolved_records:
                if existing_id is not None:
                    db.execute(
                        "UPDATE episodes SET updated_at=? WHERE id=? AND account_id=? "
                        "AND conversation_id=?",
                        (stamp, existing_id, *scope),
                    )
                    target_id = existing_id
                else:
                    dupe = db.execute(
                        "SELECT id FROM episodes WHERE account_id=? AND conversation_id=? "
                        "AND summary=?",
                        (*scope, summary),
                    ).fetchone()
                    if dupe is not None:
                        target_id = str(dupe[0])
                        db.execute(
                            "UPDATE episodes SET updated_at=?, model=?, vector=? WHERE id=?",
                            (stamp, EMBEDDING_MODEL, blob, target_id),
                        )
                    else:
                        target_id = uuid4().hex
                        db.execute(
                            "INSERT INTO episodes VALUES (?,?,?,?,?,?,?)",
                            (target_id, *scope, summary, stamp, EMBEDDING_MODEL, blob),
                        )
                if merged_msgs:
                    db.executemany(
                        "INSERT OR IGNORE INTO episode_sources VALUES (?,?)",
                        [(target_id, mid) for mid in sorted(merged_msgs)],
                    )
            return int(
                db.execute(
                    "SELECT COUNT(*) FROM episodes WHERE account_id=? AND conversation_id=? "
                    "AND model=?",
                    (*scope, EMBEDDING_MODEL),
                ).fetchone()[0]
            )

    def save_curated(
        self,
        scope: tuple[str, str],
        batch_json: str,
        source_ids: list[str],
        entries: list[tuple[str | None, str, bytes]],
    ) -> None:
        stamp = datetime.now(UTC).isoformat()
        with sqlite3.connect(self.path) as db:
            db.execute("PRAGMA foreign_keys=ON")
            if not db.execute(
                "INSERT OR IGNORE INTO processed_batches VALUES (?,?,?)",
                (*scope, batch_json),
            ).rowcount:
                return
            for update_id, summary, vector in entries:
                if update_id:
                    saved = db.execute(
                        "UPDATE OR IGNORE episodes SET summary=?,updated_at=?,model=?,vector=? "
                        "WHERE id=? AND account_id=? AND conversation_id=?",
                        (summary, stamp, EMBEDDING_MODEL, vector, update_id, *scope),
                    )
                    eid = update_id
                else:
                    eid = uuid4().hex
                    saved = db.execute(
                        "INSERT OR IGNORE INTO episodes VALUES (?,?,?,?,?,?,?)",
                        (eid, *scope, summary, stamp, EMBEDDING_MODEL, vector),
                    )
                if saved.rowcount:
                    db.executemany(
                        "INSERT OR IGNORE INTO episode_sources VALUES (?,?)",
                        [(eid, mid) for mid in source_ids],
                    )
