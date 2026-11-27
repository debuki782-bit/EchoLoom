"""Persistent, immutable per-account short references (M1, U1, G1, E1, ...)."""

from __future__ import annotations

import re
import sqlite3
import threading
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime

from ..contacts import is_group_conversation

SHORT_REF_PREFIXES: dict[str, str] = {
    "message": "M",
    "user": "U",
    "chat": "G",
    "event": "E",
    "note": "N",
    "entity": "D",
    "memory": "P",
    "image": "I",
    "file": "F",
    "audio": "A",
}

SHORT_REF_PATTERN = re.compile(r"^[MUGENDPIFA][1-9][0-9]*$", re.IGNORECASE)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _bounded_id(value: str, name: str) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    cleaned = value.strip()
    if not cleaned or len(cleaned) > 256 or "\0" in cleaned:
        raise ValueError(f"{name} is empty or exceeds its limit")
    return cleaned


def identity_ref_kind(identifier: str) -> str:
    """Return 'chat' for group conversations and 'user' for individual contacts/DMs."""
    return "chat" if is_group_conversation(identifier) else "user"


def _allocate_unlocked(
    db: sqlite3.Connection,
    *,
    account_id: str,
    kind: str,
    target_id: str,
    now: str,
) -> str:
    existing = db.execute(
        "SELECT short_ref FROM short_refs WHERE account_id=? AND kind=? AND target_id=?",
        (account_id, kind, target_id),
    ).fetchone()
    if existing is not None:
        return str(existing[0])
    prefix = SHORT_REF_PREFIXES[kind]
    counter = db.execute(
        "SELECT next_seq FROM short_ref_counters WHERE account_id=? AND kind=?",
        (account_id, kind),
    ).fetchone()
    if counter is not None:
        seq = int(counter[0])
    else:
        max_existing = db.execute(
            "SELECT MAX(CAST(SUBSTR(short_ref, 2) AS INTEGER)) "
            "FROM short_refs WHERE account_id=? AND kind=?",
            (account_id, kind),
        ).fetchone()[0]
        seq = max(1, int(max_existing or 0) + 1)
    short_ref = f"{prefix}{seq}"
    db.execute(
        "INSERT INTO short_refs(account_id,kind,short_ref,target_id,created_at) "
        "VALUES (?,?,?,?,?)",
        (account_id, kind, short_ref, target_id, now),
    )
    db.execute(
        "INSERT INTO short_ref_counters(account_id,kind,next_seq) VALUES (?,?,?) "
        "ON CONFLICT(account_id,kind) DO UPDATE SET next_seq=excluded.next_seq",
        (account_id, kind, seq + 1),
    )
    return short_ref


def backfill_event_book_short_refs(
    db: sqlite3.Connection,
    transaction_fn: Callable[[], AbstractContextManager[None]],
) -> int:
    """Idempotently assign short refs to any existing unmapped event_book rows."""
    has_table = (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_book'"
        ).fetchone()
        is not None
    )
    if not has_table:
        return 0
    rows = db.execute(
        "SELECT e.account_id, e.event_id FROM event_book e "
        "WHERE e.account_id <> '' AND e.event_id <> '' "
        "AND NOT EXISTS ("
        "SELECT 1 FROM short_refs r "
        "WHERE r.account_id=e.account_id AND r.kind='event' AND r.target_id=e.event_id"
        ") ORDER BY e.account_id ASC, e.created_at ASC, e.event_id ASC"
    ).fetchall()
    if not rows:
        return 0
    now = _now()
    assigned = 0
    with transaction_fn():
        for row in rows:
            _allocate_unlocked(
                db,
                account_id=str(row[0]),
                kind="event",
                target_id=str(row[1]),
                now=now,
            )
            assigned += 1
    return assigned


def backfill_short_refs(
    db: sqlite3.Connection,
    transaction_fn: Callable[[], AbstractContextManager[None]],
) -> int:
    """Idempotently backfill immutable short refs for all existing unmapped records."""
    pending: list[tuple[str, str, str]] = []
    table_specs = (
        ("message", "source_messages", "message_id", "captured_at"),
        ("note", "annotations", "note_id", "created_at"),
        ("entity", "entity_terms", "entity_id", "created_at"),
        ("memory", "sender_memories", "memory_id", "created_at"),
        ("image", "message_media", "image_id", "created_at"),
        ("file", "message_files", "file_id", "created_at"),
        ("audio", "message_audio", "audio_id", "created_at"),
    )
    for kind, table, id_col, order_col in table_specs:
        rows = db.execute(
            f"SELECT t.account_id, t.{id_col} FROM {table} t "
            f"WHERE t.account_id <> '' AND t.{id_col} <> '' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM short_refs r "
            f"WHERE r.account_id=t.account_id AND r.kind=? AND r.target_id=t.{id_col}"
            f") ORDER BY t.account_id ASC, t.{order_col} ASC, t.{id_col} ASC",
            (kind,),
        ).fetchall()
        for row in rows:
            pending.append((str(row[0]), kind, str(row[1])))

    identity_seen: set[tuple[str, str, str]] = set()

    def _add_identity(account_id: str, ident: str) -> None:
        cleaned_acct = account_id.strip()
        cleaned_ident = ident.strip()
        if not cleaned_acct or not cleaned_ident:
            return
        kind = identity_ref_kind(cleaned_ident)
        key = (cleaned_acct, kind, cleaned_ident)
        if key not in identity_seen:
            identity_seen.add(key)
            pending.append(key)

    identity_queries = (
        (
            "SELECT sub.account_id, sub.ident FROM ("
            "SELECT account_id, sender_id AS ident, MIN(captured_at) AS first_at "
            "FROM source_messages WHERE account_id <> '' AND sender_id <> '' "
            "GROUP BY account_id, sender_id "
            "UNION ALL "
            "SELECT account_id, conversation_id AS ident, MIN(captured_at) AS first_at "
            "FROM source_messages WHERE account_id <> '' AND conversation_id <> '' "
            "GROUP BY account_id, conversation_id"
            ") sub "
            "WHERE NOT EXISTS ("
            "SELECT 1 FROM short_refs r "
            "WHERE r.account_id=sub.account_id AND r.kind IN ('user','chat') "
            "AND r.target_id=sub.ident"
            ") "
            "GROUP BY sub.account_id, sub.ident "
            "ORDER BY sub.account_id ASC, MIN(sub.first_at) ASC, sub.ident ASC"
        ),
        (
            "SELECT p.account_id, p.sender_id FROM sender_profiles p "
            "WHERE p.account_id <> '' AND p.sender_id <> '' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM short_refs r "
            "WHERE r.account_id=p.account_id AND r.kind IN ('user','chat') "
            "AND r.target_id=p.sender_id"
            ") ORDER BY p.account_id ASC, p.updated_at ASC, p.sender_id ASC"
        ),
        (
            "SELECT c.account_id, c.contact_id FROM contact_directory c "
            "WHERE c.account_id <> '' AND c.contact_id <> '' "
            "AND NOT EXISTS ("
            "SELECT 1 FROM short_refs r "
            "WHERE r.account_id=c.account_id AND r.kind IN ('user','chat') "
            "AND r.target_id=c.contact_id"
            ") ORDER BY c.account_id ASC, c.updated_at ASC, c.contact_id ASC"
        ),
    )
    for query in identity_queries:
        for row in db.execute(query).fetchall():
            _add_identity(str(row[0]), str(row[1]))

    assigned = 0
    if pending:
        now = _now()
        with transaction_fn():
            for account_id, kind, target_id in pending:
                _allocate_unlocked(
                    db,
                    account_id=account_id,
                    kind=kind,
                    target_id=target_id,
                    now=now,
                )
                assigned += 1
    assigned += backfill_event_book_short_refs(db, transaction_fn)
    return assigned


class ShortRefRepo:
    """Repository owning immutable short_refs and monotonic short_ref_counters tables."""

    identity_ref_kind = staticmethod(identity_ref_kind)

    def __init__(
        self,
        db: sqlite3.Connection,
        lock: threading.RLock,
        transaction_fn: Callable[[], AbstractContextManager[None]],
    ) -> None:
        self.db = db
        self._lock = lock
        self._transaction = transaction_fn

    def ensure_short_ref(self, *, account_id: str, kind: str, target_id: str) -> str:
        """Return the immutable short ref for `(account_id, kind, target_id)`, creating if needed."""
        account_id = _bounded_id(account_id, "account_id")
        if kind not in SHORT_REF_PREFIXES:
            raise ValueError(f"unsupported short_ref kind: {kind!r}")
        target_id = _bounded_id(target_id, "target_id")
        now = _now()
        with self._transaction():
            return _allocate_unlocked(
                self.db,
                account_id=account_id,
                kind=kind,
                target_id=target_id,
                now=now,
            )

    def ensure_short_refs_batch(
        self, *, account_id: str, items: Sequence[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        """Assign or return immutable short refs for a batch of `(kind, target_id)` pairs."""
        account_id = _bounded_id(account_id, "account_id")
        unique_items: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for kind, raw_target in items:
            if kind not in SHORT_REF_PREFIXES:
                raise ValueError(f"unsupported short_ref kind: {kind!r}")
            if not isinstance(raw_target, str):
                raise TypeError("target_id must be a string")
            target_id = raw_target.strip()
            if not target_id:
                continue
            if len(target_id) > 256 or "\0" in target_id:
                raise ValueError("target_id exceeds its limit")
            key = (kind, target_id)
            if key not in seen:
                seen.add(key)
                unique_items.append(key)
        if not unique_items:
            return {}
        now = _now()
        result: dict[tuple[str, str], str] = {}
        with self._transaction():
            for kind, target_id in unique_items:
                result[(kind, target_id)] = _allocate_unlocked(
                    self.db,
                    account_id=account_id,
                    kind=kind,
                    target_id=target_id,
                    now=now,
                )
        return result

    def short_refs_map(
        self, *, account_id: str, items: Sequence[tuple[str, str]]
    ) -> dict[tuple[str, str], str]:
        """Strictly read-only lookup of existing short refs (safe under PRAGMA query_only=ON)."""
        if not items:
            return {}
        by_kind: dict[str, list[str]] = {}
        for kind, raw_target in items:
            if kind not in SHORT_REF_PREFIXES or not isinstance(raw_target, str):
                continue
            target_id = raw_target.strip()
            if not target_id:
                continue
            bucket = by_kind.setdefault(kind, [])
            if target_id not in bucket:
                bucket.append(target_id)
        if not by_kind:
            return {}
        result: dict[tuple[str, str], str] = {}
        with self._lock:
            for kind, target_ids in by_kind.items():
                for start in range(0, len(target_ids), 500):
                    chunk = target_ids[start : start + 500]
                    placeholders = ",".join("?" for _ in chunk)
                    rows = self.db.execute(
                        "SELECT target_id,short_ref FROM short_refs "
                        f"WHERE account_id=? AND kind=? AND target_id IN ({placeholders})",
                        (account_id, kind, *chunk),
                    ).fetchall()
                    for row in rows:
                        result[(kind, str(row[0]))] = str(row[1])
        return result

    def resolve_short_ref(
        self,
        *,
        account_id: str,
        value: str,
        kinds: Sequence[str] | None = None,
    ) -> str | None:
        """Resolve a short ref (case-insensitive) to target_id, or pass raw IDs through."""
        if not isinstance(value, str):
            return None
        cleaned = value.strip()
        if not SHORT_REF_PATTERN.match(cleaned):
            return cleaned
        upper_ref = cleaned.upper()
        with self._lock:
            row = self.db.execute(
                "SELECT kind,target_id FROM short_refs WHERE account_id=? AND short_ref=?",
                (account_id, upper_ref),
            ).fetchone()
        if row is None:
            return None
        if kinds is not None and str(row[0]) not in set(kinds):
            return None
        return str(row[1])

    def backfill_event_book(self) -> int:
        """Idempotently assign short refs to any existing unmapped event_book rows."""
        return backfill_event_book_short_refs(self.db, self._transaction)
