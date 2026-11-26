"""Repository owning annotations, facts, entities, sender profiles/tags, and audit tables."""

from __future__ import annotations

import re
import sqlite3
import threading
import unicodedata
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from uuid import uuid4

MAX_NOTE_CHARS = 4000
MAX_FACT_CHARS = 2000
MAX_ENTITY_TERM_CHARS = 80
MAX_ENTITY_NOTE_CHARS = 500
MAX_SENDER_MEMORY_CHARS = 500
MAX_QUERY_CHARS = 300


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _bounded_text(value: str, limit: int) -> str:
    value = value.strip()
    if not value or len(value) > limit or "\0" in value:
        raise ValueError("text is empty or exceeds its limit")
    return value


def _like_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _query_tokens(query: str, *, max_terms: int = 6) -> list[str]:
    """Split a bounded search string into unique non-empty keywords."""
    return list(dict.fromkeys(part for part in query.split() if part))[:max_terms]


class KnowledgeRepo:
    """Versioned annotations, facts, entity dictionary, sender profiles/tags, and audit log."""

    def __init__(
        self,
        db: sqlite3.Connection,
        lock: threading.RLock,
        transaction_fn: Callable[[], AbstractContextManager[None]],
    ) -> None:
        self.db = db
        self._lock = lock
        self._transaction = transaction_fn

    def _message_in_scope(self, message_id: str, account_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages "
                "WHERE message_id=? AND account_id=? AND deleted_at IS NULL",
                (message_id, account_id),
            ).fetchone()
            is not None
        )

    def _message_exists_for_sender(self, account_id: str, sender_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages WHERE account_id=? AND sender_id=? "
                "AND deleted_at IS NULL LIMIT 1",
                (account_id, sender_id),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _tag_width(tag: str) -> int:
        return sum(2 if unicodedata.east_asian_width(char) in ("F", "W") else 1 for char in tag)

    def sender_tags_at(self, *, account_id: str, sender_id: str, at: str) -> list[str]:
        """Reconstruct a sender's tag snapshot at a message timestamp from append-only diffs."""
        with self._lock:
            rows = self.db.execute(
                "SELECT action,tag FROM sender_tag_diffs WHERE account_id=? AND sender_id=? "
                "AND at<=? ORDER BY at,diff_id",
                (account_id, sender_id, at),
            ).fetchall()
        tags: list[str] = []
        for row in rows:
            action, tag = str(row["action"]), str(row["tag"])
            if action in ("add", "replace_add") and tag not in tags:
                tags.append(tag)
            elif action in ("remove", "replace_remove") and tag in tags:
                tags.remove(tag)
        return tags[-5:]

    def sender_tags_many_at(
        self, *, account_id: str, requests: Sequence[tuple[str, str, str]]
    ) -> dict[str, list[str]]:
        """Reconstruct many timestamped tag snapshots with one indexed diff query."""
        requested: dict[str, list[tuple[str, str]]] = {}
        for message_id, sender_id, at in requests:
            if message_id and sender_id:
                requested.setdefault(sender_id, []).append((at, message_id))
        if not requested:
            return {}
        sender_ids = list(requested)
        placeholders = ",".join("?" for _ in sender_ids)
        max_at = max(at for snapshots in requested.values() for at, _ in snapshots)
        with self._lock:
            rows = self.db.execute(
                "SELECT sender_id,action,tag,at FROM sender_tag_diffs "
                "WHERE account_id=? AND sender_id IN (" + placeholders + ") AND at<=? "
                "ORDER BY sender_id,at,diff_id",
                (account_id, *sender_ids, max_at),
            ).fetchall()
        changes: dict[str, list[sqlite3.Row]] = {}
        for row in rows:
            changes.setdefault(str(row["sender_id"]), []).append(row)
        result: dict[str, list[str]] = {}
        for sender_id, snapshots in requested.items():
            history = changes.get(sender_id, [])
            history_index = 0
            tags: list[str] = []
            for at, message_id in sorted(snapshots):
                while history_index < len(history) and str(history[history_index]["at"]) <= at:
                    row = history[history_index]
                    history_index += 1
                    action, tag = str(row["action"]), str(row["tag"])
                    if action in ("add", "replace_add") and tag not in tags:
                        tags.append(tag)
                    elif action in ("remove", "replace_remove") and tag in tags:
                        tags.remove(tag)
                result[message_id] = tags[-5:]
        return result

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
        if action not in ("add", "remove", "replace"):
            return {"status": "rejected", "reason": "action_must_be_add_remove_or_replace"}
        if not self._message_exists_for_sender(account_id, sender_id):
            return {"status": "unavailable", "reason": "sender_not_in_archive"}
        if not isinstance(tags, (list, tuple)) or not 1 <= len(tags) <= 5:
            return {"status": "rejected", "reason": "tags_must_contain_1_to_5_items"}
        normalized: list[str] = []
        for value in tags:
            if not isinstance(value, str):
                return {"status": "rejected", "reason": "tag_must_be_text"}
            tag = " ".join(value.split())
            if not tag or self._tag_width(tag) > 20 or "\0" in tag:
                return {"status": "rejected", "reason": "tag_width_must_be_1_to_20"}
            if tag not in normalized:
                normalized.append(tag)
        current = set(
            self.sender_tags_at(
                account_id=account_id, sender_id=sender_id, at="9999-12-31T23:59:59+00:00"
            )
        )
        next_tags = (
            set(normalized)
            if action == "replace"
            else (current | set(normalized) if action == "add" else current - set(normalized))
        )
        if len(next_tags) > 5:
            return {"status": "rejected", "reason": "sender_tag_limit_is_5"}
        now = _now()
        changes: list[tuple[str, str]] = []
        if action == "replace":
            changes.extend(("replace_remove", tag) for tag in sorted(current - next_tags))
            changes.extend(("replace_add", tag) for tag in sorted(next_tags - current))
        elif action == "add":
            changes.extend(("add", tag) for tag in sorted(next_tags - current))
        else:
            changes.extend(("remove", tag) for tag in sorted(current - next_tags))
        with self._transaction():
            for diff_action, tag in changes:
                self.db.execute(
                    "INSERT INTO sender_tag_diffs VALUES (?,?,?,?,?,?,?,?)",
                    (
                        uuid4().hex,
                        account_id,
                        sender_id,
                        diff_action,
                        tag,
                        source_message_id,
                        source_task_id,
                        now,
                    ),
                )
            self.db.execute(
                "DELETE FROM sender_tags WHERE account_id=? AND sender_id=?",
                (account_id, sender_id),
            )
            self.db.executemany(
                "INSERT INTO sender_tags VALUES (?,?,?,?,?)",
                ((account_id, sender_id, tag, now, now) for tag in sorted(next_tags)),
            )
        return {"status": "updated", "sender_id": sender_id, "tags": sorted(next_tags)}

    def note_belongs_to(self, *, note_id: str, account_id: str, conversation_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM annotations a JOIN source_messages s "
                "ON s.message_id=a.message_id WHERE a.note_id=? AND a.account_id=? "
                "AND s.account_id=? AND s.conversation_id=? AND s.deleted_at IS NULL",
                (note_id, account_id, account_id, conversation_id),
            ).fetchone()
            is not None
        )

    def note_conversation(self, *, note_id: str, account_id: str) -> str | None:
        row = self.db.execute(
            "SELECT s.conversation_id FROM annotations a JOIN source_messages s "
            "ON s.message_id=a.message_id WHERE a.note_id=? AND a.account_id=? "
            "AND s.account_id=? AND s.deleted_at IS NULL",
            (note_id, account_id, account_id),
        ).fetchone()
        return str(row[0]) if row is not None else None

    def add_note(
        self,
        *,
        account_id: str,
        message_id: str,
        content: str,
        author_id: str = "agent",
        source: str = "agent_observation",
    ) -> str:
        content = _bounded_text(content, MAX_NOTE_CHARS)
        if source not in ("agent_observation", "owner_instruction"):
            raise ValueError("unsupported annotation source")
        if not self._message_in_scope(message_id, account_id):
            raise KeyError("source message unavailable")
        note_id, now = uuid4().hex, _now()
        with self._transaction():
            self.db.execute(
                "INSERT INTO annotations VALUES (?,?,?,?,?,?,?,?,1,'active',?,?)",
                (
                    note_id,
                    account_id,
                    message_id,
                    "agent",
                    author_id,
                    "untrusted_annotation",
                    content,
                    source,
                    now,
                    now,
                ),
            )
            self._note_revision(note_id, 1, "create", author_id, content, source, now)
            self._audit(
                account_id, "agent", author_id, "note.create", "annotation", note_id, 1, now
            )
        return note_id

    def ensure_note(self, *, account_id: str, message_id: str, content: str) -> None:
        """Attach a note to an archived message if identical active content does not exist."""
        already_saved = any(
            row["content"] == content
            for row in self.notes_for_message(account_id=account_id, message_id=message_id)
        )
        if not already_saved:
            self.add_note(account_id=account_id, message_id=message_id, content=content)

    def revise_note(
        self, *, account_id: str, note_id: str, content: str | None, actor_id: str = "agent"
    ) -> int:
        if content is not None:
            content = _bounded_text(content, MAX_NOTE_CHARS)
        with self._transaction():
            row = self.db.execute(
                "SELECT * FROM annotations WHERE note_id=? AND account_id=?",
                (note_id, account_id),
            ).fetchone()
            if row is None or row["author_kind"] != "agent" or row["state"] != "active":
                raise KeyError("editable agent annotation unavailable")
            version, now = row["version"] + 1, _now()
            state = "active" if content is not None else "deleted"
            new_content = content if content is not None else row["content"]
            self.db.execute(
                "UPDATE annotations SET content=?,version=?,state=?,updated_at=? WHERE note_id=?",
                (new_content, version, state, now, note_id),
            )
            self._note_revision(
                note_id,
                version,
                "update" if content else "delete",
                actor_id,
                new_content,
                row["source"],
                now,
            )
            self._audit(
                account_id,
                "agent",
                actor_id,
                "note.update" if content else "note.delete",
                "annotation",
                note_id,
                version,
                now,
            )
        return version

    def notes_for_message(self, *, account_id: str, message_id: str) -> list[dict[str, str]]:
        rows = self.db.execute(
            "SELECT note_id,content,author_id,source,trust_label,created_at,version "
            "FROM annotations WHERE account_id=? AND message_id=? AND state='active' "
            "ORDER BY created_at,note_id",
            (account_id, message_id),
        )
        return [dict(row) for row in rows]

    def notes_for_messages(
        self, *, account_id: str, message_ids: Sequence[str]
    ) -> dict[str, list[dict[str, object]]]:
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        with self._lock:
            rows = self.db.execute(
                "SELECT message_id,note_id,content,author_id,source,trust_label,"
                "created_at,version "
                f"FROM annotations WHERE account_id=? AND state='active' "
                f"AND message_id IN ({placeholders}) ORDER BY created_at,note_id",
                (account_id, *message_ids),
            ).fetchall()
        notes_by_message: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            notes_by_message.setdefault(str(row["message_id"]), []).append(dict(row))
        return notes_by_message

    def _search_tokenized(
        self,
        *,
        select_columns: str,
        from_where_base: str,
        base_params: Sequence[object],
        haystack_expr: str,
        order_expr: str,
        query: str,
        limit: int,
    ) -> list[dict]:
        """Run multi-keyword AND query first, then rank multi-word OR matches on zero AND hits."""
        tokens = _query_tokens(query)
        if not tokens:
            return []
        and_sql = " AND ".join(f"{haystack_expr} LIKE ? ESCAPE '\\'" for _ in tokens)
        and_params = [f"%{_like_escape(tok)}%" for tok in tokens]
        rows = self.db.execute(
            f"SELECT {select_columns} {from_where_base} AND ({and_sql}) "
            f"ORDER BY {order_expr} LIMIT ?",
            (*base_params, *and_params, limit),
        ).fetchall()
        if rows or len(tokens) < 2:
            return [dict(row) for row in rows]
        broad = [tok for tok in tokens if len(tok) >= 2] or tokens
        score_expr = " + ".join(
            f"(CASE WHEN {haystack_expr} LIKE ? ESCAPE '\\' THEN 1 ELSE 0 END)"
            for _ in broad
        )
        or_sql = " OR ".join(f"{haystack_expr} LIKE ? ESCAPE '\\'" for _ in broad)
        patterns = [f"%{_like_escape(tok)}%" for tok in broad]
        rows = self.db.execute(
            f"SELECT {select_columns} {from_where_base} AND ({or_sql}) "
            f"ORDER BY ({score_expr}) DESC, {order_expr} LIMIT ?",
            (*base_params, *patterns, *patterns, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def search_notes(
        self, *, account_id: str, conversation_id: str | None, query: str, limit: int = 8
    ) -> list[dict[str, str]]:
        query = _bounded_text(query, MAX_QUERY_CHARS).casefold()
        if not 1 <= limit <= 20:
            raise ValueError("limit out of range")
        where_chat = "AND s.conversation_id=? " if conversation_id is not None else ""
        base_params: list[object] = [account_id, account_id]
        if conversation_id is not None:
            base_params.append(conversation_id)
        base_params.append(_now())
        return self._search_tokenized(
            select_columns=(
                "a.note_id,a.content,a.message_id,s.conversation_id,"
                "a.author_id,a.source,a.trust_label,a.version,s.captured_at"
            ),
            from_where_base=(
                "FROM annotations a JOIN source_messages s ON s.message_id=a.message_id "
                "WHERE a.account_id=? AND s.account_id=? "
                + where_chat
                + "AND s.deleted_at IS NULL AND s.expires_at>? AND a.state='active'"
            ),
            base_params=base_params,
            haystack_expr="(coalesce(s.body_text,'') || ' ' || coalesce(a.content,''))",
            order_expr="a.updated_at DESC",
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
        fact_key = _bounded_text(fact_key, 100).casefold()
        content = _bounded_text(content, MAX_FACT_CHARS)
        if not self._message_in_scope(source_message_id, account_id):
            raise KeyError("source message unavailable")
        now, fact_id = _now(), uuid4().hex
        with self._transaction():
            prev = self.db.execute(
                "SELECT fact_id,version FROM user_facts WHERE account_id=? AND fact_key=? "
                "AND state='active' ORDER BY version DESC LIMIT 1",
                (account_id, fact_key),
            ).fetchone()
            latest = self.db.execute(
                "SELECT COALESCE(MAX(version),0) FROM user_facts WHERE account_id=? AND fact_key=?",
                (account_id, fact_key),
            ).fetchone()[0]
            version = latest + 1
            if prev:
                self.db.execute(
                    "UPDATE user_facts SET state='superseded',updated_at=? WHERE fact_id=?",
                    (now, prev["fact_id"]),
                )
            self.db.execute(
                "INSERT INTO user_facts VALUES (?,?,?,?,?,?,?,?,'active',?,?)",
                (
                    fact_id,
                    account_id,
                    fact_key,
                    content,
                    source_message_id,
                    actor_id,
                    "untrusted_memory",
                    version,
                    now,
                    now,
                ),
            )
            self._audit(
                account_id, "agent", actor_id, "fact.upsert", "user_fact", fact_id, version, now
            )
        return fact_id

    def search_facts(self, *, account_id: str, query: str, limit: int = 8) -> list[dict]:
        query = _bounded_text(query, MAX_QUERY_CHARS).casefold()
        if not 1 <= limit <= 20:
            raise ValueError("limit out of range")
        return self._search_tokenized(
            select_columns=(
                "fact_id,fact_key,content,source_message_id,author_id,trust_label,version"
            ),
            from_where_base="FROM user_facts WHERE account_id=? AND state='active'",
            base_params=(account_id,),
            haystack_expr="(coalesce(fact_key,'') || ' ' || coalesce(content,''))",
            order_expr="updated_at DESC",
            query=query,
            limit=limit,
        )

    def delete_fact(self, *, account_id: str, fact_id: str, actor_id: str = "agent") -> None:
        now = _now()
        with self._transaction():
            cur = self.db.execute(
                "UPDATE user_facts SET state='deleted',updated_at=?,version=version+1 "
                "WHERE fact_id=? AND account_id=? AND state='active'",
                (now, fact_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("active memory unavailable")
            row = self.db.execute(
                "SELECT version FROM user_facts WHERE fact_id=?", (fact_id,)
            ).fetchone()
            self._audit(
                account_id,
                "agent",
                actor_id,
                "fact.delete",
                "user_fact",
                fact_id,
                row["version"],
                now,
            )

    def fact_key_index(self, *, account_id: str, limit: int = 40) -> list[str]:
        """Return most-recently-updated active user_facts keys as a compact lookup index."""
        if not 1 <= limit <= 100:
            raise ValueError("limit out of range")
        with self._lock:
            rows = self.db.execute(
                "SELECT fact_key FROM user_facts WHERE account_id=? AND state='active' "
                "ORDER BY updated_at DESC, fact_id DESC LIMIT ?",
                (account_id, limit),
            ).fetchall()
        return list(dict.fromkeys(str(row["fact_key"]) for row in rows if row["fact_key"]))

    def memory_consolidation_candidates(
        self,
        *,
        account_id: str,
        sender_threshold: int = 8,
        fact_threshold: int = 20,
        limit: int = 5,
    ) -> dict[str, object]:
        """Identify fragmented sender_memories or oversized user_facts via deterministic SQL."""
        if sender_threshold < 2 or fact_threshold < 2 or not 1 <= limit <= 20:
            raise ValueError("consolidation thresholds out of range")
        with self._lock:
            sender_rows = self.db.execute(
                "SELECT sender_id, COUNT(*) AS cnt FROM sender_memories "
                "WHERE account_id=? AND state='active' "
                "GROUP BY sender_id HAVING COUNT(*) >= ? "
                "ORDER BY cnt DESC, sender_id LIMIT ?",
                (account_id, sender_threshold, limit),
            ).fetchall()
            fact_count = int(
                self.db.execute(
                    "SELECT COUNT(*) FROM user_facts WHERE account_id=? AND state='active'",
                    (account_id,),
                ).fetchone()[0]
            )
            excess_fact_rows = (
                self.db.execute(
                    "SELECT fact_key FROM user_facts WHERE account_id=? AND state='active' "
                    "ORDER BY updated_at ASC, fact_id ASC LIMIT ?",
                    (account_id, limit),
                ).fetchall()
                if fact_count >= fact_threshold
                else []
            )
        return {
            "bloated_senders": [
                {"sender_id": str(row["sender_id"]), "count": int(row["cnt"])}
                for row in sender_rows
            ],
            "active_fact_count": fact_count,
            "fact_threshold": fact_threshold,
            "excess_fact_keys": [
                str(row["fact_key"]) for row in excess_fact_rows if row["fact_key"]
            ],
        }

    def upsert_entity(
        self, *, account_id: str, term: str, explanation: str, actor_id: str = "agent"
    ) -> str:
        term = _bounded_text(term, MAX_ENTITY_TERM_CHARS)
        explanation = _bounded_text(explanation, MAX_ENTITY_NOTE_CHARS)
        if any(ord(char) < 32 for char in term):
            raise ValueError("entity term contains control characters")
        now = _now()
        with self._transaction():
            row = self.db.execute(
                "SELECT entity_id,version FROM entity_terms WHERE account_id=? AND term=? "
                "AND state='active' ORDER BY version DESC LIMIT 1",
                (account_id, term),
            ).fetchone()
            latest = self.db.execute(
                "SELECT COALESCE(MAX(version),0) FROM entity_terms WHERE account_id=? AND term=?",
                (account_id, term),
            ).fetchone()[0]
            version = latest + 1
            if row:
                self.db.execute(
                    "UPDATE entity_terms SET state='deleted',updated_at=? WHERE entity_id=?",
                    (now, row["entity_id"]),
                )
            entity_id = uuid4().hex
            self.db.execute(
                "INSERT INTO entity_terms VALUES (?,?,?,?,?,?,?,'active',?,?)",
                (
                    entity_id,
                    account_id,
                    term,
                    explanation,
                    actor_id,
                    "untrusted_dictionary",
                    version,
                    now,
                    now,
                ),
            )
            self._audit(
                account_id,
                "agent",
                actor_id,
                "entity.upsert",
                "entity_term",
                entity_id,
                version,
                now,
            )
        return entity_id

    def upsert_sender_memory(
        self, *, account_id: str, sender_id: str, topic: str, content: str
    ) -> tuple[str, bool]:
        """Add one compact sender note; identical active notes are idempotent."""
        if not self._message_exists_for_sender(account_id, sender_id):
            raise KeyError("sender is not present in this account's message archive")
        topic = _bounded_text(topic, 80)
        content = _bounded_text(content, MAX_SENDER_MEMORY_CHARS)
        now = _now()
        with self._transaction():
            existing = self.db.execute(
                "SELECT memory_id FROM sender_memories WHERE account_id=? AND sender_id=? "
                "AND topic=? AND content=? AND state='active' "
                "ORDER BY updated_at DESC LIMIT 1",
                (account_id, sender_id, topic, content),
            ).fetchone()
            if existing is not None:
                return str(existing["memory_id"]), False
            memory_id = "sender_" + uuid4().hex[:24]
            self.db.execute(
                "INSERT INTO sender_memories(memory_id,account_id,sender_id,topic,content,"
                "trust_label,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,'untrusted_reference_data','active',?,?)",
                (memory_id, account_id, sender_id, topic, content, now, now),
            )
        return memory_id, True

    def sender_memories(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, list[dict[str, object]]]:
        identifiers = list(dict.fromkeys(sender_ids))
        if not identifiers:
            return {}
        placeholders = ",".join("?" for _ in identifiers)
        rows = self.db.execute(
            "SELECT memory_id,sender_id,topic,content,trust_label,updated_at "
            "FROM sender_memories WHERE account_id=? AND state='active' "
            f"AND sender_id IN ({placeholders}) ORDER BY updated_at DESC,memory_id",
            (account_id, *identifiers),
        ).fetchall()
        result: dict[str, list[dict[str, object]]] = {}
        for row in rows:
            result.setdefault(str(row["sender_id"]), []).append(dict(row))
        return result

    def sender_remarks(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, str]:
        identifiers = list(dict.fromkeys(sender_ids))
        if not identifiers:
            return {}
        placeholders = ",".join("?" for _ in identifiers)
        with self._lock:
            rows = self.db.execute(
                "SELECT sender_id,remark FROM sender_profiles "
                "WHERE account_id=? AND state='active' AND sender_id IN ("
                f"{placeholders})",
                (account_id, *identifiers),
            ).fetchall()
        return {str(row["sender_id"]): str(row["remark"]) for row in rows}

    def set_human_sender_remark(
        self,
        *,
        account_id: str,
        sender_id: str,
        remark: str,
        actor: str = "dashboard-user",
    ) -> dict[str, object]:
        """Set or clear a human-owned display remark; Agent tools cannot call this."""
        account_id = _bounded_text(account_id, 256)
        sender_id = _bounded_text(sender_id, 256)
        if "\0" in sender_id or "\0" in account_id:
            raise ValueError("invalid sender identity")
        if not isinstance(remark, str) or len(remark) > 120 or "\0" in remark:
            raise ValueError("remark exceeds its limit")
        remark = remark.strip()
        with self._transaction():
            known = self.db.execute(
                "SELECT 1 FROM source_messages WHERE account_id=? AND sender_id=? "
                "AND deleted_at IS NULL LIMIT 1",
                (account_id, sender_id),
            ).fetchone()
            if known is None:
                known = self.db.execute(
                    "SELECT 1 FROM source_messages WHERE account_id=? AND conversation_id=? "
                    "AND deleted_at IS NULL LIMIT 1",
                    (account_id, sender_id),
                ).fetchone()
            if known is None:
                raise KeyError("sender is not present in this account's archive")
            current = self.db.execute(
                "SELECT remark,version FROM sender_profiles WHERE account_id=? AND sender_id=?",
                (account_id, sender_id),
            ).fetchone()
            version = int(current["version"]) + 1 if current else 1
            action = "delete" if not remark else "set"
            self.db.execute(
                "INSERT INTO sender_profiles("
                "account_id,sender_id,remark,version,state,updated_by,updated_at) "
                "VALUES (?,?,?,?,?,?,?) ON CONFLICT(account_id,sender_id) DO UPDATE SET "
                "remark=excluded.remark,version=excluded.version,state=excluded.state,"
                "updated_by=excluded.updated_by,updated_at=excluded.updated_at",
                (
                    account_id,
                    sender_id,
                    remark,
                    version,
                    "deleted" if not remark else "active",
                    actor,
                    _now(),
                ),
            )
            now = _now()
            self.db.execute(
                "INSERT INTO sender_profile_revisions VALUES (?,?,?,?,?,?,?,?)",
                (uuid4().hex, account_id, sender_id, version, remark, action, actor, now),
            )
            self._audit(
                account_id,
                "human",
                actor,
                f"sender_remark.{action}",
                "sender_profile",
                sender_id,
                version,
                now,
            )
        return {"status": "cleared" if not remark else "saved", "version": version}

    def priority_sender_profiles(
        self,
        *,
        account_id: str,
        tag: str = "唯一主人",
        current_conversation_id: str | None = None,
        exclude_message_ids: Sequence[str] = (),
    ) -> list[dict[str, object]]:
        """Load account-wide priority sender profiles plus optional cross-chat activity."""
        with self._lock:
            rows = self.db.execute(
                "SELECT sender_id,tag FROM sender_tags WHERE account_id=? AND tag=? "
                "ORDER BY created_at,sender_id",
                (account_id, tag),
            ).fetchall()
        sender_ids = list(dict.fromkeys(str(row["sender_id"]) for row in rows))
        memories = self.sender_memories(account_id=account_id, sender_ids=sender_ids)
        now_dt = datetime.now(UTC)
        now_iso = now_dt.isoformat()
        excluded = tuple(mid for mid in exclude_message_ids if isinstance(mid, str) and mid)
        profiles: list[dict[str, object]] = []
        for sender_id in sender_ids:
            entry: dict[str, object] = {
                "sender_id": sender_id,
                "tags": [tag],
                "memories": memories.get(sender_id, []),
            }
            if current_conversation_id:
                not_in = (
                    f" AND message_id NOT IN ({','.join('?' for _ in excluded)})"
                    if excluded
                    else ""
                )
                with self._lock:
                    prior_row = self.db.execute(
                        "SELECT captured_at FROM source_messages "
                        "WHERE account_id=? AND conversation_id=? AND sender_id=? "
                        "AND direction='inbound' AND deleted_at IS NULL "
                        "AND (expires_at='' OR expires_at>?)"
                        + not_in
                        + " ORDER BY captured_at DESC,message_id DESC LIMIT 1",
                        (account_id, current_conversation_id, sender_id, now_iso, *excluded),
                    ).fetchone()
                same_gap: int | None = None
                floor_dt = now_dt - timedelta(hours=12)
                if prior_row is not None and prior_row["captured_at"]:
                    prior_dt = datetime.fromisoformat(str(prior_row["captured_at"])).astimezone(UTC)
                    same_gap = max(0, int((now_dt - prior_dt).total_seconds()) // 60)
                    window_start = max(prior_dt, floor_dt).isoformat()
                else:
                    window_start = floor_dt.isoformat()
                with self._lock:
                    other_rows = self.db.execute(
                        "SELECT conversation_id, count(*) AS cnt, max(captured_at) AS last_at "
                        "FROM source_messages WHERE account_id=? AND sender_id=? "
                        "AND conversation_id<>? AND direction='inbound' AND deleted_at IS NULL "
                        "AND captured_at>=? AND (expires_at='' OR expires_at>?) "
                        "GROUP BY conversation_id ORDER BY last_at DESC LIMIT 3",
                        (account_id, sender_id, current_conversation_id, window_start, now_iso),
                    ).fetchall()
                other_activity = []
                for orow in other_rows:
                    last_dt = datetime.fromisoformat(str(orow["last_at"])).astimezone(UTC)
                    other_activity.append(
                        {
                            "conversation_id": str(orow["conversation_id"]),
                            "count": int(orow["cnt"]),
                            "minutes_ago": max(0, int((now_dt - last_dt).total_seconds()) // 60),
                        }
                    )
                entry["same_chat_gap_minutes"] = same_gap
                entry["other_chat_activity"] = other_activity
            profiles.append(entry)
        return profiles

    def delete_sender_memory(self, *, account_id: str, memory_id: str) -> bool:
        with self._transaction():
            cursor = self.db.execute(
                "UPDATE sender_memories SET state='deleted',updated_at=? "
                "WHERE account_id=? AND memory_id=? AND state='active'",
                (_now(), account_id, memory_id),
            )
        return cursor.rowcount == 1

    def delete_entity(self, *, account_id: str, entity_id: str, actor_id: str = "agent") -> None:
        now = _now()
        with self._transaction():
            cur = self.db.execute(
                "UPDATE entity_terms SET state='deleted',version=version+1,updated_at=? "
                "WHERE entity_id=? AND account_id=? AND state='active'",
                (now, entity_id, account_id),
            )
            if not cur.rowcount:
                raise KeyError("active entity unavailable")
            row = self.db.execute(
                "SELECT version FROM entity_terms WHERE entity_id=?", (entity_id,)
            ).fetchone()
            self._audit(
                account_id,
                "agent",
                actor_id,
                "entity.delete",
                "entity_term",
                entity_id,
                row["version"],
                now,
            )

    def match_entities(self, *, account_id: str, text: str, limit: int = 30) -> list[dict]:
        return self.match_entities_many(account_id=account_id, texts=(text,), limit=limit)[0]

    def match_entities_many(
        self, *, account_id: str, texts: Sequence[str], limit: int = 30
    ) -> list[list[dict]]:
        """Match one fresh dictionary snapshot against many texts in a single pass."""
        if len(texts) > 100 or any(len(text) > 12000 for text in texts) or not 1 <= limit <= 50:
            raise ValueError("entity match input out of range")
        unique_texts = list(dict.fromkeys(texts))
        with self._lock:
            rows = self.db.execute(
                "SELECT entity_id,term,explanation,author_id,trust_label,version "
                "FROM entity_terms WHERE account_id=? AND state='active' "
                "ORDER BY length(term) DESC,term",
                (account_id,),
            ).fetchall()
        patterns = [
            (
                dict(row),
                re.compile(
                    r"(?<![A-Za-z0-9_])" + re.escape(str(row["term"])) + r"(?![A-Za-z0-9_])",
                    re.IGNORECASE,
                ),
            )
            for row in rows
        ]
        by_text: dict[str, list[dict]] = {}
        for text in unique_texts:
            by_text[text] = [entity for entity, pattern in patterns if pattern.search(text)][:limit]
        return [by_text[text] for text in texts]

    def purge_for_message(self, *, message_id: str, now: str) -> None:
        """Soft-delete annotations and facts anchored to an expired source message."""
        self.db.execute(
            "UPDATE annotations SET state='deleted',updated_at=? "
            "WHERE message_id=? AND state='active'",
            (now, message_id),
        )
        self.db.execute(
            "UPDATE user_facts SET state='deleted',updated_at=?,version=version+1 "
            "WHERE source_message_id=? AND state='active'",
            (now, message_id),
        )

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
        self.db.execute(
            "INSERT INTO annotation_revisions VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                uuid4().hex,
                note_id,
                version,
                action,
                "agent",
                actor,
                "untrusted_annotation",
                content,
                source,
                now,
            ),
        )

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
        self.db.execute(
            "INSERT INTO memory_audit VALUES (?,?,?,?,?,?,?,?,?)",
            (uuid4().hex, scope, actor_kind, actor, action, object_type, object_id, version, now),
        )
