"""Conversation discovery, bounded history retrieval and internal session dispatch."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import sqlite3
import time
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

from .contacts import ContactIdentity, is_contact_id, is_group_conversation, is_room_id
from .context import (
    ContextTurn,
    ContextWindow,
    _safe_identity_name,
    render_message_body,
)
from .context_builder import ContextBuilder, ContextSpec
from .memory_store import MAX_QUERY_CHARS, MemoryStore
from .prompt_catalog import fill_prompt
from .tool_timing import turn_deadline_monotonic

logger = logging.getLogger(__name__)

_TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "source_messages": frozenset(
        {
            "message_id",
            "account_id",
            "conversation_id",
            "sender_id",
            "platform_message_id",
            "captured_at",
            "body_text",
            "expires_at",
            "deleted_at",
            "event_time_context_json",
            "direction",
            "delivery_state",
        }
    ),
    "annotations": frozenset(
        {
            "note_id",
            "account_id",
            "message_id",
            "author_kind",
            "author_id",
            "trust_label",
            "content",
            "source",
            "version",
            "state",
            "created_at",
        }
    ),
    "entity_terms": frozenset(
        {
            "entity_id",
            "account_id",
            "term",
            "explanation",
            "author_id",
            "trust_label",
            "version",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "message_media": frozenset(
        {
            "image_id",
            "account_id",
            "conversation_id",
            "message_id",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "error_code",
            "vision_summary",
            "vision_model_id",
            "vision_inspected_at",
        }
    ),
    "message_audio": frozenset(
        {
            "audio_id",
            "account_id",
            "conversation_id",
            "message_id",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "duration_ms",
            "error_code",
            "transcript",
            "transcript_model_id",
            "transcript_state",
        }
    ),
    "message_files": frozenset(
        {
            "file_id",
            "account_id",
            "conversation_id",
            "message_id",
            "filename",
            "fileext",
            "expected_bytes",
            "expected_md5",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "error_code",
            "summary",
            "summary_model_id",
            "summarized_at",
            "created_at",
            "updated_at",
        }
    ),
    "user_facts": frozenset(
        {
            "fact_id",
            "account_id",
            "fact_key",
            "content",
            "source_message_id",
            "author_id",
            "trust_label",
            "version",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "sender_memories": frozenset(
        {
            "memory_id",
            "account_id",
            "sender_id",
            "topic",
            "content",
            "trust_label",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "event_book": frozenset(
        {
            "event_id",
            "account_id",
            "priority",
            "content",
            "exit_condition",
            "expires_at",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "cross_session_tasks": frozenset(
        {
            "task_id",
            "account_id",
            "source_conversation_id",
            "target_conversation_id",
            "task_text",
            "state",
            "result_text",
            "created_at",
            "finished_at",
        }
    ),
}
_ALLOWED_SQL_ACTIONS = frozenset(
    filter(
        None,
        (
            getattr(sqlite3, "SQLITE_SELECT", None),
            getattr(sqlite3, "SQLITE_READ", None),
            getattr(sqlite3, "SQLITE_FUNCTION", None),
            getattr(sqlite3, "SQLITE_RECURSIVE", None),
        ),
    )
)


class CrossSessionService:
    SOFT_30_MIN = 5
    HARD_40_MIN = 8
    HARD_60_MIN = 10

    def __init__(
        self,
        store: MemoryStore,
        *,
        resolve_contact: Callable[[str], ContactIdentity | None] | None = None,
        dispatch_reply: Callable[[ContextWindow | ContextSpec, str], Awaitable[str]] | None = None,
        context_builder: ContextBuilder | None = None,
        row_limit: int = 100,
        byte_limit: int = 16_000,
    ):
        if not 1 <= row_limit <= 200 or not 512 <= byte_limit <= 128_000:
            raise ValueError("invalid query result limits")
        self.store = store
        self.resolve_contact = resolve_contact
        self.dispatch_reply = dispatch_reply
        self.context_builder = context_builder
        self.row_limit = row_limit
        self.byte_limit = byte_limit
        self.db = store.db
        self._lock = store._lock
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS cross_session_dispatches ("
            "dispatch_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,pair_key TEXT NOT NULL,"
            "source_conversation_id TEXT NOT NULL,target_conversation_id TEXT NOT NULL,"
            "dispatched_at TEXT NOT NULL);"
            "CREATE INDEX IF NOT EXISTS cross_session_pair_time "
            "ON cross_session_dispatches(account_id,pair_key,dispatched_at);"
            "CREATE TABLE IF NOT EXISTS cross_session_tasks ("
            "task_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,source_conversation_id TEXT NOT NULL,"
            "target_conversation_id TEXT NOT NULL,task_text TEXT NOT NULL,state TEXT NOT NULL,"
            "result_text TEXT,created_at TEXT NOT NULL,finished_at TEXT);"
        )
        # A process exit can leave delegation waits mid-flight. They cannot be
        # resumed, so a restart ends them as interrupted instead of leaking rows.
        with self._lock, self.db:
            swept = self.db.execute(
                "UPDATE cross_session_tasks SET state='interrupted',result_text=?,"
                "finished_at=? WHERE state='running'",
                (
                    "服务重启中断了跨会话委托等待；结果未知，不自动恢复。",
                    datetime.now(UTC).isoformat(),
                ),
            )
        if swept.rowcount:
            logger.info(
                "event=cross_session_tasks_interrupted_on_restart count=%s",
                swept.rowcount,
            )

    def list_conversations(
        self, *, account_id: str, query: str = "", limit: int = 30
    ) -> list[dict[str, object]]:
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")
        query = self._query(query) if query else ""
        trusted_ids = self._trusted_name_ids(account_id)
        rows = self.store.list_conversations(account_id=account_id, limit=200)
        if query:
            needle = query.casefold()
            candidates = []
            for row in rows[:30]:
                conversation_id = str(row["conversation_id"])
                identity = self._resolve(conversation_id)
                label = _safe_identity_name(conversation_id, identity, trusted_ids)
                if any(
                    needle in str(value).casefold()
                    for value in (
                        conversation_id,
                        row["last_message_preview"],
                        row["last_sender_id"],
                        label,
                    )
                ):
                    candidates.append(row)
            # Search deeper in the message archive when the most recent preview is not enough.
            if len(candidates) < limit:
                matches = self.store.search_messages(
                    account_id=account_id,
                    query=query,
                    limit=min(50, limit * 2),
                )
                by_id = {str(row["conversation_id"]): row for row in rows}
                for message in matches:
                    conversation = str(message["conversation_id"])
                    if conversation in by_id and conversation not in {
                        str(row["conversation_id"]) for row in candidates
                    }:
                        candidates.append(by_id[conversation])
            rows = candidates
        results = []
        for row in rows[:limit]:
            entry = dict(row)
            conversation_id = str(row["conversation_id"])
            identity = self._resolve(conversation_id) if len(results) < 10 else None
            entry["display_name"] = _safe_identity_name(
                conversation_id, identity, trusted_ids
            )
            preview = entry.get("last_message_preview")
            if isinstance(preview, str):
                entry["last_message_preview"] = render_message_body(preview)
            results.append(entry)
        return results

    def query_messages(
        self,
        *,
        account_id: str,
        current_conversation_id: str | None = None,
        conversation_id: str | None = None,
        all_conversations: bool = False,
        message_id: str = "",
        query: str = "",
        sender_id: str = "",
        message_type: int = 0,
        start_at: str = "",
        end_at: str = "",
        before: int = 5,
        after: int = 5,
        limit: int = 0,
        byte_limit: int | None = None,
    ) -> dict[str, object]:
        """Query the archive by exact message ID, text, or time without waking an Agent."""
        budget = self.byte_limit if byte_limit is None else byte_limit
        if not 512 <= budget <= 128_000:
            raise ValueError("byte_limit must be between 512 and 128000")
        if not 0 <= before <= 24 or not 0 <= after <= 24 or before + after > 48:
            raise ValueError("before and after must be 0..24 and total at most 48")
        if message_type and not 1 <= message_type <= 255:
            raise ValueError("message_type must be between 1 and 255")
        if sender_id and (len(sender_id) > 160 or "\0" in sender_id):
            raise ValueError("sender_id must be at most 160 characters")

        scope = conversation_id or (None if all_conversations else current_conversation_id)
        if conversation_id and not self._known_conversation(account_id, conversation_id):
            return {"status": "unavailable", "reason": "conversation_not_in_archive", "messages": []}

        target_id = ""
        matched_count = 0
        rows: list[dict[str, object]]
        selected_scope = scope
        query_text = self._query(query) if query else ""
        now_text = datetime.now(UTC).isoformat()

        if message_id:
            if not isinstance(message_id, str) or len(message_id) > 160 or "\0" in message_id:
                raise ValueError("message_id must be 1..160 characters")
            params: list[object] = [account_id, now_text, message_id, message_id]
            sql = (
                "SELECT message_id,platform_message_id,account_id,conversation_id,sender_id,"
                "captured_at,raw_json,body_text,direction,delivery_state,event_time_context_json "
                "FROM source_messages WHERE account_id=? AND deleted_at IS NULL AND expires_at>? "
                "AND (message_id=? OR platform_message_id=?)"
            )
            if conversation_id:
                sql += " AND conversation_id=?"
                params.append(conversation_id)
            matches = self.db.execute(sql, params).fetchmany(2)
            if not matches:
                return {"status": "unavailable", "reason": "message_not_found", "messages": []}
            if len(matches) > 1:
                candidates = list(dict.fromkeys(str(row["conversation_id"]) for row in matches))
                return {
                    "status": "ambiguous",
                    "reason": "platform_message_id_matches_multiple_chats",
                    "candidate_conversation_ids": candidates,
                    "messages": [],
                }
            target = matches[0]
            target_id = str(target["message_id"])
            selected_scope = str(target["conversation_id"])
            target_time = str(target["captured_at"])
            with self._lock:
                prior = self.db.execute(
                    "SELECT message_id,platform_message_id,account_id,conversation_id,sender_id,"
                    "captured_at,raw_json,body_text,direction,delivery_state,event_time_context_json "
                    "FROM source_messages WHERE account_id=? AND conversation_id=? "
                    "AND deleted_at IS NULL AND expires_at>? AND "
                    "(captured_at<? OR (captured_at=? AND message_id<?)) "
                    "ORDER BY captured_at DESC,message_id DESC LIMIT ?",
                    (account_id, selected_scope, now_text, target_time, target_time, target_id, before),
                ).fetchall()
                following = self.db.execute(
                    "SELECT message_id,platform_message_id,account_id,conversation_id,sender_id,"
                    "captured_at,raw_json,body_text,direction,delivery_state,event_time_context_json "
                    "FROM source_messages WHERE account_id=? AND conversation_id=? "
                    "AND deleted_at IS NULL AND expires_at>? AND "
                    "(captured_at>? OR (captured_at=? AND message_id>?)) "
                    "ORDER BY captured_at,message_id LIMIT ?",
                    (account_id, selected_scope, now_text, target_time, target_time, target_id, after),
                ).fetchall()
            rows = [self._query_message_dict(row) for row in reversed(prior)]
            rows.extend(self._query_message_dict(row) for row in (target, *following))
            matched_count = len(rows)
        else:
            if not query_text and not start_at and not end_at:
                end = datetime.now(UTC)
                start = end - timedelta(hours=24)
            else:
                start = self._parse_query_time(start_at) if start_at else None
                end = self._parse_query_time(end_at, end_of_day=True) if end_at else None
            if start is not None and end is not None and start > end:
                raise ValueError("start_at must be earlier than end_at")
            if start is not None and not end_at:
                end = datetime.now(UTC)
            if end is not None and not start_at:
                start = end - timedelta(hours=24)
            default_limit = 20 if not query_text or start_at or end_at else 10
            limit = default_limit if limit == 0 else limit
            if not 1 <= limit <= 50:
                raise ValueError("limit must be between 1 and 50")

            def fetch(scope_id: str | None, row_limit: int):
                clauses = ["account_id=?", "deleted_at IS NULL", "expires_at>?", "captured_at<=?"]
                values: list[object] = [account_id, now_text, now_text]
                if scope_id:
                    clauses.append("conversation_id=?")
                    values.append(scope_id)
                if query_text:
                    clauses.append("body_text LIKE ?")
                    values.append(f"%{query_text}%")
                if sender_id:
                    clauses.append("sender_id=?")
                    values.append(sender_id)
                if message_type:
                    clauses.append(
                        "(raw_json LIKE ? OR raw_json LIKE ?)"
                    )
                    values.extend((f'%"msgtype":{message_type},%', f'%"msgtype":{message_type}}}%'))
                if start is not None:
                    clauses.append("captured_at>=?")
                    values.append(start.astimezone(UTC).isoformat())
                if end is not None:
                    clauses.append("captured_at<=?")
                    values.append(end.astimezone(UTC).isoformat())
                where = " AND ".join(clauses)
                count = int(self.db.execute(
                    f"SELECT count(*) FROM source_messages WHERE {where}", values,
                ).fetchone()[0])
                found = self.db.execute(
                    "SELECT message_id,platform_message_id,account_id,conversation_id,sender_id,"
                    "captured_at,raw_json,body_text,direction,delivery_state,event_time_context_json "
                    f"FROM source_messages WHERE {where} "
                    "ORDER BY captured_at DESC,message_id DESC LIMIT ?",
                    (*values, row_limit),
                ).fetchall()
                return count, [self._query_message_dict(row) for row in reversed(found)]

            matched_count, rows = fetch(scope, limit)
            if not rows and scope and scope == current_conversation_id and not conversation_id:
                # A local miss may be a cross-chat question; report the expanded scope explicitly.
                selected_scope = None
                matched_count, rows = fetch(None, limit)
            else:
                selected_scope = scope
            if not rows:
                return {
                    "status": "completed", "scope": "all_chats" if selected_scope is None else selected_scope,
                    "matched_count": 0, "returned_count": 0, "truncated": False,
                    "bytes_used": 0, "first_message_id": None, "last_message_id": None,
                    "messages": [],
                }

        enriched = self.store.enrich_context_messages(account_id=account_id, messages=rows)
        trusted_ids = self._trusted_name_ids(account_id)
        sender_ids = list(dict.fromkeys(str(row.get("sender_id", "")) for row in enriched))[:20]
        chat_ids = list(dict.fromkeys(str(row.get("conversation_id", "")) for row in enriched))[:20]
        sender_names = {identifier: self._resolve(identifier) for identifier in sender_ids}
        chat_names = {identifier: self._resolve(identifier) for identifier in chat_ids}
        for row in enriched:
            sender = str(row.get("sender_id", ""))
            chat = str(row.get("conversation_id", ""))
            row["display_name"] = _safe_identity_name(sender, sender_names.get(sender), trusted_ids)
            row["conversation_name"] = _safe_identity_name(chat, chat_names.get(chat), trusted_ids)

        bounded = []
        for source in enriched:
            row = dict(source)
            body = row.get("body_text")
            if isinstance(body, str):
                row["body_text"] = render_message_body(body)
            bounded.append(row)

        def encoded_size() -> int:
            return len(json.dumps(bounded, ensure_ascii=False, default=str).encode("utf-8"))

        was_truncated = matched_count > len(rows)
        while bounded and encoded_size() > budget:
            removable = next(
                (index for index, row in enumerate(bounded)
                 if str(row.get("message_id", "")) != target_id),
                None,
            )
            if removable is None:
                # Preserve the exact target and core identity while trimming verbose enrichments.
                row = bounded[0]
                for optional in ("entities", "sender_memories", "annotations", "image", "audio"):
                    if row.get(optional):
                        row[optional] = [] if isinstance(row[optional], list) else None
                        was_truncated = True
                        if encoded_size() <= budget:
                            break
                if encoded_size() > budget and isinstance(row.get("body_text"), str):
                    original = row["body_text"]
                    if original.startswith("*{") and original.endswith("}*"):
                        original = original[2:-2]
                    marker = "…（较早正文已截断）…"
                    low, high, best = 0, len(original), render_message_body(marker)
                    while low <= high:
                        keep = (low + high) // 2
                        row["body_text"] = render_message_body(
                            marker + (original[-keep:] if keep else "")
                        )
                        if encoded_size() <= budget:
                            best = row["body_text"]
                            low = keep + 1
                        else:
                            high = keep - 1
                    row["body_text"] = best
                    was_truncated = True
                if encoded_size() > budget:
                    keep = (
                        "message_id", "conversation_id", "sender_id", "captured_at",
                        "direction", "message_type", "display_name", "conversation_name",
                        "body_text",
                    )
                    bounded[0] = {key: row[key] for key in keep if key in row}
                break
            bounded.pop(removable)
            was_truncated = True
        bytes_used = encoded_size()
        message_ids = [str(row.get("message_id", "")) for row in bounded]
        return {
            "status": "completed",
            "scope": "all_chats" if selected_scope is None else selected_scope,
            "matched_count": matched_count,
            "returned_count": len(bounded),
            "truncated": was_truncated,
            "bytes_used": bytes_used,
            "first_message_id": message_ids[0] if message_ids else None,
            "last_message_id": message_ids[-1] if message_ids else None,
            "messages": bounded,
        }

    @staticmethod
    def _parse_query_time(value: str, *, end_of_day: bool = False) -> datetime:
        if not isinstance(value, str) or not value.strip() or len(value) > 40:
            raise ValueError("time must be an ISO date or date-time")
        try:
            parsed = datetime.fromisoformat(value.strip())
        except ValueError as exc:
            raise ValueError("time must be an ISO date or date-time") from exc
        if len(value.strip()) == 10 and end_of_day:
            parsed += timedelta(days=1, microseconds=-1)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
        return parsed.astimezone(UTC)

    @staticmethod
    def _query_message_dict(row: sqlite3.Row) -> dict[str, object]:
        message = MemoryStore._message_dict(row)
        try:
            raw = json.loads(row["raw_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            raw = {}
        if isinstance(raw, dict):
            message_type = raw.get("msgtype", raw.get("type"))
            if isinstance(message_type, (str, int)) and str(message_type).isdigit():
                message["message_type"] = int(message_type)
        return message

    def search_messages(
        self, *, account_id: str, query: str, conversation_id: str | None = None, limit: int = 20
    ) -> list[dict[str, object]]:
        query = self._query(query)
        if conversation_id is not None and not self._known_conversation(
            account_id, conversation_id
        ):
            return []
        rows = self.store.search_messages(
            account_id=account_id,
            query=query,
            conversation_id=conversation_id,
            limit=min(limit, 50),
        )
        enriched = self.store.enrich_context_messages(account_id=account_id, messages=rows)
        trusted_ids = self._trusted_name_ids(account_id)
        resolved_senders: dict[str, ContactIdentity | None] = {}
        for sender_id in dict.fromkeys(str(row.get("sender_id", "")) for row in enriched):
            if len(resolved_senders) >= 10:
                break
            resolved_senders[sender_id] = self._resolve(sender_id)
        resolved_chats: dict[str, ContactIdentity | None] = {}
        for conversation in dict.fromkeys(str(row.get("conversation_id", "")) for row in enriched):
            if len(resolved_chats) >= 10:
                break
            resolved_chats[conversation] = self._resolve(conversation)
        for row in enriched:
            sender_id = str(row.get("sender_id", ""))
            conversation_id = str(row.get("conversation_id", ""))
            sender = resolved_senders.get(sender_id)
            chat = resolved_chats.get(conversation_id)
            row["display_name"] = _safe_identity_name(sender_id, sender, trusted_ids)
            row["conversation_name"] = _safe_identity_name(
                conversation_id, chat, trusted_ids
            )
        return self._bound_rows(enriched)

    def read_conversation_context(
        self, *, account_id: str, conversation_id: str, query: str = "", limit: int = 30,
        _wrap_message_bodies: bool = True,
    ) -> list[dict[str, object]]:
        if not 1 <= limit <= 50 or not self._known_conversation(account_id, conversation_id):
            return []
        recent = self.store.recent_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=50,
        )
        selected = []
        if query:
            selected = self.store.search_messages(
                account_id=account_id,
                conversation_id=conversation_id,
                query=self._query(query),
                limit=min(20, limit),
            )
        selected_ids = {str(row["message_id"]) for row in selected}
        recent_candidates = [row for row in recent if str(row["message_id"]) not in selected_ids]
        remaining = max(0, limit - len(selected))
        supplemental = recent_candidates[-remaining:] if remaining else []
        by_id = {str(row["message_id"]): row for row in (*selected, *supplemental)}
        records = sorted(
            by_id.values(),
            key=lambda row: (
                str(row.get("captured_at", "")),
                str(row.get("message_id", "")),
            ),
        )
        enriched = self.store.enrich_context_messages(account_id=account_id, messages=records)
        trusted_ids = self._trusted_name_ids(account_id)
        identities = {conversation_id: self._resolve(conversation_id)}
        sender_ids = list(dict.fromkeys(str(row.get("sender_id", "")) for row in enriched))[-10:]
        sender_identities = {sender: self._resolve(sender) for sender in sender_ids}
        for row in enriched:
            sender = str(row.get("sender_id", ""))
            identity = sender_identities.get(sender)
            row["display_name"] = _safe_identity_name(sender, identity, trusted_ids)
            row["conversation_name"] = _safe_identity_name(
                conversation_id, identities[conversation_id], trusted_ids
            )
        return self._bound_rows(enriched, wrap_message_bodies=_wrap_message_bodies)

    async def dispatch(
        self,
        *,
        account_id: str,
        source_conversation_id: str,
        target_conversation_id: str,
        task: str,
        return_result: bool = True,
        operation_key: str = "",
    ) -> dict[str, object]:
        if self.dispatch_reply is None:
            return {"status": "unavailable", "reason": "session_dispatch_not_configured"}
        idle_source = source_conversation_id.startswith("idle-event:")
        if (
            not idle_source and not self._known_conversation(account_id, source_conversation_id)
        ) or not self._known_conversation(account_id, target_conversation_id):
            return {"status": "unavailable", "reason": "conversation_not_in_archive"}
        if source_conversation_id == target_conversation_id:
            return {"status": "rejected", "reason": "target_is_current_session"}
        try:
            task_text = self._validate_task(task)
        except (TypeError, ValueError) as exc:
            return {"status": "rejected", "reason": str(exc)}
        task_id = "xst_" + (
            hashlib.sha256(json.dumps(
                [account_id, source_conversation_id, target_conversation_id, operation_key, task_text],
                ensure_ascii=False,
            ).encode()).hexdigest()[:24] if operation_key else uuid4().hex[:24]
        )
        with self._lock:
            previous = self.db.execute(
                "SELECT state,result_text FROM cross_session_tasks WHERE task_id=?", (task_id,),
            ).fetchone()
        if previous is not None and previous["state"] in ("running", "completed"):
            replay: dict[str, object] = {
                "status": "in_progress" if previous["state"] == "running" else "completed",
                "task_id": task_id, "target_conversation_id": target_conversation_id,
                "result_available": bool(previous["result_text"]), "replayed": True,
            }
            if return_result and previous["result_text"]:
                replay["result"] = previous["result_text"]
            return replay
        now = datetime.now(UTC)
        admission = self._rate_limit(
            account_id=account_id,
            source="__account_idle__" if idle_source else source_conversation_id,
            target=target_conversation_id,
            now=now,
        )
        if admission is not None and admission.get("status") == "rate_limited":
            return admission
        soft_warning = admission

        target_is_group = is_group_conversation(target_conversation_id)
        current = ContextTurn(
            sender_id="Agent内部协调器",
            sender_origin="cross_session_task",
            text=fill_prompt("cross_session_task", task_text=task_text),
            message_id=task_id,
            source_conversation_id=target_conversation_id,
            source_is_group=target_is_group,
        )
        spec = ContextSpec.for_cross_session(
            account_id=account_id,
            conversation_id=target_conversation_id,
            is_group=target_is_group,
            synthetic_turns=(current,),
            query_hint=task_text,
            max_user_bytes=24_000,
            extra_identifiers=(source_conversation_id,),
        )
        if getattr(self.dispatch_reply, "_accepts_context_spec", False):
            dispatch_payload: ContextWindow | ContextSpec = spec
        else:
            builder = self.context_builder or ContextBuilder(
                self.store,
                resolve_contact=self._resolve if self.resolve_contact is not None else None,
            )
            dispatch_payload = (await builder.build(spec)).context
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO cross_session_tasks VALUES (?,?,?,?,?,'running',NULL,?,NULL) "
                "ON CONFLICT(task_id) DO UPDATE SET state='running',result_text=NULL,"
                "created_at=excluded.created_at,finished_at=NULL",
                (
                    task_id,
                    account_id,
                    source_conversation_id,
                    target_conversation_id,
                    task_text,
                    now.isoformat(),
                ),
            )
        wait_seconds = 600.0
        deadline = turn_deadline_monotonic.get()
        if deadline is not None:
            # Leave the caller time to consume a timeout result rather than
            # letting its outer watchdog interrupt target cleanup.
            wait_seconds = max(0.0, min(wait_seconds, deadline - time.monotonic() - 1.0))
        try:
            try:
                result = await asyncio.wait_for(
                    self.dispatch_reply(dispatch_payload, task_id), timeout=wait_seconds,
                )
            except TimeoutError:
                with self._lock, self.db:
                    self.db.execute(
                        "UPDATE cross_session_tasks SET state='timed_out',finished_at=? "
                        "WHERE task_id=? AND state='running'",
                        (datetime.now(UTC).isoformat(), task_id),
                    )
                return {
                    "status": "timed_out",
                    "task_id": task_id,
                    "target_conversation_id": target_conversation_id,
                    "wait_seconds": wait_seconds,
                    "reason": "parent_turn_deadline" if wait_seconds < 600 else "target_timeout",
                }
            if not isinstance(result, str):
                result = ""
            result = result[:4000]
            with self._lock, self.db:
                self.db.execute(
                    "UPDATE cross_session_tasks SET state='completed',result_text=?,finished_at=? "
                    "WHERE task_id=? AND state='running'",
                    (result, datetime.now(UTC).isoformat(), task_id),
                )
            response: dict[str, object] = {
                "status": "completed",
                "task_id": task_id,
                "target_conversation_id": target_conversation_id,
                "result_available": bool(result),
            }
            if return_result and result:
                response["result"] = result
            if soft_warning is not None:
                response["soft_warning"] = soft_warning
            return response
        except asyncio.CancelledError:
            with self._lock, self.db:
                self.db.execute(
                    "UPDATE cross_session_tasks SET state='interrupted',finished_at=? "
                    "WHERE task_id=? AND state='running'",
                    (datetime.now(UTC).isoformat(), task_id),
                )
            raise
        except Exception as exc:  # noqa: BLE001
            with self._lock, self.db:
                self.db.execute(
                    "UPDATE cross_session_tasks SET state='failed',finished_at=? "
                    "WHERE task_id=? AND state='running'",
                    (datetime.now(UTC).isoformat(), task_id),
                )
            logger.warning(
                "event=cross_session_task_failed task=%s error_type=%s",
                task_id[:12],
                type(exc).__name__,
            )
            return {"status": "failed", "task_id": task_id, "reason": type(exc).__name__}

    def query_sql(
        self, *, account_id: str, query: str, parameters: Sequence[object] = ()
    ) -> dict[str, object]:
        """Execute a bounded SELECT over approved columns in a read-only connection."""
        if not isinstance(query, str) or len(query) > 4000:
            return {"status": "rejected", "reason": "query_length_limit"}
        statement = query.strip().rstrip(";").strip()
        if (
            not statement.casefold().startswith("select ")
            or ";" in statement
            or "--" in statement
            or "/*" in statement
            or "*/" in statement
        ):
            return {"status": "rejected", "reason": "select_only_single_statement"}
        if (
            re.search(
                r"\b(OR|UNION|INTERSECT|EXCEPT|JOIN|ATTACH|PRAGMA)\b", statement, re.IGNORECASE
            )
            or len(re.findall(r"\bSELECT\b", statement, re.IGNORECASE)) != 1
        ):
            return {"status": "rejected", "reason": "single_table_scoped_select_only"}
        if not re.search(r"\baccount_id\s*=\s*\?", statement, re.IGNORECASE):
            return {"status": "rejected", "reason": "query_must_scope_current_account"}
        account_placeholder = re.search(r"\baccount_id\s*=\s*\?", statement, re.IGNORECASE)
        where_scope = re.search(
            r"\bWHERE\s+(?:\w+\.)?account_id\s*=\s*\?", statement, re.IGNORECASE
        )
        if (
            account_placeholder is None
            or where_scope is None
            or statement[: where_scope.end()].count("?") != 1
        ):
            return {"status": "rejected", "reason": "account_scope_must_use_first_parameter"}
        if len(parameters) > 50 or any(not self._sql_scalar(value) for value in parameters):
            return {"status": "rejected", "reason": "invalid_parameter_list"}
        conn = None
        try:
            sql = statement
            # The first parameter always binds the current account_id predicate.
            params = (account_id, *parameters)
            safe_path = self.store.path.as_posix()
            conn = sqlite3.connect(f"file:{safe_path}?mode=ro", uri=True, timeout=1)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")

            def authorize(action, arg1, arg2, _database, _source):
                if action not in _ALLOWED_SQL_ACTIONS:
                    return sqlite3.SQLITE_DENY
                if action == sqlite3.SQLITE_READ:
                    allowed = _TABLE_COLUMNS.get(str(arg1))
                    if allowed is None or (arg2 and arg2 not in allowed):
                        return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            conn.set_authorizer(authorize)
            opcodes = 0

            def progress() -> int:
                nonlocal opcodes
                opcodes += 100
                return int(opcodes > 50_000)

            conn.set_progress_handler(progress, 100)
            rows = conn.execute(sql, params).fetchmany(self.row_limit + 1)
            values = [dict(row) for row in rows[: self.row_limit]]
            for row in values:
                for name in ("body_text", "last_message_preview"):
                    body = row.get(name)
                    if isinstance(body, str):
                        row[name] = render_message_body(body)
            encoded = json.dumps(values, ensure_ascii=False, default=str)
            while values and len(encoded.encode("utf-8")) > self.byte_limit:
                values.pop()
                encoded = json.dumps(values, ensure_ascii=False, default=str)
            return {"status": "completed", "rows": values, "truncated": len(rows) > len(values)}
        except Exception as exc:  # noqa: BLE001
            return {"status": "rejected", "reason": type(exc).__name__}
        finally:
            if conn is not None:
                conn.close()

    def _rate_limit(
        self, *, account_id: str, source: str, target: str, now: datetime
    ) -> dict[str, object] | None:
        left, right = sorted((source, target))
        pair = hashlib.sha256(f"{left}\0{right}".encode()).hexdigest()
        current = now.isoformat()
        with self._lock, self.db:
            rows = self.db.execute(
                "SELECT dispatched_at FROM cross_session_dispatches WHERE account_id=? "
                "AND pair_key=? AND dispatched_at>=? ORDER BY dispatched_at",
                (account_id, pair, (now - timedelta(hours=1)).isoformat()),
            ).fetchall()
            times = [datetime.fromisoformat(row[0]) for row in rows]
            for window, limit, label in (
                (timedelta(minutes=60), self.HARD_60_MIN, "hard_60m"),
                (timedelta(minutes=40), self.HARD_40_MIN, "hard_40m"),
            ):
                recent = [stamp for stamp in times if stamp >= now - window]
                if len(recent) + 1 >= limit:
                    expire_index = len(recent) - (limit - 1)
                    retry_at = recent[expire_index] + window
                    return {
                        "status": "rate_limited",
                        "level": label,
                        "count": len(recent) + 1,
                        "wait_seconds": max(
                            1,
                            int((retry_at - now).total_seconds()) + 1,
                        ),
                    }
            soft_count = sum(stamp >= now - timedelta(minutes=30) for stamp in times)
            self.db.execute(
                "INSERT INTO cross_session_dispatches VALUES (?,?,?,?,?,?)",
                (uuid4().hex, account_id, pair, source, target, current),
            )
        if soft_count + 1 >= self.SOFT_30_MIN:
            return {
                "status": "soft_warning",
                "level": "soft_30m",
                "count": soft_count + 1,
                "message": "30 分钟内该会话对交流频繁；本次仍已转交。",
            }
        return None

    def _known_conversation(self, account_id: str, conversation_id: str) -> bool:
        if (
            not isinstance(conversation_id, str)
            or not conversation_id.strip()
            or len(conversation_id) > 256
            or conversation_id.startswith("idle-event:")
        ):
            return False
        if not (is_contact_id(conversation_id) or is_room_id(conversation_id) or ":" in conversation_id):
            row_hit = self.db.execute(
                "SELECT 1 FROM source_messages WHERE account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL LIMIT 1",
                (account_id, conversation_id),
            ).fetchone()
            return row_hit is not None
        return any(
            row[0] == conversation_id
            for row in self.db.execute(
                "SELECT conversation_id FROM source_messages WHERE account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL LIMIT 1",
                (account_id, conversation_id),
            ).fetchall()
        )

    def _resolve(self, identifier: str) -> ContactIdentity | None:
        if self.resolve_contact is None or not isinstance(identifier, str) or not identifier.strip():
            return None
        if identifier.startswith(("idle-event:", "Agent内部", "系统空闲")):
            return None
        try:
            return self.resolve_contact(identifier)
        except Exception:  # noqa: BLE001
            return None

    def _display(self, identifier: str) -> str:
        identity = self._resolve(identifier)
        return identity.display_name if identity and identity.display_name else identifier

    def _trusted_name_ids(self, account_id: str) -> set[str]:
        return {account_id} | {
            str(profile["sender_id"])
            for profile in self.store.priority_sender_profiles(account_id=account_id)
            if isinstance(profile.get("sender_id"), str)
        }

    def _bound_rows(
        self, rows: list[dict[str, object]], *, wrap_message_bodies: bool = True
    ) -> list[dict[str, object]]:
        bounded = []
        for source in rows:
            row = dict(source)
            body = row.get("body_text")
            if isinstance(body, str) and len(body) > 2000:
                body = "…" + body[-2000:]
            if wrap_message_bodies and isinstance(body, str):
                row["body_text"] = render_message_body(body)
            bounded.append(row)

        def encoded_size() -> int:
            return len(json.dumps(bounded, ensure_ascii=False, default=str).encode("utf-8"))

        while bounded and encoded_size() > self.byte_limit:
            bounded.pop(0)
        return bounded

    @staticmethod
    def _query(query: str) -> str:
        if not isinstance(query, str):
            raise TypeError("query must be text")
        cleaned = " ".join(query.split())
        if not cleaned or len(cleaned) > MAX_QUERY_CHARS or "\0" in cleaned:
            raise ValueError("query must be 1..300 characters")
        return cleaned

    @staticmethod
    def _validate_task(task: str) -> str:
        if not isinstance(task, str):
            raise TypeError("task must be text")
        value = " ".join(task.split())
        if not value or len(value) > 1000 or "\0" in value:
            raise ValueError("task must be 1..1000 characters")
        return value

    @staticmethod
    def _sql_scalar(value: object) -> bool:
        return (
            value is None
            or isinstance(value, str)
            and len(value) <= 2000
            or isinstance(value, (int, float))
            and not isinstance(value, bool)
        )
