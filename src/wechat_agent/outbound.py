"""Outbound delivery ledger, idempotency, retries, failure notices, and archiving."""
from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import random
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .contacts import is_contact_id, is_group_conversation
from .context import ContextWindow
from .models import ConversationKey, ReplyDraft
from .turn_scope import (
    active_conversation,
    check_outbound_freshness,
    mark_turn_freshness_held,
    operation_id,
    operation_receipt_ids,
    quote_context,
)

logger = logging.getLogger(__name__)
_BUBBLE_RNG = random.SystemRandom()


def _segment_pause_seconds(
    text: str,
    *,
    rng: random.Random | None = None,
) -> float:
    """Return a human-paced inter-bubble delay with random jitter."""
    generator = rng or _BUBBLE_RNG
    chars = len(text.strip())
    base = 0.55 + min(1.35, chars * 0.032)
    jitter = generator.triangular(0.72, 1.38, 1.0)
    pause = base * jitter + generator.uniform(0.0, 0.20)
    return round(min(2.6, max(0.5, pause)), 3)


async def _invoke_retry_reply(
    retry_reply_fn: Callable[..., Awaitable[str]],
    retry_context: ContextWindow,
    turn_label: str,
) -> str:
    params = list(inspect.signature(retry_reply_fn).parameters.values())
    accepts_two = any(
        param.kind == inspect.Parameter.VAR_POSITIONAL for param in params
    ) or sum(
        1
        for param in params
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )
    ) >= 2
    if accepts_two:
        return await retry_reply_fn(retry_context, turn_label)
    return await retry_reply_fn(retry_context)


class OutboundService:
    """Owns tool-receipts.sqlite3, outbound idempotency, retries, notices, and archiving."""

    MAX_SEND_ATTEMPTS = 4

    def __init__(
        self,
        db_path: Path | str,
        *,
        account: str,
        default_room: str,
        gateway: Any = None,
        clock: Any = None,
        memory_store: Any = None,
        can_send: Callable[[], bool] | None = None,
        text_sender: Callable[[ReplyDraft], Any] | None = None,
    ) -> None:
        raw_path = str(db_path)
        if raw_path != ":memory:":
            Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(raw_path) if raw_path != ":memory:" else None
        self.account = account
        self._default_room = default_room
        self.gateway = gateway
        self.clock = clock
        self.memory_store = memory_store
        self.can_send: Callable[[], bool] = (
            can_send if can_send is not None else (lambda: True)
        )
        self._text_sender = text_sender
        self.db = sqlite3.connect(raw_path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        self.db.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY,state TEXT)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS send_side_effect_scopes (scope TEXT PRIMARY KEY)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS delivery_failure_scopes ("
            "scope TEXT PRIMARY KEY,status TEXT NOT NULL,recorded_at TEXT NOT NULL,"
            "failure_count INTEGER NOT NULL DEFAULT 0,reason TEXT,"
            "send_attempt_count INTEGER NOT NULL DEFAULT 0,last_send_status TEXT)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS delivery_failure_notices ("
            "scope TEXT PRIMARY KEY,account_id TEXT NOT NULL,conversation_id TEXT NOT NULL,"
            "status TEXT NOT NULL,reason TEXT,"
            "failure_count INTEGER NOT NULL,created_at TEXT NOT NULL,acknowledged_at TEXT)"
        )
        notice_columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(delivery_failure_notices)")
        }
        if "account_id" not in notice_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_notices ADD COLUMN account_id "
                "TEXT NOT NULL DEFAULT ''"
            )
        if "conversation_id" not in notice_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_notices ADD COLUMN conversation_id "
                "TEXT NOT NULL DEFAULT ''"
            )
        failure_columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(delivery_failure_scopes)")
        }
        if "failure_count" not in failure_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_scopes ADD COLUMN failure_count "
                "INTEGER NOT NULL DEFAULT 0"
            )
            self.db.execute(
                "UPDATE delivery_failure_scopes SET failure_count=1 WHERE status IS NOT NULL"
            )
        if "reason" not in failure_columns:
            self.db.execute("ALTER TABLE delivery_failure_scopes ADD COLUMN reason TEXT")
        if "send_attempt_count" not in failure_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_scopes ADD COLUMN send_attempt_count "
                "INTEGER NOT NULL DEFAULT 0"
            )
            self.db.execute(
                "UPDATE delivery_failure_scopes SET send_attempt_count=failure_count"
            )
        if "last_send_status" not in failure_columns:
            self.db.execute("ALTER TABLE delivery_failure_scopes ADD COLUMN last_send_status TEXT")
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    @property
    def room(self) -> str:
        return active_conversation.get() or self._default_room

    def _now(self) -> str:
        if self.clock is not None:
            return str(self.clock.now())
        return datetime.now(UTC).isoformat()

    def _dispatch_text(self, draft: ReplyDraft) -> Any:
        if self._text_sender is not None:
            return self._text_sender(draft)
        return self.gateway.send_text(draft)

    def update_operation_state(self, receipt: str, state: str) -> None:
        self.db.execute("UPDATE operations SET state=? WHERE id=?", (state, receipt))
        self.db.commit()

    def _record_side_effect_scope(self, scope: str) -> None:
        if not scope:
            return
        scopes = tuple(dict.fromkeys((
            scope,
            *(operation_receipt_ids.get() if scope == operation_id.get() else ()),
        )))
        with self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                ((item,) for item in scopes),
            )

    def send_failure_status(self, scope: str | None = None) -> str | None:
        scope = scope or operation_id.get()
        if not scope:
            return None
        row = self.db.execute(
            "SELECT status,failure_count FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        return str(row[0]) if row and int(row[1]) > 0 else None

    def send_failure_count(self, scope: str | None = None) -> int:
        scope = scope or operation_id.get()
        if not scope:
            return 0
        row = self.db.execute(
            "SELECT failure_count FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        return int(row[0]) if row else 0

    def send_failure_reason(self, scope: str | None = None) -> str | None:
        scope = scope or operation_id.get()
        if not scope:
            return None
        row = self.db.execute(
            "SELECT reason,failure_count FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        return str(row[0]) if row and int(row[1]) > 0 and row[0] else None

    def send_attempt_count(self, scope: str | None = None) -> int:
        scope = scope or operation_id.get()
        if not scope:
            return 0
        row = self.db.execute(
            "SELECT send_attempt_count FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        return int(row[0]) if row else 0

    def delivery_state(self, scope: str) -> dict[str, object]:
        row = self.db.execute(
            "SELECT failure_count,status,reason,send_attempt_count,last_send_status "
            "FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        if scope == operation_id.get() and row is None:
            inputs = [
                self.delivery_state(value)
                for value in operation_receipt_ids.get()
                if value != scope
            ]
            if inputs and all(
                state.get("last_send_status") in ("queued", "accepted_by_server")
                for state in inputs
            ):
                return {**inputs[0], "replayed_input_send": True}
            counts = [state.get("send_attempt_count", 0) for state in inputs]
            if counts and all(
                isinstance(count, int) and count >= self.MAX_SEND_ATTEMPTS
                for count in counts
            ):
                return {**inputs[0], "replayed_input_send": True}
        attempted = self.db.execute(
            "SELECT 1 FROM send_side_effect_scopes WHERE scope=?",
            (scope,),
        ).fetchone() is not None
        if row is None:
            return {
                "failure_count": 0,
                "last_failure_status": None,
                "last_failure_reason": None,
                "send_attempt_count": 0,
                "last_send_status": None,
                "side_effect_started": attempted,
            }
        failures = int(row[0])
        return {
            "failure_count": failures,
            "last_failure_status": row[1] if failures else None,
            "last_failure_reason": row[2] if failures else None,
            "send_attempt_count": int(row[3]),
            "last_send_status": row[4],
            "side_effect_started": attempted,
        }

    def record_send_result(
        self,
        scope: str,
        status: str,
        reason: str | None = None,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> int:
        if not scope:
            return self.send_failure_count(scope)
        account = account_id or self.account
        conversation = conversation_id or self.room
        failed = status in ("rejected", "unknown")
        scopes = tuple(dict.fromkeys((scope, *(
            operation_receipt_ids.get() if scope == operation_id.get() else ()
        ))))
        now = self._now()
        self.db.executemany(
            "INSERT INTO delivery_failure_scopes "
            "(scope,status,recorded_at,failure_count,reason,send_attempt_count,last_send_status) "
            "VALUES (?,?,?, ?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
            "status=CASE WHEN excluded.failure_count>0 THEN excluded.status "
            "ELSE delivery_failure_scopes.status END,"
            "recorded_at=excluded.recorded_at,"
            "failure_count=delivery_failure_scopes.failure_count+excluded.failure_count,"
            "reason=CASE WHEN excluded.failure_count>0 THEN excluded.reason "
            "ELSE delivery_failure_scopes.reason END,"
            "send_attempt_count=delivery_failure_scopes.send_attempt_count+1,"
            "last_send_status=excluded.last_send_status",
            ((value, status, now, int(failed), reason, 1, status) for value in scopes),
        )
        row = self.db.execute(
            "SELECT failure_count FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        failures = int(row[0]) if row else 0
        if failures >= self.MAX_SEND_ATTEMPTS:
            # Persist terminal failure and its next-turn reminder in one database.
            self.db.execute(
                "INSERT OR IGNORE INTO delivery_failure_notices "
                "(scope,account_id,conversation_id,status,reason,failure_count,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    scope,
                    account,
                    conversation,
                    status,
                    (reason or "未取得具体原因")[:300],
                    failures,
                    now,
                ),
            )
        self.db.commit()
        logger.info(
            "event=tool_delivery_recorded operation=%s account_sha=%s conversation_sha=%s "
            "status=%s failure_count=%s reason=%s",
            scope[:12],
            hashlib.sha256(account.encode()).hexdigest()[:12],
            hashlib.sha256(conversation.encode()).hexdigest()[:12],
            status,
            failures,
            str(reason or "none")[:100],
        )
        return failures

    def queue_delivery_failure_notice(
        self,
        scope: str,
        status: str,
        reason: str | None,
        failure_count: int,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        if not scope or status not in (
            "rejected", "unknown", "agent_retry_unknown", "retry_reply_empty",
        ):
            return
        account = account_id or self.account
        conversation = conversation_id or self.room
        self.db.execute(
            "INSERT OR IGNORE INTO delivery_failure_notices "
            "(scope,account_id,conversation_id,status,reason,failure_count,created_at) "
            "VALUES (?,?,?,?,?,?,?)",
            (
                scope,
                account,
                conversation,
                status,
                (reason or "未取得具体原因")[:300],
                failure_count,
                self._now(),
            ),
        )
        self.db.commit()
        logger.warning(
            "event=tool_delivery_notice_queued operation=%s account_sha=%s "
            "conversation_sha=%s status=%s failures=%s",
            scope[:12],
            hashlib.sha256(account.encode()).hexdigest()[:12],
            hashlib.sha256(conversation.encode()).hexdigest()[:12],
            status,
            failure_count,
        )

    def pending_delivery_failure_notices(
        self,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict[str, object]]:
        account = account_id or self.account
        conversation = conversation_id or self.room
        rows = self.db.execute(
            "SELECT scope,status,reason,failure_count,created_at "
            "FROM delivery_failure_notices WHERE account_id=? AND conversation_id=? "
            "AND acknowledged_at IS NULL "
            "ORDER BY created_at,scope LIMIT 20",
            (account, conversation),
        ).fetchall()
        columns = ("scope", "status", "reason", "failure_count", "created_at")
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def acknowledge_delivery_failure_notices(
        self,
        scopes: tuple[str, ...],
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        if not scopes:
            return
        account = account_id or self.account
        conversation = conversation_id or self.room
        placeholders = ",".join("?" for _ in scopes)
        self.db.execute(
            f"UPDATE delivery_failure_notices SET acknowledged_at=? "
            f"WHERE account_id=? AND conversation_id=? AND acknowledged_at IS NULL "
            f"AND scope IN ({placeholders})",
            (self._now(), account, conversation, *scopes),
        )
        self.db.commit()

    @staticmethod
    def format_delivery_notice(row: Mapping[str, object]) -> str:
        status = str(row["status"])
        if status == "rejected":
            certainty = "Hook 明确拒绝，未确认发送。"
        elif status == "unknown":
            certainty = "结果未知，消息可能已送达；再次发送可能造成重复。"
        else:
            certainty = "发送后的自动重试流程中断，最终送达状态未确认。"
        return (
            f"上次发送未能确认成功（失败/尝试 {row['failure_count']} 次）。{certainty}"
            f"原因：{row['reason'] or '未知'}。该提醒只用于本轮判断，不会自动补发；"
            "会话历史中的旧提醒不代表当前仍有待处理失败。"
        )

    def is_send_blocked(self, scope: str | None = None) -> bool:
        scope = scope if scope is not None else operation_id.get()
        if not scope or not self.can_send():
            return False
        state = self.delivery_state(scope)
        return (
            int(state.get("failure_count", 0) or 0) >= self.MAX_SEND_ATTEMPTS
            or int(state.get("send_attempt_count", 0) or 0) >= self.MAX_SEND_ATTEMPTS
            or state.get("last_send_status") in ("queued", "accepted_by_server")
        )

    def send_block_feedback(self, scope: str | None = None) -> dict[str, object]:
        scope = scope if scope is not None else operation_id.get()
        if not scope or not self.can_send():
            return {"status": "already_attempted_or_no_route", "delivered": False}
        state = self.delivery_state(scope)
        last_status = state.get("last_send_status")
        if last_status in ("queued", "accepted_by_server"):
            return {
                "status": "already_submitted",
                "delivered": False,
                "last_send_status": last_status,
                "send_attempt_count": int(state.get("send_attempt_count", 0) or 0),
                "reason": "previous_send_was_accepted_or_queued_do_not_duplicate",
            }
        return {
            "status": "retry_limit_reached",
            "delivered": False,
            "failure_count": int(state.get("failure_count", 0) or 0),
            "send_attempt_count": int(state.get("send_attempt_count", 0) or 0),
            "last_reason": state.get("last_failure_reason"),
        }

    def retry_feedback(
        self,
        status: str,
        reason: str | None,
        *,
        scope: str | None = None,
    ) -> dict[str, object]:
        result: dict[str, object] = {
            "status": status,
            "reason": reason or "send_result_unclassified",
        }
        if status in ("rejected", "unknown"):
            failures = self.send_failure_count(scope)
            result.update({
                "failed_attempt": failures,
                "retries_remaining": max(0, self.MAX_SEND_ATTEMPTS - failures),
                "may_have_been_sent": status == "unknown",
            })
            if failures >= self.MAX_SEND_ATTEMPTS:
                result["status"] = "retry_limit_reached"
                result["last_failure_status"] = status
                result["retries_remaining"] = 0
        return result

    def _claim(self, action: str, value: str) -> str | None:
        scope = operation_id.get()
        if not scope or not self.can_send():
            return None
        state = self.delivery_state(scope)
        failure_count = int(state.get("failure_count", 0) or 0)
        send_attempt_count = int(state.get("send_attempt_count", 0) or 0)
        if (send_attempt_count >= self.MAX_SEND_ATTEMPTS
                or state.get("last_send_status") in ("queued", "accepted_by_server")):
            return None
        if (action in ("text", "quote", "image", "generate_and_send_image")
                and failure_count >= self.MAX_SEND_ATTEMPTS):
            return None
        retry_marker = failure_count if action in (
            "text", "quote", "image", "generate_and_send_image",
        ) else 0
        receipt = hashlib.sha256(
            f"{scope}\0{action}\0{retry_marker}\0{value}".encode()
        ).hexdigest()
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO operations VALUES (?,'attempted')", (receipt,),
            )
            if cursor.rowcount and action in (
                "text", "quote", "image", "generate_and_send_image",
            ):
                scopes = tuple(dict.fromkeys((scope, *operation_receipt_ids.get())))
                self.db.executemany(
                    "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                    ((item,) for item in scopes),
                )
        return receipt if cursor.rowcount else None

    def _claim_segment(self, scope: str, index: int, action: str, value: str) -> str | None:
        state = self.delivery_state(scope)
        failure_count = int(state.get("failure_count", 0) or 0)
        if failure_count >= self.MAX_SEND_ATTEMPTS:
            return None
        receipt = hashlib.sha256(
            f"{scope}\0seg:{index}:{action}\0{failure_count}\0{value}".encode()
        ).hexdigest()
        with self.db:
            cursor = self.db.execute(
                "INSERT OR IGNORE INTO operations VALUES (?,'attempted')", (receipt,),
            )
            if cursor.rowcount:
                scopes = tuple(dict.fromkeys((scope, *operation_receipt_ids.get())))
                self.db.executemany(
                    "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                    ((item,) for item in scopes),
                )
        return receipt if cursor.rowcount else None

    def archive_outbound_text(
        self,
        text: str,
        *,
        operation: str,
        state: str,
        account_id: str | None = None,
        conversation_id: str | None = None,
        note: str | None = None,
        quoted: tuple[str, str, str] | None = None,
    ) -> tuple[str | None, str]:
        valid_states = ("accepted_by_server", "unknown") if quoted is not None else (
            "queued", "unknown",
        )
        if self.memory_store is None or state not in valid_states:
            return None, "not_saved"
        account = account_id or self.account
        conversation = conversation_id or self.room
        body = (
            json.dumps({
                "kind": "quoted_message",
                "text": text,
                "quote": {
                    "author_id": quoted[0],
                    "source_conversation_id": quoted[1],
                    "content": quoted[2],
                },
            }, ensure_ascii=False)
            if quoted is not None
            else text
        )
        try:
            archived = self.memory_store.archive_agent_message(
                account_id=account,
                conversation_id=conversation,
                text=body,
                operation_id=operation,
                delivery_state=state,
                note=note,
            )
            return archived.message_id, ("saved" if note else "not_requested")
        except Exception:
            logger.warning("event=optional_local_operation_failed", exc_info=True)
            return None, "rejected"

    def archive_outbound_image(
        self,
        path: Path,
        *,
        operation: str,
        state: str,
        account_id: str | None = None,
        conversation_id: str | None = None,
        note: str | None = None,
        library_prompt: str | None = None,
        quick_send: bool | None = None,
    ) -> dict[str, str]:
        if not self.memory_store or state not in ("queued", "unknown"):
            return {}
        account = account_id or self.account
        conversation = conversation_id or self.room
        source = path.resolve()
        already_archived = source.is_relative_to(self.memory_store.media_root)
        if not already_archived:
            folder = self.memory_store.media_root / "sent-images"
            folder.mkdir(parents=True, exist_ok=True)
            source = folder / (uuid4().hex + path.suffix.lower())
            source.write_bytes(path.read_bytes())
        mime = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }.get(source.suffix.lower(), "")
        return self.memory_store.archive_agent_image(
            account_id=account,
            conversation_id=conversation,
            artifact_path=source,
            mime_type=mime,
            operation_id=operation,
            delivery_state=state,
            note=note,
            library_prompt=note if library_prompt is None else library_prompt,
            quick_send=(not already_archived) if quick_send is None else quick_send,
        )

    def send_image_path(
        self,
        path: Path,
        *,
        conversation_id: str | None = None,
    ) -> tuple[str, str | None]:
        return self.gateway.send_image(conversation_id or self.room, path)

    async def send_multi_segments(
        self,
        segments: list[str],
        *,
        quote_current_message: bool = False,
        note: str | None = None,
        _record_delivery: bool = True,
    ) -> dict[str, object]:
        if self.is_send_blocked():
            return self.send_block_feedback()
        scope = operation_id.get()
        if not scope or not self.can_send():
            return {"status": "already_attempted_or_no_route", "delivered": False}

        segment_results: list[dict[str, object]] = []
        final_state = "queued"
        final_reason: str | None = None
        q_ctx = quote_context.get() if quote_current_message else None
        can_quote_first = False
        if q_ctx is not None:
            author_id, _author_name, source_room, source_text = q_ctx
            channel = getattr(self.gateway, "channel", "wechat")
            valid_author = is_contact_id(author_id) if channel == "wechat" else bool(author_id)
            valid_room = (
                (source_room == self.room and is_group_conversation(source_room))
                if channel == "wechat"
                else bool(source_room)
            )
            can_quote_first = (
                valid_author
                and valid_room
                and bool(source_text)
                and len(source_text) <= 8192
                and len(segments[0]) <= 4000
            )

        for index, seg_text in enumerate(segments):
            is_last = index == len(segments) - 1
            seg_note = note if is_last else None
            use_quote = index == 0 and can_quote_first
            seg_receipt = self._claim_segment(
                scope, index, "quote" if use_quote else "text", seg_text,
            )
            if not seg_receipt:
                return {"status": "already_attempted_or_no_route", "delivered": False}
            if index > 0:
                await asyncio.sleep(_segment_pause_seconds(seg_text))

            if use_quote and q_ctx is not None:
                author_id, _author_name, source_room, source_text = q_ctx
                try:
                    state, reason = await asyncio.to_thread(
                        self.gateway.send_quote, self.room, seg_text, q_ctx,
                    )
                except Exception:  # noqa: BLE001
                    state = "unknown"
                    reason = "sender_exception_delivery_may_have_occurred"
                self.update_operation_state(seg_receipt, state)
                message_id, _ = self.archive_outbound_text(
                    seg_text,
                    operation=seg_receipt,
                    state=state,
                    note=seg_note,
                    quoted=(author_id, source_room, source_text),
                )
            else:
                try:
                    send_res = await asyncio.to_thread(
                        self._dispatch_text,
                        ReplyDraft("tool", ConversationKey(self.account, self.room), seg_text, 0),
                    )
                    state = send_res.status
                    reason = getattr(send_res, "reason", None)
                except Exception:  # noqa: BLE001
                    state = "unknown"
                    reason = "sender_exception_delivery_may_have_occurred"
                self.update_operation_state(seg_receipt, state)
                message_id, _ = self.archive_outbound_text(
                    seg_text,
                    operation=seg_receipt,
                    state=state,
                    note=seg_note,
                )

            segment_results.append({
                "index": index,
                "status": state,
                "message_id": message_id,
            })
            final_state = state
            final_reason = reason
            if state in ("rejected", "unknown"):
                break

        if _record_delivery:
            self.record_send_result(scope, final_state, final_reason)
        feedback = self.retry_feedback(final_state, final_reason)
        result: dict[str, object] = {
            **feedback,
            "delivered": False,
            "sent_count": len(segment_results),
            "segments": segment_results,
            "message_id": segment_results[-1]["message_id"] if segment_results else None,
            "message_id_kind": "local",
            "sender_id": self.account,
        }
        if quote_current_message and not can_quote_first:
            result["quote_fallback"] = "plain_text"
            result["native_quote_rendered"] = False
        return result

    async def send_text(
        self,
        text: str,
        note: str | None = None,
        *,
        _record_delivery: bool = True,
    ) -> dict[str, object]:
        if self.is_send_blocked():
            return self.send_block_feedback()
        receipt = self._claim("text", text)
        if not receipt:
            return {"status": "already_attempted_or_no_route"}
        try:
            result = await asyncio.to_thread(
                self._dispatch_text,
                ReplyDraft("tool", ConversationKey(self.account, self.room), text, 0),
            )
            state = result.status
            reason = getattr(result, "reason", None)
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "sender_exception_delivery_may_have_occurred"
        self.update_operation_state(receipt, state)
        if _record_delivery:
            self.record_send_result(operation_id.get(), state, reason)
        message_id, note_state = self.archive_outbound_text(
            text, operation=receipt, state=state, note=note,
        )
        feedback = self.retry_feedback(state, reason)
        return {
            **feedback,
            "delivered": False,
            "message_id": message_id,
            "sender_id": self.account,
            "message_id_kind": "local",
            "note_status": note_state,
        }

    async def send_quote(
        self,
        reply_text: str,
        note: str | None = None,
        *,
        _record_delivery: bool = True,
    ) -> dict[str, object]:
        if self.is_send_blocked():
            return {**self.send_block_feedback(), "native_quote_rendered": False}
        context = quote_context.get()
        if (not context or not reply_text or len(reply_text) > 4000
                or not self.can_send() or not operation_id.get()):
            return {
                "status": "unavailable",
                "reason": "current_message_not_quotable",
                "native_quote_rendered": False,
                "delivered": False,
            }
        author_id, _author_name, source_room, source_text = context
        if (not is_contact_id(author_id) or source_room != self.room
                or not source_room.endswith("@chatroom")
                or not source_text or len(source_text) > 8192):
            return {
                "status": "unavailable",
                "native_quote_rendered": False,
                "delivered": False,
            }
        receipt = self._claim("quote", reply_text)
        if not receipt:
            return {
                "status": "already_attempted_or_no_route",
                "native_quote_rendered": False,
                "delivered": False,
            }

        try:
            state, reason = await asyncio.to_thread(
                self.gateway.send_quote, self.room, reply_text, context,
            )
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "sender_exception_delivery_may_have_occurred"
        self.update_operation_state(receipt, state)
        if _record_delivery:
            self.record_send_result(operation_id.get(), state, reason)
        message_id, _ = self.archive_outbound_text(
            reply_text,
            operation=receipt,
            state=state,
            note=note,
            quoted=(author_id, source_room, source_text),
        )
        feedback = self.retry_feedback(state, reason)
        return {
            **feedback,
            "native_quote_rendered": False,
            "delivered": False,
            "source_message_id_available": False,
            "message_id": message_id,
            "message_id_kind": "local",
            "sender_id": self.account,
        }

    async def send_commit_image(
        self,
        image_id: str,
        path: Path,
        *,
        segment_index: int,
        note: str | None = None,
        record_delivery: bool = True,
    ) -> dict[str, object]:
        scope = operation_id.get()
        if not scope or not self.can_send():
            return {"status": "already_attempted_or_no_route", "delivered": False}
        receipt = self._claim_segment(scope, segment_index, "image", image_id)
        if not receipt:
            return {"status": "already_attempted_or_no_route", "delivered": False}
        try:
            state, reason = await asyncio.to_thread(self.send_image_path, path)
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "hook_transport_exception_delivery_may_have_occurred"
        if record_delivery:
            self.record_send_result(scope, state, reason)
        self.update_operation_state(receipt, state)
        archive_status = (
            "not_configured" if self.memory_store is None else
            "not_submitted" if state not in ("queued", "unknown") else "saved"
        )
        archived: dict[str, str] = {}
        if self.memory_store is not None:
            try:
                archived = self.archive_outbound_image(
                    path, operation=receipt, state=state, note=note,
                )
                if not archived and state in ("queued", "unknown"):
                    archive_status = "failed"
            except Exception:  # noqa: BLE001
                archive_status = "failed"
        feedback = self.retry_feedback(state, reason)
        return {
            **feedback,
            "delivered": False,
            "message_id": archived.get("message_id"),
            "image_id": archived.get("image_id") or image_id,
            "message_id_kind": "local",
            "archive_status": archive_status,
        }

    async def send_image(
        self,
        image_id: str,
        path: Path,
        *,
        note: str | None = None,
    ) -> dict[str, object]:
        if self.is_send_blocked():
            return self.send_block_feedback()
        receipt = self._claim("image", image_id)
        if not receipt:
            return {"status": "already_attempted_or_no_route"}
        try:
            state, reason = await asyncio.to_thread(self.send_image_path, path)
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "hook_transport_exception_delivery_may_have_occurred"
        self.record_send_result(operation_id.get(), state, reason)
        self.update_operation_state(receipt, state)
        archive_status = (
            "not_configured" if self.memory_store is None else
            "not_submitted" if state not in ("queued", "unknown") else "saved"
        )
        archived: dict[str, str] = {}
        if self.memory_store is not None:
            try:
                archived = self.archive_outbound_image(
                    path, operation=receipt, state=state, note=note,
                )
                if not archived and state in ("queued", "unknown"):
                    archive_status = "failed"
            except Exception:  # noqa: BLE001
                archive_status = "failed"
        feedback = self.retry_feedback(state, reason)
        return {
            **feedback,
            "delivered": False,
            "message_id": archived.get("message_id"),
            "image_id": archived.get("image_id"),
            "message_id_kind": "local",
            "archive_status": archive_status,
        }

    async def deliver_auto_reply(
        self,
        scope: str,
        account_id: str,
        conversation_id: str,
        text: str,
        context: ContextWindow | None = None,
        retry_reply_fn: Callable[..., Awaitable[str]] | None = None,
        *,
        archive_operation_id: str | None = None,
        max_attempts: int = MAX_SEND_ATTEMPTS,
    ) -> str:
        """Deliver an auto-reply with 1+3 bounded retry orchestration and failure notices."""
        if not text:
            delivery = self.delivery_state(scope)
            if delivery.get("last_send_status") in ("queued", "accepted_by_server"):
                return "agent_tool_send_unconfirmed"
            return "empty_reply"

        state = "unknown"
        for _ in range(max_attempts):
            delivery = self.delivery_state(scope)
            failures = int(delivery.get("failure_count", 0) or 0)
            send_attempts = int(delivery.get("send_attempt_count", 0) or 0)
            last_status = delivery.get("last_failure_status")
            last_reason = delivery.get("last_failure_reason")
            if delivery.get("last_send_status") in ("queued", "accepted_by_server"):
                return "agent_tool_send_unconfirmed"
            if failures >= self.MAX_SEND_ATTEMPTS or send_attempts >= self.MAX_SEND_ATTEMPTS:
                self.queue_delivery_failure_notice(
                    scope,
                    str(last_status or "unknown"),
                    str(last_reason) if last_reason else None,
                    max(failures, send_attempts),
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "retry_limit_reached"
            if not self.can_send():
                return "send_blocked"
            if send_attempts == 0 and not delivery.get("side_effect_started"):
                decision = check_outbound_freshness()
                if decision is not None and decision.kind == "hold":
                    mark_turn_freshness_held()
                    logger.info(
                        "event=outbound_freshness_held scope=%s conflict_count=%s",
                        scope[:12],
                        len(decision.conflicts),
                    )
                    return "freshness_held"

            attempt_number = send_attempts + 1
            self._record_side_effect_scope(scope)
            send_started = time.perf_counter()
            try:
                result = await asyncio.to_thread(
                    self._dispatch_text,
                    ReplyDraft(
                        scope,
                        ConversationKey(account_id, conversation_id),
                        text,
                        0,
                    ),
                )
                state = str(result.status)
                reason = getattr(result, "reason", None)
            except Exception:  # noqa: BLE001
                state = "unknown"
                reason = "sender_exception_delivery_may_have_occurred"

            logger.info(
                "event=send_attempt receipt=%s attempt=%s status=%s reason=%s elapsed_ms=%s",
                scope[:12],
                attempt_number,
                state,
                str(reason or "none")[:100],
                int((time.perf_counter() - send_started) * 1000),
            )
            failures = self.record_send_result(
                scope,
                state,
                reason,
                account_id=account_id,
                conversation_id=conversation_id,
            )
            if archive_operation_id is not None and (
                retry_reply_fn is None or attempt_number == 1
            ):
                archive_op = archive_operation_id
            else:
                archive_op = f"{scope}:auto-{attempt_number}"
            self.archive_outbound_text(
                text,
                operation=archive_op,
                state=state,
                account_id=account_id,
                conversation_id=conversation_id,
            )
            if state not in ("rejected", "unknown"):
                return state
            if failures >= self.MAX_SEND_ATTEMPTS:
                self.queue_delivery_failure_notice(
                    scope,
                    state,
                    reason,
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "retry_limit_reached"
            if retry_reply_fn is None or context is None:
                self.queue_delivery_failure_notice(
                    scope,
                    state,
                    reason,
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return state

            notice = (
                f"上一条发送结果：{state}；原因：{reason or '未知'}；"
                f"失败 {failures}/4，尚可重试 {self.MAX_SEND_ATTEMPTS - failures} 次。"
                + ("消息可能已经送达，重试可能造成重复。" if state == "unknown" else "")
                + "请根据原因修正下一条消息，并简短告知用户失败原因；不要无限重试。"
            )
            retry_context = replace(
                context,
                turns=(),
                history_turns=(),
                recent_other_turns=(),
                delivery_notices=(notice,),
            )
            try:
                text = await _invoke_retry_reply(
                    retry_reply_fn, retry_context, f"retry_{failures}",
                )
            except Exception:  # noqa: BLE001
                failed_status = state
                self.queue_delivery_failure_notice(
                    scope,
                    failed_status,
                    f"重试流程中断；上一发送结果 {failed_status}：{reason or '未知'}",
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "agent_retry_unknown"
            if not text:
                post_retry = self.delivery_state(scope)
                if post_retry.get("last_send_status") in ("queued", "accepted_by_server"):
                    return "agent_tool_send_unconfirmed"
                failed_status = state
                self.queue_delivery_failure_notice(
                    scope,
                    failed_status,
                    f"重试 Agent 未返回可发送内容；上一发送原因：{reason or '未知'}",
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "retry_reply_empty"
        return state
