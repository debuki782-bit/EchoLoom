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
from contextlib import nullcontext
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from .contacts import is_contact_id, is_group_conversation
from .context import ContextWindow
from .db_async import run_db
from .models import ConversationKey, ReplyDraft
from .outbound_store import OutboundReceiptStore
from .turn_scope import (
    active_conversation,
    check_outbound_freshness,
    current_turn_scope,
    mark_turn_freshness_held,
    operation_id,
    operation_receipt_ids,
    quote_context,
)

logger = logging.getLogger(__name__)
_BUBBLE_RNG = random.SystemRandom()


async def _store_db(
    svc: Any, fn: Callable[..., Any], *args: object, **kwargs: object
) -> Any:
    store = getattr(svc, "store", None)
    if store is not None:
        return await run_db(fn, *args, db=store.db, lock=store._lock, **kwargs)
    return fn(*args, **kwargs)


async def _memory_db(
    svc: Any, fn: Callable[..., Any], *args: object, **kwargs: object
) -> Any:
    mem = getattr(svc, "memory_store", None)
    if mem is not None:
        return await run_db(fn, *args, db=mem.db, lock=mem._lock, **kwargs)
    return fn(*args, **kwargs)


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
        self.store = OutboundReceiptStore(
            db_path,
            account=account,
            default_room=default_room,
            clock=clock,
        )
        self.db_path = self.store.db_path
        self.account = account
        self._default_room = default_room
        self.gateway = gateway
        self.clock = clock
        self.memory_store = memory_store
        self.can_send: Callable[[], bool] = (
            can_send if can_send is not None else (lambda: True)
        )
        self._text_sender = text_sender
        self.db = self.store.db

    def close(self) -> None:
        self.store.close()

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
        self.store.update_operation_state(receipt, state)

    def _record_side_effect_scope(self, scope: str) -> None:
        self.store.record_side_effect_scope(
            scope,
            active_operation_id=operation_id.get(),
            receipt_ids=operation_receipt_ids.get(),
        )

    def send_failure_status(self, scope: str | None = None) -> str | None:
        return self.store.send_failure_status(scope or operation_id.get())

    def send_failure_count(self, scope: str | None = None) -> int:
        return self.store.send_failure_count(scope or operation_id.get())

    def send_failure_reason(self, scope: str | None = None) -> str | None:
        return self.store.send_failure_reason(scope or operation_id.get())

    def send_attempt_count(self, scope: str | None = None) -> int:
        return self.store.send_attempt_count(scope or operation_id.get())

    def delivery_state(self, scope: str) -> dict[str, object]:
        return self.store.delivery_state(
            scope,
            active_operation_id=operation_id.get(),
            receipt_ids=operation_receipt_ids.get(),
        )

    def record_send_result(
        self,
        scope: str,
        status: str,
        reason: str | None = None,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> int:
        return self.store.record_send_result(
            scope,
            status,
            reason,
            account_id=account_id,
            conversation_id=conversation_id,
            active_operation_id=operation_id.get(),
            receipt_ids=operation_receipt_ids.get(),
            active_room=self.room,
        )

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
        self.store.queue_delivery_failure_notice(
            scope,
            status,
            reason,
            failure_count,
            account_id=account_id,
            conversation_id=conversation_id,
            active_room=self.room,
        )

    def pending_delivery_failure_notices(
        self,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> list[dict[str, object]]:
        return self.store.pending_delivery_failure_notices(
            account_id=account_id,
            conversation_id=conversation_id,
            active_room=self.room,
        )

    def acknowledge_delivery_failure_notices(
        self,
        scopes: tuple[str, ...],
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> None:
        self.store.acknowledge_delivery_failure_notices(
            scopes,
            account_id=account_id,
            conversation_id=conversation_id,
            active_room=self.room,
        )

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
        return self.store.claim(
            operation_id.get(),
            action,
            value,
            can_send=self.can_send,
            receipt_ids=operation_receipt_ids.get(),
        )

    def _claim_segment(self, scope: str, index: int, action: str, value: str) -> str | None:
        return self.store.claim_segment(
            scope,
            index,
            action,
            value,
            receipt_ids=operation_receipt_ids.get(),
        )

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
        if await _store_db(self, self.is_send_blocked):
            return await _store_db(self, self.send_block_feedback)
        scope = operation_id.get()
        if not scope or not await _store_db(self, self.can_send):
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
            seg_receipt = await _store_db(
                self,
                self._claim_segment,
                scope,
                index,
                "quote" if use_quote else "text",
                seg_text,
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
                await _store_db(self, self.update_operation_state, seg_receipt, state)
                message_id, _ = await _memory_db(
                    self,
                    self.archive_outbound_text,
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
                await _store_db(self, self.update_operation_state, seg_receipt, state)
                message_id, _ = await _memory_db(
                    self,
                    self.archive_outbound_text,
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
            await _store_db(self, self.record_send_result, scope, final_state, final_reason)
        feedback = await _store_db(self, self.retry_feedback, final_state, final_reason)
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
        if await _store_db(self, self.is_send_blocked):
            return await _store_db(self, self.send_block_feedback)
        receipt = await _store_db(self, self._claim, "text", text)
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
        await _store_db(self, self.update_operation_state, receipt, state)
        if _record_delivery:
            await _store_db(self, self.record_send_result, operation_id.get(), state, reason)
        message_id, note_state = await _memory_db(
            self,
            self.archive_outbound_text,
            text,
            operation=receipt,
            state=state,
            note=note,
        )
        feedback = await _store_db(self, self.retry_feedback, state, reason)
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
        if await _store_db(self, self.is_send_blocked):
            return {**(await _store_db(self, self.send_block_feedback)), "native_quote_rendered": False}
        context = quote_context.get()
        if (not context or not reply_text or len(reply_text) > 4000
                or not await _store_db(self, self.can_send) or not operation_id.get()):
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
        receipt = await _store_db(self, self._claim, "quote", reply_text)
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
        await _store_db(self, self.update_operation_state, receipt, state)
        if _record_delivery:
            await _store_db(self, self.record_send_result, operation_id.get(), state, reason)
        message_id, _ = await _memory_db(
            self,
            self.archive_outbound_text,
            reply_text,
            operation=receipt,
            state=state,
            note=note,
            quoted=(author_id, source_room, source_text),
        )
        feedback = await _store_db(self, self.retry_feedback, state, reason)
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
        if not scope or not await _store_db(self, self.can_send):
            return {"status": "already_attempted_or_no_route", "delivered": False}
        receipt = await _store_db(
            self, self._claim_segment, scope, segment_index, "image", image_id,
        )
        if not receipt:
            return {"status": "already_attempted_or_no_route", "delivered": False}
        try:
            state, reason = await asyncio.to_thread(self.send_image_path, path)
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "hook_transport_exception_delivery_may_have_occurred"
        if record_delivery:
            await _store_db(self, self.record_send_result, scope, state, reason)
        await _store_db(self, self.update_operation_state, receipt, state)
        archive_status = (
            "not_configured" if self.memory_store is None else
            "not_submitted" if state not in ("queued", "unknown") else "saved"
        )
        archived: dict[str, str] = {}
        if self.memory_store is not None:
            try:
                archived = await _memory_db(
                    self,
                    self.archive_outbound_image,
                    path,
                    operation=receipt,
                    state=state,
                    note=note,
                )
                if not archived and state in ("queued", "unknown"):
                    archive_status = "failed"
            except Exception:  # noqa: BLE001
                archive_status = "failed"
        feedback = await _store_db(self, self.retry_feedback, state, reason)
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
        if await _store_db(self, self.is_send_blocked):
            return await _store_db(self, self.send_block_feedback)
        receipt = await _store_db(self, self._claim, "image", image_id)
        if not receipt:
            return {"status": "already_attempted_or_no_route"}
        try:
            state, reason = await asyncio.to_thread(self.send_image_path, path)
        except Exception:  # noqa: BLE001
            state = "unknown"
            reason = "hook_transport_exception_delivery_may_have_occurred"
        await _store_db(self, self.record_send_result, operation_id.get(), state, reason)
        await _store_db(self, self.update_operation_state, receipt, state)
        archive_status = (
            "not_configured" if self.memory_store is None else
            "not_submitted" if state not in ("queued", "unknown") else "saved"
        )
        archived: dict[str, str] = {}
        if self.memory_store is not None:
            try:
                archived = await _memory_db(
                    self,
                    self.archive_outbound_image,
                    path,
                    operation=receipt,
                    state=state,
                    note=note,
                )
                if not archived and state in ("queued", "unknown"):
                    archive_status = "failed"
            except Exception:  # noqa: BLE001
                archive_status = "failed"
        feedback = await _store_db(self, self.retry_feedback, state, reason)
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
            delivery = await _store_db(self, self.delivery_state, scope)
            if delivery.get("last_send_status") in ("queued", "accepted_by_server"):
                return "agent_tool_send_unconfirmed"
            return "empty_reply"

        state = "unknown"
        store = getattr(self, "store", None)
        freshness_off_thread = current_turn_scope().freshness_thread_safe
        lock_ctx = store._lock if store is not None else nullcontext()

        def _prepare_attempt(*, allow_freshness: bool) -> tuple[str, int, int]:
            with lock_ctx:
                delivery = self.delivery_state(scope)
                failures_local = int(delivery.get("failure_count", 0) or 0)
                send_attempts = int(delivery.get("send_attempt_count", 0) or 0)
                last_status = delivery.get("last_failure_status")
                last_reason = delivery.get("last_failure_reason")
                if delivery.get("last_send_status") in ("queued", "accepted_by_server"):
                    return ("agent_tool_send_unconfirmed", 0, failures_local)
                if (
                    failures_local >= self.MAX_SEND_ATTEMPTS
                    or send_attempts >= self.MAX_SEND_ATTEMPTS
                ):
                    self.queue_delivery_failure_notice(
                        scope,
                        str(last_status or "unknown"),
                        str(last_reason) if last_reason else None,
                        max(failures_local, send_attempts),
                        account_id=account_id,
                        conversation_id=conversation_id,
                    )
                    return ("retry_limit_reached", 0, failures_local)
                if not self.can_send():
                    return ("send_blocked", 0, failures_local)
                if allow_freshness and send_attempts == 0 and not delivery.get("side_effect_started"):
                    decision = check_outbound_freshness()
                    if decision is not None and decision.kind == "hold":
                        mark_turn_freshness_held()
                        logger.info(
                            "event=outbound_freshness_held scope=%s conflict_count=%s",
                            scope[:12],
                            len(decision.conflicts),
                        )
                        return ("freshness_held", 0, failures_local)
                self._record_side_effect_scope(scope)
                return ("ok", send_attempts + 1, failures_local)

        def _finish_attempt(
            attempt_number: int,
            attempt_text: str,
            next_state: str,
            next_reason: str | None,
            *,
            archive_inline: bool,
        ) -> tuple[str, str, str | None, int, str]:
            with lock_ctx:
                next_failures = self.record_send_result(
                    scope,
                    next_state,
                    next_reason,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                archive_op = (
                    archive_operation_id
                    if archive_operation_id is not None
                    and (retry_reply_fn is None or attempt_number == 1)
                    else f"{scope}:auto-{attempt_number}"
                )
                if archive_inline:
                    self.archive_outbound_text(
                        attempt_text,
                        operation=archive_op,
                        state=next_state,
                        account_id=account_id,
                        conversation_id=conversation_id,
                    )
                if next_state not in ("rejected", "unknown"):
                    return ("return", next_state, next_reason, next_failures, archive_op)
                if next_failures >= self.MAX_SEND_ATTEMPTS or retry_reply_fn is None or context is None:
                    self.queue_delivery_failure_notice(
                        scope,
                        next_state,
                        next_reason,
                        next_failures,
                        account_id=account_id,
                        conversation_id=conversation_id,
                    )
                    final = "retry_limit_reached" if next_failures >= self.MAX_SEND_ATTEMPTS else next_state
                    return ("return", final, next_reason, next_failures, archive_op)
                return ("retry", next_state, next_reason, next_failures, archive_op)

        def _dispatch_sync(attempt_text: str) -> tuple[str, str | None]:
            try:
                result = self._dispatch_text(
                    ReplyDraft(scope, ConversationKey(account_id, conversation_id), attempt_text, 0),
                )
                return str(result.status), getattr(result, "reason", None)
            except Exception:  # noqa: BLE001
                return "unknown", "sender_exception_delivery_may_have_occurred"

        for _ in range(max_attempts):
            if store is not None:
                if not freshness_off_thread:
                    pre = await _store_db(self, self.delivery_state, scope)
                    if int(pre.get("send_attempt_count", 0) or 0) == 0 and not pre.get(
                        "side_effect_started"
                    ):
                        decision = check_outbound_freshness()
                        if decision is not None and decision.kind == "hold":
                            mark_turn_freshness_held()
                            logger.info(
                                "event=outbound_freshness_held scope=%s conflict_count=%s",
                                scope[:12],
                                len(decision.conflicts),
                            )
                            return "freshness_held"

                def _attempt_cycle(attempt_text: str = text) -> tuple[str, str, str | None, int]:
                    prep, attempt_num, prep_failures = _prepare_attempt(
                        allow_freshness=freshness_off_thread,
                    )
                    if prep != "ok":
                        return ("return", prep, None, prep_failures)
                    started = time.perf_counter()
                    next_state, next_reason = _dispatch_sync(attempt_text)
                    logger.info(
                        "event=send_attempt receipt=%s attempt=%s status=%s reason=%s elapsed_ms=%s",
                        scope[:12],
                        attempt_num,
                        next_state,
                        str(next_reason or "none")[:100],
                        int((time.perf_counter() - started) * 1000),
                    )
                    act, out_state, out_reason, out_fail, _ = _finish_attempt(
                        attempt_num,
                        attempt_text,
                        next_state,
                        next_reason,
                        archive_inline=True,
                    )
                    return (act, out_state, out_reason, out_fail)

                action, state, reason, failures = await asyncio.to_thread(_attempt_cycle)
                if action == "return":
                    return state
            else:
                prep, attempt_number, _ = _prepare_attempt(allow_freshness=True)
                if prep != "ok":
                    return prep
                started = time.perf_counter()
                state, reason = await asyncio.to_thread(_dispatch_sync, text)
                logger.info(
                    "event=send_attempt receipt=%s attempt=%s status=%s reason=%s elapsed_ms=%s",
                    scope[:12],
                    attempt_number,
                    state,
                    str(reason or "none")[:100],
                    int((time.perf_counter() - started) * 1000),
                )
                action, out_state, reason, failures, archive_op = _finish_attempt(
                    attempt_number,
                    text,
                    state,
                    reason,
                    archive_inline=False,
                )
                await _memory_db(
                    self,
                    self.archive_outbound_text,
                    text,
                    operation=archive_op,
                    state=state,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                if action == "return":
                    return out_state

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
                await _store_db(
                    self,
                    self.queue_delivery_failure_notice,
                    scope,
                    failed_status,
                    f"重试流程中断；上一发送结果 {failed_status}：{reason or '未知'}",
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "agent_retry_unknown"
            if not text:
                post_retry = await _store_db(self, self.delivery_state, scope)
                if post_retry.get("last_send_status") in ("queued", "accepted_by_server"):
                    return "agent_tool_send_unconfirmed"
                failed_status = state
                await _store_db(
                    self,
                    self.queue_delivery_failure_notice,
                    scope,
                    failed_status,
                    f"重试 Agent 未返回可发送内容；上一发送原因：{reason or '未知'}",
                    failures,
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
                return "retry_reply_empty"
        return state
