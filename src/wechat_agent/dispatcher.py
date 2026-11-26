"""Worker dispatcher: single-source-of-truth inbox execution, circuit breaker, and delivery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import ExitStack
from contextvars import ContextVar
from dataclasses import replace
from datetime import UTC, datetime

from .contacts import ContactIdentity
from .context import ContextWindow, fit_context_window
from .context_assembler import AssembledBatch, MessageContextAssembler
from .context_builder import ContextBuilder, ContextSpec
from .conversation_scheduler import ConversationScheduler, MessageBatch
from .ingress import sha
from .media import WeChatFileIngestor, WeChatImageIngestor, WeChatVoiceIngestor
from .memory_store import MemoryStore
from .message_ingestor import MessageIngestor
from .models import ReplyDraft
from .outbound import OutboundService
from .runtime_contract import BudgetExhausted as AdkBudgetExhausted
from .runtime_contract import ModelCallIndeterminate, ModelProviderUnavailable
from .runtime_contract import ModelCallsLimitExceeded as LlmCallsLimitExceededError
from .temporal_context import DailyEnvironmentProvider
from .tool_timing import turn_deadline_monotonic, turn_started_monotonic
from .turn_scope import operation_receipt_ids, tool_turn_scope
from .wechat_gateway import MessageEvent, SendResult, default_archive_envelope

logger = logging.getLogger(__name__)

_assembled_batch: ContextVar[AssembledBatch | None] = ContextVar(
    "assembled_message_batch",
    default=None,
)
_inbox_batch: ContextVar[MessageBatch | None] = ContextVar("inbox_message_batch", default=None)
_batch_scheduler: ContextVar[ConversationScheduler | None] = ContextVar(
    "inbox_batch_scheduler",
    default=None,
)

COMPLETED_ATTEMPT_STATES = frozenset(
    {
        "queued",
        "accepted_by_server",
        "sent",
        "agent_tool_send_unconfirmed",
        "empty_reply",
        "followup_scheduled",
    }
)


def current_message_id(context: ContextWindow) -> str:
    """Mutation provenance must point into this turn, never appended history."""
    return next(
        (turn.message_id for turn in reversed(context.turns) if turn.message_id),
        "",
    )


class _CallbackOutboundAdapter:
    """Bridge raw Pilot send/ledger callbacks onto OutboundService.deliver_auto_reply."""

    MAX_SEND_ATTEMPTS = OutboundService.MAX_SEND_ATTEMPTS

    def __init__(
        self,
        *,
        send: Callable[[ReplyDraft], SendResult],
        delivery_state: Callable[[str], dict[str, object]] | None = None,
        record_send_result: Callable[[str, str, str | None], int] | None = None,
        record_delivery_notice: Callable[[str, str, str | None, int], None] | None = None,
        memory_store: MemoryStore | None = None,
    ) -> None:
        self._send = send
        self._delivery_state = delivery_state
        self._record_send_result = record_send_result
        self._record_delivery_notice = record_delivery_notice
        self.memory_store = memory_store
        self._fallback: dict[str, dict[str, object]] = {}

    def can_send(self) -> bool:
        return True

    def _record_side_effect_scope(self, _scope: str) -> None:
        return None

    def _dispatch_text(self, draft: ReplyDraft) -> SendResult:
        return self._send(draft)

    def delivery_state(self, scope: str) -> dict[str, object]:
        if self._delivery_state is not None:
            return dict(self._delivery_state(scope))
        return dict(
            self._fallback.get(
                scope,
                {
                    "failure_count": 0,
                    "last_failure_status": None,
                    "last_failure_reason": None,
                    "send_attempt_count": 0,
                    "last_send_status": None,
                    "side_effect_started": False,
                },
            )
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
        _ = (account_id, conversation_id)
        if self._record_send_result is not None:
            return int(self._record_send_result(scope, status, reason))
        entry = self._fallback.setdefault(
            scope,
            {
                "failure_count": 0,
                "last_failure_status": None,
                "last_failure_reason": None,
                "send_attempt_count": 0,
                "last_send_status": None,
                "side_effect_started": True,
            },
        )
        entry["send_attempt_count"] = int(entry["send_attempt_count"] or 0) + 1
        entry["last_send_status"] = status
        if status in ("rejected", "unknown"):
            entry["failure_count"] = int(entry["failure_count"] or 0) + 1
            entry["last_failure_status"] = status
            entry["last_failure_reason"] = reason
        return int(entry["failure_count"] or 0)

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
        _ = (account_id, conversation_id)
        if self._record_delivery_notice is None:
            return
        self._record_delivery_notice(scope, status, reason, failure_count)
        logger.warning(
            "event=send_failure_notice_queued receipt=%s status=%s failures=%s reason=%s",
            scope[:12],
            status,
            failure_count,
            str(reason or "unknown")[:100],
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
        _ = quoted
        if self.memory_store is None or state not in ("queued", "unknown"):
            return None, "not_saved"
        try:
            archived = self.memory_store.archive_agent_message(
                account_id=str(account_id or ""),
                conversation_id=str(conversation_id or ""),
                text=text,
                operation_id=operation,
                delivery_state=state,
                note=note,
            )
            return archived.message_id, ("saved" if note else "not_requested")
        except Exception:
            logger.warning("event=optional_local_operation_failed", exc_info=True)
            return None, "rejected"


class Dispatcher:
    """Single-source-of-truth worker executing durable inbox batches and replies."""

    PROCESS_TIMEOUT_SECONDS = 180.0

    def __init__(
        self,
        db: sqlite3.Connection,
        scheduler: ConversationScheduler,
        account: str,
        room: str,
        reply: Callable[[ContextWindow], Awaitable[str]],
        send: Callable[[ReplyDraft], SendResult] | None = None,
        *,
        outbound: OutboundService | None = None,
        limit: int | None = 6,
        resolve_contact: Callable[[str], ContactIdentity | None] | None = None,
        memory_store: MemoryStore | None = None,
        image_ingestor: WeChatImageIngestor | None = None,
        voice_ingestor: WeChatVoiceIngestor | None = None,
        file_ingestor: WeChatFileIngestor | None = None,
        daily_environment: DailyEnvironmentProvider | None = None,
        record_activity: Callable[[str, str], Awaitable[None]] | None = None,
        delivery_state: Callable[[str], dict[str, object]] | None = None,
        record_send_result: Callable[[str, str, str | None], int] | None = None,
        pending_delivery_notices: Callable[[], list[dict[str, object]]] | None = None,
        acknowledge_delivery_notices: Callable[[tuple[str, ...]], None] | None = None,
        record_delivery_notice: Callable[[str, str, str | None, int], None] | None = None,
        context_assembler: MessageContextAssembler | None = None,
        message_ingestor: MessageIngestor | None = None,
        context_builder: ContextBuilder | None = None,
        max_user_bytes: int = 32_000,
        max_user_bytes_for_conversation: Callable[[str, str], int] | None = None,
        special_command_handler: Callable[[MessageEvent], Awaitable[str | None]] | None = None,
        archive_envelope=default_archive_envelope,
        provider_check: Callable[[], Awaitable[bool]] | None = None,
        provider_probe_base_seconds: float = 5.0,
        provider_probe_max_seconds: float = 30.0,
        on_model_indeterminate: Callable[[], Awaitable[None] | None] | None = None,
        indeterminate_cooldown_base_seconds: float = 30.0,
        indeterminate_cooldown_max_seconds: float = 600.0,
        in_scope: Callable[[MessageEvent], bool] | None = None,
        is_paused: Callable[[], bool] | None = None,
        is_wechat_ready: Callable[[], bool] | None = None,
        on_budget_recovered: Callable[[MessageEvent], None] | None = None,
        blocked_conversations_provider: Callable[[], set[tuple[str, str]]] | None = None,
        is_followup_scheduled: Callable[[str, str], bool] | None = None,
        get_cancelled_followup: Callable[[str, str], int | None] | None = None,
        clear_cancelled_followup: Callable[[str, str, int], None] | None = None,
        followup_factory: (
            Callable[[ContextWindow, str], Callable[[str, int], Awaitable[dict[str, object]]]]
            | None
        ) = None,
        scheduled_followups_count: Callable[[], int] | None = None,
        drain_queue: Callable[[], None] | None = None,
        worker_id: str = "default",
        heartbeat_interval_seconds: float = 2.0,
        process_timeout_seconds: float = PROCESS_TIMEOUT_SECONDS,
    ) -> None:
        self.db = db
        self.scheduler = scheduler
        self.account = account
        self.room = room
        self.reply = reply
        self.send = send
        self.outbound = outbound
        self.limit = limit
        self.resolve_contact = resolve_contact
        self.memory_store = memory_store
        self.image_ingestor = image_ingestor
        self.voice_ingestor = voice_ingestor
        self.file_ingestor = file_ingestor
        self.daily_environment = daily_environment
        self.record_activity = record_activity
        self.delivery_state = delivery_state
        self.record_send_result = record_send_result
        self.pending_delivery_notices = pending_delivery_notices
        self.acknowledge_delivery_notices = acknowledge_delivery_notices
        self.record_delivery_notice = record_delivery_notice
        self.context_assembler = context_assembler
        self._message_ingestor = message_ingestor
        self._context_builder = context_builder
        self.max_user_bytes = max_user_bytes
        self.max_user_bytes_for_conversation = max_user_bytes_for_conversation
        self._default_ingestor = MessageIngestor(
            memory_store,
            image_ingestor=image_ingestor,
            voice_ingestor=voice_ingestor,
            file_ingestor=file_ingestor,
            archive_envelope=archive_envelope,
        )
        self._default_builder = ContextBuilder(
            memory_store,
            resolve_contact=resolve_contact,
            daily_environment=daily_environment,
        )
        self.special_command_handler = special_command_handler
        self.archive_envelope = archive_envelope
        self.provider_check = provider_check
        self._provider_probe_base_seconds = float(provider_probe_base_seconds)
        self._provider_probe_max_seconds = float(provider_probe_max_seconds)
        self._provider_ready = True
        self._provider_failures = 0
        self._provider_next_probe_monotonic = 0.0
        self._provider_probe_lock = asyncio.Lock()
        self.on_model_indeterminate = on_model_indeterminate
        self._indeterminate_cooldown_base_seconds = max(0.0, float(indeterminate_cooldown_base_seconds))
        self._indeterminate_cooldown_max_seconds = max(
            self._indeterminate_cooldown_base_seconds,
            float(indeterminate_cooldown_max_seconds),
        )
        self._indeterminate_failures = 0
        self._indeterminate_cooldown_until_monotonic = 0.0
        self._in_scope = in_scope or (lambda item: item.account_id == self.account)
        self._is_paused = is_paused or (lambda: False)
        self._is_wechat_ready = is_wechat_ready or (lambda: True)
        self.on_budget_recovered = on_budget_recovered
        self.blocked_conversations_provider = blocked_conversations_provider
        self.is_followup_scheduled = is_followup_scheduled
        self.get_cancelled_followup = get_cancelled_followup
        self.clear_cancelled_followup = clear_cancelled_followup
        self.followup_factory = followup_factory
        self.scheduled_followups_count = scheduled_followups_count
        self.drain_queue = drain_queue
        self.worker_id = worker_id
        self.heartbeat_interval_seconds = float(heartbeat_interval_seconds)
        self.process_timeout_seconds = float(process_timeout_seconds)
        self._in_flight: dict[str, object] | None = None

    @property
    def provider_ready(self) -> bool:
        return self._provider_ready

    @property
    def provider_probe_base_seconds(self) -> float:
        return self._provider_probe_base_seconds

    @property
    def has_pending_work(self) -> bool:
        return self.scheduler.active_count() > 0 or self._in_flight is not None

    def in_flight_snapshot(self, *, include_started_at: bool = False) -> dict[str, object] | None:
        if self._in_flight is None:
            return None
        snapshot: dict[str, object] = {
            "sender_sha": self._in_flight["sender_sha"],
            "message_type": self._in_flight["message_type"],
            "elapsed_seconds": round(
                time.monotonic() - float(self._in_flight["started_monotonic"]),
                1,
            ),
        }
        if include_started_at and "started_at" in self._in_flight:
            snapshot["started_at"] = self._in_flight["started_at"]
        return snapshot

    def publish_heartbeat(self) -> None:
        """Persist current worker readiness and in-flight snapshot to worker_heartbeat."""
        try:
            self.scheduler.update_worker_heartbeat(
                worker_id=self.worker_id,
                pid=os.getpid(),
                wechat_ready=self._is_wechat_ready(),
                provider_ready=self._provider_ready,
                in_flight=self.in_flight_snapshot(include_started_at=True),
                scheduled_followups=(
                    self.scheduled_followups_count()
                    if self.scheduled_followups_count is not None
                    else 0
                ),
            )
        except sqlite3.Error as exc:
            logger.warning("event=worker_heartbeat_write_failed error_type=%s", type(exc).__name__)

    def _next_provider_backoff(self) -> float:
        return min(
            self._provider_probe_max_seconds,
            self._provider_probe_base_seconds * (2 ** min(max(0, self._provider_failures - 1), 4)),
        )

    def trip_provider_circuit(self, reason: str) -> float:
        self._provider_failures += 1
        self._provider_ready = False
        delay = self._next_provider_backoff()
        self._provider_next_probe_monotonic = time.monotonic() + delay
        logger.warning(
            "event=provider_circuit_opened reason=%s failures=%s retry_in_seconds=%.2f",
            reason,
            self._provider_failures,
            delay,
        )
        return delay

    def mark_provider_healthy(self, *, source: str) -> None:
        if not self._provider_ready or self._provider_failures > 0:
            if not self._provider_ready:
                logger.info("event=provider_circuit_closed source=%s", source)
            self._provider_ready = True
            self._provider_failures = 0
            self._provider_next_probe_monotonic = 0.0
            self.scheduler.wake_deferred_batches()
        self._indeterminate_failures = 0
        self._indeterminate_cooldown_until_monotonic = 0.0

    def trip_indeterminate_cooldown(self, reason: str) -> float:
        """Apply worker-wide exponential cooldown after ambiguous timeout/upstream errors."""
        self._indeterminate_failures += 1
        if self._indeterminate_cooldown_base_seconds <= 0:
            cooldown = 0.0
        else:
            exponent = min(max(0, self._indeterminate_failures - 1), 5)
            cooldown = min(
                self._indeterminate_cooldown_max_seconds,
                self._indeterminate_cooldown_base_seconds * (2 ** exponent),
            )
        self._indeterminate_cooldown_until_monotonic = max(
            self._indeterminate_cooldown_until_monotonic,
            time.monotonic() + cooldown,
        )
        logger.warning(
            "event=model_indeterminate_cooldown reason=%s failures=%s cooldown_seconds=%.2f",
            reason,
            self._indeterminate_failures,
            cooldown,
        )
        return cooldown

    def _batch_has_side_effect(self, batch_scope: str) -> bool:
        delivery = self.current_delivery_state(batch_scope)
        return (
            int(delivery.get("send_attempt_count", 0) or 0) > 0
            or delivery.get("last_send_status")
            in ("queued", "accepted_by_server", "unknown")
        )

    async def handle_model_indeterminate(
        self,
        batch: MessageBatch | None = None,
        *,
        reason: str = "worker_turn_indeterminate",
        scope_override: str | None = None,
    ) -> tuple[float, bool]:
        """Recycle stale provider clients, open cooldown, and check outbound side effects."""
        if self.on_model_indeterminate is not None:
            try:
                outcome = self.on_model_indeterminate()
                if inspect.isawaitable(outcome):
                    await outcome
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - client refresh must stay non-fatal
                logger.warning(
                    "event=indeterminate_client_refresh_failed error_type=%s",
                    type(exc).__name__,
                )
        cooldown = self.trip_indeterminate_cooldown(reason)
        if batch is not None:
            scope = hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
        else:
            scope = str(scope_override or "")
        has_side_effect = self._batch_has_side_effect(scope) if scope else False
        return cooldown, has_side_effect

    async def ensure_provider_ready(self) -> bool:
        if self._provider_ready:
            return True
        if time.monotonic() < self._provider_next_probe_monotonic:
            return False
        if self.provider_check is None:
            return True
        async with self._provider_probe_lock:
            if self._provider_ready:
                return True
            if time.monotonic() < self._provider_next_probe_monotonic:
                return False
            try:
                ok = bool(await self.provider_check())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                ok = False
            if ok:
                self.mark_provider_healthy(source="probe")
                return True
            self._provider_failures += 1
            delay = self._next_provider_backoff()
            self._provider_next_probe_monotonic = time.monotonic() + delay
            logger.warning(
                "event=provider_probe_failed failures=%s retry_in_seconds=%.2f",
                self._provider_failures,
                delay,
            )
            return False

    def current_delivery_state(self, scope: str) -> dict[str, object]:
        if self.outbound is not None:
            return self.outbound.delivery_state(scope)
        if self.delivery_state is not None:
            return dict(self.delivery_state(scope))
        return {}

    def _pending_notices(self) -> list[dict[str, object]]:
        if self.outbound is not None:
            return self.outbound.pending_delivery_failure_notices()
        if self.pending_delivery_notices is not None:
            return self.pending_delivery_notices()
        return []

    def _acknowledge_notices(self, scopes: tuple[str, ...]) -> None:
        if self.outbound is not None:
            self.outbound.acknowledge_delivery_failure_notices(scopes)
        elif self.acknowledge_delivery_notices is not None:
            self.acknowledge_delivery_notices(scopes)

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
    ) -> str:
        """Deliver an auto-reply via OutboundService (or callback adapter in unit tests)."""
        if self.outbound is not None:
            return await self.outbound.deliver_auto_reply(
                scope,
                account_id,
                conversation_id,
                text,
                context,
                retry_reply_fn,
                archive_operation_id=archive_operation_id,
            )
        if self.send is None:
            return "empty_reply" if not text else "send_blocked"
        adapter = _CallbackOutboundAdapter(
            send=self.send,
            delivery_state=self.delivery_state,
            record_send_result=self.record_send_result,
            record_delivery_notice=self.record_delivery_notice,
            memory_store=self.memory_store,
        )
        return await OutboundService.deliver_auto_reply(
            adapter,  # type: ignore[arg-type]
            scope,
            account_id,
            conversation_id,
            text,
            context,
            retry_reply_fn,
            archive_operation_id=archive_operation_id,
        )

    def _record_attempt_audit(self, receipt: str, state: str) -> None:
        with self.db:
            self.db.execute(
                "INSERT INTO attempts (id,state) VALUES (?,?) "
                "ON CONFLICT(id) DO UPDATE SET state=excluded.state",
                (receipt, state),
            )

    def _check_single_attempt_gate(self, receipt: str, *, is_batch: bool) -> bool:
        """Return True if execution should proceed; attempts NEVER blocks inbox batches."""
        if is_batch:
            self._record_attempt_audit(receipt, "started")
            return True
        existing = self.db.execute(
            "SELECT state FROM attempts WHERE id=?",
            (receipt,),
        ).fetchone()
        if existing is not None and str(existing[0]) in COMPLETED_ATTEMPT_STATES:
            logger.info("event=process_duplicate receipt=%s", receipt[:12])
            return False
        self._record_attempt_audit(receipt, "started")
        return True

    def _test_limit_reached(self) -> bool:
        if self.limit is None:
            return False
        count = self.db.execute(
            "SELECT count(*) FROM attempts "
            "WHERE state NOT IN ('provider_unavailable','budget_exhausted')",
        ).fetchone()[0]
        return int(count) >= self.limit

    async def reply_logged(self, context: ContextWindow, receipt: str, turn: str) -> str:
        started_at = time.perf_counter()
        try:
            text = await self.reply(context)
        except Exception as exc:
            logger.warning(
                "event=agent_turn_failed receipt=%s turn=%s error_type=%s elapsed_ms=%s",
                receipt[:12],
                turn,
                type(exc).__name__,
                int((time.perf_counter() - started_at) * 1000),
            )
            raise
        logger.info(
            "event=agent_turn_finished receipt=%s turn=%s has_reply=%s reply_chars=%s "
            "elapsed_ms=%s",
            receipt[:12],
            turn,
            bool(text),
            len(text),
            int((time.perf_counter() - started_at) * 1000),
        )
        return text

    @property
    def message_ingestor(self) -> MessageIngestor:
        if self._message_ingestor is not None:
            return self._message_ingestor
        if self.context_assembler is not None:
            return self.context_assembler.ingestor
        return self._default_ingestor

    @message_ingestor.setter
    def message_ingestor(self, value: MessageIngestor | None) -> None:
        self._message_ingestor = value

    @property
    def context_builder(self) -> ContextBuilder:
        if self._context_builder is not None:
            return self._context_builder
        if self.context_assembler is not None:
            return self.context_assembler.builder
        return self._default_builder

    @context_builder.setter
    def context_builder(self, value: ContextBuilder | None) -> None:
        self._context_builder = value

    def _effective_max_user_bytes(
        self,
        account_id: str | None = None,
        conversation_id: str | None = None,
    ) -> int:
        if (
            self.max_user_bytes_for_conversation is not None
            and account_id
            and conversation_id
        ):
            return self.max_user_bytes_for_conversation(account_id, conversation_id)
        if self.context_assembler is not None:
            return self.context_assembler.max_user_bytes
        return self.max_user_bytes

    def _has_custom_assemble(self) -> bool:
        assembler = self.context_assembler
        if assembler is None:
            return False
        return (
            "assemble" in vars(assembler)
            or type(assembler).assemble is not MessageContextAssembler.assemble
        )

    async def _assemble_observations(
        self, observations: Sequence[MessageEvent],
    ) -> AssembledBatch:
        items = tuple(observations)
        if self._has_custom_assemble() or (
            self.context_assembler is not None and self.context_assembler.upgrade_recent_media
        ):
            assert self.context_assembler is not None
            return await self.context_assembler.assemble(items)
        ingested = await self.message_ingestor.ingest(items)
        first = items[0]
        built = await self.context_builder.build(
            ContextSpec.for_chat(
                ingested=ingested,
                max_user_bytes=self._effective_max_user_bytes(
                    first.account_id,
                    first.conversation_id,
                ),
            )
        )
        return AssembledBatch(
            context=built.context,
            memory_message_ids=ingested.message_ids,
            quote_context=built.quote_context,
        )

    async def process_single(self, item: MessageEvent) -> str:
        """Process one direct item or a pre-assembled inbox batch bound via ContextVars."""
        started_at = time.perf_counter()
        if self._is_paused():
            logger.info("event=process_skipped reason=paused")
            return "paused"
        if not self._in_scope(item):
            logger.info("event=process_skipped reason=outside_test_scope")
            return "outside_test_scope"
        if self._test_limit_reached():
            return "test_limit"
        batch = _inbox_batch.get()
        receipt = (
            hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
            if batch is not None
            else hashlib.sha256(
                f"{item.account_id}\0{item.conversation_id}\0{item.sender_id}\0"
                f"{item.capture_tick_ms}\0{item.source_msg_id}".encode()
            ).hexdigest()
        )
        if not self._check_single_attempt_gate(receipt, is_batch=batch is not None):
            return "duplicate_receipt"

        timing_token = turn_started_monotonic.set(time.monotonic())
        logger.info(
            "event=process_started receipt=%s account_sha=%s conversation_sha=%s "
            "sender_sha=%s message_type=%s",
            receipt[:12],
            sha(item.account_id),
            sha(item.conversation_id),
            sha(item.sender_id),
            item.message_type,
        )
        if self.record_activity is not None:
            try:
                await self.record_activity(item.account_id, item.conversation_id)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "event=inbound_activity_record_failed receipt=%s error_type=%s",
                    receipt[:12],
                    type(exc).__name__,
                )
        state = "unknown"
        turn_scope = ExitStack()
        phase = "agent"
        prepared = _assembled_batch.get()
        try:
            if prepared is not None:
                context = prepared.context
                quote_source = prepared.quote_context
            else:
                assembled = await self._assemble_observations((item,))
                context = assembled.context
                quote_source = assembled.quote_context
            cancelled_followup_notice = (
                self.get_cancelled_followup(item.account_id, item.conversation_id)
                if self.get_cancelled_followup is not None
                else None
            )
            if cancelled_followup_notice is not None:
                context = replace(context, cancelled_followup=True)
            schedule_fn = (
                self.followup_factory(context, receipt)
                if self.followup_factory is not None
                else None
            )
            turn_scope.enter_context(
                tool_turn_scope(
                    context,
                    receipt,
                    receipt_ids=batch.receipt_ids if batch is not None else (),
                    quote=quote_source,
                    schedule=schedule_fn,
                )
            )
            pending_notices = self._pending_notices()
            if pending_notices:
                context = replace(
                    context,
                    delivery_notices=tuple(
                        OutboundService.format_delivery_notice(row) for row in pending_notices
                    ),
                )
            logger.info(
                "event=context_ready receipt=%s current_count=%s same_chat_history=%s "
                "other_chat_history=%s pending_send_notices=%s identity_count=%s",
                receipt[:12],
                len(context.turns),
                len(context.history_turns),
                len(context.recent_other_turns),
                len(pending_notices),
                len(context.identities),
            )
            command_reply = (
                await self.special_command_handler(item)
                if self.special_command_handler is not None
                else None
            )
            text = (
                command_reply
                if command_reply is not None
                else await self.reply_logged(context, receipt, "initial")
            )
            if batch is not None:
                scheduler = _batch_scheduler.get() or self.scheduler
                if not scheduler.mark_model_seen(batch):
                    logger.error(
                        "event=inbox_batch_mark_seen_failed batch=%s size=%s",
                        batch.batch_id[:12],
                        len(batch.receipt_ids),
                    )
            if cancelled_followup_notice and self.clear_cancelled_followup is not None:
                self.clear_cancelled_followup(
                    item.account_id, item.conversation_id, cancelled_followup_notice,
                )
            scheduled_followup = bool(
                self.is_followup_scheduled is not None
                and self.is_followup_scheduled(item.account_id, item.conversation_id)
            )
            if scheduled_followup:
                text = ""
            self._acknowledge_notices(tuple(str(row["scope"]) for row in pending_notices))
            if self._is_paused():
                state = "cancelled_pause"
            elif scheduled_followup:
                state = "followup_scheduled"
            elif not text:
                delivery = self.current_delivery_state(receipt)
                if delivery.get("last_send_status") in ("queued", "accepted_by_server"):
                    state = "agent_tool_send_unconfirmed"
                else:
                    state = "empty_reply"
            else:
                phase = "send"

                async def retry_reply(retry_ctx: ContextWindow, turn_label: str) -> str:
                    return await self.reply_logged(retry_ctx, receipt, turn_label)

                state = await self.deliver_auto_reply(
                    receipt,
                    item.account_id,
                    item.conversation_id,
                    text,
                    context,
                    retry_reply,
                )
        except Exception as exc:
            state = f"{phase}_unknown"
            logger.warning(
                "event=process_failed receipt=%s phase=%s error_type=%s",
                receipt[:12],
                phase,
                type(exc).__name__,
            )
            if isinstance(exc, AdkBudgetExhausted):
                state = "budget_exhausted"
                raise
            if isinstance(exc, LlmCallsLimitExceededError):
                state = "llm_calls_limit_exceeded"
                raise
            if isinstance(exc, ModelProviderUnavailable):
                state = "provider_unavailable"
                raise
            if isinstance(exc, ModelCallIndeterminate):
                state = "model_call_indeterminate"
                raise
        finally:
            turn_started_monotonic.reset(timing_token)
            turn_scope.close()
            self._record_attempt_audit(receipt, state)
            logger.info(
                "event=process_finished receipt=%s state=%s elapsed_ms=%s",
                receipt[:12],
                state,
                int((time.perf_counter() - started_at) * 1000),
            )
        return state

    async def process_batch(self, batch: MessageBatch) -> str | None:
        """Execute one claimed inbox batch under single-source-of-truth retry semantics."""
        item = batch.observations[-1]
        started = time.monotonic()
        self._in_flight = {
            "sender_sha": sha(item.sender_id),
            "message_type": item.message_type,
            "started_monotonic": started,
            "started_at": datetime.now(UTC).isoformat(),
            "batch_size": len(batch.observations),
        }
        self.publish_heartbeat()
        logger.info(
            "event=worker_batch_started batch=%s size=%s sender_sha=%s message_type=%s",
            batch.batch_id[:12],
            len(batch.observations),
            sha(item.sender_id),
            item.message_type,
        )
        prepared_token = None
        deadline_token = None
        retry_error = "model_did_not_complete"
        deferred = False
        process_state: str | None = None
        batch_token = _inbox_batch.set(batch)
        scheduler_token = _batch_scheduler.set(self.scheduler)
        receipt_ids_token = operation_receipt_ids.set(batch.receipt_ids)
        try:
            prepared = await self._assemble_observations(batch.observations)
            prepared_token = _assembled_batch.set(prepared)
            latest_time = batch.observations[-1].event_time_context
            synthetic = replace(
                item,
                text=f"合并的本轮输入包含 {len(batch.observations)} 条按时间排序的消息。",
                raw_envelope_json="",
                raw_message_json="",
                source_msg_id=None,
                event_time_context=latest_time,
            )
            deadline_token = turn_deadline_monotonic.set(
                time.monotonic() + self.process_timeout_seconds,
            )
            process_state = await asyncio.wait_for(
                self.process_single(synthetic),
                timeout=self.process_timeout_seconds,
            )
            if process_state not in ("paused", "outside_test_scope", "test_limit"):
                self.mark_provider_healthy(source="worker_turn")
        except TimeoutError:
            retry_error = "process_timeout"
            logger.error(
                "event=worker_batch_timeout batch=%s size=%s timeout_seconds=%s",
                batch.batch_id[:12],
                len(batch.observations),
                self.process_timeout_seconds,
            )
        except AdkBudgetExhausted:
            retry_after = self.scheduler.defer_until_budget_reset(batch)
            deferred = True
            retry_error = self.scheduler.BUDGET_EXHAUSTED_ERROR
            logger.error(
                "event=worker_batch_blocked batch=%s reason=model_budget_exhausted "
                "provider_called=false inbox_state=pending retry_after=%s",
                batch.batch_id[:12],
                retry_after.isoformat(),
            )
        except LlmCallsLimitExceededError:
            retry_error = "model_llm_calls_limit_exceeded"
            logger.error(
                "event=worker_batch_blocked batch=%s reason=llm_calls_limit_exceeded "
                "provider_called=true inbox_state=pending retry=true",
                batch.batch_id[:12],
            )
        except ModelProviderUnavailable:
            batch_receipt = hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
            has_side_effect = self._batch_has_side_effect(batch_receipt)
            if not has_side_effect:
                delay = self.trip_provider_circuit("worker_turn_unavailable")
                self.scheduler.defer_batch(
                    batch, "provider_unavailable", delay_seconds=delay,
                )
                deferred = True
            else:
                retry_error = "provider_unavailable_after_send"
                logger.warning(
                    "event=worker_batch_provider_unavailable_after_send batch=%s",
                    batch.batch_id[:12],
                )
        except ModelCallIndeterminate:
            cooldown, has_side_effect = await self.handle_model_indeterminate(
                batch,
                reason="worker_turn_indeterminate",
            )
            retry_error = (
                "model_indeterminate_after_send"
                if has_side_effect
                else "model_call_indeterminate"
            )
            logger.warning(
                "event=worker_batch_indeterminate batch=%s has_side_effect=%s "
                "failures=%s cooldown_seconds=%.2f",
                batch.batch_id[:12],
                has_side_effect,
                self._indeterminate_failures,
                cooldown,
            )
            self.scheduler.release_for_retry(
                batch,
                retry_error,
                min_delay_seconds=cooldown,
            )
            deferred = True
        except Exception as exc:  # noqa: BLE001
            retry_error = type(exc).__name__
            logger.warning(
                "event=worker_batch_failed batch=%s error_type=%s",
                batch.batch_id[:12],
                type(exc).__name__,
            )
        finally:
            elapsed_ms = int((time.monotonic() - started) * 1000)
            pending_after = -1
            try:
                # Partial/failed model turns always return to pending with bounded backoff;
                # already model_seen rows are unaffected.
                if not deferred:
                    self.scheduler.release_for_retry(batch, retry_error)
                pending_after = self.scheduler.pending_count()
            except Exception:
                logger.exception(
                    "event=worker_batch_cleanup_error batch=%s",
                    batch.batch_id[:12],
                )
            logger.info(
                "event=worker_batch_finished batch=%s size=%s elapsed_ms=%s pending=%s",
                batch.batch_id[:12],
                len(batch.observations),
                elapsed_ms,
                pending_after,
            )
            if prepared_token is not None:
                _assembled_batch.reset(prepared_token)
            _batch_scheduler.reset(scheduler_token)
            _inbox_batch.reset(batch_token)
            operation_receipt_ids.reset(receipt_ids_token)
            if deadline_token is not None:
                turn_deadline_monotonic.reset(deadline_token)
            self._in_flight = None
            self.publish_heartbeat()
            if self.drain_queue is not None:
                self.drain_queue()
        return process_state

    async def dispatch_to_session(
        self, target: ContextWindow | ContextSpec, task_id: str,
    ) -> str:
        """Activate a target with its pending inbox and target-local tool scope."""
        batch = None
        timing_token = turn_started_monotonic.set(time.monotonic())
        try:
            batch = self.scheduler.claim_conversation_batch(
                target.account_id, target.conversation_id,
            )
            quote_ctx: tuple[str, str, str, str] | None = None
            if isinstance(target, ContextSpec):
                spec = target
                if batch is not None:
                    if self._has_custom_assemble():
                        assert self.context_assembler is not None
                        assembled = await self.context_assembler.assemble(batch.observations)
                        window = replace(
                            assembled.context,
                            turns=(*assembled.context.turns, *spec.synthetic_turns),
                        )
                        window = fit_context_window(window, max_bytes=spec.max_user_bytes)
                        quote_ctx = assembled.quote_context
                    else:
                        ingested = await self.message_ingestor.ingest(batch.observations)
                        spec = replace(
                            spec,
                            observations=ingested.observations,
                            ingested_records=ingested.records,
                            message_times=ingested.message_times,
                        )
                        built = await self.context_builder.build(spec)
                        window = built.context
                        quote_ctx = built.quote_context
                    if self.record_activity is not None:
                        await self.record_activity(target.account_id, target.conversation_id)
                else:
                    built = await self.context_builder.build(spec)
                    window = built.context
                    quote_ctx = built.quote_context
            else:
                window = target
                if batch is not None:
                    if self._has_custom_assemble():
                        assert self.context_assembler is not None
                        assembled = await self.context_assembler.assemble(batch.observations)
                        window = replace(
                            assembled.context,
                            turns=(*assembled.context.turns, *target.turns),
                        )
                        window = fit_context_window(
                            window,
                            max_bytes=self._effective_max_user_bytes(
                                target.account_id,
                                target.conversation_id,
                            ),
                        )
                        quote_ctx = assembled.quote_context
                    else:
                        ingested = await self.message_ingestor.ingest(batch.observations)
                        built = await self.context_builder.build(
                            ContextSpec.for_cross_session(
                                account_id=target.account_id,
                                conversation_id=target.conversation_id,
                                is_group=target.is_group,
                                synthetic_turns=target.turns,
                                ingested=ingested,
                                max_user_bytes=min(
                                    24_000,
                                    self._effective_max_user_bytes(
                                        target.account_id,
                                        target.conversation_id,
                                    ),
                                ),
                            )
                        )
                        window = built.context
                        quote_ctx = built.quote_context
                    if self.record_activity is not None:
                        await self.record_activity(target.account_id, target.conversation_id)
                elif self.context_assembler is not None:
                    window = fit_context_window(
                        window,
                        max_bytes=self._effective_max_user_bytes(
                            target.account_id,
                            target.conversation_id,
                        ),
                    )
            schedule_fn = (
                self.followup_factory(window, task_id)
                if self.followup_factory is not None
                else None
            )
            with tool_turn_scope(
                window,
                task_id,
                receipt_ids=batch.receipt_ids if batch is not None else (),
                quote=quote_ctx,
                schedule=schedule_fn,
            ):
                value = await self.reply_logged(window, task_id, "cross_session")
            if batch is not None and not self.scheduler.mark_model_seen(batch):
                logger.error("event=cross_session_batch_mark_seen_failed task=%s", task_id[:12])
            return value
        finally:
            if batch is not None:
                self.scheduler.release_for_retry(batch, "cross_session_target_incomplete")
            turn_started_monotonic.reset(timing_token)

    dispatch_to_session._accepts_context_spec = True  # type: ignore[attr-defined]

    async def _heartbeat_loop(self) -> None:
        while True:
            self.publish_heartbeat()
            await asyncio.sleep(max(0.2, self.heartbeat_interval_seconds))

    async def run_worker(self) -> None:
        """Main durable worker loop with periodic heartbeat and transient error guard."""
        next_budget_wake_monotonic = 0.0
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            self.publish_heartbeat()
            while True:
                try:
                    if self._is_paused() or not self._is_wechat_ready():
                        await asyncio.sleep(0.5)
                        continue
                    now_monotonic = time.monotonic()
                    if now_monotonic >= next_budget_wake_monotonic:
                        next_budget_wake_monotonic = now_monotonic + 15.0
                        recovered = self.scheduler.wake_budget_deferred_batches(
                            now=datetime.now(UTC),
                        )
                        if recovered:
                            if self.on_budget_recovered is not None:
                                for rec_item in recovered:
                                    self.on_budget_recovered(rec_item)
                            logger.info(
                                "event=budget_reset_inbox_recovered queued=%s",
                                len(recovered),
                            )
                    if not self._provider_ready:
                        if self.scheduler.active_count() == 0:
                            await asyncio.sleep(0.5)
                            continue
                        if not await self.ensure_provider_ready():
                            await asyncio.sleep(
                                min(0.5, max(0.02, self._provider_probe_base_seconds))
                            )
                            continue
                        if not self._provider_ready:
                            # Half-Open trial without provider_check: ensure deferred rows
                            # are claimable together with any newer inbound messages.
                            self.scheduler.wake_deferred_batches()
                    cooldown_remaining = (
                        self._indeterminate_cooldown_until_monotonic - time.monotonic()
                    )
                    if cooldown_remaining > 0:
                        await asyncio.sleep(min(0.5, cooldown_remaining))
                        continue
                    blocked = (
                        self.blocked_conversations_provider()
                        if self.blocked_conversations_provider is not None
                        else None
                    )
                    batch = self.scheduler.claim_ready_batch(blocked_conversations=blocked)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("event=worker_loop_error")
                    await asyncio.sleep(1.0)
                    continue
                if batch is None:
                    await asyncio.sleep(0.25)
                    continue
                await self.process_batch(batch)
        finally:
            heartbeat_task.cancel()
            await asyncio.gather(heartbeat_task, return_exceptions=True)
