"""Worker dispatcher: single-source-of-truth inbox execution, circuit breaker, and delivery."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import ExitStack
from dataclasses import replace
from datetime import UTC, datetime

from .contacts import ContactIdentity
from .context import ContextWindow, fit_context_window
from .context_assembler import AssembledBatch, MessageContextAssembler
from .context_builder import ContextBuilder, ContextSpec
from .conversation_scheduler import ConversationScheduler, MessageBatch
from .db_async import is_db_thread_safe, run_db
from .dispatcher_support import (
    ProviderCircuitBreaker,
    account_aliases_for_context,
    allow_single_attempt,
    attempt_limit_reached,
    batch_execution_scope,
    current_batch_scope,
    deliver_via_callbacks,
    record_attempt_state,
)
from .freshness_policy import FreshnessDecision, evaluate_outbound_freshness
from .ingress import sha
from .media import WeChatImageIngestor, WeChatVoiceIngestor
from .memory_store import MemoryStore
from .message_ingestor import MessageIngestor
from .models import ReplyDraft
from .outbound import OutboundService
from .runtime_contract import BudgetExhausted as AdkBudgetExhausted
from .runtime_contract import ModelCallsLimitExceeded as LlmCallsLimitExceededError
from .runtime_contract import ModelProviderUnavailable
from .temporal_context import DailyEnvironmentProvider
from .tool_timing import turn_deadline_monotonic, turn_started_monotonic
from .turn_scope import operation_receipt_ids, tool_turn_scope
from .wechat_gateway import MessageEvent, SendResult, default_archive_envelope

logger = logging.getLogger(__name__)

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
        max_freshness_holds: int = 1,
        account_aliases_provider: Callable[[str], Sequence[str]] | None = None,
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
            archive_envelope=archive_envelope,
        )
        self._default_builder = ContextBuilder(
            memory_store,
            resolve_contact=resolve_contact,
            daily_environment=daily_environment,
        )
        self.special_command_handler = special_command_handler
        self.archive_envelope = archive_envelope
        self._circuit = ProviderCircuitBreaker(
            provider_check=provider_check,
            base_seconds=provider_probe_base_seconds,
            max_seconds=provider_probe_max_seconds,
            on_healthy=self.scheduler.wake_deferred_batches,
        )
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
        self.max_freshness_holds = max(0, int(max_freshness_holds))
        self.account_aliases_provider = account_aliases_provider
        self._in_flight: dict[str, object] | None = None

    async def _db(self, fn, *args, **kwargs):
        return await run_db(fn, *args, db=self.db, **kwargs)

    async def _outbound_db(self, fn, *args, **kwargs):
        target_db = self.outbound.store.db if self.outbound is not None else self.db
        return await run_db(fn, *args, db=target_db, **kwargs)

    def _batch_has_side_effect(self, batch: MessageBatch) -> bool:
        batch_receipt = hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
        delivery = self.current_delivery_state(batch_receipt)
        return (
            int(delivery.get("send_attempt_count", 0) or 0) > 0
            or delivery.get("last_send_status")
            in ("queued", "accepted_by_server", "unknown")
        )

    @property
    def provider_check(self):
        return self._circuit.provider_check

    @provider_check.setter
    def provider_check(self, value) -> None:
        self._circuit.provider_check = value

    @property
    def _provider_ready(self) -> bool:
        return self._circuit.ready

    @_provider_ready.setter
    def _provider_ready(self, value: bool) -> None:
        self._circuit.ready = bool(value)

    @property
    def _provider_failures(self) -> int:
        return self._circuit.failures

    @_provider_failures.setter
    def _provider_failures(self, value: int) -> None:
        self._circuit.failures = int(value)

    @property
    def _provider_next_probe_monotonic(self) -> float:
        return self._circuit.next_probe_monotonic

    @_provider_next_probe_monotonic.setter
    def _provider_next_probe_monotonic(self, value: float) -> None:
        self._circuit.next_probe_monotonic = float(value)

    @property
    def provider_ready(self) -> bool:
        return self._circuit.ready

    @property
    def provider_probe_base_seconds(self) -> float:
        return self._circuit.base_seconds

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
        return self._circuit.next_backoff()

    def trip_provider_circuit(self, reason: str) -> float:
        return self._circuit.trip(reason)

    def mark_provider_healthy(self, *, source: str) -> None:
        self._circuit.mark_healthy(source=source)

    async def ensure_provider_ready(self) -> bool:
        return await self._circuit.ensure_ready()

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
        return await deliver_via_callbacks(
            send=self.send,
            delivery_state=self.delivery_state,
            record_send_result=self.record_send_result,
            record_delivery_notice=self.record_delivery_notice,
            memory_store=self.memory_store,
            scope=scope,
            account_id=account_id,
            conversation_id=conversation_id,
            text=text,
            context=context,
            retry_reply_fn=retry_reply_fn,
            archive_operation_id=archive_operation_id,
        )

    def _record_attempt_audit(self, receipt: str, state: str) -> None:
        record_attempt_state(self.db, receipt, state)

    def _check_single_attempt_gate(self, receipt: str, *, is_batch: bool) -> bool:
        """Return True if execution should proceed; attempts NEVER blocks inbox batches."""
        return allow_single_attempt(
            self.db, receipt, is_batch=is_batch, completed_states=COMPLETED_ATTEMPT_STATES,
        )

    def _test_limit_reached(self) -> bool:
        return attempt_limit_reached(self.db, self.limit)

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
        if await self._db(self._test_limit_reached):
            return "test_limit"
        batch_scope = current_batch_scope()
        batch = batch_scope.batch
        receipt = (
            hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
            if batch is not None
            else hashlib.sha256(
                f"{item.account_id}\0{item.conversation_id}\0{item.sender_id}\0"
                f"{item.capture_tick_ms}\0{item.source_msg_id}".encode()
            ).hexdigest()
        )
        if not await self._db(self._check_single_attempt_gate, receipt, is_batch=batch is not None):
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
        prepared = batch_scope.assembled
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
            held_flag = [False]
            freshness_fn: Callable[[], FreshnessDecision] | None = None
            if batch is not None and self.max_freshness_holds > 0:
                active_batch = batch
                prior_holds = await self._db(self.scheduler.batch_freshness_holds, active_batch)
                batch_senders = tuple(obs.sender_id for obs in active_batch.observations)
                account_aliases = account_aliases_for_context(
                    item.account_id, context, self.account_aliases_provider,
                )

                def _check_batch_freshness() -> FreshnessDecision:
                    with self.scheduler._lock:
                        pending = self.scheduler.pending_inbound_snapshots(
                            item.account_id,
                            item.conversation_id,
                            exclude_receipt_ids=active_batch.receipt_ids,
                        )
                    return evaluate_outbound_freshness(
                        account_id=item.account_id,
                        is_group=context.is_group,
                        batch_sender_ids=batch_senders,
                        pending_inbound=pending,
                        prior_holds=prior_holds,
                        max_holds=self.max_freshness_holds,
                        account_aliases=account_aliases,
                    )

                freshness_fn = _check_batch_freshness
            turn_scope.enter_context(
                tool_turn_scope(
                    context,
                    receipt,
                    receipt_ids=batch.receipt_ids if batch is not None else (),
                    quote=quote_source,
                    schedule=schedule_fn,
                    freshness=freshness_fn,
                    freshness_held=held_flag,
                    freshness_thread_safe=is_db_thread_safe(self.db),
                )
            )
            pending_notices = await self._outbound_db(self._pending_notices)
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
            scheduled_followup = bool(
                self.is_followup_scheduled is not None
                and self.is_followup_scheduled(item.account_id, item.conversation_id)
            )
            if (
                not held_flag[0]
                and command_reply is None
                and text
                and not scheduled_followup
                and not self._is_paused()
                and freshness_fn is not None
                and freshness_fn().kind == "hold"
            ):
                held_flag[0] = True
            if held_flag[0]:
                state = "freshness_held"
                return state
            if batch is not None:
                scheduler = current_batch_scope().scheduler or self.scheduler
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
            if scheduled_followup:
                text = ""
            if pending_notices:
                await self._outbound_db(
                    self._acknowledge_notices, tuple(str(row["scope"]) for row in pending_notices),
                )
            if self._is_paused():
                state = "cancelled_pause"
            elif scheduled_followup:
                state = "followup_scheduled"
            elif not text:
                delivery = await self._outbound_db(self.current_delivery_state, receipt)
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
        finally:
            turn_started_monotonic.reset(timing_token)
            turn_scope.close()
            await self._db(self._record_attempt_audit, receipt, state)
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
        await self._db(self.publish_heartbeat)
        logger.info(
            "event=worker_batch_started batch=%s size=%s sender_sha=%s message_type=%s",
            batch.batch_id[:12],
            len(batch.observations),
            sha(item.sender_id),
            item.message_type,
        )
        deadline_token = None
        retry_error = "model_did_not_complete"
        deferred = False
        process_state: str | None = None
        receipt_ids_token = operation_receipt_ids.set(batch.receipt_ids)
        try:
            prepared = await self._assemble_observations(batch.observations)
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
            with batch_execution_scope(
                batch=batch,
                assembled=prepared,
                scheduler=self.scheduler,
            ):
                process_state = await asyncio.wait_for(
                    self.process_single(synthetic),
                    timeout=self.process_timeout_seconds,
                )
            if process_state == "freshness_held" and not await self._db(
                self._batch_has_side_effect, batch
            ):
                hold_count = await self._db(self.scheduler.batch_freshness_holds, batch) + 1
                await self._db(
                    self.scheduler.defer_batch, batch, f"freshness_held:{hold_count}", delay_seconds=0.0,
                )
                deferred = True
                logger.info(
                    "event=worker_batch_freshness_held batch=%s size=%s hold_count=%s",
                    batch.batch_id[:12],
                    len(batch.observations),
                    hold_count,
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
            retry_after = await self._db(self.scheduler.defer_until_budget_reset, batch)
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
            if not await self._db(self._batch_has_side_effect, batch):
                delay = self.trip_provider_circuit("worker_turn_unavailable")
                await self._db(
                    self.scheduler.defer_batch, batch, "provider_unavailable", delay_seconds=delay,
                )
                deferred = True
            else:
                retry_error = "provider_unavailable_after_send"
                logger.warning(
                    "event=worker_batch_provider_unavailable_after_send batch=%s",
                    batch.batch_id[:12],
                )
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
                    await self._db(self.scheduler.release_for_retry, batch, retry_error)
                pending_after = await self._db(self.scheduler.pending_count)
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
            operation_receipt_ids.reset(receipt_ids_token)
            if deadline_token is not None:
                turn_deadline_monotonic.reset(deadline_token)
            self._in_flight = None
            await self._db(self.publish_heartbeat)
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
            batch = await self._db(
                self.scheduler.claim_conversation_batch, target.account_id, target.conversation_id,
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
            if batch is not None and not await self._db(self.scheduler.mark_model_seen, batch):
                logger.error("event=cross_session_batch_mark_seen_failed task=%s", task_id[:12])
            return value
        finally:
            if batch is not None:
                await self._db(
                    self.scheduler.release_for_retry, batch, "cross_session_target_incomplete",
                )
            turn_started_monotonic.reset(timing_token)

    dispatch_to_session._accepts_context_spec = True  # type: ignore[attr-defined]

    async def _heartbeat_loop(self) -> None:
        while True:
            await self._db(self.publish_heartbeat)
            await asyncio.sleep(max(0.2, self.heartbeat_interval_seconds))

    async def run_worker(self) -> None:
        """Main durable worker loop with periodic heartbeat and transient error guard."""
        next_budget_wake_monotonic = 0.0
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        try:
            await self._db(self.publish_heartbeat)
            while True:
                try:
                    if self._is_paused() or not self._is_wechat_ready():
                        await asyncio.sleep(0.5)
                        continue
                    now_monotonic = time.monotonic()
                    if now_monotonic >= next_budget_wake_monotonic:
                        next_budget_wake_monotonic = now_monotonic + 15.0
                        recovered = await self._db(
                            self.scheduler.wake_budget_deferred_batches, now=datetime.now(UTC),
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
                        if await self._db(self.scheduler.active_count) == 0:
                            await asyncio.sleep(0.5)
                            continue
                        if not await self.ensure_provider_ready():
                            await asyncio.sleep(
                                min(0.5, max(0.02, self.provider_probe_base_seconds))
                            )
                            continue
                        if not self._provider_ready:
                            # Half-Open trial without provider_check: ensure deferred rows
                            # are claimable together with any newer inbound messages.
                            await self._db(self.scheduler.wake_deferred_batches)
                    blocked = (
                        self.blocked_conversations_provider()
                        if self.blocked_conversations_provider is not None
                        else None
                    )
                    batch = await self._db(
                        self.scheduler.claim_ready_batch, blocked_conversations=blocked,
                    )
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
