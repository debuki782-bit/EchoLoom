"""Dispatcher support components: BatchExecutionScope, circuit breaker, callback adapter."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from .channel_gateway import SendResult
from .context import ContextWindow
from .context_assembler import AssembledBatch
from .conversation_scheduler import ConversationScheduler, MessageBatch
from .memory_store import MemoryStore
from .models import ReplyDraft
from .outbound import OutboundService


def account_aliases_for_context(
    account_id: str,
    context: ContextWindow,
    account_aliases_provider: Callable[[str], Sequence[str]] | None,
) -> tuple[str, ...]:
    """Collect deduplicated self-aliases for group self-mention and freshness guards."""
    aliases: list[str] = []
    if account_aliases_provider is not None:
        aliases.extend(
            alias.strip()
            for alias in account_aliases_provider(account_id)
            if isinstance(alias, str) and alias.strip()
        )
    identity = context.identities.get(account_id)
    if identity is not None:
        for field_val in (identity.display_name, identity.nickname, identity.remark):
            if isinstance(field_val, str) and field_val.strip():
                aliases.append(field_val.strip())
    return tuple(dict.fromkeys(aliases))


def record_attempt_state(db, receipt: str, state: str) -> None:
    """Persist attempt outcome state into pilot-receipts attempts ledger."""
    with db:
        db.execute(
            "INSERT INTO attempts (id,state) VALUES (?,?) "
            "ON CONFLICT(id) DO UPDATE SET state=excluded.state",
            (receipt, state),
        )


def allow_single_attempt(
    db,
    receipt: str,
    *,
    is_batch: bool,
    completed_states: frozenset[str],
) -> bool:
    """Return True if execution should proceed; attempts never blocks inbox batches."""
    if is_batch:
        record_attempt_state(db, receipt, "started")
        return True
    existing = db.execute("SELECT state FROM attempts WHERE id=?", (receipt,)).fetchone()
    if existing is not None and str(existing[0]) in completed_states:
        logger.info("event=process_duplicate receipt=%s", receipt[:12])
        return False
    record_attempt_state(db, receipt, "started")
    return True


def attempt_limit_reached(db, limit: int | None) -> bool:
    """Return True when cumulative live attempts reached configured smoke limit."""
    if limit is None:
        return False
    count = db.execute(
        "SELECT count(*) FROM attempts WHERE state NOT IN ('provider_unavailable','budget_exhausted')",
    ).fetchone()[0]
    return int(count) >= limit


async def deliver_via_callbacks(
    *,
    send,
    delivery_state,
    record_send_result,
    record_delivery_notice,
    memory_store,
    scope: str,
    account_id: str,
    conversation_id: str,
    text: str,
    context: ContextWindow | None,
    retry_reply_fn,
    archive_operation_id: str | None,
) -> str:
    """Fallback delivery path for Dispatcher instances constructed with plain callbacks."""
    adapter = CallbackOutboundAdapter(
        send=send,
        delivery_state=delivery_state,
        record_send_result=record_send_result,
        record_delivery_notice=record_delivery_notice,
        memory_store=memory_store,
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

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BatchExecutionScope:
    """Turn-local durable inbox batch metadata passed from worker/trigger to process_single."""

    batch: MessageBatch | None = None
    assembled: AssembledBatch | None = None
    scheduler: ConversationScheduler | None = None


_batch_scope: ContextVar[BatchExecutionScope | None] = ContextVar(
    "echoloom_batch_execution_scope",
    default=None,
)


def current_batch_scope() -> BatchExecutionScope:
    """Return active batch execution scope or an empty default."""
    return _batch_scope.get() or BatchExecutionScope()


@contextmanager
def batch_execution_scope(
    *,
    batch: MessageBatch | None = None,
    assembled: AssembledBatch | None = None,
    scheduler: ConversationScheduler | None = None,
):
    """Bind durable batch state for the duration of one execution pass."""
    token = _batch_scope.set(
        BatchExecutionScope(batch=batch, assembled=assembled, scheduler=scheduler)
    )
    try:
        yield
    finally:
        _batch_scope.reset(token)


class ProviderCircuitBreaker:
    """Exponential-backoff circuit breaker and serialized probe gate for upstream models."""

    def __init__(
        self,
        *,
        provider_check: Callable[[], Awaitable[bool]] | None = None,
        base_seconds: float = 5.0,
        max_seconds: float = 30.0,
        on_healthy: Callable[[], None] | None = None,
    ) -> None:
        self.provider_check = provider_check
        self.base_seconds = float(base_seconds)
        self.max_seconds = float(max_seconds)
        self._on_healthy = on_healthy
        self.ready = True
        self.failures = 0
        self.next_probe_monotonic = 0.0
        self._probe_lock = asyncio.Lock()

    def next_backoff(self) -> float:
        return min(
            self.max_seconds,
            self.base_seconds * (2 ** min(max(0, self.failures - 1), 4)),
        )

    def trip(self, reason: str) -> float:
        self.failures += 1
        self.ready = False
        delay = self.next_backoff()
        self.next_probe_monotonic = time.monotonic() + delay
        logger.warning(
            "event=provider_circuit_opened reason=%s failures=%s retry_in_seconds=%.2f",
            reason,
            self.failures,
            delay,
        )
        return delay

    def mark_healthy(self, *, source: str) -> None:
        if not self.ready or self.failures > 0:
            if not self.ready:
                logger.info("event=provider_circuit_closed source=%s", source)
            self.ready = True
            self.failures = 0
            self.next_probe_monotonic = 0.0
            if self._on_healthy is not None:
                self._on_healthy()

    async def ensure_ready(self) -> bool:
        if self.ready:
            return True
        if time.monotonic() < self.next_probe_monotonic:
            return False
        if self.provider_check is None:
            return True
        async with self._probe_lock:
            if self.ready:
                return True
            if time.monotonic() < self.next_probe_monotonic:
                return False
            try:
                ok = bool(await self.provider_check())
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                ok = False
            if ok:
                self.mark_healthy(source="probe")
                return True
            self.failures += 1
            delay = self.next_backoff()
            self.next_probe_monotonic = time.monotonic() + delay
            logger.warning(
                "event=provider_probe_failed failures=%s retry_in_seconds=%.2f",
                self.failures,
                delay,
            )
            return False


class CallbackOutboundAdapter:
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
