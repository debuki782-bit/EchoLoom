"""Authorized account Pilot facade composing Ingress, Dispatcher, and Triggers."""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI

from .contacts import ContactIdentity
from .context import ContextWindow
from .context_assembler import MessageContextAssembler
from .context_builder import ContextBuilder, ContextSpec
from .conversation_scheduler import ConversationScheduler
from .dispatcher import Dispatcher, current_message_id
from .ingress import Ingress, create_ingress_app, read_attempt_counts, sha
from .media import WeChatFileIngestor, WeChatImageIngestor, WeChatVoiceIngestor
from .memory_store import MemoryStore
from .message_ingestor import MessageIngestor
from .models import ReplyDraft
from .outbound import OutboundService
from .temporal_context import DailyEnvironmentProvider
from .time_scheduler import TimeScheduler
from .triggers import FollowupTrigger, IdleEventTrigger, Triggers
from .wechat_gateway import (
    MessageEvent,
    SendResult,
    default_archive_envelope,
    default_event_parser,
)

logger = logging.getLogger(__name__)
_current_message_id = current_message_id


class _ForwardAttr:
    """Descriptor forwarding attribute access along a dotted component path."""

    def __init__(self, *path: str, readonly: bool = False, class_default=None) -> None:
        self._path = path
        self._readonly = readonly
        self._class_default = class_default

    def _target(self, obj: Pilot):
        cur: object = obj
        for part in self._path[:-1]:
            cur = getattr(cur, part)
        return cur

    def __get__(self, obj: Pilot | None, owner=None):
        if obj is None:
            return self._class_default
        return getattr(self._target(obj), self._path[-1])

    def __set__(self, obj: Pilot, value) -> None:
        if self._readonly:
            raise AttributeError(f"attribute {self._path[-1]!r} is read-only")
        setattr(self._target(obj), self._path[-1], value)


class _ProcessTimeoutDescriptor:
    def __get__(self, obj: Pilot | None, owner=None) -> float:
        if obj is None:
            return Dispatcher.PROCESS_TIMEOUT_SECONDS
        return obj.dispatcher.process_timeout_seconds

    def __set__(self, obj: Pilot, value: float) -> None:
        timeout = float(value)
        obj.dispatcher.process_timeout_seconds = timeout
        obj.ingress.process_timeout_seconds = timeout


class Pilot:
    """In-process compatibility facade composing Ingress + Dispatcher + Triggers."""

    PROCESS_TIMEOUT_SECONDS = _ProcessTimeoutDescriptor()
    FOLLOWUP_MERGE_WINDOW_SECONDS = _ForwardAttr(
        "triggers", "followups", "merge_window_seconds",
        class_default=FollowupTrigger.DEFAULT_MERGE_WINDOW_SECONDS,
    )
    _next_idle_delay_seconds = staticmethod(IdleEventTrigger.next_idle_delay_seconds)

    # Forwarded compatibility properties (read/write where tests mutate them).
    paused = _ForwardAttr("ingress", "paused", readonly=True)
    registered_group_ids = _ForwardAttr("ingress", "registered_group_ids", readonly=True)
    parse_events = _ForwardAttr("ingress", "parse_events", readonly=True)
    received = _ForwardAttr("ingress", "received", readonly=True)
    events = _ForwardAttr("ingress", "events", readonly=True)

    provider_ready = _ForwardAttr("dispatcher", "provider_ready", readonly=True)
    has_pending_work = _ForwardAttr("dispatcher", "has_pending_work", readonly=True)
    reply = _ForwardAttr("dispatcher", "reply")
    send = _ForwardAttr("dispatcher", "send")
    context_assembler = _ForwardAttr("dispatcher", "context_assembler")
    message_ingestor = _ForwardAttr("dispatcher", "message_ingestor")
    context_builder = _ForwardAttr("dispatcher", "context_builder")
    memory_store = _ForwardAttr("dispatcher", "memory_store")
    _in_flight = _ForwardAttr("dispatcher", "_in_flight")

    wechat_ready = _ForwardAttr("triggers", "login_watcher", "wechat_ready", readonly=True)
    _wechat_ready = _ForwardAttr("triggers", "login_watcher", "wechat_ready")
    _login_ready_monotonic = _ForwardAttr("triggers", "login_watcher", "login_ready_monotonic")
    login_check = _ForwardAttr("triggers", "login_watcher", "login_check")
    time_scheduler = _ForwardAttr("triggers", "time_scheduler")
    idle_event_handler = _ForwardAttr("triggers", "idle_events", "idle_event_handler")
    session_maintenance = _ForwardAttr("triggers", "session_maintenance", "session_maintenance")
    _scheduled_followups = _ForwardAttr(
        "triggers", "followups", "scheduled_followups", readonly=True,
    )

    def __init__(
        self,
        path: Path,
        account: str,
        room: str,
        reply: Callable[[ContextWindow], Awaitable[str]],
        send: Callable[[ReplyDraft], SendResult],
        *,
        limit: int | None = 6,
        mode: str = "authorized_test_group_pilot",
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
        session_maintenance: Callable[[], Awaitable[int]] | None = None,
        context_assembler: MessageContextAssembler | None = None,
        message_ingestor: MessageIngestor | None = None,
        context_builder: ContextBuilder | None = None,
        max_user_bytes: int = 32_000,
        max_user_bytes_for_conversation: Callable[[str, str], int] | None = None,
        login_check: Callable[[], Awaitable[bool]] | None = None,
        initialize_after_login: Callable[[], Awaitable[None]] | None = None,
        registered_group_ids: set[str] | None = None,
        admit_group: Callable[[str], bool] | None = None,
        special_command_handler: Callable[[MessageEvent], Awaitable[str | None]] | None = None,
        parse_events: Callable[[object], tuple[MessageEvent, ...]] = default_event_parser,
        normalize_event: Callable[[MessageEvent], MessageEvent | None] | None = None,
        in_scope_check: Callable[[MessageEvent], bool] | None = None,
        registry=None,
        archive_envelope=default_archive_envelope,
        provider_check: Callable[[], Awaitable[bool]] | None = None,
        provider_probe_base_seconds: float = 5.0,
        provider_probe_max_seconds: float = 30.0,
        on_model_indeterminate: Callable[[], Awaitable[None] | None] | None = None,
        indeterminate_cooldown_base_seconds: float = 30.0,
        indeterminate_cooldown_max_seconds: float = 600.0,
        outbound: OutboundService | None = None,
    ) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY,state TEXT);"
            "CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY,paused INTEGER);"
            "INSERT OR IGNORE INTO control VALUES (1,0);"
        )
        # A crash after claiming may have submitted to the model or Hook.
        self.db.execute("UPDATE attempts SET state='unknown' WHERE state='started'")
        self.db.commit()

        self.account = account
        self.room = room
        self.limit = limit
        self.mode = mode
        self.scheduler = ConversationScheduler(self.db, recover_running=True)
        self.queue: asyncio.Queue[MessageEvent] = asyncio.Queue(maxsize=16)
        self._worker_task: asyncio.Task | None = None
        self._login_watch_task: asyncio.Task | None = None

        self.ingress = Ingress(
            self.db, account, room,
            scheduler=self.scheduler, mode=mode, limit=limit,
            process_timeout_seconds=Dispatcher.PROCESS_TIMEOUT_SECONDS,
            registered_group_ids=registered_group_ids, admit_group=admit_group,
            parse_events=parse_events, normalize_event=normalize_event,
            in_scope_check=in_scope_check, registry=registry,
            queue_depth_provider=self.queue.qsize,
        )
        self.dispatcher = Dispatcher(
            self.db, self.scheduler, account, room, reply, send,
            outbound=outbound, limit=limit, resolve_contact=resolve_contact,
            memory_store=memory_store, image_ingestor=image_ingestor,
            voice_ingestor=voice_ingestor, file_ingestor=file_ingestor,
            daily_environment=daily_environment,
            record_activity=record_activity, delivery_state=delivery_state,
            record_send_result=record_send_result,
            pending_delivery_notices=pending_delivery_notices,
            acknowledge_delivery_notices=acknowledge_delivery_notices,
            record_delivery_notice=record_delivery_notice,
            context_assembler=context_assembler,
            message_ingestor=message_ingestor,
            context_builder=context_builder,
            max_user_bytes=max_user_bytes,
            max_user_bytes_for_conversation=max_user_bytes_for_conversation,
            special_command_handler=special_command_handler,
            archive_envelope=archive_envelope, provider_check=provider_check,
            provider_probe_base_seconds=provider_probe_base_seconds,
            provider_probe_max_seconds=provider_probe_max_seconds,
            on_model_indeterminate=on_model_indeterminate,
            indeterminate_cooldown_base_seconds=indeterminate_cooldown_base_seconds,
            indeterminate_cooldown_max_seconds=indeterminate_cooldown_max_seconds,
            in_scope=self.ingress.in_scope,
            is_paused=lambda: self.ingress.paused,
            is_wechat_ready=lambda: self.triggers.login_watcher.wechat_ready,
            drain_queue=self._drain_queue,
        )
        self.triggers = Triggers(
            account, self.dispatcher,
            is_paused=lambda: self.ingress.paused,
            login_check=login_check, initialize_after_login=initialize_after_login,
            session_maintenance=session_maintenance,
            queue_size_provider=self.queue.qsize,
            on_login_state_change=self.dispatcher.publish_heartbeat,
        )

        self.ingress.on_queued = self._on_inbound_queued
        self.ingress.on_paused = lambda _item: self.triggers.on_paused()
        self.ingress.on_inbound_followup = (
            lambda item: self.triggers.followups.notify_inbound(
                item.account_id, item.conversation_id,
            )
        )
        self.dispatcher.on_budget_recovered = self.triggers.record_account_inbound
        self.dispatcher.blocked_conversations_provider = (
            self.triggers.followups.blocked_conversations
        )
        self.dispatcher.is_followup_scheduled = self.triggers.followups.is_scheduled
        self.dispatcher.get_cancelled_followup = self.triggers.followups.get_cancelled_notice
        self.dispatcher.clear_cancelled_followup = self.triggers.followups.clear_cancelled_notice
        self.dispatcher.followup_factory = self.triggers.followups.followup_for
        self.dispatcher.scheduled_followups_count = self.triggers.followups.active_count

        logger.info(
            "event=pilot_started mode=%s account_sha=%s conversation_sha=%s "
            "paused=%s queue_capacity=%s retry_limit=%s",
            self.mode, sha(self.account), sha(self.room), self.paused,
            self.scheduler.MAX_PENDING_MESSAGES, self.scheduler.MAX_PROCESS_ATTEMPTS,
        )

    def _on_inbound_queued(self, item: MessageEvent) -> None:
        try:
            self.queue.put_nowait(item)
        except asyncio.QueueFull:
            pass
        self.triggers.record_account_inbound(item)

    def _drain_queue(self) -> None:
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
                self.queue.task_done()
            except asyncio.QueueEmpty:
                break

    def offer(self, item: MessageEvent) -> str:
        return self.ingress.offer(item)

    async def process(self, item: MessageEvent) -> str:
        return await self.dispatcher.process_single(item)

    async def worker(self) -> None:
        await self.dispatcher.run_worker()

    async def dispatch_to_session(
        self, window: ContextWindow | ContextSpec, task_id: str,
    ) -> str:
        return await self.dispatcher.dispatch_to_session(window, task_id)

    dispatch_to_session._accepts_context_spec = True  # type: ignore[attr-defined]

    async def login_watch_worker(self) -> None:
        await self.triggers.login_watcher.run()

    async def session_maintenance_worker(self) -> None:
        await self.triggers.session_maintenance.run()

    async def idle_event_worker(self) -> None:
        await self.triggers.idle_events.run()

    def configure_idle_events(
        self,
        scheduler: TimeScheduler,
        handler: Callable[[str, int, str], Awaitable[None]],
    ) -> None:
        self.triggers.configure_idle_events(scheduler, handler)

    def recover_scheduled_followups(self) -> int:
        return self.triggers.followups.recover_scheduled_followups()

    async def _schedule_followup(
        self, context: ContextWindow, parent_receipt: str, task_description: str, wait_seconds: int,
    ) -> dict[str, object]:
        return await self.triggers.followups.schedule_followup(
            context, parent_receipt, task_description, wait_seconds,
        )

    def _record_account_inbound(self, item: MessageEvent) -> None:
        self.triggers.record_account_inbound(item)

    def _cancel_idle_task(self, reason: str) -> None:
        self.triggers.idle_events.cancel(reason)

    def _cancel_maintenance_task(self) -> None:
        self.triggers.session_maintenance.cancel()

    def _cancel_followup(self, key: tuple[str, str], reason: str) -> None:
        self.triggers.followups.cancel(key, reason)

    def _cancel_all_followups(self) -> None:
        self.triggers.followups.cancel_all()

    def recover_archived_backlog(self, *, max_age_hours: int = 24) -> dict[str, int]:
        return self.ingress.recover_archived_backlog(
            self.memory_store,
            on_existing_active=self.triggers.record_account_inbound,
            max_age_hours=max_age_hours,
        )

    def recover_failed_after_login(self, *, include_legacy_failed: bool = False) -> dict[str, int]:
        """Wake due pending budget-deferred batches; never auto-replay legacy failed rows."""
        items: list[MessageEvent] = []
        if include_legacy_failed:
            items.extend(
                self.scheduler.requeue_failed_after_restart(
                    {
                        "previous_send_attempt_turn_incomplete_no_replay",
                        "model_llm_calls_limit_exceeded",
                    }
                )
            )
        budget_items = self.scheduler.wake_budget_deferred_batches(now=datetime.now(UTC))
        items.extend(budget_items)
        for item in items:
            self._on_inbound_queued(item)
        logger.info(
            "event=failed_inbox_login_recovery queued=%s budget_reset_queued=%s "
            "include_legacy_failed=%s conversation_sha=%s",
            len(items), len(budget_items), include_legacy_failed, sha(self.room),
        )
        return {"queued": len(items), "budget_reset_queued": len(budget_items)}

    def recover_latest_hook_status_message(self, status: object) -> str:
        from .connectors.base import recover_status_event

        item = recover_status_event(
            status, account_id=self.account, conversation_id=self.room,
        )
        return self.recover_latest_event(item)

    def recover_latest_event(self, item: MessageEvent | None) -> str:
        if item is None:
            logger.info("event=hook_status_recovery_skipped reason=no_valid_latest_message")
            return "no_valid_latest_message"
        result = self.offer(item)
        logger.info(
            "event=hook_status_recovery result=%s room_sha=%s sender_sha=%s",
            result, sha(item.conversation_id), sha(item.sender_id),
        )
        return result

    def status(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "tool_control_version": 1,
            "trusted_gateway": bool(self.login_check is not None and self.wechat_ready),
            "provider_ready": self.provider_ready,
            "worker_alive": None if self._worker_task is None else not self._worker_task.done(),
            "login_watch_alive": (
                None if self._login_watch_task is None else not self._login_watch_task.done()
            ),
            "paused": self.paused,
            "received": self.received,
            "last_events": list(self.events),
            "queue": self.scheduler.active_count(),
            "queue_including_failed": self.scheduler.pending_count(),
            "scheduled_followups": self.triggers.followups.active_count(),
            "in_flight": self.dispatcher.in_flight_snapshot(),
            "inbox_states": self.scheduler.state_counts(),
            "inbox_failures": self.scheduler.failure_counts(),
            "deferred_counts": self.scheduler.deferred_counts(),
            "queue_capacity": self.scheduler.MAX_PENDING_MESSAGES,
            "model_budget_blocked_batches": self.scheduler.budget_blocked_count(),
            "process_timeout_seconds": self.PROCESS_TIMEOUT_SECONDS,
            "limit": self.limit,
            "attempts": read_attempt_counts(self.db),
        }


def create_pilot_app(
    pilot: Pilot,
    *,
    close_episode_memory: Callable[[], Awaitable[None]] | None = None,
    close_gateway: Callable[[], None] | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        pilot.recover_scheduled_followups()
        task = asyncio.create_task(pilot.worker())
        login_watch = asyncio.create_task(pilot.login_watch_worker())
        pilot._worker_task = task
        pilot._login_watch_task = login_watch
        maintenance = (
            asyncio.create_task(pilot.session_maintenance_worker())
            if pilot.session_maintenance is not None
            else None
        )
        idle_worker = (
            asyncio.create_task(pilot.idle_event_worker())
            if pilot.time_scheduler is not None
            else None
        )
        try:
            yield
        finally:
            pilot._worker_task = None
            pilot._login_watch_task = None
            for bg in (login_watch, maintenance):
                if bg is not None:
                    bg.cancel()
                    await asyncio.gather(bg, return_exceptions=True)
            if idle_worker is not None:
                pilot._cancel_idle_task("pilot_shutdown")
                idle_worker.cancel()
                await asyncio.gather(idle_worker, return_exceptions=True)
            followups = list(pilot._scheduled_followups.values())
            pilot._cancel_all_followups()
            if followups:
                await asyncio.gather(*followups, return_exceptions=True)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            if close_episode_memory is not None:
                await close_episode_memory()
            if close_gateway is not None:
                await asyncio.to_thread(close_gateway)

    return create_ingress_app(pilot.ingress, status_provider=pilot.status, lifespan=lifespan)
