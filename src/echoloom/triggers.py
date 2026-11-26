"""Decoupled background triggers: login watch, session maintenance, idle events, followups."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta

from .contacts import is_group_conversation
from .context import ContextWindow, build_hook_context
from .context_assembler import AssembledBatch
from .conversation_scheduler import ConversationScheduler
from .dispatcher import (
    COMPLETED_ATTEMPT_STATES,
    Dispatcher,
)
from .dispatcher_support import batch_execution_scope
from .ingress import sha
from .runtime_contract import ModelProviderUnavailable
from .time_scheduler import TimeScheduler
from .tool_timing import turn_deadline_monotonic, turn_started_monotonic
from .turn_scope import tool_turn_scope
from .wechat_gateway import MessageEvent

logger = logging.getLogger(__name__)


class LoginWatcher:
    """Polls Hook login readiness and executes one-time post-login initialization."""

    def __init__(
        self,
        scheduler: ConversationScheduler,
        *,
        login_check: Callable[[], Awaitable[bool]] | None = None,
        initialize_after_login: Callable[[], Awaitable[None]] | None = None,
        on_state_change: Callable[[], None] | None = None,
        poll_interval_seconds: float = 2.0,
    ) -> None:
        self.scheduler = scheduler
        self.login_check = login_check
        self.initialize_after_login = initialize_after_login
        self.on_state_change = on_state_change
        self.poll_interval_seconds = float(poll_interval_seconds)
        self._wechat_ready = login_check is None
        self._login_ready_monotonic: float | None = (
            time.monotonic() if self._wechat_ready else None
        )

    @property
    def wechat_ready(self) -> bool:
        return self._wechat_ready

    @wechat_ready.setter
    def wechat_ready(self, value: bool) -> None:
        self._wechat_ready = bool(value)

    @property
    def login_ready_monotonic(self) -> float | None:
        return self._login_ready_monotonic

    @login_ready_monotonic.setter
    def login_ready_monotonic(self, value: float | None) -> None:
        self._login_ready_monotonic = value

    async def run(self) -> None:
        if self.login_check is None:
            self._wechat_ready = True
            return
        while True:
            try:
                connected = await self.login_check()
                if connected and not self._wechat_ready:
                    try:
                        if self.initialize_after_login is not None:
                            await self.initialize_after_login()
                        self._wechat_ready = True
                        self._login_ready_monotonic = time.monotonic()
                        if self.on_state_change is not None:
                            self.on_state_change()
                        logger.info(
                            "event=wechat_login_initialization_completed pending=%s",
                            self.scheduler.pending_count(),
                        )
                    except Exception as exc:  # noqa: BLE001
                        logger.warning(
                            "event=wechat_login_initialization_failed error_type=%s",
                            type(exc).__name__,
                        )
                elif not connected and self._wechat_ready:
                    self._wechat_ready = False
                    self._login_ready_monotonic = None
                    if self.on_state_change is not None:
                        self.on_state_change()
                    logger.info("event=wechat_login_lost")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.info("event=wechat_login_probe_failed error_type=%s", type(exc).__name__)
            await asyncio.sleep(self.poll_interval_seconds)


class SessionMaintenanceTrigger:
    """Schedules idle 8h session compaction without hot-looping time_jobs."""

    NOOP_DEFER_MINUTES = 30

    def __init__(
        self,
        account: str,
        dispatcher: Dispatcher,
        login_watcher: LoginWatcher,
        *,
        is_paused: Callable[[], bool],
        session_maintenance: Callable[[], Awaitable[int]] | None = None,
        time_scheduler: TimeScheduler | None = None,
        noop_defer_seconds: float = NOOP_DEFER_MINUTES * 60,
    ) -> None:
        self.account = account
        self.dispatcher = dispatcher
        self.login_watcher = login_watcher
        self._is_paused = is_paused
        self.session_maintenance = session_maintenance
        self.time_scheduler = time_scheduler
        self.noop_defer_seconds = float(noop_defer_seconds)
        self._maintenance_task: asyncio.Task | None = None

    def cancel(self) -> None:
        task = self._maintenance_task
        if task is not None and not task.done():
            task.cancel()
            logger.info("event=session_maintenance_cancelled reason=new_inbound")

    async def run(self) -> None:
        while True:
            scheduler = self.time_scheduler
            if self.session_maintenance is None or scheduler is None:
                await asyncio.sleep(1)
                continue
            if not scheduler.has_scheduled_job(self.account, "session_maintenance"):
                activity = scheduler.account_activity_state(self.account)
                if activity and activity.get("last_inbound_at"):
                    last = datetime.fromisoformat(str(activity["last_inbound_at"]))
                    scheduler.schedule(
                        account_id=self.account,
                        conversation_id=None,
                        kind="session_maintenance",
                        due_at=max(last + timedelta(hours=8), datetime.now(UTC)),
                        payload={"idle_hours": 8},
                        replace_existing=True,
                    )
            job = scheduler.claim_due(kind="session_maintenance")
            if job is None:
                await asyncio.sleep(1)
                continue
            if self._is_paused():
                scheduler.defer(
                    job.job_id, datetime.now(UTC) + timedelta(minutes=5), "agent_paused",
                )
                continue
            if not self.login_watcher.wechat_ready:
                scheduler.defer(
                    job.job_id, datetime.now(UTC) + timedelta(minutes=5), "wechat_not_logged_in",
                )
                continue
            login_ready_at = self.login_watcher.login_ready_monotonic
            if login_ready_at is not None and time.monotonic() - login_ready_at < 15:
                scheduler.defer(
                    job.job_id, datetime.now(UTC) + timedelta(seconds=20), "login_catchup_window",
                )
                continue
            if self.dispatcher.has_pending_work:
                scheduler.defer(
                    job.job_id, datetime.now(UTC) + timedelta(minutes=1), "message_work_pending",
                )
                continue
            if not self.dispatcher.provider_ready and not await self.dispatcher.ensure_provider_ready():
                scheduler.defer(
                    job.job_id, datetime.now(UTC) + timedelta(seconds=30), "provider_unavailable",
                )
                continue
            task = asyncio.create_task(self.session_maintenance())
            self._maintenance_task = task
            try:
                summarized = await task
                scheduler.finish(job.job_id)
                scheduler.prune()
                if summarized:
                    logger.info("event=session_maintenance_completed summarized=%s", summarized)
                # Prevent tight 1Hz rescheduling when last_inbound_at is already > 8h old
                # and no conversations qualified for compaction (or after compaction completed).
                if not scheduler.has_scheduled_job(self.account, "session_maintenance"):
                    now = datetime.now(UTC)
                    next_due = now + timedelta(seconds=self.noop_defer_seconds)
                    scheduler.schedule(
                        account_id=self.account,
                        conversation_id=None,
                        kind="session_maintenance",
                        due_at=next_due,
                        payload={
                            "idle_hours": 8,
                            "reason": "noop_cooldown" if not summarized else "post_summary_cooldown",
                        },
                        replace_existing=True,
                    )
            except asyncio.CancelledError:
                if asyncio.current_task() and asyncio.current_task().cancelling():
                    raise
                logger.info("event=session_maintenance_interrupted reason=new_inbound")
            except ModelProviderUnavailable:
                delay = self.dispatcher.trip_provider_circuit("session_maintenance_unavailable")
                scheduler.defer(
                    job.job_id,
                    datetime.now(UTC) + timedelta(seconds=max(30, int(delay))),
                    "provider_unavailable",
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("event=session_maintenance_failed error_type=%s", type(exc).__name__)
                scheduler.defer(
                    job.job_id,
                    datetime.now(UTC) + timedelta(minutes=5),
                    f"error:{type(exc).__name__}",
                )
            finally:
                if self._maintenance_task is task:
                    self._maintenance_task = None


class IdleEventTrigger:
    """Claims durable account-idle events with stepped backoff and inbound preemption."""

    def __init__(
        self,
        account: str,
        dispatcher: Dispatcher,
        login_watcher: LoginWatcher,
        *,
        is_paused: Callable[[], bool],
        time_scheduler: TimeScheduler | None = None,
        idle_event_handler: Callable[[str, int, str], Awaitable[None]] | None = None,
    ) -> None:
        self.account = account
        self.dispatcher = dispatcher
        self.login_watcher = login_watcher
        self._is_paused = is_paused
        self.time_scheduler = time_scheduler
        self.idle_event_handler = idle_event_handler
        self._idle_task: asyncio.Task | None = None

    @staticmethod
    def next_idle_delay_seconds(
        consecutive_idle_events: int,
        *,
        rng: random.Random | None = None,
    ) -> int:
        """Back off idle wakeups when no new human messages arrive."""
        generator = rng or random.SystemRandom()
        count = max(0, int(consecutive_idle_events))
        if count == 0:
            return generator.randint(30 * 60, 60 * 60)
        if count == 1:
            return generator.randint(90 * 60, 150 * 60)
        if count == 2:
            return generator.randint(180 * 60, 300 * 60)
        return generator.randint(360 * 60, 480 * 60)

    def cancel(self, reason: str) -> None:
        task = self._idle_task
        if task is not None and not task.done():
            task.cancel()
            logger.info("event=idle_event_cancelled reason=%s", reason)

    def reschedule(self, due_at: datetime) -> None:
        if self.time_scheduler is None:
            return
        self.time_scheduler.schedule(
            account_id=self.account,
            conversation_id=None,
            kind="idle_event",
            due_at=due_at,
            replace_existing=True,
        )

    async def run(self) -> None:
        """Claim durable account-idle events; inbound work always preempts them."""
        while True:
            scheduler = self.time_scheduler
            if scheduler is None or self.idle_event_handler is None:
                await asyncio.sleep(1)
                continue
            if self._is_paused() or not self.login_watcher.wechat_ready:
                await asyncio.sleep(1)
                continue
            if scheduler.next_idle_job(self.account) is None:
                activity = scheduler.account_activity_state(self.account)
                if activity and activity.get("last_inbound_at"):
                    consecutive = int(activity.get("consecutive_idle_events") or 0)
                    anchor = datetime.fromisoformat(str(activity["last_inbound_at"]))
                    if consecutive > 0:
                        recent_runs = scheduler.recent_idle_event_times(
                            self.account,
                            datetime.now(UTC) - timedelta(hours=24),
                        )
                        if recent_runs and recent_runs[0] > anchor:
                            anchor = recent_runs[0]
                    due = anchor + timedelta(
                        seconds=self.next_idle_delay_seconds(consecutive),
                    )
                    scheduler.schedule(
                        account_id=self.account,
                        conversation_id=None,
                        kind="idle_event",
                        due_at=max(due, datetime.now(UTC)),
                        replace_existing=True,
                    )
            job = scheduler.claim_due(kind="idle_event")
            if job is None:
                await asyncio.sleep(1)
                continue
            now = datetime.now(UTC)
            cap_times = scheduler.recent_idle_event_times(
                self.account,
                now - timedelta(hours=24),
            )
            if len(cap_times) >= 10:
                self.reschedule(cap_times[-1] + timedelta(hours=24))
                scheduler.finish(job.job_id)
                logger.info("event=idle_event_deferred reason=rolling_24h_cap")
                continue
            if (
                self._is_paused()
                or not self.login_watcher.wechat_ready
                or self.dispatcher.has_pending_work
            ):
                scheduler.defer(job.job_id, now + timedelta(minutes=5), "preconditions_not_met")
                continue
            if not self.dispatcher.provider_ready and not await self.dispatcher.ensure_provider_ready():
                scheduler.defer(job.job_id, now + timedelta(minutes=2), "provider_unavailable")
                continue
            started_at = datetime.now(UTC)
            event_id, event_number, model_id = scheduler.record_idle_event(
                self.account,
                at=started_at,
            )
            logger.info(
                "event=idle_event_started event_number=%s model=%s", event_number, model_id,
            )
            task = asyncio.create_task(
                self.idle_event_handler(event_id, event_number, model_id),
            )
            self._idle_task = task
            fallback_outcome = "timeout"
            fallback_summary = "空闲任务被中断或未能写入活动摘要；已自动记录并停止。"
            try:
                await asyncio.wait_for(task, timeout=30 * 60)
            except asyncio.CancelledError:
                if asyncio.current_task() and asyncio.current_task().cancelling():
                    raise
                fallback_outcome = "interrupted"
                fallback_summary = "收到新消息，空闲活动已暂停让位。"
                logger.info("event=idle_event_interrupted reason=new_inbound")
            except TimeoutError:
                logger.warning("event=idle_event_timeout event_number=%s", event_number)
            except ModelProviderUnavailable:
                self.dispatcher.trip_provider_circuit("idle_event_unavailable")
                fallback_outcome = "failed"
                fallback_summary = "空闲任务执行异常（ModelProviderUnavailable）；已自动记录并停止。"
                logger.warning(
                    "event=idle_event_failed event_number=%s error_type=ModelProviderUnavailable",
                    event_number,
                )
            except Exception as exc:  # noqa: BLE001
                fallback_outcome = "failed"
                fallback_summary = f"空闲任务执行异常（{type(exc).__name__}）；已自动记录并停止。"
                logger.warning(
                    "event=idle_event_failed event_number=%s error_type=%s",
                    event_number,
                    type(exc).__name__,
                )
            finally:
                if self._idle_task is task:
                    self._idle_task = None
                if not scheduler.has_idle_activity(self.account, event_id):
                    scheduler.record_idle_activity(
                        account_id=self.account,
                        event_id=event_id,
                        event_number=event_number,
                        model_id="system",
                        summary=fallback_summary,
                        started_at=started_at,
                        finished_at=datetime.now(UTC),
                        outcome=fallback_outcome,
                    )
                scheduler.finish(job.job_id)
                scheduler.prune()
            current = scheduler.account_activity_state(self.account) or {}
            if (
                current.get("last_inbound_at")
                and datetime.fromisoformat(str(current["last_inbound_at"])) <= started_at
            ):
                consecutive = int(current.get("consecutive_idle_events") or 0)
                self.reschedule(
                    datetime.now(UTC)
                    + timedelta(seconds=self.next_idle_delay_seconds(consecutive))
                )


class FollowupTrigger:
    """Manages per-conversation scheduled followups with 60s inbound merge window."""

    DEFAULT_MERGE_WINDOW_SECONDS = 60.0

    def __init__(
        self,
        account: str,
        dispatcher: Dispatcher,
        login_watcher: LoginWatcher,
        *,
        is_paused: Callable[[], bool],
        time_scheduler: TimeScheduler | None = None,
        queue_size_provider: Callable[[], int] | None = None,
        merge_window_seconds: float = DEFAULT_MERGE_WINDOW_SECONDS,
    ) -> None:
        self.account = account
        self.dispatcher = dispatcher
        self.login_watcher = login_watcher
        self._is_paused = is_paused
        self.time_scheduler = time_scheduler
        self._queue_size_provider = queue_size_provider
        self.merge_window_seconds = float(merge_window_seconds)
        self.scheduled_followups: dict[tuple[str, str], asyncio.Task] = {}
        self._followup_inbound_events: dict[tuple[str, str], asyncio.Event] = {}
        self._followup_inbound_at: dict[tuple[str, str], float] = {}
        self._cancelled_followup_notices: dict[tuple[str, str], int] = {}

    def active_count(self) -> int:
        return len(self.scheduled_followups)

    def is_scheduled(self, account_id: str, conversation_id: str) -> bool:
        return (account_id, conversation_id) in self.scheduled_followups

    def get_cancelled_notice(self, account_id: str, conversation_id: str) -> int | None:
        return self._cancelled_followup_notices.get((account_id, conversation_id))

    def clear_cancelled_notice(self, account_id: str, conversation_id: str, token: int) -> None:
        key = (account_id, conversation_id)
        if self._cancelled_followup_notices.get(key) == token:
            self._cancelled_followup_notices.pop(key, None)

    def blocked_conversations(self) -> set[tuple[str, str]]:
        return {
            key
            for key, event in self._followup_inbound_events.items()
            if event.is_set() and key in self.scheduled_followups
        }

    def notify_inbound(self, account_id: str, conversation_id: str) -> None:
        key = (account_id, conversation_id)
        if key in self.scheduled_followups:
            event = self._followup_inbound_events.get(key)
            if event is not None and not event.is_set():
                self._followup_inbound_at[key] = time.monotonic()
                event.set()
                logger.info(
                    "event=followup_merge_started conversation_sha=%s window_seconds=60",
                    sha(conversation_id),
                )

    def cancel(self, key: tuple[str, str], reason: str) -> None:
        if self.time_scheduler is not None:
            self.time_scheduler.cancel(
                account_id=key[0],
                conversation_id=key[1],
                kind="followup",
            )
        task = self.scheduled_followups.pop(key, None)
        self._followup_inbound_events.pop(key, None)
        self._followup_inbound_at.pop(key, None)
        if reason == "new_inbound" and task is not None and not task.done():
            self._cancelled_followup_notices[key] = (
                self._cancelled_followup_notices.get(key, 0) + 1
            )
        if task is not None and not task.done():
            logger.info("event=scheduled_followup_cancelled reason=%s", reason)
            task.cancel()

    def cancel_all(self) -> None:
        for key in tuple(self.scheduled_followups):
            self.cancel(key, "pilot_shutdown")

    def recover_scheduled_followups(self) -> int:
        """Re-arm durable followups after service restart without replaying their original turn."""
        if self.time_scheduler is None:
            return 0
        restored = 0
        now = datetime.now(UTC)
        for job in self.time_scheduler.scheduled_jobs(self.account, "followup"):
            if not job.conversation_id:
                continue
            key = (job.account_id, job.conversation_id)
            if key in self.scheduled_followups:
                continue
            task = str(job.payload.get("task_description", ""))
            parent = str(job.payload.get("parent_receipt", job.job_id))
            context = ContextWindow(
                account_id=job.account_id,
                conversation_id=job.conversation_id,
                is_group=is_group_conversation(job.conversation_id),
                turns=(),
            )
            wait_seconds = max(0, int((job.due_at - now).total_seconds()))
            inbound_event = asyncio.Event()
            if self.dispatcher.scheduler.has_pending_conversation(*key):
                inbound_event.set()
                self._followup_inbound_at[key] = time.monotonic()
            self._followup_inbound_events[key] = inbound_event
            timer = asyncio.create_task(
                self._run_scheduled_followup(
                    key, context, parent, task, wait_seconds, job.job_id, inbound_event,
                )
            )
            self.scheduled_followups[key] = timer
            restored += 1
        if restored:
            logger.info("event=followup_jobs_recovered count=%s", restored)
        return restored

    async def schedule_followup(
        self,
        context: ContextWindow,
        parent_receipt: str,
        task_description: str,
        wait_seconds: int,
    ) -> dict[str, object]:
        if self._queue_size_provider is not None and self._queue_size_provider():
            return {"status": "cancelled", "reason": "new_message_waiting"}
        key = (context.account_id, context.conversation_id)
        self.cancel(key, "replaced")
        job_id = None
        if self.time_scheduler is not None:
            job_id = self.time_scheduler.schedule(
                account_id=context.account_id,
                conversation_id=context.conversation_id,
                kind="followup",
                due_at=datetime.now(UTC) + timedelta(seconds=wait_seconds),
                payload={"parent_receipt": parent_receipt, "task_description": task_description},
                replace_existing=True,
            )
        inbound_event = asyncio.Event()
        self._followup_inbound_events[key] = inbound_event
        task = asyncio.create_task(
            self._run_scheduled_followup(
                key, context, parent_receipt, task_description, wait_seconds, job_id, inbound_event,
            )
        )
        self.scheduled_followups[key] = task
        logger.info(
            "event=scheduled_followup_created receipt=%s wait_seconds=%s",
            parent_receipt[:12],
            wait_seconds,
        )
        return {
            "status": "scheduled",
            "wait_seconds": wait_seconds,
            "merge_window_seconds": 60,
            "cancel_on_new_message": False,
        }

    def followup_for(
        self, context: ContextWindow, receipt: str,
    ) -> Callable[[str, int], Awaitable[dict[str, object]]]:
        calls = 0

        async def schedule(task_description: str, wait_seconds: int) -> dict[str, object]:
            nonlocal calls
            if calls >= 3:
                return {"status": "limit_reached", "reason": "max_3_calls_per_turn"}
            calls += 1
            return await self.schedule_followup(context, receipt, task_description, wait_seconds)

        return schedule

    async def _run_scheduled_followup(
        self,
        key: tuple[str, str],
        context: ContextWindow,
        parent_receipt: str,
        task_description: str,
        wait_seconds: int,
        job_id: str | None = None,
        inbound_event: asyncio.Event | None = None,
    ) -> None:
        receipt = hashlib.sha256(
            f"followup\0{parent_receipt}\0{task_description}".encode()
        ).hexdigest()
        state = "cancelled"
        batch = None
        deferred_batch = False
        deferred_job = False
        deadline_token = None
        timing_token = turn_started_monotonic.set(time.monotonic())
        recorded_attempt = False
        try:
            await asyncio.sleep(wait_seconds)
            inbound_at = self._followup_inbound_at.get(key)
            if inbound_event is not None and inbound_event.is_set():
                if inbound_at is None:
                    inbound_at = time.monotonic()
                    self._followup_inbound_at[key] = inbound_at
                remaining = inbound_at + self.merge_window_seconds - time.monotonic()
                if remaining > 0:
                    logger.info(
                        "event=followup_merge_waiting conversation_sha=%s seconds=%s",
                        sha(key[1]),
                        int(remaining),
                    )
                    await asyncio.sleep(remaining)
            if self.scheduled_followups.get(key) is not asyncio.current_task():
                return
            probe_base = self.dispatcher.provider_probe_base_seconds
            if job_id and self.time_scheduler is not None:
                while (
                    self._is_paused()
                    or not self.login_watcher.wechat_ready
                    or (
                        not self.dispatcher.provider_ready
                        and not await self.dispatcher.ensure_provider_ready()
                    )
                ):
                    if self.scheduled_followups.get(key) is not asyncio.current_task():
                        return
                    sleep_seconds = (
                        10.0
                        if (self._is_paused() or not self.login_watcher.wechat_ready)
                        else min(10.0, max(0.02, probe_base))
                    )
                    await asyncio.sleep(sleep_seconds)
                if not self.time_scheduler.claim_job(job_id):
                    state = "cancelled_or_claimed"
                    return
            else:
                while (
                    not self.dispatcher.provider_ready
                    and not await self.dispatcher.ensure_provider_ready()
                ):
                    if self.scheduled_followups.get(key) is not asyncio.current_task():
                        return
                    await asyncio.sleep(min(10.0, max(0.02, probe_base)))
            if self._is_paused():
                state = "paused"
                return
            if self.dispatcher._test_limit_reached():
                state = "test_limit"
                return

            wake_context = replace(
                context, turns=(), delivery_notices=(), scheduled_task=task_description,
            )
            prepared: AssembledBatch | None = None
            if inbound_event is not None and inbound_event.is_set():
                batch = self.dispatcher.scheduler.claim_conversation_batch(*key)
                if batch is not None:
                    if self.dispatcher.context_assembler is not None:
                        prepared = await self.dispatcher.context_assembler.assemble(
                            batch.observations,
                        )
                    else:
                        prepared = AssembledBatch(
                            context=build_hook_context(batch.observations),
                            memory_message_ids=(),
                            quote_context=None,
                        )
                    prepared = replace(
                        prepared,
                        context=replace(prepared.context, scheduled_task=task_description),
                    )
                    wake_context = replace(
                        wake_context,
                        turns=prepared.context.turns,
                        history_turns=prepared.context.history_turns,
                        recent_other_turns=prepared.context.recent_other_turns,
                        global_timeline_turns=prepared.context.global_timeline_turns,
                        identities=prepared.context.identities,
                    )
                    logger.info(
                        "event=followup_merge_dispatched conversation_sha=%s batch_size=%s",
                        sha(key[1]),
                        len(batch.observations),
                    )

            if batch is None:
                existing = self.dispatcher.db.execute(
                    "SELECT state FROM attempts WHERE id=?",
                    (receipt,),
                ).fetchone()
                if existing is not None and str(existing[0]) in COMPLETED_ATTEMPT_STATES:
                    state = "duplicate_receipt"
                    return
            self.dispatcher._record_attempt_audit(receipt, "started")
            recorded_attempt = True

            timeout = self.dispatcher.process_timeout_seconds
            deadline_token = turn_deadline_monotonic.set(time.monotonic() + timeout)
            with (
                batch_execution_scope(
                    batch=batch,
                    assembled=prepared,
                    scheduler=self.dispatcher.scheduler if batch is not None else None,
                ),
                tool_turn_scope(
                    wake_context,
                    receipt,
                    receipt_ids=batch.receipt_ids if batch is not None else (),
                ),
            ):
                logger.info("event=scheduled_followup_wakeup receipt=%s", receipt[:12])
                if batch is not None:
                    latest = batch.observations[-1]
                    synthetic = replace(
                        latest,
                        text=f"合并的本轮输入包含 {len(batch.observations)} 条按时间排序的消息。",
                        raw_envelope_json="",
                        raw_message_json="",
                        source_msg_id=None,
                        event_time_context=latest.event_time_context,
                    )
                    process_state = await asyncio.wait_for(
                        self.dispatcher.process_single(synthetic),
                        timeout=timeout,
                    )
                    if process_state not in ("paused", "outside_test_scope", "test_limit"):
                        self.dispatcher.mark_provider_healthy(source="scheduled_followup")
                    state = (
                        process_state
                        if isinstance(process_state, str) and process_state
                        else "merged_inbound_processed"
                    )
                else:
                    text = await asyncio.wait_for(
                        self.dispatcher.reply_logged(wake_context, receipt, "scheduled_followup"),
                        timeout=timeout,
                    )
                    self.dispatcher.mark_provider_healthy(source="scheduled_followup")
                    state = await self.dispatcher.deliver_auto_reply(
                        receipt,
                        key[0],
                        key[1],
                        text,
                        context=wake_context,
                        retry_reply_fn=None,
                        archive_operation_id=f"{receipt}:scheduled",
                    )
        except asyncio.CancelledError:
            logger.info("event=scheduled_followup_cancelled reason=new_inbound_or_replaced")
            raise
        except ModelProviderUnavailable:
            batch_receipt = (
                hashlib.sha256(f"batch\0{batch.batch_id}".encode()).hexdigest()
                if batch is not None
                else receipt
            )
            delivery = self.dispatcher.current_delivery_state(batch_receipt)
            has_side_effect = (
                int(delivery.get("send_attempt_count", 0) or 0) > 0
                or delivery.get("last_send_status")
                in ("queued", "accepted_by_server", "unknown")
            )
            if not has_side_effect:
                delay = self.dispatcher.trip_provider_circuit("scheduled_followup_unavailable")
                state = "provider_unavailable"
                if batch is not None:
                    self.dispatcher.scheduler.defer_batch(
                        batch, "provider_unavailable", delay_seconds=delay,
                    )
                    deferred_batch = True
                elif job_id and self.time_scheduler is not None:
                    deferred_job = self.time_scheduler.defer(
                        job_id,
                        datetime.now(UTC) + timedelta(seconds=delay),
                        "provider_unavailable",
                    )
            else:
                state = "provider_unavailable_after_send"
            logger.warning("event=scheduled_followup_failed error_type=ModelProviderUnavailable")
        except Exception as exc:  # noqa: BLE001
            state = "scheduled_followup_unknown"
            logger.warning("event=scheduled_followup_failed error_type=%s", type(exc).__name__)
        finally:
            if batch is not None and not deferred_batch:
                self.dispatcher.scheduler.release_for_retry(batch, "scheduled_followup_incomplete")
            if deadline_token is not None:
                turn_deadline_monotonic.reset(deadline_token)
            turn_started_monotonic.reset(timing_token)
            if self.scheduled_followups.get(key) is asyncio.current_task():
                self.scheduled_followups.pop(key, None)
                self._followup_inbound_events.pop(key, None)
                self._followup_inbound_at.pop(key, None)
            if recorded_attempt:
                self.dispatcher._record_attempt_audit(receipt, state)
            if job_id and self.time_scheduler is not None and not deferred_job:
                self.time_scheduler.finish(
                    job_id,
                    error=state
                    if state
                    not in (
                        "agent_tool_send_unconfirmed",
                        "empty_reply",
                        "queued",
                        "accepted_by_server",
                        "sent",
                        "scheduled_followup",
                        "merged_inbound_processed",
                    )
                    else None,
                )


class Triggers:
    """Composite container owning all four decoupled background triggers."""

    def __init__(
        self,
        account: str,
        dispatcher: Dispatcher,
        *,
        is_paused: Callable[[], bool],
        login_check: Callable[[], Awaitable[bool]] | None = None,
        initialize_after_login: Callable[[], Awaitable[None]] | None = None,
        session_maintenance: Callable[[], Awaitable[int]] | None = None,
        time_scheduler: TimeScheduler | None = None,
        idle_event_handler: Callable[[str, int, str], Awaitable[None]] | None = None,
        queue_size_provider: Callable[[], int] | None = None,
        merge_window_seconds: float = FollowupTrigger.DEFAULT_MERGE_WINDOW_SECONDS,
        on_login_state_change: Callable[[], None] | None = None,
    ) -> None:
        self.account = account
        self.login_watcher = LoginWatcher(
            dispatcher.scheduler,
            login_check=login_check,
            initialize_after_login=initialize_after_login,
            on_state_change=on_login_state_change,
        )
        self.session_maintenance = SessionMaintenanceTrigger(
            account,
            dispatcher,
            self.login_watcher,
            is_paused=is_paused,
            session_maintenance=session_maintenance,
            time_scheduler=time_scheduler,
        )
        self.idle_events = IdleEventTrigger(
            account,
            dispatcher,
            self.login_watcher,
            is_paused=is_paused,
            time_scheduler=time_scheduler,
            idle_event_handler=idle_event_handler,
        )
        self.followups = FollowupTrigger(
            account,
            dispatcher,
            self.login_watcher,
            is_paused=is_paused,
            time_scheduler=time_scheduler,
            queue_size_provider=queue_size_provider,
            merge_window_seconds=merge_window_seconds,
        )

    @property
    def time_scheduler(self) -> TimeScheduler | None:
        return self.session_maintenance.time_scheduler

    @time_scheduler.setter
    def time_scheduler(self, value: TimeScheduler | None) -> None:
        self.session_maintenance.time_scheduler = value
        self.idle_events.time_scheduler = value
        self.followups.time_scheduler = value

    def configure_idle_events(
        self,
        scheduler: TimeScheduler,
        handler: Callable[[str, int, str], Awaitable[None]],
    ) -> None:
        self.time_scheduler = scheduler
        self.idle_events.idle_event_handler = handler

    def record_account_inbound(self, item: MessageEvent) -> None:
        scheduler = self.time_scheduler
        if scheduler is None:
            return
        now = datetime.now(UTC)
        scheduler.record_inbound(item.account_id, item.conversation_id, at=now)
        self.idle_events.cancel("new_inbound")
        self.session_maintenance.cancel()
        due = now + timedelta(seconds=IdleEventTrigger.next_idle_delay_seconds(0))
        scheduler.schedule(
            account_id=item.account_id,
            conversation_id=None,
            kind="idle_event",
            due_at=due,
            replace_existing=True,
        )
        logger.info("event=account_idle_retimed conversation_sha=%s", sha(item.conversation_id))

    def on_paused(self) -> None:
        self.idle_events.cancel("paused")
        self.session_maintenance.cancel()
        if self.time_scheduler is not None:
            self.time_scheduler.cancel(account_id=self.account, kind="idle_event")
            self.time_scheduler.cancel(account_id=self.account, kind="session_maintenance")
