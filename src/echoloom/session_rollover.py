"""Session routing, rollover policy, summarization, and safety-block recovery."""

from __future__ import annotations

import asyncio
import inspect
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from . import runtime_contract as types
from .agent_profiles import LUNA_ACTIVE_COMPRESSION_TOKENS, LUNA_IDLE_COMPRESSION_TOKENS
from .context import ContextWindow
from .ports import Clock
from .prompt_catalog import fill_prompt, load_prompt
from .runtime_contract import AgentFramework, ModelProviderUnavailable
from .runtime_contract import RunOptions as RunConfig
from .runtime_scope import model_usage_scope
from .session_history import (
    HISTORY_END,
    HISTORY_START,
    MODEL_CONTEXT_LIMITS,
    SUMMARY_MAX_RETRIES,
    PreparedSession,
    _parse_utc,
    count_effective_messages,
    estimated_event_bytes,
    events_as_records,
    format_rollover_history,
    live_session_events,
    route_hash,
    session_keys,
    strip_injected_history,
    window_session_id,
)
from .session_recycle_policy import (
    CacheRecycleRule,
    evaluate_cache_recycle,
    resolve_cache_recycle_rule,
)
from .usage import (
    DAILY_ACCOUNT_BUDGET_MICROUSD,
    IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
    IMAGE_READ_DAILY_BUDGET_MICROUSD,
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
)

if TYPE_CHECKING:
    from .cross_chat_dedupe import CrossChatDedupeStore

logger = logging.getLogger(__name__)


class SessionRolloverPolicy:
    """Owns active_sessions routing table, idle/size rollover, summaries, and safety fallback."""

    def __init__(
        self,
        *,
        routing_db: sqlite3.Connection | None = None,
        sessions: object | None = None,
        framework: AgentFramework | None = None,
        summary_model: object | None = None,
        summary_framework: AgentFramework | None = None,
        initial_summary: Callable[[str, str], str | None] | None = None,
        effective_message_count: Callable[..., int] | None = None,
        last_human_message_age: Callable[[str, str], int | None] | None = None,
        model_id: str = "",
        idle_session_hours: int = 8,
        max_history_records: int = 50,
        max_history_bytes: int = 32_000,
        idle_context_byte_threshold: int | None = None,
        hard_context_byte_threshold: int | None = None,
        idle_effective_message_threshold: int = 50,
        token_context_limits: bool | None = None,
        idle_context_token_threshold: int = LUNA_IDLE_COMPRESSION_TOKENS,
        active_context_token_threshold: int = LUNA_ACTIVE_COMPRESSION_TOKENS,
        summary_prompt: str = "session_summary",
        summary_system_prompt: str = "session_summary_system",
        profile_id: str = "default",
        profile_daily_budget_microusd: int = DAILY_ACCOUNT_BUDGET_MICROUSD,
        profile_text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD,
        profile_image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD,
        profile_image_generation_budget_microusd: int = IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
        profile_for_conversation: Callable[[str, str], str] | None = None,
        dedupe: CrossChatDedupeStore | None = None,
        cache_tier: str | None = None,
        cache_recycle_idle_seconds: int | None = None,
        cache_recycle_min_tokens: int | None = None,
        clock: Clock | None = None,
    ) -> None:
        if (
            idle_session_hours < 1
            or not 1 <= max_history_records <= 200
            or max_history_bytes < 1
            or idle_effective_message_threshold < 1
        ):
            raise ValueError("session window limits must be positive")
        self.routing_db = routing_db
        self._routing_memory: dict[str, dict[str, object]] = {}
        self.sessions = sessions
        self.framework = framework
        self.summary_model = summary_model
        self.summary_framework = summary_framework if summary_framework is not None else framework
        self.initial_summary = initial_summary
        self.effective_message_count = effective_message_count
        self.last_human_message_age = last_human_message_age
        self.model_id = model_id
        self.idle_session_hours = idle_session_hours
        self.max_history_records = max_history_records
        self.max_history_bytes = max_history_bytes
        hard_token_threshold, default_idle_byte_threshold = MODEL_CONTEXT_LIMITS.get(
            model_id, (30_000, 80_000)
        )
        self.idle_context_byte_threshold = (
            idle_context_byte_threshold
            if idle_context_byte_threshold is not None
            else default_idle_byte_threshold
        )
        self.hard_context_byte_threshold = (
            hard_context_byte_threshold
            if hard_context_byte_threshold is not None
            else hard_token_threshold * 4
        )
        self.idle_effective_message_threshold = idle_effective_message_threshold
        self.token_context_limits = (
            model_id == "gpt-6-luna" if token_context_limits is None else token_context_limits
        )
        self.idle_context_token_threshold = idle_context_token_threshold
        self.active_context_token_threshold = active_context_token_threshold
        self.runner_threshold = (
            self.active_context_token_threshold
            if self.token_context_limits
            else hard_token_threshold
        )
        self.summary_prompt = summary_prompt
        self.summary_system_prompt = summary_system_prompt
        self.profile_id = profile_id
        self.profile_daily_budget_microusd = profile_daily_budget_microusd
        self.profile_text_budget_microusd = profile_text_budget_microusd
        self.profile_image_read_budget_microusd = profile_image_read_budget_microusd
        self.profile_image_generation_budget_microusd = profile_image_generation_budget_microusd
        self.profile_for_conversation = profile_for_conversation
        self.dedupe = dedupe
        self.clock = clock
        self.cache_recycle_rule: CacheRecycleRule | None = resolve_cache_recycle_rule(
            cache_tier=cache_tier,
            idle_seconds=cache_recycle_idle_seconds,
            min_context_tokens=cache_recycle_min_tokens,
        )
        if self.routing_db is not None:
            self._ensure_schema(self.routing_db)

    def _now(self, now: datetime | None = None) -> datetime:
        if now is not None:
            return (now if now.tzinfo is not None else now.replace(tzinfo=UTC)).astimezone(UTC)
        if self.clock is not None:
            parsed = _parse_utc(self.clock.now())
            if parsed is not None:
                return parsed
        return datetime.now(UTC)

    @staticmethod
    def _ensure_schema(db: sqlite3.Connection) -> None:
        db.execute(
            "CREATE TABLE IF NOT EXISTS active_sessions (route TEXT PRIMARY KEY, "
            "generation INTEGER NOT NULL, last_message_at TEXT, latest_summary TEXT, "
            "account_id TEXT, conversation_id TEXT, safety_recovery_generation INTEGER, "
            "summary_only_generation INTEGER)"
        )
        columns = {row[1] for row in db.execute("PRAGMA table_info(active_sessions)")}
        for name, kind in (
            ("last_message_at", "TEXT"),
            ("latest_summary", "TEXT"),
            ("account_id", "TEXT"),
            ("conversation_id", "TEXT"),
            ("safety_recovery_generation", "INTEGER"),
            ("summary_only_generation", "INTEGER"),
        ):
            if name not in columns:
                db.execute(f"ALTER TABLE active_sessions ADD COLUMN {name} {kind}")
        db.commit()

    def generation(self, route: str) -> int:
        return int(self.route_state(route)["generation"])

    def route_state(self, route: str) -> dict[str, object]:
        if self.routing_db is None:
            return dict(
                self._routing_memory.get(
                    route,
                    {
                        "generation": 0,
                        "last_message_at": None,
                        "latest_summary": None,
                        "safety_recovery_generation": None,
                        "summary_only_generation": None,
                    },
                )
            )
        row = self.routing_db.execute(
            "SELECT generation,last_message_at,latest_summary,account_id,conversation_id,"
            "safety_recovery_generation,summary_only_generation "
            "FROM active_sessions WHERE route=?",
            (route,),
        ).fetchone()
        if row is None:
            return {
                "generation": 0,
                "last_message_at": None,
                "latest_summary": None,
                "safety_recovery_generation": None,
                "summary_only_generation": None,
            }
        return {
            "generation": int(row[0]),
            "last_message_at": row[1],
            "latest_summary": row[2],
            "account_id": row[3],
            "conversation_id": row[4],
            "safety_recovery_generation": row[5],
            "summary_only_generation": row[6],
        }

    def save_route_state(
        self, route: str, state: dict[str, object], *, delete: bool = False
    ) -> None:
        if self.routing_db is None:
            if delete:
                self._routing_memory.pop(route, None)
            else:
                self._routing_memory[route] = dict(state)
            return
        if delete:
            self.routing_db.execute("DELETE FROM active_sessions WHERE route=?", (route,))
        else:
            self.routing_db.execute(
                "INSERT INTO active_sessions "
                "(route,generation,last_message_at,latest_summary,account_id,conversation_id,"
                "safety_recovery_generation,summary_only_generation) VALUES (?,?,?,?,?,?,?,?) "
                "ON CONFLICT(route) DO UPDATE SET "
                "generation=excluded.generation,last_message_at=excluded.last_message_at,"
                "latest_summary=excluded.latest_summary,account_id=excluded.account_id,"
                "conversation_id=excluded.conversation_id,"
                "safety_recovery_generation=excluded.safety_recovery_generation,"
                "summary_only_generation=excluded.summary_only_generation",
                (
                    route,
                    int(state.get("generation", 0)),
                    state.get("last_message_at"),
                    state.get("latest_summary"),
                    state.get("account_id"),
                    state.get("conversation_id"),
                    state.get("safety_recovery_generation"),
                    state.get("summary_only_generation"),
                ),
            )
        self.routing_db.commit()

    def list_active_routes(self) -> list[tuple[str, dict[str, object]]]:
        if self.routing_db is None:
            return [(route, dict(state)) for route, state in self._routing_memory.items()]
        rows = self.routing_db.execute(
            "SELECT route,generation,last_message_at,latest_summary,account_id,"
            "conversation_id,safety_recovery_generation,summary_only_generation "
            "FROM active_sessions WHERE last_message_at IS NOT NULL"
        ).fetchall()
        return [
            (
                row[0],
                {
                    "generation": int(row[1]),
                    "last_message_at": row[2],
                    "latest_summary": row[3],
                    "account_id": row[4],
                    "conversation_id": row[5],
                    "safety_recovery_generation": row[6],
                    "summary_only_generation": row[7],
                },
            )
            for row in rows
        ]

    def record_activity(
        self,
        route: str,
        account_id: str,
        conversation_id: str,
        *,
        now: datetime | None = None,
    ) -> None:
        state = self.route_state(route)
        state["account_id"] = account_id
        state["conversation_id"] = conversation_id
        state["last_message_at"] = self._now(now).isoformat()
        self.save_route_state(route, state)

    def register_route(self, route: str, account_id: str, conversation_id: str) -> None:
        state = self.route_state(route)
        state["account_id"] = account_id
        state["conversation_id"] = conversation_id
        self.save_route_state(route, state)

    def is_idle_time(self, last_message_at: object, *, now: datetime | None = None) -> bool:
        last = _parse_utc(last_message_at)
        if last is None:
            return False
        return self._now(now) - last >= timedelta(hours=self.idle_session_hours)

    def should_rollover_on_idle(
        self,
        *,
        context_tokens: int | None,
        message_count: int,
        total_bytes: int,
    ) -> bool:
        # Token-limited models require real token volume; short messages cannot bypass it.
        if self.token_context_limits:
            return context_tokens is not None and context_tokens >= self.idle_context_token_threshold
        return (
            message_count >= self.idle_effective_message_threshold
            or total_bytes >= self.idle_context_byte_threshold
        )

    def should_rollover_on_hard_limit(
        self,
        *,
        context_tokens: int | None,
        total_bytes: int,
    ) -> bool:
        if self.token_context_limits:
            return (
                context_tokens is not None
                and context_tokens >= self.active_context_token_threshold
            )
        return (
            bool(getattr(self.framework, "uses_project_compaction", False))
            and total_bytes >= self.hard_context_byte_threshold
        )

    async def prepare_turn(
        self,
        window: ContextWindow,
        *,
        count_tokens: Callable[[str, ContextWindow], Awaitable[int | None]] | None = None,
        summarize_fn: Callable[..., object] | None = None,
        now: datetime | None = None,
    ) -> PreparedSession:
        assert self.sessions is not None
        user_id, base_session_id = session_keys(window.account_id, window.conversation_id)
        route = route_hash(user_id, base_session_id)
        state = self.route_state(route)
        state["account_id"] = window.account_id
        state["conversation_id"] = window.conversation_id
        generation = int(state["generation"])
        safety_recovery_turn = state.get("safety_recovery_generation") == generation
        summary_only_turn = state.get("summary_only_generation") == generation
        new_session = False
        session_id = window_session_id(base_session_id, generation)
        session = await self.sessions.get_session(
            app_name="wechat_agent_mock",
            user_id=user_id,
            session_id=session_id,
        )
        current_now = self._now(now)
        last_message_at = state.get("last_message_at")
        last_dt = _parse_utc(last_message_at)
        idle = self.is_idle_time(last_message_at, now=current_now)
        idle_seconds = (
            max(0.0, (current_now - last_dt).total_seconds())
            if last_dt is not None
            else 0.0
        )
        session_events = live_session_events(session.events) if session is not None else []
        session_bytes = sum(estimated_event_bytes(event) for event in session_events)
        incoming_bytes = sum(
            len(value.encode("utf-8"))
            for value in (
                window.model_user_content(),
                window.model_cross_chat_context(),
                window.model_reference_context(),
                window.model_session_context(),
                window.daily_prompt_context or "",
            )
        )
        context_tokens: int | None = None
        if (
            self.token_context_limits
            and session is not None
            and session_events
            and count_tokens is not None
        ):
            preview = (
                self.dedupe.filter_unseen(route, generation, window)[0]
                if self.dedupe is not None
                else window
            )
            context_tokens = await count_tokens(session_id, preview)

        message_count = 0
        maintenance_ready = False
        if session is not None and idle:
            user_events = [
                event for event in session_events if getattr(event, "author", None) == "user"
            ]
            if self.effective_message_count is not None and session_events:
                first_event_at = min(float(event.timestamp) for event in session_events)
                session_started = datetime.fromtimestamp(first_event_at, UTC)
                message_count = count_effective_messages(
                    self.effective_message_count,
                    window.account_id,
                    window.conversation_id,
                    session_started,
                )
            else:
                message_count = len(user_events)
            maintenance_ready = self.should_rollover_on_idle(
                context_tokens=context_tokens,
                message_count=message_count,
                total_bytes=session_bytes + incoming_bytes,
            )

        hard_limit = self.should_rollover_on_hard_limit(
            context_tokens=context_tokens,
            total_bytes=session_bytes + incoming_bytes,
        )
        cache_decision = evaluate_cache_recycle(
            idle_seconds=idle_seconds if last_dt is not None else 0.0,
            context_tokens=context_tokens,
            session_bytes=session_bytes,
            token_context_limits=self.token_context_limits,
            rule=(
                self.cache_recycle_rule
                if session is not None and session_events and last_dt is not None
                else None
            ),
        )
        if session is not None and (
            (idle and maintenance_ready) or hard_limit or cache_decision.recycle
        ):
            summary_reason = (
                "active_context_threshold"
                if hard_limit
                else ("eight_hour_idle_rollover" if (idle and maintenance_ready) else cache_decision.reason)
            )
            log_reason = (
                "active_context_threshold"
                if hard_limit
                else ("eight_hour_threshold" if (idle and maintenance_ready) else cache_decision.reason)
            )
            summary = await self.summarize_session_with_retries(
                session,
                user_id,
                reason=summary_reason,
                summarize_fn=summarize_fn,
            )
            state["latest_summary"] = summary
            idle_hours = int(idle_seconds // 3600) if last_dt is not None else 0
            logger.info(
                "event=session_rotated route=%s old_generation=%s reason=%s "
                "message_count=%s event_bytes=%s incoming_bytes=%s idle_hours=%s context_tokens=%s",
                route[:12],
                generation,
                log_reason,
                message_count,
                session_bytes,
                incoming_bytes,
                idle_hours,
                context_tokens,
            )
            generation += 1
            state["generation"] = generation
            state["last_message_at"] = None
            self.save_route_state(route, state)
            if self.dedupe is not None:
                self.dedupe.prune_before_generation(route, generation)
            session_id = window_session_id(base_session_id, generation)
            session = None

        if (
            (session is None or not session_events)
            and self.initial_summary
            and not state.get("latest_summary")
        ):
            state["latest_summary"] = self.initial_summary(
                window.account_id, window.conversation_id
            )

        if session is None:
            await self.sessions.create_session(
                app_name="wechat_agent_mock",
                user_id=user_id,
                session_id=session_id,
            )
            new_session = True
        elif self.dedupe is not None:
            # Seed the dedupe ledger from pre-migration Session events so a
            # deployed runner does not re-inject rows that were already shown.
            self.dedupe.seed_from_session(route, generation, session)

        return PreparedSession(
            user_id=user_id,
            base_session_id=base_session_id,
            route=route,
            state=state,
            generation=generation,
            session_id=session_id,
            session=session,
            session_events=session_events,
            session_bytes=session_bytes,
            new_session=new_session,
            safety_recovery_turn=safety_recovery_turn,
            summary_only_turn=summary_only_turn,
        )

    def build_rollover_history(
        self,
        prepared: PreparedSession,
        window: ContextWindow,
    ) -> tuple[str, int]:
        hydrate_first = bool(getattr(self.framework, "hydrate_first_session", False))
        if not ((prepared.new_session or not prepared.session_events) and (
            prepared.generation > 0 or hydrate_first
        )):
            return "", 0
        records = [] if prepared.summary_only_turn else window.model_history_records()
        history = self.history_context(prepared.state.get("latest_summary"), records)
        if prepared.summary_only_turn:
            logger.info(
                "event=safety_fallback_context_assembled route=%s generation=%s summary_present=%s",
                prepared.route[:12],
                prepared.generation,
                bool(prepared.state.get("latest_summary")),
            )
        return history, len(records)

    async def finalize_turn(
        self,
        prepared: PreparedSession,
        *,
        after_run: object,
        safety_block_code: str | None,
        summarize_fn: Callable[..., object] | None = None,
        freshness_held: bool = False,
        now: datetime | None = None,
    ) -> int:
        state = prepared.state
        generation = prepared.generation
        if freshness_held and safety_block_code is None:
            if prepared.new_session and self.sessions is not None:
                try:
                    await self.sessions.delete_session(
                        app_name="wechat_agent_mock",
                        user_id=prepared.user_id,
                        session_id=prepared.session_id,
                    )
                except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                    logger.debug(
                        "event=held_session_cleanup_failed route=%s error_type=%s",
                        prepared.route[:12],
                        type(exc).__name__,
                    )
            return generation
        if safety_block_code is not None:
            summary: str | None = None
            if after_run is not None:
                summary = await self.summarize_session_with_retries(
                    after_run,
                    prepared.user_id,
                    reason="safety_block",
                    summarize_fn=summarize_fn,
                )
            state["latest_summary"] = summary
            # Use the same summary and generation rollover as idle maintenance. The
            # next turn will use the common history assembler with this summary.
            if self.sessions is not None:
                try:
                    await self.sessions.delete_session(
                        app_name="wechat_agent_mock",
                        user_id=prepared.user_id,
                        session_id=prepared.session_id,
                    )
                except Exception as exc:  # noqa: BLE001 - generation rotation still isolates it
                    logger.error(
                        "event=safety_block_session_delete_failed route=%s error_type=%s",
                        prepared.route[:12],
                        type(exc).__name__,
                    )
            generation += 1
            if prepared.safety_recovery_turn or prepared.summary_only_turn:
                state["safety_recovery_generation"] = None
                state["summary_only_generation"] = generation
                recovery_stage = "summary_only"
            else:
                state["safety_recovery_generation"] = generation
                state["summary_only_generation"] = None
                recovery_stage = "shared_history"
            logger.warning(
                "event=model_safety_block_isolated route=%s code=%s summary_saved=%s "
                "next_context=%s source_redaction=not_attributed",
                prepared.route[:12],
                safety_block_code,
                bool(summary),
                recovery_stage,
            )
        elif prepared.safety_recovery_turn:
            state["safety_recovery_generation"] = None
        elif prepared.summary_only_turn:
            state["summary_only_generation"] = None
        state["generation"] = generation
        state["last_message_at"] = (
            None if safety_block_code is not None else self._now(now).isoformat()
        )
        self.save_route_state(prepared.route, state)
        return generation

    async def summarize_idle_sessions(
        self,
        *,
        count_tokens: Callable[..., Awaitable[int | None]] | None = None,
        summarize_fn: Callable[..., object] | None = None,
        now: datetime | None = None,
    ) -> int:
        """Summarize each idle session once, including after service downtime."""
        assert self.sessions is not None
        summarized = 0
        routes = self.list_active_routes()
        current_now = self._now(now)
        for route, state in routes:
            last_value = state.get("last_message_at")
            account_id = state.get("account_id")
            conversation_id = state.get("conversation_id")
            if not all(
                isinstance(value, str) and value
                for value in (last_value, account_id, conversation_id)
            ):
                continue
            assert isinstance(last_value, str)
            assert isinstance(account_id, str)
            assert isinstance(conversation_id, str)
            if (
                self.profile_for_conversation is not None
                and self.profile_for_conversation(account_id, conversation_id) != self.profile_id
            ):
                continue
            last = _parse_utc(last_value)
            if last is None:
                continue
            idle_seconds = max(0.0, (current_now - last).total_seconds())
            cache_rule = self.cache_recycle_rule
            cache_window_elapsed = (
                cache_rule is not None
                and cache_rule.idle_seconds is not None
                and idle_seconds > cache_rule.idle_seconds
            )
            user_id, base_session_id = session_keys(account_id, conversation_id)
            generation = int(state["generation"])
            session_id = window_session_id(base_session_id, generation)
            session = await self.sessions.get_session(
                app_name="wechat_agent_mock",
                user_id=user_id,
                session_id=session_id,
            )
            if session is None:
                continue
            live_events = live_session_events(session.events)
            if not live_events:
                continue
            event_bytes = sum(estimated_event_bytes(event) for event in live_events)
            user_events = [
                event for event in live_events if getattr(event, "author", None) == "user"
            ]
            if self.effective_message_count is not None:
                first_event_at = min(float(event.timestamp) for event in live_events)
                session_started = datetime.fromtimestamp(first_event_at, UTC)
                user_message_count = count_effective_messages(
                    self.effective_message_count,
                    account_id,
                    conversation_id,
                    session_started,
                )
            else:
                user_message_count = len(user_events)
            if self.last_human_message_age is not None:
                human_age_minutes = self.last_human_message_age(account_id, conversation_id)
                idle = (
                    human_age_minutes is not None
                    and human_age_minutes >= self.idle_session_hours * 60
                )
            else:
                idle = current_now - last >= timedelta(hours=self.idle_session_hours)
            if not (idle or cache_window_elapsed):
                continue
            context_tokens = None
            if self.token_context_limits and count_tokens is not None:
                try:
                    context_tokens = await count_tokens(session_id=session_id)
                except ModelProviderUnavailable:
                    raise
                except Exception as exc:  # noqa: BLE001 - counter outage must not abort other routes
                    logger.warning(
                        "event=idle_context_count_failed route=%s error_type=%s",
                        route[:12],
                        type(exc).__name__,
                    )
                    continue
            high_context = idle and self.should_rollover_on_idle(
                context_tokens=context_tokens,
                message_count=user_message_count,
                total_bytes=event_bytes,
            )
            cache_decision = evaluate_cache_recycle(
                idle_seconds=idle_seconds,
                context_tokens=context_tokens,
                session_bytes=event_bytes,
                token_context_limits=self.token_context_limits,
                rule=cache_rule,
            )
            if not (high_context or cache_decision.recycle):
                continue
            reason = "eight_hour_idle" if high_context else cache_decision.reason
            with model_usage_scope(
                account_id,
                conversation_id,
                profile_id=self.profile_id,
                daily_budget_microusd=self.profile_daily_budget_microusd,
                text_budget_microusd=self.profile_text_budget_microusd,
                image_read_budget_microusd=self.profile_image_read_budget_microusd,
                image_generation_budget_microusd=self.profile_image_generation_budget_microusd,
            ):
                summary = await self.summarize_session_with_retries(
                    session,
                    user_id,
                    reason=reason,
                    summarize_fn=summarize_fn,
                )
            state["latest_summary"] = summary
            state["generation"] = generation + 1
            state["last_message_at"] = None
            self.save_route_state(route, state)
            if self.dedupe is not None:
                self.dedupe.prune_before_generation(route, generation + 1)
            summarized += 1
            logger.info(
                "event=session_summarized route=%s generation=%s reason=%s "
                "idle_hours=%s message_count=%s event_bytes=%s context_tokens=%s",
                route[:12],
                generation,
                "eight_hour_idle_and_threshold" if high_context else cache_decision.reason,
                int(idle_seconds // 3600),
                user_message_count,
                event_bytes,
                context_tokens,
            )
        return summarized

    async def delete_conversation(self, account_id: str, conversation_id: str) -> None:
        assert self.sessions is not None
        user_id, base_session_id = session_keys(account_id, conversation_id)
        route = route_hash(user_id, base_session_id)
        generation = self.generation(route)
        for index in range(generation + 1):
            session_id = window_session_id(base_session_id, index)
            if (
                await self.sessions.get_session(
                    app_name="wechat_agent_mock",
                    user_id=user_id,
                    session_id=session_id,
                )
                is not None
            ):
                await self.sessions.delete_session(
                    app_name="wechat_agent_mock",
                    user_id=user_id,
                    session_id=session_id,
                )
        self.save_route_state(route, {}, delete=True)
        if self.dedupe is not None:
            self.dedupe.delete_route(route)

    async def summarize_session(
        self, session: object, user_id: str, *, reason: str = "summarize"
    ) -> str | None:
        records = events_as_records(session)
        if not records:
            return None
        if getattr(self.summary_model, "offline", False):
            return None
        assert self.summary_framework is not None
        transcript = "\n".join(records)
        prompt = fill_prompt(self.summary_prompt, transcript=transcript)
        summary_service = self.summary_framework.sessions()
        summary_runner = self.summary_framework.runner(
            model=self.summary_model,
            sessions=summary_service,
            instruction=load_prompt(self.summary_system_prompt),
            app_name="wechat_session_summary",
        )
        summary_session_id = "summary-" + uuid4().hex
        try:
            await summary_service.create_session(
                app_name="wechat_session_summary",
                user_id=user_id,
                session_id=summary_session_id,
            )
            result = ""
            blocked = False
            async with asyncio.timeout(60):
                async for event in summary_runner.run_async(
                    user_id=user_id,
                    session_id=summary_session_id,
                    new_message=types.Content(
                        role="user",
                        parts=[types.Part.from_text(text=prompt)],
                    ),
                    run_config=RunConfig(max_llm_calls=1),
                ):
                    if str(getattr(event, "error_code", "")).upper() in {
                        "PROHIBITED_CONTENT",
                        "SAFETY",
                    }:
                        blocked = True
                    if event.is_final_response() and event.content:
                        result = "".join(part.text or "" for part in event.content.parts or [])
            if result.strip() and not blocked:
                return result.strip()[:2000]
        except ModelProviderUnavailable:
            if reason != "safety_block":
                raise
            logger.warning(
                "event=session_summary_failed error_type=%s",
                "ModelProviderUnavailable",
            )
        except Exception as exc:  # noqa: BLE001 - safety reset must not restore old history
            logger.warning(
                "event=session_summary_failed error_type=%s",
                type(exc).__name__,
            )
        finally:
            await summary_runner.close()
            if hasattr(summary_service, "close"):
                await summary_service.close()
        return None

    async def summarize_session_with_retries(
        self,
        session: object,
        user_id: str,
        *,
        reason: str,
        summarize_fn: Callable[..., object] | None = None,
    ) -> str | None:
        """Use the same bounded summary path for idle and safety-triggered rollover."""
        fn = summarize_fn if summarize_fn is not None else self.summarize_session
        max_attempts = SUMMARY_MAX_RETRIES + 1
        try:
            accepts_reason = "reason" in inspect.signature(fn).parameters
        except (ValueError, TypeError):
            accepts_reason = False
        for attempt in range(1, max_attempts + 1):
            try:
                if accepts_reason:
                    summary = await fn(session, user_id, reason=reason)
                else:
                    summary = await fn(session, user_id)
            except ModelProviderUnavailable:
                if reason != "safety_block":
                    raise
                summary = None
            if summary:
                if attempt > 1:
                    logger.info(
                        "event=session_summary_retry_succeeded reason=%s attempt=%s",
                        reason,
                        attempt,
                    )
                return summary
            logger.warning(
                "event=session_summary_attempt_failed reason=%s attempt=%s max_attempts=%s",
                reason,
                attempt,
                max_attempts,
            )
        logger.error(
            "event=session_summary_retries_exhausted reason=%s retries=%s",
            reason,
            SUMMARY_MAX_RETRIES,
        )
        return None

    def history_context(self, summary: object, records: list[str]) -> str:
        return format_rollover_history(
            summary,
            records,
            max_history_records=self.max_history_records,
            max_history_bytes=self.max_history_bytes,
        )
