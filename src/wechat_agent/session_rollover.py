"""Session routing, rollover policy, summarization, and safety-block recovery."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import logging
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from uuid import uuid4

from . import runtime_contract as types
from .agent_profiles import LUNA_ACTIVE_COMPRESSION_TOKENS, LUNA_IDLE_COMPRESSION_TOKENS
from .context import ContextWindow
from .prompt_catalog import fill_prompt, load_prompt
from .runtime_contract import AgentFramework, ModelProviderUnavailable
from .runtime_contract import RunOptions as RunConfig
from .runtime_scope import model_usage_scope
from .usage import (
    DAILY_ACCOUNT_BUDGET_MICROUSD,
    IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
    IMAGE_READ_DAILY_BUDGET_MICROUSD,
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
)

if TYPE_CHECKING:
    from .cross_chat_dedupe import CrossChatDedupeStore

logger = logging.getLogger(__name__)

HISTORY_START = "【历史会话内容开始】\n"
HISTORY_END = "\n【历史会话内容结束】\n\n"
SUMMARY_MAX_RETRIES = 3

MODEL_CONTEXT_LIMITS: dict[str, tuple[int, int]] = {
    # Token threshold for model-side compaction; byte threshold for project maintenance.
    "gemini-3.5-flash-lite": (30_000, 80_000),
    "gemini-3.8-flash": (45_000, 120_000),
    "gemini-4-hydrogen": (45_000, 120_000),
    "gemini-4-argon": (60_000, 160_000),
}


def session_keys(account_id: str, conversation_id: str) -> tuple[str, str]:
    if not account_id or not conversation_id or "\0" in account_id + conversation_id:
        raise ValueError("invalid session identity")
    return (
        "account-" + hashlib.sha256(account_id.encode()).hexdigest(),
        "identity-safe-v2-conversation-"
        + hashlib.sha256(f"{account_id}\0{conversation_id}".encode()).hexdigest(),
    )


def route_hash(user_id: str, base_session_id: str) -> str:
    return hashlib.sha256(f"{user_id}\0{base_session_id}".encode()).hexdigest()


def window_session_id(base_session_id: str, generation: int) -> str:
    return base_session_id if generation == 0 else f"{base_session_id}-window-{generation}"


def strip_injected_history(text: str) -> str:
    """Remove the injected rollover block before saving a turn as history."""
    markers = (
        (HISTORY_START, HISTORY_END),
        ("【历史会话参考开始】\n", "\n【历史会话参考结束】\n\n"),
    )
    for start, finish in markers:
        if text.startswith(start):
            end = text.find(finish, len(start))
            if end >= 0:
                return text[end + len(finish) :]
    return text


def live_session_events(events: object) -> list[object]:
    """Return session events that survive rewind markers without importing framework SDKs."""
    items = list(events) if events is not None else []
    if not items:
        return []
    first_idx_by_inv: dict[object, int] | None = None
    kept: list[object] = []
    i = len(items) - 1
    while i >= 0:
        event = items[i]
        rewind_target = getattr(
            getattr(event, "actions", None),
            "rewind_before_invocation_id",
            None,
        )
        if rewind_target:
            if first_idx_by_inv is None:
                first_idx_by_inv = {}
                for idx, item in enumerate(items):
                    invocation_id = getattr(item, "invocation_id", None)
                    if invocation_id and invocation_id not in first_idx_by_inv:
                        first_idx_by_inv[invocation_id] = idx
            rewind_idx = first_idx_by_inv.get(rewind_target)
            if rewind_idx is not None and rewind_idx < i:
                i = rewind_idx
        else:
            kept.append(event)
        i -= 1
    kept.reverse()
    return kept


def estimated_event_bytes(event: object) -> int:
    """Text-scale estimate for tier thresholds; binary payloads get a fixed allowance.

    Multimodal parts serialize as base64 and would dwarf any text budget, so an
    archived image counts as a fixed allowance (~2,000 bytes) instead of its raw
    size. Tool parts get small flat allowances: this gates maintenance tiers,
    it does not replace the real token counter.
    """
    total = 128  # per-event envelope
    content = getattr(event, "content", None)
    for part in getattr(content, "parts", None) or []:
        text = getattr(part, "text", None)
        if isinstance(text, str) and text:
            total += len(text.encode("utf-8"))
            continue
        if (
            getattr(part, "inline_data", None) is not None
            or getattr(part, "file_data", None) is not None
        ):
            total += 2000
            continue
        fc = getattr(part, "function_call", None)
        if fc is not None:
            args_obj = getattr(fc, "args", None)
            args_bytes = (
                len(json.dumps(args_obj, ensure_ascii=False, default=str).encode("utf-8"))
                if args_obj
                else 0
            )
            total += max(512, min(32_768, args_bytes + 128))
            continue
        fr = getattr(part, "function_response", None)
        if fr is not None:
            resp_obj = getattr(fr, "response", None)
            resp_bytes = (
                len(json.dumps(resp_obj, ensure_ascii=False, default=str).encode("utf-8"))
                if resp_obj
                else 0
            )
            fr_images = sum(
                2000
                for sub in (getattr(fr, "parts", None) or [])
                if getattr(sub, "inline_data", None) is not None
                or getattr(sub, "file_data", None) is not None
            )
            total += max(1024, min(32_768, resp_bytes + 128)) + fr_images
            continue
        total += 256
    return total


def events_as_records(session: object) -> list[str]:
    records: list[str] = []
    for event in live_session_events(getattr(session, "events", None)):
        author = getattr(event, "author", "")
        if author not in ("user", "wechat_mock_agent"):
            continue
        if author != "user" and not event.is_final_response():
            continue
        content = getattr(event, "content", None)
        text = "".join(part.text or "" for part in getattr(content, "parts", []) or [])
        text = strip_injected_history(text).strip()
        if author == "user":
            text = text.split("\n\n【此时此刻其它聊天串（非本会话）近况】", 1)[0].strip()
        if text:
            label = "用户上下文" if author == "user" else "Agent 回复"
            records.append(f"[{label}] {text[:8000]}")
    return records


def count_effective_messages(
    fn: Callable[..., int] | None,
    account_id: str,
    conversation_id: str,
    since: datetime,
) -> int:
    """Invoke effective_message_count with positional or keyword-only signature."""
    if fn is None:
        return 0
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):
        return int(fn(account_id, conversation_id, since))
    try:
        sig.bind(account_id, conversation_id, since)
    except TypeError:
        return int(
            fn(
                account_id=account_id,
                conversation_id=conversation_id,
                since=since,
            )
        )
    return int(fn(account_id, conversation_id, since))


@dataclass
class PreparedSession:
    """Resolved session state for a single turn after rollover checks."""

    user_id: str
    base_session_id: str
    route: str
    state: dict[str, object]
    generation: int
    session_id: str
    session: object
    session_events: list[object]
    session_bytes: int
    new_session: bool
    safety_recovery_turn: bool
    summary_only_turn: bool


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
        if self.routing_db is not None:
            self._ensure_schema(self.routing_db)

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
        state["last_message_at"] = (now or datetime.now(UTC)).isoformat()
        self.save_route_state(route, state)

    def register_route(self, route: str, account_id: str, conversation_id: str) -> None:
        state = self.route_state(route)
        state["account_id"] = account_id
        state["conversation_id"] = conversation_id
        self.save_route_state(route, state)

    def is_idle_time(self, last_message_at: object, *, now: datetime | None = None) -> bool:
        if not isinstance(last_message_at, str) or not last_message_at:
            return False
        try:
            last = datetime.fromisoformat(last_message_at)
            if last.tzinfo is None:
                last = last.replace(tzinfo=UTC)
        except ValueError:
            return False
        return (now or datetime.now(UTC)) - last.astimezone(UTC) >= timedelta(
            hours=self.idle_session_hours
        )

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
        now = datetime.now(UTC)
        last_message_at = state.get("last_message_at")
        idle = self.is_idle_time(last_message_at, now=now)
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
        if session is not None and ((idle and maintenance_ready) or hard_limit):
            if idle or hard_limit:
                summary = await self.summarize_session_with_retries(
                    session,
                    user_id,
                    reason=(
                        "active_context_threshold" if hard_limit else "eight_hour_idle_rollover"
                    ),
                    summarize_fn=summarize_fn,
                )
                state["latest_summary"] = summary
            idle_hours = 0
            if idle and isinstance(last_message_at, str):
                last_dt = datetime.fromisoformat(last_message_at)
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=UTC)
                idle_hours = int((now - last_dt.astimezone(UTC)).total_seconds() // 3600)
            logger.info(
                "event=session_rotated route=%s old_generation=%s reason=%s "
                "message_count=%s event_bytes=%s incoming_bytes=%s idle_hours=%s context_tokens=%s",
                route[:12],
                generation,
                "active_context_threshold" if hard_limit else "eight_hour_threshold",
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
    ) -> int:
        state = prepared.state
        generation = prepared.generation
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
            None if safety_block_code is not None else datetime.now(UTC).isoformat()
        )
        self.save_route_state(prepared.route, state)
        return generation

    async def summarize_idle_sessions(
        self,
        *,
        count_tokens: Callable[..., Awaitable[int | None]] | None = None,
        summarize_fn: Callable[..., object] | None = None,
    ) -> int:
        """Summarize each idle session once, including after service downtime."""
        assert self.sessions is not None
        summarized = 0
        routes = self.list_active_routes()
        now = datetime.now(UTC)
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
            try:
                last = datetime.fromisoformat(last_value)
                if last.tzinfo is None:
                    last = last.replace(tzinfo=UTC)
            except ValueError:
                continue
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
                idle = now - last.astimezone(UTC) >= timedelta(hours=self.idle_session_hours)
            if not idle:
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
            high_context = self.should_rollover_on_idle(
                context_tokens=context_tokens,
                message_count=user_message_count,
                total_bytes=event_bytes,
            )
            if not high_context:
                continue
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
                    reason="eight_hour_idle",
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
                "eight_hour_idle_and_threshold",
                int((now - last.astimezone(UTC)).total_seconds() // 3600),
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
        entries: list[str] = []
        remaining = self.max_history_bytes

        def clipped_tail(value: str, budget: int) -> str:
            raw = value.encode("utf-8")
            if len(raw) <= budget:
                return value
            marker = "…（较早内容已截断）…"
            marker_bytes = marker.encode("utf-8")
            if budget < len(marker_bytes):
                return ""
            tail_budget = budget - len(marker_bytes)
            return marker + (raw[-tail_budget:].decode("utf-8", "ignore") if tail_budget else "")

        if isinstance(summary, str) and summary:
            summary_budget = min(remaining, 3000)
            prefix = "[上一会话摘要] "
            clipped_summary = prefix + clipped_tail(
                summary, max(0, summary_budget - len(prefix.encode("utf-8")))
            )
            if clipped_summary:
                entries.append(clipped_summary)
                remaining -= len(clipped_summary.encode("utf-8"))
        record_limit = max(0, self.max_history_records - len(entries))
        selected_records: list[str] = []
        # `records` are chronological. Spend the byte budget newest-first so an
        # older long message cannot displace the beginning of a newer one.
        newest_first = list(reversed(records))[:record_limit]
        for record in newest_first:
            if remaining <= 0:
                break
            clipped = clipped_tail(record, remaining)
            if clipped:
                selected_records.append(clipped)
                remaining -= len(clipped.encode("utf-8"))
        entries.extend(reversed(selected_records))
        if not entries:
            logger.info(
                "event=rollover_history_injected summary_present=%s records_included=0 bytes=0",
                bool(summary),
            )
            return ""
        rendered = "\n".join(entries)
        logger.info(
            "event=rollover_history_injected summary_present=%s records_included=%s bytes=%s",
            bool(summary),
            len(selected_records),
            len(rendered.encode("utf-8")),
        )
        return HISTORY_START + rendered + HISTORY_END
