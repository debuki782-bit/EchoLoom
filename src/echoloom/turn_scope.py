"""Turn-local ContextVar scope consolidated into a single immutable ``TurnScopeState``."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, replace
from typing import Any

from .context import ContextWindow, _safe_identity_name
from .freshness_policy import FreshnessDecision


@dataclass(frozen=True)
class TurnScopeState:
    """Immutable per-turn scope shared across scheduling, runtime, and tools."""

    operation_id: str = ""
    operation_receipt_ids: tuple[str, ...] = ()
    quote_context: tuple[str, str, str, str] | None = None
    memory_context: dict[str, object] | None = None
    active_conversation: str | None = None
    followup_scheduler: Callable[[str, int], Awaitable[dict[str, object]]] | None = None
    freshness_guard: Callable[[], FreshnessDecision] | None = None
    freshness_held_flag: list[bool] | None = None
    freshness_thread_safe: bool = True


_DEFAULT_SCOPE = TurnScopeState()
_turn_scope: ContextVar[TurnScopeState | None] = ContextVar(
    "echoloom_turn_scope",
    default=None,
)


class _ScopeFieldVar[T]:
    """Copy-on-write field lens exposing ContextVar-compatible ``get``/``set``/``reset``."""

    def __init__(self, field_name: str) -> None:
        self._field_name = field_name

    def get(self) -> T:
        state = _turn_scope.get()
        return getattr(_DEFAULT_SCOPE if state is None else state, self._field_name)

    def set(self, value: T) -> Token[TurnScopeState | None]:
        state = _turn_scope.get() or _DEFAULT_SCOPE
        return _turn_scope.set(replace(state, **{self._field_name: value}))

    def reset(self, token: Token[TurnScopeState | None]) -> None:
        _turn_scope.reset(token)


operation_id: _ScopeFieldVar[str] = _ScopeFieldVar("operation_id")
operation_receipt_ids: _ScopeFieldVar[tuple[str, ...]] = _ScopeFieldVar(
    "operation_receipt_ids"
)
quote_context: _ScopeFieldVar[tuple[str, str, str, str] | None] = _ScopeFieldVar(
    "quote_context"
)
memory_context: _ScopeFieldVar[dict[str, object] | None] = _ScopeFieldVar(
    "memory_context"
)
active_conversation: _ScopeFieldVar[str | None] = _ScopeFieldVar("active_conversation")
followup_scheduler: _ScopeFieldVar[
    Callable[[str, int], Awaitable[dict[str, object]]] | None
] = _ScopeFieldVar("followup_scheduler")
freshness_guard: _ScopeFieldVar[Callable[[], FreshnessDecision] | None] = (
    _ScopeFieldVar("freshness_guard")
)
freshness_held_flag: _ScopeFieldVar[list[bool] | None] = _ScopeFieldVar(
    "freshness_held_flag"
)


def current_turn_scope() -> TurnScopeState:
    """Return the active turn scope or the empty default."""
    return _turn_scope.get() or _DEFAULT_SCOPE


def check_outbound_freshness() -> FreshnessDecision | None:
    """Evaluate any turn-scoped pre-send freshness guard."""
    guard = current_turn_scope().freshness_guard
    return guard() if guard is not None else None


def mark_turn_freshness_held() -> None:
    """Record that the current turn's outbound send was held for newer inbound messages."""
    flag = current_turn_scope().freshness_held_flag
    if flag is not None:
        flag[0] = True


def is_turn_freshness_held() -> bool:
    """Return True if the active turn held its send due to newer inbound messages."""
    flag = current_turn_scope().freshness_held_flag
    return bool(flag and flag[0])


def tool_memory_context(window: ContextWindow) -> dict[str, object]:
    """Use the same visible messages and participant choices for every activation."""
    visible = tuple(
        turn
        for turn in (
            *window.turns,
            *window.history_turns,
            *window.recent_other_turns,
            *window.global_timeline_turns,
        )
        if turn.message_id
        and turn.sender_origin
        not in (
            "cross_session_task",
            "idle_task",
            "idle_finalize",
        )
    )
    current_ids = tuple(turn.message_id for turn in window.turns if turn in visible)
    conversations = {
        turn.message_id: turn.source_conversation_id or window.conversation_id
        for turn in visible
    }
    trusted_ids = {window.account_id} | {
        str(row["sender_id"])
        for row in window.priority_sender_references
        if isinstance(row.get("sender_id"), str)
    }
    participants = sorted(
        {
            turn.sender_id
            for turn in visible
            if window.is_idle_event
            or (turn.source_conversation_id or window.conversation_id) == window.conversation_id
        }
    )
    return {
        "account_id": window.account_id,
        "conversation_id": window.conversation_id,
        "message_id": current_ids[-1] if current_ids else "",
        "current_message_ids": current_ids,
        "message_ids": tuple(dict.fromkeys(turn.message_id for turn in visible)),
        "message_conversations": conversations,
        "context_window": window,
        "sender_choices": [
            {
                "sender_id": sender_id,
                "display_name": _safe_identity_name(sender_id, identity, trusted_ids),
                "nickname": identity.nickname if identity else None,
                "remark": identity.remark if identity else None,
            }
            for sender_id in participants
            for identity in [window.identities.get(sender_id)]
        ],
        "allowed_conversations": tuple(
            sorted(set(conversations.values()) | {window.conversation_id})
        ),
    }


@contextmanager
def tool_turn_scope(
    window: ContextWindow,
    operation: str,
    *,
    receipt_ids: tuple[str, ...] | list[str] = (),
    quote: tuple[str, str, str, str] | None = None,
    schedule: Callable[[str, int], Awaitable[dict[str, object]]] | None = None,
    freshness: Callable[[], FreshnessDecision] | None = None,
    freshness_held: list[bool] | None = None,
    freshness_thread_safe: bool = True,
):
    """Bind and restore target-local tool state via one ContextVar token."""
    state = TurnScopeState(
        operation_id=operation,
        operation_receipt_ids=tuple(receipt_ids),
        quote_context=quote,
        memory_context=tool_memory_context(window),
        active_conversation=window.conversation_id,
        followup_scheduler=schedule,
        freshness_guard=freshness,
        freshness_held_flag=freshness_held,
        freshness_thread_safe=freshness_thread_safe,
    )
    token: Any = _turn_scope.set(state)
    try:
        yield state
    finally:
        _turn_scope.reset(token)
