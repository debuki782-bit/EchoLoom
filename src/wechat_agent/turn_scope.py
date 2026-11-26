"""Turn-local ContextVars and scope helpers shared across scheduling, runtime, and tools."""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from contextlib import contextmanager
from contextvars import ContextVar

from .context import ContextWindow, _safe_identity_name

operation_id: ContextVar[str] = ContextVar("wechat_operation", default="")
operation_receipt_ids: ContextVar[tuple[str, ...]] = ContextVar(
    "wechat_operation_receipts", default=(),
)
quote_context: ContextVar[tuple[str, str, str, str] | None] = ContextVar(
    "wechat_quote_context", default=None,
)
memory_context: ContextVar[dict[str, object] | None] = ContextVar(
    "wechat_memory_context", default=None,
)
active_conversation: ContextVar[str | None] = ContextVar(
    "wechat_active_conversation", default=None,
)
followup_scheduler: ContextVar[
    Callable[[str, int], Awaitable[dict[str, object]]] | None
] = ContextVar(
    "wechat_followup_scheduler", default=None,
)


def tool_memory_context(window: ContextWindow) -> dict[str, object]:
    """Use the same visible messages and participant choices for every activation."""
    visible = tuple(
        turn for turn in (
            *window.turns, *window.history_turns,
            *window.recent_other_turns, *window.global_timeline_turns,
        ) if turn.message_id and turn.sender_origin not in (
            "cross_session_task", "idle_task", "idle_finalize",
        )
    )
    current_ids = tuple(turn.message_id for turn in window.turns if turn in visible)
    current_senders = tuple(
        dict.fromkeys(
            turn.sender_id
            for turn in window.turns
            if turn in visible and turn.direction != "outbound" and turn.sender_id
        )
    )
    conversations = {
        turn.message_id: turn.source_conversation_id or window.conversation_id
        for turn in visible
    }
    trusted_ids = {window.account_id} | {
        str(row["sender_id"]) for row in window.priority_sender_references
        if isinstance(row.get("sender_id"), str)
    }
    participants = sorted({
        turn.sender_id for turn in visible
        if window.is_idle_event or (
            turn.source_conversation_id or window.conversation_id
        ) == window.conversation_id
    })
    return {
        "account_id": window.account_id,
        "conversation_id": window.conversation_id,
        "message_id": current_ids[-1] if current_ids else "",
        "current_message_ids": current_ids,
        "current_sender_ids": current_senders,
        "is_idle_event": bool(window.is_idle_event),
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
        "allowed_conversations": tuple(sorted(
            set(conversations.values()) | {window.conversation_id}
        )),
    }


def current_turn_owner_authorized(
    command_owner_ids: frozenset[str] | set[str],
) -> tuple[bool, str]:
    """Allow privileged file writes/sends only when every human sender in the turn is a configured owner."""
    if not command_owner_ids:
        return False, "owner_ids_not_configured"
    ctx = memory_context.get()
    if not ctx:
        return False, "turn_context_unavailable"
    if ctx.get("is_idle_event"):
        return False, "idle_turn_cannot_run_owner_action"
    senders = ctx.get("current_sender_ids")
    if not isinstance(senders, (tuple, list)) or not senders:
        return False, "no_current_human_sender"
    if any(not isinstance(sid, str) or sid not in command_owner_ids for sid in senders):
        return False, "owner_permission_required"
    return True, "ok"


@contextmanager
def tool_turn_scope(
    window: ContextWindow,
    operation: str,
    *,
    receipt_ids=(),
    quote=None,
    schedule=None,
):
    """Bind and restore target-local tool state, including cancellation and nesting."""
    bindings: tuple[tuple[ContextVar, object], ...] = (
        (operation_id, operation),
        (operation_receipt_ids, tuple(receipt_ids)),
        (active_conversation, window.conversation_id),
        (memory_context, tool_memory_context(window)),
        (quote_context, quote),
        (followup_scheduler, schedule),
    )
    tokens = [(variable, variable.set(value)) for variable, value in bindings]
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)
