"""Pure session-key, history estimation, and rollover text helpers (zero SDK/IO)."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

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


def live_session_events(events: Iterable[Any] | None) -> list[Any]:
    """Return session events that survive rewind markers without importing framework SDKs."""
    items = list(events) if events is not None else []
    if not items:
        return []
    first_idx_by_inv: dict[object, int] | None = None
    kept: list[Any] = []
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
    """Text-scale estimate for tier thresholds; binary payloads get a fixed allowance."""
    total = 128
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


def _parse_utc(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


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


def format_rollover_history(
    summary: object,
    records: list[str],
    *,
    max_history_records: int,
    max_history_bytes: int,
) -> str:
    """Render bounded previous-session summary and recent history block."""
    entries: list[str] = []
    remaining = max_history_bytes

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
    record_limit = max(0, max_history_records - len(entries))
    selected_records: list[str] = []
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
