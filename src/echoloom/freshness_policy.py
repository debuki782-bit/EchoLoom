"""Pure-function pre-send freshness policy inspired by blackboard hold/attach guards."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True, repr=False)
class PendingInboundSnapshot:
    """Minimal immutable view of a pending inbox message for freshness checks."""

    receipt_id: str
    sender_id: str
    text: str
    is_group: bool
    received_at: str
    source_msg_id: int | None = None
    last_error: str | None = None

    def __repr__(self) -> str:
        return (
            f"PendingInboundSnapshot(receipt_id={self.receipt_id[:12]!r}, "
            f"is_group={self.is_group})"
        )


@dataclass(frozen=True)
class FreshnessDecision:
    """Outcome of checking pending inbox arrivals before sending an outbound reply."""

    kind: Literal["commit", "hold"]
    reason: str
    conflicts: tuple[PendingInboundSnapshot, ...] = ()
    holds_used: int = 0


def _mentions_target(
    text: str,
    *,
    account_id: str,
    account_aliases: Sequence[str],
) -> bool:
    if not text:
        return False
    lowered = text.casefold()
    for alias in (account_id, *account_aliases):
        cleaned = alias.strip()
        if not cleaned:
            continue
        if f"@{cleaned.casefold()}" in lowered:
            return True
    return bool(account_id and account_id in text)


def _is_conflicting_inbound(
    item: PendingInboundSnapshot,
    *,
    account_id: str,
    is_group: bool,
    batch_sender_ids: frozenset[str],
    account_aliases: Sequence[str],
) -> bool:
    if item.sender_id == account_id:
        return False
    if not is_group:
        return True
    if item.sender_id in batch_sender_ids:
        return True
    return _mentions_target(
        item.text,
        account_id=account_id,
        account_aliases=account_aliases,
    )


def evaluate_outbound_freshness(
    *,
    account_id: str,
    is_group: bool,
    batch_sender_ids: Sequence[str],
    pending_inbound: Sequence[PendingInboundSnapshot],
    prior_holds: int = 0,
    max_holds: int = 1,
    account_aliases: Sequence[str] = (),
) -> FreshnessDecision:
    """Decide whether an outbound reply should commit or hold for newer conflicting messages."""
    if max_holds < 1 or prior_holds >= max_holds:
        return FreshnessDecision(
            kind="commit",
            reason="hold_limit_reached",
            holds_used=prior_holds,
        )
    if not pending_inbound:
        return FreshnessDecision(
            kind="commit",
            reason="no_pending_inbound",
            holds_used=prior_holds,
        )
    sender_set = frozenset(sender for sender in batch_sender_ids if sender)
    conflicts = tuple(
        item
        for item in pending_inbound
        if _is_conflicting_inbound(
            item,
            account_id=account_id,
            is_group=is_group,
            batch_sender_ids=sender_set,
            account_aliases=account_aliases,
        )
    )
    if conflicts:
        return FreshnessDecision(
            kind="hold",
            reason="newer_conflicting_inbound",
            conflicts=conflicts,
            holds_used=prior_holds,
        )
    return FreshnessDecision(
        kind="commit",
        reason="non_conflicting_inbound",
        holds_used=prior_holds,
    )
