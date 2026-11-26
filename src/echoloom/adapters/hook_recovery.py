"""Recover the latest validated Hook message when Pilot was offline."""

from __future__ import annotations

import json
import time
from dataclasses import replace

from ..connectors.base import MessageEvent
from .hook_events import parse_callback


def observation_from_hook_status(
    status: object,
    *,
    account_id: str,
    conversation_id: str,
) -> MessageEvent | None:
    """Build an idempotent inbound observation from Hook's latest status item.

    The Hook status endpoint exposes only its latest capture, not a durable
    queue. Its processor counter is used as the synthetic source fingerprint.
    The regular callback parser performs routing and sender-prefix validation.
    The observation keeps Hook's exact text shape; the shared context renderer
    removes the group-member prefix for model display.
    """
    if not isinstance(status, dict) or not conversation_id.endswith("@chatroom"):
        return None
    if (
        status.get("IsLogin") != 1
        or status.get("SyncBatchToUsername") != account_id
        or status.get("SyncBatchFromUsername") != conversation_id
    ):
        return None

    sequence = status.get("SyncBatchProcessorCalls")
    item_count = status.get("SyncBatchItemCount")
    message_type = status.get("SyncBatchLastMsgType")
    content = status.get("SyncBatchContent")
    if (
        not isinstance(sequence, int)
        or isinstance(sequence, bool)
        or sequence < 1
        or not isinstance(item_count, int)
        or isinstance(item_count, bool)
        or item_count < 1
        or message_type != 1
        or not isinstance(content, str)
        or not content
        or len(content.encode("utf-8")) > 4096
    ):
        return None

    # Stable synthetic `time` keeps the receipt identical after Agent restart;
    # the separate capture tick is current because Hook status has no event tick.
    source_record = {
        "fromid": conversation_id,
        "toid": account_id,
        "room": conversation_id,
        "sender": "",
        "msg": content,
        "msgtype": message_type,
        "msgsvrid": 0,
        "time": sequence,
    }
    # Group text uses `sender:\\nbody` in `msg`; extract sender through the
    # same callback contract as live Hook events.
    sender, separator, _body = content.partition(":\n")
    if not separator or sender == account_id:
        return None
    source_record["sender"] = sender
    payload = {
        "ServerPort": status.get("ServerPort", 30001),
        "msgnumber": 1,
        "sendorrecv": 2,
        "selfwxid": account_id,
        "msglist": [source_record],
    }
    try:
        item = parse_callback(payload)[0]
    except (TypeError, ValueError):
        return None
    if item.conversation_id != conversation_id or item.account_id != account_id:
        return None

    raw_record = json.loads(item.raw_message_json)
    raw_record["hook_status_recovery_sequence"] = sequence
    raw_message_json = json.dumps(
        raw_record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    raw_envelope = dict(payload)
    raw_envelope["msglist"] = [raw_record]
    return replace(
        item,
        capture_tick_ms=time.monotonic_ns() // 1_000_000,
        raw_message_json=raw_message_json,
        raw_envelope_json=json.dumps(
            raw_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ),
    )
