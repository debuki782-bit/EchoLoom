"""Pinned Hook callback decoding; no application policy is granted here."""
import json

from ..contacts import is_contact_id
from ..connectors.base import MessageEvent, SenderOrigin


def archive_envelope(item: MessageEvent, *, preserve_raw: bool = False) -> dict:
    """Keep the two established archive representations byte-for-byte compatible."""
    if preserve_raw and item.raw_envelope_json:
        return json.loads(item.raw_envelope_json)
    return {
        "fromid": item.conversation_id if preserve_raw or item.is_group else item.sender_id,
        "toid": item.account_id,
        "room": item.conversation_id if item.is_group else "",
        "sender": item.sender_id,
        "msg": item.text,
        "msgtype": item.message_type if preserve_raw else item.original_message_type or item.message_type,
        "msgsvrid": item.source_msg_id or 0,
        "time": item.capture_tick_ms,
    }


def parse_callback(payload: object) -> tuple[MessageEvent, ...]:
    if not isinstance(payload, dict):
        raise TypeError("callback must be an object")
    direction = payload.get("sendorrecv")
    messages = payload.get("msglist")
    if type(direction) is not int or direction != 2:
        raise ValueError("only inbound callbacks are accepted")
    if not isinstance(messages, list) or not 1 <= len(messages) <= 64:
        raise ValueError("msglist must contain 1 to 64 messages")
    account = payload.get("selfwxid")
    if not isinstance(account, str) or not account:
        first = messages[0]
        account = first.get("toid") if isinstance(first, dict) else None
        if not isinstance(account, str) or not account:
            raise ValueError("selfwxid or first message toid is required")
    raw_envelope = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                              separators=(",", ":"))
    return tuple(_parse_message(account, message, raw_envelope) for message in messages)


def _parse_message(account: str, value: object, raw_envelope_json: str) -> MessageEvent:
    if not isinstance(value, dict):
        raise TypeError("message must be an object")
    from_id = value.get("fromid")
    to_id = value.get("toid")
    room = value.get("room")
    sender = value.get("sender")
    body = value.get("msg")
    if not isinstance(from_id, str) or not from_id:
        raise ValueError("fromid is required")
    if not isinstance(to_id, str) or not isinstance(room, str):
        raise TypeError("message routing fields must be strings")
    if not isinstance(sender, str) or not isinstance(body, str):
        raise TypeError("message identity and body fields must be strings")
    msg_type = value.get("msgtype")
    source_id = value.get("msgsvrid")
    capture_tick = value.get("time")
    if (
        not isinstance(msg_type, int) or isinstance(msg_type, bool)
        or not isinstance(source_id, int) or isinstance(source_id, bool)
        or not isinstance(capture_tick, int) or isinstance(capture_tick, bool)
    ):
        raise TypeError("message type, source id and capture tick must be integers")
    if capture_tick < 0 or source_id < 0:
        raise ValueError("negative source metadata")
    is_group = bool(room)
    if is_group:
        if room != from_id or not room.endswith("@chatroom") or not sender:
            raise ValueError("invalid group routing shape")
        if msg_type == 1:
            if sender == room or not is_contact_id(sender):
                raise ValueError("group text requires a stable member wxid")
            if not body.startswith(sender + ":\n"):
                raise ValueError("group sender does not match native content prefix")
        conversation_id = room
        sender_origin: SenderOrigin = "hook_group_member_wxid"
    else:
        if (
            sender != from_id or not to_id or to_id == from_id
            or from_id == account or from_id.endswith("@chatroom")
        ):
            raise ValueError("invalid private routing shape")
        conversation_id = from_id
        sender_origin = "native_private_fromid"
    return MessageEvent(
        account_id=account,
        conversation_id=conversation_id,
        sender_id=sender,
        sender_origin=sender_origin,
        text=body,
        is_group=is_group,
        message_type=msg_type,
        source_msg_id=source_id if source_id > 0 else None,
        capture_tick_ms=capture_tick,
        raw_envelope_json=raw_envelope_json,
        raw_message_json=json.dumps(value, ensure_ascii=False, sort_keys=True,
                                    separators=(",", ":")),
        original_message_type=msg_type,
    )
