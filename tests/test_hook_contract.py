"""Contract checks use synthetic IDs with the verified private callback shape."""

import json
from pathlib import Path

import pytest

from echoloom.hook_contract import parse_callback


def fixture() -> dict[str, object]:
    path = Path(__file__).resolve().parents[1] / "fixtures" / "hook-private-text-shape.json"
    return json.loads(path.read_text(encoding="utf-8"))


def test_private_sender_can_enter_future_context_without_inventing_name() -> None:
    observed = parse_callback(fixture())
    assert len(observed) == 1
    message = observed[0]
    assert message.account_id == "sample_self_alias"
    assert message.conversation_id == "wxid_sender_example"
    assert message.sender_id == "wxid_sender_example"
    assert message.sender_origin == "native_private_fromid"
    assert message.display_name is None
    assert message.source_msg_id is None
    assert message.message_type == 1
    assert message.capture_tick_ms == 123456789
    assert message.text not in repr(message)
    assert message.sender_id not in repr(message)


def test_group_sender_is_stable_hook_wxid_from_body_prefix() -> None:
    callback = fixture()
    callback["msglist"] = [{
        "fromid": "test@chatroom", "toid": "sample_self_alias",
        "room": "test@chatroom", "sender": "wxid_member_candidate",
        "msg": "wxid_member_candidate:\n大家早上好", "msgtype": 1,
        "msgsvrid": 0, "time": 123456789,
    }]
    message = parse_callback(callback)[0]
    assert message.is_group
    assert message.conversation_id == "test@chatroom"
    assert message.sender_origin == "hook_group_member_wxid"
    assert message.source_msg_id is None
    assert message.capture_tick_ms == 123456789
    assert message.display_name is None


@pytest.mark.parametrize("field,value", [
    ("sender", "spoofed"), ("toid", "wxid_sender_example"),
    ("msgsvrid", True), ("time", -1),
])
def test_inconsistent_private_fields_are_rejected(field: str, value: object) -> None:
    callback = fixture()
    messages = callback["msglist"]
    assert isinstance(messages, list)
    messages[0][field] = value
    with pytest.raises((TypeError, ValueError)):
        parse_callback(callback)


def test_outgoing_frame_is_not_inbound_evidence() -> None:
    callback = fixture()
    callback["sendorrecv"] = 1
    with pytest.raises(ValueError, match="inbound"):
        parse_callback(callback)


def test_missing_top_level_selfwxid_uses_the_first_inbound_recipient():
    callback = fixture()
    callback.pop("selfwxid")
    assert parse_callback(callback)[0].account_id == "wxid_account_example"


def test_group_text_needs_a_member_candidate_and_room_route() -> None:
    callback = fixture()
    message = callback["msglist"][0]
    message.update({"fromid": "room@chatroom", "room": "room@chatroom",
                    "sender": "room@chatroom"})
    with pytest.raises(ValueError, match="stable member wxid"):
        parse_callback(callback)
    message["sender"] = "not-a-wxid"
    with pytest.raises(ValueError, match="stable member wxid"):
        parse_callback(callback)
    message["room"] = ""
    with pytest.raises(ValueError, match="private routing"):
        parse_callback(callback)
