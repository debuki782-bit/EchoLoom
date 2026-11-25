"""Contract checks for the diagnostic receiver; no live WeChat is required."""

import json
import runpy
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
probe = runpy.run_path(str(ROOT / "experiments" / "wechat-hook-poc" / "receiver.py"))
summarize = probe["summarize"]


def test_private_callback_shape_keeps_sender_but_not_body() -> None:
    fixture = json.loads((ROOT / "fixtures" / "hook-private-text-shape.json").read_text("utf-8"))
    item = fixture["msglist"][0]
    summary = summarize(item)
    serialized = json.dumps(summary, ensure_ascii=False)
    assert summary["sender_origin"] == "native_fromid"
    assert summary["is_group"] is False
    assert summary["from_sha"] == summary["sender_sha"]
    assert summary["source_id_nonzero"] is False
    assert summary["native_mention_ids_present"] is False
    assert item["msg"] not in serialized
    assert item["fromid"] not in serialized


def test_group_sender_is_explicitly_unverified_and_mention_is_not_inferred() -> None:
    item = {
        "fromid": "group-1@chatroom",
        "toid": "wxid_bot",
        "room": "group-1@chatroom",
        "sender": "wxid_member",
        "msgtype": 1,
        "msgsvrid": 0,
        "msg": "wxid_member:\n@机器人 早上好",
    }
    summary = summarize(item)
    assert summary["is_group"] is True
    assert summary["from_is_room"] is True
    assert summary["sender_is_room"] is False
    assert summary["sender_origin"] == "body_prefix_unverified"
    assert summary["native_mention_ids_present"] is False
    assert item["msg"] not in json.dumps(summary, ensure_ascii=False)


def test_invalid_message_cannot_enter_probe_history() -> None:
    with pytest.raises((TypeError, ValueError)):
        summarize({"fromid": "room@chatroom", "msgtype": True, "msgsvrid": 0})
