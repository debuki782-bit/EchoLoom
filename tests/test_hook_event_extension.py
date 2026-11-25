import json
import sqlite3

from wechat_agent.adapters.hook_event_extension import event_envelope
from wechat_agent.adapters.hook_events import parse_callback
from wechat_agent.context import _message_text, build_hook_context
from wechat_agent.conversation_scheduler import ConversationScheduler
from wechat_agent.rich_message import normalize_message

BOT, MEMBER, OTHER = "wxid_bot_example", "wxid_member_example", "wxid_other_example"


def pat(**changes):
    return {"event": "pat", "from": MEMBER, "to": BOT, "type": 10002,
            "body": '<sysmsg type="pat"/>', "source_id": "7654321",
            "actor": MEMBER, "target": BOT, "chat": BOT, "suffix": "", **changes}


def test_private_pat_reaches_context_with_exact_actor_and_target():
    item = parse_callback(event_envelope(pat(), BOT, 123))[0]
    normalized = normalize_message(item)
    assert normalized.original_message_type == 10002
    context = build_hook_context([normalized])
    text, _ = _message_text(context.turns[0].text, context.turns[0], False, {})
    assert "[拍一拍]" in text and MEMBER in text and BOT in text
    assert normalized.sender_id == MEMBER and normalized.conversation_id == MEMBER


def test_group_pat_requires_exact_chat_and_stable_member():
    event = pat(**{"from": "room@chatroom", "chat": "room@chatroom", "target": OTHER})
    item = parse_callback(event_envelope(event, BOT, 123))[0]
    assert item.is_group and item.sender_id == MEMBER
    assert normalize_message(item) is not None
    assert event_envelope({**event, "chat": "other@chatroom"}, BOT, 123) is None
    assert event_envelope({**event, "actor": "nickname"}, BOT, 123) is None


def test_wrong_account_outbound_and_forged_actor_are_rejected():
    assert event_envelope(pat(to=OTHER), BOT, 123) is None
    assert event_envelope(pat(actor=BOT), BOT, 123) is None
    assert event_envelope(pat(actor=OTHER), BOT, 123) is None
    item = parse_callback(event_envelope(pat(), BOT, 123))[0]
    from dataclasses import replace
    assert normalize_message(replace(item, text=json.dumps({
        "kind": "pat_message", "actor_id": OTHER, "target_id": BOT}))) is None
    assert normalize_message(replace(item, text='ordinary system notification')) is None


def test_retries_use_same_server_identity_and_new_pat_has_new_identity():
    scheduler = ConversationScheduler(sqlite3.connect(":memory:"))
    try:
        first = normalize_message(parse_callback(event_envelope(pat(), BOT, 123))[0])
        again = normalize_message(parse_callback(event_envelope(pat(), BOT, 123))[0])
        new = normalize_message(parse_callback(event_envelope(pat(source_id="7654322"), BOT, 124))[0])
        assert scheduler.enqueue(first)[0] == "queued"
        assert scheduler.enqueue(again)[0] == "duplicate"
        assert scheduler.enqueue(new)[0] == "queued"
    finally:
        scheduler.db.close()


def test_only_long_cards_supplement_base_hook_and_keep_link_in_context():
    body = '<msg><appmsg><title>标题</title><type>4</type><url>https://b23.tv/test</url><extra>' + 'x' * 5000 + '</extra></appmsg></msg>'
    event = {"event": "long_card", "from": MEMBER, "to": BOT, "type": 49,
             "body": body, "source_id": "9876543"}
    item = normalize_message(parse_callback(event_envelope(event, BOT, 123))[0])
    context = build_hook_context([item])
    text, _ = _message_text(context.turns[0].text, context.turns[0], False, {})
    assert "https://b23.tv/test" in text and 'xxxxx' not in text
    assert event_envelope({**event, "body": "short"}, BOT, 123) is None
    assert event_envelope({**event, "type": 1}, BOT, 123) is None
    group = {**event, "from": "room@chatroom", "body": MEMBER + ':\n' + body}
    assert parse_callback(event_envelope(group, BOT, 123))[0].sender_id == MEMBER
    assert event_envelope({**group, "body": body}, BOT, 123) is None
