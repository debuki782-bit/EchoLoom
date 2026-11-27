"""Tests for out-of-the-box Telegram, Feishu, and Discord channel connectors."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import httpx

from echoloom.connectors import (
    ChannelConnector,
    ConnectorConfig,
    DiscordConnector,
    FeishuConnector,
    MessageEvent,
    TelegramConnector,
    create_gateway,
)
from echoloom.models import ConversationKey, ReplyDraft


def test_telegram_connector_auth_parse_send_and_fetch_image(tmp_path: Path):
    calls: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append((request.method, str(request.url)))
        if request.url.path.endswith("/sendMessage"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 99}})
        if request.url.path.endswith("/sendPhoto"):
            return httpx.Response(200, json={"ok": True, "result": {"message_id": 100}})
        if request.url.path.endswith("/getFile"):
            return httpx.Response(200, json={"ok": True, "result": {"file_path": "photos/p1.png"}})
        if request.url.path.endswith("/photos/p1.png"):
            return httpx.Response(200, content=b"PNGDATA", headers={"content-type": "image/png"})
        return httpx.Response(404)

    cfg = ConnectorConfig(
        workspace=tmp_path,
        account_id="tg_bot_1",
        send_enabled=True,
        source_id="telegram",
        channel="telegram",
        auth_token="123:ABC",
        hmac_secret="tg-hook-secret",
        outbound_url="https://api.telegram.org",
        transport=httpx.MockTransport(handler),
    )
    tg = create_gateway(cfg, factory="echoloom.connectors.telegram:TelegramConnector")
    assert isinstance(tg, TelegramConnector)
    assert isinstance(tg, ChannelConnector)

    assert tg.verify_request({"X-Telegram-Bot-Api-Secret-Token": "tg-hook-secret"}, b"{}")
    assert not tg.verify_request({"X-Telegram-Bot-Api-Secret-Token": "wrong"}, b"{}")
    assert not tg.verify_request({}, b"{}")

    events = tg.parse_events(
        {
            "update_id": 1,
            "message": {
                "message_id": 42,
                "date": 1_700_000_000,
                "chat": {"id": -100123, "type": "supergroup", "title": "Ops Group"},
                "from": {"id": 777, "first_name": "Alice", "last_name": "Chen"},
                "text": "Status check",
            },
        }
    )
    assert len(events) == 1
    ev = events[0]
    assert ev.channel == "telegram"
    assert ev.conversation_id == "-100123"
    assert ev.sender_id == "777"
    assert ev.is_group is True
    assert ev.source_msg_id == 42
    assert tg.resolve_contact("777").nickname == "Alice Chen"

    photo_events = tg.parse_events(
        {
            "update_id": 2,
            "message": {
                "message_id": 43,
                "date": 1_700_000_001,
                "chat": {"id": 777, "type": "private"},
                "from": {"id": 777, "username": "alice"},
                "photo": [{"file_id": "file_small"}, {"file_id": "file_big", "file_unique_id": "uniq1"}],
            },
        }
    )
    assert photo_events[0].content_kind == "image"
    assert photo_events[0].original_message_type is None
    refs = json.loads(photo_events[0].text)["image_references"]
    dest = tmp_path / "img.png"
    digest, mime = tg.fetch_original_image(photo_events[0], refs, dest)
    assert dest.read_bytes() == b"PNGDATA"
    assert mime == "image/png"
    assert digest == hashlib.sha256(b"PNGDATA").hexdigest()

    res = tg.send_text(ReplyDraft("op1", ConversationKey("tg_bot_1", "-100123"), "All green", 0))
    assert res.status == "queued"
    q_status, _ = tg.send_quote("-100123", "Ack", ("777", "Alice", "-100123", "Status check"))
    assert q_status == "queued"
    img_status, _ = tg.send_image("-100123", dest)
    assert img_status == "queued"
    assert any(path.endswith("/sendMessage") for _, path in calls)
    assert any(path.endswith("/sendPhoto") for _, path in calls)


def test_feishu_connector_auth_parse_token_and_outbound(tmp_path: Path):
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/open-apis/auth/v3/tenant_access_token/internal"):
            return httpx.Response(200, json={"code": 0, "tenant_access_token": "t-live", "expire": 7200})
        if request.url.path.endswith("/open-apis/im/v1/images"):
            return httpx.Response(200, json={"code": 0, "data": {"image_key": "img_v2_abc"}})
        if request.url.path.endswith("/open-apis/im/v1/messages"):
            return httpx.Response(200, json={"code": 0, "data": {"message_id": "om_out_1"}})
        return httpx.Response(404)

    cfg = ConnectorConfig(
        workspace=tmp_path,
        account_id="cli_app_1",
        send_enabled=True,
        source_id="feishu",
        channel="feishu",
        options={
            "app_id": "cli_app_1",
            "app_secret": "sec_1",
            "verification_token": "vtok-123",
            "encrypt_key": "enc-key",
        },
        transport=httpx.MockTransport(handler),
    )
    feishu = create_gateway(cfg, factory="echoloom.connectors.feishu:FeishuConnector")
    assert isinstance(feishu, FeishuConnector)
    assert isinstance(feishu, ChannelConnector)

    # URL verification challenge produces valid empty event tuple and validates token/sig.
    challenge_body = json.dumps({"type": "url_verification", "token": "vtok-123", "challenge": "c"}).encode()
    sig = hashlib.sha256(b"1700000nonce1enc-key" + challenge_body).hexdigest()
    headers = {
        "X-Lark-Request-Timestamp": "1700000",
        "X-Lark-Request-Nonce": "nonce1",
        "X-Lark-Signature": sig,
    }
    assert feishu.verify_request(headers, challenge_body)
    assert not feishu.verify_request(headers, b'{"type":"url_verification","token":"bad"}')
    assert feishu.parse_events({"type": "url_verification", "challenge": "abc"}) == ()

    events = feishu.parse_events(
        {
            "schema": "2.0",
            "header": {"event_type": "im.message.receive_v1", "token": "vtok-123"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_123", "user_id": "emp_1"}},
                "message": {
                    "message_id": "om_in_1",
                    "chat_id": "oc_group_1",
                    "chat_type": "group",
                    "message_type": "text",
                    "create_time": "1700000000000",
                    "content": json.dumps({"text": "部署完成"}),
                },
            },
        }
    )
    assert len(events) == 1
    assert events[0].conversation_id == "oc_group_1"
    assert events[0].sender_id == "ou_123"
    assert events[0].is_group is True
    assert events[0].text == "部署完成"

    res = feishu.send_text(ReplyDraft("op2", ConversationKey("cli_app_1", "oc_group_1"), "收到", 0))
    assert res.status == "queued"
    img_path = tmp_path / "shot.png"
    img_path.write_bytes(b"PNG")
    img_status, _ = feishu.send_image("oc_group_1", img_path)
    assert img_status == "queued"


def test_discord_connector_parse_auth_and_send(tmp_path: Path):
    seen_auth: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_auth.append(request.headers.get("authorization", ""))
        if request.url.path == "/api/v10/channels/chan_9/messages":
            return httpx.Response(200, json={"id": "999888777"})
        return httpx.Response(404)

    cfg = ConnectorConfig(
        workspace=tmp_path,
        account_id="bot_99",
        send_enabled=True,
        source_id="discord",
        channel="discord",
        auth_token="disc-bot-token",
        transport=httpx.MockTransport(handler),
    )
    dc = create_gateway(cfg, factory="echoloom.connectors.discord:DiscordConnector")
    assert isinstance(dc, DiscordConnector)
    assert isinstance(dc, ChannelConnector)

    assert dc.verify_request({"Authorization": "Bearer disc-bot-token"}, b"{}")
    assert not dc.verify_request({"Authorization": "Bearer wrong"}, b"{}")

    events = dc.parse_events(
        {
            "t": "MESSAGE_CREATE",
            "d": {
                "id": "123456789012345678",
                "channel_id": "chan_9",
                "guild_id": "guild_1",
                "timestamp": "2026-01-02T03:04:05+00:00",
                "author": {"id": "user_5", "global_name": "Bob"},
                "content": "hi discord",
                "referenced_message": {
                    "id": "123456789012345600",
                    "author": {"id": "bot_99", "username": "Echo"},
                    "content": "ping",
                },
            },
        }
    )
    assert len(events) == 1
    assert events[0].content_kind == "quote"
    assert events[0].original_message_type is None
    assert events[0].source_msg_id == 123456789012345678
    assert dc.resolve_contact("user_5").nickname == "Bob"

    res = dc.send_text(ReplyDraft("op3", ConversationKey("bot_99", "chan_9"), "pong", 0))
    assert res.status == "queued"
    assert seen_auth[-1] == "Bot disc-bot-token"


def test_scheduler_preserves_connector_channel_source_and_content_kind(tmp_path: Path):
    import sqlite3

    from echoloom.conversation_scheduler import ConversationScheduler

    db = sqlite3.connect(tmp_path / "sched.sqlite3")
    try:
        scheduler = ConversationScheduler(db)
        event = MessageEvent(
            account_id="bot_99",
            conversation_id="chan_9",
            sender_id="user_5",
            sender_origin="channel_group_member",
            text='{"kind":"voice_message"}',
            is_group=True,
            message_type=1,
            source_msg_id=None,
            capture_tick_ms=1700000000000,
            source_id="discord-main",
            channel="discord",
            content_kind="voice",
        )
        status, receipt = scheduler.enqueue(event)
        assert status == "queued"
        restored = scheduler._from_payload(scheduler._payload(event))
        assert restored.source_id == "discord-main"
        assert restored.channel == "discord"
        assert restored.content_kind == "voice"
        assert restored.original_message_type is None
        assert scheduler.receipt_id(restored) == receipt
    finally:
        db.close()
