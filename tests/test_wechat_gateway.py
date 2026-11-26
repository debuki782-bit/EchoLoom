"""The same application must work through Hook or a non-HTTP backend."""
import asyncio
import io
import json
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from PIL import Image

from echoloom.adapters.hook import HookGateway
from echoloom.contacts import ContactIdentity
from echoloom.context_assembler import MessageContextAssembler
from echoloom.conversation_scheduler import ConversationScheduler
from echoloom.hook_contract import HookObservation
from echoloom.hook_outbound import HookSendResult
from echoloom.media import WeChatImageIngestor, WeChatVoiceIngestor, image_for_model
from echoloom.memory_store import MemoryStore
from echoloom.mock import SystemClock
from echoloom.models import ConversationKey, ReplyDraft
from echoloom.pilot import Pilot, create_pilot_app
from echoloom.wechat_gateway import (
    GatewayConfig,
    MessageEvent,
    SendResult,
    VoicePayload,
    WeChatGateway,
    create_gateway,
)
from echoloom.wechat_tools import WeChatTools, operation_id, quote_context

ACCOUNT, ROOM, MEMBER = "wxid_bot_example", "example@chatroom", "wxid_member_example"


def event(*, source_id=9001, message_type=1, text="hello"):
    return MessageEvent(ACCOUNT, ROOM, MEMBER, "hook_group_member_wxid", text,
                        True, message_type, source_id, 123,
                        original_message_type=message_type)


def recording(root):
    return create_gateway(GatewayConfig(root, account_id=ACCOUNT, send_enabled=True,
        allowed_conversations=frozenset({ConversationKey(ACCOUNT, ROOM)})),
        factory="echoloom.adapters.recording:RecordingGateway")


def png():
    output = io.BytesIO()
    Image.new("RGB", (24, 16), "purple").save(output, "PNG")
    return output.getvalue()


def forbid_hook_http(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("an injected offline backend must never call Hook HTTP")
    monkeypatch.setattr(httpx.Client, "request", forbidden)


def test_legacy_types_are_aliases_and_queue_identity_survives_restart(tmp_path):
    assert HookObservation is MessageEvent
    assert HookSendResult is SendResult
    original = event(source_id=None)
    original = replace(original, raw_message_json='{"msg":"private body","time":123}')
    first = ConversationScheduler(__import__("sqlite3").connect(tmp_path / "inbox.sqlite3"))
    try:
        state, receipt = first.enqueue(original)
        assert state == "queued"
        stored = first.db.execute("SELECT payload_json FROM pilot_inbox").fetchone()[0]
        restored = first._from_payload(stored)
        assert first.receipt_id(restored) == receipt
        assert first._payload(restored) == first._payload(original)
        assert "private body" not in repr(original)
    finally:
        first.db.close()


def test_factory_rejects_incomplete_backend(tmp_path, monkeypatch):
    assert isinstance(recording(tmp_path), WeChatGateway)
    module = SimpleNamespace(build=lambda config: object())
    monkeypatch.setitem(sys.modules, "incomplete_wechat_backend", module)
    with pytest.raises(TypeError, match="required contract"):
        create_gateway(GatewayConfig(tmp_path), factory="incomplete_wechat_backend:build")
    with pytest.raises(ValueError, match="module:factory"):
        create_gateway(GatewayConfig(tmp_path), factory="missing_separator")


def test_different_callback_to_archive_media_context_and_send_without_hook(tmp_path, monkeypatch):
    forbid_hook_http(monkeypatch)

    async def scenario():
        gateway = recording(tmp_path)
        gateway.contacts[MEMBER] = ContactIdentity("Example member", user_name=MEMBER)
        gateway.images[ROOM] = png()
        memory = MemoryStore(tmp_path / "data/memory.sqlite3")
        ingestor = WeChatImageIngestor(tmp_path, gateway=gateway)
        assembler = MessageContextAssembler(memory, resolve_contact=gateway.resolve_contact,
            image_ingestor=ingestor, archive_envelope=gateway.archive_envelope)
        calls = []

        async def reply(context):
            calls.append(context)
            return "offline reply"

        pilot = Pilot(tmp_path / "pilot.sqlite3", ACCOUNT, ROOM, reply, gateway.send_text,
            limit=None, memory_store=memory, context_assembler=assembler,
            parse_events=gateway.parse_events, archive_envelope=gateway.archive_envelope)
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        worker = asyncio.create_task(pilot.worker())
        app = create_pilot_app(pilot)
        photo = event(source_id=9002, message_type=3, text='<msg><img md5="' + 'a'*32 + '"/></msg>')
        try:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
                response = await client.post("/callback", json=gateway.envelope(event(), photo))
                assert response.status_code == 200
                assert response.json()["accepted"] == 2
            async with asyncio.timeout(5):
                while pilot.scheduler.pending_count() or not gateway.outbound:
                    await asyncio.sleep(0.01)
            assert len(calls) == 1
            assert len(calls[0].turns) == 2  # existing debounce merges the two messages
            assert len(gateway.outbound) == 1
            rows = memory.db.execute("SELECT platform_message_id FROM source_messages "
                                     "WHERE direction='inbound' ORDER BY platform_message_id").fetchall()
            assert [r[0] for r in rows] == ["9001", "9002"]
            saved = memory.get_latest_ready_image(account_id=ACCOUNT, conversation_id=ROOM)
            payload, mime = image_for_model(Path(saved["artifact_path"]), saved["mime_type"])
            assert payload == png() and mime == "image/png"
            assert saved["source_kind"] == "original"
            assert pilot.offer(gateway.parse_events(gateway.envelope(event()))[0]) == "duplicate"
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            pilot.db.close()
            memory.close()
            gateway.close()

    asyncio.run(scenario())


def test_alternate_archive_backlog_replays_through_its_own_parser(tmp_path, monkeypatch):
    forbid_hook_http(monkeypatch)

    async def scenario():
        gateway = recording(tmp_path)
        memory = MemoryStore(tmp_path / "data/memory.sqlite3")
        item = gateway.parse_events(gateway.envelope(event()))[0]
        assembler = MessageContextAssembler(memory, archive_envelope=gateway.archive_envelope)
        await assembler.assemble((item,))
        pilot = Pilot(tmp_path / "pilot.sqlite3", ACCOUNT, ROOM, lambda context: None,
            gateway.send_text, memory_store=memory, parse_events=gateway.parse_events,
            archive_envelope=gateway.archive_envelope)
        try:
            assert pilot.recover_archived_backlog()["queued"] == 1
            assert pilot.recover_archived_backlog()["existing"] == 1
            assert pilot.scheduler.pending_count() == 1
        finally:
            pilot.db.close()
            memory.close()

    asyncio.run(scenario())


def test_business_text_quote_and_image_use_injected_backend_and_keep_claims(tmp_path, monkeypatch):
    forbid_hook_http(monkeypatch)

    async def scenario():
        gateway = recording(tmp_path)
        memory = MemoryStore(tmp_path / "data/memory.sqlite3")
        tools = WeChatTools(tmp_path, ACCOUNT, ROOM, None, None, None, SystemClock(),
                            memory_store=memory, gateway=gateway)
        image = memory.media_root / "fixture.png"
        image.parent.mkdir(exist_ok=True)
        image.write_bytes(png())
        tools.images["fixture"] = image
        try:
            token = operation_id.set("send-text")
            try:
                result = await tools.send_wechat_text("hello")
                assert result["status"] == "queued" and result["delivered"] is False
                assert (await tools.send_wechat_text("hello"))["status"] == "already_submitted"
            finally:
                operation_id.reset(token)
            token = operation_id.set("send-quote")
            quoted = quote_context.set((MEMBER, "Example member", ROOM, "quoted text"))
            try:
                result = await tools.reply_with_wechat_quote("quoted reply")
                assert result["status"] == "unknown" and result["delivered"] is False
                assert result["message_id"]
            finally:
                operation_id.reset(token)
                quote_context.reset(quoted)
            token = operation_id.set("send-image")
            try:
                result = await tools.send_wechat_image("fixture")
                assert result["status"] == "queued" and result["archive_status"] == "saved"
            finally:
                operation_id.reset(token)
            assert [r["kind"] for r in gateway.outbound] == ["text", "quote", "image"]
            tools.can_send = lambda: False
            token = operation_id.set("paused-send")
            try:
                await tools.send_wechat_text("must not send")
            finally:
                operation_id.reset(token)
            assert len(gateway.outbound) == 3
        finally:
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_voice_input_does_not_require_hook_hex_envelope(tmp_path, monkeypatch):
    forbid_hook_http(monkeypatch)
    gateway = recording(tmp_path)
    gateway.voices[ROOM] = VoicePayload(b"#!SILK_V3synthetic", 100)
    monkeypatch.setitem(sys.modules, "pysilk", SimpleNamespace(
        decode=lambda source, output, rate: output.write(b"\0\0" * 2400)))
    memory = MemoryStore(tmp_path / "data/memory.sqlite3")
    item = event(message_type=34)
    archived = memory.archive_message(account_id=ACCOUNT, conversation_id=ROOM, sender_id=MEMBER,
                                     raw_envelope={"voice": "synthetic"}, body_text="voice")
    try:
        metadata = WeChatVoiceIngestor(tmp_path, gateway=gateway).ingest(
            item=item, message_id=archived.message_id, store=memory)
        assert metadata["state"] == "ready"
    finally:
        memory.close()


def test_hook_backend_preserves_wire_requests_and_account_verification(tmp_path):
    account_dir = tmp_path / "xwechat_files" / (ACCOUNT + "_suffix")
    profile = account_dir / "db_storage/contact/contact.db"
    profile.parent.mkdir(parents=True)
    seen = []

    def respond(request):
        seen.append((request.url.path, json.loads(request.content) if request.content else None))
        assert request.url.port == 30009
        if request.url.path == "/QueryDB/status":
            return httpx.Response(200, json={"IsLogin": 1, "SqliteContactDbPath": str(profile)})
        if request.url.path == "/GetContact":
            return httpx.Response(200, json={"status": 0, "contact_found": True,
                                            "UserName": MEMBER, "NickName": "Example member"})
        if request.url.path == "/Decode_Pic":
            Path(json.loads(request.content)["dst_path"]).write_bytes(png())
            return httpx.Response(200, json={"ret": 0})
        return httpx.Response(200, json={"ret": 0, "queued": True})

    gateway = HookGateway(GatewayConfig(tmp_path, account_id=ACCOUNT, send_enabled=True,
        allowed_conversations=frozenset({ConversationKey(ACCOUNT, ROOM)}),
        base_url="http://127.0.0.1:30009", transport=httpx.MockTransport(respond)))
    try:
        state = gateway.account_state(ACCOUNT, ROOM)
        assert state.logged_in and state.account_matches
        assert state.storage_root == account_dir.parent
        assert gateway.account_state("wxid_other").account_matches is False
        assert gateway.resolve_contact(MEMBER).nickname == "Example member"
        draft = ReplyDraft("event", ConversationKey(ACCOUNT, ROOM), "hello", 0)
        assert gateway.send_text(draft).status == "queued"
        assert gateway.send_image(ROOM, tmp_path / "image.png")[0] == "queued"
        assert gateway.send_quote(ROOM, "reply", (MEMBER, "Example member", ROOM, "source"))[0] == "accepted_by_server"
        gateway.decode_image(tmp_path / "input.dat", tmp_path / "output.png")
        assert Path(tmp_path / "output.png").read_bytes() == png()
        assert ("/SendTextMsg", {"wxidorgid": ROOM, "msg": "hello"}) in seen
        quote = next(payload for path, payload in seen if path == "/ForwardXMLMsg")
        assert quote["to_wxid"] == ROOM and quote["type"] == 57
        from xml.etree import ElementTree as ET
        root = ET.fromstring(quote["content"])
        assert root.findtext("refermsg/chatusr") == MEMBER
        assert root.findtext("refermsg/fromusr") == ROOM
        assert root.findtext("refermsg/svrid") == "0"
    finally:
        gateway.close()


def test_business_modules_do_not_embed_hook_endpoints():
    root = Path(__file__).parents[1] / "src/echoloom"
    module_paths = [
        root / name
        for name in (
            "wechat_tools.py",
            "media.py",
            "pilot.py",
            "context_assembler.py",
            "ingress.py",
            "dispatcher.py",
            "outbound.py",
            "triggers.py",
            "turn_scope.py",
            "message_ingestor.py",
            "context_builder.py",
            "session_rollover.py",
            "cross_chat_dedupe.py",
            "turn_hooks.py",
            "bootstrap.py",
        )
    ]
    repo_paths = sorted((root / "repos").glob("*.py"))
    toolset_paths = sorted((root / "toolsets").glob("*.py"))
    assert repo_paths and toolset_paths
    module_paths.extend(repo_paths)
    module_paths.extend(toolset_paths)
    for path in module_paths:
        source = path.read_text("utf-8")
        for endpoint in ("/SendTextMsg", "/SendImgMsg", "/ForwardXMLMsg", "/Decode_Pic", "/QueryDB/status"):
            assert endpoint not in source, (path.relative_to(root).as_posix(), endpoint)
