import asyncio
import base64
import hashlib
import io
import json
import random
from pathlib import Path
from types import SimpleNamespace

import httpx
from PIL import Image

from echoloom.event_book import EventBook
from echoloom.hook_outbound import HookSendResult
from echoloom.memory_store import MemoryStore
from echoloom.mock import SystemClock
from echoloom.storage import Repository
from echoloom.usage import UsageLedger
from echoloom.wechat_tools import (
    WeChatTools,
    _segment_pause_seconds,
    active_conversation,
    memory_context,
    operation_id,
    quote_context,
)


def test_sender_update_uses_stable_id_when_safe_display_names_collide(tmp_path: Path):
    (tmp_path / "data").mkdir()
    memory = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    for sender_id in ("wxid_first", "wxid_second"):
        memory.archive_message(
            account_id="bot",
            conversation_id="room@chatroom",
            sender_id=sender_id,
            raw_envelope={"sender": sender_id},
            body_text="曾在当前聊天发言",
        )
    tools = WeChatTools(
        tmp_path, "bot", "room@chatroom", None, None, None, SystemClock(),
        memory_store=memory,
    )
    choices = [
        {"sender_id": "wxid_first", "display_name": "联系人-AAA"},
        {"sender_id": "wxid_second", "display_name": "联系人-AAA"},
    ]
    try:
        ambiguous = tools._update_sender_from_entry(
            {"sender": "联系人-AAA", "type": "偏好", "content": "喜欢咖啡"},
            choices,
        )
        assert ambiguous["status"] == "ambiguous"
        assert ambiguous["reason"] == "sender_name_matches_multiple_people_use_sender_id"
        assert ambiguous["choices"] == [
            {"sender_id": "wxid_first", "name": "联系人-AAA"},
            {"sender_id": "wxid_second", "name": "联系人-AAA"},
        ]
        saved = tools._update_sender_from_entry(
            {"sender": "wxid_second", "type": "偏好", "content": "喜欢咖啡"},
            choices,
        )
        assert saved["status"] == "saved"
        assert saved["sender_id"] == "wxid_second"
        assert memory.sender_memories(account_id="bot", sender_ids=["wxid_first"]) == {}
        assert "wxid_second" in memory.sender_memories(
            account_id="bot", sender_ids=["wxid_second"],
        )
    finally:
        tools.db.close()
        memory.close()


def test_text_tool_requires_route_deduplicates_and_obeys_pause(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        def submit(self, draft):
            self.calls.append(draft)
            return HookSendResult("queued")

    async def scenario():
        (tmp_path / "data").mkdir()
        sender = Sender()
        tools = WeChatTools(tmp_path, "bot", "nonexistent@chatroom", sender, None, None,
                            SystemClock())
        assert (await tools.send_wechat_text("hello"))["status"] == "already_attempted_or_no_route"
        token = operation_id.set("one-inbound-event")
        try:
            assert (await tools.send_wechat_text("hello"))["status"] == "queued"
            assert (await tools.send_wechat_text("hello"))["status"] == "already_submitted"
            tools.can_send = lambda: False
            assert (await tools.send_wechat_text("another"))["status"] == "already_attempted_or_no_route"
            assert len(sender.calls) == 1
        finally:
            operation_id.reset(token)
            tools.db.close()

    asyncio.run(scenario())


def test_text_tool_uses_dynamically_active_conversation(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        def submit(self, draft):
            self.calls.append(draft)
            return HookSendResult("queued")

    async def scenario():
        (tmp_path / "data").mkdir()
        sender = Sender()
        tools = WeChatTools(tmp_path, "bot", "configured-room@chatroom", sender,
                            None, None, SystemClock())
        conversation = "new-room@chatroom"
        route_token = active_conversation.set(conversation)
        operation_token = operation_id.set("dynamic-chat-send")
        try:
            result = await tools.send_wechat_text("reply in the new chat")
            assert result["status"] == "queued"
            assert sender.calls[0].key.conversation_id == conversation
        finally:
            operation_id.reset(operation_token)
            active_conversation.reset(route_token)
            tools.db.close()

    asyncio.run(scenario())


def test_send_failures_stop_after_initial_plus_three_retries_and_report_limit(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        def submit(self, draft):
            self.calls.append(draft)
            return HookSendResult("rejected", "test_rejection")

    async def scenario():
        (tmp_path / "data").mkdir()
        sender = Sender()
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", sender, None, None, SystemClock(),
        )
        token = operation_id.set("bounded-send-test")
        try:
            result = None
            for index in range(4):
                result = await tools.send_wechat_text(f"尝试 {index + 1}")
            assert result["status"] == "retry_limit_reached"
            assert result["retries_remaining"] == 0
            assert result["last_failure_status"] == "rejected"
            assert len(sender.calls) == 4
            blocked = await tools.send_wechat_text("第五条不能发")
            assert blocked["status"] == "retry_limit_reached"
            assert len(sender.calls) == 4
        finally:
            operation_id.reset(token)
            tools.db.close()

    asyncio.run(scenario())


def test_commit_agent_turn_cannot_repeat_distinct_sends_after_hook_queued(tmp_path):
    class Sender:
        def __init__(self):
            self.calls = []

        def submit(self, draft):
            self.calls.append(draft)
            return HookSendResult("queued")

    async def scenario():
        (tmp_path / "data").mkdir()
        sender = Sender()
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", sender, None, None, SystemClock(),
        )
        token = operation_id.set("repeated-commit-turn")
        try:
            first = await tools.commit_agent_turn(reply_text="第一条测试回复")
            second = await tools.commit_agent_turn(reply_text="相似但不同的第二条回复")
            assert first["reply"]["status"] == "queued"
            assert second["reply"]["status"] == "already_submitted"
            assert len(sender.calls) == 1
        finally:
            operation_id.reset(token)
            tools.db.close()

    asyncio.run(scenario())


def test_voice_transcription_is_on_demand_cached_and_current_chat_scoped(tmp_path):
    class Files:
        def __init__(self):
            self.uploads = 0
            self.deletes = []

        async def upload(self, **kwargs):
            self.uploads += 1
            assert kwargs["config"].mime_type == "audio/wav"
            return SimpleNamespace(uri="https://files.invalid/audio", name="file-1")

        async def delete(self, *, name):
            self.deletes.append(name)

    class Interactions:
        def __init__(self):
            self.calls = 0

        async def create(self, **kwargs):
            self.calls += 1
            assert kwargs["model"] == "gemini-3.5-transcribe"
            assert kwargs["input"][0]["mime_type"] == "audio/wav"
            return SimpleNamespace(
                output_text="明天上午十点开会。",
                usage=SimpleNamespace(total_input_tokens=100, total_output_tokens=10),
            )

    async def scenario():
        (tmp_path / "data").mkdir()
        store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        archived = store.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msgtype": 34}, source_record={"msgtype": 34},
            body_text='{"kind":"voice_message"}',
        )
        record = store.begin_audio_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=archived.message_id, duration_ms=10_000,
        )
        path = store.audio_root / "voice.wav"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"local wav bytes")
        store.complete_audio_ingest(
            audio_id=str(record["audio_id"]), account_id="bot",
            artifact_path=str(path.resolve()), byte_count=path.stat().st_size,
            sha256=hashlib.sha256(path.read_bytes()).hexdigest(), duration_ms=10_000,
        )
        files, interactions = Files(), Interactions()
        client = SimpleNamespace(
            aio=SimpleNamespace(files=files, interactions=interactions),
        )
        repo = Repository(tmp_path / "data" / "usage.sqlite3")
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, client,
            UsageLedger(repo, SystemClock()), SystemClock(),
            memory_store=store,
        )
        token = memory_context.set({"account_id": "bot", "message_id": archived.message_id})
        try:
            result = await tools.transcribe_latest_wechat_voice()
            assert result == {
                "transcribed": True, "transcript": "明天上午十点开会。",
                "audio_id": record["audio_id"], "cached": False,
            }
            cached = await tools.transcribe_latest_wechat_voice()
            assert cached["cached"] is True
            assert cached["transcript"] == "明天上午十点开会。"
            assert interactions.calls == 1 and files.uploads == 1
            assert files.deletes == ["file-1"]
            assert store.audio_metadata_for_message(
                account_id="bot", message_id=archived.message_id,
            )["transcript_state"] == "ready"
            failed_voice = store.archive_message(
                account_id="bot", conversation_id="room@chatroom", sender_id="member",
                raw_envelope={"msgtype": 34, "msgsvrid": 2},
                source_record={"msgtype": 34, "msgsvrid": 2},
                body_text='{"kind":"voice_message"}',
            )
            store.fail_audio_ingest_for_message(
                account_id="bot", message_id=failed_voice.message_id,
                error_code="voice_payload_unavailable",
            )
            memory_context.set({"account_id": "bot", "message_id": failed_voice.message_id})
            unavailable = await tools.transcribe_latest_wechat_voice()
            assert unavailable == {
                "error": "voice_not_available", "reason": "voice_payload_unavailable",
            }
            assert interactions.calls == 1
        finally:
            memory_context.reset(token)
            tools.db.close()
            store.close()
            repo.close()

    asyncio.run(scenario())


def test_generate_image_reserves_usage_and_sends_through_loopback_hook(tmp_path):
    class Images:
        def __init__(self):
            self.calls = 0

        async def generate(self, **kwargs):
            self.calls += 1
            assert kwargs["model"] == WeChatTools.IMAGE_MODEL
            assert kwargs["prompt"] == "一只红熊猫贴纸"
            assert kwargs["size"] == "1024x1024"
            assert kwargs["quality"] == "medium"
            assert kwargs["output_format"] == "png"
            assert kwargs["n"] == 1
            return SimpleNamespace(
                data=[
                    SimpleNamespace(
                        b64_json=base64.b64encode(b"\x89PNG\r\n\x1a\nsmall png").decode(),
                    ),
                ],
                usage=SimpleNamespace(input_tokens=100, output_tokens=1120),
            )

    class Sender:
        def submit(self, draft):
            return HookSendResult("queued")

    def hook(request):
        assert request.url.path == "/SendImgMsg"
        payload = json.loads(request.content)
        assert payload["wxidorgid"] == "room@chatroom"
        assert Path(payload["path"]).is_file()
        return httpx.Response(200, json={"ret": 0, "queued": True})

    async def scenario():
        (tmp_path / "data").mkdir()
        repo = Repository(tmp_path / "usage.sqlite3")
        images = Images()
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", Sender(),
            None,
            UsageLedger(repo, SystemClock()), SystemClock(),
            transport=httpx.MockTransport(hook),
            openai_client=SimpleNamespace(images=images),
        )
        token = operation_id.set("inbound-event")
        try:
            result = await tools.generate_and_send_wechat_image("一只红熊猫贴纸")
            assert result["status"] == "queued"
            assert result["image_generated"] is True
            assert result["message_id"] is None and result["note_status"] == "not_saved"
            duplicate = await tools.generate_and_send_wechat_image("一只红熊猫贴纸")
            assert duplicate["status"] == "already_submitted"
            assert images.calls == 1
            row = repo.conn.execute(
                "select state,actual_microusd from usage_reservations"
            ).fetchone()
            assert tuple(row) == ("settled", 34100)
        finally:
            operation_id.reset(token)
            tools.db.close()
            repo.close()

    asyncio.run(scenario())


def test_quote_tool_builds_type57_and_reports_unverified_rendering(tmp_path):
    requests = []

    def hook(request):
        payload = json.loads(request.content)
        requests.append(payload)
        assert request.url.path == "/ForwardXMLMsg"
        assert payload["to_wxid"] == "room@chatroom"
        assert payload["type"] == 57
        assert "<type>57</type>" in payload["content"]
        assert "<svrid>0</svrid>" in payload["content"]
        assert "wxid_member" in payload["content"]
        assert "原消息内容" in payload["content"]
        return httpx.Response(200, json={"ret": 0, "retmsg": "success"})

    async def scenario():
        (tmp_path / "data").mkdir()
        tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                            SystemClock(), transport=httpx.MockTransport(hook))
        op = operation_id.set("inbound-event")
        quote = quote_context.set(("wxid_member", "琴一", "room@chatroom", "原消息内容"))
        try:
            result = await tools.reply_with_wechat_quote("【引用测试】回复内容")
            assert result == {
                "status": "accepted_by_server", "reason": "server_accepted_not_delivery_proof",
                "native_quote_rendered": False, "delivered": False,
                "source_message_id_available": False, "message_id": None,
                "message_id_kind": "local", "sender_id": "bot",
            }
            assert await tools.reply_with_wechat_quote("【引用测试】回复内容") == {
                "status": "already_submitted", "native_quote_rendered": False,
                "delivered": False, "last_send_status": "accepted_by_server",
                "send_attempt_count": 1,
                "reason": "previous_send_was_accepted_or_queued_do_not_duplicate",
            }
            assert len(requests) == 1
        finally:
            quote_context.reset(quote)
            operation_id.reset(op)
            tools.db.close()

    asyncio.run(scenario())


def test_quote_tool_reports_when_current_message_is_not_quotable(tmp_path):
    async def scenario():
        (tmp_path / "data").mkdir()
        calls = []
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, None, None, None,
            transport=httpx.MockTransport(lambda request: calls.append(request)),
        )
        op = operation_id.set("inbound-event")
        try:
            result = await tools.reply_with_wechat_quote("reply")
            assert result == {
                "status": "unavailable",
                "reason": "current_message_not_quotable",
                "native_quote_rendered": False,
                "delivered": False,
            }
            assert calls == []
        finally:
            operation_id.reset(op)
            tools.db.close()

    asyncio.run(scenario())


def test_commit_turn_falls_back_to_text_when_quote_is_unavailable(tmp_path: Path):
    class Sender:
        def __init__(self):
            self.calls = []

        def submit(self, draft):
            self.calls.append(draft)
            return HookSendResult("queued")

    async def scenario():
        (tmp_path / "data").mkdir()
        sender = Sender()
        tools = WeChatTools(
            tmp_path, "bot", "private-contact", sender, None, None, SystemClock(),
        )
        op = operation_id.set("quote-fallback-test")
        try:
            result = await tools.commit_agent_turn(
                reply_text="这条私聊无法原生引用，但文字回复应照常发送。",
                quote_current_message=True,
            )
            assert result["reply"]["status"] == "queued"
            assert result["reply"]["quote_fallback"] == "plain_text"
            assert result["reply"]["native_quote_rendered"] is False
            assert len(sender.calls) == 1
            assert sender.calls[0].text == "这条私聊无法原生引用，但文字回复应照常发送。"
        finally:
            operation_id.reset(op)
            tools.db.close()

    asyncio.run(scenario())


def test_commit_turn_defaults_tag_add_and_updates_known_cross_chat_sender(tmp_path: Path):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "data" / "memory.sqlite3")
        message = memory.archive_message(
            account_id="bot", conversation_id="other-room@chatroom",
            sender_id="contact:opaque-42",
            raw_envelope={"sender": "contact:opaque-42"},
            source_record={"sender": "contact:opaque-42"},
            body_text="跨会话联系人",
        )
        tools = WeChatTools(
            tmp_path, "bot", "private-room", None, None, None, SystemClock(),
            memory_store=memory,
        )
        ctx = memory_context.set({"account_id": "bot", "message_id": message.message_id})
        op = operation_id.set("cross-chat-profile-test")
        try:
            result = await tools.commit_agent_turn(
                sender_updates=[{
                    "sender_id": "contact:opaque-42", "type": "身份关系",
                    "content": "群里的熟悉成员",
                }, {
                    "sender_id": "not-in-account-archive", "type": "身份关系",
                    "content": "无效对象",
                }],
                tag_updates=[
                    {"sender_id": "contact:opaque-42", "tags": ["熟悉成员"]},
                    {"sender_id": "not-in-account-archive", "tags": ["无效对象"]},
                ],
            )
            assert result["sender_updates"][0]["status"] == "saved"
            assert result["sender_updates"][1]["status"] == "rejected"
            assert result["sender_updates"][0]["sender_id"] == "contact:opaque-42"
            assert result["tag_updates"][0]["status"] == "updated"
            assert result["tag_updates"][1]["status"] == "unavailable"
            assert memory.sender_tags_at(
                account_id="bot", sender_id="contact:opaque-42",
                at="9999-12-31T23:59:59+00:00",
            ) == ["熟悉成员"]
        finally:
            operation_id.reset(op)
            memory_context.reset(ctx)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_exact_image_reference_shortcut_reads_saved_original_only_on_request(tmp_path):
    output = io.BytesIO()
    Image.new("RGB", (12, 8), (10, 20, 30)).save(output, format="JPEG")
    image_bytes = output.getvalue()
    image_reference = "https://wx.qpic.cn/original.jpg?token=opaque"

    async def scenario():
        (tmp_path / "data").mkdir()
        repo = Repository(tmp_path / "usage.sqlite3")
        memory = MemoryStore(tmp_path / "agent-memory.sqlite3")
        archived = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_member",
            raw_envelope={"msg": "image event"}, source_record={"msg": "image event"},
            body_text="image event",
        )
        row = memory.begin_image_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=archived.message_id,
            original_references=[{"field": "cdnhdimgurl", "value": image_reference}],
        )
        image_path = tmp_path / "received-images" / "original.jpg"
        image_path.parent.mkdir(parents=True)
        image_path.write_bytes(image_bytes)
        memory.complete_image_ingest(
            image_id=row["image_id"], account_id="bot", artifact_path=str(image_path.resolve()),
            mime_type="image/jpeg", byte_count=len(image_bytes),
            sha256=hashlib.sha256(image_bytes).hexdigest(),
            source_sha256=hashlib.sha256(image_bytes).hexdigest(),
        )
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None,
            None,
            UsageLedger(repo, SystemClock()), SystemClock(),
            memory_store=memory,
        )
        context_token = memory_context.set({
            "account_id": "bot", "conversation_id": "room@chatroom",
            "message_id": archived.message_id,
        })
        try:
            result = await tools.inspect_wechat_image(image_reference, "图里有什么？")
            assert result["image_attached_to_main_agent"] is True
            assert result["image_part"].inline_data.data == image_bytes
            assert result["image_part"].inline_data.mime_type == "image/jpeg"
            assert repo.conn.execute(
                "SELECT count(*) FROM usage_reservations"
            ).fetchone()[0] == 0
            notes = await tools.commit_agent_turn(image_notes=[
                {"image_id": str(row["image_id"]), "content": "一只猫坐在窗台上。"},
                {"image_id": "missing-image", "content": "无效图片"},
            ])
            assert [item["status"] for item in notes["image_notes"]] == [
                "saved", "unavailable",
            ]
            assert memory.image_metadata_for_message(
                account_id="bot", message_id=archived.message_id,
            )["vision_summary"] == "一只猫坐在窗台上。"
            current_turn = memory.archive_message(
                account_id="bot", conversation_id="room@chatroom", sender_id="wxid_member",
                raw_envelope={"msg": "看看刚才那张"}, source_record={"msg": "看看刚才那张"},
                body_text="看看刚才那张",
            )
            turn_token = memory_context.set({
                "account_id": "bot", "conversation_id": "room@chatroom",
                "message_id": current_turn.message_id,
            })
            try:
                latest = await tools.inspect_latest_wechat_image("图里有什么？")
            finally:
                memory_context.reset(turn_token)
            assert latest["selection"] == "latest_saved_image_in_current_group"
            assert latest["image_part"].inline_data.data == image_bytes
        finally:
            memory_context.reset(context_token)
            tools.db.close()
            memory.close()
            repo.close()

    asyncio.run(scenario())


def test_latest_image_does_not_substitute_old_image_for_failed_current_image(tmp_path):
    async def scenario():
        (tmp_path / "data").mkdir()
        repo = Repository(tmp_path / "usage.sqlite3")
        memory = MemoryStore(tmp_path / "agent-memory.sqlite3")
        previous = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_member",
            raw_envelope={"msg": "old image"}, source_record={"msg": "old image"},
            body_text="old image",
        )
        previous_row = memory.begin_image_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=previous.message_id, original_references=[],
        )
        content = io.BytesIO()
        Image.new("RGB", (8, 8)).save(content, format="PNG")
        data = content.getvalue()
        old_file = tmp_path / "received-images" / "old.png"
        old_file.parent.mkdir(parents=True, exist_ok=True)
        old_file.write_bytes(data)
        memory.complete_image_ingest(
            image_id=previous_row["image_id"], account_id="bot", artifact_path=str(old_file.resolve()),
            mime_type="image/png", byte_count=len(data), sha256=hashlib.sha256(data).hexdigest(),
            source_sha256=hashlib.sha256(data).hexdigest(),
        )
        archived = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_member",
            raw_envelope={"msg": "image event"}, source_record={"msg": "image event"},
            body_text="image event",
        )
        row = memory.begin_image_ingest(account_id="bot", conversation_id="room@chatroom",
                                        message_id=archived.message_id, original_references=[])
        memory.fail_image_ingest(image_id=row["image_id"], account_id="bot",
                                 error_code="original_image_not_found_in_cache")
        tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None,
                            UsageLedger(repo, SystemClock()), SystemClock(), memory_store=memory)
        token = memory_context.set({"account_id": "bot", "message_id": archived.message_id,
                                    "message_ids": (archived.message_id, previous.message_id),
                                    "current_message_ids": (archived.message_id,)})
        try:
            result = await tools.inspect_latest_wechat_image("看这张图")
            assert result["error"] == "current_image_not_available"
            assert result["image_id"] == row["image_id"]
            assert result["reason"] == "original_image_not_found_in_cache"
            assert memory.retry_image_ingest(image_id=row["image_id"], account_id="bot",
                                             error_code="original_image_not_found_in_cache")
            memory.complete_image_ingest(
                image_id=row["image_id"], account_id="bot", artifact_path=str(old_file.resolve()),
                mime_type="image/png", byte_count=len(data), sha256=hashlib.sha256(data).hexdigest(),
                source_sha256=hashlib.sha256(data).hexdigest(), source_kind="thumbnail_inferred",
            )
            attached = await tools.inspect_latest_wechat_image("看这张图")
            assert attached["selection"] == "current_batch_image"
            assert attached["source"] == "inferred_cached_thumbnail"
            assert "不能保证对应" in attached["warning"]
            assert attached["image_part"].inline_data.data == data
        finally:
            memory_context.reset(token)
            tools.db.close()
            memory.close()
            repo.close()

    asyncio.run(scenario())


def test_gif_image_tool_returns_sampled_contact_sheet_to_main_agent(tmp_path):
    image_bytes_io = io.BytesIO()
    frames = [Image.new("RGB", (20, 12), color) for color in
              ((255, 0, 0), (0, 255, 0), (0, 0, 255), (255, 255, 0),
               (255, 0, 255), (0, 255, 255))]
    frames[0].save(image_bytes_io, format="GIF", save_all=True,
                   append_images=frames[1:], duration=100, loop=0)
    gif_bytes = image_bytes_io.getvalue()

    async def scenario():
        (tmp_path / "data").mkdir()
        repo = Repository(tmp_path / "usage.sqlite3")
        memory = MemoryStore(tmp_path / "agent-memory.sqlite3")
        archived = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_member",
            raw_envelope={"msg": "gif event"}, source_record={"msg": "gif event"},
            body_text="gif event",
        )
        row = memory.begin_image_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=archived.message_id, original_references=[],
        )
        image_path = tmp_path / "received-images" / "animated.gif"
        image_path.parent.mkdir(parents=True)
        image_path.write_bytes(gif_bytes)
        memory.complete_image_ingest(
            image_id=row["image_id"], account_id="bot", artifact_path=str(image_path.resolve()),
            mime_type="image/gif", byte_count=len(gif_bytes),
            sha256=hashlib.sha256(gif_bytes).hexdigest(),
            source_sha256=hashlib.sha256(gif_bytes).hexdigest(),
        )
        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, None,
            UsageLedger(repo, SystemClock()), SystemClock(), memory_store=memory,
        )
        context_token = memory_context.set({
            "account_id": "bot", "conversation_id": "room@chatroom",
            "message_id": archived.message_id,
        })
        try:
            result = await tools.inspect_wechat_image(str(row["image_id"]), "看看动图")
            assert result["image_attached_to_main_agent"] is True
            assert result["image_part"].inline_data.mime_type == "image/png"
            sheet = Image.open(io.BytesIO(result["image_part"].inline_data.data))
            assert sheet.size == (984, 800)
            assert memory.get_image_by_id(
                account_id="bot", conversation_id="room@chatroom",
                image_id=str(row["image_id"]),
            )["mime_type"] == "image/gif"
        finally:
            memory_context.reset(context_token)
            tools.db.close()
            memory.close()
            repo.close()

    asyncio.run(scenario())


def test_image_library_lists_current_chat_by_default_and_reads_global_ids_on_request(
    tmp_path: Path,
):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "data" / "memory.sqlite3")
        tools = WeChatTools(
            tmp_path, "bot", "current@chatroom", None, None, None,
            SystemClock(), memory_store=memory,
        )
        image_ids: dict[str, str] = {}
        image_bytes: dict[str, bytes] = {}
        for conversation_id in ("current@chatroom", "other@chatroom"):
            archived = memory.archive_message(
                account_id="bot", conversation_id=conversation_id, sender_id="member",
                raw_envelope={"msg": "image"}, source_record={"msg": "image"},
                body_text="image",
            )
            image_row = memory.begin_image_ingest(
                account_id="bot", conversation_id=conversation_id,
                message_id=archived.message_id, original_references=[],
            )
            repeated = memory.begin_image_ingest(
                account_id="bot", conversation_id=conversation_id,
                message_id=archived.message_id, original_references=[],
            )
            assert repeated["image_id"] == image_row["image_id"]
            image_io = io.BytesIO()
            Image.new("RGB", (8, 8), (10, 20, 30)).save(image_io, format="PNG")
            data = image_io.getvalue()
            path = memory.media_root / f"{image_row['image_id']}.png"
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            digest = hashlib.sha256(data).hexdigest()
            memory.complete_image_ingest(
                image_id=str(image_row["image_id"]), account_id="bot",
                artifact_path=str(path), mime_type="image/png", byte_count=len(data),
                sha256=digest, source_sha256=digest,
            )
            image_ids[conversation_id] = str(image_row["image_id"])
            image_bytes[conversation_id] = data
        foreign = memory.archive_message(
            account_id="other-bot", conversation_id="private", sender_id="friend",
            raw_envelope={"msg": "foreign"}, source_record={"msg": "foreign"},
            body_text="foreign",
        )
        foreign_row = memory.begin_image_ingest(
            account_id="other-bot", conversation_id="private",
            message_id=foreign.message_id, original_references=[],
        )
        foreign_path = memory.media_root / f"{foreign_row['image_id']}.png"
        foreign_path.write_bytes(image_bytes["current@chatroom"])
        foreign_digest = hashlib.sha256(image_bytes["current@chatroom"]).hexdigest()
        memory.complete_image_ingest(
            image_id=str(foreign_row["image_id"]), account_id="other-bot",
            artifact_path=str(foreign_path), mime_type="image/png",
            byte_count=len(image_bytes["current@chatroom"]), sha256=foreign_digest,
            source_sha256=foreign_digest,
        )
        memory.save_image_description(
            account_id="bot", image_id=image_ids["other@chatroom"],
            description="蓝色背景的小猫", model_id="offline-test",
        )
        memory.close()
        memory = MemoryStore(tmp_path / "data" / "memory.sqlite3")
        tools.memory_store = memory
        persisted = memory.get_image_by_id(
            account_id="bot", conversation_id="other@chatroom",
            image_id=image_ids["other@chatroom"],
        )
        assert persisted is not None
        assert Path(str(persisted["artifact_path"])).is_file()
        fresh_tools = WeChatTools(
            tmp_path, "bot", "current@chatroom", None, None, None,
            SystemClock(), memory_store=memory,
        )
        assert fresh_tools._image(image_ids["other@chatroom"]).read_bytes() == (
            image_bytes["other@chatroom"]
        )
        context = memory_context.set({
            "account_id": "bot", "conversation_id": "current@chatroom",
            "message_id": next(iter(memory.list_images(
                account_id="bot", conversation_id="current@chatroom", limit=1,
            ))) ["message_id"],
        })
        try:
            current = tools.list_wechat_images()
            assert current["scope"] == "current_conversation"
            assert [image["image_id"] for image in current["images"]] == [
                image_ids["current@chatroom"],
            ]

            catalog = tools.list_wechat_images(include_all_conversations=True)
            assert catalog["scope"] == "all_conversations"
            assert {image["image_id"] for image in catalog["images"]} == set(image_ids.values())
            assert all(image["image_id"] != foreign_row["image_id"] for image in catalog["images"])
            other_entry = next(
                image for image in catalog["images"]
                if image["image_id"] == image_ids["other@chatroom"]
            )
            assert other_entry["conversation_id"] == "other@chatroom"
            assert other_entry["description"] == "蓝色背景的小猫"

            inspected = await tools.inspect_wechat_image(
                image_ids["other@chatroom"], "看看这张旧图",
            )
            assert inspected["image_attached_to_main_agent"] is True
            assert inspected["image_part"].inline_data.data == image_bytes["other@chatroom"]
        finally:
            memory_context.reset(context)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_batch_note_and_dictionary_tools_return_per_item_status(tmp_path):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        messages = [memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msg": text}, source_record={"msg": text}, body_text=text,
        ) for text in ("第一条", "第二条")]
        tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                            SystemClock(), memory_store=memory)
        token = memory_context.set({
            "account_id": "bot", "message_id": messages[-1].message_id,
            "message_ids": tuple(message.message_id for message in messages),
        })
        try:
            notes = await tools.add_message_notes([
                {"message_id": messages[0].message_id, "content": "第一条的简短备注"},
                {"message_id": messages[1].message_id, "content": "第二条的简短备注"},
                {"message_id": "outside-id", "content": "越界备注"},
            ])
            assert [entry["status"] for entry in notes["results"]] == [
                "saved", "saved", "unavailable",
            ]
            dictionary = await tools.upsert_entity_dictionary_batch([
                {"term": "批处理词甲", "explanation": "释义甲"},
                {"term": "批处理词乙", "explanation": "释义乙"},
                {"term": "", "explanation": "无效条目"},
            ])
            assert [entry["status"] for entry in dictionary["results"]] == [
                "saved", "saved", "rejected",
            ]
            assert len(memory.notes_for_message(
                account_id="bot", message_id=messages[0].message_id,
            )) == 1
            assert memory.db.execute(
                "SELECT count(*) FROM entity_terms WHERE account_id='bot' AND state='active'",
            ).fetchone()[0] == 2
        finally:
            memory_context.reset(token)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_commit_agent_turn_batches_updates_and_silent_reply(tmp_path):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        message = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msg": "约定"}, source_record={"msg": "约定"}, body_text="约定",
        )
        sender = SimpleNamespace(calls=[])
        event_book = EventBook(memory)
        tools = WeChatTools(tmp_path, "bot", "room@chatroom", sender, None, None,
                            SystemClock(), memory_store=memory, event_book=event_book)
        token = memory_context.set({
            "account_id": "bot", "message_id": message.message_id,
            "message_ids": (message.message_id,),
            "message_conversations": {message.message_id: "room@chatroom"},
            "allowed_conversations": ("room@chatroom",),
        })
        operation_token = operation_id.set("turn-idempotency-test")
        try:
            arguments = {
                "message_notes": [{"message_id": message.message_id, "content": "后续约定"}],
                "dictionary_updates": [{"term": "回归词", "explanation": "测试释义"}],
                "event_updates": [{
                    "action": "add", "priority": "ordinary", "content": "核对联调结果",
                    "exit_condition": "联调完成",
                }],
                "tag_updates": [{"sender_id": "member", "action": "add", "tags": ["熟悉成员"]}],
            }
            result = await tools.commit_agent_turn(**arguments)
            repeated = await tools.commit_agent_turn(**arguments)
            assert result["status"] == "completed"
            assert result["reply"] == {"status": "omitted", "sent": False}
            assert not sender.calls
            assert result["message_notes"]["results"][0]["status"] == "saved"
            assert result["dictionary_updates"]["results"][0]["status"] == "saved"
            assert result["event_updates"][0]["status"] == "saved"
            assert result["tag_updates"][0]["status"] == "updated"
            assert result["event_updates"] == repeated["event_updates"]
            assert memory.db.execute(
                "SELECT count(*) FROM annotations WHERE account_id='bot' AND state='active'",
            ).fetchone()[0] == 1
            assert memory.db.execute(
                "SELECT count(*) FROM event_book_revisions WHERE account_id='bot'",
            ).fetchone()[0] == 1
            assert memory.enrich_context_messages(account_id="bot", messages=[{
                "message_id": message.message_id, "sender_id": "member",
                "captured_at": "9999-12-31T23:59:59+00:00",
            }])[0]["sender_tags"] == ["熟悉成员"]
        finally:
            operation_id.reset(operation_token)
            memory_context.reset(token)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_segment_pause_seconds_adds_bounded_random_jitter():
    rng = random.Random(42)
    short_samples = [_segment_pause_seconds("收到", rng=rng) for _ in range(20)]
    long_samples = [_segment_pause_seconds("这是一段比较长一点的消息用来测试不同长度下的随机抖动表现" * 2, rng=rng) for _ in range(20)]
    assert all(0.5 <= s <= 2.6 for s in short_samples + long_samples)
    assert len(set(short_samples)) > 5
    assert len(set(long_samples)) > 5
    assert sum(long_samples) / len(long_samples) > sum(short_samples) / len(short_samples)


def test_commit_agent_turn_multi_bubble_waits_with_jitter(tmp_path, monkeypatch):
    import echoloom.wechat_tools as wt_mod

    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr(wt_mod.asyncio, "sleep", fake_sleep)

    async def scenario():
        (tmp_path / "data").mkdir()
        sent: list[str] = []

        class Gateway:
            def send_text(self, draft):
                sent.append(draft.text)
                return HookSendResult("queued", "hook_queued_not_delivery_proof")

        tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None, SystemClock(), gateway=Gateway())
        token = memory_context.set({
            "account_id": "bot", "message_id": "m1", "message_ids": ("m1",),
            "message_conversations": {"m1": "room@chatroom"},
            "allowed_conversations": ("room@chatroom",),
        })
        op_token = operation_id.set("turn-multi-jitter-test")
        try:
            result = await tools.commit_agent_turn(
                reply_text="第一条短消息",
                reply_messages=["第二条稍微长一点的回复内容测试一下", "第三条结束啦"],
            )
            assert result["status"] == "completed"
            assert result["reply"]["sent_count"] == 3
            assert sent == ["第一条短消息", "第二条稍微长一点的回复内容测试一下", "第三条结束啦"]
            assert len(waits) == 2
            assert all(0.5 <= w <= 2.6 for w in waits)
        finally:
            operation_id.reset(op_token)
            memory_context.reset(token)
            tools.db.close()

    asyncio.run(scenario())


def test_commit_agent_turn_supports_sticker_only_and_text_image_combo(tmp_path, monkeypatch):
    from PIL import Image

    from echoloom.memory_store import MemoryStore

    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr("echoloom.wechat_tools.asyncio.sleep", fake_sleep)

    async def scenario():
        (tmp_path / "data").mkdir()
        store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        sticker_src = tmp_path / "tietie.jpg"
        Image.new("RGB", (32, 32), (120, 90, 220)).save(sticker_src, format="JPEG")
        imported = store.import_library_image(
            account_id="bot",
            source_path=sticker_src,
            description="示例表情包「贴贴~」",
            display_order=0,
            quick_send=True,
        )
        sticker_id = str(imported["image_id"])

        events: list[tuple[str, str]] = []

        class Gateway:
            def send_text(self, draft):
                events.append(("text", draft.text))
                return HookSendResult("queued", "hook_queued_not_delivery_proof")

            def send_image(self, room, path):
                events.append(("image", Path(path).name))
                return "queued", "hook_queued_not_delivery_proof"

        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, None, None,
            SystemClock(), memory_store=store, gateway=Gateway(),
        )
        ctx_token = memory_context.set({
            "account_id": "bot", "message_id": "m1", "message_ids": ("m1",),
            "message_conversations": {"m1": "room@chatroom"},
            "allowed_conversations": ("room@chatroom",),
        })
        try:
            # 1) Text + sticker (default after)
            op1 = operation_id.set("turn-combo-after")
            try:
                res1 = await tools.commit_agent_turn(
                    reply_text="好耶！", reply_image_id=sticker_id,
                )
                assert res1["reply"]["status"] == "queued"
                assert res1["reply_image"]["status"] == "queued"
                assert [kind for kind, _ in events] == ["text", "image"]
                assert len(waits) == 1
                # Duplicate call in same turn must stay blocked
                dup1 = await tools.commit_agent_turn(
                    reply_text="别重复发", reply_image_id=sticker_id,
                )
                assert dup1["reply"]["status"] == "already_submitted"
                assert dup1["reply_image"]["status"] == "already_submitted"
            finally:
                operation_id.reset(op1)

            # 2) Sticker before multi-bubble text
            events.clear()
            waits.clear()
            op2 = operation_id.set("turn-combo-before")
            try:
                res2 = await tools.commit_agent_turn(
                    reply_messages=["哼", "看我表情"],
                    reply_image_id=sticker_id,
                    reply_image_position="before",
                )
                assert res2["reply"]["status"] == "queued"
                assert res2["reply_image"]["status"] == "queued"
                assert [kind for kind, _ in events] == ["image", "text", "text"]
                assert len(waits) == 2
            finally:
                operation_id.reset(op2)

            # 3) Sticker-only turn without text
            events.clear()
            waits.clear()
            op3 = operation_id.set("turn-sticker-only")
            try:
                res3 = await tools.commit_agent_turn(reply_image_id=sticker_id)
                assert res3["reply"] == {"status": "omitted", "sent": False}
                assert res3["reply_image"]["status"] == "queued"
                assert [kind for kind, _ in events] == ["image"]
                assert waits == []
            finally:
                operation_id.reset(op3)

            # Re-sending an existing library sticker must not duplicate quick_send slots
            quick = store.quick_send_images(account_id="bot", limit=10)
            assert len(quick) == 1
            assert quick[0]["image_id"] == sticker_id
        finally:
            memory_context.reset(ctx_token)
            tools.db.close()
            store.close()

    asyncio.run(scenario())


def test_toolsets_split_tool_names_property_forwarding_and_dead_method_removal(tmp_path: Path):
    from echoloom.toolsets import MediaToolset, MemoryToolset, ReplyToolset

    (tmp_path / "data").mkdir()
    tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None, SystemClock())
    try:
        assert isinstance(tools.reply_tools, ReplyToolset)
        assert isinstance(tools.media_tools, MediaToolset)
        assert isinstance(tools.memory_tools, MemoryToolset)

        # Static TOOL_NAMES must match runtime agent_tools() with zero drift.
        for toolset in (tools.reply_tools, tools.media_tools, tools.memory_tools, tools):
            runtime_names = tuple(t.name for t in toolset.agent_tools())
            assert toolset.TOOL_NAMES == runtime_names

        reply_set = set(ReplyToolset.TOOL_NAMES)
        media_set = set(MediaToolset.TOOL_NAMES)
        memory_set = set(MemoryToolset.TOOL_NAMES)
        assert reply_set == {"commit_agent_turn", "schedule_followup"}
        assert media_set == {
            "list_wechat_images",
            "manage_wechat_image_library",
            "read_wechat_image",
            "read_wechat_file",
            "transcribe_latest_wechat_voice",
            "send_wechat_image",
            "send_wechat_file",
            "generate_and_send_wechat_image",
        }
        assert memory_set == {
            "remember_user_fact",
            "search_user_memory",
            "search_message_notes",
        }
        assert not (reply_set & media_set)
        assert not (reply_set & memory_set)
        assert not (media_set & memory_set)
        assert set(WeChatTools.TOOL_NAMES) == reply_set | media_set | memory_set
        assert len(WeChatTools.TOOL_NAMES) == 13

        # Uninitialized schema inspection (used by OpenAI schema tests) stays functional.
        uninit_names = tuple(t.name for t in WeChatTools.__new__(WeChatTools).agent_tools())
        assert uninit_names == WeChatTools.TOOL_NAMES

        # Unregistered compat delegates must remain callable but NOT in agent_tools().
        for compat in (
            "send_wechat_text",
            "reply_with_wechat_quote",
            "inspect_wechat_image",
            "inspect_latest_wechat_image",
            "adk_tools",
        ):
            assert callable(getattr(tools, compat))
            assert compat not in WeChatTools.TOOL_NAMES

        # Dead 0-caller methods must be removed while delivery_state keeps send_attempt_count.
        for dead in (
            "add_message_note",
            "revise_message_note",
            "delete_message_note",
            "upsert_entity_dictionary",
            "send_failure_status",
            "send_failure_reason",
            "send_attempt_count",
        ):
            assert not hasattr(tools, dead), dead
        assert "send_attempt_count" in tools.delivery_state("missing-scope")

        # Post-construction attribute assignments must forward to composed toolsets & outbound.
        fake_img_ingestor = object()
        fake_voice_ingestor = object()
        fake_openai = object()
        fake_store = SimpleNamespace(name="store")
        def gate() -> bool:
            return False
        tools.image_ingestor = fake_img_ingestor
        tools.voice_ingestor = fake_voice_ingestor
        tools.openai_client = fake_openai
        tools.memory_store = fake_store
        tools.can_send = gate
        assert tools.media_tools.image_ingestor is fake_img_ingestor
        assert tools.media_tools.voice_ingestor is fake_voice_ingestor
        assert tools.media_tools.openai_client is fake_openai
        assert tools.outbound.memory_store is fake_store
        assert tools.memory_tools.memory_store is fake_store
        assert tools.media_tools.memory_store is fake_store
        assert tools.outbound.can_send is gate
        assert tools.db is tools.outbound.db
    finally:
        tools.db.close()


def test_toolset_modules_have_no_hardcoded_hook_endpoints():
    root = Path(__file__).parents[1] / "src" / "echoloom"
    files = (
        root / "wechat_tools.py",
        root / "toolsets" / "__init__.py",
        root / "toolsets" / "reply.py",
        root / "toolsets" / "media.py",
        root / "toolsets" / "memory.py",
        root / "toolsets" / "workspace.py",
    )
    endpoints = (
        "/SendTextMsg", "/SendImgMsg", "/SendFileMsg",
        "/ForwardXMLMsg", "/Decode_Pic", "/Download_File", "/Upload_File", "/QueryDB/status",
    )
    for path in files:
        source = path.read_text("utf-8")
        for endpoint in endpoints:
            assert endpoint not in source, (path.name, endpoint)


def test_idle_mode_unlocks_historical_notes_user_facts_and_cross_chat_search(tmp_path):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        hist_room = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msg": "群里的历史消息"}, source_record={"msg": "群里的历史消息"},
            body_text="群里的历史消息",
        )
        other_acc = memory.archive_message(
            account_id="other_bot", conversation_id="other_room@chatroom", sender_id="member",
            raw_envelope={"msg": "别账号消息"}, source_record={"msg": "别账号消息"},
            body_text="别账号消息",
        )
        tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                            SystemClock(), memory_store=memory)

        # Idle turn where queried message_id is NOT in the 100-msg window mapping
        idle_token = memory_context.set({
            "account_id": "bot",
            "conversation_id": "idle-event:idle_123",
            "message_id": "",
            "is_idle_event": True,
            "message_ids": (),
            "message_conversations": {},
            "allowed_conversations": ("idle-event:idle_123",),
        })
        try:
            notes = await tools.add_message_notes([
                {"message_id": hist_room.message_id, "content": "窗口外历史消息也可正常补注"},
                {"message_id": other_acc.message_id, "content": "跨账号必须拒绝"},
            ])
            assert [r["status"] for r in notes["results"]] == ["saved", "unavailable"]

            # remember_user_fact in Idle requires explicit source_message_id
            missing_src = await tools.remember_user_fact("k1", "没有来源")
            assert missing_src["status"] == "rejected"
            assert missing_src["reason"] == "source_message_id_required_in_idle"

            saved_fact = await tools.remember_user_fact(
                "k1", "由Idle锚定真实消息写入", source_message_id=hist_room.message_id
            )
            assert saved_fact["status"] == "saved"
            assert saved_fact["source_message_id"] == hist_room.message_id

            # Idle search_message_notes searches across all account conversations
            found_notes = await tools.search_message_notes("窗口外 补注")
            assert len(found_notes["notes"]) == 1
        finally:
            memory_context.reset(idle_token)

        # Regular chat must still block cross-room notes and cross-room fact anchoring
        chat_token = memory_context.set({
            "account_id": "bot",
            "conversation_id": "wxid_dm",
            "message_id": "dm_msg",
            "is_idle_event": False,
            "message_ids": ("dm_msg",),
            "message_conversations": {"dm_msg": "wxid_dm"},
            "allowed_conversations": ("wxid_dm",),
        })
        try:
            cross_note = await tools.add_message_notes([
                {"message_id": hist_room.message_id, "content": "越权注群消息"},
            ])
            assert cross_note["results"][0]["status"] == "unavailable"
            cross_fact = await tools.remember_user_fact(
                "k2", "越权事实", source_message_id=hist_room.message_id
            )
            assert cross_fact["status"] == "rejected"
        finally:
            memory_context.reset(chat_token)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_phase2_memory_and_tag_tools_support_short_refs_and_safe_disambiguation(tmp_path: Path):
    from echoloom.contacts import ContactIdentity
    from echoloom.context import ContextTurn, ContextWindow
    from echoloom.turn_scope import tool_memory_context

    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        msg_alice = memory.archive_message(
            account_id="bot",
            conversation_id="room@chatroom",
            sender_id="wxid_alice",
            raw_envelope={"msg": "我每周五晚上打羽毛球"},
            source_record={"msg": "我每周五晚上打羽毛球"},
            body_text="我每周五晚上打羽毛球",
        )
        msg_bob = memory.archive_message(
            account_id="bot",
            conversation_id="room@chatroom",
            sender_id="wxid_bob",
            raw_envelope={"msg": "我也报个名"},
            source_record={"msg": "我也报个名"},
            body_text="我也报个名",
        )
        msg_charlie = memory.archive_message(
            account_id="bot",
            conversation_id="room@chatroom",
            sender_id="wxid_charlie",
            raw_envelope={"msg": "收到"},
            source_record={"msg": "收到"},
            body_text="收到",
        )

        refs = memory.short_refs_map(
            account_id="bot",
            items=[
                ("message", msg_alice.message_id),
                ("message", msg_bob.message_id),
                ("user", "wxid_alice"),
                ("user", "wxid_bob"),
                ("user", "wxid_charlie"),
                ("chat", "room@chatroom"),
            ],
        )
        m1 = refs[("message", msg_alice.message_id)]
        u_alice = refs[("user", "wxid_alice")]
        u_bob = refs[("user", "wxid_bob")]
        u_charlie = refs[("user", "wxid_charlie")]
        g_room = refs[("chat", "room@chatroom")]

        window = ContextWindow(
            account_id="bot",
            conversation_id="room@chatroom",
            is_group=True,
            turns=(
                ContextTurn("wxid_alice", "hook_group_member_wxid", "我每周五晚上打羽毛球", message_id=msg_alice.message_id),
                ContextTurn("wxid_bob", "hook_group_member_wxid", "我也报个名", message_id=msg_bob.message_id),
                ContextTurn("wxid_charlie", "hook_group_member_wxid", "收到", message_id=msg_charlie.message_id),
            ),
            identities={
                "wxid_alice": ContactIdentity(display_name="小明", user_name="wxid_alice", nickname="小明"),
                "wxid_bob": ContactIdentity(display_name="小明", user_name="wxid_bob", nickname="小明"),
                "wxid_charlie": ContactIdentity(display_name="老陈", user_name="wxid_charlie", remark="老陈"),
            },
        )
        # Attach short_refs dynamically (Phase 2 ContextWindow field)
        object.__setattr__(window, "short_refs", refs)
        ctx_dict = tool_memory_context(window)
        by_sid = {row["sender_id"]: row for row in ctx_dict["sender_choices"]}
        assert by_sid["wxid_alice"]["short_ref"] == u_alice
        assert by_sid["wxid_bob"]["short_ref"] == u_bob
        assert by_sid["wxid_charlie"]["short_ref"] == u_charlie

        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, None, None,
            SystemClock(), memory_store=memory,
        )
        token = memory_context.set(ctx_dict)
        op_token = operation_id.set("phase2-short-ref-ops")
        try:
            # 1) remember_user_fact with lowercase M# & invalid/wrong-kind rejection
            bad_fact = await tools.remember_user_fact("sport", "无效", source_message_id="M999")
            assert bad_fact == {"status": "rejected", "reason": "source_message_unavailable"}
            wrong_kind_fact = await tools.remember_user_fact("sport", "错类型", source_message_id=u_alice)
            assert wrong_kind_fact == {"status": "rejected", "reason": "source_message_unavailable"}
            saved_fact = await tools.remember_user_fact(
                "sport", "周五晚上打羽毛球", source_message_id=m1.lower(),
            )
            assert saved_fact["status"] == "saved"
            assert saved_fact["source_message_id"] == msg_alice.message_id

            found_facts = await tools.search_user_memory("羽毛球")
            assert len(found_facts["memories"]) == 1
            assert found_facts["memories"][0]["source_message_id"] == msg_alice.message_id
            assert found_facts["memories"][0]["source_message_ref"] == m1

            # 2) add_message_notes with M# and G# -> returns note_ref; search_message_notes enriches refs
            note_res = await tools.add_message_notes([
                {"message_id": m1.lower(), "conversation_id": g_room.lower(), "content": "羽毛球固定约局"},
                {"message_id": "M999", "content": "不存在"},
                {"message_id": m1, "conversation_id": "G999", "content": "无效会话"},
            ])
            assert [r["status"] for r in note_res["results"]] == ["saved", "unavailable", "unavailable"]
            saved_note = note_res["results"][0]
            assert saved_note["message_id"] == msg_alice.message_id
            assert saved_note["note_ref"].startswith("N")

            searched_notes = await tools.search_message_notes("固定约局")
            assert len(searched_notes["notes"]) == 1
            assert searched_notes["notes"][0]["short_ref"] == saved_note["note_ref"]
            assert searched_notes["notes"][0]["message_ref"] == m1

            # 3) Entity delete by lowercase D#
            ent = await tools.upsert_entity_dictionary_batch([
                {"term": "羽球局", "explanation": "周五晚固定羽毛球活动"},
            ])
            ent_id = ent["results"][0]["entity_id"]
            d_ref = memory.short_refs_map(account_id="bot", items=[("entity", ent_id)])[("entity", ent_id)]
            assert (await tools.delete_entity_dictionary_entry("D999"))["status"] == "rejected"
            assert (await tools.delete_entity_dictionary_entry(d_ref.lower()))["status"] == "deleted"

            # 4) Sender memory: U#, unique name, ambiguous name (returns short_ref), and P# delete
            amb_sender = tools._update_sender_from_entry(
                {"sender": "小明", "type": "偏好", "content": "喜欢打球"},
                ctx_dict["sender_choices"],
            )
            assert amb_sender["status"] == "ambiguous"
            assert {c["short_ref"] for c in amb_sender["choices"]} == {u_alice, u_bob}

            saved_mem = tools.upsert_sender_memory(u_alice.lower(), "偏好", "周五打羽毛球")
            assert saved_mem["status"] == "saved"
            mem_id = str(saved_mem["memory_id"])
            p_ref = memory.short_refs_map(account_id="bot", items=[("memory", mem_id)])[("memory", mem_id)]
            assert tools.delete_sender_memory("P999")["status"] == "unavailable"
            assert tools.delete_sender_memory(p_ref.lower())["status"] == "deleted"

            # 5) tag_updates: U#, raw sender_id, unique name via sender/name, and ambiguous disambiguation
            tag_res = tools.memory_tools.apply_tag_updates([
                {"sender_id": u_alice.lower(), "tags": ["球友"]},
                {"sender": "老陈", "tags": ["热心"]},
                {"name": "小明", "tags": ["重名测试"]},
                {"sender_id": "U999", "tags": ["不存在"]},
            ])
            assert tag_res[0]["status"] == "updated" and tag_res[0]["sender_id"] == "wxid_alice"
            assert tag_res[1]["status"] == "updated" and tag_res[1]["sender_id"] == "wxid_charlie"
            assert tag_res[2]["status"] == "ambiguous"
            assert tag_res[2]["reason"] == "sender_name_matches_multiple_people_use_sender_id"
            assert {c["short_ref"] for c in tag_res[2]["choices"]} == {u_alice, u_bob}
            assert tag_res[3]["status"] == "unavailable"
        finally:
            operation_id.reset(op_token)
            memory_context.reset(token)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())


def test_phase2_media_and_reply_tools_resolve_image_and_file_short_refs(tmp_path: Path):
    async def scenario():
        (tmp_path / "data").mkdir()
        memory = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")

        # Create archived image
        img_msg = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_alice",
            raw_envelope={"msg": "img"}, source_record={"msg": "img"}, body_text="img",
        )
        img_row = memory.begin_image_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=img_msg.message_id, original_references=[],
        )
        img_id = str(img_row["image_id"])
        img_buf = io.BytesIO()
        Image.new("RGB", (16, 16), (30, 120, 200)).save(img_buf, format="PNG")
        img_bytes = img_buf.getvalue()
        img_path = memory.media_root / f"{img_id}.png"
        img_path.parent.mkdir(parents=True, exist_ok=True)
        img_path.write_bytes(img_bytes)
        img_sha = hashlib.sha256(img_bytes).hexdigest()
        memory.complete_image_ingest(
            image_id=img_id, account_id="bot", artifact_path=str(img_path.resolve()),
            mime_type="image/png", byte_count=len(img_bytes), sha256=img_sha, source_sha256=img_sha,
        )
        i_ref = memory.short_refs_map(account_id="bot", items=[("image", img_id)])[("image", img_id)]

        # Create archived text file
        file_msg = memory.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="wxid_alice",
            raw_envelope={"msg": "file"}, source_record={"msg": "file"}, body_text="file",
        )
        file_row = memory.begin_file_ingest(
            account_id="bot", conversation_id="room@chatroom",
            message_id=file_msg.message_id, original_references=[],
            filename="readme.txt", fileext="txt",
        )
        file_id = str(file_row["file_id"])
        file_bytes = b"line-one\nline-two\n"
        file_path = memory.file_root / f"{file_id}.txt"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_bytes(file_bytes)
        memory.complete_file_ingest(
            file_id=file_id, account_id="bot", artifact_path=str(file_path.resolve()),
            mime_type="text/plain", byte_count=len(file_bytes),
            sha256=hashlib.sha256(file_bytes).hexdigest(),
        )
        f_ref = memory.short_refs_map(account_id="bot", items=[("file", file_id)])[("file", file_id)]
        m_file_ref = memory.short_refs_map(
            account_id="bot", items=[("message", file_msg.message_id)],
        )[("message", file_msg.message_id)]

        sent_images: list[Path] = []

        class Gateway:
            def send_image(self, room, path):
                sent_images.append(Path(path))
                return "queued", "hook_queued_not_delivery_proof"

        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", None, None, None,
            SystemClock(), memory_store=memory, gateway=Gateway(),
        )
        ctx_token = memory_context.set({
            "account_id": "bot",
            "conversation_id": "room@chatroom",
            "message_id": file_msg.message_id,
            "current_message_ids": (file_msg.message_id,),
            "allowed_conversations": ("room@chatroom",),
        })
        try:
            # list_wechat_images exposes short_ref
            listed = tools.list_wechat_images()
            assert listed["images"][0]["image_id"] == img_id
            assert listed["images"][0]["short_ref"] == i_ref

            # manage_wechat_image_library resolves I#
            managed = tools.manage_wechat_image_library(i_ref.lower(), description="蓝色方块", quick_send=True)
            assert managed["status"] == "updated"
            assert tools.manage_wechat_image_library("I999", quick_send=True)["status"] == "unavailable"

            # apply_image_notes resolves I#
            note_res = tools.memory_tools.apply_image_notes([
                {"image_id": i_ref.lower(), "content": "一张蓝色方块测试图"},
                {"image_id": "I999", "content": "无效短编号"},
            ])
            assert note_res[0]["status"] == "saved" and note_res[0]["image_id"] == img_id
            assert note_res[1]["status"] == "unavailable"

            # read_wechat_image resolves I# (returns cached vision_summary) and preserves "latest"
            read_by_ref = await tools.read_wechat_image(i_ref.lower(), "图里是什么")
            assert read_by_ref["image_id"] == img_id
            assert read_by_ref["description"] == "一张蓝色方块测试图"
            assert read_by_ref["cached"] is True
            read_latest = await tools.read_wechat_image("latest", "图里是什么")
            assert read_latest["image_id"] == img_id
            assert (await tools.read_wechat_image("I999", "图里是什么"))["error"] == "saved_image_not_found"

            # _image, send_wechat_image, and commit_agent_turn(reply_image_id) resolve I#
            assert tools._image(i_ref.lower()) == img_path.resolve()
            op1 = operation_id.set("send-img-by-short-ref")
            try:
                sent_direct = await tools.send_wechat_image(i_ref.lower())
                assert sent_direct["status"] == "queued"
            finally:
                operation_id.reset(op1)

            op2 = operation_id.set("commit-img-by-short-ref")
            try:
                committed = await tools.commit_agent_turn(reply_image_id=i_ref.lower())
                assert committed["reply_image"]["status"] == "queued"
            finally:
                operation_id.reset(op2)

            op3 = operation_id.set("commit-img-invalid-ref")
            try:
                bad_commit = await tools.commit_agent_turn(reply_image_id="I999")
                assert bad_commit["reply_image"]["status"] == "rejected"
            finally:
                operation_id.reset(op3)

            assert len(sent_images) == 2

            # read_wechat_file resolves F#, raw file_id, M#, and preserves "latest"
            by_f = await tools.read_wechat_file(f_ref.lower())
            assert by_f["file_id"] == file_id and "line-one" in str(by_f["content"])
            by_raw = await tools.read_wechat_file(file_id)
            assert by_raw["file_id"] == file_id
            by_m = await tools.read_wechat_file(m_file_ref.lower())
            assert by_m["file_id"] == file_id
            by_latest = await tools.read_wechat_file("latest")
            assert by_latest["file_id"] == file_id
            missing_file = await tools.read_wechat_file("F999")
            assert missing_file["error"] == "file_not_found"
        finally:
            memory_context.reset(ctx_token)
            tools.db.close()
            memory.close()

    asyncio.run(scenario())
