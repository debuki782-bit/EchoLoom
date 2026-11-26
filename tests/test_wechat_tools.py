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

from wechat_agent.event_book import EventBook
from wechat_agent.hook_outbound import HookSendResult
from wechat_agent.memory_store import MemoryStore
from wechat_agent.mock import SystemClock
from wechat_agent.storage import Repository
from wechat_agent.usage import UsageLedger
from wechat_agent.wechat_tools import (
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
    import wechat_agent.wechat_tools as wt_mod

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

    from wechat_agent.memory_store import MemoryStore

    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    monkeypatch.setattr("wechat_agent.wechat_tools.asyncio.sleep", fake_sleep)

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
    from wechat_agent.toolsets import MediaToolset, MemoryToolset, ReplyToolset

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
    root = Path(__file__).parents[1] / "src" / "wechat_agent"
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
