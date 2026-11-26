"""Test-only observations must not acquire production gateway trust."""

import asyncio
import json
import sqlite3
import threading
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from echoloom.runtime_contract import (
    BudgetExhausted as AdkBudgetExhausted,
    ModelCallsLimitExceeded as LlmCallsLimitExceededError,
    ModelProviderUnavailable,
)

from echoloom.adk_runner import ScopedAdkRunner
from echoloom.contacts import ContactIdentity
from echoloom.context import build_hook_context
from echoloom.context_assembler import MessageContextAssembler
from echoloom.hook_contract import HookObservation, parse_callback
from echoloom.hook_outbound import HookSendResult, HookTextSender
from echoloom.memory_store import MemoryStore
from echoloom.models import ConversationKey
from echoloom.pilot import Pilot, _current_message_id
from echoloom.rich_message import normalize_message
from echoloom.time_scheduler import TimeScheduler
from echoloom.wechat_tools import quote_context


def observed(text="你好", *, room="room@chatroom", tick=123, sender="member"):
    return HookObservation("bot", room, sender, "hook_group_member_wxid", text, True, 1, None, tick)


def test_end_to_end_hook_callback_to_adk_and_mock_hook_send(tmp_path: Path):
    async def scenario():
        route = ConversationKey("bot", "room@chatroom")
        received_send = threading.Event()
        hook_requests = []

        def hook_response(request):
            hook_requests.append(request)
            received_send.set()
            return httpx.Response(200, json={"ret": 0, "queued": True}, request=request)

        sender = HookTextSender(
            frozenset({route}),
            enabled=True,
            transport=httpx.MockTransport(hook_response),
        )
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        assembler = MessageContextAssembler(memory, max_user_bytes=6000)
        adk = ScopedAdkRunner(tmp_path / "adk-sessions.sqlite3")
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            adk.reply,
            sender.submit,
            memory_store=memory,
            context_assembler=assembler,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        callback = {
            "selfwxid": "bot",
            "sendorrecv": 2,
            "msglist": [{
                "fromid": "room@chatroom",
                "toid": "bot",
                "room": "room@chatroom",
                "sender": "wxid_member_e2e",
                "msg": "wxid_member_e2e:\n端到端联调消息",
                "msgtype": 1,
                "msgsvrid": 9001,
                "time": 1_792_000_000,
            }],
        }
        observations = parse_callback(callback)
        assert len(observations) == 1
        assert pilot.offer(observations[0]) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        try:
            assert await asyncio.to_thread(received_send.wait, 5)
            assert len(hook_requests) == 1
            request = hook_requests[0]
            assert request.url.path == "/SendTextMsg"
            payload = json.loads(request.content)
            assert payload == {
                "wxidorgid": "room@chatroom",
                "msg": "[MOCK ADK] 已收到消息。",
            }
            assert pilot.scheduler.pending_count() == 0
            assert pilot.scheduler.state_counts().get("model_seen") == 1
            archived = memory.db.execute(
                "SELECT count(*) FROM source_messages WHERE account_id='bot'",
            ).fetchone()[0]
            assert archived == 2  # inbound plus queued Agent reply
            inbound = memory.db.execute(
                "SELECT sender_id,body_text FROM source_messages WHERE direction='inbound'",
            ).fetchone()
            assert tuple(inbound) == ("wxid_member_e2e", "wxid_member_e2e:\n端到端联调消息")
            trace_files = list((tmp_path / "session-traces").glob("*.jsonl"))
            assert len(trace_files) == 1
            trace_text = trace_files[0].read_text(encoding="utf-8")
            assert "端到端联调消息" in trace_text
            assert "[MOCK ADK] 已收到消息。" in trace_text
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            sender.close()
            await adk.close()
            memory.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_new_group_hook_event_flows_through_dynamic_route_and_reply(tmp_path: Path):
    async def scenario():
        new_room = "new-room@chatroom"
        requests = []

        def hook_response(request):
            requests.append(request)
            return httpx.Response(200, json={"ret": 0, "queued": True}, request=request)

        sender = HookTextSender(
            frozenset({ConversationKey("bot", "room@chatroom")}),
            enabled=True,
            transport=httpx.MockTransport(hook_response),
        )
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        adk = ScopedAdkRunner(tmp_path / "adk-sessions.sqlite3")

        def admit(room):
            sender.allow_conversation(ConversationKey("bot", room))
            return True

        pilot = Pilot(
            tmp_path / "pilot.sqlite3", "bot", "room@chatroom",
            adk.reply, sender.submit, memory_store=memory,
            context_assembler=MessageContextAssembler(memory, max_user_bytes=6000),
            admit_group=admit,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        callback = {
            "selfwxid": "bot", "sendorrecv": 2,
            "msglist": [{
                "fromid": new_room, "toid": "bot", "room": new_room,
                "sender": "wxid_member_e2e",
                "msg": "wxid_member_e2e:\n新聊天串验证",
                "msgtype": 1, "msgsvrid": 9101, "time": 1_792_000_100,
            }],
        }
        try:
            observations = parse_callback(callback)
            assert pilot.offer(observations[0]) == "queued_test"
            worker = asyncio.create_task(pilot.worker())
            for _ in range(100):
                if requests:
                    break
                await asyncio.sleep(0.05)
            assert len(requests) == 1
            assert json.loads(requests[0].content)["wxidorgid"] == new_room
            assert pilot.scheduler.state_counts() == {"model_seen": 1}
            assert await adk.sessions.list_sessions(
                app_name="wechat_agent_mock",
            )
        finally:
            if "worker" in locals():
                worker.cancel()
                await asyncio.gather(worker, return_exceptions=True)
            sender.close()
            await adk.close()
            memory.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_pilot_receipt_prevents_same_callback_sending_twice_across_restart(tmp_path: Path):
    async def scenario():
        sent = []

        async def reply(context):
            assert context.turns[0].sender_id == "member"
            return "测试回复"

        def send(draft):
            sent.append(draft)
            return HookSendResult("queued")

        path = tmp_path / "pilot.sqlite3"
        pilot = Pilot(path, "bot", "room@chatroom", reply, send)
        assert await pilot.process(observed()) == "queued"
        pilot.db.close()
        pilot = Pilot(path, "bot", "room@chatroom", reply, send)
        assert await pilot.process(observed()) == "duplicate_receipt"
        assert len(sent) == 1
        assert pilot.status()["trusted_gateway"] is False
        pilot.db.close()

    asyncio.run(scenario())


def test_status_reports_live_hook_login_readiness(tmp_path: Path):
    async def scenario():
        async def login_check():
            return True

        pilot = Pilot(
            tmp_path / "pilot.sqlite3", "bot", "room@chatroom", lambda _ctx: None,
            lambda _draft: HookSendResult("queued"), login_check=login_check,
        )
        worker = asyncio.create_task(pilot.login_watch_worker())
        try:
            await asyncio.sleep(0.01)
            assert pilot.status()["trusted_gateway"] is True
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            pilot.db.close()

    asyncio.run(scenario())


def test_llm_call_limit_is_terminal_for_batch_and_does_not_hot_replay(tmp_path: Path):
    async def scenario():
        calls = 0
        failed = asyncio.Event()

        async def reply(_context):
            nonlocal calls
            calls += 1
            failed.set()
            raise LlmCallsLimitExceededError("bounded ADK tool loop")

        pilot = Pilot(
            tmp_path / "llm-limit.sqlite3", "bot", "room@chatroom", reply,
            lambda _draft: HookSendResult("queued"),
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        assert pilot.offer(observed()) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        try:
            await asyncio.wait_for(failed.wait(), timeout=2)
            for _ in range(50):
                if pilot.scheduler.deferred_counts().get("model_llm_calls_limit_exceeded"):
                    break
                await asyncio.sleep(0.01)
            # Single-source-of-truth: never kill unseen messages as failed; backoff in pending.
            assert pilot.scheduler.failed_count() == 0
            assert pilot.scheduler.state_counts() == {"pending": 1}
            assert pilot.scheduler.deferred_counts() == {"model_llm_calls_limit_exceeded": 1}
            await asyncio.sleep(0.15)
            assert calls == 1
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            pilot.db.close()

    asyncio.run(scenario())


def test_followup_wait_merges_same_chat_inbound_without_global_debounce(tmp_path: Path):
    async def scenario():
        received = []

        async def reply(context):
            received.append(context)
            return ""

        pilot = Pilot(
            tmp_path / "pilot.sqlite3", "bot", "room@chatroom", reply,
            lambda _: HookSendResult("queued"),
        )
        pilot.FOLLOWUP_MERGE_WINDOW_SECONDS = 0.02
        context = build_hook_context((observed("原始提问"),))
        timer = None
        try:
            result = await pilot._schedule_followup(context, "parent", "等回应", 0)
            assert result["merge_window_seconds"] == 60
            assert result["cancel_on_new_message"] is False
            assert pilot.offer(observed("补充上下文", tick=124)) == "queued_test"
            timer = pilot._scheduled_followups[("bot", "room@chatroom")]
            await asyncio.wait_for(timer, timeout=2)
            assert len(received) == 1
            assert received[0].scheduled_task == "等回应"
            assert any(turn.text == "补充上下文" for turn in received[0].turns)
            assert pilot.scheduler.state_counts().get("model_seen") == 1
        finally:
            if timer is not None and not timer.done():
                timer.cancel()
                await asyncio.gather(timer, return_exceptions=True)
            pilot.db.close()

    asyncio.run(scenario())


def test_startup_backlog_recovery_enqueues_only_unreplied_legacy_inbound_once(
    tmp_path: Path,
):
    payload = {
        "sendorrecv": 2,
        "selfwxid": "bot",
        "msglist": [
            {
                "fromid": "wxid_friend",
                "toid": "bot",
                "room": "",
                "sender": "wxid_friend",
                "msg": "服务重启前的未处理消息",
                "msgtype": 1,
                "msgsvrid": 0,
                "time": 123,
            }
        ],
    }
    event = parse_callback(payload)[0]
    memory = MemoryStore(tmp_path / "memory.sqlite3")
    archived = memory.archive_message(
        account_id=event.account_id,
        conversation_id=event.conversation_id,
        sender_id=event.sender_id,
        raw_envelope=payload,
        source_record=json.loads(event.raw_message_json),
        body_text=event.text,
    )
    pilot = Pilot(
        tmp_path / "pilot.sqlite3",
        "bot",
        "room@chatroom",
        lambda _: asyncio.sleep(0, result=""),
        lambda _: HookSendResult("queued"),
        memory_store=memory,
    )
    try:
        assert archived.message_id
        first = pilot.recover_archived_backlog()
        assert first == {"scanned": 1, "queued": 1, "existing": 0, "skipped": 0}
        assert pilot.scheduler.pending_count() == 1
        second = pilot.recover_archived_backlog()
        assert second == {"scanned": 1, "queued": 0, "existing": 1, "skipped": 0}
        assert pilot.scheduler.pending_count() == 1
        memory.archive_agent_message(
            account_id="bot",
            conversation_id="wxid_friend",
            text="已回复",
            operation_id="backlog-test",
            delivery_state="queued",
        )
        assert pilot.recover_archived_backlog()["scanned"] == 0
    finally:
        pilot.db.close()
        memory.close()


def test_agent_tool_queued_send_is_not_reported_as_sent(tmp_path: Path):
    async def scenario():
        send_calls = []

        async def reply(context):
            return "tool reply"

        def delivery_state(_scope):
            return {
                "failure_count": 0,
                "last_failure_status": None,
                "last_failure_reason": None,
                "send_attempt_count": 1,
                "last_send_status": "queued",
            }

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda draft: send_calls.append(draft) or HookSendResult("queued"),
            delivery_state=delivery_state,
        )
        assert await pilot.process(observed()) == "agent_tool_send_unconfirmed"
        assert send_calls == []
        pilot.db.execute(
            "INSERT INTO attempts VALUES (?,?)",
            ("legacy-tool-receipt", "sent_by_agent_tool"),
        )
        pilot.db.commit()
        assert pilot.status()["attempts"]["agent_tool_send_unconfirmed"] == 2
        assert "sent_by_agent_tool" not in pilot.status()["attempts"]
        pilot.db.close()

    asyncio.run(scenario())


def test_pilot_scope_pause_and_limit_stop_model_and_send(tmp_path: Path):
    async def scenario():
        calls = []

        async def reply(context):
            calls.append(context)
            return "reply"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda draft: HookSendResult("queued"),
            limit=1,
        )
        assert pilot.offer(observed(room="other@chatroom")) == "outside_test_scope"
        assert pilot.offer(observed(sender="bot")) == "observation_only"
        assert await pilot.process(observed()) == "queued"
        assert await pilot.process(observed(tick=124)) == "test_limit"
        assert len(calls) == 1
        assert pilot.offer(observed("/ai off")) == "paused"
        assert pilot.offer(observed("/ai on")) == "paused"
        assert await pilot.process(observed(tick=125)) == "paused"
        pilot.db.close()

    asyncio.run(scenario())


def test_pause_while_model_runs_prevents_pending_send(tmp_path: Path):
    async def scenario():
        entered, release = asyncio.Event(), asyncio.Event()
        sent = []

        async def reply(context):
            entered.set()
            await release.wait()
            return "old reply"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda draft: sent.append(draft) or HookSendResult("queued"),
        )
        task = asyncio.create_task(pilot.process(observed()))
        await entered.wait()
        pilot.offer(observed("/ai off", tick=124))
        release.set()
        assert await task == "cancelled_pause"
        assert not sent
        pilot.db.close()

    asyncio.run(scenario())


def test_pilot_resolves_sender_and_quoted_author_by_stable_id(tmp_path: Path):
    async def scenario():
        seen = []
        looked_up = []

        async def reply(context):
            assert quote_context.get() is None
            seen.append(context)
            return "已识别"

        def resolve(user_id):
            looked_up.append(user_id)
            names = {
                "wxid_member": "消息发送者",
                "wxid_author": "引用作者",
                "room@chatroom": "测试群名",
            }
            return ContactIdentity(names[user_id])

        raw = HookObservation(
            "bot",
            "room@chatroom",
            "wxid_member",
            "hook_group_member_wxid",
            "wxid_member:\n<msg><appmsg><type>57</type><title>回应</title><refermsg>"
            "<type>1</type><fromusr>room@chatroom</fromusr><chatusr>wxid_author</chatusr>"
            "<displayname>不可信别名</displayname><content>原消息</content>"
            "</refermsg></appmsg></msg>",
            True,
            49,
            None,
            200,
        )
        normalized = normalize_message(raw)
        assert normalized is not None
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            resolve_contact=resolve,
        )
        assert await pilot.process(normalized) == "queued"
        context = seen[0]
        assert context.identities["room@chatroom"].display_name == "测试群名"
        current = context.turns[0]
        quote = json.loads(current.text)["quote"]
        assert quote["author_id"] == "wxid_author"
        assert context.identities["wxid_member"].display_name == "消息发送者"
        assert context.identities["wxid_author"].display_name == "引用作者"
        assert set(looked_up) == {"wxid_member", "wxid_author", "room@chatroom"}
        serialized = context.model_user_content()
        assert "room@chatroom" in serialized
        assert "wxid_member" in serialized and "wxid_author" in serialized
        assert "不可信别名" not in serialized
        pilot.db.close()

    asyncio.run(scenario())


def test_pilot_archives_image_and_adds_only_reference_metadata_to_agent_context(tmp_path: Path):
    async def scenario():
        seen = []
        ingested = []
        image_url = "https://wx.qpic.cn/group-image?sig=private"

        async def reply(context):
            seen.append(context)
            return "收到图片"

        class ImageIngestor:
            def ingest(self, *, item, message_id, references, store):
                ingested.append((item, message_id, references))
                return {
                    "state": "ready",
                    "mime_type": "image/png",
                    "byte_count": 123,
                    "sha256": "a" * 64,
                }

        raw = HookObservation(
            "bot",
            "room@chatroom",
            "wxid_member",
            "hook_group_member_wxid",
            f'wxid_member:\n<msg><img cdnhdimgurl="{image_url}"/></msg>',
            True,
            3,
            None,
            321,
        )
        image = normalize_message(raw)
        assert image is not None
        memory = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            memory_store=memory,
            image_ingestor=ImageIngestor(),
        )
        assert pilot.offer(raw) == "queued_test"
        assert await pilot.process(image) == "queued"
        assert len(ingested) == 1
        event = seen[0].turns[0]
        message_data = json.loads(event.text)
        assert message_data["kind"] == "image_message"
        assert event.image_metadata["state"] == "ready"
        serialized = seen[0].model_user_content()
        assert image_url not in serialized
        assert "image_bytes" not in serialized and "artifact_path" not in serialized
        assert "authorization_verified" not in serialized
        pilot.db.close()
        memory.close()

    asyncio.run(scenario())


def test_pilot_archives_sticker_media_for_latest_image_tool(tmp_path: Path):
    async def scenario():
        ingested = []
        seen = []

        async def reply(context):
            seen.append(context)
            return "收到表情"

        class ImageIngestor:
            def ingest(self, *, item, message_id, references, store):
                ingested.append((item.original_message_type, references))
                return {
                    "state": "ready",
                    "mime_type": "image/gif",
                    "byte_count": 80,
                    "sha256": "a" * 64,
                }

        raw = HookObservation(
            "bot",
            "room@chatroom",
            "wxid_member",
            "hook_group_member_wxid",
            '<msg><emoji cdnurl="https://wx.qpic.cn/sticker.gif"/></msg>',
            True,
            47,
            None,
            456,
        )
        event = normalize_message(raw)
        assert event is not None
        memory = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            memory_store=memory,
            image_ingestor=ImageIngestor(),
        )

        assert await pilot.process(event) == "queued"
        assert ingested == [(47, [{"field": "cdnurl", "value": "https://wx.qpic.cn/sticker.gif"}])]
        assert json.loads(seen[0].turns[0].text)["kind"] == "image_message"
        assert "sticker.gif" not in seen[0].model_user_content()
        pilot.db.close()
        memory.close()

    asyncio.run(scenario())


def test_pilot_stores_voice_bytes_separately_and_keeps_sender_turn_text_only(tmp_path: Path):
    async def scenario():
        seen = []

        async def reply(context):
            seen.append(context)
            return "收到语音"

        class VoiceIngestor:
            def ingest(self, *, item, message_id, store):
                return {"audio_id": "aud_test", "state": "ready", "duration_ms": 1000}

        raw_record = {
            "fromid": "room@chatroom",
            "toid": "bot",
            "room": "room@chatroom",
            "sender": "wxid_member",
            "msg": "<msg><voicemsg/></msg>",
            "msgtype": 34,
            "msgsvrid": 7,
            "time": 123,
            "voice_hex": "secret-audio",
        }
        envelope = {"sendorrecv": 2, "selfwxid": "bot", "msglist": [raw_record]}
        raw = HookObservation(
            "bot",
            "room@chatroom",
            "wxid_member",
            "hook_group_member_wxid",
            raw_record["msg"],
            True,
            34,
            7,
            123,
            raw_envelope_json=json.dumps(envelope),
            raw_message_json=json.dumps(raw_record),
            original_message_type=34,
        )
        event = normalize_message(raw)
        assert event is not None
        memory = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            memory_store=memory,
            voice_ingestor=VoiceIngestor(),
        )

        assert await pilot.process(event) == "queued"
        archived = memory.db.execute("SELECT raw_json FROM source_messages").fetchone()[0]
        assert "secret-audio" not in archived
        serialized = seen[0].model_user_content()
        assert "secret-audio" not in serialized
        assert json.loads(seen[0].turns[0].text)["kind"] == "voice_message"
        assert seen[0].turns[0].audio_metadata["audio_id"] == "aud_test"
        pilot.db.close()
        memory.close()

    asyncio.run(scenario())


def test_pilot_attaches_per_message_time_and_daily_prompt_separately(tmp_path: Path):
    async def scenario():
        seen = []

        class DailyEnvironment:
            def system_prompt_context(self):
                return "深圳今日天气与节假日背景"

        async def reply(context):
            seen.append(context)
            return "收到"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            daily_environment=DailyEnvironment(),
            memory_store=MemoryStore(tmp_path / "data" / "agent-memory.sqlite3"),
        )
        assert pilot.offer(observed(tick=1_000_000)) == "queued_test"
        queued = pilot.queue.get_nowait()
        assert queued.event_time_context is not None
        assert await pilot.process(queued) == "queued"
        assert seen[0].turns[0].message_time is not None
        assert set(seen[0].turns[0].message_time) == {
            "timezone",
            "local_minute",
            "time_period",
        }
        assert seen[0].daily_prompt_context == "深圳今日天气与节假日背景"
        assert "深圳今日天气与节假日背景" not in seen[0].model_user_content()
        saved = pilot.memory_store.db.execute(
            "SELECT event_time_context_json FROM source_messages",
        ).fetchone()
        assert saved[0] is not None
        assert "local_minute" in saved[0] and "time_period" in saved[0]
        pilot.memory_store.db.close()
        pilot.db.close()

    asyncio.run(scenario())


def test_inbound_resets_session_activity_before_model_turn(tmp_path: Path):
    async def scenario():
        order = []

        async def record_activity(account_id, conversation_id):
            order.append(("activity", account_id, conversation_id))

        async def reply(_context):
            order.append(("model",))
            return ""

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            record_activity=record_activity,
        )
        assert await pilot.process(observed()) == "empty_reply"
        assert order == [
            ("activity", "bot", "room@chatroom"),
            ("model",),
        ]
        pilot.db.close()

    asyncio.run(scenario())


def test_empty_adk_result_after_tool_send_is_not_recorded_as_empty_reply(tmp_path: Path):
    async def scenario():
        pilot = Pilot(
            tmp_path / "pilot.sqlite3", "bot", "room@chatroom",
            lambda _context: asyncio.sleep(0, result=""),
            lambda _: HookSendResult("queued"),
            delivery_state=lambda _scope: {
                "last_send_status": "queued", "failure_count": 0,
            },
        )
        assert await pilot.process(observed()) == "agent_tool_send_unconfirmed"
        pilot.db.close()

    asyncio.run(scenario())


def test_current_mutation_message_id_does_not_come_from_cross_chat_tail():
    context = build_hook_context((observed("当前输入"),))
    current_turn = replace(context.turns[-1], message_id="current-message")
    other_turn = replace(
        current_turn,
        message_id="other-chat-message",
        source_conversation_id="other@chatroom",
    )
    context = replace(
        context,
        turns=(current_turn,),
        global_timeline_turns=(other_turn,),
    )
    assert _current_message_id(context) == "current-message"


def test_new_valid_group_is_admitted_from_any_valid_hook_sender(tmp_path: Path):
    admitted = []
    pilot = Pilot(
        tmp_path / "pilot.sqlite3", "bot", "room@chatroom",
        lambda _context: asyncio.sleep(0, result=""),
        lambda _: HookSendResult("queued"),
        admit_group=lambda room: admitted.append(room) is None,
    )
    new_room = "new-room@chatroom"
    member_message = observed(
        "wxid_member9:\nhello", room=new_room, sender="wxid_member9",
    )
    assert pilot.offer(member_message) == "queued_test"
    assert admitted == [new_room]
    assert new_room in pilot.registered_group_ids

    followup = observed(
        "wxid_member2:\n继续", room=new_room, sender="wxid_member2",
    )
    assert pilot.offer(followup) == "queued_test"
    pilot.db.close()


def test_new_group_is_not_enrolled_when_route_persistence_fails(tmp_path: Path):
    def fail_to_persist(_room):
        raise OSError("disk unavailable")

    pilot = Pilot(
        tmp_path / "pilot.sqlite3", "bot", "room@chatroom",
        lambda _context: asyncio.sleep(0, result=""),
        lambda _: HookSendResult("queued"),
        admit_group=fail_to_persist,
    )
    new_room = "new-room@chatroom"
    message = observed("wxid_member9:\nhello", room=new_room, sender="wxid_member9")
    assert pilot.offer(message) == "group_route_persist_failed"
    assert new_room not in pilot.registered_group_ids
    assert pilot.scheduler.pending_count() == 0
    pilot.db.close()


def test_invalid_group_route_or_sender_is_not_auto_admitted(tmp_path: Path):
    admitted = []
    pilot = Pilot(
        tmp_path / "pilot.sqlite3", "bot", "room@chatroom",
        lambda _context: asyncio.sleep(0, result=""),
        lambda _: HookSendResult("queued"),
        admit_group=lambda room: admitted.append(room) is None,
    )
    invalid = observed("message", room="not-a-room", sender="wxid_member9")
    assert pilot.offer(invalid) == "outside_test_scope"
    invalid_sender = observed("message", room="new-room@chatroom", sender="member")
    assert pilot.offer(invalid_sender) == "outside_test_scope"
    assert admitted == []
    pilot.db.close()


def test_live_worker_injects_backlog_as_one_context_and_marks_all_seen(tmp_path: Path):
    async def scenario():
        contexts = []
        memory = MemoryStore(tmp_path / "memory.sqlite3")

        async def reply(context):
            contexts.append(context)
            return ""

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _: HookSendResult("queued"),
            memory_store=memory,
            context_assembler=MessageContextAssembler(memory, max_user_bytes=6000),
        )
        pilot.scheduler.debounce_seconds = 0.05
        pilot.scheduler.max_wait_seconds = 0.2
        worker = asyncio.create_task(pilot.worker())
        try:
            assert pilot.offer(observed("积压第一条", tick=100)) == "queued_test"
            assert pilot.offer(observed("积压第二条", tick=101)) == "queued_test"
            for _ in range(80):
                states = pilot.scheduler.state_counts()
                if states.get("model_seen") == 2:
                    break
                await asyncio.sleep(0.05)
            assert pilot.scheduler.state_counts() == {"model_seen": 2}
            assert len(contexts) == 1
            assert len(contexts[0].turns) == 2
            rendered = contexts[0].model_user_content()
            assert rendered.index("积压第一条") < rendered.index("积压第二条")
        finally:
            worker.cancel()
            try:
                await worker
            except asyncio.CancelledError:
                pass
            memory.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_live_worker_splits_large_backlog_without_marking_unseen_input(tmp_path: Path):
    async def scenario():
        seen = []
        memory = MemoryStore(tmp_path / "memory.sqlite3")

        async def reply(context):
            seen.extend(turn.text for turn in context.turns)
            return ""

        pilot = Pilot(tmp_path / "pilot.sqlite3", "bot", "room@chatroom", reply,
                      lambda _: HookSendResult("queued"), memory_store=memory,
                      context_assembler=MessageContextAssembler(memory, max_user_bytes=6000))
        pilot.scheduler.debounce_seconds = 0.05
        pilot.scheduler.max_wait_seconds = 0.2
        messages = [str(index) + "长" * 5000 for index in range(3)]
        for index, message in enumerate(messages):
            assert pilot.offer(observed(message, tick=200 + index)) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        try:
            for _ in range(120):
                if pilot.scheduler.state_counts().get("model_seen") == len(messages):
                    break
                await asyncio.sleep(0.05)
            assert pilot.scheduler.state_counts() == {"model_seen": len(messages)}
            assert seen == messages
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            memory.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_provider_outage_defers_past_retry_limit_coalesces_and_stops_probing_when_idle(
    tmp_path: Path,
):
    async def scenario():
        provider_ok = False
        probe_calls = 0
        ten_probes = asyncio.Event()
        first_fail = asyncio.Event()
        replied = asyncio.Event()
        reply_contexts = []
        sent_drafts = []

        async def provider_check() -> bool:
            nonlocal probe_calls
            probe_calls += 1
            if probe_calls >= 10:
                ten_probes.set()
            return provider_ok

        async def reply(context):
            if not provider_ok:
                first_fail.set()
                raise ModelProviderUnavailable("upstream unreachable")
            reply_contexts.append(context)
            replied.set()
            return "合并恢复回复"

        def send(draft):
            sent_drafts.append(draft)
            return HookSendResult("queued")

        memory = MemoryStore(tmp_path / "memory.sqlite3")
        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            send,
            limit=1,
            memory_store=memory,
            context_assembler=MessageContextAssembler(memory, max_user_bytes=6000),
            provider_check=provider_check,
            provider_probe_base_seconds=0.01,
            provider_probe_max_seconds=0.02,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        assert pilot.provider_ready is True
        assert pilot.status()["provider_ready"] is True

        worker = asyncio.create_task(pilot.worker())
        try:
            # Closed circuit with empty inbox must make 0 probes.
            await asyncio.sleep(0.05)
            assert probe_calls == 0

            assert pilot.offer(observed("断网第一条", tick=101)) == "queued_test"
            await asyncio.wait_for(first_fail.wait(), timeout=2)
            for _ in range(50):
                if not pilot.provider_ready:
                    break
                await asyncio.sleep(0.01)
            assert pilot.provider_ready is False
            assert pilot.status()["provider_ready"] is False

            # Second message arrives during the prolonged outage (> MAX_PROCESS_ATTEMPTS probes).
            assert pilot.offer(observed("断网第二条", tick=102)) == "queued_test"
            await asyncio.wait_for(ten_probes.wait(), timeout=3)
            assert probe_calls >= 10
            assert pilot.scheduler.failed_count() == 0
            assert pilot.scheduler.deferred_counts() == {"provider_unavailable": 1}
            outage_attempts = pilot.db.execute(
                "SELECT attempts FROM pilot_inbox WHERE last_error='provider_unavailable'"
            ).fetchone()[0]
            assert outage_attempts == 0

            # Recover provider; deferred msg1 + msg2 must coalesce into 1 reply + 1 send.
            provider_ok = True
            await asyncio.wait_for(replied.wait(), timeout=3)
            for _ in range(50):
                if sent_drafts and pilot.scheduler.active_count() == 0:
                    break
                await asyncio.sleep(0.01)

            assert len(reply_contexts) == 1
            assert [turn.text for turn in reply_contexts[0].turns] == [
                "断网第一条",
                "断网第二条",
            ]
            assert len(sent_drafts) == 1
            assert sent_drafts[0].text == "合并恢复回复"
            assert pilot.scheduler.failed_count() == 0
            assert pilot.scheduler.state_counts() == {"model_seen": 2}
            assert pilot.provider_ready is True
            assert pilot.status()["provider_ready"] is True

            # Verify 0 additional probes once inbox is empty and circuit is closed.
            probes_after_recovery = probe_calls
            await asyncio.sleep(0.1)
            assert probe_calls == probes_after_recovery
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            memory.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_session_maintenance_worker_defers_without_spinning_on_type_error_and_provider_error(
    tmp_path: Path,
):
    async def scenario():
        for exc_factory, expected_error, expected_provider_ready in (
            (lambda: TypeError("missing kwarg"), "error:TypeError", True),
            (lambda: ModelProviderUnavailable("offline"), "provider_unavailable", False),
        ):
            sub = tmp_path / ("type_err" if expected_provider_ready else "provider_err")
            sub.mkdir()
            sched_db = sqlite3.connect(sub / "sched.sqlite3")
            time_scheduler = TimeScheduler(sched_db)
            calls = 0
            called = asyncio.Event()

            async def failing_maintenance() -> int:
                nonlocal calls
                calls += 1
                called.set()
                raise exc_factory()

            pilot = Pilot(
                sub / "pilot.sqlite3",
                "bot",
                "room@chatroom",
                lambda _ctx: asyncio.sleep(0, result=""),
                lambda _draft: HookSendResult("queued"),
                session_maintenance=failing_maintenance,
            )
            pilot.time_scheduler = time_scheduler
            pilot._login_ready_monotonic = time.monotonic() - 30.0
            before = datetime.now(UTC)
            time_scheduler.schedule(
                account_id="bot",
                conversation_id=None,
                kind="session_maintenance",
                due_at=before - timedelta(seconds=1),
                payload={"idle_hours": 8},
                replace_existing=True,
            )

            maint_task = asyncio.create_task(pilot.session_maintenance_worker())
            try:
                await asyncio.wait_for(called.wait(), timeout=2)
                await asyncio.sleep(0.15)
                assert calls == 1
                row = sched_db.execute(
                    "SELECT state,due_at,last_error FROM time_jobs WHERE kind='session_maintenance'",
                ).fetchone()
                assert row is not None
                assert row["state"] == "scheduled"
                assert row["last_error"] == expected_error
                assert datetime.fromisoformat(str(row["due_at"])) > before + timedelta(seconds=20)
                assert pilot.provider_ready is expected_provider_ready
            finally:
                maint_task.cancel()
                await asyncio.gather(maint_task, return_exceptions=True)
                pilot.db.close()
                sched_db.close()

    asyncio.run(scenario())


def test_provider_unavailable_after_send_side_effect_uses_retry_path_not_defer(tmp_path: Path):
    from echoloom.mock import SystemClock
    from echoloom.outbound import OutboundService
    from echoloom.wechat_gateway import SendResult

    async def scenario():
        attempted = asyncio.Event()
        completed = asyncio.Event()
        sent = []
        calls = 0

        class FakeGateway:
            def send_text(self, draft):
                sent.append(draft)
                return SendResult("queued", "ok")

        outbound = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account="bot",
            default_room="room@chatroom",
            gateway=FakeGateway(),
            clock=SystemClock(),
        )

        async def reply(_ctx):
            nonlocal calls
            calls += 1
            if calls == 1:
                res = await outbound.send_text("首次工具发送已提交")
                assert res["status"] == "queued"
                attempted.set()
                raise ModelProviderUnavailable("late connect fail")
            # On retry after backoff, OutboundService must suppress duplicate send!
            dup = await outbound.send_text("重试绝不能重复发送")
            assert dup["status"] == "already_submitted"
            completed.set()
            return ""

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _draft: HookSendResult("queued"),
            outbound=outbound,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        assert pilot.offer(observed("已发后续失败", tick=501)) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        try:
            await asyncio.wait_for(attempted.wait(), timeout=2)
            for _ in range(50):
                row = pilot.db.execute(
                    "SELECT state,attempts,last_error FROM pilot_inbox",
                ).fetchone()
                if row and row[0] == "pending" and row[2] == "provider_unavailable_after_send":
                    break
                await asyncio.sleep(0.01)
            row = pilot.db.execute(
                "SELECT state,attempts,last_error FROM pilot_inbox",
            ).fetchone()
            assert row is not None
            assert tuple(row) == ("pending", 1, "provider_unavailable_after_send")
            assert pilot.provider_ready is True

            # Fast-forward backoff to verify retry completes cleanly with zero duplicate send.
            pilot.db.execute("UPDATE pilot_inbox SET next_attempt_at=NULL")
            pilot.db.commit()
            await asyncio.wait_for(completed.wait(), timeout=3)
            for _ in range(50):
                if len(sent) == 1 and pilot.scheduler.state_counts() == {"model_seen": 1}:
                    break
                await asyncio.sleep(0.02)
            assert pilot.scheduler.state_counts() == {"model_seen": 1}
            assert len(sent) == 1
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            outbound.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_idle_event_worker_trips_circuit_and_defers_subsequent_idle_jobs(tmp_path: Path):
    async def scenario():
        sched_db = sqlite3.connect(tmp_path / "sched.sqlite3")
        time_scheduler = TimeScheduler(sched_db)
        idle_calls = 0
        ran = asyncio.Event()

        async def idle_handler(_event_id: str, _event_number: int, _model_id: str) -> None:
            nonlocal idle_calls
            idle_calls += 1
            ran.set()
            raise ModelProviderUnavailable("idle connect error")

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            lambda _ctx: asyncio.sleep(0, result=""),
            lambda _draft: HookSendResult("queued"),
            provider_check=lambda: asyncio.sleep(0, result=False),
            provider_probe_base_seconds=60.0,
            provider_probe_max_seconds=60.0,
        )
        pilot.configure_idle_events(time_scheduler, idle_handler)
        time_scheduler.record_inbound("bot", "room@chatroom", at=datetime.now(UTC) - timedelta(hours=1))
        job1 = time_scheduler.schedule(
            account_id="bot",
            conversation_id=None,
            kind="idle_event",
            due_at=datetime.now(UTC) - timedelta(seconds=1),
            replace_existing=True,
        )

        idle_task = asyncio.create_task(pilot.idle_event_worker())
        try:
            await asyncio.wait_for(ran.wait(), timeout=2)
            for _ in range(50):
                state_row = sched_db.execute(
                    "SELECT state FROM time_jobs WHERE job_id=?", (job1,),
                ).fetchone()
                if state_row and state_row[0] == "completed":
                    break
                await asyncio.sleep(0.01)
            assert idle_calls == 1
            assert pilot.provider_ready is False
            job2 = time_scheduler.schedule(
                account_id="bot",
                conversation_id=None,
                kind="idle_event",
                due_at=datetime.now(UTC) - timedelta(seconds=1),
                replace_existing=True,
            )
            for _ in range(150):
                row2 = sched_db.execute(
                    "SELECT state,last_error FROM time_jobs WHERE job_id=?", (job2,),
                ).fetchone()
                if row2 and row2[1] == "provider_unavailable":
                    break
                await asyncio.sleep(0.02)
            row2 = sched_db.execute(
                "SELECT state,last_error FROM time_jobs WHERE job_id=?", (job2,),
            ).fetchone()
            assert row2 is not None
            assert tuple(row2) == ("scheduled", "provider_unavailable")
            assert idle_calls == 1
        finally:
            idle_task.cancel()
            await asyncio.gather(idle_task, return_exceptions=True)
            pilot.db.close()
            sched_db.close()

    asyncio.run(scenario())


def test_idle_delay_backs_off_across_consecutive_idle_events():
    import random

    rng = random.Random(42)
    assert 30 * 60 <= Pilot._next_idle_delay_seconds(0, rng=rng) <= 60 * 60
    assert 90 * 60 <= Pilot._next_idle_delay_seconds(1, rng=rng) <= 150 * 60
    assert 180 * 60 <= Pilot._next_idle_delay_seconds(2, rng=rng) <= 300 * 60
    assert 360 * 60 <= Pilot._next_idle_delay_seconds(3, rng=rng) <= 480 * 60
    assert 360 * 60 <= Pilot._next_idle_delay_seconds(10, rng=rng) <= 480 * 60


def test_worker_survives_transient_scheduler_error_and_budget_blocks_schedule_next_day(tmp_path: Path):
    async def scenario():
        replied = asyncio.Event()
        attempts = 0
        fail_claim_once = True

        async def reply(_context):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise AdkBudgetExhausted("daily budget reached")
            replied.set()
            return "预算恢复后补回"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _draft: HookSendResult("queued"),
            limit=2,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        orig_claim = pilot.scheduler.claim_ready_batch

        def flaky_claim(*args, **kwargs):
            nonlocal fail_claim_once
            if fail_claim_once:
                fail_claim_once = False
                raise sqlite3.OperationalError("database is locked")
            return orig_claim(*args, **kwargs)

        pilot.scheduler.claim_ready_batch = flaky_claim
        assert pilot.offer(observed("抗锁与预算恢复测试", tick=301)) == "queued_test"
        worker = asyncio.create_task(pilot.worker())
        pilot._worker_task = worker
        try:
            for _ in range(100):
                if pilot.scheduler.budget_blocked_count() == 1:
                    break
                await asyncio.sleep(0.05)
            assert not worker.done()
            st = pilot.status()
            assert st["worker_alive"] is True
            assert st["model_budget_blocked_batches"] == 1
            assert st["deferred_counts"] == {"model_budget_exhausted_no_call": 1}
            assert pilot.scheduler.failed_count() == 0
            row = pilot.db.execute(
                "SELECT state,attempts,next_attempt_at FROM pilot_inbox "
                "WHERE last_error='model_budget_exhausted_no_call'",
            ).fetchone()
            assert row is not None
            assert row["state"] == "pending"
            assert row["attempts"] == 0
            expected_reset = pilot.scheduler.next_budget_reset_utc(datetime.now(UTC))
            assert abs((datetime.fromisoformat(str(row["next_attempt_at"])) - expected_reset).total_seconds()) < 5

            # Simulate next Asia/Shanghai day arrival via past next_attempt_at + wake.
            past = (datetime.now(UTC) - timedelta(minutes=1)).isoformat()
            pilot.db.execute("UPDATE pilot_inbox SET next_attempt_at=?", (past,))
            pilot.db.commit()
            assert len(pilot.scheduler.wake_budget_deferred_batches(now=datetime.now(UTC))) == 1
            await asyncio.wait_for(replied.wait(), timeout=3)
            for _ in range(50):
                if pilot.scheduler.state_counts() == {"model_seen": 1}:
                    break
                await asyncio.sleep(0.02)
            assert pilot.scheduler.state_counts() == {"model_seen": 1}
            assert pilot.status()["model_budget_blocked_batches"] == 0
        finally:
            pilot._worker_task = None
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            pilot.db.close()

    asyncio.run(scenario())


def test_standalone_ingress_synthesizes_full_status_schema_from_worker_heartbeat(tmp_path: Path):
    from echoloom.ingress import Ingress, create_ingress_app

    async def scenario():
        db_path = tmp_path / "scheme-a.sqlite3"
        pilot = Pilot(
            tmp_path / "reference-pilot.sqlite3",
            "bot",
            "room@chatroom",
            lambda _ctx: asyncio.sleep(0, result=""),
            lambda _draft: HookSendResult("queued"),
            mode="selfhosted_group",
        )
        reference_keys = set(pilot.status().keys())
        pilot.db.close()

        ingress = Ingress(db_path, "bot", "room@chatroom", mode="selfhosted_group")
        app = create_ingress_app(ingress)
        try:
            # Before any worker heartbeat, worker_alive must be False and schema must match 100%.
            initial = ingress.status()
            assert set(initial.keys()) == reference_keys
            assert initial["worker_alive"] is False
            assert initial["login_watch_alive"] is False
            assert initial["trusted_gateway"] is False
            assert initial["provider_ready"] is False
            assert initial["in_flight"] is None

            now = datetime.now(UTC)
            ingress.scheduler.update_worker_heartbeat(
                worker_id="worker-a",
                pid=9999,
                wechat_ready=True,
                provider_ready=True,
                in_flight={
                    "sender_sha": "deadbeef1234",
                    "message_type": 1,
                    "started_at": (now - timedelta(seconds=3.5)).isoformat(),
                },
                scheduled_followups=1,
                now=now,
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://local",
            ) as client:
                health = await client.get("/health")
                assert health.status_code == 200
                assert health.json() == {"status": "ready", "mode": "selfhosted_group"}

                resp = await client.get("/status")
                assert resp.status_code == 200
                body = resp.json()
                assert set(body.keys()) == reference_keys
                assert body["worker_alive"] is True
                assert body["login_watch_alive"] is True
                assert body["trusted_gateway"] is True
                assert body["provider_ready"] is True
                assert body["scheduled_followups"] == 1
                assert body["in_flight"]["sender_sha"] == "deadbeef1234"
                assert body["in_flight"]["message_type"] == 1
                assert body["in_flight"]["elapsed_seconds"] >= 3.0

            # Stale heartbeat (>10s old) must immediately flip worker_alive to False.
            stale = ingress.status(now=now + timedelta(seconds=20))
            assert stale["worker_alive"] is False
            assert stale["trusted_gateway"] is False
            assert stale["provider_ready"] is False
            assert stale["in_flight"] is None
        finally:
            ingress.close()

    asyncio.run(scenario())


def test_session_maintenance_trigger_defers_30_minutes_on_noop_and_prunes(tmp_path: Path):
    async def scenario():
        sched_db = sqlite3.connect(tmp_path / "maint.sqlite3")
        time_scheduler = TimeScheduler(sched_db)
        old_ts = (datetime.now(UTC) - timedelta(days=10)).isoformat()
        sched_db.execute(
            "INSERT INTO time_jobs(job_id,account_id,conversation_id,kind,due_at,"
            "payload_json,state,created_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?)",
            ("aged-maint-job", "bot", None, "session_maintenance", old_ts, "{}", "completed", old_ts, old_ts),
        )
        sched_db.commit()

        # Simulate last inbound > 8h ago (exact trigger condition of the 100k-row hot loop bug).
        time_scheduler.record_inbound(
            "bot", "room@chatroom", at=datetime.now(UTC) - timedelta(hours=9),
        )
        calls = 0
        ran = asyncio.Event()

        async def noop_maintenance() -> int:
            nonlocal calls
            calls += 1
            ran.set()
            return 0

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            lambda _ctx: asyncio.sleep(0, result=""),
            lambda _draft: HookSendResult("queued"),
            session_maintenance=noop_maintenance,
        )
        pilot.time_scheduler = time_scheduler
        pilot._login_ready_monotonic = time.monotonic() - 30.0
        before = datetime.now(UTC)
        maint_task = asyncio.create_task(pilot.session_maintenance_worker())
        try:
            await asyncio.wait_for(ran.wait(), timeout=3)
            await asyncio.sleep(0.25)
            assert calls == 1
            # Aged completed job must have been pruned!
            aged_left = sched_db.execute(
                "SELECT count(*) FROM time_jobs WHERE job_id='aged-maint-job'",
            ).fetchone()[0]
            assert aged_left == 0
            # Next job must be scheduled ~30 minutes in the future, not immediately.
            scheduled = sched_db.execute(
                "SELECT due_at FROM time_jobs WHERE kind='session_maintenance' AND state='scheduled'",
            ).fetchall()
            assert len(scheduled) == 1
            next_due = datetime.fromisoformat(str(scheduled[0][0]))
            assert next_due >= before + timedelta(minutes=29)
            completed_count = sched_db.execute(
                "SELECT count(*) FROM time_jobs WHERE kind='session_maintenance' AND state='completed'",
            ).fetchone()[0]
            assert completed_count == 1
        finally:
            maint_task.cancel()
            await asyncio.gather(maint_task, return_exceptions=True)
            pilot.db.close()
            sched_db.close()

    asyncio.run(scenario())


def test_attempts_table_never_blocks_pilot_inbox_batch_replay_and_outbound_dedups(tmp_path: Path):
    import hashlib
    from echoloom.mock import SystemClock
    from echoloom.outbound import OutboundService
    from echoloom.wechat_gateway import SendResult

    async def scenario():
        sent = []

        class FakeGateway:
            def send_text(self, draft):
                sent.append(draft)
                return SendResult("queued", "ok")

        outbound = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account="bot",
            default_room="room@chatroom",
            gateway=FakeGateway(),
            clock=SystemClock(),
        )
        reply_calls = 0
        done = asyncio.Event()

        async def reply(_context):
            nonlocal reply_calls
            reply_calls += 1
            done.set()
            return "不该被 attempts 堵死的回复"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "bot",
            "room@chatroom",
            reply,
            lambda _draft: HookSendResult("queued"),
            outbound=outbound,
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        item = observed("被旧 attempts 污染的消息", tick=777)
        assert pilot.offer(item) == "queued_test"

        # Pre-seed stale attempts row with the exact batch receipt hash to prove attempts
        # no longer blocks pilot_inbox replay.
        receipt = pilot.scheduler.receipt_id(item)
        batch_id = hashlib.sha256(receipt.encode()).hexdigest()
        batch_receipt = hashlib.sha256(f"batch\0{batch_id}".encode()).hexdigest()
        pilot.db.execute("INSERT INTO attempts VALUES (?,?)", (batch_receipt, "agent_unknown"))
        pilot.db.commit()

        worker = asyncio.create_task(pilot.worker())
        try:
            await asyncio.wait_for(done.wait(), timeout=3)
            for _ in range(50):
                if len(sent) == 1 and pilot.scheduler.state_counts() == {"model_seen": 1}:
                    break
                await asyncio.sleep(0.02)
            assert reply_calls == 1
            assert len(sent) == 1
            assert pilot.scheduler.state_counts() == {"model_seen": 1}
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            outbound.close()
            pilot.db.close()

    asyncio.run(scenario())


def test_layer3_scheduler_modules_have_no_hardcoded_hook_endpoints():
    root = Path(__file__).resolve().parents[1] / "src" / "echoloom"
    for name in (
        "conversation_scheduler.py",
        "ingress.py",
        "dispatcher.py",
        "triggers.py",
        "pilot.py",
        "time_scheduler.py",
    ):
        source = (root / name).read_text("utf-8")
        for endpoint in (
            "/SendTextMsg",
            "/SendImgMsg",
            "/ForwardXMLMsg",
            "/Decode_Pic",
            "/QueryDB/status",
        ):
            assert endpoint not in source, (name, endpoint)
