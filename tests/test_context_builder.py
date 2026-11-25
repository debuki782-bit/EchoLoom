"""Tests for read/write separation: MessageIngestor (write) and ContextBuilder (pure read-only)."""

from __future__ import annotations

import asyncio
import hashlib
import json
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
import pytest
from PIL import Image

from wechat_agent.contacts import ContactIdentity
from wechat_agent.context import ContextTurn
from wechat_agent.context_builder import BuiltContext, ContextBuilder, ContextSpec
from wechat_agent.cross_session import CrossSessionService
from wechat_agent.hook_contract import HookObservation
from wechat_agent.hook_outbound import HookSendResult
from wechat_agent.media import WeChatImageIngestor
from wechat_agent.memory_store import MemoryStore
from wechat_agent.message_ingestor import IngestedBatch, MessageIngestor
from wechat_agent.pilot import Pilot
from wechat_agent.rich_message import normalize_message
from wechat_agent.time_scheduler import TimeScheduler


def _obs(
    text: str,
    *,
    account: str = "wxid_bot001",
    room: str = "room@chatroom",
    sender: str = "wxid_member01",
    is_group: bool = True,
    tick: int = 1000,
    source_msg_id: int | None = None,
    original_message_type: int = 1,
) -> HookObservation:
    body = f"{sender}:\n{text}" if is_group and original_message_type == 1 else text
    return HookObservation(
        account_id=account,
        conversation_id=room,
        sender_id=sender,
        sender_origin="hook_group_member_wxid" if is_group else "native_private_fromid",
        text=body,
        is_group=is_group,
        message_type=1,
        source_msg_id=source_msg_id,
        capture_tick_ms=tick,
        original_message_type=original_message_type,
        event_time_context={
            "timezone": "Asia/Shanghai",
            "local_minute": "2026-11-25 14:30",
            "time_period": "下午",
        },
    )


def _seed_store(tmp_path: Path) -> tuple[MemoryStore, TimeScheduler]:
    store = MemoryStore(tmp_path / "memory.sqlite3")
    scheduler = TimeScheduler(store.db, lock=store._lock)
    store.archive_message(
        account_id="wxid_bot001",
        conversation_id="room@chatroom",
        sender_id="wxid_owner001",
        raw_envelope={"seq": 1},
        body_text="wxid_owner001:\n早安蓝莓",
        source_message_id=5001,
    )
    store.archive_message(
        account_id="wxid_bot001",
        conversation_id="room@chatroom",
        sender_id="wxid_member01",
        raw_envelope={"seq": 2},
        body_text="wxid_member01:\n今天讨论部署方案",
        source_message_id=5002,
    )
    store.archive_message(
        account_id="wxid_bot001",
        conversation_id="other@chatroom",
        sender_id="wxid_owner001",
        raw_envelope={"seq": 3},
        body_text="wxid_owner001:\n在另一个群里冒泡",
        source_message_id=5003,
    )
    store.upsert_entity(account_id="wxid_bot001", term="蓝莓", explanation="测试代号")
    store.update_sender_tags(
        account_id="wxid_bot001",
        sender_id="wxid_owner001",
        tags=["唯一主人"],
        action="add",
    )
    store.upsert_sender_memory(
        account_id="wxid_bot001",
        sender_id="wxid_owner001",
        topic="称呼",
        content="主人的重要偏好",
    )
    store.set_human_sender_remark(
        account_id="wxid_bot001",
        sender_id="wxid_owner001",
        remark="主人备注",
    )
    store.set_human_sender_remark(
        account_id="wxid_bot001",
        sender_id="wxid_member01",
        remark="小组成员备注",
    )
    img_path = store.media_root / "quick.png"
    img_path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 8), "blue").save(img_path)
    saved_img = store.archive_agent_image(
        account_id="wxid_bot001",
        conversation_id="room@chatroom",
        artifact_path=img_path,
        mime_type="image/png",
        operation_id="quick-op",
        delivery_state="not_sent",
        library_prompt="开心猫猫图",
    )
    store.manage_image_library(
        account_id="wxid_bot001",
        image_id=str(saved_img["image_id"]),
        quick_send=True,
        display_order=1,
    )
    now = datetime.now(UTC)
    scheduler.record_idle_activity(
        account_id="wxid_bot001",
        event_id="idle_prev_1",
        event_number=1,
        model_id="gemini-3.8-flash",
        summary="整理了昨日对话摘要",
        started_at=now - timedelta(minutes=20),
        finished_at=now - timedelta(minutes=19),
    )
    return store, scheduler


def test_context_builder_is_strictly_read_only_across_all_four_presets(tmp_path: Path):
    """Enable SQLite PRAGMA query_only=ON and verify all 4 ContextSpec presets execute 0 writes."""
    store, scheduler = _seed_store(tmp_path)

    class DailyEnv:
        def system_prompt_context(self) -> str:
            return "深圳今日晴朗 24C"

    def resolve(uid: str) -> ContactIdentity | None:
        return {
            "wxid_owner001": ContactIdentity("昵称A", user_name="wxid_owner001", nickname="昵称A"),
            "wxid_member01": ContactIdentity("昵称B", user_name="wxid_member01", nickname="昵称B"),
            "room@chatroom": ContactIdentity("主测试群", user_name="room@chatroom", nickname="主测试群"),
            "other@chatroom": ContactIdentity("隔壁群", user_name="other@chatroom", nickname="隔壁群"),
        }.get(uid)

    builder = ContextBuilder(
        store,
        resolve_contact=resolve,
        daily_environment=DailyEnv(),
        time_scheduler=scheduler,
    )

    # Lock SQLite strictly read-only: ANY INSERT/UPDATE/DELETE raises sqlite3.OperationalError!
    store.db.execute("PRAGMA query_only = ON")
    assert store.db.execute("PRAGMA query_only").fetchone()[0] == 1

    async def scenario():
        # 1. for_chat preset (32KB, 50/20)
        chat_spec = ContextSpec.for_chat(observations=(_obs("你好呀", tick=2000),))
        assert chat_spec.mode == "chat"
        assert chat_spec.max_user_bytes == 32_000
        assert chat_spec.same_chat_limit == 50
        assert chat_spec.other_chat_limit == 20
        chat_built = await builder.build(chat_spec)
        assert isinstance(chat_built, BuiltContext)
        assert len(chat_built.context.turns) == 1
        assert len(chat_built.context.history_turns) >= 2
        assert len(chat_built.context.recent_other_turns) == 1
        assert chat_built.context.daily_prompt_context == "深圳今日晴朗 24C"
        assert chat_built.context.quick_send_images
        assert chat_built.context.priority_sender_references
        assert chat_built.context.identities["wxid_member01"].remark == "小组成员备注"
        assert chat_built.quote_context is not None
        assert chat_built.quote_context[0] == "wxid_member01"
        assert "小组成员备注" in chat_built.quote_context[1]

        # 2. for_followup preset (32KB, 50/20 + scheduled_task), both empty and merged observations
        fu_empty_spec = ContextSpec.for_followup(
            account_id="wxid_bot001",
            conversation_id="room@chatroom",
            scheduled_task="跟进部署进展",
        )
        assert fu_empty_spec.mode == "followup"
        assert fu_empty_spec.max_user_bytes == 32_000
        assert fu_empty_spec.same_chat_limit == 50
        assert fu_empty_spec.other_chat_limit == 20
        fu_empty = await builder.build(fu_empty_spec)
        assert fu_empty.context.turns == ()
        assert fu_empty.context.scheduled_task == "跟进部署进展"
        assert "跟进部署进展" in fu_empty.context.model_user_content()

        fu_merged = await builder.build(
            ContextSpec.for_followup(
                observations=(_obs("刚发了新进展", tick=2001),),
                scheduled_task="跟进部署进展",
            )
        )
        assert len(fu_merged.context.turns) == 1
        assert "刚发了新进展" in fu_merged.context.model_user_content()
        assert "跟进部署进展" in fu_merged.context.model_user_content()

        # 3. for_cross_session preset (24KB, 50/0 + query_hint + merged pending batch)
        task_turn = ContextTurn(
            sender_id="Agent内部协调器",
            sender_origin="cross_session_task",
            text="请核对部署方案",
            message_id="xst_1",
            source_conversation_id="room@chatroom",
            source_is_group=True,
        )
        xs_spec = ContextSpec.for_cross_session(
            account_id="wxid_bot001",
            conversation_id="room@chatroom",
            synthetic_turns=(task_turn,),
            observations=(_obs("目标群刚到的待处理消息", tick=2002),),
            query_hint="部署方案",
            extra_identifiers=("other@chatroom",),
        )
        assert xs_spec.mode == "cross_session"
        assert xs_spec.max_user_bytes == 24_000
        assert xs_spec.same_chat_limit == 50
        assert xs_spec.other_chat_limit == 0
        xs_built = await builder.build(xs_spec)
        assert len(xs_built.context.turns) == 2
        assert "目标群刚到的待处理消息" in xs_built.context.turns[0].text
        assert xs_built.context.turns[1] == task_turn
        assert xs_built.context.recent_other_turns == ()
        assert any("今天讨论部署方案" in turn.text for turn in xs_built.context.history_turns)
        assert "other@chatroom" in xs_built.context.identities

        # 4. for_idle preset (80KB, 0/0, global_timeline_limit=100, idle_summaries_limit=20)
        idle_spec = ContextSpec.for_idle(account_id="wxid_bot001", event_id="ev_01")
        assert idle_spec.mode == "idle"
        assert idle_spec.max_user_bytes == 80_000
        assert idle_spec.same_chat_limit == 0
        assert idle_spec.other_chat_limit == 0
        assert idle_spec.global_timeline_limit == 100
        assert idle_spec.idle_summaries_limit == 20
        idle_built = await builder.build(idle_spec)
        assert idle_built.context.is_idle_event is True
        assert len(idle_built.context.global_timeline_turns) >= 3
        assert idle_built.context.history_turns == ()
        assert idle_built.context.recent_other_turns == ()
        assert any("整理了昨日对话摘要" in s for s in idle_built.context.idle_activity_summaries)
        # Bug fixes over legacy run_idle_event: remarks & priority_sender_profiles present!
        assert idle_built.context.identities["wxid_owner001"].remark == "主人备注"
        assert idle_built.context.identities["wxid_member01"].remark == "小组成员备注"
        assert len(idle_built.context.priority_sender_references) == 1
        ref_text = idle_built.context.model_reference_context()
        assert "主人的重要偏好" in ref_text
        assert "主人备注" in ref_text
        assert "小组成员备注" in ref_text

    try:
        asyncio.run(scenario())
    finally:
        store.db.execute("PRAGMA query_only = OFF")
        store.close()


def test_idle_context_builder_resolves_contacts_in_parallel(tmp_path: Path):
    """Prove ContextBuilder resolves contacts concurrently via asyncio.gather, not serially."""
    store, scheduler = _seed_store(tmp_path)
    barrier = threading.Barrier(4, timeout=3.0)
    resolved_ids: list[str] = []

    def parallel_resolve(uid: str) -> ContactIdentity:
        # Four distinct IDs (owner, member, room, other) must arrive concurrently.
        # Serial resolution would deadlock and raise BrokenBarrierError.
        if uid in {"wxid_owner001", "wxid_member01", "room@chatroom", "other@chatroom"}:
            barrier.wait()
        resolved_ids.append(uid)
        return ContactIdentity(uid, user_name=uid, nickname=uid)

    builder = ContextBuilder(
        store,
        resolve_contact=parallel_resolve,
        time_scheduler=scheduler,
    )
    try:
        started = time.monotonic()
        built = asyncio.run(
            builder.build(ContextSpec.for_idle(account_id="wxid_bot001", event_id="par_1"))
        )
        elapsed = time.monotonic() - started
        assert elapsed < 3.0
        assert {"wxid_owner001", "wxid_member01", "room@chatroom", "other@chatroom"}.issubset(
            set(resolved_ids)
        )
        assert built.context.identities["wxid_owner001"].remark == "主人备注"
    finally:
        store.close()


def test_context_builder_exact_quote_lookup_works_in_multi_message_batch(tmp_path: Path):
    """Verify exact quote_target attachment works at index 0 in a multi-message batch."""
    store, _ = _seed_store(tmp_path)
    quoted_raw = HookObservation(
        "wxid_bot001",
        "room@chatroom",
        "wxid_member01",
        "hook_group_member_wxid",
        "wxid_member01:\n<msg><appmsg><type>57</type><title>回复第一条</title><refermsg>"
        "<type>1</type><svrid>5001</svrid><fromusr>room@chatroom</fromusr>"
        "<chatusr>wxid_owner001</chatusr><content>早安蓝莓</content>"
        "</refermsg></appmsg></msg>",
        True,
        49,
        5010,
        3000,
    )
    quoted_event = normalize_message(quoted_raw)
    assert quoted_event is not None
    second_event = _obs("紧接着补一句", tick=3001, source_msg_id=5011)

    ingestor = MessageIngestor(store)
    builder = ContextBuilder(store)

    async def scenario():
        ingested = await ingestor.ingest((quoted_event, second_event))
        store.db.execute("PRAGMA query_only = ON")
        built = await builder.build(ContextSpec.for_chat(ingested=ingested))
        assert len(built.context.turns) == 2
        assert built.context.turns[0].quote_target is not None
        assert built.context.turns[0].quote_target.get("platform_message_id") == "5001"
        assert built.context.turns[1].quote_target is None
        assert "平台消息 ID 精确匹配 5001" in built.context.model_user_content()

    try:
        asyncio.run(scenario())
    finally:
        store.db.execute("PRAGMA query_only = OFF")
        store.close()


def test_message_ingestor_enforces_allow_account_scan_false_and_redacts_voice_hex(tmp_path: Path):
    """MessageIngestor must pass allow_account_scan=False and strip voice_hex before archiving."""
    store = MemoryStore(tmp_path / "memory.sqlite3")
    scan_flags: list[bool] = []

    class SpyImageIngestor:
        def ingest(self, *, item, message_id, references, store, allow_account_scan=True):
            scan_flags.append(allow_account_scan)
            return {"state": "ready", "image_id": "img_spy"}

    class SpyVoiceIngestor:
        def ingest(self, *, item, message_id, store):
            return {"state": "ready", "audio_id": "aud_spy"}

    raw_img = HookObservation(
        "wxid_bot001",
        "room@chatroom",
        "wxid_member01",
        "hook_group_member_wxid",
        '<msg><img md5="' + "a" * 32 + '"/></msg>',
        True,
        3,
        6001,
        100,
    )
    img_event = normalize_message(raw_img)
    assert img_event is not None

    raw_voice_rec = {
        "fromid": "room@chatroom",
        "toid": "wxid_bot001",
        "room": "room@chatroom",
        "sender": "wxid_member01",
        "msg": "<msg><voicemsg/></msg>",
        "msgtype": 34,
        "msgsvrid": 6002,
        "time": 101,
        "voice_hex": "deadbeef-secret",
    }
    raw_voice = HookObservation(
        "wxid_bot001",
        "room@chatroom",
        "wxid_member01",
        "hook_group_member_wxid",
        "<msg><voicemsg/></msg>",
        True,
        34,
        6002,
        101,
        raw_envelope_json=json.dumps({"msglist": [raw_voice_rec]}),
        raw_message_json=json.dumps(raw_voice_rec),
        original_message_type=34,
    )
    voice_event = normalize_message(raw_voice)
    assert voice_event is not None

    ingestor = MessageIngestor(
        store,
        image_ingestor=SpyImageIngestor(),  # type: ignore[arg-type]
        voice_ingestor=SpyVoiceIngestor(),  # type: ignore[arg-type]
    )
    try:
        batch = asyncio.run(ingestor.ingest((img_event, voice_event)))
        assert isinstance(batch, IngestedBatch)
        assert scan_flags == [False]
        assert len(batch.message_ids) == 2
        assert len(batch.records) == 2
        assert len(batch.message_times) == 2
        assert batch.records[0]["image"] == {"state": "ready", "image_id": "img_spy"}
        assert batch.records[1]["audio"] == {"state": "ready", "audio_id": "aud_spy"}
        raw_rows = [
            row[0] for row in store.db.execute("SELECT raw_json FROM source_messages").fetchall()
        ]
        assert all("deadbeef-secret" not in raw for raw in raw_rows)
        with pytest.raises(AttributeError):
            batch.message_ids = ()  # type: ignore[misc]
    finally:
        store.close()


def test_message_ingestor_real_image_ingestor_performs_zero_cross_chat_scan(tmp_path: Path):
    """End-to-end verification with real WeChatImageIngestor: 0 cross-chat decodes on inbound."""
    source_root = tmp_path / "xwechat_files"
    other_dir = source_root / "wxid_bot001_x" / "msg" / "attach" / "other-chat" / "2026-11" / "Img"
    other_dir.mkdir(parents=True)
    (other_dir / ("f" * 32 + "_h.dat")).write_bytes(b"encrypted-cross-chat")

    decode_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        decode_calls.append(request.url.path)
        return httpx.Response(200, json={"ret": -1})

    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    real_ingestor = WeChatImageIngestor(
        tmp_path,
        source_root=source_root,
        transport=httpx.MockTransport(handler),
    )
    md5 = hashlib.md5(b"missing-local").hexdigest()
    img_event = normalize_message(
        HookObservation(
            "wxid_bot001",
            "room@chatroom",
            "wxid_member01",
            "hook_group_member_wxid",
            f'<msg><img md5="{md5}"/></msg>',
            True,
            3,
            7001,
            200,
        )
    )
    assert img_event is not None

    try:
        batch = asyncio.run(
            MessageIngestor(store, image_ingestor=real_ingestor).ingest((img_event,))
        )
        assert decode_calls == []
        assert batch.records[0]["image"] == {
            "state": "failed",
            "error_code": "original_image_not_found_in_cache",
        }
    finally:
        store.close()


def test_cross_session_dispatch_to_pilot_builds_context_window_only_once(tmp_path: Path):
    """Eliminate double ContextWindow stitching: CrossSession -> Pilot.dispatch_to_session builds once."""
    store, _ = _seed_store(tmp_path)
    build_calls: list[ContextSpec] = []

    class CountingContextBuilder(ContextBuilder):
        async def build(self, spec: ContextSpec) -> BuiltContext:
            build_calls.append(spec)
            return await super().build(spec)

    builder = CountingContextBuilder(
        store,
        resolve_contact=lambda uid: ContactIdentity(uid, user_name=uid, nickname=uid),
    )
    received_windows = []

    async def reply(window):
        received_windows.append(window)
        return "单次组装完成"

    pilot = Pilot(
        tmp_path / "pilot.sqlite3",
        "wxid_bot001",
        "room@chatroom",
        reply,
        lambda _draft: HookSendResult("queued"),
        memory_store=store,
        context_builder=builder,
    )
    pending_item = _obs("目标群里刚到的未读消息", room="other@chatroom", tick=8001, source_msg_id=8001)
    assert pilot.scheduler.enqueue(pending_item)[0] == "queued"

    service = CrossSessionService(
        store,
        dispatch_reply=pilot.dispatch_to_session,
        context_builder=builder,
    )
    try:
        result = asyncio.run(
            service.dispatch(
                account_id="wxid_bot001",
                source_conversation_id="room@chatroom",
                target_conversation_id="other@chatroom",
                task="顺便看下隔壁群消息",
            )
        )
        assert result["status"] == "completed"
        assert result["result"] == "单次组装完成"
        # Exactly ONE ContextBuilder.build() call across the entire cross-session dispatch!
        assert len(build_calls) == 1
        assert build_calls[0].mode == "cross_session"
        assert len(build_calls[0].observations) == 1
        assert len(build_calls[0].synthetic_turns) == 1
        assert len(received_windows) == 1
        window = received_windows[0]
        assert len(window.turns) == 2
        assert "目标群里刚到的未读消息" in window.turns[0].text
        assert window.turns[1].sender_origin == "cross_session_task"
        assert pilot.scheduler.state_counts() == {"model_seen": 1}
    finally:
        pilot.db.close()
        store.close()


def test_layer4_context_modules_have_no_hardcoded_hook_endpoints():
    root = Path(__file__).resolve().parents[1] / "src" / "wechat_agent"
    for name in (
        "message_ingestor.py",
        "context_builder.py",
        "context_assembler.py",
        "cross_session.py",
        "dispatcher.py",
        "pilot.py",
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
