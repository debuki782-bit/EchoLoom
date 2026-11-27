"""Delayed originals, native WXGF decoding, and both model reading paths."""
import asyncio
import hashlib
import io
import json
from pathlib import Path

import av
import httpx
import pytest
from PIL import Image

from echoloom.hook_contract import HookObservation
from echoloom.media import WeChatImageIngestor, image_for_model
from echoloom.memory_store import MemoryStore
from echoloom.mock import SystemClock
from echoloom.wechat_tools import WeChatTools, memory_context
from echoloom.wxgf import wxgf_image


@pytest.fixture(scope="module")
def wxgf():
    # Encode a synthetic image; private WeChat images never become fixtures.
    codec = av.CodecContext.create("libx265", "w")
    codec.width, codec.height, codec.pix_fmt = 48, 32, "yuv420p"
    codec.options = {"x265-params": "log-level=error:pools=none:frame-threads=1"}
    frame = av.VideoFrame.from_image(Image.new("RGB", (48, 32), "red"))
    hevc = b"".join(bytes(p) for p in [*codec.encode(frame), *codec.encode(None)])
    return b"wxgf\x08\x00\x00\x00" + len(hevc).to_bytes(4, "big") + hevc


def test_wxgf_preserves_native_resolution_and_source(tmp_path, wxgf):
    original = tmp_path / "original.wxgf"
    original.write_bytes(wxgf)
    payload, mime = image_for_model(original, "application/x-wechat-wxgf")
    assert mime == "image/png"
    with Image.open(io.BytesIO(payload)) as image:
        assert image.size == (48, 32)
        assert image.getpixel((24, 16))[0] > 200
    assert original.read_bytes() == wxgf
    with pytest.raises(ValueError, match="dimensions_exceed"):
        wxgf_image(wxgf, max_pixels=100)


def test_wxgf_rejects_missing_payload_and_ambiguous_tracks(wxgf):
    with pytest.raises(ValueError, match="payload_missing"):
        wxgf_image(b"wxgf\x08" + b"\x00" * 20)
    with pytest.raises(ValueError, match="multitrack"):
        wxgf_image(wxgf + wxgf[8:])
    # Three-byte Annex B prefix must also be recognized.
    three_byte = wxgf[12:][1:]
    result, _ = wxgf_image(wxgf[:8] + len(three_byte).to_bytes(4, "big") + three_byte)
    assert Image.open(io.BytesIO(result)).size == (48, 32)


def _setup(tmp_path, wxgf, *, state="failed", declared_length=None):
    source_root = tmp_path / "xwechat_files"
    image_dir = source_root / "bot_abc" / "msg" / "attach" / "other-chat" / "2026-11" / "Img"
    image_dir.mkdir(parents=True)
    # Filename differs from message MD5, as in actual WeChat caches.
    original = image_dir / ("a" * 32 + "_h.dat")
    refs = [{"field": "md5", "value": hashlib.md5(wxgf).hexdigest()}]
    if declared_length is not None:
        refs.append({"field": "length", "value": str(declared_length)})
    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    message = store.archive_message(account_id="bot", conversation_id="room@chatroom",
                                    sender_id="member", raw_envelope={"msg": "image"},
                                    body_text="image")
    row = store.begin_image_ingest(account_id="bot", conversation_id="room@chatroom",
                                   message_id=message.message_id, original_references=refs)
    calls = []

    def decode(request):
        body = json.loads(request.content)
        calls.append(body["src_path"])
        Path(body["dst_path"]).write_bytes(wxgf)
        return httpx.Response(200, json={"ret": 0})

    ingestor = WeChatImageIngestor(tmp_path, source_root=source_root,
                                   transport=httpx.MockTransport(decode))
    if state == "failed":
        store.fail_image_ingest(image_id=row["image_id"], account_id="bot",
                                 error_code="image_ingest_failed")
    else:
        ingestor.artifact_root.mkdir(parents=True, exist_ok=True)
        thumbnail = ingestor.artifact_root / "old-thumb.png"
        output = io.BytesIO()
        Image.new("RGB", (4, 4)).save(output, format="PNG")
        thumbnail.write_bytes(output.getvalue())
        sha = hashlib.sha256(output.getvalue()).hexdigest()
        store.complete_image_ingest(image_id=row["image_id"], account_id="bot",
                                    artifact_path=str(thumbnail.resolve()), mime_type="image/png",
                                    byte_count=len(output.getvalue()), sha256=sha,
                                    source_sha256=sha, source_kind="thumbnail_inferred")
        store.save_image_description(account_id="bot", image_id=row["image_id"],
                                      description="stale thumbnail note", model_id="fake")
    original.write_bytes(b"encrypted-dat")
    return store, message, row["image_id"], ingestor, original, calls


@pytest.mark.parametrize("declared_length", [None, 999999])
def test_cache_identity_uses_decoded_md5_not_filename_or_declared_size(
    tmp_path, wxgf, declared_length,
):
    store, _message, image_id, ingestor, original, calls = _setup(
        tmp_path, wxgf, declared_length=declared_length,
    )
    try:
        image = store.get_image_by_id(account_id="bot", conversation_id=None,
                                      image_id=image_id, include_unready=True)
        ready = ingestor.prepare_for_read(image, store=store)
        assert ready["state"] == "ready"
        assert Path(ready["artifact_path"]).read_bytes() == wxgf
        assert calls == [str(original.resolve())]
        assert store.get_image_by_id(account_id="other", conversation_id=None,
                                     image_id=image_id, include_unready=True) is None
        # The stored input has not been replayed or changed.
        assert store.db.execute("SELECT count(*) FROM source_messages").fetchone()[0] == 1
    finally:
        store.close()


def test_wrong_md5_is_never_promoted_and_repeated_probes_are_memoized(tmp_path, wxgf):
    store, _, _, ingestor, _, calls = _setup(tmp_path, wxgf)
    item = HookObservation("bot", "room@chatroom", "member", "hook_group_member_wxid",
                           "", True, 3, None, 0)
    refs = [{"field": "md5", "value": "b" * 32}]
    try:
        for _ in range(2):
            assert ingestor._fetch_account_cached_original(item, refs, tmp_path / "out") is None
        assert len(calls) == 1
        assert not (tmp_path / "out").exists()
    finally:
        store.close()


@pytest.mark.parametrize("state", ["failed", "thumbnail"])
@pytest.mark.parametrize("followup", [False, True])
def test_main_read_recovers_current_image_and_delayed_original(tmp_path, wxgf, state, followup):
    store, message, image_id, ingestor, _, _ = _setup(tmp_path, wxgf, state=state)
    tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                        SystemClock(), memory_store=store)
    tools.image_ingestor = ingestor
    if followup:
        message = store.archive_message(account_id="bot", conversation_id="room@chatroom",
                                          sender_id="member", raw_envelope={"msg": "看刚才的图"},
                                          body_text="看刚才的图")
    token = memory_context.set({"account_id": "bot", "message_id": message.message_id})
    try:
        result = asyncio.run(tools.inspect_latest_wechat_image("看原图"))
        assert result["image_attached_to_main_agent"] is True
        assert result["selection"] == (
            "latest_saved_image_in_current_group" if followup else "current_batch_image"
        )
        assert "warning" not in result
        assert Image.open(io.BytesIO(result["image_part"].inline_data.data)).size == (48, 32)
        image = store.get_image_by_id(account_id="bot", conversation_id=None, image_id=image_id)
        assert image["source_kind"] == "original"
        assert not image["vision_summary"]  # A thumbnail description must not mask the original.
    finally:
        memory_context.reset(token)
        tools.db.close()
        store.close()


def test_reference_lookup_can_recover_failed_image(tmp_path, wxgf):
    store, _, image_id, ingestor, _, _ = _setup(tmp_path, wxgf)
    tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                        SystemClock(), memory_store=store)
    tools.image_ingestor = ingestor
    token = memory_context.set({"account_id": "bot"})
    try:
        result = asyncio.run(tools.inspect_wechat_image(hashlib.md5(wxgf).hexdigest(), "看图"))
        assert result["image_id"] == image_id
        assert result["image_attached_to_main_agent"] is True
    finally:
        memory_context.reset(token)
        tools.db.close()
        store.close()


def test_image_archive_accepts_explicit_library_description(tmp_path):
    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    path = store.media_root / "generated.png"
    path.parent.mkdir(parents=True, exist_ok=True)
    Image.new("RGB", (8, 8), "red").save(path)
    try:
        saved = store.archive_agent_image(account_id="bot", conversation_id="room@chatroom",
                                          artifact_path=path, mime_type="image/png",
                                          operation_id="generated", delivery_state="not_sent",
                                          note="发送备注", library_prompt="图库描述")
        images = store.list_images(account_id="bot")
        assert images[0]["image_id"] == saved["image_id"]
        assert images[0]["description"] == "图库描述"
    finally:
        store.close()


def test_text_description_uses_same_upgrade_path_before_vision_call(tmp_path, wxgf, monkeypatch):
    store, message, image_id, ingestor, _, _ = _setup(tmp_path, wxgf, state="thumbnail")
    tools = WeChatTools(tmp_path, "bot", "room@chatroom", None, None, None,
                        SystemClock(), memory_store=store)
    tools.image_ingestor = ingestor
    token = memory_context.set({"account_id": "bot", "message_id": message.message_id})

    def stop_before_paid_call(path, mime):
        assert mime == "application/x-wechat-wxgf"
        assert Path(path).read_bytes() == wxgf
        raise ValueError("original_reached_vision_input")

    monkeypatch.setattr("echoloom.toolsets.media.image_for_model", stop_before_paid_call)
    try:
        result = asyncio.run(tools.read_wechat_image())
        assert result["error"] == "original_reached_vision_input"
        assert result["image_id"] == image_id
    finally:
        memory_context.reset(token)
        tools.db.close()
        store.close()


def test_read_wechat_image_uses_gemini_4_hydrogen_and_settles_usage(tmp_path, wxgf):
    from types import SimpleNamespace

    from echoloom.runtime_scope import model_usage_scope
    from echoloom.storage import Repository
    from echoloom.usage import UsageLedger

    store, message, image_id, ingestor, _, _ = _setup(tmp_path, wxgf, state="original")
    repo = Repository(tmp_path / "usage.sqlite3")
    clock = SystemClock()
    usage = UsageLedger(repo, clock)
    seen = {}

    async def count_tokens(*, model, contents):
        seen["count_model"] = model
        return SimpleNamespace(total_tokens=1000)

    async def generate_content(*, model, contents, config):
        seen["gen_model"] = model
        seen["thinking_level"] = getattr(getattr(config, "thinking_config", None), "thinking_level", None)
        return SimpleNamespace(
            text="画面里是一张红色测试图。",
            usage_metadata=SimpleNamespace(
                prompt_token_count=1000,
                total_token_count=1100,
                cached_content_token_count=0,
            ),
        )

    fake_client = SimpleNamespace(
        aio=SimpleNamespace(
            models=SimpleNamespace(
                count_tokens=count_tokens,
                generate_content=generate_content,
            )
        )
    )
    tools = WeChatTools(
        tmp_path, "bot", "room@chatroom", None, fake_client, usage, clock, memory_store=store
    )
    tools.image_ingestor = ingestor
    token = memory_context.set({"account_id": "bot", "message_id": message.message_id})
    try:
        with model_usage_scope("bot", "room@chatroom"):
            result = asyncio.run(tools.read_wechat_image())
        assert result["image_id"] == image_id
        assert result["model_id"] == "gemini-4-hydrogen"
        assert result["description"] == "画面里是一张红色测试图。"
        assert seen["count_model"] == "gemini-4-hydrogen"
        assert seen["gen_model"] == "gemini-4-hydrogen"
        assert seen["thinking_level"] == "LOW"
        row = repo.conn.execute(
            "SELECT model_id, state, actual_microusd FROM usage_reservations"
        ).fetchone()
        assert tuple(row) == ("gemini-4-hydrogen", "settled", 2250)
    finally:
        memory_context.reset(token)
        tools.db.close()
        repo.close()
        store.close()


def test_context_assembler_upgrades_recent_thumbnail_before_display(tmp_path, wxgf):
    from echoloom.context_assembler import MessageContextAssembler

    store, _, image_id, ingestor, _, _ = _setup(tmp_path, wxgf, state="thumbnail")
    assembler = MessageContextAssembler(
        store,
        image_ingestor=ingestor,
        archive_envelope=lambda item: {"msg": item.text},
        upgrade_recent_media=True,
    )
    followup = HookObservation(
        "bot",
        "room@chatroom",
        "member",
        "hook_group_member_wxid",
        "member:\n这张图写的是什么",
        True,
        1,
        None,
        1234,
        original_message_type=1,
    )
    try:
        batch = asyncio.run(assembler.assemble((followup,)))
        window = batch.context
        image = store.get_image_by_id(account_id="bot", conversation_id=None, image_id=image_id)
        assert image["source_kind"] == "original"
        assert not image["vision_summary"]
        rendered_history = "\n".join(turn.text for turn in window.history_turns)
        assert "低清缩略图" not in window.model_user_content()
        assert "低清缩略图" not in rendered_history
    finally:
        store.close()


def test_context_assembler_default_skips_synchronous_thumbnail_upgrade(tmp_path, wxgf):
    from echoloom.context_assembler import MessageContextAssembler

    store, _, image_id, ingestor, _, calls = _setup(tmp_path, wxgf, state="thumbnail")
    assembler = MessageContextAssembler(
        store,
        image_ingestor=ingestor,
        archive_envelope=lambda item: {"msg": item.text},
    )
    followup = HookObservation(
        "bot",
        "room@chatroom",
        "member",
        "hook_group_member_wxid",
        "member:\n这张图写的是什么",
        True,
        1,
        None,
        1235,
        original_message_type=1,
    )
    try:
        asyncio.run(assembler.assemble((followup,)))
        assert calls == []
        image = store.get_image_by_id(account_id="bot", conversation_id=None, image_id=image_id)
        assert image["source_kind"] == "thumbnail_inferred"
    finally:
        store.close()


def test_account_cached_original_persists_md5_cache_across_ingestor_restarts(tmp_path, wxgf):
    store, _, _, ingestor, _, calls = _setup(tmp_path, wxgf)
    item = HookObservation("bot", "room@chatroom", "member", "hook_group_member_wxid",
                           "", True, 3, None, 0)
    refs = [{"field": "md5", "value": "c" * 32}]
    try:
        assert ingestor._fetch_account_cached_original(item, refs, tmp_path / "out1") is None
        assert len(calls) == 1
        restarted = WeChatImageIngestor(
            tmp_path,
            source_root=ingestor.source_root,
            transport=ingestor.gateway.image_source.transport,
        )
        assert restarted._fetch_account_cached_original(item, refs, tmp_path / "out2") is None
        assert len(calls) == 1
    finally:
        store.close()


def test_fast_ingest_triggers_zero_cross_chat_decodes_and_prepare_for_read_recovers(
    tmp_path, wxgf,
):
    source_root = tmp_path / "xwechat_files"
    other_dir = source_root / "bot_abc" / "msg" / "attach" / "other-chat" / "2026-11" / "Img"
    other_dir.mkdir(parents=True)
    original = other_dir / ("a" * 32 + "_h.dat")
    original.write_bytes(b"encrypted-original")
    decoy = other_dir / ("b" * 32 + ".dat")
    decoy.write_bytes(b"encrypted-decoy")

    calls: list[str] = []

    def decode(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        calls.append(body["src_path"])
        payload = wxgf if Path(body["src_path"]) == original.resolve() else b"unrelated"
        Path(body["dst_path"]).write_bytes(payload)
        return httpx.Response(200, json={"ret": 0})

    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    ingestor = WeChatImageIngestor(
        tmp_path, source_root=source_root, transport=httpx.MockTransport(decode),
    )
    refs = [{"field": "md5", "value": hashlib.md5(wxgf).hexdigest()}]
    item = HookObservation(
        "bot", "room@chatroom", "member", "hook_group_member_wxid", "", True, 3, None, 0,
    )
    try:
        message = store.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msg": "image"}, body_text="image",
        )
        fast = ingestor.ingest(
            item=item,
            message_id=message.message_id,
            references=refs,
            store=store,
            allow_account_scan=False,
        )
        assert fast == {"state": "failed", "error_code": "original_image_not_found_in_cache"}
        assert calls == []

        row = store.get_image_for_message(
            account_id="bot", conversation_id="room@chatroom", message_id=message.message_id,
        )
        assert row is not None and row["state"] == "failed"
        failed = store.get_image_by_id(
            account_id="bot",
            conversation_id="room@chatroom",
            image_id=str(row["image_id"]),
            include_unready=True,
        )
        assert failed is not None and failed["state"] == "failed"

        ready = ingestor.prepare_for_read(failed, store=store)
        assert ready["state"] == "ready"
        assert ready["source_kind"] == "original"
        assert Path(ready["artifact_path"]).read_bytes() == wxgf
        assert str(original.resolve()) in calls
    finally:
        store.close()


def test_fast_ingest_uses_unique_thumbnail_without_account_scan_then_upgrades_on_read(
    tmp_path, wxgf,
):
    import os
    from datetime import datetime, timedelta, timezone

    source_root = tmp_path / "xwechat_files"
    other_dir = source_root / "bot_abc" / "msg" / "attach" / "other-chat" / "2026-11" / "Img"
    other_dir.mkdir(parents=True)
    original = other_dir / ("a" * 32 + "_h.dat")
    original.write_bytes(b"encrypted-original")

    room_hash = hashlib.md5(b"room@chatroom", usedforsecurity=False).hexdigest()
    thumb_dir = source_root / "bot_abc" / "msg" / "attach" / room_hash / "2026-11" / "Img"
    thumb_dir.mkdir(parents=True)
    thumb_buf = io.BytesIO()
    Image.new("RGB", (24, 16), "blue").save(thumb_buf, format="JPEG")
    thumb_bytes = thumb_buf.getvalue()
    thumb_dat = thumb_dir / "thumb_t.dat"
    thumb_dat.write_bytes(b"x" * (len(thumb_bytes) + 32))
    minute = datetime(2026, 11, 25, 14, 0, tzinfo=timezone(timedelta(hours=8)))
    os.utime(thumb_dat, (minute.timestamp() + 10, minute.timestamp() + 10))

    calls: list[str] = []

    def decode(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        src = Path(body["src_path"])
        calls.append(str(src))
        payload = thumb_bytes if src == thumb_dat.resolve() else wxgf
        Path(body["dst_path"]).write_bytes(payload)
        return httpx.Response(200, json={"ret": 0})

    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    ingestor = WeChatImageIngestor(
        tmp_path, source_root=source_root, transport=httpx.MockTransport(decode),
    )
    refs = [
        {"field": "md5", "value": hashlib.md5(wxgf).hexdigest()},
        {"field": "cdnthumblength", "value": str(len(thumb_bytes))},
        {"field": "cdnthumbwidth", "value": "48"},
        {"field": "cdnthumbheight", "value": "32"},
    ]
    item = HookObservation(
        "bot", "room@chatroom", "member", "hook_group_member_wxid", "", True, 3, None, 0,
        event_time_context={"local_minute": "2026-11-25 14:00", "timezone": "Asia/Shanghai"},
    )
    try:
        message = store.archive_message(
            account_id="bot", conversation_id="room@chatroom", sender_id="member",
            raw_envelope={"msg": "image"}, body_text="image",
        )
        fast = ingestor.ingest(
            item=item,
            message_id=message.message_id,
            references=refs,
            store=store,
            allow_account_scan=False,
        )
        assert fast["state"] == "ready"
        assert fast["source_kind"] == "thumbnail_inferred"
        # Cross-chat original was never probed during fast ingest; only same-chat thumbnail ran.
        assert all("other-chat" not in path for path in calls)

        row = store.get_image_for_message(
            account_id="bot", conversation_id="room@chatroom", message_id=message.message_id,
        )
        assert row is not None and row["source_kind"] == "thumbnail_inferred"
        saved = store.get_image_by_id(
            account_id="bot",
            conversation_id="room@chatroom",
            image_id=str(row["image_id"]),
        )
        assert saved is not None and saved["source_kind"] == "thumbnail_inferred"

        upgraded = ingestor.prepare_for_read(saved, store=store)
        assert upgraded["state"] == "ready"
        assert upgraded["source_kind"] == "original"
        assert Path(upgraded["artifact_path"]).read_bytes() == wxgf
        assert str(original.resolve()) in calls
    finally:
        store.close()

