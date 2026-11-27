import hashlib
import io
import json
import os
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from PIL import Image

from echoloom.context import build_hook_context
from echoloom.hook_contract import HookObservation
from echoloom.media import WeChatImageIngestor, wechat_storage_root_from_status
from echoloom.memory_store import MemoryStore
from echoloom.rich_message import normalize_message, normalized_image_reference_records

PNG = b"\x89PNG\r\n\x1a\n" + b"original-png-payload"
URL = "https://wx.qpic.cn/original.png?token=opaque"


def test_storage_root_comes_from_matching_logged_in_hook_profile(tmp_path: Path):
    root = tmp_path / "custom-wechat-files"
    contact = root / "bot_123" / "db_storage" / "contact" / "contact.db"
    contact.parent.mkdir(parents=True)
    status = {"IsLogin": 1, "SqliteContactDbPath": str(contact)}
    assert wechat_storage_root_from_status(status, "bot") == root
    assert wechat_storage_root_from_status(status, "other") is None
    assert wechat_storage_root_from_status({**status, "IsLogin": 0}, "bot") is None


def _image_event(body: str = f'<msg><img cdnhdimgurl="{URL}"/></msg>'):
    return HookObservation(
        "bot", "room@chatroom", "wxid_member", "hook_group_member_wxid", body,
        True, 3, None, 123, raw_envelope_json=json.dumps({"msg": body}),
        raw_message_json=json.dumps({"msg": body}), original_message_type=3,
    )


def _archive(store: MemoryStore, event: HookObservation):
    return store.archive_message(
        account_id=event.account_id, conversation_id=event.conversation_id,
        sender_id=event.sender_id, raw_envelope={"msg": event.text},
        source_record={"msg": event.text}, body_text=event.text,
    )


def test_image_download_saves_original_once_and_context_has_no_pixels_or_path(tmp_path: Path):
    requests = []

    def serve(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "image/png"}, content=PNG)

    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    event = normalize_message(_image_event())
    assert event is not None
    archived = _archive(store, event)
    references = list(normalized_image_reference_records(event.text))
    ingestor = WeChatImageIngestor(tmp_path, transport=httpx.MockTransport(serve))

    metadata = ingestor.ingest(item=event, message_id=archived.message_id,
                               references=references, store=store)
    assert metadata == {
        "state": "ready", "mime_type": "image/png", "byte_count": len(PNG),
        "sha256": hashlib.sha256(PNG).hexdigest(),
    }
    saved = store.get_image_for_message(
        account_id="bot", conversation_id="room@chatroom", message_id=archived.message_id,
    )
    assert saved is not None and saved["state"] == "ready"
    artifact = Path(saved["artifact_path"])
    assert artifact.read_bytes() == PNG
    assert artifact.parent == (tmp_path / "data" / "received-images").resolve()
    assert json.loads(saved["original_references_json"]) == references

    model_input = build_hook_context((event,), stored=[{
        "message_id": archived.message_id, "image": metadata,
    }])
    serialized = model_input.model_user_content()
    assert "ready" in serialized
    assert "image_reference_count" not in serialized
    assert URL not in serialized
    assert str(artifact) not in serialized
    assert "original-png-payload" not in serialized
    assert "image_bytes" not in serialized

    replay = ingestor.ingest(item=event, message_id=archived.message_id,
                             references=references, store=store)
    assert replay == metadata
    assert len(requests) == 1
    store.close()


def test_image_fetch_failure_is_attached_to_event_without_fallback_thumbnail(tmp_path: Path):
    requests = []

    def fail(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(404)

    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    body = ('<msg><img cdnhdimgurl="https://wx.qpic.cn/original.png" '
            'cdnthumburl="https://wx.qpic.cn/thumb.jpg"/></msg>')
    event = normalize_message(_image_event(body))
    assert event is not None
    archived = _archive(store, event)
    metadata = WeChatImageIngestor(
        tmp_path, transport=httpx.MockTransport(fail),
    ).ingest(item=event, message_id=archived.message_id,
             references=list(normalized_image_reference_records(event.text)), store=store)
    assert metadata == {"state": "failed", "error_code": "image_url_fetch_failed"}
    assert len(requests) == 1
    assert requests[0].url.path == "/original.png"
    assert not list((tmp_path / "data" / "received-images").glob("*"))
    store.close()


def test_emoji_cdn_url_is_saved_as_gif_for_on_demand_vision(tmp_path: Path):
    import hashlib
    import io

    from PIL import Image
    buffer = io.BytesIO()
    Image.new('RGB', (2, 2), 'red').save(buffer, format='GIF')
    gif = buffer.getvalue()
    requests = []

    def serve(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, headers={"content-type": "image/gif"}, content=gif)

    raw = HookObservation(
        "bot", "room@chatroom", "wxid_member", "hook_group_member_wxid",
        f'<msg><emoji md5="{hashlib.md5(gif).hexdigest()}" '
        'cdnurl="http://wxapp.tc.qq.com/sticker.gif?token=x&amp;bizid=1023"/></msg>',
        True, 47, None, 456, original_message_type=47,
    )
    event = normalize_message(raw)
    assert event is not None
    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    archived = _archive(store, event)
    references = list(normalized_image_reference_records(event.text))

    metadata = WeChatImageIngestor(
        tmp_path, transport=httpx.MockTransport(serve),
    ).ingest(item=event, message_id=archived.message_id,
             references=references, store=store)

    assert metadata["state"] == "ready"
    saved = store.get_image_for_message(
        account_id="bot", conversation_id="room@chatroom", message_id=archived.message_id,
    )
    assert saved is not None
    assert Path(str(saved["artifact_path"])).suffix == ".gif"
    assert Path(str(saved["artifact_path"])).read_bytes() == gif
    assert len(requests) == 1
    assert requests[0].url.scheme == "https"
    assert requests[0].url.host == "wxapp.tc.qq.com"
    assert requests[0].url.path == "/sticker.gif"
    assert requests[0].url.params["bizid"] == "1023"
    store.close()


def test_wechat_voice_ingestor_decodes_bounded_silk_to_wav(tmp_path: Path, monkeypatch):
    import sys
    import types
    import wave

    def decode(silk, pcm, sample_rate):
        assert silk.read().startswith(b"#!SILK_V3")
        assert sample_rate == 24000
        pcm.write(b"\x00\x00" * 240)

    monkeypatch.setitem(sys.modules, "pysilk", types.SimpleNamespace(decode=decode))
    voice_hex = (b"\x02#!SILK_V3payload").hex()
    raw = HookObservation(
        "bot", "room@chatroom", "wxid_member", "hook_group_member_wxid",
        '<msg><voicemsg voicelength="10"/></msg>', True, 34, None, 789,
        raw_message_json=json.dumps({"voice_hex": voice_hex,
                                     "msg": '<msg><voicemsg voicelength="10"/></msg>'}),
        original_message_type=34,
    )
    event = normalize_message(raw)
    assert event is not None
    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    archived = _archive(store, event)

    from echoloom.media import WeChatVoiceIngestor
    metadata = WeChatVoiceIngestor(tmp_path).ingest(
        item=event, message_id=archived.message_id, store=store,
    )

    assert metadata["state"] == "ready"
    assert metadata["duration_ms"] == 10
    path = Path(str(store.get_audio_for_message(
        account_id="bot", conversation_id="room@chatroom", message_id=archived.message_id,
    )["artifact_path"]))
    assert path.is_relative_to(store.audio_root)
    with wave.open(str(path), "rb") as audio:
        assert audio.getframerate() == 24000
        assert audio.getnchannels() == 1
        assert audio.getnframes() == 240
    store.close()


def test_voice_artifact_is_removed_when_source_message_expires(tmp_path: Path):
    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    source = store.archive_message(
        account_id="bot", conversation_id="room@chatroom", sender_id="member",
        raw_envelope={"msgtype": 34}, source_record={"msgtype": 34},
        body_text='{"kind":"voice_message"}',
    )
    record = store.begin_audio_ingest(
        account_id="bot", conversation_id="room@chatroom",
        message_id=source.message_id, duration_ms=1000,
    )
    artifact = store.audio_root / "voice.wav"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"wav")
    store.complete_audio_ingest(
        audio_id=str(record["audio_id"]), account_id="bot",
        artifact_path=str(artifact.resolve()), byte_count=3,
        sha256=hashlib.sha256(b"wav").hexdigest(), duration_ms=1000,
    )

    assert store.purge_expired(now="9999-01-01T00:00:00+00:00") == 1

    assert not artifact.exists()
    assert store.audio_metadata_for_message(
        account_id="bot", message_id=source.message_id,
    )["state"] == "deleted"
    store.close()


def test_local_dat_original_uses_hook_decoder_not_thumbnail(tmp_path: Path):
    source_root = tmp_path / "wechat-files"
    room_hash = hashlib.md5(b"room@chatroom", usedforsecurity=False).hexdigest()
    image_hash = "0123456789abcdef0123456789abcdef"
    original = source_root / "bot" / "msg" / "attach" / room_hash / "2026" / "Img" / f"{image_hash}.dat"
    original.parent.mkdir(parents=True)
    original.write_bytes(b"encrypted-source")
    decoded_paths = []

    def decode(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        assert request.url.path == "/Decode_Pic"
        assert Path(body["src_path"]) == original
        assert body["src_path"].endswith(f"{image_hash}.dat")
        assert "_t.dat" not in body["src_path"]
        destination = Path(body["dst_path"])
        destination.write_bytes(PNG)
        decoded_paths.append(destination)
        return httpx.Response(200, json={"ret": 0})

    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    event = normalize_message(_image_event(
        f'<msg><img hdmd5="{image_hash}"/></msg>',
    ))
    assert event is not None
    archived = _archive(store, event)
    metadata = WeChatImageIngestor(
        tmp_path, source_root=source_root, transport=httpx.MockTransport(decode),
    ).ingest(item=event, message_id=archived.message_id,
             references=list(normalized_image_reference_records(event.text)), store=store)
    assert metadata["state"] == "ready"
    saved = store.get_image_for_message(
        account_id="bot", conversation_id="room@chatroom", message_id=archived.message_id,
    )
    assert saved is not None and Path(saved["artifact_path"]).read_bytes() == PNG
    assert len(decoded_paths) == 1 and not decoded_paths[0].exists()
    store.close()


def test_missing_original_is_retried_on_replayed_input_when_cache_appears(tmp_path: Path):
    source_root = tmp_path / "wechat-files"
    room_hash = hashlib.md5(b"room@chatroom", usedforsecurity=False).hexdigest()
    image_hash = "0123456789abcdef0123456789abcdef"
    original = source_root / "bot" / "msg" / "attach" / room_hash / "2026" / "Img" / f"{image_hash}.dat"
    original.parent.mkdir(parents=True)

    def decode(request: httpx.Request) -> httpx.Response:
        Path(json.loads(request.content)["dst_path"]).write_bytes(PNG)
        return httpx.Response(200, json={"ret": 0})

    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    event = normalize_message(_image_event(f'<msg><img hdmd5="{image_hash}"/></msg>'))
    assert event is not None
    archived = _archive(store, event)
    ingestor = WeChatImageIngestor(tmp_path, source_root=source_root,
                                   transport=httpx.MockTransport(decode))
    refs = list(normalized_image_reference_records(event.text))
    first = ingestor.ingest(item=event, message_id=archived.message_id, references=refs, store=store)
    assert first["error_code"] == "original_image_not_found_in_cache"
    original.write_bytes(b"encrypted-source")
    second = ingestor.ingest(item=event, message_id=archived.message_id, references=refs, store=store)
    assert second["state"] == "ready"
    store.close()


def test_thumbnail_fallback_requires_unique_metadata_match_and_marks_quality(tmp_path: Path):
    source_root = tmp_path / "wechat-files"
    room_hash = hashlib.md5(b"room@chatroom", usedforsecurity=False).hexdigest()
    image_dir = source_root / "bot" / "msg" / "attach" / room_hash / "2026" / "Img"
    image_dir.mkdir(parents=True)
    image = io.BytesIO()
    Image.new("RGB", (21, 28), "red").save(image, format="JPEG")
    payload = image.getvalue()
    first = image_dir / "one_t.dat"
    first.write_bytes(b"x" * (len(payload) + 32))
    minute = datetime(2026, 11, 1, 16, 42, tzinfo=timezone(timedelta(hours=8)))
    os.utime(first, (minute.timestamp() + 20, minute.timestamp() + 20))

    def decode(request: httpx.Request) -> httpx.Response:
        Path(json.loads(request.content)["dst_path"]).write_bytes(payload)
        return httpx.Response(200, json={"ret": 0})

    xml = (f'<msg><img md5="0123456789abcdef0123456789abcdef" '
           f'cdnthumblength="{len(payload)}" cdnthumbwidth="364" cdnthumbheight="486"/></msg>')
    event = normalize_message(replace(_image_event(xml), event_time_context={
        "local_minute": "2026-11-01 16:42", "timezone": "Asia/Shanghai",
    }))
    assert event is not None
    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    refs = list(normalized_image_reference_records(event.text))
    ingestor = WeChatImageIngestor(tmp_path, source_root=source_root,
                                   transport=httpx.MockTransport(decode))
    message = _archive(store, event)
    result = ingestor.ingest(item=event, message_id=message.message_id, references=refs, store=store)
    assert result["state"] == "ready" and result["source_kind"] == "thumbnail_inferred"
    saved = store.get_image_for_message(account_id="bot", conversation_id="room@chatroom",
                                        message_id=message.message_id)
    assert saved["source_kind"] == "thumbnail_inferred"

    second = image_dir / "two_t.dat"
    second.write_bytes(first.read_bytes())
    os.utime(second, (minute.timestamp() + 21, minute.timestamp() + 21))
    another = _archive(store, replace(event, sender_id="another"))
    result = ingestor.ingest(item=event, message_id=another.message_id, references=refs, store=store)
    assert result == {"state": "failed", "error_code": "thumbnail_match_ambiguous"}
    store.close()


def test_fast_ingest_skips_cross_chat_scan_but_keeps_local_native_and_remote_paths(tmp_path: Path):
    import pytest

    from echoloom.adapters.recording import RecordingGateway
    from echoloom.wechat_gateway import GatewayConfig

    source_root = tmp_path / "wechat-files"
    other_dir = source_root / "bot_1" / "msg" / "attach" / "other-chat" / "2026" / "Img"
    other_dir.mkdir(parents=True)
    other_dat = other_dir / ("f" * 32 + "_h.dat")
    other_dat.write_bytes(b"encrypted-cross-chat")

    room_hash = hashlib.md5(b"room@chatroom", usedforsecurity=False).hexdigest()
    local_hash = "0123456789abcdef0123456789abcdef"
    local_dir = source_root / "bot_1" / "msg" / "attach" / room_hash / "2026" / "Img"
    local_dir.mkdir(parents=True)
    local_dat = local_dir / f"{local_hash}.dat"
    local_dat.write_bytes(b"encrypted-local")

    decode_calls: list[str] = []
    remote_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Decode_Pic":
            body = json.loads(request.content)
            decode_calls.append(body["src_path"])
            Path(body["dst_path"]).write_bytes(PNG)
            return httpx.Response(200, json={"ret": 0})
        remote_calls.append(str(request.url))
        return httpx.Response(200, headers={"content-type": "image/png"}, content=PNG)

    store = MemoryStore(tmp_path / "data" / "agent-memory.sqlite3")
    ingestor = WeChatImageIngestor(
        tmp_path, source_root=source_root, transport=httpx.MockTransport(handler),
    )
    try:
        # 1. Same-chat local original still succeeds with allow_account_scan=False.
        local_event = normalize_message(_image_event(f'<msg><img hdmd5="{local_hash}"/></msg>'))
        assert local_event is not None
        local_msg = _archive(store, local_event)
        res_local = ingestor.ingest(
            item=local_event,
            message_id=local_msg.message_id,
            references=list(normalized_image_reference_records(local_event.text)),
            store=store,
            allow_account_scan=False,
        )
        assert res_local["state"] == "ready"
        assert decode_calls == [str(local_dat.resolve())]

        # 2. Native downloader runs without cross-chat Decode_Pic when allow_account_scan=False.
        decode_calls.clear()
        native_calls: list[str] = []

        class FakeNativeDownloader:
            def fetch(self, account_id, references, destination):
                native_calls.append(account_id)
                destination.write_bytes(PNG)
                return True

        ingestor.native_downloader = FakeNativeDownloader()
        missing_md5 = hashlib.md5(PNG).hexdigest()
        native_event = normalize_message(_image_event(f'<msg><img md5="{missing_md5}"/></msg>'))
        assert native_event is not None
        native_msg = _archive(store, replace(native_event, sender_id="wxid_native"))
        res_native = ingestor.ingest(
            item=native_event,
            message_id=native_msg.message_id,
            references=list(normalized_image_reference_records(native_event.text)),
            store=store,
            allow_account_scan=False,
        )
        assert res_native["state"] == "ready"
        assert native_calls == ["bot"]
        assert decode_calls == []

        # 3. Remote CDN URL still runs without cross-chat Decode_Pic when allow_account_scan=False.
        ingestor.native_downloader = None
        remote_event = normalize_message(_image_event(
            f'<msg><img md5="{missing_md5}" cdnhdimgurl="{URL}"/></msg>',
        ))
        assert remote_event is not None
        remote_msg = _archive(store, replace(remote_event, sender_id="wxid_remote"))
        res_remote = ingestor.ingest(
            item=remote_event,
            message_id=remote_msg.message_id,
            references=list(normalized_image_reference_records(remote_event.text)),
            store=store,
            allow_account_scan=False,
        )
        assert res_remote["state"] == "ready"
        assert decode_calls == []
        assert remote_calls == [URL]

        # 4. allow_account_scan is keyword-only across gateway and image source APIs.
        recording = RecordingGateway(GatewayConfig(tmp_path, account_id="bot"))
        recording.images["room@chatroom"] = PNG
        out_path = tmp_path / "recorded.png"
        digest, _ = recording.fetch_original_image(
            remote_event, [], out_path, allow_account_scan=False,
        )
        assert digest == hashlib.sha256(PNG).hexdigest()
        with pytest.raises(TypeError):
            recording.fetch_original_image(remote_event, [], out_path, False)
        with pytest.raises(TypeError):
            ingestor.gateway.fetch_original_image(remote_event, [], out_path, False)
    finally:
        store.close()
