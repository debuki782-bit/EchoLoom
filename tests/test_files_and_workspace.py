"""Tests for inbound/outbound WeChat file transfer (1B) and workspace tools (2A)."""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import httpx

from echoloom import agent_profiles, prompt_catalog
from echoloom.adapters.hook import HookGateway
from echoloom.adapters.hook_media import HookImageSource
from echoloom.adapters.hook_native_images import (
    file_download_spec,
    file_upload_spec,
)
from echoloom.context import _format_turn, _message_text
from echoloom.context_assembler import MessageContextAssembler
from echoloom.cross_session import CrossSessionService
from echoloom.media import WeChatFileIngestor, WeChatImageIngestor
from echoloom.memory_store import MemoryStore
from echoloom.mock import SystemClock
from echoloom.models import ConversationKey
from echoloom.outbound import OutboundService
from echoloom.rich_message import (
    extract_file_references,
    normalize_message,
    normalized_file_metadata,
    normalized_file_references,
)
from echoloom.storage import Repository
from echoloom.toolsets import MediaToolset, WorkspaceToolset
from echoloom.turn_scope import tool_turn_scope
from echoloom.usage import UsageLedger
from echoloom.wechat_gateway import GatewayConfig, MessageEvent, create_gateway

ACCOUNT = "wxid_bot_example"
ROOM = "example@chatroom"
OWNER = "wxid_owner_example"
OTHER = "wxid_stranger_example"


def _file_xml(
    *,
    title: str = "report.txt",
    app_type: str = "6",
    totallen: int = 12,
    md5_hex: str = "0123456789abcdef0123456789abcdef",
    fileext: str = "txt",
    cdn_url: str = "30" + "ab" * 96,
    aeskey: str = "fedcba9876543210fedcba9876543210",
    attachid: str = "",
) -> str:
    attach = attachid or (f"@cdn_{cdn_url}_{aeskey}_1" if app_type == "6" else "")
    extra = (
        f"<cdnattachurl>{cdn_url}</cdnattachurl><aeskey>{aeskey}</aeskey><attachid>{attach}</attachid>"
        if app_type == "6"
        else "<fileuploadtoken>tok_123</fileuploadtoken><status>0</status>"
    )
    md5_tag = f"<md5>{md5_hex}</md5>" if app_type == "6" else ""
    return (
        f"{OWNER}:\n"
        f"<msg><appmsg><title>{title}</title><type>{app_type}</type>"
        f"<appattach><totallen>{totallen}</totallen>{extra}<fileext>{fileext}</fileext></appattach>"
        f"{md5_tag}</appmsg></msg>"
    )


def _file_event(xml: str, *, sender_id: str = OWNER, source_msg_id: int = 5001) -> MessageEvent:
    return MessageEvent(
        account_id=ACCOUNT,
        conversation_id=ROOM,
        sender_id=sender_id,
        sender_origin="hook_group_member_wxid",
        text=xml,
        is_group=True,
        message_type=49,
        source_msg_id=source_msg_id,
        capture_tick_ms=1_700_000_000_000,
        original_message_type=49,
    )


def test_file_normalization_redacts_secrets_and_handles_upload_notice():
    secret_cdn = "30" + "cd" * 96
    secret_aes = "00112233445566778899aabbccddeeff"
    md5_hex = hashlib.md5(b"hello world!").hexdigest()
    item = _file_event(
        _file_xml(title="notes.pdf", totallen=12, md5_hex=md5_hex, fileext="pdf", cdn_url=secret_cdn, aeskey=secret_aes)
    )
    norm = normalize_message(item)
    assert norm is not None and norm.message_type == 1 and norm.original_message_type == 49

    refs = list(normalized_file_references(norm.text))
    ref_map = {r["field"]: r["value"] for r in refs}
    assert ref_map["cdnattachurl"] == secret_cdn
    assert ref_map["aeskey"] == secret_aes
    assert ref_map["md5"] == md5_hex

    meta = normalized_file_metadata(norm.text)
    assert meta == {
        "kind": "file_message",
        "filename": "notes.pdf",
        "fileext": "pdf",
        "size_bytes": 12,
        "md5": md5_hex,
    }
    assert secret_cdn not in json.dumps(meta)
    assert secret_aes not in json.dumps(meta)

    notice = normalize_message(_file_event(_file_xml(title="notes.pdf", app_type="74")))
    assert notice is not None
    notice_meta = normalized_file_metadata(notice.text)
    assert notice_meta is not None and notice_meta["kind"] == "file_upload_notice"
    assert normalized_file_references(notice.text) == ()


def test_extract_file_references_backfills_from_attachid_and_builds_native_specs(tmp_path):
    fileid = "30" + "ef" * 96
    aeskey = "abcdef0123456789abcdef0123456789"
    md5_hex = hashlib.md5(b"payload").hexdigest()
    xml = (
        f"<msg><appmsg><title>spec.txt</title><type>6</type>"
        f"<appattach><totallen>7</totallen><attachid>@cdn_{fileid}_{aeskey}_1</attachid><fileext>txt</fileext></appattach>"
        f"<md5>{md5_hex}</md5></appmsg></msg>"
    )
    refs = extract_file_references(xml)
    spec = file_download_spec(ACCOUNT, refs)
    assert spec is not None
    assert spec["fileid"] == fileid
    assert spec["aeskey"] == aeskey
    assert spec["file_type"] == 5
    assert spec["expected_bytes"] == 7

    up_file = tmp_path / "out.bin"
    up_file.write_bytes(b"payload")
    up_spec = file_upload_spec(ACCOUNT, up_file, md5_hex, 7)
    assert up_spec is not None and up_spec["file_type"] == 5 and len(up_spec["job_id"]) == 64


def test_hook_image_source_fetch_received_file_local_cache_and_native_fallback(tmp_path):
    payload = b"important report content"
    md5_hex = hashlib.md5(payload).hexdigest()
    local_dir = tmp_path / "xwechat_files" / f"{ACCOUNT}_abc" / "msg" / "file" / "2026-11"
    local_dir.mkdir(parents=True)
    cached = local_dir / "summary.txt"
    cached.write_bytes(payload)

    norm = normalize_message(_file_event(_file_xml(title="summary.txt", totallen=len(payload), md5_hex=md5_hex)))
    assert norm is not None
    refs = list(normalized_file_references(norm.text))

    source = HookImageSource(tmp_path, source_root=tmp_path / "xwechat_files")
    out1 = tmp_path / "fetched1.txt"
    sha1, size1 = source.fetch_received_file(norm, refs, out1)
    assert size1 == len(payload)
    assert sha1 == hashlib.sha256(payload).hexdigest()
    assert out1.read_bytes() == payload

    # Remove local file and verify fallback to :30002 /Download_File
    cached.unlink()
    dl_dir = tmp_path / "data" / "native-file-downloads"
    dl_dir.mkdir(parents=True)

    def respond_native(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/Download_File":
            body = json.loads(request.content)
            (dl_dir / f"{body['job_id']}.bin").write_bytes(payload)
            return httpx.Response(200, json={"job_id": body["job_id"], "state": "queued"})
        if request.url.path.startswith("/Download_File/"):
            job_id = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"job_id": job_id, "state": "ready", "byte_count": len(payload)})
        return httpx.Response(404)

    source_with_native = HookImageSource(
        tmp_path,
        source_root=tmp_path / "xwechat_files",
        native_download_url="http://127.0.0.1:30002",
        transport=httpx.MockTransport(respond_native),
    )
    assert source_with_native.native_downloader is not None
    out2 = tmp_path / "fetched2.txt"
    sha2, size2 = source_with_native.fetch_received_file(norm, refs, out2)
    assert sha2 == sha1 and size2 == len(payload) and out2.read_bytes() == payload


def test_hook_gateway_send_file_primary_and_native_upload_fallback(tmp_path):
    artifact = tmp_path / "deliverable.txt"
    payload = b"generated deliverable"
    artifact.write_bytes(payload)

    # 1. Primary /SendFileMsg endpoint works
    primary_seen: list[tuple[str, dict]] = []

    def respond_primary(request: httpx.Request) -> httpx.Response:
        primary_seen.append((request.url.path, json.loads(request.content)))
        if request.url.path == "/SendFileMsg":
            return httpx.Response(200, json={"ret": 0, "sent": True})
        return httpx.Response(404)

    gw1 = HookGateway(
        GatewayConfig(
            tmp_path,
            account_id=ACCOUNT,
            send_enabled=True,
            base_url="http://127.0.0.1:30001",
            transport=httpx.MockTransport(respond_primary),
            send_cooldown_range=(0.0, 0.0),
        )
    )
    status1, reason1 = gw1.send_file(ROOM, artifact)
    gw1.close()
    assert status1 == "accepted_by_server" and reason1 == "hook_file_send_accepted"
    assert primary_seen == [("/SendFileMsg", {"to_wxid": ROOM, "file_path": str(artifact)})]

    # 2. Fallback to :30002 /Upload_File + :30001 /ForwardXMLMsg when /SendFileMsg returns 404
    fallback_seen: list[tuple[int | None, str, dict]] = []
    fileid = "30" + "12" * 96
    aeskey = "99887766554433221100ffeeddccbbaa"

    def respond_fallback(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content) if request.content else {}
        fallback_seen.append((request.url.port, request.url.path, body))
        if request.url.port == 30001 and request.url.path == "/SendFileMsg":
            return httpx.Response(404, json={"ret": -1})
        if request.url.port == 30002 and request.url.path == "/Upload_File":
            return httpx.Response(200, json={"job_id": body["job_id"], "state": "queued"})
        if request.url.port == 30002 and request.url.path.startswith("/Upload_File/"):
            job_id = request.url.path.rsplit("/", 1)[-1]
            return httpx.Response(200, json={"job_id": job_id, "state": "ready", "fileid": fileid, "aeskey": aeskey})
        if request.url.port == 30001 and request.url.path == "/ForwardXMLMsg":
            return httpx.Response(200, json={"ret": 0})
        return httpx.Response(404)

    gw2 = HookGateway(
        GatewayConfig(
            tmp_path,
            account_id=ACCOUNT,
            send_enabled=True,
            base_url="http://127.0.0.1:30001",
            native_download_url="http://127.0.0.1:30002",
            transport=httpx.MockTransport(respond_fallback),
            send_cooldown_range=(0.0, 0.0),
        )
    )
    status2, reason2 = gw2.send_file(ROOM, artifact)
    gw2.close()
    assert status2 == "accepted_by_server" and reason2 == "native_upload_and_forward_accepted"
    fwd = next(b for port, path, b in fallback_seen if path == "/ForwardXMLMsg")
    assert fwd["type"] == 6 and fwd["to_wxid"] == ROOM
    assert fileid in fwd["content"] and aeskey in fwd["content"]


def test_file_ingest_context_purge_and_cross_session_redaction(tmp_path):
    payload = b"alpha\nbeta\ngamma\n"
    md5_hex = hashlib.md5(payload).hexdigest()
    secret_cdn = "30" + "99" * 96
    secret_aes = "1234567890abcdef1234567890abcdef"
    raw_event = _file_event(_file_xml(title="plan.md", totallen=len(payload), md5_hex=md5_hex, fileext="md", cdn_url=secret_cdn, aeskey=secret_aes))
    norm = normalize_message(raw_event)
    assert norm is not None

    gw = create_gateway(
        GatewayConfig(tmp_path, account_id=ACCOUNT, send_enabled=True, allowed_conversations=frozenset({ConversationKey(ACCOUNT, ROOM)})),
        factory="echoloom.adapters.recording:RecordingGateway",
    )
    gw.files[ROOM] = payload
    memory = MemoryStore(tmp_path / "data/memory.sqlite3")
    try:
        assembler = MessageContextAssembler(
            memory,
            image_ingestor=WeChatImageIngestor(tmp_path, gateway=gw),
            file_ingestor=WeChatFileIngestor(tmp_path, gateway=gw),
            archive_envelope=gw.archive_envelope,
        )
        batch = asyncio.run(assembler.assemble((norm,)))
        turn = batch.context.turns[0]
        assert turn.file_metadata is not None
        assert turn.file_metadata["state"] == "ready"
        rendered = _format_turn(turn, {}, True, ROOM)
        assert "plan.md" in rendered
        assert secret_cdn not in rendered and secret_aes not in rendered
        body_text, _ = _message_text(turn.text, turn, True, {})
        assert "plan.md" in body_text and secret_cdn not in body_text and secret_aes not in body_text

        saved_path = Path(str(memory.get_file_by_id(account_id=ACCOUNT, conversation_id=ROOM, file_id=str(turn.file_metadata["file_id"]))["artifact_path"]))
        assert saved_path.is_file() and saved_path.read_bytes() == payload

        cross = CrossSessionService(memory)
        sql_res = cross.query_sql(
            account_id=ACCOUNT,
            query="SELECT file_id, filename, state, byte_count FROM message_files WHERE account_id=?",
        )
        assert sql_res["status"] == "completed"
        assert sql_res["rows"] and sql_res["rows"][0]["filename"] == "plan.md"
        denied = cross.query_sql(
            account_id=ACCOUNT,
            query="SELECT original_references_json FROM message_files WHERE account_id=?",
        )
        assert denied["status"] == "rejected"

        memory.purge_expired(now="2999-01-01T00:00:00+00:00")
        assert not saved_path.exists()
        assert memory.file_metadata_for_message(account_id=ACCOUNT, message_id=batch.memory_message_ids[0])["state"] == "deleted"
    finally:
        memory.close()


def _ctx_window(sender_id: str, *, is_idle: bool = False):
    from echoloom.context import ContextTurn, ContextWindow
    turn = ContextTurn(sender_id=sender_id, sender_origin="hook_group_member_wxid", text="hi", message_id="msg_1")
    return ContextWindow(account_id=ACCOUNT, conversation_id=ROOM, is_group=True, turns=(turn,), is_idle_event=is_idle)


def test_workspace_toolset_sandboxing_owner_gating_and_send_wechat_file(tmp_path):
    ws_tools = WorkspaceToolset(tmp_path, command_owner_ids={OWNER})
    gw = create_gateway(
        GatewayConfig(tmp_path, account_id=ACCOUNT, send_enabled=True, allowed_conversations=frozenset({ConversationKey(ACCOUNT, ROOM)})),
        factory="echoloom.adapters.recording:RecordingGateway",
    )
    memory = MemoryStore(tmp_path / "data/memory.sqlite3")
    outbound = OutboundService(tmp_path / "data/tool-receipts.sqlite3", account=ACCOUNT, default_room=ROOM, gateway=gw, clock=SystemClock(), memory_store=memory)
    media = MediaToolset(root=tmp_path, account=ACCOUNT, default_room=ROOM, outbound=outbound, gateway=gw, memory_store=memory, command_owner_ids={OWNER})

    try:
        # Unauthorized non-owner & idle turns rejected
        with tool_turn_scope(_ctx_window(OTHER), "op-other"):
            assert ws_tools.write_workspace_file("a.md", "bad")["reason"] == "owner_permission_required"
            assert asyncio.run(media.send_wechat_file("a.md"))["reason"] == "owner_permission_required"
        with tool_turn_scope(_ctx_window(OWNER, is_idle=True), "op-idle"):
            assert ws_tools.write_workspace_file("a.md", "idle")["reason"] == "idle_turn_cannot_run_owner_action"

        # Mixed owner + non-owner batch rejected
        from echoloom.context import ContextTurn, ContextWindow
        mixed = ContextWindow(
            account_id=ACCOUNT,
            conversation_id=ROOM,
            is_group=True,
            turns=(
                ContextTurn(OWNER, "hook_group_member_wxid", "create file", message_id="m1"),
                ContextTurn(OTHER, "hook_group_member_wxid", "overwrite secret", message_id="m2"),
            ),
        )
        with tool_turn_scope(mixed, "op-mixed"):
            assert ws_tools.write_workspace_file("a.md", "mixed")["reason"] == "owner_permission_required"

        # Authorized owner turn succeeds and enforces safety rules
        with tool_turn_scope(_ctx_window(OWNER), "op-owner-1"):
            created = ws_tools.write_workspace_file("notes/todo.md", "line1\nline2\n")
            assert created["status"] == "written"
            dup_err = ws_tools.write_workspace_file("notes/todo.md", "x", overwrite=False)
            assert dup_err["status"] == "rejected"
            edited = ws_tools.edit_workspace_file("notes/todo.md", "line2", "line2-done", expected_sha256=created["sha256"])
            assert edited["status"] == "edited"
            read_back = ws_tools.read_workspace_file("notes/todo.md")
            assert read_back["content"] == "line1\nline2-done"
            assert (tmp_path / "data/workspace/notes/todo.md").read_text("utf-8") == "line1\nline2-done\n"
            listing = ws_tools.list_workspace_files("notes")
            assert listing["items"][0]["name"] == "todo.md"

            # Path traversal / backslash rejected
            assert ws_tools.read_workspace_file("../secrets.txt")["status"] == "rejected"
            assert ws_tools.write_workspace_file("notes\\bad.txt", "x")["status"] == "rejected"
            assert ws_tools.write_workspace_file("received-files/x.txt", "x")["status"] == "rejected"

            # Send workspace deliverable back via WeChat
            sent = asyncio.run(media.send_wechat_file("notes/todo.md", filename="done.md"))
            assert sent["status"] == "queued" and sent["filename"] == "done.md"
            assert any(item["kind"] == "file" and item["filename"] == "done.md" for item in gw.outbound)
            archived_out = memory.get_file_by_id(account_id=ACCOUNT, conversation_id=ROOM, file_id=str(sent["file_id"]))
            assert archived_out is not None and archived_out["filename"] == "done.md"
    finally:
        memory.close()
        outbound.close()


def _make_docx_bytes(paragraphs: list[str]) -> bytes:
    buf = io.BytesIO()
    body = "".join(f"<w:p><w:r><w:t>{p}</w:t></w:r></w:p>" for p in paragraphs)
    xml = f'<?xml version="1.0" encoding="UTF-8"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>{body}</w:body></w:document>'
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("word/document.xml", xml)
    return buf.getvalue()


def _make_xlsx_bytes() -> bytes:
    buf = io.BytesIO()
    shared = '<?xml version="1.0"?><sst><si><t>Name</t></si><si><t>Alice</t></si></sst>'
    sheet1 = '<?xml version="1.0"?><worksheet><sheetData><row r="1"><c r="A1" t="s"><v>0</v></c><c r="B1"><v>99</v></c></row><row r="2"><c r="A2" t="s"><v>1</v></c><c r="B2"><v>42</v></c></row></sheetData></worksheet>'
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("xl/sharedStrings.xml", shared)
        zf.writestr("xl/worksheets/sheet1.xml", sheet1)
    return buf.getvalue()


def test_read_wechat_file_docx_xlsx_and_pdf_with_gemini_mock(tmp_path):
    memory = MemoryStore(tmp_path / "data/memory.sqlite3")
    gw = create_gateway(GatewayConfig(tmp_path, account_id=ACCOUNT, send_enabled=True), factory="echoloom.adapters.recording:RecordingGateway")
    outbound = OutboundService(tmp_path / "data/tool-receipts.sqlite3", account=ACCOUNT, default_room=ROOM, gateway=gw, clock=SystemClock(), memory_store=memory)
    repo = Repository(tmp_path / "data/usage.sqlite3")
    usage = UsageLedger(repo, clock=SystemClock())

    seen_calls: list[dict] = []

    class FakeModels:
        async def count_tokens(self, *, model, contents):
            return SimpleNamespace(total_tokens=120)

        async def generate_content(self, *, model, contents, config):
            seen_calls.append({"model": model, "contents": contents})
            return SimpleNamespace(
                text="这份 PDF 包含合同要点与交付时间表。",
                usage_metadata=SimpleNamespace(prompt_token_count=120, total_token_count=160, cached_content_token_count=0),
            )

    fake_client = SimpleNamespace(aio=SimpleNamespace(models=FakeModels()))
    media = MediaToolset(
        root=tmp_path,
        account=ACCOUNT,
        default_room=ROOM,
        outbound=outbound,
        gateway=gw,
        client=fake_client,
        usage=usage,
        memory_store=memory,
    )

    try:
        msg_ids = []
        ingestor = WeChatFileIngestor(tmp_path, gateway=gw)
        for idx, (fname, ext, content) in enumerate([
            ("doc.docx", "docx", _make_docx_bytes(["第一段：架构说明", "第二段：验收标准"])),
            ("sheet.xlsx", "xlsx", _make_xlsx_bytes()),
            ("contract.pdf", "pdf", b"%PDF-1.4\n%fake\n"),
        ], start=1):
            gw.files[ROOM] = content
            md5_hex = hashlib.md5(content).hexdigest()
            norm = normalize_message(_file_event(_file_xml(title=fname, totallen=len(content), md5_hex=md5_hex, fileext=ext), source_msg_id=8000 + idx))
            assert norm is not None
            msg = memory.archive_message(account_id=ACCOUNT, conversation_id=ROOM, sender_id=OWNER, raw_envelope={"id": idx}, body_text=norm.text, source_message_id=8000 + idx)
            meta = ingestor.ingest(item=norm, message_id=msg.message_id, references=list(normalized_file_references(norm.text)), store=memory, metadata=normalized_file_metadata(norm.text))
            assert meta["state"] == "ready"
            msg_ids.append((msg.message_id, meta["file_id"]))

        with tool_turn_scope(_ctx_window(OWNER), "op-read-files"):
            docx_res = asyncio.run(media.read_wechat_file(str(msg_ids[0][1])))
            assert docx_res["kind"] == "docx_extracted_text" and "第一段：架构说明" in str(docx_res["content"])

            xlsx_res = asyncio.run(media.read_wechat_file(str(msg_ids[1][1])))
            assert xlsx_res["kind"] == "xlsx_extracted_table" and "Name\t99" in str(xlsx_res["content"]) and "Alice\t42" in str(xlsx_res["content"])

            pdf_res = asyncio.run(media.read_wechat_file(str(msg_ids[2][1])))
            assert pdf_res["summary"] == "这份 PDF 包含合同要点与交付时间表。" and pdf_res["cached"] is False
            pdf_part = seen_calls[0]["contents"][0].parts[1]
            assert pdf_part.inline_data.mime_type == "application/pdf"

            pdf_cached = asyncio.run(media.read_wechat_file(str(msg_ids[2][1])))
            assert pdf_cached["cached"] is True and len(seen_calls) == 1
    finally:
        memory.close()
        outbound.close()
        repo.conn.close()


def test_tool_descriptions_cover_all_media_and_workspace_tools():
    profiles = agent_profiles.load_agent_profiles()
    assert "workspace" in profiles.profiles[profiles.default_profile].toolsets
    desc_path = prompt_catalog._resolve_prompt_file("tool_descriptions.json")
    descriptions = json.loads(desc_path.read_text("utf-8"))
    for name in (*MediaToolset.TOOL_NAMES, *WorkspaceToolset.TOOL_NAMES):
        assert name in descriptions and descriptions[name].strip(), name


def test_native_download_service_handles_file_download_and_ephemeral_upload(tmp_path):
    import importlib.util

    script = Path(__file__).resolve().parents[1] / "scripts/run-native-image-download.py"
    spec_mod = importlib.util.spec_from_file_location("native_service_test", script)
    assert spec_mod and spec_mod.loader
    mod = importlib.util.module_from_spec(spec_mod)
    spec_mod.loader.exec_module(mod)

    payload = b"native file binary content"
    md5_hex = hashlib.md5(payload).hexdigest()
    secret_aes = "00112233445566778899aabbccddeeff"
    secret_fileid = "30" + "77" * 96
    upload_src = tmp_path / "to_upload.txt"
    upload_src.write_bytes(payload)

    class StubAdapter:
        def submit_file_download(self, spec, destination):
            Path(destination).write_bytes(payload)
            return 0

        def result(self, spec):
            return {"completed": True, "code": 0, "resource_bound": True}

        def submit_file_upload(self, spec):
            return 0

        def pop_file_upload(self, spec):
            return {"completed": True, "code": 0, "fileid": secret_fileid, "aeskey": secret_aes}

    svc = mod.DownloadService(
        tmp_path / "img_dl",
        StubAdapter(),
        ACCOUNT,
        lambda: True,
        file_root=tmp_path / "file_dl",
    )
    try:
        dl_spec = file_download_spec(
            ACCOUNT,
            [
                {"field": "cdnattachurl", "value": secret_fileid},
                {"field": "aeskey", "value": secret_aes},
                {"field": "md5", "value": md5_hex},
                {"field": "totallen", "value": str(len(payload))},
            ],
        )
        assert dl_spec is not None
        svc.submit(dl_spec, mode="file_download")

        up_spec = file_upload_spec(ACCOUNT, upload_src, md5_hex, len(payload))
        assert up_spec is not None
        svc.submit(up_spec, mode="file_upload")
        svc.pending.join()

        dl_res = svc.get(dl_spec["job_id"])
        assert dl_res and dl_res["state"] == "ready"
        up_res = svc.get(up_spec["job_id"], include_ephemeral_upload=True)
        assert up_res and up_res["state"] == "ready"
        assert up_res["fileid"] == secret_fileid and up_res["aeskey"] == secret_aes

        # Verify persistent SQLite receipts never retain raw CDN/AES credentials
        raw_rows = svc.db.execute("SELECT job_id, state, reason, metadata_json FROM jobs").fetchall()
        dumped = json.dumps(raw_rows)
        assert secret_fileid not in dumped and secret_aes not in dumped
    finally:
        svc.close()


def test_ingress_drops_upload_notice_74_and_queues_file_message_6(tmp_path):
    from echoloom.ingress import Ingress

    ingress = Ingress(tmp_path / "inbox.sqlite3", ACCOUNT, ROOM)
    try:
        notice_ev = _file_event(_file_xml(title="resume.pdf", app_type="74", fileext="pdf"), source_msg_id=None)
        file_ev = _file_event(_file_xml(title="resume.pdf", app_type="6", fileext="pdf"), source_msg_id=5002)
        assert ingress.offer(notice_ev) == "observation_only"
        assert ingress.scheduler.active_count() == 0
        assert ingress.offer(file_ev) == "queued_test"
        assert ingress.scheduler.active_count() == 1
    finally:
        ingress.close()


def _make_text_pdf_bytes(lines: list[str]) -> bytes:
    """Build a minimal valid 1-page PDF-1.4 containing ASCII text lines."""
    stream_ops = ["BT", "/F1 12 Tf", "72 720 Td"]
    for idx, line in enumerate(lines):
        escaped = line.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        if idx > 0:
            stream_ops.append("0 -16 Td")
        stream_ops.append(f"({escaped}) Tj")
    stream_ops.append("ET")
    stream_data = "\n".join(stream_ops).encode("latin-1")
    objects = [
        b"1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        b"2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n",
        (
            b"3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>\nendobj\n"
        ),
        b"4 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj\n",
        (
            f"5 0 obj\n<< /Length {len(stream_data)} >>\nstream\n".encode("ascii")
            + stream_data
            + b"\nendstream\nendobj\n"
        ),
    ]
    out = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for obj in objects:
        offsets.append(len(out))
        out.extend(obj)
    xref_pos = len(out)
    out.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode("ascii"))
    for off in offsets[1:]:
        out.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    out.extend(
        f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode("ascii")
    )
    return bytes(out)


def test_read_text_pdf_extracts_lines_and_multimodal_receives_chat_context(tmp_path):
    from echoloom.context import ContextTurn, ContextWindow

    memory = MemoryStore(tmp_path / "data/memory.sqlite3")
    gw = create_gateway(
        GatewayConfig(tmp_path, account_id=ACCOUNT, send_enabled=True),
        factory="echoloom.adapters.recording:RecordingGateway",
    )
    outbound = OutboundService(
        tmp_path / "data/tool-receipts.sqlite3",
        account=ACCOUNT,
        default_room=ROOM,
        gateway=gw,
        clock=SystemClock(),
        memory_store=memory,
    )
    repo = Repository(tmp_path / "data/usage.sqlite3")
    usage = UsageLedger(repo, clock=SystemClock())
    ws_tools = WorkspaceToolset(tmp_path, command_owner_ids={OWNER})

    seen_calls: list[dict] = []

    class FakeModels:
        async def count_tokens(self, *, model, contents):
            return SimpleNamespace(total_tokens=150)

        async def generate_content(self, *, model, contents, config):
            seen_calls.append({"model": model, "contents": contents})
            return SimpleNamespace(
                text=f"视觉回答#{len(seen_calls)}",
                usage_metadata=SimpleNamespace(prompt_token_count=150, total_token_count=190, cached_content_token_count=0),
            )

    fake_client = SimpleNamespace(aio=SimpleNamespace(models=FakeModels()))
    media = MediaToolset(
        root=tmp_path,
        account=ACCOUNT,
        default_room=ROOM,
        outbound=outbound,
        gateway=gw,
        client=fake_client,
        usage=usage,
        memory_store=memory,
    )
    try:
        ingestor = WeChatFileIngestor(tmp_path, gateway=gw)
        text_pdf = _make_text_pdf_bytes(["Milestone Alpha 2026-11-26", "Owner: Alice, Budget: $1200"])
        gw.files[ROOM] = text_pdf
        norm_text = normalize_message(
            _file_event(_file_xml(title="milestone.pdf", totallen=len(text_pdf), md5_hex=hashlib.md5(text_pdf).hexdigest(), fileext="pdf"), source_msg_id=9001)
        )
        assert norm_text is not None
        msg_text = memory.archive_message(account_id=ACCOUNT, conversation_id=ROOM, sender_id=OWNER, raw_envelope={"id": 1}, body_text=norm_text.text, source_message_id=9001)
        meta_text = ingestor.ingest(item=norm_text, message_id=msg_text.message_id, references=list(normalized_file_references(norm_text.text)), store=memory, metadata=normalized_file_metadata(norm_text.text))
        assert meta_text["state"] == "ready"
        text_file_id = str(meta_text["file_id"])

        scan_pdf = b"%PDF-1.4\n%scanned-image-only\n"
        gw.files[ROOM] = scan_pdf
        norm_scan = normalize_message(
            _file_event(_file_xml(title="scan.pdf", totallen=len(scan_pdf), md5_hex=hashlib.md5(scan_pdf).hexdigest(), fileext="pdf"), source_msg_id=9002)
        )
        assert norm_scan is not None
        msg_scan = memory.archive_message(account_id=ACCOUNT, conversation_id=ROOM, sender_id=OWNER, raw_envelope={"id": 2}, body_text=norm_scan.text, source_message_id=9002)
        meta_scan = ingestor.ingest(item=norm_scan, message_id=msg_scan.message_id, references=list(normalized_file_references(norm_scan.text)), store=memory, metadata=normalized_file_metadata(norm_scan.text))
        assert meta_scan["state"] == "ready"
        scan_file_id = str(meta_scan["file_id"])

        ctx = ContextWindow(
            account_id=ACCOUNT,
            conversation_id=ROOM,
            is_group=True,
            turns=(
                ContextTurn(OWNER, "hook_group_member_wxid", "上一句讨论预算上限 1200 美元", message_id="ctx-prev"),
                ContextTurn(OWNER, "hook_group_member_wxid", "看看这里面的时间戳和扫描件", message_id=msg_text.message_id),
            ),
        )
        with tool_turn_scope(ctx, "op-pdf-modes"):
            # 1) Text PDF uses local extraction with 0 multimodal calls
            res = asyncio.run(media.read_wechat_file(text_file_id, question="提取预算数字"))
            assert res["kind"] == "pdf_extracted_text"
            assert res["extraction_mode"] == "local_pdf_text"
            assert "=== Page 1/1 ===" in str(res["content"])
            assert "Milestone Alpha 2026-11-26" in str(res["content"])
            assert "Budget: $1200" in str(res["content"])
            assert len(seen_calls) == 0

            # 2) Workspace read_workspace_file can also paginate text PDFs under received-files/
            archived_row = memory.get_file_by_id(account_id=ACCOUNT, conversation_id=ROOM, file_id=text_file_id)
            assert archived_row is not None
            archived_name = Path(str(archived_row["artifact_path"])).name
            ws_res = ws_tools.read_workspace_file(f"received-files/{archived_name}")
            assert ws_res["status"] == "completed" and ws_res["kind"] == "pdf_extracted_text"
            assert "Milestone Alpha 2026-11-26" in str(ws_res["content"])

            # 3) Explicit visual_read=True triggers multimodal WITH same-chat context
            vis_res = asyncio.run(media.read_wechat_file(text_file_id, question="核对排版上的盖章", visual_read=True))
            assert vis_res["extraction_mode"] == "multimodal_pdf"
            assert len(seen_calls) == 1
            prompt_sent = seen_calls[0]["contents"][0].parts[0].text
            assert "核对排版上的盖章" in prompt_sent and "上一句讨论预算上限 1200 美元" in prompt_sent

            # 4) Scanned PDF falls back to multimodal, caches default summary, and lets custom question bypass cache
            scan_default = asyncio.run(media.read_wechat_file(scan_file_id))
            assert scan_default["summary"] == "视觉回答#2" and scan_default["cached"] is False
            assert len(seen_calls) == 2 and "上一句讨论预算上限 1200 美元" in seen_calls[1]["contents"][0].parts[0].text

            scan_detail = asyncio.run(media.read_wechat_file(scan_file_id, question="第2行落款日期是什么？"))
            assert scan_detail["summary"] == "视觉回答#3" and scan_detail["cached"] is False
            assert len(seen_calls) == 3

            # Default general summary must remain preserved (not overwritten by narrow detail answer)
            scan_recheck = asyncio.run(media.read_wechat_file(scan_file_id))
            assert scan_recheck["cached"] is True and scan_recheck["summary"] == "视觉回答#2"
    finally:
        memory.close()
        outbound.close()
        repo.conn.close()
