"""Local, read-only browser interface for session traces and tool availability."""

from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import Counter, defaultdict, deque
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, Response
from pydantic import BaseModel

from . import local_config, prompt_catalog
from .agent_tools import ReadOnlyToolset
from .cross_session_tools import CrossSessionToolset
from .dashboard_memory import (
    SENDER_BLOAT_THRESHOLD,
    register_memory_routes,
    require_local_mutation,
    resolve_display_names,
    resolve_live_nicknames,
    sqlite_error_guard,
)
from .event_book import EventBook
from .idle_tools import IdleActivityToolset
from .memory_store import MemoryStore
from .project_tools import ProjectTools
from .tool_policy import read_policy, set_tool_enabled
from .toolsets import MediaToolset, MemoryToolset, ReplyToolset, WorkspaceToolset
from .trace_view import DEFAULT_TRACE_DIR, _format_time, _record_epoch, load_records
from .web_tools import WebToolset
from .wechat_gateway import GatewayConfig, create_gateway, default_gateway_factory

ROOT = Path(__file__).resolve().parents[2]
ASSETS = ROOT / "src" / "echoloom" / "trace_dashboard_static"


class ToolSetting(BaseModel):
    enabled: bool


class SenderRemarkUpdate(BaseModel):
    account_id: str
    sender_id: str
    remark: str


def _tool_names(cls) -> set[str]:
    """Read registered tool names from static TOOL_NAMES or fallback to agent_tools()."""
    static_names = getattr(cls, "TOOL_NAMES", None)
    if static_names is not None:
        return set(static_names)
    instance = object.__new__(cls)
    return {tool.name for tool in instance.agent_tools()}


def tool_inventory(records: list[dict]) -> list[dict]:
    wechat = (
        _tool_names(ReplyToolset)
        | _tool_names(MediaToolset)
        | _tool_names(MemoryToolset)
    )
    shared = (
        wechat
        | _tool_names(CrossSessionToolset)
        | _tool_names(ProjectTools)
        | _tool_names(WorkspaceToolset)
    )
    main = shared | _tool_names(WebToolset) | _tool_names(ReadOnlyToolset)
    idle = main | _tool_names(IdleActivityToolset)
    desc_path = prompt_catalog._resolve_prompt_file("tool_descriptions.json")
    if not desc_path.is_file():
        desc_path = ROOT / "prompts" / "tool_descriptions.json"
    descriptions = json.loads(desc_path.read_text("utf-8"))
    try:
        policy = read_policy()
    except sqlite3.Error:
        policy = None
    counts = Counter()
    latest = {}
    for record in records:
        if record.get("record") == "model_request":
            continue
        for part in record.get("content") or []:
            if isinstance(part, dict) and isinstance(part.get("tool_call"), dict):
                name = str(part["tool_call"].get("name") or "")
                if name:
                    counts[name] += 1
                    latest[name] = _format_time(_record_epoch(record))
    return [{"name": name, "description": descriptions.get(name, ""),
             "main": name in main, "idle": name in idle,
             "enabled": policy.get(name, True) if policy is not None else None,
             "calls": counts[name],
             "last_call": latest.get(name)} for name in sorted(main | idle | set(counts))]


def _conversations(records: list[dict]) -> list[dict]:
    grouped: dict[str, dict] = {}
    for record in records:
        conversation = str(record.get("conversation_id") or "")
        row = grouped.setdefault(conversation, {"id": conversation, "sessions": set(),
            "requests": 0, "inputs": 0, "tool_calls": 0, "last_epoch": None, "first_epoch": None})
        if record.get("session_id"):
            row["sessions"].add(str(record["session_id"]))
        time = _record_epoch(record)
        if time is not None:
            row["first_epoch"] = min(row["first_epoch"], time) if row["first_epoch"] is not None else time
            row["last_epoch"] = max(row["last_epoch"], time) if row["last_epoch"] is not None else time
        if record.get("record") == "model_request":
            row["requests"] += 1
        else:
            if record.get("author") == "user" and any(
                isinstance(part, dict) and isinstance(part.get("text"), str) and part["text"]
                for part in record.get("content") or []
            ):
                row["inputs"] += 1
            row["tool_calls"] += sum(1 for p in record.get("content") or []
                                      if isinstance(p, dict) and "tool_call" in p)
    output = []
    for row in grouped.values():
        row["sessions"] = len(row["sessions"])
        row["first"] = _format_time(row.pop("first_epoch"))
        row["last"] = _format_time(row.pop("last_epoch"))
        output.append(row)
    return sorted(output, key=lambda x: x["last"], reverse=True)


def _public_record(record: dict) -> dict:
    return {key: value for key, value in record.items() if key != "_file"} | {
        "display_time": _format_time(_record_epoch(record)),
    }


def _transcript(records: list[dict]) -> list[dict]:
    """Pair real tool events and expose committed replies without inventing delivery."""
    entries: list[dict] = []
    pending: dict[tuple[str, str], deque] = defaultdict(deque)
    for record in records:
        common = {"time": _format_time(_record_epoch(record)),
                  "session_id": str(record.get("session_id") or ""),
                  "conversation_id": str(record.get("conversation_id") or ""),
                  "error": record.get("error_code") or record.get("run_error_type")}
        if record.get("record") == "model_request":
            entries.append({**common, "kind": "request", "model": record.get("model"),
                            "contents": record.get("contents") or []})
            continue
        parts = record.get("content") or []
        for part in parts:
            if not isinstance(part, dict):
                continue
            if isinstance(part.get("text"), str) and part["text"]:
                text = part["text"]
                if record.get("author") == "user":
                    # The rollover block is history; the input after this marker is the new batch.
                    current = text.rsplit("本轮聊天记录：", 1)
                    extracted = [
                        {"sender": sender.strip(), "text": body}
                        for sender, body in re.findall(
                            r"(?m)^(?!\s*引用(?:正文|快照)：)([^\n]+?)：\*\{([\s\S]*?)\}\*",
                            current[-1],
                        )
                        if sender.strip()
                    ] if len(current) > 1 else []
                    entries.append({**common, "kind": "input", "messages": extracted,
                                    "raw": text})
                else:
                    entries.append({**common, "kind": "assistant", "text": text})
            if isinstance(part.get("tool_call"), dict):
                call = part["tool_call"]
                name = str(call.get("name") or "")
                args = call.get("args") if isinstance(call.get("args"), dict) else {}
                entry = {**common, "kind": "tool", "name": name, "args": args,
                         "result": None, "result_time": None}
                entries.append(entry)
                pending[(common["session_id"], name)].append(entry)
            if isinstance(part.get("tool_result"), dict):
                response = part["tool_result"]
                name = str(response.get("name") or "")
                queue = pending[(common["session_id"], name)]
                if queue:
                    target = queue.popleft()
                    target["result"] = response.get("result")
                    target["result_time"] = common["time"]
                else:
                    entries.append({**common, "kind": "tool_result", "name": name,
                                    "result": response.get("result")})
        if common["error"] and not parts:
            entries.append({**common, "kind": "error"})
    return entries


def create_trace_dashboard(
    trace_dir: Path = DEFAULT_TRACE_DIR,
    *,
    gateway=None,
    memory_db_path: Path | None = None,
    episode_db_path: Path | None = None,
) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    memory_store: MemoryStore | None = None
    event_book: EventBook | None = None
    resolved_memory_db = memory_db_path or (ROOT / "data" / "agent-memory.sqlite3")
    resolved_episode_db = episode_db_path or (ROOT / "data" / "episode-memory.sqlite3")
    gateway = (
        gateway
        if gateway is not None
        else create_gateway(
            GatewayConfig(ROOT, contact_timeout_seconds=0.5),
            factory=os.environ.get("ECHOLOOM_GATEWAY_FACTORY")
            or os.environ.get("WECHAT_GATEWAY_FACTORY")
            or default_gateway_factory(),
        )
    )
    contact_resolver = gateway.resolve_contact

    def get_memory_store() -> MemoryStore:
        nonlocal memory_store
        if memory_store is None:
            memory_store = MemoryStore(resolved_memory_db)
        return memory_store

    def get_event_book() -> EventBook:
        nonlocal event_book
        if event_book is None:
            event_book = EventBook(get_memory_store())
        return event_book

    @app.middleware("http")
    async def local_only(request: Request, call_next):
        host = request.headers.get("host", "").lower()
        if host.split(":", 1)[0] not in {"127.0.0.1", "localhost"}:
            return JSONResponse({"detail": "本地访问限定"}, status_code=403)
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = "default-src 'self'; style-src 'self'; script-src 'self'"
        return response

    @app.get("/")
    def index():
        return FileResponse(ASSETS / "index.html")

    @app.get("/assets/{name}")
    def asset(name: str):
        if name not in {"app.js", "memory.js", "style.css"}:
            raise HTTPException(404)
        return FileResponse(ASSETS / name)

    @app.get("/health")
    def health():
        return {"status": "ready"}

    @app.get("/api/overview")
    async def overview():
        records = load_records(trace_dir)
        conversations = _conversations(records)
        cfg = local_config.load_local_runtime_config(ROOT)
        default_status_url = f"http://{cfg.server.ingress_host}:{cfg.server.ingress_port}/status"
        status_url = os.environ.get("ECHOLOOM_STATUS_URL", default_status_url)
        try:
            async with httpx.AsyncClient(timeout=1.5, trust_env=False) as client:
                response = await client.get(status_url)
                status = response.json() if response.is_success else None
        except (httpx.HTTPError, ValueError):
            status = None
        brand_name = cfg.brand_name
        conv_ids = [str(item["id"]) for item in conversations if item.get("id")]
        store_for_names: MemoryStore | None = None
        if memory_db_path is not None or resolved_memory_db.is_file():
            try:
                store_for_names = get_memory_store()
            except sqlite3.Error:
                store_for_names = None
        try:
            names = await resolve_display_names(
                store=store_for_names,
                extra_ids=conv_ids,
                contact_resolver=contact_resolver,
                episode_db_path=resolved_episode_db,
            )
        except sqlite3.Error:
            names = await resolve_display_names(
                store=None,
                extra_ids=conv_ids,
                contact_resolver=contact_resolver,
            )
        return {"brand_name": brand_name,
                "conversations": conversations, "names": names, "tools": tool_inventory(records),
                "pilot": {key: status.get(key) for key in (
                    "mode", "trusted_gateway", "paused", "queue", "in_flight", "inbox_failures",
                    "tool_control_version"
                )} if isinstance(status, dict) else None,
                "trace_records": len(records)}

    @app.get("/api/records")
    def records(conversation: str = "", kind: str = "all", offset: int = 0, limit: int = 100):
        if kind not in {"all", "events", "requests", "tools"} or not 0 <= offset or not 1 <= limit <= 200:
            raise HTTPException(400, "筛选参数无效")
        selected = []
        for row in load_records(trace_dir):
            if conversation and str(row.get("conversation_id") or "") != conversation:
                continue
            is_request = row.get("record") == "model_request"
            has_tool = any(isinstance(part, dict) and ("tool_call" in part or "tool_result" in part)
                           for part in row.get("content") or [])
            if ((kind == "events" and is_request) or (kind == "requests" and not is_request)
                    or (kind == "tools" and (is_request or not has_tool))):
                continue
            selected.append(row)
        selected.reverse()
        return {"total": len(selected), "offset": offset,
                "records": [_public_record(row) for row in selected[offset:offset + limit]]}

    @app.get("/api/transcript")
    def transcript(conversation: str, kind: str = "all", offset: int = 0, limit: int = 50):
        if not conversation or kind not in {"all", "messages", "tools", "requests"} or offset < 0 or not 1 <= limit <= 200:
            raise HTTPException(400, "筛选参数无效")
        rows = [row for row in load_records(trace_dir) if row.get("conversation_id") == conversation]
        entries = _transcript(rows)
        if kind == "messages":
            entries = [e for e in entries if e["kind"] in {"input", "assistant"}
                       or (e["kind"] == "tool" and e.get("args", {}).get("reply_text"))]
        elif kind == "tools":
            entries = [e for e in entries if e["kind"] in {"tool", "tool_result"}]
        elif kind == "requests":
            entries = [e for e in entries if e["kind"] == "request"]
        end = max(0, len(entries) - offset)
        start = max(0, end - limit)
        return {"total": len(entries), "offset": offset, "entries": entries[start:end]}

    @app.get("/api/calls")
    def calls(offset: int = 0, limit: int = 50):
        if offset < 0 or not 1 <= limit <= 200:
            raise HTTPException(400, "筛选参数无效")
        entries = [e for e in _transcript(load_records(trace_dir))
                   if e["kind"] in {"tool", "tool_result"}]
        entries.reverse()
        return {"total": len(entries), "offset": offset, "entries": entries[offset:offset + limit]}

    @app.post("/api/tools/{name}")
    def set_tool(name: str, setting: ToolSetting, request: Request):
        require_local_mutation(request)
        known = {item["name"] for item in tool_inventory([]) if item["main"] or item["idle"]}
        if name not in known:
            raise HTTPException(404, "工具未注册")
        try:
            set_tool_enabled(name, setting.enabled)
        except sqlite3.Error as exc:
            raise HTTPException(503, "无法保存工具设置") from exc
        return {"name": name, "enabled": setting.enabled}

    @app.get("/api/senders")
    async def senders():
        store = get_memory_store()
        with sqlite_error_guard(), store._lock:
            contact_rows = store.db.execute(
                "SELECT m.account_id,m.sender_id,MAX(m.captured_at) AS last_seen,"
                "COUNT(*) AS message_count,COALESCE(p.remark,'') AS remark "
                "FROM source_messages m LEFT JOIN sender_profiles p "
                "ON p.account_id=m.account_id AND p.sender_id=m.sender_id AND p.state='active' "
                "WHERE m.deleted_at IS NULL AND m.sender_id NOT LIKE '%@chatroom' "
                "GROUP BY m.account_id,m.sender_id "
                "ORDER BY last_seen DESC LIMIT 100"
            ).fetchall()
            group_rows = store.db.execute(
                "SELECT m.account_id,m.conversation_id AS sender_id,MAX(m.captured_at) AS last_seen,"
                "COUNT(*) AS message_count,COALESCE(p.remark,'') AS remark "
                "FROM source_messages m LEFT JOIN sender_profiles p "
                "ON p.account_id=m.account_id AND p.sender_id=m.conversation_id AND p.state='active' "
                "WHERE m.deleted_at IS NULL AND m.conversation_id LIKE '%@chatroom' "
                "GROUP BY m.account_id,m.conversation_id "
                "ORDER BY last_seen DESC LIMIT 100"
            ).fetchall()
            by_account: dict[str, list[str]] = defaultdict(list)
            all_ids_by_account: dict[str, list[str]] = defaultdict(list)
            for row in contact_rows:
                acct = str(row["account_id"])
                sid = str(row["sender_id"])
                by_account[acct].append(sid)
                all_ids_by_account[acct].append(sid)
            for row in group_rows:
                acct = str(row["account_id"])
                sid = str(row["sender_id"])
                all_ids_by_account[acct].append(sid)
            memories_by_pair: dict[tuple[str, str], list[dict[str, object]]] = {}
            tags_by_pair: dict[tuple[str, str], list[str]] = {}
            dir_by_account: dict[str, dict[str, dict[str, str]]] = {}
            for acct, sids in by_account.items():
                mem_map = store.sender_memories(account_id=acct, sender_ids=sids)
                for sid, entries in mem_map.items():
                    memories_by_pair[(acct, sid)] = entries
                placeholders = ",".join("?" for _ in sids)
                tag_rows = store.db.execute(
                    "SELECT sender_id, tag FROM sender_tags "
                    f"WHERE account_id=? AND sender_id IN ({placeholders}) "
                    "ORDER BY created_at, tag",
                    (acct, *sids),
                ).fetchall()
                for tag_row in tag_rows:
                    tags_by_pair.setdefault((acct, str(tag_row["sender_id"])), []).append(
                        str(tag_row["tag"])
                    )
            for acct, ids in all_ids_by_account.items():
                dir_by_account[acct] = store.contact_directory_map(
                    account_id=acct, contact_ids=ids
                )

        combined_rows: list[tuple[bool, sqlite3.Row]] = [
            *((False, row) for row in contact_rows),
            *((True, row) for row in group_rows),
        ]
        combined_rows.sort(key=lambda pair: str(pair[1]["last_seen"]), reverse=True)
        live_nicknames = await resolve_live_nicknames(
            [str(row["sender_id"]) for _, row in combined_rows],
            contact_resolver,
        )
        pending_cache: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        seen_cache: set[tuple[str, str]] = set()
        for _, row in combined_rows:
            acct = str(row["account_id"])
            sid = str(row["sender_id"])
            live_nick = live_nicknames.get(sid, "")
            cached_nick = str(
                dir_by_account.get(acct, {}).get(sid, {}).get("nickname") or ""
            ).strip()
            if live_nick and live_nick != cached_nick and (acct, sid) not in seen_cache:
                seen_cache.add((acct, sid))
                pending_cache[acct].append((sid, live_nick, "hook"))
        for acct, cache_entries in pending_cache.items():
            try:
                store.upsert_contact_directory_batch(account_id=acct, entries=cache_entries)
            except (sqlite3.Error, ValueError):
                pass

        result = []
        for is_group, row in combined_rows:
            acct = str(row["account_id"])
            sid = str(row["sender_id"])
            cached_nick = str(
                dir_by_account.get(acct, {}).get(sid, {}).get("nickname") or ""
            ).strip()
            nickname = live_nicknames.get(sid, "") or cached_nick
            memories = [] if is_group else memories_by_pair.get((acct, sid), [])
            result.append(
                {
                    "account_id": acct,
                    "sender_id": sid,
                    "is_group": is_group,
                    "nickname": nickname,
                    "remark": str(row["remark"]),
                    "last_seen": str(row["last_seen"]),
                    "message_count": int(row["message_count"]),
                    "tags": [] if is_group else tags_by_pair.get((acct, sid), []),
                    "memories": memories,
                    "memory_count": 0 if is_group else len(memories),
                    "bloated": False if is_group else len(memories) >= SENDER_BLOAT_THRESHOLD,
                }
            )
        return {"senders": result}

    @app.post("/api/senders/remark")
    def set_sender_remark(update: SenderRemarkUpdate, request: Request):
        require_local_mutation(request)
        if (not update.account_id.strip() or len(update.account_id) > 256
                or not update.sender_id.strip() or len(update.sender_id) > 256
                or len(update.remark) > 120):
            raise HTTPException(400, "备注或联系人标识超出限制")
        try:
            with sqlite_error_guard():
                return get_memory_store().set_human_sender_remark(
                    account_id=update.account_id,
                    sender_id=update.sender_id,
                    remark=update.remark,
                )
        except KeyError as exc:
            raise HTTPException(404, "联系人不在当前账号的消息档案中") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    register_memory_routes(
        app,
        get_memory_store=get_memory_store,
        get_event_book=get_event_book,
        get_episode_db_path=lambda: resolved_episode_db,
        contact_resolver=contact_resolver,
    )

    @app.get("/api/records.jsonl")
    def export_records(conversation: str):
        if not conversation or len(conversation) > 256:
            raise HTTPException(400, "请选择聊天")
        rows = [r for r in load_records(trace_dir) if r.get("conversation_id") == conversation]
        if not rows:
            raise HTTPException(404, "没有匹配的记录")
        body = "\n".join(json.dumps({k: v for k, v in row.items() if k != "_file"},
                                    ensure_ascii=False) for row in rows) + "\n"
        return Response(body, media_type="application/x-ndjson",
                        headers={"Content-Disposition": 'attachment; filename="agent-trace.jsonl"'})

    return app


def main(argv: list[str] | None = None) -> None:
    import argparse

    import uvicorn

    srv = local_config.load_local_runtime_config(ROOT).server
    parser = argparse.ArgumentParser(description="Run local EchoLoom trace and memory workbench")
    parser.add_argument("--host", default=srv.dashboard_host)
    parser.add_argument("--port", type=int, default=srv.dashboard_port)
    args = parser.parse_args(argv)
    if not 1024 <= args.port <= 65535:
        parser.error("port must be between 1024 and 65535")
    uvicorn.run(create_trace_dashboard(), host=args.host, port=args.port, access_log=False)

