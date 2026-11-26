"""Local, read-only browser interface for session traces and tool availability."""

from __future__ import annotations

import asyncio
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
from .contacts import is_contact_id, is_room_id
from .cross_session_tools import CrossSessionToolset
from .idle_tools import IdleActivityToolset
from .memory_store import MemoryStore
from .project_tools import ProjectTools
from .tool_policy import read_policy, set_tool_enabled
from .toolsets import MediaToolset, MemoryToolset, ReplyToolset
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
    shared = wechat | _tool_names(CrossSessionToolset) | _tool_names(ProjectTools)
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
                            r"(?m)^([^\n]*?\|[^\n]*?)：\*\{([\s\S]*?)\}\*", current[-1]
                        )
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


def create_trace_dashboard(trace_dir: Path = DEFAULT_TRACE_DIR, *, gateway=None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
    memory_store: MemoryStore | None = None
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
            memory_store = MemoryStore(ROOT / "data" / "agent-memory.sqlite3")
        return memory_store

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
        if name not in {"app.js", "style.css"}:
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
        return {"brand_name": brand_name,
                "conversations": conversations, "tools": tool_inventory(records),
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
        origin = request.headers.get("origin", "")
        if origin != f"http://{request.headers.get('host', '')}" or request.headers.get("x-dashboard") != "1":
            raise HTTPException(403, "只接受本地页面的操作")
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
        with store._lock:
            rows = store.db.execute(
                "SELECT m.account_id,m.sender_id,MAX(m.captured_at) AS last_seen,"
                "COUNT(*) AS message_count,COALESCE(p.remark,'') AS remark "
                "FROM source_messages m LEFT JOIN sender_profiles p "
                "ON p.account_id=m.account_id AND p.sender_id=m.sender_id AND p.state='active' "
                "WHERE m.deleted_at IS NULL GROUP BY m.account_id,m.sender_id "
                "ORDER BY last_seen DESC LIMIT 100"
            ).fetchall()
        semaphore = asyncio.Semaphore(16)
        async def contact_label(sender_id: str) -> str:
            if not sender_id or sender_id.startswith(("idle-event:", "Agent", "系统")):
                return ""
            def lookup() -> str:
                identity = contact_resolver(sender_id)
                return identity.nickname if identity and identity.nickname else ""
            async with semaphore:
                return await asyncio.to_thread(lookup)
        labels = await asyncio.gather(*(contact_label(str(row["sender_id"])) for row in rows))
        return {"senders": [
            {"account_id": str(row["account_id"]), "sender_id": str(row["sender_id"]),
             "nickname": nickname, "remark": str(row["remark"]),
             "last_seen": str(row["last_seen"]), "message_count": int(row["message_count"])}
            for row, nickname in zip(rows, labels, strict=True)
        ]}

    @app.post("/api/senders/remark")
    def set_sender_remark(update: SenderRemarkUpdate, request: Request):
        origin = request.headers.get("origin", "")
        if (origin != f"http://{request.headers.get('host', '')}"
                or request.headers.get("x-dashboard") != "1"):
            raise HTTPException(403, "只接受本地管理页面的操作")
        if (not update.account_id.strip() or len(update.account_id) > 256
                or not update.sender_id.strip() or len(update.sender_id) > 256
                or len(update.remark) > 120):
            raise HTTPException(400, "备注或联系人标识超出限制")
        try:
            return get_memory_store().set_human_sender_remark(
                account_id=update.account_id,
                sender_id=update.sender_id,
                remark=update.remark,
            )
        except KeyError as exc:
            raise HTTPException(404, "联系人不在当前账号的消息档案中") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

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
    parser = argparse.ArgumentParser(description="Run local EchoLoom read-only trace dashboard")
    parser.add_argument("--host", default=srv.dashboard_host)
    parser.add_argument("--port", type=int, default=srv.dashboard_port)
    args = parser.parse_args(argv)
    if not 1024 <= args.port <= 65535:
        parser.error("port must be between 1024 and 65535")
    uvicorn.run(create_trace_dashboard(), host=args.host, port=args.port, access_log=False)

