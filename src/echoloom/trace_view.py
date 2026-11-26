"""Read-only renderer for session JSONL traces.

Answers three questions per model turn: what the model was shown (including
the injected runtime tail), what it replied, and which tools ran with what
results. Trace files are written by ``ScopedAdkRunner``; this module never
writes anything.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

DEFAULT_TRACE_DIR = Path(__file__).resolve().parents[2] / "data" / "session-traces"
_RECORDS_CACHE: dict[
    Path, tuple[tuple[tuple[str, int, int], ...], list[dict[str, object]]]
] = {}


def _record_epoch(record: dict[str, object]) -> float | None:
    """Normalize event epoch floats and model_request ISO strings to epoch seconds."""
    stamp = record.get("timestamp")
    if isinstance(stamp, (int, float)) and not isinstance(stamp, bool):
        return float(stamp)
    if isinstance(stamp, str):
        try:
            moment = datetime.fromisoformat(stamp)
        except ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=UTC)
        return moment.timestamp()
    return None


def _format_time(epoch: float | None) -> str:
    if epoch is None:
        return "?"
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


def _clip(text: str, limit: int, full: bool) -> str:
    if full or len(text) <= limit:
        return text
    return f"{text[:limit]}……[已截断，原 {len(text)} 字符，用 --full 查看]"


def _indent(text: str) -> str:
    return "\n".join(("  " + line if line else line) for line in text.splitlines())


def _dump(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def load_records(
    trace_dir: Path, conversation_prefix: str = ""
) -> list[dict[str, object]]:
    """Load and chronologically merge all trace records, optionally per chat."""
    resolved = Path(trace_dir).resolve()
    files = sorted(resolved.glob("*.jsonl")) if resolved.is_dir() else []
    signature_list: list[tuple[str, int, int]] = []
    for path in files:
        try:
            st = path.stat()
            signature_list.append((path.name, st.st_size, st.st_mtime_ns))
        except OSError:
            signature_list.append((path.name, -1, -1))
    signature = tuple(signature_list)
    cached = _RECORDS_CACHE.get(resolved)
    if cached is not None and cached[0] == signature:
        all_records = cached[1]
    else:
        all_records = []
        for path in files:
            try:
                content = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for line in content.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                except ValueError:
                    continue
                if not isinstance(record, dict):
                    continue
                record = dict(record)
                record["_file"] = path.name
                all_records.append(record)
        all_records.sort(key=lambda item: _record_epoch(item) or 0.0)
        if len(_RECORDS_CACHE) >= 8:
            _RECORDS_CACHE.clear()
        _RECORDS_CACHE[resolved] = (signature, all_records)
    if not conversation_prefix:
        return list(all_records)
    return [
        record
        for record in all_records
        if str(record.get("conversation_id") or "").startswith(conversation_prefix)
    ]


def summarize(trace_dir: Path) -> list[dict[str, object]]:
    """Group trace records per conversation: sessions, turns, tool calls, range."""
    grouped: dict[str, dict[str, object]] = {}
    for record in load_records(trace_dir):
        conversation = str(record.get("conversation_id") or "(未知会话)")
        entry = grouped.setdefault(
            conversation,
            {
                "conversation_id": conversation,
                "sessions": set(),
                "turns": 0,
                "tool_calls": 0,
                "first": None,
                "last": None,
            },
        )
        entry["sessions"].add(str(record.get("session_id") or ""))
        epoch = _record_epoch(record)
        if epoch is not None:
            if entry["first"] is None or epoch < entry["first"]:
                entry["first"] = epoch
            if entry["last"] is None or epoch > entry["last"]:
                entry["last"] = epoch
        if record.get("record") == "model_request":
            entry["turns"] = int(entry["turns"]) + 1
            continue
        for part in record.get("content") or []:
            if isinstance(part, dict) and "tool_call" in part:
                entry["tool_calls"] = int(entry["tool_calls"]) + 1
    rows = [
        {
            "conversation_id": str(entry["conversation_id"]),
            "sessions": len(entry["sessions"]),
            "turns": int(entry["turns"]),
            "tool_calls": int(entry["tool_calls"]),
            "first": _format_time(entry["first"]),
            "last": _format_time(entry["last"]),
        }
        for entry in grouped.values()
    ]
    rows.sort(key=lambda row: str(row["last"]), reverse=True)
    return rows


def render(
    records: list[dict[str, object]],
    *,
    full: bool = False,
    text_limit: int = 4000,
    tool_limit: int = 2000,
) -> str:
    """Render merged records as a readable per-turn transcript."""
    out: list[str] = []
    for record in records:
        when = _format_time(_record_epoch(record))
        if record.get("record") == "model_request":
            session = str(record.get("session_id") or "")
            out.append("")
            out.append(f"════════ 模型请求 {when} · 会话 {session[:24]} ════════")
            for content in record.get("contents") or []:
                role = str(content.get("role") or "?")
                for part in content.get("parts") or []:
                    if not isinstance(part, dict):
                        continue
                    if isinstance(part.get("text"), str) and part["text"]:
                        out.append(f"[{role}]")
                        out.append(_indent(_clip(str(part["text"]), text_limit, full)))
                    elif "inline_data_bytes" in part:
                        out.append(f"[{role}] <图片 {part['inline_data_bytes']} 字节，仅计字节数>")
                    elif "tool_call" in part:
                        out.append(f"  🔧 {_clip(_dump(part['tool_call']), tool_limit, full)}")
                    elif "tool_result" in part:
                        out.append(f"  📋 {_clip(_dump(part['tool_result']), tool_limit, full)}")
            continue

        author = str(record.get("author") or "")
        final = bool(record.get("final"))
        errors = []
        if record.get("error_code"):
            errors.append(f"error_code={record['error_code']}")
        if record.get("run_error_type"):
            errors.append(f"run_error={record['run_error_type']}")
        suffix = f" · {'; '.join(errors)}" if errors else ""
        wrote_header = False
        for part in record.get("content") or []:
            if not isinstance(part, dict):
                continue
            if isinstance(part.get("text"), str) and part["text"]:
                if final:
                    label = "模型回复"
                elif author == "user":
                    label = "本轮输入（持久）"
                else:
                    label = author or "内容"
                out.append(f"── {label} {when}{suffix} ──")
                wrote_header = True
                suffix = ""
                out.append(_indent(_clip(str(part["text"]), text_limit, full)))
            elif "tool_call" in part:
                out.append(f"  🔧 调用 {_clip(_dump(part['tool_call']), tool_limit, full)}")
            elif "tool_result" in part:
                out.append(f"  📋 结果 {_clip(_dump(part['tool_result']), tool_limit, full)}")
        if not wrote_header and errors:
            out.append(f"── 事件 {when}{suffix} ──")
    return "\n".join(out)
