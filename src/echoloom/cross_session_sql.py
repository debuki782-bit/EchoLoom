"""Authorizer-guarded read-only SQLite query helper and cross-session time parser."""

from __future__ import annotations

import json
import logging
import re
import sqlite3
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

from .context import render_message_body

logger = logging.getLogger(__name__)

TABLE_COLUMNS: dict[str, frozenset[str]] = {
    "source_messages": frozenset(
        {
            "message_id",
            "account_id",
            "conversation_id",
            "sender_id",
            "platform_message_id",
            "captured_at",
            "body_text",
            "expires_at",
            "deleted_at",
            "event_time_context_json",
            "direction",
            "delivery_state",
        }
    ),
    "annotations": frozenset(
        {
            "note_id",
            "account_id",
            "message_id",
            "author_kind",
            "author_id",
            "trust_label",
            "content",
            "source",
            "version",
            "state",
            "created_at",
        }
    ),
    "entity_terms": frozenset(
        {
            "entity_id",
            "account_id",
            "term",
            "explanation",
            "author_id",
            "trust_label",
            "version",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "message_media": frozenset(
        {
            "image_id",
            "account_id",
            "conversation_id",
            "message_id",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "error_code",
            "vision_summary",
            "vision_model_id",
            "vision_inspected_at",
        }
    ),
    "message_audio": frozenset(
        {
            "audio_id",
            "account_id",
            "conversation_id",
            "message_id",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "duration_ms",
            "error_code",
            "transcript",
            "transcript_model_id",
            "transcript_state",
        }
    ),
    "message_files": frozenset(
        {
            "file_id",
            "account_id",
            "conversation_id",
            "message_id",
            "filename",
            "fileext",
            "expected_bytes",
            "expected_md5",
            "state",
            "mime_type",
            "byte_count",
            "sha256",
            "error_code",
            "summary",
            "summary_model_id",
            "summarized_at",
            "created_at",
            "updated_at",
        }
    ),
    "user_facts": frozenset(
        {
            "fact_id",
            "account_id",
            "fact_key",
            "content",
            "source_message_id",
            "author_id",
            "trust_label",
            "version",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "sender_memories": frozenset(
        {
            "memory_id",
            "account_id",
            "sender_id",
            "topic",
            "content",
            "trust_label",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "event_book": frozenset(
        {
            "event_id",
            "account_id",
            "priority",
            "content",
            "exit_condition",
            "expires_at",
            "state",
            "created_at",
            "updated_at",
        }
    ),
    "cross_session_tasks": frozenset(
        {
            "task_id",
            "account_id",
            "source_conversation_id",
            "target_conversation_id",
            "task_text",
            "state",
            "result_text",
            "created_at",
            "finished_at",
        }
    ),
    "short_refs": frozenset(
        {
            "account_id",
            "kind",
            "short_ref",
            "target_id",
            "created_at",
        }
    ),
}

ALLOWED_SQL_ACTIONS = frozenset(
    filter(
        None,
        (
            getattr(sqlite3, "SQLITE_SELECT", None),
            getattr(sqlite3, "SQLITE_READ", None),
            getattr(sqlite3, "SQLITE_FUNCTION", None),
            getattr(sqlite3, "SQLITE_RECURSIVE", None),
        ),
    )
)


def parse_query_time(value: str, *, end_of_day: bool = False) -> datetime:
    if not isinstance(value, str) or not value.strip() or len(value) > 40:
        raise ValueError("time must be an ISO date or date-time")
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError as exc:
        raise ValueError("time must be an ISO date or date-time") from exc
    if len(value.strip()) == 10 and end_of_day:
        parsed += timedelta(days=1, microseconds=-1)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone(timedelta(hours=8)))
    return parsed.astimezone(UTC)


def is_sql_scalar(value: object) -> bool:
    return (
        value is None
        or (isinstance(value, str) and len(value) <= 2000)
        or (isinstance(value, (int, float)) and not isinstance(value, bool))
    )


def execute_scoped_readonly_sql(
    db_path: Path,
    *,
    account_id: str,
    query: str,
    parameters: Sequence[object] = (),
    row_limit: int = 100,
    byte_limit: int = 16_000,
) -> dict[str, object]:
    """Execute a bounded SELECT over approved columns in an authorizer-protected connection."""
    if not isinstance(query, str) or len(query) > 4000:
        return {"status": "rejected", "reason": "query_length_limit"}
    statement = query.strip().rstrip(";").strip()
    if (
        not statement.casefold().startswith("select ")
        or ";" in statement
        or "--" in statement
        or "/*" in statement
        or "*/" in statement
    ):
        return {"status": "rejected", "reason": "select_only_single_statement"}
    if (
        re.search(
            r"\b(OR|UNION|INTERSECT|EXCEPT|JOIN|ATTACH|PRAGMA)\b",
            statement,
            re.IGNORECASE,
        )
        or len(re.findall(r"\bSELECT\b", statement, re.IGNORECASE)) != 1
    ):
        return {"status": "rejected", "reason": "single_table_scoped_select_only"}
    if not re.search(r"\baccount_id\s*=\s*\?", statement, re.IGNORECASE):
        return {"status": "rejected", "reason": "query_must_scope_current_account"}
    account_placeholder = re.search(r"\baccount_id\s*=\s*\?", statement, re.IGNORECASE)
    where_scope = re.search(
        r"\bWHERE\s+(?:\w+\.)?account_id\s*=\s*\?",
        statement,
        re.IGNORECASE,
    )
    if (
        account_placeholder is None
        or where_scope is None
        or statement[: where_scope.end()].count("?") != 1
    ):
        return {"status": "rejected", "reason": "account_scope_must_use_first_parameter"}
    if len(parameters) > 50 or any(not is_sql_scalar(value) for value in parameters):
        return {"status": "rejected", "reason": "invalid_parameter_list"}
    conn = None
    try:
        params = (account_id, *parameters)
        safe_path = db_path.as_posix()
        conn = sqlite3.connect(f"file:{safe_path}?mode=ro", uri=True, timeout=1)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")

        def authorize(action, arg1, arg2, _database, _source):
            if action not in ALLOWED_SQL_ACTIONS:
                return sqlite3.SQLITE_DENY
            if action == sqlite3.SQLITE_READ:
                allowed = TABLE_COLUMNS.get(str(arg1))
                if allowed is None or (arg2 and arg2 not in allowed):
                    return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        conn.set_authorizer(authorize)
        opcodes = 0

        def progress() -> int:
            nonlocal opcodes
            opcodes += 100
            return int(opcodes > 50_000)

        conn.set_progress_handler(progress, 100)
        rows = conn.execute(statement, params).fetchmany(row_limit + 1)
        values = [dict(row) for row in rows[:row_limit]]
        for row in values:
            for name in ("body_text", "last_message_preview"):
                body = row.get(name)
                if isinstance(body, str):
                    row[name] = render_message_body(body)
        encoded = json.dumps(values, ensure_ascii=False, default=str)
        while values and len(encoded.encode("utf-8")) > byte_limit:
            values.pop()
            encoded = json.dumps(values, ensure_ascii=False, default=str)
        return {
            "status": "completed",
            "rows": values,
            "truncated": len(rows) > len(values),
        }
    except Exception as exc:  # noqa: BLE001
        logger.debug("event=cross_session_sql_rejected error_type=%s", type(exc).__name__)
        return {"status": "rejected", "reason": type(exc).__name__}
    finally:
        if conn is not None:
            conn.close()
