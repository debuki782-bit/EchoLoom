"""Durable, bounded scheduler shared by reminders, followups and idle work."""

from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4


@dataclass(frozen=True, repr=False)
class TimeJob:
    job_id: str
    account_id: str
    conversation_id: str | None
    kind: str
    due_at: datetime
    payload: dict[str, object]

    def __repr__(self) -> str:
        return f"TimeJob(kind={self.kind!r}, job_id={self.job_id[:10]!r})"


class TimeScheduler:
    """SQLite-backed one-shot jobs with restart recovery and bounded idle history."""

    MAX_PAYLOAD_BYTES = 4096
    MAX_TERMINAL_JOBS_PER_KIND = 500

    def __init__(self, db: sqlite3.Connection, *, lock=None, idle_model="gemini-3.8-flash"):
        self.idle_model = idle_model
        self.db = db
        self._lock = lock or threading.RLock()
        self.db.row_factory = sqlite3.Row
        with self._lock:
            self.db.executescript(
                "CREATE TABLE IF NOT EXISTS time_jobs ("
                "job_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,conversation_id TEXT,"
                "kind TEXT NOT NULL,due_at TEXT NOT NULL,payload_json TEXT NOT NULL,"
                "state TEXT NOT NULL CHECK(state IN "
                "('scheduled','running','completed','cancelled','failed')),"
                "created_at TEXT NOT NULL,started_at TEXT,finished_at TEXT,last_error TEXT);"
                "CREATE INDEX IF NOT EXISTS time_jobs_due "
                "ON time_jobs(state,due_at,kind);"
                "CREATE INDEX IF NOT EXISTS time_jobs_scope "
                "ON time_jobs(account_id,conversation_id,kind,state);"
                "CREATE TABLE IF NOT EXISTS account_activity ("
                "account_id TEXT PRIMARY KEY,last_inbound_at TEXT NOT NULL,"
                "consecutive_idle_events INTEGER NOT NULL DEFAULT 0);"
                "CREATE TABLE IF NOT EXISTS idle_activity_summaries ("
                "activity_id TEXT PRIMARY KEY,event_id TEXT NOT NULL UNIQUE,account_id TEXT NOT NULL,"
                "event_number INTEGER NOT NULL,"
                "model_id TEXT NOT NULL,summary TEXT NOT NULL,started_at TEXT NOT NULL,"
                "finished_at TEXT NOT NULL,outcome TEXT NOT NULL);"
                "CREATE TABLE IF NOT EXISTS idle_event_runs ("
                "event_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,event_number INTEGER NOT NULL,"
                "model_id TEXT NOT NULL,started_at TEXT NOT NULL,state TEXT NOT NULL);"
                "CREATE INDEX IF NOT EXISTS idle_activity_recent "
                "ON idle_activity_summaries(account_id,finished_at DESC);"
            )
            columns = {row[1] for row in self.db.execute("PRAGMA table_info(idle_activity_summaries)")}
            if "tool_calls_json" not in columns:
                self.db.execute(
                    "ALTER TABLE idle_activity_summaries ADD COLUMN tool_calls_json TEXT NOT NULL DEFAULT '[]'"
                )
            self.db.execute(
                "UPDATE time_jobs SET state='scheduled',started_at=NULL WHERE state='running'"
            )
            interrupted = self.db.execute(
                "SELECT event_id,account_id,event_number,model_id,started_at FROM idle_event_runs "
                "WHERE state='running'"
            ).fetchall()
            for row in interrupted:
                now = self._now().isoformat()
                self.db.execute(
                    "UPDATE idle_event_runs SET state='interrupted' WHERE event_id=?",
                    (row["event_id"],),
                )
                self.db.execute(
                    "INSERT OR IGNORE INTO idle_activity_summaries "
                    "(activity_id,event_id,account_id,event_number,model_id,summary,started_at,finished_at,outcome) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        uuid4().hex,
                        row["event_id"],
                        row["account_id"],
                        row["event_number"],
                        row["model_id"],
                        "服务重启中断了空闲活动；系统已记录并结束该任务。",
                        row["started_at"],
                        now,
                        "interrupted",
                    ),
                )
            self.db.commit()
        self.prune()

    @staticmethod
    def _now(value: datetime | None = None) -> datetime:
        result = value or datetime.now(UTC)
        if result.tzinfo is None:
            raise ValueError("scheduler timestamps must be timezone-aware")
        return result.astimezone(UTC)

    def schedule(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        kind: str,
        due_at: datetime,
        payload: dict[str, object] | None = None,
        replace_existing: bool = False,
    ) -> str:
        if not account_id or not kind or len(kind) > 64:
            raise ValueError("invalid scheduler identity")
        due = self._now(due_at)
        encoded = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        if len(encoded.encode("utf-8")) > self.MAX_PAYLOAD_BYTES:
            raise ValueError("scheduler payload exceeds limit")
        now = self._now().isoformat()
        job_id = uuid4().hex
        with self._lock, self.db:
            if replace_existing:
                self.db.execute(
                    "UPDATE time_jobs SET state='cancelled',finished_at=? "
                    "WHERE account_id=? AND conversation_id IS ? AND kind=? "
                    "AND state='scheduled'",
                    (now, account_id, conversation_id, kind),
                )
            self.db.execute(
                "INSERT INTO time_jobs(job_id,account_id,conversation_id,kind,due_at,"
                "payload_json,state,created_at) VALUES (?,?,?,?,?,?,'scheduled',?)",
                (job_id, account_id, conversation_id, kind, due.isoformat(), encoded, now),
            )
        return job_id

    def cancel(
        self,
        *,
        job_id: str | None = None,
        account_id: str | None = None,
        conversation_id: str | None = None,
        kind: str | None = None,
    ) -> int:
        if job_id is None and (account_id is None or kind is None):
            raise ValueError("cancel requires a job id or account and kind")
        clauses = ["state='scheduled'"]
        params: list[object] = [self._now().isoformat()]
        if job_id is not None:
            clauses.append("job_id=?")
            params.append(job_id)
        else:
            clauses.extend(("account_id=?", "kind=?"))
            params.extend((account_id, kind))
            if conversation_id is not None:
                clauses.append("conversation_id=?")
                params.append(conversation_id)
        with self._lock, self.db:
            cursor = self.db.execute(
                "UPDATE time_jobs SET state='cancelled',finished_at=? WHERE "
                + " AND ".join(clauses),
                params,
            )
        return int(cursor.rowcount)

    def claim_due(self, *, now: datetime | None = None, kind: str | None = None) -> TimeJob | None:
        current = self._now(now)
        with self._lock, self.db:
            if kind is not None:
                row = self.db.execute(
                    "SELECT * FROM time_jobs WHERE state='scheduled' AND due_at<=? "
                    "AND kind=? ORDER BY due_at,created_at,job_id LIMIT 1",
                    (current.isoformat(), kind),
                ).fetchone()
            else:
                row = self.db.execute(
                    "SELECT * FROM time_jobs WHERE state='scheduled' AND due_at<=? "
                    "ORDER BY due_at,created_at,job_id LIMIT 1",
                    (current.isoformat(),),
                ).fetchone()
            if row is None:
                return None
            cursor = self.db.execute(
                "UPDATE time_jobs SET state='running',started_at=? "
                "WHERE job_id=? AND state='scheduled'",
                (current.isoformat(), row["job_id"]),
            )
            if cursor.rowcount != 1:
                return None
        return TimeJob(
            job_id=str(row["job_id"]),
            account_id=str(row["account_id"]),
            conversation_id=row["conversation_id"],
            kind=str(row["kind"]),
            due_at=datetime.fromisoformat(row["due_at"]),
            payload=json.loads(row["payload_json"]),
        )

    def claim_job(self, job_id: str, *, now: datetime | None = None) -> bool:
        current = self._now(now)
        with self._lock, self.db:
            cursor = self.db.execute(
                "UPDATE time_jobs SET state='running',started_at=? WHERE job_id=? "
                "AND state='scheduled' AND due_at<=?",
                (current.isoformat(), job_id, current.isoformat()),
            )
        return cursor.rowcount == 1

    def finish(self, job_id: str, *, error: str | None = None) -> bool:
        status = "failed" if error else "completed"
        with self._lock, self.db:
            cursor = self.db.execute(
                "UPDATE time_jobs SET state=?,finished_at=?,last_error=? "
                "WHERE job_id=? AND state='running'",
                (
                    status,
                    self._now().isoformat(),
                    " ".join(str(error).split())[:120] if error else None,
                    job_id,
                ),
            )
        return cursor.rowcount == 1

    def defer(self, job_id: str, due_at: datetime, reason: str) -> bool:
        due = self._now(due_at)
        with self._lock, self.db:
            cursor = self.db.execute(
                "UPDATE time_jobs SET state='scheduled',due_at=?,started_at=NULL,last_error=? "
                "WHERE job_id=? AND state='running'",
                (due.isoformat(), " ".join(reason.split())[:120], job_id),
            )
        return cursor.rowcount == 1

    def record_inbound(
        self, account_id: str, conversation_id: str, *, at: datetime | None = None
    ) -> None:
        moment = self._now(at)
        now = moment.isoformat()
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO account_activity(account_id,last_inbound_at,"
                "consecutive_idle_events) VALUES (?, ?,0) "
                "ON CONFLICT(account_id) DO UPDATE SET last_inbound_at=excluded.last_inbound_at,"
                "consecutive_idle_events=0",
                (account_id, now),
            )
            self.db.execute(
                "UPDATE time_jobs SET state='cancelled',finished_at=? "
                "WHERE account_id=? AND state IN ('scheduled','running') AND kind IN "
                "('idle_event','session_maintenance')",
                (now, account_id),
            )
            self.db.execute(
                "UPDATE time_jobs SET state='cancelled',finished_at=? "
                "WHERE account_id=? AND conversation_id=? AND state IN ('scheduled','running') "
                "AND kind='followup'",
                (now, account_id, conversation_id),
            )
        self.schedule(
            account_id=account_id,
            conversation_id=conversation_id,
            kind="session_maintenance",
            due_at=moment + timedelta(hours=8),
            payload={"idle_hours": 8},
            replace_existing=True,
        )

    def account_activity_state(self, account_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM account_activity WHERE account_id=?",
                (account_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def record_idle_event(
        self, account_id: str, *, at: datetime | None = None
    ) -> tuple[str, int, str]:
        now = self._now(at)
        event_id = "idle_" + uuid4().hex[:24]
        with self._lock, self.db:
            self.db.execute(
                "INSERT INTO account_activity(account_id,last_inbound_at,"
                "consecutive_idle_events) VALUES (?, ?,1) "
                "ON CONFLICT(account_id) DO UPDATE SET "
                "consecutive_idle_events=consecutive_idle_events+1",
                (account_id, now.isoformat()),
            )
            state = self.account_activity_state(account_id)
            raw_num = state.get("consecutive_idle_events") if state is not None else 0
            event_number = (
                raw_num
                if isinstance(raw_num, int) and not isinstance(raw_num, bool)
                else int(str(raw_num or 0))
            )
            model_id = self.idle_model
            self.db.execute(
                "INSERT INTO idle_event_runs VALUES (?,?,?,?,?,'running')",
                (event_id, account_id, event_number, model_id, now.isoformat()),
            )
        return event_id, event_number, model_id

    def record_idle_activity(
        self,
        *,
        account_id: str,
        event_id: str,
        event_number: int,
        model_id: str,
        summary: str,
        started_at: datetime,
        finished_at: datetime,
        outcome: str = "completed",
        tool_calls: list[dict] | None = None,
    ) -> str:
        text = " ".join(summary.split())
        if (
            not account_id
            or not event_id
            or not 1 <= event_number
            or not model_id
            or not text
            or len(text) > 2000
            or outcome not in ("completed", "interrupted", "timeout", "failed")
        ):
            raise ValueError("invalid idle activity record")
        start, finish = self._now(started_at), self._now(finished_at)
        calls_json = json.dumps(tool_calls or [], ensure_ascii=False, default=str)
        activity_id = uuid4().hex
        with self._lock, self.db:
            existing = self.db.execute(
                "SELECT activity_id FROM idle_activity_summaries WHERE event_id=? AND account_id=?",
                (event_id, account_id),
            ).fetchone()
            if existing is not None:
                self.db.execute(
                    "UPDATE idle_activity_summaries SET summary=?,finished_at=?,outcome=?,tool_calls_json=? "
                    "WHERE event_id=? AND account_id=?",
                    (text, finish.isoformat(), outcome, calls_json, event_id, account_id),
                )
                self.db.execute(
                    "UPDATE idle_event_runs SET state=? WHERE event_id=? AND account_id=?",
                    (outcome, event_id, account_id),
                )
                return str(existing[0])
            self.db.execute(
                "INSERT INTO idle_activity_summaries "
                "(activity_id,event_id,account_id,event_number,model_id,summary,started_at,finished_at,outcome,tool_calls_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    activity_id,
                    event_id,
                    account_id,
                    event_number,
                    model_id,
                    text,
                    start.isoformat(),
                    finish.isoformat(),
                    outcome,
                    calls_json,
                ),
            )
            self.db.execute(
                "UPDATE idle_event_runs SET state=? WHERE event_id=? AND account_id=?",
                (outcome, event_id, account_id),
            )
        return activity_id

    def recent_idle_activities(self, account_id: str, limit: int = 20) -> list[str]:
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        with self._lock:
            rows = self.db.execute(
                "SELECT summary,tool_calls_json FROM idle_activity_summaries WHERE account_id=? "
                "AND outcome<>'failed' "
                "ORDER BY finished_at DESC,activity_id DESC LIMIT ?",
                (account_id, limit),
            ).fetchall()
        return [str(row[0]) + ("；成功工具调用：" + str(row[1]) if row[1] != "[]" else "")
                for row in rows]

    def last_completed_idle_at(self, account_id: str) -> str | None:
        """Return the completion timestamp of the latest completed idle event, if any."""
        with self._lock:
            row = self.db.execute(
                "SELECT finished_at FROM idle_activity_summaries WHERE account_id=? "
                "AND outcome='completed' ORDER BY finished_at DESC,activity_id DESC LIMIT 1",
                (account_id,),
            ).fetchone()
        return str(row[0]) if row is not None and row[0] else None

    def has_idle_activity(self, account_id: str, event_id: str) -> bool:
        with self._lock:
            row = self.db.execute(
                "SELECT 1 FROM idle_activity_summaries WHERE account_id=? AND event_id=?",
                (account_id, event_id),
            ).fetchone()
        return row is not None

    def update_idle_tool_calls(self, account_id: str, event_id: str, tool_calls: list[dict]) -> None:
        """Attach completed calls to an existing activity without changing its outcome."""
        with self._lock, self.db:
            self.db.execute(
                "UPDATE idle_activity_summaries SET tool_calls_json=? WHERE account_id=? AND event_id=?",
                (json.dumps(tool_calls, ensure_ascii=False, default=str), account_id, event_id),
            )

    def recent_idle_event_times(self, account_id: str, since: datetime) -> list[datetime]:
        with self._lock:
            rows = self.db.execute(
                "SELECT started_at FROM idle_event_runs WHERE account_id=? "
                "AND started_at>=? "
                "ORDER BY started_at DESC LIMIT 10",
                (account_id, self._now(since).isoformat()),
            ).fetchall()
        return [datetime.fromisoformat(row[0]) for row in rows]

    def idle_events_since(self, account_id: str, since: datetime) -> int:
        with self._lock:
            row = self.db.execute(
                "SELECT count(*) FROM idle_event_runs WHERE account_id=? AND started_at>=?",
                (account_id, self._now(since).isoformat()),
            ).fetchone()
        return int(row[0])

    def next_idle_job(self, account_id: str) -> TimeJob | None:
        with self._lock:
            row = self.db.execute(
                "SELECT * FROM time_jobs WHERE account_id=? AND kind='idle_event' "
                "AND state='scheduled' ORDER BY due_at LIMIT 1",
                (account_id,),
            ).fetchone()
        if row is None:
            return None
        return TimeJob(
            job_id=str(row["job_id"]),
            account_id=str(row["account_id"]),
            conversation_id=row["conversation_id"],
            kind=str(row["kind"]),
            due_at=datetime.fromisoformat(row["due_at"]),
            payload=json.loads(row["payload_json"]),
        )

    def has_scheduled_job(self, account_id: str, kind: str) -> bool:
        with self._lock:
            row = self.db.execute(
                "SELECT 1 FROM time_jobs WHERE account_id=? AND kind=? "
                "AND state='scheduled' LIMIT 1",
                (account_id, kind),
            ).fetchone()
        return row is not None

    def scheduled_jobs(self, account_id: str, kind: str) -> list[TimeJob]:
        with self._lock:
            rows = self.db.execute(
                "SELECT * FROM time_jobs WHERE account_id=? AND kind=? AND state='scheduled' "
                "ORDER BY due_at,created_at,job_id",
                (account_id, kind),
            ).fetchall()
        return [
            TimeJob(
                job_id=str(row["job_id"]),
                account_id=str(row["account_id"]),
                conversation_id=row["conversation_id"],
                kind=str(row["kind"]),
                due_at=datetime.fromisoformat(row["due_at"]),
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    def prune(
        self,
        *,
        before: datetime | None = None,
        max_terminal_per_kind: int | None = MAX_TERMINAL_JOBS_PER_KIND,
    ) -> int:
        """Delete aged terminal jobs and cap retained terminal rows per job kind."""
        cutoff = self._now(before or (datetime.now(UTC) - timedelta(days=7)))
        deleted = 0
        with self._lock, self.db:
            cursor = self.db.execute(
                "DELETE FROM time_jobs WHERE state IN ('completed','cancelled','failed') "
                "AND coalesce(finished_at, created_at)<?",
                (cutoff.isoformat(),),
            )
            deleted += int(cursor.rowcount)
            if max_terminal_per_kind is not None and max_terminal_per_kind >= 0:
                kinds = [
                    str(row[0])
                    for row in self.db.execute(
                        "SELECT DISTINCT kind FROM time_jobs "
                        "WHERE state IN ('completed','cancelled','failed')"
                    ).fetchall()
                ]
                for kind in kinds:
                    c2 = self.db.execute(
                        "DELETE FROM time_jobs WHERE job_id IN ("
                        "SELECT job_id FROM time_jobs "
                        "WHERE kind=? AND state IN ('completed','cancelled','failed') "
                        "ORDER BY coalesce(finished_at, created_at) DESC, job_id DESC "
                        "LIMIT -1 OFFSET ?"
                        ")",
                        (kind, max_terminal_per_kind),
                    )
                    deleted += int(c2.rowcount)
        return deleted
