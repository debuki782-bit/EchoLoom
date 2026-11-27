"""Account-wide, bounded event book dynamically injected into every session."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from uuid import uuid4

from .memory_store import MemoryStore


class EventBook:
    HIGH_PRIORITY_SLOTS = 4
    HIGH_PRIORITY_CHARS = 200
    ORDINARY_TOTAL_CHARS = 20_000
    ORDINARY_ENTRY_CHARS = 1_000
    EXIT_CONDITION_CHARS = 500

    def __init__(self, store: MemoryStore):
        self.store = store
        self.db = store.db
        self._lock = store._lock
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS event_book ("
            "event_id TEXT PRIMARY KEY,account_id TEXT NOT NULL,priority TEXT NOT NULL "
            "CHECK(priority IN ('high','ordinary')),content TEXT NOT NULL,"
            "exit_condition TEXT NOT NULL,expires_at TEXT,state TEXT NOT NULL "
            "CHECK(state IN ('active','deleted','expired')),created_at TEXT NOT NULL,"
            "updated_at TEXT NOT NULL);"
            "CREATE INDEX IF NOT EXISTS event_book_active "
            "ON event_book(account_id,state,priority,updated_at);"
            "CREATE TABLE IF NOT EXISTS event_book_revisions ("
            "revision_id TEXT PRIMARY KEY,event_id TEXT NOT NULL,account_id TEXT NOT NULL,"
            "action TEXT NOT NULL,priority TEXT NOT NULL,content TEXT NOT NULL,"
            "exit_condition TEXT NOT NULL,expires_at TEXT,created_at TEXT NOT NULL);"
        )
        self.store.short_refs.backfill_event_book()

    def _record_revision(self, account_id: str, event_id: str, action: str,
                         priority: str, content: str, exit_condition: str,
                         expires_at: str | None, at: str) -> None:
        self.db.execute(
            "INSERT INTO event_book_revisions VALUES (?,?,?,?,?,?,?,?,?)",
            (uuid4().hex, event_id, account_id, action, priority, content,
             exit_condition, expires_at, at),
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(UTC).isoformat()

    def _expire_due(self, account_id: str) -> None:
        now = self._now()
        with self.store.transaction():
            rows = self.db.execute(
                "SELECT event_id,priority,content,exit_condition,expires_at FROM event_book "
                "WHERE account_id=? AND state='active' AND expires_at IS NOT NULL AND expires_at<=?",
                (account_id, now),
            ).fetchall()
            self.db.execute(
                "UPDATE event_book SET state='expired',updated_at=? WHERE account_id=? "
                "AND state='active' AND expires_at IS NOT NULL AND expires_at<=?",
                (now, account_id, now),
            )
            for row in rows:
                self._record_revision(account_id, row["event_id"], "expire", row["priority"],
                                      row["content"], row["exit_condition"], row["expires_at"], now)

    def list_entries(self, account_id: str) -> list[dict[str, object]]:
        self._expire_due(account_id)
        rows = self.db.execute(
            "SELECT event_id,priority,content,exit_condition,expires_at,created_at,updated_at "
            "FROM event_book WHERE account_id=? AND state='active' "
            "ORDER BY CASE priority WHEN 'high' THEN 0 ELSE 1 END,updated_at DESC,event_id",
            (account_id,),
        ).fetchall()
        entries = [dict(row) for row in rows]
        if entries:
            refs = self.store.short_refs_map(
                account_id=account_id,
                items=[("event", str(entry["event_id"])) for entry in entries],
            )
            for entry in entries:
                entry["short_ref"] = refs.get(("event", str(entry["event_id"])), "")
        return entries

    def render(self, account_id: str) -> str:
        entries = self.list_entries(account_id)
        now = datetime.now(UTC)
        high = [row for row in entries if row["priority"] == "high"]
        ordinary = [row for row in entries if row["priority"] == "ordinary"]
        lines = ["【账号级事件簿｜跨全部会话动态快照】"]
        lines.append("高优事件（最多 4 条，每条最多 200 字）：")
        lines.extend(
            f"- [{row.get('short_ref') or row['event_id']}] {row['content']}"
            f"（退出条件：{row['exit_condition']}）"
            for row in high
        )
        if not high:
            lines.append("- 当前为空。")
        lines.append("普通事件（只处理仍有效且未满足退出条件的事项）：")
        for row in ordinary:
            updated_age = self._elapsed_label(row["updated_at"], now)
            stale = self._elapsed_seconds(row["updated_at"], now) >= 72 * 60 * 60
            freshness = "已过时，先复核并更新或删除" if stale else "未过时"
            ref = row.get("short_ref") or row["event_id"]
            lines.append(
                f"- [{ref}] {row['content']}（生成时间：{updated_age}前；{freshness}；"
                f"退出条件：{row['exit_condition']}；到期：{row['expires_at'] or '未设定'}）"
            )
        if not ordinary:
            lines.append("- 当前为空。")
        lines.append(
            "本快照跨所有聊天共享，仅作当前参考。只有清楚说明值得跨会话记住的事实、"
            "承诺或时间安排才写入；普通事件必须具备明确退出条件，完成、失效或到期后要删除/更新。"
            "普通事件距最近修改满 72 小时即视为过时；过时内容先复核，仍有效则更新，不再有效则删除。"
        )
        return "\n".join(lines)

    @staticmethod
    def _elapsed_seconds(value: object, now: datetime) -> float:
        try:
            timestamp = datetime.fromisoformat(str(value))
        except (TypeError, ValueError):
            return 0.0
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        return max(0.0, (now - timestamp.astimezone(UTC)).total_seconds())

    @classmethod
    def _elapsed_label(cls, value: object, now: datetime) -> str:
        seconds = int(cls._elapsed_seconds(value, now))
        minutes = seconds // 60
        days, remainder = divmod(minutes, 24 * 60)
        hours, remainder_minutes = divmod(remainder, 60)
        return f"{days}天{hours}小时{remainder_minutes}分"

    def add(
        self,
        *,
        account_id: str,
        priority: str,
        content: str,
        exit_condition: str,
        expires_at: str | None = None,
    ) -> dict[str, object]:
        content = self._validate_text(
            content, 200 if priority == "high" else self.ORDINARY_ENTRY_CHARS
        )
        exit_condition = self._validate_text(exit_condition, self.EXIT_CONDITION_CHARS)
        if priority not in ("high", "ordinary"):
            return {"status": "rejected", "reason": "priority_must_be_high_or_ordinary"}
        expires = self._validate_expiry(expires_at)
        self._expire_due(account_id)
        with self.store.transaction():
            if priority == "high":
                count = self.db.execute(
                    "SELECT count(*) FROM event_book WHERE account_id=? "
                    "AND priority='high' AND state='active'",
                    (account_id,),
                ).fetchone()[0]
                if count >= self.HIGH_PRIORITY_SLOTS:
                    return {
                        "status": "full",
                        "reason": "high_priority_slots_full",
                        "capacity": self.HIGH_PRIORITY_SLOTS,
                        "hint": "先修订或删除一条现有高优事件，再决定是否替换。",
                    }
            else:
                used = self.db.execute(
                    "SELECT coalesce(sum(length(content)),0) FROM event_book "
                    "WHERE account_id=? AND priority='ordinary' AND state='active'",
                    (account_id,),
                ).fetchone()[0]
                if used + len(content) > self.ORDINARY_TOTAL_CHARS:
                    return {
                        "status": "full",
                        "reason": "ordinary_character_budget_full",
                        "used_chars": int(used),
                        "capacity_chars": self.ORDINARY_TOTAL_CHARS,
                    }
            event_id = "evt_" + uuid4().hex[:24]
            now = self._now()
            self.db.execute(
                "INSERT INTO event_book VALUES (?,?,?,?,?,?,'active',?,?)",
                (event_id, account_id, priority, content, exit_condition, expires, now, now),
            )
            self._record_revision(account_id, event_id, "add", priority, content,
                                  exit_condition, expires, now)
            short_ref = self.store.ensure_short_ref(
                account_id=account_id, kind="event", target_id=event_id
            )
        return {
            "status": "saved",
            "event_id": event_id,
            "short_ref": short_ref,
            "priority": priority,
        }

    def update(
        self,
        *,
        account_id: str,
        event_id: str,
        content: str | None = None,
        exit_condition: str | None = None,
        expires_at: str | None = None,
    ) -> dict[str, object]:
        resolved_id = self.store.resolve_short_ref(
            account_id=account_id, value=event_id, kinds=("event",)
        )
        if not resolved_id:
            return {"status": "unavailable"}
        current = self._owned(account_id, resolved_id)
        if current is None:
            return {"status": "unavailable"}
        next_content = (
            self._validate_text(
                content,
                self.HIGH_PRIORITY_CHARS
                if current["priority"] == "high"
                else self.ORDINARY_ENTRY_CHARS,
            )
            if content is not None
            else str(current["content"])
        )
        next_exit = (
            self._validate_text(exit_condition, self.EXIT_CONDITION_CHARS)
            if exit_condition is not None
            else str(current["exit_condition"])
        )
        next_expiry = (
            None
            if expires_at == "clear"
            else self._validate_expiry(expires_at)
            if expires_at is not None
            else current["expires_at"]
        )
        if current["priority"] == "ordinary":
            used = self.db.execute(
                "SELECT coalesce(sum(length(content)),0) FROM event_book WHERE account_id=? "
                "AND priority='ordinary' AND state='active' AND event_id<>?",
                (account_id, resolved_id),
            ).fetchone()[0]
            if used + len(next_content) > self.ORDINARY_TOTAL_CHARS:
                return {"status": "full", "reason": "ordinary_character_budget_full"}
        with self.store.transaction():
            self.db.execute(
                "UPDATE event_book SET content=?,exit_condition=?,expires_at=?,updated_at=? "
                "WHERE event_id=? AND account_id=? AND state='active'",
                (next_content, next_exit, next_expiry, self._now(), resolved_id, account_id),
            )
            self._record_revision(account_id, resolved_id, "update", current["priority"],
                                  next_content, next_exit, next_expiry, self._now())
        short_ref = self.store.short_refs_map(
            account_id=account_id, items=[("event", resolved_id)]
        ).get(("event", resolved_id), "")
        result: dict[str, object] = {"status": "updated", "event_id": resolved_id}
        if short_ref:
            result["short_ref"] = short_ref
        return result

    def delete(self, *, account_id: str, event_id: str) -> dict[str, object]:
        resolved_id = self.store.resolve_short_ref(
            account_id=account_id, value=event_id, kinds=("event",)
        )
        if not resolved_id:
            return {"status": "unavailable"}
        with self.store.transaction():
            row = self.db.execute(
                "SELECT priority,content,exit_condition,expires_at FROM event_book "
                "WHERE event_id=? AND account_id=? AND state='active'",
                (resolved_id, account_id),
            ).fetchone()
            now = self._now()
            cursor = self.db.execute(
                "UPDATE event_book SET state='deleted',updated_at=? WHERE event_id=? "
                "AND account_id=? AND state='active'",
                (now, resolved_id, account_id),
            )
            if cursor.rowcount and row is not None:
                self._record_revision(account_id, resolved_id, "delete", row["priority"],
                                      row["content"], row["exit_condition"], row["expires_at"], now)
        return {"status": "deleted" if cursor.rowcount else "unavailable"}

    def _owned(self, account_id: str, event_id: str) -> sqlite3.Row | None:
        self._expire_due(account_id)
        return self.db.execute(
            "SELECT * FROM event_book WHERE event_id=? AND account_id=? AND state='active'",
            (event_id, account_id),
        ).fetchone()

    @staticmethod
    def _validate_text(value: str, limit: int) -> str:
        if not isinstance(value, str):
            raise TypeError("event content must be text")
        text = " ".join(value.split())
        if not text or len(text) > limit or "\0" in text:
            raise ValueError(f"event text is empty or exceeds {limit} chars")
        return text

    @staticmethod
    def _validate_expiry(value: str | None) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str):
            raise TypeError("expiry must be an ISO timestamp")
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            raise ValueError("expiry must be an ISO timestamp") from None
        if parsed.tzinfo is None:
            raise ValueError("expiry must include a timezone")
        return parsed.astimezone(UTC).isoformat()
