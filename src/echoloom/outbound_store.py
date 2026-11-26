"""SQLite persistence store for outbound receipts, retries, and failure notices."""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from .db_async import register_thread_safe_db, unregister_thread_safe_db

logger = logging.getLogger(__name__)


class OutboundReceiptStore:
    """Owns ``tool-receipts.sqlite3`` schema, claim deduplication, and notice ledger."""

    MAX_SEND_ATTEMPTS = 4

    def __init__(
        self,
        db_path: Path | str,
        *,
        account: str,
        default_room: str,
        clock: object = None,
    ) -> None:
        raw_path = str(db_path)
        if raw_path != ":memory:":
            Path(raw_path).parent.mkdir(parents=True, exist_ok=True)
        self.db_path = Path(raw_path) if raw_path != ":memory:" else None
        self.account = account
        self.default_room = default_room
        self.clock = clock
        self.db = sqlite3.connect(raw_path, check_same_thread=False)
        self._lock = register_thread_safe_db(self.db, owner=self)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        self.db.execute("CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY,state TEXT)")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS send_side_effect_scopes (scope TEXT PRIMARY KEY)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS delivery_failure_scopes ("
            "scope TEXT PRIMARY KEY,status TEXT NOT NULL,recorded_at TEXT NOT NULL,"
            "failure_count INTEGER NOT NULL DEFAULT 0,reason TEXT,"
            "send_attempt_count INTEGER NOT NULL DEFAULT 0,last_send_status TEXT)"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS delivery_failure_notices ("
            "scope TEXT PRIMARY KEY,account_id TEXT NOT NULL,conversation_id TEXT NOT NULL,"
            "status TEXT NOT NULL,reason TEXT,"
            "failure_count INTEGER NOT NULL,created_at TEXT NOT NULL,acknowledged_at TEXT)"
        )
        notice_columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(delivery_failure_notices)")
        }
        if "account_id" not in notice_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_notices ADD COLUMN account_id "
                "TEXT NOT NULL DEFAULT ''"
            )
        if "conversation_id" not in notice_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_notices ADD COLUMN conversation_id "
                "TEXT NOT NULL DEFAULT ''"
            )
        failure_columns = {
            row[1] for row in self.db.execute("PRAGMA table_info(delivery_failure_scopes)")
        }
        if "failure_count" not in failure_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_scopes ADD COLUMN failure_count "
                "INTEGER NOT NULL DEFAULT 0"
            )
            self.db.execute(
                "UPDATE delivery_failure_scopes SET failure_count=1 WHERE status IS NOT NULL"
            )
        if "reason" not in failure_columns:
            self.db.execute("ALTER TABLE delivery_failure_scopes ADD COLUMN reason TEXT")
        if "send_attempt_count" not in failure_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_scopes ADD COLUMN send_attempt_count "
                "INTEGER NOT NULL DEFAULT 0"
            )
            self.db.execute(
                "UPDATE delivery_failure_scopes SET send_attempt_count=failure_count"
            )
        if "last_send_status" not in failure_columns:
            self.db.execute(
                "ALTER TABLE delivery_failure_scopes ADD COLUMN last_send_status TEXT"
            )
        self.db.commit()

    def close(self) -> None:
        with self._lock:
            unregister_thread_safe_db(self.db)
            self.db.close()

    def _now(self) -> str:
        if self.clock is not None:
            return str(self.clock.now())  # type: ignore[attr-defined]
        return datetime.now(UTC).isoformat()

    def update_operation_state(self, receipt: str, state: str) -> None:
        with self._lock:
            self.db.execute("UPDATE operations SET state=? WHERE id=?", (state, receipt))
            self.db.commit()

    def record_side_effect_scope(
        self,
        scope: str,
        *,
        active_operation_id: str,
        receipt_ids: tuple[str, ...],
    ) -> None:
        if not scope:
            return
        scopes = tuple(
            dict.fromkeys((scope, *(receipt_ids if scope == active_operation_id else ())))
        )
        with self._lock, self.db:
            self.db.executemany(
                "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                ((item,) for item in scopes),
            )

    def send_failure_status(self, scope: str | None) -> str | None:
        if not scope:
            return None
        with self._lock:
            row = self.db.execute(
                "SELECT status,failure_count FROM delivery_failure_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
        return str(row[0]) if row and int(row[1]) > 0 else None

    def send_failure_count(self, scope: str | None) -> int:
        if not scope:
            return 0
        with self._lock:
            row = self.db.execute(
                "SELECT failure_count FROM delivery_failure_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
        return int(row[0]) if row else 0

    def send_failure_reason(self, scope: str | None) -> str | None:
        if not scope:
            return None
        with self._lock:
            row = self.db.execute(
                "SELECT reason,failure_count FROM delivery_failure_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
        return str(row[0]) if row and int(row[1]) > 0 and row[0] else None

    def send_attempt_count(self, scope: str | None) -> int:
        if not scope:
            return 0
        with self._lock:
            row = self.db.execute(
                "SELECT send_attempt_count FROM delivery_failure_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
        return int(row[0]) if row else 0

    def delivery_state(
        self,
        scope: str,
        *,
        active_operation_id: str,
        receipt_ids: tuple[str, ...],
    ) -> dict[str, object]:
        with self._lock:
            return self._delivery_state_locked(
                scope,
                active_operation_id=active_operation_id,
                receipt_ids=receipt_ids,
            )

    def _delivery_state_locked(
        self,
        scope: str,
        *,
        active_operation_id: str,
        receipt_ids: tuple[str, ...],
    ) -> dict[str, object]:
        row = self.db.execute(
            "SELECT failure_count,status,reason,send_attempt_count,last_send_status "
            "FROM delivery_failure_scopes WHERE scope=?",
            (scope,),
        ).fetchone()
        if scope == active_operation_id and row is None:
            inputs = [
                self._delivery_state_locked(
                    value,
                    active_operation_id=active_operation_id,
                    receipt_ids=receipt_ids,
                )
                for value in receipt_ids
                if value != scope
            ]
            if inputs and all(
                state.get("last_send_status") in ("queued", "accepted_by_server")
                for state in inputs
            ):
                return {**inputs[0], "replayed_input_send": True}
            counts = [state.get("send_attempt_count", 0) for state in inputs]
            if counts and all(
                isinstance(count, int) and count >= self.MAX_SEND_ATTEMPTS
                for count in counts
            ):
                return {**inputs[0], "replayed_input_send": True}
        attempted = (
            self.db.execute(
                "SELECT 1 FROM send_side_effect_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
            is not None
        )
        if row is None:
            return {
                "failure_count": 0,
                "last_failure_status": None,
                "last_failure_reason": None,
                "send_attempt_count": 0,
                "last_send_status": None,
                "side_effect_started": attempted,
            }
        failures = int(row[0])
        return {
            "failure_count": failures,
            "last_failure_status": row[1] if failures else None,
            "last_failure_reason": row[2] if failures else None,
            "send_attempt_count": int(row[3]),
            "last_send_status": row[4],
            "side_effect_started": attempted,
        }

    def record_send_result(
        self,
        scope: str,
        status: str,
        reason: str | None = None,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
        active_operation_id: str = "",
        receipt_ids: tuple[str, ...] = (),
        active_room: str = "",
    ) -> int:
        if not scope:
            return self.send_failure_count(scope)
        account = account_id or self.account
        conversation = conversation_id or active_room or self.default_room
        failed = status in ("rejected", "unknown")
        scopes = tuple(
            dict.fromkeys((scope, *(receipt_ids if scope == active_operation_id else ())))
        )
        with self._lock:
            now = self._now()
            self.db.executemany(
                "INSERT INTO delivery_failure_scopes "
                "(scope,status,recorded_at,failure_count,reason,send_attempt_count,last_send_status) "
                "VALUES (?,?,?, ?,?,?,?) ON CONFLICT(scope) DO UPDATE SET "
                "status=CASE WHEN excluded.failure_count>0 THEN excluded.status "
                "ELSE delivery_failure_scopes.status END,"
                "recorded_at=excluded.recorded_at,"
                "failure_count=delivery_failure_scopes.failure_count+excluded.failure_count,"
                "reason=CASE WHEN excluded.failure_count>0 THEN excluded.reason "
                "ELSE delivery_failure_scopes.reason END,"
                "send_attempt_count=delivery_failure_scopes.send_attempt_count+1,"
                "last_send_status=excluded.last_send_status",
                ((value, status, now, int(failed), reason, 1, status) for value in scopes),
            )
            row = self.db.execute(
                "SELECT failure_count FROM delivery_failure_scopes WHERE scope=?",
                (scope,),
            ).fetchone()
            failures = int(row[0]) if row else 0
            if failures >= self.MAX_SEND_ATTEMPTS:
                self.db.execute(
                    "INSERT OR IGNORE INTO delivery_failure_notices "
                    "(scope,account_id,conversation_id,status,reason,failure_count,created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (
                        scope,
                        account,
                        conversation,
                        status,
                        (reason or "未取得具体原因")[:300],
                        failures,
                        now,
                    ),
                )
            self.db.commit()
        logger.info(
            "event=tool_delivery_recorded operation=%s account_sha=%s conversation_sha=%s "
            "status=%s failure_count=%s reason=%s",
            scope[:12],
            hashlib.sha256(account.encode()).hexdigest()[:12],
            hashlib.sha256(conversation.encode()).hexdigest()[:12],
            status,
            failures,
            str(reason or "none")[:100],
        )
        return failures

    def queue_delivery_failure_notice(
        self,
        scope: str,
        status: str,
        reason: str | None,
        failure_count: int,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
        active_room: str = "",
    ) -> None:
        if not scope or status not in (
            "rejected",
            "unknown",
            "agent_retry_unknown",
            "retry_reply_empty",
        ):
            return
        account = account_id or self.account
        conversation = conversation_id or active_room or self.default_room
        with self._lock:
            self.db.execute(
                "INSERT OR IGNORE INTO delivery_failure_notices "
                "(scope,account_id,conversation_id,status,reason,failure_count,created_at) "
                "VALUES (?,?,?,?,?,?,?)",
                (
                    scope,
                    account,
                    conversation,
                    status,
                    (reason or "未取得具体原因")[:300],
                    failure_count,
                    self._now(),
                ),
            )
            self.db.commit()
        logger.warning(
            "event=tool_delivery_notice_queued operation=%s account_sha=%s "
            "conversation_sha=%s status=%s failures=%s",
            scope[:12],
            hashlib.sha256(account.encode()).hexdigest()[:12],
            hashlib.sha256(conversation.encode()).hexdigest()[:12],
            status,
            failure_count,
        )

    def pending_delivery_failure_notices(
        self,
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
        active_room: str = "",
    ) -> list[dict[str, object]]:
        account = account_id or self.account
        conversation = conversation_id or active_room or self.default_room
        with self._lock:
            rows = self.db.execute(
                "SELECT scope,status,reason,failure_count,created_at "
                "FROM delivery_failure_notices WHERE account_id=? AND conversation_id=? "
                "AND acknowledged_at IS NULL "
                "ORDER BY created_at,scope LIMIT 20",
                (account, conversation),
            ).fetchall()
        columns = ("scope", "status", "reason", "failure_count", "created_at")
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def acknowledge_delivery_failure_notices(
        self,
        scopes: tuple[str, ...],
        *,
        account_id: str | None = None,
        conversation_id: str | None = None,
        active_room: str = "",
    ) -> None:
        if not scopes:
            return
        account = account_id or self.account
        conversation = conversation_id or active_room or self.default_room
        placeholders = ",".join("?" for _ in scopes)
        with self._lock:
            self.db.execute(
                f"UPDATE delivery_failure_notices SET acknowledged_at=? "
                f"WHERE account_id=? AND conversation_id=? AND acknowledged_at IS NULL "
                f"AND scope IN ({placeholders})",
                (self._now(), account, conversation, *scopes),
            )
            self.db.commit()

    def claim(
        self,
        scope: str,
        action: str,
        value: str,
        *,
        can_send: Callable[[], bool],
        receipt_ids: tuple[str, ...],
    ) -> str | None:
        if not scope or not can_send():
            return None
        with self._lock:
            state = self._delivery_state_locked(
                scope,
                active_operation_id=scope,
                receipt_ids=receipt_ids,
            )
            failure_count = int(state.get("failure_count", 0) or 0)
            send_attempt_count = int(state.get("send_attempt_count", 0) or 0)
            if (
                send_attempt_count >= self.MAX_SEND_ATTEMPTS
                or state.get("last_send_status") in ("queued", "accepted_by_server")
            ):
                return None
            if (
                action in ("text", "quote", "image", "generate_and_send_image")
                and failure_count >= self.MAX_SEND_ATTEMPTS
            ):
                return None
            retry_marker = (
                failure_count
                if action in ("text", "quote", "image", "generate_and_send_image")
                else 0
            )
            receipt = hashlib.sha256(
                f"{scope}\0{action}\0{retry_marker}\0{value}".encode()
            ).hexdigest()
            with self.db:
                cursor = self.db.execute(
                    "INSERT OR IGNORE INTO operations VALUES (?,'attempted')",
                    (receipt,),
                )
                if cursor.rowcount and action in (
                    "text",
                    "quote",
                    "image",
                    "generate_and_send_image",
                ):
                    scopes = tuple(dict.fromkeys((scope, *receipt_ids)))
                    self.db.executemany(
                        "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                        ((item,) for item in scopes),
                    )
            return receipt if cursor.rowcount else None

    def claim_segment(
        self,
        scope: str,
        index: int,
        action: str,
        value: str,
        *,
        receipt_ids: tuple[str, ...],
    ) -> str | None:
        with self._lock:
            state = self._delivery_state_locked(
                scope,
                active_operation_id=scope,
                receipt_ids=receipt_ids,
            )
            failure_count = int(state.get("failure_count", 0) or 0)
            if failure_count >= self.MAX_SEND_ATTEMPTS:
                return None
            receipt = hashlib.sha256(
                f"{scope}\0seg:{index}:{action}\0{failure_count}\0{value}".encode()
            ).hexdigest()
            with self.db:
                cursor = self.db.execute(
                    "INSERT OR IGNORE INTO operations VALUES (?,'attempted')",
                    (receipt,),
                )
                if cursor.rowcount:
                    scopes = tuple(dict.fromkeys((scope, *receipt_ids)))
                    self.db.executemany(
                        "INSERT OR IGNORE INTO send_side_effect_scopes VALUES (?)",
                        ((item,) for item in scopes),
                    )
            return receipt if cursor.rowcount else None
