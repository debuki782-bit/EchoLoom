"""Durable per-conversation inbox, heartbeat, and batch scheduler."""

from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import sqlite3
import threading
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .db_async import db_lock
from .freshness_policy import PendingInboundSnapshot
from .usage import DAILY_BUDGET_TIMEZONE
from .wechat_gateway import MessageEvent

logger = logging.getLogger(__name__)


@dataclass(frozen=True, repr=False)
class MessageBatch:
    batch_id: str
    receipt_ids: tuple[str, ...]
    observations: tuple[MessageEvent, ...]

    def __repr__(self) -> str:
        return f"MessageBatch(size={len(self.receipt_ids)})"


class ConversationScheduler:
    """Persist Hook callbacks before ack; claim a quiet-window batch per chat."""

    MAX_BATCH_MESSAGES = 20
    MAX_BATCH_TEXT_BYTES = 24_000
    MAX_PENDING_MESSAGES = 2048
    MAX_PROCESS_ATTEMPTS = 8
    ALERT_ATTEMPTS_THRESHOLD = 8
    BUDGET_EXHAUSTED_ERROR = "model_budget_exhausted_no_call"

    def __init__(
        self,
        db: sqlite3.Connection,
        *,
        debounce_seconds: float = 2.0,
        max_wait_seconds: float = 10.0,
        recover_running: bool = True,
    ):
        if debounce_seconds <= 0 or max_wait_seconds < debounce_seconds:
            raise ValueError("invalid batch timing")
        self.db = db
        self._lock = db_lock(db) or threading.RLock()
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=NORMAL")
        self.db.execute("PRAGMA busy_timeout=5000")
        self.debounce_seconds = debounce_seconds
        self.max_wait_seconds = max_wait_seconds
        self.db.executescript(
            "CREATE TABLE IF NOT EXISTS pilot_inbox ("
            "receipt_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, "
            "conversation_id TEXT NOT NULL, sender_id TEXT NOT NULL, "
            "payload_json TEXT NOT NULL, received_at TEXT NOT NULL, "
            "state TEXT NOT NULL CHECK(state IN ('pending','running','model_seen','failed')), "
            "batch_id TEXT, model_seen_at TEXT, attempts INTEGER NOT NULL DEFAULT 0, "
            "last_error TEXT, next_attempt_at TEXT);"
            "CREATE INDEX IF NOT EXISTS pilot_inbox_pending_order "
            "ON pilot_inbox(state,received_at,receipt_id);"
            "CREATE INDEX IF NOT EXISTS pilot_inbox_conversation "
            "ON pilot_inbox(state,account_id,conversation_id,received_at);"
            "CREATE TABLE IF NOT EXISTS worker_heartbeat ("
            "worker_id TEXT PRIMARY KEY, pid INTEGER NOT NULL, "
            "wechat_ready INTEGER NOT NULL, provider_ready INTEGER NOT NULL, "
            "in_flight_json TEXT, scheduled_followups INTEGER NOT NULL DEFAULT 0, "
            "updated_at TEXT NOT NULL);"
        )
        columns = {row["name"] for row in self.db.execute("PRAGMA table_info(pilot_inbox)")}
        if "next_attempt_at" not in columns:
            self.db.execute("ALTER TABLE pilot_inbox ADD COLUMN next_attempt_at TEXT")
        if recover_running:
            # A worker process may exit while a batch is in flight. Leave it eligible for replay.
            # Never touch legacy state='failed' rows on startup.
            self.db.execute(
                "UPDATE pilot_inbox SET state='pending',batch_id=NULL WHERE state='running'"
            )
        self.db.commit()

    @contextmanager
    def _immediate_tx(self) -> Iterator[None]:
        """Acquire a SQLite write lock up front for Scheme A multi-process safety."""
        with self._lock:
            if self.db.in_transaction:
                yield
                return
            self.db.execute("BEGIN IMMEDIATE")
            try:
                yield
                self.db.commit()
            except BaseException:
                if self.db.in_transaction:
                    self.db.rollback()
                raise

    @staticmethod
    def _payload(item: MessageEvent) -> str:
        return json.dumps(
            {
                "account_id": item.account_id,
                "conversation_id": item.conversation_id,
                "sender_id": item.sender_id,
                "sender_origin": item.sender_origin,
                "text": item.text,
                "is_group": item.is_group,
                "message_type": item.message_type,
                "source_msg_id": item.source_msg_id,
                "capture_tick_ms": item.capture_tick_ms,
                "original_message_type": item.original_message_type,
                "raw_message_json": item.raw_message_json,
                "event_time_context": item.event_time_context,
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def receipt_id(cls, item: MessageEvent) -> str:
        if item.source_msg_id is not None:
            source = f"platform:{item.source_msg_id}"
        else:
            raw = item.raw_message_json or cls._payload(item)
            source = "content:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()
        identity = f"{item.account_id}\x00{item.conversation_id}\x00{item.sender_id}\x00{source}"
        return hashlib.sha256(identity.encode("utf-8")).hexdigest()

    @staticmethod
    def next_budget_reset_utc(now: datetime | None = None) -> datetime:
        """Return the next Asia/Shanghai 00:00:05 daily budget boundary in UTC."""
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        local_now = current.astimezone(DAILY_BUDGET_TIMEZONE)
        next_day = local_now.date() + timedelta(days=1)
        next_local_reset = datetime(
            next_day.year,
            next_day.month,
            next_day.day,
            0,
            0,
            5,
            tzinfo=DAILY_BUDGET_TIMEZONE,
        )
        return next_local_reset.astimezone(UTC)

    def update_worker_heartbeat(
        self,
        *,
        worker_id: str = "default",
        pid: int | None = None,
        wechat_ready: bool,
        provider_ready: bool,
        in_flight: Mapping[str, object] | None = None,
        scheduled_followups: int = 0,
        now: datetime | None = None,
    ) -> None:
        """Upsert a worker liveness/status heartbeat row for Scheme A Ingress."""
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        in_flight_json = (
            json.dumps(dict(in_flight), ensure_ascii=False, sort_keys=True)
            if in_flight is not None
            else None
        )
        worker_pid = os.getpid() if pid is None else int(pid)
        with self._immediate_tx():
            self.db.execute(
                "INSERT INTO worker_heartbeat "
                "(worker_id,pid,wechat_ready,provider_ready,in_flight_json,"
                "scheduled_followups,updated_at) VALUES (?,?,?,?,?,?,?) "
                "ON CONFLICT(worker_id) DO UPDATE SET "
                "pid=excluded.pid,"
                "wechat_ready=excluded.wechat_ready,"
                "provider_ready=excluded.provider_ready,"
                "in_flight_json=excluded.in_flight_json,"
                "scheduled_followups=excluded.scheduled_followups,"
                "updated_at=excluded.updated_at",
                (
                    worker_id,
                    worker_pid,
                    int(bool(wechat_ready)),
                    int(bool(provider_ready)),
                    in_flight_json,
                    int(scheduled_followups),
                    current.astimezone(UTC).isoformat(),
                ),
            )

    def read_worker_heartbeat(
        self,
        max_age_seconds: float = 10.0,
        *,
        worker_id: str | None = None,
        now: datetime | None = None,
    ) -> dict[str, object] | None:
        """Read the latest worker heartbeat with freshness classification."""
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        if worker_id is None:
            row = self.db.execute(
                "SELECT worker_id,pid,wechat_ready,provider_ready,in_flight_json,"
                "scheduled_followups,updated_at "
                "FROM worker_heartbeat ORDER BY updated_at DESC LIMIT 1"
            ).fetchone()
        else:
            row = self.db.execute(
                "SELECT worker_id,pid,wechat_ready,provider_ready,in_flight_json,"
                "scheduled_followups,updated_at "
                "FROM worker_heartbeat WHERE worker_id=?",
                (worker_id,),
            ).fetchone()
        if row is None:
            return None
        updated_at_raw = str(row["updated_at"])
        try:
            updated_dt = datetime.fromisoformat(updated_at_raw)
            if updated_dt.tzinfo is None:
                updated_dt = updated_dt.replace(tzinfo=UTC)
            age_seconds = max(0.0, (current.astimezone(UTC) - updated_dt.astimezone(UTC)).total_seconds())
        except ValueError:
            age_seconds = float("inf")
        in_flight: dict[str, object] | None = None
        if row["in_flight_json"]:
            try:
                parsed = json.loads(str(row["in_flight_json"]))
                if isinstance(parsed, dict):
                    in_flight = parsed
            except json.JSONDecodeError:
                in_flight = None
        return {
            "worker_id": str(row["worker_id"]),
            "pid": int(row["pid"]),
            "wechat_ready": bool(row["wechat_ready"]),
            "provider_ready": bool(row["provider_ready"]),
            "in_flight": in_flight,
            "scheduled_followups": int(row["scheduled_followups"] or 0),
            "updated_at": updated_at_raw,
            "age_seconds": round(age_seconds, 3) if math.isfinite(age_seconds) else None,
            "alive": age_seconds <= max_age_seconds,
        }

    def enqueue(self, item: MessageEvent) -> tuple[str, str]:
        """Return (status, receipt); status is queued, duplicate, or backpressure."""
        receipt = self.receipt_id(item)
        payload = self._payload(item)
        if len(payload.encode("utf-8")) > 1_500_000:
            return "oversized", receipt
        with self._immediate_tx():
            existing = self.db.execute(
                "SELECT state,payload_json FROM pilot_inbox WHERE receipt_id=?",
                (receipt,),
            ).fetchone()
            if existing is not None:
                previous = json.loads(existing["payload_json"])
                incoming = json.loads(payload)
                for volatile in ("capture_tick_ms", "event_time_context"):
                    previous.pop(volatile, None)
                    incoming.pop(volatile, None)
                if previous != incoming:
                    return "source_id_conflict", receipt
                return "duplicate", receipt
            pending = self.db.execute(
                "SELECT count(*) FROM pilot_inbox WHERE state IN ('pending','running','failed')"
            ).fetchone()[0]
            if pending >= self.MAX_PENDING_MESSAGES:
                return "backpressure", receipt
            self.db.execute(
                "INSERT INTO pilot_inbox (receipt_id,account_id,conversation_id,sender_id,"
                "payload_json,received_at,state) VALUES (?,?,?,?,?,?,'pending')",
                (
                    receipt,
                    item.account_id,
                    item.conversation_id,
                    item.sender_id,
                    payload,
                    datetime.now(UTC).isoformat(),
                ),
            )
        return "queued", receipt

    def pending_count(self) -> int:
        return int(
            self.db.execute(
                "SELECT count(*) FROM pilot_inbox WHERE state IN ('pending','running','failed')"
            ).fetchone()[0]
        )

    def active_count(self) -> int:
        return int(
            self.db.execute(
                "SELECT count(*) FROM pilot_inbox WHERE state IN ('pending','running')",
            ).fetchone()[0]
        )

    def has_pending_conversation(self, account_id: str, conversation_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM pilot_inbox WHERE account_id=? AND conversation_id=? "
                "AND state IN ('pending','running') LIMIT 1",
                (account_id, conversation_id),
            ).fetchone()
            is not None
        )

    @staticmethod
    def _freshness_hold_count(last_error: object) -> int:
        if not isinstance(last_error, str):
            return 0
        if last_error == "freshness_held":
            return 1
        if last_error.startswith("freshness_held:"):
            suffix = last_error.split(":", 1)[1]
            return int(suffix) if suffix.isdigit() else 1
        return 0

    def batch_freshness_holds(self, batch: MessageBatch) -> int:
        """Return the highest prior freshness-hold count recorded on receipts in this batch."""
        if not batch.receipt_ids:
            return 0
        placeholders = ",".join("?" for _ in batch.receipt_ids)
        rows = self.db.execute(
            f"SELECT last_error FROM pilot_inbox WHERE receipt_id IN ({placeholders})",
            batch.receipt_ids,
        ).fetchall()
        return max((self._freshness_hold_count(row["last_error"]) for row in rows), default=0)

    def pending_inbound_snapshots(
        self,
        account_id: str,
        conversation_id: str,
        *,
        exclude_receipt_ids: Sequence[str] = (),
    ) -> tuple[PendingInboundSnapshot, ...]:
        """Return ordered pending inbox snapshots for pre-send freshness checks."""
        excluded = set(exclude_receipt_ids)
        rows = self.db.execute(
            "SELECT receipt_id,sender_id,payload_json,received_at,last_error "
            "FROM pilot_inbox "
            "WHERE state='pending' AND account_id=? AND conversation_id=? "
            "ORDER BY rowid",
            (account_id, conversation_id),
        ).fetchall()
        snapshots: list[PendingInboundSnapshot] = []
        for row in rows:
            receipt_id = str(row["receipt_id"])
            if receipt_id in excluded:
                continue
            try:
                payload = json.loads(str(row["payload_json"]))
            except json.JSONDecodeError:
                payload = {}
            raw_source_id = payload.get("source_msg_id")
            source_msg_id = (
                raw_source_id
                if isinstance(raw_source_id, int) and not isinstance(raw_source_id, bool)
                else None
            )
            snapshots.append(
                PendingInboundSnapshot(
                    receipt_id=receipt_id,
                    sender_id=str(row["sender_id"]),
                    text=str(payload.get("text") or ""),
                    is_group=bool(payload.get("is_group")),
                    received_at=str(row["received_at"]),
                    source_msg_id=source_msg_id,
                    last_error=str(row["last_error"]) if row["last_error"] is not None else None,
                )
            )
        return tuple(snapshots)

    def receipt_state(self, item: MessageEvent) -> str | None:
        row = self.db.execute(
            "SELECT state FROM pilot_inbox WHERE receipt_id=?",
            (self.receipt_id(item),),
        ).fetchone()
        return str(row[0]) if row is not None else None

    def state_counts(self) -> dict[str, int]:
        return {
            str(state): int(count)
            for state, count in self.db.execute(
                "SELECT state,count(*) FROM pilot_inbox GROUP BY state"
            ).fetchall()
        }

    def failed_count(self, error: str | None = None) -> int:
        if error is None:
            row = self.db.execute(
                "SELECT count(*) FROM pilot_inbox WHERE state='failed'",
            ).fetchone()
        else:
            row = self.db.execute(
                "SELECT count(*) FROM pilot_inbox WHERE state='failed' AND last_error=?",
                (error,),
            ).fetchone()
        return int(row[0])

    def failure_counts(self) -> dict[str, int]:
        return {
            str(error or "unknown"): int(count)
            for error, count in self.db.execute(
                "SELECT last_error,count(*) FROM pilot_inbox WHERE state='failed' "
                "GROUP BY last_error ORDER BY count(*) DESC,last_error LIMIT 20",
            ).fetchall()
        }

    def deferred_counts(self) -> dict[str, int]:
        """Return pending rows waiting on next_attempt_at grouped by last_error."""
        return {
            str(error or "unknown"): int(count)
            for error, count in self.db.execute(
                "SELECT last_error,count(*) FROM pilot_inbox "
                "WHERE state='pending' AND next_attempt_at IS NOT NULL "
                "GROUP BY last_error ORDER BY count(*) DESC,last_error LIMIT 20",
            ).fetchall()
        }

    def budget_blocked_count(self) -> int:
        """Count pending rows currently deferred due to daily model budget exhaustion."""
        row = self.db.execute(
            "SELECT count(*) FROM pilot_inbox "
            "WHERE state='pending' AND last_error=?",
            (self.BUDGET_EXHAUSTED_ERROR,),
        ).fetchone()
        return int(row[0])

    def requeue_failed_after_restart(
        self, retryable_errors: set[str],
    ) -> tuple[MessageEvent, ...]:
        """Explicit manual recovery for legacy state='failed' rows only.

        Never called automatically during startup or live worker polling.
        """
        if not retryable_errors:
            return ()
        placeholders = ",".join("?" for _ in retryable_errors)
        with self._immediate_tx():
            rows = self.db.execute(
                "SELECT receipt_id,payload_json,last_error,attempts FROM pilot_inbox "
                "WHERE state='failed' AND ((attempts<? AND last_error IN ("
                f"{placeholders})) OR last_error='previous_send_attempt_turn_incomplete_no_replay') "
                "ORDER BY rowid",
                (self.MAX_PROCESS_ATTEMPTS, *sorted(retryable_errors)),
            ).fetchall()
            if not rows:
                return ()
            self.db.executemany(
                "UPDATE pilot_inbox SET state='pending',batch_id=NULL,last_error=?,"
                "attempts=?,next_attempt_at=NULL WHERE receipt_id=? AND state='failed'",
                (
                    (
                        "requeued_after_login_init",
                        0 if row["last_error"] ==
                        "previous_send_attempt_turn_incomplete_no_replay"
                        else int(row["attempts"]),
                        str(row["receipt_id"]),
                    )
                    for row in rows
                ),
            )
            return tuple(self._from_payload(str(row["payload_json"])) for row in rows)

    def defer_until_budget_reset(
        self,
        batch: MessageBatch,
        *,
        now: datetime | None = None,
        reason: str = BUDGET_EXHAUSTED_ERROR,
    ) -> datetime:
        """Defer a running batch to pending until next Asia/Shanghai 00:00:05 UTC."""
        retry_at = self.next_budget_reset_utc(now=now)
        safe_reason = " ".join(str(reason).split())[:120]
        with self._immediate_tx():
            self.db.execute(
                "UPDATE pilot_inbox SET state='pending',batch_id=NULL,"
                "attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,"
                "last_error=?,next_attempt_at=? "
                "WHERE batch_id=? AND state='running'",
                (safe_reason, retry_at.isoformat(), batch.batch_id),
            )
        return retry_at

    def wake_budget_deferred_batches(
        self,
        now: datetime | None = None,
        *,
        force: bool = False,
    ) -> tuple[MessageEvent, ...]:
        """Clear next_attempt_at for pending budget-deferred rows once due (or forced).

        Never touches historical legacy state='failed' rows.
        """
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        current_utc = current.astimezone(UTC)
        with self._immediate_tx():
            rows = self.db.execute(
                "SELECT receipt_id,payload_json,next_attempt_at FROM pilot_inbox "
                "WHERE state='pending' AND last_error=? ORDER BY rowid",
                (self.BUDGET_EXHAUSTED_ERROR,),
            ).fetchall()
            eligible = []
            for row in rows:
                if force:
                    eligible.append(row)
                    continue
                next_at = row["next_attempt_at"]
                if next_at is None:
                    eligible.append(row)
                    continue
                try:
                    due = datetime.fromisoformat(str(next_at))
                except ValueError:
                    continue
                if due.tzinfo is None:
                    due = due.replace(tzinfo=UTC)
                if due.astimezone(UTC) <= current_utc:
                    eligible.append(row)
            if not eligible:
                return ()
            self.db.executemany(
                "UPDATE pilot_inbox SET next_attempt_at=NULL,"
                "last_error='woken_after_budget_reset' "
                "WHERE receipt_id=? AND state='pending'",
                ((str(row["receipt_id"]),) for row in eligible),
            )
            return tuple(self._from_payload(str(row["payload_json"])) for row in eligible)

    def requeue_budget_exhausted(
        self,
        *,
        now: datetime | None = None,
        max_age_hours: int | None = None,
    ) -> tuple[MessageEvent, ...]:
        """Compat wrapper waking due pending budget-deferred rows without touching legacy failed."""
        _ = max_age_hours
        return self.wake_budget_deferred_batches(now=now)

    def claim_ready_batch(
        self,
        *,
        now: datetime | None = None,
        blocked_conversations: set[tuple[str, str]] | None = None,
    ) -> MessageBatch | None:
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        current_utc = current.astimezone(UTC)
        current_iso = current_utc.isoformat()
        with self._immediate_tx():
            candidates = self.db.execute(
                "SELECT account_id,conversation_id,min(received_at) AS first_at "
                "FROM pilot_inbox WHERE state='pending' "
                "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
                "GROUP BY account_id,conversation_id "
                "ORDER BY first_at,account_id,conversation_id LIMIT 100",
                (current_iso,),
            ).fetchall()
            first = next(
                (row for row in candidates
                 if (str(row[0]), str(row[1])) not in (blocked_conversations or set())),
                None,
            )
            if first is None:
                return None
            rows = self.db.execute(
                "SELECT receipt_id,payload_json,received_at FROM pilot_inbox "
                "WHERE state='pending' AND account_id=? AND conversation_id=? "
                "AND (next_attempt_at IS NULL OR next_attempt_at<=?) "
                # rowid is the durable enqueue order; timestamps can tie at
                # microsecond resolution, while receipt hashes are not ordered.
                "ORDER BY rowid LIMIT ?",
                (first[0], first[1], current_iso, self.MAX_BATCH_MESSAGES),
            ).fetchall()
            if not rows:
                return None
            times = [datetime.fromisoformat(row["received_at"]) for row in rows]
            quiet = current_utc >= max(times).astimezone(UTC) + timedelta(
                seconds=self.debounce_seconds,
            )
            wait_capped = current_utc >= min(times).astimezone(UTC) + timedelta(
                seconds=self.max_wait_seconds,
            )
            full = len(rows) == self.MAX_BATCH_MESSAGES
            if not (quiet or wait_capped or full):
                return None
            rows = self._prefix_within_text_budget(rows)
            receipt_ids = tuple(str(row["receipt_id"]) for row in rows)
            batch_id = hashlib.sha256("\0".join(receipt_ids).encode()).hexdigest()
            self.db.executemany(
                "UPDATE pilot_inbox SET state='running',batch_id=?,attempts=attempts+1 "
                "WHERE receipt_id=? AND state='pending'",
                ((batch_id, receipt) for receipt in receipt_ids),
            )
            observations = tuple(self._from_payload(row["payload_json"]) for row in rows)
        return MessageBatch(batch_id, receipt_ids, observations)

    def claim_conversation_batch(
        self, account_id: str, conversation_id: str, *, now: datetime | None = None
    ) -> MessageBatch | None:
        """Immediately claim one ordered pending batch for internal session activation."""
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        with self._immediate_tx():
            rows = self.db.execute(
                "SELECT receipt_id,payload_json FROM pilot_inbox WHERE state='pending' "
                "AND account_id=? AND conversation_id=? "
                "ORDER BY rowid LIMIT ?",
                (account_id, conversation_id, self.MAX_BATCH_MESSAGES),
            ).fetchall()
            if not rows:
                return None
            rows = self._prefix_within_text_budget(rows)
            receipts = tuple(str(row["receipt_id"]) for row in rows)
            batch_id = hashlib.sha256("\0".join(receipts).encode()).hexdigest()
            self.db.executemany(
                "UPDATE pilot_inbox SET state='running',batch_id=?,attempts=attempts+1 "
                "WHERE receipt_id=? AND state='pending'",
                ((batch_id, receipt) for receipt in receipts),
            )
            observations = tuple(self._from_payload(row["payload_json"]) for row in rows)
        return MessageBatch(batch_id, receipts, observations)

    @classmethod
    def _prefix_within_text_budget(cls, rows):
        """Choose a FIFO prefix; a single long message is processed whole."""
        selected = []
        total = 0
        for row in rows:
            payload = json.loads(row["payload_json"])
            size = len(str(payload.get("text") or "").encode("utf-8"))
            if selected and total + size > cls.MAX_BATCH_TEXT_BYTES:
                break
            selected.append(row)
            total += size
        return selected

    def mark_model_seen(self, batch: MessageBatch) -> bool:
        """Mark every input seen only after the Agent session completed normally."""
        if not batch.receipt_ids:
            return False
        placeholders = ",".join("?" for _ in batch.receipt_ids)
        with self._immediate_tx():
            count = self.db.execute(
                f"SELECT count(*) FROM pilot_inbox WHERE state='running' AND batch_id=? "
                f"AND receipt_id IN ({placeholders})",
                (batch.batch_id, *batch.receipt_ids),
            ).fetchone()[0]
            if count != len(batch.receipt_ids):
                return False
            now = datetime.now(UTC).isoformat()
            self.db.executemany(
                "UPDATE pilot_inbox SET state='model_seen',model_seen_at=?,last_error=NULL,"
                "next_attempt_at=NULL "
                "WHERE receipt_id=? AND batch_id=? AND state='running'",
                ((now, receipt, batch.batch_id) for receipt in batch.receipt_ids),
            )
        return True

    def release_for_retry(
        self,
        batch: MessageBatch,
        error: str,
        *,
        min_delay_seconds: float = 0.0,
        now: datetime | None = None,
    ) -> None:
        """Always return unseen running rows to pending with bounded backoff (max 300s).

        Never transitions unseen human messages to state='failed'.
        """
        if not (math.isfinite(min_delay_seconds) and min_delay_seconds >= 0):
            raise ValueError("min_delay_seconds must be finite and non-negative")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        current_utc = current.astimezone(UTC)
        safe_error = " ".join(str(error).split())[:120]
        repeated_alerts: list[tuple[str, int, int]] = []
        with self._immediate_tx():
            rows = self.db.execute(
                "SELECT receipt_id,attempts FROM pilot_inbox WHERE batch_id=? AND state='running'",
                (batch.batch_id,),
            ).fetchall()
            updates = []
            for row in rows:
                attempts = int(row["attempts"])
                base_delay = min(300, 2 ** min(max(0, attempts), 9))
                delay = max(float(min_delay_seconds), float(base_delay))
                retry_at = (current_utc + timedelta(seconds=delay)).isoformat()
                if attempts >= self.ALERT_ATTEMPTS_THRESHOLD:
                    repeated_alerts.append((str(row["receipt_id"]), attempts, round(delay)))
                updates.append(
                    (
                        safe_error,
                        retry_at,
                        row["receipt_id"],
                        batch.batch_id,
                    )
                )
            self.db.executemany(
                "UPDATE pilot_inbox SET state='pending',batch_id=NULL,last_error=?,"
                "next_attempt_at=? "
                "WHERE receipt_id=? AND batch_id=? AND state='running'",
                updates,
            )
        for receipt_id, attempts, delay in repeated_alerts:
            logger.error(
                "event=inbox_batch_repeated_retry batch=%s receipt=%s attempts=%s "
                "delay_seconds=%s error=%s",
                batch.batch_id[:12],
                receipt_id[:12],
                attempts,
                delay,
                safe_error,
            )

    def fail_batch(
        self,
        batch: MessageBatch,
        error: str,
        *,
        retry_after: datetime | None = None,
    ) -> int:
        """Legacy explicit administrative failure marker (never called by live runtime)."""
        if retry_after is not None and retry_after.tzinfo is None:
            raise ValueError("retry_after must be timezone-aware")
        safe_error = " ".join(str(error).split())[:120]
        next_at = retry_after.astimezone(UTC).isoformat() if retry_after is not None else None
        with self._immediate_tx():
            cursor = self.db.execute(
                "UPDATE pilot_inbox SET state='failed',batch_id=NULL,last_error=?,"
                "next_attempt_at=? WHERE batch_id=? AND state='running'",
                (safe_error, next_at, batch.batch_id),
            )
        return int(cursor.rowcount)

    def defer_batch(
        self,
        batch: MessageBatch,
        reason: str = "provider_unavailable",
        *,
        delay_seconds: float = 5.0,
        now: datetime | None = None,
    ) -> int:
        """Return an unexecuted running batch to pending without burning attempts."""
        if not (math.isfinite(delay_seconds) and delay_seconds >= 0):
            raise ValueError("delay_seconds must be finite and non-negative")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        safe_reason = " ".join(str(reason).split())[:120]
        next_attempt_at = (
            None
            if delay_seconds == 0
            else (current.astimezone(UTC) + timedelta(seconds=delay_seconds)).isoformat()
        )
        with self._immediate_tx():
            cursor = self.db.execute(
                "UPDATE pilot_inbox SET state='pending',batch_id=NULL,"
                "attempts=CASE WHEN attempts>0 THEN attempts-1 ELSE 0 END,"
                "last_error=?,next_attempt_at=? "
                "WHERE batch_id=? AND state='running'",
                (safe_reason, next_attempt_at, batch.batch_id),
            )
        return int(cursor.rowcount)

    def wake_deferred_batches(self, *, reason: str = "provider_unavailable") -> int:
        """Clear backoff timestamps on deferred batches so they coalesce and run now."""
        safe_reason = " ".join(str(reason).split())[:120]
        with self._immediate_tx():
            cursor = self.db.execute(
                "UPDATE pilot_inbox SET next_attempt_at=NULL "
                "WHERE state='pending' AND last_error=?",
                (safe_reason,),
            )
        return int(cursor.rowcount)

    def resolve_replayed_batch(
        self, batch: MessageBatch, side_effected_receipts: set[str], error: str,
    ) -> tuple[int, int]:
        """Fail receipts already sent in an earlier turn and requeue only unseen ones."""
        if not side_effected_receipts.issubset(batch.receipt_ids):
            raise ValueError("side-effect receipts must belong to the claimed batch")
        safe_error = " ".join(str(error).split())[:120]
        with self._immediate_tx():
            failed = 0
            for receipt in side_effected_receipts:
                cursor = self.db.execute(
                    "UPDATE pilot_inbox SET state='failed',batch_id=NULL,last_error=?,"
                    "next_attempt_at=NULL WHERE receipt_id=? AND batch_id=? AND state='running'",
                    (safe_error, receipt, batch.batch_id),
                )
                failed += int(cursor.rowcount)
            placeholders = ",".join("?" for _ in side_effected_receipts)
            if side_effected_receipts:
                cursor = self.db.execute(
                    "UPDATE pilot_inbox SET state='pending',batch_id=NULL,"
                    "last_error='deferred_after_prior_send',next_attempt_at=NULL "
                    f"WHERE batch_id=? AND state='running' AND receipt_id NOT IN ({placeholders})",
                    (batch.batch_id, *sorted(side_effected_receipts)),
                )
                released = int(cursor.rowcount)
            else:
                released = 0
        return failed, released

    @staticmethod
    def _from_payload(payload: str) -> MessageEvent:
        value = json.loads(payload)
        return MessageEvent(
            account_id=value["account_id"],
            conversation_id=value["conversation_id"],
            sender_id=value["sender_id"],
            sender_origin=value["sender_origin"],
            text=value["text"],
            is_group=value["is_group"],
            message_type=value["message_type"],
            source_msg_id=value["source_msg_id"],
            capture_tick_ms=value["capture_tick_ms"],
            raw_message_json=value.get("raw_message_json", ""),
            original_message_type=value.get("original_message_type"),
            event_time_context=value.get("event_time_context"),
        )
