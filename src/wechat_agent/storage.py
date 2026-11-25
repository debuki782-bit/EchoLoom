"""SQLite repository with transaction boundaries for inbox and privacy state."""

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .models import InboundEvent, ReplyDraft


class Repository:
    def __init__(self, path: Path):
        self._lock = threading.RLock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self.conn = sqlite3.connect(
            path, timeout=0.5, isolation_level=None, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        version = self.conn.execute("PRAGMA user_version").fetchone()[0]
        if version > 5:
            raise ValueError(f"unsupported database schema version: {version}")
        if version == 0:
            self.conn.executescript(
            """
            BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS inbox (
                event_id TEXT PRIMARY KEY, account_id TEXT NOT NULL,
                conversation_id TEXT NOT NULL, sender_id TEXT NOT NULL,
                text TEXT NOT NULL, is_group INTEGER NOT NULL,
                message_type TEXT NOT NULL, privacy_epoch INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS drafts (
                event_id TEXT PRIMARY KEY REFERENCES inbox(event_id),
                account_id TEXT NOT NULL, conversation_id TEXT NOT NULL,
                text TEXT NOT NULL, privacy_epoch INTEGER NOT NULL,
                state TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS group_privacy (
                account_id TEXT NOT NULL, room_id TEXT NOT NULL,
                state TEXT NOT NULL, privacy_epoch INTEGER NOT NULL,
                paused_by TEXT, PRIMARY KEY(account_id, room_id)
            );
            CREATE TABLE IF NOT EXISTS privacy_controls (
                account_id TEXT NOT NULL, room_id TEXT NOT NULL,
                event_id TEXT NOT NULL, action TEXT NOT NULL,
                result TEXT NOT NULL, at TEXT NOT NULL,
                PRIMARY KEY(account_id, room_id, event_id)
            );
            PRAGMA user_version=1;
            COMMIT;
            """
            )
        elif version == 1:
            self._backup(path, "v2")
        if version < 2:
            self.conn.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE outbox (
                    event_id TEXT PRIMARY KEY REFERENCES drafts(event_id),
                    account_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    privacy_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN (
                        'awaiting_approval', 'ready', 'sending', 'sent',
                        'failed', 'unknown', 'cancelled', 'mock_sent'
                    )),
                    attempts INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                INSERT INTO outbox (
                    event_id, account_id, conversation_id, privacy_epoch,
                    state, attempts, updated_at
                )
                SELECT d.event_id, d.account_id, d.conversation_id,
                    d.privacy_epoch,
                    CASE d.state
                        WHEN 'mock_sent' THEN 'mock_sent'
                        WHEN 'dropped' THEN 'cancelled'
                        ELSE 'awaiting_approval'
                    END,
                    0, i.created_at
                FROM drafts d JOIN inbox i ON i.event_id=d.event_id;
                PRAGMA user_version=2;
                COMMIT;
                """
            )
        if version < 3:
            if version in (1, 2):
                self._backup(path, "v3")
            self.conn.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE inbound_queue (
                    queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transport TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    is_group INTEGER NOT NULL,
                    message_type TEXT NOT NULL,
                    privacy_epoch INTEGER NOT NULL,
                    state TEXT NOT NULL CHECK(state IN ('queued','running','completed','cancelled')),
                    received_at TEXT NOT NULL,
                    UNIQUE(transport, account_id, source_id)
                );
                CREATE INDEX inbound_queue_state_order
                    ON inbound_queue(state, queue_id);
                CREATE INDEX inbound_queue_conversation
                    ON inbound_queue(account_id, conversation_id, state);
                PRAGMA user_version=3;
                COMMIT;
                """
            )
        if version < 4:
            if version in (1, 2, 3):
                self._backup(path, "v4")
            self.conn.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE usage_reservations (
                    request_id TEXT PRIMARY KEY,
                    account_id TEXT NOT NULL,
                    conversation_id TEXT NOT NULL,
                    feature TEXT NOT NULL,
                    model_id TEXT NOT NULL,
                    month_utc TEXT NOT NULL,
                    reserved_microusd INTEGER NOT NULL CHECK(reserved_microusd > 0),
                    actual_microusd INTEGER CHECK(actual_microusd >= 0),
                    state TEXT NOT NULL CHECK(state IN ('reserved','settled','released')),
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX usage_reservations_month_state
                    ON usage_reservations(month_utc, state);
                PRAGMA user_version=4;
                COMMIT;
                """
            )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS usage_reservations_account_created "
            "ON usage_reservations(account_id, created_at, state)"
        )
        version = int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        if version < 5:
            self.conn.execute(
                "ALTER TABLE usage_reservations ADD COLUMN budget_scope_id "
                "TEXT NOT NULL DEFAULT 'default'"
            )
            self.conn.execute(
                "CREATE INDEX IF NOT EXISTS usage_reservations_scope_created "
                "ON usage_reservations(account_id, budget_scope_id, created_at, state)"
            )
            self.conn.execute("PRAGMA user_version=5")
            self.conn.commit()

    def _backup(self, path: Path, target_version: str) -> None:
        if path == Path(":memory:"):
            return
        backup = path.with_name(f"{path.name}.pre-{target_version}-{time.time_ns()}.bak")
        with sqlite3.connect(backup) as target:
            self.conn.backup(target)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
            else:
                self.conn.execute("COMMIT")

    @contextmanager
    def read_lock(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            yield self.conn

    def privacy(self, account_id: str, room_id: str) -> tuple[str, int, str | None]:
        with self.read_lock() as conn:
            row = conn.execute(
                "SELECT state, privacy_epoch, paused_by FROM group_privacy "
                "WHERE account_id=? AND room_id=?",
                (account_id, room_id),
            ).fetchone()
        return (row["state"], row["privacy_epoch"], row["paused_by"]) if row else ("active", 0, None)

    def event_match(self, event: InboundEvent) -> str | None:
        row = self.conn.execute(
            "SELECT account_id,conversation_id,sender_id,text,is_group,message_type "
            "FROM inbox WHERE event_id=?", (event.event_id,)
        ).fetchone()
        if row is None:
            return None
        same = (
            row["account_id"] == event.account_id
            and row["conversation_id"] == event.conversation_id
            and row["sender_id"] == event.sender_id
            and row["text"] == event.text
            and row["is_group"] == int(event.is_group)
            and row["message_type"] == event.message_type
        )
        return "duplicate" if same else "event_id_conflict"

    def save_event(self, event: InboundEvent, epoch: int, at: str) -> None:
        self.conn.execute(
            "INSERT INTO inbox VALUES (?,?,?,?,?,?,?,?,?)",
            (event.event_id, event.account_id, event.conversation_id, event.sender_id,
             event.text, int(event.is_group), event.message_type, epoch, at),
        )

    def save_draft(self, draft: ReplyDraft) -> None:
        self.conn.execute(
            "INSERT INTO drafts VALUES (?,?,?,?,?,?)",
            (draft.event_id, draft.key.account_id, draft.key.conversation_id,
             draft.text, draft.privacy_epoch, "draft"),
        )
        self.conn.execute(
            "INSERT INTO outbox VALUES (?,?,?,?,?,?,?)",
            (draft.event_id, draft.key.account_id, draft.key.conversation_id,
             draft.privacy_epoch, "awaiting_approval", 0,
             self.conn.execute("SELECT created_at FROM inbox WHERE event_id=?", (draft.event_id,))
             .fetchone()[0]),
        )

    def status(self) -> dict[str, int]:
        with self.read_lock() as conn:
            return {
                "inbox": conn.execute("SELECT count(*) FROM inbox").fetchone()[0],
                "drafts": conn.execute("SELECT count(*) FROM drafts WHERE state='draft'").fetchone()[0],
                "mock_sent": conn.execute("SELECT count(*) FROM drafts WHERE state='mock_sent'").fetchone()[0],
                "paused_groups": conn.execute(
                    "SELECT count(*) FROM group_privacy WHERE state='paused'"
                ).fetchone()[0],
            }

    def pipeline_status(self) -> dict[str, int]:
        with self.read_lock() as conn:
            counts: dict[str, int] = {}
            for state in ("queued", "running"):
                counts[f"inbound_{state}"] = conn.execute(
                    "SELECT count(*) FROM inbound_queue WHERE state=?", (state,)
                ).fetchone()[0]
            for state in ("awaiting_approval", "ready", "sending", "unknown"):
                counts[f"outbox_{state}"] = conn.execute(
                    "SELECT count(*) FROM outbox WHERE state=?", (state,)
                ).fetchone()[0]
            return counts

    def close(self) -> None:
        with self._lock:
            self.conn.close()
