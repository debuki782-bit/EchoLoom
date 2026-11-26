"""Repository owning source_messages and message_jev_labels tables."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

from ..contacts import is_group_conversation

MAX_QUERY_CHARS = 300
DEFAULT_RETENTION_DAYS = 365


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _bounded_text(value: str, limit: int) -> str:
    value = value.strip()
    if not value or len(value) > limit or "\0" in value:
        raise ValueError("text is empty or exceeds its limit")
    return value


def _source_platform_message_id(source_record: dict | None) -> str | None:
    if not isinstance(source_record, dict):
        return None
    value = source_record.get("msgsvrid")
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        return None
    return str(value)


@dataclass(frozen=True)
class StoredMessage:
    message_id: str
    raw_sha256: str
    raw_json: str
    captured_at: str = ""


class MessageRepo:
    """Durable archive and query repository for source_messages and message_jev_labels."""

    def __init__(
        self,
        db: sqlite3.Connection,
        lock: threading.RLock,
        transaction_fn: Callable[[], AbstractContextManager[None]],
        *,
        ensure_note: Callable[..., None] | None = None,
    ) -> None:
        self.db = db
        self._lock = lock
        self._transaction = transaction_fn
        self._ensure_note = ensure_note

    def archive_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        sender_id: str,
        raw_envelope: dict,
        body_text: str,
        source_record: dict | None = None,
        event_time_context: Mapping[str, object] | None = None,
        direction: str = "inbound",
        delivery_state: str | None = None,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        source_message_id: int | None = None,
    ) -> StoredMessage:
        if retention_days < 1 or retention_days > 3650:
            raise ValueError("retention_days out of range")
        if direction not in ("inbound", "outbound"):
            raise ValueError("invalid message direction")
        if delivery_state is not None and len(delivery_state) > 40:
            raise ValueError("delivery state too long")
        raw_json = json.dumps(
            raw_envelope, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        event_time_record = dict(event_time_context) if event_time_context is not None else None
        if event_time_record is not None:
            # Keep one display timestamp. The host-derived UTC minute is only
            # an estimate and duplicates the local minute at this resolution.
            event_time_record.pop("event_utc_minute", None)
        event_time_json = (
            json.dumps(
                event_time_record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            if event_time_record is not None
            else None
        )
        raw_hash = hashlib.sha256(raw_json.encode("utf-8")).hexdigest()
        raw_message = source_record
        fingerprint_source = json.dumps(
            raw_message if isinstance(raw_message, dict) else raw_envelope,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        source_fingerprint = hashlib.sha256(
            f"{account_id}\0{conversation_id}\0{sender_id}\0{fingerprint_source}".encode()
        ).hexdigest()
        if source_message_id is not None and (
            type(source_message_id) is not int or source_message_id <= 0
        ):
            raise ValueError("source_message_id must be a positive integer")
        platform_message_id = (
            str(source_message_id)
            if source_message_id is not None
            else _source_platform_message_id(source_record)
        )
        message_id = uuid4().hex
        now = datetime.now(UTC)
        expires = (now + timedelta(days=retention_days)).isoformat()
        with self._transaction():
            existing = self.db.execute(
                "SELECT message_id,raw_sha256,raw_json,platform_message_id,captured_at "
                "FROM source_messages WHERE account_id=? AND source_fingerprint=?",
                (account_id, source_fingerprint),
            ).fetchone()
            if existing is not None:
                if event_time_json is not None:
                    self.db.execute(
                        "UPDATE source_messages SET event_time_context_json=? "
                        "WHERE message_id=? AND event_time_context_json IS NULL",
                        (event_time_json, existing["message_id"]),
                    )
                if platform_message_id and not existing["platform_message_id"]:
                    self.db.execute(
                        "UPDATE source_messages SET platform_message_id=? WHERE message_id=?",
                        (platform_message_id, existing["message_id"]),
                    )
                return StoredMessage(
                    existing["message_id"],
                    existing["raw_sha256"],
                    existing["raw_json"],
                    str(existing["captured_at"] or ""),
                )
            if platform_message_id:
                existing_platform = self.db.execute(
                    "SELECT message_id,raw_sha256,raw_json,captured_at FROM source_messages "
                    "WHERE account_id=? AND conversation_id=? AND platform_message_id=?",
                    (account_id, conversation_id, platform_message_id),
                ).fetchone()
                if existing_platform is not None:
                    return StoredMessage(
                        existing_platform["message_id"],
                        existing_platform["raw_sha256"],
                        existing_platform["raw_json"],
                        str(existing_platform["captured_at"] or ""),
                    )
            captured_iso = now.isoformat()
            self.db.execute(
                "INSERT INTO source_messages "
                "(message_id,account_id,conversation_id,sender_id,captured_at,raw_json,"
                "platform_message_id,raw_sha256,body_text,expires_at,deleted_at,"
                "source_fingerprint,event_time_context_json,direction,delivery_state) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,NULL,?,?,?,?)",
                (
                    message_id,
                    account_id,
                    conversation_id,
                    sender_id,
                    captured_iso,
                    raw_json,
                    platform_message_id,
                    raw_hash,
                    body_text,
                    expires,
                    source_fingerprint,
                    event_time_json,
                    direction,
                    delivery_state,
                ),
            )
        return StoredMessage(message_id, raw_hash, raw_json, captured_iso)

    def archive_agent_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        text: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
    ) -> StoredMessage:
        """Persist an Agent text send; message ID is local unless Hook proves otherwise."""
        del library_prompt
        if not operation_id or len(operation_id) > 256:
            raise ValueError("invalid operation ID")
        if delivery_state not in ("queued", "accepted_by_server", "unknown"):
            raise ValueError("only submitted or uncertain sends are archived")
        raw = {
            "direction": "outbound",
            "fromid": account_id,
            "toid": conversation_id,
            "room": conversation_id if is_group_conversation(conversation_id) else "",
            "msg": text,
            "msgtype": 1,
            "local_operation_id": operation_id,
            "delivery_state": delivery_state,
        }
        archived = self.archive_message(
            account_id=account_id,
            conversation_id=conversation_id,
            sender_id=account_id,
            raw_envelope=raw,
            source_record=raw,
            body_text=text,
            direction="outbound",
            delivery_state=delivery_state,
        )
        if note:
            if self._ensure_note is None:
                raise RuntimeError("note storage is not configured on MessageRepo")
            self._ensure_note(
                account_id=account_id,
                message_id=archived.message_id,
                content=note,
            )
        return archived

    def get_event_time_context(
        self, *, message_id: str, account_id: str
    ) -> dict[str, object] | None:
        row = self.db.execute(
            "SELECT event_time_context_json FROM source_messages "
            "WHERE message_id=? AND account_id=? AND deleted_at IS NULL",
            (message_id, account_id),
        ).fetchone()
        if row is None or row["event_time_context_json"] is None:
            return None
        value = json.loads(row["event_time_context_json"])
        if not isinstance(value, dict):
            return None
        value.pop("event_utc_minute", None)  # normalize legacy rows on read
        return value

    def recent_other_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 20,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        """Return up to 20 messages from other chats captured in the last hour.

        Results are selected newest-first to enforce the global cap, then
        returned oldest-first for chronological context rendering.
        """
        if not 1 <= limit <= 20:
            raise ValueError("limit must be between 1 and 20")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        now_text = current.astimezone(UTC).isoformat()
        since_text = (current.astimezone(UTC) - timedelta(hours=1)).isoformat()
        rows = self.db.execute(
            "SELECT message_id,conversation_id,sender_id,body_text,captured_at,direction,"
            "delivery_state,"
            "event_time_context_json FROM source_messages WHERE account_id=? "
            "AND conversation_id<>? AND captured_at>=? AND captured_at<=? "
            "AND deleted_at IS NULL AND expires_at>? "
            "ORDER BY captured_at DESC,message_id DESC LIMIT ?",
            (account_id, conversation_id, since_text, now_text, now_text, limit),
        ).fetchall()
        messages: list[dict[str, object]] = []
        for row in reversed(rows):
            try:
                event_time = json.loads(row["event_time_context_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                event_time = {}
            if not isinstance(event_time, dict):
                event_time = {}
            event_time.pop("event_utc_minute", None)
            if not event_time.get("local_minute"):
                captured = datetime.fromisoformat(row["captured_at"]).astimezone(
                    timezone(timedelta(hours=8)),
                )
                event_time["local_minute"] = captured.strftime("%Y-%m-%d %H:%M")
                event_time.setdefault("timezone", "Asia/Shanghai")
            messages.append(
                {
                    "message_id": row["message_id"],
                    "conversation_id": row["conversation_id"],
                    "sender_id": row["sender_id"],
                    "body_text": row["body_text"],
                    "captured_at": row["captured_at"],
                    "direction": row["direction"],
                    "delivery_state": row["delivery_state"],
                    "event_time_context": event_time,
                }
            )
        return messages

    def recent_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 50,
        exclude_message_id: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        """Return the newest same-chat records, in chronological order."""
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")
        current = now or datetime.now(UTC)
        if current.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        now_text = current.astimezone(UTC).isoformat()
        sql = (
            "SELECT message_id,conversation_id,sender_id,body_text,captured_at,direction,"
            "delivery_state,"
            "event_time_context_json FROM source_messages WHERE account_id=? "
            "AND conversation_id=? AND captured_at<=? AND deleted_at IS NULL "
            "AND expires_at>?"
        )
        params: list[object] = [account_id, conversation_id, now_text, now_text]
        if exclude_message_id:
            sql += " AND message_id<>?"
            params.append(exclude_message_id)
        sql += " ORDER BY captured_at DESC,message_id DESC LIMIT ?"
        params.append(limit)
        rows = self.db.execute(sql, params).fetchall()
        messages: list[dict[str, object]] = []
        for row in reversed(rows):
            try:
                event_time = json.loads(row["event_time_context_json"] or "{}")
            except (TypeError, json.JSONDecodeError):
                event_time = {}
            if not isinstance(event_time, dict):
                event_time = {}
            event_time.pop("event_utc_minute", None)
            if not event_time.get("local_minute"):
                captured = datetime.fromisoformat(row["captured_at"]).astimezone(
                    timezone(timedelta(hours=8)),
                )
                event_time["local_minute"] = captured.strftime("%Y-%m-%d %H:%M")
                event_time.setdefault("timezone", "Asia/Shanghai")
            messages.append(
                {
                    "message_id": row["message_id"],
                    "conversation_id": row["conversation_id"],
                    "sender_id": row["sender_id"],
                    "body_text": row["body_text"],
                    "captured_at": row["captured_at"],
                    "direction": row["direction"],
                    "delivery_state": row["delivery_state"],
                    "event_time_context": event_time,
                }
            )
        return messages

    def recent_unanswered_inbound(
        self,
        *,
        account_id: str,
        since: datetime,
        limit: int = 2048,
    ) -> list[dict[str, object]]:
        """Find recent inbound tails with no later archived Agent send per chat.

        This is a migration/recovery path for messages archived before the durable
        Pilot inbox existed. New messages use the inbox state as their seen marker.
        """
        if not account_id or not 1 <= limit <= 2048 or since.tzinfo is None:
            raise ValueError("invalid unanswered-message query")
        cutoff = since.astimezone(UTC).isoformat()
        with self._lock:
            rows = self.db.execute(
                "SELECT s.message_id,s.conversation_id,s.sender_id,s.captured_at,"
                "s.raw_json,s.body_text,s.source_fingerprint,s.platform_message_id "
                "FROM source_messages s WHERE s.account_id=? AND s.direction='inbound' "
                "AND s.deleted_at IS NULL AND s.expires_at>? AND s.captured_at>=? "
                "AND s.captured_at>coalesce((SELECT max(o.captured_at) "
                "FROM source_messages o WHERE o.account_id=s.account_id "
                "AND o.conversation_id=s.conversation_id AND o.direction='outbound' "
                "AND o.deleted_at IS NULL), '') "
                "ORDER BY s.captured_at,s.message_id LIMIT ?",
                (account_id, _now(), cutoff, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_conversations(self, *, account_id: str, limit: int = 100) -> list[dict[str, object]]:
        """List known private/group chats newest-first from the durable message store."""
        if not 1 <= limit <= 200:
            raise ValueError("limit must be between 1 and 200")
        now_text = _now()
        rows = self.db.execute(
            "SELECT conversation_id,max(captured_at) AS last_at,count(*) AS message_count "
            "FROM source_messages WHERE account_id=? AND deleted_at IS NULL "
            "AND expires_at>? GROUP BY conversation_id ORDER BY last_at DESC "
            "LIMIT ?",
            (account_id, now_text, limit),
        ).fetchall()
        results: list[dict[str, object]] = []
        for row in rows:
            latest = self.db.execute(
                "SELECT message_id,sender_id,body_text,direction,captured_at "
                "FROM source_messages WHERE account_id=? AND conversation_id=? "
                "AND deleted_at IS NULL AND expires_at>? "
                "ORDER BY captured_at DESC,message_id DESC LIMIT 1",
                (account_id, row["conversation_id"], now_text),
            ).fetchone()
            if latest is None:
                continue
            results.append(
                {
                    "conversation_id": str(row["conversation_id"]),
                    "is_group": is_group_conversation(str(row["conversation_id"])),
                    "last_at": str(row["last_at"]),
                    "message_count": int(row["message_count"]),
                    "last_message_id": str(latest["message_id"]),
                    "last_sender_id": str(latest["sender_id"]),
                    "last_message_preview": str(latest["body_text"])[:200],
                    "last_direction": str(latest["direction"]),
                }
            )
        return results

    def search_messages(
        self,
        *,
        account_id: str,
        query: str,
        conversation_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        """Search message text with bounded, read-only LIKE queries."""
        query = _bounded_text(query, MAX_QUERY_CHARS)
        if not 1 <= limit <= 50:
            raise ValueError("limit must be between 1 and 50")
        needle = f"%{query}%"
        sql = (
            "SELECT message_id,conversation_id,sender_id,body_text,captured_at,direction,"
            "delivery_state,event_time_context_json FROM source_messages "
            "WHERE account_id=? AND deleted_at IS NULL AND expires_at>? AND body_text LIKE ?"
        )
        params: list[object] = [account_id, _now(), needle]
        if conversation_id is not None:
            sql += " AND conversation_id=?"
            params.append(conversation_id)
        sql += " ORDER BY captured_at DESC,message_id DESC LIMIT ?"
        params.append(limit)
        rows = self.db.execute(sql, params).fetchall()
        return [self.message_dict(row) for row in reversed(rows)]

    def messages_for_idle_event(
        self, *, account_id: str, limit: int = 100
    ) -> list[dict[str, object]]:
        """Return the account's latest messages in chronological order."""
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        now_text = _now()
        rows = self.db.execute(
            "SELECT message_id,conversation_id,sender_id,body_text,captured_at,direction,"
            "delivery_state,event_time_context_json FROM source_messages "
            "WHERE account_id=? AND deleted_at IS NULL AND expires_at>? "
            "AND captured_at<=? ORDER BY captured_at DESC,message_id DESC LIMIT ?",
            (account_id, now_text, now_text, limit),
        ).fetchall()
        return [self.message_dict(row) for row in reversed(rows)]

    @staticmethod
    def message_dict(row: sqlite3.Row) -> dict[str, object]:
        try:
            event_time = json.loads(row["event_time_context_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            event_time = {}
        if not isinstance(event_time, dict):
            event_time = {}
        event_time.pop("event_utc_minute", None)
        if not event_time.get("local_minute"):
            captured = datetime.fromisoformat(row["captured_at"]).astimezone(
                timezone(timedelta(hours=8)),
            )
            event_time["local_minute"] = captured.strftime("%Y-%m-%d %H:%M")
            event_time.setdefault("timezone", "Asia/Shanghai")
        return {
            "message_id": str(row["message_id"]),
            "conversation_id": str(row["conversation_id"]),
            "sender_id": str(row["sender_id"]),
            "body_text": str(row["body_text"]),
            "captured_at": str(row["captured_at"]),
            "direction": str(row["direction"]),
            "delivery_state": row["delivery_state"],
            "event_time_context": event_time,
            "source_is_group": is_group_conversation(str(row["conversation_id"])),
        }

    _message_dict = message_dict

    def reconstruct_message(self, message_id: str, account_id: str) -> dict:
        row = self.db.execute(
            "SELECT raw_json,raw_sha256 FROM source_messages "
            "WHERE message_id=? AND account_id=? AND deleted_at IS NULL",
            (message_id, account_id),
        ).fetchone()
        if row is None:
            raise KeyError("source message unavailable")
        if hashlib.sha256(row["raw_json"].encode("utf-8")).hexdigest() != row["raw_sha256"]:
            raise ValueError("source message integrity check failed")
        return json.loads(row["raw_json"])

    def find_message_by_platform_id(
        self, *, account_id: str, conversation_id: str, platform_message_id: str
    ) -> dict | None:
        """Resolve one quoted source by exact platform ID and conversation scope."""
        if (
            not account_id
            or not conversation_id
            or not platform_message_id
            or len(platform_message_id) > 32
            or not platform_message_id.isascii()
            or not platform_message_id.isdecimal()
            or not platform_message_id.strip("0")
        ):
            return None
        now_text = datetime.now(UTC).isoformat()
        row = self.db.execute(
            "SELECT message_id,platform_message_id,account_id,conversation_id,sender_id,"
            "body_text,captured_at,direction,delivery_state,event_time_context_json "
            "FROM source_messages WHERE account_id=? AND conversation_id=? "
            "AND platform_message_id=? AND deleted_at IS NULL AND expires_at>?",
            (account_id, conversation_id, platform_message_id, now_text),
        ).fetchone()
        if row is None:
            return None
        try:
            event_time = json.loads(row["event_time_context_json"] or "{}")
        except (TypeError, json.JSONDecodeError):
            event_time = {}
        if not isinstance(event_time, dict):
            event_time = {}
        event_time.pop("event_utc_minute", None)
        if not event_time.get("local_minute"):
            captured = datetime.fromisoformat(row["captured_at"]).astimezone(
                timezone(timedelta(hours=8)),
            )
            event_time["local_minute"] = captured.strftime("%Y-%m-%d %H:%M")
            event_time.setdefault("timezone", "Asia/Shanghai")
        return {
            "message_id": row["message_id"],
            "platform_message_id": row["platform_message_id"],
            "account_id": row["account_id"],
            "conversation_id": row["conversation_id"],
            "sender_id": row["sender_id"],
            "body_text": row["body_text"],
            "captured_at": row["captured_at"],
            "direction": row["direction"],
            "delivery_state": row["delivery_state"],
            "event_time_context": event_time,
        }

    def _previous_message_times_many(
        self,
        *,
        account_id: str,
        requests: Sequence[tuple[str, str, str, str]],
    ) -> dict[str, tuple[str | None, str | None]]:
        """Fetch prior chat/sender timestamps for a context batch in one SQL round trip."""
        unique: dict[str, tuple[str, str, str]] = {}
        for message_id, conversation_id, sender_id, captured_at in requests:
            if message_id and conversation_id and sender_id and captured_at:
                unique.setdefault(message_id, (conversation_id, sender_id, captured_at))
        if not unique:
            return {}
        values = ",".join("(?,?,?,?)" for _ in unique)
        parameters: list[str] = []
        for message_id, (conversation_id, sender_id, captured_at) in unique.items():
            parameters.extend((message_id, conversation_id, sender_id, captured_at))
        parameters.extend((account_id, account_id))
        with self._lock:
            rows = self.db.execute(
                "WITH requested(message_id,conversation_id,sender_id,captured_at) AS (VALUES "
                + values
                + ") "
                "SELECT r.message_id, "
                "(SELECT p.captured_at FROM source_messages p WHERE p.account_id=? "
                "AND p.conversation_id=r.conversation_id AND p.captured_at<r.captured_at "
                "AND p.deleted_at IS NULL AND (p.expires_at='' OR p.expires_at>r.captured_at) "
                "ORDER BY p.captured_at DESC,p.message_id DESC LIMIT 1) AS prior_chat, "
                "(SELECT p.captured_at FROM source_messages p WHERE p.account_id=? "
                "AND p.conversation_id=r.conversation_id AND p.sender_id=r.sender_id "
                "AND p.captured_at<r.captured_at AND p.deleted_at IS NULL "
                "AND (p.expires_at='' OR p.expires_at>r.captured_at) "
                "ORDER BY p.captured_at DESC,p.message_id DESC LIMIT 1) AS prior_sender "
                "FROM requested r",
                parameters,
            ).fetchall()
        return {
            str(row["message_id"]): (row["prior_chat"], row["prior_sender"]) for row in rows
        }

    def minutes_since_last_human_message(
        self, *, account_id: str, conversation_id: str
    ) -> int | None:
        row = self.db.execute(
            "SELECT captured_at FROM source_messages WHERE account_id=? AND conversation_id=? "
            "AND sender_id<>? AND direction='inbound' AND deleted_at IS NULL "
            "AND (expires_at='' OR expires_at>?) ORDER BY captured_at DESC,message_id DESC LIMIT 1",
            (account_id, conversation_id, account_id, _now()),
        ).fetchone()
        if row is None:
            return None
        stamp = datetime.fromisoformat(str(row[0]))
        return max(0, int((datetime.now(UTC) - stamp.astimezone(UTC)).total_seconds()) // 60)

    def count_effective_messages(
        self, *, account_id: str, conversation_id: str, since: datetime
    ) -> int:
        """Count archived chat messages in a Session window, excluding invalid sends."""
        if since.tzinfo is None:
            raise ValueError("since must be timezone-aware")
        now = _now()
        row = self.db.execute(
            "SELECT count(*) FROM source_messages WHERE account_id=? AND conversation_id=? "
            "AND captured_at>=? AND deleted_at IS NULL AND expires_at>? "
            "AND direction IN ('inbound','outbound') "
            "AND (delivery_state IS NULL OR delivery_state NOT IN ('rejected','failed'))",
            (account_id, conversation_id, since.astimezone(UTC).isoformat(), now),
        ).fetchone()
        return int(row[0])

    def message_belongs_to(
        self, *, message_id: str, account_id: str, conversation_id: str
    ) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages WHERE message_id=? AND account_id=? "
                "AND conversation_id=? AND deleted_at IS NULL",
                (message_id, account_id, conversation_id),
            ).fetchone()
            is not None
        )

    def message_conversation(
        self, *, message_id: str, account_id: str
    ) -> str | None:
        """Return the active conversation ID for an account-archived message, if present."""
        if not message_id or not account_id:
            return None
        row = self.db.execute(
            "SELECT conversation_id FROM source_messages WHERE message_id=? AND account_id=? "
            "AND deleted_at IS NULL AND expires_at>?",
            (message_id, account_id, _now()),
        ).fetchone()
        return str(row["conversation_id"]) if row is not None else None

    def idle_curation_snapshot(
        self, *, account_id: str, since_at: str | None
    ) -> dict[str, object]:
        """Collect objective message deltas and active memory inventory for idle runs."""
        now_dt = datetime.now(UTC)
        now_text = now_dt.isoformat()
        since_iso: str | None = None
        if since_at:
            try:
                parsed = datetime.fromisoformat(since_at)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=UTC)
                since_iso = parsed.astimezone(UTC).isoformat()
            except ValueError:
                since_iso = None

        clauses = [
            "s.account_id=?",
            "s.direction='inbound'",
            "s.deleted_at IS NULL",
            "s.expires_at>?",
            "s.captured_at<=?",
        ]
        params: list[object] = [account_id, now_text, now_text]
        if since_iso is not None:
            clauses.append("s.captured_at>?")
            params.append(since_iso)
        where = " AND ".join(clauses)

        with self._lock:
            conv_rows = self.db.execute(
                f"SELECT s.conversation_id, COUNT(*) AS cnt, MAX(s.captured_at) AS latest_at "
                f"FROM source_messages s WHERE {where} "
                f"GROUP BY s.conversation_id ORDER BY cnt DESC, latest_at DESC",
                params,
            ).fetchall()
            new_total = sum(int(row["cnt"]) for row in conv_rows)
            unannotated = int(
                self.db.execute(
                    f"SELECT COUNT(*) FROM source_messages s WHERE {where} "
                    "AND NOT EXISTS ("
                    "SELECT 1 FROM annotations a "
                    "WHERE a.account_id=s.account_id AND a.message_id=s.message_id "
                    "AND a.state='active'"
                    ")",
                    params,
                ).fetchone()[0]
            )
            counts: dict[str, int] = {
                "annotations": int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM annotations WHERE account_id=? AND state='active'",
                        (account_id,),
                    ).fetchone()[0]
                ),
                "user_facts": int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM user_facts WHERE account_id=? AND state='active'",
                        (account_id,),
                    ).fetchone()[0]
                ),
                "sender_memories": int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM sender_memories "
                        "WHERE account_id=? AND state='active'",
                        (account_id,),
                    ).fetchone()[0]
                ),
                "entity_terms": int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM entity_terms WHERE account_id=? AND state='active'",
                        (account_id,),
                    ).fetchone()[0]
                ),
                "event_book_active": 0,
                "event_book_stale_72h": 0,
            }
            has_event_book = (
                self.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name='event_book'"
                ).fetchone()
                is not None
            )
            if has_event_book:
                counts["event_book_active"] = int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM event_book WHERE account_id=? AND state='active' "
                        "AND (expires_at IS NULL OR expires_at>?)",
                        (account_id, now_text),
                    ).fetchone()[0]
                )
                stale_cutoff = (now_dt - timedelta(hours=72)).isoformat()
                counts["event_book_stale_72h"] = int(
                    self.db.execute(
                        "SELECT COUNT(*) FROM event_book WHERE account_id=? AND state='active' "
                        "AND priority='ordinary' AND updated_at<=? "
                        "AND (expires_at IS NULL OR expires_at>?)",
                        (account_id, stale_cutoff, now_text),
                    ).fetchone()[0]
                )
        return {
            "since_at": since_iso,
            "new_human_messages": new_total,
            "unannotated_new_messages": unannotated,
            "by_conversation": [
                {
                    "conversation_id": str(row["conversation_id"]),
                    "count": int(row["cnt"]),
                    "latest_at": str(row["latest_at"]),
                }
                for row in conv_rows[:5]
            ],
            "active_counts": counts,
        }

    def message_exists_for_sender(self, account_id: str, sender_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages WHERE account_id=? AND sender_id=? "
                "AND deleted_at IS NULL LIMIT 1",
                (account_id, sender_id),
            ).fetchone()
            is not None
        )

    def save_jev_labels(
        self,
        *,
        account_id: str,
        labels: Sequence[Mapping[str, object]],
    ) -> int:
        """Persist strategy probabilities against the exact archived inbound messages."""
        if not account_id:
            raise ValueError("account ID is required")
        now = _now()
        saved = 0
        with self._transaction():
            for label in labels:
                message_id = label.get("message_id")
                probabilities = label.get("probabilities")
                if not isinstance(message_id, str) or not message_id:
                    continue
                if not isinstance(probabilities, Mapping):
                    raise ValueError("Jev label requires strategy probabilities")  # noqa: TRY004
                values: dict[str, int] = {}
                for key in ("reply", "concise", "no_reply"):
                    value = probabilities.get(key)
                    if (
                        not isinstance(value, int)
                        or isinstance(value, bool)
                        or not 0 <= value <= 100
                    ):
                        raise ValueError("Jev strategy probability is invalid")
                    values[key] = value
                if sum(values.values()) != 100:
                    raise ValueError("Jev strategy probabilities must sum to 100")
                multi = label.get("multi_turn_pct", 0)
                if not isinstance(multi, int) or isinstance(multi, bool) or not 0 <= multi <= 100:
                    raise ValueError("Jev multi-turn probability is invalid")
                archived = self.db.execute(
                    "SELECT direction FROM source_messages WHERE message_id=? AND account_id=? "
                    "AND deleted_at IS NULL",
                    (message_id, account_id),
                ).fetchone()
                if archived is None or str(archived["direction"]) != "inbound":
                    continue
                self.db.execute(
                    "INSERT INTO message_jev_labels "
                    "(account_id,message_id,reply_pct,concise_pct,no_reply_pct,multi_turn_pct,"
                    "model,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(account_id,message_id) DO UPDATE SET "
                    "reply_pct=excluded.reply_pct,concise_pct=excluded.concise_pct,"
                    "no_reply_pct=excluded.no_reply_pct,multi_turn_pct=excluded.multi_turn_pct,"
                    "model=excluded.model,updated_at=excluded.updated_at",
                    (
                        account_id,
                        message_id,
                        values["reply"],
                        values["concise"],
                        values["no_reply"],
                        multi,
                        str(label.get("model") or "gemini-decision-1"),
                        now,
                        now,
                    ),
                )
                saved += 1
        return saved

    def jev_labels_for_messages(
        self, *, account_id: str, message_ids: Sequence[str]
    ) -> dict[str, dict[str, object]]:
        if not message_ids:
            return {}
        placeholders = ",".join("?" for _ in message_ids)
        with self._lock:
            rows = self.db.execute(
                "SELECT message_id,reply_pct,concise_pct,no_reply_pct,multi_turn_pct,"
                "model,updated_at "
                f"FROM message_jev_labels WHERE account_id=? AND message_id IN ({placeholders})",
                (account_id, *message_ids),
            ).fetchall()
        return {
            str(row["message_id"]): {
                "probabilities": {
                    "reply": int(row["reply_pct"]),
                    "concise": int(row["concise_pct"]),
                    "no_reply": int(row["no_reply_pct"]),
                },
                "multi_turn_pct": int(row["multi_turn_pct"]),
                "model": str(row["model"]),
                "labeled_at": str(row["updated_at"]),
            }
            for row in rows
        }

    def soft_delete_expired(self, *, now: str) -> list[str]:
        """Soft-delete expired source_messages inside the caller's transaction."""
        rows = self.db.execute(
            "SELECT message_id FROM source_messages WHERE expires_at<=? AND deleted_at IS NULL",
            (now,),
        ).fetchall()
        message_ids = [str(row["message_id"]) for row in rows]
        for message_id in message_ids:
            self.db.execute(
                "UPDATE source_messages SET deleted_at=? WHERE message_id=?",
                (now, message_id),
            )
        return message_ids

    def _message_in_scope(self, message_id: str, account_id: str) -> bool:
        return (
            self.db.execute(
                "SELECT 1 FROM source_messages "
                "WHERE message_id=? AND account_id=? AND deleted_at IS NULL",
                (message_id, account_id),
            ).fetchone()
            is not None
        )

    def _conversation_for_message(self, message_id: str, account_id: str) -> str:
        row = self.db.execute(
            "SELECT conversation_id FROM source_messages WHERE message_id=? AND account_id=?",
            (message_id, account_id),
        ).fetchone()
        if row is None:
            raise KeyError("source message unavailable")
        return str(row["conversation_id"])
