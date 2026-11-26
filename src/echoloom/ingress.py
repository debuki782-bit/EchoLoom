"""Thin loopback HTTP ingress and durable inbound admission for WeChat Agent."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
from collections import deque
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request

from .connectors.base import connector_in_scope, connector_normalize_event
from .contacts import is_contact_id, is_room_id
from .conversation_scheduler import ConversationScheduler
from .db_async import db_lock, register_thread_safe_db, run_db, unregister_thread_safe_db
from .gateway_ingress import bounded_body as _bounded_body
from .rich_message import normalize_message
from .temporal_context import message_time_context
from .wechat_gateway import MessageEvent, default_event_parser

logger = logging.getLogger(__name__)


def sha(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:12]


def read_attempt_counts(db: sqlite3.Connection) -> dict[str, int]:
    """Aggregate audit attempt counts, folding legacy sent_by_agent_tool."""
    attempts = {
        str(row[0]): int(row[1])
        for row in db.execute("SELECT state,count(*) FROM attempts GROUP BY state")
    }
    legacy_tool_sends = attempts.pop("sent_by_agent_tool", 0)
    if legacy_tool_sends:
        attempts["agent_tool_send_unconfirmed"] = (
            attempts.get("agent_tool_send_unconfirmed", 0) + legacy_tool_sends
        )
    return attempts


def _ensure_control_and_audit_tables(db: sqlite3.Connection) -> None:
    db.executescript(
        "CREATE TABLE IF NOT EXISTS attempts (id TEXT PRIMARY KEY,state TEXT);"
        "CREATE TABLE IF NOT EXISTS control (id INTEGER PRIMARY KEY,paused INTEGER);"
        "INSERT OR IGNORE INTO control VALUES (1,0);"
    )
    db.commit()


class Ingress:
    """Thin inbound admission layer owning scope checks, pause flag, and inbox writes.

    Supports both in-process Pilot composition and standalone Scheme A operation
    where `/status` is synthesized from `worker_heartbeat` + `pilot_inbox`.
    """

    def __init__(
        self,
        db_or_path: sqlite3.Connection | Path | str,
        account: str,
        room: str,
        *,
        scheduler: ConversationScheduler | None = None,
        mode: str = "authorized_test_group_pilot",
        limit: int | None = None,
        process_timeout_seconds: float = 180.0,
        registered_group_ids: set[str] | None = None,
        admit_group: Callable[[str], bool] | None = None,
        parse_events: Callable[[object], tuple[MessageEvent, ...]] = default_event_parser,
        normalize_event: Callable[[MessageEvent], MessageEvent | None] | None = None,
        in_scope_check: Callable[[MessageEvent], bool] | None = None,
        registry: Any = None,
        heartbeat_max_age_seconds: float = 10.0,
        on_queued: Callable[[MessageEvent], None] | None = None,
        on_paused: Callable[[MessageEvent], None] | None = None,
        on_inbound_followup: Callable[[MessageEvent], None] | None = None,
        queue_depth_provider: Callable[[], int] | None = None,
        recover_running: bool = False,
    ) -> None:
        self._owns_db = isinstance(db_or_path, (Path, str))
        if isinstance(db_or_path, (Path, str)):
            raw = str(db_or_path)
            if raw != ":memory:":
                Path(raw).parent.mkdir(parents=True, exist_ok=True)
            self.db = sqlite3.connect(raw, check_same_thread=False)
            self._lock = register_thread_safe_db(self.db, owner=self)
            self.db.execute("PRAGMA journal_mode=WAL")
            self.db.execute("PRAGMA synchronous=NORMAL")
            self.db.execute("PRAGMA busy_timeout=5000")
        else:
            self.db = db_or_path
            self._lock = db_lock(self.db) or threading.RLock()
        _ensure_control_and_audit_tables(self.db)
        self.scheduler = (
            scheduler
            if scheduler is not None
            else ConversationScheduler(self.db, recover_running=recover_running)
        )
        self.account = account
        self.room = room
        self.mode = mode
        self.limit = limit
        self.process_timeout_seconds = float(process_timeout_seconds)
        self.registered_group_ids = set(registered_group_ids or ())
        self.admit_group = admit_group
        self.parse_events = parse_events
        self._normalize_event = normalize_event
        self._in_scope_check = in_scope_check
        self.registry = registry
        self.heartbeat_max_age_seconds = float(heartbeat_max_age_seconds)
        self.on_queued = on_queued
        self.on_paused = on_paused
        self.on_inbound_followup = on_inbound_followup
        self._queue_depth_provider = queue_depth_provider
        self.events: deque[dict[str, object]] = deque(maxlen=16)
        self.received = 0

    def close(self) -> None:
        if self._owns_db:
            unregister_thread_safe_db(self.db)
            self.db.close()

    @property
    def paused(self) -> bool:
        with self._lock:
            return bool(self.db.execute("SELECT paused FROM control WHERE id=1").fetchone()[0])

    def set_paused(self, paused: bool) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE control SET paused=? WHERE id=1", (int(bool(paused)),))

    def in_scope(self, item: MessageEvent) -> bool:
        if self._in_scope_check is not None:
            return bool(self._in_scope_check(item))
        if self.registry is not None:
            src = self.registry.for_event(item)
            return connector_in_scope(
                src.connector,
                item,
                account=self.account,
                default_room=self.room,
                registered_group_ids=self.registered_group_ids,
            )
        if item.account_id != self.account:
            return False
        if item.is_group:
            if item.conversation_id == self.room:
                return True
            if item.conversation_id in self.registered_group_ids:
                return is_contact_id(item.sender_id) or item.sender_id == self.account
            return is_room_id(item.conversation_id) and is_contact_id(item.sender_id)
        return is_contact_id(item.conversation_id)

    def _offer_result(self, item: MessageEvent, result: str) -> str:
        queue_depth = (
            self._queue_depth_provider()
            if self._queue_depth_provider is not None
            else self.scheduler.active_count()
        )
        logger.info(
            "event=inbound_offer result=%s account_sha=%s conversation_sha=%s "
            "sender_sha=%s is_group=%s message_type=%s original_type=%s text_chars=%s "
            "source_id_present=%s queue_depth=%s",
            result,
            sha(item.account_id),
            sha(item.conversation_id),
            sha(item.sender_id),
            item.is_group,
            item.message_type,
            item.original_message_type,
            len(item.text),
            item.source_msg_id is not None,
            queue_depth,
        )
        return result

    def offer(self, item: MessageEvent) -> str:
        """Validate, normalize, persist into pilot_inbox, and notify hooks."""
        self.received += 1
        if not self.in_scope(item):
            return self._offer_result(item, "outside_test_scope")
        if self.paused:
            self.events.append({"room_sha": sha(item.conversation_id), "state": "paused"})
            return self._offer_result(item, "paused")
        item = replace(item, event_time_context=message_time_context(item.capture_tick_ms))
        self.events.append(
            {
                "room_sha": sha(item.conversation_id),
                "sender_sha": sha(item.sender_id),
                "msgtype": item.message_type,
            }
        )
        if item.sender_id in (self.account, self.room):
            return self._offer_result(item, "observation_only")
        if self._normalize_event is not None:
            normalized = self._normalize_event(item)
        elif self.registry is not None:
            src = self.registry.for_event(item)
            normalized = connector_normalize_event(src.connector, item)
        elif item.channel != "wechat" and item.message_type == 1:
            normalized = item
        else:
            normalized = normalize_message(item)
        if normalized is None:
            return self._offer_result(item, "observation_only")
        item = normalized
        if (
            item.is_group
            and item.conversation_id != self.room
            and item.conversation_id not in self.registered_group_ids
        ):
            try:
                admitted = (
                    self.admit_group is not None
                    and self.admit_group(item.conversation_id)
                )
            except OSError as exc:
                logger.warning(
                    "event=group_route_persist_failed conversation_sha=%s error_type=%s",
                    sha(item.conversation_id),
                    type(exc).__name__,
                )
                return self._offer_result(item, "group_route_persist_failed")
            if not admitted:
                return self._offer_result(item, "unverified_group")
            self.registered_group_ids.add(item.conversation_id)
            logger.info(
                "event=group_route_admitted conversation_sha=%s sender_sha=%s",
                sha(item.conversation_id),
                sha(item.sender_id),
            )
        if self.on_inbound_followup is not None:
            self.on_inbound_followup(item)
        if item.text.strip() == "/ai off":
            self.set_paused(True)
            if self.on_paused is not None:
                self.on_paused(item)
            return self._offer_result(item, "paused")
        try:
            result, receipt = self.scheduler.enqueue(item)
        except sqlite3.Error as exc:
            logger.error("event=inbox_persist_failed error_type=%s", type(exc).__name__)
            return self._offer_result(item, "storage_error")
        if result == "backpressure":
            return self._offer_result(item, "busy")
        if result == "oversized":
            return self._offer_result(item, "oversized")
        if result == "queued" and self.on_queued is not None:
            self.on_queued(item)
        logger.info("event=inbox_saved receipt=%s disposition=%s", receipt[:12], result)
        if result == "duplicate":
            return self._offer_result(item, "duplicate")
        return self._offer_result(item, "queued_test")

    def recover_archived_backlog(
        self,
        memory_store: Any,
        *,
        on_existing_active: Callable[[MessageEvent], None] | None = None,
        max_age_hours: int = 24,
    ) -> dict[str, int]:
        """Re-enqueue legacy un-replied inbound tails not represented in the durable inbox."""
        if memory_store is None or not 1 <= max_age_hours <= 24 * 30:
            return {"scanned": 0, "queued": 0, "existing": 0, "skipped": 0}
        since = datetime.now(UTC) - timedelta(hours=max_age_hours)
        rows = memory_store.recent_unanswered_inbound(
            account_id=self.account,
            since=since,
            limit=self.scheduler.MAX_PENDING_MESSAGES,
        )
        counts = {"scanned": len(rows), "queued": 0, "existing": 0, "skipped": 0}
        for row in rows:
            try:
                envelope = json.loads(str(row["raw_json"]))
                observations = self.parse_events(envelope)
            except (TypeError, ValueError, json.JSONDecodeError):
                counts["skipped"] += 1
                continue
            matched = False
            for item in observations:
                if (
                    item.account_id != self.account
                    or item.conversation_id != row["conversation_id"]
                    or item.sender_id != row["sender_id"]
                ):
                    continue
                source_record = json.loads(item.raw_message_json) if item.raw_message_json else {}
                fingerprint_source = json.dumps(
                    source_record if isinstance(source_record, dict) else envelope,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                fingerprint_material = (
                    f"{self.account}\0{item.conversation_id}\0{item.sender_id}\0"
                    f"{fingerprint_source}"
                )
                fingerprint = hashlib.sha256(fingerprint_material.encode()).hexdigest()
                if row["source_fingerprint"] and fingerprint != row["source_fingerprint"]:
                    continue
                if row["platform_message_id"] and str(item.source_msg_id or "") != str(
                    row["platform_message_id"]
                ):
                    continue
                matched = True
                if not self.in_scope(item):
                    counts["skipped"] += 1
                    break
                existing_state = self.scheduler.receipt_state(item)
                if existing_state in ("pending", "running") and on_existing_active is not None:
                    on_existing_active(item)
                result = self.offer(item)
                if result == "queued_test":
                    counts["queued"] += 1
                elif result in ("duplicate", "observation_only"):
                    counts["existing"] += 1
                else:
                    counts["skipped"] += 1
                break
            if not matched:
                counts["skipped"] += 1
        if counts["queued"] or counts["skipped"]:
            logger.info(
                "event=legacy_backlog_recovery scanned=%s queued=%s existing=%s skipped=%s "
                "lookback_hours=%s",
                counts["scanned"],
                counts["queued"],
                counts["existing"],
                counts["skipped"],
                max_age_hours,
            )
        return counts

    def status(self, *, now: datetime | None = None) -> dict[str, object]:
        """Synthesize full /status JSON from worker_heartbeat + pilot_inbox (Scheme A)."""
        current = now or datetime.now(UTC)
        with self._lock:
            hb = self.scheduler.read_worker_heartbeat(
                max_age_seconds=self.heartbeat_max_age_seconds,
                now=current,
            )
            paused = self.paused
            queue = self.scheduler.active_count()
            queue_incl_failed = self.scheduler.pending_count()
            inbox_states = self.scheduler.state_counts()
            inbox_failures = self.scheduler.failure_counts()
            deferred_counts = self.scheduler.deferred_counts()
            budget_blocked = self.scheduler.budget_blocked_count()
            attempts = read_attempt_counts(self.db)
        worker_alive = bool(hb is not None and hb.get("alive"))
        in_flight: dict[str, object] | None = None
        if worker_alive and hb is not None and isinstance(hb.get("in_flight"), dict):
            raw_flight = hb["in_flight"]
            elapsed = raw_flight.get("elapsed_seconds")
            started_at = raw_flight.get("started_at")
            if isinstance(started_at, str):
                try:
                    started_dt = datetime.fromisoformat(started_at)
                    if started_dt.tzinfo is None:
                        started_dt = started_dt.replace(tzinfo=UTC)
                    elapsed = round(
                        max(
                            0.0,
                            (current.astimezone(UTC) - started_dt.astimezone(UTC)).total_seconds(),
                        ),
                        1,
                    )
                except ValueError:
                    pass
            in_flight = {
                "sender_sha": raw_flight.get("sender_sha"),
                "message_type": raw_flight.get("message_type"),
                "elapsed_seconds": 0.0 if elapsed is None else elapsed,
            }
        return {
            "mode": self.mode,
            "tool_control_version": 1,
            "trusted_gateway": bool(hb.get("wechat_ready")) if (worker_alive and hb) else False,
            "provider_ready": bool(hb.get("provider_ready")) if (worker_alive and hb) else False,
            "worker_alive": worker_alive,
            "login_watch_alive": worker_alive,
            "paused": paused,
            "received": self.received,
            "last_events": list(self.events),
            "queue": queue,
            "queue_including_failed": queue_incl_failed,
            "scheduled_followups": (
                int(hb.get("scheduled_followups") or 0) if (worker_alive and hb) else 0
            ),
            "in_flight": in_flight,
            "inbox_states": inbox_states,
            "inbox_failures": inbox_failures,
            "deferred_counts": deferred_counts,
            "queue_capacity": self.scheduler.MAX_PENDING_MESSAGES,
            "model_budget_blocked_batches": budget_blocked,
            "process_timeout_seconds": self.process_timeout_seconds,
            "limit": self.limit,
            "attempts": attempts,
        }


def create_ingress_app(
    ingress: Ingress,
    *,
    status_provider: Callable[[], dict[str, object]] | None = None,
    lifespan: Any = None,
) -> FastAPI:
    """Create the loopback-only FastAPI app for /health, /status, and /callback."""
    app = FastAPI(lifespan=lifespan, docs_url=None, redoc_url=None)

    @app.get("/health")
    async def health():
        return {"status": "ready", "mode": ingress.mode}

    @app.get("/status")
    async def status():
        if status_provider is not None:
            return status_provider()
        return await run_db(ingress.status, db=ingress.db, lock=ingress._lock)

    @app.get("/sources")
    async def list_sources():
        if ingress.registry is None:
            return {"sources": [{"source_id": "default", "channel": "default", "capabilities": []}]}
        return {
            "default_source": ingress.registry.default_source_id,
            "sources": [
                {
                    "source_id": src.source_id,
                    "channel": src.channel,
                    "capabilities": sorted(src.connector.capabilities),
                }
                for src in ingress.registry.ordered_sources()
            ],
        }

    def _offer_batch(items: tuple[MessageEvent, ...]) -> dict[str, object]:
        results = [ingress.offer(item) for item in items]
        if any(result == "storage_error" for result in results):
            raise HTTPException(503, "inbound message persistence unavailable")
        if any(result == "busy" for result in results):
            raise HTTPException(503, "inbound message backlog is full")
        if any(result == "oversized" for result in results):
            raise HTTPException(413, "inbound message exceeds persistence limit")
        logger.info(
            "event=hook_callback_received batch_size=%s queued=%s observation_only=%s rejected=%s",
            len(items),
            results.count("queued_test"),
            results.count("observation_only"),
            len(results) - results.count("queued_test") - results.count("observation_only"),
        )
        return {"accepted": len(items), "results": results}

    @app.post("/callback")
    async def callback(request: Request):
        if request.client is None or request.client.host not in ("127.0.0.1", "::1"):
            raise HTTPException(403, "loopback only")
        raw_body = await _bounded_body(request)
        if ingress.registry is not None:
            default_src = ingress.registry.default()
            if not ingress.registry.verify_request(default_src.source_id, request.headers, raw_body):
                raise HTTPException(401, "invalid source authentication")
        try:
            items = ingress.parse_events(json.loads(raw_body))
        except (ValueError, TypeError) as exc:
            logger.warning("event=hook_callback_rejected error_type=%s", type(exc).__name__)
            raise HTTPException(400, "invalid callback") from None
        return await run_db(_offer_batch, items, db=ingress.db, lock=ingress._lock)

    @app.post("/sources/{source_id}/events")
    async def source_events(source_id: str, request: Request):
        registry = ingress.registry
        if registry is None:
            raise HTTPException(404, "multi-source registry unavailable")
        src = registry.get(source_id)
        if src is None:
            raise HTTPException(404, "unknown source")
        raw_body = await _bounded_body(request)
        has_token = bool(
            getattr(src.connector, "auth_token", "")
            or getattr(src.connector, "hmac_secret", "")
        )
        is_loopback = request.client is not None and request.client.host in ("127.0.0.1", "::1")
        if has_token:
            if not registry.verify_request(source_id, request.headers, raw_body):
                raise HTTPException(401, "invalid source authentication")
        elif not is_loopback:
            raise HTTPException(403, "loopback or token auth required")
        try:
            payload = json.loads(raw_body)
            raw_items = src.connector.parse_events(payload)
            items = tuple(
                replace(ev, source_id=src.source_id, channel=ev.channel or src.channel)
                for ev in raw_items
            )
            for item in items:
                registry.remember_conversation(item.account_id, item.conversation_id, src.source_id)
        except (ValueError, TypeError) as exc:
            logger.warning("event=source_callback_rejected source_id=%s error_type=%s", source_id, type(exc).__name__)
            raise HTTPException(400, "invalid source payload") from None
        response = await run_db(_offer_batch, items, db=ingress.db, lock=ingress._lock)
        response["source_id"] = src.source_id
        return response

    return app
