"""Local dashboard REST endpoints for inspecting and maintaining EchoLoom memory stores."""

from __future__ import annotations

import asyncio
import sqlite3
from collections import defaultdict
from collections.abc import Callable, Iterator, Sequence
from contextlib import closing, contextmanager
from datetime import UTC, datetime
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from .contacts import ContactIdentity
from .event_book import EventBook
from .memory_store import MemoryStore, _like_escape

VALID_SENDER_TOPICS = frozenset({"身份关系", "称呼", "偏好", "互动习惯", "承诺", "其他"})
DASHBOARD_ACTOR = "dashboard-user"
FACT_INDEX_LIMIT = 40
EPISODE_BLOAT_THRESHOLD = 12
SENDER_BLOAT_THRESHOLD = 8


class SenderMemoryCreate(BaseModel):
    account_id: str
    sender_id: str
    topic: str
    content: str


class SenderTagsUpdate(BaseModel):
    account_id: str
    sender_id: str
    action: str = "add"
    tags: list[str]


class FactUpsert(BaseModel):
    account_id: str
    fact_key: str
    content: str
    source_message_id: str


class EventCreate(BaseModel):
    account_id: str
    priority: str = "ordinary"
    content: str
    exit_condition: str
    expires_at: str | None = None


class EventUpdate(BaseModel):
    account_id: str
    content: str | None = None
    exit_condition: str | None = None
    expires_at: str | None = None


class EntityUpsert(BaseModel):
    account_id: str
    term: str
    explanation: str


class AnnotationUpdate(BaseModel):
    account_id: str
    content: str


def require_local_mutation(request: Request) -> None:
    """Enforce same-origin + custom header guard on local dashboard mutations."""
    origin = request.headers.get("origin", "")
    if (
        origin != f"http://{request.headers.get('host', '')}"
        or request.headers.get("x-dashboard") != "1"
    ):
        raise HTTPException(403, "只接受本地管理页面的操作")


@contextmanager
def sqlite_error_guard(detail: str = "数据库正忙，请稍后重试") -> Iterator[None]:
    """Translate SQLite operational and lock errors into HTTP 503 responses."""
    try:
        yield
    except sqlite3.Error as exc:
        raise HTTPException(503, detail) from exc


def _episode_table_exists(db: sqlite3.Connection) -> bool:
    return (
        db.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='episodes'"
        ).fetchone()
        is not None
    )


def _list_accounts(store: MemoryStore, episode_db_path: Path | None) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def _add(account_id: str) -> None:
        cleaned = account_id.strip()
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            ordered.append(cleaned)

    with store._lock:
        for row in store.db.execute(
            "SELECT account_id FROM source_messages WHERE deleted_at IS NULL "
            "GROUP BY account_id ORDER BY MAX(captured_at) DESC"
        ).fetchall():
            _add(str(row[0]))
        for table in (
            "user_facts",
            "entity_terms",
            "sender_memories",
            "sender_tags",
            "sender_profiles",
            "contact_directory",
            "event_book",
        ):
            has_table = (
                store.db.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
                    (table,),
                ).fetchone()
                is not None
            )
            if not has_table:
                continue
            for row in store.db.execute(
                f"SELECT DISTINCT account_id FROM {table} ORDER BY account_id"
            ).fetchall():
                _add(str(row[0]))

    if episode_db_path is not None and episode_db_path.is_file():
        try:
            with closing(sqlite3.connect(episode_db_path, timeout=5.0)) as ep_db:
                if _episode_table_exists(ep_db):
                    for row in ep_db.execute(
                        "SELECT DISTINCT account_id FROM episodes ORDER BY account_id"
                    ).fetchall():
                        _add(str(row[0]))
        except sqlite3.Error:
            pass
    return ordered


def _resolve_account(
    store: MemoryStore, episode_db_path: Path | None, requested: str
) -> tuple[list[str], str]:
    accounts = _list_accounts(store, episode_db_path)
    cleaned = requested.strip()
    if cleaned:
        if cleaned not in accounts:
            accounts.append(cleaned)
        return accounts, cleaned
    return accounts, (accounts[0] if accounts else "")


def _episode_stats(episode_db_path: Path | None, account_id: str) -> tuple[bool, int, int]:
    if episode_db_path is None or not episode_db_path.is_file():
        return False, 0, 0
    try:
        with closing(sqlite3.connect(episode_db_path, timeout=5.0)) as ep_db:
            if not _episode_table_exists(ep_db):
                return False, 0, 0
            if not account_id:
                return True, 0, 0
            total = int(
                ep_db.execute(
                    "SELECT COUNT(*) FROM episodes WHERE account_id=?",
                    (account_id,),
                ).fetchone()[0]
            )
            bloated = int(
                ep_db.execute(
                    "SELECT COUNT(DISTINCT conversation_id) FROM ("
                    "SELECT conversation_id FROM episodes WHERE account_id=? "
                    "GROUP BY conversation_id, model HAVING COUNT(*) >= ?"
                    ")",
                    (account_id, EPISODE_BLOAT_THRESHOLD),
                ).fetchone()[0]
            )
            return True, total, bloated
    except sqlite3.Error:
        return False, 0, 0


def _source_previews(
    store: MemoryStore, account_id: str, message_ids: list[str]
) -> dict[str, dict[str, str]]:
    unique_ids = list(dict.fromkeys(mid for mid in message_ids if mid))
    if not unique_ids:
        return {}
    placeholders = ",".join("?" for _ in unique_ids)
    with store._lock:
        rows = store.db.execute(
            "SELECT message_id, conversation_id, sender_id, body_text, captured_at, direction "
            f"FROM source_messages WHERE account_id=? AND message_id IN ({placeholders})",
            (account_id, *unique_ids),
        ).fetchall()
    return {
        str(row["message_id"]): {
            "message_id": str(row["message_id"]),
            "conversation_id": str(row["conversation_id"]),
            "sender_id": str(row["sender_id"]),
            "body_text": str(row["body_text"])[:400],
            "captured_at": str(row["captured_at"]),
            "direction": str(row["direction"]),
        }
        for row in rows
    }


def format_human_display_name(remark: str, nickname: str) -> str:
    """Format a human-facing display label without AI sensitive-word masking."""
    clean_remark = remark.strip()
    clean_nick = nickname.strip()
    if clean_remark and clean_nick and clean_remark != clean_nick:
        return f"{clean_remark}（{clean_nick}）"
    return clean_remark or clean_nick


def _should_query_contact_resolver(contact_id: str) -> bool:
    cleaned = contact_id.strip()
    if not cleaned:
        return False
    return (
        not cleaned.startswith(("idle-event:", "Agent", "系统"))
        and cleaned != DASHBOARD_ACTOR
    )


async def resolve_live_nicknames(
    contact_ids: Sequence[str],
    contact_resolver: Callable[[str], ContactIdentity | None] | None,
) -> dict[str, str]:
    """Query live contact nicknames concurrently with bounded parallelism."""
    if contact_resolver is None:
        return {}
    unique_ids = list(
        dict.fromkeys(
            cid.strip() for cid in contact_ids if _should_query_contact_resolver(cid)
        )
    )
    if not unique_ids:
        return {}
    semaphore = asyncio.Semaphore(16)

    async def _lookup(cid: str) -> tuple[str, str]:
        def _call() -> str:
            try:
                identity = contact_resolver(cid)
            except (OSError, RuntimeError, ValueError, TypeError):
                return ""
            if identity and isinstance(identity.nickname, str):
                return identity.nickname.strip()
            return ""

        async with semaphore:
            return cid, await asyncio.to_thread(_call)

    pairs = await asyncio.gather(*(_lookup(cid) for cid in unique_ids))
    return {cid: nick for cid, nick in pairs if nick}


def _episode_conversation_ids(episode_db_path: Path | None, account_id: str) -> list[str]:
    if episode_db_path is None or not episode_db_path.is_file() or not account_id:
        return []
    try:
        with closing(sqlite3.connect(episode_db_path, timeout=5.0)) as ep_db:
            if not _episode_table_exists(ep_db):
                return []
            return [
                str(row[0])
                for row in ep_db.execute(
                    "SELECT DISTINCT conversation_id FROM episodes WHERE account_id=?",
                    (account_id,),
                ).fetchall()
                if str(row[0]).strip()
            ]
    except sqlite3.Error:
        return []


async def resolve_display_names(
    *,
    store: MemoryStore | None,
    account_ids: Sequence[str] | None = None,
    extra_ids: Sequence[str] = (),
    contact_resolver: Callable[[str], ContactIdentity | None] | None = None,
    episode_db_path: Path | None = None,
) -> dict[str, str]:
    """Build {id: display_label} from remarks, live resolver (write-through), and cache."""
    remarks: dict[str, str] = {}
    cached_nicknames: dict[str, str] = {}
    dir_by_acct: dict[str, dict[str, dict[str, str]]] = {}
    owner_accounts: dict[str, list[str]] = defaultdict(list)
    seen_ids: dict[str, None] = {}
    target_accounts: list[str] = []

    def _register(cid: str, acct: str | None = None) -> None:
        cleaned = cid.strip()
        if not cleaned:
            return
        seen_ids.setdefault(cleaned, None)
        if acct and acct not in owner_accounts[cleaned]:
            owner_accounts[cleaned].append(acct)

    if store is not None:
        if account_ids is not None:
            target_accounts = [a.strip() for a in account_ids if a.strip()]
        else:
            target_accounts = _list_accounts(store, episode_db_path)

        # Iterate oldest-to-newest so the most recently active account wins on collisions.
        for acct in reversed(target_accounts):
            _register(acct, acct)
            acct_remarks = store.all_sender_remarks(account_id=acct)
            acct_dir = store.contact_directory_map(account_id=acct)
            dir_by_acct[acct] = acct_dir
            for cid, remark in acct_remarks.items():
                _register(cid, acct)
                if remark.strip():
                    remarks[cid] = remark.strip()
            for cid, entry in acct_dir.items():
                _register(cid, acct)
                nick = str(entry.get("nickname") or "").strip()
                if nick:
                    cached_nicknames[cid] = nick
            with store._lock:
                msg_rows = store.db.execute(
                    "SELECT DISTINCT conversation_id, sender_id FROM source_messages "
                    "WHERE account_id=? AND deleted_at IS NULL",
                    (acct,),
                ).fetchall()
            for row in msg_rows:
                _register(str(row[0]), acct)
                _register(str(row[1]), acct)
            for ep_conv in _episode_conversation_ids(episode_db_path, acct):
                _register(ep_conv, acct)

    for extra in extra_ids:
        _register(extra)

    all_ids = list(seen_ids)
    live_nicknames = await resolve_live_nicknames(all_ids, contact_resolver)

    if store is not None and live_nicknames and target_accounts:
        pending_by_acct: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
        primary_acct = target_accounts[0]
        for cid, nick in live_nicknames.items():
            accts = owner_accounts.get(cid) or [primary_acct]
            for acct in accts:
                cached = str(dir_by_acct.get(acct, {}).get(cid, {}).get("nickname") or "").strip()
                if nick != cached:
                    pending_by_acct[acct].append((cid, nick, "hook"))
        for acct, entries in pending_by_acct.items():
            try:
                store.upsert_contact_directory_batch(account_id=acct, entries=entries)
            except (sqlite3.Error, ValueError):
                pass

    names: dict[str, str] = {}
    for cid in all_ids:
        label = format_human_display_name(
            remarks.get(cid, ""),
            live_nicknames.get(cid, "") or cached_nicknames.get(cid, ""),
        )
        if label:
            names[cid] = label
    return names


def register_memory_routes(
    app: FastAPI,
    *,
    get_memory_store: Callable[[], MemoryStore],
    get_event_book: Callable[[], EventBook],
    get_episode_db_path: Callable[[], Path | None],
    contact_resolver: Callable[[str], ContactIdentity | None] | None = None,
) -> None:
    """Attach local memory inspection and curation endpoints to the dashboard FastAPI app."""

    @app.get("/api/memory/summary")
    async def memory_summary(account_id: str = ""):
        store = get_memory_store()
        event_book = get_event_book()
        episode_path = get_episode_db_path()
        with sqlite_error_guard():
            accounts, selected = _resolve_account(store, episode_path, account_id)
            if not selected:
                ep_available, _, _ = _episode_stats(episode_path, "")
                return {
                    "accounts": accounts,
                    "account_id": "",
                    "names": {},
                    "counts": {
                        "facts_total": 0,
                        "facts_in_index": 0,
                        "fact_index_limit": FACT_INDEX_LIMIT,
                        "events_high": 0,
                        "events_high_limit": EventBook.HIGH_PRIORITY_SLOTS,
                        "events_ordinary": 0,
                        "events_ordinary_chars": 0,
                        "events_ordinary_limit": EventBook.ORDINARY_TOTAL_CHARS,
                        "events_stale_72h": 0,
                        "entities_total": 0,
                        "annotations_total": 0,
                        "sender_memories_total": 0,
                        "bloated_senders": 0,
                        "episodes_available": ep_available,
                        "episodes_total": 0,
                        "episodes_bloated_chats": 0,
                    },
                }

            events = event_book.list_entries(selected)
            now = datetime.now(UTC)
            high_events = [row for row in events if row["priority"] == "high"]
            ordinary_events = [row for row in events if row["priority"] == "ordinary"]
            stale_72h = sum(
                1
                for row in ordinary_events
                if EventBook._elapsed_seconds(row["updated_at"], now) >= 72 * 60 * 60
            )
            ordinary_chars = sum(len(str(row["content"])) for row in ordinary_events)

            with store._lock:
                facts_total = int(
                    store.db.execute(
                        "SELECT COUNT(*) FROM user_facts WHERE account_id=? AND state='active'",
                        (selected,),
                    ).fetchone()[0]
                )
                entities_total = int(
                    store.db.execute(
                        "SELECT COUNT(*) FROM entity_terms WHERE account_id=? AND state='active'",
                        (selected,),
                    ).fetchone()[0]
                )
                annotations_total = int(
                    store.db.execute(
                        "SELECT COUNT(*) FROM annotations WHERE account_id=? AND state='active'",
                        (selected,),
                    ).fetchone()[0]
                )
                sender_memories_total = int(
                    store.db.execute(
                        "SELECT COUNT(*) FROM sender_memories "
                        "WHERE account_id=? AND state='active'",
                        (selected,),
                    ).fetchone()[0]
                )
                bloated_senders = int(
                    store.db.execute(
                        "SELECT COUNT(*) FROM ("
                        "SELECT sender_id FROM sender_memories "
                        "WHERE account_id=? AND state='active' "
                        "GROUP BY sender_id HAVING COUNT(*) >= ?"
                        ")",
                        (selected, SENDER_BLOAT_THRESHOLD),
                    ).fetchone()[0]
                )

            indexed_keys = store.fact_key_index(account_id=selected, limit=FACT_INDEX_LIMIT)
            ep_available, ep_total, ep_bloated = _episode_stats(episode_path, selected)
            names = await resolve_display_names(
                store=store,
                account_ids=[selected],
                extra_ids=accounts,
                contact_resolver=contact_resolver,
                episode_db_path=episode_path,
            )
        return {
            "accounts": accounts,
            "account_id": selected,
            "names": names,
            "counts": {
                "facts_total": facts_total,
                "facts_in_index": len(indexed_keys),
                "fact_index_limit": FACT_INDEX_LIMIT,
                "events_high": len(high_events),
                "events_high_limit": EventBook.HIGH_PRIORITY_SLOTS,
                "events_ordinary": len(ordinary_events),
                "events_ordinary_chars": ordinary_chars,
                "events_ordinary_limit": EventBook.ORDINARY_TOTAL_CHARS,
                "events_stale_72h": stale_72h,
                "entities_total": entities_total,
                "annotations_total": annotations_total,
                "sender_memories_total": sender_memories_total,
                "bloated_senders": bloated_senders,
                "episodes_available": ep_available,
                "episodes_total": ep_total,
                "episodes_bloated_chats": ep_bloated,
            },
        }

    # --- Sender profile memories & tags ---

    @app.post("/api/senders/memories")
    def create_sender_memory(payload: SenderMemoryCreate, request: Request):
        require_local_mutation(request)
        topic = payload.topic.strip()
        if topic not in VALID_SENDER_TOPICS:
            raise HTTPException(400, "资料分类无效")
        try:
            with sqlite_error_guard():
                memory_id, created = get_memory_store().upsert_sender_memory(
                    account_id=payload.account_id.strip(),
                    sender_id=payload.sender_id.strip(),
                    topic=topic,
                    content=payload.content,
                )
        except KeyError as exc:
            raise HTTPException(404, "联系人不在当前账号的消息档案中") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"status": "saved" if created else "unchanged", "memory_id": memory_id}

    @app.delete("/api/senders/memories/{memory_id}")
    def delete_sender_memory(memory_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not memory_id.strip():
            raise HTTPException(400, "缺少账号或资料 ID")
        with sqlite_error_guard():
            deleted = get_memory_store().delete_sender_memory(
                account_id=account_id.strip(),
                memory_id=memory_id.strip(),
            )
        if not deleted:
            raise HTTPException(404, "未找到对应的人物资料")
        return {"status": "deleted", "memory_id": memory_id}

    @app.post("/api/senders/tags")
    def update_sender_tags(payload: SenderTagsUpdate, request: Request):
        require_local_mutation(request)
        with sqlite_error_guard():
            result = get_memory_store().update_sender_tags(
                account_id=payload.account_id.strip(),
                sender_id=payload.sender_id.strip(),
                action=payload.action.strip(),
                tags=payload.tags,
                source_task_id=DASHBOARD_ACTOR,
            )
        status = result.get("status")
        if status == "unavailable":
            raise HTTPException(404, str(result.get("reason") or "联系人不存在"))
        if status != "updated":
            raise HTTPException(400, str(result.get("reason") or "标签更新失败"))
        return result

    # --- User facts ---

    @app.get("/api/memory/facts")
    def list_facts(account_id: str = "", q: str = ""):
        store = get_memory_store()
        with sqlite_error_guard():
            _, selected = _resolve_account(store, get_episode_db_path(), account_id)
            if not selected:
                return {"account_id": "", "facts": [], "index_limit": FACT_INDEX_LIMIT}
            indexed = store.fact_key_index(account_id=selected, limit=FACT_INDEX_LIMIT)
            rank_by_key = {key: idx + 1 for idx, key in enumerate(indexed)}
            with store._lock:
                rows = store.db.execute(
                    "SELECT f.fact_id, f.account_id, f.fact_key, f.content, "
                    "f.source_message_id, f.author_id, f.trust_label, f.version, "
                    "f.created_at, f.updated_at, "
                    "s.conversation_id AS src_conversation_id, s.sender_id AS src_sender_id, "
                    "s.body_text AS src_body_text, s.captured_at AS src_captured_at "
                    "FROM user_facts f "
                    "LEFT JOIN source_messages s ON s.message_id=f.source_message_id "
                    "AND s.account_id=f.account_id "
                    "WHERE f.account_id=? AND f.state='active' "
                    "ORDER BY f.updated_at DESC, f.fact_id DESC",
                    (selected,),
                ).fetchall()
        needle = q.strip().casefold()
        items = []
        for row in rows:
            fact_key = str(row["fact_key"])
            content = str(row["content"])
            src_body = str(row["src_body_text"] or "")
            if needle and needle not in f"{fact_key} {content} {src_body}".casefold():
                continue
            rank = rank_by_key.get(fact_key)
            items.append(
                {
                    "fact_id": str(row["fact_id"]),
                    "account_id": str(row["account_id"]),
                    "fact_key": fact_key,
                    "content": content,
                    "source_message_id": str(row["source_message_id"]),
                    "author_id": str(row["author_id"]),
                    "trust_label": str(row["trust_label"]),
                    "version": int(row["version"]),
                    "created_at": str(row["created_at"]),
                    "updated_at": str(row["updated_at"]),
                    "in_index": rank is not None,
                    "index_rank": rank,
                    "source": (
                        {
                            "message_id": str(row["source_message_id"]),
                            "conversation_id": str(row["src_conversation_id"] or ""),
                            "sender_id": str(row["src_sender_id"] or ""),
                            "body_text": src_body[:400],
                            "captured_at": str(row["src_captured_at"] or ""),
                        }
                        if row["src_body_text"] is not None
                        else None
                    ),
                }
            )
        return {"account_id": selected, "facts": items, "index_limit": FACT_INDEX_LIMIT}

    @app.post("/api/memory/facts")
    def upsert_fact(payload: FactUpsert, request: Request):
        require_local_mutation(request)
        try:
            with sqlite_error_guard():
                fact_id = get_memory_store().remember_fact(
                    account_id=payload.account_id.strip(),
                    fact_key=payload.fact_key,
                    content=payload.content,
                    source_message_id=payload.source_message_id.strip(),
                    actor_id=DASHBOARD_ACTOR,
                )
        except KeyError as exc:
            raise HTTPException(404, "来源消息不存在或已失效") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"status": "saved", "fact_id": fact_id}

    @app.delete("/api/memory/facts/{fact_id}")
    def delete_fact(fact_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not fact_id.strip():
            raise HTTPException(400, "缺少账号或事实 ID")
        try:
            with sqlite_error_guard():
                get_memory_store().delete_fact(
                    account_id=account_id.strip(),
                    fact_id=fact_id.strip(),
                    actor_id=DASHBOARD_ACTOR,
                )
        except KeyError as exc:
            raise HTTPException(404, "未找到对应的长期事实") from exc
        return {"status": "deleted", "fact_id": fact_id}

    @app.get("/api/memory/recent-messages")
    def recent_source_messages(account_id: str = "", limit: int = 30):
        store = get_memory_store()
        bounded = max(1, min(limit, 80))
        with sqlite_error_guard():
            _, selected = _resolve_account(store, get_episode_db_path(), account_id)
            if not selected:
                return {"account_id": "", "messages": []}
            with store._lock:
                rows = store.db.execute(
                    "SELECT message_id, conversation_id, sender_id, "
                    "body_text, captured_at, direction "
                    "FROM source_messages WHERE account_id=? AND deleted_at IS NULL "
                    "ORDER BY captured_at DESC, message_id DESC LIMIT ?",
                    (selected, bounded),
                ).fetchall()
        return {
            "account_id": selected,
            "messages": [
                {
                    "message_id": str(row["message_id"]),
                    "conversation_id": str(row["conversation_id"]),
                    "sender_id": str(row["sender_id"]),
                    "body_text": str(row["body_text"])[:160],
                    "captured_at": str(row["captured_at"]),
                    "direction": str(row["direction"]),
                }
                for row in rows
            ],
        }

    # --- Event book ---

    @app.get("/api/memory/events")
    def list_events(account_id: str = "", q: str = ""):
        store = get_memory_store()
        event_book = get_event_book()
        with sqlite_error_guard():
            _, selected = _resolve_account(store, get_episode_db_path(), account_id)
            if not selected:
                return {
                    "account_id": "",
                    "events": [],
                    "quotas": {
                        "high_used": 0,
                        "high_limit": EventBook.HIGH_PRIORITY_SLOTS,
                        "ordinary_chars_used": 0,
                        "ordinary_chars_limit": EventBook.ORDINARY_TOTAL_CHARS,
                    },
                }
            entries = event_book.list_entries(selected)
        now = datetime.now(UTC)
        high_used = sum(1 for row in entries if row["priority"] == "high")
        ordinary_chars = sum(
            len(str(row["content"])) for row in entries if row["priority"] == "ordinary"
        )
        needle = q.strip().casefold()
        items = []
        for row in entries:
            content = str(row["content"])
            exit_cond = str(row["exit_condition"])
            if needle and needle not in f"{content} {exit_cond} {row['event_id']}".casefold():
                continue
            elapsed_seconds = EventBook._elapsed_seconds(row["updated_at"], now)
            age_hours = int(elapsed_seconds // 3600)
            items.append(
                {
                    "event_id": str(row["event_id"]),
                    "priority": str(row["priority"]),
                    "content": content,
                    "exit_condition": exit_cond,
                    "expires_at": row["expires_at"],
                    "created_at": str(row["created_at"]),
                    "updated_at": str(row["updated_at"]),
                    "age_hours": age_hours,
                    "stale_72h": row["priority"] == "ordinary" and elapsed_seconds >= 72 * 60 * 60,
                }
            )
        return {
            "account_id": selected,
            "events": items,
            "quotas": {
                "high_used": high_used,
                "high_limit": EventBook.HIGH_PRIORITY_SLOTS,
                "ordinary_chars_used": ordinary_chars,
                "ordinary_chars_limit": EventBook.ORDINARY_TOTAL_CHARS,
            },
        }

    @app.post("/api/memory/events")
    def create_event(payload: EventCreate, request: Request):
        require_local_mutation(request)
        try:
            with sqlite_error_guard():
                result = get_event_book().add(
                    account_id=payload.account_id.strip(),
                    priority=payload.priority.strip(),
                    content=payload.content,
                    exit_condition=payload.exit_condition,
                    expires_at=payload.expires_at,
                )
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        if result.get("status") != "saved":
            raise HTTPException(400, str(result.get("hint") or result.get("reason") or "保存失败"))
        return result

    @app.patch("/api/memory/events/{event_id}")
    def update_event(event_id: str, payload: EventUpdate, request: Request):
        require_local_mutation(request)
        try:
            with sqlite_error_guard():
                result = get_event_book().update(
                    account_id=payload.account_id.strip(),
                    event_id=event_id.strip(),
                    content=payload.content,
                    exit_condition=payload.exit_condition,
                    expires_at=payload.expires_at,
                )
        except (TypeError, ValueError) as exc:
            raise HTTPException(400, str(exc)) from exc
        status = result.get("status")
        if status == "unavailable":
            raise HTTPException(404, "未找到对应的活跃事件")
        if status != "updated":
            raise HTTPException(400, str(result.get("reason") or "更新失败"))
        return result

    @app.delete("/api/memory/events/{event_id}")
    def delete_event(event_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not event_id.strip():
            raise HTTPException(400, "缺少账号或事件 ID")
        with sqlite_error_guard():
            result = get_event_book().delete(
                account_id=account_id.strip(),
                event_id=event_id.strip(),
            )
        if result.get("status") != "deleted":
            raise HTTPException(404, "未找到对应的活跃事件")
        return result

    # --- Entity dictionary ---

    @app.get("/api/memory/entities")
    def list_entities(account_id: str = "", q: str = ""):
        store = get_memory_store()
        with sqlite_error_guard():
            _, selected = _resolve_account(store, get_episode_db_path(), account_id)
            if not selected:
                return {"account_id": "", "entities": []}
            with store._lock:
                rows = store.db.execute(
                    "SELECT entity_id, account_id, term, explanation, author_id, trust_label, "
                    "version, created_at, updated_at "
                    "FROM entity_terms WHERE account_id=? AND state='active' "
                    "ORDER BY updated_at DESC, term ASC",
                    (selected,),
                ).fetchall()
        needle = q.strip().casefold()
        items = []
        for row in rows:
            term = str(row["term"])
            explanation = str(row["explanation"])
            if needle and needle not in f"{term} {explanation}".casefold():
                continue
            items.append(
                {
                    "entity_id": str(row["entity_id"]),
                    "account_id": str(row["account_id"]),
                    "term": term,
                    "explanation": explanation,
                    "author_id": str(row["author_id"]),
                    "trust_label": str(row["trust_label"]),
                    "version": int(row["version"]),
                    "created_at": str(row["created_at"]),
                    "updated_at": str(row["updated_at"]),
                }
            )
        return {"account_id": selected, "entities": items}

    @app.post("/api/memory/entities")
    def upsert_entity(payload: EntityUpsert, request: Request):
        require_local_mutation(request)
        try:
            with sqlite_error_guard():
                entity_id = get_memory_store().upsert_entity(
                    account_id=payload.account_id.strip(),
                    term=payload.term,
                    explanation=payload.explanation,
                    actor_id=DASHBOARD_ACTOR,
                )
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"status": "saved", "entity_id": entity_id}

    @app.delete("/api/memory/entities/{entity_id}")
    def delete_entity(entity_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not entity_id.strip():
            raise HTTPException(400, "缺少账号或词条 ID")
        try:
            with sqlite_error_guard():
                get_memory_store().delete_entity(
                    account_id=account_id.strip(),
                    entity_id=entity_id.strip(),
                    actor_id=DASHBOARD_ACTOR,
                )
        except KeyError as exc:
            raise HTTPException(404, "未找到对应的词典词条") from exc
        return {"status": "deleted", "entity_id": entity_id}

    # --- Message annotations ---

    @app.get("/api/memory/annotations")
    def list_annotations(account_id: str = "", q: str = ""):
        store = get_memory_store()
        q_clean = q.strip()
        needle = q_clean.casefold()
        with sqlite_error_guard():
            _, selected = _resolve_account(store, get_episode_db_path(), account_id)
            if not selected:
                return {"account_id": "", "annotations": []}
            sql = (
                "SELECT a.note_id, a.account_id, a.message_id, a.author_kind, a.author_id, "
                "a.trust_label, a.content, a.source, a.version, a.created_at, a.updated_at, "
                "s.conversation_id AS src_conversation_id, s.sender_id AS src_sender_id, "
                "s.body_text AS src_body_text, s.captured_at AS src_captured_at "
                "FROM annotations a "
                "LEFT JOIN source_messages s ON s.message_id=a.message_id "
                "AND s.account_id=a.account_id "
                "WHERE a.account_id=? AND a.state='active'"
            )
            params: list[object] = [selected]
            if q_clean:
                pattern = f"%{_like_escape(q_clean)}%"
                sql += (
                    " AND (a.content LIKE ? ESCAPE '\\' "
                    "OR COALESCE(s.body_text, '') LIKE ? ESCAPE '\\' "
                    "OR COALESCE(s.conversation_id, '') LIKE ? ESCAPE '\\')"
                )
                params.extend((pattern, pattern, pattern))
            sql += " ORDER BY a.updated_at DESC, a.note_id DESC LIMIT 300"
            with store._lock:
                rows = store.db.execute(sql, params).fetchall()
        items = []
        for row in rows:
            content = str(row["content"])
            src_body = str(row["src_body_text"] or "")
            conv_id = str(row["src_conversation_id"] or "")
            if needle and needle not in f"{content} {src_body} {conv_id}".casefold():
                continue
            items.append(
                {
                    "note_id": str(row["note_id"]),
                    "account_id": str(row["account_id"]),
                    "message_id": str(row["message_id"]),
                    "author_kind": str(row["author_kind"]),
                    "author_id": str(row["author_id"]),
                    "trust_label": str(row["trust_label"]),
                    "content": content,
                    "source": str(row["source"]),
                    "version": int(row["version"]),
                    "created_at": str(row["created_at"]),
                    "updated_at": str(row["updated_at"]),
                    "editable": str(row["author_kind"]) == "agent",
                    "source_message": (
                        {
                            "message_id": str(row["message_id"]),
                            "conversation_id": conv_id,
                            "sender_id": str(row["src_sender_id"] or ""),
                            "body_text": src_body[:400],
                            "captured_at": str(row["src_captured_at"] or ""),
                        }
                        if row["src_body_text"] is not None
                        else None
                    ),
                }
            )
        return {"account_id": selected, "annotations": items}

    @app.patch("/api/memory/annotations/{note_id}")
    def update_annotation(note_id: str, payload: AnnotationUpdate, request: Request):
        require_local_mutation(request)
        try:
            with sqlite_error_guard():
                version = get_memory_store().revise_note(
                    account_id=payload.account_id.strip(),
                    note_id=note_id.strip(),
                    content=payload.content,
                    actor_id=DASHBOARD_ACTOR,
                )
        except KeyError as exc:
            raise HTTPException(404, "未找到可编辑的消息批注") from exc
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        return {"status": "updated", "note_id": note_id, "version": version}

    @app.delete("/api/memory/annotations/{note_id}")
    def delete_annotation(note_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not note_id.strip():
            raise HTTPException(400, "缺少账号或批注 ID")
        try:
            with sqlite_error_guard():
                version = get_memory_store().revise_note(
                    account_id=account_id.strip(),
                    note_id=note_id.strip(),
                    content=None,
                    actor_id=DASHBOARD_ACTOR,
                )
        except KeyError as exc:
            raise HTTPException(404, "未找到可删除的消息批注") from exc
        return {"status": "deleted", "note_id": note_id, "version": version}

    # --- Episode vector memory ---

    @app.get("/api/memory/episodes")
    def list_episodes(account_id: str = "", conversation_id: str = "", q: str = ""):
        store = get_memory_store()
        episode_path = get_episode_db_path()
        q_clean = q.strip()
        needle = q_clean.casefold()
        with sqlite_error_guard("无法读取事件记忆数据库"):
            _, selected = _resolve_account(store, episode_path, account_id)
            if episode_path is None or not episode_path.is_file():
                return {
                    "account_id": selected,
                    "available": False,
                    "bloat_threshold": EPISODE_BLOAT_THRESHOLD,
                    "conversations": [],
                    "episodes": [],
                }
            with closing(sqlite3.connect(episode_path, timeout=5.0)) as ep_db:
                ep_db.row_factory = sqlite3.Row
                if not _episode_table_exists(ep_db):
                    return {
                        "account_id": selected,
                        "available": False,
                        "bloat_threshold": EPISODE_BLOAT_THRESHOLD,
                        "conversations": [],
                        "episodes": [],
                    }
                if not selected:
                    return {
                        "account_id": "",
                        "available": True,
                        "bloat_threshold": EPISODE_BLOAT_THRESHOLD,
                        "conversations": [],
                        "episodes": [],
                    }
                model_rows = ep_db.execute(
                    "SELECT conversation_id, model, COUNT(*) AS cnt, MAX(updated_at) AS newest_at "
                    "FROM episodes WHERE account_id=? "
                    "GROUP BY conversation_id, model",
                    (selected,),
                ).fetchall()
                total_by_conv: dict[str, int] = {}
                max_model_by_conv: dict[str, int] = {}
                newest_by_conv: dict[str, str] = {}
                count_by_conv_model: dict[tuple[str, str], int] = {}
                for row in model_rows:
                    conv = str(row["conversation_id"])
                    model_name = str(row["model"])
                    cnt = int(row["cnt"])
                    newest = str(row["newest_at"])
                    total_by_conv[conv] = total_by_conv.get(conv, 0) + cnt
                    max_model_by_conv[conv] = max(max_model_by_conv.get(conv, 0), cnt)
                    if newest > newest_by_conv.get(conv, ""):
                        newest_by_conv[conv] = newest
                    count_by_conv_model[(conv, model_name)] = cnt
                sorted_convs = sorted(
                    total_by_conv,
                    key=lambda conv: (total_by_conv[conv], newest_by_conv[conv]),
                    reverse=True,
                )
                conversations = [
                    {
                        "conversation_id": conv,
                        "count": total_by_conv[conv],
                        "newest_at": newest_by_conv[conv],
                        "bloated": max_model_by_conv[conv] >= EPISODE_BLOAT_THRESHOLD,
                    }
                    for conv in sorted_convs
                ]

                sql = (
                    "SELECT id, account_id, conversation_id, summary, updated_at, model "
                    "FROM episodes WHERE account_id=?"
                )
                params: list[object] = [selected]
                if conversation_id.strip():
                    sql += " AND conversation_id=?"
                    params.append(conversation_id.strip())
                if q_clean:
                    pattern = f"%{_like_escape(q_clean)}%"
                    sql += (
                        " AND (summary LIKE ? ESCAPE '\\' "
                        "OR conversation_id LIKE ? ESCAPE '\\')"
                    )
                    params.extend((pattern, pattern))
                sql += " ORDER BY updated_at DESC, id DESC LIMIT 300"
                ep_rows = ep_db.execute(sql, params).fetchall()

                ep_ids = [str(row["id"]) for row in ep_rows]
                sources_by_ep: dict[str, list[str]] = {}
                all_message_ids: list[str] = []
                if ep_ids:
                    placeholders = ",".join("?" for _ in ep_ids)
                    for src_row in ep_db.execute(
                        f"SELECT episode_id, message_id FROM episode_sources "
                        f"WHERE episode_id IN ({placeholders}) ORDER BY message_id",
                        ep_ids,
                    ).fetchall():
                        eid = str(src_row["episode_id"])
                        mid = str(src_row["message_id"])
                        sources_by_ep.setdefault(eid, []).append(mid)
                        all_message_ids.append(mid)

            previews = _source_previews(store, selected, all_message_ids)
        episodes = []
        for row in ep_rows:
            summary = str(row["summary"])
            conv = str(row["conversation_id"])
            if needle and needle not in f"{summary} {conv}".casefold():
                continue
            eid = str(row["id"])
            model_name = str(row["model"])
            mids = sources_by_ep.get(eid, [])
            conv_model_cnt = count_by_conv_model.get((conv, model_name), 1)
            episodes.append(
                {
                    "id": eid,
                    "account_id": str(row["account_id"]),
                    "conversation_id": conv,
                    "summary": summary,
                    "updated_at": str(row["updated_at"]),
                    "model": model_name,
                    "conversation_count": total_by_conv.get(conv, 1),
                    "conversation_bloated": conv_model_cnt >= EPISODE_BLOAT_THRESHOLD,
                    "source_message_ids": mids,
                    "sources": [previews[mid] for mid in mids if mid in previews],
                }
            )
        return {
            "account_id": selected,
            "available": True,
            "bloat_threshold": EPISODE_BLOAT_THRESHOLD,
            "conversations": conversations,
            "episodes": episodes,
        }

    @app.delete("/api/memory/episodes/{episode_id}")
    def delete_episode(episode_id: str, account_id: str, request: Request):
        require_local_mutation(request)
        if not account_id.strip() or not episode_id.strip():
            raise HTTPException(400, "缺少账号或事件记忆 ID")
        episode_path = get_episode_db_path()
        if episode_path is None or not episode_path.is_file():
            raise HTTPException(404, "事件记忆数据库尚未初始化")
        try:
            with closing(sqlite3.connect(episode_path, timeout=5.0)) as ep_db:
                ep_db.execute("PRAGMA foreign_keys=ON")
                if not _episode_table_exists(ep_db):
                    raise HTTPException(404, "事件记忆表不存在")
                with ep_db:
                    cursor = ep_db.execute(
                        "DELETE FROM episodes WHERE id=? AND account_id=?",
                        (episode_id.strip(), account_id.strip()),
                    )
                if cursor.rowcount == 0:
                    raise HTTPException(404, "未找到对应的事件记忆")
        except HTTPException:
            raise
        except sqlite3.Error as exc:
            raise HTTPException(503, "删除事件记忆失败") from exc
        return {"status": "deleted", "id": episode_id}
