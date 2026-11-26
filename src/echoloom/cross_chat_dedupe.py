"""Persistent cross-chat message deduplication per session generation."""

from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterable
from dataclasses import replace
from datetime import UTC, datetime

from .context import ContextWindow
from .session_rollover import live_session_events


class CrossChatDedupeStore:
    """Tracks cross-chat message IDs already shown in each (route, generation) session.

    Callers filter unseen messages before dispatch (`filter_unseen`) and commit
    `mark_seen` only after dispatch may have reached the provider, so pre-dispatch
    `ModelProviderUnavailable` outages retry cleanly without losing cross-chat context.
    """

    def __init__(self, routing_db: sqlite3.Connection | None = None) -> None:
        self._db = routing_db
        self._seen_memory: set[tuple[str, int, str]] = set()
        if self._db is not None:
            self._ensure_schema(self._db)

    @staticmethod
    def _ensure_schema(db: sqlite3.Connection) -> None:
        db.execute(
            "CREATE TABLE IF NOT EXISTS session_other_message_seen ("
            "route TEXT NOT NULL,generation INTEGER NOT NULL,message_id TEXT NOT NULL,"
            "seen_at TEXT NOT NULL,PRIMARY KEY(route,generation,message_id))"
        )
        db.commit()

    @staticmethod
    def extract_session_message_ids(session: object) -> tuple[str, ...]:
        """Extract cross-chat message IDs embedded in legacy pre-migration session events."""
        ids: list[str] = []
        for event in live_session_events(getattr(session, "events", None)):
            if getattr(event, "author", "") != "user":
                continue
            content = getattr(event, "content", None)
            text = "".join(part.text or "" for part in getattr(content, "parts", []) or [])
            section = text.split("此时此刻其它聊天串（非本会话）近况", 1)
            if len(section) < 2:
                continue
            ids.extend(re.findall(r"内部消息 ID：([^；\n]+)", section[1]))
        return tuple(dict.fromkeys(ids))

    def seed_from_session(self, route: str, generation: int, session: object) -> tuple[str, ...]:
        """Warm the dedupe table from legacy session events so old rows are not re-injected."""
        ids = self.extract_session_message_ids(session)
        self.mark_seen(route, generation, ids)
        return ids

    def seen_message_ids(self, route: str, generation: int) -> set[str]:
        if self._db is None:
            return {
                message_id
                for seen_route, seen_generation, message_id in self._seen_memory
                if seen_route == route and seen_generation == generation
            }
        rows = self._db.execute(
            "SELECT message_id FROM session_other_message_seen WHERE route=? AND generation=?",
            (route, generation),
        ).fetchall()
        return {str(row[0]) for row in rows}

    def filter_unseen(
        self,
        route: str,
        generation: int,
        window: ContextWindow,
    ) -> tuple[ContextWindow, tuple[str, ...]]:
        """Return a copy of `window` omitting already-seen other-chat turns, plus new IDs.

        Pure read: never marks IDs as seen. Call `mark_seen` only after the request
        has been dispatched (never on pre-dispatch `ModelProviderUnavailable`).
        """
        turns = window.recent_other_turns
        if not turns:
            return window, ()
        seen = self.seen_message_ids(route, generation)
        selected = []
        new_ids: list[str] = []
        local_seen: set[str] = set()
        for turn in turns:
            message_id = turn.message_id
            if message_id and (message_id in seen or message_id in local_seen):
                continue
            selected.append(turn)
            if message_id:
                local_seen.add(message_id)
                new_ids.append(message_id)
        return replace(window, recent_other_turns=tuple(selected)), tuple(new_ids)

    def mark_seen(
        self,
        route: str,
        generation: int,
        message_ids: Iterable[str],
    ) -> None:
        ids = tuple(message_ids)
        if not ids:
            return
        now = datetime.now(UTC).isoformat()
        if self._db is None:
            self._seen_memory.update((route, generation, message_id) for message_id in ids)
            return
        self._db.executemany(
            "INSERT OR IGNORE INTO session_other_message_seen "
            "(route,generation,message_id,seen_at) VALUES (?,?,?,?)",
            [(route, generation, message_id, now) for message_id in ids],
        )
        self._db.commit()

    def prune_before_generation(self, route: str, generation: int) -> None:
        if self._db is None:
            self._seen_memory = {
                item
                for item in self._seen_memory
                if not (item[0] == route and item[1] < generation)
            }
            return
        self._db.execute(
            "DELETE FROM session_other_message_seen WHERE route=? AND generation<?",
            (route, generation),
        )
        self._db.commit()

    def delete_route(self, route: str) -> None:
        if self._db is None:
            self._seen_memory = {item for item in self._seen_memory if item[0] != route}
            return
        self._db.execute("DELETE FROM session_other_message_seen WHERE route=?", (route,))
        self._db.commit()
