"""Local persistent on/off policy shared by the Pilot and management interface."""

from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path

POLICY_PATH = Path(__file__).resolve().parents[2] / "data" / "tool-controls.sqlite3"


def read_policy(path: Path | None = None) -> dict[str, bool]:
    path = path or POLICY_PATH
    if not path.exists():
        return {}
    with sqlite3.connect(f"file:{path.as_posix()}?mode=ro", uri=True, timeout=1) as db:
        return {str(name): bool(enabled) for name, enabled in db.execute(
            "SELECT name,enabled FROM tool_controls"
        )}


def tool_enabled(name: str) -> bool:
    try:
        return read_policy().get(name, True)
    except sqlite3.Error:
        return False  # Cannot verify the configured policy: do not execute the tool.


def set_tool_enabled(name: str, enabled: bool, path: Path | None = None) -> None:
    path = path or POLICY_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path, timeout=2) as db:
        db.execute("CREATE TABLE IF NOT EXISTS tool_controls (name TEXT PRIMARY KEY, "
                   "enabled INTEGER NOT NULL CHECK(enabled IN (0,1)), updated_at TEXT NOT NULL)")
        db.execute("INSERT INTO tool_controls VALUES (?,?,?) ON CONFLICT(name) DO UPDATE SET "
                   "enabled=excluded.enabled,updated_at=excluded.updated_at",
                   (name, int(enabled), datetime.now(UTC).isoformat()))
