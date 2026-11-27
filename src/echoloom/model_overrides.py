"""Small durable per-chat model selection and owner command parser."""

from __future__ import annotations

import re
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

from .model_catalog import load_model_catalog

MODEL_LABELS = load_model_catalog().labels_map()
_MODEL_COMMANDS = (
    ("gpt-6-luna", re.compile(r"^(?:切换|换|改用|使用)(?:到|成|为)?\s*(?:OpenAI\s*)?(?:GPT[- ]?6[- ]?)?Luna[。！!]*$", re.IGNORECASE)),
    ("gemini-4-argon", re.compile(r"^(?:(?:临时)?(?:切换|换|使用|改用)(?:到|成|为)?|把模型(?:切换|换)(?:为|成))\s*(?:gemini\s*)?4\s*argon(?:\s*一小时)?[。！!]*$", re.IGNORECASE)),
    ("gemini-4-hydrogen", re.compile(r"^(?:(?:切换|换|使用|改用)(?:到|成|为)?|把模型(?:切换|换)(?:为|成))\s*(?:gemini\s*)?4\s*hydrogen[。！!]*$", re.IGNORECASE)),
    ("gemini-3.8-flash", re.compile(r"^(?:(?:切换|换|使用|改用)(?:到|成|为)?|把模型(?:切换|换)(?:为|成))\s*(?:gemini\s*)?3[.]?8\s*flash[。！!]*$", re.IGNORECASE)),
    ("gemini-3.5-flash-lite", re.compile(r"^(?:(?:切换|换|使用|改用)(?:到|成|为)?|把模型(?:切换|换)(?:为|成))\s*(?:gemini\s*)?3[.]?5\s*flash\s*lite[。！!]*$", re.IGNORECASE)),
)


class ModelOverrideStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=10, check_same_thread=False)
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS conversation_model_overrides ("
            "account_id TEXT NOT NULL,conversation_id TEXT NOT NULL,model_id TEXT NOT NULL,"
            "expires_at TEXT,updated_at TEXT NOT NULL,"
            "PRIMARY KEY(account_id,conversation_id))"
        )
        self.db.commit()

    def set(self, account_id: str, conversation_id: str, model_id: str,
            *, temporary_seconds: int | None = None) -> None:
        catalog = load_model_catalog()
        spec = catalog.get(model_id)
        if spec is None and model_id not in MODEL_LABELS:
            raise ValueError("unsupported model")
        max_temp = spec.temporary_only_max_seconds if spec is not None else None
        if max_temp is not None and not 1 <= (temporary_seconds or 0) <= max_temp:
            raise ValueError(f"{model_id} can only be enabled temporarily (<= {max_temp}s)")
        if max_temp is None and temporary_seconds is not None:
            raise ValueError("only temporary-tier models accept temporary_seconds")
        now = datetime.now(UTC)
        expires = (now + timedelta(seconds=temporary_seconds)).isoformat() if temporary_seconds else None
        self.db.execute(
            "INSERT INTO conversation_model_overrides VALUES(?,?,?,?,?) "
            "ON CONFLICT(account_id,conversation_id) DO UPDATE SET "
            "model_id=excluded.model_id,expires_at=excluded.expires_at,updated_at=excluded.updated_at",
            (account_id, conversation_id, model_id, expires, now.isoformat()),
        )
        self.db.commit()

    def get(self, account_id: str, conversation_id: str) -> str | None:
        row = self.db.execute(
            "SELECT model_id,expires_at FROM conversation_model_overrides "
            "WHERE account_id=? AND conversation_id=?", (account_id, conversation_id),
        ).fetchone()
        if row is None:
            return None
        if row[1] and datetime.fromisoformat(row[1]) <= datetime.now(UTC):
            self.db.execute(
                "DELETE FROM conversation_model_overrides WHERE account_id=? AND conversation_id=?",
                (account_id, conversation_id),
            )
            self.db.commit()
            return None
        return str(row[0])

    def clear(self, account_id: str, conversation_id: str) -> None:
        self.db.execute(
            "DELETE FROM conversation_model_overrides WHERE account_id=? AND conversation_id=?",
            (account_id, conversation_id),
        )
        self.db.commit()


def parse_model_command(text: str) -> tuple[str, str] | None:
    """Return (kind, model_id); the caller must check the exact owner ID."""
    value = " ".join(text.strip().split())
    if value in {"模型列表", "可选模型", "可选模型列表", "有哪些模型", "有哪些可用模型", "能用什么模型", "模型有哪些"}:
        return ("list", "")
    if value in {"切回默认模型", "恢复默认模型"}:
        return ("default", "")
    for model_id, pattern in _MODEL_COMMANDS:
        if pattern.fullmatch(value):
            return ("set", model_id)
    return None


def model_list_text(allowed_models=None) -> str:
    example = "Gemini 4 Hydrogen" if "gemini-4-hydrogen" in (allowed_models or ()) else (
        "Luna" if allowed_models and "gpt-6-luna" in allowed_models else "Gemini 3.8 Flash"
    )
    if allowed_models is not None:
        return "当前可选模型：" + "、".join(MODEL_LABELS[m] for m in allowed_models) + f"。可说：切换到 {example}；或恢复默认模型。"
    return "可选模型：轻量版 Gemini 3.5 Flash Lite、日常版 Gemini 3.8 Flash、Gemini 4 Hydrogen。需要临时高性能时可说“切换到 Gemini 4 Argon 一小时”，一小时后自动切回聊天档案的默认模型。"
