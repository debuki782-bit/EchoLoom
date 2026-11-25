"""Load the centralized, model-visible prompt resources from the project root."""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path

from . import local_config

_PROMPT_DIR = Path(__file__).resolve().parents[2] / "prompts"
_LOCAL_PROMPT_DIR: Path | None = Path(__file__).resolve().parents[2] / "prompts.local"
_PROMPT_NAMES = frozenset(
    {
        "persona",
        "main_agent",
        "session_summary",
        "session_summary_system",
        "idle_agent",
        "idle_finalize",
        "idle_fallback",
        "cross_session_task",
        "jev_strategy",
    }
)
_TOKEN = re.compile(r"\{\{([a-z][a-z0-9_]*)\}\}")
_LOCK = threading.RLock()
_PROMPT_CACHE: dict[str, str] = {}
_RUNTIME_CACHE: dict[str, str] | None = None
_TOOL_CACHE: dict[str, str] | None = None


def _resolve_prompt_file(filename: str) -> Path:
    if local_config.local_overrides_enabled():
        vault_candidate = local_config.vault_dir(_PROMPT_DIR.parent) / "prompts" / filename
        if vault_candidate.is_file():
            return vault_candidate
    if _LOCAL_PROMPT_DIR is not None:
        local = _LOCAL_PROMPT_DIR / filename
        if local.is_file():
            return local
    return _PROMPT_DIR / filename


def _read_prompt_snapshot() -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    prompts: dict[str, str] = {}
    for name in _PROMPT_NAMES:
        path = _resolve_prompt_file(f"{name}.md")
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            raise ValueError(f"prompt {name!r} is empty")
        prompts[name] = text
    runtime = json.loads(_resolve_prompt_file("runtime_context.json").read_text(encoding="utf-8"))
    tools = json.loads(_resolve_prompt_file("tool_descriptions.json").read_text(encoding="utf-8"))
    for label, value in (("runtime_context.json", runtime), ("tool_descriptions.json", tools)):
        if not isinstance(value, dict) or any(
            not isinstance(key, str) or not isinstance(text, str)
            for key, text in value.items()
        ):
            raise ValueError(f"{label} must map names to text")
    if any(not value.strip() for value in (*runtime.values(), *tools.values())):
        raise ValueError("prompt resource contains an empty entry")
    return prompts, runtime, tools


def preload_prompts() -> None:
    """Load all model-facing text once. Startup fails if any resource is invalid."""
    prompts, runtime, tools = _read_prompt_snapshot()
    global _RUNTIME_CACHE, _TOOL_CACHE
    with _LOCK:
        _PROMPT_CACHE.clear()
        _PROMPT_CACHE.update(prompts)
        _RUNTIME_CACHE, _TOOL_CACHE = runtime, tools


def refresh_prompts() -> tuple[bool, str]:
    """Atomically replace prompt memory only after every file parses successfully."""
    try:
        prompts, runtime, tools = _read_prompt_snapshot()
    except Exception as exc:  # preserve last known-good snapshot on every read/parse error
        return False, type(exc).__name__
    global _RUNTIME_CACHE, _TOOL_CACHE
    with _LOCK:
        _PROMPT_CACHE.clear()
        _PROMPT_CACHE.update(prompts)
        _RUNTIME_CACHE, _TOOL_CACHE = runtime, tools
    return True, "reloaded"


def load_prompt(name: str) -> str:
    """Return preloaded text, lazily preloading when used outside the service entrypoint."""
    if name not in _PROMPT_NAMES:
        raise KeyError(f"unknown prompt: {name}")
    with _LOCK:
        value = _PROMPT_CACHE.get(name)
    if value is None:
        preload_prompts()
        with _LOCK:
            value = _PROMPT_CACHE[name]
    return value


def fill_prompt(name: str, **values: object) -> str:
    """Fill named double-brace fields while leaving ordinary braces untouched."""
    text = load_prompt(name)
    required = set(_TOKEN.findall(text))
    missing = required - values.keys()
    if missing:
        raise ValueError(f"missing prompt values: {', '.join(sorted(missing))}")
    for key in required:
        text = text.replace("{{" + key + "}}", str(values[key]))
    return text


def runtime_templates() -> dict[str, str]:
    """Return a copy of the preloaded dynamic-context templates."""
    with _LOCK:
        value = _RUNTIME_CACHE
    if value is None:
        preload_prompts()
        with _LOCK:
            value = _RUNTIME_CACHE
    return dict(value or {})


def fill_runtime(template: str, **values: object) -> str:
    """Fill double-brace placeholders in an already loaded runtime template."""
    required = set(_TOKEN.findall(template))
    missing = required - values.keys()
    if missing:
        raise ValueError(f"missing runtime values: {', '.join(sorted(missing))}")
    for key in required:
        template = template.replace("{{" + key + "}}", str(values[key]))
    return template


def _tool_descriptions() -> dict[str, str]:
    """Return a copy of cached tool descriptions."""
    with _LOCK:
        value = _TOOL_CACHE
    if value is None:
        preload_prompts()
        with _LOCK:
            value = _TOOL_CACHE
    return dict(value or {})


def tool_description(name: str) -> str:
    """Return the single maintained ADK description for a function tool."""
    value = _tool_descriptions()
    if not isinstance(value, dict) or name not in value or not isinstance(value[name], str):
        raise KeyError(f"missing tool description: {name}")
    return value[name]
