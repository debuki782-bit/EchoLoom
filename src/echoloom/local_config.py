"""Centralized sensitive configuration vault (`.local/`) and privacy boundary audit.

Keeps private identities, local prompt assets, location metadata, source tokens,
and host-specific Hook paths out of version-controlled files while remaining
top-level free of LLM SDK imports so standalone Ingress can read it safely.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
VAULT_DIRNAME = ".local"
_USE_LOCAL_OVERRIDES = True


@dataclass(frozen=True)
class ServerSettings:
    ingress_host: str = "127.0.0.1"
    ingress_port: int = 8080
    dashboard_host: str = "127.0.0.1"
    dashboard_port: int = 8767
    episode_rag_enabled: bool = True


@dataclass(frozen=True)
class LocationSettings:
    timezone: str = ""
    weather_enabled: bool = False
    weather_city: str = ""
    weather_latitude: float = 0.0
    weather_longitude: float = 0.0


@dataclass(frozen=True)
class ContextSettings:
    priority_sender_present_anchor: str = ""
    priority_sender_absent_anchor: str = ""


@dataclass(frozen=True)
class WeChatHookSettings:
    hook_url: str = "http://127.0.0.1:30001"
    native_download_url: str = "http://127.0.0.1:30002"
    event_extension_url: str = "http://127.0.0.1:30003"
    weixin_exe: str = ""
    launcher_exe: str = ""
    launcher_sha256: str = ""
    hook_dll: str = ""
    hook_dll_sha256: str = ""
    weixin_version: str = ""
    native_download_dll: str = ""
    weixin_dll_sha256: str = ""


@dataclass(frozen=True)
class SourceSpec:
    source_id: str
    factory: str
    channel: str = "custom"
    enabled: bool = True
    account_id: str = ""
    default_conversation_id: str = ""
    base_url: str = ""
    outbound_url: str = ""
    native_download_url: str = ""
    auth_token: str = ""
    auth_token_env: str = ""
    hmac_secret: str = ""
    hmac_secret_env: str = ""
    send_enabled: bool = True
    allowed_conversations: tuple[str, ...] = ()
    extra: dict[str, Any] = field(default_factory=dict)

    def resolved_auth_token(self) -> str:
        if self.auth_token_env:
            env_val = os.environ.get(self.auth_token_env, "").strip()
            if env_val:
                return env_val
        return self.auth_token.strip()

    def resolved_hmac_secret(self) -> str:
        if self.hmac_secret_env:
            env_val = os.environ.get(self.hmac_secret_env, "").strip()
            if env_val:
                return env_val
        return self.hmac_secret.strip()


@dataclass(frozen=True)
class LocalRuntimeConfig:
    server: ServerSettings = field(default_factory=ServerSettings)
    account: str = ""
    default_room: str = ""
    approved_group_conversations: tuple[str, ...] = ()
    brand_name: str = "EchoLoom"
    extra_sensitive_name_terms: tuple[str, ...] = ()
    location: LocationSettings = field(default_factory=LocationSettings)
    context: ContextSettings = field(default_factory=ContextSettings)
    webhook_secret: str = ""
    wechat_hook: WeChatHookSettings = field(default_factory=WeChatHookSettings)


def vault_dir(workspace: Path | None = None) -> Path:
    base = Path(workspace) if workspace is not None else ROOT
    return base / VAULT_DIRNAME


def local_overrides_enabled() -> bool:
    return _USE_LOCAL_OVERRIDES


def _load_toml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def resolve_profile_path(base_path: Path, workspace: Path | None = None) -> Path:
    """Resolve agent_profiles config: `.local/agent_profiles.toml` -> legacy `.local.toml` -> base."""
    if not _USE_LOCAL_OVERRIDES:
        return base_path
    root = Path(workspace) if workspace is not None else (
        base_path.parents[1] if base_path.parent.name == "config" else ROOT
    )
    vault_profile = vault_dir(root) / "agent_profiles.toml"
    if base_path.name == "agent_profiles.toml" and vault_profile.is_file():
        return vault_profile
    legacy_local = base_path.with_name("agent_profiles.local.toml")
    if base_path.name == "agent_profiles.toml" and legacy_local.is_file():
        return legacy_local
    return base_path


def resolve_prompt_file(
    filename: str,
    base_dir: Path,
    *,
    legacy_local_dir: Path | None = None,
    workspace: Path | None = None,
) -> Path:
    """Resolve prompt file: `.local/prompts/<name>` -> `prompts.local/<name>` -> `prompts/<name>`."""
    if _USE_LOCAL_OVERRIDES:
        root = Path(workspace) if workspace is not None else (
            base_dir.parent if base_dir.name == "prompts" else ROOT
        )
        vault_file = vault_dir(root) / "prompts" / filename
        if vault_file.is_file():
            return vault_file
        if legacy_local_dir is not None:
            legacy_file = legacy_local_dir / filename
            if legacy_file.is_file():
                return legacy_file
    return base_dir / filename


def resolve_route_path(workspace: Path | None = None) -> Path:
    """Resolve mutable live route JSON: `.local/route.json` if present, else `data/live-route.json`."""
    base = Path(workspace) if workspace is not None else ROOT
    if _USE_LOCAL_OVERRIDES:
        vault_route = vault_dir(base) / "route.json"
        if vault_route.is_file():
            return vault_route
    return base / "data" / "live-route.json"


def _table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    val = raw.get(key)
    return val if isinstance(val, dict) else {}


def load_local_runtime_config(workspace: Path | None = None) -> LocalRuntimeConfig:
    """Load centralized `.local/runtime.toml` (returns safe OSS defaults when disabled or absent)."""
    if not _USE_LOCAL_OVERRIDES:
        return LocalRuntimeConfig()
    base = Path(workspace) if workspace is not None else ROOT
    raw = _load_toml(vault_dir(base) / "runtime.toml")
    if not raw:
        return LocalRuntimeConfig()

    srv = _table(raw, "server")
    ident = _table(raw, "identity")
    loc = _table(raw, "location")
    ctx = _table(raw, "context")
    sec = _table(raw, "security")
    hook = _table(raw, "wechat_hook")

    def _port(raw_val: object, default: int) -> int:
        if isinstance(raw_val, int) and not isinstance(raw_val, bool) and 1 <= raw_val <= 65535:
            return raw_val
        return default

    approved = tuple(
        str(v)
        for v in ident.get("approved_group_conversations", ())
        if isinstance(v, str) and v.strip()
    )
    extra_terms = tuple(
        str(v).strip()
        for v in ident.get("extra_sensitive_name_terms", ())
        if isinstance(v, str) and v.strip()
    )
    return LocalRuntimeConfig(
        server=ServerSettings(
            ingress_host=str(srv.get("ingress_host") or "127.0.0.1").strip() or "127.0.0.1",
            ingress_port=_port(srv.get("ingress_port"), 8080),
            dashboard_host=str(srv.get("dashboard_host") or "127.0.0.1").strip() or "127.0.0.1",
            dashboard_port=_port(srv.get("dashboard_port"), 8767),
            episode_rag_enabled=bool(srv.get("episode_rag_enabled", True)),
        ),
        account=str(ident.get("account") or "").strip(),
        default_room=str(ident.get("default_room") or "").strip(),
        approved_group_conversations=approved,
        brand_name=str(ident.get("brand_name") or "EchoLoom").strip() or "EchoLoom",
        extra_sensitive_name_terms=extra_terms,
        location=LocationSettings(
            timezone=str(loc.get("timezone") or "").strip(),
            weather_enabled=bool(loc.get("weather_enabled", False)),
            weather_city=str(loc.get("weather_city") or "").strip(),
            weather_latitude=float(loc.get("weather_latitude", 0.0) or 0.0),
            weather_longitude=float(loc.get("weather_longitude", 0.0) or 0.0),
        ),
        context=ContextSettings(
            priority_sender_present_anchor=str(
                ctx.get("priority_sender_present_anchor") or ""
            ).strip(),
            priority_sender_absent_anchor=str(
                ctx.get("priority_sender_absent_anchor") or ""
            ).strip(),
        ),
        webhook_secret=str(sec.get("webhook_secret") or "").strip(),
        wechat_hook=WeChatHookSettings(
            hook_url=str(hook.get("hook_url") or "http://127.0.0.1:30001").strip(),
            native_download_url=str(
                hook.get("native_download_url") or "http://127.0.0.1:30002"
            ).strip(),
            event_extension_url=str(
                hook.get("event_extension_url") or "http://127.0.0.1:30003"
            ).strip(),
            weixin_exe=str(hook.get("weixin_exe") or "").strip(),
            launcher_exe=str(hook.get("launcher_exe") or "").strip(),
            launcher_sha256=str(hook.get("launcher_sha256") or "").strip(),
            hook_dll=str(hook.get("hook_dll") or "").strip(),
            hook_dll_sha256=str(hook.get("hook_dll_sha256") or "").strip(),
            weixin_version=str(hook.get("weixin_version") or "").strip(),
            native_download_dll=str(hook.get("native_download_dll") or "").strip(),
            weixin_dll_sha256=str(hook.get("weixin_dll_sha256") or "").strip(),
        ),
    )


def load_source_configs(
    workspace: Path | None = None,
    *,
    allow_example_fallback: bool = False,
) -> tuple[str, dict[str, SourceSpec]]:
    """Load configured message sources from `.local/sources.toml` (or example template if requested)."""
    base = Path(workspace) if workspace is not None else ROOT
    raw: dict[str, Any] = {}
    if _USE_LOCAL_OVERRIDES:
        raw = _load_toml(vault_dir(base) / "sources.toml")
    if not raw and allow_example_fallback:
        raw = _load_toml(base / "config" / "sources.example.toml")
    if not raw:
        return "", {}
    default_source = str(raw.get("default_source") or "").strip()
    sources_raw = raw.get("sources")
    if not isinstance(sources_raw, dict):
        return default_source, {}
    specs: dict[str, SourceSpec] = {}
    for source_id, entry in sources_raw.items():
        if not isinstance(source_id, str) or not isinstance(entry, dict):
            continue
        if not bool(entry.get("enabled", True)):
            continue
        factory = str(entry.get("factory") or "").strip()
        if not factory:
            continue
        known_keys = {
            "enabled", "factory", "channel", "account_id", "default_conversation_id",
            "base_url", "outbound_url", "native_download_url", "auth_token",
            "auth_token_env", "hmac_secret", "hmac_secret_env", "send_enabled",
            "allowed_conversations",
        }
        allowed = tuple(
            str(item)
            for item in entry.get("allowed_conversations", ())
            if isinstance(item, str) and item.strip()
        )
        specs[source_id] = SourceSpec(
            source_id=source_id,
            factory=factory,
            channel=str(entry.get("channel") or source_id).strip(),
            enabled=True,
            account_id=str(entry.get("account_id") or "").strip(),
            default_conversation_id=str(entry.get("default_conversation_id") or "").strip(),
            base_url=str(entry.get("base_url") or "").strip(),
            outbound_url=str(entry.get("outbound_url") or "").strip(),
            native_download_url=str(entry.get("native_download_url") or "").strip(),
            auth_token=str(entry.get("auth_token") or "").strip(),
            auth_token_env=str(entry.get("auth_token_env") or "").strip(),
            hmac_secret=str(entry.get("hmac_secret") or "").strip(),
            hmac_secret_env=str(entry.get("hmac_secret_env") or "").strip(),
            send_enabled=bool(entry.get("send_enabled", True)),
            allowed_conversations=allowed,
            extra={k: v for k, v in entry.items() if k not in known_keys},
        )
    if not default_source and specs:
        default_source = next(iter(specs))
    return default_source, specs


_SYNTHETIC_WXID_PREFIXES = (
    "a", "b", "a123", "abc", "author", "account", "bootstrap", "bot", "contact", "current",
    "example", "first", "friend", "from", "guest", "legacy", "local", "member", "native",
    "not", "other", "owner", "person", "quoted", "red1", "remark", "remote", "renamed",
    "second", "sender", "someone", "source", "target", "test", "trace", "uses", "vault",
)
_TRACKED_AUDIT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "real_wxid",
        re.compile(
            r"\bwxid_(?!"
            + "|".join(_SYNTHETIC_WXID_PREFIXES)
            + r")[A-Za-z0-9_-]{6,}\b"
        ),
    ),
    ("api_key_literal", re.compile(r"(?:AIza[0-9A-Za-z_-]{30,}|sk-(?!test|mock|example)[0-9A-Za-z]{20,})")),
    ("home_user_path", re.compile(r"C:[\\/]+Users[\\/]+(?!<user>|example|Other\b)[A-Za-z0-9_.-]+")),
)


def audit_tracked_files(workspace: Path | None = None) -> list[dict[str, object]]:
    """Scan git-tracked files for leaked private identifiers or secret tokens."""
    base = Path(workspace) if workspace is not None else ROOT
    files = subprocess.check_output(
        ["git", "ls-files"], cwd=str(base), text=True, encoding="utf-8"
    ).splitlines()
    findings: list[dict[str, object]] = []
    for rel in files:
        target = base / rel
        if not target.is_file() or target.suffix.lower() in {".png", ".jpg", ".db", ".sqlite3", ".pyc"}:
            continue
        text = target.read_text(encoding="utf-8", errors="ignore")
        for lineno, line in enumerate(text.splitlines(), 1):
            for kind, pattern in _TRACKED_AUDIT_PATTERNS:
                if pattern.search(line):
                    findings.append({"file": rel, "line": lineno, "kind": kind})
    return findings


def vault_status(workspace: Path | None = None) -> dict[str, object]:
    """Return non-secret inventory of centralized local vault and credentials."""
    from .credentials import OPENAI_TARGET, TARGET, load_key

    base = Path(workspace) if workspace is not None else ROOT
    vdir = vault_dir(base)
    cfg = load_local_runtime_config(base)
    default_source, sources = load_source_configs(base)
    prompt_files = sorted(p.name for p in (vdir / "prompts").glob("*")) if (vdir / "prompts").is_dir() else []
    try:
        gemini_stored = load_key(target=TARGET) is not None
        openai_stored = load_key(target=OPENAI_TARGET) is not None
    except OSError:
        gemini_stored = False
        openai_stored = False
    return {
        "vault_dir": str(vdir),
        "runtime_toml_present": (vdir / "runtime.toml").is_file(),
        "sources_toml_present": (vdir / "sources.toml").is_file(),
        "agent_profiles_present": (vdir / "agent_profiles.toml").is_file(),
        "route_json_present": (vdir / "route.json").is_file(),
        "local_prompt_files": prompt_files,
        "default_source": default_source,
        "configured_sources": list(sources),
        "brand_name": cfg.brand_name,
        "weather_enabled": cfg.location.weather_enabled,
        "credentials": {
            "gemini_key_stored": gemini_stored or bool(os.environ.get("GEMINI_API_KEY")),
            "openai_key_stored": openai_stored or bool(os.environ.get("OPENAI_API_KEY")),
            "webhook_secret_configured": bool(
                cfg.webhook_secret or os.environ.get("ECHOLOOM_WEBHOOK_SECRET")
            ),
        },
    }


def init_local_vault(workspace: Path | None = None) -> dict[str, list[str]]:
    """Create `.local/` from tracked `config/*.toml` templates without overwriting existing files."""
    base = Path(workspace) if workspace is not None else ROOT
    vdir = vault_dir(base)
    (vdir / "prompts").mkdir(parents=True, exist_ok=True)
    pairs = (
        (base / "config" / "runtime.example.toml", vdir / "runtime.toml"),
        (base / "config" / "sources.example.toml", vdir / "sources.toml"),
        (base / "config" / "agent_profiles.toml", vdir / "agent_profiles.toml"),
    )
    created: list[str] = []
    skipped: list[str] = []
    for src, dst in pairs:
        if not src.is_file():
            continue
        if dst.exists():
            skipped.append(str(dst.relative_to(base)).replace("\\", "/"))
            continue
        dst.write_text(src.read_text(encoding="utf-8"), encoding="utf-8")
        created.append(str(dst.relative_to(base)).replace("\\", "/"))
    return {"created": created, "skipped": skipped}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Initialize, inspect, and audit centralized .local vault")
    parser.add_argument(
        "action",
        choices=("status", "audit", "runtime-json", "init"),
        default="status",
        nargs="?",
    )
    args = parser.parse_args(argv)
    if args.action == "init":
        print(json.dumps(init_local_vault(ROOT), ensure_ascii=False, indent=2))
        return 0
    if args.action == "status":
        print(json.dumps(vault_status(ROOT), ensure_ascii=False, indent=2))
        return 0
    if args.action == "runtime-json":
        cfg = load_local_runtime_config(ROOT)
        default_source, sources = load_source_configs(ROOT, allow_example_fallback=True)
        print(json.dumps({
            "brand_name": cfg.brand_name,
            "default_source": default_source,
            "sources": list(sources),
            "server": {
                "ingress_host": cfg.server.ingress_host,
                "ingress_port": cfg.server.ingress_port,
                "dashboard_host": cfg.server.dashboard_host,
                "dashboard_port": cfg.server.dashboard_port,
                "episode_rag_enabled": cfg.server.episode_rag_enabled,
            },
            "wechat_hook": {
                "hook_url": cfg.wechat_hook.hook_url,
                "native_download_url": cfg.wechat_hook.native_download_url,
                "event_extension_url": cfg.wechat_hook.event_extension_url,
                "weixin_exe": cfg.wechat_hook.weixin_exe,
                "launcher_exe": cfg.wechat_hook.launcher_exe,
                "launcher_sha256": cfg.wechat_hook.launcher_sha256,
                "hook_dll": cfg.wechat_hook.hook_dll,
                "hook_dll_sha256": cfg.wechat_hook.hook_dll_sha256,
                "weixin_version": cfg.wechat_hook.weixin_version,
                "native_download_dll": cfg.wechat_hook.native_download_dll,
                "weixin_dll_sha256": cfg.wechat_hook.weixin_dll_sha256,
            },
        }, ensure_ascii=False))
        return 0
    findings = audit_tracked_files(ROOT)
    print(json.dumps({"clean": not findings, "findings": findings}, ensure_ascii=False, indent=2))
    return 1 if findings else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
