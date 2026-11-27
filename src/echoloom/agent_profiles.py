"""Readable per-conversation Agent configuration profiles."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path

from .local_config import resolve_profile_path as _resolve_vault_profile_path
from .model_catalog import load_model_catalog
from .prompt_catalog import load_prompt
from .session_recycle_policy import resolve_cache_recycle_rule

PROFILE_PATH = Path(__file__).resolve().parents[2] / "config" / "agent_profiles.toml"
LOCAL_PROFILE_FILENAME = "agent_profiles.local.toml"
_USE_LOCAL_OVERRIDES = True
_MODELS = load_model_catalog().all_model_ids()
_TOOLSETS = frozenset({"wechat", "cross_session", "web", "project", "workspace", "status"})
LUNA_INPUT_TOKENS = 128_000
LUNA_IDLE_COMPRESSION_TOKENS = 64_000
LUNA_ACTIVE_COMPRESSION_TOKENS = 96_000
DEFAULT_IDLE_SESSION_HOURS = 8
DEFAULT_MAX_HISTORY_RECORDS = 50
DEFAULT_MAX_HISTORY_BYTES = 32_000
DEFAULT_CONTEXT_WINDOW_BYTES = 32_000


@dataclass(frozen=True)
class AgentProfile:
    profile_id: str
    model_id: str
    persona_prompt: str
    main_prompt: str
    summary_prompt: str
    summary_system_prompt: str
    toolsets: tuple[str, ...]
    daily_budget_microusd: int
    text_budget_microusd: int
    image_read_budget_microusd: int
    image_generation_budget_microusd: int
    input_token_limit: int = LUNA_INPUT_TOKENS
    idle_compression_tokens: int = LUNA_IDLE_COMPRESSION_TOKENS
    active_compression_tokens: int = LUNA_ACTIVE_COMPRESSION_TOKENS
    output_token_limit: int | None = None
    token_context_limits: bool | None = None
    idle_session_hours: int = DEFAULT_IDLE_SESSION_HOURS
    max_history_records: int = DEFAULT_MAX_HISTORY_RECORDS
    max_history_bytes: int = DEFAULT_MAX_HISTORY_BYTES
    context_window_bytes: int = DEFAULT_CONTEXT_WINDOW_BYTES
    cache_tier: str | None = None
    cache_recycle_idle_seconds: int | None = None
    cache_recycle_min_tokens: int | None = None


@dataclass(frozen=True)
class AgentProfiles:
    default_profile: str
    profiles: dict[str, AgentProfile]
    bindings: dict[tuple[str, str], str]
    account_daily_budget_microusd: int
    account_text_budget_microusd: int
    account_image_read_budget_microusd: int
    account_image_generation_budget_microusd: int
    command_owner_ids: frozenset[str]
    memory_model_id: str = "gpt-6-luna"

    def for_conversation(self, account_id: str, conversation_id: str) -> AgentProfile:
        profile_id = self.bindings.get((account_id, conversation_id), self.default_profile)
        return self.profiles[profile_id]


def _positive_int(
    value: object,
    *,
    field: str,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    return value


def _usd_to_microusd(value: object, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative USD amount")
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative USD amount") from exc
    exp = amount.as_tuple().exponent
    if not amount.is_finite() or amount < 0 or (isinstance(exp, int) and exp < -6):
        raise ValueError(f"{field} must be a non-negative USD amount with at most 6 decimals")
    return int(amount * 1_000_000)


def resolve_profile_path(path: Path = PROFILE_PATH) -> Path:
    """Prefer centralized `.local/agent_profiles.toml`, then `agent_profiles.local.toml`."""
    if not _USE_LOCAL_OVERRIDES:
        return path
    resolved = _resolve_vault_profile_path(path)
    if resolved != path:
        return resolved
    if path.name == PROFILE_PATH.name:
        local = path.with_name(LOCAL_PROFILE_FILENAME)
        if local.is_file():
            return local
    return path


def load_agent_profiles(path: Path = PROFILE_PATH) -> AgentProfiles:
    """Load and validate profiles once at service startup; edits apply after restart."""
    resolved_path = resolve_profile_path(path)
    raw = tomllib.loads(resolved_path.read_text(encoding="utf-8"))
    catalog = load_model_catalog()
    supported_models = catalog.all_model_ids()
    memory_model = raw.get("memory_model", catalog.roles.default_memory_model)
    if not isinstance(memory_model, str) or memory_model not in supported_models:
        raise ValueError(f"memory_model {memory_model!r} is not in config/models.toml")
    default_id = raw.get("default_profile")
    raw_profiles = raw.get("profiles")
    if not isinstance(default_id, str) or not isinstance(raw_profiles, dict):
        raise ValueError("agent profile config requires default_profile and [profiles.*]")
    resolved_profiles: dict[str, dict[str, object]] = {}

    def resolve_profile(profile_id: str, chain: tuple[str, ...] = ()) -> dict[str, object]:
        if profile_id in resolved_profiles:
            return dict(resolved_profiles[profile_id])
        if profile_id in chain:
            raise ValueError("profile inheritance contains a cycle")
        item = raw_profiles.get(profile_id)
        if not isinstance(item, dict):
            raise ValueError(f"unknown or invalid profile {profile_id!r}")
        parent = item.get("extends")
        if parent is not None:
            if not isinstance(parent, str) or not parent:
                raise ValueError(f"profile {profile_id}: extends must be a profile name")
            merged = resolve_profile(parent, (*chain, profile_id))
        else:
            merged = {}
        merged.update({key: value for key, value in item.items() if key != "extends"})
        resolved_profiles[profile_id] = dict(merged)
        return merged

    profiles: dict[str, AgentProfile] = {}
    for profile_id in raw_profiles:
        item = resolve_profile(profile_id)
        if not isinstance(profile_id, str) or not profile_id or not isinstance(item, dict):
            raise ValueError("profile names must map to tables")
        model_id = item.get("model", catalog.roles.default_memory_model)
        if model_id not in supported_models:
            raise ValueError(f"profile {profile_id}: unsupported model {model_id!r}")
        persona = item.get("persona_prompt", "persona")
        main = item.get("main_prompt", "main_agent")
        summary = item.get("summary_prompt", "session_summary")
        summary_system = item.get("summary_system_prompt", "session_summary_system")
        if (
            not isinstance(persona, str)
            or not isinstance(main, str)
            or not isinstance(summary, str)
            or not isinstance(summary_system, str)
        ):
            raise ValueError(f"profile {profile_id}: prompt names must be strings")
        # Validate names here so a typo cannot silently fall back to another prompt.
        load_prompt(persona)
        load_prompt(main)
        load_prompt(summary)
        load_prompt(summary_system)
        toolsets = item.get("toolsets", sorted(_TOOLSETS))
        if (not isinstance(toolsets, list) or any(x not in _TOOLSETS for x in toolsets)
                or len(set(toolsets)) != len(toolsets)):
            raise ValueError(f"profile {profile_id}: invalid toolsets")
        idle_limit = item.get("idle_compression_tokens", LUNA_IDLE_COMPRESSION_TOKENS)
        active_limit = item.get("active_compression_tokens", LUNA_ACTIVE_COMPRESSION_TOKENS)
        input_limit = item.get("input_token_limit", LUNA_INPUT_TOKENS)
        if (
            type(idle_limit) is not int
            or type(active_limit) is not int
            or type(input_limit) is not int
            or idle_limit <= 0
            or not (idle_limit < active_limit < input_limit)
        ):
            raise ValueError(f"profile {profile_id}: require positive idle < active < input token limits")
        raw_output_limit = item.get("output_token_limit")
        output_token_limit = (
            _positive_int(raw_output_limit, field=f"{profile_id}.output_token_limit")
            if raw_output_limit is not None
            else None
        )
        raw_token_limits = item.get("token_context_limits")
        if raw_token_limits is not None and not isinstance(raw_token_limits, bool):
            raise ValueError(f"profile {profile_id}: token_context_limits must be a boolean")
        idle_session_hours = _positive_int(
            item.get("idle_session_hours", DEFAULT_IDLE_SESSION_HOURS),
            field=f"{profile_id}.idle_session_hours",
        )
        max_history_records = _positive_int(
            item.get("max_history_records", DEFAULT_MAX_HISTORY_RECORDS),
            field=f"{profile_id}.max_history_records",
            maximum=200,
        )
        max_history_bytes = _positive_int(
            item.get("max_history_bytes", DEFAULT_MAX_HISTORY_BYTES),
            field=f"{profile_id}.max_history_bytes",
        )
        context_window_bytes = _positive_int(
            item.get("context_window_bytes", DEFAULT_CONTEXT_WINDOW_BYTES),
            field=f"{profile_id}.context_window_bytes",
            minimum=512,
        )
        raw_cache_tier = item.get("cache_tier")
        if raw_cache_tier is not None and not isinstance(raw_cache_tier, str):
            raise ValueError(f"profile {profile_id}: cache_tier must be a string")
        cache_tier = raw_cache_tier.strip() if isinstance(raw_cache_tier, str) else None
        if cache_tier == "":
            cache_tier = None
        raw_cache_idle = item.get("cache_recycle_idle_seconds")
        cache_recycle_idle_seconds = (
            _positive_int(
                raw_cache_idle,
                field=f"{profile_id}.cache_recycle_idle_seconds",
                minimum=1,
            )
            if raw_cache_idle is not None
            else None
        )
        raw_cache_min_tokens = item.get("cache_recycle_min_tokens")
        cache_recycle_min_tokens = (
            _positive_int(
                raw_cache_min_tokens,
                field=f"{profile_id}.cache_recycle_min_tokens",
                minimum=0,
            )
            if raw_cache_min_tokens is not None
            else None
        )
        resolve_cache_recycle_rule(
            cache_tier=cache_tier,
            idle_seconds=cache_recycle_idle_seconds,
            min_context_tokens=cache_recycle_min_tokens,
        )
        profiles[profile_id] = AgentProfile(
            profile_id=profile_id,
            model_id=model_id,
            persona_prompt=persona,
            main_prompt=main,
            summary_prompt=summary,
            summary_system_prompt=summary_system,
            toolsets=tuple(toolsets),
            daily_budget_microusd=_usd_to_microusd(
                item.get("daily_budget_usd", "4.00"), field=f"{profile_id}.daily_budget_usd"),
            text_budget_microusd=_usd_to_microusd(
                item.get("text_budget_usd", "2.00"), field=f"{profile_id}.text_budget_usd"),
            image_read_budget_microusd=_usd_to_microusd(
                item.get("image_read_budget_usd", "1.00"),
                field=f"{profile_id}.image_read_budget_usd"),
            image_generation_budget_microusd=_usd_to_microusd(
                item.get("image_generation_budget_usd", "1.00"),
                field=f"{profile_id}.image_generation_budget_usd"),
            input_token_limit=input_limit,
            idle_compression_tokens=idle_limit,
            active_compression_tokens=active_limit,
            output_token_limit=output_token_limit,
            token_context_limits=raw_token_limits,
            idle_session_hours=idle_session_hours,
            max_history_records=max_history_records,
            max_history_bytes=max_history_bytes,
            context_window_bytes=context_window_bytes,
            cache_tier=cache_tier,
            cache_recycle_idle_seconds=cache_recycle_idle_seconds,
            cache_recycle_min_tokens=cache_recycle_min_tokens,
        )
    if default_id not in profiles:
        raise ValueError("default_profile must name a configured profile")
    bindings: dict[tuple[str, str], str] = {}
    rows = raw.get("bindings", [])
    if not isinstance(rows, list):
        raise ValueError("bindings must be an array of tables")
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("each binding must be a table")
        account = row.get("account_id")
        conversation = row.get("conversation_id")
        profile_id = row.get("profile")
        if (
            not isinstance(account, str)
            or not account.strip()
            or not isinstance(conversation, str)
            or not conversation.strip()
            or not isinstance(profile_id, str)
            or not profile_id.strip()
        ):
            raise ValueError("each binding needs account_id, conversation_id and profile")
        if profile_id not in profiles:
            raise ValueError(f"binding references unknown profile {profile_id!r}")
        key = (account.strip(), conversation.strip())
        if key in bindings:
            raise ValueError("duplicate account/conversation profile binding")
        bindings[key] = profile_id
    account_budget = _usd_to_microusd(
        raw.get("account_daily_budget_usd", "4.00"),
        field="account_daily_budget_usd",
    )
    account_text = _usd_to_microusd(
        raw.get("account_text_budget_usd", "2.00"), field="account_text_budget_usd")
    account_read = _usd_to_microusd(
        raw.get("account_image_read_budget_usd", "1.00"),
        field="account_image_read_budget_usd")
    account_generation = _usd_to_microusd(
        raw.get("account_image_generation_budget_usd", "1.00"),
        field="account_image_generation_budget_usd")
    owner_ids = raw.get("command_owner_ids", [])
    if not isinstance(owner_ids, list) or any(not isinstance(value, str) or not value for value in owner_ids):
        raise ValueError("command_owner_ids must be a list of non-empty IDs")
    if account_text + account_read + account_generation > account_budget:
        raise ValueError("account feature budgets cannot exceed account_daily_budget_usd")
    return AgentProfiles(default_id, profiles, bindings, account_budget,
                         account_text, account_read, account_generation, frozenset(owner_ids), memory_model)
