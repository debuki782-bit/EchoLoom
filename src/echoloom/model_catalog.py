"""Declarative model catalog loader (`config/models.toml` + optional `.local/models.toml`).

Keeps model pricing, default token limits, thinking presets, and auxiliary role model IDs
out of Python source code while remaining zero-LLM-SDK at import time.
"""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from . import local_config
from .session_recycle_policy import CacheRecycleRule, resolve_cache_recycle_rule

MODELS_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "models.toml"


@dataclass(frozen=True)
class RateTier:
    until_date: str
    input_rate_microusd: int
    output_rate_microusd: int
    cached_input_rate_microusd: int
    cache_write_rate_microusd: int


@dataclass(frozen=True)
class ModelSpec:
    model_id: str
    provider: str
    label: str
    input_token_limit: int
    output_token_limit: int
    input_rate_microusd: int
    output_rate_microusd: int
    cached_input_rate_microusd: int
    cache_write_rate_microusd: int
    thinking_level: str | None = None
    temporary_only_max_seconds: int | None = None
    cache_tier: str | None = None
    cache_recycle_idle_seconds: int | None = None
    cache_recycle_min_tokens: int | None = None
    rate_tiers: tuple[RateTier, ...] = ()

    def _active_tier(self, at: str) -> RateTier | None:
        if not self.rate_tiers:
            return None
        day = datetime.fromisoformat(at).date().isoformat()
        for tier in self.rate_tiers:
            if day <= tier.until_date:
                return tier
        return None

    def rates_for(self, at: str) -> tuple[int, int]:
        tier = self._active_tier(at)
        if tier is not None:
            return tier.input_rate_microusd, tier.output_rate_microusd
        return self.input_rate_microusd, self.output_rate_microusd

    def cached_input_rate_for(self, at: str) -> int:
        tier = self._active_tier(at)
        if tier is not None:
            return tier.cached_input_rate_microusd
        return self.cached_input_rate_microusd

    def cache_write_rate_for(self, at: str) -> int:
        tier = self._active_tier(at)
        if tier is not None:
            return tier.cache_write_rate_microusd
        return self.cache_write_rate_microusd


@dataclass(frozen=True)
class ModelRoles:
    default_memory_model: str = "gpt-6-luna"
    decision_model: str = "gemini-decision-1"
    decision_endpoint: str = "https://generativelanguage.googleapis.com/v1beta/decisions"
    web_model: str = "gemini-3.8-flash"
    image_description_model: str = "gemini-4-hydrogen"
    transcribe_model: str = "gemini-3.5-transcribe"
    image_generation_model: str = "gpt-image-2.5-flare"
    image_generation_reservation_microusd: int = 100_000
    embedding_model: str = "text-embedding-3-small"
    embedding_dimensions: int = 1536


@dataclass(frozen=True)
class ModelCatalog:
    roles: ModelRoles
    models: dict[str, ModelSpec]

    def get(self, model_id: str) -> ModelSpec | None:
        return self.models.get(model_id)

    def require(self, model_id: str) -> ModelSpec:
        spec = self.models.get(model_id)
        if spec is None:
            raise ValueError("model is not in the verified price table")
        return spec

    def provider_for(self, model_id: str) -> str:
        spec = self.models.get(model_id)
        if spec is not None:
            return spec.provider
        if model_id.startswith("gemini-"):
            return "gemini"
        return "openai"

    def token_limits_map(self) -> dict[str, tuple[int, int]]:
        return {
            mid: (spec.input_token_limit, spec.output_token_limit)
            for mid, spec in self.models.items()
        }

    def labels_map(self) -> dict[str, str]:
        return {mid: spec.label for mid, spec in self.models.items()}

    def gemini_model_ids(self) -> tuple[str, ...]:
        return tuple(mid for mid, spec in self.models.items() if spec.provider == "gemini")

    def all_model_ids(self) -> frozenset[str]:
        return frozenset(self.models.keys())

    def cache_rule_for(self, model_id: str) -> CacheRecycleRule | None:
        spec = self.models.get(model_id)
        if spec is None:
            return None
        return resolve_cache_recycle_rule(
            cache_tier=spec.cache_tier,
            idle_seconds=spec.cache_recycle_idle_seconds,
            min_context_tokens=spec.cache_recycle_min_tokens,
        )


def _nonneg_int(value: object, *, field: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    return value


def _read_toml_file(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    return raw if isinstance(raw, dict) else {}


def _merge_catalog_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "roles": dict(base.get("roles") or {}),
        "models": {
            key: dict(val)
            for key, val in (base.get("models") or {}).items()
            if isinstance(val, dict)
        },
    }
    if isinstance(override.get("roles"), dict):
        merged["roles"].update(override["roles"])
    if isinstance(override.get("models"), dict):
        for model_id, entry in override["models"].items():
            if not isinstance(entry, dict):
                continue
            existing = dict(merged["models"].get(model_id) or {})
            existing.update(entry)
            merged["models"][model_id] = existing
    return merged


def _parse_model_spec(model_id: str, item: dict[str, Any]) -> ModelSpec:
    provider = str(item.get("provider") or "").strip().lower()
    if provider not in {"openai", "gemini"}:
        raise ValueError(f"model {model_id}: unsupported provider {provider!r}")
    label = str(item.get("label") or model_id).strip() or model_id
    input_limit = _nonneg_int(
        item.get("input_token_limit"), field=f"{model_id}.input_token_limit", minimum=1
    )
    output_limit = _nonneg_int(
        item.get("output_token_limit"), field=f"{model_id}.output_token_limit", minimum=1
    )
    input_rate = _nonneg_int(item.get("input_rate_microusd"), field=f"{model_id}.input_rate_microusd")
    output_rate = _nonneg_int(
        item.get("output_rate_microusd"), field=f"{model_id}.output_rate_microusd"
    )
    cached_rate = _nonneg_int(
        item.get("cached_input_rate_microusd", input_rate),
        field=f"{model_id}.cached_input_rate_microusd",
    )
    write_rate = _nonneg_int(
        item.get("cache_write_rate_microusd", input_rate),
        field=f"{model_id}.cache_write_rate_microusd",
    )
    thinking = item.get("thinking_level")
    if thinking is not None:
        thinking = str(thinking).strip().upper() or None
    temp_max = item.get("temporary_only_max_seconds")
    if temp_max is not None:
        temp_max = _nonneg_int(temp_max, field=f"{model_id}.temporary_only_max_seconds", minimum=1)
    raw_cache_tier = item.get("cache_tier")
    cache_tier = str(raw_cache_tier).strip() if raw_cache_tier is not None else None
    if cache_tier == "":
        cache_tier = None
    raw_cache_idle = item.get("cache_recycle_idle_seconds")
    cache_recycle_idle_seconds = (
        _nonneg_int(
            raw_cache_idle,
            field=f"{model_id}.cache_recycle_idle_seconds",
            minimum=1,
        )
        if raw_cache_idle is not None
        else None
    )
    raw_cache_min_tokens = item.get("cache_recycle_min_tokens")
    cache_recycle_min_tokens = (
        _nonneg_int(
            raw_cache_min_tokens,
            field=f"{model_id}.cache_recycle_min_tokens",
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
    tiers_raw = item.get("rate_tiers") or ()
    tiers: list[RateTier] = []
    if isinstance(tiers_raw, list):
        for idx, entry in enumerate(tiers_raw):
            if not isinstance(entry, dict):
                raise ValueError(f"{model_id}.rate_tiers[{idx}] must be a table")
            until = str(entry.get("until_date") or "").strip()
            if not until:
                raise ValueError(f"{model_id}.rate_tiers[{idx}].until_date is required")
            t_in = _nonneg_int(
                entry.get("input_rate_microusd"),
                field=f"{model_id}.rate_tiers[{idx}].input_rate_microusd",
            )
            t_out = _nonneg_int(
                entry.get("output_rate_microusd"),
                field=f"{model_id}.rate_tiers[{idx}].output_rate_microusd",
            )
            t_cached = _nonneg_int(
                entry.get("cached_input_rate_microusd", t_in),
                field=f"{model_id}.rate_tiers[{idx}].cached_input_rate_microusd",
            )
            t_write = _nonneg_int(
                entry.get("cache_write_rate_microusd", t_in),
                field=f"{model_id}.rate_tiers[{idx}].cache_write_rate_microusd",
            )
            tiers.append(RateTier(until, t_in, t_out, t_cached, t_write))
    tiers.sort(key=lambda tier: tier.until_date)
    return ModelSpec(
        model_id=model_id,
        provider=provider,
        label=label,
        input_token_limit=input_limit,
        output_token_limit=output_limit,
        input_rate_microusd=input_rate,
        output_rate_microusd=output_rate,
        cached_input_rate_microusd=cached_rate,
        cache_write_rate_microusd=write_rate,
        thinking_level=thinking,
        temporary_only_max_seconds=temp_max,
        cache_tier=cache_tier,
        cache_recycle_idle_seconds=cache_recycle_idle_seconds,
        cache_recycle_min_tokens=cache_recycle_min_tokens,
        rate_tiers=tuple(tiers),
    )


def load_model_catalog(
    path: Path | None = None,
    *,
    workspace: Path | None = None,
) -> ModelCatalog:
    """Load `config/models.toml` merged with optional `.local/models.toml`."""
    if path is not None:
        base_path = Path(path)
        root = workspace if workspace is not None else (
            base_path.parents[1] if base_path.parent.name == "config" else local_config.ROOT
        )
    else:
        root = Path(workspace) if workspace is not None else local_config.ROOT
        candidate = root / "config" / "models.toml"
        base_path = candidate if candidate.is_file() else MODELS_CONFIG_PATH

    base_raw = _read_toml_file(base_path)
    if not base_raw:
        raise ValueError(f"model catalog missing or empty: {base_path}")
    override_raw: dict[str, Any] = {}
    if local_config.local_overrides_enabled():
        override_raw = _read_toml_file(local_config.vault_dir(root) / "models.toml")
    merged = _merge_catalog_dicts(base_raw, override_raw)

    roles_raw = merged.get("roles") if isinstance(merged.get("roles"), dict) else {}
    roles = ModelRoles(
        default_memory_model=str(roles_raw.get("default_memory_model") or "gpt-6-luna").strip(),
        decision_model=str(roles_raw.get("decision_model") or "gemini-decision-1").strip(),
        decision_endpoint=str(
            roles_raw.get("decision_endpoint")
            or "https://generativelanguage.googleapis.com/v1beta/decisions"
        ).strip(),
        web_model=str(roles_raw.get("web_model") or "gemini-3.8-flash").strip(),
        image_description_model=str(
            roles_raw.get("image_description_model") or "gemini-4-hydrogen"
        ).strip(),
        transcribe_model=str(
            roles_raw.get("transcribe_model") or "gemini-3.5-transcribe"
        ).strip(),
        image_generation_model=str(
            roles_raw.get("image_generation_model") or "gpt-image-2.5-flare"
        ).strip(),
        image_generation_reservation_microusd=_nonneg_int(
            roles_raw.get("image_generation_reservation_microusd", 100_000),
            field="roles.image_generation_reservation_microusd",
            minimum=1,
        ),
        embedding_model=str(
            roles_raw.get("embedding_model") or "text-embedding-3-small"
        ).strip(),
        embedding_dimensions=_nonneg_int(
            roles_raw.get("embedding_dimensions", 1536),
            field="roles.embedding_dimensions",
            minimum=1,
        ),
    )

    models_raw = merged.get("models") if isinstance(merged.get("models"), dict) else {}
    models = {
        mid: _parse_model_spec(mid, entry)
        for mid, entry in models_raw.items()
        if isinstance(mid, str) and mid.strip() and isinstance(entry, dict)
    }
    if not models:
        raise ValueError("model catalog must define at least one model")
    return ModelCatalog(roles=roles, models=models)
