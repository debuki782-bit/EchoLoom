"""Pure-function prompt-cache-aware session recycle policy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

CacheTier = Literal["long_cache_1h", "short_cache_5m", "subscription_1h", "apikey_5m"]

_TIER_PRESETS: dict[str, tuple[int, int]] = {
    "long_cache_1h": (3600, 64_000),
    "subscription_1h": (3600, 64_000),
    "short_cache_5m": (720, 8_000),
    "apikey_5m": (720, 8_000),
}


@dataclass(frozen=True)
class CacheRecycleRule:
    """Prompt-cache recycle thresholds for an idle session."""

    cache_tier: str | None = None
    idle_seconds: int | None = None
    min_context_tokens: int | None = None
    min_context_bytes: int | None = None


@dataclass(frozen=True)
class CacheRecycleDecision:
    """Deterministic decision returned by evaluate_cache_recycle."""

    recycle: bool
    reason: str


def resolve_cache_recycle_rule(
    *,
    cache_tier: str | None = None,
    idle_seconds: int | None = None,
    min_context_tokens: int | None = None,
    min_context_bytes: int | None = None,
) -> CacheRecycleRule | None:
    """Resolve optional cache-tier presets and per-model/profile overrides."""
    normalized_tier = cache_tier.strip() if isinstance(cache_tier, str) else None
    if normalized_tier == "":
        normalized_tier = None
    preset_idle: int | None = None
    preset_tokens: int | None = None
    if normalized_tier is not None:
        if normalized_tier not in _TIER_PRESETS:
            supported = ", ".join(sorted(_TIER_PRESETS))
            raise ValueError(f"unsupported cache_tier {normalized_tier!r}; expected {supported}")
        preset_idle, preset_tokens = _TIER_PRESETS[normalized_tier]

    resolved_idle = idle_seconds if idle_seconds is not None else preset_idle
    if resolved_idle is None:
        return None
    if resolved_idle < 1:
        raise ValueError("cache_recycle_idle_seconds must be >= 1")

    resolved_tokens = min_context_tokens if min_context_tokens is not None else preset_tokens
    if resolved_tokens is not None and resolved_tokens < 0:
        raise ValueError("cache_recycle_min_tokens must be >= 0")
    if min_context_bytes is not None and min_context_bytes < 0:
        raise ValueError("cache_recycle_min_bytes must be >= 0")

    return CacheRecycleRule(
        cache_tier=normalized_tier,
        idle_seconds=resolved_idle,
        min_context_tokens=resolved_tokens,
        min_context_bytes=min_context_bytes,
    )


def evaluate_cache_recycle(
    *,
    idle_seconds: float,
    context_tokens: int | None,
    session_bytes: int,
    token_context_limits: bool,
    rule: CacheRecycleRule | None,
) -> CacheRecycleDecision:
    """Return whether an idle session's prompt cache is cold enough to summarize and rotate."""
    if rule is None or rule.idle_seconds is None:
        return CacheRecycleDecision(recycle=False, reason="disabled")
    if idle_seconds <= rule.idle_seconds:
        return CacheRecycleDecision(recycle=False, reason="within_cache_window")

    if token_context_limits and rule.min_context_tokens is not None:
        if context_tokens is None or context_tokens < rule.min_context_tokens:
            return CacheRecycleDecision(recycle=False, reason="below_min_context_tokens")
        return CacheRecycleDecision(recycle=True, reason="cache_cold_recycle")

    min_bytes = rule.min_context_bytes
    if min_bytes is None and rule.min_context_tokens is not None:
        min_bytes = rule.min_context_tokens * 3
    if min_bytes is not None and session_bytes < min_bytes:
        return CacheRecycleDecision(recycle=False, reason="below_min_context_bytes")

    return CacheRecycleDecision(recycle=True, reason="cache_cold_recycle")
