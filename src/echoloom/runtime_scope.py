"""Application-owned route, profile, and media budget scope in one ContextVar."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, replace

from .usage import (
    DAILY_ACCOUNT_BUDGET_MICROUSD,
    IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
    IMAGE_READ_DAILY_BUDGET_MICROUSD,
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
)


@dataclass(frozen=True)
class UsageScopeState:
    account_id: str
    conversation_id: str
    profile_id: str = "default"
    daily_budget_microusd: int = DAILY_ACCOUNT_BUDGET_MICROUSD
    text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD
    image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD
    image_generation_budget_microusd: int = IMAGE_GENERATION_DAILY_BUDGET_MICROUSD
    image_access_blocked: bool = False


_usage_scope: ContextVar[UsageScopeState | None] = ContextVar(
    "echoloom_usage_scope",
    default=None,
)


class _ImageBlockedProxy:
    """Compatibility wrapper over ``UsageScopeState.image_access_blocked``."""

    def get(self) -> bool:
        scope = _usage_scope.get()
        return scope.image_access_blocked if scope is not None else False

    def set(self, value: bool) -> Token[UsageScopeState | None]:
        scope = _usage_scope.get() or UsageScopeState("", "")
        return _usage_scope.set(replace(scope, image_access_blocked=bool(value)))

    def reset(self, token: Token[UsageScopeState | None]) -> None:
        _usage_scope.reset(token)


class _RouteProxy:
    def get(self) -> tuple[str, str, str] | None:
        scope = _usage_scope.get()
        if scope is None or not scope.account_id or not scope.conversation_id:
            return None
        return (scope.account_id, scope.conversation_id, scope.profile_id)


class _BudgetsProxy:
    def get(self) -> tuple[int, int, int, int] | None:
        scope = _usage_scope.get()
        if scope is None:
            return None
        return (
            scope.daily_budget_microusd,
            scope.text_budget_microusd,
            scope.image_read_budget_microusd,
            scope.image_generation_budget_microusd,
        )


_route = _RouteProxy()
_profile_budgets = _BudgetsProxy()
_image_access_blocked = _ImageBlockedProxy()


@contextmanager
def model_usage_scope(
    account_id: str,
    conversation_id: str,
    *,
    profile_id: str = "default",
    daily_budget_microusd: int = DAILY_ACCOUNT_BUDGET_MICROUSD,
    text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD,
    image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD,
    image_generation_budget_microusd: int = IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
):
    """Bind server routing around a runner call; never infer it from LLM input."""
    if not account_id or not conversation_id:
        raise ValueError("missing usage route")
    token = _usage_scope.set(
        UsageScopeState(
            account_id=account_id,
            conversation_id=conversation_id,
            profile_id=profile_id,
            daily_budget_microusd=daily_budget_microusd,
            text_budget_microusd=text_budget_microusd,
            image_read_budget_microusd=image_read_budget_microusd,
            image_generation_budget_microusd=image_generation_budget_microusd,
            image_access_blocked=False,
        )
    )
    try:
        yield
    finally:
        _usage_scope.reset(token)


def block_image_access() -> None:
    """Mark image pixels inaccessible for the active turn."""
    _image_access_blocked.set(True)


def image_access_blocked() -> bool:
    """True after this model turn cannot include image pixels; prevents repeat reads."""
    return _image_access_blocked.get()


def current_model_usage_route() -> tuple[str, str] | None:
    """Return the account/conversation bound to the active Agent turn."""
    scope = _usage_scope.get()
    if scope is None or not scope.account_id or not scope.conversation_id:
        return None
    return (scope.account_id, scope.conversation_id)


def current_model_usage_profile() -> tuple[str, int, int, int, int]:
    """Return the active profile id and its per-scope daily caps."""
    scope = _usage_scope.get()
    if scope is None:
        return (
            "default",
            DAILY_ACCOUNT_BUDGET_MICROUSD,
            TEXT_MODEL_DAILY_BUDGET_MICROUSD,
            IMAGE_READ_DAILY_BUDGET_MICROUSD,
            IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
        )
    return (
        scope.profile_id,
        scope.daily_budget_microusd,
        scope.text_budget_microusd,
        scope.image_read_budget_microusd,
        scope.image_generation_budget_microusd,
    )


def current_model_usage_scope_kwargs() -> dict[str, object]:
    """Carry the active chat profile into nested tool Agents."""
    profile_id, daily, text, image_read, image_generation = current_model_usage_profile()
    return {
        "profile_id": profile_id,
        "daily_budget_microusd": daily,
        "text_budget_microusd": text,
        "image_read_budget_microusd": image_read,
        "image_generation_budget_microusd": image_generation,
    }
