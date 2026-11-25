"""Application-owned route, profile and media budget scope."""
from contextlib import contextmanager
from contextvars import ContextVar
from .usage import (DAILY_ACCOUNT_BUDGET_MICROUSD, TEXT_MODEL_DAILY_BUDGET_MICROUSD, IMAGE_READ_DAILY_BUDGET_MICROUSD, IMAGE_GENERATION_DAILY_BUDGET_MICROUSD)

_route: ContextVar[tuple[str, str, str] | None] = ContextVar("model_usage_route", default=None)
_profile_budgets: ContextVar[tuple[int, int, int, int] | None] = ContextVar(
    "model_profile_budgets", default=None,
)
_image_access_blocked: ContextVar[bool] = ContextVar("image_access_blocked", default=False)

@contextmanager
def model_usage_scope(
    account_id: str, conversation_id: str, *, profile_id: str = "default",
    daily_budget_microusd: int = DAILY_ACCOUNT_BUDGET_MICROUSD,
    text_budget_microusd: int = TEXT_MODEL_DAILY_BUDGET_MICROUSD,
    image_read_budget_microusd: int = IMAGE_READ_DAILY_BUDGET_MICROUSD,
    image_generation_budget_microusd: int = IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
):
    """Bind server routing around a runner call; never infer it from LLM input."""
    if not account_id or not conversation_id:
        raise ValueError("missing usage route")
    token = _route.set((account_id, conversation_id, profile_id))
    budget_token = _profile_budgets.set((
        daily_budget_microusd, text_budget_microusd,
        image_read_budget_microusd, image_generation_budget_microusd,
    ))
    image_token = _image_access_blocked.set(False)
    try:
        yield
    finally:
        _image_access_blocked.reset(image_token)
        _profile_budgets.reset(budget_token)
        _route.reset(token)


def image_access_blocked() -> bool:
    """True after this model turn cannot include image pixels; prevents repeat reads."""
    return _image_access_blocked.get()


def current_model_usage_route() -> tuple[str, str] | None:
    """Return the account/conversation bound to the active Agent turn."""
    route = _route.get()
    return (route[0], route[1]) if route is not None else None


def current_model_usage_profile() -> tuple[str, int, int, int, int]:
    """Return the active profile id and its per-scope daily caps."""
    route = _route.get()
    budgets = _profile_budgets.get()
    if route is None or budgets is None:
        return ("default", DAILY_ACCOUNT_BUDGET_MICROUSD,
                TEXT_MODEL_DAILY_BUDGET_MICROUSD, IMAGE_READ_DAILY_BUDGET_MICROUSD,
                IMAGE_GENERATION_DAILY_BUDGET_MICROUSD)
    return (route[2], *budgets)


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
