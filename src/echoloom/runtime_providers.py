"""Provider client construction, usage ledger wiring, and health probe factory."""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

from . import local_config
from .agent_profiles import AgentProfile, AgentProfiles
from .credentials import OPENAI_TARGET
from .model_catalog import load_model_catalog
from .runtime_route import ProviderBundle

logger = logging.getLogger("echoloom.startup")


def build_providers(
    root: Path,
    profile_config: AgentProfiles,
    *,
    load_key_fn: Callable[..., str | None],
    genai_resolver: Callable[[], Any],
    refreshable_gemini_resolver: Callable[[], Any],
) -> ProviderBundle:
    """Build OpenAI/Gemini clients, refreshable pool probe, and budgeted model factories."""
    from google.genai import types
    from openai import AsyncOpenAI

    from .frameworks.adk_budget import BudgetedAdkModel
    from .frameworks.adk_openai import OpenAIAdkModel
    from .frameworks.google_web import GoogleWebModel
    from .mock import SystemClock
    from .storage import Repository
    from .usage import UsageLedger

    workspace = Path(root)
    catalog = load_model_catalog(workspace=workspace)
    runtime_cfg = local_config.load_local_runtime_config(workspace)
    rag_env = os.environ.get("ECHOLOOM_EPISODE_RAG") or os.environ.get("WECHAT_AGENT_EPISODE_RAG")
    episode_rag_enabled = (
        rag_env == "1" if rag_env is not None else runtime_cfg.server.episode_rag_enabled
    )
    used_models = {p.model_id for p in profile_config.profiles.values()} | {
        profile_config.memory_model_id
    }
    has_web_tool = any("web" in p.toolsets for p in profile_config.profiles.values())
    needs_openai = (
        any(catalog.provider_for(mid) == "openai" for mid in used_models)
        or episode_rag_enabled
    )
    needs_gemini = (
        any(catalog.provider_for(mid) == "gemini" for mid in used_models)
        or has_web_tool
    )
    openai_key = os.environ.get("OPENAI_API_KEY") or load_key_fn(target=OPENAI_TARGET)
    if needs_openai and not openai_key:
        raise SystemExit(
            "OpenAI credential missing: store --service openai or set OPENAI_API_KEY "
            "(required by OpenAI profiles, memory_model, or episode_rag_enabled=true)"
        )
    openai_client = (
        AsyncOpenAI(api_key=openai_key, timeout=60, max_retries=0) if openai_key else None
    )
    gemini_key = os.environ.get("GEMINI_API_KEY") or load_key_fn()
    if needs_gemini and not gemini_key:
        raise SystemExit(
            "Gemini credential missing: store --service model or set GEMINI_API_KEY"
        )

    clock = SystemClock()
    repo = Repository(workspace / "data" / "pilot-usage.sqlite3")
    genai_module = genai_resolver()
    refreshable_gemini_cls = refreshable_gemini_resolver()

    def usage_ledger() -> UsageLedger:
        return UsageLedger(repo, clock)

    def new_gemini_client(timeout_ms: int):
        if not gemini_key:
            raise RuntimeError("Gemini client requested without GEMINI_API_KEY configured")
        return genai_module.Client(
            api_key=gemini_key,
            http_options=types.HttpOptions(
                timeout=timeout_ms,
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )

    gemini_clients: dict[int, Any] = {}

    def get_gemini_client(timeout_ms: int):
        client = gemini_clients.get(timeout_ms)
        if client is None:
            client = new_gemini_client(timeout_ms)
            gemini_clients[timeout_ms] = client
        return client

    def new_model_client():
        return new_gemini_client(20000)

    model_client = new_model_client() if gemini_key else None

    def get_model_client():
        return model_client

    async def count_model_tokens(**kwargs):
        active = get_model_client()
        if active is None:
            raise RuntimeError("Gemini token counter unavailable without GEMINI_API_KEY")
        return await active.aio.models.count_tokens(**kwargs)

    count_model_tokens._gemini_client_provider = get_model_client  # type: ignore[attr-defined]

    def _collect_stale_gemini_clients() -> list[Any]:
        stale_clients: list[Any] = []
        for timeout_key in list(gemini_clients):
            stale_clients.append(gemini_clients[timeout_key])
            gemini_clients[timeout_key] = new_gemini_client(timeout_key)
        return stale_clients

    def _schedule_close_clients(stale_clients: list[Any]) -> None:
        if not stale_clients:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        async def _close_all() -> None:
            for old_client in stale_clients:
                with suppress(Exception):
                    await old_client.aio.aclose()

        loop.create_task(_close_all())

    def invalidate_gemini_clients() -> None:
        _schedule_close_clients(_collect_stale_gemini_clients())

    async def refresh_gemini_clients() -> None:
        stale_clients = _collect_stale_gemini_clients()
        for old_client in stale_clients:
            with suppress(Exception):
                await old_client.aio.aclose()

    def make_adk_model(
        profile: AgentProfile,
        *,
        timeout_ms: int | None = None,
        web: bool = False,
    ):
        is_gemini = catalog.provider_for(profile.model_id) == "gemini"
        resolved_timeout_ms = (
            (90_000 if (is_gemini and not web) else 60_000)
            if timeout_ms is None
            else timeout_ms
        )
        delegate: Any
        if not is_gemini:
            if openai_client is None:
                raise RuntimeError(f"OpenAI client required for model {profile.model_id!r}")
            delegate = OpenAIAdkModel(
                profile.model_id,
                openai_client,
                timeout_seconds=resolved_timeout_ms / 1000,
            )
            token_counter = None
        elif web:
            delegate = GoogleWebModel(profile.model_id, get_model_client)
            token_counter = count_model_tokens
        else:
            delegate = refreshable_gemini_cls(
                model=profile.model_id,
                client_provider=lambda t=resolved_timeout_ms: get_gemini_client(t),
            )
            token_counter = count_model_tokens
        return BudgetedAdkModel(
            delegate,
            usage_ledger(),
            clock,
            model_id=profile.model_id,
            budget_microusd=profile_config.account_daily_budget_microusd,
            max_input_tokens=profile.input_token_limit,
            max_output_tokens=4096 if web else profile.output_token_limit,
            token_counter=token_counter,
            http_timeout_ms=resolved_timeout_ms,
            use_streaming=(is_gemini and not web),
            on_indeterminate=invalidate_gemini_clients if is_gemini else None,
            budget_scope_id=profile.profile_id,
            scope_budget_microusd=profile.daily_budget_microusd,
            text_budget_microusd=profile.text_budget_microusd,
            image_read_budget_microusd=profile.image_read_budget_microusd,
            image_generation_budget_microusd=profile.image_generation_budget_microusd,
            account_text_budget_microusd=profile_config.account_text_budget_microusd,
            account_image_read_budget_microusd=profile_config.account_image_read_budget_microusd,
            idle_compression_tokens=profile.idle_compression_tokens,
            active_compression_tokens=profile.active_compression_tokens,
        )

    default_profile = profile_config.profiles[profile_config.default_profile]

    async def provider_check() -> bool:
        nonlocal model_client
        if model_client is None:
            if openai_client is None:
                return False
            try:
                async with asyncio.timeout(5.0):
                    counted = await openai_client.responses.input_tokens.count(
                        model=default_profile.model_id,
                        input=[
                            {"role": "user", "content": [{"type": "input_text", "text": "ping"}]}
                        ],
                    )
                return isinstance(getattr(counted, "input_tokens", None), int)
            except Exception as exc:  # noqa: BLE001
                logger.info(
                    "event=provider_probe_check_failed error_type=%s",
                    type(exc).__name__,
                )
                return False
        try:
            async with asyncio.timeout(5.0):
                probe_model = (
                    default_profile.model_id
                    if catalog.provider_for(default_profile.model_id) == "gemini"
                    else catalog.roles.web_model
                )
                counted = await model_client.aio.models.count_tokens(
                    model=probe_model,
                    contents="ping",
                )
            return isinstance(getattr(counted, "total_tokens", None), int)
        except Exception as exc:  # noqa: BLE001
            logger.info(
                "event=provider_probe_check_failed error_type=%s",
                type(exc).__name__,
            )
            stale_clients: list[Any] = [model_client, *_collect_stale_gemini_clients()]
            model_client = new_model_client()
            for old_client in stale_clients:
                with suppress(Exception):
                    await old_client.aio.aclose()
            return False

    return ProviderBundle(
        clock=clock,
        repo=repo,
        openai_client=openai_client,
        get_model_client=get_model_client,
        get_gemini_client=get_gemini_client,
        make_adk_model=make_adk_model,
        provider_check=provider_check,
        usage_ledger=usage_ledger,
        refresh_gemini_clients=refresh_gemini_clients,
    )
