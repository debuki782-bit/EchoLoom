"""Composition root for the WeChat AI Agent runtime, worker, and standalone Ingress.

Top-level imports are intentionally free of LLM SDKs (``google.adk``, ``google.genai``,
``openai``, ``episode_memory``, ``session_runtime``) so Scheme A standalone
Ingress (``scripts/run-ingress.py`` and ``--role ingress``) stays zero-LLM-import clean.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable, Mapping
from contextlib import ExitStack, asynccontextmanager, contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

import httpx
import uvicorn
from fastapi import FastAPI

from . import local_config
from .connectors.base import (
    AccountState,
    GatewayConfig,
    WeChatGateway,
    connector_ensure_ready,
    connector_normalize_event,
    create_gateway,
    default_gateway_factory,
)
from .connectors.registry import SourceRegistry
from .contacts import is_group_conversation, is_room_id
from .credentials import OPENAI_TARGET, load_key
from .ingress import Ingress, create_ingress_app, sha
from .model_catalog import load_model_catalog
from .models import ConversationKey
from .pilot import Pilot, create_pilot_app

if TYPE_CHECKING:
    from .agent_profiles import AgentProfile, AgentProfileConfig
    from .context_assembler import MessageContextAssembler
    from .context_builder import ContextBuilder
    from .cross_session import CrossSessionService
    from .cross_session_tools import CrossSessionToolset
    from .episode_memory import EpisodeMemory
    from .event_book import EventBook
    from .idle_tools import IdleActivityToolset
    from .media import WeChatImageIngestor, WeChatVoiceIngestor
    from .memory_store import MemoryStore
    from .message_ingestor import MessageIngestor
    from .mock import SystemClock
    from .model_overrides import ModelOverrideStore
    from .outbound import OutboundService
    from .project_tools import ProjectTools
    from .storage import Repository
    from .temporal_context import DailyEnvironmentProvider
    from .time_scheduler import TimeScheduler
    from .toolsets import MediaToolset, MemoryToolset, ReplyToolset
    from .web_tools import WebSearchUsage
    from .wechat_tools import WeChatTools

    class RefreshableGemini:  # pragma: no cover - lazily built in __getattr__
        def __init__(self, *, client_provider: Callable[[], Any] | None = None, **kwargs: Any): ...
        @property
        def api_client(self) -> Any: ...

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_GATEWAY_FACTORY = default_gateway_factory()
SUPPORTED_MODELS = load_model_catalog().gemini_model_ids()

logger = logging.getLogger("wechat_agent.startup")
ingress_logger = logging.getLogger("wechat_agent.ingress_server")

__all__ = [
    "DEFAULT_GATEWAY_FACTORY",
    "ROOT",
    "SUPPORTED_MODELS",
    "LiveRoute",
    "ProviderBundle",
    "RefreshableGemini",
    "RunnerBundle",
    "RuntimeBundle",
    "ServiceBundle",
    "admit_group_route",
    "build_ingress_app",
    "build_providers",
    "build_runners",
    "build_runtime_bundle",
    "build_services",
    "load_live_route",
    "run_ingress",
    "run_pilot",
    "run_worker_loop",
    "sync_approved_rooms_from_route",
]


def _build_refreshable_gemini_class():
    from google.adk.models.google_llm import Gemini
    from pydantic import PrivateAttr

    class RefreshableGemini(Gemini):
        """Gemini adapter reading the active refreshed client without per-loop stale pools."""

        _client_provider = PrivateAttr(default=None)

        def __init__(self, *, client_provider=None, **kwargs):
            super().__init__(**kwargs)
            self._client_provider = client_provider

        @property
        def api_client(self):
            if self._client_provider is not None:
                return self._client_provider()
            return super().api_client

    RefreshableGemini.__module__ = __name__
    return RefreshableGemini


def __getattr__(name: str) -> Any:
    if name == "RefreshableGemini":
        cls = _build_refreshable_gemini_class()
        globals()[name] = cls
        return cls
    if name == "ScopedAgentRuntime":
        from .session_runtime import ScopedAgentRuntime

        globals()[name] = ScopedAgentRuntime
        return ScopedAgentRuntime
    if name == "WeChatTools":
        from .wechat_tools import WeChatTools

        globals()[name] = WeChatTools
        return WeChatTools
    if name == "genai":
        from google import genai

        globals()[name] = genai
        return genai
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _resolve(name: str) -> Any:
    if name in globals():
        return globals()[name]
    return __getattr__(name)


@contextmanager
def _temporary_overrides(overrides: Mapping[str, object] | None):
    if not overrides:
        yield
        return
    sentinel = object()
    previous: dict[str, object] = {}
    module_globals = globals()
    try:
        for name, value in overrides.items():
            previous[name] = module_globals.get(name, sentinel)
            module_globals[name] = value
        yield
    finally:
        for name, old_value in previous.items():
            if old_value is sentinel:
                module_globals.pop(name, None)
            else:
                module_globals[name] = old_value


@dataclass
class LiveRoute:
    """Validated account/group routing state loaded from data/live-route.json."""

    route_path: Path
    saved: dict[str, object]
    account: str
    room: str
    approved_rooms: set[str]


@dataclass
class ProviderBundle:
    """Model provider clients, usage ledger repo, budgeted factories, and health probe."""

    clock: SystemClock
    repo: Repository
    openai_client: Any
    get_model_client: Callable[[], Any]
    get_gemini_client: Callable[[int], Any]
    make_adk_model: Callable[..., Any]
    provider_check: Callable[[], Awaitable[bool]]
    usage_ledger: Callable[[], Any]

    @property
    def model_client(self) -> Any:
        return self.get_model_client()


@dataclass
class ServiceBundle:
    """Layer 2/4/5 services and toolsets wired for one account runtime."""

    memory_store: MemoryStore
    time_scheduler: TimeScheduler
    event_book: EventBook
    episode_memory: EpisodeMemory | None
    outbound: OutboundService
    tools: WeChatTools
    reply_tools: ReplyToolset
    media_tools: MediaToolset
    memory_tools: MemoryToolset
    model_overrides: ModelOverrideStore
    image_ingestor: WeChatImageIngestor
    voice_ingestor: WeChatVoiceIngestor
    daily_environment: DailyEnvironmentProvider
    message_ingestor: MessageIngestor
    context_builder: ContextBuilder
    context_assembler: MessageContextAssembler
    web_usage: WebSearchUsage
    cross_sessions: CrossSessionService
    cross_tools: CrossSessionToolset
    project_tools: ProjectTools
    idle_tools: IdleActivityToolset
    profile_tools: Callable[[AgentProfile, Any], list[Any]]


@dataclass
class RunnerBundle:
    """Profile chat runner cache, idle runners, reply dispatcher, and owner commands."""

    runner_cache: dict[tuple[str, str], Any]
    runner_for: Callable[[AgentProfile, str | None], Any]
    runner_for_conversation: Callable[[str], Any]
    idle_runners: dict[str, Any]
    reply_with_profile: Callable[[Any], Awaitable[str]]
    handle_special_command: Callable[[Any], Awaitable[str | None]]


@dataclass
class RuntimeBundle:
    """Complete assembled runtime for ``all``, ``worker``, or ``ingress`` roles."""

    role: Literal["all", "ingress", "worker"]
    root: Path
    route: LiveRoute
    gateway: WeChatGateway | None = None
    is_logged_in: bool = False
    ingress: Ingress | None = None
    app: FastAPI | None = None
    ingress_db: sqlite3.Connection | None = None
    profile_config: AgentProfileConfig | None = None
    providers: ProviderBundle | None = None
    services: ServiceBundle | None = None
    runners: RunnerBundle | None = None
    pilot: Pilot | None = None
    run_idle_event: Callable[[str, int, str], Awaitable[None]] | None = None
    close_episode_memory: Callable[[], Awaitable[None]] | None = None


def load_live_route(root: Path) -> LiveRoute:
    """Load and validate live route from `.local/route.json`, `data/live-route.json`, or config."""
    workspace = Path(root)
    route_path = local_config.resolve_route_path(workspace)
    saved = json.loads(route_path.read_text("utf-8")) if route_path.exists() else {}
    runtime_cfg = local_config.load_local_runtime_config(workspace)
    default_source_id, source_specs = local_config.load_source_configs(workspace)
    if not saved and not runtime_cfg.account and not source_specs:
        default_source_id, source_specs = local_config.load_source_configs(
            workspace, allow_example_fallback=True
        )
    default_spec = source_specs.get(default_source_id) if default_source_id else None

    account = saved.get("account") or runtime_cfg.account or (
        default_spec.account_id if default_spec else ""
    )
    room = saved.get("room") or runtime_cfg.default_room or (
        default_spec.default_conversation_id if default_spec else ""
    )
    configured_rooms = saved.get("approved_group_conversations")
    if configured_rooms is None:
        configured_rooms = list(runtime_cfg.approved_group_conversations)
    if (
        not isinstance(room, str)
        or not room.strip()
        or not isinstance(account, str)
        or not account.strip()
        or account.endswith("@chatroom")
    ):
        raise SystemExit("Saved conversation route or account is invalid.")
    if not isinstance(configured_rooms, list) or any(
        not isinstance(value, str) or not value.strip() for value in configured_rooms
    ):
        raise SystemExit("Saved approved group conversations are invalid.")
    approved_rooms = set(configured_rooms) | {room}
    return LiveRoute(
        route_path=route_path,
        saved=saved,
        account=account,
        room=room,
        approved_rooms=approved_rooms,
    )


def admit_group_route(
    route: LiveRoute,
    conversation_id: str,
    *,
    gateway: WeChatGateway | None = None,
) -> bool:
    """Persist a newly admitted group conversation atomically and allow it on the gateway."""
    if (
        not isinstance(conversation_id, str)
        or not conversation_id.strip()
        or len(conversation_id) > 256
        or "\0" in conversation_id
    ):
        return False
    channel = getattr(gateway, "channel", "")
    if channel == "wechat" and not is_room_id(conversation_id):
        return False
    if conversation_id not in route.approved_rooms:
        next_rooms = route.approved_rooms | {conversation_id}
        next_saved = {
            **route.saved,
            "approved_group_conversations": sorted(next_rooms - {route.room}),
        }
        route.route_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = route.route_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(next_saved, ensure_ascii=False), encoding="utf-8")
        temporary.replace(route.route_path)
        route.approved_rooms.add(conversation_id)
        route.saved.update(next_saved)
    if gateway is not None:
        gateway.allow_conversation(ConversationKey(route.account, conversation_id))
    return True


def sync_approved_rooms_from_route(
    route: LiveRoute,
    *,
    gateway: WeChatGateway,
    registered_group_ids: set[str],
) -> None:
    """Refresh approved group conversations written by an independent Ingress process."""
    if not route.route_path.exists():
        return
    try:
        latest_saved = json.loads(route.route_path.read_text("utf-8"))
    except (OSError, ValueError):
        return
    latest_groups = latest_saved.get("approved_group_conversations", [])
    if not isinstance(latest_groups, list):
        return
    for group_id in latest_groups:
        if (
            isinstance(group_id, str)
            and group_id.strip()
            and group_id not in route.approved_rooms
        ):
            route.approved_rooms.add(group_id)
            gateway.allow_conversation(ConversationKey(route.account, group_id))
            registered_group_ids.add(group_id)


def build_providers(root: Path, profile_config: AgentProfileConfig) -> ProviderBundle:
    """Build OpenAI/Gemini clients, refreshable pool probe, and budgeted model factories."""
    from google.genai import types
    from openai import AsyncOpenAI

    from .frameworks.adk_budget import BudgetedAdkModel
    from .frameworks.google_web import GoogleWebModel
    from .frameworks.adk_openai import OpenAIAdkModel
    from .mock import SystemClock
    from .storage import Repository
    from .usage import UsageLedger

    workspace = Path(root)
    catalog = load_model_catalog(workspace=workspace)
    runtime_cfg = local_config.load_local_runtime_config(workspace)
    rag_env = os.environ.get("WECHAT_AGENT_EPISODE_RAG")
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
    openai_key = os.environ.get("OPENAI_API_KEY") or load_key(target=OPENAI_TARGET)
    if needs_openai and not openai_key:
        raise SystemExit(
            "OpenAI credential missing: store --service openai or set OPENAI_API_KEY "
            "(required by OpenAI profiles, memory_model, or episode_rag_enabled=true)"
        )
    openai_client = (
        AsyncOpenAI(api_key=openai_key, timeout=60, max_retries=0) if openai_key else None
    )
    gemini_key = os.environ.get("GEMINI_API_KEY") or load_key()
    if needs_gemini and not gemini_key:
        raise SystemExit(
            "Gemini credential missing: store --service model or set GEMINI_API_KEY"
        )

    clock = SystemClock()
    repo = Repository(workspace / "data" / "pilot-usage.sqlite3")
    genai_module = _resolve("genai")
    refreshable_gemini_cls = _resolve("RefreshableGemini")

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

    def make_adk_model(
        profile: AgentProfile,
        *,
        timeout_ms: int = 60_000,
        web: bool = False,
    ):
        if catalog.provider_for(profile.model_id) == "openai":
            if openai_client is None:
                raise RuntimeError(f"OpenAI client required for model {profile.model_id!r}")
            delegate = OpenAIAdkModel(
                profile.model_id,
                openai_client,
                timeout_seconds=timeout_ms / 1000,
            )
            token_counter = None
        elif web:
            delegate = GoogleWebModel(profile.model_id, get_model_client)
            token_counter = count_model_tokens
        else:
            delegate = refreshable_gemini_cls(
                model=profile.model_id,
                client_provider=lambda t=timeout_ms: get_gemini_client(t),
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
            http_timeout_ms=timeout_ms,
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
                        input=[{"role": "user", "content": [{"type": "input_text", "text": "ping"}]}],
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
            stale_clients: list[Any] = [model_client]
            model_client = new_model_client()
            for timeout_key in list(gemini_clients):
                stale_clients.append(gemini_clients[timeout_key])
                gemini_clients[timeout_key] = new_gemini_client(timeout_key)
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
    )


def build_services(
    root: Path,
    account: str,
    room: str,
    gateway: WeChatGateway,
    profile_config: AgentProfileConfig,
    providers: ProviderBundle,
) -> ServiceBundle:
    """Wire Layer 2/4/5 data stores, media ingestors, context builder, and domain toolsets."""
    from .agent_tools import AgentToolStatus, ReadOnlyToolset
    from .context_assembler import MessageContextAssembler
    from .context_builder import ContextBuilder
    from .cross_session import CrossSessionService
    from .cross_session_tools import CrossSessionToolset
    from .episode_memory import EpisodeMemory
    from .event_book import EventBook
    from .idle_tools import IdleActivityToolset
    from .media import WeChatImageIngestor, WeChatVoiceIngestor
    from .memory_store import MemoryStore
    from .message_ingestor import MessageIngestor
    from .model_overrides import ModelOverrideStore
    from .outbound import OutboundService
    from .project_tools import ProjectTools
    from .temporal_context import DailyEnvironmentProvider
    from .time_scheduler import TimeScheduler
    from .web_tools import WebSearchUsage, WebToolset

    workspace = Path(root)
    catalog = load_model_catalog(workspace=workspace)
    runtime_cfg = local_config.load_local_runtime_config(workspace)
    default_profile = profile_config.profiles[profile_config.default_profile]
    model_overrides = ModelOverrideStore(
        workspace / "data" / "conversation-model-overrides.sqlite3"
    )
    memory_store = MemoryStore(workspace / "data" / "agent-memory.sqlite3")
    time_scheduler = TimeScheduler(
        memory_store.db,
        lock=memory_store._lock,
        idle_model=profile_config.memory_model_id,
    )
    event_book = EventBook(memory_store)

    episode_memory = None
    rag_env = os.environ.get("WECHAT_AGENT_EPISODE_RAG")
    episode_rag_enabled = (
        rag_env == "1" if rag_env is not None else runtime_cfg.server.episode_rag_enabled
    )
    if episode_rag_enabled and providers.openai_client is not None:
        curator_model = providers.make_adk_model(
            replace(default_profile, model_id=profile_config.memory_model_id),
            timeout_ms=15_000,
        )
        curator_model.output_limit = 800
        episode_memory = EpisodeMemory(
            workspace / "data" / "episode-memory.sqlite3",
            providers.openai_client,
            curator_model,
            providers.usage_ledger(),
            account_budget=profile_config.account_daily_budget_microusd,
            account_text_budget=profile_config.account_text_budget_microusd,
        )
        logger.info("event=episode_memory_enabled")

    outbound = OutboundService(
        workspace / "data" / "tool-receipts.sqlite3",
        account=account,
        default_room=room,
        gateway=gateway,
        clock=providers.clock,
        memory_store=memory_store,
    )
    wechat_tools_cls = _resolve("WeChatTools")
    tools = wechat_tools_cls(
        workspace,
        account,
        room,
        gateway,
        providers.model_client,
        providers.usage_ledger(),
        providers.clock,
        budget=profile_config.account_daily_budget_microusd,
        account_text_budget=profile_config.account_text_budget_microusd,
        account_image_read_budget=profile_config.account_image_read_budget_microusd,
        account_image_generation_budget=profile_config.account_image_generation_budget_microusd,
        memory_store=memory_store,
        event_book=event_book,
        openai_client=providers.openai_client,
        gateway=gateway,
        outbound_service=outbound,
    )

    image_ingestor = WeChatImageIngestor(workspace, gateway=gateway)
    logger.info(
        "event=native_image_download_configured enabled=%s",
        image_ingestor.native_downloader is not None,
    )
    tools.image_ingestor = image_ingestor
    voice_ingestor = WeChatVoiceIngestor(workspace, gateway=gateway)
    daily_environment = DailyEnvironmentProvider()
    contact_resolver = gateway.resolve_contact

    message_ingestor = MessageIngestor(
        memory_store,
        image_ingestor=image_ingestor,
        voice_ingestor=voice_ingestor,
        archive_envelope=gateway.archive_envelope,
    )
    context_builder = ContextBuilder(
        memory_store,
        resolve_contact=contact_resolver,
        daily_environment=daily_environment,
        time_scheduler=time_scheduler,
    )
    context_assembler = MessageContextAssembler(
        memory_store,
        resolve_contact=contact_resolver,
        image_ingestor=image_ingestor,
        voice_ingestor=voice_ingestor,
        daily_environment=daily_environment,
        max_user_bytes=profile_config.profiles[profile_config.default_profile].context_window_bytes,
        archive_envelope=gateway.archive_envelope,
    )
    context_assembler.ingestor = message_ingestor
    context_assembler.builder = context_builder

    web_usage = WebSearchUsage(providers.repo)
    cross_sessions = CrossSessionService(
        memory_store,
        resolve_contact=contact_resolver,
        dispatch_reply=None,
    )
    cross_tools = CrossSessionToolset(cross_sessions, account)
    project_tools = ProjectTools(workspace)
    idle_tools = IdleActivityToolset(time_scheduler)

    def profile_tools(profile: AgentProfile, _model: Any) -> list[Any]:
        # Public queries/URLs only; the helper never receives chat history.
        web_tools: list[Any] = []
        if "web" in profile.toolsets and providers.get_model_client() is not None:
            web_model = providers.make_adk_model(
                replace(profile, model_id=catalog.roles.web_model),
                web=True,
            )
            web_tools = WebToolset(web_model, web_usage).agent_tools()
        groups = {
            "wechat": tools.agent_tools(),
            "cross_session": cross_tools.agent_tools(),
            "project": project_tools.agent_tools(),
            "web": web_tools,
            "status": ReadOnlyToolset(
                AgentToolStatus(
                    mode="selfhosted_group",
                    automatic_send_enabled=True,
                    supported_sends=("text", "image", "quoted_reply"),
                )
            ).agent_tools(),
        }
        return [tool for group in profile.toolsets for tool in groups.get(group, [])]

    return ServiceBundle(
        memory_store=memory_store,
        time_scheduler=time_scheduler,
        event_book=event_book,
        episode_memory=episode_memory,
        outbound=tools.outbound,
        tools=tools,
        reply_tools=tools.reply_tools,
        media_tools=tools.media_tools,
        memory_tools=tools.memory_tools,
        model_overrides=model_overrides,
        image_ingestor=image_ingestor,
        voice_ingestor=voice_ingestor,
        daily_environment=daily_environment,
        message_ingestor=message_ingestor,
        context_builder=context_builder,
        context_assembler=context_assembler,
        web_usage=web_usage,
        cross_sessions=cross_sessions,
        cross_tools=cross_tools,
        project_tools=project_tools,
        idle_tools=idle_tools,
        profile_tools=profile_tools,
    )


def build_runners(
    root: Path,
    account: str,
    profile_config: AgentProfileConfig,
    providers: ProviderBundle,
    services: ServiceBundle,
) -> RunnerBundle:
    """Build main chat profile runner cache, idle runners, and owner command handler."""
    from .frameworks.adk import AdkFramework
    from .idle_tools import IDLE_SUMMARY_MAX_MODEL_CALLS, IDLE_WORK_MAX_MODEL_CALLS
    from .model_overrides import MODEL_LABELS, model_list_text, parse_model_command
    from .prompt_catalog import refresh_prompts

    workspace = Path(root)
    catalog = load_model_catalog(workspace=workspace)
    runtime_cls = _resolve("ScopedAgentRuntime")
    default_profile = profile_config.profiles[profile_config.default_profile]
    runner_cache: dict[tuple[str, str], Any] = {}

    def count_effective_messages(
        account_id: str,
        conversation_id: str,
        since: datetime,
    ) -> int:
        return services.memory_store.count_effective_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            since=since,
        )

    def runner_for(profile: AgentProfile, model_id: str | None = None):
        selected = replace(profile, model_id=model_id) if model_id else profile
        cache_key = (profile.profile_id, selected.model_id)
        cached = runner_cache.get(cache_key)
        if cached is not None:
            return cached
        profile_model = providers.make_adk_model(selected)
        profile_summary_model = providers.make_adk_model(
            replace(selected, model_id=profile_config.memory_model_id),
        )
        model_spec = catalog.get(selected.model_id)
        effective_cache_tier = selected.cache_tier or (
            model_spec.cache_tier if model_spec is not None else None
        )
        effective_cache_idle = (
            selected.cache_recycle_idle_seconds
            if selected.cache_recycle_idle_seconds is not None
            else (model_spec.cache_recycle_idle_seconds if model_spec is not None else None)
        )
        effective_cache_min_tokens = (
            selected.cache_recycle_min_tokens
            if selected.cache_recycle_min_tokens is not None
            else (model_spec.cache_recycle_min_tokens if model_spec is not None else None)
        )
        extra_tools = services.profile_tools(profile, profile_model)
        runner = runtime_cls(
            workspace / "data" / "live-sessions.sqlite3",
            model=profile_model,
            framework=AdkFramework(project_compaction=True),
            summary_model=profile_summary_model,
            summary_framework=AdkFramework(project_compaction=True),
            extra_tools=extra_tools,
            idle_session_hours=profile.idle_session_hours,
            max_history_records=profile.max_history_records,
            max_history_bytes=profile.max_history_bytes,
            token_context_limits=profile.token_context_limits,
            event_book_context=services.event_book.render,
            effective_message_count=count_effective_messages,
            last_human_message_age=lambda account_id, conversation_id: (
                services.memory_store.minutes_since_last_human_message(
                    account_id=account_id,
                    conversation_id=conversation_id,
                )
            ),
            jev_label_sink=lambda labels: services.memory_store.save_jev_labels(
                account_id=account,
                labels=labels,
            ),
            episode_memory=services.episode_memory,
            profile_id=profile.profile_id,
            persona_prompt=profile.persona_prompt,
            main_prompt=profile.main_prompt,
            summary_prompt=profile.summary_prompt,
            summary_system_prompt=profile.summary_system_prompt,
            profile_daily_budget_microusd=profile.daily_budget_microusd,
            profile_text_budget_microusd=profile.text_budget_microusd,
            profile_image_read_budget_microusd=profile.image_read_budget_microusd,
            profile_image_generation_budget_microusd=profile.image_generation_budget_microusd,
            profile_for_conversation=(
                lambda account_id, conversation_id: profile_config.for_conversation(
                    account_id,
                    conversation_id,
                ).profile_id
            ),
            cache_tier=effective_cache_tier,
            cache_recycle_idle_seconds=effective_cache_idle,
            cache_recycle_min_tokens=effective_cache_min_tokens,
            clock=providers.clock,
        )
        runner_cache[cache_key] = runner
        logger.info(
            "event=agent_profile_loaded profile=%s model=%s tools=%s",
            profile.profile_id,
            selected.model_id,
            ",".join(profile.toolsets),
        )
        return runner

    def runner_for_conversation(conversation_id: str):
        profile = profile_config.for_conversation(account, conversation_id)
        override = services.model_overrides.get(account, conversation_id)
        if override and override not in SUPPORTED_MODELS:
            logger.warning("event=legacy_model_override_ignored runtime=google_adk")
            override = None
        return runner_for(profile, override or profile.model_id)

    runner_for(default_profile)

    logger.info(
        "event=idle_runtime_configured work_model_calls=%s summary_model_calls=%s "
        "summary_ends_turn=true",
        IDLE_WORK_MAX_MODEL_CALLS,
        IDLE_SUMMARY_MAX_MODEL_CALLS,
    )
    idle_runners: dict[str, Any] = {}
    for idle_model_id in (profile_config.memory_model_id,):
        idle_model = providers.make_adk_model(
            replace(default_profile, model_id=idle_model_id)
        )
        idle_runners[idle_model_id] = runtime_cls(
            workspace / "data" / "idle-sessions.sqlite3",
            model=idle_model,
            framework=AdkFramework(project_compaction=True),
            summary_model=idle_model,
            summary_framework=AdkFramework(project_compaction=True),
            profile_id=default_profile.profile_id,
            persona_prompt=default_profile.persona_prompt,
            main_prompt="idle_agent",
            summary_prompt=default_profile.summary_prompt,
            summary_system_prompt=default_profile.summary_system_prompt,
            idle_session_hours=default_profile.idle_session_hours,
            max_history_records=default_profile.max_history_records,
            max_history_bytes=default_profile.max_history_bytes,
            token_context_limits=default_profile.token_context_limits,
            profile_daily_budget_microusd=default_profile.daily_budget_microusd,
            profile_text_budget_microusd=default_profile.text_budget_microusd,
            profile_image_read_budget_microusd=default_profile.image_read_budget_microusd,
            profile_image_generation_budget_microusd=default_profile.image_generation_budget_microusd,
            extra_tools=(
                services.profile_tools(default_profile, idle_model)
                + services.idle_tools.agent_tools()
            ),
            event_book_context=services.event_book.render,
            effective_message_count=count_effective_messages,
        )

    async def reply_with_profile(window) -> str:
        return await runner_for_conversation(window.conversation_id).reply(window)

    async def handle_special_command(item) -> str | None:
        if item.sender_id not in profile_config.command_owner_ids:
            return None
        if item.original_message_type not in (None, 1):
            return None
        text = item.text.strip()
        if text in {"刷新提示词", "重新加载提示词"}:
            ok, result = refresh_prompts()
            if ok:
                runner_cache.clear()
                return "提示词文件已重新读入内存，之后的模型请求会使用新版本。"
            return f"提示词刷新失败（{result}）；旧版本仍在使用，没有切换到不完整内容。"
        command = parse_model_command(text)
        if command is None:
            return None
        kind, model_id = command
        if kind == "list":
            return model_list_text(SUPPORTED_MODELS)
        if kind == "default":
            services.model_overrides.clear(account, item.conversation_id)
            profile = profile_config.for_conversation(account, item.conversation_id)
            return f"已恢复这个聊天的档案默认模型：{MODEL_LABELS[profile.model_id]}。"
        if model_id not in SUPPORTED_MODELS:
            return "当前运行时使用 Google ADK。" + model_list_text(SUPPORTED_MODELS)
        temporary_seconds = 3600 if model_id == "gemini-4-argon" else None
        services.model_overrides.set(
            account,
            item.conversation_id,
            model_id,
            temporary_seconds=temporary_seconds,
        )
        suffix = (
            "，一小时后自动恢复档案默认模型"
            if temporary_seconds
            else "，持续使用到你再次切换"
        )
        return f"这个聊天已切换为{MODEL_LABELS[model_id]}{suffix}。"

    return RunnerBundle(
        runner_cache=runner_cache,
        runner_for=runner_for,
        runner_for_conversation=runner_for_conversation,
        idle_runners=idle_runners,
        reply_with_profile=reply_with_profile,
        handle_special_command=handle_special_command,
    )


def build_ingress_app(
    root: Path | None = None,
    *,
    heartbeat_max_age_seconds: float = 10.0,
) -> tuple[Ingress, FastAPI]:
    """Build thin standalone Ingress and FastAPI app without importing any LLM SDK."""
    workspace = Path(root) if root is not None else ROOT
    route = load_live_route(workspace)

    def persist_ingress_group_route(conversation_id: str) -> bool:
        return admit_group_route(route, conversation_id)

    registry = SourceRegistry.from_workspace(
        workspace,
        account_id=route.account,
        allowed_conversations=frozenset(
            ConversationKey(route.account, group) for group in route.approved_rooms
        ),
        send_enabled=False,
        active_account_provider=lambda: route.account,
    )
    gateway = registry.as_gateway()

    db_path = workspace / "data" / "pilot-receipts.sqlite3"
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(db_path, check_same_thread=False)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("PRAGMA busy_timeout=5000")

    ingress = Ingress(
        db,
        route.account,
        route.room,
        mode="selfhosted_group",
        limit=None,
        registered_group_ids=route.approved_rooms - {route.room},
        admit_group=persist_ingress_group_route,
        parse_events=gateway.parse_events,
        normalize_event=lambda item: connector_normalize_event(
            registry.for_event(item).connector, item
        ),
        registry=registry,
        heartbeat_max_age_seconds=heartbeat_max_age_seconds,
        recover_running=False,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        try:
            yield
        finally:
            db.close()
            registry.close()

    app = create_ingress_app(ingress, lifespan=lifespan)
    return ingress, app


def run_ingress(
    root: Path | None = None,
    *,
    host: str | None = None,
    port: int | None = None,
    heartbeat_max_age_seconds: float = 10.0,
) -> None:
    """Zero-LLM standalone HTTP Ingress entrypoint."""
    workspace = Path(root) if root is not None else ROOT
    srv = local_config.load_local_runtime_config(workspace).server
    resolved_host = host or os.environ.get("WECHAT_AGENT_HOST") or srv.ingress_host
    resolved_port = (
        port
        if port is not None
        else int(os.environ.get("WECHAT_AGENT_PORT") or srv.ingress_port)
    )
    ingress, app = build_ingress_app(
        workspace,
        heartbeat_max_age_seconds=heartbeat_max_age_seconds,
    )
    ingress_logger.info(
        "event=ingress_start_requested mode=%s role=ingress account_sha=%s "
        "conversation_sha=%s host=%s port=%s",
        ingress.mode,
        sha(ingress.account),
        sha(ingress.room),
        resolved_host,
        resolved_port,
    )
    uvicorn.run(app, host=resolved_host, port=resolved_port, access_log=True, log_level="info")


def build_runtime_bundle(
    root: Path | None = None,
    *,
    role: Literal["all", "ingress", "worker"] = "all",
    heartbeat_max_age_seconds: float = 10.0,
) -> RuntimeBundle:
    """Assemble the complete runtime bundle for ``all``, ``worker``, or ``ingress``."""
    if role not in ("all", "ingress", "worker"):
        raise ValueError(f"Unsupported runtime role: {role!r}")

    workspace = Path(root) if root is not None else ROOT
    route = load_live_route(workspace)
    if role == "ingress":
        ingress, app = build_ingress_app(
            workspace,
            heartbeat_max_age_seconds=heartbeat_max_age_seconds,
        )
        return RuntimeBundle(
            role="ingress",
            root=workspace,
            route=route,
            ingress=ingress,
            app=app,
            ingress_db=ingress.db,
        )

    from .agent_profiles import load_agent_profiles
    from .context_builder import ContextSpec
    from .prompt_catalog import preload_prompts
    from .tool_timing import turn_deadline_monotonic
    from .triggers import IdleEventTrigger
    from .turn_scope import tool_turn_scope

    account = route.account
    room = route.room
    registry = SourceRegistry.from_workspace(
        workspace,
        account_id=account,
        allowed_conversations=frozenset(
            ConversationKey(account, group) for group in route.approved_rooms
        ),
        send_enabled=True,
        active_account_provider=lambda: account,
    )
    gateway = registry.as_gateway()
    factory = registry.default().factory
    try:
        status = gateway.account_state(account, room, timeout_seconds=5.0)
    except (httpx.HTTPError, ValueError, OSError) as exc:
        status = AccountState(False, False)
        logger.warning(
            "event=hook_preflight_unavailable error_type=%s "
            "pilot_starts_unready=true inbound_and_send_disabled=true",
            type(exc).__name__,
        )
    is_logged_in = status.logged_in
    logger.info(
        "event=wechat_gateway_configured factory=%s capabilities=%s",
        factory,
        ",".join(sorted(gateway.capabilities)),
    )
    if is_logged_in and not status.account_matches:
        raise SystemExit("Logged-in Hook profile does not match the saved account.")
    logger.info(
        "event=pilot_route_preflight passed=true is_login=%s account_sha=%s conversation_sha=%s",
        is_logged_in,
        sha(account),
        sha(room),
    )
    route.route_path.parent.mkdir(exist_ok=True)
    if is_logged_in:
        route.saved.update({"account": account, "room": room})
        route.route_path.write_text(
            json.dumps(route.saved, ensure_ascii=False),
            encoding="utf-8",
        )

    profile_config = load_agent_profiles(workspace / "config" / "agent_profiles.toml")
    preload_prompts()
    providers = build_providers(workspace, profile_config)
    services = build_services(workspace, account, room, gateway, profile_config, providers)
    runners = build_runners(workspace, account, profile_config, providers, services)

    def persist_new_group_route(conversation_id: str) -> bool:
        return admit_group_route(route, conversation_id, gateway=gateway)

    async def login_check() -> bool:
        try:
            state = await asyncio.to_thread(gateway.account_state, account, room)
        except (httpx.HTTPError, ValueError):
            return False
        if not state.logged_in:
            return False
        if not state.account_matches:
            logger.error(
                "event=wechat_login_profile_mismatch expected_sha=%s account_matches=%s",
                sha(account),
                state.account_matches,
            )
            return False
        return True

    async def initialize_after_login() -> None:
        state = await asyncio.to_thread(gateway.account_state, account, room)
        if not state.logged_in or not state.account_matches:
            raise RuntimeError("Hook login/account self-check failed")
        if services.time_scheduler.account_activity_state(account) is None:
            archived = services.memory_store.list_conversations(account_id=account, limit=200)
            if archived:
                latest = max(archived, key=lambda row: str(row["last_at"]))
                services.time_scheduler.record_inbound(
                    account,
                    str(latest["conversation_id"]),
                    at=datetime.fromisoformat(str(latest["last_at"])),
                )
        await runners.runner_for_conversation(room).register_conversation(account, room)
        native_ready = await asyncio.to_thread(
            connector_ensure_ready,
            gateway,
            account,
        )
        failed_recovery = pilot.recover_failed_after_login()
        if role == "worker":
            # In decoupled Scheme A mode, standalone Ingress (:8080) remains online
            # across worker restarts, so redundant archived/latest-event recovery is skipped.
            backlog = {"queued": 0}
            hook_recovery = "skipped_scheme_a_worker"
        else:
            backlog = pilot.recover_archived_backlog(max_age_hours=24)
            hook_recovery = pilot.recover_latest_event(state.latest_event)
        logger.info(
            "event=wechat_login_self_check_passed role=%s account_sha=%s conversation_sha=%s "
            "pending_inbox=%s legacy_backlog_queued=%s failed_batch_recovery=%s "
            "hook_latest_recovery=%s native_image_download_ready=%s",
            role,
            sha(account),
            sha(room),
            pilot.scheduler.pending_count(),
            backlog["queued"],
            failed_recovery["queued"],
            hook_recovery,
            native_ready,
        )

    async def maintain_idle_sessions() -> int:
        await runners.runner_for_conversation(room).register_conversation(account, room)
        if pilot.has_pending_work:
            logger.info("event=session_maintenance_deferred reason=message_work_pending")
            return 0
        try:
            current_state = await asyncio.to_thread(gateway.account_state, account, room)
        except Exception as exc:  # noqa: BLE001
            logger.info(
                "event=session_maintenance_deferred reason=hook_status_unavailable error_type=%s",
                type(exc).__name__,
            )
            return 0
        if not current_state.logged_in or not current_state.account_matches:
            logger.info("event=session_maintenance_deferred reason=wechat_not_logged_in")
            return 0
        if pilot.has_pending_work:
            logger.info("event=session_maintenance_deferred reason=message_work_pending")
            return 0
        summarized = 0
        for profile in profile_config.profiles.values():
            summarized += await runners.runner_for(profile).summarize_idle_sessions()
        return summarized

    async def record_session_activity(account_id: str, conversation_id: str) -> None:
        if role == "worker":
            if is_group_conversation(conversation_id) and conversation_id not in route.approved_rooms:
                sync_approved_rooms_from_route(
                    route,
                    gateway=gateway,
                    registered_group_ids=pilot.registered_group_ids,
                )
            now_utc = datetime.now(UTC)
            services.time_scheduler.record_inbound(account_id, conversation_id, at=now_utc)
            pilot._cancel_idle_task("new_inbound")
            pilot._cancel_maintenance_task()
            services.time_scheduler.schedule(
                account_id=account_id,
                conversation_id=None,
                kind="idle_event",
                due_at=now_utc
                + timedelta(seconds=IdleEventTrigger.next_idle_delay_seconds(0)),
                replace_existing=True,
            )
        # The durable inbound path already schedules maintenance from Hook arrival time.
        # This updates the runner route when queued input actually starts processing.
        await runners.runner_for_conversation(conversation_id).record_message_activity(
            account_id,
            conversation_id,
        )

    default_profile = profile_config.profiles[profile_config.default_profile]
    pilot = Pilot(
        workspace / "data" / "pilot-receipts.sqlite3",
        account,
        room,
        runners.reply_with_profile,
        gateway.send_text,
        limit=None,
        mode="selfhosted_group",
        resolve_contact=gateway.resolve_contact,
        memory_store=services.memory_store,
        image_ingestor=services.image_ingestor,
        voice_ingestor=services.voice_ingestor,
        daily_environment=services.daily_environment,
        record_activity=record_session_activity,
        delivery_state=services.tools.delivery_state,
        record_send_result=services.tools.record_send_result,
        pending_delivery_notices=services.tools.pending_delivery_failure_notices,
        acknowledge_delivery_notices=services.tools.acknowledge_delivery_failure_notices,
        record_delivery_notice=services.tools.queue_delivery_failure_notice,
        outbound=services.outbound,
        session_maintenance=maintain_idle_sessions,
        context_assembler=services.context_assembler,
        message_ingestor=services.message_ingestor,
        context_builder=services.context_builder,
        max_user_bytes=default_profile.context_window_bytes,
        max_user_bytes_for_conversation=lambda acct, conv: (
            profile_config.for_conversation(acct, conv).context_window_bytes
        ),
        login_check=login_check,
        initialize_after_login=initialize_after_login,
        registered_group_ids=route.approved_rooms - {room},
        admit_group=persist_new_group_route,
        special_command_handler=runners.handle_special_command,
        parse_events=gateway.parse_events,
        normalize_event=lambda item: connector_normalize_event(
            registry.for_event(item).connector, item
        ),
        registry=registry,
        archive_envelope=gateway.archive_envelope,
        provider_check=providers.provider_check,
    )

    async def run_idle_event(event_id: str, event_number: int, model_id: str) -> None:
        built = await services.context_builder.build(
            ContextSpec.for_idle(account_id=account, event_id=event_id),
        )
        window = built.context
        started_at = datetime.now(UTC)
        model = services.idle_tools.bind(account, event_id, event_number, model_id, started_at)
        scope = ExitStack()
        scope.enter_context(tool_turn_scope(window, f"idle:{event_id}"))
        deadline_token = turn_deadline_monotonic.set(time.monotonic() + 30 * 60)
        selected_runner = runners.idle_runners[model_id]
        try:
            await services.idle_tools.run_with_summary(selected_runner, window)
        finally:
            try:
                services.idle_tools.finish(account, event_id)
            except Exception:
                logger.warning("event=idle_tool_history_finalize_failed", exc_info=True)
            turn_deadline_monotonic.reset(deadline_token)
            scope.close()
            services.idle_tools.unbind(model)

    services.cross_sessions.dispatch_reply = pilot.dispatch_to_session
    pilot.configure_idle_events(services.time_scheduler, run_idle_event)
    services.tools.can_send = lambda: not pilot.paused and pilot.wechat_ready
    close_episode_memory = (
        services.episode_memory.close if services.episode_memory is not None else None
    )

    return RuntimeBundle(
        role=role,
        root=workspace,
        route=route,
        gateway=gateway,
        is_logged_in=is_logged_in,
        profile_config=profile_config,
        providers=providers,
        services=services,
        runners=runners,
        pilot=pilot,
        run_idle_event=run_idle_event,
        close_episode_memory=close_episode_memory,
    )


async def _scheme_a_ipc_sync_loop(pilot: Pilot) -> None:
    """Propagate cross-process pause and inbound signals from Ingress SQLite writes."""
    was_paused = pilot.paused
    while True:
        try:
            paused_now = pilot.paused
            if paused_now and not was_paused:
                pilot.triggers.on_paused()
            was_paused = paused_now
            if pilot.scheduler.active_count() > 0:
                pilot._cancel_idle_task("new_inbound")
                pilot._cancel_maintenance_task()
                for key in tuple(pilot._scheduled_followups):
                    if pilot.scheduler.has_pending_conversation(*key):
                        pilot.triggers.followups.notify_inbound(*key)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("event=scheme_a_ipc_sync_error", exc_info=True)
        await asyncio.sleep(0.5)


async def _run_worker_async(
    pilot: Pilot,
    *,
    close_gateway=None,
    close_episode_memory=None,
) -> None:
    """Run all Pilot background loops in decoupled Scheme A worker mode without HTTP :8080."""
    pilot.recover_scheduled_followups()
    worker_task = asyncio.create_task(pilot.worker())
    login_watch = asyncio.create_task(pilot.login_watch_worker())
    pilot._worker_task = worker_task
    pilot._login_watch_task = login_watch
    maintenance = (
        asyncio.create_task(pilot.session_maintenance_worker())
        if pilot.session_maintenance is not None
        else None
    )
    idle_worker = (
        asyncio.create_task(pilot.idle_event_worker())
        if pilot.time_scheduler is not None
        else None
    )
    ipc_sync = asyncio.create_task(_scheme_a_ipc_sync_loop(pilot))
    tasks = [
        task
        for task in (worker_task, login_watch, maintenance, idle_worker, ipc_sync)
        if task is not None
    ]
    try:
        await asyncio.gather(*tasks)
    finally:
        pilot._worker_task = None
        pilot._login_watch_task = None
        for bg in (ipc_sync, login_watch, maintenance):
            if bg is not None:
                bg.cancel()
                await asyncio.gather(bg, return_exceptions=True)
        if idle_worker is not None:
            pilot._cancel_idle_task("worker_shutdown")
            idle_worker.cancel()
            await asyncio.gather(idle_worker, return_exceptions=True)
        followups = list(pilot._scheduled_followups.values())
        pilot._cancel_all_followups()
        if followups:
            await asyncio.gather(*followups, return_exceptions=True)
        worker_task.cancel()
        await asyncio.gather(worker_task, return_exceptions=True)
        if close_episode_memory is not None:
            await close_episode_memory()
        if close_gateway is not None:
            await asyncio.to_thread(close_gateway)


def run_worker_loop(
    pilot: Pilot,
    *,
    close_gateway=None,
    close_episode_memory=None,
) -> None:
    """Blocking entrypoint for ``--role worker``."""
    try:
        asyncio.run(
            _run_worker_async(
                pilot,
                close_gateway=close_gateway,
                close_episode_memory=close_episode_memory,
            )
        )
    except KeyboardInterrupt:
        logger.info("event=worker_shutdown_requested reason=keyboard_interrupt")


def run_pilot(
    root: Path | None = None,
    *,
    role: Literal["all", "ingress", "worker"] = "all",
    host: str | None = None,
    port: int | None = None,
    overrides: Mapping[str, object] | None = None,
) -> None:
    """Build and start the Pilot, Worker, or standalone Ingress process for ``role``."""
    if role not in ("all", "ingress", "worker"):
        raise ValueError(f"Unsupported runtime role: {role!r}")

    workspace = Path(root) if root is not None else ROOT
    srv = local_config.load_local_runtime_config(workspace).server
    resolved_host = host or os.environ.get("WECHAT_AGENT_HOST") or srv.ingress_host
    resolved_port = (
        port
        if port is not None
        else int(os.environ.get("WECHAT_AGENT_PORT") or srv.ingress_port)
    )
    with _temporary_overrides(overrides):
        if role == "ingress":
            run_ingress(workspace, host=resolved_host, port=resolved_port)
            return

        bundle = build_runtime_bundle(workspace, role=role)
        assert bundle.pilot is not None
        assert bundle.gateway is not None
        assert bundle.profile_config is not None
        default_profile = bundle.profile_config.profiles[bundle.profile_config.default_profile]
        logger.info(
            "event=pilot_start_requested mode=selfhosted_group role=%s model=%s send_enabled=true",
            role,
            default_profile.model_id,
        )
        if role == "worker":
            run_worker_loop(
                bundle.pilot,
                close_gateway=bundle.gateway.close,
                close_episode_memory=bundle.close_episode_memory,
            )
            return
        uvicorn.run(
            create_pilot_app(
                bundle.pilot,
                close_gateway=bundle.gateway.close,
                close_episode_memory=bundle.close_episode_memory,
            ),
            host=resolved_host,
            port=resolved_port,
            access_log=True,
            log_level="info",
        )
