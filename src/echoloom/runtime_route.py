"""Runtime bundle dataclasses and route persistence helpers extracted from bootstrap."""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from . import local_config
from .channel_gateway import ChannelConnector
from .contacts import is_room_id
from .models import ConversationKey

if TYPE_CHECKING:
    from fastapi import FastAPI

    from .agent_profiles import AgentProfile, AgentProfiles
    from .channel_tools import ChannelToolkit
    from .context_assembler import MessageContextAssembler
    from .context_builder import ContextBuilder
    from .cross_session import CrossSessionService
    from .cross_session_tools import CrossSessionToolset
    from .episode_memory import EpisodeMemory
    from .event_book import EventBook
    from .idle_tools import IdleActivityToolset
    from .ingress import Ingress
    from .media import WeChatImageIngestor, WeChatVoiceIngestor
    from .memory_store import MemoryStore
    from .message_ingestor import MessageIngestor
    from .mock import SystemClock
    from .model_overrides import ModelOverrideStore
    from .outbound import OutboundService
    from .pilot import Pilot
    from .project_tools import ProjectTools
    from .storage import Repository
    from .temporal_context import DailyEnvironmentProvider
    from .time_scheduler import TimeScheduler
    from .toolsets import MediaToolset, MemoryToolset, ReplyToolset
    from .web_tools import WebSearchUsage


@dataclass
class LiveRoute:
    """Validated account/group routing state loaded from config or `.local/route.json`."""

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
    tools: ChannelToolkit
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
    gateway: ChannelConnector | None = None
    is_logged_in: bool = False
    ingress: Ingress | None = None
    app: FastAPI | None = None
    ingress_db: sqlite3.Connection | None = None
    profile_config: AgentProfiles | None = None
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
    gateway: ChannelConnector | None = None,
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
    gateway: ChannelConnector,
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
