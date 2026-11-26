"""Multi-source connector registry and unified outbound/media routing proxy."""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass, replace
from pathlib import Path

from .. import local_config
from ..contacts import ContactIdentity
from ..models import ConversationKey, ReplyDraft
from ..turn_scope import active_conversation, memory_context
from .base import (
    AccountState,
    GatewayConfig,
    MessageEvent,
    SendResult,
    VoicePayload,
    WeChatGateway,
    connector_ensure_ready,
    connector_fetch_received_file,
    connector_in_scope,
    connector_normalize_event,
    connector_send_file,
    connector_verify_request,
    create_gateway,
    default_gateway_factory,
)

logger = logging.getLogger(__name__)


@dataclass
class RegisteredSource:
    source_id: str
    channel: str
    factory: str
    connector: WeChatGateway
    account_id: str = ""
    default_conversation_id: str = ""


class RouterGateway:
    """Composite WeChatGateway proxy routing I/O across one or more registered sources."""

    def __init__(self, registry: SourceRegistry) -> None:
        self.registry = registry

    @property
    def capabilities(self) -> frozenset[str]:
        caps: set[str] = set()
        for src in self.registry.sources.values():
            caps.update(src.connector.capabilities)
        return frozenset(caps)

    def _route(self, account_id: str = "", conversation_id: str = "", source_id: str = "") -> WeChatGateway:
        return self.registry.resolve(
            account_id=account_id,
            conversation_id=conversation_id,
            source_id=source_id,
        ).connector

    def parse_events(self, payload: object, *, source_id: str = "") -> tuple[MessageEvent, ...]:
        src = self.registry.get(source_id) if source_id else self.registry.default()
        if src is None:
            raise ValueError(f"Unknown message source: {source_id!r}")
        events = src.connector.parse_events(payload)
        stamped: list[MessageEvent] = []
        for item in events:
            updated = item
            if updated.source_id in ("", "default"):
                updated = replace(updated, source_id=src.source_id, channel=src.channel)
            self.registry.remember_conversation(
                updated.account_id, updated.conversation_id, updated.source_id
            )
            stamped.append(updated)
        return tuple(stamped)

    def normalize_event(self, item: MessageEvent) -> MessageEvent | None:
        src = self.registry.for_event(item)
        return connector_normalize_event(src.connector, item)

    def in_scope(
        self,
        item: MessageEvent,
        *,
        account: str,
        default_room: str = "",
        registered_group_ids: set[str] | frozenset[str] = frozenset(),
    ) -> bool:
        src = self.registry.for_event(item)
        return connector_in_scope(
            src.connector,
            item,
            account=account,
            default_room=default_room,
            registered_group_ids=registered_group_ids,
        )

    def archive_envelope(self, item: MessageEvent, *, preserve_raw: bool = False) -> dict:
        return self._route(item.account_id, item.conversation_id, item.source_id).archive_envelope(
            item, preserve_raw=preserve_raw
        )

    def account_state(
        self,
        account_id: str,
        conversation_id: str = "",
        *,
        timeout_seconds: float = 2.0,
    ) -> AccountState:
        return self.registry.default().connector.account_state(
            account_id, conversation_id, timeout_seconds=timeout_seconds
        )

    def ensure_ready(self, account_id: str) -> bool:
        ready = False
        for src in self.registry.sources.values():
            if connector_ensure_ready(src.connector, account_id):
                ready = True
        return ready

    def resolve_contact(self, user_id: str) -> ContactIdentity | None:
        for src in self.registry.ordered_sources():
            identity = src.connector.resolve_contact(user_id)
            if identity is not None:
                return identity
        return None

    def allow_conversation(self, key: ConversationKey) -> None:
        for src in self.registry.sources.values():
            src.connector.allow_conversation(key)

    def send_text(self, draft: ReplyDraft) -> SendResult:
        connector = self._route(draft.key.account_id, draft.key.conversation_id)
        return connector.send_text(draft)

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        active = active_conversation()
        acct = active[0] if active is not None else ""
        return self._route(acct, conversation_id).send_image(conversation_id, path)

    def send_file(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        ctx = memory_context.get()
        acct = str(ctx.get("account_id") or "") if isinstance(ctx, dict) else ""
        return connector_send_file(self._route(acct, conversation_id), conversation_id, path)

    def send_quote(
        self,
        conversation_id: str,
        text: str,
        quote: tuple[str, str, str, str],
    ) -> tuple[str, str | None]:
        active = active_conversation()
        acct = active[0] if active is not None else ""
        return self._route(acct, conversation_id).send_quote(conversation_id, text, quote)

    def decode_image(self, source: Path, destination: Path) -> None:
        self.registry.default().connector.decode_image(source, destination)

    def fetch_original_image(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]:
        return self._route(
            item.account_id, item.conversation_id, item.source_id
        ).fetch_original_image(
            item, references, destination, allow_account_scan=allow_account_scan
        )

    def fetch_thumbnail(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, str]:
        return self._route(
            item.account_id, item.conversation_id, item.source_id
        ).fetch_thumbnail(item, references, destination)

    def fetch_received_file(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, int]:
        return connector_fetch_received_file(
            self._route(item.account_id, item.conversation_id, item.source_id),
            item,
            references,
            destination,
        )

    def fetch_account_cached_original(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ):
        connector = self._route(item.account_id, item.conversation_id, item.source_id)
        fn = getattr(connector, "fetch_account_cached_original", None)
        if callable(fn):
            return fn(item, references, destination)
        return None

    def voice_payload(self, item: MessageEvent) -> VoicePayload:
        return self._route(item.account_id, item.conversation_id, item.source_id).voice_payload(item)

    def close(self) -> None:
        self.registry.close()


class SourceRegistry:
    """Registry managing one or multiple active channel connectors."""

    def __init__(
        self,
        sources: Iterable[RegisteredSource] = (),
        *,
        default_source_id: str = "",
    ) -> None:
        self.sources: dict[str, RegisteredSource] = {}
        for src in sources:
            self.sources[src.source_id] = src
        if not self.sources:
            raise ValueError("SourceRegistry requires at least one registered source")
        self.default_source_id = (
            default_source_id if default_source_id in self.sources else next(iter(self.sources))
        )
        self._conversation_sources: dict[tuple[str, str], str] = {}
        self._conversation_only_sources: dict[str, str] = {}

    def get(self, source_id: str | None) -> RegisteredSource | None:
        if not source_id:
            return None
        return self.sources.get(source_id)

    def default(self) -> RegisteredSource:
        return self.sources[self.default_source_id]

    def ordered_sources(self) -> list[RegisteredSource]:
        default_src = self.default()
        return [default_src] + [
            src for sid, src in self.sources.items() if sid != default_src.source_id
        ]

    def remember_conversation(self, account_id: str, conversation_id: str, source_id: str) -> None:
        if source_id not in self.sources or not conversation_id:
            return
        if account_id:
            self._conversation_sources[(account_id, conversation_id)] = source_id
        self._conversation_only_sources[conversation_id] = source_id

    def for_event(self, item: MessageEvent) -> RegisteredSource:
        if item.source_id in self.sources:
            return self.sources[item.source_id]
        for src in self.sources.values():
            if src.channel == item.channel:
                return src
        return self.resolve(account_id=item.account_id, conversation_id=item.conversation_id)

    def resolve(
        self,
        *,
        account_id: str = "",
        conversation_id: str = "",
        source_id: str = "",
    ) -> RegisteredSource:
        if source_id and source_id in self.sources:
            return self.sources[source_id]
        if account_id and conversation_id:
            hit = self._conversation_sources.get((account_id, conversation_id))
            if hit and hit in self.sources:
                return self.sources[hit]
        if conversation_id:
            hit = self._conversation_only_sources.get(conversation_id)
            if hit and hit in self.sources:
                return self.sources[hit]
            for src in self.sources.values():
                if src.channel == "wechat" and (
                    conversation_id.endswith("@chatroom") or conversation_id.startswith(("wxid_", "gh_"))
                ):
                    return src
        return self.default()

    def verify_request(self, source_id: str, headers, raw_body: bytes) -> bool:
        src = self.get(source_id)
        if src is None:
            return False
        return connector_verify_request(src.connector, headers, raw_body)

    def as_gateway(self) -> WeChatGateway:
        if len(self.sources) == 1:
            return self.default().connector
        return RouterGateway(self)

    def close(self) -> None:
        for src in self.sources.values():
            src.connector.close()

    @classmethod
    def from_workspace(
        cls,
        workspace: Path,
        *,
        account_id: str = "",
        allowed_conversations: frozenset[ConversationKey] = frozenset(),
        send_enabled: bool = True,
    ) -> SourceRegistry:
        """Build a SourceRegistry from env overrides, `.local/sources.toml`, or single default."""
        env_factory = (
            os.environ.get("ECHOLOOM_GATEWAY_FACTORY", "").strip()
            or os.environ.get("WECHAT_GATEWAY_FACTORY", "").strip()
        )
        default_source_id, specs = local_config.load_source_configs(workspace)
        if not env_factory and not specs:
            fallback_factory = default_gateway_factory(workspace)
            if "webhook" in fallback_factory.casefold():
                default_source_id, specs = local_config.load_source_configs(
                    workspace, allow_example_fallback=True
                )
        if env_factory or not specs:
            factory = env_factory or default_gateway_factory(workspace)
            channel = "webhook" if "webhook" in factory.casefold() else (
                "recording" if "recording" in factory.casefold() else "wechat"
            )
            hook_cfg = local_config.load_local_runtime_config(workspace).wechat_hook
            base_url = os.environ.get("WECHAT_HOOK_URL") or hook_cfg.hook_url
            native_url = os.environ.get("WECHAT_NATIVE_IMAGE_DOWNLOAD_URL")
            if native_url is None and channel == "wechat":
                native_url = hook_cfg.native_download_url
            cfg = GatewayConfig(
                workspace=workspace,
                account_id=account_id,
                allowed_conversations=allowed_conversations,
                send_enabled=send_enabled,
                base_url=base_url,
                native_download_url=native_url or None,
                source_id=channel,
                channel=channel,
                auth_token=os.environ.get("ECHOLOOM_WEBHOOK_SECRET", "").strip(),
                outbound_url=os.environ.get("ECHOLOOM_WEBHOOK_OUTBOUND_URL", "").strip(),
            )
            connector = create_gateway(cfg, factory=factory)
            return cls(
                [RegisteredSource(channel, channel, factory, connector, account_id=account_id)],
                default_source_id=channel,
            )

        hook_cfg = local_config.load_local_runtime_config(workspace).wechat_hook
        registered: list[RegisteredSource] = []
        for sid, spec in specs.items():
            eff_account = spec.account_id or account_id
            eff_allowed = set(allowed_conversations)
            for conv in spec.allowed_conversations:
                eff_allowed.add(ConversationKey(eff_account, conv))
            base_url = spec.base_url or (
                os.environ.get("WECHAT_HOOK_URL") or hook_cfg.hook_url
                if spec.channel == "wechat"
                else "http://127.0.0.1:30001"
            )
            native_url = spec.native_download_url or (
                os.environ.get("WECHAT_NATIVE_IMAGE_DOWNLOAD_URL", hook_cfg.native_download_url)
                if spec.channel == "wechat"
                else ""
            )
            cfg = GatewayConfig(
                workspace=workspace,
                account_id=eff_account,
                allowed_conversations=frozenset(eff_allowed),
                send_enabled=bool(send_enabled and spec.send_enabled),
                base_url=base_url,
                native_download_url=native_url or None,
                source_id=sid,
                channel=spec.channel,
                outbound_url=spec.outbound_url,
                auth_token=spec.resolved_auth_token(),
                hmac_secret=spec.resolved_hmac_secret(),
                default_conversation_id=spec.default_conversation_id,
                options=spec.extra,
            )
            connector = create_gateway(cfg, factory=spec.factory)
            registered.append(
                RegisteredSource(
                    source_id=sid,
                    channel=spec.channel,
                    factory=spec.factory,
                    connector=connector,
                    account_id=eff_account,
                    default_conversation_id=spec.default_conversation_id,
                )
            )
        return cls(registered, default_source_id=default_source_id)
