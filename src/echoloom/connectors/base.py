"""Channel-agnostic message source and connector boundary.

Keeps core scheduling, storage, context assembly, and ADK runtime completely
independent of any desktop Hook, transport protocol, or platform-specific SDK.
"""

from __future__ import annotations

import hmac
import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, field
from importlib import import_module
from pathlib import Path
from typing import Literal, Protocol, runtime_checkable

from ..models import ConversationKey, ReplyDraft

SenderOrigin = str


@dataclass(frozen=True, repr=False)
class MessageEvent:
    account_id: str = field(repr=False)
    conversation_id: str = field(repr=False)
    sender_id: str = field(repr=False)
    sender_origin: SenderOrigin
    text: str = field(repr=False)
    is_group: bool
    message_type: int
    source_msg_id: int | None
    capture_tick_ms: int
    display_name: None = None
    raw_envelope_json: str = field(default="", repr=False)
    raw_message_json: str = field(default="", repr=False)
    original_message_type: int | None = None
    event_time_context: Mapping[str, object] | None = field(default=None, repr=False)
    source_id: str = "default"
    channel: str = "wechat"
    content_kind: str = "text"

    def __repr__(self) -> str:
        return (
            f"MessageEvent(channel={self.channel!r}, source_id={self.source_id!r}, "
            f"is_group={self.is_group}, message_type={self.message_type}, "
            f"sender_origin={self.sender_origin!r})"
        )


@dataclass(frozen=True)
class SendResult:
    status: Literal["rejected", "queued", "unknown"]
    reason: str | None = None

    @property
    def ledger_outcome(self) -> Literal["failed", "unknown"]:
        return "failed" if self.status == "rejected" else "unknown"


@dataclass(frozen=True)
class AccountState:
    logged_in: bool
    account_matches: bool
    storage_root: Path | None = None
    latest_event: MessageEvent | None = None


@dataclass(frozen=True)
class VoicePayload:
    data: bytes = field(default=b"", repr=False)
    duration_ms: int | None = None
    error_code: str | None = None
    format: str = "silk"


@dataclass(frozen=True)
class ConnectorConfig:
    workspace: Path
    account_id: str = ""
    allowed_conversations: frozenset[ConversationKey] = frozenset()
    send_enabled: bool = False
    source_root: Path | None = None
    native_download_url: str | None = None
    base_url: str = "http://127.0.0.1:30001"
    contact_timeout_seconds: float = 0.75
    transport: object = field(default=None, repr=False, compare=False)
    text_sender: object = field(default=NotImplemented, repr=False, compare=False)
    send_cooldown_range: tuple[float, float] = (0.55, 1.35)
    source_id: str = "default"
    channel: str = "wechat"
    outbound_url: str = ""
    auth_token: str = ""
    hmac_secret: str = ""
    default_conversation_id: str = ""
    options: Mapping[str, object] = field(default_factory=dict, repr=False, compare=False)


GatewayConfig = ConnectorConfig


@runtime_checkable
class ChannelConnector(Protocol):
    """Synchronous bounded connector boundary for inbound sources and outbound replies."""

    capabilities: frozenset[str]

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]: ...
    def archive_envelope(self, item: MessageEvent, *, preserve_raw: bool = False) -> dict: ...
    def account_state(
        self,
        account_id: str,
        conversation_id: str = "",
        *,
        timeout_seconds: float = 2.0,
    ) -> AccountState: ...
    def resolve_contact(self, user_id: str): ...
    def allow_conversation(self, key: ConversationKey) -> None: ...
    def send_text(self, draft: ReplyDraft) -> SendResult: ...
    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]: ...
    def send_quote(
        self,
        conversation_id: str,
        text: str,
        quote: tuple[str, str, str, str],
    ) -> tuple[str, str | None]: ...
    def decode_image(self, source: Path, destination: Path) -> None: ...
    def fetch_original_image(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]: ...
    def fetch_thumbnail(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, str]: ...
    def voice_payload(self, item: MessageEvent) -> VoicePayload: ...
    def close(self) -> None: ...


WeChatGateway = ChannelConnector


def default_gateway_factory(workspace: Path | None = None) -> str:
    """Resolve active default connector: env -> `.local/sources.toml` -> legacy WeChat -> Webhook."""
    import json
    import os

    from .. import local_config

    for env_key in ("ECHOLOOM_GATEWAY_FACTORY", "WECHAT_GATEWAY_FACTORY"):
        val = os.environ.get(env_key, "").strip()
        if val:
            return val
    default_id, specs = local_config.load_source_configs(workspace)
    if default_id and default_id in specs:
        return specs[default_id].factory
    if workspace is not None or local_config.local_overrides_enabled():
        route_path = local_config.resolve_route_path(workspace)
        if route_path.is_file():
            try:
                raw = json.loads(route_path.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                raw = {}
            acct = str(raw.get("account") or "")
            room = str(raw.get("room") or "")
            if acct.startswith("wxid_") or room.endswith("@chatroom"):
                return "echoloom.connectors.wechat_hook:WeChatHookConnector"
    return "echoloom.connectors.webhook:WebhookConnector"


def create_gateway(config: GatewayConfig, *, factory: str | None = None) -> WeChatGateway:
    """Load an operator-configured connector factory; never take factory names from messages."""
    if factory is not None:
        selected = factory
    else:
        import os

        selected = (
            os.environ.get("ECHOLOOM_GATEWAY_FACTORY", "").strip()
            or os.environ.get("WECHAT_GATEWAY_FACTORY", "").strip()
        )
        if not selected:
            if config.channel == "wechat" and not config.outbound_url:
                selected = "echoloom.connectors.wechat_hook:WeChatHookConnector"
            else:
                selected = default_gateway_factory(config.workspace)
    module_name, separator, attribute = selected.partition(":")
    if not separator or not module_name or not attribute or ":" in attribute:
        raise ValueError("WeChat gateway factory must be module:factory")
    gateway = getattr(import_module(module_name), attribute)(config)
    if not isinstance(gateway, WeChatGateway):
        raise TypeError("WeChat gateway does not implement the required contract")
    return gateway


def connector_normalize_event(gateway: object, item: MessageEvent) -> MessageEvent | None:
    """Normalize an inbound event through the owning connector when available."""
    normalize_fn = getattr(gateway, "normalize_event", None)
    if callable(normalize_fn):
        return normalize_fn(item)
    if getattr(gateway, "channel", item.channel) == "wechat" or item.message_type != 1:
        return import_module("echoloom.rich_message").normalize_message(item)
    return item


def connector_in_scope(
    gateway: object,
    item: MessageEvent,
    *,
    account: str,
    default_room: str = "",
    registered_group_ids: set[str] | frozenset[str] = frozenset(),
) -> bool:
    """Check whether an inbound event is admitted by its source connector."""
    in_scope_fn = getattr(gateway, "in_scope", None)
    if callable(in_scope_fn):
        return bool(
            in_scope_fn(
                item,
                account=account,
                default_room=default_room,
                registered_group_ids=registered_group_ids,
            )
        )
    if item.account_id != account:
        return False
    return bool(item.conversation_id and item.sender_id)


def connector_ensure_ready(gateway: object, account_id: str) -> bool:
    """Run optional connector-side post-login/source-ready initialization cleanly."""
    ensure_fn = getattr(gateway, "ensure_ready", None)
    if callable(ensure_fn):
        return bool(ensure_fn(account_id))
    return False


def connector_verify_request(
    gateway: object,
    headers: Mapping[str, str],
    raw_body: bytes,
) -> bool:
    """Verify optional Bearer token / HMAC signature configured on a connector."""
    verify_fn = getattr(gateway, "verify_request", None)
    if callable(verify_fn):
        return bool(verify_fn(headers, raw_body))
    return True


def verify_bearer_or_hmac(
    headers: Mapping[str, str],
    raw_body: bytes,
    *,
    auth_token: str = "",
    hmac_secret: str = "",
) -> bool:
    """Constant-time Bearer token and/or `X-Signature-256` HMAC check for webhook sources."""
    norm = {k.casefold(): v for k, v in headers.items()}
    if auth_token:
        header_val = norm.get("authorization", "").strip()
        expected = f"Bearer {auth_token}"
        if not hmac.compare_digest(header_val.encode("utf-8"), expected.encode("utf-8")):
            return False
    if hmac_secret:
        sig = norm.get("x-signature-256", "").strip()
        if sig.startswith("sha256="):
            sig = sig[7:]
        digest = hmac.new(hmac_secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig.lower().encode("utf-8"), digest.encode("utf-8")):
            return False
    return True


def default_event_parser(payload: object) -> tuple[MessageEvent, ...]:
    """Compatibility default for legacy tests/callers predating connector injection."""
    return import_module("echoloom.adapters.hook_events").parse_callback(payload)


def default_archive_envelope(item: MessageEvent, *, preserve_raw: bool = False) -> dict:
    """Compatibility serializer for legacy tests/callers predating connector injection."""
    return import_module("echoloom.adapters.hook_events").archive_envelope(
        item, preserve_raw=preserve_raw
    )


def recover_status_event(
    status: object,
    *,
    account_id: str,
    conversation_id: str,
    gateway: object | None = None,
) -> MessageEvent | None:
    """Compatibility status-snapshot recovery helper delegated behind connector boundary."""
    fn = getattr(gateway, "observation_from_status", None) if gateway is not None else None
    if callable(fn):
        return fn(status, account_id=account_id, conversation_id=conversation_id)
    return import_module("echoloom.adapters.hook_recovery").observation_from_hook_status(
        status, account_id=account_id, conversation_id=conversation_id
    )


def resolve_storage_root(status: object, account_id: str) -> Path | None:
    """Compatibility helper delegating storage discovery behind the connector boundary."""
    return import_module("echoloom.adapters.hook_media").wechat_storage_root_from_status(
        status, account_id
    )
