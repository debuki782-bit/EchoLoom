"""Canonical channel-agnostic gateway exports (re-exported by ``wechat_gateway``)."""

from .connectors.base import (
    AccountState,
    ChannelConnector,
    ConnectorConfig,
    GatewayConfig,
    MessageEvent,
    SenderOrigin,
    SendResult,
    VoicePayload,
    WeChatGateway,
    create_gateway,
    default_archive_envelope,
    default_event_parser,
    default_gateway_factory,
)

create_connector = create_gateway
default_connector_factory = default_gateway_factory

__all__ = [
    "AccountState",
    "ChannelConnector",
    "ConnectorConfig",
    "GatewayConfig",
    "MessageEvent",
    "SendResult",
    "SenderOrigin",
    "VoicePayload",
    "WeChatGateway",
    "create_connector",
    "create_gateway",
    "default_archive_envelope",
    "default_connector_factory",
    "default_event_parser",
    "default_gateway_factory",
]
