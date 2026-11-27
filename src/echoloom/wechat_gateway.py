"""Compatibility shim re-exporting canonical connector types from ``channel_gateway``."""

from .channel_gateway import (
    AccountState,
    ChannelConnector,
    ConnectorConfig,
    GatewayConfig,
    MessageEvent,
    SenderOrigin,
    SendResult,
    VoicePayload,
    WeChatGateway,
    create_connector,
    create_gateway,
    default_archive_envelope,
    default_connector_factory,
    default_event_parser,
    default_gateway_factory,
)

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
