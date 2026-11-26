"""Pluggable multi-source channel connectors for EchoLoom."""

from __future__ import annotations

from importlib import import_module

from .base import (
    AccountState,
    ChannelConnector,
    ConnectorConfig,
    GatewayConfig,
    MessageEvent,
    SenderOrigin,
    SendResult,
    VoicePayload,
    WeChatGateway,
    connector_ensure_ready,
    connector_in_scope,
    connector_normalize_event,
    connector_verify_request,
    create_gateway,
    default_gateway_factory,
)

__all__ = [
    "AccountState",
    "ChannelConnector",
    "ConnectorConfig",
    "DiscordConnector",
    "FeishuConnector",
    "GatewayConfig",
    "MessageEvent",
    "RecordingConnector",
    "RecordingGateway",
    "RegisteredSource",
    "RouterGateway",
    "SendResult",
    "SenderOrigin",
    "SourceRegistry",
    "TelegramConnector",
    "VoicePayload",
    "WeChatGateway",
    "WeChatHookConnector",
    "WebhookConnector",
    "connector_ensure_ready",
    "connector_in_scope",
    "connector_normalize_event",
    "connector_verify_request",
    "create_gateway",
    "default_gateway_factory",
]

_LAZY_EXPORTS = {
    "DiscordConnector": ("echoloom.connectors.discord", "DiscordConnector"),
    "FeishuConnector": ("echoloom.connectors.feishu", "FeishuConnector"),
    "RecordingConnector": ("echoloom.connectors.recording", "RecordingConnector"),
    "RecordingGateway": ("echoloom.connectors.recording", "RecordingGateway"),
    "RegisteredSource": ("echoloom.connectors.registry", "RegisteredSource"),
    "RouterGateway": ("echoloom.connectors.registry", "RouterGateway"),
    "SourceRegistry": ("echoloom.connectors.registry", "SourceRegistry"),
    "TelegramConnector": ("echoloom.connectors.telegram", "TelegramConnector"),
    "WebhookConnector": ("echoloom.connectors.webhook", "WebhookConnector"),
    "WeChatHookConnector": ("echoloom.connectors.wechat_hook", "WeChatHookConnector"),
}


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        module_name, attr = _LAZY_EXPORTS[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
