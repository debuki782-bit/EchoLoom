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
    "GatewayConfig",
    "MessageEvent",
    "RecordingConnector",
    "RecordingGateway",
    "RegisteredSource",
    "RouterGateway",
    "SendResult",
    "SourceRegistry",
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
    "RecordingConnector": ("wechat_agent.connectors.recording", "RecordingConnector"),
    "RecordingGateway": ("wechat_agent.connectors.recording", "RecordingGateway"),
    "RegisteredSource": ("wechat_agent.connectors.registry", "RegisteredSource"),
    "RouterGateway": ("wechat_agent.connectors.registry", "RouterGateway"),
    "SourceRegistry": ("wechat_agent.connectors.registry", "SourceRegistry"),
    "WebhookConnector": ("wechat_agent.connectors.webhook", "WebhookConnector"),
    "WeChatHookConnector": ("wechat_agent.connectors.wechat_hook", "WeChatHookConnector"),
}


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        module_name, attr = _LAZY_EXPORTS[name]
        value = getattr(import_module(module_name), attr)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
