"""Tests verifying backward-compatibility aliases for the legacy ``wechat_agent`` package."""

from __future__ import annotations

import importlib


def test_legacy_wechat_agent_package_aliases_resolve_to_echoloom() -> None:
    import wechat_agent.bootstrap as legacy_bootstrap
    from wechat_agent.connectors.webhook import WebhookConnector as LegacyWebhook

    import echoloom.bootstrap as canonical_bootstrap
    import echoloom.connectors.webhook as canonical_webhook
    import wechat_agent

    assert legacy_bootstrap.build_runtime_bundle is canonical_bootstrap.build_runtime_bundle
    assert LegacyWebhook is canonical_webhook.WebhookConnector
    assert (
        importlib.import_module("wechat_agent.connectors.base").ChannelConnector
        is importlib.import_module("echoloom.connectors.base").ChannelConnector
    )
    assert wechat_agent.bootstrap.load_live_route is canonical_bootstrap.load_live_route
