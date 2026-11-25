"""Tests for centralized `.local/` vault isolation, Hook decoupling, and multi-source routing."""

from __future__ import annotations

import ast
import asyncio
import base64
import hashlib
import hmac
import io
import json
import shutil
import wave
from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from wechat_agent import agent_profiles, bootstrap, local_config, prompt_catalog
from wechat_agent.connectors import (
    GatewayConfig,
    RegisteredSource,
    SourceRegistry,
    WebhookConnector,
)
from wechat_agent.contacts import ContactIdentity, is_group_conversation
from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.media import WeChatVoiceIngestor
from wechat_agent.memory_store import MemoryStore
from wechat_agent.models import ConversationKey, ReplyDraft
from wechat_agent.pilot import Pilot
from wechat_agent.project_tools import ProjectTools
from wechat_agent.temporal_context import DailyEnvironmentProvider, LocalWeatherProvider


def _wav_bytes(samples: int = 2400) -> bytes:
    buf = io.BytesIO()
    with wave.open(buf, "wb") as out:
        out.setnchannels(1)
        out.setsampwidth(2)
        out.setframerate(24000)
        out.writeframes(b"\x00\x00" * samples)
    return buf.getvalue()


def test_vault_precedence_and_project_tools_secret_boundary(tmp_path: Path, monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")
    shutil.copytree(repo_root / "prompts", tmp_path / "prompts")
    (tmp_path / "prompts.local").mkdir()
    (tmp_path / ".local" / "prompts").mkdir(parents=True)

    base_prof = (tmp_path / "config" / "agent_profiles.toml").read_text(encoding="utf-8")
    (tmp_path / "config" / "agent_profiles.local.toml").write_text(
        base_prof.replace("wxid_owner_example", "wxid_legacy_owner"), encoding="utf-8"
    )
    (tmp_path / ".local" / "agent_profiles.toml").write_text(
        base_prof.replace("wxid_owner_example", "wxid_vault_owner"), encoding="utf-8"
    )
    (tmp_path / "prompts.local" / "persona.md").write_text("legacy persona", encoding="utf-8")
    (tmp_path / ".local" / "prompts" / "persona.md").write_text("vault persona", encoding="utf-8")
    (tmp_path / ".local" / "runtime.toml").write_text(
        '[identity]\nbrand_name = "CustomNeko"\nextra_sensitive_name_terms = ["SecretBot"]\n'
        '[context]\npriority_sender_present_anchor = "PRESENT_ANCHOR"\n'
        '[security]\nwebhook_secret = "super-secret"\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)
    monkeypatch.setattr(local_config, "ROOT", tmp_path)
    monkeypatch.setattr(agent_profiles, "_USE_LOCAL_OVERRIDES", True)
    monkeypatch.setattr(prompt_catalog, "_PROMPT_DIR", tmp_path / "prompts")
    monkeypatch.setattr(prompt_catalog, "_LOCAL_PROMPT_DIR", tmp_path / "prompts.local")

    loaded_profiles = agent_profiles.load_agent_profiles(
        tmp_path / "config" / "agent_profiles.toml"
    )
    assert "wxid_vault_owner" in loaded_profiles.command_owner_ids
    assert "wxid_legacy_owner" not in loaded_profiles.command_owner_ids

    prompt_catalog.refresh_prompts()
    try:
        assert prompt_catalog.load_prompt("persona") == "vault persona"
    finally:
        monkeypatch.undo()
        prompt_catalog.refresh_prompts()

    # Re-enable vault locally to verify ProjectTools isolation and ContextWindow integration.
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)
    monkeypatch.setattr(local_config, "ROOT", tmp_path)

    tools = ProjectTools(tmp_path)
    assert tools.read_project_resource(".local/runtime.toml")["status"] == "rejected"
    assert tools.read_project_resource(".local/agent_profiles.toml")["status"] == "rejected"
    assert tools.list_project_resources(".local")["status"] == "rejected"
    read_vault_prompt = tools.read_project_resource(".local/prompts/persona.md")
    assert read_vault_prompt["status"] == "completed"
    upd = tools.update_own_prompt(
        "persona.md", read_vault_prompt["sha256"], "vault persona", "vault persona v2"
    )
    assert upd["status"] == "updated"
    assert (tmp_path / ".local" / "prompts" / "persona.md").read_text(encoding="utf-8") == "vault persona v2"
    assert (tmp_path / "prompts.local" / "persona.md").read_text(encoding="utf-8") == "legacy persona"

    win = ContextWindow(
        account_id="bot",
        conversation_id="room:ops",
        is_group=True,
        turns=(
            ContextTurn("user:owner", "webhook", "hi", source_conversation_id="room:ops", source_is_group=True),
            ContextTurn("user:untrusted", "webhook", "hello", source_conversation_id="room:ops", source_is_group=True),
        ),
        identities={
            "user:owner": ContactIdentity("Owner", user_name="user:owner", nickname="Owner"),
            "user:untrusted": ContactIdentity("SecretBot Fake", user_name="user:untrusted", nickname="SecretBot Fake"),
        },
        priority_sender_references=({"sender_id": "user:owner", "memories": ()},),
    )
    rendered = win.model_user_content()
    ref_rendered = win.model_reference_context()
    assert "PRESENT_ANCHOR" in ref_rendered
    assert "联系人-" in rendered
    assert "SecretBot Fake" not in rendered


def test_local_weather_provider_stays_disabled_unless_configured(monkeypatch) -> None:
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", False)
    env_off = DailyEnvironmentProvider()
    text_off = env_off.system_prompt_context()
    assert "深圳" not in text_off
    assert "今天是" in text_off

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        assert request.url.params["latitude"] == "35.6762"
        return httpx.Response(
            200,
            json={
                "current": {
                    "time": "2026-11-01T12:00",
                    "temperature_2m": 18.5,
                    "apparent_temperature": 18.0,
                    "relative_humidity_2m": 60,
                    "precipitation": 0,
                    "weather_code": 0,
                    "wind_speed_10m": 5.0,
                },
                "daily": {
                    "time": ["2026-11-01"],
                    "weather_code": [1],
                    "temperature_2m_min": [12.0],
                    "temperature_2m_max": [21.0],
                    "precipitation_probability_max": [10],
                },
            },
        )

    weather = LocalWeatherProvider(
        city="Tokyo",
        latitude=35.6762,
        longitude=139.6503,
        timezone_name="Asia/Tokyo",
        enabled=True,
        transport=httpx.MockTransport(handler),
    )
    summary = weather.summary_for_today()
    assert summary.startswith("Tokyo当前天气")
    assert len(requests) == 1


def test_webhook_connector_auth_media_and_outbound_without_hook(tmp_path: Path) -> None:
    outbound_seen: list[tuple[dict[str, str], dict[str, object]]] = []

    def transport_handler(request: httpx.Request) -> httpx.Response:
        outbound_seen.append((dict(request.headers), json.loads(request.content)))
        return httpx.Response(200, json={"ok": True})

    secret = "hmac-secret-123"
    token = "bearer-tok-456"
    connector = WebhookConnector(
        GatewayConfig(
            workspace=tmp_path,
            account_id="bot_webhook",
            send_enabled=True,
            source_id="webhook",
            channel="webhook",
            outbound_url="https://bridge.example.invalid/reply",
            auth_token=token,
            hmac_secret=secret,
            transport=httpx.MockTransport(transport_handler),
        )
    )
    assert connector.account_state("bot_webhook").logged_in is True
    assert is_group_conversation("room:eng-ops") is True
    assert is_group_conversation("tg:123456") is False

    wav = _wav_bytes()
    payload = {
        "events": [
            {
                "conversation_id": "room:eng-ops",
                "sender_id": "tg:42",
                "sender_name": "Alice",
                "conversation_name": "Eng Ops",
                "text": "部署完成了吗？",
                "is_group": True,
                "message_id": 1001,
            },
            {
                "conversation_id": "room:eng-ops",
                "sender_id": "tg:42",
                "kind": "voice",
                "audio_base64": base64.b64encode(wav).decode("ascii"),
                "audio_format": "wav",
                "duration_ms": 100,
                "message_id": 1002,
            },
        ]
    }
    raw_body = json.dumps(payload).encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    assert connector.verify_request(
        {"Authorization": f"Bearer {token}", "X-Signature-256": f"sha256={sig}"},
        raw_body,
    )
    assert not connector.verify_request({"Authorization": "Bearer wrong"}, raw_body)

    events = connector.parse_events(payload)
    assert len(events) == 2
    assert events[0].channel == "webhook" and events[0].is_group is True
    assert connector.resolve_contact("tg:42").nickname == "Alice"

    store = MemoryStore(tmp_path / "data" / "memory.sqlite3")
    archived = store.archive_message(
        account_id="bot_webhook",
        conversation_id="room:eng-ops",
        sender_id="tg:42",
        raw_envelope=connector.archive_envelope(events[1]),
        body_text=events[1].text,
    )
    try:
        audio_meta = WeChatVoiceIngestor(tmp_path, gateway=connector).ingest(
            item=events[1], message_id=archived.message_id, store=store
        )
        assert audio_meta["state"] == "ready"
        stored_audio = store.get_audio_for_message(
            account_id="bot_webhook",
            conversation_id="room:eng-ops",
            message_id=archived.message_id,
        )
        assert stored_audio is not None
        assert Path(str(stored_audio["artifact_path"])).read_bytes() == wav
    finally:
        store.close()

    res = connector.send_text(
        ReplyDraft("ev-1", ConversationKey("bot_webhook", "room:eng-ops"), "已上线", 0)
    )
    assert res.status == "queued"
    assert len(outbound_seen) == 1
    sent_headers, sent_body = outbound_seen[0]
    assert sent_headers["authorization"] == f"Bearer {token}"
    assert sent_headers["x-signature-256"].startswith("sha256=")
    assert sent_body["conversation_id"] == "room:eng-ops" and sent_body["text"] == "已上线"


def test_multisource_registry_ingress_and_outbound_routing(tmp_path: Path) -> None:
    async def scenario() -> None:
        alerts_src = WebhookConnector(
            GatewayConfig(
                workspace=tmp_path,
                account_id="echo_bot",
                send_enabled=True,
                source_id="alerts",
                channel="alerts",
                auth_token="alerts-token",
            )
        )
        chat_src = WebhookConnector(
            GatewayConfig(
                workspace=tmp_path,
                account_id="echo_bot",
                send_enabled=True,
                source_id="feishu",
                channel="feishu",
            )
        )
        registry = SourceRegistry(
            [
                RegisteredSource("feishu", "feishu", "webhook", chat_src, account_id="echo_bot"),
                RegisteredSource("alerts", "alerts", "webhook", alerts_src, account_id="echo_bot"),
            ],
            default_source_id="feishu",
        )
        router = registry.as_gateway()

        async def fake_reply(_window) -> str:
            return "ack"

        pilot = Pilot(
            tmp_path / "pilot.sqlite3",
            "echo_bot",
            "room:default",
            fake_reply,
            router.send_text,
            parse_events=router.parse_events,
            normalize_event=router.normalize_event,
            registry=registry,
            archive_envelope=router.archive_envelope,
            admit_group=lambda cid: bool(cid),
        )
        app = bootstrap.create_pilot_app(pilot)
        try:
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 50001)),
                base_url="http://127.0.0.1",
            ) as client:
                sources_resp = (await client.get("/sources")).json()
                assert {s["source_id"] for s in sources_resp["sources"]} == {"feishu", "alerts"}

                assert (await client.post("/sources/unknown/events", json={"events": []})).status_code == 404
                unauth = await client.post(
                    "/sources/alerts/events",
                    json={"conversation_id": "room:ops", "sender_id": "prometheus", "text": "CPU high"},
                )
                assert unauth.status_code == 401

                ok = await client.post(
                    "/sources/alerts/events",
                    headers={"Authorization": "Bearer alerts-token"},
                    json={"conversation_id": "room:ops", "sender_id": "prometheus", "text": "CPU high", "is_group": True},
                )
                assert ok.status_code == 200
                assert ok.json()["results"] == ["queued_test"]

                legacy_ok = await client.post(
                    "/callback",
                    json={"conversation_id": "feishu:ou_999", "sender_id": "feishu:ou_999", "text": "hi"},
                )
                assert legacy_ok.status_code == 200
                assert legacy_ok.json()["results"] == ["queued_test"]

            router.send_text(ReplyDraft("e1", ConversationKey("echo_bot", "room:ops"), "checking alerts", 0))
            router.send_text(ReplyDraft("e2", ConversationKey("echo_bot", "feishu:ou_999"), "hello feishu", 0))
            assert [row["text"] for row in alerts_src.outbound] == ["checking alerts"]
            assert [row["text"] for row in chat_src.outbound] == ["hello feishu"]
        finally:
            pilot.db.close()
            registry.close()

    asyncio.run(scenario())


def test_load_live_route_and_build_ingress_without_chatroom_or_hook(tmp_path: Path, monkeypatch) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")
    shutil.copytree(repo_root / "prompts", tmp_path / "prompts")
    (tmp_path / ".local").mkdir()
    (tmp_path / ".local" / "sources.toml").write_text(
        'default_source = "webhook"\n'
        '[sources.webhook]\n'
        'enabled = true\n'
        'factory = "wechat_agent.connectors.webhook:WebhookConnector"\n'
        'channel = "webhook"\n'
        'account_id = "echo_bot"\n'
        'default_conversation_id = "room:webhook-main"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)

    route = bootstrap.load_live_route(tmp_path)
    assert route.account == "echo_bot"
    assert route.room == "room:webhook-main"

    ingress, app = bootstrap.build_ingress_app(tmp_path)
    try:
        assert bootstrap.admit_group_route(route, "room:new-channel") is True
        remote_client = TestClient(app, client=("192.0.2.10", 50002))
        denied = remote_client.post(
            "/sources/webhook/events",
            json={"conversation_id": "discord:chan-42", "sender_id": "discord:user-7", "text": "ping"},
        )
        assert denied.status_code == 403

        with TestClient(app, client=("127.0.0.1", 50002)) as client:
            resp = client.post(
                "/sources/webhook/events",
                json={
                    "conversation_id": "discord:chan-42",
                    "sender_id": "discord:user-7",
                    "text": "ping",
                    "is_group": True,
                },
            )
            assert resp.status_code == 200
            assert resp.json()["results"] == ["queued_test"]
    finally:
        ingress.close()


def test_business_modules_never_import_concrete_hook_adapters() -> None:
    root = Path(__file__).resolve().parents[1] / "src" / "wechat_agent"
    business_files = [
        root / name
        for name in (
            "wechat_tools.py",
            "media.py",
            "pilot.py",
            "context_assembler.py",
            "ingress.py",
            "dispatcher.py",
            "outbound.py",
            "triggers.py",
            "turn_scope.py",
            "message_ingestor.py",
            "context.py",
            "context_builder.py",
            "cross_session.py",
            "session_rollover.py",
            "cross_chat_dedupe.py",
            "turn_hooks.py",
            "bootstrap.py",
            "wechat_gateway.py",
            "trace_dashboard.py",
        )
    ]
    business_files.extend(sorted((root / "repos").glob("*.py")))
    business_files.extend(sorted((root / "toolsets").glob("*.py")))
    forbidden_targets = (
        "adapters.hook",
        "connectors.wechat_hook",
        "hook_contract",
        "hook_outbound",
        "hook_status_recovery",
    )
    for path in business_files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            modules: list[str] = []
            if isinstance(node, ast.Import):
                modules = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module]
            for mod in modules:
                assert not any(bad in mod for bad in forbidden_targets), (
                    f"Hook coupling import {mod!r} in {path.relative_to(root)}"
                )


def test_clean_git_cold_start_uses_webhook_without_local_vault_or_hook(
    tmp_path: Path, monkeypatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")
    shutil.copytree(repo_root / "prompts", tmp_path / "prompts")
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", False)
    monkeypatch.setattr(local_config, "ROOT", tmp_path)
    monkeypatch.delenv("ECHOLOOM_GATEWAY_FACTORY", raising=False)
    monkeypatch.delenv("WECHAT_GATEWAY_FACTORY", raising=False)

    route = bootstrap.load_live_route(tmp_path)
    assert route.account == "bot_local"
    assert route.room == "room:general"

    ingress, app = bootstrap.build_ingress_app(tmp_path)
    try:
        assert ingress.registry is not None
        assert ingress.registry.default().channel == "webhook"
        assert isinstance(ingress.registry.default().connector, WebhookConnector)
        with TestClient(app, client=("127.0.0.1", 50003)) as client:
            sources_resp = client.get("/sources")
            assert sources_resp.status_code == 200
            assert sources_resp.json()["default_source"] == "webhook"
            ev = client.post(
                "/sources/webhook/events",
                json={
                    "conversation_id": "room:general",
                    "sender_id": "user:tester",
                    "text": "hello from clean git checkout",
                    "is_group": True,
                },
            )
            assert ev.status_code == 200
            assert ev.json()["results"] == ["queued_test"]
    finally:
        ingress.close()


def test_init_local_vault_is_idempotent_and_server_settings_load(
    tmp_path: Path, monkeypatch,
) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")

    first = local_config.init_local_vault(tmp_path)
    assert set(first["created"]) == {
        ".local/runtime.toml",
        ".local/sources.toml",
        ".local/agent_profiles.toml",
    }
    assert first["skipped"] == []

    runtime_file = tmp_path / ".local" / "runtime.toml"
    runtime_file.write_text(
        "[server]\n"
        'ingress_host = "127.0.0.1"\n'
        "ingress_port = 9091\n"
        'dashboard_host = "127.0.0.1"\n'
        "dashboard_port = 9767\n"
        "episode_rag_enabled = false\n",
        encoding="utf-8",
    )

    second = local_config.init_local_vault(tmp_path)
    assert second["created"] == []
    assert set(second["skipped"]) == set(first["created"])

    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)
    cfg = local_config.load_local_runtime_config(tmp_path)
    assert cfg.server.ingress_port == 9091
    assert cfg.server.dashboard_port == 9767
    assert cfg.server.episode_rag_enabled is False


def test_build_providers_validates_api_keys_on_demand(tmp_path: Path, monkeypatch) -> None:
    from types import SimpleNamespace

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", False)
    monkeypatch.setattr(local_config, "ROOT", tmp_path)
    monkeypatch.setattr(bootstrap, "load_key", lambda *args, **kwargs: None)
    monkeypatch.setenv("WECHAT_AGENT_EPISODE_RAG", "0")
    monkeypatch.setattr(
        bootstrap.genai,
        "Client",
        lambda **kwargs: SimpleNamespace(kind="fake_gemini", kwargs=kwargs),
    )

    # Case A: pure Gemini profile boots without OPENAI_API_KEY
    gemini_toml = tmp_path / "gemini_only.toml"
    gemini_toml.write_text(
        'default_profile = "default"\n'
        'memory_model = "gemini-3.8-flash"\n'
        'account_daily_budget_microusd = 4000000\n'
        '[profiles.default]\n'
        'model = "gemini-3.8-flash"\n'
        'toolsets = ["wechat"]\n',
        encoding="utf-8",
    )
    gemini_profiles = agent_profiles.load_agent_profiles(gemini_toml)
    monkeypatch.setenv("GEMINI_API_KEY", "dummy-gemini-key-only")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    gemini_bundle = bootstrap.build_providers(tmp_path, gemini_profiles)
    try:
        assert gemini_bundle.model_client is not None
        assert gemini_bundle.openai_client is None
    finally:
        gemini_bundle.repo.close()

    # Case B: pure OpenAI profile without web toolset boots without GEMINI_API_KEY
    openai_toml = tmp_path / "openai_only.toml"
    openai_toml.write_text(
        'default_profile = "default"\n'
        'memory_model = "gpt-6-luna"\n'
        'account_daily_budget_microusd = 4000000\n'
        '[profiles.default]\n'
        'model = "gpt-6-luna"\n'
        'toolsets = ["wechat"]\n',
        encoding="utf-8",
    )
    openai_profiles = agent_profiles.load_agent_profiles(openai_toml)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-only-dummy-key")
    openai_bundle = bootstrap.build_providers(tmp_path, openai_profiles)
    try:
        assert openai_bundle.openai_client is not None
        assert openai_bundle.model_client is None
    finally:
        openai_bundle.repo.close()
