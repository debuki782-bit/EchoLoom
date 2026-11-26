from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from echoloom import prompt_catalog
from echoloom.agent_profiles import load_agent_profiles
from echoloom.model_overrides import ModelOverrideStore, parse_model_command
from echoloom.storage import Repository
from echoloom.usage import UsageLedger


def test_live_profile_models_and_daily_caps() -> None:
    config = load_agent_profiles()
    profile = config.profiles[config.default_profile]
    assert profile.model_id == "gemini-4-hydrogen"
    assert config.memory_model_id == "gpt-6-luna"
    assert config.for_conversation("wxid_bot_example", "wxid_contact_example").model_id == "gemini-4-hydrogen"
    assert config.for_conversation("wxid_bot_example", "other-private-chat").model_id == "gemini-4-hydrogen"
    assert config.for_conversation("other-account", "wxid_contact_example").model_id == "gemini-4-hydrogen"
    assert profile.input_token_limit == 128_000
    assert profile.idle_compression_tokens == 64_000
    assert profile.active_compression_tokens == 96_000
    assert config.account_daily_budget_microusd == 4_000_000
    assert config.account_text_budget_microusd == 2_000_000
    assert config.account_image_read_budget_microusd == 1_000_000
    assert config.account_image_generation_budget_microusd == 1_000_000
    assert "wxid_owner_example" in config.command_owner_ids


def test_context_limits_inherit_and_can_be_overridden(tmp_path):
    from echoloom.agent_profiles import PROFILE_PATH
    config = tmp_path / "profiles.toml"
    config.write_text(PROFILE_PATH.read_text('utf-8') + '''
[profiles.lab]
extends = "default"
idle_compression_tokens = 72000
active_compression_tokens = 100000
''', 'utf-8')
    lab = load_agent_profiles(config).profiles['lab']
    assert (lab.input_token_limit, lab.idle_compression_tokens, lab.active_compression_tokens) == (
        128_000, 72_000, 100_000,
    )


@pytest.mark.parametrize('replacement', ['true', '-1', '96000', '128001'])
def test_invalid_context_limit_is_rejected_before_loading(tmp_path, replacement):
    from echoloom.agent_profiles import PROFILE_PATH
    config = tmp_path / "profiles.toml"
    config.write_text(PROFILE_PATH.read_text('utf-8').replace(
        'idle_compression_tokens = 64000', 'idle_compression_tokens = ' + replacement,
    ), 'utf-8')
    with pytest.raises(ValueError, match='idle < active < input'):
        load_agent_profiles(config)


def test_argon_override_is_chat_scoped_and_expires_in_one_hour(tmp_path: Path) -> None:
    store = ModelOverrideStore(tmp_path / "overrides.sqlite3")
    command = parse_model_command("切换到 Gemini 4 Argon 一小时")
    assert command == ("set", "gemini-4-argon")
    store.set("account", "chat-a", command[1], temporary_seconds=3600)
    assert store.get("account", "chat-a") == "gemini-4-argon"
    expires = datetime.fromisoformat(store.db.execute(
        "SELECT expires_at FROM conversation_model_overrides WHERE conversation_id='chat-a'"
    ).fetchone()[0])
    assert 3599 <= (expires - datetime.now(UTC)).total_seconds() <= 3600
    assert store.get("account", "chat-b") is None
    with pytest.raises(ValueError):
        store.set("account", "chat-a", "gemini-4-argon")
    assert parse_model_command("模型列表") == ("list", "")
    assert parse_model_command("切换到 Gemini 3.5 Flash Lite") == (
        "set", "gemini-3.5-flash-lite",
    )
    assert parse_model_command("把模型换成 Gemini 3.8 Flash") == (
        "set", "gemini-3.8-flash",
    )
    assert parse_model_command("切换到 Gemini 4 Hydrogen") == (
        "set", "gemini-4-hydrogen",
    )


def test_prompt_refresh_is_atomic_and_keeps_last_good_snapshot(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in prompt_catalog._PROMPT_NAMES:
        (tmp_path / f"{name}.md").write_text(f"old {name}", encoding="utf-8")
    (tmp_path / "runtime_context.json").write_text(
        json.dumps({"runtime_tail_start": "old runtime"}), encoding="utf-8",
    )
    (tmp_path / "tool_descriptions.json").write_text(
        json.dumps({"tool": "old tool"}), encoding="utf-8",
    )
    source_dir = prompt_catalog._PROMPT_DIR
    monkeypatch.setattr(prompt_catalog, "_PROMPT_DIR", tmp_path)
    prompt_catalog.preload_prompts()
    assert prompt_catalog.load_prompt("persona") == "old persona"
    (tmp_path / "persona.md").write_text("new persona", encoding="utf-8")
    assert prompt_catalog.refresh_prompts() == (True, "reloaded")
    assert prompt_catalog.load_prompt("persona") == "new persona"
    (tmp_path / "runtime_context.json").write_text("{broken", encoding="utf-8")
    ok, error = prompt_catalog.refresh_prompts()
    assert not ok and error == "JSONDecodeError"
    assert prompt_catalog.load_prompt("persona") == "new persona"
    monkeypatch.setattr(prompt_catalog, "_PROMPT_DIR", source_dir)
    prompt_catalog.preload_prompts()


def test_account_feature_budget_is_shared_between_profiles(tmp_path: Path) -> None:
    class Clock:
        def now(self) -> str:
            return datetime(2026, 11, 2, tzinfo=UTC).isoformat()

    repo = Repository(tmp_path / "usage.sqlite3")
    ledger = UsageLedger(repo, Clock())
    common = dict(
        account_id="bot", feature="adk_chat", model_id="gemini-3.8-flash",
        maximum_microusd=70, budget_microusd=500,
        account_feature_budget_microusd=100, budget_group="text_model",
    )
    assert ledger.reserve(request_id="one", conversation_id="a", budget_scope_id="a",
                          **common).status == "reserved"
    assert ledger.reserve(request_id="two", conversation_id="b", budget_scope_id="b",
                          **common).status == "account_feature_budget_exhausted"
    repo.close()


def test_local_agent_profiles_override_when_enabled(tmp_path: Path, monkeypatch) -> None:
    from echoloom import agent_profiles

    repo_root = Path(__file__).resolve().parents[1]
    base_text = (repo_root / "config" / "agent_profiles.toml").read_text(encoding="utf-8")
    base_path = tmp_path / "agent_profiles.toml"
    local_path = tmp_path / "agent_profiles.local.toml"
    base_path.write_text(base_text, encoding="utf-8")
    local_path.write_text(
        base_text.replace("wxid_owner_example", "wxid_local_owner"),
        encoding="utf-8",
    )

    monkeypatch.setattr(agent_profiles, "_USE_LOCAL_OVERRIDES", False)
    assert agent_profiles.resolve_profile_path(base_path) == base_path
    assert "wxid_owner_example" in agent_profiles.load_agent_profiles(base_path).command_owner_ids

    monkeypatch.setattr(agent_profiles, "_USE_LOCAL_OVERRIDES", True)
    assert agent_profiles.resolve_profile_path(base_path) == local_path
    local_loaded = agent_profiles.load_agent_profiles(base_path)
    assert "wxid_local_owner" in local_loaded.command_owner_ids
    assert "wxid_owner_example" not in local_loaded.command_owner_ids


def test_local_prompt_dir_overrides_and_falls_back(tmp_path: Path, monkeypatch) -> None:
    import shutil
    from echoloom import prompt_catalog

    repo_prompts = Path(__file__).resolve().parents[1] / "prompts"
    base_prompts = tmp_path / "prompts"
    local_prompts = tmp_path / "prompts.local"
    shutil.copytree(repo_prompts, base_prompts)
    local_prompts.mkdir()
    (local_prompts / "persona.md").write_text("LOCAL PERSONA OVERRIDE", encoding="utf-8")

    monkeypatch.setattr(prompt_catalog, "_PROMPT_DIR", base_prompts)
    monkeypatch.setattr(prompt_catalog, "_LOCAL_PROMPT_DIR", local_prompts)
    prompt_catalog.refresh_prompts()
    try:
        assert prompt_catalog.load_prompt("persona") == "LOCAL PERSONA OVERRIDE"
        assert prompt_catalog.load_prompt("main_agent") == (
            base_prompts / "main_agent.md"
        ).read_text(encoding="utf-8").strip()
    finally:
        monkeypatch.undo()
        prompt_catalog.refresh_prompts()


def test_profile_rollover_and_output_limits_inherit_override_and_validate(tmp_path: Path) -> None:
    from echoloom.agent_profiles import PROFILE_PATH

    config = tmp_path / "profiles.toml"
    base = PROFILE_PATH.read_text("utf-8")
    default_loaded = load_agent_profiles(PROFILE_PATH).profiles["default"]
    assert default_loaded.output_token_limit is None
    assert default_loaded.token_context_limits is True
    assert default_loaded.idle_session_hours == 8
    assert default_loaded.max_history_records == 50
    assert default_loaded.max_history_bytes == 32_000
    assert default_loaded.context_window_bytes == 32_000

    config.write_text(
        base
        + """
[profiles.custom_rollover]
extends = "default"
output_token_limit = 10000
token_context_limits = false
idle_session_hours = 12
max_history_records = 80
max_history_bytes = 48000
context_window_bytes = 40000
""",
        encoding="utf-8",
    )
    custom = load_agent_profiles(config).profiles["custom_rollover"]
    assert custom.output_token_limit == 10_000
    assert custom.token_context_limits is False
    assert custom.idle_session_hours == 12
    assert custom.max_history_records == 80
    assert custom.max_history_bytes == 48_000
    assert custom.context_window_bytes == 40_000

    for bad_line in (
        "output_token_limit = 0",
        "token_context_limits = 'yes'",
        "idle_session_hours = 0",
        "max_history_records = 201",
        "max_history_bytes = 0",
        "context_window_bytes = 511",
    ):
        bad_cfg = tmp_path / "bad.toml"
        bad_cfg.write_text(base + f"\n[profiles.bad]\nextends = 'default'\n{bad_line}\n", "utf-8")
        with pytest.raises(ValueError):
            load_agent_profiles(bad_cfg)


def test_model_catalog_loads_base_overrides_and_rate_tiers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import shutil
    from echoloom import local_config, model_catalog

    repo_root = Path(__file__).resolve().parents[1]
    shutil.copytree(repo_root / "config", tmp_path / "config")
    (tmp_path / ".local").mkdir()
    (tmp_path / ".local" / "models.toml").write_text(
        '[roles]\nweb_model = "gemini-3.5-flash-lite"\n\n'
        '[models."gemini-3.8-flash"]\nlabel = "Local Flash Override"\n\n'
        '[models."custom-local-model"]\nprovider = "openai"\nlabel = "Custom Local"\n'
        'input_token_limit = 64000\noutput_token_limit = 4096\n'
        'input_rate_microusd = 50000\noutput_rate_microusd = 200000\n',
        encoding="utf-8",
    )

    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", False)
    base_cat = model_catalog.load_model_catalog(workspace=tmp_path)
    assert base_cat.roles.web_model == "gemini-3.8-flash"
    flash = base_cat.require("gemini-3.8-flash")
    assert flash.rates_for("2026-11-26T00:00:00+00:00") == (750_000, 3_750_000)
    assert flash.rates_for("2027-01-01T00:00:00+00:00") == (1_500_000, 7_500_000)
    assert base_cat.require("gemini-4-argon").temporary_only_max_seconds == 3600
    assert base_cat.require("gemini-4-hydrogen").thinking_level == "LOW"
    assert base_cat.get("custom-local-model") is None

    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)
    local_cat = model_catalog.load_model_catalog(workspace=tmp_path)
    assert local_cat.roles.web_model == "gemini-3.5-flash-lite"
    assert local_cat.roles.default_memory_model == "gpt-6-luna"
    assert local_cat.require("gemini-3.8-flash").label == "Local Flash Override"
    assert local_cat.require("custom-local-model").provider == "openai"
    assert local_cat.require("gemini-4-hydrogen").provider == "gemini"


def test_explicit_utc_timezone_in_local_runtime_is_honored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from echoloom import local_config, temporal_context

    (tmp_path / ".local").mkdir()
    (tmp_path / ".local" / "runtime.toml").write_text(
        '[location]\ntimezone = "UTC"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(local_config, "ROOT", tmp_path)
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", False)
    assert temporal_context.configured_timezone() == ("Asia/Shanghai", temporal_context.SHENZHEN_TZ)

    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)
    assert temporal_context.configured_timezone() == ("UTC", UTC)

