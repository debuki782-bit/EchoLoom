"""Offline tests must not discover the desktop's paid Jev credential."""
import pytest


@pytest.fixture(autouse=True)
def no_desktop_jev_credential(monkeypatch):
    from echoloom import agent_profiles, jev_strategy, local_config, prompt_catalog
    monkeypatch.setattr(jev_strategy, 'load_key', lambda **kwargs: None)
    monkeypatch.delenv('GEMINI_API_KEY', raising=False)
    monkeypatch.setattr(local_config, '_USE_LOCAL_OVERRIDES', False)
    monkeypatch.setattr(agent_profiles, '_USE_LOCAL_OVERRIDES', False)
    monkeypatch.setattr(prompt_catalog, '_LOCAL_PROMPT_DIR', None)
