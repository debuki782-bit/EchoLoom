"""Official SDK adapter exercised with a fake client and no API key."""

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from echoloom.models import InboundEvent
from echoloom.providers import GeminiProvider, ModelCallIndeterminate, SafetyBlocked, _rates
from echoloom.storage import Repository
from echoloom.usage import UsageLedger


class FixedClock:
    def now(self) -> str:
        return "2026-10-29T01:00:00+00:00"


class FakeModels:
    def __init__(self, response: Any) -> None:
        self.response = response
        self.calls: list[dict[str, Any]] = []

    def generate_content(self, **kwargs: Any) -> Any:
        self.calls.append(kwargs)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def fake_response(*, text: str = "你好", blocked: bool = False) -> Any:
    return SimpleNamespace(
        text=text,
        usage_metadata=SimpleNamespace(prompt_token_count=30, total_token_count=50),
        prompt_feedback=SimpleNamespace(block_reason="SAFETY" if blocked else None),
        candidates=[],
    )


def provider(tmp_path: Path, response: Any, *, budget: int = 10000) -> tuple[
    GeminiProvider, FakeModels, Repository,
]:
    repo = Repository(tmp_path / "agent.sqlite3")
    models = FakeModels(response)
    client = SimpleNamespace(models=models)
    adapter = GeminiProvider(client, UsageLedger(repo, FixedClock()), FixedClock(),
                             model_id="gemini-3.5-flash-lite", budget_microusd=budget,
                             enabled=True)
    return adapter, models, repo


def event(event_id: str = "one") -> InboundEvent:
    return InboundEvent(event_id, "bot", "friend", "friend", "你好")


def test_gemini_argon_uses_date_appropriate_rates() -> None:
    assert _rates("gemini-4-argon", "2026-11-30T23:59:00+00:00") == (
        2_000_000, 10_000_000,
    )
    assert _rates("gemini-4-argon", "2026-12-01T00:00:00+00:00") == (
        4_000_000, 20_000_000,
    )


def test_gemini_hydrogen_uses_verified_rates() -> None:
    from echoloom.providers import _cached_input_rate

    assert _rates("gemini-4-hydrogen", "2026-11-13T01:00:00+00:00") == (
        1_500_000, 7_500_000,
    )
    assert _cached_input_rate("gemini-4-hydrogen", "2026-11-13T01:00:00+00:00") == 75_000


def test_one_call_uses_sender_context_and_settles_actual_usage(tmp_path: Path) -> None:
    adapter, models, repo = provider(tmp_path, fake_response())
    assert adapter.reply(event()) == "你好"
    assert len(models.calls) == 1
    sent = models.calls[0]
    assert sent["model"] == "gemini-3.5-flash-lite"
    assert sent["config"].max_output_tokens == 512
    assert "friend | friend：*{你好}*" in sent["contents"]
    row = repo.conn.execute("SELECT state,actual_microusd FROM usage_reservations").fetchone()
    assert (row["state"], row["actual_microusd"]) == ("settled", 59)
    with pytest.raises(RuntimeError, match="already_settled"):
        adapter.reply(event())
    assert len(models.calls) == 1
    repo.close()


def test_budget_exhaustion_never_calls_sdk(tmp_path: Path) -> None:
    adapter, models, repo = provider(tmp_path, fake_response(), budget=1)
    with pytest.raises(RuntimeError, match="budget_exhausted"):
        adapter.reply(event())
    assert models.calls == []
    assert repo.conn.execute("SELECT count(*) FROM usage_reservations").fetchone()[0] == 0
    repo.close()


def test_ambiguous_api_error_keeps_reservation_and_does_not_retry(tmp_path: Path) -> None:
    adapter, models, repo = provider(tmp_path, TimeoutError("possibly charged"))
    with pytest.raises(ModelCallIndeterminate, match="reservation retained"):
        adapter.reply(event())
    assert len(models.calls) == 1
    assert repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0] == "reserved"
    with pytest.raises(RuntimeError, match="already_reserved"):
        adapter.reply(event())
    assert len(models.calls) == 1
    repo.close()


def test_explicit_safety_block_is_not_empty_reply(tmp_path: Path) -> None:
    adapter, models, repo = provider(tmp_path, fake_response(text="", blocked=True))
    with pytest.raises(SafetyBlocked, match="explicitly blocked"):
        adapter.reply(event())
    assert len(models.calls) == 1
    assert repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0] == "settled"
    repo.close()


def test_disabled_provider_and_oversized_input_make_no_call(tmp_path: Path) -> None:
    adapter, models, repo = provider(tmp_path, fake_response())
    adapter.enabled = False
    with pytest.raises(RuntimeError, match="disabled"):
        adapter.reply(event())
    adapter.enabled = True
    adapter.max_input_bytes = 1
    with pytest.raises(ValueError, match="byte limit"):
        adapter.reply(event())
    assert models.calls == []
    repo.close()
