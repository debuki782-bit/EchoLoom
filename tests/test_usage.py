"""Offline budget reservations; no Google API or credential is used."""

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from echoloom.storage import Repository
from echoloom.usage import UsageLedger


class MutableClock:
    def __init__(self) -> None:
        self.current = datetime(2026, 10, 29, tzinfo=UTC)

    def now(self) -> str:
        return self.current.isoformat()


def reserve(ledger: UsageLedger, request_id: str, *, amount: int = 100,
            budget: int = 250, account: str = "bot") -> str:
    return ledger.reserve(
        request_id=request_id, account_id=account,
        conversation_id="friend", feature="chat",
        model_id="gemini-3.5-flash-lite", maximum_microusd=amount,
        budget_microusd=budget,
    ).status


def test_reservation_idempotency_settlement_and_restart(tmp_path: Path) -> None:
    path = tmp_path / "agent.sqlite3"
    repo = Repository(path)
    ledger = UsageLedger(repo, MutableClock())
    assert reserve(ledger, "one") == "reserved"
    assert reserve(ledger, "one") == "already_reserved"
    assert reserve(ledger, "one", amount=200) == "request_id_conflict"
    assert ledger.committed_microusd("bot", "2026-10") == 100
    repo.close()
    repo = Repository(path)
    ledger = UsageLedger(repo, MutableClock())
    assert ledger.committed_microusd("bot", "2026-10") == 100
    assert ledger.settle("one", 70) == "settled"
    assert reserve(ledger, "one") == "already_settled"
    assert ledger.settle("one", 70) == "duplicate"
    assert ledger.settle("one", 80) == "conflict"
    assert ledger.committed_microusd("bot", "2026-10") == 70
    assert reserve(ledger, "two", amount=180) == "reserved"
    assert reserve(ledger, "three", amount=1) == "budget_exhausted"
    assert ledger.release("two") == "released"
    assert reserve(ledger, "two", amount=180) == "already_released"
    assert ledger.committed_microusd("bot", "2026-10") == 70
    assert reserve(ledger, "three", amount=1) == "reserved"
    repo.close()


def test_over_reservation_is_recorded_and_blocks_following_calls(tmp_path: Path) -> None:
    repo = Repository(tmp_path / "agent.sqlite3")
    ledger = UsageLedger(repo, MutableClock())
    assert reserve(ledger, "one", amount=200) == "reserved"
    assert ledger.settle("one", 260) == "over_reservation"
    assert ledger.committed_microusd("bot", "2026-10") == 260
    assert reserve(ledger, "two", amount=1) == "budget_exhausted"
    assert ledger.release("one") == "not_reserved"
    repo.close()


def test_concurrent_reservations_do_not_exceed_budget(tmp_path: Path) -> None:
    repo = Repository(tmp_path / "agent.sqlite3")
    ledger = UsageLedger(repo, MutableClock())
    with ThreadPoolExecutor(max_workers=10) as pool:
        statuses = list(pool.map(lambda i: reserve(ledger, f"request-{i}"), range(20)))
    assert statuses.count("reserved") == 2
    assert statuses.count("budget_exhausted") == 18
    assert ledger.committed_microusd("bot", "2026-10") == 200
    repo.close()


def test_account_and_month_are_separate_budgets(tmp_path: Path) -> None:
    clock = MutableClock()
    repo = Repository(tmp_path / "agent.sqlite3")
    ledger = UsageLedger(repo, clock)
    assert reserve(ledger, "one", amount=250) == "reserved"
    assert reserve(ledger, "two", account="other", amount=250) == "reserved"
    clock.current = datetime(2026, 11, 1, tzinfo=UTC)
    assert reserve(ledger, "three", amount=250) == "reserved"
    assert ledger.committed_microusd("bot", "2026-10") == 250
    assert ledger.committed_microusd("bot", "2026-11") == 250
    repo.close()


def test_daily_budget_groups_share_account_cap_and_keep_separate_subcaps(
    tmp_path: Path,
) -> None:
    repo = Repository(tmp_path / "daily-budget.sqlite3")
    ledger = UsageLedger(repo, MutableClock())

    def grouped(request_id: str, *, feature: str, amount: int, group: str,
                cap: int) -> str:
        return ledger.reserve(
            request_id=request_id, account_id="bot", conversation_id="friend",
            feature=feature, model_id="gemini-3.5-flash-lite",
            maximum_microusd=amount, budget_microusd=1_500_000,
            feature_budget_microusd=cap, budget_group=group,
        ).status

    assert grouped("text", feature="adk_chat", amount=1_000_000,
                   group="text_model", cap=1_000_000) == "reserved"
    assert grouped("voice", feature="voice_transcription", amount=1,
                   group="text_model", cap=1_000_000) == "feature_budget_exhausted"
    assert grouped("read-image", feature="adk_chat_image_context", amount=250_000,
                   group="image_read", cap=250_000) == "reserved"
    assert grouped("imagegen", feature="image_generate", amount=250_000,
                   group="image_generation", cap=250_000) == "reserved"
    assert ledger.reserve(
        request_id="over-total", account_id="bot", conversation_id="friend",
        feature="misc", model_id="gemini-3.5-flash-lite", maximum_microusd=1,
        budget_microusd=1_500_000,
    ).status == "budget_exhausted"
    assert ledger.committed_daily_microusd("bot", "2026-10-29") == 1_500_000
    assert ledger.committed_daily_microusd(
        "bot", "2026-10-29", budget_group="image_read",
    ) == 250_000
    repo.close()


def test_daily_budget_resets_at_shanghai_midnight(tmp_path: Path) -> None:
    clock = MutableClock()
    clock.current = datetime(2026, 10, 31, 15, 59, tzinfo=UTC)
    repo = Repository(tmp_path / "daily-reset.sqlite3")
    ledger = UsageLedger(repo, clock)
    assert reserve(ledger, "before-midnight", amount=250, budget=250) == "reserved"
    assert ledger.committed_daily_microusd("bot", "2026-10-31") == 250

    clock.current = datetime(2026, 10, 31, 16, 1, tzinfo=UTC)
    assert reserve(ledger, "after-midnight", amount=250, budget=250) == "reserved"
    assert ledger.committed_daily_microusd("bot", "2026-11-01") == 250
    repo.close()


def test_stale_reservations_can_be_listed_and_released_without_touching_settled(tmp_path: Path) -> None:
    from datetime import timedelta

    clock = MutableClock()
    clock.current = datetime(2026, 11, 26, 10, 0, tzinfo=UTC)
    repo = Repository(tmp_path / "stale.sqlite3")
    ledger = UsageLedger(repo, clock)

    assert ledger.reserve(
        request_id="settled-1",
        account_id="bot",
        conversation_id="room",
        feature="adk_chat",
        model_id="gemini-4-hydrogen",
        maximum_microusd=150_000,
        budget_microusd=500_000,
        budget_group="text_model",
    ).status == "reserved"
    assert ledger.settle("settled-1", 50_000) == "settled"
    assert ledger.reserve(
        request_id="stuck-1",
        account_id="bot",
        conversation_id="room",
        feature="adk_chat",
        model_id="gemini-4-hydrogen",
        maximum_microusd=150_000,
        budget_microusd=500_000,
        budget_group="text_model",
    ).status == "reserved"

    clock.current = clock.current + timedelta(seconds=400)
    assert ledger.reserve(
        request_id="fresh-1",
        account_id="bot",
        conversation_id="room",
        feature="adk_chat",
        model_id="gemini-4-hydrogen",
        maximum_microusd=150_000,
        budget_microusd=500_000,
        budget_group="text_model",
    ).status == "reserved"

    stale = ledger.stale_reservations("bot", "2026-11-26", min_age_seconds=300.0, budget_group="text_model")
    assert [row["request_id"] for row in stale] == ["stuck-1"]
    released = ledger.release_stale_reservations(
        "bot",
        "2026-11-26",
        min_age_seconds=300.0,
        budget_group="text_model",
        reason="timeout cleanup",
    )
    assert released == 1
    assert ledger.committed_daily_microusd("bot", "2026-11-26", budget_group="text_model") == 200_000
    repo.close()
