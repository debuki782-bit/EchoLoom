import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from echoloom.conversation_scheduler import ConversationScheduler
from echoloom.hook_contract import HookObservation


def observed(text: str, *, conversation: str = "room@chatroom", tick: int = 1,
             sender: str = "member") -> HookObservation:
    return HookObservation(
        account_id="bot", conversation_id=conversation, sender_id=sender,
        sender_origin="hook_group_member_wxid", text=text, is_group=True,
        message_type=1, source_msg_id=None, capture_tick_ms=tick,
        raw_message_json=(
            '{"fromid":"room@chatroom","sender":"member",'
            f'"msg":"{text}","time":{tick}' + "}"
        ),
    )


def test_durable_batch_recovers_and_marks_all_inputs_seen(tmp_path):
    path = tmp_path / "pilot.sqlite3"
    db = sqlite3.connect(path)
    scheduler = ConversationScheduler(db, debounce_seconds=2.0, max_wait_seconds=10)
    first = observed("第一条", tick=1)
    second = observed("第二条", tick=2)
    assert scheduler.enqueue(first)[0] == "queued"
    assert scheduler.enqueue(first)[0] == "duplicate"
    assert scheduler.enqueue(second)[0] == "queued"
    db.close()

    recovered_db = sqlite3.connect(path)
    recovered = ConversationScheduler(recovered_db, debounce_seconds=2.0,
                                      max_wait_seconds=10)
    assert recovered.pending_count() == 2
    assert recovered.claim_ready_batch(now=datetime.now(UTC)) is None
    batch = recovered.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=3))
    assert batch is not None
    assert [item.text for item in batch.observations] == ["第一条", "第二条"]
    assert recovered.state_counts() == {"running": 2}
    assert recovered.mark_model_seen(batch)
    assert recovered.state_counts() == {"model_seen": 2}
    assert recovered.pending_count() == 0
    assert recovered.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=20)) is None
    recovered_db.close()


def test_default_merge_window_uses_short_quiet_period_for_regular_inbound(tmp_path):
    db = sqlite3.connect(tmp_path / "short-window.sqlite3")
    scheduler = ConversationScheduler(db)
    assert scheduler.debounce_seconds == 2.0
    assert scheduler.max_wait_seconds == 10
    scheduler.enqueue(observed("窗口开始", tick=21))
    scheduler.enqueue(observed("窗口内第二条", tick=22))
    queued_at = datetime.now(UTC)
    assert scheduler.claim_ready_batch(now=queued_at + timedelta(seconds=1.9)) is None
    batch = scheduler.claim_ready_batch(now=queued_at + timedelta(seconds=2.1))
    assert batch is not None
    assert [item.text for item in batch.observations] == ["窗口开始", "窗口内第二条"]
    db.close()


def test_large_batch_is_claimed_fifo_without_losing_unclaimed_receipts(tmp_path):
    db = sqlite3.connect(tmp_path / "large.sqlite3")
    scheduler = ConversationScheduler(db)
    messages = [observed(str(i) + "长" * 5000, tick=i + 1) for i in range(3)]
    for message in messages:
        assert scheduler.enqueue(message)[0] == "queued"
    first = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=20))
    assert first is not None and first.observations == (messages[0],)
    assert scheduler.mark_model_seen(first)
    assert scheduler.receipt_state(messages[1]) == "pending"
    second = scheduler.claim_conversation_batch("bot", "room@chatroom")
    assert second is not None and second.observations == (messages[1],)
    assert scheduler.mark_model_seen(second)
    third = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=20))
    assert third is not None and third.observations == (messages[2],)
    db.close()


def test_running_batch_returns_to_pending_after_restart(tmp_path):
    path = tmp_path / "pilot.sqlite3"
    db = sqlite3.connect(path)
    scheduler = ConversationScheduler(db, debounce_seconds=0.1, max_wait_seconds=1)
    scheduler.enqueue(observed("恢复我", tick=8))
    batch = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert batch is not None
    db.close()

    recovered_db = sqlite3.connect(path)
    recovered = ConversationScheduler(recovered_db)
    assert recovered.state_counts() == {"pending": 1}
    retry = recovered.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=61))
    assert retry is not None
    assert retry.receipt_ids == batch.receipt_ids
    recovered_db.close()


def test_duplicate_identity_checks_payload_and_retry_is_bounded(tmp_path, caplog):
    db = sqlite3.connect(tmp_path / "retry.sqlite3")
    scheduler = ConversationScheduler(db, debounce_seconds=0.1, max_wait_seconds=1)
    original = observed("同一 ID", tick=1)
    original = replace(original, source_msg_id="wx-42",
                       raw_message_json='{"msg":"同一 ID"}')
    changed = replace(original, text="不同正文",
                      raw_message_json='{"msg":"不同正文"}')
    assert scheduler.enqueue(original)[0] == "queued"
    assert scheduler.enqueue(original)[0] == "duplicate"
    assert scheduler.enqueue(changed)[0] == "source_id_conflict"

    base = datetime.now(UTC)
    caplog.set_level("ERROR", logger="echoloom.conversation_scheduler")
    for attempt in range(scheduler.ALERT_ATTEMPTS_THRESHOLD + 2):
        claim_now = base + timedelta(seconds=attempt * 400 + 10)
        batch = scheduler.claim_ready_batch(now=claim_now)
        assert batch is not None
        scheduler.release_for_retry(batch, "mock_failure", now=claim_now)
        row = db.execute(
            "SELECT state,attempts,next_attempt_at FROM pilot_inbox",
        ).fetchone()
        assert row["state"] == "pending"
        next_at = datetime.fromisoformat(str(row["next_attempt_at"]))
        delay = (next_at - claim_now).total_seconds()
        expected = min(300, 2 ** min(int(row["attempts"]), 9))
        assert delay == pytest.approx(expected, abs=0.01)

    assert scheduler.state_counts() == {"pending": 1}
    assert scheduler.failed_count() == 0
    assert scheduler.deferred_counts() == {"mock_failure": 1}
    assert any("event=inbox_batch_repeated_retry" in rec.message for rec in caplog.records)
    db.close()


def test_only_allowlisted_failed_batches_requeue_after_login_restart(tmp_path):
    path = tmp_path / "login-recovery.sqlite3"
    db = sqlite3.connect(path)
    scheduler = ConversationScheduler(db, debounce_seconds=0.1, max_wait_seconds=1)
    scheduler.enqueue(observed("旧误隔离批次", tick=31))
    first = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert first is not None
    scheduler.fail_batch(first, "previous_send_attempt_turn_incomplete_no_replay")
    db.execute(
        "UPDATE pilot_inbox SET attempts=? WHERE last_error=?",
        (scheduler.MAX_PROCESS_ATTEMPTS, "previous_send_attempt_turn_incomplete_no_replay"),
    )
    db.commit()

    scheduler.enqueue(observed("未知失败批次", tick=32))
    second = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert second is not None
    scheduler.fail_batch(second, "some_other_error")

    # A pending row under the new state machine must remain untouched by manual legacy recovery.
    scheduler.enqueue(observed("新状态机待重试消息", tick=33))
    third = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert third is not None
    scheduler.release_for_retry(third, "transient_error")
    db.close()

    # Restarting ConversationScheduler must NEVER auto-replay legacy failed rows.
    reopened_db = sqlite3.connect(path)
    reopened = ConversationScheduler(reopened_db, debounce_seconds=0.1, max_wait_seconds=1)
    assert reopened.state_counts() == {"failed": 2, "pending": 1}

    recovered = reopened.requeue_failed_after_restart({
        "previous_send_attempt_turn_incomplete_no_replay",
        "model_llm_calls_limit_exceeded",
    })
    assert [item.text for item in recovered] == ["旧误隔离批次"]
    assert reopened.state_counts() == {"failed": 1, "pending": 2}
    attempts = reopened_db.execute(
        "SELECT attempts FROM pilot_inbox WHERE last_error='requeued_after_login_init'"
    ).fetchone()[0]
    assert attempts == 0
    assert reopened.deferred_counts() == {"transient_error": 1}
    assert reopened.requeue_failed_after_restart({
        "previous_send_attempt_turn_incomplete_no_replay",
    }) == ()
    reopened_db.close()


def test_defer_batch_preserves_attempts_wakes_and_coalesces_newer_inbound(tmp_path):
    db = sqlite3.connect(tmp_path / "defer.sqlite3")
    scheduler = ConversationScheduler(db, debounce_seconds=0.1, max_wait_seconds=1.0)
    first = observed("第一条离线消息", tick=1)
    assert scheduler.enqueue(first)[0] == "queued"

    t0 = datetime.now(UTC)
    for round_idx in range(12):
        assert 12 > scheduler.MAX_PROCESS_ATTEMPTS
        claim_now = t0 + timedelta(seconds=round_idx * 10 + 1)
        batch = scheduler.claim_ready_batch(now=claim_now)
        assert batch is not None
        assert [item.text for item in batch.observations] == ["第一条离线消息"]
        with pytest.raises(ValueError):
            scheduler.defer_batch(batch, delay_seconds=-1.0, now=claim_now)
        with pytest.raises(ValueError):
            scheduler.defer_batch(batch, delay_seconds=5.0, now=datetime.now())
        updated = scheduler.defer_batch(
            batch, "  provider_unavailable  ", delay_seconds=5.0, now=claim_now,
        )
        assert updated == 1
        row = db.execute(
            "SELECT state,batch_id,attempts,last_error,next_attempt_at FROM pilot_inbox",
        ).fetchone()
        assert row["state"] == "pending"
        assert row["batch_id"] is None
        assert row["attempts"] == 0
        assert row["last_error"] == "provider_unavailable"
        assert row["next_attempt_at"] is not None

    # First message is still backed off, and a second message arrives in the same chat.
    second = observed("第二条后续消息", tick=2)
    assert scheduler.enqueue(second)[0] == "queued"
    assert scheduler.wake_deferred_batches() == 1
    next_at = db.execute(
        "SELECT next_attempt_at FROM pilot_inbox WHERE last_error='provider_unavailable'",
    ).fetchone()[0]
    assert next_at is None

    coalesced = scheduler.claim_ready_batch(now=t0 + timedelta(seconds=500))
    assert coalesced is not None
    assert [item.text for item in coalesced.observations] == [
        "第一条离线消息",
        "第二条后续消息",
    ]
    db.close()


def test_budget_exhausted_requeues_after_utc_day_reset_without_waking_ancient_rows(tmp_path):
    db = sqlite3.connect(tmp_path / "budget-reset.sqlite3")
    scheduler = ConversationScheduler(db, debounce_seconds=0.1, max_wait_seconds=1.0)
    today_msg = observed("今日预算耗尽消息", tick=1)
    assert scheduler.enqueue(today_msg)[0] == "queued"
    batch = scheduler.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert batch is not None

    # 2026-11-25 23:35:00 Asia/Shanghai == 2026-11-25 15:35:00 UTC.
    # Next Asia/Shanghai 00:00:05 is 2026-11-25 16:00:05 UTC (25m5s later, NOT next UTC day).
    t0 = datetime(2026, 11, 25, 15, 35, tzinfo=UTC)
    expected_reset = datetime(2026, 11, 25, 16, 0, 5, tzinfo=UTC)
    retry_at = scheduler.defer_until_budget_reset(batch, now=t0)
    assert retry_at == expected_reset

    row = db.execute(
        "SELECT state,attempts,last_error,next_attempt_at FROM pilot_inbox",
    ).fetchone()
    assert row["state"] == "pending"
    assert row["attempts"] == 0
    assert row["last_error"] == "model_budget_exhausted_no_call"
    assert datetime.fromisoformat(str(row["next_attempt_at"])) == expected_reset
    assert scheduler.budget_blocked_count() == 1
    assert scheduler.deferred_counts() == {"model_budget_exhausted_no_call": 1}

    # Historical legacy state='failed' budget rows must NEVER be auto-woken.
    for idx, rec_at in enumerate((
        "2026-11-25T14:00:00+00:00",
        "2026-11-18T12:00:00+00:00",
    ), start=2):
        legacy_item = observed(f"历史遗留死信-{idx}", tick=idx)
        rid = scheduler.receipt_id(legacy_item)
        db.execute(
            "INSERT INTO pilot_inbox (receipt_id,account_id,conversation_id,sender_id,"
            "payload_json,received_at,state,attempts,last_error,next_attempt_at) "
            "VALUES (?,?,?,?,?,?,'failed',1,'model_budget_exhausted_no_call',NULL)",
            (
                rid,
                legacy_item.account_id,
                legacy_item.conversation_id,
                legacy_item.sender_id,
                scheduler._payload(legacy_item),
                rec_at,
            ),
        )
    db.commit()

    assert scheduler.wake_budget_deferred_batches(now=t0 + timedelta(minutes=20)) == ()
    woken = scheduler.wake_budget_deferred_batches(now=expected_reset)
    assert [i.text for i in woken] == ["今日预算耗尽消息"]
    assert scheduler.budget_blocked_count() == 0
    assert scheduler.failed_count("model_budget_exhausted_no_call") == 2
    db.close()


def test_scheduler_enables_wal_heartbeat_and_non_destructive_ingress_open(tmp_path):
    path = tmp_path / "scheme-a.sqlite3"
    worker_db = sqlite3.connect(path)
    worker_sched = ConversationScheduler(worker_db, debounce_seconds=0.1, max_wait_seconds=1.0)
    assert str(worker_db.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal"
    assert int(worker_db.execute("PRAGMA busy_timeout").fetchone()[0]) == 5000

    assert worker_sched.enqueue(observed("运行中批次", tick=99))[0] == "queued"
    claimed = worker_sched.claim_ready_batch(now=datetime.now(UTC) + timedelta(seconds=2))
    assert claimed is not None

    # Opening a second connection with recover_running=False (standalone Ingress)
    # must NOT reset the worker's running batch back to pending.
    ingress_db = sqlite3.connect(path)
    ingress_sched = ConversationScheduler(ingress_db, recover_running=False)
    assert ingress_sched.state_counts() == {"running": 1}

    t0 = datetime(2026, 11, 25, 12, 0, 0, tzinfo=UTC)
    assert ingress_sched.read_worker_heartbeat(max_age_seconds=10.0, now=t0) is None
    worker_sched.update_worker_heartbeat(
        worker_id="worker-1",
        pid=4321,
        wechat_ready=True,
        provider_ready=True,
        in_flight={"sender_sha": "abc123", "message_type": 1, "elapsed_seconds": 1.2},
        scheduled_followups=2,
        now=t0,
    )
    fresh = ingress_sched.read_worker_heartbeat(
        max_age_seconds=10.0, now=t0 + timedelta(seconds=4),
    )
    assert fresh is not None
    assert fresh["alive"] is True
    assert fresh["pid"] == 4321
    assert fresh["wechat_ready"] is True
    assert fresh["provider_ready"] is True
    assert fresh["scheduled_followups"] == 2
    assert fresh["in_flight"] == {"sender_sha": "abc123", "message_type": 1, "elapsed_seconds": 1.2}

    stale = ingress_sched.read_worker_heartbeat(
        max_age_seconds=10.0, now=t0 + timedelta(seconds=15),
    )
    assert stale is not None
    assert stale["alive"] is False
    ingress_db.close()
    worker_db.close()
