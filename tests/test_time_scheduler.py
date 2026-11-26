import sqlite3
from datetime import UTC, datetime, timedelta

from echoloom.time_scheduler import TimeScheduler


def test_time_scheduler_claims_defers_and_cancels_account_jobs(tmp_path):
    db = sqlite3.connect(tmp_path / "scheduler.sqlite3")
    scheduler = TimeScheduler(db)
    now = datetime(2026, 10, 30, 1, 0, tzinfo=UTC)
    idle = scheduler.schedule(account_id="bot", conversation_id=None, kind="idle_event", due_at=now)
    claimed = scheduler.claim_due(now=now, kind="idle_event")
    assert claimed is not None and claimed.job_id == idle
    assert scheduler.defer(idle, now + timedelta(minutes=5), "waiting")
    assert scheduler.claim_due(now=now, kind="idle_event") is None

    scheduler.record_inbound("bot", "wxid_sender123", at=now)
    assert scheduler.next_idle_job("bot") is None
    assert scheduler.has_scheduled_job("bot", "session_maintenance")
    assert not scheduler.claim_job(idle, now=now + timedelta(minutes=10))
    db.close()


def test_idle_events_use_38_flash_and_only_apply_rolling_daily_cap(tmp_path):
    db = sqlite3.connect(tmp_path / "idle.sqlite3")
    scheduler = TimeScheduler(db)
    start = datetime(2026, 10, 30, 1, 0, tzinfo=UTC)
    event_ids = []
    for index in range(1, 5):
        event_id, number, model = scheduler.record_idle_event(
            "bot",
            at=start + timedelta(hours=index),
        )
        event_ids.append(event_id)
        assert number == index
        assert model == "gemini-3.8-flash"
        scheduler.record_idle_activity(
            account_id="bot",
            event_id=event_id,
            event_number=number,
            model_id=model,
            summary=f"活动 {index}",
            started_at=start,
            finished_at=start + timedelta(hours=index),
        )
    assert len(scheduler.recent_idle_activities("bot")) == 4
    state = scheduler.account_activity_state("bot")
    assert state["consecutive_idle_events"] == 4
    assert scheduler.has_idle_activity("bot", event_ids[0])
    scheduler.record_inbound("bot", "wxid_sender123", at=start + timedelta(hours=5))
    assert scheduler.account_activity_state("bot")["consecutive_idle_events"] == 0
    db.close()


def test_crash_recovery_records_interrupted_idle_event(tmp_path):
    path = tmp_path / "recover.sqlite3"
    db = sqlite3.connect(path)
    scheduler = TimeScheduler(db)
    now = datetime(2026, 10, 30, 1, 0, tzinfo=UTC)
    event_id, _, _ = scheduler.record_idle_event("bot", at=now)
    db.close()

    recovered_db = sqlite3.connect(path)
    recovered = TimeScheduler(recovered_db)
    assert recovered.has_idle_activity("bot", event_id)
    assert recovered.recent_idle_activities("bot")[0].startswith("服务重启中断")
    recovered_db.close()


def test_recall_skips_failed_placeholder_summaries(tmp_path):
    db = sqlite3.connect(tmp_path / "recall.sqlite3")
    scheduler = TimeScheduler(db)
    start = datetime(2026, 11, 1, 1, 0, tzinfo=UTC)
    for index in range(3):
        event_id, number, model = scheduler.record_idle_event("bot", at=start + timedelta(hours=index))
        scheduler.record_idle_activity(
            account_id="bot",
            event_id=event_id,
            event_number=number,
            model_id=model,
            summary=f"正常活动 {index}",
            started_at=start,
            finished_at=start + timedelta(hours=index),
        )
    # The two-record-failure fallback writes outcome='failed' placeholders.
    scheduler.record_idle_activity(
        account_id="bot",
        event_id="idle_placeholder0000000000000000",
        event_number=1,
        model_id="gemini-3.8-flash",
        summary="模型完成了空闲轮次，但两次记录请求均未写入；系统已记为无摘要。",
        started_at=start,
        finished_at=start + timedelta(hours=4),
        outcome="failed",
    )
    recalled = scheduler.recent_idle_activities("bot")
    assert recalled == ["正常活动 2", "正常活动 1", "正常活动 0"]
    db.close()


def test_prune_caps_completed_session_maintenance_jobs_and_deletes_aged_rows(tmp_path):
    db = sqlite3.connect(tmp_path / "prune.sqlite3")
    scheduler = TimeScheduler(db)
    now = datetime.now(UTC)
    old_ts = (now - timedelta(days=10)).isoformat()
    recent_ts = (now - timedelta(hours=1)).isoformat()

    # Insert aged completed jobs + 520 recent completed session_maintenance rows + 1 scheduled row.
    with db:
        for i in range(15):
            db.execute(
                "INSERT INTO time_jobs(job_id,account_id,conversation_id,kind,due_at,"
                "payload_json,state,created_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (f"old-{i}", "bot", None, "session_maintenance", old_ts, "{}", "completed", old_ts, old_ts),
            )
        for i in range(520):
            ts = (now - timedelta(minutes=520 - i)).isoformat()
            db.execute(
                "INSERT INTO time_jobs(job_id,account_id,conversation_id,kind,due_at,"
                "payload_json,state,created_at,finished_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (f"recent-{i}", "bot", None, "session_maintenance", recent_ts, "{}", "completed", ts, ts),
            )
    scheduled_id = scheduler.schedule(
        account_id="bot",
        conversation_id=None,
        kind="session_maintenance",
        due_at=now + timedelta(minutes=30),
    )

    deleted = scheduler.prune()
    assert deleted == 35  # 15 aged + 20 over the 500 per-kind cap
    remaining_completed = db.execute(
        "SELECT count(*) FROM time_jobs WHERE kind='session_maintenance' AND state='completed'"
    ).fetchone()[0]
    assert remaining_completed == TimeScheduler.MAX_TERMINAL_JOBS_PER_KIND
    assert scheduler.has_scheduled_job("bot", "session_maintenance")
    assert db.execute(
        "SELECT state FROM time_jobs WHERE job_id=?", (scheduled_id,),
    ).fetchone()[0] == "scheduled"
    db.close()
