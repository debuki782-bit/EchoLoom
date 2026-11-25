import sqlite3
from datetime import UTC, datetime

from wechat_agent.idle_tools import IdleActivityToolset
from wechat_agent.time_scheduler import TimeScheduler
from wechat_agent.tool_timing import _record_idle_tool, adk_function_tool


def test_idle_summary_persists_only_successful_calls_and_partial_bundle(tmp_path):
    db = sqlite3.connect(tmp_path / "idle.sqlite3")
    scheduler = TimeScheduler(db)
    idle = IdleActivityToolset(scheduler)
    now = datetime.now(UTC)
    event_id, number, model = scheduler.record_idle_event("bot", at=now)
    token = idle.bind("bot", event_id, number, model, now)
    try:
        def search_user_memory(query):
            return {"memories": []}

        adk_function_tool(search_user_memory, include_elapsed=False).func(query="设备")
        _record_idle_tool("search_web", {"query": "失败"}, {"ok": False})
        _record_idle_tool(
            "commit_agent_turn",
            {"sender_updates": [{"sender": "a"}, {"sender": "b"}]},
            {"status": "completed", "sender_updates": [
                {"status": "saved"}, {"status": "rejected"},
            ]},
        )
        assert idle.record_idle_activity("完成整理")["status"] == "recorded"
        _record_idle_tool("list_account_events", {}, {"status": "completed", "events": []})
        idle.finish("bot", event_id)
    finally:
        idle.unbind(token)
    summary = scheduler.recent_idle_activities("bot")[0]
    assert "完成整理" in summary and "search_user_memory" in summary
    assert "list_account_events" in summary and '"sender": "a"' in summary
    assert '"sender": "b"' not in summary and "search_web" not in summary
    db.close()


def test_existing_scheduler_database_migrates_without_losing_idle_summary(tmp_path):
    db = sqlite3.connect(tmp_path / "old.sqlite3")
    db.execute("CREATE TABLE idle_activity_summaries (activity_id TEXT PRIMARY KEY, "
               "event_id TEXT UNIQUE,account_id TEXT,event_number INTEGER,model_id TEXT, "
               "summary TEXT,started_at TEXT,finished_at TEXT,outcome TEXT)")
    db.execute("INSERT INTO idle_activity_summaries VALUES (?,?,?,?,?,?,?,?,?)", (
        "a", "e", "bot", 1, "model", "旧记录", "2026-01-01", "2026-01-01", "completed"))
    scheduler = TimeScheduler(db)
    assert scheduler.recent_idle_activities("bot") == ["旧记录"]
    db.close()
