from datetime import UTC, datetime, timedelta

from echoloom.cross_session import CrossSessionService
from echoloom.memory_store import MemoryStore


def _archive(store, account, conversation, sender, body):
    raw = {"fromid": conversation, "toid": account, "msg": body}
    return store.archive_message(
        account_id=account,
        conversation_id=conversation,
        sender_id=sender,
        raw_envelope=raw,
        body_text=body,
    )


def test_read_only_sql_is_bounded_and_account_scoped(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _archive(store, "bot", "wxid_friend123", "wxid_friend123", "hello")
    service = CrossSessionService(store)
    result = service.query_sql(
        account_id="bot",
        query="SELECT message_id FROM source_messages WHERE account_id=? LIMIT ?",
        parameters=(1,),
    )
    assert result["status"] == "completed"
    assert len(result["rows"]) == 1
    assert result["rows"][0]["message_id"]
    assert (
        service.query_sql(
            account_id="bot",
            query="SELECT message_id FROM source_messages WHERE account_id=? OR 1=1",
        )["status"]
        == "rejected"
    )
    assert (
        service.query_sql(
            account_id="bot",
            query="SELECT raw_json FROM source_messages WHERE account_id=?",
        )["status"]
        == "rejected"
    )
    store.close()


def test_message_bodies_are_braced_in_search_and_sql_results(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    body = "hello {close}*"
    _archive(store, "bot", "wxid_friend123", "wxid_friend123", body)
    service = CrossSessionService(store)

    searched = service.search_messages(account_id="bot", query="hello", limit=5)
    assert searched[0]["body_text"] == "*{hello ｛close｝*}*"
    queried = service.query_sql(
        account_id="bot",
        query="SELECT body_text FROM source_messages WHERE account_id=? LIMIT ?",
        parameters=(1,),
    )
    assert queried["rows"][0]["body_text"] == "*{hello ｛close｝*}*"
    store.close()


def test_long_search_result_keeps_complete_message_brackets(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _archive(store, "bot", "wxid_friend123", "wxid_friend123", "needle" + "x" * 2100 + "{tail}")
    service = CrossSessionService(store)

    body = service.search_messages(account_id="bot", query="needle", limit=5)[0]["body_text"]
    assert body.startswith("*{…")
    assert body.endswith("}*")
    assert "｛tail｝" in body
    assert "{" not in body[2:-2] and "}" not in body[2:-2]
    store.close()


def test_message_id_query_returns_exact_chat_window_and_current_metadata(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    chat = "wxid_friend123"
    messages = [
        _archive(store, "bot", chat, chat, f"窗口消息 {index}")
        for index in range(5)
    ]
    now = datetime.now(UTC)
    for index, message in enumerate(messages):
        store.db.execute(
            "UPDATE source_messages SET captured_at=? WHERE message_id=?",
            ((now - timedelta(minutes=5 - index)).isoformat(), message.message_id),
        )
    store.db.commit()
    service = CrossSessionService(store)

    result = service.query_messages(
        account_id="bot",
        message_id=messages[2].message_id,
        before=1,
        after=2,
    )

    assert result["status"] == "completed"
    assert [row["message_id"] for row in result["messages"]] == [
        item.message_id for item in messages[1:5]
    ]
    assert result["messages"][1]["body_text"] == "*{窗口消息 2}*"
    assert result["bytes_used"] <= service.byte_limit
    assert result["returned_count"] == 4
    store.close()


def test_message_query_truncates_only_when_needed_and_reports_it(tmp_path):
    store = MemoryStore(tmp_path / "long-query.sqlite3")
    chat = "wxid_friend123"
    body = "起点" + "文" * 2500 + "终点"
    message = _archive(store, "bot", chat, chat, body)
    service = CrossSessionService(store)
    full = service.query_messages(account_id="bot", message_id=message.message_id)
    assert full["messages"][0]["body_text"] == f"*{{{body}}}*"
    assert not full["truncated"]
    limited = service.query_messages(account_id="bot", message_id=message.message_id,
                                     byte_limit=1200)
    assert limited["truncated"] and limited["bytes_used"] <= 1200
    assert "较早正文已截断" in limited["messages"][0]["body_text"]
    assert "终点" in limited["messages"][0]["body_text"]
    store.close()


def test_time_query_is_account_scoped_and_respects_chat_or_global_scope(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    current_chat = "wxid_friend123"
    other_chat = "wxid_friend456"
    in_window = _archive(store, "bot", current_chat, current_chat, "窗口内提到转发")
    other_window = _archive(store, "bot", other_chat, other_chat, "另一个聊天转发")
    old = _archive(store, "bot", other_chat, other_chat, "很久以前转发")
    now = datetime.now(UTC)
    for message, age in ((in_window, 10), (other_window, 20), (old, 180)):
        store.db.execute(
            "UPDATE source_messages SET captured_at=? WHERE message_id=?",
            ((now - timedelta(minutes=age)).isoformat(), message.message_id),
        )
    store.db.commit()
    service = CrossSessionService(store)
    start = (now - timedelta(hours=1)).isoformat()
    end = now.isoformat()

    global_result = service.query_messages(
        account_id="bot", query="转发", all_conversations=True,
        start_at=start, end_at=end, limit=20,
    )
    local_result = service.query_messages(
        account_id="bot", query="转发", current_conversation_id=current_chat,
        start_at=start, end_at=end, limit=20,
    )

    assert {row["message_id"] for row in global_result["messages"]} == {
        in_window.message_id, other_window.message_id,
    }
    assert [row["message_id"] for row in local_result["messages"]] == [in_window.message_id]
    assert old.message_id not in {row["message_id"] for row in global_result["messages"]}
    assert global_result["matched_count"] == 2
    store.close()


def test_query_can_filter_archived_hook_message_type(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    store.archive_message(
        account_id="bot",
        conversation_id="wxid_friend123",
        sender_id="wxid_friend123",
        raw_envelope={"fromid": "wxid_friend123", "toid": "bot", "msg": "表情", "msgtype": 47},
        source_record={"msgtype": 47},
        body_text="图片/表情消息",
    )
    store.archive_message(
        account_id="bot",
        conversation_id="wxid_friend123",
        sender_id="wxid_friend123",
        raw_envelope={"fromid": "wxid_friend123", "toid": "bot", "msg": "文本", "msgtype": 1},
        source_record={"msgtype": 1},
        body_text="普通文本",
    )
    service = CrossSessionService(store)

    result = service.query_messages(
        account_id="bot", all_conversations=True, message_type=47,
    )

    assert result["matched_count"] == result["returned_count"] == 1
    assert result["messages"][0]["message_type"] == 47
    store.close()


def test_pair_dispatch_soft_warning_then_hard_cooldown_is_bidirectional(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    service = CrossSessionService(store)
    now = datetime(2026, 10, 30, 1, 0, tzinfo=UTC)
    for _ in range(4):
        assert (
            service._rate_limit(
                account_id="bot", source="wxid_a123", target="room@chatroom", now=now
            )
            is None
        )
    assert (
        service._rate_limit(account_id="bot", source="room@chatroom", target="wxid_a123", now=now)[
            "status"
        ]
        == "soft_warning"
    )
    for _ in range(2):
        assert (
            service._rate_limit(
                account_id="bot",
                source="wxid_a123",
                target="room@chatroom",
                now=now + timedelta(seconds=1),
            )
            is not None
        )
    blocked = service._rate_limit(
        account_id="bot", source="room@chatroom", target="wxid_a123", now=now + timedelta(seconds=2)
    )
    assert blocked["status"] == "rate_limited"
    assert blocked["level"] == "hard_40m"
    store.close()


def test_dispatch_runs_target_turn_and_returns_optional_result(tmp_path):
    import asyncio

    store = MemoryStore(tmp_path / "memory.sqlite3")
    _archive(store, "bot", "wxid_source123", "wxid_source123", "source")
    _archive(store, "bot", "wxid_target123", "wxid_target123", "target context")
    calls = []

    async def dispatch(window, task_id):
        calls.append((
            window.conversation_id,
            window.history_turns[0].text,
            "\n".join(window.model_history_records()),
            task_id,
        ))
        return "target result"

    service = CrossSessionService(store, dispatch_reply=dispatch)
    result = asyncio.run(
        service.dispatch(
            account_id="bot",
            source_conversation_id="wxid_source123",
            target_conversation_id="wxid_target123",
            task="请查看上下文",
            return_result=True,
        )
    )
    assert result["status"] == "completed"
    assert result["result"] == "target result"
    assert calls[0][0] == "wxid_target123"
    assert calls[0][1] == "target context"
    assert calls[0][2].count("*{target context}*") == 1
    store.close()


def test_restart_interrupts_leftover_running_delegation_tasks(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _archive(store, "bot", "wxid_source123", "wxid_source123", "source")
    CrossSessionService(store)  # first process creates the schema
    store.db.execute(
        "INSERT INTO cross_session_tasks VALUES "
        "('task_left','bot','wxid_source123','wxid_target123','待办','running',NULL,"
        "'2026-10-30T01:00:00+00:00',NULL)"
    )
    store.db.commit()

    restarted = CrossSessionService(store)
    row = restarted.db.execute(
        "SELECT state,result_text,finished_at FROM cross_session_tasks "
        "WHERE task_id='task_left'"
    ).fetchone()
    assert row["state"] == "interrupted"
    assert row["result_text"].startswith("服务重启")
    assert row["finished_at"] is not None

    # A second restart must not rewrite already-interrupted rows.
    CrossSessionService(store)
    row_again = restarted.db.execute(
        "SELECT state,finished_at FROM cross_session_tasks WHERE task_id='task_left'"
    ).fetchone()
    assert row_again["state"] == "interrupted"
    assert row_again["finished_at"] == row["finished_at"]
    store.close()


def test_query_messages_supports_space_separated_keywords(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _archive(store, "bot", "room@chatroom", "member", "近期有降价风险，特别是内存和固态硬盘")
    _archive(store, "bot", "room@chatroom", "member", "今天只聊天气")
    service = CrossSessionService(store)

    res = service.query_messages(
        account_id="bot",
        conversation_id="room@chatroom",
        query="降价 内存 固态",
    )
    assert res["status"] == "completed"
    assert res["matched_count"] == 1
    assert "内存" in res["messages"][0]["body_text"]
    store.close()
