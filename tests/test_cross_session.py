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


def test_phase2_short_refs_name_disambiguation_dispatch_and_sql(tmp_path):
    import asyncio

    store = MemoryStore(tmp_path / "memory.sqlite3")
    m_g1 = _archive(store, "bot", "room_alpha@chatroom", "wxid_alice", "周五开会讨论架构")
    _archive(store, "bot", "room_alpha@chatroom", "wxid_bob", "收到架构议程")
    _archive(store, "bot", "room_beta@chatroom", "wxid_charlie", "周末聚餐安排")
    _archive(store, "bot", "wxid_dm_only", "wxid_dm_only", "私聊确认细节")

    # Populate remarks + contact_directory for safe unique & ambiguous name resolution
    store.set_human_sender_remark(account_id="bot", sender_id="room_alpha@chatroom", remark="研发群")
    store.set_human_sender_remark(account_id="bot", sender_id="wxid_alice", remark="唯一阿丽")
    store.upsert_contact_directory(account_id="bot", contact_id="room_alpha@chatroom", nickname="共享群名")
    store.upsert_contact_directory(account_id="bot", contact_id="room_beta@chatroom", nickname="共享群名")
    store.upsert_contact_directory(account_id="bot", contact_id="wxid_bob", nickname="重名小王")
    store.upsert_contact_directory(account_id="bot", contact_id="wxid_charlie", nickname="重名小王")

    refs = store.short_refs_map(
        account_id="bot",
        items=[
            ("message", m_g1.message_id),
            ("chat", "room_alpha@chatroom"),
            ("chat", "room_beta@chatroom"),
            ("user", "wxid_alice"),
            ("user", "wxid_bob"),
            ("user", "wxid_charlie"),
            ("user", "wxid_dm_only"),
        ],
    )
    m1_ref = refs[("message", m_g1.message_id)]
    g1_ref = refs[("chat", "room_alpha@chatroom")]
    u_alice = refs[("user", "wxid_alice")]
    u_bob = refs[("user", "wxid_bob")]
    u_charlie = refs[("user", "wxid_charlie")]
    u_dm = refs[("user", "wxid_dm_only")]

    dispatched_targets: list[str] = []

    async def fake_dispatch(window, task_id):
        dispatched_targets.append(window.conversation_id)
        return "done"

    service = CrossSessionService(store, dispatch_reply=fake_dispatch)
    try:
        # 1) list_conversations attaches short_ref and supports querying by G#
        chats = service.list_conversations(account_id="bot", limit=10)
        by_cid = {row["conversation_id"]: row for row in chats}
        assert by_cid["room_alpha@chatroom"]["short_ref"] == g1_ref
        assert by_cid["wxid_dm_only"]["short_ref"] == u_dm
        by_short_query = service.list_conversations(account_id="bot", query=g1_ref.lower(), limit=10)
        assert any(row["conversation_id"] == "room_alpha@chatroom" for row in by_short_query)

        # 2) query_messages by M# (case-insensitive) and enriched short_ref/sender_ref/conversation_ref
        by_msg_ref = service.query_messages(account_id="bot", message_id=m1_ref.lower(), before=0, after=0)
        assert by_msg_ref["status"] == "completed"
        row = by_msg_ref["messages"][0]
        assert row["message_id"] == m_g1.message_id
        assert row["short_ref"] == m1_ref
        assert row["sender_ref"] == u_alice
        assert row["conversation_ref"] == g1_ref
        assert service.query_messages(account_id="bot", message_id="M999")["status"] == "unavailable"
        assert service.query_messages(account_id="bot", message_id=u_alice)["status"] == "unavailable"

        # 3) query_messages by G# / U# / unique name / ambiguous name for conversation_id
        by_g_ref = service.query_messages(account_id="bot", conversation_id=g1_ref.lower())
        assert by_g_ref["status"] == "completed" and by_g_ref["matched_count"] == 2
        by_chat_name = service.query_messages(account_id="bot", conversation_id="研发群")
        assert by_chat_name["status"] == "completed" and by_chat_name["scope"] == "room_alpha@chatroom"
        amb_chat = service.query_messages(account_id="bot", conversation_id="共享群名")
        assert amb_chat["status"] == "ambiguous"
        assert amb_chat["reason"] == "conversation_name_matches_multiple_chats"
        assert {c["conversation_id"] for c in amb_chat["candidates"]} == {
            "room_alpha@chatroom", "room_beta@chatroom",
        }
        assert all(c.get("short_ref") for c in amb_chat["candidates"])
        assert service.query_messages(account_id="bot", conversation_id="G999")["status"] == "unavailable"

        # 4) query_messages by U# / unique name / ambiguous name for sender_id
        by_u_ref = service.query_messages(
            account_id="bot", all_conversations=True, sender_id=u_alice.lower(),
        )
        assert by_u_ref["status"] == "completed" and by_u_ref["matched_count"] == 1
        assert by_u_ref["messages"][0]["sender_id"] == "wxid_alice"
        by_sender_name = service.query_messages(
            account_id="bot", all_conversations=True, sender_id="唯一阿丽",
        )
        assert by_sender_name["status"] == "completed" and by_sender_name["matched_count"] == 1
        amb_sender = service.query_messages(
            account_id="bot", all_conversations=True, sender_id="重名小王",
        )
        assert amb_sender["status"] == "ambiguous"
        assert amb_sender["reason"] == "sender_name_matches_multiple_people"
        assert {c["sender_id"] for c in amb_sender["candidates"]} == {"wxid_bob", "wxid_charlie"}
        assert {c["short_ref"] for c in amb_sender["candidates"]} == {u_bob, u_charlie}
        assert service.query_messages(
            account_id="bot", all_conversations=True, sender_id="U999",
        )["status"] == "unavailable"

        # 5) dispatch resolves G# / U# target_conversation_id and rejects unknown G999
        disp = asyncio.run(
            service.dispatch(
                account_id="bot",
                source_conversation_id="wxid_dm_only",
                target_conversation_id=g1_ref.lower(),
                task="查看群内最新进度",
            )
        )
        assert disp["status"] == "completed"
        assert disp["target_conversation_id"] == "room_alpha@chatroom"
        assert dispatched_targets == ["room_alpha@chatroom"]

        bad_disp = asyncio.run(
            service.dispatch(
                account_id="bot",
                source_conversation_id="wxid_dm_only",
                target_conversation_id="G999",
                task="无效目标",
            )
        )
        assert bad_disp["status"] == "unavailable"

        # 6) query_sql allows read-only SELECT on short_refs
        sql_res = service.query_sql(
            account_id="bot",
            query="SELECT kind, short_ref, target_id FROM short_refs WHERE account_id=? AND short_ref=?",
            parameters=(g1_ref,),
        )
        assert sql_res["status"] == "completed"
        assert sql_res["rows"] == [
            {"kind": "chat", "short_ref": g1_ref, "target_id": "room_alpha@chatroom"}
        ]
    finally:
        store.close()
