import asyncio
import json
import shutil
import sqlite3
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from echoloom import tool_policy
from echoloom.contacts import ContactIdentity
from echoloom.event_book import EventBook
from echoloom.memory_store import MemoryStore
from echoloom.tool_timing import adk_function_tool
from echoloom.toolsets import MediaToolset, MemoryToolset, ReplyToolset, WorkspaceToolset
from echoloom.trace_dashboard import ASSETS, _tool_names, _transcript, create_trace_dashboard
from echoloom.wechat_tools import WeChatTools


def test_dashboard_trace_filters_and_live_tool_switch(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    records = [
        {"record": "model_request", "session_id": "s1", "conversation_id": "wxid_a",
         "timestamp": "2026-11-01T00:00:00+00:00", "contents": [{"role": "model", "parts": [
             {"tool_call": {"name": "read_gateway_status", "args": {}}}]}]},
        {"session_id": "s1", "conversation_id": "wxid_a", "author": "model",
         "timestamp": 1793491201, "content": [{"tool_call": {
             "name": "read_gateway_status", "args": {}}}]},
        {"session_id": "s2", "conversation_id": "wxid_b", "author": "model",
         "timestamp": 1793491202, "content": [{"text": "另一会话"}]},
    ]
    (tmp_path / "trace.jsonl").write_text("\n".join(json.dumps(r) for r in records), encoding="utf-8")
    app = create_trace_dashboard(tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        assert client.get("/").status_code == 200
        assert client.get("/health").json() == {"status": "ready"}
        overview = client.get("/api/overview").json()
        assert len(overview["conversations"]) == 2
        status_tool = next(t for t in overview["tools"] if t["name"] == "read_gateway_status")
        assert status_tool["calls"] == 1 and status_tool["enabled"] is True
        calls = client.get("/api/records", params={"kind": "tools"}).json()
        assert calls["total"] == 1  # historical calls inside a request aren't counted
        assert client.get("/api/records", params={"conversation": "wxid_b"}).json()["total"] == 1
        transcript = client.get("/api/transcript", params={"conversation": "wxid_a"}).json()
        assert [entry["kind"] for entry in transcript["entries"]] == ["request", "tool"]
        assert client.get("/api/calls").json()["total"] == 1
        assert client.post("/api/tools/read_gateway_status", json={"enabled": False}).status_code == 403
        headers = {"origin": "http://127.0.0.1:8767", "x-dashboard": "1"}
        assert client.post("/api/tools/unknown", json={"enabled": False}, headers=headers).status_code == 404
        assert client.post("/api/tools/read_gateway_status", json={"enabled": False}, headers=headers).status_code == 200

        invoked = []
        def read_gateway_status():
            invoked.append(True)
            return {"mode": "test"}

        response = asyncio.run(adk_function_tool(read_gateway_status).func())
        assert response["reason"] == "tool_disabled_by_operator" and not invoked
        assert client.post("/api/tools/read_gateway_status", json={"enabled": True}, headers=headers).status_code == 200
        assert asyncio.run(adk_function_tool(read_gateway_status).func())["mode"] == "test"
        assert invoked
        assert client.get("/", headers={"host": "evil.example"}).status_code == 403
        exported = client.get("/api/records.jsonl", params={"conversation": "wxid_a"})
        assert exported.status_code == 200 and len(exported.text.splitlines()) == 2
        assert "_file" not in exported.text


def test_transcript_pairs_tools_without_confusing_history_and_delivery():
    rows = [
        {"session_id": "s", "conversation_id": "wxid_a", "author": "user", "timestamp": 1,
         "content": [{"text": "【历史会话内容开始】\n旧内容 *{旧消息}*\n"
                     "本轮聊天记录：\n[消息ID：m1\nwxid_a | 小明：*{新消息}*\n"
                     "引用快照：*{不是新消息}*]"}]},
        {"session_id": "s", "conversation_id": "wxid_a", "author": "agent", "timestamp": 2,
         "content": [{"tool_call": {"name": "commit_agent_turn", "args": {"reply_text": "你好"}}}]},
        {"session_id": "s", "conversation_id": "wxid_a", "author": "agent", "timestamp": 3,
         "content": [{"tool_result": {"name": "commit_agent_turn", "result": {
             "status": "completed", "reply": {"status": "queued", "delivered": False}}}}]},
    ]
    entries = _transcript(rows)
    assert entries[0]["messages"] == [{"sender": "wxid_a | 小明", "text": "新消息"}]
    assert len(entries) == 2
    assert entries[1]["args"]["reply_text"] == "你好"
    assert entries[1]["result"]["reply"] == {"status": "queued", "delivered": False}
    assert entries[1]["result_time"] != entries[1]["time"]


def test_overview_lists_all_11_wechat_tools_via_static_tool_names(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    expected_wechat_tools = (
        set(ReplyToolset.TOOL_NAMES)
        | set(MediaToolset.TOOL_NAMES)
        | set(MemoryToolset.TOOL_NAMES)
    )
    assert len(expected_wechat_tools) == 13
    assert expected_wechat_tools == set(WeChatTools.TOOL_NAMES)
    expected_workspace_tools = set(WorkspaceToolset.TOOL_NAMES)
    assert len(expected_workspace_tools) == 4

    class StaticOnlyToolset:
        TOOL_NAMES = ("alpha_tool", "beta_tool")

        def agent_tools(self):
            raise AssertionError("should not instantiate or call agent_tools when TOOL_NAMES exists")

    class FallbackToolset:
        def agent_tools(self):
            return [type("Tool", (), {"name": "fallback_tool"})()]

    assert _tool_names(StaticOnlyToolset) == {"alpha_tool", "beta_tool"}
    assert _tool_names(FallbackToolset) == {"fallback_tool"}

    app = create_trace_dashboard(tmp_path)
    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        overview = client.get("/api/overview").json()
        by_name = {item["name"]: item for item in overview["tools"]}
        assert expected_wechat_tools.issubset(by_name)
        assert expected_workspace_tools.issubset(by_name)
        for name in expected_wechat_tools:
            assert by_name[name]["main"] is True
            assert by_name[name]["idle"] is True


def test_dashboard_memory_workbench_end_to_end(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    memory_db = tmp_path / "agent-memory.sqlite3"
    episode_db = tmp_path / "episode-memory.sqlite3"
    store = MemoryStore(memory_db)
    event_book = EventBook(store)

    stored_msg = store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_alice",
        sender_id="wxid_alice",
        raw_envelope={"msglist": [{"msgsvrid": 1001}]},
        body_text="我对芒果过敏，以后点甜品别放芒果。",
    )
    fact_id = store.remember_fact(
        account_id="wxid_bot",
        fact_key="alice_allergy",
        content="Alice 对芒果过敏",
        source_message_id=stored_msg.message_id,
    )
    note_id = store.add_note(
        account_id="wxid_bot",
        message_id=stored_msg.message_id,
        content="饮食禁忌线索",
    )
    entity_id = store.upsert_entity(
        account_id="wxid_bot",
        term="小橘",
        explanation="Alice 养的橘猫",
    )
    event_res = event_book.add(
        account_id="wxid_bot",
        priority="ordinary",
        content="周五和 Alice 约晚饭",
        exit_condition="周五晚饭结束后删除",
    )
    stale_at = (datetime.now(UTC) - timedelta(hours=80)).isoformat()
    with store.transaction():
        store.db.execute(
            "UPDATE event_book SET updated_at=? WHERE event_id=?",
            (stale_at, event_res["event_id"]),
        )
    store.close()

    app = create_trace_dashboard(
        tmp_path,
        memory_db_path=memory_db,
        episode_db_path=episode_db,
    )
    headers = {"origin": "http://127.0.0.1:8767", "x-dashboard": "1"}

    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        # Static asset & CSS fix verification
        assert client.get("/assets/memory.js").status_code == 200
        css_text = client.get("/assets/style.css").text
        assert "+@media" not in css_text

        # Summary & missing episode DB graceful degradation
        summary = client.get("/api/memory/summary").json()
        assert summary["account_id"] == "wxid_bot"
        assert summary["counts"]["facts_total"] == 1
        assert summary["counts"]["facts_in_index"] == 1
        assert summary["counts"]["events_stale_72h"] == 1
        assert summary["counts"]["entities_total"] == 1
        assert summary["counts"]["annotations_total"] == 1
        assert summary["counts"]["episodes_available"] is False
        assert client.get("/api/memory/episodes").json()["available"] is False

        # Local mutation guard
        assert client.post(
            "/api/senders/tags",
            json={"account_id": "wxid_bot", "sender_id": "wxid_alice", "action": "add", "tags": ["好友"]},
        ).status_code == 403

        # Senders profile: tags + sender memories
        tag_res = client.post(
            "/api/senders/tags",
            json={"account_id": "wxid_bot", "sender_id": "wxid_alice", "action": "add", "tags": ["唯一主人"]},
            headers=headers,
        )
        assert tag_res.status_code == 200
        assert tag_res.json()["tags"] == ["唯一主人"]

        mem_res = client.post(
            "/api/senders/memories",
            json={
                "account_id": "wxid_bot",
                "sender_id": "wxid_alice",
                "topic": "偏好",
                "content": "喜欢无糖美式",
            },
            headers=headers,
        )
        assert mem_res.status_code == 200
        memory_id = mem_res.json()["memory_id"]

        senders = client.get("/api/senders").json()["senders"]
        alice = next(item for item in senders if item["sender_id"] == "wxid_alice")
        assert alice["tags"] == ["唯一主人"]
        assert alice["memory_count"] == 1
        assert alice["memories"][0]["memory_id"] == memory_id

        assert client.delete(
            f"/api/senders/memories/{memory_id}",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200

        # User facts: list with provenance + versioned update + delete
        facts = client.get("/api/memory/facts", params={"account_id": "wxid_bot"}).json()["facts"]
        assert len(facts) == 1
        assert facts[0]["fact_id"] == fact_id
        assert facts[0]["in_index"] is True
        assert facts[0]["index_rank"] == 1
        assert "芒果" in facts[0]["source"]["body_text"]

        recents = client.get("/api/memory/recent-messages", params={"account_id": "wxid_bot"}).json()
        assert recents["messages"][0]["message_id"] == stored_msg.message_id

        updated_fact = client.post(
            "/api/memory/facts",
            json={
                "account_id": "wxid_bot",
                "fact_key": "alice_allergy",
                "content": "Alice 对芒果和菠萝过敏",
                "source_message_id": stored_msg.message_id,
            },
            headers=headers,
        )
        assert updated_fact.status_code == 200
        new_fact_id = updated_fact.json()["fact_id"]
        facts_after = client.get("/api/memory/facts", params={"account_id": "wxid_bot"}).json()["facts"]
        assert facts_after[0]["fact_id"] == new_fact_id
        assert facts_after[0]["version"] == 2
        assert facts_after[0]["author_id"] == "dashboard-user"
        assert client.delete(
            f"/api/memory/facts/{new_fact_id}",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200

        # Event book: list stale + create + update + delete
        events = client.get("/api/memory/events", params={"account_id": "wxid_bot"}).json()
        assert events["events"][0]["stale_72h"] is True
        created_evt = client.post(
            "/api/memory/events",
            json={
                "account_id": "wxid_bot",
                "priority": "high",
                "content": "下周一提交项目评审材料",
                "exit_condition": "评审材料提交后删除",
            },
            headers=headers,
        )
        assert created_evt.status_code == 200
        high_event_id = created_evt.json()["event_id"]
        patched_evt = client.patch(
            f"/api/memory/events/{high_event_id}",
            json={
                "account_id": "wxid_bot",
                "content": "下周二提交项目评审材料",
                "exit_condition": "评审材料提交后删除",
            },
            headers=headers,
        )
        assert patched_evt.status_code == 200
        assert client.delete(
            f"/api/memory/events/{high_event_id}",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200

        # Entity dictionary: list + update + delete
        entities = client.get("/api/memory/entities", params={"account_id": "wxid_bot"}).json()["entities"]
        assert entities[0]["entity_id"] == entity_id
        ent_up = client.post(
            "/api/memory/entities",
            json={"account_id": "wxid_bot", "term": "小橘", "explanation": "Alice 养的两岁橘猫"},
            headers=headers,
        )
        assert ent_up.status_code == 200
        new_entity_id = ent_up.json()["entity_id"]
        assert client.delete(
            f"/api/memory/entities/{new_entity_id}",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200

        # Annotations: list + patch + delete
        notes = client.get("/api/memory/annotations", params={"account_id": "wxid_bot"}).json()["annotations"]
        assert notes[0]["note_id"] == note_id
        assert notes[0]["editable"] is True
        assert client.patch(
            f"/api/memory/annotations/{note_id}",
            json={"account_id": "wxid_bot", "content": "饮食禁忌（已复核）"},
            headers=headers,
        ).json()["version"] == 2
        assert client.delete(
            f"/api/memory/annotations/{note_id}",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200

        # Episode vector memory DB: inspect + bloated flag + cascade delete
        with sqlite3.connect(episode_db) as ep_db:
            ep_db.executescript(
                "CREATE TABLE episodes ("
                "id TEXT PRIMARY KEY, account_id TEXT NOT NULL, conversation_id TEXT NOT NULL, "
                "summary TEXT NOT NULL, updated_at TEXT NOT NULL, model TEXT NOT NULL, vector BLOB NOT NULL);"
                "CREATE TABLE episode_sources ("
                "episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE, "
                "message_id TEXT NOT NULL, PRIMARY KEY (episode_id, message_id));"
            )
            now_iso = datetime.now(UTC).isoformat()
            for idx in range(12):
                ep_db.execute(
                    "INSERT INTO episodes VALUES (?,?,?,?,?,?,?)",
                    (f"ep_{idx}", "wxid_bot", "wxid_alice", f"事件摘要 {idx}", now_iso, "text-embedding", b"\x00\x00"),
                )
            ep_db.execute("INSERT INTO episode_sources VALUES (?,?)", ("ep_0", stored_msg.message_id))

        ep_resp = client.get("/api/memory/episodes", params={"account_id": "wxid_bot"}).json()
        assert ep_resp["available"] is True
        assert ep_resp["conversations"][0]["bloated"] is True
        ep0 = next(item for item in ep_resp["episodes"] if item["id"] == "ep_0")
        assert ep0["sources"][0]["message_id"] == stored_msg.message_id

        assert client.delete(
            "/api/memory/episodes/ep_0",
            params={"account_id": "wxid_bot"},
            headers=headers,
        ).status_code == 200
        with sqlite3.connect(episode_db) as ep_db:
            assert ep_db.execute("SELECT COUNT(*) FROM episode_sources WHERE episode_id='ep_0'").fetchone()[0] == 0


def test_dashboard_memory_search_past_300_bloat_per_model_and_sqlite_busy(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    memory_db = tmp_path / "agent-memory.sqlite3"
    episode_db = tmp_path / "episode-memory.sqlite3"
    store = MemoryStore(memory_db)
    stored_msg = store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_alice",
        sender_id="wxid_alice",
        raw_envelope={"msglist": [{"msgsvrid": 2001}]},
        body_text="测试消息正文",
    )
    old_note = store.add_note(
        account_id="wxid_bot",
        message_id=stored_msg.message_id,
        content="历史批注 needle_alpha",
    )
    store.db.execute("PRAGMA synchronous=OFF")
    for idx in range(305):
        store.add_note(
            account_id="wxid_bot",
            message_id=stored_msg.message_id,
            content=f"较新批注 {idx}",
        )
    with store.transaction():
        store.db.execute(
            "UPDATE annotations SET updated_at='2020-01-01T00:00:00+00:00' WHERE note_id=?",
            (old_note,),
        )
    store.db.execute("PRAGMA synchronous=NORMAL")
    store.close()

    with sqlite3.connect(episode_db) as ep_db:
        ep_db.executescript(
            "CREATE TABLE episodes ("
            "id TEXT PRIMARY KEY, account_id TEXT NOT NULL, conversation_id TEXT NOT NULL, "
            "summary TEXT NOT NULL, updated_at TEXT NOT NULL, model TEXT NOT NULL, vector BLOB NOT NULL);"
            "CREATE TABLE episode_sources ("
            "episode_id TEXT NOT NULL REFERENCES episodes(id) ON DELETE CASCADE, "
            "message_id TEXT NOT NULL, PRIMARY KEY (episode_id, message_id));"
        )
        rows = [
            ("ep_oldest", "wxid_bot", "wxid_old", "陈年事件 needle_beta", "2020-01-01T00:00:00+00:00", "m-a", b"\x00")
        ]
        rows.extend(
            (f"ep_new_{idx}", "wxid_bot", "wxid_recent", f"常规摘要 {idx}", f"2026-06-01T00:{idx // 60:02d}:{idx % 60:02d}+00:00", f"m-{idx // 10}", b"\x00")
            for idx in range(305)
        )
        for idx in range(6):
            rows.append((f"ep_split_a_{idx}", "wxid_bot", "wxid_split", f"模型A {idx}", "2026-06-02T00:00:00+00:00", "m-a", b"\x00"))
            rows.append((f"ep_split_b_{idx}", "wxid_bot", "wxid_split", f"模型B {idx}", "2026-06-02T00:00:00+00:00", "m-b", b"\x00"))
        ep_db.executemany("INSERT INTO episodes VALUES (?,?,?,?,?,?,?)", rows)

    app = create_trace_dashboard(
        tmp_path,
        memory_db_path=memory_db,
        episode_db_path=episode_db,
    )
    headers = {"origin": "http://127.0.0.1:8767", "x-dashboard": "1"}

    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        css_text = client.get("/assets/style.css").text
        assert "#export-jsonl{display:none}" in css_text
        assert ".header-button{display:none}" not in css_text

        unfiltered_notes = client.get("/api/memory/annotations", params={"account_id": "wxid_bot"}).json()["annotations"]
        assert len(unfiltered_notes) == 300
        assert all(item["note_id"] != old_note for item in unfiltered_notes)
        searched_notes = client.get(
            "/api/memory/annotations",
            params={"account_id": "wxid_bot", "q": "needle_alpha"},
        ).json()["annotations"]
        assert [item["note_id"] for item in searched_notes] == [old_note]

        unfiltered_eps = client.get("/api/memory/episodes", params={"account_id": "wxid_bot"}).json()["episodes"]
        assert len(unfiltered_eps) == 300
        assert all(item["id"] != "ep_oldest" for item in unfiltered_eps)
        searched_eps_res = client.get(
            "/api/memory/episodes",
            params={"account_id": "wxid_bot", "q": "needle_beta"},
        ).json()
        assert [item["id"] for item in searched_eps_res["episodes"]] == ["ep_oldest"]

        # wxid_split has 6 + 6 across two models -> below 12 per (conversation_id, model)
        split_conv = next(c for c in searched_eps_res["conversations"] if c["conversation_id"] == "wxid_split")
        assert split_conv["count"] == 12
        assert split_conv["bloated"] is False
        summary_counts = client.get("/api/memory/summary", params={"account_id": "wxid_bot"}).json()["counts"]
        assert summary_counts["episodes_bloated_chats"] == 0

        # Simulate SQLite busy error from domain layer -> HTTP 503 via sqlite_error_guard
        def _raise_locked(*_args, **_kwargs):
            raise sqlite3.OperationalError("database is locked")

        monkeypatch.setattr(MemoryStore, "remember_fact", _raise_locked)
        monkeypatch.setattr(MemoryStore, "set_human_sender_remark", _raise_locked)
        assert client.post(
            "/api/memory/facts",
            json={
                "account_id": "wxid_bot",
                "fact_key": "k",
                "content": "v",
                "source_message_id": stored_msg.message_id,
            },
            headers=headers,
        ).status_code == 503
        assert client.post(
            "/api/senders/remark",
            json={"account_id": "wxid_bot", "sender_id": "wxid_alice", "remark": "Alice"},
            headers=headers,
        ).status_code == 503


class _StubGateway:
    def __init__(self, contacts: dict[str, ContactIdentity | None]):
        self._contacts = contacts

    def resolve_contact(self, user_id: str) -> ContactIdentity | None:
        return self._contacts.get(user_id)


def test_overview_and_memory_summary_names_resolution_and_write_through(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    memory_db = tmp_path / "agent-memory.sqlite3"
    store = MemoryStore(memory_db)
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_owner",
        sender_id="wxid_owner",
        raw_envelope={"msglist": [{"msgsvrid": 3001}]},
        body_text="在吗",
    )
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_same",
        sender_id="wxid_same",
        raw_envelope={"msglist": [{"msgsvrid": 3002}]},
        body_text="hi",
    )
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_cached",
        sender_id="wxid_cached",
        raw_envelope={"msglist": [{"msgsvrid": 3003}]},
        body_text="离线消息",
    )
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="12345@chatroom",
        sender_id="wxid_owner",
        raw_envelope={"msglist": [{"msgsvrid": 3004}]},
        body_text="群里说一句",
    )
    store.set_human_sender_remark(account_id="wxid_bot", sender_id="wxid_owner", remark="主人")
    store.set_human_sender_remark(account_id="wxid_bot", sender_id="wxid_same", remark="小明")
    store.set_human_sender_remark(account_id="wxid_bot", sender_id="12345@chatroom", remark="研发群")
    store.upsert_contact_directory(
        account_id="wxid_bot",
        contact_id="wxid_cached",
        nickname="离线好友",
        source="hook",
    )
    store.close()

    trace_records = [
        {
            "session_id": "s1",
            "conversation_id": "wxid_trace_only",
            "author": "model",
            "timestamp": 1793491202,
            "content": [{"text": "hello"}],
        }
    ]
    (tmp_path / "trace.jsonl").write_text(
        "\n".join(json.dumps(r) for r in trace_records),
        encoding="utf-8",
    )

    gateway = _StubGateway(
        {
            "wxid_owner": ContactIdentity(display_name="A.E.I.O.U.", user_name="wxid_owner", nickname="A.E.I.O.U."),
            "wxid_same": ContactIdentity(display_name="小明", user_name="wxid_same", nickname="小明"),
            "wxid_cached": None,
            "wxid_trace_only": ContactIdentity(display_name="实时好友", user_name="wxid_trace_only", nickname="实时好友"),
            "12345@chatroom": None,
        }
    )
    app = create_trace_dashboard(tmp_path, gateway=gateway, memory_db_path=memory_db)
    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        overview = client.get("/api/overview").json()
        names = overview["names"]
        assert names["wxid_owner"] == "主人（A.E.I.O.U.）"
        assert names["wxid_same"] == "小明"
        assert names["wxid_cached"] == "离线好友"
        assert names["wxid_trace_only"] == "实时好友"
        assert names["12345@chatroom"] == "研发群"

        summary = client.get("/api/memory/summary", params={"account_id": "wxid_bot"}).json()
        assert summary["names"]["wxid_owner"] == "主人（A.E.I.O.U.）"
        assert summary["names"]["wxid_cached"] == "离线好友"
        assert summary["names"]["12345@chatroom"] == "研发群"

    verify_store = MemoryStore(memory_db)
    cached_map = verify_store.contact_directory_map(account_id="wxid_bot")
    verify_store.close()
    assert cached_map["wxid_owner"]["nickname"] == "A.E.I.O.U."
    assert cached_map["wxid_trace_only"]["nickname"] == "实时好友"


def test_senders_lists_chatrooms_and_saves_group_remark(tmp_path, monkeypatch):
    monkeypatch.setattr(tool_policy, "POLICY_PATH", tmp_path / "controls.sqlite3")
    memory_db = tmp_path / "agent-memory.sqlite3"
    store = MemoryStore(memory_db)
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="wxid_alice",
        sender_id="wxid_alice",
        raw_envelope={"msglist": [{"msgsvrid": 4001}]},
        body_text="私聊消息",
    )
    store.archive_message(
        account_id="wxid_bot",
        conversation_id="99999@chatroom",
        sender_id="wxid_bob",
        raw_envelope={"msglist": [{"msgsvrid": 4002}]},
        body_text="群聊消息",
    )
    store.upsert_contact_directory(
        account_id="wxid_bot",
        contact_id="wxid_bob",
        nickname="缓存鲍勃",
        source="hook",
    )
    store.close()

    gateway = _StubGateway(
        {
            "wxid_alice": ContactIdentity(display_name="爱丽丝", user_name="wxid_alice", nickname="爱丽丝"),
            "wxid_bob": None,
            "99999@chatroom": None,
        }
    )
    app = create_trace_dashboard(tmp_path, gateway=gateway, memory_db_path=memory_db)
    headers = {"origin": "http://127.0.0.1:8767", "x-dashboard": "1"}
    with TestClient(app, base_url="http://127.0.0.1:8767") as client:
        rows = client.get("/api/senders").json()["senders"]
        by_id = {row["sender_id"]: row for row in rows}
        assert by_id["wxid_alice"]["is_group"] is False
        assert by_id["wxid_alice"]["nickname"] == "爱丽丝"
        assert by_id["wxid_bob"]["is_group"] is False
        assert by_id["wxid_bob"]["nickname"] == "缓存鲍勃"

        group = by_id["99999@chatroom"]
        assert group["is_group"] is True
        assert group["nickname"] == ""
        assert group["remark"] == ""
        assert group["tags"] == []
        assert group["memories"] == []
        assert group["memory_count"] == 0
        assert group["bloated"] is False
        assert group["message_count"] == 1

        res = client.post(
            "/api/senders/remark",
            json={"account_id": "wxid_bot", "sender_id": "99999@chatroom", "remark": "后端群"},
            headers=headers,
        )
        assert res.status_code == 200
        rows_after = client.get("/api/senders").json()["senders"]
        group_after = next(row for row in rows_after if row["sender_id"] == "99999@chatroom")
        assert group_after["remark"] == "后端群"
        assert group_after["is_group"] is True

        overview = client.get("/api/overview").json()
        assert overview["names"]["99999@chatroom"] == "后端群"


def test_transcript_parses_old_and_new_sender_formats_and_ignores_quote_body():
    rows = [
        {
            "session_id": "s1",
            "conversation_id": "wxid_a",
            "author": "user",
            "timestamp": 1,
            "content": [
                {
                    "text": (
                        "【历史会话内容开始】\n"
                        "旧发送人：*{历史旧消息}*\n"
                        "本轮聊天记录：\n"
                        "[消息ID：m1；2026-11-27 20:00\n"
                        "wxid_a | 主人（A.E.I.O.U.）：*{旧格式消息}*\n"
                        "引用快照：*{旧引用快照}*]\n"
                        "[消息ID：m2；2026-11-27 20:01\n"
                        "主人（A.E.I.O.U.）：*{新格式第一行\n新格式第二行}*\n"
                        "引用消息（未匹配到消息库原记录）：\n"
                        "引用发送人 ID：wxid_b\n"
                        "引用来源聊天 ID：wxid_a\n"
                        "引用正文：*{被引用正文不应拆成气泡}*]\n"
                        "[消息ID：m3；2026-11-27 20:02\n"
                        "Agent：*{助手历史回复}*]"
                    )
                }
            ],
        }
    ]
    entries = _transcript(rows)
    assert entries[0]["messages"] == [
        {"sender": "wxid_a | 主人（A.E.I.O.U.）", "text": "旧格式消息"},
        {"sender": "主人（A.E.I.O.U.）", "text": "新格式第一行\n新格式第二行"},
        {"sender": "Agent", "text": "助手历史回复"},
    ]


def test_dashboard_static_js_syntax_and_csp():
    index_html = (ASSETS / "index.html").read_text(encoding="utf-8")
    assert "style=" not in index_html
    assert "onclick=" not in index_html
    node_bin = shutil.which("node")
    if node_bin is not None:
        for script_name in ("app.js", "memory.js"):
            script_path = Path(ASSETS) / script_name
            proc = subprocess.run(
                [node_bin, "--check", str(script_path)],
                capture_output=True,
                text=True,
                check=False,
            )
            assert proc.returncode == 0, f"{script_name} syntax check failed: {proc.stderr}"


