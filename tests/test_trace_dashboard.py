import asyncio
import json

from fastapi.testclient import TestClient

from echoloom import tool_policy
from echoloom.tool_timing import adk_function_tool
from echoloom.toolsets import MediaToolset, MemoryToolset, ReplyToolset, WorkspaceToolset
from echoloom.trace_dashboard import _tool_names, _transcript, create_trace_dashboard
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
