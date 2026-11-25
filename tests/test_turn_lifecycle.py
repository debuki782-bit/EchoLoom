"""Small real queue/SDK/tool chains; HTTP is local MockTransport, never live WeChat."""
import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
import pytest
from google.genai import types
from test_openai_runtime import FakeAdkModel

from wechat_agent.contacts import ContactIdentity
from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.context_assembler import MessageContextAssembler
from wechat_agent.cross_session import CrossSessionService
from wechat_agent.cross_session_tools import CrossSessionToolset
from wechat_agent.event_book import EventBook
from wechat_agent.hook_contract import HookObservation
from wechat_agent.hook_outbound import HookTextSender
from wechat_agent.memory_store import MemoryStore
from wechat_agent.mock import SystemClock
from wechat_agent.models import ConversationKey
from wechat_agent.pilot import Pilot
from wechat_agent.runtime_contract import ToolContext
from wechat_agent.session_runtime import ScopedAgentRuntime
from wechat_agent.tool_timing import turn_deadline_monotonic
from wechat_agent.wechat_tools import (
    WeChatTools,
    active_conversation,
    memory_context,
    operation_receipt_ids,
    tool_turn_scope,
)

SOURCE = "source@chatroom"
TARGET = "target@chatroom"
PERSON = "wxid_target001"


def setup_chain(tmp_path, hook_reply=None):
    (tmp_path / "data").mkdir()
    store = MemoryStore(tmp_path / "memory.sqlite3")
    for chat in (SOURCE, TARGET):
        store.archive_message(account_id="bot", conversation_id=chat, sender_id=PERSON,
                              raw_envelope={"chat": chat}, body_text="先前消息")
    requests = []

    def hook(request):
        requests.append(request)
        return httpx.Response(200, json=hook_reply or {"ret": 0, "queued": True})

    sender = HookTextSender(
        frozenset(ConversationKey("bot", chat) for chat in (SOURCE, TARGET)),
        enabled=True, transport=httpx.MockTransport(hook),
    )
    tools = WeChatTools(tmp_path, "bot", SOURCE, sender, None, None, SystemClock(),
                        memory_store=store, event_book=EventBook(store))

    async def reply(window):
        return ""

    pilot = Pilot(tmp_path / "inbox.sqlite3", "bot", SOURCE, reply, sender.submit,
                  memory_store=store, delivery_state=tools.delivery_state,
                  record_send_result=tools.record_send_result,
                  context_assembler=MessageContextAssembler(
                      store, resolve_contact=lambda _: ContactIdentity(
                          "张三", nickname="张三",
                      ),
                  ))
    item = HookObservation(
        account_id="bot", conversation_id=TARGET, sender_id=PERSON, is_group=True,
        text="目标的新消息", sender_origin="hook", message_type=1,
        source_msg_id=81234, capture_tick_ms=1234,
    )
    assert pilot.scheduler.enqueue(item)[0] == "queued"
    service = CrossSessionService(store, dispatch_reply=pilot.dispatch_to_session)
    return store, tools, sender, pilot, service, requests


def close_chain(store, tools, sender, pilot):
    pilot.db.close()
    tools.db.close()
    sender.close()
    store.close()


def source_window():
    return ContextWindow("bot", SOURCE, True, turns=(
        ContextTurn(PERSON, "hook", "请去隔壁", message_id="source-msg"),
    ))


@pytest.mark.parametrize("phase", ["assembly", "model"])
def test_cancel_releases_target_and_restores_source_context(tmp_path, phase):
    async def scenario():
        store, tools, sender, pilot, service, _ = setup_chain(tmp_path)
        entered = asyncio.Event()

        async def blocked(*args):
            entered.set()
            await asyncio.Event().wait()

        if phase == "assembly":
            pilot.context_assembler.assemble = blocked
        else:
            pilot.reply = blocked
        try:
            with tool_turn_scope(source_window(), "source-turn", receipt_ids=("source-receipt",)):
                task = asyncio.create_task(service.dispatch(
                    account_id="bot", source_conversation_id=SOURCE,
                    target_conversation_id=TARGET, task="认一下人", operation_key="source-turn",
                ))
                await asyncio.wait_for(entered.wait(), 2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                assert active_conversation.get() == SOURCE
                assert operation_receipt_ids.get() == ("source-receipt",)
            assert pilot.scheduler.state_counts().get("running", 0) == 0
            assert pilot.scheduler.state_counts()["pending"] == 1
            assert service.db.execute("SELECT state FROM cross_session_tasks").fetchone()[0] == "interrupted"
        finally:
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_cross_session_timeout_returns_before_parent_and_releases_batch(tmp_path):
    async def scenario():
        store, tools, sender, pilot, service, requests = setup_chain(tmp_path)

        async def blocked(window):
            await asyncio.Event().wait()

        pilot.reply = blocked
        deadline = turn_deadline_monotonic.set(time.monotonic() + 1.08)
        try:
            result = await service.dispatch(
                account_id="bot", source_conversation_id=SOURCE,
                target_conversation_id=TARGET, task="稍等一下",
            )
            assert result["status"] == "timed_out"
            assert result["reason"] == "parent_turn_deadline"
            assert 0 < result["wait_seconds"] < 0.1
            assert pilot.scheduler.state_counts()["pending"] == 1
            assert pilot.scheduler.state_counts().get("running", 0) == 0
            assert requests == []
        finally:
            turn_deadline_monotonic.reset(deadline)
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_sdk_cross_session_replay_keeps_partial_updates_and_sends_once(tmp_path):
    async def scenario():
        store, tools, sender, pilot, service, requests = setup_chain(tmp_path)
        arguments = json.dumps({
            "reply_text": "收到，已认人",
            "sender_updates": ["坏参数", {"sender": "张三", "type": "称呼", "content": "张三"}],
            "dictionary_updates": [{"term": "暗号甲", "explanation": "测试暗号"}],
        }, ensure_ascii=False)
        responses = [[types.Part.from_function_call(
            name="commit_agent_turn",
            args=json.loads(arguments),
        )] for _ in range(2)]
        runtime = ScopedAgentRuntime(tmp_path / "sessions.sqlite3", model=FakeAdkModel(responses),
                                     extra_tools=tools.agent_tools())
        first = True
        seen_scopes = []

        async def reply(window):
            nonlocal first
            seen_scopes.append((memory_context.get(), operation_receipt_ids.get()))
            value = await runtime.reply(window)
            if first:
                first = False
                raise RuntimeError("模拟发送后、任务收尾前中断")
            return value

        pilot.reply = reply
        facade = CrossSessionToolset(service, "bot")
        try:
            with tool_turn_scope(source_window(), "source-turn", receipt_ids=("source-receipt",)):
                failed = await facade.delegate_to_wechat_conversation(TARGET, "请认人并回复")
                assert failed["status"] == "failed"
                assert pilot.scheduler.state_counts()["pending"] == 1
                result = await facade.delegate_to_wechat_conversation(TARGET, "请认人并回复")
                again = await facade.delegate_to_wechat_conversation(TARGET, "请认人并回复")
                assert result["status"] == "completed" and again["replayed"] is True
                assert result["task_id"] == failed["task_id"] == again["task_id"]
            assert len(requests) == 1
            assert json.loads(requests[0].content)["wxidorgid"] == TARGET
            assert pilot.scheduler.state_counts()["model_seen"] == 1
            assert all(ctx["conversation_id"] == TARGET for ctx, _ in seen_scopes)
            assert all(ids and ids != ("source-receipt",) for _, ids in seen_scopes)
            assert all(ctx["sender_choices"] for ctx, _ in seen_scopes)
            assert tools.delivery_state("source-receipt")["side_effect_started"] is False
            assert all(tools.delivery_state(ids[0])["side_effect_started"] for _, ids in seen_scopes)
            assert store.db.execute("SELECT count(*) FROM sender_memories").fetchone()[0] == 1
            assert store.db.execute("SELECT count(*) FROM entity_terms").fetchone()[0] == 1
        finally:
            await runtime.close()
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_mutation_receipt_failure_rolls_back_then_retry_writes_once(tmp_path):
    from google.adk.tools.tool_context import ToolContext as AdkToolContext
    from wechat_agent.frameworks.adk import to_adk

    async def scenario():
        store, tools, sender, pilot, _, requests = setup_chain(tmp_path)
        tool = next(to_adk(t) for t in tools.agent_tools() if t.name == "commit_agent_turn")
        args = json.loads(json.dumps({"dictionary_updates": [{"term": "原子暗号", "explanation": "含义"}]}))
        store.db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON agent_tool_mutations "
                         "BEGIN SELECT RAISE(ABORT, 'receipt_write_failed'); END")
        store.db.commit()
        try:
            with tool_turn_scope(source_window(), "atomic-op"):
                ctx = SimpleNamespace(_invocation_context=SimpleNamespace(end_invocation=False))
                result = await tool.func(**args, tool_context=ctx)
                assert result["status"] == "partial"
                assert "receipt_write_failed" in str(result)
                assert store.db.execute("SELECT count(*) FROM entity_terms").fetchone()[0] == 0
                store.db.execute("DROP TRIGGER fail_receipt")
                store.db.commit()
                for _ in range(2):
                    await tool.func(**args, tool_context=ctx)
                assert store.db.execute("SELECT count(*) FROM entity_terms").fetchone()[0] == 1
                assert store.db.execute("SELECT count(*) FROM agent_tool_mutations").fetchone()[0] == 1
            assert requests == []
        finally:
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_failed_update_group_does_not_block_valid_group_or_reply(tmp_path):
    from wechat_agent.frameworks.adk import to_adk

    async def scenario():
        store, tools, sender, pilot, _, requests = setup_chain(tmp_path)
        tool = next(to_adk(t) for t in tools.agent_tools() if t.name == "commit_agent_turn")
        store.db.execute("CREATE TRIGGER fail_dictionary BEFORE INSERT ON entity_terms "
                         "BEGIN SELECT RAISE(ABORT, 'dictionary_unavailable'); END")
        store.db.commit()
        try:
            with tool_turn_scope(source_window(), "partial-op"):
                ctx = SimpleNamespace(_invocation_context=SimpleNamespace(end_invocation=False))
                result = await tool.func(
                    dictionary_updates=[{"term": "失败暗号", "explanation": "含义"}],
                    event_updates=[{"action": "add", "content": "合法事件", "exit_condition": "验证后移除"}],
                    reply_text="合法回复",
                    tool_context=ctx,
                )
                assert result["status"] == "partial" and ctx._invocation_context.end_invocation is True
                assert result["dictionary_updates"]["status"] == "failed"
                assert result["event_updates"][0]["status"] == "saved"
                assert result["reply"]["status"] == "queued"
                assert "turn_elapsed_ms" in result
            assert len(requests) == 1
        finally:
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_rebatch_suppresses_completed_inputs_without_blocking_new_message(tmp_path):
    async def scenario():
        store, tools, sender, pilot, _, requests = setup_chain(tmp_path)
        try:
            with tool_turn_scope(source_window(), "delegated-op", receipt_ids=("old-input",)):
                assert (await tools.send_wechat_text("已经提交"))["status"] == "queued"
            # Recovery may claim the same input under a different operation ID.
            with tool_turn_scope(source_window(), "worker-replay", receipt_ids=("old-input",)):
                assert (await tools.send_wechat_text("不要重复发送"))["status"] == "already_submitted"
            # A genuine new input in the next batch must remain sendable.
            with tool_turn_scope(source_window(), "new-batch", receipt_ids=("old-input", "new-input")):
                assert (await tools.send_wechat_text("这是对新消息的回复"))["status"] == "queued"
            assert len(requests) == 2
        finally:
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


def test_rebatch_preserves_three_retry_cap(tmp_path):
    async def scenario():
        store, tools, sender, pilot, _, requests = setup_chain(
            tmp_path, hook_reply={"ret": -1, "queued": False},
        )
        try:
            with tool_turn_scope(source_window(), "failing-op", receipt_ids=("old-input",)):
                for attempt in range(4):
                    result = await tools.send_wechat_text(f"第 {attempt + 1} 次发送尝试")
                    assert result["status"] == ("rejected" if attempt < 3 else "retry_limit_reached")
            with tool_turn_scope(source_window(), "worker-replay", receipt_ids=("old-input",)):
                result = await tools.send_wechat_text("不允许绕过重试上限")
                assert result["status"] == "retry_limit_reached"
                assert result["send_attempt_count"] == 4
            assert len(requests) == 4
        finally:
            close_chain(store, tools, sender, pilot)
    asyncio.run(scenario())


@pytest.mark.parametrize("web_enabled", [True, False])
def test_production_bootstrap_uses_configured_route_and_shared_tools(tmp_path, monkeypatch, web_enabled):
    """Run the real composition root, without starting a server or using credentials."""
    from wechat_agent import bootstrap

    (tmp_path / "data").mkdir()
    (tmp_path / "config").mkdir()
    config = Path("config/agent_profiles.toml").read_text("utf-8")
    if not web_enabled:
        config = config.replace('"web", ', '')
    (tmp_path / "config/agent_profiles.toml").write_text(config, "utf-8")
    (tmp_path / "data/live-route.json").write_text(json.dumps({
        "account": "wxid_bootstrap001", "room": "another-valid-group@chatroom",
    }), "utf-8")
    captured, runtimes, clients, toolsets = {}, [], [], []
    real_runtime, real_tools, real_http = bootstrap.ScopedAgentRuntime, bootstrap.WeChatTools, httpx.Client

    def runtime(*args, **kwargs):
        value = real_runtime(*args, **kwargs)
        runtimes.append(value)
        return value

    def tools(*args, **kwargs):
        value = real_tools(*args, **kwargs)
        toolsets.append(value)
        return value

    def hook(request):
        assert request.method == "GET" and request.url.path == "/QueryDB/status"
        return httpx.Response(200, json={
            "IsLogin": 1,
            "SqliteContactDbPath": "C:/xwechat_files/wxid_bootstrap001_abc/db_storage/contact.db",
        })

    def http_client(*args, **kwargs):
        value = real_http(*args, **{**kwargs, "transport": httpx.MockTransport(hook)})
        clients.append(value)
        return value

    monkeypatch.setattr(bootstrap, "load_key", lambda **_: "offline-placeholder")
    monkeypatch.setenv("OPENAI_API_KEY", "offline-placeholder")
    monkeypatch.setattr(bootstrap, "ScopedAgentRuntime", runtime)
    monkeypatch.setattr(bootstrap, "WeChatTools", tools)
    monkeypatch.setattr(bootstrap.httpx, "Client", http_client)
    monkeypatch.setattr(bootstrap.genai, "Client", lambda **_: SimpleNamespace(
        aio=SimpleNamespace(models=SimpleNamespace(count_tokens=AsyncMock())),
    ))
    monkeypatch.setattr(
        bootstrap, "create_pilot_app", lambda pilot, **_: captured.setdefault("pilot", pilot)
    )
    monkeypatch.setenv("WECHAT_AGENT_EPISODE_RAG", "1")
    monkeypatch.setattr(bootstrap.uvicorn, "run", lambda *args, **kwargs: None)
    try:
        bootstrap.run_pilot(tmp_path, role="all")
        assert len(runtimes) == 2
        main_tools = {tool.name for tool in runtimes[0].runner.tools}
        idle_tools = {tool.name for tool in runtimes[1].runner.tools}
        assert {"search_web", "read_web_page"}.issubset(main_tools) is web_enabled
        assert {"search_web", "read_web_page"}.issubset(idle_tools) is web_enabled
        assert idle_tools - {"record_idle_activity"} == main_tools - {"search_episode_memory"}
        assert "search_episode_memory" in main_tools
        pilot = captured["pilot"]
        assert pilot.room == "another-valid-group@chatroom"
        assert pilot.time_scheduler.idle_model == runtimes[1].model_id
    finally:
        for value in runtimes:
            asyncio.run(value.close())
        if runtimes:
            runtimes[0].model._usage.repository.close()
        for value in toolsets:
            value.db.close()
        if "pilot" in captured:
            captured["pilot"].db.close()
            captured["pilot"].memory_store.close()
        for value in clients:
            value.close()


def test_run_group_pilot_cli_forwards_to_bootstrap(tmp_path, monkeypatch):
    """Thin CLI scripts/run-group-pilot.py preserves --run and --role forwarding."""
    from wechat_agent import bootstrap

    spec = importlib.util.spec_from_file_location("pilot_cli_test", "scripts/run-group-pilot.py")
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)

    calls = []
    monkeypatch.setattr(cli, "ROOT", tmp_path)
    monkeypatch.setattr(
        bootstrap,
        "run_pilot",
        lambda root, *, role="all", overrides=None: calls.append((root, role, overrides)),
    )
    monkeypatch.setattr(sys, "argv", ["run-group-pilot.py", "--run"])
    cli.main()
    assert calls == [(tmp_path, "all", None)]

    with pytest.raises(SystemExit):
        cli.main([])
