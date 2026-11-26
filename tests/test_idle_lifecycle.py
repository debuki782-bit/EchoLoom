"""Offline regressions for completed writes and idle call-limit recovery."""
import asyncio
import json
import sqlite3
from datetime import UTC, datetime

import pytest
from google.genai import types
from pydantic import PrivateAttr
from test_openai_runtime import FakeAdkModel

from echoloom.context import ContextTurn, ContextWindow
from echoloom.idle_tools import IdleActivityToolset
from echoloom.runtime_contract import ModelCallsLimitExceeded
from echoloom.session_runtime import ScopedAgentRuntime
from echoloom.time_scheduler import TimeScheduler
from echoloom.tool_timing import agent_function_tool


def call(name, arguments, index):
    return [types.Part(function_call=types.FunctionCall(
        id=f"call_{index}",
        name=name,
        args=dict(arguments),
    ))]


class IdleModel(FakeAdkModel):
    _tool_names: list = PrivateAttr(default_factory=list)

    def __init__(self, replies):
        super().__init__(replies)
        self._tool_names = []

    @property
    def tool_names(self):
        return self._tool_names

    async def generate_content_async(self, llm_request, stream=False):
        names = set()
        for tool in getattr(llm_request.config, "tools", None) or ():
            for decl in getattr(tool, "function_declarations", None) or ():
                names.add(decl.name)
        self._tool_names.append(names)
        async for item in super().generate_content_async(llm_request, stream=stream):
            yield item


def setup_idle(tmp_path):
    db = sqlite3.connect(tmp_path / "activities.sqlite3")
    scheduler = TimeScheduler(db, idle_model="gpt-6-luna")
    idle = IdleActivityToolset(scheduler)
    now = datetime.now(UTC)
    event_id, number, model = scheduler.record_idle_event("bot", at=now)
    room = f"idle-event:{event_id}"
    window = ContextWindow(
        account_id="bot", conversation_id=room, is_group=False, is_idle_event=True,
        turns=(ContextTurn(sender_id="system", sender_origin="idle_task",
                           text="复核已有证据并记录结果", message_id=f"idle:{event_id}"),),
    )
    token = idle.bind("bot", event_id, number, model, now)
    return db, scheduler, idle, event_id, window, token


@pytest.mark.parametrize("invalid_first", [False, True])
def test_summary_success_ends_sdk_run_but_rejected_summary_can_be_corrected(tmp_path, invalid_first):
    async def scenario():
        db, scheduler, idle, event_id, window, token = setup_idle(tmp_path)
        replies = [call("record_idle_activity", {"summary": "已完成复核"}, 2)]
        if invalid_first:
            replies.insert(0, call("record_idle_activity", {"summary": ""}, 1))
        model = IdleModel(replies + [call("record_idle_activity", {"summary": "重复覆盖"}, 3)])
        runtime = ScopedAgentRuntime(tmp_path / "session.sqlite3", model=model,
                                     extra_tools=idle.agent_tools())
        try:
            await idle.run_with_summary(runtime, window)
            assert scheduler.has_idle_activity("bot", event_id)
            assert len(model.requests) == 1 + int(invalid_first)
            assert scheduler.recent_idle_activities("bot") == ["已完成复核"]
        finally:
            await runtime.close()
            idle.unbind(token)
            db.close()
    asyncio.run(scenario())


def test_call_limit_keeps_last_write_and_recovers_summary_from_same_session(tmp_path):
    async def scenario():
        db, scheduler, idle, event_id, window, token = setup_idle(tmp_path)
        writes = []

        def search_user_memory(query: str):
            return {"memories": []}

        def remember_user_fact(content: str):
            writes.append(content)
            return {"status": "saved", "memory_id": "confirmed_memory"}

        # The 24th model call executes a real write before the SDK refuses call 25.
        replies = [call("search_user_memory", {"query": "旧资料"}, i) for i in range(23)]
        replies += [call("remember_user_fact", {"content": "已有证据支持的资料"}, 23),
                    call("record_idle_activity", {"summary": "资料已保存，其余复核未完成"}, 24)]
        model = IdleModel(replies)
        runtime = ScopedAgentRuntime(tmp_path / "session.sqlite3", model=model,
            extra_tools=[agent_function_tool(search_user_memory),
                         agent_function_tool(remember_user_fact), *idle.agent_tools()])
        try:
            await idle.run_with_summary(runtime, window)
            idle.finish("bot", event_id)
            assert scheduler.has_idle_activity("bot", event_id)
            assert writes == ["已有证据支持的资料"]
            assert len(model.requests) == 25
            assert model.tool_names[-1] == {"record_idle_activity"}
            assert all("remember_user_fact" in tools for tools in model.tool_names[:-1])
            assert "confirmed_memory" in str(model.requests[-1][1])
            assert "第 1/24 次" in str(model.requests[0][1])
            assert "第 24/24 次" in str(model.requests[23][1])
            assert "停止扩展任务" in str(model.requests[23][1])
            assert "第 1/2 次" in str(model.requests[24][1])
            assert "第 1/24 次" not in str(model.requests[23][1])
            row = db.execute("SELECT summary,outcome,tool_calls_json FROM idle_activity_summaries "
                             "WHERE event_id=?", (event_id,)).fetchone()
            assert row[:2] == ("资料已保存，其余复核未完成", "completed")
            assert sum(c["tool"] == "remember_user_fact" for c in json.loads(row[2])) == 1
        finally:
            await runtime.close()
            idle.unbind(token)
            db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("final_replies", [
    ["只说了总结，没有保存"],
    [call("record_idle_activity", {"summary": ""}, i) for i in range(3)],
    [call("remember_user_fact", {"content": "不允许在收尾阶段写资料"}, 1)],
])
def test_finalize_is_bounded_and_cannot_restart_work_and_retains_fallback_history(
    tmp_path, final_replies,
):
    async def scenario():
        db, scheduler, idle, event_id, window, token = setup_idle(tmp_path)
        writes = []

        def remember_user_fact(content: str):
            writes.append(content)
            return {"status": "saved"}

        model = IdleModel([call("remember_user_fact", {"content": "原任务已保存"}, 0),
                           "任务停止但未记录总结", *final_replies])
        runtime = ScopedAgentRuntime(tmp_path / "session.sqlite3", model=model,
            extra_tools=[agent_function_tool(remember_user_fact), *idle.agent_tools()])
        try:
            await idle.run_with_summary(runtime, window)
            assert scheduler.has_idle_activity("bot", event_id)
            assert writes == ["原任务已保存"]
            assert len(model.requests) <= 4  # Two work calls, at most two finalizer calls.
            assert all(tools == {"record_idle_activity"} for tools in model.tool_names[2:])
            row = db.execute("SELECT summary,outcome,tool_calls_json FROM idle_activity_summaries "
                             "WHERE event_id=?", (event_id,)).fetchone()
            assert row[1] == "failed"
            assert "remember_user_fact" in row[0]
            assert json.loads(row[2]) == [
                {"tool": "remember_user_fact", "args": {"content": "原任务已保存"}},
            ]
        finally:
            await runtime.close()
            idle.unbind(token)
            db.close()
    asyncio.run(scenario())


def test_limit_after_summary_keeps_saved_summary_without_another_model_run(tmp_path):
    async def scenario():
        db, scheduler, idle, event_id, window, token = setup_idle(tmp_path)

        class Runner:
            calls = 0

            async def reply(self, window, **kwargs):
                self.calls += 1
                idle.record_idle_activity("异常前已确认的结果")
                raise ModelCallsLimitExceeded("after write")

        runner = Runner()
        try:
            await idle.run_with_summary(runner, window)
            assert runner.calls == 1
            assert scheduler.has_idle_activity("bot", event_id)
            assert scheduler.recent_idle_activities("bot") == ["异常前已确认的结果"]
        finally:
            idle.unbind(token)
            db.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("during_finalize", [False, True])
def test_new_message_cancellation_does_not_start_or_continue_finalization(tmp_path, during_finalize):
    async def scenario():
        db, scheduler, idle, event_id, window, token = setup_idle(tmp_path)

        class Runner:
            calls = 0

            async def reply(self, window, **kwargs):
                self.calls += 1
                if during_finalize and self.calls == 1:
                    raise ModelCallsLimitExceeded("work limit")
                raise asyncio.CancelledError()

        runner = Runner()
        try:
            with pytest.raises(asyncio.CancelledError):
                await idle.run_with_summary(runner, window)
            assert runner.calls == 1 + int(during_finalize)
            assert not scheduler.has_idle_activity("bot", event_id)
        finally:
            idle.unbind(token)
            db.close()
    asyncio.run(scenario())
