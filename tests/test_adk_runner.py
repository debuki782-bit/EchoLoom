"""An actual ADK Runner with a fake model must keep sessions separate."""

import asyncio
import json
import sqlite3
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types

from echoloom.adk_runner import DeterministicLlm, ScopedAdkRunner
from echoloom.context import build_hook_context
from echoloom.hook_contract import HookObservation
from echoloom.hook_outbound import HookSendResult
from echoloom.mock import SystemClock
from echoloom.wechat_tools import WeChatTools, operation_id


def observed(account: str, room: str, sender: str, text: str) -> HookObservation:
    return HookObservation(
        account_id=account,
        conversation_id=room,
        sender_id=sender,
        sender_origin="hook_group_member_wxid"
        if room.endswith("@chatroom")
        else "native_private_fromid",
        text=text,
        is_group=room.endswith("@chatroom"),
        message_type=1,
        source_msg_id=None,
        capture_tick_ms=1234,
    )


def content_texts(request: object) -> list[str]:
    return [
        part.text or ""
        for item in request.contents  # type: ignore[attr-defined]
        for part in item.parts or []
    ]


def test_adk_fake_runs_and_separates_private_and_group_sessions() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        friend = build_hook_context((observed("bot", "friend", "friend", "私聊一"),))
        group = build_hook_context((observed("bot", "room@chatroom", "member", "群聊一"),))
        assert await runner.reply(friend) == "[MOCK ADK] 已收到消息。"
        assert await runner.reply(group) == "[MOCK ADK] 已收到消息。"
        assert len(runner.model.requests) == 2
        first = "\n".join(content_texts(runner.model.requests[0]))
        second = "\n".join(content_texts(runner.model.requests[1]))
        assert "私聊一" in first and "群聊一" not in first
        assert "群聊一" in second and "私聊一" not in second
        assert "member | member：*{群聊一}*" in group.model_user_content()

    asyncio.run(scenario())


def test_session_trace_is_per_session_and_redacts_tool_secrets(tmp_path: Path) -> None:
    class TraceEvent:
        id = "evt-1"
        author = "assistant"
        timestamp = "2026-10-31T01:00:00Z"
        partial = False
        content = types.Content(parts=[
            types.Part(text="模型回复"),
            types.Part(text="隐藏思考", thought=True),
            types.Part.from_function_call(
                name="send_message",
                args={"text": "hello", "api_key": "never-write-this"},
            ),
            types.Part.from_function_response(
                name="send_message", response={"state": "queued"},
            ),
        ])

        @staticmethod
        def is_final_response():
            return True

    runner = ScopedAdkRunner(trace_directory=tmp_path)
    runner._append_session_trace(
        session_id="session-secret-id",
        route="route-hash",
        conversation_id="room@chatroom",
        events=[TraceEvent()],
        error_type=None,
    )

    trace_files = list(tmp_path.glob("*.jsonl"))
    assert len(trace_files) == 1
    record = json.loads(trace_files[0].read_text(encoding="utf-8"))
    rendered = trace_files[0].read_text(encoding="utf-8")
    assert record["author"] == "assistant"
    assert record["final"] is True
    assert record["content"][0] == {"text": "模型回复"}
    assert "tool_call" in record["content"][1]
    assert record["content"][1]["tool_call"]["args"]["api_key"] == "[redacted]"
    assert record["content"][2]["tool_result"]["result"] == {"state": "queued"}
    assert "隐藏思考" not in rendered
    assert "never-write-this" not in rendered


def test_per_message_time_and_daily_context_use_separate_prompt_fields() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        window = build_hook_context(
            (observed("bot", "room@chatroom", "member", "今天深圳天气如何？"),),
            message_times=({"local_minute": "2026-10-29 10:03", "season": "秋季"},),
            daily_prompt_context="深圳当前天气：阴，24°C。元旦还有64天。",
        )
        await runner.reply(window)
        request = runner.model.requests[0]
        user_message = request.contents[-2].parts[0].text
        assert "2026-10-29 10:03" in user_message
        assert "秋季" not in user_message
        assert "深圳当前天气" not in user_message
        assert "深圳当前天气" not in request.config.system_instruction
        assert request.contents[-1].parts[0].text.startswith("【本轮动态召回尾块")
        assert "深圳当前天气" in request.contents[-1].parts[0].text
        assert "【当前实际时间】" in request.contents[-1].parts[0].text
        assert "时段：" in request.contents[-1].parts[0].text
        await runner.close()

    asyncio.run(scenario())


def test_last_human_message_gap_is_dynamic_tail_and_hidden_until_over_30_minutes():
    async def scenario() -> None:
        ages = iter((30, 31, 47))
        lookups = []

        def age(account_id, conversation_id):
            lookups.append((account_id, conversation_id))
            return next(ages)

        runner = ScopedAdkRunner(last_human_message_age=age)
        for text in ("刚来一条", "内部委托唤醒", "定时唤醒"):
            window = build_hook_context((observed("bot", "friend", "friend", text),))
            await runner.reply(window)
        tails = [request.contents[-1].parts[0].text for request in runner.model.requests]
        assert "停顿" not in tails[0]
        assert "本讨论串停顿：距最近一条真人消息约 31 分钟" in tails[1]
        assert "本讨论串停顿：距最近一条真人消息约 47 分钟" in tails[2]
        assert lookups == [("bot", "friend")] * 3
        await runner.close()

    asyncio.run(scenario())


def test_dynamic_recall_is_a_non_persisted_tail_and_replaces_old_snapshot() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        first = build_hook_context(
            (observed("bot", "friend", "friend", "第一轮消息"),), daily_prompt_context="动态资料 A"
        )
        second = build_hook_context(
            (observed("bot", "friend", "friend", "第二轮消息"),), daily_prompt_context="动态资料 B"
        )
        await runner.reply(first)
        await runner.reply(second)

        request_a, request_b = runner.model.requests
        first_tail = request_a.contents[-1]
        second_contents = "\n".join(content_texts(request_b))
        assert first_tail.parts[0].text.startswith("【本轮动态召回尾块")
        assert "动态资料 A" in first_tail.parts[0].text
        assert "动态资料 B" in request_b.contents[-1].parts[0].text
        assert "动态资料 A" not in second_contents
        assert "动态资料 B" not in request_b.config.system_instruction
        assert "第一轮消息" in second_contents and "第二轮消息" in second_contents
        await runner.close()

    asyncio.run(scenario())


def test_commit_agent_turn_ends_adk_run_after_one_send(tmp_path: Path) -> None:
    class RepeatingCommitLlm(DeterministicLlm):
        def __init__(self):
            super().__init__()

        async def generate_content_async(self, llm_request, stream=False):
            self._requests.append(llm_request)
            yield LlmResponse(content=types.Content(
                role="model",
                parts=[types.Part.from_function_call(
                    name="commit_agent_turn", args={"reply_text": "只发送一次"},
                )],
            ))

    async def scenario():
        (tmp_path / "data").mkdir()
        sent = []

        class Sender:
            def submit(self, draft):
                sent.append(draft)
                return HookSendResult("queued")

        tools = WeChatTools(
            tmp_path, "bot", "room@chatroom", Sender(), None, None, SystemClock(),
        )
        model = RepeatingCommitLlm()
        runner = ScopedAdkRunner(model=model, extra_tools=tools.adk_tools())
        token = operation_id.set("commit-agent-turn-single-send")
        try:
            window = build_hook_context((observed("bot", "room@chatroom", "member", "触发"),))
            await runner.reply(window)
            assert len(model.requests) == 1
            assert len(sent) == 1
        finally:
            operation_id.reset(token)
            await runner.close()
            tools.db.close()

    asyncio.run(scenario())


def test_cross_chat_recall_is_dynamic_tail_front_half_and_deduped() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        other = {
            "message_id": "other-1",
            "conversation_id": "friend-other",
            "sender_id": "friend-other",
            "body_text": "另一聊天中的新消息",
            "direction": "inbound",
            "captured_at": "2026-10-30T01:00:00+00:00",
            "event_time_context": {"local_minute": "2026-10-30 09:00"},
        }
        first = build_hook_context(
            (observed("bot", "friend-current", "friend-current", "当前聊天第一条"),),
            recent_other_messages=(other,),
        )
        second = build_hook_context(
            (observed("bot", "friend-current", "friend-current", "当前聊天第二条"),),
            recent_other_messages=(other,),
        )
        try:
            await runner.reply(first)
            await runner.reply(second)
            request_a, request_b = runner.model.requests
            tail_a = request_a.contents[-1].parts[0].text
            all_b = "\n".join(content_texts(request_b))
            assert "【此时此刻其它聊天串（非本会话）近况】" in tail_a
            assert "它们说明本轮之外的聊天此刻在做什么" in tail_a
            assert "另一聊天中的新消息" in tail_a
            assert tail_a.index("此时此刻其它聊天串（非本会话）近况") < tail_a.index("当前会话背景")
            assert "另一聊天中的新消息" not in request_a.contents[-2].parts[0].text
            assert "另一聊天中的新消息" not in all_b
            assert "当前聊天第一条" in all_b and "当前聊天第二条" in all_b
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_adk_same_chat_keeps_history_without_cross_account_leak() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        first = build_hook_context((observed("bot-a", "friend", "friend", "第一轮"),))
        second = build_hook_context((observed("bot-a", "friend", "friend", "第二轮"),))
        other = build_hook_context((observed("bot-b", "friend", "friend", "另一账号"),))
        await runner.reply(first)
        await runner.reply(second)
        await runner.reply(other)
        same_chat = "\n".join(content_texts(runner.model.requests[1]))
        other_account = "\n".join(content_texts(runner.model.requests[2]))
        assert "第一轮" in same_chat and "第二轮" in same_chat
        assert "第一轮" not in other_account and "第二轮" not in other_account
        assert "另一账号" in other_account

    asyncio.run(scenario())


def test_sqlite_adk_session_survives_runner_restart(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "adk-sessions.sqlite3"
        first = build_hook_context((observed("bot", "friend", "friend", "重启前"),))
        second = build_hook_context((observed("bot", "friend", "friend", "重启后"),))
        runner = ScopedAdkRunner(path)
        assert await runner.reply(first) == "[MOCK ADK] 已收到消息。"
        await runner.close()
        runner = ScopedAdkRunner(path)
        assert await runner.reply(second) == "[MOCK ADK] 已收到消息。"
        seen = "\n".join(content_texts(runner.model.requests[0]))
        assert "重启前" in seen and "重启后" in seen
        await runner.close()

    asyncio.run(scenario())


def test_adk_session_history_is_managed_by_adk_without_event_count_rollover(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "rolling-sessions.sqlite3"
        first = build_hook_context((observed("bot", "friend", "friend", "窗口一旧消息"),))
        second = build_hook_context((observed("bot", "friend", "friend", "窗口二新消息"),))
        runner = ScopedAdkRunner(path)
        await runner.reply(first)
        await runner.reply(second)
        current = "\n".join(content_texts(runner.model.requests[1]))
        assert "窗口二新消息" in current and "窗口一旧消息" in current
        await runner.close()

        with sqlite3.connect(path) as conn:
            count = conn.execute("SELECT count(*) FROM sessions").fetchone()[0]
        assert count == 1

        runner = ScopedAdkRunner(path)
        try:
            third = build_hook_context((observed("bot", "friend", "friend", "窗口二续接"),))
            await runner.reply(third)
            resumed = "\n".join(content_texts(runner.model.requests[0]))
            assert "窗口二续接" in resumed
            assert "窗口二新消息" in resumed
            assert "窗口一旧消息" in resumed
        finally:
            await runner.close()
        with sqlite3.connect(path) as conn:
            count = conn.execute("SELECT count(*) FROM sessions").fetchone()[0]
        with sqlite3.connect(tmp_path / "rolling-sessions-routing.sqlite3") as conn:
            generation = conn.execute("SELECT generation FROM active_sessions").fetchone()[0]
        assert count == 1
        assert generation == 0

    asyncio.run(scenario())


def test_adk_does_not_roll_session_on_project_event_window_limit(tmp_path: Path):
    async def scenario() -> None:
        path = tmp_path / "incoming-window.sqlite3"
        first = build_hook_context((observed("bot", "friend", "friend", "此前消息"),))
        second = replace(
            build_hook_context((observed("bot", "friend", "friend", "本轮新消息"),)),
            history_turns=(
                build_hook_context((observed("bot", "friend", "friend", "此前消息"),)).turns
            ),
        )
        runner = ScopedAdkRunner(path)
        try:
            await runner.reply(first)
            await runner.reply(second)
            current = "\n".join(content_texts(runner.model.requests[-1]))
            assert "本轮新消息" in current and "此前消息" in current
            with sqlite3.connect(path.with_name("incoming-window-routing.sqlite3")) as conn:
                generation = conn.execute("SELECT generation FROM active_sessions").fetchone()[0]
            assert generation == 0
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_high_context_session_is_summarized_once_during_maintenance(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "high-context.sqlite3"
        runner = ScopedAdkRunner(path)
        try:
            await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "高上下文阈值测试"),))
            )
            runner.idle_effective_message_threshold = 1
            with sqlite3.connect(path.with_name("high-context-routing.sqlite3")) as conn:
                conn.execute(
                    "UPDATE active_sessions SET last_message_at=?",
                    ((datetime.now(UTC) - timedelta(hours=9)).isoformat(),),
                )
            assert await runner.summarize_idle_sessions() == 1
            assert await runner.summarize_idle_sessions() == 0
            with sqlite3.connect(tmp_path / "high-context-routing.sqlite3") as conn:
                generation, last_message_at = conn.execute(
                    "SELECT generation,last_message_at FROM active_sessions"
                ).fetchone()
            assert generation == 1
            assert last_message_at is None
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_session_summary_retries_three_times_after_initial_failure() -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner()
        attempts = 0

        async def succeeds_on_third_attempt(session, user_id):
            nonlocal attempts
            attempts += 1
            return "摘要完成" if attempts == 3 else None

        runner._summarize_session = succeeds_on_third_attempt  # type: ignore[method-assign]
        try:
            assert await runner._summarize_session_with_retries(
                object(), "user", reason="test",
            ) == "摘要完成"
            assert attempts == 3

            attempts = 0

            async def always_fails(session, user_id):
                nonlocal attempts
                attempts += 1

            runner._summarize_session = always_fails  # type: ignore[method-assign]
            assert await runner._summarize_session_with_retries(
                object(), "user", reason="test",
            ) is None
            assert attempts == 4
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_safety_block_uses_same_bounded_summary_retry_path(tmp_path: Path) -> None:
    class SafetyBlockedLlm(BaseLlm):
        def __init__(self) -> None:
            super().__init__(model="offline-safety-block")

        async def generate_content_async(self, llm_request, stream: bool = False):
            yield LlmResponse(
                error_code="PROHIBITED_CONTENT",
                error_message="offline safety regression",
            )

    async def scenario() -> None:
        runner = ScopedAdkRunner(tmp_path / "safety-summary.sqlite3", model=SafetyBlockedLlm())
        attempts = 0

        async def fail_summary(session, user_id):
            nonlocal attempts
            attempts += 1

        runner._summarize_session = fail_summary  # type: ignore[method-assign]
        try:
            assert await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "安全拦截回归"),))
            ) == ""
            assert attempts == 4
            with sqlite3.connect(tmp_path / "safety-summary-routing.sqlite3") as conn:
                generation = conn.execute(
                    "SELECT generation FROM active_sessions"
                ).fetchone()[0]
            assert generation == 1
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_deleted_sqlite_chat_stays_deleted_after_restart_without_erasing_other_chat(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        path = tmp_path / "delete-sessions.sqlite3"
        runner = ScopedAdkRunner(path)
        await runner.reply(build_hook_context((observed("bot", "a", "a", "删除此历史"),)))
        await runner.reply(build_hook_context((observed("bot", "b", "b", "保留此历史"),)))
        await runner.delete_conversation("bot", "a")
        await runner.close()
        runner = ScopedAdkRunner(path)
        try:
            await runner.reply(build_hook_context((observed("bot", "a", "a", "新会话"),)))
            await runner.reply(build_hook_context((observed("bot", "b", "b", "续接"),)))
            deleted = "\n".join(content_texts(runner.model.requests[0]))
            retained = "\n".join(content_texts(runner.model.requests[1]))
            assert "删除此历史" not in deleted and "新会话" in deleted
            assert "保留此历史" in retained and "删除此历史" not in retained
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_model_request_snapshot_is_traced_with_runtime_tail(tmp_path: Path) -> None:
    async def scenario() -> None:
        runner = ScopedAdkRunner(
            tmp_path / "trace-sessions.sqlite3", trace_directory=tmp_path / "traces",
        )
        try:
            await runner.reply(
                build_hook_context(
                    (observed("bot", "wxid_trace1", "wxid_trace1", "看一下trace"),)
                )
            )
        finally:
            await runner.close()

    asyncio.run(scenario())
    trace_files = list((tmp_path / "traces").glob("*.jsonl"))
    assert len(trace_files) == 1
    records = [
        json.loads(line)
        for line in trace_files[0].read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    snapshots = [item for item in records if item.get("record") == "model_request"]
    assert snapshots
    text = "\n".join(
        str(part.get("text", ""))
        for content in snapshots[-1]["contents"]
        for part in content["parts"]
    )
    assert "本轮聊天记录" in text
    assert "【本轮动态召回尾块】" in text
    assert snapshots[-1]["conversation_id"] == "wxid_trace1"


def test_safety_block_redacts_model_request_snapshot(tmp_path: Path) -> None:
    class BlockingLlm(BaseLlm):
        def __init__(self) -> None:
            super().__init__(model="offline-safety-block")

        async def generate_content_async(self, llm_request, stream: bool = False):
            yield LlmResponse(
                error_code="PROHIBITED_CONTENT",
                error_message="offline safety regression",
            )

    async def scenario() -> None:
        runner = ScopedAdkRunner(
            tmp_path / "redact-sessions.sqlite3",
            model=BlockingLlm(),
            trace_directory=tmp_path / "traces",
        )

        async def no_summary(session, user_id):
            return None

        runner._summarize_session = no_summary  # type: ignore[method-assign]
        try:
            await runner.reply(
                build_hook_context(
                    (observed("bot", "wxid_red1", "wxid_red1", "敏感正文"),)
                )
            )
        finally:
            await runner.close()

    asyncio.run(scenario())
    trace_files = list((tmp_path / "traces").glob("*.jsonl"))
    assert len(trace_files) == 1
    body = trace_files[0].read_text(encoding="utf-8")
    assert "敏感正文" not in body
    records = [json.loads(line) for line in body.splitlines() if line.strip()]
    snapshots = [item for item in records if item.get("record") == "model_request"]
    assert snapshots
    for snapshot in snapshots:
        for content in snapshot["contents"]:
            for part in content["parts"]:
                if isinstance(part.get("text"), str):
                    assert part["text"] == "[prompt blocked; body omitted]"


def test_session_byte_estimate_does_not_count_image_payloads() -> None:
    from types import SimpleNamespace
    from echoloom.adk_runner import _estimated_event_bytes

    text_event = SimpleNamespace(content=SimpleNamespace(parts=[
        SimpleNamespace(text="你好" * 500),
    ]))
    image_event = SimpleNamespace(content=SimpleNamespace(parts=[
        SimpleNamespace(text="看看这张图"),
        SimpleNamespace(inline_data=SimpleNamespace(data=b"x" * 500_000)),
    ]))
    tool_event = SimpleNamespace(content=SimpleNamespace(parts=[
        SimpleNamespace(function_call=SimpleNamespace(name="x", args={})),
        SimpleNamespace(function_response=SimpleNamespace(name="x", response={})),
    ]))

    text_bytes = _estimated_event_bytes(text_event)
    assert text_bytes >= 3000
    # A 500KB image must count as a small allowance, not its raw size.
    image_bytes = _estimated_event_bytes(image_event)
    assert image_bytes < text_bytes + 2200
    assert _estimated_event_bytes(tool_event) < 2500
    rich_tool_event = SimpleNamespace(content=SimpleNamespace(parts=[
        SimpleNamespace(function_response=SimpleNamespace(name="web", response={"body": "a" * 4000})),
    ]))
    assert _estimated_event_bytes(rich_tool_event) >= 4000


def test_live_session_events_filters_rewound_invocations_without_adk_imports() -> None:
    import inspect
    from types import SimpleNamespace
    import echoloom.session_runtime as session_runtime
    from echoloom.session_runtime import _live_session_events

    source = inspect.getsource(session_runtime)
    assert "google.adk" not in source and "google.genai" not in source

    def ev(inv_id: str, text: str, *, rewind_before: str | None = None):
        actions = SimpleNamespace(rewind_before_invocation_id=rewind_before)
        return SimpleNamespace(
            invocation_id=inv_id,
            author="user",
            actions=actions,
            content=SimpleNamespace(parts=[SimpleNamespace(text=text)]),
        )

    assert _live_session_events(None) == []
    assert _live_session_events([]) == []
    events = [
        ev("inv-1", "keep-1"),
        ev("inv-1", "keep-2"),
        ev("inv-2", "drop-1"),
        ev("inv-3", "drop-2"),
        ev("inv-r1", "rewind", rewind_before="inv-2"),
        ev("inv-4", "keep-3"),
    ]
    kept = _live_session_events(events)
    assert [e.content.parts[0].text for e in kept] == ["keep-1", "keep-2", "keep-3"]
    assert _live_session_events([ev("inv-1", "only"), ev("inv-r", "", rewind_before="inv-1")]) == []


def test_model_provider_unavailable_preserves_idle_session_seen_ids_and_hydration(
    tmp_path: Path,
) -> None:
    from google.adk.events import Event, EventActions
    from pydantic import PrivateAttr
    from echoloom.runtime_contract import ModelCallIndeterminate, ModelProviderUnavailable
    from echoloom.session_runtime import _live_session_events, session_keys

    class ControllableLlm(BaseLlm):
        _requests: list = PrivateAttr(default_factory=list)
        _outcomes: list = PrivateAttr(default_factory=list)

        def __init__(self, outcomes: list[object]) -> None:
            super().__init__(model="offline-controllable")
            self._outcomes = list(outcomes)

        @property
        def requests(self) -> list:
            return self._requests

        async def generate_content_async(self, llm_request, stream: bool = False):
            self._requests.append(llm_request.model_copy(deep=True))
            outcome = self._outcomes.pop(0) if self._outcomes else "默认回复"
            if isinstance(outcome, BaseException):
                raise outcome
            yield LlmResponse(
                content=types.Content(
                    role="model",
                    parts=[types.Part.from_text(text=str(outcome))],
                )
            )

    def other_msg(message_id: str, text: str) -> dict[str, object]:
        return {
            "message_id": message_id,
            "conversation_id": "friend-other",
            "sender_id": "friend-other",
            "body_text": text,
            "direction": "inbound",
            "captured_at": "2026-10-30T01:00:00+00:00",
            "event_time_context": {"local_minute": "2026-10-30 09:00"},
        }

    async def scenario() -> None:
        path = tmp_path / "outage-recovery.sqlite3"
        chat_llm = ControllableLlm([
            "第一轮正常回复",
            ModelProviderUnavailable("offline-turn-2a"),
            ModelProviderUnavailable("offline-turn-2b"),
            "第二轮恢复回复",
            ModelProviderUnavailable("offline-gen1-a"),
            ModelProviderUnavailable("offline-gen1-b"),
            ModelProviderUnavailable("offline-gen1-c"),
            "轮转后恢复回复",
            "轮转后下一轮",
            ModelCallIndeterminate("ambiguous-error"),
            "模糊异常后下一轮",
        ])
        summary_llm = ControllableLlm([
            ModelProviderUnavailable("offline-summary-idle"),
            ModelProviderUnavailable("offline-summary-reply"),
            "八小时闲置压缩摘要",
        ])
        runner = ScopedAdkRunner(
            path,
            model=chat_llm,
            summary_model=summary_llm,
        )
        user_id, base_session_id = session_keys("bot", "friend")
        routing_path = path.with_name("outage-recovery-routing.sqlite3")
        try:
            # 1) Turn 1 succeeds on generation 0.
            assert await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "第一轮内容"),))
            ) == "第一轮正常回复"

            # 2) Existing live session experiences repeated ModelProviderUnavailable:
            #    rewound turn leaves 0 live orphan events and does NOT commit seen_ids.
            other_a = other_msg("other-a", "现有会话跨群消息")
            turn2 = build_hook_context(
                (observed("bot", "friend", "friend", "第二轮内容"),),
                recent_other_messages=(other_a,),
            )
            for _ in range(2):
                try:
                    await runner.reply(turn2)
                    raise AssertionError("expected ModelProviderUnavailable")
                except ModelProviderUnavailable:
                    pass

            session0 = await runner.sessions.get_session(
                app_name="wechat_agent_mock",
                user_id=user_id,
                session_id=base_session_id,
            )
            assert session0 is not None
            live0 = _live_session_events(session0.events)
            assert len(live0) == 2  # Only turn 1 user + model reply survive.
            assert "第二轮内容" not in "\n".join(
                part.text or ""
                for ev in live0
                for part in (getattr(getattr(ev, "content", None), "parts", None) or [])
            )
            with sqlite3.connect(routing_path) as conn:
                seen_rows = conn.execute(
                    "SELECT message_id FROM session_other_message_seen"
                ).fetchall()
            assert seen_rows == []

            # When provider recovers, cross-chat row is still delivered and turn 2 runs once.
            assert await runner.reply(turn2) == "第二轮恢复回复"
            recovered_req = "\n".join(content_texts(chat_llm.requests[-1]))
            assert "现有会话跨群消息" in recovered_req
            assert recovered_req.count("第二轮内容") == 1

            # 3) Backdate 9h idle; ModelProviderUnavailable must NOT rotate/destroy session.
            runner.idle_effective_message_threshold = 1
            stale_ts = (datetime.now(UTC) - timedelta(hours=9)).isoformat()
            with sqlite3.connect(routing_path) as conn:
                conn.execute("UPDATE active_sessions SET last_message_at=?", (stale_ts,))
                conn.commit()

            try:
                await runner.summarize_idle_sessions()
                raise AssertionError("expected ModelProviderUnavailable in summarize_idle_sessions")
            except ModelProviderUnavailable:
                pass
            with sqlite3.connect(routing_path) as conn:
                gen, last_at, latest_sum = conn.execute(
                    "SELECT generation,last_message_at,latest_summary FROM active_sessions"
                ).fetchone()
            assert gen == 0 and last_at == stale_ts and latest_sum is None

            # Also test idle rollover during reply() when summary provider is offline.
            other_b = other_msg("other-b", "闲置轮转失败时不能吞掉的跨群消息")
            try:
                await runner.reply(
                    build_hook_context(
                        (observed("bot", "friend", "friend", "离线触发轮转"),),
                        recent_other_messages=(other_b,),
                    )
                )
                raise AssertionError("expected ModelProviderUnavailable during idle rollover")
            except ModelProviderUnavailable:
                pass
            with sqlite3.connect(routing_path) as conn:
                gen, last_at, latest_sum = conn.execute(
                    "SELECT generation,last_message_at,latest_summary FROM active_sessions"
                ).fetchone()
                seen_gen0 = conn.execute(
                    "SELECT message_id FROM session_other_message_seen WHERE generation=0"
                ).fetchall()
            assert gen == 0 and last_at == stale_ts and latest_sum is None
            assert ("other-b",) not in seen_gen0

            # Summary provider recovers -> summarizes once and advances to generation 1.
            assert await runner.summarize_idle_sessions() == 1
            with sqlite3.connect(routing_path) as conn:
                gen, last_at, latest_sum = conn.execute(
                    "SELECT generation,last_message_at,latest_summary FROM active_sessions"
                ).fetchone()
            assert gen == 1 and last_at is None and latest_sum == "八小时闲置压缩摘要"

            # 4) Repeated ModelProviderUnavailable on rotated generation 1:
            #    leaves 0 live events, keeps seen_ids uncommitted, and preserves hydration.
            hist_turn = build_hook_context((observed("bot", "friend", "friend", "同窗历史消息"),)).turns
            other_c = other_msg("other-c", "轮转世代跨群消息")
            rotated_window = replace(
                build_hook_context(
                    (observed("bot", "friend", "friend", "轮转后新消息"),),
                    recent_other_messages=(other_c,),
                ),
                history_turns=hist_turn,
            )
            gen1_session_id = f"{base_session_id}-window-1"
            for _ in range(2):
                try:
                    await runner.reply(rotated_window)
                    raise AssertionError("expected ModelProviderUnavailable on gen 1")
                except ModelProviderUnavailable:
                    pass
                assert await runner.sessions.get_session(
                    app_name="wechat_agent_mock",
                    user_id=user_id,
                    session_id=gen1_session_id,
                ) is None
                with sqlite3.connect(routing_path) as conn:
                    assert conn.execute(
                        "SELECT message_id FROM session_other_message_seen WHERE generation=1"
                    ).fetchall() == []
                    gen, last_at, latest_sum = conn.execute(
                        "SELECT generation,last_message_at,latest_summary FROM active_sessions"
                    ).fetchone()
                assert gen == 1 and last_at is None and latest_sum == "八小时闲置压缩摘要"

            # Even if an empty/rewound-only session already exists in DB (not new_session),
            # a 3rd failure leaves 0 live events and recovery STILL hydrates summary & history.
            existing_empty = await runner.sessions.create_session(
                app_name="wechat_agent_mock",
                user_id=user_id,
                session_id=gen1_session_id,
            )
            await runner.sessions.append_event(
                existing_empty,
                Event(
                    invocation_id="orphan-inv",
                    author="user",
                    content=types.Content(role="user", parts=[types.Part.from_text(text="孤儿消息")]),
                ),
            )
            await runner.sessions.append_event(
                existing_empty,
                Event(
                    invocation_id="rewind-inv",
                    author="user",
                    actions=EventActions(rewind_before_invocation_id="orphan-inv"),
                ),
            )
            try:
                await runner.reply(rotated_window)
                raise AssertionError("expected 3rd ModelProviderUnavailable on gen 1")
            except ModelProviderUnavailable:
                pass
            persisted_gen1 = await runner.sessions.get_session(
                app_name="wechat_agent_mock",
                user_id=user_id,
                session_id=gen1_session_id,
            )
            assert _live_session_events(persisted_gen1.events if persisted_gen1 else []) == []

            # Recovery on gen 1 hydrates latest_summary, history_turns, and recent_other_turns once.
            assert await runner.reply(rotated_window) == "轮转后恢复回复"
            gen1_req = "\n".join(content_texts(chat_llm.requests[-1]))
            assert "八小时闲置压缩摘要" in gen1_req
            assert "同窗历史消息" in gen1_req
            assert "轮转世代跨群消息" in gen1_req
            assert gen1_req.count("轮转后新消息") == 1

            # Next turn on gen 1 deduplicates other-c.
            assert await runner.reply(
                build_hook_context(
                    (observed("bot", "friend", "friend", "确认去重"),),
                    recent_other_messages=(other_c,),
                )
            ) == "轮转后下一轮"
            assert "轮转世代跨群消息" not in "\n".join(content_texts(chat_llm.requests[-1]))

            # 5) Contrast: ModelCallIndeterminate DOES commit seen_ids to prevent duplicates.
            other_d = other_msg("other-d", "模糊错误跨群消息")
            try:
                await runner.reply(
                    build_hook_context(
                        (observed("bot", "friend", "friend", "触发模糊异常"),),
                        recent_other_messages=(other_d,),
                    )
                )
                raise AssertionError("expected ModelCallIndeterminate")
            except ModelCallIndeterminate:
                pass
            assert await runner.reply(
                build_hook_context(
                    (observed("bot", "friend", "friend", "模糊异常重试"),),
                    recent_other_messages=(other_d,),
                )
            ) == "模糊异常后下一轮"
            assert "模糊错误跨群消息" not in "\n".join(content_texts(chat_llm.requests[-1]))
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_keyword_only_effective_message_count_works_in_idle_and_reply(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str, datetime]] = []

    def kw_only_count(*, account_id: str, conversation_id: str, since: datetime) -> int:
        assert since.tzinfo is not None
        calls.append((account_id, conversation_id, since))
        return 5

    async def scenario() -> None:
        path = tmp_path / "kw-count.sqlite3"
        runner = ScopedAdkRunner(path, effective_message_count=kw_only_count)
        routing_path = path.with_name("kw-count-routing.sqlite3")
        try:
            runner.idle_effective_message_threshold = 3
            await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "第一轮"),))
            )
            stale = (datetime.now(UTC) - timedelta(hours=9)).isoformat()
            with sqlite3.connect(routing_path) as conn:
                conn.execute("UPDATE active_sessions SET last_message_at=?", (stale,))
                conn.commit()
            # 1) summarize_idle_sessions() exercises _count_effective_messages.
            assert await runner.summarize_idle_sessions() == 1
            assert len(calls) == 1

            # 2) _reply() idle path also exercises _count_effective_messages.
            await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "第二轮"),))
            )
            with sqlite3.connect(routing_path) as conn:
                conn.execute("UPDATE active_sessions SET last_message_at=?", (stale,))
                conn.commit()
            await runner.reply(
                build_hook_context((observed("bot", "friend", "friend", "第三轮触发闲置轮转"),))
            )
            assert len(calls) == 2
            with sqlite3.connect(routing_path) as conn:
                generation = conn.execute(
                    "SELECT generation FROM active_sessions"
                ).fetchone()[0]
            assert generation == 2
        finally:
            await runner.close()

    asyncio.run(scenario())
