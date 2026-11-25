"""Unified Google ADK loop with OpenAIAdkModel and BudgetedAdkModel; no network or WeChat."""
import asyncio
from types import SimpleNamespace

import httpx
import pytest
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr

from wechat_agent.agent_profiles import (
    LUNA_ACTIVE_COMPRESSION_TOKENS,
    LUNA_IDLE_COMPRESSION_TOKENS,
)
from wechat_agent.context import build_hook_context
from wechat_agent.frameworks.adk import AdkFramework, to_adk
from wechat_agent.frameworks.adk_budget import BudgetedAdkModel
from wechat_agent.frameworks.adk_openai import OpenAIAdkModel
from wechat_agent.hook_contract import HookObservation
from wechat_agent.runtime_contract import AgentTool, Part as RuntimePart
from wechat_agent.session_runtime import ScopedAgentRuntime


def window(text='你好', room='friend'):
    return build_hook_context((HookObservation(
        account_id='bot', conversation_id=room, sender_id='person',
        sender_origin='native_private_fromid', text=text, is_group=False,
        message_type=1, source_msg_id=None, capture_tick_ms=123),))


class FakeAdkModel(BaseLlm):
    _requests: list = PrivateAttr(default_factory=list)
    _replies: list = PrivateAttr(default_factory=list)
    _context_tokens: int = PrivateAttr(default=100)
    _idle_compression_tokens: int = PrivateAttr(default=LUNA_IDLE_COMPRESSION_TOKENS)
    _active_compression_tokens: int = PrivateAttr(default=LUNA_ACTIVE_COMPRESSION_TOKENS)

    def __init__(self, replies=None):
        super().__init__(model='gpt-6-luna')
        self._requests = []
        self._replies = list(replies or ['收到'])
        self._context_tokens = 100

    @property
    def requests(self):
        return self._requests

    @property
    def context_tokens(self):
        return self._context_tokens

    @context_tokens.setter
    def context_tokens(self, value: int) -> None:
        self._context_tokens = value

    @property
    def idle_compression_tokens(self) -> int:
        return self._idle_compression_tokens

    @property
    def active_compression_tokens(self) -> int:
        return self._active_compression_tokens

    async def count_request_tokens(self, request) -> int:
        return self._context_tokens

    async def generate_content_async(self, llm_request, stream=False):
        inst = getattr(llm_request.config, 'system_instruction', None)
        contents = [c.model_dump() for c in llm_request.contents]
        self._requests.append((inst, contents))
        reply = self._replies.pop(0) if self._replies else '收到'
        parts = reply if isinstance(reply, list) else [types.Part.from_text(text=reply)]
        yield LlmResponse(content=types.Content(role='model', parts=parts))


# Keep backwards alias for any external test import.
FakeModel = FakeAdkModel


def _make_budgeted_openai(client, usage, clock, *, model_id='gpt-6-luna'):
    return BudgetedAdkModel(
        OpenAIAdkModel(model_id, client),
        usage,
        clock,
        model_id=model_id,
        budget_microusd=4_000_000,
        http_timeout_ms=60_000,
    )


def test_sdk_sessions_tail_replacement_restart_and_trace(tmp_path):
    async def scenario():
        model = FakeAdkModel()
        runtime = ScopedAgentRuntime(tmp_path/'adk.sqlite3', model=model,
                                    event_book_context=lambda _: '事件 A')
        assert await runtime.reply(window('第一条')) == '收到'
        runtime.event_book_context = lambda _: '事件 B'
        assert await runtime.reply(window('第二条')) == '收到'
        encoded = str(model.requests[-1][1])
        assert '第一条' in encoded and '第二条' in encoded
        assert '事件 B' in encoded and '事件 A' not in encoded
        assert encoded.count('事件 B') == 1
        await runtime.reply(window('其它聊天', room='other'))
        assert '第一条' not in str(model.requests[-1][1])
        await runtime.close()
        resumed = ScopedAgentRuntime(tmp_path/'adk.sqlite3', model=model)
        await resumed.reply(window('服务重启之后'))
        assert '第一条' in str(model.requests[-1][1])
        assert '事件 A' not in str(model.requests[-1][1])
        assert list((tmp_path/'session-traces').glob('*.jsonl'))
        await resumed.close()
    asyncio.run(scenario())


def test_sdk_tool_commit_ends_run_without_extra_model_request(tmp_path):
    async def scenario():
        writes = []
        async def commit(reply_text: str = '', tool_context=None):
            writes.append(reply_text)
            tool_context.end_turn()
            return {'status': 'completed', 'reply': {'status': 'queued'}}
        model = FakeAdkModel([[types.Part(function_call=types.FunctionCall(
            id='call_1', name='commit', args={'reply_text': '一次发送'},
        ))]])
        runtime = ScopedAgentRuntime(tmp_path/'test.sqlite3', model=model,
            extra_tools=[AgentTool(commit, 'commit', '提交本轮')])
        assert await runtime.reply(window()) == ''
        assert writes == ['一次发送']
        assert len(model.requests) == 1
        await runtime.close()
    asyncio.run(scenario())


def test_media_runtime_part_converts_to_adk_part():
    async def inspect():
        part = RuntimePart.from_bytes(data=b'image', mime_type='image/png')
        return {'image_id': 'image1', 'image_part': part}

    tool = to_adk(AgentTool(inspect, 'inspect', '看图片'))
    async def check():
        res = await tool.func()
        assert isinstance(res['image_part'], types.Part)
        assert res['image_part'].inline_data.mime_type == 'image/png'
        assert res['image_part'].inline_data.data == b'image'
    asyncio.run(check())


def test_real_wechat_tools_register_and_commit_partial_success(tmp_path):
    from wechat_agent.wechat_tools import WeChatTools, operation_id
    from wechat_agent.mock import SystemClock
    from wechat_agent.hook_outbound import HookSendResult
    async def scenario():
        (tmp_path/'data').mkdir()
        sent = []
        sender = SimpleNamespace(submit=lambda draft: sent.append(draft) or HookSendResult('queued'))
        service = WeChatTools(tmp_path, 'bot', 'friend', sender, None, None, SystemClock())
        tools = {t.name: to_adk(t) for t in service.agent_tools()}
        assert 'read_wechat_image' in tools
        token = operation_id.set('stable_test_operation')
        try:
            ctx = SimpleNamespace(_invocation_context=SimpleNamespace(end_invocation=False))
            result = await tools['commit_agent_turn'].func(
                reply_text='你好',
                sender_updates=['bad', {'sender_id': 7}],
                tag_updates='bad',
                tool_context=ctx,
            )
            assert result['status'] == 'partial'
            assert 'rejected' in str(result)
            assert len(sent) == 1 and ctx._invocation_context.end_invocation is True
        finally:
            operation_id.reset(token)
            service.db.close()
    asyncio.run(scenario())


def test_sdk_image_tool_pixels_reach_next_model_request(tmp_path):
    async def scenario():
        def view():
            return {'image_id': 'img1', 'image_part': RuntimePart.from_bytes(
                data=b'pixels', mime_type='image/png')}
        model = FakeAdkModel([[types.Part(function_call=types.FunctionCall(
            id='image_call', name='view', args={},
        ))], '看到了'])
        runtime = ScopedAgentRuntime(tmp_path/'images.sqlite3', model=model,
            extra_tools=[AgentTool(view, 'view', '看图片')])
        assert await runtime.reply(window()) == '看到了'
        assert b'pixels' in str(model.requests[-1][1]).encode() or 'pixels' in str(model.requests[-1][1])
        await runtime.close()
    asyncio.run(scenario())


def test_official_client_request_usage_and_unknown_accounting(tmp_path):
    import httpx as httpx2
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.providers import ModelCallIndeterminate
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    async def scenario():
        calls = []
        missing_usage = False

        def handle(request):
            calls.append(request.url.path)
            if request.url.path.endswith('/input_tokens'):
                return httpx2.Response(200, json={'object': 'response.input_tokens', 'input_tokens': 100})
            body = {
                'id': 'resp_test', 'object': 'response', 'created_at': 1, 'status': 'completed',
                'model': 'gpt-6-luna', 'output': [{'id': 'message_api', 'type': 'message',
                 'role': 'assistant', 'status': 'completed', 'content': [
                     {'type': 'output_text', 'text': '已收到', 'annotations': []}]}],
                'usage': {'input_tokens': 100, 'output_tokens': 10, 'total_tokens': 110,
                          'input_tokens_details': {'cached_tokens': 60},
                          'output_tokens_details': {'reasoning_tokens': 0}}}
            if missing_usage:
                body.pop('usage')
            return httpx2.Response(200, json=body)

        client = AsyncOpenAI(api_key='offline-placeholder', max_retries=0,
            http_client=httpx2.AsyncClient(transport=httpx2.MockTransport(handle)))
        repo = Repository(tmp_path/'usage.sqlite3')
        model = _make_budgeted_openai(client, UsageLedger(repo, SystemClock()), SystemClock())
        runtime = ScopedAgentRuntime(tmp_path/'api.sqlite3', model=model)
        assert await runtime.reply(window()) == '已收到'
        row = repo.conn.execute('SELECT state,actual_microusd FROM usage_reservations').fetchone()
        assert row['state'] == 'settled' and row['actual_microusd'] == 10
        assert calls == ['/v1/responses/input_tokens', '/v1/responses']
        missing_usage = True
        with pytest.raises(ModelCallIndeterminate):
            await runtime.reply(window('缺少用量的响应'))
        assert repo.conn.execute("SELECT count(*) FROM usage_reservations WHERE state='reserved'").fetchone()[0] == 1
        assert calls.count('/v1/responses') == 2  # No automatic replay after HTTP 200.
        assert calls.count('/v1/responses/input_tokens') == 3
        await runtime.close()
        await client.close()
        repo.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('tokens,expected', [(63_999, 0), (64_000, 1)])
def test_luna_idle_compression_uses_tokens_not_message_count(tmp_path, tokens, expected):
    from datetime import UTC, datetime, timedelta
    async def scenario():
        model, summary = FakeAdkModel(), FakeAdkModel(['闲置后的共同记忆'])
        runtime = ScopedAgentRuntime(tmp_path/'idle-limit.sqlite3', model=model,
            summary_model=summary, effective_message_count=lambda *args: 5000)
        try:
            await runtime.reply(window('很多短消息也不能绕过体积阈值'))
            model.context_tokens = tokens
            runtime._routing_db.execute('UPDATE active_sessions SET last_message_at=?',
                ((datetime.now(UTC) - timedelta(hours=9)).isoformat(),))
            runtime._routing_db.commit()
            assert await runtime.summarize_idle_sessions() == expected
            assert await runtime.summarize_idle_sessions() == 0
            assert len(summary.requests) == expected
        finally:
            await runtime.close()
    asyncio.run(scenario())


def test_luna_idle_requires_eight_hours_and_skips_unavailable_counter(tmp_path):
    from datetime import UTC, datetime, timedelta
    async def scenario():
        model, summary = FakeAdkModel(), FakeAdkModel(['摘要'])
        runtime = ScopedAgentRuntime(tmp_path/'idle-time.sqlite3', model=model, summary_model=summary)
        try:
            await runtime.reply(window())
            model.context_tokens = 100_000
            runtime._routing_db.execute('UPDATE active_sessions SET last_message_at=?',
                ((datetime.now(UTC) - timedelta(hours=7, minutes=59)).isoformat(),))
            runtime._routing_db.commit()
            assert await runtime.summarize_idle_sessions() == 0
            runtime._routing_db.execute('UPDATE active_sessions SET last_message_at=?',
                ((datetime.now(UTC) - timedelta(hours=9)).isoformat(),))
            runtime._routing_db.commit()
            async def unavailable(**kwargs):
                raise OSError('offline counter')
            runtime.runner.count_context_tokens = unavailable
            assert await runtime.summarize_idle_sessions() == 0
            assert not summary.requests
            assert runtime._routing_db.execute('SELECT generation FROM active_sessions').fetchone()[0] == 0
        finally:
            await runtime.close()
    asyncio.run(scenario())


def test_luna_active_compression_happens_before_new_turn_once(tmp_path):
    async def scenario():
        model, summary = FakeAdkModel(), FakeAdkModel(['上一轮的重要约定'])
        runtime = ScopedAgentRuntime(tmp_path/'active-limit.sqlite3', model=model, summary_model=summary)
        try:
            await runtime.reply(window('旧消息'))
            model.context_tokens = 95_999
            runtime.hard_context_byte_threshold = 1  # Legacy byte thresholds must not trigger Luna.
            await runtime.reply(window('阈值以下'))
            assert not summary.requests
            model.context_tokens = 96_000
            await runtime.reply(window('触发整理后的新消息'))
            assert len(summary.requests) == 1
            latest = str(model.requests[-1][1])
            assert '上一轮的重要约定' in latest and '触发整理后的新消息' in latest
            assert '阈值以下' not in latest
            assert runtime._routing_db.execute('SELECT generation FROM active_sessions').fetchone()[0] == 1
            model.context_tokens = 100
            await runtime.reply(window('继续聊天'))
            assert len(summary.requests) == 1
            assert '触发整理后的新消息' in str(model.requests[-1][1])
        finally:
            await runtime.close()
    asyncio.run(scenario())


@pytest.mark.parametrize('tokens,allowed', [(128_000, True), (128_001, False)])
def test_luna_complete_request_input_limit(tmp_path, tokens, allowed):
    import json
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    async def scenario():
        requests = []

        def handle(request):
            body = json.loads(request.content)
            requests.append((request.url.path, body))
            if request.url.path.endswith('/input_tokens'):
                assert body['instructions'] and body['tools']
                assert '新消息' in str(body['input']) and '【当前实际时间】' in str(body['input'])
                return httpx.Response(200, json={'object': 'response.input_tokens', 'input_tokens': tokens})
            return httpx.Response(200, json={
                'id': 'resp_limit', 'object': 'response', 'created_at': 1, 'status': 'completed',
                'model': 'gpt-6-luna', 'output': [{'id': 'msg_limit', 'type': 'message',
                    'role': 'assistant', 'status': 'completed', 'content': [
                        {'type': 'output_text', 'text': '收到', 'annotations': []}]}],
                'usage': {'input_tokens': tokens, 'output_tokens': 2, 'total_tokens': tokens + 2,
                    'input_tokens_details': {'cached_tokens': 0},
                    'output_tokens_details': {'reasoning_tokens': 0}},
            })

        client = AsyncOpenAI(api_key='offline-placeholder', max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)))
        repo = Repository(tmp_path/'usage.sqlite3')
        model = _make_budgeted_openai(client, UsageLedger(repo, SystemClock()), SystemClock())
        runtime = ScopedAgentRuntime(tmp_path/'input-limit.sqlite3', model=model,
            extra_tools=[AgentTool(lambda: 'ok', 'check', '用于完整输入计数的工具说明')])
        try:
            if allowed:
                assert await runtime.reply(window('新消息')) == '收到'
            else:
                with pytest.raises(ValueError, match='exceeds budget'):
                    await runtime.reply(window('新消息'))
            assert sum(path.endswith('/responses') for path, _ in requests) == int(allowed)
            assert repo.conn.execute('SELECT count(*) FROM usage_reservations').fetchone()[0] == int(allowed)
        finally:
            await runtime.close()
            await client.close()
            repo.close()
    asyncio.run(scenario())


def test_pilot_queue_to_adk_tool_to_hook_format_once(tmp_path):
    import json
    from wechat_agent.hook_outbound import HookTextSender
    from wechat_agent.models import ConversationKey
    from wechat_agent.mock import SystemClock
    from wechat_agent.pilot import Pilot
    from wechat_agent.wechat_tools import WeChatTools

    async def scenario():
        (tmp_path/'data').mkdir()
        requests = []
        def hook(request):
            requests.append(request)
            return httpx.Response(200, json={'result': 'OK'})
        sender = HookTextSender(frozenset({ConversationKey('bot', 'room@chatroom')}),
                                enabled=True, transport=httpx.MockTransport(hook))
        service = WeChatTools(tmp_path, 'bot', 'room@chatroom', sender, None, None, SystemClock())
        model = FakeAdkModel([[types.Part(function_call=types.FunctionCall(
            id='commit_once', name='commit_agent_turn', args={'reply_text': '集成测试回复'},
        ))]])
        runtime = ScopedAgentRuntime(tmp_path/'integration.sqlite3', model=model,
                                      extra_tools=service.agent_tools())
        pilot = Pilot(tmp_path/'pilot.sqlite3', 'bot', 'room@chatroom', runtime.reply, sender.submit)
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.02
        observation = HookObservation(account_id='bot', conversation_id='room@chatroom',
            sender_id='wxid_person', sender_origin='hook_group_member_wxid', text='在吗',
            is_group=True, message_type=1, source_msg_id=None, capture_tick_ms=123)
        assert pilot.offer(observation) == 'queued_test'
        worker = asyncio.create_task(pilot.worker())
        try:
            for _ in range(100):
                if pilot.scheduler.state_counts().get('model_seen'):
                    break
                await asyncio.sleep(.02)
            assert pilot.scheduler.state_counts().get('model_seen') == 1
            assert len(requests) == 1 and requests[0].url.path == '/SendTextMsg'
            assert json.loads(requests[0].content)['msg'] == '集成测试回复'
            assert len(model.requests) == 1
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
            await runtime.close()
            service.db.close()
            pilot.db.close()
            sender.close()
    asyncio.run(scenario())


def test_all_business_tools_generate_adk_schema_without_tool_context():
    from wechat_agent.cross_session_tools import CrossSessionToolset
    from wechat_agent.project_tools import ProjectTools
    from wechat_agent.idle_tools import IdleActivityToolset
    from wechat_agent.wechat_tools import WeChatTools
    for cls in (CrossSessionToolset, ProjectTools, IdleActivityToolset, WeChatTools):
        for tool in cls.__new__(cls).agent_tools():
            decl = to_adk(tool)._get_declaration().model_dump(exclude_none=True)
            props = (decl.get('parameters_json_schema') or {}).get('properties', {})
            assert 'tool_context' not in props


def test_sdk_idle_summary_runs_once_and_is_in_next_session(tmp_path):
    from datetime import UTC, datetime, timedelta
    async def scenario():
        model, summary = FakeAdkModel(), FakeAdkModel(['已压缩的共同记忆'])
        runtime = ScopedAgentRuntime(tmp_path/'summary.sqlite3', model=model, summary_model=summary,
                                    initial_summary=lambda a, c: '从旧运行时承接的摘要')
        await runtime.reply(window())
        assert '从旧运行时承接的摘要' in str(model.requests[-1][1])
        runtime.idle_context_token_threshold = 1
        runtime._routing_db.execute('UPDATE active_sessions SET last_message_at=?',
            ((datetime.now(UTC) - timedelta(hours=9)).isoformat(),))
        runtime._routing_db.commit()
        assert await runtime.summarize_idle_sessions() == 1
        assert await runtime.summarize_idle_sessions() == 0
        await runtime.reply(window('新消息'))
        assert '已压缩的共同记忆' in str(model.requests[-1][1])
        assert len(summary.requests) == 1
        await runtime.close()
    asyncio.run(scenario())


def test_openai_count_input_tokens_transport_failure_raises_provider_unavailable(tmp_path):
    from google.adk.models.llm_request import LlmRequest
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.runtime_contract import ModelProviderUnavailable
    from wechat_agent.runtime_scope import model_usage_scope
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    async def scenario():
        for err_cls in (httpx.ConnectError, httpx.ReadTimeout, httpx.ProxyError):
            def handle(request, cls=err_cls):
                raise cls('network down', request=request)

            client = AsyncOpenAI(
                api_key='offline-placeholder',
                max_retries=0,
                http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
            )
            repo = Repository(tmp_path / f'count-{err_cls.__name__}.sqlite3')
            usage = UsageLedger(repo, SystemClock())
            model = _make_budgeted_openai(client, usage, SystemClock())
            req = LlmRequest(
                model='gpt-6-luna',
                contents=[types.Content(role='user', parts=[types.Part.from_text(text='hi')])],
                config=types.GenerateContentConfig(system_instruction='sys'),
            )
            try:
                with model_usage_scope('bot', 'friend'):
                    with pytest.raises(ModelProviderUnavailable):
                        async for _ in model.generate_content_async(req):
                            pass
                assert repo.conn.execute('SELECT count(*) FROM usage_reservations').fetchone()[0] == 0
            finally:
                await client.close()
                repo.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    'err_cls,expected_name,expected_state',
    [
        (httpx.ConnectError, 'ModelProviderUnavailable', 'released'),
        (httpx.ConnectTimeout, 'ModelProviderUnavailable', 'released'),
        (httpx.ProxyError, 'ModelProviderUnavailable', 'released'),
        (httpx.ReadTimeout, 'ModelCallIndeterminate', 'reserved'),
        (httpx.WriteError, 'ModelCallIndeterminate', 'reserved'),
        (httpx.RemoteProtocolError, 'ModelCallIndeterminate', 'reserved'),
    ],
)
def test_openai_post_reserve_pre_vs_post_dispatch_errors(
    tmp_path, err_cls, expected_name, expected_state,
):
    from google.adk.models.llm_request import LlmRequest
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.runtime_contract import ModelCallIndeterminate, ModelProviderUnavailable
    from wechat_agent.runtime_scope import model_usage_scope
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    expected_exc = ModelProviderUnavailable if expected_name == 'ModelProviderUnavailable' else ModelCallIndeterminate

    async def scenario():
        def handle(request):
            if request.url.path.endswith('/input_tokens'):
                return httpx.Response(200, json={'object': 'response.input_tokens', 'input_tokens': 100})
            raise err_cls('boom', request=request)

        client = AsyncOpenAI(
            api_key='offline-placeholder',
            max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        )
        repo = Repository(tmp_path / 'post-reserve.sqlite3')
        usage = UsageLedger(repo, SystemClock())
        model = _make_budgeted_openai(client, usage, SystemClock())
        req = LlmRequest(
            model='gpt-6-luna',
            contents=[types.Content(role='user', parts=[types.Part.from_text(text='hi')])],
            config=types.GenerateContentConfig(system_instruction='sys'),
        )
        try:
            with model_usage_scope('bot', 'friend'):
                with pytest.raises(expected_exc):
                    async for _ in model.generate_content_async(req):
                        pass
            state = repo.conn.execute('SELECT state FROM usage_reservations').fetchone()[0]
            assert state == expected_state
            month = SystemClock().now()[:7]
            if expected_state == 'released':
                assert usage.committed_microusd('bot', month) == 0
            else:
                assert usage.committed_microusd('bot', month) > 0
        finally:
            await client.close()
            repo.close()

    asyncio.run(scenario())


def test_adk_openai_rollback_and_mid_turn_indeterminate(tmp_path):
    import json
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.runtime_contract import (
        Content,
        ModelCallIndeterminate,
        ModelProviderUnavailable,
        Part,
        RunOptions,
    )
    from wechat_agent.runtime_scope import model_usage_scope
    from wechat_agent.session_rollover import live_session_events
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    async def scenario():
        response_bodies: list[dict] = []
        outcomes: list[object] = [
            httpx.ConnectError('fail-1'),
            httpx.ConnectTimeout('fail-2'),
            httpx.ProxyError('fail-3'),
            {
                'id': 'resp_ok', 'object': 'response', 'created_at': 1, 'status': 'completed',
                'model': 'gpt-6-luna',
                'output': [{
                    'id': 'msg_ok', 'type': 'message', 'role': 'assistant', 'status': 'completed',
                    'content': [{'type': 'output_text', 'text': '恢复回复', 'annotations': []}],
                }],
                'usage': {
                    'input_tokens': 100, 'output_tokens': 10, 'total_tokens': 110,
                    'input_tokens_details': {'cached_tokens': 0},
                    'output_tokens_details': {'reasoning_tokens': 0},
                },
            },
            {
                'id': 'resp_tool', 'object': 'response', 'created_at': 2, 'status': 'completed',
                'model': 'gpt-6-luna',
                'output': [{
                    'id': 'fc_1', 'type': 'function_call', 'call_id': 'call_ping_1',
                    'name': 'ping_tool', 'arguments': '{}', 'status': 'completed',
                }],
                'usage': {
                    'input_tokens': 100, 'output_tokens': 10, 'total_tokens': 110,
                    'input_tokens_details': {'cached_tokens': 0},
                    'output_tokens_details': {'reasoning_tokens': 0},
                },
            },
            httpx.ConnectError('mid-turn-drop'),
        ]

        def handle(request):
            if request.url.path.endswith('/input_tokens'):
                return httpx.Response(200, json={'object': 'response.input_tokens', 'input_tokens': 100})
            body = json.loads(request.content)
            response_bodies.append(body)
            outcome = outcomes.pop(0)
            if isinstance(outcome, Exception):
                raise type(outcome)(str(outcome), request=request)
            return httpx.Response(200, json=outcome)

        tool_calls: list[str] = []

        def ping_tool() -> str:
            tool_calls.append('called')
            return 'pong'

        client = AsyncOpenAI(
            api_key='offline-placeholder',
            max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        )
        repo = Repository(tmp_path / 'openai-rollback-usage.sqlite3')
        usage = UsageLedger(repo, SystemClock())
        model = _make_budgeted_openai(client, usage, SystemClock())
        framework = AdkFramework(project_compaction=True)
        sessions = framework.sessions(tmp_path / 'adk-rollback-sessions.sqlite3')
        await sessions.create_session(app_name='wechat_agent_mock', user_id='user-1', session_id='sess-1')
        runner = framework.runner(
            model=model,
            sessions=sessions,
            instruction='简洁回复',
            tools=[AgentTool(ping_tool, 'ping_tool', '测试工具')],
        )
        msg = Content(role='user', parts=[Part.from_text(text='你好在吗')])
        try:
            with model_usage_scope('bot', 'friend'):
                for _ in range(3):
                    with pytest.raises(ModelProviderUnavailable):
                        async for _ in runner.run_async(
                            user_id='user-1',
                            session_id='sess-1',
                            new_message=msg,
                            run_config=RunOptions(max_llm_calls=6),
                        ):
                            pass
                    sess = await sessions.get_session(
                        app_name='wechat_agent_mock', user_id='user-1', session_id='sess-1'
                    )
                    assert live_session_events(sess.events) == []

                month = SystemClock().now()[:7]
                assert usage.committed_microusd('bot', month) == 0

                replies = []
                async for ev in runner.run_async(
                    user_id='user-1',
                    session_id='sess-1',
                    new_message=msg,
                    run_config=RunOptions(max_llm_calls=6),
                ):
                    if ev.is_final_response():
                        replies.append(''.join(p.text or '' for p in ev.content.parts))
                assert replies == ['恢复回复']

                # On the 4th (first successful) call, the user prompt appears only ONCE.
                fourth_user_inputs = [
                    item for item in response_bodies[3]['input']
                    if isinstance(item, dict) and item.get('role') == 'user'
                    and '你好在吗' in str(item.get('content', ''))
                ]
                assert len(fourth_user_inputs) == 1

                states = [
                    r[0]
                    for r in repo.conn.execute(
                        'SELECT state FROM usage_reservations ORDER BY rowid'
                    ).fetchall()
                ]
                assert states == ['released', 'released', 'released', 'settled']

                # Mid-turn failure after tool call raises ModelCallIndeterminate without rewinding tool outputs.
                mid_msg = Content(role='user', parts=[Part.from_text(text='调用工具后断网')])
                with pytest.raises(ModelCallIndeterminate):
                    async for _ in runner.run_async(
                        user_id='user-1',
                        session_id='sess-1',
                        new_message=mid_msg,
                        run_config=RunOptions(max_llm_calls=6),
                    ):
                        pass
                assert tool_calls == ['called']
                sess = await sessions.get_session(
                    app_name='wechat_agent_mock', user_id='user-1', session_id='sess-1'
                )
                assert any(
                    getattr(part, 'function_response', None) is not None
                    for ev in live_session_events(sess.events)
                    for part in (getattr(getattr(ev, 'content', None), 'parts', None) or ())
                )
                all_states = [
                    r[0]
                    for r in repo.conn.execute(
                        'SELECT state FROM usage_reservations ORDER BY rowid'
                    ).fetchall()
                ]
                assert all_states == ['released', 'released', 'released', 'settled', 'settled', 'released']
        finally:
            await runner.close()
            await sessions.close()
            await client.close()
            repo.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ('status_code', 'expected_exc', 'expected_state'),
    [
        (429, 'ModelProviderUnavailable', 'released'),
        (403, 'ModelProviderUnavailable', 'released'),
        (400, 'ModelCallIndeterminate', 'released'),
        (404, 'ModelCallIndeterminate', 'released'),
        (500, 'ModelCallIndeterminate', 'reserved'),
    ],
)
def test_openai_http_reject_status_releases_or_retains_reservation(
    tmp_path, status_code: int, expected_exc: str, expected_state: str,
):
    from google.adk.models.llm_request import LlmRequest
    from openai import AsyncOpenAI
    from wechat_agent.mock import SystemClock
    from wechat_agent.runtime_contract import ModelCallIndeterminate, ModelProviderUnavailable
    from wechat_agent.runtime_scope import model_usage_scope
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    exc_map = {
        'ModelProviderUnavailable': ModelProviderUnavailable,
        'ModelCallIndeterminate': ModelCallIndeterminate,
    }

    async def scenario():
        def handle(request):
            if request.url.path.endswith('/responses/input_tokens'):
                return httpx.Response(200, json={'object': 'response.input_tokens', 'input_tokens': 50})
            return httpx.Response(status_code, json={'error': {'message': 'rejected', 'type': 'err'}})

        client = AsyncOpenAI(
            api_key='offline-placeholder',
            max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        )
        repo = Repository(tmp_path / f'openai-{status_code}.sqlite3')
        usage = UsageLedger(repo, SystemClock())
        model = _make_budgeted_openai(client, usage, SystemClock())
        req = LlmRequest(
            model='gpt-6-luna',
            contents=[types.Content(role='user', parts=[types.Part.from_text(text='hi')])],
            config=types.GenerateContentConfig(system_instruction='sys'),
        )
        try:
            with model_usage_scope('bot', 'room@chatroom'):
                with pytest.raises(exc_map[expected_exc]):
                    async for _ in model.generate_content_async(req):
                        pass
            state = repo.conn.execute('SELECT state FROM usage_reservations').fetchone()[0]
            assert state == expected_state
            month = SystemClock().now()[:7]
            if expected_state == 'released':
                assert usage.committed_microusd('bot', month) == 0
            else:
                assert usage.committed_microusd('bot', month) > 0
        finally:
            await client.close()
            repo.close()

    asyncio.run(scenario())


def test_openai_count_tokens_matches_create_options_and_uses_lru_cache():
    import json
    from google.adk.models.llm_request import LlmRequest
    from openai import AsyncOpenAI

    async def scenario():
        count_bodies = []

        def handle(request):
            body = json.loads(request.content)
            count_bodies.append(body)
            return httpx.Response(200, json={'object': 'response.input_tokens', 'input_tokens': len(str(body))})

        client = AsyncOpenAI(
            api_key='offline-placeholder',
            max_retries=0,
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(handle)),
        )
        model = OpenAIAdkModel('gpt-6-luna', client)
        req_a = LlmRequest(model='gpt-6-luna', contents=[types.Content(role='user', parts=[types.Part.from_text(text='alpha')])])
        req_b = LlmRequest(model='gpt-6-luna', contents=[types.Content(role='user', parts=[types.Part.from_text(text='beta')])])
        try:
            a1 = await model.count_request_tokens(req_a)
            b1 = await model.count_request_tokens(req_b)
            a2 = await model.count_request_tokens(req_a)
            b2 = await model.count_request_tokens(req_b)
            assert (a1, b1) == (a2, b2)
            assert len(count_bodies) == 2
            assert count_bodies[0]['parallel_tool_calls'] is False
            assert count_bodies[0]['reasoning'] == {'effort': 'none'}
        finally:
            await client.close()

    asyncio.run(scenario())


def test_gemini_budgeted_runtime_enforces_profile_token_compression_limits(tmp_path):
    from wechat_agent.mock import SystemClock
    from wechat_agent.storage import Repository
    from wechat_agent.usage import UsageLedger

    class FakeGeminiLlm(BaseLlm):
        _requests: list = PrivateAttr(default_factory=list)
        _token_value: int = PrivateAttr(default=100)

        def __init__(self):
            super().__init__(model='gemini-4-hydrogen')
            self._requests = []
            self._token_value = 100

        async def generate_content_async(self, llm_request, stream=False):
            self._requests.append(llm_request.model_copy(deep=True))
            yield LlmResponse(
                content=types.Content(role='model', parts=[types.Part.from_text(text='收到')]),
                usage_metadata=types.GenerateContentResponseUsageMetadata(
                    prompt_token_count=100, candidates_token_count=5, total_token_count=105,
                ),
            )

    async def scenario():
        repo = Repository(tmp_path / 'gemini-rollover-usage.sqlite3')
        usage = UsageLedger(repo, SystemClock())
        delegate = FakeGeminiLlm()
        summary = FakeAdkModel(['Gemini 主动压缩摘要'])

        async def count_tokens(*, model, contents, config=None):
            return SimpleNamespace(total_tokens=delegate._token_value)

        budgeted = BudgetedAdkModel(
            delegate,
            usage,
            SystemClock(),
            model_id='gemini-4-hydrogen',
            budget_microusd=4_000_000,
            max_input_tokens=128_000,
            idle_compression_tokens=64_000,
            active_compression_tokens=96_000,
            token_counter=count_tokens,
        )
        runtime = ScopedAgentRuntime(
            tmp_path / 'gemini-rollover.sqlite3',
            model=budgeted,
            summary_model=summary,
        )
        try:
            assert runtime.token_context_limits is True
            assert await runtime.reply(window('第一轮')) == '收到'
            delegate._token_value = 95_999
            runtime.hard_context_byte_threshold = 1
            assert await runtime.reply(window('未超 active_compression_tokens')) == '收到'
            assert not summary.requests
            delegate._token_value = 96_000
            assert await runtime.reply(window('触发 active_compression_tokens')) == '收到'
            assert len(summary.requests) == 1
            assert runtime._routing_db.execute('SELECT generation FROM active_sessions').fetchone()[0] == 1
        finally:
            await runtime.close()
            repo.close()

    asyncio.run(scenario())


def test_count_context_tokens_includes_call_budget_for_idle_turns(tmp_path):
    class CaptureCountLlm(FakeAdkModel):
        _counted_requests: list = PrivateAttr(default_factory=list)

        def __init__(self):
            super().__init__()
            self._counted_requests = []

        async def count_request_tokens(self, request) -> int:
            self._counted_requests.append(request.model_copy(deep=True))
            return 250

    async def scenario():
        model = CaptureCountLlm()
        framework = AdkFramework(project_compaction=True)
        sessions = framework.sessions(tmp_path / 'idle-count.sqlite3')
        await sessions.create_session(app_name='wechat_agent_mock', user_id='u1', session_id='s1')
        runner = framework.runner(model=model, sessions=sessions, instruction='sys')
        try:
            counted = await runner.count_context_tokens(
                session_id='s1',
                user_id='u1',
                new_message=types.Content(role='user', parts=[types.Part.from_text(text='task')]),
                max_llm_calls=24,
                show_call_budget=True,
            )
            assert counted == 250
            texts = [p.text for c in model._counted_requests[-1].contents for p in c.parts or [] if p.text]
            assert any('【本次运行调用额度】当前第 1/24 次模型调用' in t for t in texts)
        finally:
            await runner.close()
            await sessions.close()

    asyncio.run(scenario())
