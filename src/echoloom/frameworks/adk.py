"""Google ADK adapter for the application-owned conversation runtime."""
import inspect
from contextvars import ContextVar
from functools import wraps
from types import SimpleNamespace
from typing import Any

from google.adk.agents import LlmAgent
from google.adk.agents.invocation_context import LlmCallsLimitExceededError
from google.adk.agents.run_config import RunConfig
from google.adk.apps._configs import EventsCompactionConfig
from google.adk.apps.app import App
from google.adk.models.llm_request import LlmRequest
from google.adk.runners import Runner
from google.adk.sessions import DatabaseSessionService, InMemorySessionService
from google.adk.tools import FunctionTool
from google.adk.tools.tool_context import ToolContext
from google.genai import types

from ..episode_memory import SEARCH_TOOL
from ..runtime_contract import (
    BudgetExhausted,
    ModelCallIndeterminate,
    ModelCallsLimitExceeded,
    ModelProviderUnavailable,
)
from ..runtime_contract import Part as RuntimePart
from ..turn_scope import is_turn_freshness_held

_call_budget_state: ContextVar[list[int] | None] = ContextVar(
    "adk_call_budget_state",
    default=None,
)


def _convert_runtime_parts(value):
    inline = getattr(value, "inline_data", None)
    if isinstance(value, RuntimePart) and inline is not None:
        return types.Part.from_bytes(data=inline.data, mime_type=inline.mime_type)
    if inline is not None and not isinstance(value, types.Part):
        return types.Part.from_bytes(data=inline.data, mime_type=inline.mime_type)
    if isinstance(value, dict):
        return {key: _convert_runtime_parts(item) for key, item in value.items()}
    return [_convert_runtime_parts(item) for item in value] if isinstance(value, list) else value


def to_adk(tool):
    if not hasattr(tool, 'function'):
        return tool
    function = tool.function
    signature = inspect.signature(function)
    if not inspect.iscoroutinefunction(function) and 'tool_context' not in signature.parameters:
        @wraps(function)
        def invoke_sync(*args, **kwargs):
            return _convert_runtime_parts(function(*args, **kwargs))

        return FunctionTool(invoke_sync)

    @wraps(function)
    async def invoke(*args, **kwargs):
        context = kwargs.get('tool_context')
        if context is not None:
            def end():
                context._invocation_context.end_invocation = True
            kwargs['tool_context'] = SimpleNamespace(end_turn=end)
        result = function(*args, **kwargs)
        result = await result if inspect.isawaitable(result) else result
        return _convert_runtime_parts(result)

    invoke_fn: Any = invoke
    invoke_fn.__signature__ = signature.replace(
        parameters=[
            parameter.replace(annotation=ToolContext) if name == "tool_context" else parameter
            for name, parameter in signature.parameters.items()
        ]
    )
    invoke.__annotations__ = {**function.__annotations__, 'tool_context': ToolContext}
    return FunctionTool(invoke)


class _ConversationSessions(DatabaseSessionService):
    async def append_event(self, session, event):
        # Keep retrieved episodes in this invocation, without making them
        # permanent input to unrelated future turns.
        parts = event.content.parts if event.content is not None else None
        if not parts or not any(
            getattr(part.function_call or part.function_response, 'name', None) == SEARCH_TOOL
            for part in parts
        ):
            return await super().append_event(session, event)
        stored = event.model_copy(deep=True)
        stored.content.parts = [part for part in stored.content.parts if
            getattr(part.function_call or part.function_response, 'name', None) != SEARCH_TOOL]
        if not stored.content.parts:
            stored.content = None
        result = await super().append_event(session, stored)
        for index, saved in enumerate(session.events):
            if saved.id == result.id:
                session.events[index] = event
                break
        return event


def _format_call_budget_notice(call_index: int, max_llm_calls: int) -> str:
    remaining = max(0, max_llm_calls - call_index)
    notice = (
        f"【本次运行调用额度】当前第 {call_index}/{max_llm_calls} 次模型调用；"
        f"本次之后最多还可调用 {remaining} 次。"
    )
    if remaining <= 3:
        notice += "接近调用上限，停止扩展任务，依据已有工具回执完成收尾和活动总结。"
    return notice


class AdkFramework:
    def __init__(self, *, project_compaction=False, hydrate_first_session=False):
        self.uses_project_compaction = project_compaction
        self.hydrate_first_session = hydrate_first_session

    def default_model(self):
        raise ValueError("AdkFramework requires an explicit model instance")

    def sessions(self, database_path=None):
        if database_path is None:
            return InMemorySessionService()
        return _ConversationSessions(db_url=f'sqlite+aiosqlite:///{database_path.as_posix()}')

    def runner(self, *, model, sessions, instruction, tools=(), callback=None,
               app_name='wechat_agent_mock', threshold=80_000):
        adk_tools = [to_adk(tool) for tool in tools]

        def before(context, request):
            if callback:
                callback(context, request)
            budget = _call_budget_state.get()
            if budget is not None:
                budget[0] += 1
                notice = _format_call_budget_notice(budget[0], budget[1])
                request.contents = [
                    *request.contents,
                    types.Content(role="user", parts=[types.Part.from_text(text=notice)]),
                ]
            request.contents = [
                content if isinstance(content, types.Content)
                else types.Content(**content.model_dump())
                for content in request.contents
            ]

        agent = LlmAgent(name='wechat_mock_agent', model=model, instruction=instruction,
                         tools=adk_tools, before_model_callback=before)
        runner = Runner(app=App(name=app_name, root_agent=agent,
                        events_compaction_config=(None if self.uses_project_compaction else
                            EventsCompactionConfig(token_threshold=threshold, event_retention_size=8))),
                        session_service=sessions)
        return _Runner(
            runner,
            model=model,
            sessions=sessions,
            instruction=instruction,
            callback=callback,
            app_name=app_name,
            tools=tools,
            adk_tools=adk_tools,
        )


class _Runner:
    def __init__(
        self,
        runner,
        *,
        model=None,
        sessions=None,
        instruction="",
        callback=None,
        app_name="wechat_agent_mock",
        tools=(),
        adk_tools=(),
    ):
        self.delegate = runner
        self.model = model
        self.sessions = sessions
        self.instruction = instruction
        self.callback = callback
        self.app_name = app_name
        self.tools = list(tools)
        self.adk_tools = list(adk_tools)

    async def count_context_tokens(
        self,
        *,
        session_id,
        user_id=None,
        new_message=None,
        max_llm_calls: int | None = None,
        show_call_budget: bool = False,
    ):
        """Measure ADK session history plus transient tail, tools, and call-budget notice."""
        counter = getattr(self.model, "count_request_tokens", None)
        if not callable(counter) or self.sessions is None:
            return None
        session = None
        if user_id is not None:
            session = await self.sessions.get_session(
                app_name=self.app_name, user_id=user_id, session_id=session_id,
            )
        else:
            listed = await self.sessions.list_sessions(app_name=self.app_name)
            match = next(
                (item for item in getattr(listed, "sessions", ()) if item.id == session_id),
                None,
            )
            if match is not None:
                session = await self.sessions.get_session(
                    app_name=self.app_name, user_id=match.user_id, session_id=session_id,
                )
        from ..session_rollover import live_session_events

        contents = [
            event.content.model_copy(deep=True)
            for event in live_session_events(getattr(session, "events", None))
            if getattr(event, "content", None) is not None
        ]
        if new_message is not None:
            contents.append(
                new_message if isinstance(new_message, types.Content)
                else types.Content(**new_message.model_dump())
            )
        instruction = self.instruction(None) if callable(self.instruction) else self.instruction
        declarations = [
            decl
            for tool in self.adk_tools
            if (decl := tool._get_declaration()) is not None
        ]
        tools: list[Any] = [types.Tool(function_declarations=declarations)] if declarations else []
        request = LlmRequest(
            model=getattr(self.model, "model", ""),
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=instruction,
                tools=tools,
            ),
        )
        if self.callback:
            self.callback(None, request)
        if show_call_budget and max_llm_calls is not None and max_llm_calls > 0:
            request.contents = [
                *request.contents,
                types.Content(
                    role="user",
                    parts=[types.Part.from_text(text=_format_call_budget_notice(1, int(max_llm_calls)))],
                ),
            ]
        if self.callback or (show_call_budget and max_llm_calls is not None):
            request.contents = [
                content if isinstance(content, types.Content)
                else types.Content(**content.model_dump())
                for content in request.contents
            ]
        return await counter(request)

    async def run_async(self, *, new_message, run_config, **kwargs):
        content = types.Content(**new_message.model_dump())
        invocation_id: str | None = None
        progressed = False
        budget_token = (
            _call_budget_state.set([0, int(run_config.max_llm_calls)])
            if getattr(run_config, "show_call_budget", False)
            else None
        )
        try:
            async for event in self.delegate.run_async(
                new_message=content,
                run_config=RunConfig(max_llm_calls=run_config.max_llm_calls),
                yield_user_message=True,
                **kwargs,
            ):
                if getattr(event, "author", None) == "user" and invocation_id is None:
                    invocation_id = getattr(event, "invocation_id", None)
                    continue
                parts = getattr(getattr(event, "content", None), "parts", None) or []
                if any(
                    getattr(part, "function_call", None)
                    or getattr(part, "function_response", None)
                    or (getattr(part, "text", None) and not getattr(part, "thought", False))
                    for part in parts
                ):
                    progressed = True
                yield event
            if is_turn_freshness_held() and invocation_id is not None:
                await self.delegate.rewind_async(
                    user_id=kwargs["user_id"],
                    session_id=kwargs["session_id"],
                    rewind_before_invocation_id=invocation_id,
                )
        except LlmCallsLimitExceededError as exc:
            raise ModelCallsLimitExceeded(str(exc)) from exc
        except (ModelProviderUnavailable, ModelCallIndeterminate, BudgetExhausted) as exc:
            if not progressed and invocation_id is not None:
                await self.delegate.rewind_async(
                    user_id=kwargs["user_id"],
                    session_id=kwargs["session_id"],
                    rewind_before_invocation_id=invocation_id,
                )
            if isinstance(exc, ModelProviderUnavailable) and progressed:
                raise ModelCallIndeterminate(
                    "ADK provider became unavailable after turn progress"
                ) from exc
            raise
        finally:
            if budget_token is not None:
                _call_budget_state.reset(budget_token)

    async def rewind_invocation(self, *, user_id: str, session_id: str, invocation_id: str) -> None:
        await self.delegate.rewind_async(
            user_id=user_id,
            session_id=session_id,
            rewind_before_invocation_id=invocation_id,
        )

    async def close(self):
        await self.delegate.close()
