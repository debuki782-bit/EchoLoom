"""ADK keeps episode recall usable now and transient across later turns."""
import asyncio
import inspect
import json
import shutil
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx
from google.adk.events import Event
from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr

from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.frameworks.adk import AdkFramework, to_adk
from wechat_agent.frameworks.adk_budget import BudgetedAdkModel
from wechat_agent.frameworks.google_web import GoogleWeb, GoogleWebModel
from wechat_agent.runtime_contract import AgentTool
from wechat_agent.session_runtime import ScopedAgentRuntime


class RecallModel(BaseLlm):
    _requests: list = PrivateAttr(default_factory=list)

    def __init__(self):
        super().__init__(model="offline-adk-refit")

    async def generate_content_async(self, llm_request, stream=False):
        self._requests.append(llm_request.model_copy(deep=True))
        if len(self._requests) == 1:
            part = types.Part.from_function_call(name="search_episode_memory", args={"query": "约定"})
        else:
            part = types.Part.from_text(text="记得这件事。")
        yield LlmResponse(content=types.Content(role="model", parts=[part]))


def test_episode_tool_results_are_available_only_in_current_adk_invocation(tmp_path):
    async def scenario():
        async def search_episode_memory(query: str) -> dict:
            """Search event memory for the current conversation."""
            return {"summary": "unique_episode_recall_payload"}

        model = RecallModel()
        runtime = ScopedAgentRuntime(
            tmp_path / "sessions.sqlite3", model=model, framework=AdkFramework(),
            extra_tools=[AgentTool(search_episode_memory, "search_episode_memory", "Search episodes")],
            jev_classifier=type("NoJev", (), {"classify_window": AsyncMock(return_value=())})(),
        )
        try:
            window = ContextWindow("bot", "friend", False,
                                   (ContextTurn("friend", "test", "记得约定吗"),))
            assert await runtime.reply(window) == "记得这件事。"
            assert "unique_episode_recall_payload" in model._requests[1].model_dump_json()
            assert await runtime.reply(window) == "记得这件事。"
            followup = model._requests[2].model_dump_json()
            assert "unique_episode_recall_payload" not in followup
            assert "search_episode_memory" not in str(model._requests[2].contents)
        finally:
            await runtime.close()

    asyncio.run(scenario())


def test_production_assembly_uses_google_web_and_keeps_background_memory(tmp_path, monkeypatch):
    from wechat_agent import bootstrap

    root = Path(__file__).resolve().parents[1]
    shutil.copytree(root / "config", tmp_path / "config")
    shutil.copytree(root / "prompts", tmp_path / "prompts")
    (tmp_path / "data").mkdir()
    (tmp_path / "data/live-route.json").write_text(json.dumps({
        "account": "wxid_bot", "room": "test@chatroom",
    }), encoding="utf8")
    monkeypatch.setattr(bootstrap, "load_key", lambda **kwargs: "offline-not-a-real-key")
    monkeypatch.setenv("OPENAI_API_KEY", "offline-not-a-real-key")
    monkeypatch.setenv("WECHAT_AGENT_EPISODE_RAG", "1")
    real_client = httpx.Client

    def offline_client(*args, **kwargs):
        kwargs["transport"] = httpx.MockTransport(lambda request: httpx.Response(200, json={
            "IsLogin": 1, "SqliteContactDbPath": r"D:\xwechat_files\wxid_bot_test\contact.db",
        }))
        return real_client(*args, **kwargs)

    monkeypatch.setattr(bootstrap.httpx, "Client", offline_client)
    runtimes = []

    def capture_runtime(database_path, **kwargs):
        runtimes.append((database_path, kwargs))
        return SimpleNamespace(model_id=kwargs["model"].model)

    monkeypatch.setattr(bootstrap, "ScopedAgentRuntime", capture_runtime)
    bundle = bootstrap.build_runtime_bundle(tmp_path, role="all")
    try:
        assert len(runtimes) == 2
        for index, (database_path, kwargs) in enumerate(runtimes):
            if index == 0:
                assert database_path.name == "live-sessions.sqlite3"
                assert isinstance(kwargs["framework"], AdkFramework)
                assert kwargs["framework"].uses_project_compaction
                assert isinstance(kwargs["model"], BudgetedAdkModel)
                assert kwargs["model"].model == "gemini-4-hydrogen"
            else:
                assert database_path.name == "idle-sessions.sqlite3"
                assert isinstance(kwargs["framework"], AdkFramework)
                assert kwargs["framework"].uses_project_compaction
                assert isinstance(kwargs["model"], BudgetedAdkModel)
                assert kwargs["model"].model == "gpt-6-luna"
            assert isinstance(kwargs["summary_framework"], AdkFramework)
            assert kwargs["summary_framework"].uses_project_compaction
            assert isinstance(kwargs["summary_model"], BudgetedAdkModel)
            assert kwargs["summary_model"].model == "gpt-6-luna"
            for tool in kwargs["extra_tools"]:
                # Parse every real production tool schema without issuing any API call.
                assert to_adk(tool)._get_declaration().name == tool.name
            web = next(tool for tool in kwargs["extra_tools"] if tool.name == "search_web")
            service = inspect.unwrap(web.function).__self__.service
            assert isinstance(service, GoogleWeb)
            assert isinstance(service.model, BudgetedAdkModel)
            assert isinstance(service.model._delegate, GoogleWebModel)
            assert service.model.model == "gemini-3.8-flash"
        episode = runtimes[0][1]["episode_memory"]
        assert episode.curator.model.model == "gpt-6-luna"
    finally:
        if bundle.close_episode_memory is not None:
            asyncio.run(bundle.close_episode_memory())
        if bundle.pilot is not None:
            bundle.pilot.db.close()
        if bundle.services is not None:
            bundle.services.tools.db.close()
            bundle.services.memory_store.close()
        if bundle.providers is not None:
            bundle.providers.repo.close()
        if bundle.gateway is not None:
            bundle.gateway.close()


def test_adk_conversation_compacts_through_independent_luna_framework(tmp_path):
    from test_openai_runtime import FakeAdkModel

    async def scenario():
        chat_model = RecallModel()
        organizer = FakeAdkModel(replies=["主人回来后已经安抚助手；约定明晚继续讨论。"])
        runtime = ScopedAgentRuntime(
            tmp_path / "split.sqlite3", model=chat_model,
            framework=AdkFramework(project_compaction=True), summary_model=organizer,
            summary_framework=AdkFramework(project_compaction=True),
        )
        try:
            assert runtime.runner.delegate.app.events_compaction_config is None
            history = SimpleNamespace(events=[Event(author="user", content=types.Content(
                role="user", parts=[types.Part.from_text(text="明晚继续讨论，别担心，我回来了。")],
            ))])
            summary = await runtime._summarize_session(history, "test-user")
            assert summary == "主人回来后已经安抚助手；约定明晚继续讨论。"
            assert not chat_model._requests
            assert len(organizer.requests) == 1
            assert "明晚继续讨论" in str(organizer.requests[0][1])
        finally:
            await runtime.close()

    asyncio.run(scenario())
