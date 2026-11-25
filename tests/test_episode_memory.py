import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr

from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.episode_memory import (
    DIMENSIONS,
    EpisodeDraft,
    EpisodeMemory,
    InformationAgentResult,
    RecallTurn,
    recall_turn,
)
from wechat_agent.mock import SystemClock
from wechat_agent.runtime_scope import model_usage_scope
from wechat_agent.storage import Repository
from wechat_agent.usage import UsageLedger


class _FakeCuratorLlm(BaseLlm):
    _replies: list[str] = PrivateAttr(default_factory=list)
    _payloads: list[str] = PrivateAttr(default_factory=list)

    def __init__(self, replies: list[InformationAgentResult]) -> None:
        super().__init__(model="gpt-6-luna")
        self._replies = [item.model_dump_json() for item in replies]
        self._payloads = []

    async def generate_content_async(self, llm_request, stream=False):
        text = "".join(
            part.text or ""
            for content in llm_request.contents
            for part in (content.parts or ())
        )
        self._payloads.append(text)
        body = self._replies.pop(0) if self._replies else InformationAgentResult(events=[]).model_dump_json()
        yield LlmResponse(
            content=types.Content(role="model", parts=[types.Part.from_text(text=body)])
        )


def test_episode_ingest_recall_scope_and_timeout(tmp_path, monkeypatch):
    async def scenario():
        repo = Repository(tmp_path / "usage.sqlite3")
        usage = UsageLedger(repo, SystemClock())
        embedding = [1.0] + [0.0] * (DIMENSIONS - 1)
        embed = AsyncMock(return_value=SimpleNamespace(
            data=[SimpleNamespace(embedding=embedding)], usage=SimpleNamespace(total_tokens=12),
        ))
        curator_model = _FakeCuratorLlm(
            [InformationAgentResult(events=[EpisodeDraft(summary="小林计划周末去厦门。")])]
        )
        memory = EpisodeMemory(
            tmp_path / "episodes.sqlite3", SimpleNamespace(embeddings=SimpleNamespace(create=embed)),
            curator_model, usage, account_budget=10000, account_text_budget=10000,
        )

        def turn(room, message_id, history=()):
            return RecallTurn(ContextWindow(
                "account", room, True,
                (ContextTurn("person", "hook", "周末的旅行安排", message_id=message_id),),
                history_turns=history,
            ), eligible=True)

        with model_usage_scope("account", "room"):
            original = turn("room", "original")
            memory.enqueue(original)
            await memory.jobs.join()
            memory.enqueue(original)
            await memory.jobs.join()
            assert len(curator_model._payloads) == 1
            assert json.loads(curator_model._payloads[0])["new"][0]["text"] == "周末的旅行安排"
            await memory.automatic(original)
            assert original.automatic == []
            # Visible source messages suppress duplicate injection.
            visible = turn("room", "other", original.window.turns)
            await memory.automatic(visible)
            assert visible.automatic == []
            new = turn("room", "new")
            await memory.automatic(new)
            assert [row["summary"] for row in new.automatic] == ["小林计划周末去厦门。"]
            token = recall_turn.set(new)
            try:
                assert (await memory.search_episode_memory("上次说去哪里旅行"))["events"] == []
                assert (await memory.search_episode_memory("再查一次"))["status"] == "limit"
            finally:
                recall_turn.reset(token)

            async def slow(**_):
                await asyncio.sleep(10)

            embed.side_effect = slow
            monkeypatch.setattr("wechat_agent.episode_memory.AUTO_SECONDS", 0.02)
            waiting = turn("room", "waiting")
            await asyncio.wait_for(memory.automatic(waiting), timeout=1)
            assert waiting.automatic == []
        with model_usage_scope("account", "other-room"):
            foreign = turn("other-room", "elsewhere")
            await memory.automatic(foreign)
            assert foreign.automatic == []
        await memory.close()
        repo.close()

    asyncio.run(scenario())


def test_information_agent_receives_message_time_and_separate_speaker_identity(tmp_path):
    async def scenario():
        repo = Repository(tmp_path / "usage.sqlite3")
        curator_model = _FakeCuratorLlm([InformationAgentResult(events=[])])
        memory = EpisodeMemory(
            tmp_path / "episodes.sqlite3", SimpleNamespace(), curator_model,
            UsageLedger(repo, SystemClock()), account_budget=10000, account_text_budget=10000,
        )
        turn = RecallTurn(ContextWindow(
            "bot", "room", True,
            (ContextTurn("person", "hook", "我明天去厦门", message_id="new",
                         message_time={"local_minute": "2026-11-06 23:00"}),),
            history_turns=(ContextTurn(
                "bot", "outbound", "也许可以去上海", direction="outbound",
                message_time={"local_minute": "2026-11-06 22:59"},
            ),),
        ), eligible=True)
        await memory._curate(turn)
        payload = json.loads(curator_model._payloads[0])
        assert payload["new"][0]["message_time"] == "2026-11-06 23:00"
        assert payload["new"][0]["speaker_id"] == "person"
        assert payload["recent"][0]["direction"] == "outbound"
        assert payload["recent"][0]["speaker_id"] == "bot"
        assert payload["recent"][0]["message_time"] == "2026-11-06 22:59"
        await memory.close()
        repo.close()

    asyncio.run(scenario())
