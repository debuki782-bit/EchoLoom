"""Google's real SDK wire format, retrieval verification and complete billing."""

import asyncio
import json

import httpx
import pytest
from google import genai
from google.genai import types

from echoloom.frameworks.adk_budget import BudgetedAdkModel
from echoloom.frameworks.google_web import GoogleWebModel
from echoloom.mock import SystemClock
from echoloom.runtime_scope import model_usage_scope
from echoloom.storage import Repository
from echoloom.usage import UsageLedger
from echoloom.web_tools import WebSearchUsage, WebToolset


def exercise(tmp_path, *, search=True, queries=None, urls=None, budget=4_000_000,
             finish="STOP", missing_usage=False, fail=False):
    async def run():
        bodies = []

        def handler(request):
            bodies.append(json.loads(request.content))
            if fail:
                raise httpx.ReadTimeout("offline timeout")
            candidate = {
                "content": {"role": "model", "parts": [{"text": "网页摘要"}]},
                "finishReason": finish,
                "groundingMetadata": {
                    "webSearchQueries": queries or [],
                    "groundingChunks": [{"web": {"title": "来源", "uri": "https://example.com/page"}}],
                },
                "urlContextMetadata": {"urlMetadata": urls or []},
            }
            payload = {"candidates": [candidate]}
            if not missing_usage:
                payload["usageMetadata"] = {
                    "promptTokenCount": 100, "cachedContentTokenCount": 60,
                    "toolUsePromptTokenCount": 1000,
                    "candidatesTokenCount": 20, "thoughtsTokenCount": 10,
                    "totalTokenCount": 1130,
                }
            return httpx.Response(200, json=payload)

        client = genai.Client(api_key="offline-placeholder", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(handler)},
            retry_options=types.HttpRetryOptions(attempts=1),
        ))
        repo = Repository(tmp_path / "usage.sqlite3")
        clock = SystemClock()
        model = BudgetedAdkModel(
            GoogleWebModel("gemini-3.8-flash", client), UsageLedger(repo, clock), clock,
            model_id="gemini-3.8-flash", budget_microusd=budget, max_output_tokens=4096,
        )
        usage = WebSearchUsage(repo)
        tools = WebToolset(model, usage)
        try:
            with model_usage_scope("bot", "room"):
                result = (await tools.search_web("公开查询") if search else
                          await tools.read_web_page("https://example.com/page"))
            rows = [dict(row) for row in repo.conn.execute(
                "SELECT state,actual_microusd,reserved_microusd FROM usage_reservations")]
            count = repo.conn.execute("SELECT coalesce(sum(query_count),0) FROM web_search_usage").fetchone()[0]
            return result, bodies, rows, count
        finally:
            await client.aio.aclose()
            client.close()
            repo.close()

    return asyncio.run(run())


def test_search_uses_google_only_and_bills_tool_inputs_thoughts_and_queries(tmp_path):
    result, bodies, rows, count = exercise(tmp_path, queries=["query one", "query two"])
    assert result["ok"] and result["provider"] == "google"
    assert result["search_queries"] == count == 2
    assert "https://example.com/page" in result["result"]
    assert bodies[0]["tools"] == [{"googleSearch": {}}]
    config = bodies[0]["generationConfig"]
    assert config["thinkingConfig"]["thinking_level"] == "LOW"
    assert config["maxOutputTokens"] == 4096
    assert bodies[0]["contents"] == [{"role": "user", "parts": [{"text": "公开查询"}]}]
    # (40*.75 + 60*.075 + 1000*.75 + 30*3.75) microUSD + 2*$0.014.
    assert rows[0]["state"] == "settled"
    assert rows[0]["actual_microusd"] == 28_897


def test_url_reader_requires_retrieval_success_and_does_not_enable_search(tmp_path):
    result, bodies, rows, count = exercise(tmp_path, search=False, urls=[{
        "retrievedUrl": "https://example.com/page#part",
        "urlRetrievalStatus": "URL_RETRIEVAL_STATUS_SUCCESS",
    }])
    assert result["ok"] and result["requested_url"] == "https://example.com/page"
    assert bodies[0]["tools"] == [{"urlContext": {}}]
    assert rows[0]["actual_microusd"] == 897 and count == 0


@pytest.mark.parametrize("search,queries,urls", [
    (True, [], []),
    (False, ["search result is not page content"], []),
    (False, [], [{"retrievedUrl": "https://example.com/other",
                  "urlRetrievalStatus": "URL_RETRIEVAL_STATUS_SUCCESS"}]),
    (False, [], [{"retrievedUrl": "https://example.com/page",
                  "urlRetrievalStatus": "URL_RETRIEVAL_STATUS_ERROR"}]),
])
def test_text_and_citations_cannot_substitute_for_retrieval(tmp_path, search, queries, urls):
    result, _, rows, _ = exercise(tmp_path, search=search, queries=queries, urls=urls)
    assert not result["ok"] and rows[0]["state"] == "settled"


def test_budget_exhaustion_prevents_provider_request(tmp_path):
    result, bodies, rows, _ = exercise(tmp_path, budget=1)
    assert not result["ok"] and not bodies and not rows


@pytest.mark.parametrize("kwargs", [{"missing_usage": True}, {"fail": True}])
def test_unknown_billing_retains_reservation_without_retry(tmp_path, kwargs):
    result, bodies, rows, _ = exercise(tmp_path, **kwargs)
    assert not result["ok"] and len(bodies) == 1
    assert rows[0]["state"] == "reserved"


def test_truncated_summary_does_not_claim_complete_read(tmp_path):
    result, _, rows, _ = exercise(tmp_path, queries=["query"], finish="MAX_TOKENS")
    assert not result["ok"] and rows[0]["state"] == "settled"


def test_google_web_model_uses_refreshed_client_provider(tmp_path):
    async def scenario():
        def handler(request):
            return httpx.Response(200, json={
                "candidates": [{
                    "content": {"role": "model", "parts": [{"text": "热切换成功"}]},
                    "finishReason": "STOP",
                    "groundingMetadata": {
                        "webSearchQueries": ["q"],
                        "groundingChunks": [{"web": {"title": "来源", "uri": "https://example.com/ok"}}],
                    },
                }],
                "usageMetadata": {
                    "promptTokenCount": 10, "cachedContentTokenCount": 0,
                    "toolUsePromptTokenCount": 0, "candidatesTokenCount": 5,
                    "thoughtsTokenCount": 0, "totalTokenCount": 15,
                },
            })

        old_client = genai.Client(api_key="offline-old", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(handler)},
            retry_options=types.HttpRetryOptions(attempts=1),
        ))
        new_client = genai.Client(api_key="offline-new", http_options=types.HttpOptions(
            async_client_args={"transport": httpx.MockTransport(handler)},
            retry_options=types.HttpRetryOptions(attempts=1),
        ))
        current = old_client
        repo = Repository(tmp_path / "usage.sqlite3")
        clock = SystemClock()
        model = BudgetedAdkModel(
            GoogleWebModel("gemini-3.8-flash", lambda: current),
            UsageLedger(repo, clock), clock,
            model_id="gemini-3.8-flash", budget_microusd=4_000_000, max_output_tokens=4096,
        )
        tools = WebToolset(model, WebSearchUsage(repo))
        try:
            current = new_client
            await old_client.aio.aclose()
            with model_usage_scope("bot", "room"):
                result = await tools.search_web("切换后查询")
            assert result["ok"] and "热切换成功" in result["result"]
        finally:
            await new_client.aio.aclose()
            old_client.close()
            new_client.close()
            repo.close()

    asyncio.run(scenario())
