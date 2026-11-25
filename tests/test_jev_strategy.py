"""Offline tests for Google Decision 1 classification."""

from __future__ import annotations

import asyncio
import json

import httpx

from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.jev_strategy import JevStrategyClassifier


def _sample_window() -> ContextWindow:
    return ContextWindow(
        account_id="bot",
        conversation_id="room@chatroom",
        is_group=True,
        turns=(
            ContextTurn(
                sender_id="wxid_owner",
                sender_origin="hook_group_member_wxid",
                text="怎么大半夜不回我消息",
                message_id="msg-1",
            ),
        ),
    )


def test_google_decision_primary_excludes_noul_criteria_and_parses_scores(monkeypatch) -> None:
    requests: list[tuple[str, dict, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8"))
        requests.append((str(request.url), dict(request.headers), body))
        return httpx.Response(200, json={
            "answers": {
                "strategy": {
                    "type": "choice",
                    "choice": "reply",
                    "probabilities": {"reply": 0.82, "concise": 0.14, "no_reply": 0.04},
                },
                "multi_turn": {
                    "type": "noul",
                    "noul": 0.788,
                },
            },
            "usage": {"total_input_tokens": 120, "total_output_tokens": 0},
        })

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        "wechat_agent.jev_strategy.httpx.AsyncClient",
        lambda **kwargs: real_async_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    classifier = JevStrategyClassifier(google_api_key="test-gemini-key")
    results = asyncio.run(classifier.classify_window(_sample_window(), persona="我是助手"))
    assert len(results) == 1
    assert results[0]["model"] == "gemini-decision-1"
    assert results[0]["probabilities"] == {"reply": 82, "concise": 14, "no_reply": 4}
    assert results[0]["multi_turn_pct"] == 79
    url, headers, payload = requests[0]
    assert url.endswith("/v1beta/decisions")
    assert headers["x-goog-api-key"] == "test-gemini-key"
    assert payload["model"] == "gemini-decision-1"
    assert "input" in payload and "criteria" not in payload["questions"]["multi_turn"]


def test_google_decision_returns_empty_without_fallback_when_google_fails(monkeypatch) -> None:
    urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        urls.append(str(request.url))
        return httpx.Response(503, json={"error": {"message": "unavailable"}})

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        "wechat_agent.jev_strategy.httpx.AsyncClient",
        lambda **kwargs: real_async_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    classifier = JevStrategyClassifier(google_api_key="test-gemini-key")
    results = asyncio.run(classifier.classify_window(_sample_window(), persona="我是助手"))
    assert len(urls) == 1
    assert urls[0].endswith("/v1beta/decisions")
    assert results == ()
