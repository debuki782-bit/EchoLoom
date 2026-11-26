"""Offline tests for Google Decision 1 classification."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from echoloom.context import ContextTurn, ContextWindow
from echoloom.jev_strategy import JevStrategyClassifier


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
        "echoloom.jev_strategy.httpx.AsyncClient",
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
        "echoloom.jev_strategy.httpx.AsyncClient",
        lambda **kwargs: real_async_client(transport=httpx.MockTransport(handler), **kwargs),
    )
    classifier = JevStrategyClassifier(google_api_key="test-gemini-key")
    results = asyncio.run(classifier.classify_window(_sample_window(), persona="我是助手"))
    assert len(urls) == 1
    assert urls[0].endswith("/v1beta/decisions")
    assert results == ()


def test_decision_model_and_endpoint_can_be_swapped_via_config_and_env(
    tmp_path, monkeypatch,
) -> None:
    import shutil

    from echoloom import local_config

    shutil.copytree(
        Path(__file__).resolve().parents[1] / "config",
        tmp_path / "config",
    )
    (tmp_path / ".local").mkdir()
    (tmp_path / ".local" / "models.toml").write_text(
        '[roles]\n'
        'decision_model = "custom-decision-2"\n'
        'decision_endpoint = "https://example.invalid/v1/decisions"\n',
        encoding="utf-8",
    )
    monkeypatch.setattr(local_config, "ROOT", tmp_path)
    monkeypatch.setattr(local_config, "_USE_LOCAL_OVERRIDES", True)

    seen: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append((str(request.url), json.loads(request.content.decode("utf-8"))))
        return httpx.Response(200, json={
            "answers": {
                "strategy": {
                    "type": "choice",
                    "probabilities": {"reply": 0.6, "concise": 0.3, "no_reply": 0.1},
                },
                "multi_turn": {"type": "noul", "noul": 0.2},
            }
        })

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        "echoloom.jev_strategy.httpx.AsyncClient",
        lambda **kwargs: real_async_client(transport=httpx.MockTransport(handler), **kwargs),
    )

    classifier = JevStrategyClassifier(google_api_key="test-gemini-key")
    results = asyncio.run(classifier.classify_window(_sample_window(), persona="我是助手"))
    assert results[0]["model"] == "custom-decision-2"
    assert seen[0][0] == "https://example.invalid/v1/decisions"
    assert seen[0][1]["model"] == "custom-decision-2"

    # Environment variables override catalog roles cleanly.
    monkeypatch.setenv("ECHOLOOM_DECISION_MODEL", "env-decision-model")
    monkeypatch.setenv("ECHOLOOM_DECISION_ENDPOINT", "http://127.0.0.1:9999/decisions")
    results_env = asyncio.run(classifier.classify_window(_sample_window(), persona="我是助手"))
    assert results_env[0]["model"] == "env-decision-model"
    assert seen[1][0] == "http://127.0.0.1:9999/decisions"

