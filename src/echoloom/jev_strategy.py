"""Decision-model-based, advisory response strategy classification for inbound turns."""

from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlsplit

import httpx

from .context import ContextWindow
from .credentials import load_key
from .model_catalog import load_model_catalog
from .prompt_catalog import load_prompt
from .temporal_context import current_time_context

logger = logging.getLogger(__name__)

_DEFAULT_DECISIONS_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/decisions"
_DEFAULT_DECISIONS_MODEL = "gemini-decision-1"
_STRATEGIES = ("reply", "concise", "no_reply")
_WAIT_SECONDS = 4.0
_missing_key_logged = False


def _validate_decision_endpoint(url: str) -> str:
    cleaned = url.strip() or _DEFAULT_DECISIONS_ENDPOINT
    parsed = urlsplit(cleaned)
    is_loopback = parsed.hostname in ("127.0.0.1", "localhost", "::1")
    if parsed.scheme == "https" and parsed.hostname:
        return cleaned
    if parsed.scheme == "http" and is_loopback:
        return cleaned
    raise ValueError("decision_endpoint must use https:// or loopback http://")


def _strategy_questions() -> dict[str, object]:
    return {
        "strategy": {
            "type": "choice",
            "instructions": load_prompt("jev_strategy"),
            "criteria": {
                "reply": "需要正常展开回应；不只是简短接一句，也不是主要靠玩梗。",
                "concise": "值得回应，但简短回应已经足够。",
                "no_reply": "按 Jev 策略提示词中的定义判断；对象明确是别人或没有 @ 都不能单独作为不回复理由。",
            },
        },
        "multi_turn": {
            "type": "noul",
            "instructions": (
                "若决定回应，拆成 2～3 条简短连续微信气泡发送（例如先发一句情绪反应/吐槽/撒娇，"
                "再跟一句正事、追问或补刀），是否比塞进单条长消息更符合自然微信聊天节奏？"
                "（同一条消息里写多句话、简短单句接话、不需要回复，或只是等待对方稍后回复，均判为否。）"
            ),
        },
    }


def _parse_decision_answers(data: dict[str, Any]) -> tuple[dict[str, int], float] | None:
    answers = data.get("answers", {})
    strategy = answers.get("strategy", {})
    raw_probabilities = strategy.get("probabilities", {})
    if not isinstance(raw_probabilities, dict) or any(
        not isinstance(raw_probabilities.get(name), (int, float)) for name in _STRATEGIES
    ):
        return None
    probabilities = {name: float(raw_probabilities[name]) for name in _STRATEGIES}
    if any(not math.isfinite(value) for value in probabilities.values()):
        return None
    probabilities = {name: max(0.0, value) for name, value in probabilities.items()}
    total = sum(probabilities.values())
    if total <= 0:
        return None
    percentages = {name: round(value * 100 / total) for name, value in probabilities.items()}
    # Keep the displayed distribution summing to exactly 100 after rounding.
    leader = max(probabilities, key=probabilities.get)
    percentages[leader] += 100 - sum(percentages.values())
    multi_answer = answers.get("multi_turn", {})
    multi = multi_answer.get("noul", 0.0)
    if not isinstance(multi, (int, float)) or not math.isfinite(float(multi)):
        multi = 0.0
    return percentages, min(1.0, max(0.0, float(multi)))


class JevStrategyClassifier:
    """Classify each inbound message concurrently via a configurable decision model."""

    def __init__(
        self,
        *,
        google_api_key: str | None = None,
        model: str | None = None,
        endpoint: str | None = None,
        wait_seconds: float = _WAIT_SECONDS,
        call_record_sink: Callable[[dict[str, object]], None] | None = None,
    ):
        self._google_api_key = google_api_key
        self._configured_model = model.strip() if isinstance(model, str) and model.strip() else None
        self._configured_endpoint = (
            _validate_decision_endpoint(endpoint)
            if isinstance(endpoint, str) and endpoint.strip()
            else None
        )
        self.wait_seconds = wait_seconds
        self._call_record_sink = call_record_sink

    def _resolved_target(self) -> tuple[str, str]:
        roles = load_model_catalog().roles
        model = (
            self._configured_model
            or os.environ.get("ECHOLOOM_DECISION_MODEL", "").strip()
            or roles.decision_model
            or _DEFAULT_DECISIONS_MODEL
        )
        raw_endpoint = (
            self._configured_endpoint
            or os.environ.get("ECHOLOOM_DECISION_ENDPOINT", "").strip()
            or roles.decision_endpoint
            or _DEFAULT_DECISIONS_ENDPOINT
        )
        return model, _validate_decision_endpoint(raw_endpoint)

    def _google_key(self) -> str:
        gemini_key = (self._google_api_key or os.environ.get("GEMINI_API_KEY", "")).strip()
        if not gemini_key:
            try:
                gemini_key = (load_key() or "").strip()
            except OSError:
                gemini_key = ""
        return gemini_key

    async def classify_window(
        self,
        window: ContextWindow,
        *,
        persona: str,
        cross_chat_context: str = "",
        event_book_context: str = "",
    ) -> tuple[dict[str, object], ...]:
        global _missing_key_logged
        gemini_key = self._google_key()
        if not gemini_key or not window.turns:
            if not gemini_key and not _missing_key_logged:
                logger.warning("event=jev_disabled reason=api_key_not_configured")
                _missing_key_logged = True
            return ()

        # Each message gets its own target and at most five earlier turns. Messages
        # that arrived later are deliberately excluded from this message's judgment.
        async with httpx.AsyncClient(
            # Honor local HTTP(S)_PROXY configured for egress in restricted regions.
            timeout=httpx.Timeout(8.0, connect=5.0), trust_env=True
        ) as client:
            tasks = [
                asyncio.create_task(
                    self._classify_one(
                        client,
                        gemini_key,
                        window,
                        index,
                        persona,
                        cross_chat_context,
                        event_book_context,
                    )
                )
                for index, turn in enumerate(window.turns)
                if turn.direction == "inbound"
            ]
            if not tasks:
                return ()
            done, pending = await asyncio.wait(tasks, timeout=self.wait_seconds)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            entries: list[dict[str, Any]] = []
            for task in done:
                try:
                    result = task.result()
                except Exception as exc:  # noqa: BLE001 - one failed message must not discard others
                    logger.info("event=jev_message_unavailable error_type=%s", type(exc).__name__)
                    continue
                if result:
                    entries.append(result)

        if not entries:
            return ()
        entries.sort(key=lambda item: item["index"])
        return tuple(
            {
                "index": int(entry["index"]),
                "message_id": str(entry["message_id"]),
                "probabilities": dict(entry["probabilities"]),
                "multi_turn_pct": round(float(entry["multi"]) * 100),
                "model": str(entry["model"]),
            }
            for entry in entries
        )

    async def _classify_one(
        self,
        client: httpx.AsyncClient,
        gemini_key: str,
        window: ContextWindow,
        index: int,
        persona: str,
        cross_chat_context: str,
        event_book_context: str,
    ) -> dict[str, Any] | None:
        target = window.turns[index]
        previous = (*window.history_turns, *window.turns[:index])[-5:]
        focused = replace(
            window,
            turns=(target,),
            history_turns=(),
            recent_other_turns=(),
            global_timeline_turns=(),
            idle_activity_summaries=(),
            delivery_notices=(),
            scheduled_task=None,
            cancelled_followup=False,
        )
        history = ""
        if previous:
            history_window = replace(
                window,
                turns=tuple(previous),
                history_turns=(),
                recent_other_turns=(),
                global_timeline_turns=(),
                idle_activity_summaries=(),
                delivery_notices=(),
                scheduled_task=None,
                cancelled_followup=False,
            )
            history = history_window.model_user_content()[:8000]
        state: dict[str, object] = {
            "简化人物设定": persona[:5000],
            "聊天身份与类型": focused.model_session_context(),
            "联系人关系、标签、词典": focused.model_reference_context()[:7000],
            "待标注消息（最新一条；只判断这一条）": focused.model_user_content(),
            "此前最多五条历史（只作理解上下文，不作为标注对象）": history,
            "其他聊天近期上下文": cross_chat_context[:4000],
            "当前事件背景": event_book_context[:4000],
            "时间": current_time_context(),
            "目标消息ID": target.message_id or f"batch-{index + 1}",
        }
        model, endpoint = self._resolved_target()
        headers = {"x-goog-api-key": gemini_key, "Content-Type": "application/json"}
        rendered_input = "\n\n".join(
            f"【{section}】\n{value}"
            for section, value in state.items()
            if isinstance(value, str) and value.strip()
        )
        payload: dict[str, object] = {
            "model": model,
            "input": rendered_input,
            "questions": _strategy_questions(),
        }
        call_started = asyncio.get_running_loop().time()
        try:
            response = await client.post(endpoint, headers=headers, json=payload)
            response.raise_for_status()
            data = response.json()
        except (httpx.HTTPError, ValueError) as exc:
            logger.info(
                "event=jev_backend_failed backend=%s error_type=%s",
                model,
                type(exc).__name__,
            )
            raise
        if self._call_record_sink is not None:
            try:
                self._call_record_sink({
                    "timestamp": datetime.now(UTC).isoformat(),
                    "endpoint": endpoint,
                    "model": model,
                    "request": payload,
                    "response": data,
                    "latency_ms": round(
                        (asyncio.get_running_loop().time() - call_started) * 1000
                    ),
                })
            except Exception as exc:  # noqa: BLE001 - diagnostics must not affect classification
                logger.info("event=jev_call_record_failed error_type=%s", type(exc).__name__)
        parsed = _parse_decision_answers(data)
        if parsed is None:
            return None
        percentages, multi = parsed
        return {
            "index": index,
            "message_id": target.message_id or f"batch-{index + 1}",
            "probabilities": percentages,
            "multi": multi,
            "model": model,
        }
