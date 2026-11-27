"""Offline-testable Gemini text adapter; no client or key is created by default."""

import hashlib
from typing import Any

import httpx
from google.genai import types

from .context import build_event_context
from .model_catalog import load_model_catalog
from .models import InboundEvent
from .ports import Clock
from .runtime_contract import ModelCallIndeterminate, ModelProviderUnavailable
from .usage import (
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
    UsageLedger,
)

_PRE_DISPATCH_CONNECT_ERRORS = (
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.ProxyError,
)
# HTTP statuses where the upstream definitively rejected the request before
# running billable inference, so the budget reservation is safe to release.
_UNBILLED_REJECT_STATUS_CODES = frozenset({400, 401, 403, 404, 413, 422, 429})
# Sub-set of reject statuses indicating throttling/auth/risk blocks that must
# trip the provider circuit breaker and back off retries.
_PROVIDER_CIRCUIT_STATUS_CODES = frozenset({401, 403, 429})


def _exception_chain(exc: BaseException):
    cur: BaseException | None = exc
    seen: set[int] = set()
    for _ in range(9):
        if cur is None or id(cur) in seen:
            return
        seen.add(id(cur))
        yield cur
        if cur.__cause__ is not None:
            cur = cur.__cause__
        elif not cur.__suppress_context__:
            cur = cur.__context__
        else:
            cur = None


def _is_pre_dispatch_connect_error(exc: BaseException) -> bool:
    """Return True if exc or its cause/context chain is a pre-dispatch connect failure."""
    return any(isinstance(cur, _PRE_DISPATCH_CONNECT_ERRORS) for cur in _exception_chain(exc))


def _http_status_code(exc: BaseException) -> int | None:
    """Extract an HTTP status code from httpx, google.genai, or openai exceptions."""
    for cur in _exception_chain(exc):
        if isinstance(cur, httpx.HTTPStatusError) and cur.response is not None:
            return int(cur.response.status_code)
        for attr in ("status_code", "code"):
            value = getattr(cur, attr, None)
            if type(value) is int and 100 <= value <= 599:
                return value
        resp = getattr(cur, "response", None)
        resp_status = getattr(resp, "status_code", None)
        if type(resp_status) is int and 100 <= resp_status <= 599:
            return resp_status
    return None


def _is_unbilled_reject_status(status: int | None) -> bool:
    return status in _UNBILLED_REJECT_STATUS_CODES


def _is_provider_circuit_status(status: int | None) -> bool:
    return status in _PROVIDER_CIRCUIT_STATUS_CODES


class SafetyBlocked(RuntimeError):
    """The provider explicitly reported a prompt or response safety block."""


def _rates(model_id: str, at: str) -> tuple[int, int]:
    """Standard paid USD micro-units per million tokens loaded from `config/models.toml`."""
    return load_model_catalog().require(model_id).rates_for(at)


def _cached_input_rate(model_id: str, at: str) -> int:
    """Standard cached-input rate in USD micro-units per million tokens."""
    return load_model_catalog().require(model_id).cached_input_rate_for(at)


def _cache_write_rate(model_id: str, at: str) -> int:
    """Cache-write / reservation rate for uncached input in USD micro-units per million tokens."""
    return load_model_catalog().require(model_id).cache_write_rate_for(at)


def _microusd(input_tokens: int, output_tokens: int, rates: tuple[int, int]) -> int:
    if input_tokens < 0 or output_tokens < 0:
        raise ValueError("negative token usage")
    numerator = input_tokens * rates[0] + output_tokens * rates[1]
    return (numerator + 999_999) // 1_000_000


class GeminiProvider:
    """One text-only SDK call, with no tools, cache, or automatic app retry."""

    def __init__(
        self, client: Any, usage: UsageLedger, clock: Clock, *, model_id: str,
        budget_microusd: int, max_input_bytes: int = 32768,
        max_output_tokens: int = 512, enabled: bool = False,
    ) -> None:
        if model_id not in load_model_catalog().gemini_model_ids():
            raise ValueError("unverified model ID")
        if budget_microusd < 0 or max_input_bytes < 1 or max_output_tokens < 1:
            raise ValueError("invalid model limits")
        self.client = client
        self.usage = usage
        self.clock = clock
        self.model_id = model_id
        self.budget_microusd = budget_microusd
        self.max_input_bytes = max_input_bytes
        self.max_output_tokens = max_output_tokens
        self.enabled = enabled

    def reply(self, event: InboundEvent) -> str:
        if not self.enabled:
            raise RuntimeError("real model calls are disabled")
        content = build_event_context(event).model_user_content()
        size = len(content.encode("utf-8"))
        if size > self.max_input_bytes:
            raise ValueError("model input exceeds byte limit")
        at = self.clock.now()
        rates = _rates(self.model_id, at)
        # One UTF-8 byte per token plus a fixed envelope margin is conservative
        # for this text-only request, but provider billing remains authoritative.
        estimate = _microusd(size + 4096, self.max_output_tokens, rates)
        request_id = "gemini:" + hashlib.sha256(
            f"{event.account_id}\0{event.conversation_id}\0{event.event_id}".encode()
        ).hexdigest()
        result = self.usage.reserve(
            request_id=request_id, account_id=event.account_id,
            conversation_id=event.conversation_id, feature="chat",
            model_id=self.model_id, maximum_microusd=estimate,
            budget_microusd=self.budget_microusd,
            feature_budget_microusd=TEXT_MODEL_DAILY_BUDGET_MICROUSD,
            budget_group="text_model",
            account_feature_budget_microusd=TEXT_MODEL_DAILY_BUDGET_MICROUSD,
        )
        if result.status != "reserved":
            raise RuntimeError(f"model request not issued: {result.status}")
        try:
            response = self.client.models.generate_content(
                model=self.model_id, contents=content,
                config=types.GenerateContentConfig(
                    max_output_tokens=self.max_output_tokens,
                    candidate_count=1,
                    http_options=types.HttpOptions(
                        timeout=20_000, retry_options=types.HttpRetryOptions(attempts=1),
                    ),
                ),
            )
        except Exception as exc:  # noqa: BLE001 - any post-send failure leaves billing uncertain
            if _is_pre_dispatch_connect_error(exc):
                self.usage.release(request_id)
                raise ModelProviderUnavailable(
                    "model provider unreachable; budget reservation released"
                ) from None
            status = _http_status_code(exc)
            if _is_provider_circuit_status(status):
                self.usage.release(request_id)
                raise ModelProviderUnavailable(
                    f"model provider rejected request (HTTP {status}); budget reservation released"
                ) from None
            if _is_unbilled_reject_status(status):
                self.usage.release(request_id)
                raise ModelCallIndeterminate(
                    f"model request rejected (HTTP {status}); budget reservation released"
                ) from None
            raise ModelCallIndeterminate(
                "model result is unknown; budget reservation retained"
            ) from None
        metadata = response.usage_metadata
        if (metadata is None or metadata.prompt_token_count is None
                or metadata.total_token_count is None):
            raise ModelCallIndeterminate("model usage missing; budget reservation retained")
        input_tokens = metadata.prompt_token_count
        output_tokens = metadata.total_token_count - input_tokens
        if output_tokens < 0:
            raise ModelCallIndeterminate("model usage inconsistent; budget reservation retained")
        actual = _microusd(input_tokens, output_tokens, rates)
        self.usage.settle(request_id, actual)
        if getattr(getattr(response, "prompt_feedback", None), "block_reason", None):
            raise SafetyBlocked("model explicitly blocked the prompt")
        candidates = getattr(response, "candidates", None) or []
        if any(str(getattr(candidate, "finish_reason", "")).upper().endswith("SAFETY")
               for candidate in candidates):
            raise SafetyBlocked("model explicitly blocked the response")
        return response.text or ""
