"""Budget each actual ADK model invocation without cloud access."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from google.adk.flows.llm_flows.tools._caller import _build_function_response_content
from google.adk.models.llm_request import LlmRequest
from google.adk.models.llm_response import LlmResponse
from google.adk.tools import FunctionTool
from google.genai import types
from pydantic import PrivateAttr

from echoloom.adk_budget import (
    BudgetedAdkModel,
    ModelProviderUnavailable,
    _is_pre_dispatch_connect_error,
    image_access_blocked,
    model_usage_scope,
)
from echoloom.adk_runner import DeterministicLlm, ScopedAdkRunner
from echoloom.agent_tools import AgentToolStatus
from echoloom.context import ContextTurn, ContextWindow
from echoloom.frameworks.adk import AdkFramework
from echoloom.models import InboundEvent
from echoloom.providers import GeminiProvider, ModelCallIndeterminate
from echoloom.runtime_contract import (
    AgentTool,
    RunOptions,
)
from echoloom.runtime_contract import (
    Content as RuntimeContent,
)
from echoloom.runtime_contract import (
    Part as RuntimePart,
)
from echoloom.storage import Repository
from echoloom.usage import UsageLedger


class Clock:
    def now(self) -> str:
        return "2026-10-29T00:00:00+00:00"


class PricedFake(DeterministicLlm):
    async def generate_content_async(self, llm_request, stream=False):
        self.requests.append(llm_request)
        assert llm_request.config.http_options.retry_options.attempts == 1
        yield LlmResponse(
            content=types.Content(role="model", parts=[types.Part.from_text(text="offline")]),
            usage_metadata=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=100, candidates_token_count=10, total_token_count=110,
            ),
        )


def window() -> ContextWindow:
    return ContextWindow("bot", "friend", False, (ContextTurn("friend", "test", "hello"),))


def test_adk_budget_settles_each_call_and_exhaustion_prevents_delegate(tmp_path: Path) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "budget.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.5-flash-lite"
        wrapped = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model,
                                   budget_microusd=100_000)
        runner = ScopedAdkRunner(model=wrapped)
        try:
            assert await runner.reply(window()) == "offline"
            assert await runner.reply(window()) == "offline"
            assert usage.committed_microusd("bot", "2026-10") == 110
            assert repo.conn.execute("SELECT count(*) FROM usage_reservations").fetchone()[0] == 2
        finally:
            await runner.close()
        blocked = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model,
                                   budget_microusd=0)
        runner = ScopedAdkRunner(model=blocked)
        try:
            with pytest.raises(RuntimeError, match="budget exhausted"):
                await runner.reply(window())
            assert len(fake.requests) == 2
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


def test_budgeted_model_applies_configured_http_timeout(tmp_path: Path) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "timeout.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.8-flash"
        count_calls = 0

        async def count_tokens(**_kwargs):
            nonlocal count_calls
            count_calls += 1
            return SimpleNamespace(total_tokens=100)

        wrapped = BudgetedAdkModel(
            fake,
            usage,
            Clock(),
            model_id=fake.model,
            budget_microusd=100_000,
            token_counter=count_tokens,
            http_timeout_ms=60_000,
        )
        runner = ScopedAdkRunner(model=wrapped)
        try:
            assert await runner.reply(window()) == "offline"
            assert count_calls == 1
            assert fake.requests[0].config.http_options.timeout == 60_000
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


def test_invalid_cached_token_metadata_falls_back_to_full_input_price(tmp_path: Path) -> None:
    class BadCacheMetadata(PricedFake):
        async def generate_content_async(self, llm_request, stream=False):
            self.requests.append(llm_request)
            yield LlmResponse(
                content=types.Content(
                    role="model", parts=[types.Part.from_text(text="ok")],
                ),
                usage_metadata=types.GenerateContentResponseUsageMetadata(
                    prompt_token_count=100,
                    candidates_token_count=10,
                    total_token_count=110,
                    cached_content_token_count=150,
                ),
            )

    async def scenario() -> None:
        repo = Repository(tmp_path / "bad-cache-metadata.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = BadCacheMetadata()
        fake.model = "gemini-3.5-flash-lite"
        runner = ScopedAdkRunner(
            model=BudgetedAdkModel(
                fake, usage, Clock(), model_id=fake.model, budget_microusd=100_000,
            ),
        )
        try:
            assert await runner.reply(window()) == "ok"
            assert usage.committed_microusd("bot", "2026-10") > 0
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


def test_adk_missing_usage_retains_reservation(tmp_path: Path) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "unknown.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = DeterministicLlm()
        fake.model = "gemini-3.5-flash-lite"
        wrapped = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model,
                                   budget_microusd=100_000)
        runner = ScopedAdkRunner(model=wrapped)
        try:
            with pytest.raises(ModelCallIndeterminate, match="usage missing"):
                await runner.reply(window())
            assert usage.committed_microusd("bot", "2026-10") > 0
            assert repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0] == "reserved"
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


class PricedToolFake(PricedFake):
    async def generate_content_async(self, llm_request, stream=False):
        self.requests.append(llm_request)
        has_result = any(part.function_response for content in llm_request.contents
                         for part in content.parts or [])
        part = (types.Part.from_text(text="tool result received") if has_result else
                types.Part.from_function_call(name="read_gateway_status", args={}))
        yield LlmResponse(
            content=types.Content(role="model", parts=[part]),
            usage_metadata=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=100, candidates_token_count=10, total_token_count=110,
            ),
        )


def test_tool_round_trip_is_two_separately_reserved_model_calls(tmp_path: Path) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "tools-budget.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedToolFake()
        fake.model = "gemini-3.5-flash-lite"
        runner = ScopedAdkRunner(
            model=BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model,
                                   budget_microusd=100_000),
            status_tool=AgentToolStatus(),
        )
        try:
            assert await runner.reply(window()) == "tool result received"
            assert len(fake.requests) == 2
            assert usage.committed_microusd("bot", "2026-10") == 110
            assert repo.conn.execute(
                "SELECT count(*) FROM usage_reservations WHERE state='settled'"
            ).fetchone()[0] == 2
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


def test_route_is_required_and_media_and_oversize_input_are_rejected_before_call(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "preflight.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.5-flash-lite"
        model = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model,
                                 budget_microusd=100_000)

        async def consume(request: LlmRequest) -> None:
            async for _ in model.generate_content_async(request):
                pass

        try:
            with pytest.raises(ValueError, match="bound route"):
                await consume(LlmRequest())
            with model_usage_scope("bot", "friend"):
                with pytest.raises(ValueError, match="token counter"):
                    await consume(LlmRequest(config=types.GenerateContentConfig(
                        system_instruction=types.Content(parts=[types.Part.from_bytes(
                            data=b"not a real image", mime_type="image/png",
                        )]),
                    )))
                with pytest.raises(ValueError, match="input exceeds"):
                    await consume(LlmRequest(contents=[types.Content(parts=[
                        types.Part.from_text(text="x" * 40000),
                    ])]))
            # Scope must reset even after failures; no accidental prior-chat billing.
            with pytest.raises(ValueError, match="bound route"):
                await consume(LlmRequest())
            assert not fake.requests
            assert usage.committed_microusd("bot", "2026-10") == 0
        finally:
            repo.close()

    asyncio.run(scenario())


def _image_tool_result_content(image_data: bytes):
    tool = SimpleNamespace(name="inspect_wechat_image", response_scheduling=None)
    return _build_function_response_content(
        tool,
        {
            "image_id": "image-1",
            "image_part": types.Part.from_bytes(data=image_data, mime_type="image/png"),
            "image_attached_to_main_agent": True,
        },
        "call-1",
    )


def test_adk_image_tool_result_reaches_main_model_and_uses_one_shared_budget_row(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "image-budget.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.8-flash"

        async def count_tokens(**kwargs):
            return SimpleNamespace(total_tokens=100)

        model = BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=5_000_000,
            token_counter=count_tokens,
        )
        original_content = _image_tool_result_content(b"png-pixels")
        request = LlmRequest(contents=[original_content])
        try:
            with model_usage_scope("bot", "room"):
                async for _ in model.generate_content_async(request):
                    pass
                assert not image_access_blocked()
            sent_function_response = fake.requests[0].contents[0].parts[0].function_response
            assert sent_function_response.parts[0].inline_data.data == b"png-pixels"
            assert request.contents[0].parts[0].function_response.parts[0].inline_data.data == b"png-pixels"
            rows = repo.conn.execute(
                "SELECT feature,reserved_microusd,actual_microusd FROM usage_reservations"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["feature"] == "adk_chat_image_context"
            assert rows[0]["actual_microusd"] == 113
        finally:
            repo.close()

    asyncio.run(scenario())


def test_cached_input_tokens_use_the_official_cached_rate(tmp_path: Path) -> None:
    async def scenario() -> None:
        class CachedFake(PricedFake):
            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request)
                yield LlmResponse(
                    content=types.Content(role="model", parts=[
                        types.Part.from_text(text="offline"),
                    ]),
                    usage_metadata=types.GenerateContentResponseUsageMetadata(
                        prompt_token_count=100, cached_content_token_count=50,
                        candidates_token_count=10, total_token_count=110,
                    ),
                )

        repo = Repository(tmp_path / "cached-input.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = CachedFake()
        fake.model = "gemini-3.8-flash"
        model = BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=5_000_000,
            token_counter=lambda **kwargs: asyncio.sleep(0, result=SimpleNamespace(total_tokens=100)),
        )
        try:
            with model_usage_scope("bot", "room"):
                async for _ in model.generate_content_async(LlmRequest(contents=[
                    types.Content(role="user", parts=[types.Part.from_text(text="hi")]),
                ])):
                    pass
            row = repo.conn.execute(
                "SELECT actual_microusd FROM usage_reservations"
            ).fetchone()
            assert row["actual_microusd"] == 79
        finally:
            repo.close()

    asyncio.run(scenario())


def test_image_subbudget_exhaustion_strips_only_request_copy_and_continues_text_turn(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "image-budget-exhausted.sqlite3")
        usage = UsageLedger(repo, Clock())
        assert usage.reserve(
            request_id="image-cap-full", account_id="bot", conversation_id="room",
            feature="adk_chat_image_context", model_id="gemini-3.8-flash",
            maximum_microusd=1_000_000, budget_microusd=5_000_000,
            feature_budget_microusd=1_000_000,
        ).status == "reserved"
        assert usage.settle("image-cap-full", 1_000_000) == "settled"

        fake = PricedFake()
        fake.model = "gemini-3.8-flash"

        async def count_tokens(**kwargs):
            return SimpleNamespace(total_tokens=100)

        model = BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=5_000_000,
            token_counter=count_tokens,
        )
        request = LlmRequest(contents=[_image_tool_result_content(b"preserved-pixels")])
        original_response = request.contents[0].parts[0].function_response
        try:
            with model_usage_scope("bot", "room"):
                async for _ in model.generate_content_async(request):
                    pass
                assert image_access_blocked()
            forwarded = fake.requests[0].contents[0].parts[0].function_response
            assert forwarded.parts is None
            assert "预算已用完" in forwarded.response["image_access_notice"]
            assert original_response.parts[0].inline_data.data == b"preserved-pixels"
            rows = repo.conn.execute(
                "SELECT feature,actual_microusd FROM usage_reservations ORDER BY created_at"
            ).fetchall()
            assert [row["feature"] for row in rows] == [
                "adk_chat_image_context", "adk_chat",
            ]
            assert not image_access_blocked()
        finally:
            repo.close()

    asyncio.run(scenario())


def test_real_adk_runner_persists_image_function_response_for_following_turns(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        class ImageToolFake(DeterministicLlm):
            _image_seen: list[bool] = PrivateAttr(default_factory=list)
            _tool_requested: bool = PrivateAttr(default=False)

            def __init__(self) -> None:
                super().__init__()
                self.model = "gemini-3.8-flash"

            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request.model_copy(deep=True))
                has_image = any(
                    response_part.inline_data
                    for content in llm_request.contents
                    for part in content.parts or []
                    if part.function_response is not None
                    for response_part in part.function_response.parts or []
                )
                self._image_seen.append(has_image)
                if not self._tool_requested:
                    self._tool_requested = True
                    part = types.Part.from_function_call(
                            name="attach_test_image", args={},
                    )
                else:
                    part = types.Part.from_text(text="图片已进入当前会话")
                yield LlmResponse(
                    content=types.Content(role="model", parts=[part]),
                    usage_metadata=types.GenerateContentResponseUsageMetadata(
                        prompt_token_count=100, candidates_token_count=10,
                        total_token_count=110,
                    ),
                )

        async def attach_test_image() -> dict:
            """Attach one saved image to the current model turn."""
            return {"image_part": types.Part.from_bytes(
                data=b"adk-image-pixels", mime_type="image/png",
            )}

        fake = ImageToolFake()
        runner = ScopedAdkRunner(
            tmp_path / "adk-sessions.sqlite3",
            model=fake,
            extra_tools=[FunctionTool(func=attach_test_image)],
        )
        try:
            assert await runner.reply(window()) == "图片已进入当前会话"
            assert await runner.reply(window()) == "图片已进入当前会话"
            assert fake._image_seen == [False, True, True]
        finally:
            await runner.close()

    asyncio.run(scenario())


def test_cancelled_adk_call_keeps_reservation(tmp_path: Path) -> None:
    async def scenario() -> None:
        entered = asyncio.Event()

        class WaitingFake(PricedFake):
            async def generate_content_async(self, llm_request, stream=False):
                entered.set()
                await asyncio.Event().wait()
                yield LlmResponse()  # makes this an async generator

        repo = Repository(tmp_path / "cancelled.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = WaitingFake()
        fake.model = "gemini-3.5-flash-lite"
        runner = ScopedAdkRunner(model=BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=100_000,
        ))
        try:
            task = asyncio.create_task(runner.reply(window()))
            await asyncio.wait_for(entered.wait(), timeout=5)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert usage.committed_microusd("bot", "2026-10") > 0
            assert repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0] == "reserved"
        finally:
            await runner.close()
            repo.close()

    asyncio.run(scenario())


def test_is_pre_dispatch_connect_error_chain_and_safety() -> None:
    req = httpx.Request("POST", "https://example.invalid")
    assert _is_pre_dispatch_connect_error(httpx.ConnectError("refused", request=req))
    assert _is_pre_dispatch_connect_error(httpx.ConnectTimeout("timeout", request=req))
    assert _is_pre_dispatch_connect_error(httpx.ProxyError("proxy", request=req))

    # Explicit __cause__ and unsuppressed __context__ are followed; suppressed context is not.
    wrapped_cause = RuntimeError("outer")
    wrapped_cause.__cause__ = httpx.ConnectError("refused", request=req)
    assert _is_pre_dispatch_connect_error(wrapped_cause)

    try:
        try:
            raise httpx.ProxyError("proxy", request=req)
        except httpx.ProxyError:
            raise RuntimeError("implicit")
    except RuntimeError as exc:
        assert _is_pre_dispatch_connect_error(exc)

    try:
        try:
            raise httpx.ConnectError("refused", request=req)
        except httpx.ConnectError:
            raise RuntimeError("suppressed") from None
    except RuntimeError as exc:
        assert not _is_pre_dispatch_connect_error(exc)

    # Cycle-safe and bounded at 8 hops.
    cyc_a = RuntimeError("a")
    cyc_b = RuntimeError("b")
    cyc_a.__cause__ = cyc_b
    cyc_b.__cause__ = cyc_a
    assert not _is_pre_dispatch_connect_error(cyc_a)

    chain_ok: BaseException = httpx.ConnectError("leaf", request=req)
    for idx in range(8):
        nxt = RuntimeError(f"hop-{idx}")
        nxt.__cause__ = chain_ok
        chain_ok = nxt
    assert _is_pre_dispatch_connect_error(chain_ok)

    too_deep = RuntimeError("hop-8")
    too_deep.__cause__ = chain_ok
    assert not _is_pre_dispatch_connect_error(too_deep)

    # Post-dispatch transport errors must never be treated as pre-dispatch.
    for err in (
        httpx.ReadTimeout("read", request=req),
        httpx.WriteTimeout("write", request=req),
        httpx.ReadError("read", request=req),
        httpx.WriteError("write", request=req),
        httpx.RemoteProtocolError("eof", request=req),
    ):
        assert not _is_pre_dispatch_connect_error(err)
        outer = RuntimeError("wrapped")
        outer.__cause__ = err
        assert not _is_pre_dispatch_connect_error(outer)


def test_gemini_provider_releases_on_connect_error_and_retains_on_read_timeout(
    tmp_path: Path,
) -> None:
    req = httpx.Request("POST", "https://example.invalid")

    def fake_client(exc: Exception):
        def generate_content(**_kwargs):
            raise exc

        return SimpleNamespace(models=SimpleNamespace(generate_content=generate_content))

    repo = Repository(tmp_path / "provider-errors.sqlite3")
    usage = UsageLedger(repo, Clock())
    try:
        p_connect = GeminiProvider(
            fake_client(httpx.ConnectError("refused", request=req)),
            usage,
            Clock(),
            model_id="gemini-3.5-flash-lite",
            budget_microusd=100_000,
            enabled=True,
        )
        with pytest.raises(ModelProviderUnavailable):
            p_connect.reply(InboundEvent("e-connect", "bot", "friend", "friend", "你好"))
        row = repo.conn.execute(
            "SELECT state FROM usage_reservations WHERE request_id LIKE 'gemini:%'"
        ).fetchone()
        assert row["state"] == "released"
        assert usage.committed_microusd("bot", "2026-10") == 0

        p_read = GeminiProvider(
            fake_client(httpx.ReadTimeout("slow", request=req)),
            usage,
            Clock(),
            model_id="gemini-3.5-flash-lite",
            budget_microusd=100_000,
            enabled=True,
        )
        with pytest.raises(ModelCallIndeterminate):
            p_read.reply(InboundEvent("e-read", "bot", "friend", "friend", "你好"))
        states = [
            r[0]
            for r in repo.conn.execute(
                "SELECT state FROM usage_reservations ORDER BY rowid"
            ).fetchall()
        ]
        assert states == ["released", "reserved"]
        assert usage.committed_microusd("bot", "2026-10") > 0
    finally:
        repo.close()


def test_adk_count_tokens_connect_error_raises_provider_unavailable_without_reservation(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        req = httpx.Request("POST", "https://example.invalid")
        for err in (
            httpx.ConnectError("refused", request=req),
            httpx.ReadTimeout("timeout", request=req),
            TimeoutError("overall"),
            ConnectionError("reset"),
        ):
            repo = Repository(tmp_path / f"count-{type(err).__name__}.sqlite3")
            usage = UsageLedger(repo, Clock())
            fake = PricedFake()
            fake.model = "gemini-3.5-flash-lite"

            async def failing_counter(*, _err=err, **_kwargs):
                raise _err

            model = BudgetedAdkModel(
                fake,
                usage,
                Clock(),
                model_id=fake.model,
                budget_microusd=100_000,
                token_counter=failing_counter,
            )
            try:
                with model_usage_scope("bot", "room"), pytest.raises(ModelProviderUnavailable):
                    async for _ in model.generate_content_async(
                        LlmRequest(contents=[types.Content(role="user", parts=[types.Part.from_text(text="hi")])])
                    ):
                        pass
                assert repo.conn.execute("SELECT count(*) FROM usage_reservations").fetchone()[0] == 0
                assert usage.committed_microusd("bot", "2026-10") == 0
                assert fake.requests == []
            finally:
                repo.close()

        # Non-transport ValueError and CancelledError stay untouched.
        repo = Repository(tmp_path / "count-value-err.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.5-flash-lite"

        async def value_error_counter(**_kwargs):
            raise ValueError("bad payload")

        async def cancelled_counter(**_kwargs):
            raise asyncio.CancelledError()

        try:
            for fn, expected in ((value_error_counter, ValueError), (cancelled_counter, asyncio.CancelledError)):
                model = BudgetedAdkModel(
                    fake,
                    usage,
                    Clock(),
                    model_id=fake.model,
                    budget_microusd=100_000,
                    token_counter=fn,
                )
                with model_usage_scope("bot", "room"), pytest.raises(expected):
                    async for _ in model.generate_content_async(
                        LlmRequest(contents=[types.Content(role="user", parts=[types.Part.from_text(text="hi")])])
                    ):
                        pass
                assert repo.conn.execute("SELECT count(*) FROM usage_reservations").fetchone()[0] == 0
        finally:
            repo.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "exc_factory,expected_exc,expected_state",
    [
        (lambda r: httpx.ConnectError("refused", request=r), ModelProviderUnavailable, "released"),
        (lambda r: httpx.ConnectTimeout("connect timeout", request=r), ModelProviderUnavailable, "released"),
        (lambda r: httpx.ProxyError("proxy down", request=r), ModelProviderUnavailable, "released"),
        (
            lambda r: httpx.HTTPStatusError("rate limited", request=r, response=httpx.Response(429, request=r)),
            ModelProviderUnavailable,
            "released",
        ),
        (
            lambda r: httpx.HTTPStatusError("forbidden", request=r, response=httpx.Response(403, request=r)),
            ModelProviderUnavailable,
            "released",
        ),
        (
            lambda r: httpx.HTTPStatusError("bad request", request=r, response=httpx.Response(400, request=r)),
            ModelCallIndeterminate,
            "released",
        ),
        (
            lambda r: httpx.HTTPStatusError("not found", request=r, response=httpx.Response(404, request=r)),
            ModelCallIndeterminate,
            "released",
        ),
        (
            lambda r: httpx.HTTPStatusError("server error", request=r, response=httpx.Response(503, request=r)),
            ModelCallIndeterminate,
            "reserved",
        ),
        (lambda r: httpx.ReadTimeout("read timeout", request=r), ModelCallIndeterminate, "reserved"),
        (lambda r: httpx.WriteTimeout("write timeout", request=r), ModelCallIndeterminate, "reserved"),
        (lambda r: httpx.ReadError("read err", request=r), ModelCallIndeterminate, "reserved"),
        (lambda r: httpx.WriteError("write err", request=r), ModelCallIndeterminate, "reserved"),
        (lambda r: httpx.RemoteProtocolError("eof", request=r), ModelCallIndeterminate, "reserved"),
    ],
)
def test_adk_generate_content_pre_vs_post_dispatch_errors(
    tmp_path: Path, exc_factory, expected_exc, expected_state: str,
) -> None:
    async def scenario() -> None:
        req = httpx.Request("POST", "https://example.invalid")

        class FailingLlm(DeterministicLlm):
            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request)
                raise exc_factory(req)
                yield LlmResponse()

        repo = Repository(tmp_path / "dispatch.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = FailingLlm()
        fake.model = "gemini-3.5-flash-lite"
        model = BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=100_000,
        )
        try:
            with model_usage_scope("bot", "room"), pytest.raises(expected_exc):
                async for _ in model.generate_content_async(
                    LlmRequest(contents=[types.Content(role="user", parts=[types.Part.from_text(text="hi")])])
                ):
                    pass
            state = repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0]
            assert state == expected_state
            if expected_state == "released":
                assert usage.committed_microusd("bot", "2026-10") == 0
            else:
                assert usage.committed_microusd("bot", "2026-10") > 0
        finally:
            repo.close()

    asyncio.run(scenario())


def test_real_adk_framework_rewinds_provider_unavailable_and_preserves_mid_turn_progress(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        req = httpx.Request("POST", "https://example.invalid")
        outcomes: list[object] = [
            httpx.ConnectError("fail-1", request=req),
            httpx.ConnectTimeout("fail-2", request=req),
            httpx.ProxyError("fail-3", request=req),
            types.Part.from_text(text="恢复后的回复"),
            types.Part.from_function_call(name="ping_tool", args={}),
            httpx.ConnectError("mid-turn-drop", request=req),
        ]

        class SequenceLlm(DeterministicLlm):
            def __init__(self) -> None:
                super().__init__()
                self.model = "gemini-3.5-flash-lite"

            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request.model_copy(deep=True))
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                yield LlmResponse(
                    content=types.Content(role="model", parts=[outcome]),
                    usage_metadata=types.GenerateContentResponseUsageMetadata(
                        prompt_token_count=100, candidates_token_count=10, total_token_count=110,
                    ),
                )

        tool_calls: list[str] = []

        def ping_tool() -> str:
            """Return deterministic status."""
            tool_calls.append("called")
            return "pong"

        repo = Repository(tmp_path / "adk-rewind-budget.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = SequenceLlm()
        model = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model, budget_microusd=500_000)
        framework = AdkFramework()
        sessions = framework.sessions(tmp_path / "adk-rewind-sessions.sqlite3")
        await sessions.create_session(app_name="wechat_agent_mock", user_id="u1", session_id="s1")
        runner = framework.runner(
            model=model,
            sessions=sessions,
            instruction="简洁回复",
            tools=[AgentTool(ping_tool, "ping_tool", "测试工具")],
        )
        msg = RuntimeContent(role="user", parts=[RuntimePart.from_text(text="请帮我检查")])
        try:
            with model_usage_scope("bot", "room"):
                for _ in range(3):
                    with pytest.raises(ModelProviderUnavailable):
                        async for _ in runner.run_async(
                            user_id="u1",
                            session_id="s1",
                            new_message=msg,
                            run_config=RunOptions(max_llm_calls=6),
                        ):
                            pass
                assert usage.committed_microusd("bot", "2026-10") == 0

                replies: list[str] = []
                async for event in runner.run_async(
                    user_id="u1",
                    session_id="s1",
                    new_message=msg,
                    run_config=RunOptions(max_llm_calls=6),
                ):
                    if event.is_final_response() and event.content:
                        replies.append("".join(p.text or "" for p in event.content.parts or []))
                assert replies == ["恢复后的回复"]

                # On the 4th (first successful) call, the rewound prompt appears only ONCE.
                fourth_prompt_texts = [
                    p.text
                    for c in fake.requests[3].contents
                    for p in c.parts or []
                    if getattr(p, "text", None) and "请帮我检查" in p.text
                ]
                assert fourth_prompt_texts == ["请帮我检查"]

                states = [
                    r[0]
                    for r in repo.conn.execute(
                        "SELECT state FROM usage_reservations ORDER BY rowid"
                    ).fetchall()
                ]
                assert states == ["released", "released", "released", "settled"]

                # Mid-turn failure after tool execution must raise ModelCallIndeterminate without rewinding.
                mid_turn_msg = RuntimeContent(
                    role="user", parts=[RuntimePart.from_text(text="触发工具后断网")]
                )
                with pytest.raises(ModelCallIndeterminate):
                    async for _ in runner.run_async(
                        user_id="u1",
                        session_id="s1",
                        new_message=mid_turn_msg,
                        run_config=RunOptions(max_llm_calls=6),
                    ):
                        pass
                assert tool_calls == ["called"]
                session = await sessions.get_session(
                    app_name="wechat_agent_mock", user_id="u1", session_id="s1"
                )
                assert any(
                    getattr(part, "function_response", None) is not None
                    for ev in session.events
                    for part in (getattr(getattr(ev, "content", None), "parts", None) or [])
                )
                # The latest event must NOT be a rewind for the mid-turn invocation.
                assert getattr(getattr(session.events[-1], "actions", None), "rewind_before_invocation_id", None) is None
                all_states = [
                    r[0]
                    for r in repo.conn.execute(
                        "SELECT state FROM usage_reservations ORDER BY rowid"
                    ).fetchall()
                ]
                assert all_states == ["released", "released", "released", "settled", "settled", "released"]
        finally:
            await runner.close()
            await sessions.close()
            repo.close()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("status_code", "expected_exc", "expected_state"),
    [
        (429, ModelProviderUnavailable, "released"),
        (401, ModelProviderUnavailable, "released"),
        (403, ModelProviderUnavailable, "released"),
        (400, ModelCallIndeterminate, "released"),
        (404, ModelCallIndeterminate, "released"),
        (500, ModelCallIndeterminate, "reserved"),
    ],
)
def test_adk_genai_api_error_status_releases_or_retains_reservation(
    tmp_path: Path, status_code: int, expected_exc, expected_state: str,
) -> None:
    from google.genai import errors as genai_errors

    async def scenario() -> None:
        class GenaiFailingLlm(DeterministicLlm):
            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request)
                err_cls = genai_errors.ClientError if status_code < 500 else genai_errors.ServerError
                raise err_cls(status_code, {"error": {"code": status_code, "message": "rejected"}}, None)
                yield LlmResponse()

        repo = Repository(tmp_path / f"genai-{status_code}.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = GenaiFailingLlm()
        fake.model = "gemini-4-hydrogen"
        model = BudgetedAdkModel(
            fake, usage, Clock(), model_id=fake.model, budget_microusd=500_000,
        )
        try:
            with model_usage_scope("bot", "room"), pytest.raises(expected_exc):
                async for _ in model.generate_content_async(
                    LlmRequest(contents=[types.Content(role="user", parts=[types.Part.from_text(text="hi")])])
                ):
                    pass
            state = repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0]
            assert state == expected_state
            if expected_state == "released":
                assert usage.committed_microusd("bot", "2026-10") == 0
            else:
                assert usage.committed_microusd("bot", "2026-10") > 0
        finally:
            repo.close()

    asyncio.run(scenario())


def test_gemini_mldev_count_tokens_uses_official_generate_content_request_and_caches(
    tmp_path: Path,
) -> None:
    import json

    from google import genai

    async def scenario() -> None:
        bodies: list[dict] = []

        def handler(request):
            bodies.append(json.loads(request.content))
            assert request.url.path.endswith("/models/gemini-4-hydrogen:countTokens")
            return httpx.Response(200, json={"totalTokens": 321})

        client = genai.Client(
            api_key="offline-placeholder",
            http_options=types.HttpOptions(
                async_client_args={"transport": httpx.MockTransport(handler)},
                retry_options=types.HttpRetryOptions(attempts=1),
            ),
        )
        repo = Repository(tmp_path / "official-count.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-4-hydrogen"
        model = BudgetedAdkModel(
            fake,
            usage,
            Clock(),
            model_id="gemini-4-hydrogen",
            budget_microusd=500_000,
            token_counter=client.aio.models.count_tokens,
        )
        assert model.supports_token_counting is True
        decl = types.FunctionDeclaration(
            name="ping",
            description="check ping",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={"target": types.Schema(type=types.Type.STRING)},
            ),
        )
        request = LlmRequest(
            model="gemini-4-hydrogen",
            contents=[types.Content(role="user", parts=[types.Part.from_text(text="你好")])],
            config=types.GenerateContentConfig(
                system_instruction="官方系统提示词",
                tools=[types.Tool(function_declarations=[decl])],
            ),
        )
        try:
            assert await model.count_request_tokens(request) == 321
            assert await model.count_request_tokens(request) == 321
            assert len(bodies) == 1
            body = bodies[0]
            assert "generateContentRequest" in body
            gen_req = body["generateContentRequest"]
            assert gen_req["model"] == "models/gemini-4-hydrogen"
            assert gen_req["systemInstruction"]["parts"][0]["text"] == "官方系统提示词"
            assert gen_req["tools"][0]["functionDeclarations"][0]["name"] == "ping"
            assert len(gen_req["contents"]) == 1
            serialized = json.dumps(body, ensure_ascii=False)
            assert "系统提示词：" not in serialized
            assert "工具定义 JSON：" not in serialized
        finally:
            await client.aio.aclose()
            client.close()
            repo.close()

    asyncio.run(scenario())


def test_image_subbudget_fallback_preserves_web_reservation_allowance(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        repo = Repository(tmp_path / "image-web-fallback.sqlite3")
        usage = UsageLedger(repo, Clock())
        assert usage.reserve(
            request_id="img-full",
            account_id="bot",
            conversation_id="room",
            feature="adk_chat_image_context",
            model_id="gemini-3.8-flash",
            maximum_microusd=1_000_000,
            budget_microusd=5_000_000,
            feature_budget_microusd=1_000_000,
        ).status == "reserved"
        assert usage.settle("img-full", 1_000_000) == "settled"

        fake = PricedFake()
        fake.model = "gemini-3.8-flash"

        async def count_tokens(**_kwargs):
            return SimpleNamespace(total_tokens=100)

        model = BudgetedAdkModel(
            fake,
            usage,
            Clock(),
            model_id=fake.model,
            budget_microusd=5_000_000,
            token_counter=count_tokens,
        )
        request = LlmRequest(
            contents=[_image_tool_result_content(b"pixels")],
            config=types.GenerateContentConfig(
                tools=[types.Tool(google_search=types.GoogleSearch())],
            ),
        )
        try:
            with model_usage_scope("bot", "room"):
                async for _ in model.generate_content_async(request):
                    pass
            row = repo.conn.execute(
                "SELECT reserved_microusd FROM usage_reservations WHERE feature='adk_chat'"
            ).fetchone()
            # Expect web search reservation ceiling (1_048_576 * 0.75 + 12_000 * 3.75 + 8 * 14_000 = 943_432),
            # not the tiny 100-token non-web reservation.
            assert row["reserved_microusd"] > 900_000
        finally:
            repo.close()

    asyncio.run(scenario())


def test_adk_streaming_collects_partials_and_logs_indeterminate_timeout(
    tmp_path: Path, caplog,
) -> None:
    async def scenario() -> None:
        req = httpx.Request("POST", "https://example.invalid")
        refreshed: list[str] = []

        class StreamingSplitFake(DeterministicLlm):
            _mode: str = PrivateAttr(default="ok")

            def __init__(self) -> None:
                super().__init__()
                self.model = "gemini-4-hydrogen"

            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append((llm_request, stream))
                assert stream is True
                if self._mode == "ok":
                    yield LlmResponse(
                        content=types.Content(role="model", parts=[types.Part.from_text(text="先")]),
                        partial=True,
                    )
                    yield LlmResponse(
                        content=types.Content(role="model", parts=[types.Part.from_text(text="你好")]),
                        partial=False,
                    )
                    yield LlmResponse(
                        content=types.Content(
                            role="model",
                            parts=[types.Part.from_function_call(name="ping_tool", args={})],
                        ),
                        usage_metadata=types.GenerateContentResponseUsageMetadata(
                            prompt_token_count=200,
                            candidates_token_count=20,
                            total_token_count=220,
                        ),
                        partial=False,
                    )
                else:
                    yield LlmResponse(
                        content=types.Content(role="model", parts=[types.Part.from_text(text="半截")]),
                        partial=True,
                    )
                    raise httpx.ReadTimeout("stream stalled", request=req)

        repo = Repository(tmp_path / "streaming.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = StreamingSplitFake()
        model = BudgetedAdkModel(
            fake,
            usage,
            Clock(),
            model_id=fake.model,
            budget_microusd=1_000_000,
            use_streaming=True,
            on_indeterminate=lambda: refreshed.append("refreshed"),
        )
        request = LlmRequest(
            contents=[types.Content(role="user", parts=[types.Part.from_text(text="测试流式")])],
        )
        try:
            with model_usage_scope("bot", "room"):
                items = [item async for item in model.generate_content_async(request)]
            assert len(items) == 1
            assert len(items[0].content.parts) == 2
            assert items[0].content.parts[0].text == "你好"
            assert items[0].content.parts[1].function_call.name == "ping_tool"
            assert fake.requests[0][1] is True
            assert repo.conn.execute("SELECT state FROM usage_reservations").fetchone()[0] == "settled"

            fake._mode = "timeout_mid_stream"
            caplog.set_level("WARNING", logger="echoloom.frameworks.adk_budget")
            with model_usage_scope("bot", "room"), pytest.raises(ModelCallIndeterminate):
                async for _ in model.generate_content_async(request):
                    pass
            assert refreshed == ["refreshed"]
            states = [
                r[0]
                for r in repo.conn.execute("SELECT state FROM usage_reservations ORDER BY rowid").fetchall()
            ]
            assert states == ["settled", "reserved"]
            assert any(
                "event=adk_call_indeterminate" in rec.message
                and "error_type=ReadTimeout" in rec.message
                and "saw_partial=True" in rec.message
                for rec in caplog.records
            )
        finally:
            repo.close()

    asyncio.run(scenario())


def test_real_adk_framework_rewinds_unprogressed_indeterminate_and_budget_exhausted(
    tmp_path: Path,
) -> None:
    from echoloom.runtime_contract import BudgetExhausted

    async def scenario() -> None:
        req = httpx.Request("POST", "https://example.invalid")
        outcomes: list[object] = [
            httpx.ReadTimeout("timeout-1", request=req),
            types.Part.from_text(text="超时后重跑正常回复"),
        ]

        class TimeoutThenOkLlm(DeterministicLlm):
            def __init__(self) -> None:
                super().__init__()
                self.model = "gemini-3.5-flash-lite"

            async def generate_content_async(self, llm_request, stream=False):
                self.requests.append(llm_request.model_copy(deep=True))
                outcome = outcomes.pop(0)
                if isinstance(outcome, Exception):
                    raise outcome
                yield LlmResponse(
                    content=types.Content(role="model", parts=[outcome]),
                    usage_metadata=types.GenerateContentResponseUsageMetadata(
                        prompt_token_count=100,
                        candidates_token_count=10,
                        total_token_count=110,
                    ),
                )

        repo = Repository(tmp_path / "rewind-indeterminate.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = TimeoutThenOkLlm()
        model = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model, budget_microusd=500_000)
        framework = AdkFramework()
        sessions = framework.sessions(tmp_path / "rewind-indet-sessions.sqlite3")
        await sessions.create_session(app_name="wechat_agent_mock", user_id="u1", session_id="s1")
        runner = framework.runner(model=model, sessions=sessions, instruction="简洁")
        msg = RuntimeContent(role="user", parts=[RuntimePart.from_text(text="只该出现一次的消息")])
        try:
            with model_usage_scope("bot", "room"):
                with pytest.raises(ModelCallIndeterminate):
                    async for _ in runner.run_async(
                        user_id="u1",
                        session_id="s1",
                        new_message=msg,
                        run_config=RunOptions(max_llm_calls=4),
                    ):
                        pass
                replies: list[str] = []
                async for event in runner.run_async(
                    user_id="u1",
                    session_id="s1",
                    new_message=msg,
                    run_config=RunOptions(max_llm_calls=4),
                ):
                    if event.is_final_response() and event.content:
                        replies.append("".join(p.text or "" for p in event.content.parts or []))
                assert replies == ["超时后重跑正常回复"]
                seen_on_second = [
                    p.text
                    for c in fake.requests[1].contents
                    for p in c.parts or []
                    if getattr(p, "text", None) and "只该出现一次的消息" in p.text
                ]
                assert seen_on_second == ["只该出现一次的消息"]

                # Exhaust budget before calling delegate: must also rewind cleanly.
                zero_model = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model, budget_microusd=0)
                zero_runner = framework.runner(model=zero_model, sessions=sessions, instruction="简洁")
                try:
                    with pytest.raises(BudgetExhausted):
                        async for _ in zero_runner.run_async(
                            user_id="u1",
                            session_id="s1",
                            new_message=RuntimeContent(
                                role="user",
                                parts=[RuntimePart.from_text(text="不该堆积的预算耗尽消息")],
                            ),
                            run_config=RunOptions(max_llm_calls=4),
                        ):
                            pass
                    session = await sessions.get_session(
                        app_name="wechat_agent_mock", user_id="u1", session_id="s1",
                    )
                    assert getattr(
                        getattr(session.events[-1], "actions", None),
                        "rewind_before_invocation_id",
                        None,
                    ) is not None
                finally:
                    await zero_runner.close()
        finally:
            await runner.close()
            await sessions.close()
            repo.close()

    asyncio.run(scenario())


def test_budgeted_model_serializes_pydantic_class_response_schema(tmp_path: Path) -> None:
    from echoloom.episode_memory import InformationAgentResult
    from echoloom.frameworks.adk_budget import _serialized_request_bytes

    async def scenario() -> None:
        repo = Repository(tmp_path / "schema-budget.sqlite3")
        usage = UsageLedger(repo, Clock())
        fake = PricedFake()
        fake.model = "gemini-3.5-flash-lite"
        model = BudgetedAdkModel(fake, usage, Clock(), model_id=fake.model, budget_microusd=100_000)
        request = LlmRequest(
            model=fake.model,
            contents=[types.Content(role="user", parts=[types.Part.from_text(text="extract")])],
            config=types.GenerateContentConfig(
                response_schema=InformationAgentResult,
                response_mime_type="application/json",
            ),
        )
        assert _serialized_request_bytes(request) > len(
            request.model_dump_json(exclude={"tools_dict": True, "config": {"response_schema": True}}).encode()
        )
        try:
            with model_usage_scope("bot", "room"):
                responses = [
                    resp async for resp in model.generate_content_async(request, stream=False)
                ]
            assert len(responses) == 1
            assert usage.committed_microusd("bot", "2026-10") == 55
        finally:
            repo.close()

    asyncio.run(scenario())
