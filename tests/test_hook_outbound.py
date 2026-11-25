"""No real sends: exercise request format and uncertain outcome handling."""

import json
import random

import httpx
import pytest

from wechat_agent.adapters.hook import HookGateway
from wechat_agent.adapters.hook_text import SendPacer
from wechat_agent.hook_outbound import HookTextSender
from wechat_agent.models import ConversationKey, ReplyDraft
from wechat_agent.wechat_gateway import GatewayConfig

KEY = ConversationKey("bot", "test@chatroom")
DRAFT = ReplyDraft("one", KEY, "测试", 0)


def test_sender_disabled_or_wrong_account_never_makes_a_request() -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail("request must not be made")

    sender = HookTextSender(frozenset({KEY}), transport=httpx.MockTransport(forbidden))
    assert sender.submit(DRAFT).status == "rejected"
    sender.enabled = True
    assert sender.submit(ReplyDraft("two", ConversationKey("other", KEY.conversation_id),
                                    "hi", 0)).status == "rejected"
    sender.close()


def test_dynamically_admitted_group_route_can_send_only_after_explicit_addition() -> None:
    new_key = ConversationKey("bot", "new-room@chatroom")
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200, json={"ret": 0, "queued": True}, request=request)

    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(respond))
    try:
        draft = ReplyDraft("new", new_key, "hello", 0)
        assert sender.submit(draft).status == "rejected"
        sender.allow_conversation(new_key)
        assert sender.submit(draft).status == "queued"
        assert len(calls) == 1
    finally:
        sender.close()


@pytest.mark.parametrize(("body", "status", "outcome"), [
    ({"ret": 0, "queued": True}, "queued", "unknown"),
    ({"ret": -1}, "rejected", "failed"),
    ({"ret": -2}, "rejected", "failed"),
    ({"ret": -3, "queued": False}, "unknown", "unknown"),
    ({"ret": False, "queued": True}, "unknown", "unknown"),
    ({"ret": 0}, "unknown", "unknown"),
    ([], "unknown", "unknown"),
])
def test_response_mapping_never_converts_queue_to_sent(body, status, outcome) -> None:
    calls = []

    def respond(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert str(request.url) == "http://127.0.0.1:30001/SendTextMsg"
        assert json.loads(request.content) == {"wxidorgid": KEY.conversation_id, "msg": "测试"}
        return httpx.Response(200, json=body)

    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(respond))
    try:
        result = sender.submit(DRAFT)
        assert result.status == status and result.ledger_outcome == outcome
        assert len(calls) == 1
    finally:
        sender.close()


def test_timeout_is_unknown_with_one_attempt() -> None:
    calls = []

    def timeout(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        raise httpx.ReadTimeout("private response must not escape", request=request)

    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(timeout))
    try:
        assert sender.submit(DRAFT).ledger_outcome == "unknown"
        assert len(calls) == 1
    finally:
        sender.close()


def test_redirect_is_unknown_and_not_followed() -> None:
    calls = []

    def redirect(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(307, headers={"location": "https://example.invalid"})

    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(redirect))
    try:
        assert sender.submit(DRAFT).status == "unknown"
        assert len(calls) == 1
    finally:
        sender.close()


@pytest.mark.parametrize("text", ["", "bad\0text", "文" * 6000])
def test_invalid_text_never_calls_hook(text: str) -> None:
    def forbidden(request: httpx.Request) -> httpx.Response:
        pytest.fail("invalid text must not be submitted")

    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(forbidden))
    try:
        assert sender.submit(ReplyDraft("bad", KEY, text, 0)).status == "rejected"
    finally:
        sender.close()


@pytest.mark.parametrize("response", [
    httpx.Response(200, text="not json"),
    httpx.Response(500, json={"ret": 0, "queued": True}),
    httpx.Response(200, content=b"x" * 4097),
])
def test_invalid_http_response_is_unknown(response: httpx.Response) -> None:
    sender = HookTextSender(frozenset({KEY}), enabled=True,
                            transport=httpx.MockTransport(lambda request: response))
    try:
        assert sender.submit(DRAFT).status == "unknown"
    finally:
        sender.close()


def test_send_pacer_enforces_randomized_cooldown_without_busy_sleep() -> None:
    clock = [100.0]
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock[0] += seconds

    pacer = SendPacer(
        0.55,
        1.35,
        rng=random.Random(7),
        monotonic=lambda: clock[0],
        sleep=fake_sleep,
    )
    assert pacer.wait_turn() == 0.0
    wait_1 = pacer.wait_turn()
    wait_2 = pacer.wait_turn()
    assert 0.55 <= wait_1 <= 1.35
    assert 0.55 <= wait_2 <= 1.35
    assert wait_1 != wait_2
    assert sleeps == [wait_1, wait_2]

    clock[0] += 5.0
    assert pacer.wait_turn() == 0.0


def test_hook_gateway_shares_pacer_across_send_methods(tmp_path) -> None:
    seen: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return httpx.Response(200, json={"ret": 0, "queued": True})

    gateway = HookGateway(GatewayConfig(
        tmp_path,
        account_id="bot",
        allowed_conversations=frozenset({KEY}),
        send_enabled=True,
        transport=httpx.MockTransport(respond),
    ))
    waits: list[float] = []
    clock = [10.0]

    def fake_sleep(seconds: float) -> None:
        waits.append(seconds)
        clock[0] += seconds

    shared = SendPacer(0.55, 1.35, rng=random.Random(11), monotonic=lambda: clock[0], sleep=fake_sleep)
    gateway._pacer = shared
    gateway._sender._pacer = shared
    try:
        assert gateway.send_text(ReplyDraft("bad", KEY, "", 0)).status == "rejected"
        assert waits == []

        assert gateway.send_text(DRAFT).status == "queued"
        assert gateway.send_image(KEY.conversation_id, tmp_path / "a.png")[0] == "queued"
        assert gateway.send_quote(KEY.conversation_id, "hi", ("wxid_member", "m", KEY.conversation_id, "src"))[0] == "accepted_by_server"
        assert seen == ["/SendTextMsg", "/SendImgMsg", "/ForwardXMLMsg"]
        assert len(waits) == 2
        assert all(0.55 <= w <= 1.35 for w in waits)
    finally:
        gateway.close()
