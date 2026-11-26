"""Deterministic tests for pre-send freshness policy and cache-aware session recycling."""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from google.adk.events import Event
from google.genai import types
from test_openai_runtime import FakeAdkModel

from wechat_agent.agent_profiles import load_agent_profiles
from wechat_agent.contacts import ContactIdentity
from wechat_agent.context import ContextWindow
from wechat_agent.context_assembler import MessageContextAssembler
from wechat_agent.freshness_policy import (
    PendingInboundSnapshot,
    evaluate_outbound_freshness,
)
from wechat_agent.hook_contract import HookObservation
from wechat_agent.memory_store import MemoryStore
from wechat_agent.mock import SystemClock
from wechat_agent.model_catalog import load_model_catalog
from wechat_agent.outbound import OutboundService
from wechat_agent.pilot import Pilot
from wechat_agent.session_recycle_policy import (
    CacheRecycleRule,
    evaluate_cache_recycle,
    resolve_cache_recycle_rule,
)
from wechat_agent.session_rollover import SessionRolloverPolicy, route_hash, session_keys
from wechat_agent.session_runtime import ScopedAgentRuntime
from wechat_agent.wechat_gateway import SendResult
from wechat_agent.wechat_tools import WeChatTools


def _pending(
    receipt_id: str,
    sender_id: str,
    text: str,
    *,
    is_group: bool = False,
) -> PendingInboundSnapshot:
    return PendingInboundSnapshot(
        receipt_id=receipt_id,
        sender_id=sender_id,
        text=text,
        is_group=is_group,
        received_at="2026-11-26T04:00:00+00:00",
    )


def _obs(
    account_id: str,
    conversation_id: str,
    sender_id: str,
    text: str,
    *,
    source_msg_id: int,
    is_group: bool = False,
) -> HookObservation:
    return HookObservation(
        account_id=account_id,
        conversation_id=conversation_id,
        sender_id=sender_id,
        sender_origin="hook_group_member_wxid" if is_group else "native_private_fromid",
        text=text,
        is_group=is_group,
        message_type=1,
        source_msg_id=source_msg_id,
        capture_tick_ms=1_700_000_000_000 + source_msg_id,
    )


# ---------------------------------------------------------------------------
# Pure freshness policy tests
# ---------------------------------------------------------------------------


def test_freshness_policy_private_vs_group_and_max_holds() -> None:
    private_pending = (_pending("r2", "user_peer", "等等改一下"),)
    private_decision = evaluate_outbound_freshness(
        account_id="bot",
        is_group=False,
        batch_sender_ids=("user_peer",),
        pending_inbound=private_pending,
    )
    assert private_decision.kind == "hold"
    assert len(private_decision.conflicts) == 1

    # Unrelated group member chat does not stall replies.
    group_unrelated = (_pending("r3", "other_member", "中午吃啥", is_group=True),)
    assert (
        evaluate_outbound_freshness(
            account_id="bot",
            is_group=True,
            batch_sender_ids=("alice",),
            pending_inbound=group_unrelated,
            account_aliases=("Echo",),
        ).kind
        == "commit"
    )

    # Same sender follow-up or explicit @alias in group triggers hold.
    group_same_sender = (_pending("r4", "alice", "补充一句", is_group=True),)
    assert (
        evaluate_outbound_freshness(
            account_id="bot",
            is_group=True,
            batch_sender_ids=("alice",),
            pending_inbound=group_same_sender,
        ).kind
        == "hold"
    )
    group_mention = (_pending("r5", "charlie", "@Echo 等一下", is_group=True),)
    assert (
        evaluate_outbound_freshness(
            account_id="bot",
            is_group=True,
            batch_sender_ids=("alice",),
            pending_inbound=group_mention,
            account_aliases=("Echo",),
        ).kind
        == "hold"
    )

    # Self-echo never holds, and reaching max_holds commits to avoid starvation.
    self_echo = (_pending("r6", "bot", "echo", is_group=False),)
    assert (
        evaluate_outbound_freshness(
            account_id="bot",
            is_group=False,
            batch_sender_ids=("user_peer",),
            pending_inbound=self_echo,
        ).kind
        == "commit"
    )
    assert (
        evaluate_outbound_freshness(
            account_id="bot",
            is_group=False,
            batch_sender_ids=("user_peer",),
            pending_inbound=private_pending,
            prior_holds=1,
            max_holds=1,
        ).kind
        == "commit"
    )


# ---------------------------------------------------------------------------
# End-to-end Dispatcher + Toolset + Outbound freshness hold & coalesce tests
# ---------------------------------------------------------------------------


def test_commit_agent_turn_holds_coalesces_and_rewinds_unsent_session(tmp_path: Path) -> None:
    async def scenario() -> None:
        (tmp_path / "data").mkdir()
        store = MemoryStore(tmp_path / "memory.sqlite3")
        sent: list[str] = []

        class RecordingGateway:
            channel = "webhook"

            def send_text(self, draft):
                sent.append(draft.text)
                return SendResult("queued", "ok")

            def resolve_contact(self, uid: str) -> ContactIdentity:
                return ContactIdentity(uid, nickname="Alice")

        gateway = RecordingGateway()
        outbound = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account="bot",
            default_room="peer_1",
            gateway=gateway,
            clock=SystemClock(),
            memory_store=store,
        )
        tools = WeChatTools(
            tmp_path,
            "bot",
            "peer_1",
            gateway.send_text,
            None,
            None,
            SystemClock(),
            memory_store=store,
            gateway=gateway,
            outbound_service=outbound,
        )
        first_call_args = {"reply_text": "过时回复"}
        second_call_args = {"reply_text": "合批后的准确回复"}
        model = FakeAdkModel(
            [
                [types.Part.from_function_call(name="commit_agent_turn", args=first_call_args)],
                [types.Part.from_function_call(name="commit_agent_turn", args=second_call_args)],
            ]
        )
        runtime = ScopedAgentRuntime(
            tmp_path / "adk.sqlite3",
            model=model,
            extra_tools=tools.agent_tools(),
        )
        pilot = Pilot(
            tmp_path / "inbox.sqlite3",
            "bot",
            "peer_1",
            runtime.reply,
            gateway.send_text,
            outbound=outbound,
            memory_store=store,
            in_scope_check=lambda _item: True,
            context_assembler=MessageContextAssembler(
                store,
                resolve_contact=gateway.resolve_contact,
            ),
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.01

        try:
            msg1 = _obs("bot", "peer_1", "peer_1", "帮我查明天天气", source_msg_id=101)
            msg2 = _obs("bot", "peer_1", "peer_1", "算了改查上海机票", source_msg_id=102)
            assert pilot.scheduler.enqueue(msg1)[0] == "queued"

            # Wait past debounce window, then claim batch 1.
            await asyncio.sleep(0.02)
            batch1 = pilot.scheduler.claim_ready_batch()
            assert batch1 is not None and len(batch1.observations) == 1

            # Intercept runtime.reply to inject msg2 while the model is reasoning on msg1.
            orig_reply = runtime.reply
            injected = False

            async def reply_with_concurrent_inbound(window: ContextWindow) -> str:
                nonlocal injected
                if not injected:
                    injected = True
                    assert pilot.scheduler.enqueue(msg2)[0] == "queued"
                return await orig_reply(window)

            pilot.dispatcher.reply = reply_with_concurrent_inbound
            state1 = await pilot.dispatcher.process_batch(batch1)
            assert state1 == "freshness_held"
            assert sent == []
            assert pilot.scheduler.state_counts().get("pending") == 2
            assert pilot.scheduler.state_counts().get("running", 0) == 0
            assert pilot.scheduler.batch_freshness_holds(batch1) == 1

            # Next batch coalesces [msg1, msg2] and even if msg3 arrives, max_holds=1 lets it commit.
            await asyncio.sleep(0.02)
            batch2 = pilot.scheduler.claim_ready_batch()
            assert batch2 is not None and len(batch2.observations) == 2

            msg3 = _obs("bot", "peer_1", "peer_1", "另外再发一份", source_msg_id=103)
            assert pilot.scheduler.enqueue(msg3)[0] == "queued"

            state2 = await pilot.dispatcher.process_batch(batch2)
            assert state2 == "agent_tool_send_unconfirmed"
            assert sent == ["合批后的准确回复"]
            # Second model request must see both coalesced messages and must NOT see held draft text.
            second_req = str(model.requests[-1][1])
            assert "帮我查明天天气" in second_req
            assert "算了改查上海机票" in second_req
            assert "过时回复" not in second_req
        finally:
            await runtime.close()
            outbound.close()
            pilot.db.close()
            tools.db.close()
            store.close()

    asyncio.run(scenario())


def test_plain_text_auto_reply_freshness_hold_and_group_non_conflict(tmp_path: Path) -> None:
    async def scenario() -> None:
        (tmp_path / "data").mkdir()
        store = MemoryStore(tmp_path / "memory.sqlite3")
        sent: list[str] = []

        def send_text(draft):
            sent.append(draft.text)
            return SendResult("queued", "ok")

        turns_seen: list[list[str]] = []

        async def plain_reply(window: ContextWindow) -> str:
            turns_seen.append([turn.text for turn in window.turns])
            return f"回复:{'+'.join(turn.text for turn in window.turns)}"

        pilot = Pilot(
            tmp_path / "inbox.sqlite3",
            "bot",
            "room1@chatroom",
            plain_reply,
            send_text,
            memory_store=store,
            context_assembler=MessageContextAssembler(
                store,
                resolve_contact=lambda uid: ContactIdentity(uid, nickname=uid),
            ),
        )
        pilot.scheduler.debounce_seconds = 0.01
        pilot.scheduler.max_wait_seconds = 0.01
        try:
            # 1) Group message from alice + concurrent message from same sender alice -> holds & coalesces.
            g1 = _obs("bot", "room1@chatroom", "alice", "第一句", source_msg_id=201, is_group=True)
            g2 = _obs("bot", "room1@chatroom", "alice", "补充第二句", source_msg_id=202, is_group=True)
            pilot.scheduler.enqueue(g1)
            await asyncio.sleep(0.02)
            b1 = pilot.scheduler.claim_ready_batch()
            assert b1 is not None
            pilot.scheduler.enqueue(g2)
            assert await pilot.dispatcher.process_batch(b1) == "freshness_held"
            assert sent == []

            await asyncio.sleep(0.02)
            b2 = pilot.scheduler.claim_ready_batch()
            assert b2 is not None and len(b2.observations) == 2

            # 2) Unrelated group chatter from bob during turn 2 does NOT conflict -> sends normally.
            g3 = _obs("bot", "room1@chatroom", "bob", "路过水一句", source_msg_id=203, is_group=True)
            pilot.scheduler.enqueue(g3)
            assert await pilot.dispatcher.process_batch(b2) == "queued"
            assert len(sent) == 1
            assert "第一句" in sent[0] and "补充第二句" in sent[0]
        finally:
            pilot.db.close()
            store.close()

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# Cache-aware session recycle policy & config tests
# ---------------------------------------------------------------------------


def test_session_recycle_policy_tiers_and_validation() -> None:
    assert resolve_cache_recycle_rule() is None
    with pytest.raises(ValueError, match="cache_tier"):
        resolve_cache_recycle_rule(cache_tier="unknown_tier")

    short_rule = resolve_cache_recycle_rule(cache_tier="short_cache_5m")
    assert short_rule == CacheRecycleRule(
        cache_tier="short_cache_5m",
        idle_seconds=720,
        min_context_tokens=8_000,
        min_context_bytes=None,
    )
    assert not evaluate_cache_recycle(
        idle_seconds=720,
        context_tokens=10_000,
        session_bytes=30_000,
        token_context_limits=True,
        rule=short_rule,
    ).recycle
    assert not evaluate_cache_recycle(
        idle_seconds=900,
        context_tokens=7_999,
        session_bytes=30_000,
        token_context_limits=True,
        rule=short_rule,
    ).recycle
    assert evaluate_cache_recycle(
        idle_seconds=721,
        context_tokens=8_000,
        session_bytes=30_000,
        token_context_limits=True,
        rule=short_rule,
    ).recycle


def test_cache_recycle_integrates_with_rollover_and_config(tmp_path: Path) -> None:
    # Verify optional TOML config parsing and inheritance for cache recycle fields.
    base_models = (Path(__file__).resolve().parents[1] / "config" / "models.toml").read_text("utf-8")
    custom_models = tmp_path / "models.toml"
    custom_models.write_text(
        base_models.replace(
            '[models."gpt-6-luna"]',
            '[models."gpt-6-luna"]\ncache_tier = "short_cache_5m"\ncache_recycle_min_tokens = 4000',
        ),
        encoding="utf-8",
    )
    catalog = load_model_catalog(custom_models, workspace=tmp_path)
    luna_rule = catalog.cache_rule_for("gpt-6-luna")
    assert luna_rule is not None and luna_rule.idle_seconds == 720
    assert luna_rule.min_context_tokens == 4000

    base_profiles = (
        Path(__file__).resolve().parents[1] / "config" / "agent_profiles.toml"
    ).read_text("utf-8")
    custom_profiles = tmp_path / "agent_profiles.toml"
    custom_profiles.write_text(
        base_profiles
        + '\n[profiles.fast_cache]\nextends = "default"\ncache_tier = "long_cache_1h"\n'
        + "cache_recycle_idle_seconds = 1800\ncache_recycle_min_tokens = 12000\n",
        encoding="utf-8",
    )
    loaded_profiles = load_agent_profiles(custom_profiles)
    fast_prof = loaded_profiles.profiles["fast_cache"]
    assert (
        fast_prof.cache_tier,
        fast_prof.cache_recycle_idle_seconds,
        fast_prof.cache_recycle_min_tokens,
    ) == ("long_cache_1h", 1800, 12000)

    # Verify deterministic cache-cold rollover in SessionRolloverPolicy without sleeping.
    async def scenario() -> None:
        from google.adk.sessions import InMemorySessionService

        sessions = InMemorySessionService()
        routing = sqlite3.connect(":memory:")
        summarized_reasons: list[str] = []

        async def fake_summary(_session, _user_id, *, reason: str = "summarize") -> str:
            summarized_reasons.append(reason)
            return "冷缓存摘要"

        policy = SessionRolloverPolicy(
            routing_db=routing,
            sessions=sessions,
            model_id="gpt-6-luna",
            token_context_limits=True,
            cache_tier="short_cache_5m",
            cache_recycle_idle_seconds=720,
            cache_recycle_min_tokens=4_000,
        )
        t0 = datetime(2026, 11, 26, 10, 0, tzinfo=UTC)
        user_id, base_id = session_keys("bot", "friend")
        route = route_hash(user_id, base_id)
        sess = await sessions.create_session(
            app_name="wechat_agent_mock",
            user_id=user_id,
            session_id=base_id,
        )
        await sessions.append_event(
            sess,
            Event(
                invocation_id="inv-1",
                author="user",
                timestamp=t0.timestamp(),
                content=types.Content(role="user", parts=[types.Part.from_text(text="你好")]),
            ),
        )
        policy.record_activity(route, "bot", "friend", now=t0)

        window = ContextWindow(
            account_id="bot",
            conversation_id="friend",
            is_group=False,
            turns=(),
        )

        async def high_tokens(_sid: str, _win: ContextWindow) -> int:
            return 10_000

        # At +10 minutes (600s <= 720s), session stays warm in generation 0.
        prep_warm = await policy.prepare_turn(
            window,
            count_tokens=high_tokens,
            summarize_fn=fake_summary,
            now=t0 + timedelta(seconds=600),
        )
        assert prep_warm.generation == 0
        assert summarized_reasons == []

        # At +13 minutes (780s > 720s), prompt cache is cold and context >= 4000 -> rotates!
        prep_cold = await policy.prepare_turn(
            window,
            count_tokens=high_tokens,
            summarize_fn=fake_summary,
            now=t0 + timedelta(seconds=780),
        )
        assert prep_cold.generation == 1
        assert prep_cold.state["latest_summary"] == "冷缓存摘要"
        assert summarized_reasons == ["cache_cold_recycle"]
        routing.close()

    asyncio.run(scenario())


def test_router_gateway_send_image_and_quote_use_active_account_provider(tmp_path: Path) -> None:
    from wechat_agent.connectors import (
        GatewayConfig,
        RegisteredSource,
        SourceRegistry,
        WebhookConnector,
    )

    primary = WebhookConnector(
        GatewayConfig(workspace=tmp_path, account_id="bot_a", send_enabled=True, source_id="primary", channel="webhook")
    )
    secondary = WebhookConnector(
        GatewayConfig(workspace=tmp_path, account_id="bot_b", send_enabled=True, source_id="secondary", channel="webhook")
    )
    registry = SourceRegistry(
        [
            RegisteredSource("primary", "webhook", "webhook", primary, account_id="bot_a"),
            RegisteredSource("secondary", "webhook", "webhook", secondary, account_id="bot_b"),
        ],
        default_source_id="primary",
        active_account_provider=lambda: "bot_b",
    )
    registry.remember_conversation("bot_b", "room:x", "secondary")
    router = registry.as_gateway()
    img_path = tmp_path / "pic.png"
    img_path.write_bytes(b"png")
    status, _ = router.send_image("room:x", img_path)
    assert status in ("queued", "rejected", "unavailable")
    q_status, _ = router.send_quote("room:x", "hi", ("u1", "U1", "room:x", "src"))
    assert q_status in ("queued", "accepted_by_server", "rejected", "unavailable")
    registry.close()
