"""Focused unit tests for SessionRolloverPolicy, CrossChatDedupeStore, and TurnHooks."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from wechat_agent.context import ContextTurn, ContextWindow
from wechat_agent.cross_chat_dedupe import CrossChatDedupeStore
from wechat_agent.episode_memory import RecallTurn, recall_turn
from wechat_agent.runtime_contract import ModelProviderUnavailable
from wechat_agent.session_rollover import (
    PreparedSession,
    SessionRolloverPolicy,
    route_hash,
    session_keys,
)
from wechat_agent.session_runtime import ScopedAgentRuntime
from wechat_agent.turn_hooks import (
    EpisodeRagTurnHook,
    JevStrategyTurnHook,
    TraceTurnHook,
    TurnHook,
    TurnState,
    _safe_trace_value,
)


def _window(
    text: str = "你好",
    *,
    message_id: str = "msg-1",
    room: str = "friend",
    is_idle_event: bool = False,
    scheduled_task: str | None = None,
    history_turns: tuple[ContextTurn, ...] = (),
    recent_other_turns: tuple[ContextTurn, ...] = (),
) -> ContextWindow:
    return ContextWindow(
        account_id="bot",
        conversation_id=room,
        is_group=room.endswith("@chatroom"),
        turns=(
            ContextTurn(
                sender_id="person",
                sender_origin="native_private_fromid",
                text=text,
                message_id=message_id,
            ),
        ),
        history_turns=history_turns,
        recent_other_turns=recent_other_turns,
        is_idle_event=is_idle_event,
        scheduled_task=scheduled_task,
    )


def _other_turn(message_id: str, text: str) -> ContextTurn:
    return ContextTurn(
        sender_id="other_user",
        sender_origin="native_private_fromid",
        text=text,
        message_id=message_id,
        source_conversation_id="other_chat",
    )


# ---------------------------------------------------------------------------
# SessionRolloverPolicy tests
# ---------------------------------------------------------------------------


def test_rollover_routing_db_schema_unchanged_and_persists(tmp_path: Path) -> None:
    db_path = tmp_path / "sessions-routing.sqlite3"
    conn = sqlite3.connect(db_path)
    try:
        policy = SessionRolloverPolicy(routing_db=conn)
        columns = [row[1] for row in conn.execute("PRAGMA table_info(active_sessions)").fetchall()]
        assert columns == [
            "route",
            "generation",
            "last_message_at",
            "latest_summary",
            "account_id",
            "conversation_id",
            "safety_recovery_generation",
            "summary_only_generation",
        ]
        user_id, base_id = session_keys("bot", "friend")
        route = route_hash(user_id, base_id)
        policy.record_activity(route, "bot", "friend", now=datetime(2026, 11, 25, 0, 0, tzinfo=UTC))
        state = policy.route_state(route)
        assert state["generation"] == 0
        assert state["account_id"] == "bot"
        assert state["conversation_id"] == "friend"
        assert state["last_message_at"] == "2026-11-25T00:00:00+00:00"
    finally:
        conn.close()

    conn2 = sqlite3.connect(db_path)
    try:
        policy2 = SessionRolloverPolicy(routing_db=conn2)
        assert policy2.route_state(route)["last_message_at"] == "2026-11-25T00:00:00+00:00"
        policy2.save_route_state(route, {}, delete=True)
        assert policy2.route_state(route)["last_message_at"] is None
    finally:
        conn2.close()


def test_rollover_threshold_policies_legacy_vs_token_limited() -> None:
    legacy = SessionRolloverPolicy(
        model_id="gemini-4-hydrogen",
        framework=SimpleNamespace(uses_project_compaction=True),
    )
    assert not legacy.token_context_limits
    assert legacy.should_rollover_on_idle(context_tokens=None, message_count=50, total_bytes=100)
    assert legacy.should_rollover_on_idle(
        context_tokens=None, message_count=1, total_bytes=120_000
    )
    assert not legacy.should_rollover_on_idle(
        context_tokens=None, message_count=49, total_bytes=119_999
    )
    assert legacy.should_rollover_on_hard_limit(context_tokens=None, total_bytes=180_000)

    luna = SessionRolloverPolicy(
        model_id="gpt-6-luna",
        idle_context_token_threshold=64_000,
        active_context_token_threshold=96_000,
    )
    assert luna.token_context_limits
    # Short message count or byte count cannot bypass Luna token volume.
    assert not luna.should_rollover_on_idle(
        context_tokens=63_999, message_count=5000, total_bytes=500_000
    )
    assert luna.should_rollover_on_idle(context_tokens=64_000, message_count=1, total_bytes=10)
    assert not luna.should_rollover_on_hard_limit(context_tokens=95_999, total_bytes=999_999)
    assert luna.should_rollover_on_hard_limit(context_tokens=96_000, total_bytes=10)

    now = datetime(2026, 11, 25, 12, 0, tzinfo=UTC)
    assert not legacy.is_idle_time((now - timedelta(hours=7, minutes=59)).isoformat(), now=now)
    assert legacy.is_idle_time((now - timedelta(hours=8)).isoformat(), now=now)


def test_rollover_summary_retries_distinguishes_safety_block_vs_normal_unavailable() -> None:
    async def scenario() -> None:
        policy = SessionRolloverPolicy()
        calls = 0

        async def unavailable(session, user_id, *, reason: str = "summarize"):
            nonlocal calls
            calls += 1
            raise ModelProviderUnavailable(f"offline:{reason}")

        # Normal idle/active summary propagates ModelProviderUnavailable immediately.
        with pytest.raises(ModelProviderUnavailable):
            await policy.summarize_session_with_retries(
                object(), "u1", reason="eight_hour_idle", summarize_fn=unavailable
            )
        assert calls == 1

        # Safety-block summary retries up to 3 times (4 total attempts) and returns None.
        calls = 0
        result = await policy.summarize_session_with_retries(
            object(), "u1", reason="safety_block", summarize_fn=unavailable
        )
        assert result is None
        assert calls == 4

    asyncio.run(scenario())


def test_rollover_two_stage_safety_block_fallback_and_recovery_clear() -> None:
    async def scenario() -> None:
        deleted: list[str] = []

        class FakeSessions:
            async def delete_session(self, *, app_name, user_id, session_id):
                deleted.append(session_id)

        policy = SessionRolloverPolicy(sessions=FakeSessions())
        user_id, base_id = session_keys("bot", "friend")
        route = route_hash(user_id, base_id)
        win = _window(
            "本轮消息",
            history_turns=(
                ContextTurn(
                    sender_id="person",
                    sender_origin="native_private_fromid",
                    text="上一轮历史内容",
                    message_id="hist-1",
                ),
            ),
        )

        async def fake_summary(session, uid, *, reason: str = "summarize"):
            return "安全隔离摘要"

        # Stage 1: normal turn hits safety block -> sets safety_recovery_generation = 1.
        prep0 = PreparedSession(
            user_id=user_id,
            base_session_id=base_id,
            route=route,
            state=policy.route_state(route),
            generation=0,
            session_id=base_id,
            session=None,
            session_events=[],
            session_bytes=0,
            new_session=True,
            safety_recovery_turn=False,
            summary_only_turn=False,
        )
        gen1 = await policy.finalize_turn(
            prep0, after_run=object(), safety_block_code="PROHIBITED_CONTENT",
            summarize_fn=fake_summary,
        )
        assert gen1 == 1
        assert deleted == [base_id]
        state1 = policy.route_state(route)
        assert state1["safety_recovery_generation"] == 1
        assert state1["summary_only_generation"] is None
        assert state1["latest_summary"] == "安全隔离摘要"
        assert state1["last_message_at"] is None

        # During Stage 1 recovery, shared history records ARE included alongside summary.
        prep1 = PreparedSession(
            user_id=user_id,
            base_session_id=base_id,
            route=route,
            state=state1,
            generation=1,
            session_id=f"{base_id}-window-1",
            session=None,
            session_events=[],
            session_bytes=0,
            new_session=True,
            safety_recovery_turn=True,
            summary_only_turn=False,
        )
        hist1, count1 = policy.build_rollover_history(prep1, win)
        assert count1 == 1
        assert "安全隔离摘要" in hist1 and "上一轮历史内容" in hist1

        # Stage 2: safety block AGAIN during recovery -> escalates to summary_only_generation = 2.
        gen2 = await policy.finalize_turn(
            prep1, after_run=object(), safety_block_code="SAFETY",
            summarize_fn=fake_summary,
        )
        assert gen2 == 2
        state2 = policy.route_state(route)
        assert state2["safety_recovery_generation"] is None
        assert state2["summary_only_generation"] == 2

        # During Stage 2 (summary_only), raw history records are omitted, only summary remains.
        prep2 = PreparedSession(
            user_id=user_id,
            base_session_id=base_id,
            route=route,
            state=state2,
            generation=2,
            session_id=f"{base_id}-window-2",
            session=None,
            session_events=[],
            session_bytes=0,
            new_session=True,
            safety_recovery_turn=False,
            summary_only_turn=True,
        )
        hist2, count2 = policy.build_rollover_history(prep2, win)
        assert count2 == 0
        assert "安全隔离摘要" in hist2 and "上一轮历史内容" not in hist2

        # Clean turn clears summary_only_generation and records last_message_at.
        gen_after = await policy.finalize_turn(
            prep2, after_run=object(), safety_block_code=None
        )
        assert gen_after == 2
        final_state = policy.route_state(route)
        assert final_state["summary_only_generation"] is None
        assert final_state["last_message_at"] is not None

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# CrossChatDedupeStore tests
# ---------------------------------------------------------------------------


def test_cross_chat_dedupe_uncommitted_on_provider_unavailable_and_prunes(tmp_path: Path) -> None:
    conn = sqlite3.connect(tmp_path / "dedupe-routing.sqlite3")
    try:
        store = CrossChatDedupeStore(conn)
        columns = [
            row[1] for row in conn.execute("PRAGMA table_info(session_other_message_seen)")
        ]
        assert columns == ["route", "generation", "message_id", "seen_at"]

        win = _window(
            recent_other_turns=(
                _other_turn("other-1", "隔壁消息一"),
                _other_turn("other-1", "重复ID同批去重"),
                _other_turn("other-2", "隔壁消息二"),
            )
        )
        # Filter before dispatch: pure read, commits nothing (simulates ModelProviderUnavailable).
        filtered_1, new_ids_1 = store.filter_unseen("route-a", 0, win)
        assert [t.message_id for t in filtered_1.recent_other_turns] == ["other-1", "other-2"]
        assert new_ids_1 == ("other-1", "other-2")
        assert store.seen_message_ids("route-a", 0) == set()

        # Retry after provider recovers sees the exact same messages again.
        filtered_2, new_ids_2 = store.filter_unseen("route-a", 0, win)
        assert new_ids_2 == ("other-1", "other-2")
        assert len(filtered_2.recent_other_turns) == 2

        # Commit after dispatch -> subsequent turns omit already-seen IDs.
        store.mark_seen("route-a", 0, new_ids_2)
        filtered_3, new_ids_3 = store.filter_unseen("route-a", 0, win)
        assert filtered_3.recent_other_turns == ()
        assert new_ids_3 == ()

        # Generation rollover prunes older generations while keeping newer ones.
        store.mark_seen("route-a", 1, ("other-3",))
        store.prune_before_generation("route-a", 1)
        assert store.seen_message_ids("route-a", 0) == set()
        assert store.seen_message_ids("route-a", 1) == {"other-3"}
    finally:
        conn.close()


def test_cross_chat_dedupe_seeds_from_legacy_session_events_ignoring_rewound() -> None:
    store = CrossChatDedupeStore(None)
    legacy_text = (
        "前缀\n\n【此时此刻其它聊天串（非本会话）近况】\n"
        "- 内部消息 ID：legacy-1；正文\n"
        "- 内部消息 ID：legacy-2；正文\n"
    )
    rewound_text = (
        "前缀\n\n【此时此刻其它聊天串（非本会话）近况】\n"
        "- 内部消息 ID：rewound-99；正文\n"
    )
    session = SimpleNamespace(
        events=[
            SimpleNamespace(
                invocation_id="inv-1",
                author="user",
                actions=SimpleNamespace(rewind_before_invocation_id=None),
                content=SimpleNamespace(parts=[SimpleNamespace(text=legacy_text)]),
            ),
            SimpleNamespace(
                invocation_id="inv-2",
                author="user",
                actions=SimpleNamespace(rewind_before_invocation_id=None),
                content=SimpleNamespace(parts=[SimpleNamespace(text=rewound_text)]),
            ),
            SimpleNamespace(
                invocation_id="inv-r",
                author="user",
                actions=SimpleNamespace(rewind_before_invocation_id="inv-2"),
                content=SimpleNamespace(parts=[]),
            ),
        ]
    )
    seeded = store.seed_from_session("route-legacy", 0, session)
    assert seeded == ("legacy-1", "legacy-2")
    assert "rewound-99" not in store.seen_message_ids("route-legacy", 0)


# ---------------------------------------------------------------------------
# TraceTurnHook tests
# ---------------------------------------------------------------------------


def test_trace_turn_hook_snapshots_and_atomically_redacts_on_safety_block(tmp_path: Path) -> None:
    async def scenario() -> None:
        hook = TraceTurnHook(tmp_path, model_id="test-model")
        assert isinstance(hook, TurnHook)

        # Secret redaction in _safe_trace_value.
        safe = _safe_trace_value(
            {"api_key": "sk-secret", "Authorization": "Bearer x", "blob": b"1234", "ok": "hi"}
        )
        assert safe == {
            "api_key": "[redacted]",
            "Authorization": "[redacted]",
            "blob": "[binary omitted: 4 bytes]",
            "ok": "hi",
        }

        # Write a prior normal snapshot that must remain unredacted.
        prior_state = TurnState(
            window=_window("安全历史消息"),
            route="route-1234567890abcdef",
            session_id="sess-trace-1",
            conversation_id="friend",
        )
        prior_req = SimpleNamespace(
            model="test-model",
            contents=[
                SimpleNamespace(
                    role="user",
                    parts=[
                        SimpleNamespace(text="安全历史消息", thought=False),
                        SimpleNamespace(text="隐藏思考", thought=True),
                    ],
                )
            ],
        )
        assert hook.before_model_request(prior_state, prior_req) == []
        assert len(prior_state.request_trace_ids) == 1

        # Current turn hits a safety block.
        blocked_state = TurnState(
            window=_window("敏感内容"),
            route="route-1234567890abcdef",
            session_id="sess-trace-1",
            conversation_id="friend",
        )
        blocked_req = SimpleNamespace(
            model="test-model",
            contents=[
                SimpleNamespace(
                    role="user",
                    parts=[
                        SimpleNamespace(text="敏感内容", thought=False),
                        SimpleNamespace(inline_data=SimpleNamespace(data=b"abc")),
                    ],
                )
            ],
        )
        hook.before_model_request(blocked_state, blocked_req)
        blocked_state.safety_block_code = "PROHIBITED_CONTENT"
        blocked_state.events = [
            SimpleNamespace(
                id="ev-blocked",
                author="user",
                timestamp=1.0,
                partial=False,
                error_code="PROHIBITED_CONTENT",
                content=SimpleNamespace(parts=[SimpleNamespace(text="敏感内容")]),
                is_final_response=lambda: False,
            )
        ]
        await hook.on_safety_block(blocked_state, "PROHIBITED_CONTENT")
        await hook.after_turn(blocked_state)

        files = list(tmp_path.glob("*.jsonl"))
        assert len(files) == 1
        # Atomic rewrite leaves zero leftover .tmp files.
        assert list(tmp_path.glob("*.tmp")) == []

        body = files[0].read_text(encoding="utf-8")
        assert "敏感内容" not in body
        assert "隐藏思考" not in body
        assert "安全历史消息" in body
        records = [json.loads(line) for line in body.splitlines() if line.strip()]
        req_records = [r for r in records if r.get("record") == "model_request"]
        assert len(req_records) == 2
        assert req_records[0]["contents"][0]["parts"][0]["text"] == "安全历史消息"
        assert req_records[1]["contents"][0]["parts"][0]["text"] == "[prompt blocked; body omitted]"
        assert req_records[1]["contents"][0]["parts"][1] == {"inline_data_bytes": 3}

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# EpisodeRagTurnHook tests
# ---------------------------------------------------------------------------


def test_episode_rag_turn_hook_lifecycle_tail_and_no_enqueue_on_failures() -> None:
    async def scenario() -> None:
        enqueued: list[RecallTurn] = []
        auto_calls: list[RecallTurn] = []

        class FakeEpisodeMemory:
            async def automatic(self, turn: RecallTurn) -> None:
                auto_calls.append(turn)
                turn.automatic = [{"id": "ep-1", "summary": "约定周六一起吃饭"}]

            def enqueue(self, turn: RecallTurn) -> None:
                enqueued.append(turn)

        hook = EpisodeRagTurnHook(FakeEpisodeMemory())  # type: ignore[arg-type]
        assert isinstance(hook, TurnHook)

        # 1) Normal eligible turn recalls, injects tail block, and enqueues on after_turn.
        win = _window("周六几点？")
        with hook.turn_scope(win) as episode:
            assert recall_turn.get() is episode
            state = TurnState(window=win, recall=episode)
            await hook.before_turn(state)
            assert len(auto_calls) == 1
            tail_parts = hook.before_model_request(state, SimpleNamespace())
            assert len(tail_parts) == 1
            assert tail_parts[0].startswith("【事件记忆｜仅供参考】")
            assert "约定周六一起吃饭" in tail_parts[0]
            await hook.after_turn(state)
        assert recall_turn.get() is None
        assert len(enqueued) == 1

        # 2) Error / ModelProviderUnavailable / safety block MUST NOT enqueue.
        for label, mutator in (
            ("run_error", lambda s: setattr(s, "run_error", "RuntimeError")),
            ("provider_unavailable", lambda s: setattr(s, "provider_unavailable", True)),
            ("safety_block", lambda s: setattr(s, "safety_block_code", "SAFETY")),
        ):
            with hook.turn_scope(win) as episode:
                state = TurnState(window=win, recall=episode)
                await hook.before_turn(state)
                mutator(state)
                if label == "safety_block":
                    await hook.on_safety_block(state, "SAFETY")
                await hook.after_turn(state)
                assert episode is not None and not episode.eligible
        assert len(enqueued) == 1

        # 3) Unfinished recall task is cancelled in turn_scope finally.
        started = asyncio.Event()
        cancelled = False

        class SlowEpisodeMemory:
            async def automatic(self, turn: RecallTurn) -> None:
                nonlocal cancelled
                started.set()
                try:
                    await asyncio.Event().wait()
                except asyncio.CancelledError:
                    cancelled = True
                    raise

            def enqueue(self, turn: RecallTurn) -> None:
                enqueued.append(turn)

        slow_hook = EpisodeRagTurnHook(SlowEpisodeMemory())  # type: ignore[arg-type]
        with slow_hook.turn_scope(win) as episode:
            assert episode is not None
            episode.task = asyncio.create_task(slow_hook.episode_memory.automatic(episode))
            await started.wait()
        await asyncio.sleep(0)
        assert cancelled
        assert recall_turn.get() is None

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# JevStrategyTurnHook tests
# ---------------------------------------------------------------------------


def test_jev_strategy_turn_hook_populates_labels_and_degrades_gracefully() -> None:
    async def scenario() -> None:
        persisted: list[tuple[dict[str, object], ...]] = []

        class OkClassifier:
            async def classify_window(self, window, **kwargs):
                assert kwargs["cross_chat_context"] == "跨聊上下文"
                assert kwargs["event_book_context"] == "日程事件"
                return (
                    {
                        "index": 0,
                        "message_id": "msg-1",
                        "model": "gemini-decision-1",
                        "probabilities": {"reply": 80, "concise": 15, "no_reply": 5},
                        "multi_turn_pct": 60,
                    },
                )

        hook = JevStrategyTurnHook(
            jev_classifier=OkClassifier(),  # type: ignore[arg-type]
            jev_label_sink=persisted.append,
        )
        assert isinstance(hook, TurnHook)
        state = TurnState(
            window=_window("今天忙吗"),
            cross_chat_context="跨聊上下文",
            event_book_provider=lambda: "  日程事件  ",
        )
        await hook.before_turn(state)
        assert state.window.turns[0].jev_label is not None
        assert state.window.turns[0].jev_label["model"] == "gemini-decision-1"
        assert len(persisted) == 1

        # Idle events skip Jev completely.
        idle_called = False

        class GuardClassifier:
            async def classify_window(self, window, **kwargs):
                nonlocal idle_called
                idle_called = True
                return ()

        idle_hook = JevStrategyTurnHook(jev_classifier=GuardClassifier())  # type: ignore[arg-type]
        await idle_hook.before_turn(TurnState(window=_window("idle", is_idle_event=True)))
        assert not idle_called

        # Classifier exception degrades silently without raising.
        class BrokenClassifier:
            async def classify_window(self, window, **kwargs):
                raise RuntimeError("classifier down")

        broken_hook = JevStrategyTurnHook(
            jev_classifier=BrokenClassifier()  # type: ignore[arg-type]
        )
        degraded_state = TurnState(window=_window("照常回复"))
        await broken_hook.before_turn(degraded_state)
        assert degraded_state.window.turns[0].jev_label is None

        # Broken event_book_provider and broken sink still degrade cleanly and keep label.
        def failing_sink(_):
            raise OSError("db locked")

        def failing_event_book():
            raise RuntimeError("event book down")

        class PartialOkClassifier:
            async def classify_window(self, window, **kwargs):
                assert kwargs["event_book_context"] == ""
                return (
                    {
                        "index": 0,
                        "message_id": "msg-1",
                        "model": "jev-latest",
                        "probabilities": {"reply": 50, "concise": 40, "no_reply": 10},
                        "multi_turn_pct": 20,
                    },
                )

        partial_hook = JevStrategyTurnHook(
            jev_classifier=PartialOkClassifier(),  # type: ignore[arg-type]
            jev_label_sink=failing_sink,
        )
        partial_state = TurnState(
            window=_window("不阻塞"),
            event_book_provider=failing_event_book,
        )
        await partial_hook.before_turn(partial_state)
        assert partial_state.window.turns[0].jev_label is not None
        assert partial_state.window.turns[0].jev_label["model"] == "jev-latest"

    asyncio.run(scenario())


def test_episode_rag_and_jev_hooks_run_concurrently_in_runtime(tmp_path: Path) -> None:
    from test_openai_runtime import FakeAdkModel

    async def scenario() -> None:
        rag_entered = asyncio.Event()
        jev_entered = asyncio.Event()
        overlap_verified = False

        class ConcurrentEpisodeMemory:
            async def automatic(self, turn: RecallTurn) -> None:
                nonlocal overlap_verified
                rag_entered.set()
                await asyncio.wait_for(jev_entered.wait(), timeout=1.0)
                overlap_verified = True
                turn.automatic = [{"id": "ep-concurrent", "summary": "并发召回成功"}]

            def enqueue(self, turn: RecallTurn) -> None:
                return None

            async def search_episode_memory(self, query: str) -> dict:
                return {"status": "empty", "events": []}

        class ConcurrentJevClassifier:
            async def classify_window(self, window, **kwargs):
                jev_entered.set()
                await asyncio.wait_for(rag_entered.wait(), timeout=1.0)
                return ()

        model = FakeAdkModel(["并发OK"])
        runtime = ScopedAgentRuntime(
            tmp_path / "concurrent.sqlite3",
            model=model,
            episode_memory=ConcurrentEpisodeMemory(),  # type: ignore[arg-type]
            jev_classifier=ConcurrentJevClassifier(),  # type: ignore[arg-type]
        )
        try:
            reply = await runtime.reply(_window("测试并发"))
            assert reply == "并发OK"
            assert overlap_verified
            assert "并发召回成功" in str(model.requests[-1][1])
        finally:
            await runtime.close()

    asyncio.run(scenario())


def test_uninitialized_runtime_supports_history_context_without_init() -> None:
    runtime = object.__new__(ScopedAgentRuntime)
    runtime.max_history_bytes = 300
    runtime.max_history_records = 50
    rendered = runtime._history_context(None, ["开始" + "长" * 500 + "结尾"])
    assert "（较早内容已截断）" in rendered
    assert rendered.rstrip().endswith("结尾\n【历史会话内容结束】")
