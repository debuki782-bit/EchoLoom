"""Unit tests for turn_scope.py and OutboundService (outbound.py)."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from pathlib import Path

from PIL import Image

from echoloom import turn_scope, wechat_tools
from echoloom.context import ContextTurn, ContextWindow
from echoloom.memory_store import MemoryStore
from echoloom.mock import SystemClock
from echoloom.outbound import OutboundService
from echoloom.wechat_gateway import SendResult
from echoloom.wechat_tools import WeChatTools

ACCOUNT = "wxid_bot_outbound"
ROOM = "outbound-room@chatroom"
OTHER_ROOM = "other-room@chatroom"
MEMBER = "wxid_member_001"


class FakeGateway:
    def __init__(
        self,
        *,
        text_results: list[SendResult | Exception] | None = None,
        quote_results: list[tuple[str, str | None] | Exception] | None = None,
        image_results: list[tuple[str, str | None] | Exception] | None = None,
    ) -> None:
        self.text_results = list(text_results or [])
        self.quote_results = list(quote_results or [])
        self.image_results = list(image_results or [])
        self.events: list[tuple[str, object]] = []

    def send_text(self, draft):
        self.events.append(("text", draft))
        if self.text_results:
            item = self.text_results.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return SendResult("queued", "ok")

    def send_quote(self, conversation_id: str, text: str, quote: tuple[str, str, str, str]):
        self.events.append(("quote", (conversation_id, text, quote)))
        if self.quote_results:
            item = self.quote_results.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return "accepted_by_server", "server_accepted_not_delivery_proof"

    def send_image(self, conversation_id: str, path: Path):
        self.events.append(("image", (conversation_id, Path(path))))
        if self.image_results:
            item = self.image_results.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        return "queued", "ok"


def _sample_window(conversation_id: str = ROOM) -> ContextWindow:
    return ContextWindow(
        account_id=ACCOUNT,
        conversation_id=conversation_id,
        is_group=conversation_id.endswith("@chatroom"),
        turns=(ContextTurn(MEMBER, "hook_group_member_wxid", "你好", message_id="msg-1"),),
    )


def test_turn_scope_symbols_are_reexported_verbatim_from_wechat_tools():
    for name in (
        "operation_id",
        "operation_receipt_ids",
        "quote_context",
        "memory_context",
        "active_conversation",
        "followup_scheduler",
        "tool_memory_context",
        "tool_turn_scope",
    ):
        assert getattr(wechat_tools, name) is getattr(turn_scope, name), name


def test_outbound_enables_wal_busy_timeout_and_preserves_4_table_legacy_schema(tmp_path: Path):
    db_path = tmp_path / "data" / "tool-receipts.sqlite3"
    db_path.parent.mkdir(parents=True)

    # Seed a legacy pre-backfill DB to confirm zero-migration compatibility.
    legacy = sqlite3.connect(db_path)
    legacy.execute("CREATE TABLE operations (id TEXT PRIMARY KEY,state TEXT)")
    legacy.execute("CREATE TABLE send_side_effect_scopes (scope TEXT PRIMARY KEY)")
    legacy.execute(
        "CREATE TABLE delivery_failure_scopes ("
        "scope TEXT PRIMARY KEY,status TEXT NOT NULL,recorded_at TEXT NOT NULL)"
    )
    legacy.execute(
        "INSERT INTO delivery_failure_scopes VALUES ('legacy-op','rejected','2026-01-01T00:00:00Z')"
    )
    legacy.execute(
        "CREATE TABLE delivery_failure_notices ("
        "scope TEXT PRIMARY KEY,status TEXT NOT NULL,reason TEXT,"
        "failure_count INTEGER NOT NULL,created_at TEXT NOT NULL,acknowledged_at TEXT)"
    )
    legacy.commit()
    legacy.close()

    svc = OutboundService(
        db_path, account=ACCOUNT, default_room=ROOM, gateway=FakeGateway(), clock=SystemClock(),
    )
    try:
        journal_mode = svc.db.execute("PRAGMA journal_mode").fetchone()[0]
        busy_timeout = svc.db.execute("PRAGMA busy_timeout").fetchone()[0]
        assert str(journal_mode).lower() == "wal"
        assert int(busy_timeout) == 5000

        tables = {
            row[0]
            for row in svc.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {
            "operations",
            "send_side_effect_scopes",
            "delivery_failure_scopes",
            "delivery_failure_notices",
        }.issubset(tables)

        failure_cols = {
            row[1] for row in svc.db.execute("PRAGMA table_info(delivery_failure_scopes)")
        }
        assert {
            "scope", "status", "recorded_at", "failure_count",
            "reason", "send_attempt_count", "last_send_status",
        }.issubset(failure_cols)

        notice_cols = {
            row[1] for row in svc.db.execute("PRAGMA table_info(delivery_failure_notices)")
        }
        assert {
            "scope", "account_id", "conversation_id", "status",
            "reason", "failure_count", "created_at", "acknowledged_at",
        }.issubset(notice_cols)

        legacy_state = svc.delivery_state("legacy-op")
        assert legacy_state["failure_count"] == 1
        assert legacy_state["send_attempt_count"] == 1
        assert legacy_state["last_failure_status"] == "rejected"
    finally:
        svc.close()


def test_rebatch_idempotency_deduplicates_and_preserves_retry_cap(tmp_path: Path):
    async def scenario():
        gw = FakeGateway()
        svc = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account=ACCOUNT,
            default_room=ROOM,
            gateway=gw,
            clock=SystemClock(),
        )
        try:
            with turn_scope.tool_turn_scope(_sample_window(), "op-1", receipt_ids=("in-1",)):
                res1 = await svc.send_text("首次回复")
                assert res1["status"] == "queued"

            # Same input rebatched under a new operation_id must be suppressed.
            with turn_scope.tool_turn_scope(_sample_window(), "op-2", receipt_ids=("in-1",)):
                state = svc.delivery_state("op-2")
                assert state.get("replayed_input_send") is True
                assert svc.is_send_blocked() is True
                res2 = await svc.send_text("禁止重复发送")
                assert res2["status"] == "already_submitted"

            # Adding a new receipt allows sending once for the new batch.
            with turn_scope.tool_turn_scope(
                _sample_window(), "op-3", receipt_ids=("in-1", "in-2"),
            ):
                assert svc.is_send_blocked() is False
                res3 = await svc.send_text("针对新消息的回复")
                assert res3["status"] == "queued"

            assert len(gw.events) == 2

            # Now verify 1+3 failure cap carries over across rebatch.
            gw.text_results = [SendResult("rejected", "hook_busy")] * 5
            with turn_scope.tool_turn_scope(_sample_window(), "fail-op-1", receipt_ids=("in-f",)):
                for attempt in range(4):
                    res = await svc.send_text(f"尝试 {attempt + 1}")
                    if attempt < 3:
                        assert res["status"] == "rejected"
                        assert res["retries_remaining"] == 3 - attempt
                    else:
                        assert res["status"] == "retry_limit_reached"
                        assert res["retries_remaining"] == 0
                        assert res["last_failure_status"] == "rejected"
                blocked = await svc.send_text("第五次必须被拦")
                assert blocked["status"] == "retry_limit_reached"

            with turn_scope.tool_turn_scope(_sample_window(), "fail-op-2", receipt_ids=("in-f",)):
                assert svc.is_send_blocked() is True
                replayed_block = await svc.send_text("换 op 也不允许绕过")
                assert replayed_block["status"] == "retry_limit_reached"
                assert replayed_block["send_attempt_count"] == 4

            # 2 queued sends + 4 rejected sends = 6 total calls.
            assert len(gw.events) == 6
        finally:
            svc.close()

    asyncio.run(scenario())


def test_failure_notices_lifecycle_and_formatting(tmp_path: Path):
    svc = OutboundService(
        tmp_path / "tool-receipts.sqlite3",
        account=ACCOUNT,
        default_room=ROOM,
        gateway=FakeGateway(),
        clock=SystemClock(),
    )
    try:
        # Invalid status is ignored; valid statuses are persisted and scoped by conversation.
        svc.queue_delivery_failure_notice("ignored-op", "queued", "not_a_failure", 1)
        svc.queue_delivery_failure_notice("op-rej", "rejected", "blocked_by_hook", 4)
        svc.queue_delivery_failure_notice("op-rej", "rejected", "duplicate_ignored", 4)
        svc.queue_delivery_failure_notice("op-unk", "unknown", "timeout", 2)
        svc.queue_delivery_failure_notice(
            "op-other", "agent_retry_unknown", "crash", 1, conversation_id=OTHER_ROOM,
        )

        pending = svc.pending_delivery_failure_notices()
        assert [row["scope"] for row in pending] == ["op-rej", "op-unk"]
        assert pending[0]["reason"] == "blocked_by_hook"

        other_pending = svc.pending_delivery_failure_notices(conversation_id=OTHER_ROOM)
        assert [row["scope"] for row in other_pending] == ["op-other"]

        # Formatting preserves certainty wording per failure class.
        fmt_rej = OutboundService.format_delivery_notice(pending[0])
        fmt_unk = OutboundService.format_delivery_notice(pending[1])
        fmt_retry = OutboundService.format_delivery_notice(other_pending[0])
        assert "Hook 明确拒绝" in fmt_rej
        assert "消息可能已送达" in fmt_unk
        assert "自动重试流程中断" in fmt_retry

        svc.acknowledge_delivery_failure_notices(("op-rej",))
        remaining = svc.pending_delivery_failure_notices()
        assert [row["scope"] for row in remaining] == ["op-unk"]
    finally:
        svc.close()


def test_can_send_gating_and_wechat_tools_property_forwarding(tmp_path: Path):
    async def scenario():
        (tmp_path / "data").mkdir()
        gw = FakeGateway()
        svc = OutboundService(
            tmp_path / "data" / "tool-receipts.sqlite3",
            account=ACCOUNT,
            default_room=ROOM,
            gateway=gw,
            clock=SystemClock(),
        )
        tools = WeChatTools(
            tmp_path, ACCOUNT, ROOM, None, None, None, SystemClock(),
            gateway=gw, outbound_service=svc,
        )
        try:
            assert tools.db is svc.db
            assert tools.can_send is svc.can_send

            # Runtime reassignment on WeChatTools must forward directly to OutboundService.
            gate = {"allowed": False}
            tools.can_send = lambda: gate["allowed"]
            assert svc.can_send() is False

            with turn_scope.tool_turn_scope(_sample_window(), "gated-op"):
                assert (await tools.send_wechat_text("不能发"))["status"] == (
                    "already_attempted_or_no_route"
                )
                assert (await svc.send_multi_segments(["甲", "乙"]))["status"] == (
                    "already_attempted_or_no_route"
                )
                assert (
                    await svc.deliver_auto_reply("gated-auto", ACCOUNT, ROOM, "不能自动发")
                ) == "send_blocked"
                assert svc.delivery_state("gated-auto")["side_effect_started"] is False
                assert gw.events == []

                gate["allowed"] = True
                assert (await tools.send_wechat_text("允许发了"))["status"] == "queued"
                assert len(gw.events) == 1
        finally:
            svc.close()

    asyncio.run(scenario())


def test_multi_segment_delivery_quote_archiving_and_sleep_patch(tmp_path: Path, monkeypatch):
    waits: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        waits.append(seconds)

    # Patching via wechat_tools.asyncio.sleep must intercept outbound.py's asyncio.sleep.
    monkeypatch.setattr("echoloom.wechat_tools.asyncio.sleep", fake_sleep)

    async def scenario():
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        gw = FakeGateway()
        svc = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account=ACCOUNT,
            default_room=ROOM,
            gateway=gw,
            clock=SystemClock(),
            memory_store=memory,
        )
        try:
            with turn_scope.tool_turn_scope(
                _sample_window(),
                "multi-quote-op",
                quote=(MEMBER, "成员甲", ROOM, "原消息内容"),
            ):
                res = await svc.send_multi_segments(
                    ["第一条引用气泡", "第二条普通气泡", "第三条结束"],
                    quote_current_message=True,
                    note="气泡备注",
                )
                assert res["status"] == "queued"
                assert res["sent_count"] == 3
                assert len(waits) == 2
                assert [kind for kind, _ in gw.events] == ["quote", "text", "text"]
                assert all(seg["message_id"] for seg in res["segments"])

            rows = memory.db.execute(
                "SELECT body_text, delivery_state FROM source_messages "
                "WHERE direction='outbound' ORDER BY rowid"
            ).fetchall()
            assert len(rows) == 3
            assert json.loads(rows[0][0])["kind"] == "quoted_message"
            assert rows[0][1] == "accepted_by_server"
            assert rows[1][0] == "第二条普通气泡"
            assert rows[2][0] == "第三条结束"

            # Stop immediately on segment failure and do not send subsequent segments.
            gw.events.clear()
            waits.clear()
            gw.text_results = [SendResult("queued", "ok"), SendResult("rejected", "rate_limit")]
            with turn_scope.tool_turn_scope(_sample_window(), "multi-stop-op"):
                stopped = await svc.send_multi_segments(["气泡1", "气泡2", "气泡3"])
                assert stopped["status"] == "rejected"
                assert stopped["sent_count"] == 2
                assert len(gw.events) == 2
                assert len(waits) == 1

            # Test commit image archiving into MemoryStore.
            img_path = tmp_path / "sticker.png"
            Image.new("RGB", (16, 16), (255, 100, 50)).save(img_path, format="PNG")
            with turn_scope.tool_turn_scope(_sample_window(), "img-op"):
                img_res = await svc.send_commit_image(
                    "sticker-1", img_path, segment_index=0, note="贴图备注",
                )
                assert img_res["status"] == "queued"
                assert img_res["archive_status"] == "saved"
                assert img_res["message_id"]
        finally:
            svc.close()
            memory.close()

    asyncio.run(scenario())


def test_deliver_auto_reply_orchestrates_retries_notices_and_tool_dedup(tmp_path: Path):
    async def scenario():
        memory = MemoryStore(tmp_path / "memory.sqlite3")
        gw = FakeGateway()
        svc = OutboundService(
            tmp_path / "tool-receipts.sqlite3",
            account=ACCOUNT,
            default_room=ROOM,
            gateway=gw,
            clock=SystemClock(),
            memory_store=memory,
        )
        window = _sample_window()
        try:
            # 1) Empty reply without prior send -> empty_reply
            assert await svc.deliver_auto_reply("empty-op", ACCOUNT, ROOM, "") == "empty_reply"

            # 2) Already sent via tool -> agent_tool_send_unconfirmed (no duplicate send)
            with turn_scope.tool_turn_scope(window, "tool-sent-op"):
                assert (await svc.send_text("工具已发"))["status"] == "queued"
                assert await svc.deliver_auto_reply(
                    "tool-sent-op", ACCOUNT, ROOM, "不能再自动发一遍", window,
                ) == "agent_tool_send_unconfirmed"
                assert await svc.deliver_auto_reply(
                    "tool-sent-op", ACCOUNT, ROOM, "", window,
                ) == "agent_tool_send_unconfirmed"
            assert len(gw.events) == 1

            # 3) Single-shot followup mode (retry_reply_fn=None) queues notice on failure.
            gw.text_results = [SendResult("rejected", "followup_fail")]
            state = await svc.deliver_auto_reply(
                "followup-op", ACCOUNT, ROOM, "定时跟进消息",
                context=window, retry_reply_fn=None, archive_operation_id="followup-op:scheduled",
            )
            assert state == "rejected"
            pending = svc.pending_delivery_failure_notices()
            assert any(row["scope"] == "followup-op" for row in pending)
            svc.acknowledge_delivery_failure_notices(("followup-op",))

            # 4) Retry succeeds on attempt 2 after agent corrects message.
            gw.text_results = [
                SendResult("rejected", "too_long"),
                SendResult("queued", "ok"),
            ]
            seen_retry_turns: list[tuple[str, tuple[str, ...]]] = []

            async def retry_fix(retry_ctx: ContextWindow, turn_label: str) -> str:
                seen_retry_turns.append((turn_label, retry_ctx.delivery_notices))
                return "修正后的短消息"

            ok_state = await svc.deliver_auto_reply(
                "retry-ok-op", ACCOUNT, ROOM, "原长消息", window, retry_fix,
            )
            assert ok_state == "queued"
            assert len(seen_retry_turns) == 1
            assert seen_retry_turns[0][0] == "retry_1"
            assert "失败 1/4" in seen_retry_turns[0][1][0]
            assert "too_long" in seen_retry_turns[0][1][0]

            # 5) 1+3 cap reached -> retry_limit_reached + failure notice persisted.
            gw.text_results = [SendResult("rejected", "still_down")] * 5
            retry_calls = 0

            async def retry_always(retry_ctx: ContextWindow) -> str:
                nonlocal retry_calls
                retry_calls += 1
                return f"重试文本 {retry_calls}"

            cap_state = await svc.deliver_auto_reply(
                "retry-cap-op", ACCOUNT, ROOM, "初次消息", window, retry_always,
            )
            assert cap_state == "retry_limit_reached"
            assert retry_calls == 3
            assert any(
                row["scope"] == "retry-cap-op" and row["failure_count"] == 4
                for row in svc.pending_delivery_failure_notices()
            )
            svc.acknowledge_delivery_failure_notices(("retry-cap-op",))

            # 6) Retry agent exception -> agent_retry_unknown + failure notice.
            gw.text_results = [RuntimeError("transport exploded")]

            async def retry_boom(_ctx: ContextWindow) -> str:
                raise RuntimeError("llm failed")

            boom_state = await svc.deliver_auto_reply(
                "retry-boom-op", ACCOUNT, ROOM, "初次发送", window, retry_boom,
            )
            assert boom_state == "agent_retry_unknown"
            assert any(row["scope"] == "retry-boom-op" for row in svc.pending_delivery_failure_notices())
            svc.acknowledge_delivery_failure_notices(("retry-boom-op",))

            # 7) Retry agent returns empty without tool send -> retry_reply_empty + notice.
            gw.text_results = [SendResult("rejected", "bad_content")]

            async def retry_empty(_ctx: ContextWindow) -> str:
                return ""

            empty_state = await svc.deliver_auto_reply(
                "retry-empty-op", ACCOUNT, ROOM, "初次发送", window, retry_empty,
            )
            assert empty_state == "retry_reply_empty"
            assert any(row["scope"] == "retry-empty-op" for row in svc.pending_delivery_failure_notices())
            svc.acknowledge_delivery_failure_notices(("retry-empty-op",))

            # 8) Retry agent sends via tool and returns "" -> agent_tool_send_unconfirmed.
            gw.text_results = [
                SendResult("rejected", "first_try_failed"),
                SendResult("queued", "sent_by_tool"),
            ]

            async def retry_via_tool(_ctx: ContextWindow) -> str:
                res = await svc.send_text("工具补发成功")
                assert res["status"] == "queued"
                return ""

            with turn_scope.tool_turn_scope(window, "retry-tool-op"):
                tool_state = await svc.deliver_auto_reply(
                    "retry-tool-op", ACCOUNT, ROOM, "初次发送", window, retry_via_tool,
                )
            assert tool_state == "agent_tool_send_unconfirmed"
            assert svc.pending_delivery_failure_notices() == []
        finally:
            svc.close()
            memory.close()

    asyncio.run(scenario())


def test_outbound_and_turn_scope_have_no_hardcoded_hook_endpoints():
    root = Path(__file__).parents[1] / "src" / "echoloom"
    for name in ("turn_scope.py", "outbound.py", "wechat_tools.py"):
        source = (root / name).read_text("utf-8")
        for endpoint in (
            "/SendTextMsg",
            "/SendImgMsg",
            "/ForwardXMLMsg",
            "/Decode_Pic",
            "/QueryDB/status",
        ):
            assert endpoint not in source, (name, endpoint)
