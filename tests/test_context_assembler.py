import asyncio

from echoloom.context import ContextWindow, _record_turn
from echoloom.context_assembler import MessageContextAssembler
from echoloom.hook_contract import HookObservation
from echoloom.memory_store import MemoryStore


def observed(text: str, tick: int) -> HookObservation:
    return HookObservation(
        account_id="bot",
        conversation_id="room@chatroom",
        sender_id="member",
        sender_origin="hook_group_member_wxid",
        text=f"member:\n{text}",
        is_group=True,
        message_type=1,
        source_msg_id=None,
        capture_tick_ms=tick,
        original_message_type=1,
        event_time_context={
            "timezone": "Asia/Shanghai",
            "local_minute": f"2026-10-30 09:{tick:02d}",
            "time_period": "上午",
        },
    )


def test_assembler_archives_and_enriches_a_batch_once(tmp_path):
    async def scenario():
        store = MemoryStore(tmp_path / "memory.sqlite3")
        store.upsert_entity(account_id="bot", term="蓝莓", explanation="测试词典释义")
        assembler = MessageContextAssembler(store, max_user_bytes=6000)
        batch = await assembler.assemble((observed("蓝莓第一条", 1), observed("第二条补充", 2)))
        assert len(batch.memory_message_ids) == 2
        assert batch.memory_message_ids[0] != batch.memory_message_ids[1]
        assert len(batch.context.turns) == 2
        text = batch.context.model_user_content()
        assert text.index("蓝莓第一条") < text.index("第二条补充")
        assert "member →" not in batch.context.model_reference_context()
        assert "蓝莓：测试词典释义" in batch.context.model_reference_context()
        assert (
            store.db.execute(
                "SELECT count(*) FROM source_messages WHERE direction='inbound'",
            ).fetchone()[0]
            == 2
        )
        store.close()

    asyncio.run(scenario())


def test_latest_sender_memory_is_attached_to_historical_context(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    assembler = MessageContextAssembler(store, max_user_bytes=6000)

    async def scenario():
        batch = await assembler.assemble((observed("我周末喜欢爬山", 3),))
        store.upsert_sender_memory(
            account_id="bot",
            sender_id="member",
            topic="兴趣",
            content="对方明确说周末喜欢爬山。",
        )
        records = store.enrich_context_messages(
            account_id="bot",
            messages=store.recent_messages(
                account_id="bot",
                conversation_id="room@chatroom",
                limit=10,
            ),
        )
        window = ContextWindow(
            account_id="bot",
            conversation_id="room@chatroom",
            is_group=True,
            turns=batch.context.turns,
            history_turns=tuple(_record_turn(row, sender_origin="history") for row in records),
        )
        assert "member / 兴趣：对方明确说周末喜欢爬山。" in window.model_reference_context()

    asyncio.run(scenario())
    store.close()
