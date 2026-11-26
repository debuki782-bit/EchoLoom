"""Tests for thread-safe async SQLite offloading."""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from pathlib import Path

from echoloom.context_builder import ContextBuilder, ContextSpec
from echoloom.db_async import is_db_thread_safe, register_thread_safe_db, run_db
from echoloom.memory_store import MemoryStore
from echoloom.message_ingestor import MessageIngestor
from echoloom.outbound_store import OutboundReceiptStore
from echoloom.wechat_gateway import MessageEvent


def test_run_db_respects_thread_affinity_and_offloads_registered_connections(tmp_path: Path):
    async def scenario():
        loop_thread = threading.get_ident()

        # Unregistered check_same_thread=True connection stays on the caller thread.
        unregistered = sqlite3.connect(tmp_path / "unreg.sqlite3")
        unregistered.execute("CREATE TABLE t (x INT)")

        def write_unreg() -> int:
            unregistered.execute("INSERT INTO t VALUES (1)")
            return threading.get_ident()

        try:
            assert not is_db_thread_safe(unregistered)
            assert await run_db(write_unreg, db=unregistered) == loop_thread
        finally:
            unregistered.close()

        # Registered check_same_thread=False connection executes in a worker thread.
        registered = sqlite3.connect(tmp_path / "reg.sqlite3", check_same_thread=False)
        register_thread_safe_db(registered)
        registered.execute("CREATE TABLE t (x INT)")

        def write_reg() -> int:
            with registered:
                registered.execute("INSERT INTO t VALUES (2)")
            return threading.get_ident()

        try:
            assert is_db_thread_safe(registered)
            worker_thread = await run_db(write_reg, db=registered)
            assert worker_thread != loop_thread
        finally:
            registered.close()

    asyncio.run(scenario())


def test_memory_store_and_outbound_store_offload_off_event_loop(tmp_path: Path):
    async def scenario():
        loop_thread = threading.get_ident()
        store = MemoryStore(tmp_path / "memory.sqlite3")
        outbound_store = OutboundReceiptStore(
            tmp_path / "outbound.sqlite3",
            account="bot",
            default_room="room@chatroom",
        )
        observed_threads: list[int] = []

        orig_archive = store.archive_message
        orig_recent = store.recent_messages

        def traced_archive(**kwargs):
            observed_threads.append(threading.get_ident())
            return orig_archive(**kwargs)

        def traced_recent(**kwargs):
            observed_threads.append(threading.get_ident())
            return orig_recent(**kwargs)

        store.archive_message = traced_archive  # type: ignore[method-assign]
        store.recent_messages = traced_recent  # type: ignore[method-assign]
        try:
            ingestor = MessageIngestor(store)
            event = MessageEvent(
                account_id="bot",
                conversation_id="room@chatroom",
                sender_id="alice",
                sender_origin="channel_group_member",
                text="hello async sqlite",
                is_group=True,
                message_type=1,
                source_msg_id=101,
                capture_tick_ms=1_700_000_000_000,
            )
            batch = await ingestor.ingest((event,))
            builder = ContextBuilder(store)
            built = await builder.build(ContextSpec.for_chat(ingested=batch))
            assert len(built.context.turns) == 1
            assert observed_threads
            assert all(tid != loop_thread for tid in observed_threads)
            assert is_db_thread_safe(store.db)
            assert is_db_thread_safe(outbound_store.db)
        finally:
            outbound_store.close()
            store.close()

    asyncio.run(scenario())
