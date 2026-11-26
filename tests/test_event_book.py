from datetime import UTC, datetime, timedelta

from echoloom.event_book import EventBook
from echoloom.memory_store import MemoryStore


def test_event_book_enforces_priority_slots_character_caps_and_exit_rules(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    book = EventBook(store)
    high_ids = []
    for index in range(4):
        item = book.add(
            account_id="bot",
            priority="high",
            content=f"重要事项 {index}",
            exit_condition="完成后删除",
        )
        assert item["status"] == "saved"
        high_ids.append(item["event_id"])
    assert (
        book.add(account_id="bot", priority="high", content="第五件", exit_condition="完成后删除")[
            "status"
        ]
        == "full"
    )
    assert (
        book.update(account_id="bot", event_id=high_ids[0], content="更新后的事项")["status"]
        == "updated"
    )
    assert book.delete(account_id="bot", event_id=high_ids[1])["status"] == "deleted"
    assert len([row for row in book.list_entries("bot") if row["priority"] == "high"]) == 3

    ordinary = book.add(
        account_id="bot",
        priority="ordinary",
        content="记得下周回访",
        exit_condition="回访完成即删除",
    )
    assert ordinary["status"] == "saved"
    assert "记得下周回访" in book.render("bot")
    store.close()


def test_event_book_rejects_oversized_and_expired_rows(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    book = EventBook(store)
    try:
        book.add(account_id="bot", priority="high", content="x" * 201, exit_condition="done")
    except ValueError:
        pass
    else:
        raise AssertionError("oversized high-priority event was accepted")
    entry = book.add(
        account_id="bot",
        priority="ordinary",
        content="expire me",
        exit_condition="deadline",
        expires_at="2020-01-01T00:00:00+00:00",
    )
    assert entry["status"] == "saved"
    assert all(row["content"] != "expire me" for row in book.list_entries("bot"))
    store.close()


def test_ordinary_event_generation_age_uses_latest_revision_time(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    book = EventBook(store)
    entry = book.add(
        account_id="bot",
        priority="ordinary",
        content="近期安排",
        exit_condition="安排完成后删除",
    )
    book.update(account_id="bot", event_id=entry["event_id"], content="更新后的近期安排")
    old_created = (datetime.now(UTC) - timedelta(days=10)).isoformat()
    old_updated = (datetime.now(UTC) - timedelta(hours=80)).isoformat()
    with store._lock, store.db:
        store.db.execute(
            "UPDATE event_book SET created_at=?,updated_at=? WHERE event_id=?",
            (old_created, old_updated, entry["event_id"]),
        )

    stale_render = book.render("bot")
    assert "生成时间：3天8小时" in stale_render
    assert "已过时，先复核并更新或删除" in stale_render
    assert store.db.execute(
        "SELECT count(*) FROM event_book_revisions WHERE event_id=?",
        (entry["event_id"],),
    ).fetchone()[0] == 2

    book.update(account_id="bot", event_id=entry["event_id"], content="已重新确认")
    fresh_render = book.render("bot")
    assert "生成时间：0天0小时" in fresh_render
    assert "已过时，先复核并更新或删除" not in fresh_render
    assert store.db.execute(
        "SELECT count(*) FROM event_book_revisions WHERE event_id=?",
        (entry["event_id"],),
    ).fetchone()[0] == 3
    store.close()
