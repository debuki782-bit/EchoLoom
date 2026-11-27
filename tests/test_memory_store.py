import json
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from echoloom.context import build_hook_context
from echoloom.hook_contract import parse_callback
from echoloom.memory_store import MemoryStore, StoredMessage
from echoloom.repos import KnowledgeRepo, MediaRepo, MessageRepo
from echoloom.repos import StoredMessage as RepoStoredMessage


def _callback(body="wxid_member:\n原始消息正文"):
    payload = {"sendorrecv": 2, "selfwxid": "wxid_bot", "msglist": [{
        "fromid": "room@chatroom", "toid": "wxid_bot", "room": "room@chatroom",
        "sender": "wxid_member", "msg": body, "msgtype": 1,
        "msgsvrid": 0, "time": 123,
    }]}
    return payload, parse_callback(payload)[0]


def test_raw_message_roundtrip_and_notes_are_separate_versioned_untrusted_data(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    payload, event = _callback()
    archived = store.archive_message(
        account_id=event.account_id, conversation_id=event.conversation_id,
        sender_id=event.sender_id, raw_envelope=json.loads(event.raw_envelope_json),
        source_record=json.loads(event.raw_message_json), body_text=event.text,
        event_time_context={"event_utc_minute": "2026-10-29 02:03",
                            "local_minute": "2026-10-29 10:03", "day_type": "工作日"},
    )
    assert store.get_event_time_context(
        message_id=archived.message_id, account_id=event.account_id,
    ) == {"day_type": "工作日", "local_minute": "2026-10-29 10:03"}
    note = store.add_note(account_id=event.account_id, message_id=archived.message_id,
                          content="值得保留：会议在周五。")
    assert store.reconstruct_message(archived.message_id, event.account_id) == payload
    assert store.notes_for_message(account_id=event.account_id,
                                   message_id=archived.message_id)[0]["note_id"] == note
    version = store.revise_note(account_id=event.account_id, note_id=note,
                                content="已更正：会议在周六。")
    assert version == 2
    assert len(store.db.execute(
        "SELECT * FROM annotation_revisions WHERE note_id=?", (note,),
    ).fetchall()) == 2
    replayed = store.archive_message(
        account_id=event.account_id, conversation_id=event.conversation_id,
        sender_id=event.sender_id, raw_envelope=json.loads(event.raw_envelope_json),
        source_record=json.loads(event.raw_message_json), body_text=event.text,
    )
    assert replayed.message_id == archived.message_id
    matching_notes = store.search_notes(account_id=event.account_id,
        conversation_id=event.conversation_id, query="原始消息正文")
    assert matching_notes[0]["content"] == "已更正：会议在周六。"
    context = build_hook_context((event,), stored=[{
        "message_id": archived.message_id,
        "annotations": store.notes_for_message(account_id=event.account_id,
                                                 message_id=archived.message_id),
    }]).model_user_content()
    assert "wxid_member：*{原始消息正文}*" in context
    assert "已更正：会议在周六。" in context
    assert "untrusted_annotation" not in context
    assert "已更正：会议在周六。" in context
    store.close()


def test_last_human_gap_uses_inbound_only_and_ignores_agent_outbound(tmp_path):
    store = MemoryStore(tmp_path / "gap.sqlite3")
    now = datetime.now(UTC)
    human = store.archive_message(
        account_id="wxid_bot", conversation_id="friend", sender_id="friend",
        raw_envelope={"msg": "真人消息"}, body_text="真人消息", direction="inbound",
    )
    own = store.archive_agent_message(
        account_id="wxid_bot", conversation_id="friend", text="Agent 后续消息",
        operation_id="outbound-gap-test", delivery_state="queued",
    )
    store.db.execute(
        "UPDATE source_messages SET captured_at=? WHERE message_id=?",
        ((now - timedelta(minutes=60)).isoformat(), human.message_id),
    )
    store.db.execute(
        "UPDATE source_messages SET captured_at=? WHERE message_id=?",
        (now.isoformat(), own.message_id),
    )
    store.db.commit()
    minutes = store.minutes_since_last_human_message(
        account_id="wxid_bot", conversation_id="friend",
    )
    assert minutes is not None and 59 <= minutes <= 60
    store.close()


def test_human_sender_remark_is_separate_and_keeps_revision_history(tmp_path):
    store = MemoryStore(tmp_path / "remarks.sqlite3")
    _, event = _callback()
    store.archive_message(
        account_id=event.account_id, conversation_id=event.conversation_id,
        sender_id=event.sender_id, raw_envelope=json.loads(event.raw_envelope_json),
        source_record=json.loads(event.raw_message_json), body_text=event.text,
    )

    first = store.set_human_sender_remark(
        account_id=event.account_id, sender_id=event.sender_id, remark="主人同学",
    )
    assert first == {"status": "saved", "version": 1}
    assert store.sender_remarks(
        account_id=event.account_id, sender_ids=[event.sender_id],
    ) == {event.sender_id: "主人同学"}
    second = store.set_human_sender_remark(
        account_id=event.account_id, sender_id=event.sender_id, remark="同学甲",
    )
    assert second["version"] == 2
    cleared = store.set_human_sender_remark(
        account_id=event.account_id, sender_id=event.sender_id, remark="",
    )
    assert cleared == {"status": "cleared", "version": 3}
    assert store.sender_remarks(
        account_id=event.account_id, sender_ids=[event.sender_id],
    ) == {}
    revisions = store.db.execute(
        "SELECT version,remark,action FROM sender_profile_revisions "
        "WHERE account_id=? AND sender_id=? ORDER BY version",
        (event.account_id, event.sender_id),
    ).fetchall()
    assert [(row["version"], row["remark"], row["action"]) for row in revisions] == [
        (1, "主人同学", "set"), (2, "同学甲", "set"), (3, "", "delete"),
    ]
    with pytest.raises(KeyError):
        store.set_human_sender_remark(
            account_id=event.account_id, sender_id="wxid_not_archived", remark="无效",
        )
    store.close()


def test_context_enrichment_reconstructs_message_gaps_for_whole_batch(tmp_path):
    store = MemoryStore(tmp_path / "gaps.sqlite3")
    messages = []
    for sender_id, text, captured_at in (
        ("wxid_member", "first", "2026-10-29T10:00:00+00:00"),
        ("wxid_member", "second", "2026-10-29T10:03:00+00:00"),
        ("wxid_other", "third", "2026-10-29T10:05:00+00:00"),
    ):
        body = f"{sender_id}:\n{text}"
        payload = {"msglist": [{"msg": body}]}
        archived = store.archive_message(
            account_id="wxid_bot", conversation_id="room@chatroom",
            sender_id=sender_id, raw_envelope=payload, body_text=body,
        )
        store.db.execute(
            "UPDATE source_messages SET captured_at=? WHERE message_id=?",
            (captured_at, archived.message_id),
        )
        messages.append({
            "message_id": archived.message_id, "account_id": "wxid_bot",
            "conversation_id": "room@chatroom", "sender_id": sender_id,
            "captured_at": captured_at, "body_text": text,
        })
    store.db.commit()

    enriched = store.enrich_context_messages(account_id="wxid_bot", messages=messages)
    assert [row["conversation_gap_minutes"] for row in enriched] == [None, 3, 2]
    assert [row["sender_gap_minutes"] for row in enriched] == [None, 3, None]
    store.close()


def test_sender_tags_many_at_returns_historical_snapshots(tmp_path):
    store = MemoryStore(tmp_path / "tag-snapshots.sqlite3")
    store.db.executemany(
        "INSERT INTO sender_tag_diffs VALUES (?,?,?,?,?,?,?,?)",
        (
            ("d1", "wxid_bot", "wxid_member", "add", "同学", None, None,
             "2026-10-29T10:01:00+00:00"),
            ("d2", "wxid_bot", "wxid_member", "add", "猫奴", None, None,
             "2026-10-29T10:04:00+00:00"),
        ),
    )
    snapshots = store.sender_tags_many_at(
        account_id="wxid_bot",
        requests=(
            ("m1", "wxid_member", "2026-10-29T10:03:00+00:00"),
            ("m2", "wxid_member", "2026-10-29T10:05:00+00:00"),
        ),
    )
    assert snapshots == {"m1": ["同学"], "m2": ["同学", "猫奴"]}
    store.close()


def test_existing_message_database_adds_event_time_column_without_replacing_data(tmp_path):
    path = tmp_path / "legacy-memory.sqlite3"
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE source_messages (message_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, "
            "conversation_id TEXT NOT NULL, sender_id TEXT NOT NULL, captured_at TEXT NOT NULL, "
            "raw_json TEXT NOT NULL, raw_sha256 TEXT NOT NULL, body_text TEXT NOT NULL, "
            "expires_at TEXT NOT NULL, deleted_at TEXT, source_fingerprint TEXT)"
        )
        conn.execute(
            "INSERT INTO source_messages VALUES ('old','account','chat','sender','now',"
            "'{}','hash','body','later',NULL,'fingerprint')"
        )
    store = MemoryStore(path)
    columns = {row[1] for row in store.db.execute("PRAGMA table_info(source_messages)")}
    assert "event_time_context_json" in columns
    assert store.db.execute("SELECT body_text FROM source_messages WHERE message_id='old'").fetchone()[0] == "body"
    store.close()


def test_fact_scope_conflict_update_and_source_survive_search(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _, event = _callback()
    archived = store.archive_message(account_id=event.account_id,
        conversation_id=event.conversation_id, sender_id=event.sender_id,
        raw_envelope=json.loads(event.raw_envelope_json), body_text=event.text)
    first = store.remember_fact(account_id=event.account_id, fact_key="meeting_day",
        content="周五", source_message_id=archived.message_id)
    second = store.remember_fact(account_id=event.account_id, fact_key="meeting_day",
        content="周六", source_message_id=archived.message_id)
    result = store.search_facts(account_id=event.account_id, query="周六")
    assert result[0]["fact_id"] == second
    assert result[0]["source_message_id"] == archived.message_id
    assert store.search_facts(account_id="other-account", query="周六") == []
    with pytest.raises(KeyError):
        store.delete_fact(account_id="other-account", fact_id=second)
    store.delete_fact(account_id=event.account_id, fact_id=second)
    assert store.search_facts(account_id=event.account_id, query="周六") == []
    assert store.db.execute("SELECT state FROM user_facts WHERE fact_id=?",
                            (first,)).fetchone()[0] == "superseded"
    store.close()


def test_entity_matching_is_literal_bounded_and_dictionary_is_untrusted(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    entity = store.upsert_entity(account_id="account", term="C++ (ops)",
                                 explanation="团队词条说明")
    hits = store.match_entities(account_id="account", text="聊到 C++ (ops)啦")
    assert hits[0]["entity_id"] == entity
    assert hits[0]["trust_label"] == "untrusted_dictionary"
    assert store.match_entities(account_id="account", text="XC++ (ops)X") == []
    store.delete_entity(account_id="account", entity_id=entity)
    assert store.match_entities(account_id="account", text="C++ (ops)") == []
    regex_term = store.upsert_entity(account_id="account", term=".*", explanation="literal pattern")
    assert store.match_entities(account_id="account", text=".*")[-1]["entity_id"] == regex_term
    assert store.match_entities(account_id="account", text="anything") == []
    replacement = store.upsert_entity(account_id="account", term="C++ (ops)",
                                      explanation="更新后的团队词条说明")
    assert replacement != entity
    assert store.match_entities(account_id="account", text="C++ (ops)")[0]["entity_id"] == replacement
    store.close()


def test_context_enrichment_batches_fresh_dictionary_and_message_annotations(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _, first_event = _callback("wxid_member:\n第一条里有术语 A")
    _, second_event = _callback("wxid_member:\n第二条里也有术语 A")
    first = store.archive_message(
        account_id=first_event.account_id,
        conversation_id=first_event.conversation_id,
        sender_id=first_event.sender_id,
        raw_envelope=json.loads(first_event.raw_envelope_json),
        body_text=first_event.text,
    )
    second = store.archive_message(
        account_id=second_event.account_id,
        conversation_id=second_event.conversation_id,
        sender_id=second_event.sender_id,
        raw_envelope=json.loads(second_event.raw_envelope_json),
        body_text=second_event.text,
    )
    store.add_note(account_id=first_event.account_id, message_id=first.message_id,
                   content="这条消息说明术语 A 已确认。")
    term_id = store.upsert_entity(account_id=first_event.account_id, term="术语 A",
                                  explanation="初版解释")
    records = [
        {"message_id": first.message_id, "body_text": first_event.text},
        {"message_id": second.message_id, "body_text": second_event.text},
        {"message_id": first.message_id, "body_text": first_event.text},
    ]
    enriched = store.enrich_context_messages(
        account_id=first_event.account_id, messages=records,
    )
    assert enriched[0]["annotations"][0]["content"] == "这条消息说明术语 A 已确认。"
    assert enriched[0]["entities"][0]["entity_id"] == term_id
    assert enriched[1]["entities"][0]["entity_id"] == term_id
    assert enriched[2]["entities"][0]["entity_id"] == term_id

    store.upsert_entity(account_id=first_event.account_id, term="术语 A",
                        explanation="更新后的解释")
    refreshed = store.enrich_context_messages(
        account_id=first_event.account_id, messages=records[:1],
    )
    assert refreshed[0]["entities"][0]["explanation"] == "更新后的解释"
    store.close()


def test_archive_expiry_soft_deletes_source_and_associated_memory(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _, event = _callback()
    archived = store.archive_message(account_id=event.account_id,
        conversation_id=event.conversation_id, sender_id=event.sender_id,
        raw_envelope=json.loads(event.raw_envelope_json), body_text=event.text,
        retention_days=1)
    store.add_note(account_id=event.account_id, message_id=archived.message_id,
                   content="短期备注")
    purged = store.purge_expired(now="2999-01-01T00:00:00+00:00")
    assert purged == 1
    with pytest.raises(KeyError):
        store.reconstruct_message(archived.message_id, event.account_id)
    assert store.notes_for_message(account_id=event.account_id,
                                   message_id=archived.message_id) == []
    store.close()


def test_repos_share_db_lock_and_atomic_transactions(tmp_path):
    store = MemoryStore(tmp_path / "repos-shared.sqlite3")
    assert isinstance(store.messages, MessageRepo)
    assert isinstance(store.media, MediaRepo)
    assert isinstance(store.knowledge, KnowledgeRepo)
    assert StoredMessage is RepoStoredMessage
    assert store.messages.db is store.db
    assert store.media.db is store.db
    assert store.knowledge.db is store.db
    assert store.messages._lock is store._lock
    assert store.media._lock is store._lock
    assert store.knowledge._lock is store._lock
    assert store.db.execute("PRAGMA journal_mode").fetchone()[0].lower() == "wal"
    assert store.db.execute("PRAGMA foreign_keys").fetchone()[0] == 1

    # Rollback across all three repos inside one shared MemoryStore.transaction()
    with pytest.raises(RuntimeError, match="abort-cross-repo"), store.transaction():
        msg = store.messages.archive_message(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            sender_id="wxid_member",
            raw_envelope={"msg": "rollback me"},
            body_text="rollback me",
        )
        store.media.begin_image_ingest(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            message_id=msg.message_id,
            original_references=[{"kind": "md5", "value": "abc"}],
        )
        store.knowledge.add_note(
            account_id="wxid_bot",
            message_id=msg.message_id,
            content="should roll back",
        )
        raise RuntimeError("abort-cross-repo")

    assert store.db.execute("SELECT count(*) FROM source_messages").fetchone()[0] == 0
    assert store.db.execute("SELECT count(*) FROM message_media").fetchone()[0] == 0
    assert store.db.execute("SELECT count(*) FROM annotations").fetchone()[0] == 0

    # Committed writes across all three repos persist atomically
    with store.transaction():
        saved = store.messages.archive_message(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            sender_id="wxid_member",
            raw_envelope={"msg": "commit me"},
            body_text="commit me",
        )
        media = store.media.begin_image_ingest(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            message_id=saved.message_id,
            original_references=[{"kind": "md5", "value": "xyz"}],
        )
        note_id = store.knowledge.add_note(
            account_id="wxid_bot",
            message_id=saved.message_id,
            content="persisted note",
        )

    assert store.reconstruct_message(saved.message_id, "wxid_bot") == {"msg": "commit me"}
    assert store.image_metadata_for_message(
        account_id="wxid_bot", message_id=saved.message_id
    )["image_id"] == media["image_id"]
    assert store.notes_for_message(
        account_id="wxid_bot", message_id=saved.message_id
    )[0]["note_id"] == note_id
    store.close()


def test_direct_repo_access_and_facade_parity(tmp_path):
    store = MemoryStore(tmp_path / "parity.sqlite3")
    png_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 32
    store.media_root.mkdir(parents=True, exist_ok=True)
    img_path = store.media_root / "agent-out.png"
    img_path.write_bytes(png_bytes)

    # Direct MessageRepo outbound archive with cross-repo note creation
    agent_msg = store.messages.archive_agent_message(
        account_id="wxid_bot",
        conversation_id="room@chatroom",
        text="hello from repo",
        operation_id="op-msg-1",
        delivery_state="queued",
        note="outbound note",
    )
    assert store.notes_for_message(
        account_id="wxid_bot", message_id=agent_msg.message_id
    )[0]["content"] == "outbound note"

    # Direct MediaRepo outbound image archive with cross-repo note + library visibility
    img_record = store.media.archive_agent_image(
        account_id="wxid_bot",
        conversation_id="room@chatroom",
        artifact_path=img_path,
        mime_type="image/png",
        operation_id="op-img-1",
        delivery_state="queued",
        note="cute cat sticker",
    )
    assert store.get_image_by_id(
        account_id="wxid_bot",
        conversation_id="room@chatroom",
        image_id=img_record["image_id"],
    )["image_id"] == img_record["image_id"]
    assert store.quick_send_images(account_id="wxid_bot")[0]["image_id"] == img_record["image_id"]
    assert store.knowledge.notes_for_message(
        account_id="wxid_bot", message_id=img_record["message_id"]
    )[0]["content"] == "cute cat sticker"

    # Direct MessageRepo inbound + JEV labels + KnowledgeRepo tags/facts/entities
    inbound = store.messages.archive_message(
        account_id="wxid_bot",
        conversation_id="room@chatroom",
        sender_id="wxid_member",
        raw_envelope={"msg": "聊聊 V87"},
        body_text="聊聊 V87",
        direction="inbound",
    )
    assert store.messages.save_jev_labels(
        account_id="wxid_bot",
        labels=[{
            "message_id": inbound.message_id,
            "probabilities": {"reply": 70, "concise": 20, "no_reply": 10},
            "multi_turn_pct": 40,
            "model": "jev-test",
        }],
    ) == 1
    assert store.knowledge.update_sender_tags(
        account_id="wxid_bot",
        sender_id="wxid_member",
        action="add",
        tags=["猫友"],
        source_message_id=inbound.message_id,
    )["status"] == "updated"
    store.knowledge.upsert_entity(
        account_id="wxid_bot", term="V87", explanation="测试项目代号"
    )

    # Audio ingest + transcription via direct MediaRepo, read via facade
    audio = store.media.begin_audio_ingest(
        account_id="wxid_bot",
        conversation_id="room@chatroom",
        message_id=inbound.message_id,
        duration_ms=1500,
    )
    store.audio_root.mkdir(parents=True, exist_ok=True)
    wav_path = store.audio_root / "clip.wav"
    wav_path.write_bytes(b"RIFF0000WAVE")
    store.media.complete_audio_ingest(
        audio_id=str(audio["audio_id"]),
        account_id="wxid_bot",
        artifact_path=str(wav_path),
        byte_count=12,
        sha256="a" * 64,
        duration_ms=1500,
    )
    assert store.claim_audio_transcription(
        audio_id=str(audio["audio_id"]), account_id="wxid_bot"
    ) == "claimed"
    store.media.save_audio_transcript(
        audio_id=str(audio["audio_id"]),
        account_id="wxid_bot",
        transcript="语音提到 V87",
        model_id="asr-test",
    )

    # Facade enrich_context_messages combines all three repos seamlessly
    enriched = store.enrich_context_messages(
        account_id="wxid_bot",
        messages=[{
            "message_id": inbound.message_id,
            "conversation_id": "room@chatroom",
            "sender_id": "wxid_member",
            "captured_at": "2099-01-01T00:00:00+00:00",
            "body_text": "聊聊 V87",
        }],
    )
    assert enriched[0]["jev_label"]["probabilities"] == {"reply": 70, "concise": 20, "no_reply": 10}
    assert enriched[0]["sender_tags"] == ["猫友"]
    assert enriched[0]["entities"][0]["term"] == "V87"
    assert enriched[0]["audio"]["transcript"] == "语音提到 V87"

    # MemoryStore._message_dict staticmethod preserved for cross_session.py direct callers
    row = store.db.execute(
        "SELECT message_id,conversation_id,sender_id,body_text,captured_at,direction,"
        "delivery_state,event_time_context_json FROM source_messages WHERE message_id=?",
        (inbound.message_id,),
    ).fetchone()
    assert MemoryStore._message_dict(row) == MessageRepo.message_dict(row)
    assert MemoryStore._message_dict(row)["source_is_group"] is True
    store.close()


def test_tokenized_fact_and_note_search_handles_spaced_queries_and_cross_chat(tmp_path):
    store = MemoryStore(tmp_path / "memory.sqlite3")
    _, ev_room = _callback("wxid_member:\n支持三开多任务分屏的平板设备讨论")
    archived_room = store.archive_message(
        account_id=ev_room.account_id,
        conversation_id=ev_room.conversation_id,
        sender_id=ev_room.sender_id,
        raw_envelope=json.loads(ev_room.raw_envelope_json),
        body_text=ev_room.text,
    )
    archived_dm = store.archive_message(
        account_id=ev_room.account_id,
        conversation_id="wxid_friend",
        sender_id="wxid_friend",
        raw_envelope={"msg": "周末吃火锅"},
        body_text="周末吃火锅",
    )
    store.remember_fact(
        account_id=ev_room.account_id,
        fact_key="master_interest_chess",
        content="主人近期沉迷国际象棋战术",
        source_message_id=archived_room.message_id,
    )
    store.remember_fact(
        account_id=ev_room.account_id,
        fact_key="tablet_usage",
        content="拥有支持三开分屏的平板类设备",
        source_message_id=archived_room.message_id,
    )
    store.add_note(
        account_id=ev_room.account_id,
        message_id=archived_room.message_id,
        content="夜间多屏网页浏览线索",
    )
    store.add_note(
        account_id=ev_room.account_id,
        message_id=archived_dm.message_id,
        content="私聊约饭线索",
    )

    # Spaced AND match across non-adjacent words
    assert store.search_facts(account_id=ev_room.account_id, query="平板 设备")[0]["fact_key"] == "tablet_usage"
    # Multi-word OR ranked fallback when synonym is absent ("国象" vs "国际象棋")
    assert store.search_facts(account_id=ev_room.account_id, query="国际象棋 国象")[0]["fact_key"] == "master_interest_chess"
    # Room-scoped search sees only room note; account-wide (conversation_id=None) sees both
    room_hits = store.search_notes(
        account_id=ev_room.account_id,
        conversation_id=ev_room.conversation_id,
        query="平板 浏览",
    )
    assert len(room_hits) == 1
    assert store.search_notes(
        account_id=ev_room.account_id,
        conversation_id=ev_room.conversation_id,
        query="约饭",
    ) == []
    all_hits = store.search_notes(
        account_id=ev_room.account_id,
        conversation_id=None,
        query="约饭 线索",
    )
    assert len(all_hits) == 1
    assert all_hits[0]["conversation_id"] == "wxid_friend"
    store.close()


def test_memory_store_enables_busy_timeout_and_wal(tmp_path):
    store = MemoryStore(tmp_path / "busy.sqlite3")
    try:
        assert int(store.db.execute("PRAGMA busy_timeout").fetchone()[0]) == 5000
        assert str(store.db.execute("PRAGMA journal_mode").fetchone()[0]).lower() == "wal"
    finally:
        store.close()


def test_contact_directory_upsert_batch_lookup_and_non_empty_preservation(tmp_path):
    store = MemoryStore(tmp_path / "contact-directory.sqlite3")
    try:
        # Insert new contact
        assert store.upsert_contact_directory(
            account_id="wxid_bot",
            contact_id="wxid_alice",
            nickname="  Alice  ",
            alias="alice_wx",
            source="hook",
        ) is True
        # Idempotent repeat returns False
        assert store.upsert_contact_directory(
            account_id="wxid_bot",
            contact_id="wxid_alice",
            nickname="Alice",
            alias="alice_wx",
            source="hook",
        ) is False
        # Empty nickname + empty alias returns False and preserves cached values
        assert store.upsert_contact_directory(
            account_id="wxid_bot",
            contact_id="wxid_alice",
            nickname="   ",
            alias="",
            source="hook",
        ) is False
        # Rename updates nickname while preserving existing non-empty alias
        assert store.upsert_contact_directory(
            account_id="wxid_bot",
            contact_id="wxid_alice",
            nickname="Alice Renamed",
            alias="",
            source="hook",
        ) is True
        # Empty nickname with new alias updates alias while preserving nickname
        assert store.upsert_contact_directory(
            account_id="wxid_bot",
            contact_id="wxid_alice",
            nickname="",
            alias="alice_new",
            source="hook",
        ) is True

        # Validation rejects empty/overlong/null-byte IDs or overlong/null-byte names
        with pytest.raises(ValueError):
            store.upsert_contact_directory(
                account_id="", contact_id="wxid_alice", nickname="Alice",
            )
        with pytest.raises(ValueError):
            store.upsert_contact_directory(
                account_id="wxid_bot", contact_id="wxid\0bad", nickname="Alice",
            )
        with pytest.raises(ValueError):
            store.upsert_contact_directory(
                account_id="wxid_bot", contact_id="wxid_alice", nickname="x" * 257,
            )

        # Batch upsert skips invalid/empty entries and preserves existing alias
        changed = store.upsert_contact_directory_batch(
            account_id="wxid_bot",
            entries=[
                ("wxid_alice", "Alice Batch", "dashboard"),
                ("wxid_bob", "Bob", "hook"),
                ("wxid_empty", "   ", "hook"),
                ("", "NoId", "hook"),
                ("wxid_null\0", "NullId", "hook"),
                ("wxid_overlong", "a" * 257, "hook"),
            ],
        )
        assert changed == 2
        # Repeating the same batch makes zero changes
        assert store.upsert_contact_directory_batch(
            account_id="wxid_bot",
            entries=[
                ("wxid_alice", "Alice Batch", "dashboard"),
                ("wxid_bob", "Bob", "hook"),
            ],
        ) == 0

        # Account isolation
        assert store.upsert_contact_directory(
            account_id="wxid_other_bot",
            contact_id="wxid_bob",
            nickname="Other Bob",
            source="hook",
        ) is True

        # contact_directory_map: empty sequence -> {}, filtered -> subset, None -> all for account
        assert store.contact_directory_map(account_id="wxid_bot", contact_ids=[]) == {}
        filtered = store.contact_directory_map(
            account_id="wxid_bot", contact_ids=["wxid_alice", "wxid_missing"],
        )
        assert list(filtered.keys()) == ["wxid_alice"]
        assert filtered["wxid_alice"]["nickname"] == "Alice Batch"
        assert filtered["wxid_alice"]["alias"] == "alice_new"
        assert filtered["wxid_alice"]["source"] == "dashboard"
        assert filtered["wxid_alice"]["updated_at"]

        all_rows = store.contact_directory_map(account_id="wxid_bot", contact_ids=None)
        assert set(all_rows.keys()) == {"wxid_alice", "wxid_bob"}
        assert all_rows["wxid_bob"]["nickname"] == "Bob"
    finally:
        store.close()


def test_all_sender_remarks_and_chatroom_conversation_remark(tmp_path):
    store = MemoryStore(tmp_path / "remarks-all.sqlite3")
    try:
        _, event = _callback()
        store.archive_message(
            account_id=event.account_id,
            conversation_id=event.conversation_id,
            sender_id=event.sender_id,
            raw_envelope=json.loads(event.raw_envelope_json),
            source_record=json.loads(event.raw_message_json),
            body_text=event.text,
        )
        store.archive_message(
            account_id=event.account_id,
            conversation_id="wxid_friend",
            sender_id="wxid_friend",
            raw_envelope={"msg": "hi"},
            body_text="hi",
        )
        store.archive_message(
            account_id="wxid_other_bot",
            conversation_id="other@chatroom",
            sender_id="wxid_other_member",
            raw_envelope={"msg": "other"},
            body_text="other",
        )

        # Remark on both individual sender_id and @chatroom conversation_id
        assert store.set_human_sender_remark(
            account_id=event.account_id, sender_id=event.sender_id, remark="群友甲",
        ) == {"status": "saved", "version": 1}
        assert store.set_human_sender_remark(
            account_id=event.account_id,
            sender_id=event.conversation_id,
            remark="核心测试群",
        ) == {"status": "saved", "version": 1}
        assert store.set_human_sender_remark(
            account_id=event.account_id, sender_id="wxid_friend", remark="临时好友",
        ) == {"status": "saved", "version": 1}
        assert store.set_human_sender_remark(
            account_id="wxid_other_bot", sender_id="other@chatroom", remark="别的账号群",
        ) == {"status": "saved", "version": 1}

        # Clearing one remark excludes it from all_sender_remarks (state='deleted')
        assert store.set_human_sender_remark(
            account_id=event.account_id, sender_id="wxid_friend", remark="",
        ) == {"status": "cleared", "version": 2}

        assert store.all_sender_remarks(account_id=event.account_id) == {
            event.sender_id: "群友甲",
            event.conversation_id: "核心测试群",
        }
        assert store.all_sender_remarks(account_id="wxid_other_bot") == {
            "other@chatroom": "别的账号群",
        }
    finally:
        store.close()


def test_short_refs_immutable_monotonic_eager_writes_and_resolve(tmp_path):
    from echoloom.memory_store import SHORT_REF_PREFIXES, identity_ref_kind

    assert SHORT_REF_PREFIXES == {
        "message": "M",
        "user": "U",
        "chat": "G",
        "event": "E",
        "note": "N",
        "entity": "D",
        "memory": "P",
        "image": "I",
        "file": "F",
        "audio": "A",
    }
    assert identity_ref_kind("wxid_alice") == "user"
    assert identity_ref_kind("123@chatroom") == "chat"

    store = MemoryStore(tmp_path / "short-refs.sqlite3")
    try:
        # Eager assignment on message + identity writes
        m1 = store.archive_message(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            sender_id="wxid_alice",
            raw_envelope={"msg": "first"},
            body_text="first",
        )
        m2 = store.archive_agent_message(
            account_id="wxid_bot",
            conversation_id="wxid_bob",
            text="second",
            operation_id="op-short-ref-1",
            delivery_state="queued",
            note="outbound note",
        )
        note_id = store.add_note(
            account_id="wxid_bot", message_id=m1.message_id, content="重点备忘"
        )
        entity_id = store.upsert_entity(
            account_id="wxid_bot", term="EchoLoom", explanation="项目名"
        )
        memory_id, created = store.upsert_sender_memory(
            account_id="wxid_bot", sender_id="wxid_alice", topic="爱好", content="喜欢猫"
        )
        assert created is True
        img = store.begin_image_ingest(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            message_id=m1.message_id,
            original_references=[],
        )
        aud = store.begin_audio_ingest(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            message_id=m1.message_id,
            duration_ms=1000,
        )
        fil = store.begin_file_ingest(
            account_id="wxid_bot",
            conversation_id="room@chatroom",
            message_id=m1.message_id,
            original_references=[],
            filename="doc.txt",
            fileext="txt",
        )

        refs = store.short_refs_map(
            account_id="wxid_bot",
            items=[
                ("message", m1.message_id),
                ("message", m2.message_id),
                ("user", "wxid_alice"),
                ("user", "wxid_bot"),
                ("user", "wxid_bob"),
                ("chat", "room@chatroom"),
                ("note", note_id),
                ("entity", entity_id),
                ("memory", memory_id),
                ("image", str(img["image_id"])),
                ("audio", str(aud["audio_id"])),
                ("file", str(fil["file_id"])),
            ],
        )
        assert refs[("message", m1.message_id)] == "M1"
        assert refs[("message", m2.message_id)] == "M2"
        assert refs[("user", "wxid_alice")] == "U1"
        assert refs[("chat", "room@chatroom")] == "G1"
        assert refs[("user", "wxid_bot")] == "U2"
        assert refs[("user", "wxid_bob")] == "U3"
        assert refs[("note", note_id)] == "N2"  # N1 was archive_agent_message's note
        assert refs[("entity", entity_id)] == "D1"
        assert refs[("memory", memory_id)] == "P1"
        assert refs[("image", str(img["image_id"]))] == "I1"
        assert refs[("audio", str(aud["audio_id"]))] == "A1"
        assert refs[("file", str(fil["file_id"]))] == "F1"

        # Immutable re-ensure returns the exact same ref without advancing counter
        assert (
            store.ensure_short_ref(
                account_id="wxid_bot", kind="message", target_id=m1.message_id
            )
            == "M1"
        )
        # Monotonic even if a record is soft-deleted
        store.delete_entity(account_id="wxid_bot", entity_id=entity_id)
        entity_id2 = store.upsert_entity(
            account_id="wxid_bot", term="EchoLoom", explanation="新版解释"
        )
        assert (
            store.short_refs_map(account_id="wxid_bot", items=[("entity", entity_id2)])[
                ("entity", entity_id2)
            ]
            == "D2"
        )

        # Atomic rollback on aborted transaction leaves no short_ref leaked
        with pytest.raises(RuntimeError, match="abort-short-ref"), store.transaction():
            store.ensure_short_ref(
                account_id="wxid_bot", kind="user", target_id="wxid_rolled_back"
            )
            raise RuntimeError("abort-short-ref")
        assert (
            store.short_refs_map(
                account_id="wxid_bot", items=[("user", "wxid_rolled_back")]
            )
            == {}
        )
        # Counter was also rolled back so next user gets U4
        assert (
            store.ensure_short_ref(account_id="wxid_bot", kind="user", target_id="wxid_carol")
            == "U4"
        )

        # resolve_short_ref: case-insensitive, kind-filtered, unknown -> None, raw passthrough
        assert (
            store.resolve_short_ref(account_id="wxid_bot", value="m1") == m1.message_id
        )
        assert (
            store.resolve_short_ref(account_id="wxid_bot", value="  g1  ", kinds=("chat",))
            == "room@chatroom"
        )
        assert (
            store.resolve_short_ref(account_id="wxid_bot", value="M1", kinds=("user",))
            is None
        )
        assert store.resolve_short_ref(account_id="wxid_bot", value="M999") is None
        assert (
            store.resolve_short_ref(account_id="other_bot", value="M1") is None
        )
        assert (
            store.resolve_short_ref(account_id="wxid_bot", value=" wxid_alice ")
            == "wxid_alice"
        )
        assert (
            store.resolve_short_ref(account_id="wxid_bot", value="room@chatroom")
            == "room@chatroom"
        )
        assert store.resolve_short_ref(account_id="wxid_bot", value="M0") == "M0"
        assert store.resolve_short_ref(account_id="wxid_bot", value="X1") == "X1"

        # Strictly read-only short_refs_map and resolve_short_ref under PRAGMA query_only=ON
        store.db.execute("PRAGMA query_only=ON")
        try:
            ro_map = store.short_refs_map(
                account_id="wxid_bot",
                items=[("message", m1.message_id), ("message", "missing_id")],
            )
            assert ro_map == {("message", m1.message_id): "M1"}
            assert (
                store.resolve_short_ref(account_id="wxid_bot", value="m2", kinds=("message",))
                == m2.message_id
            )
        finally:
            store.db.execute("PRAGMA query_only=OFF")
    finally:
        store.close()


def test_short_refs_idempotent_backfill_on_legacy_db(tmp_path):
    db_path = tmp_path / "legacy-short-refs.sqlite3"
    # Create a DB, insert rows directly bypassing short_refs, then reopen to trigger backfill
    store = MemoryStore(db_path)
    with store.transaction():
        store.db.execute("DELETE FROM short_refs")
        store.db.execute("DELETE FROM short_ref_counters")
        store.db.execute(
            "INSERT INTO source_messages("
            "message_id,account_id,conversation_id,sender_id,captured_at,raw_json,"
            "raw_sha256,body_text,expires_at,direction) VALUES "
            "('msg_late','bot','room@chatroom','wxid_b','2026-10-29T10:05:00+00:00','{}','h2','b','2099-01-01','inbound'),"
            "('msg_early','bot','room@chatroom','wxid_a','2026-10-29T10:00:00+00:00','{}','h1','a','2099-01-01','inbound')"
        )
        store.db.execute(
            "INSERT INTO annotations VALUES "
            "('note_1','bot','msg_early','agent','agent','untrusted_annotation','c','agent_observation',1,'active','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO entity_terms VALUES "
            "('ent_1','bot','Term','Exp','agent','untrusted_dictionary',1,'active','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO sender_memories VALUES "
            "('mem_1','bot','wxid_a','topic','content','untrusted_reference_data','active','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO sender_profiles VALUES "
            "('bot','wxid_profile','备注',1,'active','human','2026-10-29T10:02:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO contact_directory VALUES "
            "('bot','wxid_dir','目录昵称','','hook','2026-10-29T10:03:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO message_media(image_id,account_id,conversation_id,message_id,"
            "original_references_json,state,created_at,updated_at) VALUES "
            "('img_1','bot','room@chatroom','msg_early','[]','pending','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO message_audio(audio_id,account_id,conversation_id,message_id,"
            "state,created_at,updated_at) VALUES "
            "('aud_1','bot','room@chatroom','msg_early','pending','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
        store.db.execute(
            "INSERT INTO message_files(file_id,account_id,conversation_id,message_id,"
            "filename,state,created_at,updated_at) VALUES "
            "('fil_1','bot','room@chatroom','msg_early','f.txt','pending','2026-10-29T10:01:00+00:00','2026-10-29T10:01:00+00:00')"
        )
    store.close()

    reopened = MemoryStore(db_path)
    try:
        mapped = reopened.short_refs_map(
            account_id="bot",
            items=[
                ("message", "msg_early"),
                ("message", "msg_late"),
                ("chat", "room@chatroom"),
                ("user", "wxid_a"),
                ("user", "wxid_b"),
                ("user", "wxid_profile"),
                ("user", "wxid_dir"),
                ("note", "note_1"),
                ("entity", "ent_1"),
                ("memory", "mem_1"),
                ("image", "img_1"),
                ("audio", "aud_1"),
                ("file", "fil_1"),
            ],
        )
        # Messages ordered by captured_at ASC, message_id ASC
        assert mapped[("message", "msg_early")] == "M1"
        assert mapped[("message", "msg_late")] == "M2"
        assert mapped[("chat", "room@chatroom")] == "G1"
        assert mapped[("user", "wxid_a")].startswith("U")
        assert mapped[("user", "wxid_b")].startswith("U")
        assert mapped[("user", "wxid_profile")].startswith("U")
        assert mapped[("user", "wxid_dir")].startswith("U")
        assert mapped[("note", "note_1")] == "N1"
        assert mapped[("entity", "ent_1")] == "D1"
        assert mapped[("memory", "mem_1")] == "P1"
        assert mapped[("image", "img_1")] == "I1"
        assert mapped[("audio", "aud_1")] == "A1"
        assert mapped[("file", "fil_1")] == "F1"
        snapshot_before = dict(mapped)
    finally:
        reopened.close()

    # Reopening a second time is strictly idempotent (never renumbers)
    reopened_again = MemoryStore(db_path)
    try:
        snapshot_after = reopened_again.short_refs_map(
            account_id="bot", items=list(snapshot_before.keys())
        )
        assert snapshot_after == snapshot_before
    finally:
        reopened_again.close()
