"""Auditable project memory records; ADK remains the session/search adapter layer."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from .db_async import register_thread_safe_db
from .repos.enrichment import enrich_context_messages as _enrich_context_messages_impl
from .repos.knowledge_repo import (
    MAX_ENTITY_NOTE_CHARS,
    MAX_ENTITY_TERM_CHARS,
    MAX_FACT_CHARS,
    MAX_NOTE_CHARS,
    MAX_QUERY_CHARS,
    MAX_SENDER_MEMORY_CHARS,
    KnowledgeRepo,
    _like_escape,
)
from .repos.media_repo import MediaRepo
from .repos.message_repo import (
    DEFAULT_RETENTION_DAYS,
    MessageRepo,
    StoredMessage,
    _bounded_text,
    _now,
    _source_platform_message_id,
)
from .repos.schema import (
    backfill_platform_message_ids,
    init_memory_schema,
    migrate_sender_memories_to_multi_entry,
)

__all__ = [
    "DEFAULT_RETENTION_DAYS",
    "MAX_ENTITY_NOTE_CHARS",
    "MAX_ENTITY_TERM_CHARS",
    "MAX_FACT_CHARS",
    "MAX_NOTE_CHARS",
    "MAX_QUERY_CHARS",
    "MAX_SENDER_MEMORY_CHARS",
    "MemoryStore",
    "StoredMessage",
    "_bounded_text",
    "_gap_minutes",
    "_like_escape",
    "_now",
    "_source_platform_message_id",
]


def _gap_minutes(current_time: datetime, previous_at: str | None) -> int | None:
    if previous_at is None:
        return None
    previous_time = datetime.fromisoformat(previous_at)
    seconds = max(0, int((current_time - previous_time).total_seconds()))
    return seconds // 60


class MemoryStore:
    """Facade over MessageRepo, MediaRepo, and KnowledgeRepo sharing one SQLite connection."""

    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self._media_root = (path.parent / "received-images").resolve()
        self._audio_root = (path.parent / "received-audio").resolve()
        self._file_root = (path.parent / "received-files").resolve()
        self.path = Path(path).resolve()
        self._lock = threading.RLock()
        self.db = sqlite3.connect(path, timeout=1, check_same_thread=False)
        register_thread_safe_db(self.db, self._lock, owner=self)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        init_memory_schema(self.db, self._lock, self.transaction)

        self.knowledge = KnowledgeRepo(self.db, self._lock, self.transaction)
        self.messages = MessageRepo(
            self.db,
            self._lock,
            self.transaction,
            ensure_note=self.knowledge.ensure_note,
        )
        self.media = MediaRepo(
            self.db,
            self._lock,
            self.transaction,
            media_root=self._media_root,
            audio_root=self._audio_root,
            file_root=self._file_root,
            messages=self.messages,
            knowledge=self.knowledge,
        )

    @property
    def media_root(self) -> Path:
        return self._media_root

    @media_root.setter
    def media_root(self, value: Path) -> None:
        self._media_root = value
        if hasattr(self, "media"):
            self.media.media_root = value

    @property
    def audio_root(self) -> Path:
        return self._audio_root

    @audio_root.setter
    def audio_root(self, value: Path) -> None:
        self._audio_root = value
        if hasattr(self, "media"):
            self.media.audio_root = value

    @property
    def file_root(self) -> Path:
        return self._file_root

    @file_root.setter
    def file_root(self, value: Path) -> None:
        self._file_root = value
        if hasattr(self, "media"):
            self.media.file_root = value

    @contextmanager
    def transaction(self):
        """Nested writes share the caller's transaction instead of committing it early."""
        with self._lock:
            nested = self.db.in_transaction
            savepoint = "memory_" + uuid4().hex
            self.db.execute(f"SAVEPOINT {savepoint}" if nested else "BEGIN IMMEDIATE")
            try:
                yield
            except BaseException:
                if nested:
                    self.db.execute(f"ROLLBACK TO {savepoint}")
                    self.db.execute(f"RELEASE {savepoint}")
                else:
                    self.db.rollback()
                raise
            else:
                if nested:
                    self.db.execute(f"RELEASE {savepoint}")
                else:
                    self.db.commit()

    def _migrate_sender_memories_to_multi_entry(self) -> None:
        migrate_sender_memories_to_multi_entry(self.db, self._lock)

    def _backfill_platform_message_ids(self) -> None:
        backfill_platform_message_ids(self.db, self.transaction)

    # ------------------------------------------------------------------
    # Cross-repo orchestrators
    # ------------------------------------------------------------------

    def enrich_context_messages(
        self,
        *,
        account_id: str,
        messages: Sequence[Mapping[str, object]],
        entity_limit: int = 30,
    ) -> list[dict[str, object]]:
        """Attach one fresh, batched snapshot of notes, terms, and media to records."""
        return _enrich_context_messages_impl(
            account_id=account_id,
            messages=messages,
            lock=self._lock,
            knowledge=self.knowledge,
            messages_repo=self.messages,
            media=self.media,
            entity_limit=entity_limit,
        )

    def purge_expired(self, *, now: str | None = None) -> int:
        now = now or _now()
        with self.transaction():
            message_ids = self.messages.soft_delete_expired(now=now)
            for message_id in message_ids:
                self.media.purge_for_message(message_id=message_id, now=now)
                self.knowledge.purge_for_message(message_id=message_id, now=now)
            return len(message_ids)

    # ------------------------------------------------------------------
    # MessageRepo delegates
    # ------------------------------------------------------------------

    _message_dict = staticmethod(MessageRepo._message_dict)

    def archive_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        sender_id: str,
        raw_envelope: dict,
        body_text: str,
        source_record: dict | None = None,
        event_time_context: Mapping[str, object] | None = None,
        direction: str = "inbound",
        delivery_state: str | None = None,
        retention_days: int = DEFAULT_RETENTION_DAYS,
        source_message_id: int | None = None,
    ) -> StoredMessage:
        return self.messages.archive_message(
            account_id=account_id,
            conversation_id=conversation_id,
            sender_id=sender_id,
            raw_envelope=raw_envelope,
            body_text=body_text,
            source_record=source_record,
            event_time_context=event_time_context,
            direction=direction,
            delivery_state=delivery_state,
            retention_days=retention_days,
            source_message_id=source_message_id,
        )

    def archive_agent_message(
        self,
        *,
        account_id: str,
        conversation_id: str,
        text: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
    ) -> StoredMessage:
        return self.messages.archive_agent_message(
            account_id=account_id,
            conversation_id=conversation_id,
            text=text,
            operation_id=operation_id,
            delivery_state=delivery_state,
            note=note,
            library_prompt=library_prompt,
        )

    def get_event_time_context(
        self, *, message_id: str, account_id: str
    ) -> dict[str, object] | None:
        return self.messages.get_event_time_context(
            message_id=message_id,
            account_id=account_id,
        )

    def recent_other_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 20,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        return self.messages.recent_other_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            now=now,
        )

    def recent_messages(
        self,
        *,
        account_id: str,
        conversation_id: str,
        limit: int = 50,
        exclude_message_id: str | None = None,
        now: datetime | None = None,
    ) -> list[dict[str, object]]:
        return self.messages.recent_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            exclude_message_id=exclude_message_id,
            now=now,
        )

    def recent_unanswered_inbound(
        self,
        *,
        account_id: str,
        since: datetime,
        limit: int = 2048,
    ) -> list[dict[str, object]]:
        return self.messages.recent_unanswered_inbound(
            account_id=account_id,
            since=since,
            limit=limit,
        )

    def list_conversations(self, *, account_id: str, limit: int = 100) -> list[dict[str, object]]:
        return self.messages.list_conversations(account_id=account_id, limit=limit)

    def search_messages(
        self,
        *,
        account_id: str,
        query: str,
        conversation_id: str | None = None,
        limit: int = 20,
    ) -> list[dict[str, object]]:
        return self.messages.search_messages(
            account_id=account_id,
            query=query,
            conversation_id=conversation_id,
            limit=limit,
        )

    def messages_for_idle_event(
        self, *, account_id: str, limit: int = 100
    ) -> list[dict[str, object]]:
        return self.messages.messages_for_idle_event(account_id=account_id, limit=limit)

    def reconstruct_message(self, message_id: str, account_id: str) -> dict:
        return self.messages.reconstruct_message(message_id, account_id)

    def find_message_by_platform_id(
        self, *, account_id: str, conversation_id: str, platform_message_id: str
    ) -> dict | None:
        return self.messages.find_message_by_platform_id(
            account_id=account_id,
            conversation_id=conversation_id,
            platform_message_id=platform_message_id,
        )

    def _previous_message_times_many(
        self,
        *,
        account_id: str,
        requests: Sequence[tuple[str, str, str, str]],
    ) -> dict[str, tuple[str | None, str | None]]:
        return self.messages._previous_message_times_many(
            account_id=account_id,
            requests=requests,
        )

    def minutes_since_last_human_message(
        self, *, account_id: str, conversation_id: str
    ) -> int | None:
        return self.messages.minutes_since_last_human_message(
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def count_effective_messages(
        self, *, account_id: str, conversation_id: str, since: datetime
    ) -> int:
        return self.messages.count_effective_messages(
            account_id=account_id,
            conversation_id=conversation_id,
            since=since,
        )

    def message_belongs_to(
        self, *, message_id: str, account_id: str, conversation_id: str
    ) -> bool:
        return self.messages.message_belongs_to(
            message_id=message_id,
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def message_conversation(
        self, *, message_id: str, account_id: str
    ) -> str | None:
        return self.messages.message_conversation(
            message_id=message_id,
            account_id=account_id,
        )

    def idle_curation_snapshot(
        self, *, account_id: str, since_at: str | None
    ) -> dict[str, object]:
        snapshot = self.messages.idle_curation_snapshot(
            account_id=account_id,
            since_at=since_at,
        )
        snapshot["consolidation_candidates"] = self.knowledge.memory_consolidation_candidates(
            account_id=account_id,
        )
        return snapshot

    def message_exists_for_sender(self, account_id: str, sender_id: str) -> bool:
        return self.messages.message_exists_for_sender(account_id, sender_id)

    def save_jev_labels(
        self, *, account_id: str, labels: Sequence[Mapping[str, object]]
    ) -> int:
        return self.messages.save_jev_labels(account_id=account_id, labels=labels)

    def _message_in_scope(self, message_id: str, account_id: str) -> bool:
        return self.messages._message_in_scope(message_id, account_id)

    def _conversation_for_message(self, message_id: str, account_id: str) -> str:
        return self.messages._conversation_for_message(message_id, account_id)

    # ------------------------------------------------------------------
    # MediaRepo delegates
    # ------------------------------------------------------------------

    def archive_agent_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        artifact_path: Path,
        mime_type: str,
        operation_id: str,
        delivery_state: str,
        note: str | None = None,
        library_prompt: str | None = None,
        quick_send: bool = True,
    ) -> dict[str, str]:
        return self.media.archive_agent_image(
            account_id=account_id,
            conversation_id=conversation_id,
            artifact_path=artifact_path,
            mime_type=mime_type,
            operation_id=operation_id,
            delivery_state=delivery_state,
            note=note,
            library_prompt=library_prompt,
            quick_send=quick_send,
        )

    def import_library_image(
        self,
        *,
        account_id: str,
        source_path: Path,
        description: str = "",
        display_order: int = 0,
        quick_send: bool = True,
    ) -> dict[str, object]:
        return self.media.import_library_image(
            account_id=account_id,
            source_path=source_path,
            description=description,
            display_order=display_order,
            quick_send=quick_send,
        )

    def begin_image_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        original_references: Sequence[dict[str, str]],
    ) -> dict[str, object]:
        return self.media.begin_image_ingest(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            original_references=original_references,
        )

    def complete_image_ingest(
        self,
        *,
        image_id: str,
        account_id: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
        source_kind: str = "original",
    ) -> None:
        self.media.complete_image_ingest(
            image_id=image_id,
            account_id=account_id,
            artifact_path=artifact_path,
            mime_type=mime_type,
            byte_count=byte_count,
            sha256=sha256,
            source_sha256=source_sha256,
            source_kind=source_kind,
        )

    def promote_inferred_image(
        self,
        *,
        image_id: str,
        account_id: str,
        previous_sha256: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
        source_sha256: str,
    ) -> bool:
        return self.media.promote_inferred_image(
            image_id=image_id,
            account_id=account_id,
            previous_sha256=previous_sha256,
            artifact_path=artifact_path,
            mime_type=mime_type,
            byte_count=byte_count,
            sha256=sha256,
            source_sha256=source_sha256,
        )

    def fail_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> None:
        self.media.fail_image_ingest(
            image_id=image_id,
            account_id=account_id,
            error_code=error_code,
        )

    def retry_image_ingest(self, *, image_id: str, account_id: str, error_code: str) -> bool:
        return self.media.retry_image_ingest(
            image_id=image_id,
            account_id=account_id,
            error_code=error_code,
        )

    def image_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.image_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        )

    def save_image_description(
        self, *, account_id: str, image_id: str, description: str, model_id: str
    ) -> None:
        self.media.save_image_description(
            account_id=account_id,
            image_id=image_id,
            description=description,
            model_id=model_id,
        )

    def get_image_by_id(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        image_id: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_image_by_id(
            account_id=account_id,
            conversation_id=conversation_id,
            image_id=image_id,
            include_unready=include_unready,
        )

    def list_images(
        self,
        *,
        account_id: str,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        return self.media.list_images(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            offset=offset,
        )

    def quick_send_images(self, *, account_id: str, limit: int = 10) -> list[dict[str, object]]:
        return self.media.quick_send_images(account_id=account_id, limit=limit)

    def manage_image_library(
        self,
        *,
        account_id: str,
        image_id: str,
        actor: str = "agent",
        description: str | None = None,
        quick_send: bool | None = None,
        display_order: int | None = None,
    ) -> dict[str, object]:
        return self.media.manage_image_library(
            account_id=account_id,
            image_id=image_id,
            actor=actor,
            description=description,
            quick_send=quick_send,
            display_order=display_order,
        )

    def get_image_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.get_image_for_message(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def get_latest_ready_image(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_latest_ready_image(
            account_id=account_id,
            conversation_id=conversation_id,
            before_message_id=before_message_id,
            include_unready=include_unready,
        )

    def get_image_by_reference(
        self,
        *,
        account_id: str,
        conversation_id: str,
        reference: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_image_by_reference(
            account_id=account_id,
            conversation_id=conversation_id,
            reference=reference,
            include_unready=include_unready,
        )

    def begin_audio_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        duration_ms: int | None,
    ) -> dict[str, object]:
        return self.media.begin_audio_ingest(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            duration_ms=duration_ms,
        )

    def fail_audio_ingest_for_message(
        self, *, account_id: str, message_id: str, error_code: str
    ) -> dict[str, object]:
        return self.media.fail_audio_ingest_for_message(
            account_id=account_id,
            message_id=message_id,
            error_code=error_code,
        )

    def complete_audio_ingest(
        self,
        *,
        audio_id: str,
        account_id: str,
        artifact_path: str,
        byte_count: int,
        sha256: str,
        duration_ms: int,
    ) -> None:
        self.media.complete_audio_ingest(
            audio_id=audio_id,
            account_id=account_id,
            artifact_path=artifact_path,
            byte_count=byte_count,
            sha256=sha256,
            duration_ms=duration_ms,
        )

    def audio_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.audio_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        )

    def get_audio_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.get_audio_for_message(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def get_latest_ready_audio(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
    ) -> dict[str, object] | None:
        return self.media.get_latest_ready_audio(
            account_id=account_id,
            conversation_id=conversation_id,
            before_message_id=before_message_id,
        )

    def claim_audio_transcription(self, *, audio_id: str, account_id: str) -> str:
        return self.media.claim_audio_transcription(audio_id=audio_id, account_id=account_id)

    def save_audio_transcript(
        self, *, audio_id: str, account_id: str, transcript: str, model_id: str
    ) -> None:
        self.media.save_audio_transcript(
            audio_id=audio_id,
            account_id=account_id,
            transcript=transcript,
            model_id=model_id,
        )

    def finish_audio_transcription(self, *, audio_id: str, account_id: str, state: str) -> None:
        self.media.finish_audio_transcription(
            audio_id=audio_id,
            account_id=account_id,
            state=state,
        )

    # ------------------------------------------------------------------
    # KnowledgeRepo delegates
    # ------------------------------------------------------------------

    _tag_width = staticmethod(KnowledgeRepo._tag_width)

    def sender_tags_at(self, *, account_id: str, sender_id: str, at: str) -> list[str]:
        return self.knowledge.sender_tags_at(account_id=account_id, sender_id=sender_id, at=at)

    def sender_tags_many_at(
        self, *, account_id: str, requests: Sequence[tuple[str, str, str]]
    ) -> dict[str, list[str]]:
        return self.knowledge.sender_tags_many_at(account_id=account_id, requests=requests)

    def update_sender_tags(
        self,
        *,
        account_id: str,
        sender_id: str,
        action: str,
        tags: Sequence[str],
        source_message_id: str | None = None,
        source_task_id: str | None = None,
    ) -> dict[str, object]:
        return self.knowledge.update_sender_tags(
            account_id=account_id,
            sender_id=sender_id,
            action=action,
            tags=tags,
            source_message_id=source_message_id,
            source_task_id=source_task_id,
        )

    def note_belongs_to(self, *, note_id: str, account_id: str, conversation_id: str) -> bool:
        return self.knowledge.note_belongs_to(
            note_id=note_id,
            account_id=account_id,
            conversation_id=conversation_id,
        )

    def note_conversation(self, *, note_id: str, account_id: str) -> str | None:
        return self.knowledge.note_conversation(note_id=note_id, account_id=account_id)

    def add_note(
        self,
        *,
        account_id: str,
        message_id: str,
        content: str,
        author_id: str = "agent",
        source: str = "agent_observation",
    ) -> str:
        return self.knowledge.add_note(
            account_id=account_id,
            message_id=message_id,
            content=content,
            author_id=author_id,
            source=source,
        )

    def revise_note(
        self, *, account_id: str, note_id: str, content: str | None, actor_id: str = "agent"
    ) -> int:
        return self.knowledge.revise_note(
            account_id=account_id,
            note_id=note_id,
            content=content,
            actor_id=actor_id,
        )

    def notes_for_message(self, *, account_id: str, message_id: str) -> list[dict[str, str]]:
        return self.knowledge.notes_for_message(account_id=account_id, message_id=message_id)

    def search_notes(
        self, *, account_id: str, conversation_id: str, query: str, limit: int = 8
    ) -> list[dict[str, str]]:
        return self.knowledge.search_notes(
            account_id=account_id,
            conversation_id=conversation_id,
            query=query,
            limit=limit,
        )

    def remember_fact(
        self,
        *,
        account_id: str,
        fact_key: str,
        content: str,
        source_message_id: str,
        actor_id: str = "agent",
    ) -> str:
        return self.knowledge.remember_fact(
            account_id=account_id,
            fact_key=fact_key,
            content=content,
            source_message_id=source_message_id,
            actor_id=actor_id,
        )

    def search_facts(self, *, account_id: str, query: str, limit: int = 8) -> list[dict]:
        return self.knowledge.search_facts(account_id=account_id, query=query, limit=limit)

    def fact_key_index(self, *, account_id: str, limit: int = 40) -> list[str]:
        return self.knowledge.fact_key_index(account_id=account_id, limit=limit)

    def memory_consolidation_candidates(
        self,
        *,
        account_id: str,
        sender_threshold: int = 8,
        fact_threshold: int = 20,
        limit: int = 5,
    ) -> dict[str, object]:
        return self.knowledge.memory_consolidation_candidates(
            account_id=account_id,
            sender_threshold=sender_threshold,
            fact_threshold=fact_threshold,
            limit=limit,
        )

    def delete_fact(self, *, account_id: str, fact_id: str, actor_id: str = "agent") -> None:
        self.knowledge.delete_fact(account_id=account_id, fact_id=fact_id, actor_id=actor_id)

    def upsert_entity(
        self, *, account_id: str, term: str, explanation: str, actor_id: str = "agent"
    ) -> str:
        return self.knowledge.upsert_entity(
            account_id=account_id,
            term=term,
            explanation=explanation,
            actor_id=actor_id,
        )

    def upsert_sender_memory(
        self, *, account_id: str, sender_id: str, topic: str, content: str
    ) -> tuple[str, bool]:
        return self.knowledge.upsert_sender_memory(
            account_id=account_id,
            sender_id=sender_id,
            topic=topic,
            content=content,
        )

    def sender_memories(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, list[dict[str, object]]]:
        return self.knowledge.sender_memories(account_id=account_id, sender_ids=sender_ids)

    def sender_remarks(
        self, *, account_id: str, sender_ids: Sequence[str]
    ) -> dict[str, str]:
        return self.knowledge.sender_remarks(account_id=account_id, sender_ids=sender_ids)

    def set_human_sender_remark(
        self,
        *,
        account_id: str,
        sender_id: str,
        remark: str,
        actor: str = "dashboard-user",
    ) -> dict[str, object]:
        return self.knowledge.set_human_sender_remark(
            account_id=account_id,
            sender_id=sender_id,
            remark=remark,
            actor=actor,
        )

    def priority_sender_profiles(
        self,
        *,
        account_id: str,
        tag: str = "唯一主人",
        current_conversation_id: str | None = None,
        exclude_message_ids: Sequence[str] = (),
    ) -> list[dict[str, object]]:
        return self.knowledge.priority_sender_profiles(
            account_id=account_id,
            tag=tag,
            current_conversation_id=current_conversation_id,
            exclude_message_ids=exclude_message_ids,
        )

    def delete_sender_memory(self, *, account_id: str, memory_id: str) -> bool:
        return self.knowledge.delete_sender_memory(account_id=account_id, memory_id=memory_id)

    def delete_entity(self, *, account_id: str, entity_id: str, actor_id: str = "agent") -> None:
        self.knowledge.delete_entity(account_id=account_id, entity_id=entity_id, actor_id=actor_id)

    def match_entities(self, *, account_id: str, text: str, limit: int = 30) -> list[dict]:
        return self.knowledge.match_entities(account_id=account_id, text=text, limit=limit)

    def match_entities_many(
        self, *, account_id: str, texts: Sequence[str], limit: int = 30
    ) -> list[list[dict]]:
        return self.knowledge.match_entities_many(account_id=account_id, texts=texts, limit=limit)

    def _note_revision(
        self,
        note_id: str,
        version: int,
        action: str,
        actor: str,
        content: str,
        source: str,
        now: str,
    ) -> None:
        self.knowledge._note_revision(note_id, version, action, actor, content, source, now)

    def _audit(
        self,
        scope: str,
        actor_kind: str,
        actor: str,
        action: str,
        object_type: str,
        object_id: str,
        version: int,
        now: str,
    ) -> None:
        self.knowledge._audit(scope, actor_kind, actor, action, object_type, object_id, version, now)

    def begin_file_ingest(
        self,
        *,
        account_id: str,
        conversation_id: str,
        message_id: str,
        original_references: Sequence[dict[str, str]],
        filename: str,
        fileext: str = "",
        expected_bytes: int | None = None,
        expected_md5: str | None = None,
    ) -> dict[str, object]:
        return self.media.begin_file_ingest(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
            original_references=original_references,
            filename=filename,
            fileext=fileext,
            expected_bytes=expected_bytes,
            expected_md5=expected_md5,
        )

    def complete_file_ingest(
        self,
        *,
        file_id: str,
        account_id: str,
        artifact_path: str,
        mime_type: str,
        byte_count: int,
        sha256: str,
    ) -> None:
        self.media.complete_file_ingest(
            file_id=file_id,
            account_id=account_id,
            artifact_path=artifact_path,
            mime_type=mime_type,
            byte_count=byte_count,
            sha256=sha256,
        )

    def fail_file_ingest_for_message(
        self,
        *,
        account_id: str,
        message_id: str,
        error_code: str,
        filename: str = "unnamed",
        fileext: str = "",
        expected_bytes: int | None = None,
        expected_md5: str | None = None,
    ) -> dict[str, object]:
        return self.media.fail_file_ingest_for_message(
            account_id=account_id,
            message_id=message_id,
            error_code=error_code,
            filename=filename,
            fileext=fileext,
            expected_bytes=expected_bytes,
            expected_md5=expected_md5,
        )

    def archive_agent_file(
        self,
        *,
        account_id: str,
        conversation_id: str,
        artifact_path: Path,
        filename: str,
        mime_type: str,
        operation_id: str,
        delivery_state: str,
    ) -> dict[str, str]:
        return self.media.archive_agent_file(
            account_id=account_id,
            conversation_id=conversation_id,
            artifact_path=artifact_path,
            filename=filename,
            mime_type=mime_type,
            operation_id=operation_id,
            delivery_state=delivery_state,
        )

    def file_metadata_for_message(
        self, *, account_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.file_metadata_for_message(
            account_id=account_id,
            message_id=message_id,
        )

    def get_file_for_message(
        self, *, account_id: str, conversation_id: str, message_id: str
    ) -> dict[str, object] | None:
        return self.media.get_file_for_message(
            account_id=account_id,
            conversation_id=conversation_id,
            message_id=message_id,
        )

    def get_file_by_id(
        self,
        *,
        account_id: str,
        conversation_id: str | None,
        file_id: str,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_file_by_id(
            account_id=account_id,
            conversation_id=conversation_id,
            file_id=file_id,
            include_unready=include_unready,
        )

    def get_latest_ready_file(
        self,
        *,
        account_id: str,
        conversation_id: str,
        before_message_id: str | None = None,
        include_unready: bool = False,
    ) -> dict[str, object] | None:
        return self.media.get_latest_ready_file(
            account_id=account_id,
            conversation_id=conversation_id,
            before_message_id=before_message_id,
            include_unready=include_unready,
        )

    def list_files(
        self,
        *,
        account_id: str,
        conversation_id: str | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict[str, object]]:
        return self.media.list_files(
            account_id=account_id,
            conversation_id=conversation_id,
            limit=limit,
            offset=offset,
        )

    def save_file_summary(
        self, *, account_id: str, file_id: str, summary: str, model_id: str
    ) -> None:
        self.media.save_file_summary(
            account_id=account_id,
            file_id=file_id,
            summary=summary,
            model_id=model_id,
        )

    def close(self) -> None:
        self.db.close()
