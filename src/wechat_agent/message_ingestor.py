"""Pure inbound write path: archive messages and run fast local media ingestion."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass

from .media import WeChatImageIngestor, WeChatVoiceIngestor
from .memory_store import MemoryStore
from .rich_message import normalized_image_reference_records, quoted_message_reference
from .temporal_context import message_time_context
from .wechat_gateway import MessageEvent, default_archive_envelope


@dataclass(frozen=True, repr=False)
class IngestedBatch:
    """Immutable result of ingesting a same-conversation batch of inbound events."""

    observations: tuple[MessageEvent, ...]
    records: tuple[Mapping[str, object], ...]
    message_ids: tuple[str, ...]
    message_times: tuple[Mapping[str, object], ...]

    def __repr__(self) -> str:
        return f"IngestedBatch(messages={len(self.observations)})"


def entity_search_text(item: MessageEvent) -> str:
    """Extract clean plain text used for entity matching and quote context."""
    if item.original_message_type in (3, 47):
        return "图片/表情消息"
    if item.original_message_type == 34:
        return "语音消息"
    if item.is_group and item.text.startswith(item.sender_id + ":\n"):
        return item.text[len(item.sender_id) + 2 :]
    return item.text


def redact_voice_payload(value: object) -> None:
    """Remove raw binary voice_hex fields from archived payloads in place."""
    if isinstance(value, dict):
        for key in tuple(value):
            if key == "voice_hex":
                value.pop(key, None)
            else:
                redact_voice_payload(value[key])
    elif isinstance(value, list):
        for entry in value:
            redact_voice_payload(entry)


def _raw_message_dict(item: MessageEvent) -> dict[str, object] | None:
    if not item.raw_message_json:
        return None
    try:
        value = json.loads(item.raw_message_json)
    except (TypeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _call_fast_image_ingest(
    ingestor: WeChatImageIngestor,
    *,
    item: MessageEvent,
    message_id: str,
    references: list[dict[str, str]],
    store: MemoryStore,
) -> dict[str, object]:
    """Invoke image_ingestor.ingest with allow_account_scan=False (compat with 4-arg stubs)."""
    ingest_fn = ingestor.ingest
    try:
        params = inspect.signature(ingest_fn).parameters
        accepts_scan_flag = "allow_account_scan" in params or any(
            param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()
        )
    except (TypeError, ValueError):
        accepts_scan_flag = True
    if accepts_scan_flag:
        return ingest_fn(
            item=item,
            message_id=message_id,
            references=references,
            store=store,
            allow_account_scan=False,
        )
    return ingest_fn(
        item=item,
        message_id=message_id,
        references=references,
        store=store,
    )


class MessageIngestor:
    """Inbound write path: redacts voice hex, archives messages, and ingests media fast."""

    def __init__(
        self,
        memory_store: MemoryStore | None = None,
        *,
        image_ingestor: WeChatImageIngestor | None = None,
        voice_ingestor: WeChatVoiceIngestor | None = None,
        archive_envelope: Callable[[MessageEvent], dict] = default_archive_envelope,
    ) -> None:
        self.memory_store = memory_store
        self.image_ingestor = image_ingestor
        self.voice_ingestor = voice_ingestor
        self.archive_envelope = archive_envelope

    async def ingest(self, observations: Sequence[MessageEvent]) -> IngestedBatch:
        """Archive inbound observations and ingest their media without cross-chat scanning."""
        items = tuple(observations)
        if not 1 <= len(items) <= 20:
            raise ValueError("batch must contain 1 to 20 observations")
        first = items[0]
        if any(
            (item.account_id, item.conversation_id, item.is_group)
            != (first.account_id, first.conversation_id, first.is_group)
            for item in items
        ):
            raise ValueError("batch must belong to one conversation")

        message_times: list[Mapping[str, object]] = []
        if self.memory_store is None:
            for item in items:
                message_times.append(
                    item.event_time_context or message_time_context(item.capture_tick_ms)
                )
            return IngestedBatch(
                observations=items,
                records=(),
                message_ids=(),
                message_times=tuple(message_times),
            )

        records: list[dict[str, object]] = []
        message_ids: list[str] = []
        for item in items:
            event_time = item.event_time_context or message_time_context(item.capture_tick_ms)
            message_times.append(event_time)

            raw_message = _raw_message_dict(item)
            envelope = self.archive_envelope(item)
            source_record = dict(raw_message) if raw_message is not None else None
            if item.original_message_type == 34:
                redact_voice_payload(envelope)
                if source_record is not None:
                    redact_voice_payload(source_record)

            archived = self.memory_store.archive_message(
                account_id=item.account_id,
                conversation_id=item.conversation_id,
                sender_id=item.sender_id,
                raw_envelope=envelope,
                body_text=item.text,
                source_record=source_record,
                source_message_id=item.source_msg_id,
                event_time_context=event_time,
            )
            message_ids.append(archived.message_id)

            image_metadata: dict[str, object] | None = None
            if item.original_message_type in (3, 47):
                refs = list(normalized_image_reference_records(item.text))
                if self.image_ingestor is None:
                    image_metadata = {
                        "state": "unavailable",
                        "error_code": "image_ingestor_not_configured",
                    }
                else:
                    image_metadata = await asyncio.to_thread(
                        _call_fast_image_ingest,
                        self.image_ingestor,
                        item=item,
                        message_id=archived.message_id,
                        references=refs,
                        store=self.memory_store,
                    )

            audio_metadata: dict[str, object] | None = None
            if item.original_message_type == 34:
                if self.voice_ingestor is None:
                    audio_metadata = self.memory_store.fail_audio_ingest_for_message(
                        account_id=item.account_id,
                        message_id=archived.message_id,
                        error_code="voice_ingestor_not_configured",
                    )
                else:
                    audio_metadata = await asyncio.to_thread(
                        self.voice_ingestor.ingest,
                        item=item,
                        message_id=archived.message_id,
                        store=self.memory_store,
                    )

            records.append(
                {
                    "message_id": archived.message_id,
                    "captured_at": archived.captured_at,
                    "sender_id": item.sender_id,
                    "conversation_id": item.conversation_id,
                    "body_text": item.text,
                    "entity_search_text": entity_search_text(item),
                    "image": image_metadata,
                    "audio": audio_metadata,
                }
            )

        return IngestedBatch(
            observations=items,
            records=tuple(records),
            message_ids=tuple(message_ids),
            message_times=tuple(message_times),
        )

    async def upgrade_recent_media(
        self,
        observations: Sequence[MessageEvent],
        *,
        exclude_message_ids: Sequence[str] = (),
        max_upgrades: int = 2,
    ) -> int:
        """Explicit opt-in upgrade for recent historical thumbnails/failed images.

        Never called on the default inbound path; callers opt in explicitly when
        pre-display thumbnail recovery is desired.
        """
        if self.memory_store is None or self.image_ingestor is None or not observations:
            return 0
        first = observations[0]
        excluded = set(exclude_message_ids)
        quote_targets: list[dict[str, object]] = []
        for item in observations:
            quote_ref = quoted_message_reference(item.text)
            if not quote_ref:
                continue
            target = self.memory_store.find_message_by_platform_id(
                account_id=item.account_id,
                conversation_id=quote_ref["source_conversation_id"],
                platform_message_id=quote_ref["platform_message_id"],
            )
            if target is not None and not any(
                row.get("message_id") == target.get("message_id") for row in quote_targets
            ):
                quote_targets.append(target)
        quote_ids = {str(row.get("message_id")) for row in quote_targets}
        historical = [
            row
            for row in self.memory_store.recent_messages(
                account_id=first.account_id,
                conversation_id=first.conversation_id,
                limit=50,
            )
            if row.get("message_id") not in excluded and row.get("message_id") not in quote_ids
        ]
        upgraded = 0
        for candidate in (*quote_targets, *historical[:5]):
            if upgraded >= max_upgrades:
                break
            candidate_id = candidate.get("message_id")
            if not isinstance(candidate_id, str) or not candidate_id:
                continue
            conv_id = str(candidate.get("conversation_id") or first.conversation_id)
            found = self.memory_store.get_image_for_message(
                account_id=first.account_id,
                conversation_id=conv_id,
                message_id=candidate_id,
            )
            if found is not None and (
                found.get("state") == "failed"
                or found.get("source_kind") == "thumbnail_inferred"
            ):
                full_media = self.memory_store.get_image_by_id(
                    account_id=first.account_id,
                    conversation_id=conv_id,
                    image_id=str(found["image_id"]),
                    include_unready=True,
                )
                if full_media is not None:
                    upgraded += 1
                    await asyncio.to_thread(
                        self.image_ingestor.prepare_for_read,
                        full_media,
                        store=self.memory_store,
                    )
        return upgraded
