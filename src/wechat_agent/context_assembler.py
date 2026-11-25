"""Compatibility facade composing MessageIngestor (write) and ContextBuilder (read-only)."""

from __future__ import annotations

from dataclasses import dataclass

from .context import ContextWindow
from .context_builder import ContextBuilder, ContextSpec
from .media import WeChatImageIngestor, WeChatVoiceIngestor
from .memory_store import MemoryStore
from .message_ingestor import (
    MessageIngestor,
    _raw_message_dict,
    entity_search_text,
    redact_voice_payload,
)
from .temporal_context import DailyEnvironmentProvider
from .wechat_gateway import MessageEvent, default_archive_envelope


@dataclass(frozen=True, repr=False)
class AssembledBatch:
    context: ContextWindow
    memory_message_ids: tuple[str, ...]
    quote_context: tuple[str, str, str, str] | None

    def __repr__(self) -> str:
        return f"AssembledBatch(messages={len(self.memory_message_ids)})"


class MessageContextAssembler:
    """Compatibility facade: ingests inbound observations then builds a read-only context window."""

    def __init__(
        self,
        memory_store: MemoryStore,
        *,
        resolve_contact=None,
        image_ingestor: WeChatImageIngestor | None = None,
        voice_ingestor: WeChatVoiceIngestor | None = None,
        daily_environment: DailyEnvironmentProvider | None = None,
        max_user_bytes: int = 32_000,
        archive_envelope=default_archive_envelope,
        upgrade_recent_media: bool = False,
    ) -> None:
        if max_user_bytes < 512:
            raise ValueError("max_user_bytes must be at least 512")
        self.memory_store = memory_store
        self.resolve_contact = resolve_contact
        self.image_ingestor = image_ingestor
        self.voice_ingestor = voice_ingestor
        self.daily_environment = daily_environment
        self.max_user_bytes = max_user_bytes
        self.archive_envelope = archive_envelope
        self.upgrade_recent_media = bool(upgrade_recent_media)
        self.ingestor = MessageIngestor(
            memory_store,
            image_ingestor=image_ingestor,
            voice_ingestor=voice_ingestor,
            archive_envelope=archive_envelope,
        )
        self.builder = ContextBuilder(
            memory_store,
            resolve_contact=resolve_contact,
            daily_environment=daily_environment,
        )

    async def assemble(self, observations: tuple[MessageEvent, ...]) -> AssembledBatch:
        ingested = await self.ingestor.ingest(observations)
        if self.upgrade_recent_media:
            await self.ingestor.upgrade_recent_media(
                ingested.observations,
                exclude_message_ids=ingested.message_ids,
            )
        built = await self.builder.build(
            ContextSpec.for_chat(
                ingested=ingested,
                max_user_bytes=self.max_user_bytes,
            )
        )
        return AssembledBatch(
            context=built.context,
            memory_message_ids=ingested.message_ids,
            quote_context=built.quote_context,
        )

    @staticmethod
    def _raw_message(item: MessageEvent) -> dict[str, object] | None:
        return _raw_message_dict(item)

    @staticmethod
    def _entity_search_text(item: MessageEvent) -> str:
        return entity_search_text(item)

    @staticmethod
    def _redact_voice(value: object) -> None:
        redact_voice_payload(value)
