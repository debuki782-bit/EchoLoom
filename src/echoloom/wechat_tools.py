"""Facade composing ReplyToolset, MediaToolset, MemoryToolset, and OutboundService."""

import asyncio  # noqa: F401 - compatibility target for tests patching wechat_tools.asyncio.sleep
from collections.abc import Callable
from pathlib import Path

from .outbound import OutboundService, _segment_pause_seconds
from .runtime_contract import AgentTool as ToolUnion
from .runtime_contract import ToolContext
from .toolsets import MediaToolset, MemoryToolset, ReplyToolset
from .turn_scope import (
    active_conversation,
    followup_scheduler,
    memory_context,
    operation_id,
    operation_receipt_ids,
    quote_context,
    tool_memory_context,
    tool_turn_scope,
)
from .usage import (
    DAILY_ACCOUNT_BUDGET_MICROUSD,
    IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
    IMAGE_READ_DAILY_BUDGET_MICROUSD,
    TEXT_MODEL_DAILY_BUDGET_MICROUSD,
)
from .wechat_gateway import GatewayConfig, create_gateway

__all__ = [
    "ChannelToolkit",
    "MediaToolset",
    "MemoryToolset",
    "ReplyToolset",
    "WeChatTools",
    "_segment_pause_seconds",
    "active_conversation",
    "followup_scheduler",
    "memory_context",
    "operation_id",
    "operation_receipt_ids",
    "quote_context",
    "tool_memory_context",
    "tool_turn_scope",
]


class ChannelToolkit:
    """Channel-agnostic facade aggregating the 3 domain toolsets and OutboundService."""

    IMAGE_MODEL = MediaToolset.IMAGE_MODEL
    IMAGE_DESCRIPTION_MODEL = MediaToolset.IMAGE_DESCRIPTION_MODEL
    TRANSCRIBE_MODEL = MediaToolset.TRANSCRIBE_MODEL
    IMAGE_BUDGET_MICROUSD = MediaToolset.IMAGE_BUDGET_MICROUSD
    IMAGE_RESERVATION_MICROUSD = MediaToolset.IMAGE_RESERVATION_MICROUSD
    IMAGE_LIBRARY_MAX_BYTES = MediaToolset.IMAGE_LIBRARY_MAX_BYTES

    TOOL_NAMES: tuple[str, ...] = (
        *MediaToolset.TOOL_NAMES,
        *ReplyToolset.TOOL_NAMES,
        *MemoryToolset.TOOL_NAMES,
    )

    def __init__(
        self,
        root,
        account,
        room,
        sender,
        client,
        usage,
        clock,
        budget=DAILY_ACCOUNT_BUDGET_MICROUSD,
        account_text_budget=TEXT_MODEL_DAILY_BUDGET_MICROUSD,
        account_image_read_budget=IMAGE_READ_DAILY_BUDGET_MICROUSD,
        account_image_generation_budget=IMAGE_GENERATION_DAILY_BUDGET_MICROUSD,
        transport=None,
        memory_store=None,
        event_book=None,
        openai_client=None,
        gateway=None,
        outbound_service: OutboundService | None = None,
    ):
        self.root = Path(root)
        self.account = account
        self._default_room = room
        self.sender = sender
        self._clock = clock
        self.budget = budget
        self.account_text_budget = account_text_budget
        self.account_image_read_budget = account_image_read_budget
        self.account_image_generation_budget = account_image_generation_budget
        self.transport = transport
        if gateway is not None:
            self.gateway = gateway
        elif outbound_service is not None and outbound_service.gateway is not None:
            self.gateway = outbound_service.gateway
        else:
            self.gateway = create_gateway(GatewayConfig(
                self.root, account_id=account, transport=transport, text_sender=sender,
            ))
        self._memory_store = memory_store
        self._event_book = event_book
        self.outbound = (
            outbound_service
            if outbound_service is not None
            else OutboundService(
                self.root / "data" / "tool-receipts.sqlite3",
                account=account,
                default_room=room,
                gateway=self.gateway,
                clock=clock,
                memory_store=memory_store,
            )
        )
        if (
            outbound_service is not None
            and memory_store is not None
            and self.outbound.memory_store is None
        ):
            self.outbound.memory_store = memory_store

        self.memory_tools = MemoryToolset(
            account=account,
            default_room=room,
            clock=clock,
            memory_store=memory_store,
            event_book=event_book,
        )
        self.media_tools = MediaToolset(
            root=self.root,
            account=account,
            default_room=room,
            outbound=self.outbound,
            gateway=self.gateway,
            client=client,
            usage=usage,
            openai_client=openai_client,
            memory_store=memory_store,
            event_book=event_book,
            budget=budget,
            account_text_budget=account_text_budget,
            account_image_read_budget=account_image_read_budget,
            account_image_generation_budget=account_image_generation_budget,
        )
        self.reply_tools = ReplyToolset(
            outbound=self.outbound,
            memory_tools=self.memory_tools,
            resolve_image=self.media_tools._image,
        )

    @property
    def db(self):
        return self.outbound.db

    @property
    def room(self) -> str:
        return active_conversation.get() or self._default_room

    @property
    def can_send(self) -> Callable[[], bool]:
        return self.outbound.can_send

    @can_send.setter
    def can_send(self, value: Callable[[], bool]) -> None:
        self.outbound.can_send = value

    @property
    def memory_store(self):
        return self._memory_store

    @memory_store.setter
    def memory_store(self, value) -> None:
        self._memory_store = value
        self.outbound.memory_store = value
        self.memory_tools.memory_store = value
        self.media_tools.memory_store = value

    @property
    def event_book(self):
        return self._event_book

    @event_book.setter
    def event_book(self, value) -> None:
        self._event_book = value
        self.memory_tools.event_book = value
        self.media_tools.event_book = value

    @property
    def image_ingestor(self):
        return self.media_tools.image_ingestor

    @image_ingestor.setter
    def image_ingestor(self, value) -> None:
        self.media_tools.image_ingestor = value

    @property
    def voice_ingestor(self):
        return self.media_tools.voice_ingestor

    @voice_ingestor.setter
    def voice_ingestor(self, value) -> None:
        self.media_tools.voice_ingestor = value

    @property
    def openai_client(self):
        return self.media_tools.openai_client

    @openai_client.setter
    def openai_client(self, value) -> None:
        self.media_tools.openai_client = value

    @property
    def client(self):
        return self.media_tools.client

    @client.setter
    def client(self, value) -> None:
        self.media_tools.client = value

    @property
    def usage(self):
        return self.media_tools.usage

    @usage.setter
    def usage(self, value) -> None:
        self.media_tools.usage = value

    @property
    def clock(self):
        return self._clock

    @clock.setter
    def clock(self, value) -> None:
        self._clock = value
        self.outbound.clock = value
        self.memory_tools.clock = value

    @property
    def images(self) -> dict[str, Path]:
        return self.media_tools.images

    @images.setter
    def images(self, value: dict[str, Path]) -> None:
        self.media_tools.images = value

    # --- OutboundService delegates ---

    def delivery_state(self, scope: str) -> dict[str, object]:
        return self.outbound.delivery_state(scope)

    def record_send_result(
        self, scope: str, status: str, reason: str | None = None,
    ) -> int:
        return self.outbound.record_send_result(scope, status, reason)

    def queue_delivery_failure_notice(
        self, scope: str, status: str, reason: str | None, failure_count: int,
    ) -> None:
        self.outbound.queue_delivery_failure_notice(scope, status, reason, failure_count)

    def pending_delivery_failure_notices(self) -> list[dict[str, object]]:
        return self.outbound.pending_delivery_failure_notices()

    def acknowledge_delivery_failure_notices(self, scopes: tuple[str, ...]) -> None:
        self.outbound.acknowledge_delivery_failure_notices(scopes)

    # --- Unregistered compatibility delegates (not registered in agent_tools()) ---

    async def send_wechat_text(
        self, text: str, note: str | None = None, *, _record_delivery: bool = True,
    ) -> dict:
        return await self.outbound.send_text(
            text, note=note, _record_delivery=_record_delivery,
        )

    async def reply_with_wechat_quote(
        self, reply_text: str, note: str | None = None, *, _record_delivery: bool = True,
    ) -> dict:
        return await self.outbound.send_quote(
            reply_text, note=note, _record_delivery=_record_delivery,
        )

    async def inspect_wechat_image(self, image_reference: str, question: str) -> dict:
        """Deprecated unregistered compatibility delegate; use read_wechat_image()."""
        return await self.media_tools.inspect_wechat_image(image_reference, question)

    async def inspect_latest_wechat_image(self, question: str) -> dict:
        """Deprecated unregistered compatibility delegate; use read_wechat_image()."""
        return await self.media_tools.inspect_latest_wechat_image(question)

    # --- MediaToolset delegates ---

    def list_wechat_images(
        self, include_all_conversations: bool = False, limit: int = 10, offset: int = 0,
    ) -> dict:
        return self.media_tools.list_wechat_images(
            include_all_conversations=include_all_conversations,
            limit=limit,
            offset=offset,
        )

    def manage_wechat_image_library(
        self,
        image_id: str,
        description: str | None = None,
        quick_send: bool | None = None,
        display_order: int | None = None,
    ) -> dict:
        return self.media_tools.manage_wechat_image_library(
            image_id=image_id,
            description=description,
            quick_send=quick_send,
            display_order=display_order,
        )

    def _image(self, image_id: str) -> Path:
        return self.media_tools._image(image_id)

    async def _image_for_read(self, image_reference: str) -> dict | None:
        return await self.media_tools._image_for_read(image_reference)

    async def describe_wechat_image(
        self, image_reference: str, question: str = "描述图片中值得以后记住的内容。",
    ) -> dict[str, object]:
        return await self.media_tools.describe_wechat_image(image_reference, question)

    async def describe_latest_wechat_image(
        self, question: str = "描述图片中值得以后记住的内容。",
    ) -> dict[str, object]:
        return await self.media_tools.describe_latest_wechat_image(question)

    async def read_wechat_image(
        self,
        image_reference: str = "latest",
        question: str = "描述图片中值得以后记住的内容。",
    ) -> dict[str, object]:
        return await self.media_tools.read_wechat_image(image_reference, question)

    async def transcribe_latest_wechat_voice(self) -> dict:
        return await self.media_tools.transcribe_latest_wechat_voice()

    async def send_wechat_image(self, image_id: str, note: str | None = None) -> dict:
        return await self.media_tools.send_wechat_image(image_id, note=note)

    async def generate_and_send_wechat_image(
        self, prompt: str, note: str | None = None,
    ) -> dict:
        return await self.media_tools.generate_and_send_wechat_image(prompt, note=note)

    # --- MemoryToolset delegates ---

    async def remember_user_fact(self, fact_key: str, content: str) -> dict:
        return await self.memory_tools.remember_user_fact(fact_key, content)

    async def search_user_memory(self, query: str) -> dict:
        return await self.memory_tools.search_user_memory(query)

    async def search_message_notes(self, query: str) -> dict:
        return await self.memory_tools.search_message_notes(query)

    async def add_message_notes(self, notes: list[dict[str, str]]) -> dict[str, object]:
        return await self.memory_tools.add_message_notes(notes)

    async def upsert_entity_dictionary_batch(
        self, entries: list[dict[str, str]],
    ) -> dict[str, object]:
        return await self.memory_tools.upsert_entity_dictionary_batch(entries)

    async def delete_entity_dictionary_entry(self, entity_id: str) -> dict:
        return await self.memory_tools.delete_entity_dictionary_entry(entity_id)

    def upsert_sender_memory(
        self, sender_id: str, topic: str, content: str,
    ) -> dict[str, object]:
        return self.memory_tools.upsert_sender_memory(sender_id, topic, content)

    def delete_sender_memory(self, memory_id: str) -> dict[str, object]:
        return self.memory_tools.delete_sender_memory(memory_id)

    def _update_sender_from_entry(
        self, entry: object, choices: object,
    ) -> dict[str, object]:
        return self.memory_tools._update_sender_from_entry(entry, choices)

    async def _bundle_once(self, category: str, payload: object, action: Callable):
        return await self.memory_tools._bundle_once(category, payload, action)

    # --- ReplyToolset delegates ---

    async def schedule_followup(
        self, task: str, wait_seconds: int = 30,
    ) -> dict[str, object]:
        return await self.reply_tools.schedule_followup(task, wait_seconds=wait_seconds)

    async def commit_agent_turn(
        self,
        reply_text: str = "",
        reply_messages: list[str] | None = None,
        reply_image_id: str = "",
        reply_image_position: str = "after",
        reply_note: str = "",
        quote_current_message: bool = False,
        message_notes: list[dict[str, str]] | None = None,
        dictionary_updates: list[dict[str, str]] | None = None,
        dictionary_deletes: list[str] | None = None,
        event_updates: list[dict[str, str]] | None = None,
        sender_updates: list[dict[str, str]] | None = None,
        sender_deletes: list[str] | None = None,
        tag_updates: list[dict[str, object]] | None = None,
        image_notes: list[dict[str, str]] | None = None,
        tool_context: ToolContext | None = None,
    ) -> dict[str, object]:
        return await self.reply_tools.commit_agent_turn(
            reply_text=reply_text,
            reply_messages=reply_messages,
            reply_image_id=reply_image_id,
            reply_image_position=reply_image_position,
            reply_note=reply_note,
            quote_current_message=quote_current_message,
            message_notes=message_notes,
            dictionary_updates=dictionary_updates,
            dictionary_deletes=dictionary_deletes,
            event_updates=event_updates,
            sender_updates=sender_updates,
            sender_deletes=sender_deletes,
            tag_updates=tag_updates,
            image_notes=image_notes,
            tool_context=tool_context,
        )

    # --- Toolset registration ---

    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]

    def agent_tools(self) -> list[ToolUnion]:
        if getattr(self, "media_tools", None) is None:
            # Support uninitialized schema inspection via cls.__new__(cls).agent_tools().
            return [
                *object.__new__(MediaToolset).agent_tools(),
                *object.__new__(ReplyToolset).agent_tools(),
                *object.__new__(MemoryToolset).agent_tools(),
            ]
        return [
            *self.media_tools.agent_tools(),
            *self.reply_tools.agent_tools(),
            *self.memory_tools.agent_tools(),
        ]


WeChatTools = ChannelToolkit
