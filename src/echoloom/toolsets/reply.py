"""Turn commit and follow-up scheduling tools."""

import asyncio
from collections.abc import Callable
from pathlib import Path

from ..outbound import OutboundService, _segment_pause_seconds
from ..runtime_contract import AgentTool as ToolUnion
from ..runtime_contract import ToolContext
from ..tool_timing import agent_function_tool
from ..turn_scope import (
    check_outbound_freshness,
    followup_scheduler,
    mark_turn_freshness_held,
    memory_context,
    operation_id,
)
from .memory import MemoryToolset


class ReplyToolset:
    """Registered reply commit and follow-up scheduling tools."""

    TOOL_NAMES: tuple[str, ...] = (
        "commit_agent_turn",
        "schedule_followup",
    )

    def __init__(
        self,
        *,
        outbound: OutboundService,
        memory_tools: MemoryToolset,
        resolve_image: Callable[[str], Path] | None = None,
    ) -> None:
        self.outbound = outbound
        self.memory_tools = memory_tools
        self._resolve_image = resolve_image

    async def schedule_followup(
        self, task: str, wait_seconds: int = 30,
    ) -> dict[str, object]:
        scheduler = followup_scheduler.get()
        if scheduler is None:
            return {"status": "unavailable"}
        task = " ".join(task.split()) if isinstance(task, str) else ""
        if not task or len(task) > 160:
            return {"status": "rejected", "reason": "task_must_be_1_to_160_characters"}
        if type(wait_seconds) is not int or not 1 <= wait_seconds <= 60:
            return {"status": "rejected", "reason": "wait_must_be_between_1_and_60_seconds"}
        return await scheduler(task, wait_seconds)

    async def _send_commit_image(
        self,
        image_id: str,
        *,
        segment_index: int,
        note: str | None = None,
        record_delivery: bool = True,
    ) -> dict[str, object]:
        scope = operation_id.get()
        if not scope or not self.outbound.can_send():
            return {"status": "already_attempted_or_no_route", "delivered": False}
        if self._resolve_image is None:
            return {
                "status": "rejected",
                "reason": "image_resolver_not_configured",
                "delivered": False,
            }
        resolved_id = self.memory_tools._resolve_ref(image_id, ("image",))
        if not resolved_id:
            return {
                "status": "rejected",
                "reason": "unknown image_id; use list_wechat_images",
                "delivered": False,
            }
        try:
            path = self._resolve_image(resolved_id)
        except Exception as exc:  # noqa: BLE001
            return {"status": "rejected", "reason": str(exc), "delivered": False}
        return await self.outbound.send_commit_image(
            resolved_id,
            path,
            segment_index=segment_index,
            note=note,
            record_delivery=record_delivery,
        )

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
        segments: list[str] = []
        primary = reply_text.strip() if isinstance(reply_text, str) else ""
        if primary:
            segments.append(primary)
        if isinstance(reply_messages, list):
            for item in reply_messages:
                if isinstance(item, str):
                    cleaned = item.strip()
                    if cleaned and (not segments or cleaned != segments[0]):
                        segments.append(cleaned)
        segments = segments[:5]
        image_id = reply_image_id.strip() if isinstance(reply_image_id, str) else ""
        image_pos = (
            reply_image_position.strip().casefold()
            if isinstance(reply_image_position, str)
            else "after"
        )
        if image_pos not in ("after", "before"):
            image_pos = "after"

        if (segments or image_id) and not self.outbound.is_send_blocked():
            decision = check_outbound_freshness()
            if decision is not None and decision.kind == "hold":
                mark_turn_freshness_held()
                if tool_context is not None:
                    tool_context.end_turn()
                return {
                    "status": "held",
                    "delivered": False,
                    "reason": decision.reason,
                    "conflict_count": len(decision.conflicts),
                    "reply": {"status": "held", "delivered": False},
                }

        results: dict[str, object] = {}
        memory_tools = self.memory_tools
        if message_notes:
            results["message_notes"] = await memory_tools._bundle_once(
                "message_notes",
                message_notes,
                lambda: memory_tools.add_message_notes(message_notes),
            )
        if dictionary_updates:
            results["dictionary_updates"] = await memory_tools._bundle_once(
                "dictionary_updates",
                dictionary_updates,
                lambda: memory_tools.upsert_entity_dictionary_batch(dictionary_updates),
            )
        if dictionary_deletes:
            async def delete_dictionaries():
                return [
                    {
                        "entity_id": entity_id,
                        **await memory_tools.delete_entity_dictionary_entry(entity_id),
                    }
                    for entity_id in dictionary_deletes[:20]
                ]

            results["dictionary_deletes"] = await memory_tools._bundle_once(
                "dictionary_deletes", dictionary_deletes, delete_dictionaries,
            )
        if sender_updates:
            def update_senders():
                ctx = memory_context.get() or {}
                choices = ctx.get("sender_choices", [])
                return [
                    memory_tools._update_sender_from_entry(entry, choices)
                    for entry in sender_updates[:20]
                ]

            results["sender_updates"] = await memory_tools._bundle_once(
                "sender_updates", sender_updates, update_senders,
            )
        if sender_deletes:
            results["sender_deletes"] = await memory_tools._bundle_once(
                "sender_deletes",
                sender_deletes,
                lambda: [
                    {"memory_id": memory_id, **memory_tools.delete_sender_memory(memory_id)}
                    for memory_id in sender_deletes[:20]
                ],
            )
        if tag_updates:
            results["tag_updates"] = await memory_tools._bundle_once(
                "tag_updates",
                tag_updates,
                lambda: memory_tools.apply_tag_updates(tag_updates),
            )
        if image_notes:
            results["image_notes"] = await memory_tools._bundle_once(
                "image_notes",
                image_notes,
                lambda: memory_tools.apply_image_notes(image_notes),
            )
        if event_updates:
            results["event_updates"] = await memory_tools._bundle_once(
                "event_updates",
                event_updates,
                lambda: memory_tools.apply_event_updates(event_updates),
            )

        async def _send_text_part(*, record_delivery: bool) -> dict[str, object]:
            if len(segments) == 1:
                single_text = segments[0]
                if quote_current_message:
                    quote_result = await self.outbound.send_quote(
                        single_text, reply_note or None, _record_delivery=record_delivery,
                    )
                    if quote_result.get("status") == "unavailable":
                        # A quote is a presentation preference, not a prerequisite for
                        # sending the reply. The quote path returns unavailable before
                        # submitting anything, so a plain text fallback cannot duplicate
                        # a message that may already have reached Hook.
                        plain_result = await self.outbound.send_text(
                            single_text, reply_note or None, _record_delivery=record_delivery,
                        )
                        return {
                            **plain_result,
                            "quote_fallback": "plain_text",
                            "quote_reason": quote_result.get("reason", "quote_unavailable"),
                            "native_quote_rendered": False,
                        }
                    return quote_result
                return await self.outbound.send_text(
                    single_text, reply_note or None, _record_delivery=record_delivery,
                )
            return await self.outbound.send_multi_segments(
                segments,
                quote_current_message=quote_current_message,
                note=reply_note or None,
                _record_delivery=record_delivery,
            )

        if not image_id:
            if segments:
                results["reply"] = await _send_text_part(record_delivery=True)
            else:
                results["reply"] = {"status": "omitted", "sent": False}
        elif self.outbound.is_send_blocked():
            blocked = self.outbound.send_block_feedback()
            results["reply"] = blocked if segments else {"status": "omitted", "sent": False}
            results["reply_image"] = blocked
        elif not segments:
            results["reply"] = {"status": "omitted", "sent": False}
            results["reply_image"] = await self._send_commit_image(
                image_id, segment_index=0, note=reply_note or None, record_delivery=True,
            )
        else:
            if image_pos == "before":
                img_res = await self._send_commit_image(
                    image_id, segment_index=0, note=None, record_delivery=False,
                )
                if img_res.get("status") in ("queued", "accepted_by_server"):
                    await asyncio.sleep(_segment_pause_seconds("表情包"))
                text_res = await _send_text_part(record_delivery=False)
            else:
                text_res = await _send_text_part(record_delivery=False)
                if text_res.get("status") in ("queued", "accepted_by_server"):
                    await asyncio.sleep(_segment_pause_seconds("表情包"))
                img_res = await self._send_commit_image(
                    image_id, segment_index=len(segments), note=None, record_delivery=False,
                )
            results["reply"] = text_res
            results["reply_image"] = img_res
            scope = operation_id.get()
            if scope:
                statuses = [
                    (str(item.get("status") or ""), item.get("reason"))
                    for item in (text_res, img_res)
                ]
                succeeded = next(
                    ((st, rsn) for st, rsn in statuses if st in ("queued", "accepted_by_server")),
                    None,
                )
                if succeeded is not None:
                    self.outbound.record_send_result(
                        scope, succeeded[0], str(succeeded[1]) if succeeded[1] else None,
                    )
                else:
                    failed = next(
                        ((st, rsn) for st, rsn in statuses if st in ("rejected", "unknown")),
                        None,
                    )
                    if failed is not None:
                        self.outbound.record_send_result(
                            scope, failed[0], str(failed[1]) if failed[1] else None,
                        )
        # This bundled tool is the turn's commit point: it already performs any
        # send and data mutations. End the ADK invocation after its result so the
        # model cannot call it repeatedly and emit a stream of separate replies.
        if tool_context is not None:
            tool_context.end_turn()
        return {"status": "completed", **results}

    def agent_tools(self) -> list[ToolUnion]:
        return [
            agent_function_tool(fn)
            for fn in (
                self.commit_agent_turn,
                self.schedule_followup,
            )
        ]

    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from ..frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
