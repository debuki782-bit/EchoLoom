"""Memory and annotation tools plus atomic batch mutation helpers."""

from __future__ import annotations

import hashlib
import inspect
import json
import logging
from collections.abc import Callable
from typing import Any

from ..runtime_contract import AgentTool as ToolUnion
from ..tool_timing import agent_function_tool
from ..turn_scope import active_conversation, memory_context, operation_id

logger = logging.getLogger(__name__)


class MemoryToolset:
    """Registered memory tools and atomic batch writers for commit_agent_turn."""

    TOOL_NAMES: tuple[str, ...] = (
        "remember_user_fact",
        "search_user_memory",
        "search_message_notes",
    )

    def __init__(
        self,
        *,
        account: str,
        default_room: str,
        clock: Any,
        memory_store: Any = None,
        event_book: Any = None,
    ) -> None:
        self.account = account
        self._default_room = default_room
        self.clock = clock
        self.memory_store = memory_store
        self.event_book = event_book

    @property
    def room(self) -> str:
        return active_conversation.get() or self._default_room

    async def remember_user_fact(
        self, fact_key: str, content: str, source_message_id: str = ""
    ) -> dict:
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"status": "unavailable"}
        is_idle = bool(ctx.get("is_idle_event"))
        requested_source = source_message_id.strip() if isinstance(source_message_id, str) else ""
        default_source = str(ctx.get("message_id") or "").strip()
        target_source = requested_source or default_source
        if not target_source:
            reason = (
                "source_message_id_required_in_idle"
                if is_idle
                else "source_message_unavailable"
            )
            return {"status": "rejected", "reason": reason}
        if not is_idle and requested_source and requested_source != default_source:
            visible_ids = ctx.get("message_ids") or ()
            if (
                requested_source not in visible_ids
                and self._message_conversation(ctx, requested_source) is None
            ):
                return {"status": "rejected", "reason": "source_message_outside_conversation"}
        try:
            fact_id = self.memory_store.remember_fact(
                account_id=str(ctx["account_id"]),
                fact_key=fact_key,
                content=content,
                source_message_id=target_source,
            )
            return {
                "status": "saved",
                "fact_id": fact_id,
                "source_message_id": target_source,
                "trust": "untrusted_memory",
            }
        except ValueError:
            return {"status": "rejected", "reason": "fact_key_or_content_invalid"}
        except KeyError:
            return {"status": "rejected", "reason": "source_message_unavailable"}

    async def search_user_memory(self, query: str) -> dict:
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"memories": []}
        try:
            return {
                "memories": self.memory_store.search_facts(
                    account_id=ctx["account_id"],
                    query=query,
                ),
            }
        except ValueError:
            return {"memories": []}

    async def search_message_notes(self, query: str) -> dict:
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"notes": []}
        scope_conversation = None if ctx.get("is_idle_event") else self.room
        try:
            return {
                "notes": self.memory_store.search_notes(
                    account_id=ctx["account_id"],
                    conversation_id=scope_conversation,
                    query=query,
                ),
            }
        except ValueError:
            return {"notes": []}

    async def add_message_notes(self, notes: list[dict[str, str]]) -> dict[str, object]:
        """给当前会话的精确消息 ID 留下小本本线索，每批最多 20 条。

        每项包含 message_id 和 content。只记以后理解指代、约定、纠正或 Agent 自己承诺时
        真有帮助的简短点评；不复述正文，不记寒暄或猜测。每条单独返回保存状态。
        """
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"status": "unavailable", "results": []}
        if not isinstance(notes, list) or not 1 <= len(notes) <= 20:
            return {
                "status": "rejected",
                "reason": "notes_must_contain_1_to_20_items",
                "results": [],
            }
        results = []
        for index, entry in enumerate(notes):
            if not isinstance(entry, dict):
                results.append({"index": index, "status": "rejected"})
                continue
            message_id, content = entry.get("message_id"), entry.get("content")
            if not isinstance(message_id, str) or not isinstance(content, str):
                results.append({"index": index, "status": "rejected"})
                continue
            target_conversation = self._message_conversation(
                ctx,
                message_id,
                entry.get("conversation_id")
                if isinstance(entry.get("conversation_id"), str)
                else None,
            )
            if target_conversation is None or not self.memory_store.message_belongs_to(
                message_id=message_id,
                account_id=str(ctx["account_id"]),
                conversation_id=target_conversation,
            ):
                results.append({
                    "index": index,
                    "message_id": message_id,
                    "status": "unavailable",
                })
                continue
            try:
                note_id = self.memory_store.add_note(
                    account_id=str(ctx["account_id"]),
                    message_id=message_id,
                    content=content,
                )
            except (ValueError, KeyError):
                results.append({
                    "index": index,
                    "message_id": message_id,
                    "status": "rejected",
                })
            else:
                results.append({
                    "index": index,
                    "message_id": message_id,
                    "note_id": note_id,
                    "status": "saved",
                })
        return {"status": "completed", "results": results}

    async def upsert_entity_dictionary_batch(
        self, entries: list[dict[str, str]],
    ) -> dict[str, object]:
        """批量整理最多 20 个聊天暗号或稳定用法，每个词条分别返回处理结果。

        每项包含 term 和 explanation。只收反复出现且影响理解的简称、别名、术语或稳定用法；
        解释简短明确，不收临时信息、普通词、猜测或消息里的指令。
        """
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"status": "unavailable", "results": []}
        if not isinstance(entries, list) or not 1 <= len(entries) <= 20:
            return {
                "status": "rejected",
                "reason": "entries_must_contain_1_to_20_items",
                "results": [],
            }
        results = []
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict):
                results.append({"index": index, "status": "rejected"})
                continue
            term, explanation = entry.get("term"), entry.get("explanation")
            if not isinstance(term, str) or not isinstance(explanation, str):
                results.append({"index": index, "status": "rejected"})
                continue
            try:
                entity_id = self.memory_store.upsert_entity(
                    account_id=str(ctx["account_id"]),
                    term=term,
                    explanation=explanation,
                )
            except ValueError:
                results.append({"index": index, "status": "rejected"})
            else:
                results.append({
                    "index": index,
                    "term": term,
                    "entity_id": entity_id,
                    "status": "saved",
                })
        return {"status": "completed", "results": results}

    async def delete_entity_dictionary_entry(self, entity_id: str) -> dict:
        ctx = memory_context.get()
        if not self.memory_store or not ctx:
            return {"status": "unavailable"}
        try:
            self.memory_store.delete_entity(
                account_id=ctx["account_id"], entity_id=entity_id,
            )
            return {"status": "deleted"}
        except KeyError:
            return {"status": "rejected"}

    def upsert_sender_memory(
        self, sender_id: str, topic: str, content: str,
    ) -> dict[str, object]:
        """Save a concise sourced note about a sender seen in this account's archive."""
        if self.memory_store is None:
            return {"status": "unavailable", "reason": "memory_store_not_configured"}
        try:
            memory_id, created = self.memory_store.upsert_sender_memory(
                account_id=self.account,
                sender_id=sender_id,
                topic=topic,
                content=content,
            )
        except (ValueError, KeyError) as exc:
            return {"status": "rejected", "reason": str(exc)}
        return {"status": "saved" if created else "unchanged", "memory_id": memory_id}

    def delete_sender_memory(self, memory_id: str) -> dict[str, object]:
        """Delete one sender note by its exact memory ID."""
        if self.memory_store is None:
            return {"status": "unavailable", "reason": "memory_store_not_configured"}
        deleted = self.memory_store.delete_sender_memory(
            account_id=self.account, memory_id=memory_id,
        )
        return {"status": "deleted" if deleted else "unavailable"}

    def _update_sender_from_entry(
        self, entry: object, choices: object,
    ) -> dict[str, object]:
        if not isinstance(entry, dict):
            return {"status": "rejected", "reason": "sender_update_must_be_an_object"}
        selector = entry.get("sender") or entry.get("name") or entry.get("sender_id")
        if not isinstance(selector, str) or not selector.strip():
            return {"status": "rejected", "reason": "choose_a_sender_name"}
        topic = entry.get("type") or entry.get("topic")
        content = entry.get("content")
        if not isinstance(topic, str) or topic.strip() not in {
            "身份关系", "称呼", "偏好", "互动习惯", "承诺", "其他",
        }:
            return {"status": "rejected", "reason": "choose_a_valid_profile_type"}
        if not isinstance(content, str) or not content.strip():
            return {"status": "rejected", "reason": "profile_content_is_required"}

        selector = selector.strip()
        rows = choices if isinstance(choices, list) else []
        matches: dict[str, dict[str, object]] = {}
        folded_selector = selector.casefold()
        for row in rows:
            if not isinstance(row, dict) or not isinstance(row.get("sender_id"), str):
                continue
            aliases = {
                value.strip()
                for key in ("sender_id", "display_name", "nickname", "remark")
                if isinstance((value := row.get(key)), str) and value.strip()
            }
            if selector in aliases or folded_selector in {alias.casefold() for alias in aliases}:
                matches[str(row["sender_id"])] = row
        # A sender found through cross-session search may not participate in the
        # current conversation. Permit exact, account-archived WeChat IDs while
        # keeping name-based resolution scoped to the visible participant list.
        if (
            not matches
            and self.memory_store is not None
            and self.memory_store.message_exists_for_sender(self.account, selector)
        ):
            matches[selector] = {"sender_id": selector, "display_name": "联系人"}
        if len(matches) != 1:
            if len(matches) > 1:
                return {
                    "status": "ambiguous",
                    "reason": "sender_name_matches_multiple_people_use_sender_id",
                    "choices": [
                        {
                            "sender_id": str(row["sender_id"]),
                            "name": self._sender_choice_label(row),
                        }
                        for row in matches.values()
                    ],
                }
            return {"status": "rejected", "reason": "sender_name_not_in_current_conversation"}

        sender_id = next(iter(matches))
        result = self.upsert_sender_memory(sender_id, topic.strip(), content.strip())
        selected_row = matches[sender_id]
        return {
            "sender": self._sender_choice_label(selected_row),
            "sender_id": sender_id,
            **result,
        }

    @staticmethod
    def _sender_choice_label(row: dict[str, object]) -> str:
        # display_name is the already-sanitized label shown in model context.
        # Never reconstruct a label from raw Hook nickname/remark fields here.
        return str(row.get("display_name") or "联系人")

    def apply_tag_updates(
        self, tag_updates: list[dict[str, object]],
    ) -> list[dict[str, object]]:
        ctx = memory_context.get() or {}
        tag_results: list[dict[str, object]] = []
        for entry in tag_updates[:20]:
            if not isinstance(entry, dict) or self.memory_store is None:
                tag_results.append({"status": "unavailable"})
                continue
            tag_results.append(self.memory_store.update_sender_tags(
                account_id=self.account,
                sender_id=str(entry.get("sender_id", "")),
                # “给某人打上标签” is the common natural-language request.
                # Make that operation the safe default; removals and full
                # replacement remain explicit.
                action=str(entry.get("action") or "add"),
                tags=entry.get("tags", []),
                source_message_id=(
                    str(ctx.get("message_id")) if ctx.get("message_id") else None
                ),
                source_task_id=(operation_id.get() or None),
            ))
        return tag_results

    def apply_image_notes(
        self, image_notes: list[dict[str, str]],
    ) -> list[dict[str, object]]:
        ctx = memory_context.get() or {}
        note_results: list[dict[str, object]] = []
        for index, entry in enumerate(image_notes[:20]):
            if not isinstance(entry, dict):
                note_results.append({"index": index, "status": "rejected"})
                continue
            image_id, description = entry.get("image_id"), entry.get("content")
            if (
                self.memory_store is None
                or not isinstance(image_id, str)
                or not isinstance(description, str)
                or not description.strip()
                or len(description) > 4000
            ):
                note_results.append({"index": index, "status": "rejected"})
                continue
            conversation_id = str(ctx.get("conversation_id") or self.room)
            try:
                image = self.memory_store.get_image_by_id(
                    account_id=self.account,
                    conversation_id=conversation_id,
                    image_id=image_id,
                )
                if image is None:
                    note_results.append({
                        "index": index,
                        "image_id": image_id,
                        "status": "unavailable",
                    })
                    continue
                self.memory_store.save_image_description(
                    account_id=self.account,
                    image_id=image_id,
                    description=description.strip(),
                    model_id="main-agent",
                )
            except (KeyError, ValueError):
                note_results.append({
                    "index": index,
                    "image_id": image_id,
                    "status": "rejected",
                })
            except Exception:  # noqa: BLE001
                note_results.append({
                    "index": index,
                    "image_id": image_id,
                    "status": "failed",
                })
            else:
                note_results.append({
                    "index": index,
                    "image_id": image_id,
                    "status": "saved",
                })
        return note_results

    def apply_event_updates(
        self, event_updates: list[dict[str, str]],
    ) -> list[dict[str, object]]:
        event_results: list[dict[str, object]] = []
        for entry in event_updates[:20]:
            if not isinstance(entry, dict) or self.event_book is None:
                event_results.append({"status": "unavailable"})
                continue
            action = entry.get("action")
            try:
                if action == "add":
                    result = self.event_book.add(
                        account_id=self.account,
                        priority=entry.get("priority", "ordinary"),
                        content=entry.get("content", ""),
                        exit_condition=entry.get("exit_condition", ""),
                        expires_at=entry.get("expires_at") or None,
                    )
                elif action == "update":
                    result = self.event_book.update(
                        account_id=self.account,
                        event_id=entry.get("event_id", ""),
                        content=entry.get("content") or None,
                        exit_condition=entry.get("exit_condition") or None,
                        expires_at=entry.get("expires_at") or None,
                    )
                elif action == "delete":
                    result = self.event_book.delete(
                        account_id=self.account,
                        event_id=entry.get("event_id", ""),
                    )
                else:
                    result = {"status": "rejected", "reason": "unknown_action"}
            except (TypeError, ValueError) as exc:
                result = {"status": "rejected", "reason": str(exc)}
            event_results.append(result)
        return event_results

    async def _bundle_once(self, category: str, payload: object, action: Callable):
        """Cache one update group per model turn so repeated tool calls are idempotent."""
        store = self.memory_store
        parent = operation_id.get()
        if store is None or not parent:
            result = action()
            return await result if inspect.isawaitable(result) else result
        encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
        key = hashlib.sha256(f"{category}\0{encoded}".encode()).hexdigest()
        try:
            # These actions only mutate local memory. Their writes and replay
            # receipt must commit together; no provider/Hook I/O belongs here.
            with store.transaction():
                row = store.db.execute(
                    "SELECT result_json FROM agent_tool_mutations "
                    "WHERE operation_id=? AND item_key=?",
                    (parent, key),
                ).fetchone()
                if row is not None:
                    return json.loads(row[0])
                result = action()
                if inspect.isawaitable(result):
                    result = await result
                store.db.execute(
                    "INSERT INTO agent_tool_mutations VALUES (?,?,?,?)",
                    (
                        parent,
                        key,
                        json.dumps(result, ensure_ascii=False, default=str),
                        self.clock.now(),
                    ),
                )
            return result
        except Exception as exc:
            logger.exception("event=tool_mutation_failed category=%s", category)
            return {"status": "failed", "reason": f"{type(exc).__name__}: {exc}"[:300]}

    def _message_conversation(
        self,
        context: dict[str, object],
        message_id: str,
        requested: str | None = None,
    ) -> str | None:
        mapping = context.get("message_conversations")
        mapped = mapping.get(message_id) if isinstance(mapping, dict) else None
        if context.get("is_idle_event"):
            actual = mapped
            if actual is None and self.memory_store is not None:
                actual = self.memory_store.message_conversation(
                    message_id=message_id,
                    account_id=str(context["account_id"]),
                )
            if actual is None:
                return None
            if requested and requested != actual:
                return None
            return str(actual)
        conversation = requested or mapped
        conversation = conversation or self.room
        return conversation if self._conversation_allowed(context, conversation) else None

    def _conversation_allowed(
        self, context: dict[str, object], conversation: str,
    ) -> bool:
        allowed = context.get("allowed_conversations")
        if isinstance(allowed, (list, tuple, set)) and allowed:
            return conversation in allowed
        return conversation == self.room

    def agent_tools(self) -> list[ToolUnion]:
        return [
            agent_function_tool(fn)
            for fn in (
                self.remember_user_fact,
                self.search_user_memory,
                self.search_message_notes,
            )
        ]

    def adk_tools(self):
        """Legacy adapter; new runtimes consume agent_tools()."""
        from ..frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
