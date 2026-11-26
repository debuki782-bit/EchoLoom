"""Strictly read-only unified context builder for chat, followup, cross-session, and idle turns."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

from .contacts import (
    ContactIdentity,
    apply_sender_remarks,
    is_contact_id,
    is_group_conversation,
    is_room_id,
)
from .context import (
    ContextTurn,
    ContextWindow,
    _other_chat_turn,
    _record_turn,
    _safe_identity_name,
    _stored_turn,
    _unique_messages,
    fit_context_window,
)
from .db_async import run_db
from .memory_store import MAX_QUERY_CHARS, MemoryStore
from .message_ingestor import IngestedBatch, entity_search_text
from .prompt_catalog import load_prompt
from .rich_message import quoted_author_id, quoted_message_reference
from .temporal_context import DailyEnvironmentProvider, message_time_context
from .time_scheduler import TimeScheduler
from .wechat_gateway import MessageEvent


@dataclass(frozen=True)
class ContextSpec:
    """Declarative specification for building a budgeted, read-only ContextWindow."""

    mode: Literal["chat", "followup", "cross_session", "idle"]
    account_id: str
    conversation_id: str
    is_group: bool
    observations: tuple[MessageEvent, ...] = ()
    ingested_records: tuple[Mapping[str, object], ...] = ()
    message_times: tuple[Mapping[str, object], ...] = ()
    synthetic_turns: tuple[ContextTurn, ...] = ()
    same_chat_limit: int = 50
    other_chat_limit: int = 20
    global_timeline_limit: int = 0
    idle_summaries_limit: int = 0
    max_user_bytes: int = 32_000
    include_daily_env: bool = True
    include_priority_senders: bool = True
    include_quick_send_images: bool = True
    scheduled_task: str | None = None
    delivery_notices: tuple[str, ...] = ()
    cancelled_followup: bool = False
    query_hint: str = ""
    extra_identifiers: tuple[str, ...] = ()

    @classmethod
    def for_chat(
        cls,
        *,
        observations: Sequence[MessageEvent] | None = None,
        ingested: IngestedBatch | None = None,
        ingested_records: Sequence[Mapping[str, object]] = (),
        message_times: Sequence[Mapping[str, object]] = (),
        same_chat_limit: int = 50,
        other_chat_limit: int = 20,
        max_user_bytes: int = 32_000,
        include_daily_env: bool = True,
        include_priority_senders: bool = True,
        include_quick_send_images: bool = True,
        delivery_notices: Sequence[str] = (),
        cancelled_followup: bool = False,
    ) -> ContextSpec:
        if ingested is not None:
            obs = ingested.observations
            recs = ingested.records
            times = ingested.message_times
        else:
            obs = tuple(observations or ())
            recs = tuple(ingested_records)
            times = tuple(message_times)
        if not 1 <= len(obs) <= 20:
            raise ValueError("chat context requires 1 to 20 observations")
        first = obs[0]
        if any(
            (item.account_id, item.conversation_id, item.is_group)
            != (first.account_id, first.conversation_id, first.is_group)
            for item in obs
        ):
            raise ValueError("cross-conversation context is forbidden")
        return cls(
            mode="chat",
            account_id=first.account_id,
            conversation_id=first.conversation_id,
            is_group=first.is_group,
            observations=obs,
            ingested_records=recs,
            message_times=times,
            same_chat_limit=same_chat_limit,
            other_chat_limit=other_chat_limit,
            global_timeline_limit=0,
            idle_summaries_limit=0,
            max_user_bytes=max_user_bytes,
            include_daily_env=include_daily_env,
            include_priority_senders=include_priority_senders,
            include_quick_send_images=include_quick_send_images,
            delivery_notices=tuple(delivery_notices),
            cancelled_followup=cancelled_followup,
        )

    @classmethod
    def for_followup(
        cls,
        *,
        scheduled_task: str,
        account_id: str | None = None,
        conversation_id: str | None = None,
        is_group: bool | None = None,
        observations: Sequence[MessageEvent] = (),
        ingested: IngestedBatch | None = None,
        ingested_records: Sequence[Mapping[str, object]] = (),
        message_times: Sequence[Mapping[str, object]] = (),
        same_chat_limit: int = 50,
        other_chat_limit: int = 20,
        max_user_bytes: int = 32_000,
        include_daily_env: bool = True,
        include_priority_senders: bool = True,
        include_quick_send_images: bool = True,
        delivery_notices: Sequence[str] = (),
    ) -> ContextSpec:
        if ingested is not None:
            obs = ingested.observations
            recs = ingested.records
            times = ingested.message_times
        else:
            obs = tuple(observations)
            recs = tuple(ingested_records)
            times = tuple(message_times)
        if obs:
            if len(obs) > 20:
                raise ValueError("followup context accepts at most 20 observations")
            first = obs[0]
            if any(
                (item.account_id, item.conversation_id, item.is_group)
                != (first.account_id, first.conversation_id, first.is_group)
                for item in obs
            ):
                raise ValueError("cross-conversation context is forbidden")
            acct = account_id or first.account_id
            conv = conversation_id or first.conversation_id
            group = first.is_group if is_group is None else is_group
        else:
            if not account_id or not conversation_id:
                raise ValueError("followup without observations requires account_id and conversation_id")
            acct = account_id
            conv = conversation_id
            group = is_group_conversation(conversation_id, is_group=is_group)
        return cls(
            mode="followup",
            account_id=acct,
            conversation_id=conv,
            is_group=group,
            observations=obs,
            ingested_records=recs,
            message_times=times,
            same_chat_limit=same_chat_limit,
            other_chat_limit=other_chat_limit,
            global_timeline_limit=0,
            idle_summaries_limit=0,
            max_user_bytes=max_user_bytes,
            include_daily_env=include_daily_env,
            include_priority_senders=include_priority_senders,
            include_quick_send_images=include_quick_send_images,
            scheduled_task=scheduled_task,
            delivery_notices=tuple(delivery_notices),
        )

    @classmethod
    def for_cross_session(
        cls,
        *,
        account_id: str,
        conversation_id: str,
        is_group: bool | None = None,
        synthetic_turns: Sequence[ContextTurn] = (),
        observations: Sequence[MessageEvent] = (),
        ingested: IngestedBatch | None = None,
        ingested_records: Sequence[Mapping[str, object]] = (),
        message_times: Sequence[Mapping[str, object]] = (),
        query_hint: str = "",
        same_chat_limit: int = 50,
        other_chat_limit: int = 0,
        max_user_bytes: int = 24_000,
        include_daily_env: bool = True,
        include_priority_senders: bool = True,
        include_quick_send_images: bool = True,
        extra_identifiers: Sequence[str] = (),
    ) -> ContextSpec:
        if ingested is not None:
            obs = ingested.observations
            recs = ingested.records
            times = ingested.message_times
        else:
            obs = tuple(observations)
            recs = tuple(ingested_records)
            times = tuple(message_times)
        group = is_group_conversation(conversation_id, is_group=is_group)
        return cls(
            mode="cross_session",
            account_id=account_id,
            conversation_id=conversation_id,
            is_group=group,
            observations=obs,
            ingested_records=recs,
            message_times=times,
            synthetic_turns=tuple(synthetic_turns),
            same_chat_limit=same_chat_limit,
            other_chat_limit=other_chat_limit,
            global_timeline_limit=0,
            idle_summaries_limit=0,
            max_user_bytes=max_user_bytes,
            include_daily_env=include_daily_env,
            include_priority_senders=include_priority_senders,
            include_quick_send_images=include_quick_send_images,
            query_hint=query_hint,
            extra_identifiers=tuple(extra_identifiers),
        )

    @classmethod
    def for_idle(
        cls,
        *,
        account_id: str,
        event_id: str = "",
        conversation_id: str | None = None,
        task_text: str | None = None,
        synthetic_turns: Sequence[ContextTurn] = (),
        global_timeline_limit: int = 100,
        idle_summaries_limit: int = 20,
        max_user_bytes: int = 80_000,
        include_daily_env: bool = True,
        include_priority_senders: bool = True,
        include_quick_send_images: bool = True,
    ) -> ContextSpec:
        idle_conv = conversation_id or (f"idle-event:{event_id}" if event_id else "idle-event")
        turns = tuple(synthetic_turns)
        if not turns:
            prompt = task_text if task_text is not None else load_prompt("idle_fallback")
            turns = (
                ContextTurn(
                    sender_id="系统空闲调度器",
                    sender_origin="idle_task",
                    text=prompt,
                    message_id=f"idle:{event_id}" if event_id else "idle",
                    source_conversation_id=idle_conv,
                    source_is_group=False,
                ),
            )
        return cls(
            mode="idle",
            account_id=account_id,
            conversation_id=idle_conv,
            is_group=False,
            synthetic_turns=turns,
            same_chat_limit=0,
            other_chat_limit=0,
            global_timeline_limit=global_timeline_limit,
            idle_summaries_limit=idle_summaries_limit,
            max_user_bytes=max_user_bytes,
            include_daily_env=include_daily_env,
            include_priority_senders=include_priority_senders,
            include_quick_send_images=include_quick_send_images,
        )


@dataclass(frozen=True, repr=False)
class BuiltContext:
    """Result of a read-only context build pass."""

    context: ContextWindow
    quote_context: tuple[str, str, str, str] | None

    def __repr__(self) -> str:
        return f"BuiltContext(turns={len(self.context.turns)})"


class ContextBuilder:
    """100% pure read-only context builder shared across all turn modes."""

    def __init__(
        self,
        memory_store: MemoryStore | None = None,
        *,
        resolve_contact: Callable[[str], ContactIdentity | None] | None = None,
        daily_environment: DailyEnvironmentProvider | None = None,
        time_scheduler: TimeScheduler | None = None,
        idle_summaries_provider: Callable[[str, int], Sequence[str]] | None = None,
    ) -> None:
        self.memory_store = memory_store
        self.resolve_contact = resolve_contact
        self.daily_environment = daily_environment
        self.time_scheduler = time_scheduler
        self.idle_summaries_provider = idle_summaries_provider

    async def build(self, spec: ContextSpec) -> BuiltContext:
        """Build a budgeted ContextWindow without performing any writes or media decoding."""
        if spec.max_user_bytes < 512:
            raise ValueError("max_user_bytes must be at least 512")
        if spec.observations:
            if len(spec.observations) > 20:
                raise ValueError("context requires at most 20 observations")
            if any(
                (item.account_id, item.conversation_id, item.is_group)
                != (spec.account_id, spec.conversation_id, spec.is_group)
                for item in spec.observations
            ):
                raise ValueError("cross-conversation context is forbidden")
            if any(item.message_type != 1 for item in spec.observations):
                raise ValueError("only text observations are supported")

        current_records: list[dict[str, object]] = []
        message_times: list[Mapping[str, object]] = []
        quote_requests: list[tuple[int, str, Mapping[str, str]]] = []
        quote_context: tuple[str, str, str, str] | None = None

        for index, item in enumerate(spec.observations):
            event_time = (
                spec.message_times[index]
                if index < len(spec.message_times)
                else (item.event_time_context or message_time_context(item.capture_tick_ms))
            )
            message_times.append(event_time)

            if index < len(spec.ingested_records):
                record = dict(spec.ingested_records[index])
            else:
                record = {}
            record.setdefault("sender_id", item.sender_id)
            record.setdefault("conversation_id", item.conversation_id)
            record.setdefault("body_text", item.text)
            record.setdefault("entity_search_text", entity_search_text(item))
            current_records.append(record)

            if self.memory_store is not None:
                quote_ref = quoted_message_reference(item.text)
                if quote_ref:
                    quote_requests.append((index, item.account_id, quote_ref))

            if item.original_message_type in (None, 1):
                quote_context = (
                    item.sender_id,
                    "",
                    item.conversation_id,
                    entity_search_text(item),
                )

        current_ids = {
            str(row["message_id"])
            for row in current_records
            if isinstance(row.get("message_id"), str) and row.get("message_id")
        }

        if self.memory_store is not None:
            memory_store = self.memory_store

            def _load_memory_context():
                quote_targets_local: list[dict[str, object]] = []
                quote_positions_local: dict[int, int] = {}
                for q_index, q_account_id, quote_ref in quote_requests:
                    target = memory_store.find_message_by_platform_id(
                        account_id=q_account_id,
                        conversation_id=quote_ref["source_conversation_id"],
                        platform_message_id=quote_ref["platform_message_id"],
                    )
                    if target is None:
                        continue
                    target_id = target.get("message_id")
                    existing_pos = next(
                        (
                            pos
                            for pos, row in enumerate(quote_targets_local)
                            if row.get("message_id") == target_id
                        ),
                        None,
                    )
                    if existing_pos is None:
                        quote_positions_local[q_index] = len(quote_targets_local)
                        quote_targets_local.append(target)
                    else:
                        quote_positions_local[q_index] = existing_pos

                quote_ids = {
                    str(row["message_id"])
                    for row in quote_targets_local
                    if isinstance(row.get("message_id"), str) and row.get("message_id")
                }
                excluded_ids = current_ids | quote_ids

                hist_local: list[dict[str, object]] = []
                other_local: list[dict[str, object]] = []
                global_local: list[dict[str, object]] = []

                if spec.same_chat_limit > 0:
                    same_limit = min(spec.same_chat_limit, 50)
                    recent = memory_store.recent_messages(
                        account_id=spec.account_id,
                        conversation_id=spec.conversation_id,
                        limit=same_limit,
                    )
                    cleaned_hint = (
                        " ".join(spec.query_hint.split()).replace("\0", "")[:MAX_QUERY_CHARS]
                        if spec.query_hint
                        else ""
                    )
                    if cleaned_hint:
                        matched = memory_store.search_messages(
                            account_id=spec.account_id,
                            conversation_id=spec.conversation_id,
                            query=cleaned_hint,
                            limit=min(20, same_limit),
                        )
                        selected = [
                            row for row in matched if row.get("message_id") not in excluded_ids
                        ]
                        selected_ids = {str(row["message_id"]) for row in selected}
                        recent_candidates = [
                            row
                            for row in recent
                            if row.get("message_id") not in excluded_ids
                            and str(row.get("message_id")) not in selected_ids
                        ]
                        remaining = max(0, same_limit - len(selected))
                        supplemental = recent_candidates[-remaining:] if remaining else []
                        by_id = {str(row["message_id"]): row for row in (*selected, *supplemental)}
                        hist_local = sorted(
                            by_id.values(),
                            key=lambda row: (
                                str(row.get("captured_at", "")),
                                str(row.get("message_id", "")),
                            ),
                        )
                    else:
                        hist_local = [
                            row for row in recent if row.get("message_id") not in excluded_ids
                        ]

                if spec.other_chat_limit > 0:
                    other_limit = min(spec.other_chat_limit, 20)
                    other_local = [
                        row
                        for row in memory_store.recent_other_messages(
                            account_id=spec.account_id,
                            conversation_id=spec.conversation_id,
                            limit=other_limit,
                        )
                        if row.get("message_id") not in excluded_ids
                    ]

                if spec.global_timeline_limit > 0:
                    global_limit = min(spec.global_timeline_limit, 100)
                    global_local = [
                        row
                        for row in memory_store.messages_for_idle_event(
                            account_id=spec.account_id,
                            limit=global_limit,
                        )
                        if row.get("message_id") not in excluded_ids
                    ]

                all_records = (
                    current_records
                    + quote_targets_local
                    + hist_local
                    + other_local
                    + global_local
                )
                if all_records:
                    if len(all_records) <= 120:
                        enriched = memory_store.enrich_context_messages(
                            account_id=spec.account_id,
                            messages=all_records,
                        )
                    else:
                        enriched = []
                        for offset in range(0, len(all_records), 120):
                            enriched.extend(
                                memory_store.enrich_context_messages(
                                    account_id=spec.account_id,
                                    messages=all_records[offset : offset + 120],
                                )
                            )
                    n_cur = len(current_records)
                    n_qt = len(quote_targets_local)
                    n_hist = len(hist_local)
                    n_other = len(other_local)
                    enr_cur = enriched[:n_cur]
                    enr_qt = enriched[n_cur : n_cur + n_qt]
                    for idx, target_pos in quote_positions_local.items():
                        enr_cur[idx]["quote_target"] = enr_qt[target_pos]
                    start_hist = n_cur + n_qt
                    end_hist = start_hist + n_hist
                    end_other = end_hist + n_other
                    hist_local = enriched[start_hist:end_hist]
                    other_local = enriched[end_hist:end_other]
                    global_local = enriched[end_other:]
                else:
                    enr_cur = current_records
                    enr_qt = quote_targets_local

                priority_local: list[dict[str, object]] = []
                quick_local: list[dict[str, object]] = []
                if spec.include_priority_senders:
                    priority_local = memory_store.priority_sender_profiles(
                        account_id=spec.account_id,
                        current_conversation_id=(
                            None if spec.mode == "idle" else spec.conversation_id
                        ),
                        exclude_message_ids=tuple(current_ids),
                    )
                if spec.include_quick_send_images:
                    quick_local = memory_store.quick_send_images(
                        account_id=spec.account_id,
                        limit=10,
                    )
                return (
                    enr_cur,
                    enr_qt,
                    hist_local,
                    other_local,
                    global_local,
                    priority_local,
                    quick_local,
                )

            (
                enriched_current,
                enriched_quotes,
                historical,
                other,
                global_timeline,
                priority_sender_references,
                quick_send_images,
            ) = await run_db(
                _load_memory_context,
                db=memory_store.db,
                lock=memory_store._lock,
            )
        else:
            enriched_current = current_records
            enriched_quotes = []
            historical = []
            other = []
            global_timeline = []
            priority_sender_references = []
            quick_send_images = []

        priority_sender_ids = [
            str(profile["sender_id"])
            for profile in priority_sender_references
            if isinstance(profile.get("sender_id"), str)
        ]
        activity_chat_ids = [
            str(act["conversation_id"])
            for profile in priority_sender_references
            for act in (profile.get("other_chat_activity") or ())
            if isinstance(act, Mapping) and isinstance(act.get("conversation_id"), str)
        ]

        identities = await self._resolve_identities(
            spec,
            enriched_current=enriched_current,
            enriched_quotes=enriched_quotes,
            historical=historical,
            other=other,
            global_timeline=global_timeline,
            priority_sender_ids=priority_sender_ids,
            extra_identifiers=(*spec.extra_identifiers, *activity_chat_ids),
        )

        daily: str | None = None
        if spec.include_daily_env and self.daily_environment is not None:
            try:
                daily = await asyncio.to_thread(self.daily_environment.system_prompt_context)
            except Exception:  # noqa: BLE001 - weather must not block message handling
                daily = "本地天气暂不可用；不要猜测或编造天气。"

        idle_summaries: tuple[str, ...] = ()
        if spec.idle_summaries_limit > 0:
            if self.idle_summaries_provider is not None:
                idle_summaries = tuple(
                    self.idle_summaries_provider(spec.account_id, spec.idle_summaries_limit)
                )
            elif self.time_scheduler is not None:
                idle_summaries = tuple(
                    await run_db(
                        self.time_scheduler.recent_idle_activities,
                        spec.account_id,
                        limit=spec.idle_summaries_limit,
                        db=self.time_scheduler.db,
                        lock=self.time_scheduler._lock,
                    )
                )

        stored_for_turns = (
            enriched_current
            if (self.memory_store is not None or spec.ingested_records)
            else None
        )
        obs_turns = tuple(
            _stored_turn(item, index, stored_for_turns, message_times)
            for index, item in enumerate(spec.observations)
        )
        turns = (*obs_turns, *spec.synthetic_turns)

        history_origin = (
            "cross_session_history" if spec.mode == "cross_session" else "stored_other_chat"
        )
        history_turns = (
            tuple(
                _record_turn(row, sender_origin=history_origin)
                for row in historical[-spec.same_chat_limit :]
            )
            if spec.same_chat_limit > 0
            else ()
        )
        recent_other_turns = (
            tuple(
                _other_chat_turn(row)
                for row in _unique_messages(other)[-spec.other_chat_limit :]
            )
            if spec.other_chat_limit > 0
            else ()
        )
        global_timeline_turns = (
            tuple(
                _record_turn(row, sender_origin="idle_history")
                for row in global_timeline[-spec.global_timeline_limit :]
            )
            if spec.global_timeline_limit > 0
            else ()
        )

        window = ContextWindow(
            account_id=spec.account_id,
            conversation_id=spec.conversation_id,
            is_group=spec.is_group,
            turns=turns,
            history_turns=history_turns,
            recent_other_turns=recent_other_turns,
            global_timeline_turns=global_timeline_turns,
            idle_activity_summaries=idle_summaries,
            is_idle_event=(spec.mode == "idle"),
            identities=identities,
            priority_sender_references=tuple(
                item for item in priority_sender_references if isinstance(item, Mapping)
            ),
            quick_send_images=tuple(
                item for item in quick_send_images if isinstance(item, Mapping)
            ),
            daily_prompt_context=daily,
            delivery_notices=spec.delivery_notices,
            scheduled_task=spec.scheduled_task,
            cancelled_followup=spec.cancelled_followup,
        )
        window = fit_context_window(window, max_bytes=spec.max_user_bytes)

        if quote_context is not None:
            identity = identities.get(quote_context[0])
            if identity is not None and identity.display_name:
                trusted_name_ids = {spec.account_id} | set(priority_sender_ids)
                quote_context = (
                    quote_context[0],
                    _safe_identity_name(quote_context[0], identity, trusted_name_ids),
                    quote_context[2],
                    quote_context[3],
                )

        return BuiltContext(context=window, quote_context=quote_context)

    async def _resolve_identities(
        self,
        spec: ContextSpec,
        *,
        enriched_current: Sequence[Mapping[str, object]],
        enriched_quotes: Sequence[Mapping[str, object]],
        historical: Sequence[Mapping[str, object]],
        other: Sequence[Mapping[str, object]],
        global_timeline: Sequence[Mapping[str, object]],
        priority_sender_ids: Sequence[str],
        extra_identifiers: Sequence[str],
    ) -> dict[str, ContactIdentity]:
        identifiers = {spec.account_id, spec.conversation_id}
        identifiers.update(priority_sender_ids)
        identifiers.update(extra_identifiers)
        for item in spec.observations:
            identifiers.add(item.sender_id)
            author = quoted_author_id(item.text)
            if author:
                identifiers.add(author)
            quote_ref = quoted_message_reference(item.text)
            if quote_ref:
                identifiers.add(str(quote_ref["source_conversation_id"]))
        for turn in spec.synthetic_turns:
            if turn.sender_id:
                identifiers.add(turn.sender_id)
            if turn.source_conversation_id:
                identifiers.add(turn.source_conversation_id)
        for row in (
            *enriched_current,
            *enriched_quotes,
            *historical,
            *other,
            *global_timeline,
        ):
            for key in ("sender_id", "conversation_id"):
                value = row.get(key)
                if isinstance(value, str):
                    identifiers.add(value)

        strict_wechat = (
            all(getattr(obs, "channel", "wechat") == "wechat" for obs in spec.observations)
            and (
                is_contact_id(spec.conversation_id)
                or is_room_id(spec.conversation_id)
                or (spec.mode == "idle" and is_contact_id(spec.account_id))
            )
            and not any(":" in value for value in identifiers if isinstance(value, str))
        )
        if strict_wechat:
            safe = sorted(
                value for value in identifiers if is_contact_id(value) or is_room_id(value)
            )
        else:
            safe = sorted(
                value
                for value in identifiers
                if isinstance(value, str)
                and value.strip()
                and len(value) <= 256
                and "\0" not in value
                and not value.startswith(("idle-event:", "Agent内部", "系统空闲"))
                and (is_contact_id(value) or is_room_id(value) or value != spec.account_id)
            )
        identities: dict[str, ContactIdentity] = {}
        if self.resolve_contact is not None and safe:
            results = await asyncio.gather(
                *(asyncio.to_thread(self.resolve_contact, value) for value in safe)
            )
            identities = {
                key: value
                for key, value in zip(safe, results, strict=True)
                if value is not None
            }
        if self.memory_store is not None and safe:
            remarks = await run_db(
                self.memory_store.sender_remarks,
                account_id=spec.account_id,
                sender_ids=safe,
                db=self.memory_store.db,
                lock=self.memory_store._lock,
            )
            identities = apply_sender_remarks(identities, remarks)
        return identities
