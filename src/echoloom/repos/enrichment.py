"""Batched cross-repo context enrichment over messages, media, tags, and entities."""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from datetime import datetime

from .knowledge_repo import KnowledgeRepo
from .media_repo import MediaRepo
from .message_repo import MessageRepo


def _gap_minutes(current_time: datetime, previous_at: str | None) -> int | None:
    if previous_at is None:
        return None
    previous_time = datetime.fromisoformat(previous_at)
    seconds = max(0, int((current_time - previous_time).total_seconds()))
    return seconds // 60


def enrich_context_messages(
    *,
    account_id: str,
    messages: Sequence[Mapping[str, object]],
    lock: threading.RLock,
    knowledge: KnowledgeRepo,
    messages_repo: MessageRepo,
    media: MediaRepo,
    entity_limit: int = 30,
) -> list[dict[str, object]]:
    """Attach one fresh, batched snapshot of notes, terms, and media to records."""
    if not 1 <= entity_limit <= 50 or len(messages) > 120:
        raise ValueError("context enrichment limits out of range")
    message_ids = list(
        dict.fromkeys(
            message_id
            for item in messages
            if isinstance((message_id := item.get("message_id")), str) and message_id
        )
    )
    sender_ids = list(
        dict.fromkeys(
            sender_id
            for item in messages
            if isinstance((sender_id := item.get("sender_id")), str) and sender_id
        )
    )
    sender_memories_by_id = knowledge.sender_memories(
        account_id=account_id,
        sender_ids=sender_ids,
    )
    tag_requests = [
        (
            str(item["message_id"]),
            str(item.get("sender_id", "")),
            str(item.get("captured_at", "9999-12-31T23:59:59+00:00")),
        )
        for item in messages
        if isinstance(item.get("message_id"), str)
    ]
    tags_by_message = knowledge.sender_tags_many_at(
        account_id=account_id,
        requests=tag_requests,
    )
    notes_by_message: dict[str, list[dict[str, object]]] = {}
    jev_by_message: dict[str, dict[str, object]] = {}
    images_by_message: dict[str, dict[str, object]] = {}
    audio_by_message: dict[str, dict[str, object]] = {}
    with lock:
        if message_ids:
            notes_by_message = knowledge.notes_for_messages(
                account_id=account_id,
                message_ids=message_ids,
            )
            jev_by_message = messages_repo.jev_labels_for_messages(
                account_id=account_id,
                message_ids=message_ids,
            )
            images_by_message = media.image_metadata_for_messages(
                account_id=account_id,
                message_ids=message_ids,
            )
            audio_by_message = media.audio_metadata_for_messages(
                account_id=account_id,
                message_ids=message_ids,
            )
    search_texts = []
    for item in messages:
        message_id = item.get("message_id")
        key = message_id if isinstance(message_id, str) else ""
        base_image = item.get("image")
        base_audio = item.get("audio")
        image = {
            **(dict(base_image) if isinstance(base_image, Mapping) else {}),
            **images_by_message.get(key, {}),
        }
        audio = {
            **(dict(base_audio) if isinstance(base_audio, Mapping) else {}),
            **audio_by_message.get(key, {}),
        }
        search_texts.append(
            "\n".join(
                str(value)
                for value in (
                    item.get("entity_search_text"),
                    item.get("body_text"),
                    (image or {}).get("vision_summary"),
                    (audio or {}).get("transcript"),
                )
                if value
            )[:12000]
        )
    entity_matches = knowledge.match_entities_many(
        account_id=account_id,
        texts=search_texts,
        limit=entity_limit,
    )
    gap_requests = []
    for item in messages:
        message_id = item.get("message_id")
        captured_at = item.get("captured_at")
        conversation_id = item.get("conversation_id")
        sender_id = item.get("sender_id")
        if (
            isinstance(message_id, str)
            and isinstance(captured_at, str)
            and isinstance(conversation_id, str)
            and isinstance(sender_id, str)
            and message_id
        ):
            gap_requests.append((message_id, conversation_id, sender_id, captured_at))
    gaps_by_message = messages_repo._previous_message_times_many(
        account_id=account_id,
        requests=gap_requests,
    )
    enriched: list[dict[str, object]] = []
    computed: dict[str, dict[str, object]] = {}
    for index, item in enumerate(messages):
        message_id = item.get("message_id")
        cache_key = message_id if isinstance(message_id, str) else None
        if cache_key and cache_key in computed:
            data = dict(item)
            data.update(computed[cache_key])
            enriched.append(data)
            continue
        data = dict(item)
        captured_at = item.get("captured_at")
        if isinstance(cache_key, str) and cache_key in gaps_by_message:
            prior_chat, prior_sender = gaps_by_message[cache_key]
            current_time = datetime.fromisoformat(str(captured_at))
            data["conversation_gap_minutes"] = _gap_minutes(current_time, prior_chat)
            data["sender_gap_minutes"] = _gap_minutes(current_time, prior_sender)
        else:
            data["conversation_gap_minutes"] = None
            data["sender_gap_minutes"] = None
        data.update(
            {
                "annotations": notes_by_message.get(cache_key or "", []),
                "jev_label": jev_by_message.get(cache_key or ""),
                "entities": entity_matches[index],
                "sender_memories": sender_memories_by_id.get(
                    str(item.get("sender_id", "")),
                    [],
                ),
                "sender_tags": tags_by_message.get(cache_key or "", []),
                "image": {
                    **(dict(item["image"]) if isinstance(item.get("image"), Mapping) else {}),
                    **images_by_message.get(cache_key or "", {}),
                }
                or None,
                "audio": {
                    **(dict(item["audio"]) if isinstance(item.get("audio"), Mapping) else {}),
                    **audio_by_message.get(cache_key or "", {}),
                }
                or None,
            }
        )
        if cache_key:
            computed[cache_key] = {
                key: data[key]
                for key in (
                    "annotations",
                    "jev_label",
                    "entities",
                    "sender_memories",
                    "sender_tags",
                    "conversation_gap_minutes",
                    "sender_gap_minutes",
                    "image",
                    "audio",
                )
            }
        enriched.append(data)
    return enriched
