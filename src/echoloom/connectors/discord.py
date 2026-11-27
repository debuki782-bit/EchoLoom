"""Discord Bot REST / webhook bridge channel connector."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

import httpx

from ..models import ReplyDraft
from .base import ConnectorConfig, MessageEvent, SendResult
from .webhook import WebhookConnector, _safe_remote_http_url, _validate_outbound_url

_IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".webp", ".gif")
_AUDIO_SUFFIXES = (".ogg", ".opus", ".mp3", ".wav", ".m4a")


def _parse_iso_tick_ms(value: object, fallback_offset: int) -> int:
    if isinstance(value, str) and value.strip():
        try:
            dt = datetime.fromisoformat(value.strip())
            return int(dt.timestamp() * 1000)
        except ValueError:
            pass
    return time.monotonic_ns() // 1_000_000 + fallback_offset


class DiscordConnector(WebhookConnector):
    """Out-of-the-box Discord Bot REST + normalized Gateway event connector."""

    capabilities = frozenset({"text", "image", "quote", "voice"})

    def __init__(self, config: ConnectorConfig) -> None:
        super().__init__(config)
        self.channel = config.channel or "discord"
        if not self.default_account_id:
            self.default_account_id = (
                str(config.options.get("bot_id") or "").strip() or "discord-bot"
            )
        api_base = str(
            config.outbound_url
            or config.options.get("api_base")
            or "https://discord.com/api/v10"
        ).rstrip("/")
        self.api_base = _validate_outbound_url(api_base)
        self.bot_token = str(config.options.get("bot_token") or config.auth_token or "").strip()

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return super().parse_events(payload)
        if not isinstance(payload, Mapping):
            raise TypeError("discord payload must be an object")

        raw_d = payload.get("d")
        raw_msg = raw_d if isinstance(raw_d, Mapping) else payload
        if not isinstance(raw_msg, Mapping):
            raise TypeError("discord payload missing message object")

        conversation_id = str(raw_msg.get("channel_id") or "").strip()
        raw_author = raw_msg.get("author")
        author: Mapping[str, Any] = raw_author if isinstance(raw_author, Mapping) else {}
        sender_id = str(author.get("id") or "").strip()
        if not conversation_id or not sender_id:
            raise ValueError("discord message requires channel_id and author.id")

        guild_id = raw_msg.get("guild_id")
        is_group = bool(guild_id) or raw_msg.get("channel_type") in (1, 3)
        display_name = str(
            author.get("global_name") or author.get("username") or ""
        ).strip()
        self._register_contact(sender_id, display_name or None)
        capture_tick_ms = _parse_iso_tick_ms(raw_msg.get("timestamp"), 0)

        source_msg_id: int | None = None
        raw_id = str(raw_msg.get("id") or "").strip()
        if raw_id.isdigit():
            parsed_id = int(raw_id)
            if 0 < parsed_id <= (2**63 - 1):
                source_msg_id = parsed_id

        attachments = raw_msg.get("attachments")
        ref_msg = (
            raw_msg.get("referenced_message")
            if isinstance(raw_msg.get("referenced_message"), Mapping)
            else None
        )
        msg_ref = (
            raw_msg.get("message_reference")
            if isinstance(raw_msg.get("message_reference"), Mapping)
            else None
        )

        text = str(raw_msg.get("content") or "")
        content_kind = "text"

        if isinstance(attachments, list) and attachments:
            first_att = attachments[0] if isinstance(attachments[0], Mapping) else {}
            ctype = str(first_att.get("content_type") or "").casefold()
            fname = str(first_att.get("filename") or "").casefold()
            att_url = _safe_remote_http_url(first_att.get("url"))
            if ctype.startswith("image/") or fname.endswith(_IMAGE_SUFFIXES):
                content_kind = "image"
                image_key = str(first_att.get("id") or hashlib.sha256(
                    f"dc:{conversation_id}:{sender_id}:{capture_tick_ms}".encode()
                ).hexdigest()[:16])
                refs: list[dict[str, str]] = [{"field": "webhook_image_id", "value": image_key}]
                if att_url:
                    refs.append({"field": "cdnurl", "value": att_url})
                text = json.dumps(
                    {"kind": "image_message", "image_references": refs},
                    ensure_ascii=False,
                )
            elif ctype.startswith("audio/") or fname.endswith(_AUDIO_SUFFIXES):
                content_kind = "voice"
                text = json.dumps({"kind": "voice_message"}, ensure_ascii=False)

        if content_kind == "text" and (ref_msg is not None or msg_ref is not None):
            content_kind = "quote"
            ref_author = (
                ref_msg.get("author")
                if ref_msg is not None and isinstance(ref_msg.get("author"), Mapping)
                else {}
            )
            text = json.dumps(
                {
                    "kind": "quoted_message",
                    "text": text[:8192],
                    "quote": {
                        "type": "1",
                        "platform_message_id": str(
                            (ref_msg or msg_ref or {}).get("id")
                            or (msg_ref or {}).get("message_id")
                            or ""
                        )[:64],
                        "source_conversation_id": conversation_id[:512],
                        "author_id": str(ref_author.get("id") or "")[:512],
                        "display_name_hint": str(
                            ref_author.get("global_name") or ref_author.get("username") or ""
                        )[:256],
                        "content": str((ref_msg or {}).get("content") or "")[:8192],
                    },
                },
                ensure_ascii=False,
            )

        envelope_json = json.dumps(
            dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        return (
            MessageEvent(
                account_id=self.default_account_id,
                conversation_id=conversation_id,
                sender_id=sender_id,
                sender_origin="channel_group_member" if is_group else "discord_user",
                text=text,
                is_group=is_group,
                message_type=1,
                source_msg_id=source_msg_id,
                capture_tick_ms=capture_tick_ms,
                raw_envelope_json=envelope_json,
                raw_message_json=json.dumps(
                    dict(raw_msg), ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
                source_id=self.source_id,
                channel=self.channel,
                content_kind=content_kind,
            ),
        )

    def _post_channel_message(
        self,
        conversation_id: str,
        payload: dict[str, object],
    ) -> tuple[Literal["rejected", "queued", "unknown"], str]:
        self.outbound.append({"channel_id": conversation_id, **payload})
        if not self.bot_token and not self.outbound_url:
            return "rejected", "missing_discord_bot_token"
        headers = {"Content-Type": "application/json"}
        if self.bot_token:
            headers["Authorization"] = f"Bot {self.bot_token}"
        try:
            with httpx.Client(
                timeout=10.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                resp = client.post(
                    f"{self.api_base}/channels/{conversation_id}/messages",
                    json=payload,
                    headers=headers,
                )
            if 200 <= resp.status_code < 300:
                return "queued", "discord_outbound_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"discord_http_rejected_{resp.status_code}"
            return "unknown", f"discord_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "discord_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "discord_transport_error_delivery_may_have_occurred"

    def send_text(self, draft: ReplyDraft) -> SendResult:
        if not self._can_send(draft.key.account_id, draft.key.conversation_id):
            return SendResult("rejected", "sender_disabled_or_destination_not_allowed")
        if not draft.text or len(draft.text) > 2000:
            return SendResult("rejected", "empty_or_oversized_text")
        status, reason = self._post_channel_message(
            draft.key.conversation_id, {"content": draft.text},
        )
        return SendResult(status, reason)

    def send_quote(
        self,
        conversation_id: str,
        text: str,
        quote: tuple[str, str, str, str],
    ) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        author_id, author_name, _source_room, source_text = quote
        label = author_name or author_id
        preview = source_text.strip().replace("\n", " ")[:120]
        formatted = f"> {label}: {preview}\n{text}" if preview else text
        return self._post_channel_message(conversation_id, {"content": formatted[:2000]})

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        if not self.bot_token and not self.outbound_url:
            return "rejected", "missing_discord_bot_token"
        raw = Path(path).read_bytes()
        self.outbound.append(
            {
                "channel_id": conversation_id,
                "filename": Path(path).name,
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        headers = {"Authorization": f"Bot {self.bot_token}"} if self.bot_token else {}
        try:
            with httpx.Client(
                timeout=15.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                resp = client.post(
                    f"{self.api_base}/channels/{conversation_id}/messages",
                    data={"payload_json": json.dumps({})},
                    files={"files[0]": (Path(path).name, raw)},
                    headers=headers,
                )
            if 200 <= resp.status_code < 300:
                return "queued", "discord_image_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"discord_http_rejected_{resp.status_code}"
            return "unknown", f"discord_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "discord_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "discord_transport_error_delivery_may_have_occurred"
