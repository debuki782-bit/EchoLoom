"""Generic HTTP/JSON Webhook connector enabling multi-source ingestion and outbound delivery.

Allows Telegram/Discord/Feishu/OneBot/Slack bridges, RSS/event watchers, custom
apps, or local CLI tools to connect as first-class sources without any WeChat Hook.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import time
from collections import deque
from collections.abc import Mapping
from pathlib import Path
from urllib.parse import urlsplit

import httpx

from ..contacts import ContactIdentity, is_group_conversation
from ..media_artifacts import MAX_ORIGINAL_IMAGE_BYTES, ImageIngestError, _file_hash
from ..models import ConversationKey, ReplyDraft
from .base import (
    AccountState,
    GatewayConfig,
    MessageEvent,
    SendResult,
    VoicePayload,
    verify_bearer_or_hmac,
)

logger = logging.getLogger(__name__)


def _validate_outbound_url(url: str) -> str:
    cleaned = url.strip()
    if not cleaned:
        return ""
    parsed = urlsplit(cleaned)
    is_loopback = parsed.hostname in ("127.0.0.1", "localhost", "::1")
    if parsed.scheme == "https" and parsed.hostname:
        return cleaned
    if parsed.scheme == "http" and is_loopback and parsed.port is not None:
        return cleaned
    raise ValueError("Webhook outbound_url must use https:// or loopback http://")


def _safe_remote_http_url(url: object) -> str:
    if not isinstance(url, str):
        return ""
    cleaned = url.strip()
    if not cleaned or len(cleaned) > 2048:
        return ""
    parsed = urlsplit(cleaned)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return ""
    if parsed.username or parsed.password:
        return ""
    return cleaned


class WebhookConnector:
    """Platform-neutral HTTP/JSON source & egress connector."""

    capabilities = frozenset({"text", "image", "quote", "contacts", "image_fetch", "voice"})

    def __init__(self, config: GatewayConfig) -> None:
        self.config = config
        self.source_id = config.source_id or "webhook"
        self.channel = config.channel or "webhook"
        self.default_account_id = config.account_id or "bot_local"
        self.allowed: set[ConversationKey] = set(config.allowed_conversations)
        self.outbound_url = _validate_outbound_url(config.outbound_url)
        self.auth_token = config.auth_token.strip()
        self.hmac_secret = config.hmac_secret.strip()
        self.transport = config.transport
        self.contacts: dict[str, ContactIdentity] = {}
        self.images: dict[str, tuple[bytes, str]] = {}
        self.voices: dict[str, VoicePayload] = {}
        self.outbound: deque[dict[str, object]] = deque(maxlen=256)
        self.closed = False

    def verify_request(self, headers: Mapping[str, str], raw_body: bytes) -> bool:
        return verify_bearer_or_hmac(
            headers,
            raw_body,
            auth_token=self.auth_token,
            hmac_secret=self.hmac_secret,
        )

    def _register_contact(self, user_id: str, display_name: object) -> None:
        if not user_id or not isinstance(display_name, str):
            return
        cleaned = display_name.strip()
        if not cleaned or len(cleaned) > 120:
            return
        self.contacts[user_id] = ContactIdentity(
            display_name=cleaned,
            match_source="source_payload",
            user_name=user_id,
            nickname=cleaned,
        )

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            raw_items = payload["events"]
        elif isinstance(payload, dict):
            raw_items = [payload]
        else:
            raise ValueError("webhook payload must be an object or contain an events list")
        if not 1 <= len(raw_items) <= 64:
            raise ValueError("webhook events count must be between 1 and 64")

        envelope_json = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        events: list[MessageEvent] = []
        for idx, row in enumerate(raw_items):
            if not isinstance(row, dict):
                raise ValueError("webhook event entry must be an object")
            account_id = str(row.get("account_id") or self.default_account_id).strip()
            conversation_id = str(row.get("conversation_id") or "").strip()
            sender_id = str(row.get("sender_id") or "").strip()
            if not account_id or not conversation_id or not sender_id:
                raise ValueError("webhook event requires account_id, conversation_id, and sender_id")
            if any("\0" in v for v in (account_id, conversation_id, sender_id)):
                raise ValueError("invalid null byte in event routing fields")

            channel = str(row.get("channel") or self.channel).strip() or self.channel
            raw_is_group = row.get("is_group")
            is_group = is_group_conversation(
                conversation_id,
                is_group=raw_is_group if isinstance(raw_is_group, bool) else None,
            )
            self._register_contact(sender_id, row.get("sender_name"))
            if is_group:
                self._register_contact(conversation_id, row.get("conversation_name"))

            kind = str(row.get("kind") or row.get("type") or "text").strip().casefold()
            tick_val = row.get("timestamp_ms", row.get("capture_tick_ms"))
            if isinstance(tick_val, int) and not isinstance(tick_val, bool) and tick_val >= 0:
                capture_tick_ms = tick_val
            else:
                capture_tick_ms = time.monotonic_ns() // 1_000_000 + idx

            raw_msg_id = row.get("source_msg_id", row.get("message_id"))
            source_msg_id: int | None = None
            if isinstance(raw_msg_id, int) and not isinstance(raw_msg_id, bool) and raw_msg_id > 0:
                source_msg_id = raw_msg_id

            original_type = 1
            text = ""
            if kind in ("text", "message"):
                text = str(row.get("text") if row.get("text") is not None else row.get("content") or "")
                original_type = 1
            elif kind in ("image", "sticker"):
                original_type = 47 if kind == "sticker" else 3
                image_key = str(row.get("image_id") or hashlib.sha256(
                    f"{account_id}:{conversation_id}:{sender_id}:{capture_tick_ms}:{idx}".encode()
                ).hexdigest()[:16])
                refs: list[dict[str, str]] = [{"field": "webhook_image_id", "value": image_key}]
                b64 = row.get("image_base64")
                if isinstance(b64, str) and b64:
                    raw_img = base64.b64decode(b64, validate=True)
                    if len(raw_img) > MAX_ORIGINAL_IMAGE_BYTES:
                        raise ValueError("webhook image exceeds size limit")
                    self.images[image_key] = (raw_img, str(row.get("mime_type") or ""))
                    refs.append({"field": "md5", "value": hashlib.md5(raw_img).hexdigest()})
                img_url = _safe_remote_http_url(row.get("image_url"))
                if img_url:
                    refs.append({"field": "cdnurl", "value": img_url})
                body_obj: dict[str, object] = {"kind": "image_message", "image_references": refs}
                if kind == "sticker":
                    body_obj["media_kind"] = "sticker"
                text = json.dumps(body_obj, ensure_ascii=False)
            elif kind in ("voice", "audio"):
                original_type = 34
                voice_key = f"{conversation_id}:{sender_id}:{capture_tick_ms}"
                audio_b64 = row.get("audio_base64")
                if isinstance(audio_b64, str) and audio_b64:
                    audio_bytes = base64.b64decode(audio_b64, validate=True)
                    fmt = str(row.get("audio_format") or "wav").strip().lower()
                    dur = row.get("duration_ms")
                    self.voices[voice_key] = VoicePayload(
                        data=audio_bytes,
                        duration_ms=dur if isinstance(dur, int) and dur >= 0 else None,
                        format=fmt,
                    )
                    self.voices[conversation_id] = self.voices[voice_key]
                text = json.dumps({"kind": "voice_message"}, ensure_ascii=False)
            elif kind in ("link", "card", "app_message"):
                original_type = 49
                card: dict[str, object] = {
                    "kind": "app_message",
                    "text": str(row.get("title") or row.get("text") or "")[:8192],
                }
                url = _safe_remote_http_url(row.get("url"))
                if url:
                    card["url"] = url
                desc = str(row.get("description") or "").strip()
                if desc:
                    card["description"] = desc[:8192]
                text = json.dumps(card, ensure_ascii=False)
            elif kind in ("quote", "reply"):
                original_type = 49
                quote_raw = row.get("quote") if isinstance(row.get("quote"), Mapping) else {}
                text = json.dumps(
                    {
                        "kind": "quoted_message",
                        "text": str(row.get("text") or "")[:8192],
                        "quote": {
                            "type": str(quote_raw.get("type") or "1")[:32],
                            "platform_message_id": str(quote_raw.get("message_id") or "")[:64],
                            "source_conversation_id": str(
                                quote_raw.get("conversation_id") or conversation_id
                            )[:512],
                            "author_id": str(quote_raw.get("author_id") or "")[:512],
                            "display_name_hint": str(quote_raw.get("author_name") or "")[:256],
                            "content": str(quote_raw.get("text") or quote_raw.get("content") or "")[:8192],
                        },
                    },
                    ensure_ascii=False,
                )
            elif kind == "pat":
                original_type = 10002
                target_id = str(row.get("target_id") or account_id).strip()
                text = json.dumps(
                    {
                        "kind": "pat_message",
                        "actor_id": sender_id,
                        "target_id": target_id,
                        "suffix": str(row.get("suffix") or "")[:256],
                    },
                    ensure_ascii=False,
                )
            else:
                raise ValueError(f"unsupported webhook event kind: {kind!r}")

            raw_msg_json = json.dumps(
                row, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            sender_origin = "channel_group_member" if is_group else "webhook_sender"
            events.append(
                MessageEvent(
                    account_id=account_id,
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    sender_origin=sender_origin,
                    text=text,
                    is_group=is_group,
                    message_type=1,
                    source_msg_id=source_msg_id,
                    capture_tick_ms=capture_tick_ms,
                    raw_envelope_json=envelope_json,
                    raw_message_json=raw_msg_json,
                    original_message_type=original_type,
                    source_id=self.source_id,
                    channel=channel,
                    content_kind=kind,
                )
            )
        return tuple(events)

    def normalize_event(self, item: MessageEvent) -> MessageEvent | None:
        return item

    def in_scope(
        self,
        item: MessageEvent,
        *,
        account: str,
        default_room: str = "",
        registered_group_ids: set[str] | frozenset[str] = frozenset(),
    ) -> bool:
        del default_room, registered_group_ids
        if item.account_id != account and item.account_id != self.default_account_id:
            return False
        return bool(item.conversation_id and item.sender_id)

    def archive_envelope(self, item: MessageEvent, *, preserve_raw: bool = False) -> dict:
        if preserve_raw and item.raw_envelope_json:
            return json.loads(item.raw_envelope_json)
        return {
            "events": [
                {
                    "source_id": item.source_id,
                    "channel": item.channel,
                    "account_id": item.account_id,
                    "conversation_id": item.conversation_id,
                    "sender_id": item.sender_id,
                    "is_group": item.is_group,
                    "kind": item.content_kind,
                    "text": item.text,
                    "source_msg_id": item.source_msg_id,
                    "capture_tick_ms": item.capture_tick_ms,
                }
            ]
        }

    def account_state(
        self,
        account_id: str,
        conversation_id: str = "",
        *,
        timeout_seconds: float = 2.0,
    ) -> AccountState:
        del conversation_id, timeout_seconds
        matches = not self.default_account_id or account_id == self.default_account_id
        return AccountState(logged_in=not self.closed, account_matches=matches)

    def ensure_ready(self, account_id: str) -> bool:
        del account_id
        return not self.closed

    def resolve_contact(self, user_id: str) -> ContactIdentity | None:
        return self.contacts.get(user_id)

    def allow_conversation(self, key: ConversationKey) -> None:
        self.allowed.add(key)

    def _can_send(self, account_id: str, conversation_id: str) -> bool:
        return not self.closed and self.config.send_enabled and bool(account_id and conversation_id)

    def _post_outbound(self, body: dict[str, object]) -> tuple[str, str]:
        self.outbound.append(body)
        if not self.outbound_url:
            return "queued", "webhook_recorded_locally_no_outbound_url"
        encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        if self.hmac_secret:
            sig = hmac.new(self.hmac_secret.encode("utf-8"), encoded, hashlib.sha256).hexdigest()
            headers["X-Signature-256"] = f"sha256={sig}"
        try:
            with httpx.Client(
                timeout=5.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                resp = client.post(self.outbound_url, content=encoded, headers=headers)
            if 200 <= resp.status_code < 300:
                return "accepted_by_server", "webhook_outbound_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"webhook_http_rejected_{resp.status_code}"
            return "unknown", f"webhook_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "webhook_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "webhook_transport_error_delivery_may_have_occurred"

    def send_text(self, draft: ReplyDraft) -> SendResult:
        if not self._can_send(draft.key.account_id, draft.key.conversation_id):
            return SendResult("rejected", "sender_disabled_or_destination_not_allowed")
        if not draft.text or len(draft.text.encode("utf-8")) > 16384:
            return SendResult("rejected", "empty_or_oversized_text")
        status, reason = self._post_outbound(
            {
                "kind": "text",
                "source_id": self.source_id,
                "channel": self.channel,
                "account_id": draft.key.account_id,
                "conversation_id": draft.key.conversation_id,
                "text": draft.text,
                "event_id": draft.event_id,
            }
        )
        mapped = "queued" if status == "accepted_by_server" else status
        return SendResult(mapped, reason)

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        raw = Path(path).read_bytes()
        status, reason = self._post_outbound(
            {
                "kind": "image",
                "source_id": self.source_id,
                "channel": self.channel,
                "account_id": self.default_account_id,
                "conversation_id": conversation_id,
                "filename": Path(path).name,
                "sha256": hashlib.sha256(raw).hexdigest(),
                "image_base64": base64.b64encode(raw).decode("ascii"),
            }
        )
        return ("queued" if status == "accepted_by_server" else status), reason

    def send_quote(
        self,
        conversation_id: str,
        text: str,
        quote: tuple[str, str, str, str],
    ) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        author_id, author_name, source_room, source_text = quote
        return self._post_outbound(
            {
                "kind": "quote",
                "source_id": self.source_id,
                "channel": self.channel,
                "account_id": self.default_account_id,
                "conversation_id": conversation_id,
                "text": text,
                "quote": {
                    "author_id": author_id,
                    "author_name": author_name,
                    "source_conversation_id": source_room,
                    "text": source_text,
                },
            }
        )

    def decode_image(self, source: Path, destination: Path) -> None:
        raise ValueError("webhook connector accepts standard decoded image bytes only")

    def fetch_original_image(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]:
        del item, allow_account_scan
        for ref in references:
            if ref.get("field") == "webhook_image_id" and ref.get("value") in self.images:
                payload, mime = self.images[ref["value"]]
                destination.write_bytes(payload)
                return _file_hash(destination)[0], mime
        remote = next(
            (ref["value"] for ref in references if ref.get("field") == "cdnurl" and ref.get("value")),
            "",
        )
        safe = _safe_remote_http_url(remote)
        if not safe:
            raise ImageIngestError("original_image_not_found_in_cache")
        with httpx.Client(timeout=10.0, trust_env=False, transport=self.transport) as client:
            resp = client.get(safe)
            resp.raise_for_status()
            if len(resp.content) > MAX_ORIGINAL_IMAGE_BYTES:
                raise ImageIngestError("original_image_exceeds_limit")
            destination.write_bytes(resp.content)
            return _file_hash(destination)[0], resp.headers.get("content-type", "").split(";")[0]

    def fetch_thumbnail(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, str]:
        del item, references, destination
        raise ImageIngestError("original_image_not_found_in_cache")

    def voice_payload(self, item: MessageEvent) -> VoicePayload:
        key = f"{item.conversation_id}:{item.sender_id}:{item.capture_tick_ms}"
        if key in self.voices:
            return self.voices[key]
        if item.conversation_id in self.voices:
            return self.voices[item.conversation_id]
        return VoicePayload(error_code="voice_payload_unavailable")

    def close(self) -> None:
        self.closed = True
