"""Telegram Bot API channel connector."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from collections.abc import Mapping
from pathlib import Path

import httpx

from ..models import ReplyDraft
from .base import ConnectorConfig, MessageEvent, SendResult, verify_bearer_or_hmac
from .webhook import (
    MAX_ORIGINAL_IMAGE_BYTES,
    ImageIngestError,
    WebhookConnector,
    _file_hash,
    _safe_remote_http_url,
    _validate_outbound_url,
)


class TelegramConnector(WebhookConnector):
    """Out-of-the-box Telegram Bot API webhook + REST outbound connector."""

    capabilities = frozenset({"text", "image", "quote", "voice"})

    def __init__(self, config: ConnectorConfig) -> None:
        super().__init__(config)
        self.channel = config.channel or "telegram"
        if not self.default_account_id:
            self.default_account_id = (
                str(config.options.get("bot_id") or "").strip() or "telegram-bot"
            )
        api_base = str(
            config.outbound_url
            or config.options.get("api_base")
            or "https://api.telegram.org"
        ).rstrip("/")
        self.api_base = _validate_outbound_url(api_base)
        self.bot_token = str(config.options.get("bot_token") or config.auth_token or "").strip()
        self.secret_token = str(
            config.options.get("secret_token") or config.hmac_secret or ""
        ).strip()
        self.parse_mode = str(config.options.get("parse_mode") or "").strip()
        self._photo_file_ids: dict[str, str] = {}

    def verify_request(self, headers: Mapping[str, str], raw_body: bytes) -> bool:
        norm = {k.casefold(): v for k, v in headers.items()}
        if self.secret_token:
            tg_header = norm.get("x-telegram-bot-api-secret-token", "").strip()
            if tg_header:
                return hmac.compare_digest(
                    tg_header.encode("utf-8"),
                    self.secret_token.encode("utf-8"),
                )
            if self.auth_token or self.hmac_secret:
                return verify_bearer_or_hmac(
                    headers,
                    raw_body,
                    auth_token=self.auth_token,
                    hmac_secret=self.hmac_secret,
                )
            return False
        return verify_bearer_or_hmac(
            headers,
            raw_body,
            auth_token=self.auth_token,
            hmac_secret=self.hmac_secret,
        )

    def _bot_url(self, method: str) -> str:
        if self.bot_token:
            return f"{self.api_base}/bot{self.bot_token}/{method}"
        return f"{self.api_base}/{method}"

    def _file_url(self, file_path: str) -> str:
        clean = file_path.lstrip("/")
        if self.bot_token:
            return f"{self.api_base}/file/bot{self.bot_token}/{clean}"
        return f"{self.api_base}/file/{clean}"

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return super().parse_events(payload)
        if isinstance(payload, dict) and isinstance(payload.get("result"), list):
            raw_updates = payload["result"]
        elif isinstance(payload, dict):
            raw_updates = [payload]
        else:
            raise TypeError("telegram payload must be an Update object or result list")
        if not 1 <= len(raw_updates) <= 64:
            raise ValueError("telegram updates count must be between 1 and 64")

        envelope_json = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        )
        events: list[MessageEvent] = []
        for idx, update in enumerate(raw_updates):
            if not isinstance(update, Mapping):
                raise TypeError("telegram update entry must be an object")
            msg = (
                update.get("message")
                or update.get("edited_message")
                or update.get("channel_post")
                or update
            )
            if not isinstance(msg, Mapping):
                raise TypeError("telegram update missing message object")
            chat = msg.get("chat")
            if not isinstance(chat, Mapping) or chat.get("id") is None:
                raise ValueError("telegram message requires chat.id")
            conversation_id = str(chat["id"]).strip()
            chat_type = str(chat.get("type") or "private").strip().casefold()
            is_group = chat_type in ("group", "supergroup", "channel")

            sender = msg.get("from") or msg.get("sender_chat") or {}
            if not isinstance(sender, Mapping):
                sender = {}
            sender_id = str(sender.get("id") or conversation_id).strip()
            full_name = " ".join(
                part
                for part in (
                    str(sender.get("first_name") or "").strip(),
                    str(sender.get("last_name") or "").strip(),
                )
                if part
            ) or str(sender.get("username") or sender.get("title") or "").strip()
            self._register_contact(sender_id, full_name or None)
            if is_group and chat.get("title"):
                self._register_contact(conversation_id, str(chat.get("title")))

            raw_date = msg.get("date")
            if isinstance(raw_date, int) and not isinstance(raw_date, bool) and raw_date >= 0:
                capture_tick_ms = raw_date * 1000 + idx
            else:
                capture_tick_ms = time.monotonic_ns() // 1_000_000 + idx

            raw_msg_id = msg.get("message_id")
            source_msg_id = (
                raw_msg_id
                if isinstance(raw_msg_id, int) and not isinstance(raw_msg_id, bool) and raw_msg_id > 0
                else None
            )

            photos = msg.get("photo")
            voice = msg.get("voice") or msg.get("audio")
            reply_to = msg.get("reply_to_message")
            original_type = 1
            content_kind = "text"
            text = str(msg.get("text") or msg.get("caption") or "")

            if isinstance(photos, list) and photos:
                original_type = 3
                content_kind = "image"
                largest = photos[-1] if isinstance(photos[-1], Mapping) else {}
                file_id = str(largest.get("file_id") or "").strip()
                image_key = str(largest.get("file_unique_id") or file_id or hashlib.sha256(
                    f"tg:{conversation_id}:{sender_id}:{capture_tick_ms}:{idx}".encode()
                ).hexdigest()[:16])
                if file_id:
                    self._photo_file_ids[image_key] = file_id
                refs: list[dict[str, str]] = [
                    {"field": "webhook_image_id", "value": image_key},
                ]
                if file_id:
                    refs.append({"field": "telegram_file_id", "value": file_id})
                text = json.dumps(
                    {"kind": "image_message", "image_references": refs},
                    ensure_ascii=False,
                )
            elif isinstance(voice, Mapping):
                original_type = 34
                content_kind = "voice"
                text = json.dumps({"kind": "voice_message"}, ensure_ascii=False)
            elif isinstance(reply_to, Mapping):
                original_type = 49
                content_kind = "quote"
                ref_sender = reply_to.get("from") or reply_to.get("sender_chat") or {}
                ref_sender_id = (
                    str(ref_sender.get("id") or "") if isinstance(ref_sender, Mapping) else ""
                )
                ref_name = (
                    str(
                        ref_sender.get("username")
                        or ref_sender.get("first_name")
                        or ref_sender.get("title")
                        or ""
                    )
                    if isinstance(ref_sender, Mapping)
                    else ""
                )
                text = json.dumps(
                    {
                        "kind": "quoted_message",
                        "text": text[:8192],
                        "quote": {
                            "type": "1",
                            "platform_message_id": str(reply_to.get("message_id") or "")[:64],
                            "source_conversation_id": conversation_id[:512],
                            "author_id": ref_sender_id[:512],
                            "display_name_hint": ref_name[:256],
                            "content": str(
                                reply_to.get("text") or reply_to.get("caption") or ""
                            )[:8192],
                        },
                    },
                    ensure_ascii=False,
                )

            events.append(
                MessageEvent(
                    account_id=self.default_account_id,
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    sender_origin="channel_group_member" if is_group else "telegram_user",
                    text=text,
                    is_group=is_group,
                    message_type=1,
                    source_msg_id=source_msg_id,
                    capture_tick_ms=capture_tick_ms,
                    raw_envelope_json=envelope_json,
                    raw_message_json=json.dumps(
                        dict(msg), ensure_ascii=False, sort_keys=True, separators=(",", ":")
                    ),
                    original_message_type=original_type,
                    source_id=self.source_id,
                    channel=self.channel,
                    content_kind=content_kind,
                )
            )
        return tuple(events)

    def _post_telegram_json(self, method: str, payload: dict[str, object]) -> tuple[str, str]:
        self.outbound.append({"method": method, **payload})
        if not self.bot_token and not self.outbound_url:
            return "rejected", "missing_telegram_bot_token"
        try:
            with httpx.Client(
                timeout=10.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                resp = client.post(self._bot_url(method), json=payload)
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                    if isinstance(data, dict) and data.get("ok") is False:
                        return "rejected", f"telegram_api_rejected_{data.get('error_code', 400)}"
                except ValueError:
                    pass
                return "queued", "telegram_outbound_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"telegram_http_rejected_{resp.status_code}"
            return "unknown", f"telegram_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "telegram_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "telegram_transport_error_delivery_may_have_occurred"

    def send_text(self, draft: ReplyDraft) -> SendResult:
        if not self._can_send(draft.key.account_id, draft.key.conversation_id):
            return SendResult("rejected", "sender_disabled_or_destination_not_allowed")
        if not draft.text or len(draft.text) > 4096:
            return SendResult("rejected", "empty_or_oversized_text")
        payload: dict[str, object] = {
            "chat_id": draft.key.conversation_id,
            "text": draft.text,
        }
        if self.parse_mode:
            payload["parse_mode"] = self.parse_mode
        status, reason = self._post_telegram_json("sendMessage", payload)
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
        formatted = f"> {label}: {preview}\n\n{text}" if preview else text
        payload: dict[str, object] = {"chat_id": conversation_id, "text": formatted}
        if self.parse_mode:
            payload["parse_mode"] = self.parse_mode
        return self._post_telegram_json("sendMessage", payload)

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        if not self.bot_token and not self.outbound_url:
            return "rejected", "missing_telegram_bot_token"
        raw = Path(path).read_bytes()
        self.outbound.append(
            {
                "method": "sendPhoto",
                "chat_id": conversation_id,
                "filename": Path(path).name,
                "sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        try:
            with httpx.Client(
                timeout=15.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                resp = client.post(
                    self._bot_url("sendPhoto"),
                    data={"chat_id": conversation_id},
                    files={"photo": (Path(path).name, raw)},
                )
            if 200 <= resp.status_code < 300:
                return "queued", "telegram_photo_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"telegram_http_rejected_{resp.status_code}"
            return "unknown", f"telegram_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "telegram_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "telegram_transport_error_delivery_may_have_occurred"

    def fetch_original_image(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]:
        file_id = next(
            (ref["value"] for ref in references if ref.get("field") == "telegram_file_id" and ref.get("value")),
            "",
        )
        if not file_id or not self.bot_token:
            return super().fetch_original_image(
                item, references, destination, allow_account_scan=allow_account_scan,
            )
        with httpx.Client(timeout=10.0, trust_env=False, transport=self.transport) as client:
            meta = client.post(self._bot_url("getFile"), json={"file_id": file_id})
            meta.raise_for_status()
            file_path = str((meta.json().get("result") or {}).get("file_path") or "")
            if not file_path:
                raise ImageIngestError("original_image_not_found_in_cache")
            download_url = _safe_remote_http_url(self._file_url(file_path))
            if not download_url:
                raise ImageIngestError("original_image_not_found_in_cache")
            resp = client.get(download_url)
            resp.raise_for_status()
            if len(resp.content) > MAX_ORIGINAL_IMAGE_BYTES:
                raise ImageIngestError("original_image_exceeds_limit")
            destination.write_bytes(resp.content)
            return _file_hash(destination)[0], resp.headers.get("content-type", "").split(";")[0]
