"""Feishu / Lark Open Platform channel connector."""

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
    _validate_outbound_url,
)


def _feishu_post_to_text(content: Mapping[str, object]) -> str:
    """Flatten Feishu rich-text `post` locale blocks to readable plain text."""
    lines: list[str] = []
    block = content
    if "title" not in content and "content" not in content:
        first = next((v for v in content.values() if isinstance(v, Mapping)), None)
        if first is not None:
            block = first
    title = str(block.get("title") or "").strip()
    if title:
        lines.append(title)
    paragraphs = block.get("content")
    if isinstance(paragraphs, list):
        for row in paragraphs:
            if not isinstance(row, list):
                continue
            parts: list[str] = []
            for elem in row:
                if not isinstance(elem, Mapping):
                    continue
                tag = str(elem.get("tag") or "")
                if tag in ("text", "a"):
                    parts.append(str(elem.get("text") or ""))
                elif tag == "at":
                    parts.append(f"@{elem.get('user_name') or elem.get('user_id') or ''}")
            joined = "".join(parts).strip()
            if joined:
                lines.append(joined)
    return "\n".join(lines)


class FeishuConnector(WebhookConnector):
    """Out-of-the-box Feishu / Lark event v2 + OpenAPI REST connector."""

    capabilities = frozenset({"text", "image", "quote"})

    def __init__(self, config: ConnectorConfig) -> None:
        super().__init__(config)
        self.channel = config.channel or "feishu"
        if not self.default_account_id:
            self.default_account_id = (
                str(config.options.get("app_id") or "").strip() or "feishu-bot"
            )
        api_base = str(
            config.outbound_url
            or config.options.get("api_base")
            or "https://open.feishu.cn"
        ).rstrip("/")
        self.api_base = _validate_outbound_url(api_base)
        self.app_id = str(config.options.get("app_id") or "").strip()
        self.app_secret = str(config.options.get("app_secret") or "").strip()
        self.verification_token = str(config.options.get("verification_token") or "").strip()
        self.encrypt_key = str(
            config.options.get("encrypt_key") or config.hmac_secret or ""
        ).strip()
        self._static_tenant_token = str(config.auth_token or "").strip()
        self._cached_token = ""
        self._token_expires_at = 0.0

    def verify_request(self, headers: Mapping[str, str], raw_body: bytes) -> bool:
        norm = {k.casefold(): v for k, v in headers.items()}
        if self.encrypt_key:
            lark_sig = norm.get("x-lark-signature", "").strip()
            if not lark_sig:
                return False
            ts = norm.get("x-lark-request-timestamp", "")
            nonce = norm.get("x-lark-request-nonce", "")
            digest = hashlib.sha256(
                f"{ts}{nonce}{self.encrypt_key}".encode() + raw_body
            ).hexdigest()
            if not hmac.compare_digest(lark_sig.lower().encode(), digest.encode()):
                return False
        if self.verification_token:
            try:
                parsed = json.loads(raw_body)
            except ValueError:
                return False
            token = str(
                (parsed.get("header") or {}).get("token")
                or parsed.get("token")
                or ""
            )
            if not hmac.compare_digest(token.encode(), self.verification_token.encode()):
                return False
        if self.auth_token:
            return verify_bearer_or_hmac(headers, raw_body, auth_token=self.auth_token)
        return True

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        if isinstance(payload, dict) and isinstance(payload.get("events"), list):
            return super().parse_events(payload)
        if not isinstance(payload, Mapping):
            raise TypeError("feishu payload must be an object")
        if payload.get("type") == "url_verification":
            return ()

        header = payload.get("header") if isinstance(payload.get("header"), Mapping) else {}
        event_type = str(header.get("event_type") or "")
        if event_type and event_type != "im.message.receive_v1":
            return ()
        event = payload.get("event") if isinstance(payload.get("event"), Mapping) else payload
        msg = event.get("message") if isinstance(event.get("message"), Mapping) else event
        if not isinstance(msg, Mapping):
            raise TypeError("feishu payload missing message object")

        conversation_id = str(msg.get("chat_id") or "").strip()
        sender_obj = event.get("sender") if isinstance(event.get("sender"), Mapping) else {}
        sender_ids = (
            sender_obj.get("sender_id")
            if isinstance(sender_obj.get("sender_id"), Mapping)
            else {}
        )
        sender_id = str(
            sender_ids.get("open_id")
            or sender_ids.get("user_id")
            or sender_ids.get("union_id")
            or event.get("sender_id")
            or ""
        ).strip()
        if not conversation_id or not sender_id:
            raise ValueError("feishu message requires chat_id and sender_id")

        chat_type = str(msg.get("chat_type") or "p2p").strip().casefold()
        is_group = chat_type == "group"
        self._register_contact(sender_id, str(sender_ids.get("user_id") or ""))

        raw_ct = msg.get("create_time")
        try:
            capture_tick_ms = int(str(raw_ct))
        except (TypeError, ValueError):
            capture_tick_ms = time.monotonic_ns() // 1_000_000

        raw_content = msg.get("content")
        parsed_content: dict[str, object] = {}
        if isinstance(raw_content, str) and raw_content.strip():
            try:
                loaded = json.loads(raw_content)
                if isinstance(loaded, dict):
                    parsed_content = loaded
            except ValueError:
                parsed_content = {"text": raw_content}
        elif isinstance(raw_content, Mapping):
            parsed_content = dict(raw_content)

        msg_type = str(msg.get("message_type") or "text").strip().casefold()
        original_type = 1
        content_kind = "text"
        text = ""
        if msg_type == "text":
            text = str(parsed_content.get("text") or "")
        elif msg_type == "post":
            text = _feishu_post_to_text(parsed_content)
        elif msg_type == "image":
            original_type = 3
            content_kind = "image"
            image_key = str(parsed_content.get("image_key") or "").strip()
            msg_id = str(msg.get("message_id") or "").strip()
            refs: list[dict[str, str]] = [
                {"field": "webhook_image_id", "value": image_key or msg_id},
            ]
            if image_key:
                refs.append({"field": "feishu_image_key", "value": image_key})
            if msg_id:
                refs.append({"field": "feishu_message_id", "value": msg_id})
            text = json.dumps(
                {"kind": "image_message", "image_references": refs},
                ensure_ascii=False,
            )
        elif msg_type == "audio":
            original_type = 34
            content_kind = "voice"
            text = json.dumps({"kind": "voice_message"}, ensure_ascii=False)
        else:
            text = json.dumps(
                {"kind": "app_message", "text": json.dumps(parsed_content, ensure_ascii=False)[:4096]},
                ensure_ascii=False,
            )
            original_type = 49
            content_kind = msg_type

        parent_id = str(msg.get("parent_id") or "").strip()
        if parent_id and original_type == 1:
            original_type = 49
            content_kind = "quote"
            text = json.dumps(
                {
                    "kind": "quoted_message",
                    "text": text[:8192],
                    "quote": {
                        "type": "1",
                        "platform_message_id": parent_id[:64],
                        "source_conversation_id": conversation_id[:512],
                        "author_id": "",
                        "display_name_hint": "",
                        "content": "",
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
                sender_origin="channel_group_member" if is_group else "feishu_user",
                text=text,
                is_group=is_group,
                message_type=1,
                source_msg_id=None,
                capture_tick_ms=capture_tick_ms,
                raw_envelope_json=envelope_json,
                raw_message_json=json.dumps(
                    dict(msg), ensure_ascii=False, sort_keys=True, separators=(",", ":")
                ),
                original_message_type=original_type,
                source_id=self.source_id,
                channel=self.channel,
                content_kind=content_kind,
            ),
        )

    def _get_tenant_token(self, client: httpx.Client) -> str:
        if self._static_tenant_token:
            return self._static_tenant_token
        now = time.monotonic()
        if self._cached_token and now < self._token_expires_at:
            return self._cached_token
        if not self.app_id or not self.app_secret:
            return ""
        resp = client.post(
            f"{self.api_base}/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": self.app_id, "app_secret": self.app_secret},
        )
        resp.raise_for_status()
        body = resp.json()
        token = str(body.get("tenant_access_token") or "").strip()
        expire = int(body.get("expire") or 7200)
        if token:
            self._cached_token = token
            self._token_expires_at = now + max(60, expire - 120)
        return token

    def _send_im_message(
        self,
        conversation_id: str,
        *,
        msg_type: str,
        content_obj: dict[str, object],
    ) -> tuple[str, str]:
        content_str = json.dumps(content_obj, ensure_ascii=False)
        body = {
            "receive_id": conversation_id,
            "msg_type": msg_type,
            "content": content_str,
        }
        self.outbound.append(body)
        try:
            with httpx.Client(
                timeout=10.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                token = self._get_tenant_token(client)
                if not token:
                    return "rejected", "missing_feishu_credentials"
                resp = client.post(
                    f"{self.api_base}/open-apis/im/v1/messages",
                    params={"receive_id_type": "chat_id"},
                    json=body,
                    headers={"Authorization": f"Bearer {token}"},
                )
            if 200 <= resp.status_code < 300:
                try:
                    data = resp.json()
                    if isinstance(data, dict) and int(data.get("code") or 0) != 0:
                        return "rejected", f"feishu_api_rejected_{data.get('code')}"
                except ValueError:
                    pass
                return "queued", "feishu_outbound_accepted"
            if 400 <= resp.status_code < 500:
                return "rejected", f"feishu_http_rejected_{resp.status_code}"
            return "unknown", f"feishu_http_status_{resp.status_code}"
        except httpx.TimeoutException:
            return "unknown", "feishu_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "feishu_transport_error_delivery_may_have_occurred"

    def send_text(self, draft: ReplyDraft) -> SendResult:
        if not self._can_send(draft.key.account_id, draft.key.conversation_id):
            return SendResult("rejected", "sender_disabled_or_destination_not_allowed")
        if not draft.text or len(draft.text.encode("utf-8")) > 30_000:
            return SendResult("rejected", "empty_or_oversized_text")
        status, reason = self._send_im_message(
            draft.key.conversation_id,
            msg_type="text",
            content_obj={"text": draft.text},
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
        formatted = f"「{label}: {preview}」\n{text}" if preview else text
        return self._send_im_message(
            conversation_id,
            msg_type="text",
            content_obj={"text": formatted},
        )

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        if not self._can_send(self.default_account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        raw = Path(path).read_bytes()
        try:
            with httpx.Client(
                timeout=15.0,
                trust_env=False,
                follow_redirects=False,
                transport=self.transport,
            ) as client:
                token = self._get_tenant_token(client)
                if not token:
                    return "rejected", "missing_feishu_credentials"
                up = client.post(
                    f"{self.api_base}/open-apis/im/v1/images",
                    data={"image_type": "message"},
                    files={"image": (Path(path).name, raw)},
                    headers={"Authorization": f"Bearer {token}"},
                )
                if 400 <= up.status_code < 500:
                    return "rejected", f"feishu_image_upload_rejected_{up.status_code}"
                if up.status_code >= 500:
                    return "unknown", f"feishu_image_upload_status_{up.status_code}"
                image_key = str((up.json().get("data") or {}).get("image_key") or "").strip()
                if not image_key:
                    return "rejected", "feishu_missing_image_key"
        except httpx.TimeoutException:
            return "unknown", "feishu_timeout_delivery_may_have_occurred"
        except httpx.HTTPError:
            return "unknown", "feishu_transport_error_delivery_may_have_occurred"
        return self._send_im_message(
            conversation_id,
            msg_type="image",
            content_obj={"image_key": image_key},
        )

    def fetch_original_image(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
        *,
        allow_account_scan: bool = True,
    ) -> tuple[str, str]:
        image_key = next(
            (ref["value"] for ref in references if ref.get("field") == "feishu_image_key" and ref.get("value")),
            "",
        )
        msg_id = next(
            (ref["value"] for ref in references if ref.get("field") == "feishu_message_id" and ref.get("value")),
            "",
        )
        if not image_key or not msg_id:
            return super().fetch_original_image(
                item, references, destination, allow_account_scan=allow_account_scan,
            )
        with httpx.Client(timeout=10.0, trust_env=False, transport=self.transport) as client:
            token = self._get_tenant_token(client)
            if not token:
                raise ImageIngestError("original_image_not_found_in_cache")
            resp = client.get(
                f"{self.api_base}/open-apis/im/v1/messages/{msg_id}/resources/{image_key}",
                params={"type": "image"},
                headers={"Authorization": f"Bearer {token}"},
            )
            resp.raise_for_status()
            if len(resp.content) > MAX_ORIGINAL_IMAGE_BYTES:
                raise ImageIngestError("original_image_exceeds_limit")
            destination.write_bytes(resp.content)
            return _file_hash(destination)[0], resp.headers.get("content-type", "").split(";")[0]
