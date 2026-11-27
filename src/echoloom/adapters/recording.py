"""Offline backend for integration tests and adapter examples; never sends to WeChat.

It intentionally uses a different callback schema and no Hook imports, sockets,
cache layout or native libraries. Register media bytes explicitly for a replay.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path

from ..connectors.base import AccountState, GatewayConfig, MessageEvent, SendResult, VoicePayload
from ..contacts import is_contact_id
from ..media_artifacts import FileIngestError, ImageIngestError


class RecordingGateway:
    capabilities = frozenset(
        {"text", "image", "file", "quote", "contacts", "image_fetch", "file_fetch", "voice"}
    )

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.allowed = set(config.allowed_conversations)
        self.contacts: dict[str, object] = {}
        self.images: dict[str, bytes] = {}
        self.files: dict[str, bytes] = {}
        self.voices: dict[str, VoicePayload] = {}
        self.outbound: list[dict] = []
        self.closed = False

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        if not isinstance(payload, dict):
            raise TypeError("recording callbacks require an object payload")
        if not isinstance(payload.get("events"), list):
            raise TypeError("recording callbacks require an events list")
        if not 1 <= len(payload["events"]) <= 64:
            raise ValueError("invalid event count")
        events = []
        for row in payload["events"]:
            if not isinstance(row, dict):
                raise TypeError("event must be an object")
            item = MessageEvent(**row)
            if (not all(isinstance(v, str) and v for v in (
                    item.account_id, item.conversation_id, item.sender_id))
                    or not isinstance(item.text, str) or type(item.is_group) is not bool
                    or type(item.message_type) is not int or type(item.capture_tick_ms) is not int
                    or item.capture_tick_ms < 0
                    or (item.source_msg_id is not None and
                        (type(item.source_msg_id) is not int or item.source_msg_id <= 0))):
                raise ValueError("invalid event identity or metadata")
            events.append(replace(item,
                raw_envelope_json=json.dumps(payload, ensure_ascii=False, sort_keys=True,
                                             separators=(",", ":")),
                raw_message_json=json.dumps(row, ensure_ascii=False, sort_keys=True,
                                            separators=(",", ":"))))
        return tuple(events)

    @staticmethod
    def envelope(*events: MessageEvent) -> dict:
        return {"events": [asdict(event) for event in events]}

    def archive_envelope(self, item: MessageEvent, *, preserve_raw: bool = False) -> dict:
        return json.loads(item.raw_envelope_json) if item.raw_envelope_json else self.envelope(item)

    def account_state(self, account_id: str, conversation_id: str = "", *,
                      timeout_seconds: float = 2.0) -> AccountState:
        return AccountState(not self.closed, account_id == self.config.account_id)

    def resolve_contact(self, user_id):
        return self.contacts.get(user_id)

    def allow_conversation(self, key):
        self.allowed.add(key)

    def _allowed(self, account_id, conversation_id):
        return (not self.closed and self.config.send_enabled
                and account_id == self.config.account_id
                and (any(k.account_id == account_id and k.conversation_id == conversation_id
                         for k in self.allowed) or is_contact_id(conversation_id)))

    def send_text(self, draft):
        if not self._allowed(draft.key.account_id, draft.key.conversation_id):
            return SendResult("rejected", "sender_disabled_or_destination_not_allowed")
        self.outbound.append({"kind": "text", "conversation_id": draft.key.conversation_id,
                              "text": draft.text, "event_id": draft.event_id})
        return SendResult("queued", "recorded_locally_not_delivery_proof")

    def send_image(self, conversation_id: str, path: Path):
        if not self._allowed(self.config.account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        self.outbound.append({"kind": "image", "conversation_id": conversation_id,
                              "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
        return "queued", "recorded_locally_not_delivery_proof"

    def send_file(self, conversation_id: str, path: Path):
        if not self._allowed(self.config.account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        self.outbound.append({
            "kind": "file",
            "conversation_id": conversation_id,
            "filename": path.name,
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        })
        return "queued", "recorded_locally_not_delivery_proof"

    def send_quote(self, conversation_id: str, text: str, quote):
        if not self._allowed(self.config.account_id, conversation_id):
            return "rejected", "sender_disabled_or_destination_not_allowed"
        self.outbound.append({"kind": "quote", "conversation_id": conversation_id,
                              "text": text, "quote": quote})
        return "unknown", "recorded_locally_not_sent_to_wechat"

    def decode_image(self, source: Path, destination: Path):
        raise ValueError("recording backend accepts decoded image bytes only")

    def fetch_original_image(
        self,
        item,
        references,
        destination,
        *,
        allow_account_scan: bool = True,
    ):
        payload = self.images.get(item.conversation_id)
        if payload is None:
            raise ImageIngestError("original_image_not_found_in_cache")
        destination.write_bytes(payload)
        return hashlib.sha256(payload).hexdigest(), ""

    def fetch_thumbnail(self, item, references, destination):
        raise ImageIngestError("original_image_not_found_in_cache")

    def fetch_received_file(self, item, references, destination):
        payload = self.files.get(item.conversation_id)
        if payload is None:
            raise FileIngestError("received_file_not_found")
        values = {ref.get("field"): ref.get("value", "") for ref in references}
        expected_md5 = values.get("md5", "").strip().lower()
        if expected_md5 and hashlib.md5(payload, usedforsecurity=False).hexdigest() != expected_md5:
            raise FileIngestError("received_file_md5_mismatch")
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return hashlib.sha256(payload).hexdigest(), len(payload)

    def voice_payload(self, item):
        return self.voices.get(item.conversation_id,
                               VoicePayload(error_code="voice_payload_unavailable"))

    def close(self):
        self.closed = True
