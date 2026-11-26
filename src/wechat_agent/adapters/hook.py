"""The existing pinned local Hook behind the application WeChat contract."""
from __future__ import annotations

import json
import re
from pathlib import Path
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

import httpx

from ..connectors.base import AccountState, GatewayConfig, MessageEvent, VoicePayload
from ..contacts import is_contact_id, is_room_id
from ..media_artifacts import MAX_FILE_BYTES
from .hook_contacts import HookContactResolver
from .hook_events import archive_envelope, parse_callback
from .hook_media import HookImageSource, wechat_storage_root_from_status
from .hook_recovery import observation_from_hook_status
from .hook_text import HookTextSender, SendPacer


def profile_matches_account(status: dict, account: str) -> bool:
    profile_path = status.get("SqliteContactDbPath")
    if not isinstance(profile_path, str) or not profile_path or not account:
        return False
    account_key = account.casefold()
    return any(part.casefold() == account_key or part.casefold().startswith(account_key + "_")
               for part in profile_path.replace("/", "\\").split("\\"))


def _build_file_appmsg_xml(
    title: str,
    fileext: str,
    size_bytes: int,
    md5_hex: str,
    fileid: str,
    aeskey: str,
) -> str:
    appmsg = ET.Element("appmsg", {"appid": "", "sdkver": "0"})
    ET.SubElement(appmsg, "title").text = title
    ET.SubElement(appmsg, "des").text = ""
    ET.SubElement(appmsg, "action").text = "view"
    ET.SubElement(appmsg, "type").text = "6"
    ET.SubElement(appmsg, "showtype").text = "0"
    ET.SubElement(appmsg, "soundtype").text = "0"
    ET.SubElement(appmsg, "mediatagname").text = ""
    ET.SubElement(appmsg, "messageext").text = ""
    ET.SubElement(appmsg, "messageaction").text = ""
    ET.SubElement(appmsg, "content").text = ""
    ET.SubElement(appmsg, "contentattr").text = "0"
    for empty_tag in ("url", "lowurl", "dataurl", "lowdataurl"):
        ET.SubElement(appmsg, empty_tag).text = ""
    attach = ET.SubElement(appmsg, "appattach")
    ET.SubElement(attach, "totallen").text = str(size_bytes)
    ET.SubElement(attach, "attachid").text = f"@cdn_{fileid}_{aeskey}_1"
    ET.SubElement(attach, "cdnattachurl").text = fileid
    ET.SubElement(attach, "aeskey").text = aeskey
    ET.SubElement(attach, "encryver").text = "0"
    ET.SubElement(attach, "fileext").text = fileext
    for empty_tag in ("extinfo", "sourceusername", "sourcedisplayname", "thumburl"):
        ET.SubElement(appmsg, empty_tag).text = ""
    ET.SubElement(appmsg, "md5").text = md5_hex
    ET.SubElement(appmsg, "statextstr").text = ""
    return ET.tostring(appmsg, encoding="unicode")


class HookGateway:
    capabilities = frozenset({"text", "image", "file", "quote", "contacts", "image_fetch",
                              "file_fetch", "image_decode", "voice", "latest_event_recovery"})
    archive_envelope = staticmethod(archive_envelope)

    def __init__(self, config: GatewayConfig):
        self.config = config
        self.source_id = config.source_id or "wechat"
        self.channel = "wechat"
        parsed = urlsplit(config.base_url)
        if (parsed.scheme != "http" or parsed.hostname != "127.0.0.1"
                or parsed.port is None or parsed.path not in ("", "/")
                or parsed.username or parsed.password or parsed.query or parsed.fragment):
            raise ValueError("Hook gateway must use a loopback HTTP origin")
        self.base_url = config.base_url.rstrip("/")
        self.transport = config.transport
        self._pacer = SendPacer(*config.send_cooldown_range)
        self._sender = config.text_sender
        if self._sender is NotImplemented:
            self._sender = HookTextSender(config.allowed_conversations,
                enabled=config.send_enabled, private_account_id=config.account_id or None,
                transport=config.transport, base_url=self.base_url, pacer=self._pacer)
        self._contacts = HookContactResolver(self.base_url, timeout_seconds=config.contact_timeout_seconds,
                                             transport=config.transport)
        self.image_source = HookImageSource(config.workspace, source_root=config.source_root,
            native_download_url=config.native_download_url, transport=config.transport,
            base_url=self.base_url)

    def account_state(self, account_id: str, conversation_id: str = "", *,
                      timeout_seconds: float = 2.0) -> AccountState:
        with httpx.Client(timeout=timeout_seconds, trust_env=False,
                          transport=self.transport) as client:
            status = client.get(self.base_url + "/QueryDB/status").json()
        logged_in = status.get("IsLogin") == 1
        matches = profile_matches_account(status, account_id)
        storage = wechat_storage_root_from_status(status, account_id)
        if logged_in and matches and storage is not None:
            self.image_source.source_root = storage
        return AccountState(logged_in, matches, storage,
            observation_from_hook_status(status, account_id=account_id,
                                         conversation_id=conversation_id))

    def resolve_contact(self, user_id):
        return self._contacts.resolve(user_id)

    def allow_conversation(self, key):
        self._sender.allow_conversation(key)

    def send_text(self, draft):
        return self._sender.submit(draft)

    def send_image(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        self._pacer.wait_turn()
        with httpx.Client(timeout=15, trust_env=False, follow_redirects=False,
                          transport=self.transport) as hook:
            raw = hook.post(self.base_url + "/SendImgMsg", json={
                "wxidorgid": conversation_id, "path": str(path),
            })
        if raw.status_code != 200 or len(raw.content) > 4096:
            return "unknown", f"hook_http_status_{raw.status_code}"
        try:
            response = raw.json()
        except ValueError:
            return "unknown", "hook_response_invalid_json"
        if not isinstance(response, dict) or type(response.get("ret")) is not int:
            return "unknown", "hook_response_missing_integer_ret"
        if response["ret"] == 0 and response.get("queued") is True:
            return "queued", "hook_queued_not_delivery_proof"
        return "unknown", f"unclassified_hook_ret_{response['ret']}"

    def send_file(self, conversation_id: str, path: Path) -> tuple[str, str | None]:
        path_str = str(path)
        if not conversation_id or "\0" in conversation_id or "\0" in path_str:
            return "rejected", "invalid_file_send_arguments"
        try:
            if not path.is_file():
                return "rejected", "send_file_not_found"
            size = path.stat().st_size
        except OSError:
            return "rejected", "send_file_unreadable"
        if size <= 0 or size > MAX_FILE_BYTES:
            return "rejected", "send_file_size_out_of_range"

        self._pacer.wait_turn()
        try:
            with httpx.Client(timeout=60, trust_env=False, follow_redirects=False,
                              transport=self.transport) as hook:
                raw = hook.post(self.base_url + "/SendFileMsg", json={
                    "to_wxid": conversation_id,
                    "file_path": path_str,
                })
        except httpx.HTTPError:
            return "unknown", "hook_transport_or_response_error_delivery_may_have_occurred"

        if raw.status_code == 404:
            return self._send_file_via_native_upload(conversation_id, path)
        if raw.status_code != 200 or len(raw.content) > 4096:
            return "unknown", f"hook_http_status_{raw.status_code}"
        try:
            response = raw.json()
        except ValueError:
            return "unknown", "hook_response_invalid_json"
        if not isinstance(response, dict) or type(response.get("ret")) is not int:
            return "unknown", "hook_response_missing_integer_ret"
        if response["ret"] == 0:
            return "accepted_by_server", "hook_file_send_accepted"
        if response["ret"] == -5 and self.image_source.native_downloader is not None:
            return self._send_file_via_native_upload(conversation_id, path)
        if response["ret"] in (-1, -2):
            return "rejected", f"hook_pre_send_rejection_ret_{response['ret']}"
        return "unknown", f"unclassified_hook_ret_{response['ret']}"

    def _send_file_via_native_upload(
        self,
        conversation_id: str,
        path: Path,
    ) -> tuple[str, str | None]:
        downloader = self.image_source.native_downloader
        account_id = self.config.account_id
        if downloader is None or not account_id:
            return "rejected", "file_send_endpoint_unavailable"
        uploaded = downloader.upload_file(account_id, path)
        if uploaded is None:
            return "unknown", "native_file_upload_failed"
        fileid, aeskey, md5_hex, size_bytes = uploaded
        xml = _build_file_appmsg_xml(
            title=path.name,
            fileext=path.suffix.lstrip(".").lower(),
            size_bytes=size_bytes,
            md5_hex=md5_hex,
            fileid=fileid,
            aeskey=aeskey,
        )
        try:
            with httpx.Client(base_url=self.base_url, timeout=10, trust_env=False,
                              follow_redirects=False, transport=self.transport) as hook:
                raw = hook.post("/ForwardXMLMsg", json={
                    "to_wxid": conversation_id, "type": 6, "content": xml,
                })
            result = raw.json() if len(raw.content) <= 4096 else None
            if raw.status_code != 200:
                return "unknown", f"hook_http_status_{raw.status_code}"
            if not isinstance(result, dict) or type(result.get("ret")) is not int:
                return "unknown", "hook_response_missing_integer_ret"
            if result["ret"] == 0:
                return "accepted_by_server", "native_upload_and_forward_accepted"
            if result["ret"] in (-1, -2):
                return "rejected", f"hook_pre_send_rejection_ret_{result['ret']}"
            return "unknown", f"unclassified_hook_ret_{result['ret']}"
        except (httpx.HTTPError, ValueError):
            return "unknown", "hook_transport_or_response_error_delivery_may_have_occurred"

    def send_quote(self, conversation_id: str, text: str,
                   quote: tuple[str, str, str, str]) -> tuple[str, str | None]:
        author_id, author_name, source_room, source_text = quote
        appmsg = ET.Element("appmsg", {"appid": "", "sdkver": "0"})
        ET.SubElement(appmsg, "title").text = text
        ET.SubElement(appmsg, "type").text = "57"
        refer = ET.SubElement(appmsg, "refermsg")
        for name, value in (("type", "1"), ("svrid", "0"),
                            ("fromusr", source_room), ("chatusr", author_id),
                            ("displayname", author_name), ("content", source_text)):
            ET.SubElement(refer, name).text = value
        xml = ET.tostring(appmsg, encoding="unicode")
        try:
            self._pacer.wait_turn()
            with httpx.Client(base_url=self.base_url, timeout=5,
                              trust_env=False, follow_redirects=False,
                              transport=self.transport) as hook:
                response = hook.post("/ForwardXMLMsg", json={
                    "to_wxid": conversation_id, "type": 57, "content": xml,
                })
            result = response.json() if len(response.content) <= 4096 else None
            if response.status_code != 200:
                return "unknown", f"hook_http_status_{response.status_code}"
            if not isinstance(result, dict) or type(result.get("ret")) is not int:
                return "unknown", "hook_response_missing_integer_ret"
            if result["ret"] == 0:
                return "accepted_by_server", "server_accepted_not_delivery_proof"
            if result["ret"] in (-1, -2):
                return "rejected", f"hook_pre_send_rejection_ret_{result['ret']}"
            return "unknown", f"unclassified_hook_ret_{result['ret']}"
        except (httpx.HTTPError, ValueError):
            return "unknown", "hook_transport_or_response_error_delivery_may_have_occurred"

    def parse_events(self, payload: object) -> tuple[MessageEvent, ...]:
        events = parse_callback(payload)
        source_id = getattr(self, "source_id", "default")
        if source_id == "default":
            return events
        from dataclasses import replace
        return tuple(replace(ev, source_id=source_id, channel="wechat") for ev in events)

    def normalize_event(self, item: MessageEvent) -> MessageEvent | None:
        from ..rich_message import normalize_message
        return normalize_message(item)

    def in_scope(
        self,
        item: MessageEvent,
        *,
        account: str,
        default_room: str = "",
        registered_group_ids: set[str] | frozenset[str] = frozenset(),
    ) -> bool:
        if item.account_id != account:
            return False
        if item.is_group:
            if default_room and item.conversation_id == default_room:
                return True
            if item.conversation_id in registered_group_ids:
                return is_contact_id(item.sender_id) or item.sender_id == account
            return is_room_id(item.conversation_id) and is_contact_id(item.sender_id)
        return is_contact_id(item.conversation_id)

    def ensure_ready(self, account_id: str) -> bool:
        downloader = getattr(self.image_source, "native_downloader", None)
        if downloader is None or not hasattr(downloader, "ensure_running"):
            return False
        return bool(downloader.ensure_running(account_id))

    def fetch_account_cached_original(self, item, references, destination):
        return self.image_source._fetch_account_cached_original(item, references, destination)

    def decode_image(self, source: Path, destination: Path) -> None:
        with httpx.Client(timeout=15, trust_env=False, transport=self.transport) as hook:
            result = hook.post(self.base_url + "/Decode_Pic", json={
                "src_path": str(source), "dst_path": str(destination),
                "mode": 1, "wide": True,
            }).json()
        if result.get("ret") != 0 or not destination.exists():
            raise ValueError("image decode unavailable")

    def fetch_original_image(
        self,
        item,
        references,
        destination,
        *,
        allow_account_scan: bool = True,
    ):
        return self.image_source._fetch_original(
            item, references, destination, allow_account_scan=allow_account_scan,
        )

    def fetch_thumbnail(self, item, references, destination):
        return self.image_source._fetch_unique_thumbnail(item, references, destination)

    def fetch_received_file(
        self,
        item: MessageEvent,
        references: list[dict[str, str]],
        destination: Path,
    ) -> tuple[str, int]:
        return self.image_source.fetch_received_file(item, references, destination)

    def voice_payload(self, item: MessageEvent) -> VoicePayload:
        source = json.loads(item.raw_message_json) if item.raw_message_json else {}
        encoded = source.get("voice_hex") if isinstance(source, dict) else None
        if not isinstance(encoded, str) or not encoded:
            return VoicePayload(error_code="voice_payload_unavailable")
        if len(encoded) > 4_000_000 or len(encoded) % 2:
            return VoicePayload(error_code="voice_payload_size_invalid")
        try:
            silk = bytes.fromhex(encoded)
        except ValueError:
            return VoicePayload(error_code="voice_payload_invalid")
        if silk.startswith(b"\x02#!SILK_V3"):
            silk = silk[1:]
        if not silk.startswith(b"#!SILK_V3"):
            return VoicePayload(error_code="unsupported_voice_codec")
        message = source.get("msg")
        duration = None
        if isinstance(message, str) and len(message) <= 65536:
            match = re.search(r"\bvoicelength\s*=\s*['\"](\d{1,7})['\"]", message)
            duration = int(match.group(1)) if match else None
        if duration is not None and duration > 180_000:
            return VoicePayload(error_code="voice_duration_limit")
        return VoicePayload(silk, duration)

    def close(self):
        close = getattr(self._sender, "close", None)
        if close is not None:
            close()
