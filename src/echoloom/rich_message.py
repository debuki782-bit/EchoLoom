"""Normalize rich-message metadata without fetching or exposing remote media."""

import json
import re
from dataclasses import replace
from urllib.parse import urlsplit
from xml.etree import ElementTree as ET

from .contacts import is_contact_id
from .wechat_gateway import MessageEvent

_IMAGE_REFERENCE_FIELDS = (
    "cdnmidimgurl", "cdnhdimgurl", "cdnthumburl", "midimgurl", "hdimgurl",
    "thumburl", "cdnurl", "externurl", "fileid", "hdfileid", "md5", "hdmd5",
    "newmd5", "rawmd5", "length", "hdlength",
    "cdnthumblength", "cdnthumbwidth", "cdnthumbheight",
)
_FILE_MD5 = re.compile(r"^[0-9a-fA-F]{32}$")
_CDN_ATTACH_ID = re.compile(r"^@cdn_([0-9a-fA-F]{40,4096})_([0-9a-fA-F]{32})_\d+$")
_MAX_FILE_BYTES = 200_000_000
_XML_TAG = re.compile(r"<(?P<tag>[\w:.-]+)(?P<attrs>[^<>]*?)(?:/?)>", re.DOTALL)
_XML_ATTR = re.compile(
    r"(?P<name>[\w:.-]+)\s*=\s*(?:\"(?P<dq>[^\"]*)\"|'(?P<sq>[^']*)')",
    re.DOTALL,
)


def normalize_message(item: MessageEvent) -> MessageEvent | None:
    if item.message_type == 1:
        return item
    if item.message_type in (10000, 10002):
        try:
            pat = json.loads(item.text)
        except (ValueError, TypeError):
            return None
        if (not isinstance(pat, dict) or pat.get("kind") != "pat_message"
                or pat.get("actor_id") != item.sender_id
                or not is_contact_id(pat.get("target_id"))):
            return None
        return replace(item, message_type=1,
                       content_kind="pat",
                       original_message_type=item.original_message_type or item.message_type)
    if item.message_type == 3:
        refs = extract_image_references(item.text, item.sender_id if item.is_group else None)
        image_result = {"kind": "image_message", "image_references": refs}
        return replace(item, message_type=1,
                       text=json.dumps(image_result, ensure_ascii=False),
                       content_kind="image",
                       original_message_type=item.original_message_type or item.message_type)
    if item.message_type == 47:
        # Type 47 carries sticker/GIF media references in XML. Keep the same
        # private media-reference envelope as ordinary images; context rendering
        # deliberately drops the URLs and the pixels remain tool-only.
        refs = extract_image_references(item.text, item.sender_id if item.is_group else None)
        return replace(
            item, message_type=1,
            text=json.dumps({"kind": "image_message", "media_kind": "sticker",
                             "image_references": refs}, ensure_ascii=False),
            content_kind="sticker",
            original_message_type=item.original_message_type or item.message_type,
        )
    if item.message_type == 34:
        # The voice payload lives in the private Hook envelope; never put its
        # bytes into the text model context. The Agent can request a transcript.
        return replace(
            item, message_type=1,
            text=json.dumps({"kind": "voice_message"}, ensure_ascii=False),
            content_kind="voice",
            original_message_type=item.original_message_type or item.message_type,
        )
    if item.message_type != 49:
        return None
    start = item.text.find("<msg")
    if start < 0 or len(item.text) > 65536 or "<!DOCTYPE" in item.text.upper():
        return None
    try:
        root = ET.fromstring(item.text[start:])
    except ET.ParseError:
        return None
    app = root.find(".//appmsg")
    if app is None:
        return None
    app_type = (app.findtext("type") or "").strip()
    ref = app.find("refermsg")
    if app_type in ("6", "74") and ref is None:
        return _normalize_file_app_message(item, app, app_type)
    app_result: dict[str, object] = {"kind": "app_message", "app_type": app.findtext("type"),
              "text": (app.findtext("title") or "")[:8192]}
    # Shared cards carry their destination separately from the visible title.
    # Keep that public link, without exposing attachment/CDN credentials.
    url = (app.findtext("url") or "").strip()
    try:
        parsed = urlsplit(url)
        if (len(url) <= 2048 and parsed.scheme in ("http", "https") and parsed.hostname
                and not parsed.username and not parsed.password):
            app_result["url"] = url
    except ValueError:
        pass
    description = (app.findtext("des") or "").strip()
    if description:
        app_result["description"] = description[:8192]
    if ref is not None:
        from_user = (ref.findtext("fromusr") or "")[:512]
        chat_user = (ref.findtext("chatusr") or "")[:512]
        # In a group quote, fromusr identifies the source room while chatusr
        # identifies the quoted member. Prefer that exact stable contact ID.
        author_id = chat_user if is_contact_id(chat_user) else (
            from_user if is_contact_id(from_user) else "")
        app_result["kind"] = "quoted_message"
        app_result["quote"] = {
            "type": (ref.findtext("type") or "")[:32],
            # WeChat supplies the source message's server ID. Keep it as a
            # decimal string so it can be matched exactly without integer
            # precision loss or body-text search.
            "platform_message_id": _platform_message_id(ref.findtext("svrid")),
            "source_conversation_id": from_user,
            "author_id": author_id,
            "author_id_source": "chatusr" if author_id == chat_user and author_id
            else "fromusr" if author_id else "unresolved",
            # Kept only for internal diagnostics; model context resolves names
            # from Contact.UserName and intentionally drops this unverified hint.
            "display_name_hint": (ref.findtext("displayname") or "")[:256],
            "content": (ref.findtext("content") or "")[:8192],
        }
    return replace(item, message_type=1, text=json.dumps(app_result, ensure_ascii=False),
                   content_kind="quote" if app_result.get("kind") == "quoted_message" else "app",
                   original_message_type=item.original_message_type or item.message_type)


def extract_image_references(body: str, sender_id: str | None = None) -> list[dict[str, str]]:
    """Extract image URL/ID attribute values without XML entity normalization."""
    if sender_id and body.startswith(sender_id + ":\n"):
        body = body[len(sender_id) + 2:]
    start = body.find("<msg")
    if start < 0 or len(body) > 65536 or "<!DOCTYPE" in body.upper():
        return []
    xml = body[start:]
    references: list[dict[str, str]] = []
    for tag in _XML_TAG.finditer(xml):
        for attr in _XML_ATTR.finditer(tag.group("attrs")):
            name = attr.group("name").split(":")[-1].casefold()
            if name not in _IMAGE_REFERENCE_FIELDS:
                continue
            value = attr.group("dq") if attr.group("dq") is not None else attr.group("sq")
            if value:
                references.append({"field": name, "value": value})
    for name in _IMAGE_REFERENCE_FIELDS:
        child = re.compile(
            rf"<{re.escape(name)}\b[^>]*>(.*?)</{re.escape(name)}\s*>",
            re.IGNORECASE | re.DOTALL,
        )
        for match in child.finditer(xml):
            value = match.group(1)
            cdata = re.fullmatch(r"\s*<!\[CDATA\[(.*?)\]\]>\s*", value, re.DOTALL)
            value = cdata.group(1) if cdata else value.strip()
            if value:
                references.append({"field": name, "value": value})
    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for ref in references:
        key = (ref["field"], ref["value"])
        if key not in seen:
            seen.add(key)
            unique.append(ref)
    return unique


def normalized_image_references(normalized_text: str) -> tuple[str, ...]:
    return tuple(ref["value"] for ref in normalized_image_reference_records(normalized_text))


def normalized_image_reference_records(normalized_text: str) -> tuple[dict[str, str], ...]:
    try:
        message = json.loads(normalized_text)
    except (json.JSONDecodeError, TypeError):
        return ()
    if not isinstance(message, dict) or message.get("kind") != "image_message":
        return ()
    refs = message.get("image_references")
    if not isinstance(refs, list):
        return ()
    return tuple({"field": ref["field"], "value": ref["value"]} for ref in refs
                 if isinstance(ref, dict) and isinstance(ref.get("field"), str)
                 and isinstance(ref.get("value"), str))


def _unwrap_json_payload(text: str) -> object:
    if not isinstance(text, str):
        return None
    candidate = text.lstrip()
    if not candidate.startswith("{"):
        sep = text.find(":\n")
        if sep <= 0:
            return None
        prefix = text[:sep].strip()
        tail = text[sep + 2 :].lstrip()
        if not prefix or len(prefix) > 256 or "\n" in prefix or not tail.startswith("{"):
            return None
        candidate = tail
    try:
        return json.loads(candidate)
    except (json.JSONDecodeError, TypeError):
        return None


def pat_participant_ids(text: str) -> tuple[str, ...]:
    """Extract actor_id and target_id from a normalized pat_message payload."""
    message = _unwrap_json_payload(text)
    if not isinstance(message, dict) or message.get("kind") != "pat_message":
        return ()
    ids: list[str] = []
    for key in ("actor_id", "target_id"):
        val = message.get(key)
        if not isinstance(val, str):
            continue
        cleaned = val.strip()
        if not cleaned or len(cleaned) > 256 or "\0" in cleaned or "\n" in cleaned:
            continue
        if (is_contact_id(cleaned) or ":" in cleaned or "@" not in cleaned) and cleaned not in ids:
            ids.append(cleaned)
    return tuple(ids)


def quoted_author_id(normalized_text: str) -> str | None:
    message = _unwrap_json_payload(normalized_text)
    if not isinstance(message, dict) or message.get("kind") != "quoted_message":
        return None
    quote = message.get("quote")
    author = quote.get("author_id") if isinstance(quote, dict) else None
    if not isinstance(author, str) or not author.strip() or len(author) > 256:
        return None
    if "\0" in author or "\n" in author:
        return None
    return author.strip() if (is_contact_id(author) or ":" in author or "@" not in author) else None


def quoted_message_reference(normalized_text: str) -> dict[str, str] | None:
    message = _unwrap_json_payload(normalized_text)
    if not isinstance(message, dict) or message.get("kind") != "quoted_message":
        return None
    quote = message.get("quote")
    if not isinstance(quote, dict):
        return None
    message_id = quote.get("platform_message_id")
    conversation_id = quote.get("source_conversation_id")
    if (not isinstance(message_id, str) or not message_id
            or not isinstance(conversation_id, str) or not conversation_id):
        return None
    return {"platform_message_id": message_id,
            "source_conversation_id": conversation_id}


def _platform_message_id(value: str | None) -> str | None:
    if value is None:
        return None
    value = value.strip()
    if not value.isdecimal() or len(value) > 32 or not value.strip("0"):
        return None
    return value


def _normalize_file_app_message(item: MessageEvent, app: ET.Element, app_type: str) -> MessageEvent:
    attach = app.find("appattach")
    title = (app.findtext("title") or "").replace("\0", "").replace("\r", " ").replace("\n", " ").strip()[:512]
    ext = ((attach.findtext("fileext") if attach is not None else "") or "").strip().lstrip(".").casefold()[:32]
    if not ext and "." in title:
        ext = title.rsplit(".", 1)[-1].strip().casefold()[:32]
    totallen_raw = ((attach.findtext("totallen") if attach is not None else "") or "").strip()
    size_bytes: int | None = None
    if totallen_raw.isdigit():
        parsed_len = int(totallen_raw)
        if 0 <= parsed_len <= _MAX_FILE_BYTES:
            size_bytes = parsed_len

    if app_type == "74":
        payload: dict[str, object] = {
            "kind": "file_upload_notice",
            "app_type": "74",
            "text": title,
            "filename": title,
            "fileext": ext,
        }
        if size_bytes is not None:
            payload["size_bytes"] = size_bytes
        return replace(
            item,
            message_type=1,
            text=json.dumps(payload, ensure_ascii=False),
            content_kind="file_upload_notice",
            original_message_type=item.original_message_type or item.message_type,
        )

    md5_raw = (app.findtext("md5") or (attach.findtext("md5") if attach is not None else "") or "").strip().lower()
    refs = extract_file_references(item.text, item.sender_id if item.is_group else None)
    file_result: dict[str, object] = {
        "kind": "file_message",
        "app_type": "6",
        "text": title,
        "filename": title,
        "fileext": ext,
        "file_references": refs,
    }
    if size_bytes is not None:
        file_result["size_bytes"] = size_bytes
    if _FILE_MD5.fullmatch(md5_raw):
        file_result["md5"] = md5_raw
    return replace(
        item,
        message_type=1,
        text=json.dumps(file_result, ensure_ascii=False),
        content_kind="file",
        original_message_type=item.original_message_type or item.message_type,
    )


def extract_file_references(body: str, sender_id: str | None = None) -> list[dict[str, str]]:
    """Extract WeChat file-attachment download fields from <msg><appmsg> XML safely."""
    if sender_id and body.startswith(sender_id + ":\n"):
        body = body[len(sender_id) + 2:]
    start = body.find("<msg")
    if start < 0 or len(body) > 65536 or "<!DOCTYPE" in body.upper():
        return []
    try:
        root = ET.fromstring(body[start:])
    except ET.ParseError:
        return []
    app = root.find(".//appmsg")
    if app is None:
        return []
    attach = app.find("appattach")
    raw_pairs: list[tuple[str, str]] = []
    title = (app.findtext("title") or "").replace("\0", "").strip()
    if title:
        raw_pairs.append(("title", title[:512]))
    if attach is not None:
        for field in ("cdnattachurl", "aeskey", "attachid", "totallen", "fileext", "md5"):
            val = (attach.findtext(field) or "").strip()
            if val:
                raw_pairs.append((field, val[:4096]))
    md5_top = (app.findtext("md5") or "").strip()
    if md5_top:
        raw_pairs.append(("md5", md5_top.lower()[:64]))

    values = dict(raw_pairs)
    attachid = values.get("attachid", "")
    if attachid and (not values.get("cdnattachurl") or not values.get("aeskey")):
        matched = _CDN_ATTACH_ID.fullmatch(attachid)
        if matched:
            if not values.get("cdnattachurl"):
                raw_pairs.append(("cdnattachurl", matched.group(1)))
            if not values.get("aeskey"):
                raw_pairs.append(("aeskey", matched.group(2).lower()))

    seen: set[tuple[str, str]] = set()
    unique: list[dict[str, str]] = []
    for field, value in raw_pairs:
        key = (field, value)
        if key not in seen:
            seen.add(key)
            unique.append({"field": field, "value": value})
    return unique


def normalized_file_references(normalized_text: str) -> tuple[dict[str, str], ...]:
    try:
        message = json.loads(normalized_text)
    except (json.JSONDecodeError, TypeError):
        return ()
    if not isinstance(message, dict) or message.get("kind") != "file_message":
        return ()
    refs = message.get("file_references")
    if not isinstance(refs, list):
        return ()
    return tuple(
        {"field": ref["field"], "value": ref["value"]}
        for ref in refs
        if isinstance(ref, dict) and isinstance(ref.get("field"), str)
        and isinstance(ref.get("value"), str)
    )


def normalized_file_metadata(normalized_text: str) -> dict[str, object] | None:
    """Return safe public metadata (never CDN/AES credentials) for a file message."""
    try:
        message = json.loads(normalized_text)
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(message, dict) or message.get("kind") not in ("file_message", "file_upload_notice"):
        return None
    meta: dict[str, object] = {
        "kind": str(message["kind"]),
        "filename": str(message.get("filename") or message.get("text") or ""),
        "fileext": str(message.get("fileext") or ""),
    }
    if isinstance(message.get("size_bytes"), int):
        meta["size_bytes"] = message["size_bytes"]
    if isinstance(message.get("md5"), str) and _FILE_MD5.fullmatch(message["md5"]):
        meta["md5"] = message["md5"].lower()
    return meta
