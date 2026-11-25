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
                       original_message_type=item.original_message_type or item.message_type)
    if item.message_type == 3:
        refs = extract_image_references(item.text, item.sender_id if item.is_group else None)
        image_result = {"kind": "image_message", "image_references": refs}
        return replace(item, message_type=1,
                       text=json.dumps(image_result, ensure_ascii=False),
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
            original_message_type=item.original_message_type or item.message_type,
        )
    if item.message_type == 34:
        # The voice payload lives in the private Hook envelope; never put its
        # bytes into the text model context. The Agent can request a transcript.
        return replace(
            item, message_type=1,
            text=json.dumps({"kind": "voice_message"}, ensure_ascii=False),
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
    ref = app.find("refermsg")
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


def quoted_author_id(normalized_text: str) -> str | None:
    try:
        message = json.loads(normalized_text)
    except (json.JSONDecodeError, TypeError):
        return None
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
    try:
        message = json.loads(normalized_text)
    except (json.JSONDecodeError, TypeError):
        return None
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
