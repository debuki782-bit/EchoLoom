"""Resolve stable Hook sender IDs to exact local WeChat Contact rows."""

from __future__ import annotations

import re
from dataclasses import dataclass, replace

_CONTACT_ID = re.compile(r"(?:wxid_[A-Za-z0-9_-]{3,256}|gh_[A-Za-z0-9_-]{1,256})\Z")
_ROOM_ID = re.compile(r"[A-Za-z0-9_-]{1,256}@chatroom\Z")


def is_contact_id(value: object) -> bool:
    return isinstance(value, str) and _CONTACT_ID.fullmatch(value) is not None


def is_room_id(value: object) -> bool:
    return isinstance(value, str) and _ROOM_ID.fullmatch(value) is not None


is_wechat_contact_id = is_contact_id
is_wechat_room_id = is_room_id


def is_group_conversation(conversation_id: object, *, is_group: bool | None = None) -> bool:
    """Classify group vs direct conversation across WeChat and generic multi-source IDs."""
    if is_group is not None:
        return bool(is_group)
    if not isinstance(conversation_id, str):
        return False
    cid = conversation_id.strip()
    if is_room_id(cid):
        return True
    lowered = cid.casefold()
    return lowered.startswith(
        ("room:", "group:", "channel:", "thread:", "guild:")
    ) or lowered.endswith(("@g.us", "@conference"))


@dataclass(frozen=True)
class ContactIdentity:
    """Contact labels returned only after an exact Contact.UserName match."""

    display_name: str | None
    match_source: str = "contact.UserName_exact"
    user_name: str | None = None
    nickname: str | None = None
    remark: str | None = None




def _optional_label(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def apply_sender_remarks(
    identities: dict[str, ContactIdentity], remarks: dict[str, str]
) -> dict[str, ContactIdentity]:
    """Overlay human-maintained database remarks on Hook nickname identities."""
    result = dict(identities)
    for sender_id, remark in remarks.items():
        remark = remark.strip()
        if not remark:
            continue
        identity = result.get(sender_id) or ContactIdentity(
            display_name=remark, user_name=sender_id,
        )
        nickname = identity.nickname
        display_name = (
            f"{remark}（{nickname}）" if nickname
            else remark
        )
        result[sender_id] = replace(
            identity, display_name=display_name, remark=remark.strip()
        )
    return result


def __getattr__(name):
    if name == "HookContactResolver":
        from .adapters.hook_contacts import HookContactResolver
        return HookContactResolver
    raise AttributeError(name)
