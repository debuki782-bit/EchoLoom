"""Compact, conversation-scoped model context for observed WeChat events.

The database keeps its structured JSON records. This module renders selected
fields as concise text for the model and keeps conversation identity separate
from repeated per-message content.
"""

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date

from . import local_config
from .contacts import ContactIdentity, is_group_conversation
from .models import InboundEvent
from .prompt_catalog import load_prompt, runtime_templates
from .temporal_context import holiday_day_info
from .wechat_gateway import MessageEvent

_SENSITIVE_NAME_TERMS = (
    # Chinese relationship and intimacy labels, including common variants.
    "主人", "朋友", "男朋友", "女朋友", "男友", "女友",
    "老公", "老婆", "丈夫", "妻子", "夫人", "先生", "爱人", "恋人", "情人",
    "对象", "伴侣", "未婚夫", "未婚妻", "媳妇", "娘子", "相公", "夫君",
    "亲爱的", "宝贝", "宝宝", "亲亲", "心肝", "甜心", "挚爱", "最爱",
    "小可爱", "小宝贝", "小乖", "小祖宗", "我的爱", "心上人", "命中注定",
    "灵魂伴侣", "男神", "女神",
    # Japanese names and relationship forms.
    "ご主人", "主人", "友達", "友人", "親友", "恋人", "彼氏", "彼女",
    "旦那", "嫁", "奥さん", "妻", "夫", "愛人", "愛しい人", "大切な人",
    "大好きな人", "ダーリン", "ハニー", "ベイビー", "相棒", "伴侶", "最愛",
)

_SENSITIVE_NAME_ENGLISH = (
    "gemini", "master", "owner", "friend", "bestie", "bff",
    "boyfriend", "girlfriend", "husband", "wife", "spouse", "partner",
    "fiance", "fiancee", "fiancé", "fiancée", "lover", "sweetheart", "sweetie",
    "darling", "honey", "babe", "baby", "hubby", "wifey", "bae", "boo",
    "cutie", "beloved", "soulmate", "dearest", "significant other", "my love",
    "my dear", "my person", "my beloved",
)

_SENSITIVE_NAME_PATTERNS = tuple(
    re.compile(r"(?<![a-z0-9])" + re.escape(term) + r"(?![a-z0-9])")
    for term in _SENSITIVE_NAME_ENGLISH
)
def _has_sensitive_name_term(label: str) -> bool:
    folded = label.casefold()
    if any(term.casefold() in folded for term in _SENSITIVE_NAME_TERMS):
        return True
    extra_terms = local_config.load_local_runtime_config().extra_sensitive_name_terms
    for term in extra_terms:
        cleaned = term.strip().casefold()
        if not cleaned:
            continue
        if cleaned.isascii() and cleaned.isalnum():
            if re.search(r"(?<![a-z0-9])" + re.escape(cleaned) + r"(?![a-z0-9])", folded):
                return True
        elif cleaned in folded:
            return True
    # English words are matched independently of adjacent CJK characters, while
    # avoiding accidental matches inside ordinary English words.
    return any(pattern.search(folded) for pattern in _SENSITIVE_NAME_PATTERNS)


def _mask_user_controlled_display_name(user_id: str, label: str) -> str:
    """Replace risky self-selected nicknames and group names with a stable label."""
    label = _one_line(label)
    if not label or not _has_sensitive_name_term(label):
        return label
    suffix = hashlib.sha256(user_id.encode("utf-8")).hexdigest()[:10].upper()
    prefix = "群聊" if is_group_conversation(user_id) else "联系人"
    return f"{prefix}-{suffix}"


def _safe_identity_name(user_id: str, identity: ContactIdentity | None,
                        trusted_ids: set[str]) -> str:
    """Mask only self-selected nicknames; managed remarks stay visible."""
    if identity is None:
        return user_id
    remark = _one_line(identity.remark)
    nickname = _one_line(identity.nickname)
    if user_id in trusted_ids:
        if remark and nickname:
            return f"{remark}（{nickname}）"
        if remark:
            return remark
        if nickname:
            return nickname
        return _one_line(identity.display_name) or user_id
    if remark:
        safe_nickname = _mask_user_controlled_display_name(user_id, nickname) if nickname else ""
        suffix = f"（{safe_nickname}）" if safe_nickname and safe_nickname != remark else ""
        return remark + suffix
    # Nicknames can be changed by their owners; their matching human-managed
    # Remark above remains authoritative.
    label = _one_line(identity.nickname or identity.display_name) or user_id
    safe_label = _mask_user_controlled_display_name(user_id, label)
    if safe_label.startswith(("联系人-", "群聊-")):
        return safe_label
    return safe_label or user_id


def _identity_display_parts(user_id: str, identity: ContactIdentity | None,
                            trusted_ids: set[str]) -> tuple[str, tuple[str, ...]]:
    if identity is None:
        return user_id, ()
    safe_name = _safe_identity_name(user_id, identity, trusted_ids)
    if safe_name.startswith(("联系人-", "群聊-")):
        return safe_name, ()
    primary = _one_line(identity.remark)
    nickname = _one_line(identity.nickname)
    if nickname and user_id not in trusted_ids:
        nickname = _mask_user_controlled_display_name(user_id, nickname)
    if primary:
        aliases = (nickname,) if nickname and nickname != primary else ()
        return primary, aliases
    if safe_name and safe_name != user_id:
        return f"（{safe_name}）", ()
    return user_id, ()


def _ref(
    short_refs: Mapping[tuple[str, str], str],
    kind: str,
    target_id: str | None,
) -> str:
    if not target_id:
        return ""
    return short_refs.get((kind, target_id), target_id)


def _id_ref(
    short_refs: Mapping[tuple[str, str], str],
    identifier: str | None,
) -> str:
    if not identifier:
        return ""
    kind = "chat" if is_group_conversation(identifier) else "user"
    return short_refs.get((kind, identifier), identifier)


def _trusted_identity_ids(
    priority_sender_references: Sequence[Mapping[str, object]],
    account_id: str | None = None,
) -> set[str]:
    """Use the stable IDs of the bot account and explicitly prioritized owner profiles."""
    trusted: set[str] = set()
    if isinstance(account_id, str) and account_id:
        trusted.add(account_id)
    for profile in priority_sender_references:
        sender_id = profile.get("sender_id")
        if isinstance(sender_id, str) and sender_id:
            trusted.add(sender_id)
    return trusted


@dataclass(frozen=True, repr=False)
class ContextTurn:
    sender_id: str = field(repr=False)
    sender_origin: str
    text: str = field(repr=False)
    display_name: None = None
    message_id: str | None = None
    annotations: tuple[Mapping[str, object], ...] = ()
    jev_label: Mapping[str, object] | None = None
    entity_references: tuple[Mapping[str, object], ...] = ()
    sender_references: tuple[Mapping[str, object], ...] = ()
    sender_tags: tuple[str, ...] = ()
    conversation_gap_minutes: int | None = None
    sender_gap_minutes: int | None = None
    image_metadata: Mapping[str, object] | None = None
    audio_metadata: Mapping[str, object] | None = None
    file_metadata: Mapping[str, object] | None = None
    message_time: Mapping[str, object] | None = None
    source_conversation_id: str | None = None
    source_is_group: bool | None = None
    direction: str = "inbound"
    delivery_state: str | None = None
    quote_target: Mapping[str, object] | None = field(default=None, repr=False)

    def __repr__(self) -> str:
        return f"ContextTurn(sender_origin={self.sender_origin!r})"


@dataclass(frozen=True, repr=False)
class ContextWindow:
    account_id: str = field(repr=False)
    conversation_id: str = field(repr=False)
    is_group: bool
    turns: tuple[ContextTurn, ...] = field(repr=False)
    history_turns: tuple[ContextTurn, ...] = field(default=(), repr=False)
    recent_other_turns: tuple[ContextTurn, ...] = field(default=(), repr=False)
    global_timeline_turns: tuple[ContextTurn, ...] = field(default=(), repr=False)
    idle_activity_summaries: tuple[str, ...] = field(default=(), repr=False)
    is_idle_event: bool = field(default=False, repr=False)
    identities: Mapping[str, ContactIdentity] = field(default_factory=dict, repr=False)
    short_refs: Mapping[tuple[str, str], str] = field(default_factory=dict, repr=False)
    priority_sender_references: tuple[Mapping[str, object], ...] = field(default=(), repr=False)
    quick_send_images: tuple[Mapping[str, object], ...] = field(default=(), repr=False)
    user_fact_keys: tuple[str, ...] = field(default=(), repr=False)
    daily_prompt_context: str | None = field(default=None, repr=False)
    delivery_notices: tuple[str, ...] = field(default=(), repr=False)
    scheduled_task: str | None = field(default=None, repr=False)
    cancelled_followup: bool = field(default=False, repr=False)
    omitted_message_count: int = field(default=0, repr=False)
    omitted_idle_activity_count: int = field(default=0, repr=False)
    truncated_text: bool = field(default=False, repr=False)

    def __repr__(self) -> str:
        return f"ContextWindow(is_group={self.is_group}, turn_count={len(self.turns)})"

    def model_session_context(self) -> str:
        """Return chat identity once as session-level system background."""
        if self.is_idle_event:
            return "当前是账号级空闲维护专用 Session，不对应任何一个微信聊天。"
        identity = self.identities.get(self.conversation_id)
        name = _safe_identity_name(
            self.conversation_id,
            identity,
            _trusted_identity_ids(self.priority_sender_references, self.account_id),
        )
        kind = "群聊" if self.is_group else "私聊"
        conv_ref = _id_ref(self.short_refs, self.conversation_id)
        return f"当前会话：{kind}「{_one_line(name)}」；会话 ID：{conv_ref}。"

    def model_reference_context(self) -> str:
        """Render current contact labels and dictionary matches for request-time injection."""
        lines: list[str] = []
        trusted_ids = _trusted_identity_ids(self.priority_sender_references, self.account_id)
        for user_id, identity in sorted(self.identities.items()):
            if user_id in (self.account_id, self.conversation_id):
                continue
            name, aliases = _identity_display_parts(user_id, identity, trusted_ids)
            if name and name != user_id:
                alias_text = f"（{', '.join(aliases)}）" if aliases else ""
                lines.append(f"{_id_ref(self.short_refs, user_id)} → {name}{alias_text}")

        latest_entities: dict[str, Mapping[str, object]] = {}
        latest_senders: dict[tuple[str, str], Mapping[str, object]] = {}
        latest_sender_chats: dict[tuple[str, str], str] = {}
        for turn in (
            *self.turns,
            *self.history_turns,
            *self.recent_other_turns,
            *self.global_timeline_turns,
        ):
            for entity in turn.entity_references:
                term = _one_line(entity.get("term"))
                if term:
                    latest_entities[term] = entity
            for sender in turn.sender_references:
                sender_id, topic = sender.get("sender_id"), sender.get("topic")
                if isinstance(sender_id, str) and isinstance(topic, str):
                    key = (sender_id, topic)
                    latest_senders[key] = sender
                    latest_sender_chats[key] = (
                        turn.source_conversation_id or self.conversation_id
                    )
        priority_sender_ids: set[str] = set()
        priority_sender_lines: list[str] = []
        for profile in self.priority_sender_references:
            sender_id = profile.get("sender_id")
            if not isinstance(sender_id, str):
                continue
            priority_sender_ids.add(sender_id)
            priority_identity = self.identities.get(sender_id)
            name, aliases = _identity_display_parts(sender_id, priority_identity, trusted_ids)
            if aliases:
                name += f"（{', '.join(aliases)}）"
            tags = profile.get("tags")
            tag_text = ", ".join(
                _one_line(tag) for tag in tags
                if isinstance(tag, str) and _one_line(tag)
            ) if isinstance(tags, (list, tuple)) else ""
            memories = profile.get("memories")
            descriptions = [
                f"{_one_line(item.get('topic'))}：{_one_line(item.get('content'))}"
                if _one_line(item.get("topic"))
                else _one_line(item.get("content"))
                for item in memories
                if isinstance(item, Mapping) and _one_line(item.get("content"))
            ] if isinstance(memories, (list, tuple)) else []
            detail = "；".join(([f"标签：{tag_text}"] if tag_text else []) + descriptions)
            sender_ref = _id_ref(self.short_refs, sender_id)
            if detail:
                priority_sender_lines.append(f"{sender_ref} | {name}：{detail}")
            same_gap = profile.get("same_chat_gap_minutes")
            other_activity = profile.get("other_chat_activity")
            activity_parts: list[str] = []
            if isinstance(same_gap, int) and not isinstance(same_gap, bool) and same_gap >= 15:
                activity_parts.append(f"距上次在本会话发言已隔 {same_gap} 分钟")
            if isinstance(other_activity, (list, tuple)) and other_activity:
                other_items: list[str] = []
                for act in other_activity[:3]:
                    if not isinstance(act, Mapping):
                        continue
                    cid = _one_line(act.get("conversation_id"))
                    cnt = act.get("count")
                    mins = act.get("minutes_ago")
                    if not cid or not isinstance(cnt, int) or not isinstance(mins, int):
                        continue
                    chat_label = _safe_identity_name(cid, self.identities.get(cid), trusted_ids)
                    kind = "群聊" if is_group_conversation(cid) else "私聊"
                    other_items.append(f"{kind}「{_one_line(chat_label)}」{cnt} 条（最近 {mins} 分钟前）")
                if other_items:
                    activity_parts.append("近期在其它会话发言：" + "、".join(other_items))
            if activity_parts:
                priority_sender_lines.append(f"{sender_ref} 近期动态：{'；'.join(activity_parts)}")
        if priority_sender_ids and not self.is_idle_event and self.turns:
            ctx_cfg = local_config.load_local_runtime_config().context
            templates = runtime_templates()
            if any(t.direction == "inbound" and t.sender_id in priority_sender_ids for t in self.turns):
                anchor = (
                    ctx_cfg.priority_sender_present_anchor
                    or templates.get("priority_sender_present_anchor")
                    or "本轮心境锚点：本轮有主人发言——优先围绕主要绑定联系人自然交流。"
                )
                priority_sender_lines.append(anchor)
            else:
                anchor = (
                    ctx_cfg.priority_sender_absent_anchor
                    or templates.get("priority_sender_absent_anchor")
                    or "本轮心境锚点：本轮发言者不是主人——保持礼貌、克制与清晰的对话边界。"
                )
                priority_sender_lines.append(anchor)
        dictionary_lines = []
        for term, entity in sorted(latest_entities.items(), key=lambda pair: pair[0].casefold()):
            explanation = _one_line(entity.get("explanation"))
            if explanation:
                dictionary_lines.append(f"{term}：{explanation}")

        contact_block = "联系人与聊天名称（精确 ID 匹配）：\n" + (
            "\n".join(lines) if lines else "本轮没有已验证名称。"
        )
        dictionary_block = "本轮相关词典：\n" + (
            "\n".join(dictionary_lines) if dictionary_lines else "本轮没有匹配词条。"
        )
        sender_lines: list[str] = []
        for (sender_id, topic), item in sorted(
            latest_senders.items(), key=lambda pair: (pair[0][0], pair[0][1].casefold())
        ):
            content = _one_line(item.get("content"))
            if sender_id in priority_sender_ids or not content:
                continue
            sender_identity = self.identities.get(sender_id)
            primary, aliases = _identity_display_parts(sender_id, sender_identity, trusted_ids)
            label = f"{_id_ref(self.short_refs, sender_id)} | {primary}"
            if aliases:
                label += f"（{', '.join(aliases)}）"
            source_chat = latest_sender_chats.get((sender_id, topic), "")
            if is_group_conversation(source_chat):
                group_name = _safe_identity_name(
                    source_chat, self.identities.get(source_chat), trusted_ids
                )
                if group_name != source_chat:
                    label += f"；群聊：{group_name}"
            sender_lines.append(
                f"{label} / {topic}：{content}"
            )
        priority_sender_block = (
            "\n\n【主人信息｜每轮优先召回】\n" + "\n".join(priority_sender_lines)
            if priority_sender_lines else ""
        )
        sender_block = "其他发送人备注：\n" + (
            "\n".join(sender_lines) if sender_lines else "本轮没有其他发送人备注。"
        )
        if sender_lines:
            sender_block += (
                "\n资料按发送人 ID 关联；若旧称呼与当前微信昵称或备注不一致，"
                "以当前名称显示，仍按同一 ID 识别。"
            )
        result = (
            priority_sender_block
            + "\n\n【本轮联系人与词典】\n"
            + contact_block
            + "\n\n"
            + dictionary_block
            + "\n\n"
            + sender_block
        )
        if self.quick_send_images:
            image_lines = []
            for image in self.quick_send_images[:10]:
                image_id = _one_line(image.get("image_id"))
                description = _one_line(image.get("library_prompt"))[:120]
                if image_id:
                    image_ref = _ref(self.short_refs, "image", image_id)
                    image_lines.append(
                        f"{image_ref}：{description}" if description else image_ref
                    )
            if image_lines:
                result += (
                    "\n\n【当前可快速发送的图片】\n"
                    + "\n".join(image_lines)
                    + "\n需要发送时可在 commit_agent_turn 传入精确 reply_image_id（支持纯发表情包，或与 reply_text/reply_messages 组合发送），也可单独调用 send_wechat_image。"
                )
        if self.user_fact_keys:
            clean_keys = [
                cleaned for key in self.user_fact_keys if (cleaned := _one_line(key))
            ]
            if clean_keys:
                result += (
                    "\n\n【账号级长期事实目录（仅索引，未载入正文；相关时按需调用 search_user_memory）】\n"
                    + "已存 fact_key："
                    + "、".join(clean_keys)
                )
        return result

    def model_image_read_context(
        self, message_id: str, *, conversation_id: str | None = None,
        max_chars: int = 3200,
    ) -> str:
        """Render a small same-chat window and its matched dictionary for image reading."""
        target_chat = conversation_id or self.conversation_id
        if max_chars < 256:
            raise ValueError("image context limit must be at least 256 characters")
        turns = [
            turn for turn in (
                *self.history_turns, *self.turns,
                *self.recent_other_turns, *self.global_timeline_turns,
            )
            if (turn.source_conversation_id or self.conversation_id) == target_chat
        ]
        target_index = next(
            (index for index, turn in enumerate(turns) if turn.message_id == message_id),
            None,
        )
        if target_index is None:
            selected = turns[-5:]
        else:
            selected = turns[max(0, target_index - 2):target_index + 3]
        if not selected:
            return ""

        is_group = target_chat.endswith("@chatroom")
        lines = [
            _format_turn(
                turn, self.identities, is_group, target_chat,
                self.priority_sender_references,
                account_id=self.account_id,
                short_refs=self.short_refs,
            )
            for turn in selected
        ]
        entities: dict[str, Mapping[str, object]] = {}
        for turn in selected:
            for entity in turn.entity_references:
                term = _one_line(entity.get("term"))
                if term:
                    entities[term.casefold()] = entity
        dictionary = [
            f"{_one_line(entity.get('term'))}：{_one_line(entity.get('explanation'))}"
            for entity in sorted(
                entities.values(), key=lambda item: _one_line(item.get("term")).casefold()
            )
            if _one_line(entity.get("term")) and _one_line(entity.get("explanation"))
        ]
        result = "【图片所在聊天的前后消息】\n" + "\n".join(lines)
        if dictionary:
            result += "\n\n【这些消息匹配的词典】\n" + "\n".join(dictionary)
        return result[:max_chars]

    def model_cross_chat_context(self) -> str:
        """Render newly recalled messages grouped by source chat, chronologically."""
        if self.is_idle_event or not self.recent_other_turns:
            return ""
        templates = runtime_templates()
        trusted_ids = _trusted_identity_ids(self.priority_sender_references, self.account_id)
        groups: dict[str, list[ContextTurn]] = {}
        for turn in self.recent_other_turns:
            source = turn.source_conversation_id or "unknown"
            groups.setdefault(source, []).append(turn)

        sections: list[str] = []
        for conversation_id, turns in groups.items():
            identity = self.identities.get(conversation_id)
            name = _safe_identity_name(conversation_id, identity, trusted_ids)
            kind = "群聊" if conversation_id.endswith("@chatroom") else "私聊"
            lines = [
                "["
                + _format_turn(
                    turn, self.identities, kind == "群聊", self.conversation_id,
                    self.priority_sender_references,
                    account_id=self.account_id,
                    short_refs=self.short_refs,
                )
                + "]"
                for turn in turns
            ]
            conv_ref = _id_ref(self.short_refs, conversation_id)
            sections.append(
                f"讨论串「{name}」（{kind}；ID：{conv_ref}）：\n" + "\n".join(lines)
            )
        return templates["cross_chat_context_intro"] + "\n\n".join(sections)

    def model_user_content(self) -> str:
        """Render selected event fields as concise text, never as JSON."""
        templates = runtime_templates()
        if self.is_idle_event:
            groups: dict[str, list[ContextTurn]] = {}
            for turn in self.global_timeline_turns:
                source = turn.source_conversation_id or "unknown"
                groups.setdefault(source, []).append(turn)
            sections = []
            for conversation_id, turns in groups.items():
                chat_name = self.identities.get(conversation_id)
                label = _safe_identity_name(
                    conversation_id, chat_name,
                    _trusted_identity_ids(self.priority_sender_references, self.account_id),
                )
                lines = [
                    "["
                    + _format_turn(
                        turn,
                        self.identities,
                        conversation_id.endswith("@chatroom"),
                        "__idle_event__",
                        self.priority_sender_references,
                        account_id=self.account_id,
                        short_refs=self.short_refs,
                    )
                    + "]"
                    for turn in turns
                ]
                conv_ref = _id_ref(self.short_refs, conversation_id)
                sections.append(
                    f"聊天「{_one_line(label)}」（{conv_ref}）：\n" + "\n".join(lines)
                )
            content = templates["idle_history_header"] + (
                "\n\n".join(sections) if sections else templates["idle_empty_history"]
            )
            task_text = "\n\n".join(
                turn.text.strip() for turn in self.turns if turn.text.strip()
            )
            if task_text:
                content += "\n\n" + templates["idle_task_header"] + task_text
            else:
                content += "\n\n" + load_prompt("idle_fallback")
            if self.idle_activity_summaries:
                content += templates["idle_recent_summaries_header"] + "\n".join(
                    f"- {item}" for item in self.idle_activity_summaries
                )
            if self.omitted_idle_activity_count:
                content += f"\n【省略较早空闲活动 {self.omitted_idle_activity_count} 条】"
        elif self.turns:
            blocks = [
                "[" + _format_turn(
                    turn, self.identities, self.is_group, self.conversation_id,
                    self.priority_sender_references,
                    account_id=self.account_id,
                    short_refs=self.short_refs,
                ) + "]"
                for turn in self.turns
            ]
            if self.cancelled_followup:
                blocks[-1] += templates["followup_cancelled"]
            content = templates["normal_turn_header"] + "\n".join(blocks)
        else:
            content = templates["no_new_messages"]
        if self.omitted_message_count or self.truncated_text:
            details = []
            if self.omitted_message_count:
                details.append(templates["older_messages_omitted"].replace(
                    "{{count}}", str(self.omitted_message_count),
                ))
            if self.truncated_text:
                details.append(templates["long_body_truncated"])
            content = templates["length_limit_header"] + "；".join(details) + "】\n" + content
        if self.delivery_notices:
            content += templates["delivery_failure_header"] + "\n".join(self.delivery_notices)
        if self.scheduled_task:
            wakeup_key = (
                "scheduled_wakeup_with_messages"
                if self.turns and "scheduled_wakeup_with_messages" in templates
                else "scheduled_wakeup"
            )
            content += templates[wakeup_key].replace(
                "{{task}}", _one_line(self.scheduled_task),
            )
        return content

    def model_history_records(self) -> list[str]:
        """Render prior messages individually so a caller can budget the window."""
        return [
            "[" + _format_turn(
                turn, self.identities, self.is_group, self.conversation_id,
                self.priority_sender_references,
                account_id=self.account_id,
                short_refs=self.short_refs,
            ) + "]"
            for turn in self.history_turns
        ]


def _one_line(value: object) -> str:
    return " ".join(str(value or "").replace("\0", "").split())


def _message_text(
    text: str,
    turn: ContextTurn,
    is_group: bool,
    identities: Mapping[str, ContactIdentity],
    trusted_ids: set[str] | None = None,
) -> tuple[str, str | None]:
    """Return human-readable body and optional quote without leaking raw media URLs."""
    trusted = trusted_ids or set()
    if is_group and text.startswith(turn.sender_id + ":\n"):
        text = text[len(turn.sender_id) + 2 :]
    try:
        message = json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return text, None
    if not isinstance(message, dict):
        return text, None
    if message.get("kind") == "image_message":
        return ("[表情/贴图消息]" if message.get("media_kind") == "sticker" else "[图片消息]"), None
    if message.get("kind") == "voice_message":
        return "[语音消息]", None
    if message.get("kind") == "file_message":
        name = str(message.get("filename") or message.get("text") or "未命名文件").strip()
        ext = str(message.get("fileext") or "").strip()
        size_val = message.get("size_bytes")
        details = [part for part in (ext, f"{size_val} B" if isinstance(size_val, int) else "") if part]
        suffix = f"（{'，'.join(details)}）" if details else ""
        return f"[文件消息] {name}{suffix}", None
    if message.get("kind") == "file_upload_notice":
        name = str(message.get("filename") or message.get("text") or "未命名文件").strip()
        return f"[文件传输通知] {name}（对方上传准备中）", None
    if message.get("kind") == "pat_message":
        suffix = str(message.get("suffix") or "")[:256]
        actor_raw = message.get("actor_id")
        target_raw = message.get("target_id")
        actor_label = (
            _safe_identity_name(actor_raw.strip(), identities.get(actor_raw.strip()), trusted)
            if isinstance(actor_raw, str) and actor_raw.strip()
            else str(actor_raw)
        )
        target_label = (
            _safe_identity_name(target_raw.strip(), identities.get(target_raw.strip()), trusted)
            if isinstance(target_raw, str) and target_raw.strip()
            else str(target_raw)
        )
        return (f"[拍一拍] {actor_label} 拍了拍 {target_label}"
                + (f"；拍一拍后缀：{suffix}" if suffix else "")), None
    if message.get("kind") == "app_message":
        return "\n".join(str(value) for value in (
            "[分享消息]", message.get("text"), message.get("description"), message.get("url")
        ) if value), None
    if message.get("kind") != "quoted_message":
        return text, None
    quote = message.get("quote")
    body = str(message.get("text", ""))
    if not isinstance(quote, dict):
        return body, "引用内容不可用"
    author_id = quote.get("author_id")
    source_id = quote.get("source_conversation_id")
    author_label = (
        _safe_identity_name(author_id.strip(), identities.get(author_id.strip()), trusted)
        if isinstance(author_id, str) and author_id.strip()
        else "未知发送人"
    )
    source_label = (
        _safe_identity_name(source_id.strip(), identities.get(source_id.strip()), trusted)
        if isinstance(source_id, str) and source_id.strip()
        else "来源未知"
    )
    quoted = str(quote.get("content", "")).strip()
    return body, (
        f"引用发送人：{author_label}\n引用来源聊天：{source_label}\n引用正文："
        + render_message_body(quoted or "引用内容不可用")
    )


def render_message_body(text: str) -> str:
    """Bracket a displayed message and prevent its text from closing the bracket."""
    safe = str(text or "").replace("\0", "").replace("{", "｛").replace("}", "｝")
    return f"*{{{safe}}}*"


def _format_turn(
    turn: ContextTurn,
    identities: Mapping[str, ContactIdentity],
    default_is_group: bool,
    current_conversation_id: str,
    priority_sender_references: Sequence[Mapping[str, object]] = (),
    quick_send_images: Sequence[Mapping[str, object]] = (),
    account_id: str | None = None,
    short_refs: Mapping[tuple[str, str], str] | None = None,
) -> str:
    refs = short_refs or {}
    is_group = turn.source_is_group if turn.source_is_group is not None else default_is_group
    trusted_ids = _trusted_identity_ids(priority_sender_references, account_id)
    first_line: list[str] = []
    if turn.message_id:
        first_line.append(f"消息ID：{_ref(refs, 'message', turn.message_id)}")
    if turn.message_time:
        timestamp = (
            turn.message_time.get("local_minute")
            or turn.message_time.get("event_utc_minute")
            or turn.message_time.get("timestamp")
        )
        if timestamp:
            first_line.append(str(timestamp))
        period = turn.message_time.get("time_period")
        if period:
            first_line.append(f"时段：{period}")
        local_minute = turn.message_time.get("local_minute")
        if isinstance(local_minute, str) and len(local_minute) >= 10:
            try:
                day_type = holiday_day_info(date.fromisoformat(local_minute[:10]))["day_type"]
            except ValueError:
                day_type = None
            if isinstance(day_type, str):
                if day_type == "工作日" or day_type.endswith("（工作日）"):
                    first_line.append("工作日：是")
                elif "待公布" in day_type:
                    first_line.append("工作日：待定")
                else:
                    first_line.append("工作日：否")
    if turn.conversation_gap_minutes is not None:
        first_line.append(f"距上一条消息：{turn.conversation_gap_minutes} 分钟")
    if turn.sender_gap_minutes is not None:
        first_line.append(f"距发送人上一条消息：{turn.sender_gap_minutes} 分钟")
    if turn.source_conversation_id and turn.source_conversation_id != current_conversation_id:
        chat_name = _safe_identity_name(
            turn.source_conversation_id,
            identities.get(turn.source_conversation_id),
            trusted_ids,
        )
        if chat_name and chat_name != turn.source_conversation_id:
            first_line.append(f"来源聊天：{chat_name}")
        else:
            first_line.append(f"来源聊天 ID：{_id_ref(refs, turn.source_conversation_id)}")
    if turn.delivery_state:
        first_line.append(f"发送状态：{turn.delivery_state}")
    body, quote = _message_text(turn.text, turn, is_group, identities, trusted_ids)
    body = render_message_body(body)
    if turn.direction == "outbound":
        sender_line = f"Agent：{body}"
    else:
        primary, aliases = _identity_display_parts(
            turn.sender_id, identities.get(turn.sender_id), trusted_ids
        )
        alias_text = ", ".join(aliases)
        alias_suffix = f"（{alias_text}）" if alias_text else ""
        sender_line = f"{primary}{alias_suffix}：{body}"
    rendered = "；".join(first_line) + f"\n{sender_line}"
    if turn.jev_label is not None:
        probabilities = turn.jev_label.get("probabilities")
        if isinstance(probabilities, Mapping):
            scores = []
            for key in ("reply", "concise", "no_reply"):
                value = probabilities.get(key)
                if isinstance(value, int) and not isinstance(value, bool):
                    scores.append(f"{key} {value}%")
            if len(scores) == 3:
                rendered += "\n【Jev策略参考】仅作判断参考：" + "，".join(scores)
                multi = turn.jev_label.get("multi_turn_pct")
                if isinstance(multi, int) and not isinstance(multi, bool) and multi >= 35:
                    rendered += f"；多轮回复 {multi}%"
    if turn.sender_tags:
        rendered += "\n标签：" + ", ".join(turn.sender_tags)
    if turn.quote_target is not None:
        target = _quoted_record_turn(turn.quote_target)
        platform_id = str(turn.quote_target.get("platform_message_id", ""))
        rendered += f"\n引用的原消息（平台消息 ID 精确匹配 {platform_id}）：\n" + _format_turn(
            target, identities, default_is_group, current_conversation_id,
            priority_sender_references, account_id=account_id, short_refs=refs,
        )
    elif quote:
        rendered += (
            "\n引用消息（未匹配到消息库原记录）：\n"
            + quote
        )
    for note in turn.annotations:
        content = _one_line(note.get("content"))
        if content:
            note_id = note.get("note_id")
            note_ref = _ref(refs, "note", note_id) if isinstance(note_id, str) and note_id else ""
            rendered += f"\n备注{f'（{note_ref}）' if note_ref else ''}：{content}"
    if turn.image_metadata:
        image_id = turn.image_metadata.get("image_id")
        state = turn.image_metadata.get("state")
        vision_summary = turn.image_metadata.get("vision_summary")
        if image_id or state:
            image_ref = (
                _ref(refs, "image", image_id) if isinstance(image_id, str) and image_id else ""
            )
            rendered += f"\n图片：{image_ref or '无可用图片 ID'}（状态：{state or '未知'}）"
        if turn.image_metadata.get("source_kind") == "thumbnail_inferred":
            rendered += "\n图片质量：低清缩略图；由时间和尺寸元数据推测关联，可能不是该消息的图片。"
        if vision_summary:
            model_id = turn.image_metadata.get("vision_model_id")
            rendered += (
                f"\n图片备注{f'〔{model_id}〕' if model_id else ''}：{_one_line(vision_summary)}"
            )
    if turn.audio_metadata:
        audio_id = turn.audio_metadata.get("audio_id")
        state = turn.audio_metadata.get("state")
        if audio_id or state:
            audio_ref = (
                _ref(refs, "audio", audio_id) if isinstance(audio_id, str) and audio_id else ""
            )
            rendered += f"\n语音：{audio_ref or '无可用语音 ID'}（状态：{state or '未知'}）"
        transcript = turn.audio_metadata.get("transcript")
        if transcript:
            rendered += f"\n语音转写：{_one_line(transcript)}"
    if turn.file_metadata:
        file_id = turn.file_metadata.get("file_id")
        state = turn.file_metadata.get("state")
        filename = turn.file_metadata.get("filename")
        byte_count = turn.file_metadata.get("byte_count") or turn.file_metadata.get("expected_bytes")
        if file_id or state:
            file_ref = (
                _ref(refs, "file", file_id) if isinstance(file_id, str) and file_id else ""
            )
            size_part = f"，大小：{byte_count} B" if isinstance(byte_count, int) else ""
            name_part = f" {filename}" if filename else ""
            rendered += f"\n文件：{file_ref or '无可用文件 ID'}{name_part}（状态：{state or '未知'}{size_part}）"
        summary = turn.file_metadata.get("summary")
        if summary:
            model_id = turn.file_metadata.get("summary_model_id")
            rendered += f"\n文件摘要{f'〔{model_id}〕' if model_id else ''}：{_one_line(summary)}"
    return rendered


def build_hook_context(
    observations: Sequence[MessageEvent],
    *,
    identities: Mapping[str, ContactIdentity] | None = None,
    short_refs: Mapping[tuple[str, str], str] | None = None,
    stored: Sequence[Mapping[str, object]] | None = None,
    historical_messages: Sequence[Mapping[str, object]] = (),
    recent_other_messages: Sequence[Mapping[str, object]] = (),
    priority_sender_references: Sequence[Mapping[str, object]] = (),
    quick_send_images: Sequence[Mapping[str, object]] = (),
    message_times: Sequence[Mapping[str, object]] | None = None,
    daily_prompt_context: str | None = None,
) -> ContextWindow:
    """Build an observation-only window without mixing accounts or chats."""
    if not 1 <= len(observations) <= 20:
        raise ValueError("context requires 1 to 20 messages")
    first = observations[0]
    if any(
        (item.account_id, item.conversation_id, item.is_group)
        != (first.account_id, first.conversation_id, first.is_group)
        for item in observations
    ):
        raise ValueError("cross-conversation context is forbidden")
    if any(item.message_type != 1 for item in observations):
        raise ValueError("only text observations are supported")
    return ContextWindow(
        account_id=first.account_id,
        conversation_id=first.conversation_id,
        is_group=first.is_group,
        turns=tuple(
            _stored_turn(item, index, stored, message_times)
            for index, item in enumerate(observations)
        ),
        history_turns=tuple(_other_chat_turn(item) for item in historical_messages[-50:]),
        recent_other_turns=tuple(
            _other_chat_turn(item) for item in _unique_messages(recent_other_messages)[-20:]
        ),
        identities=identities or {},
        short_refs=short_refs or {},
        priority_sender_references=tuple(
            item for item in priority_sender_references if isinstance(item, Mapping)
        ),
        quick_send_images=tuple(
            item for item in quick_send_images if isinstance(item, Mapping)
        ),
        daily_prompt_context=daily_prompt_context,
    )


def _unique_messages(messages: Sequence[Mapping[str, object]]) -> list[Mapping[str, object]]:
    """Keep newest occurrences by local ID, then preserve chronological order."""
    unique: list[Mapping[str, object]] = []
    seen: set[str] = set()
    for message in reversed(messages):
        message_id = message.get("message_id")
        if isinstance(message_id, str):
            if message_id in seen:
                continue
            seen.add(message_id)
        unique.append(message)
    unique.reverse()
    return unique


def _other_chat_turn(item: Mapping[str, object]) -> ContextTurn:
    return _record_turn(item, sender_origin="stored_other_chat")


def _stored_turn(
    item: MessageEvent,
    index: int,
    stored: Sequence[Mapping[str, object]] | None,
    message_times: Sequence[Mapping[str, object]] | None,
) -> ContextTurn:
    data = dict(stored[index]) if stored is not None and index < len(stored) else {}
    data.update(
        {
            "sender_id": item.sender_id,
            "text": item.text,
            "message_time": (
                message_times[index]
                if message_times is not None and index < len(message_times)
                else None
            ),
        }
    )
    return _record_turn(data, sender_origin=item.sender_origin)


def _record_turn(item: Mapping[str, object], *, sender_origin: str) -> ContextTurn:
    """Map every stored/current/quoted record through one ContextTurn adapter."""
    sender_id = item.get("sender_id")
    conversation_id = item.get("conversation_id")
    if not isinstance(sender_id, str):
        raise ValueError("context record requires a sender ID")  # noqa: TRY004
    annotations = item.get("annotations")
    entities = item.get("entities")
    sender_references = item.get("sender_memories")
    sender_tags = item.get("sender_tags")
    image = item.get("image")
    audio = item.get("audio")
    file_meta = item.get("file")
    message_time = item.get("message_time", item.get("event_time_context"))
    quote_target = item.get("quote_target")
    source_conversation_id = item.get("source_conversation_id", conversation_id)
    message_id = item.get("message_id")
    jev_label = item.get("jev_label")
    conversation_gap_minutes = item.get("conversation_gap_minutes")
    sender_gap_minutes = item.get("sender_gap_minutes")
    source_is_group = item.get("source_is_group")
    delivery_state = item.get("delivery_state")
    return ContextTurn(
        sender_id=sender_id,
        sender_origin=sender_origin,
        text=str(item.get("text", item.get("body_text", ""))),
        message_id=(message_id if isinstance(message_id, str) else None),
        annotations=tuple(note for note in annotations if isinstance(note, Mapping))
        if isinstance(annotations, (list, tuple))
        else (),
        jev_label=(jev_label if isinstance(jev_label, Mapping) else None),
        entity_references=tuple(entity for entity in entities if isinstance(entity, Mapping))
        if isinstance(entities, (list, tuple))
        else (),
        sender_references=tuple(row for row in sender_references if isinstance(row, Mapping))
        if isinstance(sender_references, (list, tuple))
        else (),
        sender_tags=tuple(tag for tag in sender_tags if isinstance(tag, str))
        if isinstance(sender_tags, (list, tuple))
        else (),
        conversation_gap_minutes=(
            conversation_gap_minutes if isinstance(conversation_gap_minutes, int) else None
        ),
        sender_gap_minutes=(
            sender_gap_minutes if isinstance(sender_gap_minutes, int) else None
        ),
        image_metadata=image if isinstance(image, Mapping) else None,
        audio_metadata=audio if isinstance(audio, Mapping) else None,
        file_metadata=file_meta if isinstance(file_meta, Mapping) else None,
        message_time=message_time if isinstance(message_time, Mapping) else None,
        source_conversation_id=(
            source_conversation_id if isinstance(source_conversation_id, str) else None
        ),
        source_is_group=(
            source_is_group
            if isinstance(source_is_group, bool)
            else conversation_id.endswith("@chatroom")
            if isinstance(conversation_id, str)
            else None
        ),
        direction=("outbound" if item.get("direction") == "outbound" else "inbound"),
        delivery_state=(delivery_state if isinstance(delivery_state, str) else None),
        quote_target=quote_target if isinstance(quote_target, Mapping) else None,
    )


def fit_context_window(window: ContextWindow, *, max_bytes: int) -> ContextWindow:
    """Fit the assembled request tail to a UTF-8 byte cap, preserving newest chat first."""
    if max_bytes < 512:
        raise ValueError("max_bytes must be at least 512")
    current = window

    def size(value: ContextWindow) -> int:
        return sum(
            len(text.encode("utf-8"))
            for text in (
                value.model_user_content(),
                value.model_cross_chat_context(),
                value.model_reference_context(),
                value.model_session_context(),
                value.daily_prompt_context or "",
            )
        )

    omitted = current.omitted_message_count
    while size(current) > max_bytes:
        if (current.is_idle_event and current.idle_activity_summaries
                and sum(len(item.encode("utf-8")) for item in current.idle_activity_summaries)
                > max_bytes // 2):
            # Summaries are newest first. A large tool history must not crowd
            # the entire message timeline or the task out of an idle request.
            current = replace(current, idle_activity_summaries=current.idle_activity_summaries[:-1],
                              omitted_idle_activity_count=current.omitted_idle_activity_count + 1)
        elif current.global_timeline_turns:
            current = replace(
                current,
                global_timeline_turns=current.global_timeline_turns[1:],
            )
            omitted += 1
        elif current.recent_other_turns:
            current = replace(current, recent_other_turns=current.recent_other_turns[1:])
            omitted += 1
        elif current.user_fact_keys:
            keep_keys = len(current.user_fact_keys) // 2
            current = replace(
                current,
                user_fact_keys=current.user_fact_keys[:keep_keys] if keep_keys else (),
            )
        elif current.history_turns:
            current = replace(current, history_turns=current.history_turns[1:])
            omitted += 1
        elif current.is_idle_event and current.idle_activity_summaries:
            current = replace(current, idle_activity_summaries=current.idle_activity_summaries[:-1],
                              omitted_idle_activity_count=current.omitted_idle_activity_count + 1)
        else:
            break
    # Inbound messages and internal tasks are never discarded to satisfy this
    # soft window. The model budget enforces the actual request limit.
    if size(current) > max_bytes and current.is_idle_event:
        raise ValueError("idle context cannot fit without dropping the task")
    if current.omitted_message_count != omitted:
        current = replace(current, omitted_message_count=omitted)
    return current


def _quoted_record_turn(item: Mapping[str, object]) -> ContextTurn:
    """Render an exact quote match through the same formatter as ordinary turns."""
    return _record_turn(item, sender_origin="exact_quoted_message")


def build_event_context(event: InboundEvent) -> ContextWindow:
    """Prepare one accepted gateway event as user-role model data."""
    if event.message_type != "text":
        raise ValueError("only text events are supported")
    return ContextWindow(
        account_id=event.account_id,
        conversation_id=event.conversation_id,
        is_group=event.is_group,
        turns=(ContextTurn(event.sender_id, "gateway_event", event.text),),
    )
