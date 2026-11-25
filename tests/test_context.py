"""Sender provenance and scope rules for future model context."""

from dataclasses import replace

import pytest

from wechat_agent.contacts import ContactIdentity
from wechat_agent.context import build_hook_context, fit_context_window
from wechat_agent.hook_contract import HookObservation, parse_callback
from wechat_agent.rich_message import normalize_message


def observed(sender: str, text: str, *, room: str = "room@chatroom") -> HookObservation:
    return HookObservation(
        account_id="bot", conversation_id=room, sender_id=sender,
        sender_origin="hook_group_member_wxid", text=text,
        is_group=True, message_type=1, source_msg_id=None, capture_tick_ms=1234,
    )


def test_two_group_members_get_raw_ids_and_contact_labels() -> None:
    window = build_hook_context((observed("member-a", "早上好"),
                                 observed("member-b", "你是谁")), identities={
                                     "room@chatroom": ContactIdentity(
                                         "测试群名", user_name="room@chatroom",
                                         nickname="测试群名",
                                     ),
                                     "member-a": ContactIdentity(
                                         "成员甲（甲备注）", user_name="member-a",
                                         nickname="成员甲", remark="甲备注",
                                     ),
                                     "member-b": ContactIdentity(
                                         "成员乙", user_name="member-b", nickname="成员乙",
                                     ),
                                 })
    content = window.model_user_content()
    identities = window.model_reference_context()
    assert "member-a | 甲备注（成员甲）：*{早上好}*" in content
    assert "member-b | （成员乙）：*{你是谁}*" in content
    assert "群聊「测试群名」" in window.model_session_context()
    assert "member-a → 甲备注（成员甲）" in identities
    assert "member-b → （成员乙）" in identities
    assert "你是谁" not in repr(window)
    assert "member-b" not in repr(window)


def test_sensitive_terms_are_masked_only_in_user_controlled_names() -> None:
    window = build_hook_context(
        (
            observed("wxid_owner", "主人说这位是主人的同学"),
            observed("wxid_renamed", "老朋友今天来了"),
            observed("wxid_remark", "你好"),
        ),
        identities={
            "room@chatroom": ContactIdentity(
                "朋友讨论群", user_name="room@chatroom", nickname="朋友讨论群",
            ),
            "wxid_owner": ContactIdentity(
                "主人（OwnerExample）", user_name="wxid_owner",
                nickname="OwnerExample", remark="主人",
            ),
            "wxid_renamed": ContactIdentity(
                "老朋友", user_name="wxid_renamed", nickname="老朋友",
            ),
            "wxid_remark": ContactIdentity(
                "朋友（主人的同学）", user_name="wxid_remark",
                nickname="朋友", remark="主人的同学",
            ),
        },
        priority_sender_references=(
            {
                "sender_id": "wxid_owner",
                "memories": ({"topic": "关系", "content": "主人亲自备注了主人的朋友"},),
            },
        ),
    )

    content = window.model_user_content()
    context = window.model_reference_context()
    session = window.model_session_context()
    assert "wxid_owner | 主人（OwnerExample）" in content
    assert "主人的同学" in content
    assert "wxid_renamed | 联系人-" in content
    assert "老朋友今天来了" in content
    assert "wxid_remark | 主人的同学（联系人-" in content
    assert "主人亲自备注了主人的朋友" in context
    assert "朋友讨论群" not in session
    assert "群聊-" in session


def test_message_text_is_braced_and_inner_braces_are_full_width() -> None:
    text = "原文 {close}*"
    window = build_hook_context((observed("member-a", text),))
    content = window.model_user_content()
    assert "：*{原文 ｛close｝*}*" in content
    assert content.count("}*") == 1


def test_private_sender_context_preserves_raw_source_ids() -> None:
    private = replace(observed("friend", "你好", room="friend"), is_group=False,
                      sender_origin="native_private_fromid")
    window = build_hook_context((private,))
    assert "friend | friend：*{你好}*" in window.model_user_content()
    assert "私聊「friend」" in window.model_session_context()


def test_cross_chat_context_is_grouped_and_kept_out_of_user_message() -> None:
    window = build_hook_context(
        (observed("me", "当前消息"),),
        recent_other_messages=(
            {
                "message_id": "m-1", "conversation_id": "room-a@chatroom",
                "sender_id": "alice", "body_text": "群里先说",
                "direction": "inbound", "event_time_context": {"local_minute": "2026-10-30 09:01"},
            },
            {
                "message_id": "m-2", "conversation_id": "friend-b",
                "sender_id": "bot", "body_text": "私聊 Agent 回复",
                "direction": "outbound", "event_time_context": {"local_minute": "2026-10-30 09:02"},
            },
            {
                "message_id": "m-3", "conversation_id": "room-a@chatroom",
                "sender_id": "bot", "body_text": "群里 Agent 回复",
                "direction": "outbound", "event_time_context": {"local_minute": "2026-10-30 09:03"},
            },
        ),
        identities={
            "room-a@chatroom": ContactIdentity("项目群", user_name="room-a@chatroom"),
            "friend-b": ContactIdentity("小林", user_name="friend-b"),
        },
    )

    user_content = window.model_user_content()
    cross_context = window.model_cross_chat_context()
    assert "过去一小时其它聊天近况" not in user_content
    assert "项目群" in cross_context and "小林" in cross_context
    assert cross_context.index("讨论串「项目群」") < cross_context.index("讨论串「小林」")
    group = cross_context.split("讨论串「项目群」", 1)[1].split("讨论串「小林」", 1)[0]
    assert group.index("群里先说") < group.index("群里 Agent 回复")
    assert "Agent（bot） | Agent：*{群里 Agent 回复}*" in cross_context
    assert "消息ID：m-1" in cross_context


def test_context_rejects_cross_room_and_system_events() -> None:
    with pytest.raises(ValueError, match="cross-conversation"):
        build_hook_context((observed("a", "one"), observed("b", "two", room="other@chatroom")))
    with pytest.raises(ValueError, match="only text"):
        build_hook_context((replace(observed("a", "joined"), message_type=10000),))


def test_callback_to_context_keeps_group_senders_separate() -> None:
    callback = {
        "selfwxid": "bot", "sendorrecv": 2,
        "msglist": [{
            "fromid": "room@chatroom", "toid": "bot", "room": "room@chatroom",
            "sender": sender, "msg": body, "msgtype": 1,
            "msgsvrid": 0, "time": 1234,
        } for sender, body in (("wxid_member_a", "wxid_member_a:\n早上好"),
                               ("wxid_member_b", "wxid_member_b:\n你是谁"))],
    }
    window = build_hook_context(parse_callback(callback))
    content = window.model_user_content()
    assert content.index("wxid_member_a | wxid_member_a：*{早上好}*") < content.index(
        "wxid_member_b | wxid_member_b：*{你是谁}*"
    )


def test_context_budget_never_discards_current_batch() -> None:
    window = build_hook_context((
        observed("a", "旧消息" * 180),
        observed("b", "中间消息" * 180),
        observed("c", "最新消息" * 220),
    ))
    bounded = fit_context_window(window, max_bytes=1400)
    rendered = bounded.model_user_content()
    total = sum(len(part.encode("utf-8")) for part in (
        rendered, bounded.model_reference_context(), bounded.model_session_context(),
    ))
    assert total > 1400  # soft assembly window; real model budget still applies
    assert all(text in rendered for text in ("旧消息", "中间消息", "最新消息"))
    assert bounded.omitted_message_count == 0 and not bounded.truncated_text


def test_idle_window_drops_oversized_activity_history_before_its_task() -> None:
    from wechat_agent.context import ContextTurn, ContextWindow

    task = ContextTurn("system", "idle_task", "请整理", message_id="task")
    window = ContextWindow("bot", "idle", False, (task,), is_idle_event=True,
                           idle_activity_summaries=("旧工具结果" * 5000,))
    bounded = fit_context_window(window, max_bytes=3000)
    assert bounded.turns == (task,)
    assert not bounded.idle_activity_summaries
    assert len(bounded.model_user_content().encode("utf-8")) < 3000


def test_rollover_history_marks_truncated_record_and_keeps_its_tail() -> None:
    from wechat_agent.adk_runner import ScopedAdkRunner

    runner = object.__new__(ScopedAdkRunner)
    runner.max_history_bytes = 300
    runner.max_history_records = 50
    history = runner._history_context(None, ["开始" + "长" * 500 + "结尾"])
    assert "较早内容已截断" in history
    assert "结尾" in history


def test_quote_author_uses_chatusr_contact_match_and_shows_real_ids() -> None:
    raw = HookObservation(
        "bot", "room@chatroom", "wxid_current", "hook_group_member_wxid",
        'wxid_current:\n<msg><appmsg><type>57</type><title>提问</title><refermsg>'
        '<type>1</type><fromusr>room@chatroom</fromusr><chatusr>wxid_quoted</chatusr>'
        '<displayname>不可信昵称</displayname><content>引用的正文</content>'
        '</refermsg></appmsg></msg>', True, 49, None, 123,
    )
    normalized = normalize_message(raw)
    assert normalized is not None
    window = build_hook_context((normalized,), identities={
        "wxid_current": ContactIdentity("发送者"),
        "wxid_quoted": ContactIdentity("引用作者"),
    })
    serialized = window.model_user_content()
    references = window.model_reference_context()
    assert "wxid_current | （发送者）：*{提问}*" in serialized
    assert "引用消息（未匹配到消息库原记录）：\n引用发送人 ID：wxid_quoted\n引用来源聊天 ID：room@chatroom\n引用正文：*{引用的正文}*" in serialized
    assert "wxid_current → （发送者）" in references
    assert "wxid_quoted → （引用作者）" in references
    for raw_id in ("wxid_current", "wxid_quoted", "room@chatroom"):
        assert raw_id in serialized
    assert "不可信昵称" not in serialized


def test_priority_sender_activity_and_recency_anchor_render_in_reference_tail() -> None:
    master_window = build_hook_context(
        (observed("wxid_owner", "我回来了"),),
        identities={
            "wxid_owner": ContactIdentity("主人", user_name="wxid_owner", remark="主人"),
            "other@chatroom": ContactIdentity("技术群", user_name="other@chatroom", nickname="技术群"),
        },
        priority_sender_references=(
            {
                "sender_id": "wxid_owner",
                "tags": ("唯一主人",),
                "same_chat_gap_minutes": 95,
                "other_chat_activity": (
                    {"conversation_id": "other@chatroom", "count": 4, "minutes_ago": 12},
                ),
            },
        ),
    )
    master_ref = master_window.model_reference_context()
    assert "距上次在本会话发言已隔 95 分钟" in master_ref
    assert "群聊「技术群」4 条（最近 12 分钟前）" in master_ref
    assert "本轮心境锚点：本轮有主人发言" in master_ref

    outsider_window = build_hook_context(
        (observed("wxid_guest", "大家好"),),
        priority_sender_references=({"sender_id": "wxid_owner", "tags": ("唯一主人",)},),
    )
    outsider_ref = outsider_window.model_reference_context()
    assert "本轮心境锚点：本轮发言者不是主人" in outsider_ref


def test_priority_sender_cross_chat_activity_from_memory_store(tmp_path) -> None:
    from datetime import UTC, datetime, timedelta

    from wechat_agent.memory_store import MemoryStore

    store = MemoryStore(tmp_path / "memory.sqlite3")
    now = datetime.now(UTC)
    old_same = store.archive_message(
        account_id="bot", conversation_id="room-a@chatroom",
        sender_id="wxid_owner", raw_envelope={"seq": 1}, body_text="早些时候",
    )
    other_msg = store.archive_message(
        account_id="bot", conversation_id="room-b@chatroom",
        sender_id="wxid_owner", raw_envelope={"seq": 2}, body_text="在别的群冒泡",
    )
    current_msg = store.archive_message(
        account_id="bot", conversation_id="room-a@chatroom",
        sender_id="wxid_owner", raw_envelope={"seq": 3}, body_text="刚回来",
    )
    assert current_msg.captured_at
    with store.transaction():
        store.db.execute(
            "UPDATE source_messages SET captured_at=? WHERE message_id=?",
            ((now - timedelta(minutes=80)).isoformat(), old_same.message_id),
        )
        store.db.execute(
            "UPDATE source_messages SET captured_at=? WHERE message_id=?",
            ((now - timedelta(minutes=15)).isoformat(), other_msg.message_id),
        )
    store.update_sender_tags(account_id="bot", sender_id="wxid_owner", tags=["唯一主人"], action="add")
    enriched = store.enrich_context_messages(
        account_id="bot",
        messages=[{
            "message_id": current_msg.message_id,
            "captured_at": current_msg.captured_at,
            "sender_id": "wxid_owner",
            "conversation_id": "room-a@chatroom",
            "body_text": "刚回来",
        }],
    )
    assert enriched[0]["conversation_gap_minutes"] == 80
    assert enriched[0]["sender_gap_minutes"] == 80
    profiles = store.priority_sender_profiles(
        account_id="bot",
        current_conversation_id="room-a@chatroom",
        exclude_message_ids=(current_msg.message_id,),
    )
    assert len(profiles) == 1
    assert 79 <= profiles[0]["same_chat_gap_minutes"] <= 81
    assert profiles[0]["other_chat_activity"][0]["conversation_id"] == "room-b@chatroom"
    assert profiles[0]["other_chat_activity"][0]["count"] == 1
    assert old_same.message_id != current_msg.message_id
    store.close()


def test_episode_semantic_text_strips_group_prefix_and_uses_vision_summary() -> None:
    from wechat_agent.context import ContextTurn
    from wechat_agent.episode_memory import EpisodeMemory

    img_turn = ContextTurn(
        "wxid_abc", "hook_group_member_wxid",
        'wxid_abc:\n{"kind": "image_message", "media_kind": "image", "image_md5": "deadbeef"}',
        message_id="m1",
        image_metadata={"vision_summary": "一张厦门海边日落照片"},
    )
    assert EpisodeMemory._semantic_text(img_turn, include_media_notes=True) == "一张厦门海边日落照片"
    short_turn = ContextTurn("wxid_abc", "hook_group_member_wxid", "wxid_abc:\n乐", message_id="m2")
    assert EpisodeMemory._semantic_text(short_turn, include_media_notes=True) == "乐"
