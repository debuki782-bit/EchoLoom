"""Sender provenance and scope rules for future model context."""

from dataclasses import replace

import pytest

from echoloom.contacts import ContactIdentity
from echoloom.context import build_hook_context, fit_context_window
from echoloom.hook_contract import HookObservation, parse_callback
from echoloom.rich_message import normalize_message, pat_participant_ids


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
    assert "甲备注（成员甲）：*{早上好}*" in content
    assert "（成员乙）：*{你是谁}*" in content
    assert "member-a | " not in content
    assert "member-b | " not in content
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
    assert "主人（OwnerExample）：*{主人说这位是主人的同学}*" in content
    assert "wxid_owner | " not in content
    assert "主人的同学" in content
    assert "\n联系人-" in content
    assert "wxid_renamed | " not in content
    assert "老朋友今天来了" in content
    assert "主人的同学（联系人-" in content
    assert "wxid_remark | " not in content
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
    assert "friend：*{你好}*" in window.model_user_content()
    assert "friend | " not in window.model_user_content()
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
    assert "Agent：*{群里 Agent 回复}*" in cross_context
    assert "Agent（bot）" not in cross_context
    assert "来源聊天：项目群" in cross_context
    assert "来源聊天：小林" in cross_context
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
    assert content.index("wxid_member_a：*{早上好}*") < content.index(
        "wxid_member_b：*{你是谁}*"
    )
    assert "wxid_member_a | " not in content
    assert "wxid_member_b | " not in content


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
    from echoloom.context import ContextTurn, ContextWindow

    task = ContextTurn("system", "idle_task", "请整理", message_id="task")
    window = ContextWindow("bot", "idle", False, (task,), is_idle_event=True,
                           idle_activity_summaries=("旧工具结果" * 5000,))
    bounded = fit_context_window(window, max_bytes=3000)
    assert bounded.turns == (task,)
    assert not bounded.idle_activity_summaries
    assert len(bounded.model_user_content().encode("utf-8")) < 3000


def test_rollover_history_marks_truncated_record_and_keeps_its_tail() -> None:
    from echoloom.adk_runner import ScopedAdkRunner

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
    assert "（发送者）：*{提问}*" in serialized
    assert "wxid_current | " not in serialized
    assert (
        "引用消息（未匹配到消息库原记录）：\n引用发送人：引用作者\n"
        "引用来源聊天：room@chatroom\n引用正文：*{引用的正文}*"
    ) in serialized
    assert "wxid_current → （发送者）" in references
    assert "wxid_quoted → （引用作者）" in references
    assert "room@chatroom" in window.model_session_context()
    assert "不可信昵称" not in serialized


def test_pat_message_resolves_names_including_bot_self_and_masks_untrusted() -> None:
    import json

    owner_pat_json = json.dumps({
        "kind": "pat_message",
        "actor_id": "wxid_owner",
        "target_id": "wxid_bot_self",
        "suffix": "的小脑袋",
    }, ensure_ascii=False)
    stranger_pat_json = json.dumps({
        "kind": "pat_message",
        "actor_id": "wxid_stranger",
        "target_id": "wxid_unknown_target",
    }, ensure_ascii=False)

    assert pat_participant_ids(owner_pat_json) == ("wxid_owner", "wxid_bot_self")
    assert pat_participant_ids(f"wxid_owner:\n{owner_pat_json}") == ("wxid_owner", "wxid_bot_self")
    assert pat_participant_ids('{"kind": "pat_message", "actor_id": "wxid_a", "target_id": "wxid_a"}') == ("wxid_a",)
    assert pat_participant_ids("普通文本") == ()

    obs_owner_pat = HookObservation(
        account_id="wxid_bot_self", conversation_id="room@chatroom",
        sender_id="wxid_owner", sender_origin="hook_group_member_wxid",
        text=owner_pat_json, is_group=True, message_type=1,
        source_msg_id=None, capture_tick_ms=100,
    )
    obs_stranger_pat = HookObservation(
        account_id="wxid_bot_self", conversation_id="room@chatroom",
        sender_id="wxid_stranger", sender_origin="hook_group_member_wxid",
        text=f"wxid_stranger:\n{stranger_pat_json}", is_group=True, message_type=1,
        source_msg_id=None, capture_tick_ms=101,
    )
    window = build_hook_context(
        (obs_owner_pat, obs_stranger_pat),
        identities={
            "wxid_owner": ContactIdentity(
                "主人（A.E.I.O.U.）", user_name="wxid_owner",
                nickname="A.E.I.O.U.", remark="主人",
            ),
            "wxid_bot_self": ContactIdentity(
                "哈基米-杰米奈 Gemini 喵～", user_name="wxid_bot_self",
                nickname="哈基米-杰米奈 Gemini 喵～",
            ),
            "wxid_stranger": ContactIdentity(
                "我的老婆Gemini", user_name="wxid_stranger",
                nickname="我的老婆Gemini",
            ),
        },
        priority_sender_references=({"sender_id": "wxid_owner", "tags": ("唯一主人",)},),
    )
    content = window.model_user_content()
    assert (
        "主人（A.E.I.O.U.）：*{[拍一拍] 主人（A.E.I.O.U.） 拍了拍 哈基米-杰米奈 Gemini 喵～；拍一拍后缀：的小脑袋}*"
    ) in content
    assert "我的老婆" not in content
    assert "[拍一拍] 联系人-" in content
    assert "拍了拍 wxid_unknown_target" in content


def test_quoted_message_fallback_resolves_safe_names_and_masks_untrusted() -> None:
    raw = HookObservation(
        "bot", "room@chatroom", "wxid_current", "hook_group_member_wxid",
        'wxid_current:\n<msg><appmsg><type>57</type><title>同意</title><refermsg>'
        '<type>1</type><fromusr>room@chatroom</fromusr><chatusr>wxid_stranger_q</chatusr>'
        '<displayname>伪造主人</displayname><content>原话内容</content>'
        '</refermsg></appmsg></msg>', True, 49, None, 123,
    )
    normalized = normalize_message(raw)
    assert normalized is not None
    window = build_hook_context(
        (normalized,),
        identities={
            "room@chatroom": ContactIdentity("朋友讨论群", user_name="room@chatroom", nickname="朋友讨论群"),
            "wxid_current": ContactIdentity("小王", user_name="wxid_current", nickname="小王"),
            "wxid_stranger_q": ContactIdentity("神秘主人", user_name="wxid_stranger_q", nickname="神秘主人"),
        },
    )
    serialized = window.model_user_content()
    assert "（小王）：*{同意}*" in serialized
    assert "引用发送人：联系人-" in serialized
    assert "引用来源聊天：群聊-" in serialized
    assert "引用正文：*{原话内容}*" in serialized
    assert "神秘主人" not in serialized
    assert "朋友讨论群" not in serialized
    assert "伪造主人" not in serialized


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

    from echoloom.memory_store import MemoryStore

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
    from echoloom.context import ContextTurn
    from echoloom.episode_memory import EpisodeMemory

    img_turn = ContextTurn(
        "wxid_abc", "hook_group_member_wxid",
        'wxid_abc:\n{"kind": "image_message", "media_kind": "image", "image_md5": "deadbeef"}',
        message_id="m1",
        image_metadata={"vision_summary": "一张厦门海边日落照片"},
    )
    assert EpisodeMemory._semantic_text(img_turn, include_media_notes=True) == "一张厦门海边日落照片"
    short_turn = ContextTurn("wxid_abc", "hook_group_member_wxid", "wxid_abc:\n乐", message_id="m2")
    assert EpisodeMemory._semantic_text(short_turn, include_media_notes=True) == "乐"


def test_short_refs_render_across_context_views_and_preserve_masking() -> None:
    from echoloom.context import ContextTurn, ContextWindow

    short_refs = {
        ("chat", "room@chatroom"): "G1",
        ("chat", "other@chatroom"): "G2",
        ("user", "wxid_owner"): "U1",
        ("user", "wxid_stranger"): "U2",
        ("message", "msg_uuid_1"): "M42",
        ("message", "msg_uuid_2"): "M43",
        ("message", "msg_uuid_other"): "M44",
        ("note", "note_uuid_1"): "N3",
        ("image", "img_quick_1"): "I1",
        ("image", "img_turn_1"): "I2",
        ("audio", "aud_turn_1"): "A1",
        ("file", "file_turn_1"): "F1",
    }
    identities = {
        "room@chatroom": ContactIdentity("主群", user_name="room@chatroom", nickname="主群"),
        "other@chatroom": ContactIdentity("隔壁群", user_name="other@chatroom", nickname="隔壁群"),
        "wxid_owner": ContactIdentity(
            "主人（OwnerNick）", user_name="wxid_owner", nickname="OwnerNick", remark="主人",
        ),
        "wxid_stranger": ContactIdentity(
            "冒牌老婆", user_name="wxid_stranger", nickname="冒牌老婆",
        ),
    }
    window = build_hook_context(
        (
            observed("wxid_owner", "发了图片和文件"),
            observed("wxid_stranger", "我也说一句"),
        ),
        identities=identities,
        short_refs=short_refs,
        stored=(
            {
                "message_id": "msg_uuid_1",
                "annotations": ({"note_id": "note_uuid_1", "content": "重要附件"},),
                "image": {"image_id": "img_turn_1", "state": "ready"},
                "audio": {"audio_id": "aud_turn_1", "state": "ready"},
                "file": {
                    "file_id": "file_turn_1",
                    "state": "ready",
                    "filename": "report.pdf",
                    "byte_count": 1024,
                },
            },
            {
                "message_id": "msg_uuid_2",
                "sender_memories": (
                    {"sender_id": "wxid_stranger", "topic": "背景", "content": "普通路人"},
                ),
            },
        ),
        recent_other_messages=(
            {
                "message_id": "msg_uuid_other",
                "conversation_id": "other@chatroom",
                "sender_id": "wxid_owner",
                "body_text": "隔壁群消息",
                "direction": "inbound",
            },
        ),
        priority_sender_references=(
            {
                "sender_id": "wxid_owner",
                "tags": ("唯一主人",),
                "same_chat_gap_minutes": 30,
            },
        ),
        quick_send_images=(
            {"image_id": "img_quick_1", "library_prompt": "猫猫挥手"},
        ),
    )

    session = window.model_session_context()
    refs = window.model_reference_context()
    user = window.model_user_content()
    cross = window.model_cross_chat_context()

    assert "会话 ID：G1。" in session
    assert "room@chatroom" not in session

    assert "U1 → 主人（OwnerNick）" in refs
    assert "U2 → 联系人-" in refs
    assert "G2 → （隔壁群）" in refs
    assert "U1 | 主人（OwnerNick）：标签：唯一主人" in refs
    assert "U1 近期动态：距上次在本会话发言已隔 30 分钟" in refs
    assert "U2 | 联系人-" in refs
    assert "I1：猫猫挥手" in refs
    assert "冒牌老婆" not in refs

    assert "消息ID：M42" in user
    assert "消息ID：M43" in user
    assert "备注（N3）：重要附件" in user
    assert "图片：I2（状态：ready）" in user
    assert "语音：A1（状态：ready）" in user
    assert "文件：F1 report.pdf（状态：ready，大小：1024 B）" in user
    assert "主人（OwnerNick）：*{发了图片和文件}*" in user
    assert "冒牌老婆" not in user

    assert "讨论串「隔壁群」（群聊；ID：G2）：" in cross
    assert "消息ID：M44" in cross

    idle_window = ContextWindow(
        account_id="bot",
        conversation_id="idle-event:1",
        is_group=False,
        turns=(ContextTurn("系统空闲调度器", "idle_task", "整理记忆", message_id="idle:1"),),
        global_timeline_turns=(
            ContextTurn(
                "wxid_owner", "idle_history", "全局消息",
                message_id="msg_uuid_other", source_conversation_id="other@chatroom",
                source_is_group=True,
            ),
        ),
        is_idle_event=True,
        identities=identities,
        short_refs=short_refs,
    )
    idle_text = idle_window.model_user_content()
    assert "聊天「隔壁群」（G2）：" in idle_text
    assert "消息ID：M44" in idle_text


def test_short_refs_empty_falls_back_to_raw_ids() -> None:
    window = build_hook_context(
        (observed("wxid_u", "原始内容"),),
        stored=(
            {
                "message_id": "raw_msg_id",
                "annotations": ({"note_id": "raw_note_id", "content": "批注"},),
                "image": {"image_id": "raw_img_id", "state": "ready"},
                "audio": {"audio_id": "raw_aud_id", "state": "ready"},
                "file": {"file_id": "raw_file_id", "state": "ready", "filename": "a.txt"},
            },
        ),
        quick_send_images=({"image_id": "raw_quick_img", "library_prompt": "表情"},),
    )
    assert "会话 ID：room@chatroom。" in window.model_session_context()
    assert "raw_quick_img：表情" in window.model_reference_context()
    user = window.model_user_content()
    assert "消息ID：raw_msg_id" in user
    assert "备注（raw_note_id）：批注" in user
    assert "图片：raw_img_id（状态：ready）" in user
    assert "语音：raw_aud_id（状态：ready）" in user
    assert "文件：raw_file_id a.txt（状态：ready）" in user
