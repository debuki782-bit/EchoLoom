import json

from wechat_agent.hook_contract import HookObservation
from wechat_agent.rich_message import normalize_message, quoted_author_id


def test_quote_separates_source_chat_from_exact_quoted_author_id():
    item = HookObservation("bot", "room@chatroom", "wxid_member", "hook_group_member_wxid",
        'wxid_member:\n<msg><appmsg><type>57</type><title>现在的回复</title><refermsg>'
        '<type>1</type><displayname>另一位成员</displayname>'
        '<fromusr>room@chatroom</fromusr><chatusr>wxid_quoted</chatusr>'
        '<content>被引用的正文</content></refermsg></appmsg></msg>', True, 49, None, 123)
    result = normalize_message(item)
    assert result is not None and result.message_type == 1
    assert result.original_message_type == 49
    body = json.loads(result.text)
    assert body["text"] == "现在的回复"
    assert body["quote"]["content"] == "被引用的正文"
    assert body["quote"]["source_conversation_id"] == "room@chatroom"
    assert body["quote"]["author_id"] == "wxid_quoted"
    assert body["quote"]["author_id_source"] == "chatusr"
    assert quoted_author_id(result.text) == "wxid_quoted"
    assert result.sender_id == "wxid_member"


def test_image_preserves_original_references_without_image_pixels():
    item = HookObservation("bot", "room@chatroom", "member", "hook_group_member_wxid",
                           '<msg><img cdnhdimgurl="https://wx.qpic.cn/a?sig=x&amp;y=z" '
                           'hdmd5="0123456789abcdef0123456789abcdef" aeskey="secret"/>'
                           '</msg>', True, 3, None, 123)
    result = normalize_message(item)
    assert result is not None and result.message_type == 1
    assert result.original_message_type == 3
    payload = json.loads(result.text)
    assert payload == {
        "kind": "image_message",
        "image_references": [
            {"field": "cdnhdimgurl", "value": "https://wx.qpic.cn/a?sig=x&amp;y=z"},
            {"field": "hdmd5", "value": "0123456789abcdef0123456789abcdef"},
        ],
    }
    assert "secret" not in result.text
    assert "image_bytes" not in result.text


def test_emoji_or_gif_is_identified_without_exposing_payload():
    item = HookObservation(
        "bot", "room@chatroom", "member", "hook_group_member_wxid",
        '<msg><emoji md5="private-token" cdnurl="https://example.invalid/gif"/></msg>',
        True, 47, None, 123,
    )

    result = normalize_message(item)

    assert result is not None
    assert result.message_type == 1
    assert result.original_message_type == 47
    payload = json.loads(result.text)
    assert payload == {
        "kind": "image_message", "media_kind": "sticker",
        "image_references": [
            {"field": "md5", "value": "private-token"},
            {"field": "cdnurl", "value": "https://example.invalid/gif"},
        ],
    }
    assert "image_bytes" not in result.text


def test_voice_callback_is_normalized_without_embedding_audio_bytes():
    item = HookObservation(
        "bot", "room@chatroom", "member", "hook_group_member_wxid",
        "<msg><voicemsg voicelength=\"1500\"/></msg>", True, 34, None, 123,
        raw_message_json=json.dumps({"voice_hex": "02232153494c4b5f5633"}),
    )

    result = normalize_message(item)

    assert result is not None
    assert result.message_type == 1
    assert result.original_message_type == 34
    assert json.loads(result.text) == {"kind": "voice_message"}
    assert "voice_hex" not in result.text
