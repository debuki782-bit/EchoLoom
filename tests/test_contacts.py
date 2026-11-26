import json

import httpx

from echoloom.contacts import ContactIdentity, HookContactResolver, is_room_id


def test_resolver_uses_exact_user_name_and_only_takes_nickname_from_hook():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={
            "status": 0, "contact_found": True,
            "UserName": "wxid_member_123", "NickName": "微信昵称",
            "Remark": "本地备注",
        })

    resolver = HookContactResolver(transport=httpx.MockTransport(handler))
    expected = ContactIdentity("微信昵称", user_name="wxid_member_123",
                               nickname="微信昵称")
    assert resolver.resolve("wxid_member_123") == expected
    assert resolver.resolve("wxid_member_123") == expected
    assert calls == [{"wxid": "wxid_member_123"}]


def test_resolver_rejects_name_when_contact_user_name_does_not_match():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "status": 0, "contact_found": True,
            "UserName": "wxid_someone_else", "NickName": "不可关联的昵称",
        })

    resolver = HookContactResolver(transport=httpx.MockTransport(handler))
    assert resolver.resolve("wxid_member_123") is None


def test_resolver_does_not_query_non_contact_or_synthetic_ids():
    def handler(_: httpx.Request) -> httpx.Response:
        raise AssertionError("unstable IDs must not be queried")

    resolver = HookContactResolver(transport=httpx.MockTransport(handler))
    assert resolver.resolve("synth-member") is None


def test_group_room_id_resolves_by_exact_contact_username():
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={
            "status": 0, "contact_found": True,
            "UserName": "58959893267@chatroom", "NickName": "测试群名", "Remark": "",
        })

    resolver = HookContactResolver(transport=httpx.MockTransport(handler))
    assert is_room_id("58959893267@chatroom")
    assert resolver.resolve("58959893267@chatroom") == ContactIdentity(
        "测试群名", user_name="58959893267@chatroom", nickname="测试群名",
    )
    assert calls == [{"wxid": "58959893267@chatroom"}]
