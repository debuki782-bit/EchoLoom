import asyncio
import json

import httpx

from echoloom.bilibili import BilibiliReader
from echoloom.rich_message import normalize_message
from echoloom.wechat_gateway import MessageEvent


def test_long_shared_card_keeps_destination_but_not_attachment_secrets():
    item = MessageEvent("bot", "wxid_friend", "wxid_friend", "native_private_fromid",
        '<msg><appmsg><type>4</type><title>视频</title><des>UP主</des>'
        '<url>https://b23.tv/example?a=1&amp;b=2</url><appattach><aeskey>' +
        'private' * 1000 + '</aeskey></appattach></appmsg></msg>', False, 49, 1, 2)
    body = json.loads(normalize_message(item).text)
    assert body["url"] == "https://b23.tv/example?a=1&b=2"
    assert body["description"] == "UP主"
    assert "private" not in json.dumps(body)


def test_short_share_reads_metadata_and_never_claims_to_watch_video():
    requests = []

    def respond(request):
        requests.append(str(request.url))
        if request.url.host == "b23.tv":
            return httpx.Response(302, headers={"location": "https://www.bilibili.com/video/BV1234567890/"})
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {
                "title": "测试", "desc": "简介", "owner": {"name": "作者"}, "aid": 1, "cid": 2}})
        if request.url.path == "/x/player/v2":
            return httpx.Response(200, json={"code": -101})
        return httpx.Response(200, text="<html></html>")

    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read("https://b23.tv/test"))
    assert result["ok"] and result["content_kind"] == "video_metadata"
    assert result["video_watched"] is False
    assert "未获得公开字幕" in result["result"]
    assert len(requests) == 4


def test_redirect_cannot_fetch_outside_bilibili():
    seen = []

    def respond(request):
        seen.append(str(request.url))
        return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})

    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read("https://b23.tv/test"))
    assert result["ok"] is False
    assert len(seen) == 1


def test_article_reads_only_body_and_reports_truncation():
    html = '<nav>不要读导航</nav><div class="opus-module-content"><p>正文</p><p>' + '长' * 25000 + '</p></div><footer>不要读页尾</footer>'
    transport = httpx.MockTransport(lambda _: httpx.Response(200, text=html))
    result = asyncio.run(BilibiliReader(transport=transport).read("https://www.bilibili.com/opus/123"))
    assert result["ok"] and result["content_kind"] == "article"
    assert "正文" in result["result"] and "不要读" not in result["result"]
    assert result["truncated"] is True


def test_public_subtitles_are_distinguished_from_visual_content():
    def respond(request):
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {"title": "视频", "aid": 1, "cid": 2}})
        if request.url.path == "/x/player/v2":
            return httpx.Response(200, json={"code": 0, "data": {"subtitle": {"subtitles": [
                {"lan": "zh-CN", "subtitle_url": "//aisubtitle.hdslb.com/public.json"}]}}})
        if request.url.host == "aisubtitle.hdslb.com":
            return httpx.Response(200, json={"body": [{"content": "真实字幕"}]})
        return httpx.Response(200, text="<html></html>")

    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read(
        "https://www.bilibili.com/video/BV1234567890/"))
    assert result["ok"] and result["content_kind"] == "video_subtitles"
    assert "真实字幕" in result["result"] and result["video_watched"] is False


def _comment(rid, text="评论正文", **extra):
    return {"rpid": rid, "content": {"message": text}, "member": {"uname": "观众"},
            "like": 7, "rcount": 3, **extra}


def _comment_transport(data, requests):
    def respond(request):
        requests.append(request)
        if request.url.path == "/x/v2/reply":
            return httpx.Response(200, json={"code": 0, "data": data})
        raise AssertionError("Comment reads should not fetch the player or metadata")
    return httpx.MockTransport(respond)


def test_comments_are_on_demand_deduplicate_pins_and_omit_nested_replies():
    requests = []
    pin = _comment(1, "UP主置顶", replies=[_comment(99, "楼中楼不读取")])
    transport = _comment_transport({"top": {"upper": pin}, "replies": [pin, _comment(2), _comment(3)],
                                    "page": {"count": 100}}, requests)
    result = asyncio.run(BilibiliReader(transport=transport).read_comments(
        "https://www.bilibili.com/video/BV17x411w7KC/?share_session_id=private", limit=2))
    assert result["ok"] and result["returned_count"] == 2
    assert [row["comment_id"] for row in result["comments"]] == ["1", "2"]
    assert result["comments"][0]["pinned"] and result["comments"][0]["reply_count"] == 3
    assert "楼中楼不读取" not in json.dumps(result, ensure_ascii=False)
    assert "private" not in json.dumps(result)
    assert result["reported_total"] == 100 and result["sample_limited"]
    assert requests[-1].url.params["sort"] == "2" and requests[-1].url.params["ps"] == "2"
    assert len(requests) == 1  # Read comments without loading the video page or metadata.


def test_comment_text_and_total_result_have_byte_limits():
    requests = []
    rows = [_comment(i, "长评论" * 500) for i in range(1, 15)]
    transport = _comment_transport({"replies": rows}, requests)
    result = asyncio.run(BilibiliReader(transport=transport).read_comments(
        "https://www.bilibili.com/video/BV17x411w7KC/", limit=10, sort="latest"))
    assert result["ok"] and result["truncated"]
    assert 0 < result["returned_count"] <= 10
    assert all(len(c["text"].encode()) <= 600 for c in result["comments"])
    assert len(json.dumps(result["comments"], ensure_ascii=False).encode()) <= 5000
    assert requests[-1].url.params["sort"] == "0"


def test_empty_public_comment_section_is_success_not_access_failure():
    result = asyncio.run(BilibiliReader(transport=_comment_transport(
        {"replies": None, "top": None, "page": {"count": 0}}, [])).read_comments(
            "https://www.bilibili.com/video/BV17x411w7KC/"))
    assert result["ok"] and result["comments"] == [] and result["reported_total"] == 0


def test_comment_access_rejection_does_not_become_empty_comments():
    def respond(request):
        if request.url.path == "/x/v2/reply":
            return httpx.Response(200, json={"code": -101, "message": "need login"})
        return httpx.Response(200, text="<html></html>")
    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read_comments(
        "https://www.bilibili.com/video/av123/"))
    assert not result["ok"] and "comments" not in result


def test_comment_limits_and_shortlink_redirects_are_validated_before_fetch():
    requests = []
    def redirect(request):
        requests.append(request)
        return httpx.Response(302, headers={"location": "https://127.0.0.1/private"})
    reader = BilibiliReader(transport=httpx.MockTransport(redirect))
    assert not asyncio.run(reader.read_comments("https://b23.tv/a", limit=11))["ok"]
    assert not asyncio.run(reader.read_comments("https://b23.tv/a", limit=True))["ok"]
    assert not asyncio.run(reader.read_comments("https://b23.tv/a", sort="all"))["ok"]
    assert requests == []
    assert not asyncio.run(reader.read_comments("https://b23.tv/a"))["ok"]
    assert len(requests) == 1


def test_comment_video_identifiers_match_independently_observed_page_ids():
    assert BilibiliReader._video_aid("BV17x411w7KC") == 170001
    assert BilibiliReader._video_aid("BV1vr4p6oEqx") == 117575837029690
    assert BilibiliReader._video_aid("BV1edaD6SESZ") == 117371910101671
    assert BilibiliReader._video_aid("av170001") == 170001


def test_unified_video_read_resolves_share_once_preserves_part_and_includes_comments():
    requests = []
    def respond(request):
        requests.append(request)
        if request.url.host == "b23.tv":
            return httpx.Response(302, headers={"location":
                "https://www.bilibili.com/video/BV17x411w7KC/?p=2&share_session_id=private"})
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {"aid": 170001,
                "cid": 11, "bvid": "BV17x411w7KC", "title": "视频标题", "desc": "简介",
                "pages": [{"page": 2, "cid": 22, "part": "第二部分"}]}})
        if request.url.path == "/x/player/v2":
            assert request.url.params["cid"] == "22"
            return httpx.Response(200, json={"code": 0, "data": {}})
        assert request.url.path == "/x/v2/reply"
        assert request.url.params["ps"] == "3" and request.url.params["sort"] == "0"
        return httpx.Response(200, json={"code": 0, "data": {"replies": [_comment(1)]}})
    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read_video_page(
        "https://b23.tv/test", comment_limit=3, comment_sort="latest"))
    assert result["ok"] and not result["partial"] and result["video_watched"] is False
    assert "视频标题" in result["result"] and "第二部分" in result["result"]
    assert result["comments"]["returned_count"] == 1
    assert len(requests) == 4
    assert "private" not in json.dumps(result)


def test_unified_video_read_reports_partial_comments_when_video_metadata_is_unavailable():
    def respond(request):
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(412)
        assert request.url.path == "/x/v2/reply"
        return httpx.Response(200, json={"code": 0, "data": {"replies": [_comment(1)]}})
    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read_video_page(
        "https://www.bilibili.com/video/av170001/"))
    assert result["ok"] and result["partial"] and not result["video_info"]["ok"]
    assert result["comments"]["returned_count"] == 1 and result["video_watched"] is False


def test_unified_video_read_keeps_metadata_when_comments_are_unavailable():
    def respond(request):
        if request.url.path == "/x/web-interface/view":
            return httpx.Response(200, json={"code": 0, "data": {"title": "标题"}})
        return httpx.Response(200, json={"code": -101})
    result = asyncio.run(BilibiliReader(transport=httpx.MockTransport(respond)).read_video_page(
        "https://www.bilibili.com/video/av170001/"))
    assert result["ok"] and result["partial"] and "标题" in result["result"]
    assert not result["comments"]["ok"]


def test_video_site_adapter_ignores_nonvideo_urls_without_network_requests():
    def forbidden(request):
        raise AssertionError("nonvideo routes belong to the general page reader")
    reader = BilibiliReader(transport=httpx.MockTransport(forbidden))
    assert asyncio.run(reader.read_video_page("https://www.bilibili.com/read/cv123")) is None
    assert asyncio.run(reader.read_video_page("https://www.bilibili.com/opus/123")) is None
