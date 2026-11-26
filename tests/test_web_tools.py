from datetime import UTC, datetime

from echoloom.storage import Repository
from echoloom.web_tools import WebSearchUsage, _public_https_url


def test_web_search_usage_counts_queries_and_estimates_overage(tmp_path):
    repo = Repository(tmp_path / "web-usage.sqlite3")
    try:
        ledger = WebSearchUsage(repo)
        ledger.record("account-a", 3, datetime(2026, 11, 1, tzinfo=UTC))
        ledger.record("account-a", 5, datetime(2026, 11, 2, tzinfo=UTC))
        ledger.record("account-b", 20, datetime(2026, 11, 2, tzinfo=UTC))
        assert ledger.month_count("account-a", "2026-11") == 8
        assert ledger.estimated_post_tier_microusd(5_000) == 0
        assert ledger.estimated_post_tier_microusd(5_100) == 1_400_000
    finally:
        repo.close()


def test_read_web_page_accepts_public_https_and_rejects_local_or_non_https():
    assert _public_https_url("https://example.com/page") == "https://example.com/page"
    assert _public_https_url("http://example.com/page") is None
    assert _public_https_url("https://localhost/private") is None
    assert _public_https_url("https://127.0.0.1/private") is None
    assert _public_https_url("https://user:password@example.com/page") is None


def test_page_tool_routes_bilibili_videos_and_keeps_only_one_read_tool(monkeypatch):
    import asyncio
    from types import SimpleNamespace

    from echoloom.bilibili import BilibiliReader
    from echoloom.web_tools import WebToolset

    calls = []
    async def video(self, url, **kwargs):
        calls.append(("video", url, kwargs))
        return {"ok": True, "comments": {"ok": True, "comments": []}}
    async def general(url, *, search):
        calls.append(("general", url, search))
        return {"ok": True}
    monkeypatch.setattr(BilibiliReader, "read_video_page", video)
    tools = object.__new__(WebToolset)
    tools.service = SimpleNamespace(run=general)
    assert {tool.name for tool in tools.agent_tools()} == {"search_web", "read_web_page"}
    result = asyncio.run(tools.read_web_page("https://b23.tv/test", comment_limit=3,
                                              comment_sort="latest"))
    assert result["comments"]["ok"]
    assert calls == [("video", "https://b23.tv/test", {"comment_limit": 3, "comment_sort": "latest"})]
    asyncio.run(tools.read_web_page("https://example.com/page"))
    assert calls[-1] == ("general", "https://example.com/page", False)


def test_nonvideo_bilibili_urls_use_general_reader_without_comment_requests():
    import asyncio
    from types import SimpleNamespace

    from echoloom.web_tools import WebToolset

    calls = []
    async def general(url, *, search):
        calls.append(url)
        return {"ok": True, "result": "普通网页正文"}
    tools = object.__new__(WebToolset)
    tools.service = SimpleNamespace(run=general)
    result = asyncio.run(tools.read_web_page("https://www.bilibili.com/opus/123"))
    assert result["result"] == "普通网页正文"
    assert calls == ["https://www.bilibili.com/opus/123"]
