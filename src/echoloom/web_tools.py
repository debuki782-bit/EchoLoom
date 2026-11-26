"""Framework-neutral public web tools and query-count storage."""

from __future__ import annotations

import ipaddress
import logging
import re
from datetime import UTC, datetime
from urllib.parse import urlsplit
from uuid import uuid4

from .storage import Repository
from .tool_timing import agent_function_tool

logger = logging.getLogger(__name__)
_SAFE_URL = re.compile(r"^https://", re.IGNORECASE)


class WebSearchUsage:
    """Store query counts; local totals cannot establish the Google free allowance."""

    PRICE_PER_QUERY_MICROUSD = 14_000  # $14 per 1,000 queries after the included tier.
    INCLUDED_MONTHLY_QUERIES = 5_000

    def __init__(self, repository: Repository) -> None:
        self.repository = repository
        with repository.transaction() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS web_search_usage ("
                "id TEXT PRIMARY KEY, account_id TEXT NOT NULL, month_utc TEXT NOT NULL, "
                "query_count INTEGER NOT NULL, created_at TEXT NOT NULL)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS web_search_usage_month "
                "ON web_search_usage(account_id, month_utc)"
            )

    def record(self, account_id: str, query_count: int, at: datetime | None = None) -> None:
        if not account_id or query_count < 1:
            return
        now = (at or datetime.now(UTC)).astimezone(UTC)
        with self.repository.transaction() as conn:
            conn.execute(
                "INSERT INTO web_search_usage VALUES (?,?,?,?,?)",
                (uuid4().hex, account_id, now.strftime("%Y-%m"), query_count, now.isoformat()),
            )

    def month_count(self, account_id: str, month_utc: str) -> int:
        with self.repository.read_lock() as conn:
            row = conn.execute(
                "SELECT coalesce(sum(query_count),0) FROM web_search_usage "
                "WHERE account_id=? AND month_utc=?",
                (account_id, month_utc),
            ).fetchone()
        return int(row[0])

    @classmethod
    def estimated_post_tier_microusd(cls, local_month_count: int) -> int:
        return (
            max(0, local_month_count - cls.INCLUDED_MONTHLY_QUERIES) * cls.PRICE_PER_QUERY_MICROUSD
        )


def _public_https_url(value: str) -> str | None:
    raw = value.strip()
    if len(raw) > 2048 or not _SAFE_URL.match(raw):
        return None
    try:
        parsed = urlsplit(raw)
        host = parsed.hostname
        if not host or parsed.username or parsed.password:
            return None
        if host.casefold() == "localhost" or host.casefold().endswith(".localhost"):
            return None
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if address is not None and not address.is_global:
            return None
        return raw
    except ValueError:
        return None


def _event_text(event) -> str:
    if not getattr(event, "content", None):
        return ""
    return "\n".join(
        part.text
        for part in (event.content.parts or [])
        if isinstance(getattr(part, "text", None), str) and part.text
        and not getattr(part, "thought", False)
    ).strip()


def _sources(grounding_metadata) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    for chunk in getattr(grounding_metadata, "grounding_chunks", None) or []:
        web = getattr(chunk, "web", None)
        uri = getattr(web, "uri", None) if web else None
        title = getattr(web, "title", None) if web else None
        if (
            isinstance(uri, str)
            and uri.startswith("https://")
            and all(uri != item[1] for item in found)
        ):
            found.append((str(title or "来源"), uri))
    return found[:8]


class WebToolset:
    """Read-only application tools; provider execution stays in its adapter."""
    def __init__(self, model, usage: WebSearchUsage, *, service=None):
        from .frameworks.google_web import GoogleWeb
        self.service = service if service is not None else GoogleWeb(model, usage)

    async def search_web(self, query: str) -> dict:
        query = " ".join(query.split())
        if not query or len(query) > 500:
            return {"ok": False, "error": "搜索词请写 1–500 个字符。"}
        return await self.service.run(query, search=True)

    async def read_web_page(self, url: str, comment_limit: int = 5,
                            comment_sort: str = "hot") -> dict:
        url = _public_https_url(url)
        if not url:
            return {"ok": False, "error": "请提供一个公开网页的 HTTPS 地址。"}
        from .bilibili import BilibiliReader, is_bilibili_url
        if is_bilibili_url(url):
            result = await BilibiliReader().read_video_page(
                url, comment_limit=comment_limit, comment_sort=comment_sort)
            if result is not None:
                return result
        return await self.service.run(url, search=False)

    def agent_tools(self):
        return [agent_function_tool(self.search_web), agent_function_tool(self.read_web_page)]

    def adk_tools(self):
        from .frameworks.adk import to_adk
        return [to_adk(tool) for tool in self.agent_tools()]
