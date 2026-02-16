"""TikTok scraper using ScrapeCreators API — 1 credit per keyword search.

Uses /v1/tiktok/search/keyword for targeted financial content.
Categories rotate per cycle: finance, consumer, employee/workplace.
Budget: ~2 credits per cycle x every 600s = ~288 credits/day.
"""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.scrapecreators.com"
_SEARCH_ENDPOINT = f"{_BASE_URL}/v1/tiktok/search/keyword"

# Financial keyword categories for TikTok search
_CATEGORIES: list[tuple[str, list[str]]] = [
    ("finance_direct", [
        "stock market crash", "investing tips 2026", "options trading",
        "earnings report", "short squeeze", "fed meeting",
        "bull market", "bear market", "dividend stocks",
        "penny stocks", "day trading strategy", "crypto news",
        "bitcoin price", "nasdaq", "sp500", "market analysis",
    ]),
    ("consumer_behavior", [
        "boycott brand", "shrinkflation exposed", "cost of living crisis",
        "deinfluencing", "product review fail", "subscription cancel",
        "overpriced products", "budget friendly alternative",
        "brand quality decline", "restaurant prices",
    ]),
    ("employee_workplace", [
        "layoffs tech 2026", "fired from job", "hiring freeze",
        "return to office mandate", "toxic workplace", "remote work ending",
        "salary transparency", "quit my job", "startup layoffs",
        "big tech layoffs", "mass layoff", "job market",
    ]),
]


class TikTokScrapeCreatorsScraper(BaseScraper):
    """TikTok scraper via ScrapeCreators. 1 credit per keyword search."""

    @property
    def platform(self) -> str:
        return "tiktok"

    def __init__(self, interval: int = 600) -> None:
        super().__init__(interval)
        self._api_key = get_settings().scrapecreators_api_key
        self._cycle_index: int = 0

    async def _search_keyword(
        self,
        client: httpx.AsyncClient,
        keyword: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Search TikTok for a keyword. 1 credit per request."""
        resp = await client.get(
            _SEARCH_ENDPOINT,
            headers={"x-api-key": self._api_key},
            params={"query": keyword, "sort_by": "relevance"},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("[tiktok/sc] search failed (%d) for '%s'", resp.status_code, keyword)
            return []

        data = resp.json()
        posts: list[dict[str, Any]] = []

        # Response format: {"search_item_list": [{"aweme_info": {...}}, ...], "cursor": N}
        items = data.get("search_item_list", [])
        for item in items:
            info = item.get("aweme_info", item)
            video_id = str(info.get("aweme_id", "") or info.get("id", ""))
            if not video_id:
                continue

            desc = info.get("desc", "") or ""
            stats = info.get("statistics", {})
            author = info.get("author", {})
            username = author.get("unique_id", "unknown")

            posts.append(self._make_post(
                source_id=video_id,
                author=f"@{username}",
                content=desc[:2000],
                url=f"https://www.tiktok.com/@{username}/video/{video_id}",
                raw_metadata={
                    "keyword_search": keyword,
                    "category": category,
                    "views": stats.get("play_count", 0),
                    "likes": stats.get("digg_count", 0),
                    "comments": stats.get("comment_count", 0),
                    "shares": stats.get("share_count", 0),
                    "author_followers": author.get("follower_count", 0),
                    "author_nickname": author.get("nickname", ""),
                    "create_time": info.get("create_time"),
                    "source": "scrapecreators",
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape one category per cycle, 2 random keywords from it."""
        cat_name, keywords = _CATEGORIES[self._cycle_index % len(_CATEGORIES)]
        self._cycle_index += 1

        sample = random.sample(keywords, min(2, len(keywords)))

        logger.info("[tiktok/sc] scraping '%s' keywords: %s", cat_name, sample)

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for kw in sample:
                try:
                    posts = await self._search_keyword(client, kw, cat_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[tiktok/sc] error for '%s'", kw, exc_info=True)

        return all_posts
