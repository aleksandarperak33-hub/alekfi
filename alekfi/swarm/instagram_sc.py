"""Instagram scraper using ScrapeCreators API — 1 credit per profile/posts request.

Monitors finance influencer and brand profiles for new posts.
Categories rotate per cycle: finance news, brands, consumer sentiment.
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
_POSTS_ENDPOINT = f"{_BASE_URL}/v2/instagram/user/posts"

# ── Watchlists ────────────────────────────────────────────────────────

_FINANCE_NEWS = [
    "bloomberg", "wsj", "cnbc", "barrons", "fortunemag",
    "investopedia", "thestreet", "marketwatch",
    "wealthsimple", "nerdwallet",
    "goldmansachs", "jpmorgan", "morganstanley", "blackrock",
]

_BRANDS = [
    "nike", "apple", "tesla", "amazon", "starbucks",
    "mcdonalds", "walmart", "target", "costco", "netflix",
    "disney", "meta", "nvidia", "microsoft",
    "boeing", "ford", "toyota", "samsung",
]

_CONSUMER_SENTIMENT = [
    "deinfluencing", "budgetliving", "frugalliving",
    "inflation.chart", "overpriced.products",
    "sidehustlenation", "personalfinance",
]

_CATEGORIES: list[tuple[str, list[str]]] = [
    ("finance_news", _FINANCE_NEWS),
    ("brand_monitoring", _BRANDS),
    ("consumer_sentiment", _CONSUMER_SENTIMENT),
]


class InstagramScrapeCreatorsScraper(BaseScraper):
    """Instagram scraper via ScrapeCreators. 1 credit per profile posts fetch."""

    @property
    def platform(self) -> str:
        return "instagram"

    def __init__(self, interval: int = 600) -> None:
        super().__init__(interval)
        self._api_key = get_settings().scrapecreators_api_key
        self._cycle_index: int = 0

    async def _scrape_user_posts(
        self,
        client: httpx.AsyncClient,
        handle: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Get a user's recent posts. 1 credit per request."""
        resp = await client.get(
            _POSTS_ENDPOINT,
            headers={"x-api-key": self._api_key},
            params={"handle": handle},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning("[instagram/sc] posts failed (%d) for @%s", resp.status_code, handle)
            return []

        data = resp.json()
        posts: list[dict[str, Any]] = []

        # Try multiple response structures ScrapeCreators might use
        items = data.get("items", [])
        if not items:
            # Profile-style response
            media = data.get("edge_owner_to_timeline_media", {})
            edges = media.get("edges", [])
            items = [e.get("node", {}) for e in edges]
        if not items and isinstance(data, list):
            items = data

        for item in items[:10]:
            post_id = str(
                item.get("id", "")
                or item.get("pk", "")
                or self._generate_id()
            )
            shortcode = item.get("shortcode", "") or item.get("code", "")

            # Extract caption from various formats
            caption = ""
            cap_data = item.get("edge_media_to_caption", {})
            if cap_data:
                cap_edges = cap_data.get("edges", [])
                if cap_edges:
                    caption = cap_edges[0].get("node", {}).get("text", "")
            if not caption:
                cap_obj = item.get("caption", "")
                if isinstance(cap_obj, dict):
                    caption = cap_obj.get("text", "")
                elif isinstance(cap_obj, str):
                    caption = cap_obj

            if not caption:
                continue  # Skip posts with no text (images-only have low intel value)

            likes = (
                item.get("like_count", 0)
                or item.get("edge_liked_by", {}).get("count", 0)
            )
            comments = (
                item.get("comment_count", 0)
                or item.get("edge_media_to_comment", {}).get("count", 0)
            )

            posts.append(self._make_post(
                source_id=post_id,
                author=f"@{handle}",
                content=str(caption)[:2000],
                url=(
                    f"https://www.instagram.com/p/{shortcode}/"
                    if shortcode
                    else f"https://www.instagram.com/{handle}/"
                ),
                raw_metadata={
                    "handle": handle,
                    "category": category,
                    "likes": likes,
                    "comments": comments,
                    "type": item.get("media_type", item.get("__typename", "unknown")),
                    "taken_at": item.get("taken_at_timestamp") or item.get("taken_at"),
                    "source": "scrapecreators",
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape one category per cycle, 2 random profiles from it."""
        cat_name, handles = _CATEGORIES[self._cycle_index % len(_CATEGORIES)]
        self._cycle_index += 1

        sample = random.sample(handles, min(2, len(handles)))

        logger.info("[instagram/sc] scraping '%s' profiles: %s", cat_name, sample)

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for handle in sample:
                try:
                    posts = await self._scrape_user_posts(client, handle, cat_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[instagram/sc] error for @%s", handle, exc_info=True)

        return all_posts
