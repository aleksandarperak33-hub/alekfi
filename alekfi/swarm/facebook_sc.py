"""Facebook scraper using ScrapeCreators API — 1 credit per page/group scrape.

Monitors financial news pages and brand pages for sentiment signals.
Budget: ~1 credit per cycle x every 900s = ~96 credits/day.
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
_POSTS_ENDPOINT = f"{_BASE_URL}/v1/facebook/profile/posts"

# ── Financial news pages ─────────────────────────────────────────────

_NEWS_PAGES = [
    "CNBC", "Bloomberg", "WallStreetJournal", "Forbes",
    "fortune", "BusinessInsider", "Reuters", "MarketWatch",
    "TheEconomist", "FinancialTimes", "Investopedia",
    "YahooFinance", "TheMotleyFool",
]

_BRAND_PAGES = [
    "Nike", "Apple", "Tesla", "Amazon", "Starbucks",
    "McDonalds", "Walmart", "Target", "Costco", "Netflix",
    "Disney", "Meta", "nvidia", "Microsoft",
    "Boeing", "ford", "toyota", "Samsung",
]

_CATEGORIES: list[tuple[str, list[str]]] = [
    ("financial_news", _NEWS_PAGES),
    ("brand_sentiment", _BRAND_PAGES),
]


class FacebookScrapeCreatorsScraper(BaseScraper):
    """Facebook page scraper via ScrapeCreators. 1 credit per page."""

    @property
    def platform(self) -> str:
        return "facebook"

    def __init__(self, interval: int = 900) -> None:
        super().__init__(interval)
        self._api_key = get_settings().scrapecreators_api_key
        self._cycle_index: int = 0

    async def _scrape_page(
        self,
        client: httpx.AsyncClient,
        page_name: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Scrape a Facebook page's recent posts. 1 credit."""
        resp = await client.get(
            _POSTS_ENDPOINT,
            headers={"x-api-key": self._api_key},
            params={"url": f"https://www.facebook.com/{page_name}"},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning(
                "[facebook/sc] page scrape failed (%d) for %s",
                resp.status_code, page_name,
            )
            return []

        data = resp.json()

        # Handle various response structures
        if isinstance(data, list):
            posts_data = data
        else:
            posts_data = data.get("posts", data.get("items", []))

        posts: list[dict[str, Any]] = []
        for item in posts_data[:10]:
            text = (
                item.get("text", "")
                or item.get("message", "")
                or item.get("content", "")
            )
            if not text:
                continue

            post_id = str(item.get("id", "") or self._generate_id())
            post_url = item.get("url", "") or item.get("link", "")

            posts.append(self._make_post(
                source_id=post_id,
                author=page_name,
                content=text[:2000],
                url=post_url or f"https://www.facebook.com/{page_name}",
                raw_metadata={
                    "page_name": page_name,
                    "category": category,
                    "likes": item.get("likes", 0) or item.get("likeCount", 0),
                    "comments": item.get("comments", 0) or item.get("commentCount", 0),
                    "shares": item.get("shares", 0) or item.get("shareCount", 0),
                    "published_at": item.get("time") or item.get("createdAt"),
                    "source": "scrapecreators",
                },
                source_published_at=item.get("time") or item.get("createdAt"),
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape 1 page per cycle, rotating through categories."""
        cat_name, pages = _CATEGORIES[self._cycle_index % len(_CATEGORIES)]
        self._cycle_index += 1

        page = random.choice(pages)
        logger.info("[facebook/sc] scraping page: %s (%s)", page, cat_name)

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            try:
                posts = await self._scrape_page(client, page, cat_name)
                all_posts.extend(posts)
            except Exception:
                logger.warning("[facebook/sc] error for %s", page, exc_info=True)

        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_FB_POSTS = [
    ("CNBC", "BREAKING: Federal Reserve holds rates steady at 5.25-5.50%. Powell signals cuts possible later in 2026 if inflation continues to cool. Markets react mixed.", 45000, 8200),
    ("Bloomberg", "Tesla's Q4 deliveries beat estimates: 512K vehicles delivered vs 480K expected. Stock surges 8% in premarket trading.", 32000, 5600),
    ("WallStreetJournal", "Exclusive: Goldman Sachs plans to lay off 3,200 employees in first quarter. Biggest cuts since 2008 financial crisis.", 28000, 9100),
    ("Forbes", "Nvidia surpasses Apple as world's most valuable company at $3.4T market cap. AI chip demand shows no signs of slowing.", 55000, 12000),
    ("Nike", "Introducing the Air Max Dn. Engineered with Dynamic Air technology for the next generation of athletes.", 120000, 3400),
    ("Starbucks", "Our new spring menu is here! Try the Lavender Oat Latte, available at all US locations starting today.", 89000, 14500),
    ("Tesla", "Model Y was the best-selling vehicle globally in 2025. Thank you to our customers for making electric the new standard.", 200000, 25000),
    ("Reuters", "OPEC+ agrees to extend production cuts through Q2 2026. Brent crude rises 4% to $85/barrel on the news.", 18000, 3200),
    ("MarketWatch", "Home prices hit new record high in January. National median now $420K. First-time buyers increasingly priced out of market.", 22000, 7800),
    ("Amazon", "Amazon to invest $15B in AI infrastructure over next 3 years. New data centers planned in Virginia, Oregon, and Ireland.", 65000, 8900),
    ("Disney", "Disney+ subscriber count drops for third consecutive quarter. Company announces price increase and ad-supported tier expansion.", 75000, 15000),
    ("Boeing", "Boeing completes safety review of 737 MAX fleet. FAA lifts delivery pause. Production to resume at reduced rate.", 42000, 11000),
]


class MockFacebookScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "facebook"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(8, 15)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            page, text, likes, comments = random.choice(_MOCK_FB_POSTS)
            pid = self._generate_id()
            news_pages = {"CNBC", "Bloomberg", "WallStreetJournal", "Forbes", "Reuters", "MarketWatch"}
            posts.append(self._make_post(
                source_id=f"mock_{pid}",
                author=page,
                content=text,
                url=f"https://www.facebook.com/{page}/posts/{pid}",
                raw_metadata={
                    "page_name": page,
                    "category": "financial_news" if page in news_pages else "brand_sentiment",
                    "likes": likes + random.randint(-5000, 10000),
                    "comments": comments + random.randint(-1000, 3000),
                    "shares": random.randint(500, 5000),
                },
            ))
        return posts
