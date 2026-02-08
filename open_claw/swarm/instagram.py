"""Instagram scraper — uses Apify API to scrape brand/financial content."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_APIFY_API = "https://api.apify.com/v2"
_IG_ACTOR = "apify~instagram-scraper"

_SEARCH_HASHTAGS = [
    "investing", "stockmarket", "finance", "luxurylifestyle", "realestate",
    "entrepreneur", "wealthbuilding", "personalfinance", "crypto", "startup",
]


class InstagramScraper(BaseScraper):
    """Runs an Apify Instagram scraper actor and collects results."""

    @property
    def platform(self) -> str:
        return "instagram"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key

    async def _run_actor(self, client: httpx.AsyncClient, hashtag: str) -> list[dict[str, Any]]:
        run_resp = await client.post(
            f"{_APIFY_API}/acts/{_IG_ACTOR}/runs",
            params={"token": self._api_key},
            json={
                "hashtags": [hashtag],
                "resultsLimit": 10,
                "resultsType": "posts",
            },
            timeout=30,
        )
        if run_resp.status_code not in (200, 201):
            logger.warning("[instagram] actor start failed (%d) for #%s", run_resp.status_code, hashtag)
            return []

        run_data = run_resp.json().get("data", {})
        run_id = run_data.get("id")
        if not run_id:
            return []

        for _ in range(30):
            await asyncio.sleep(5)
            status_resp = await client.get(
                f"{_APIFY_API}/actor-runs/{run_id}",
                params={"token": self._api_key},
                timeout=15,
            )
            if status_resp.status_code != 200:
                continue
            status = status_resp.json().get("data", {}).get("status")
            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                logger.warning("[instagram] actor run %s ended with %s", run_id, status)
                return []
        else:
            logger.warning("[instagram] actor run %s timed out waiting", run_id)
            return []

        dataset_id = run_data.get("defaultDatasetId")
        if not dataset_id:
            return []
        items_resp = await client.get(
            f"{_APIFY_API}/datasets/{dataset_id}/items",
            params={"token": self._api_key, "format": "json"},
            timeout=20,
        )
        if items_resp.status_code != 200:
            return []

        posts: list[dict[str, Any]] = []
        for item in items_resp.json():
            post_id = str(item.get("id", self._generate_id()))
            caption = item.get("caption", "") or ""
            posts.append(self._make_post(
                source_id=post_id,
                author=item.get("ownerUsername", "unknown"),
                content=caption[:2000],
                url=item.get("url") or f"https://www.instagram.com/p/{item.get('shortCode', post_id)}/",
                raw_metadata={
                    "hashtag_search": hashtag,
                    "hashtags": item.get("hashtags", []),
                    "likes": item.get("likesCount", 0),
                    "comments": item.get("commentsCount", 0),
                    "owner_followers": item.get("ownerFullName", ""),
                    "type": item.get("type", "image"),
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for hashtag in _SEARCH_HASHTAGS[:5]:
                try:
                    posts = await self._run_actor(client, hashtag)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[instagram] error scraping #%s", hashtag, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_IG_POSTS = [
    ("@hermes_official", "Introducing the new Birkin 25 in Gold Togo leather. Craftsmanship that stands the test of time. #hermes #luxury #birkin", 450_000, 2_800),
    ("@tesla", "Model Y is now the best-selling car globally. The future is electric. #tesla #EV #sustainability", 1_200_000, 15_000),
    ("@goldmansachs", "Our latest research: AI could boost global GDP by 7% over the next decade. Full report in bio. #AI #economics", 85_000, 1_200),
    ("@nike", "Just Do It. Introducing the new Air Max Dn — engineered for the next generation. #nike #airmax", 2_500_000, 18_000),
    ("@investwithclarissa", "I turned $5K into $50K in 6 months using this options strategy. Swipe for the breakdown ➡️ #investing #options #money", 340_000, 8_500),
    ("@louisvuitton", "The art of travel. LV Tambour watch collection. #louisvuitton #luxury #watches", 890_000, 5_200),
    ("@bloomberg", "BREAKING: Federal Reserve holds rates steady, signals cuts later this year. #fed #economy #markets", 120_000, 3_400),
    ("@realestate_mogul", "Just closed on a 200-unit apartment complex in Austin. Here's the cap rate and why I'm bullish on TX. #realestate #investing", 95_000, 2_100),
    ("@apple", "iPhone 16 Pro. The most powerful iPhone ever. #apple #iphone16 #innovation", 3_800_000, 25_000),
    ("@cryptobro_mike", "Ethereum staking yields are insane right now. 5.2% APY with zero effort. Here's how I set it up. #ethereum #crypto #defi", 210_000, 6_700),
    ("@rolex", "The Oyster Perpetual Submariner. A legend since 1953. #rolex #submariner #watches", 1_500_000, 8_900),
    ("@sustainableinvestor", "ESG investing isn't dead — it's evolving. Companies with strong ESG scores outperformed by 3% last quarter. #ESG #investing", 45_000, 890),
    ("@amazon", "Small business Saturday: 60% of our marketplace is powered by independent sellers. #amazon #smallbusiness", 560_000, 4_200),
    ("@lvmh", "LVMH reports record revenue of €86.2B. Luxury demand remains resilient. #LVMH #luxury #earnings", 78_000, 1_100),
    ("@wealthsimple", "You don't need to pick stocks. A diversified ETF portfolio outperforms 90% of hedge funds. Here's proof. #investing #ETF", 180_000, 4_500),
]


class MockInstagramScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "instagram"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            author, caption, likes, comments = random.choice(_MOCK_IG_POSTS)
            pid = self._generate_id()
            posts.append(self._make_post(
                source_id=f"mock_{pid}",
                author=author,
                content=caption,
                url=f"https://www.instagram.com/p/{pid}/",
                raw_metadata={
                    "hashtags": [w.lstrip("#") for w in caption.split() if w.startswith("#")],
                    "likes": likes + random.randint(-10_000, 50_000),
                    "comments": comments + random.randint(-200, 500),
                    "type": random.choice(["image", "carousel", "video"]),
                },
            ))
        return posts
