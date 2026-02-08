"""TikTok scraper — uses Apify API to scrape finance/brand content."""

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
_TIKTOK_ACTOR = "clockworks~free-tiktok-scraper"

_SEARCH_HASHTAGS = [
    "investing", "stockmarket", "finance", "stocks", "crypto",
    "personalfinance", "moneytok", "brandreview", "productreview",
    "boycott", "inflation", "sidehustle", "budgeting",
]


class TikTokScraper(BaseScraper):
    """Runs an Apify TikTok scraper actor and collects results."""

    @property
    def platform(self) -> str:
        return "tiktok"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key

    async def _run_actor(self, client: httpx.AsyncClient, hashtag: str) -> list[dict[str, Any]]:
        run_resp = await client.post(
            f"{_APIFY_API}/acts/{_TIKTOK_ACTOR}/runs",
            params={"token": self._api_key},
            json={
                "hashtags": [hashtag],
                "resultsPerPage": 10,
                "shouldDownloadVideos": False,
            },
            timeout=30,
        )
        if run_resp.status_code not in (200, 201):
            logger.warning("[tiktok] actor start failed (%d) for #%s", run_resp.status_code, hashtag)
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
                logger.warning("[tiktok] actor run %s ended with %s", run_id, status)
                return []
        else:
            logger.warning("[tiktok] actor run %s timed out waiting", run_id)
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
            video_id = str(item.get("id", self._generate_id()))
            desc = item.get("text", "") or item.get("desc", "")
            posts.append(self._make_post(
                source_id=video_id,
                author=item.get("authorMeta", {}).get("name", "unknown"),
                content=desc[:2000],
                url=item.get("webVideoUrl") or f"https://www.tiktok.com/@{item.get('authorMeta', {}).get('name', 'user')}/video/{video_id}",
                raw_metadata={
                    "hashtag_search": hashtag,
                    "hashtags": [h.get("name", "") for h in item.get("hashtags", [])],
                    "views": item.get("playCount", 0),
                    "likes": item.get("diggCount", 0),
                    "comments": item.get("commentCount", 0),
                    "shares": item.get("shareCount", 0),
                    "author_followers": item.get("authorMeta", {}).get("fans", 0),
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
                    logger.warning("[tiktok] error scraping #%s", hashtag, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TIKTOKS = [
    ("@financewithtina", "Nike quality has gone downhill SO bad. These shoes fell apart in 2 weeks. Never buying again #nike #boycott #productfail", ["nike", "boycott", "productfail"], 2_400_000),
    ("@wallsttrapper", "$NVDA just broke out of a bull flag on the daily chart. Target $800. This is NOT financial advice #stocks #investing #nvidia", ["stocks", "investing", "nvidia"], 890_000),
    ("@thecloseteconomist", "Why is no one talking about Starbucks closing 200 stores?? This is a MASSIVE red flag for consumer spending #starbucks #economy", ["starbucks", "economy", "inflation"], 3_100_000),
    ("@genzbrokerage", "Temu products are literally falling apart. How is this company valued at $150B?? #temu #shein #cheapproducts", ["temu", "shein", "cheapproducts"], 5_600_000),
    ("@stocktok_daily", "Apple Vision Pro returns are reportedly at 50%. This could be Apple's biggest flop since the Newton #apple #visionpro", ["apple", "visionpro", "tech"], 1_700_000),
    ("@moneyhacks101", "POV: You just realized your $TSLA calls expire tomorrow and Elon is tweeting again #tesla #options #wallstreet", ["tesla", "options", "wallstreet"], 4_200_000),
    ("@retailinvestor_", "Lululemon vs Alo Yoga — which stock is the better buy right now? Full breakdown #lululemon #investing", ["lululemon", "investing", "retail"], 780_000),
    ("@cryptoking_official", "Bitcoin ETF approval means ONE thing: institutional money is coming. Are you positioned? #bitcoin #crypto #ETF", ["bitcoin", "crypto", "ETF"], 1_300_000),
    ("@economicsTikTok", "The housing market is about to crash and here are the 5 signs #housingmarket #realestate #crash", ["housingmarket", "realestate", "crash"], 6_800_000),
    ("@brandwatcher", "Chipotle portion sizes have gotten SO small. I measured them. This is shrinkflation in action #chipotle #shrinkflation", ["chipotle", "shrinkflation", "food"], 8_500_000),
    ("@sidehustlequeen", "I made $5K in a month selling on Amazon FBA. Here's exactly how #amazonfba #sidehustle #money", ["amazonfba", "sidehustle", "money"], 2_900_000),
    ("@luxurytok", "Louis Vuitton just raised prices AGAIN by 10%. When does luxury become a scam? #louisvuitton #luxury #LVMH", ["louisvuitton", "luxury", "LVMH"], 1_100_000),
    ("@marketmovers", "Boeing whistleblower just exposed MASSIVE safety issues. This is worse than the media is saying #boeing #safety #stocks", ["boeing", "safety", "stocks"], 3_400_000),
    ("@greeninvestor", "Why I'm going ALL IN on copper stocks. The green transition needs 3x more copper than we have #copper #investing #green", ["copper", "investing", "green"], 560_000),
    ("@foodtok_reviews", "McDonald's $18 Big Mac meal is why people are fed up with fast food prices #mcdonalds #inflation #foodprices", ["mcdonalds", "inflation", "foodprices"], 12_000_000),
]


class MockTikTokScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "tiktok"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(15, 30)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            author, content, hashtags, views = random.choice(_MOCK_TIKTOKS)
            vid = self._generate_id()
            posts.append(self._make_post(
                source_id=f"mock_{vid}",
                author=author,
                content=content,
                url=f"https://www.tiktok.com/{author}/video/{vid}",
                raw_metadata={
                    "hashtags": hashtags,
                    "views": views + random.randint(-100_000, 500_000),
                    "likes": random.randint(10_000, 500_000),
                    "comments": random.randint(500, 20_000),
                    "shares": random.randint(100, 10_000),
                    "author_followers": random.randint(50_000, 2_000_000),
                },
            ))
        return posts
