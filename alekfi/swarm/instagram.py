"""Instagram scraper — uses Apify API to scrape brand/financial content.

Four category types rotated per scrape cycle:
  A) FINANCE INFLUENCER — stock/trading/crypto hashtags
  B) CONSUMER BEHAVIOR — spending signals, deinfluencing, reviews
  C) BRAND MONITORING — engagement on major brand accounts
  D) (original hashtags kept in finance category)
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_APIFY_API = "https://api.apify.com/v2"
_IG_ACTOR = "apify~instagram-scraper"

# ── Hashtag categories ────────────────────────────────────────────────

_FINANCE_INFLUENCER = [
    "stocks", "trading", "investing", "wealthbuilding", "financialfreedom",
    "stockmarket", "daytrading", "options", "forex", "crypto", "bitcoin",
    "ethereum",
]

_CONSUMER_BEHAVIOR = [
    "boycott", "unboxing", "haul", "subscription", "canceled", "switching",
    "overrated", "underrated", "costoflivingcrisis", "shrinkflation",
    "budgetfriendly", "dupe", "alternative", "ripoff", "worthit",
    "deinfluencing", "brandreview", "productreview",
]

# Brand usernames to monitor for engagement/sentiment
_BRAND_USERNAMES = [
    "nike", "apple", "tesla", "amazon", "starbucks", "mcdonalds",
    "walmart", "target", "costco", "netflix", "disney", "google",
    "meta", "nvidia", "microsoft", "coca-cola", "pepsi", "boeing",
    "united", "delta", "jpmorgan", "goldmansachs", "blackrock",
    "lvmh", "hermes", "louisvuitton", "rolex", "gucci",
]

_CATEGORIES: list[tuple[str, str, list[str]]] = [
    # (category_name, scrape_type, items)
    # scrape_type: "hashtag" or "profile"
    ("finance_influencer", "hashtag", _FINANCE_INFLUENCER),
    ("consumer_behavior", "hashtag", _CONSUMER_BEHAVIOR),
    ("brand_monitoring", "profile", _BRAND_USERNAMES),
]

_RESULTS_PER_CATEGORY = 30


class InstagramScraper(BaseScraper):
    """Runs an Apify Instagram scraper actor and collects results.

    Rotates through one category per scrape cycle so that a full rotation
    covers all three categories in three consecutive runs.
    """

    @property
    def platform(self) -> str:
        return "instagram"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key
        self._cycle_index: int = 0

    async def _run_actor_hashtag(
        self,
        client: httpx.AsyncClient,
        hashtag: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Scrape posts by hashtag search."""
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
                    "category": category,
                    "hashtags": item.get("hashtags", []),
                    "likes": item.get("likesCount", 0),
                    "comments": item.get("commentsCount", 0),
                    "owner_followers": item.get("ownerFullName", ""),
                    "type": item.get("type", "image"),
                },
            ))
        return posts

    async def _run_actor_profile(
        self,
        client: httpx.AsyncClient,
        username: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Scrape recent posts from a brand profile."""
        run_resp = await client.post(
            f"{_APIFY_API}/acts/{_IG_ACTOR}/runs",
            params={"token": self._api_key},
            json={
                "directUrls": [f"https://www.instagram.com/{username}/"],
                "resultsLimit": 5,
                "resultsType": "posts",
            },
            timeout=30,
        )
        if run_resp.status_code not in (200, 201):
            logger.warning("[instagram] actor start failed (%d) for @%s", run_resp.status_code, username)
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
                author=item.get("ownerUsername", username),
                content=caption[:2000],
                url=item.get("url") or f"https://www.instagram.com/p/{item.get('shortCode', post_id)}/",
                raw_metadata={
                    "brand_username": username,
                    "category": category,
                    "hashtags": item.get("hashtags", []),
                    "likes": item.get("likesCount", 0),
                    "comments": item.get("commentsCount", 0),
                    "owner_followers": item.get("ownerFullName", ""),
                    "type": item.get("type", "image"),
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape one category per run, rotating through all three."""
        category_name, scrape_type, items = _CATEGORIES[self._cycle_index % len(_CATEGORIES)]
        self._cycle_index += 1

        logger.info("[instagram] scraping category '%s' (%s, %d items)", category_name, scrape_type, len(items))

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for item in items:
                try:
                    if scrape_type == "hashtag":
                        posts = await self._run_actor_hashtag(client, item, category_name)
                    else:
                        posts = await self._run_actor_profile(client, item, category_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[instagram] error scraping %s", item, exc_info=True)

                if len(all_posts) >= _RESULTS_PER_CATEGORY:
                    break

        return all_posts[:_RESULTS_PER_CATEGORY]


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_IG_FINANCE = [
    ("@investwithclarissa", "I turned $5K into $50K in 6 months using this options strategy. Swipe for the breakdown #investing #options #money", 340_000, 8_500),
    ("@bloomberg", "BREAKING: Federal Reserve holds rates steady, signals cuts later this year. #fed #economy #markets", 120_000, 3_400),
    ("@realestate_mogul", "Just closed on a 200-unit apartment complex in Austin. Here's the cap rate and why I'm bullish on TX. #realestate #investing", 95_000, 2_100),
    ("@cryptobro_mike", "Ethereum staking yields are insane right now. 5.2% APY with zero effort. Here's how I set it up. #ethereum #crypto #defi", 210_000, 6_700),
    ("@sustainableinvestor", "ESG investing isn't dead -- it's evolving. Companies with strong ESG scores outperformed by 3% last quarter. #ESG #investing", 45_000, 890),
    ("@wealthsimple", "You don't need to pick stocks. A diversified ETF portfolio outperforms 90% of hedge funds. Here's proof. #investing #ETF", 180_000, 4_500),
    ("@daytrader_mike", "Caught a beautiful $TSLA bounce at the 200 EMA today. $2,400 profit in 15 minutes. Chart attached. #daytrading #stocks #trading", 275_000, 7_200),
    ("@forex_signals", "EUR/USD breaking out of a 3-month range. My target is 1.12. Here's the setup. #forex #trading #financialfreedom", 160_000, 3_800),
    ("@bitcoin_maxi", "Bitcoin just reclaimed $60K. Next stop $100K. The halvening cycle never lies. #bitcoin #crypto #wealthbuilding", 520_000, 12_000),
]

_MOCK_IG_CONSUMER = [
    ("@deinfluencer_real", "You do NOT need the Stanley cup. Your $8 water bottle works fine. Companies are manipulating you. #deinfluencing #worthit #budgetfriendly", 890_000, 15_000),
    ("@boycott_now", "Still boycotting Starbucks. Down 15% this quarter. Turns out consumers DO have power. #boycott #starbucks #switching", 430_000, 9_200),
    ("@shrinkflation_watch", "Doritos bag now has 20% less chips but costs $1 more. This is robbery in plain sight. #shrinkflation #ripoff #costoflivingcrisis", 1_200_000, 22_000),
    ("@haul_queen", "My entire SHEIN haul for under $50. 12 items. But wait till you see the quality... #haul #unboxing #productreview", 650_000, 11_000),
    ("@honest_reviewer", "I tried every subscription box and ranked them. Most are NOT worth it. Thread. #subscription #brandreview #overrated", 280_000, 5_600),
    ("@dupe_finder", "The $15 Trader Joe's dupe for La Mer cream. I'm not joking. Ingredients are 90% identical. #dupe #alternative #budgetfriendly", 970_000, 18_500),
    ("@switch_stories", "Canceled my gym membership after they raised prices 40%. Found a $10/month alternative that's better. #canceled #switching #underrated", 340_000, 7_800),
]

_MOCK_IG_BRAND = [
    ("@hermes_official", "Introducing the new Birkin 25 in Gold Togo leather. Craftsmanship that stands the test of time. #hermes #luxury #birkin", 450_000, 2_800),
    ("@tesla", "Model Y is now the best-selling car globally. The future is electric. #tesla #EV #sustainability", 1_200_000, 15_000),
    ("@goldmansachs", "Our latest research: AI could boost global GDP by 7% over the next decade. Full report in bio. #AI #economics", 85_000, 1_200),
    ("@nike", "Just Do It. Introducing the new Air Max Dn -- engineered for the next generation. #nike #airmax", 2_500_000, 18_000),
    ("@louisvuitton", "The art of travel. LV Tambour watch collection. #louisvuitton #luxury #watches", 890_000, 5_200),
    ("@apple", "iPhone 16 Pro. The most powerful iPhone ever. #apple #iphone16 #innovation", 3_800_000, 25_000),
    ("@rolex", "The Oyster Perpetual Submariner. A legend since 1953. #rolex #submariner #watches", 1_500_000, 8_900),
    ("@amazon", "Small business Saturday: 60% of our marketplace is powered by independent sellers. #amazon #smallbusiness", 560_000, 4_200),
    ("@lvmh", "LVMH reports record revenue of 86.2B EUR. Luxury demand remains resilient. #LVMH #luxury #earnings", 78_000, 1_100),
    ("@starbucks", "Introducing our new spring menu. Lavender Oat Latte available now. #starbucks #spring #newdrink", 920_000, 14_000),
    ("@walmart", "Rollback prices on 5,000+ items this week. Saving you more, every day. #walmart #savings", 340_000, 3_100),
    ("@netflix", "Squid Game Season 3 drops this Friday. Are you ready? #netflix #squidgame", 4_200_000, 35_000),
]

_MOCK_IG_ALL = [
    ("finance_influencer", _MOCK_IG_FINANCE),
    ("consumer_behavior", _MOCK_IG_CONSUMER),
    ("brand_monitoring", _MOCK_IG_BRAND),
]


class MockInstagramScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "instagram"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._cycle_index: int = 0

    async def scrape(self) -> list[dict[str, Any]]:
        # Rotate through categories just like the real scraper
        category_name, mock_list = _MOCK_IG_ALL[self._cycle_index % len(_MOCK_IG_ALL)]
        self._cycle_index += 1

        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            author, caption, likes, comments = random.choice(mock_list)
            pid = self._generate_id()
            posts.append(self._make_post(
                source_id=f"mock_{pid}",
                author=author,
                content=caption,
                url=f"https://www.instagram.com/p/{pid}/",
                raw_metadata={
                    "category": category_name,
                    "hashtags": [w.lstrip("#") for w in caption.split() if w.startswith("#")],
                    "likes": likes + random.randint(-10_000, 50_000),
                    "comments": comments + random.randint(-200, 500),
                    "type": random.choice(["image", "carousel", "video"]),
                },
            ))
        return posts
