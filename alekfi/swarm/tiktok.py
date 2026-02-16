"""TikTok scraper — uses Apify API to scrape finance/brand content.

Three hashtag categories are rotated per scrape cycle to avoid rate limits:
  A) FINANCE DIRECT — market/stock/crypto hashtags
  B) CONSUMER BEHAVIOR — surges hit social weeks before earnings
  C) EMPLOYEE/WORKPLACE — layoffs, hiring, corporate sentiment
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
_TIKTOK_ACTOR = "clockworks~free-tiktok-scraper"

# ── Hashtag categories ────────────────────────────────────────────────

_FINANCE_DIRECT = [
    "stocktok", "fintok", "investing101", "stockpicks", "optionsTrading",
    "daytrader", "wallstreetbets", "bearmarket", "bullmarket", "marketcrash",
    "fedmeeting", "earnings", "shortsqueeze", "memestock", "cryptotok",
    "bitcoinnews", "etf", "sp500", "nasdaq", "pennystocks", "swingtrade",
    "technicalanalysis", "fundamentalanalysis", "dividends", "passiveincome",
    "wealthbuilding",
]

_CONSUMER_BEHAVIOR = [
    "boycott", "unboxing", "haul", "subscription", "canceled", "switching",
    "overrated", "underrated", "costoflivingcrisis", "shrinkflation",
    "budgetfriendly", "dupe", "alternative", "ripoff", "worthit",
    "notbuyingit", "deinfluencing", "brandreview", "productreview",
    "restaurantreview", "airlinereview", "hotelreview", "groceryhaul",
    "carshopping", "housemarket", "rentvsown",
]

_EMPLOYEE_WORKPLACE = [
    "layoffs", "fired", "laidoff", "quitmyjob", "toxicworkplace",
    "techworker", "corporatelife", "officelife", "returntooffice",
    "remotework", "hiringfreeze", "newjob", "joboffer", "salarytransparency",
    "worklifebalance", "startup", "faang", "bigtech",
]

_CATEGORIES: list[tuple[str, list[str]]] = [
    ("finance_direct", _FINANCE_DIRECT),
    ("consumer_behavior", _CONSUMER_BEHAVIOR),
    ("employee_workplace", _EMPLOYEE_WORKPLACE),
]

_RESULTS_PER_CATEGORY = 50


class TikTokScraper(BaseScraper):
    """Runs an Apify TikTok scraper actor and collects results.

    Rotates through one hashtag category per scrape cycle so that a full
    rotation covers all three categories in three consecutive runs.
    """

    @property
    def platform(self) -> str:
        return "tiktok"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key
        self._cycle_index: int = 0

    async def _run_actor(
        self,
        client: httpx.AsyncClient,
        hashtag: str,
        category: str,
    ) -> list[dict[str, Any]]:
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
                    "category": category,
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
        """Scrape one hashtag category per run, rotating through all three."""
        category_name, hashtags = _CATEGORIES[self._cycle_index % len(_CATEGORIES)]
        self._cycle_index += 1

        logger.info("[tiktok] scraping category '%s' (%d hashtags)", category_name, len(hashtags))

        # Calculate how many results per hashtag to aim for ~50 total
        hashtags_to_scrape = hashtags[:_RESULTS_PER_CATEGORY // 10]  # ~10 results per hashtag run

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for hashtag in hashtags_to_scrape:
                try:
                    posts = await self._run_actor(client, hashtag, category_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[tiktok] error scraping #%s", hashtag, exc_info=True)

                if len(all_posts) >= _RESULTS_PER_CATEGORY:
                    break

        return all_posts[:_RESULTS_PER_CATEGORY]


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TIKTOKS_FINANCE = [
    ("@wallsttrapper", "$NVDA just broke out of a bull flag on the daily chart. Target $800. This is NOT financial advice #stocks #investing #nvidia", ["stocks", "investing", "nvidia"], 890_000),
    ("@stocktok_daily", "Apple Vision Pro returns are reportedly at 50%. This could be Apple's biggest flop since the Newton #apple #visionpro", ["apple", "visionpro", "tech"], 1_700_000),
    ("@moneyhacks101", "POV: You just realized your $TSLA calls expire tomorrow and Elon is tweeting again #tesla #options #wallstreet", ["tesla", "options", "wallstreet"], 4_200_000),
    ("@retailinvestor_", "Lululemon vs Alo Yoga — which stock is the better buy right now? Full breakdown #lululemon #investing", ["lululemon", "investing", "retail"], 780_000),
    ("@cryptoking_official", "Bitcoin ETF approval means ONE thing: institutional money is coming. Are you positioned? #bitcoin #crypto #ETF", ["bitcoin", "crypto", "ETF"], 1_300_000),
    ("@economicsTikTok", "The housing market is about to crash and here are the 5 signs #housingmarket #realestate #crash", ["housingmarket", "realestate", "crash"], 6_800_000),
    ("@greeninvestor", "Why I'm going ALL IN on copper stocks. The green transition needs 3x more copper than we have #copper #investing #green", ["copper", "investing", "green"], 560_000),
    ("@dividendking", "These 3 dividend stocks pay 8%+ and have raised payouts for 25 straight years #dividends #passiveincome #stocktok", ["dividends", "passiveincome", "stocktok"], 1_400_000),
    ("@swingtrade_pro", "I caught a 40% move on $SMCI using this pennant breakout pattern #swingtrade #technicalanalysis #stockpicks", ["swingtrade", "technicalanalysis", "stockpicks"], 950_000),
    ("@fintok_sarah", "The Fed is holding rates AGAIN and here's what that means for your 401k #fedmeeting #sp500 #fintok", ["fedmeeting", "sp500", "fintok"], 2_300_000),
]

_MOCK_TIKTOKS_CONSUMER = [
    ("@financewithtina", "Nike quality has gone downhill SO bad. These shoes fell apart in 2 weeks. Never buying again #nike #boycott #productfail", ["nike", "boycott", "productfail"], 2_400_000),
    ("@thecloseteconomist", "Why is no one talking about Starbucks closing 200 stores?? This is a MASSIVE red flag for consumer spending #starbucks #economy", ["starbucks", "economy", "inflation"], 3_100_000),
    ("@genzbrokerage", "Temu products are literally falling apart. How is this company valued at $150B?? #temu #shein #cheapproducts", ["temu", "shein", "cheapproducts"], 5_600_000),
    ("@brandwatcher", "Chipotle portion sizes have gotten SO small. I measured them. This is shrinkflation in action #chipotle #shrinkflation", ["chipotle", "shrinkflation", "food"], 8_500_000),
    ("@sidehustlequeen", "I made $5K in a month selling on Amazon FBA. Here's exactly how #amazonfba #sidehustle #money", ["amazonfba", "sidehustle", "money"], 2_900_000),
    ("@luxurytok", "Louis Vuitton just raised prices AGAIN by 10%. When does luxury become a scam? #louisvuitton #luxury #LVMH", ["louisvuitton", "luxury", "LVMH"], 1_100_000),
    ("@foodtok_reviews", "McDonald's $18 Big Mac meal is why people are fed up with fast food prices #mcdonalds #inflation #foodprices", ["mcdonalds", "inflation", "foodprices"], 12_000_000),
    ("@deinfluence_her", "Stop buying Dyson Airwraps. The $30 dupe from Amazon works EXACTLY the same. I tested both. #deinfluencing #dupe #alternative", ["deinfluencing", "dupe", "alternative"], 7_200_000),
    ("@grocerycostmom", "My Costco haul cost $380 this week. Last year the same cart was $240. Show me where inflation is 3% #groceryhaul #costoflivingcrisis", ["groceryhaul", "costoflivingcrisis", "shrinkflation"], 4_800_000),
    ("@honest_reviews", "Canceled my Netflix, Hulu, AND Disney+ subscriptions. Streaming has become cable TV. Not worth it anymore. #canceled #subscription #ripoff", ["canceled", "subscription", "ripoff"], 3_600_000),
    ("@carbuyer_tony", "Dealers are marking up the new Toyota Camry by $8K over MSRP. Walked right out. #carshopping #ripoff #notbuyingit", ["carshopping", "ripoff", "notbuyingit"], 2_100_000),
    ("@housemarket_truth", "Rent is $2,400/month for a 1BR in Austin but the same house costs $500K to buy. The math makes NO sense. #rentvsown #housemarket", ["rentvsown", "housemarket", "costoflivingcrisis"], 5_300_000),
]

_MOCK_TIKTOKS_EMPLOYEE = [
    ("@marketmovers", "Boeing whistleblower just exposed MASSIVE safety issues. This is worse than the media is saying #boeing #safety #stocks", ["boeing", "safety", "stocks"], 3_400_000),
    ("@techworker_anon", "Just got laid off from Google after 8 years. No warning. 12,000 of us. Here's what it feels like. #layoffs #laidoff #bigtech", ["layoffs", "laidoff", "bigtech"], 9_500_000),
    ("@corporate_queen", "My company just mandated 5 days back in office. Morale has NEVER been lower. #returntooffice #corporatelife #worklifebalance", ["returntooffice", "corporatelife", "worklifebalance"], 6_100_000),
    ("@startup_reality", "I left my $200K FAANG job for a startup. 3 months later the startup ran out of money. #startup #faang #quitmyjob", ["startup", "faang", "quitmyjob"], 4_700_000),
    ("@salary_exposed", "I found out my coworker doing the SAME job makes $40K more. Here's what happened when I asked for a raise. #salarytransparency #toxicworkplace", ["salarytransparency", "toxicworkplace", "corporatelife"], 7_800_000),
    ("@recruiter_truth", "Companies posting fake job listings to collect resumes. The hiring freeze is REAL. #hiringfreeze #joboffer #fired", ["hiringfreeze", "joboffer", "fired"], 3_200_000),
    ("@remote_dev", "My manager said remote workers are 'less committed'. I literally work 12 hour days from home. #remotework #officelife #techworker", ["remotework", "officelife", "techworker"], 5_400_000),
    ("@newjob_journey", "Got an offer 40% above my old salary just by interviewing around. Always know your worth. #newjob #salarytransparency #joboffer", ["newjob", "salarytransparency", "joboffer"], 2_800_000),
]

_MOCK_TIKTOKS_ALL = [
    ("finance_direct", _MOCK_TIKTOKS_FINANCE),
    ("consumer_behavior", _MOCK_TIKTOKS_CONSUMER),
    ("employee_workplace", _MOCK_TIKTOKS_EMPLOYEE),
]


class MockTikTokScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "tiktok"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._cycle_index: int = 0

    async def scrape(self) -> list[dict[str, Any]]:
        # Rotate through categories just like the real scraper
        category_name, mock_list = _MOCK_TIKTOKS_ALL[self._cycle_index % len(_MOCK_TIKTOKS_ALL)]
        self._cycle_index += 1

        count = random.randint(15, 30)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            author, content, hashtags, views = random.choice(mock_list)
            vid = self._generate_id()
            posts.append(self._make_post(
                source_id=f"mock_{vid}",
                author=author,
                content=content,
                url=f"https://www.tiktok.com/{author}/video/{vid}",
                raw_metadata={
                    "category": category_name,
                    "hashtags": hashtags,
                    "views": views + random.randint(-100_000, 500_000),
                    "likes": random.randint(10_000, 500_000),
                    "comments": random.randint(500, 20_000),
                    "shares": random.randint(100, 10_000),
                    "author_followers": random.randint(50_000, 2_000_000),
                },
            ))
        return posts
