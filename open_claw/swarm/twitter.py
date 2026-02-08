"""Twitter/X scraper — DORMANT until TWITTER_BEARER_TOKEN is configured.

Uses Twitter API v2 recent search endpoint via httpx.
"""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_TWITTER_SEARCH = "https://api.twitter.com/2/tweets/search/recent"
_SEARCH_QUERIES = [
    "$AAPL OR $TSLA OR $NVDA OR $MSFT OR $AMZN lang:en -is:retweet",
    "$SPY OR $QQQ OR #stockmarket OR #earnings lang:en -is:retweet",
    "#oil OR #gold OR #commodities OR $CL_F OR $GC_F lang:en -is:retweet",
    "Federal Reserve OR #inflation OR CPI OR interest rates lang:en -is:retweet",
    "$BTC OR $ETH OR #crypto OR #bitcoin lang:en -is:retweet",
    "Boeing OR Tesla recall OR FDA approval lang:en -is:retweet",
    "OPEC OR sanctions OR tariffs OR trade war lang:en -is:retweet",
    "layoffs OR hiring freeze OR restructuring lang:en -is:retweet",
]

_TWEET_FIELDS = "author_id,created_at,public_metrics,entities,source"
_USER_FIELDS = "name,username,verified,public_metrics"
_MAX_RESULTS = 100


class TwitterScraper(BaseScraper):
    """Searches Twitter API v2 for financial content. Handles rate limits and pagination."""

    @property
    def platform(self) -> str:
        return "twitter"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._token = get_settings().twitter_bearer_token
        self._seen_ids: set[str] = set()

    async def _search(self, client: httpx.AsyncClient, query: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        next_token: str | None = None

        for _ in range(3):
            params: dict[str, Any] = {
                "query": query,
                "max_results": min(_MAX_RESULTS, 100),
                "tweet.fields": _TWEET_FIELDS,
                "expansions": "author_id",
                "user.fields": _USER_FIELDS,
            }
            if next_token:
                params["next_token"] = next_token

            resp = await client.get(
                _TWITTER_SEARCH,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                logger.warning("[twitter] rate limited, backing off %ds", retry_after)
                import asyncio
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code != 200:
                logger.warning("[twitter] search failed (%d): %s", resp.status_code, query)
                break

            data = resp.json()
            users_map: dict[str, dict] = {}
            for user in data.get("includes", {}).get("users", []):
                users_map[user["id"]] = user

            for tweet in data.get("data", []):
                tid = tweet["id"]
                if tid in self._seen_ids:
                    continue
                self._seen_ids.add(tid)

                author_id = tweet.get("author_id", "")
                user = users_map.get(author_id, {})
                metrics = tweet.get("public_metrics", {})

                posts.append(self._make_post(
                    source_id=tid,
                    author=f"@{user.get('username', 'unknown')}",
                    content=tweet.get("text", ""),
                    url=f"https://twitter.com/{user.get('username', 'i')}/status/{tid}",
                    raw_metadata={
                        "author_name": user.get("name", ""),
                        "author_followers": user.get("public_metrics", {}).get("followers_count", 0),
                        "author_verified": user.get("verified", False),
                        "retweet_count": metrics.get("retweet_count", 0),
                        "like_count": metrics.get("like_count", 0),
                        "reply_count": metrics.get("reply_count", 0),
                        "quote_count": metrics.get("quote_count", 0),
                        "created_at": tweet.get("created_at"),
                        "search_query": query,
                        "entities": tweet.get("entities", {}),
                    },
                ))

            next_token = data.get("meta", {}).get("next_token")
            if not next_token:
                break

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for query in _SEARCH_QUERIES:
                try:
                    posts = await self._search(client, query)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[twitter] error for query: %s", query, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TWEETS = [
    ("@jimcramer", "Jim Cramer", 2_100_000, "$AAPL looking strong here. Services segment is the story. Buy buy buy!"),
    ("@elikisos", "Eli Kisos", 450_000, "BREAKING: Boeing halts 737 MAX deliveries after new door plug concern. $BA tanking after hours."),
    ("@DeItaone", "Walter Bloomberg", 890_000, "FED'S POWELL: WE NEED GREATER CONFIDENCE THAT INFLATION IS MOVING SUSTAINABLY TO 2%"),
    ("@unusual_whales", "unusual_whales", 1_500_000, "ALERT: $NVDA call sweep — 10,000 contracts of the $800 strike expiring March. $15M in premium."),
    ("@zaborowska", "Karolina Zaborowska", 320_000, "Tesla just recalled 2.2M vehicles in the US. That's basically every Tesla on American roads. $TSLA"),
    ("@CathieDWood", "Cathie Wood", 1_800_000, "AI is the biggest technological transformation in history. $TSLA robotaxi could be a $10T opportunity by 2030."),
    ("@GRDecter", "Gary Decter", 150_000, "Shell abandoning North Sea wind projects. Another blow to offshore wind. $SHEL moving to LNG."),
    ("@EricBalchunas", "Eric Balchunas", 340_000, "Bitcoin ETF update: IBIT saw $520M inflow today alone. BlackRock's BTC ETF is now larger than their silver ETF."),
    ("@mcaborsky", "Michael Aborsky", 210_000, "Copper at $4.20/lb and climbing. Chile production falling. EV demand surging. Best commodity trade of 2025."),
    ("@MattGoldstein26", "Matt Goldstein", 180_000, "JUST IN: NYCB stock halted. Reports of additional CRE losses. Regional bank contagion fears rising."),
    ("@profgalloway", "Scott Galloway", 950_000, "Nvidia's market cap just passed Amazon. One company makes AI chips. The other delivers everything. What a time."),
    ("@lisaabramowicz1", "Lisa Abramowicz", 670_000, "10-year Treasury yield hits 4.6%. Highest since November. Mortgage rates heading to 8%. Housing freeze continues."),
    ("@elerianm", "Mohamed El-Erian", 1_400_000, "The Fed is behind the curve — again. Inflation persistence is structural, not transitory. June cut is off the table."),
    ("@BobPisani", "Bob Pisani", 520_000, "Magnificent 7 now represents 33% of S&P 500 market cap. Market breadth remains a concern. $SPY"),
    ("@PeterSchiff", "Peter Schiff", 980_000, "Gold just hit $2,250. Central banks dumping dollars and buying gold. The de-dollarization trade is real."),
    ("@jessefelder", "Jesse Felder", 210_000, "Semiconductor equipment stocks rolling over. ASML, KLAC, AMAT all breaking down. Leading indicator for chip sector."),
    ("@leadlagreport", "Michael Gayed", 290_000, "Lumber is CRASHING. Down 30% in 2 months. This is historically a leading indicator for housing and the economy."),
    ("@SaijUppal", "Saij Uppal", 85_000, "Samsung foundry yields reportedly at 20% for 3nm. TSMC at 80%. The gap is widening. $TSM $005930.KS"),
    ("@zaborowska", "Karolina Zaborowska", 320_000, "LVMH reports record revenue. Luxury spending remains resilient among high-income consumers. $MC.PA"),
    ("@WatcherGuru", "Watcher.Guru", 2_800_000, "JUST IN: MicroStrategy buys another 12,000 Bitcoin worth $800M. Total holdings now 205,000 BTC."),
]


class MockTwitterScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "twitter"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(20, 40)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            handle, name, followers, text = random.choice(_MOCK_TWEETS)
            tid = str(random.randint(1_700_000_000_000_000_000, 1_800_000_000_000_000_000))
            posts.append(self._make_post(
                source_id=tid,
                author=handle,
                content=text,
                url=f"https://twitter.com/{handle.lstrip('@')}/status/{tid}",
                raw_metadata={
                    "author_name": name,
                    "author_followers": followers,
                    "author_verified": followers > 500_000,
                    "retweet_count": random.randint(50, 5_000),
                    "like_count": random.randint(100, 20_000),
                    "reply_count": random.randint(10, 2_000),
                    "quote_count": random.randint(5, 500),
                    "search_query": random.choice(_SEARCH_QUERIES),
                },
            ))
        return posts
