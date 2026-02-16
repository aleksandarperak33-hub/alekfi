"""Twitter/X scraper using TwitterAPI.io — economical alternative to official API v2.

Pricing: ~$0.00015 per request (15 credits, 1 USD = 100K credits).
Auth: X-API-Key header.
Supports full Twitter search operators (cashtags, from:, lang:, etc.)
Rotates through 9 query categories (2 per cycle) to keep costs low.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.twitterapi.io"
_SEARCH_ENDPOINT = f"{_BASE_URL}/twitter/tweet/advanced_search"

# Same query categories as twitter.py — TwitterAPI.io supports identical operators
_QUERY_CATEGORIES: list[tuple[str, list[str]]] = [
    ("mega_cap", [
        "$AAPL OR $TSLA OR $NVDA OR $MSFT OR $AMZN lang:en -is:retweet",
        "$GOOG OR $META OR $NFLX OR $BRK.B OR $JPM lang:en -is:retweet",
    ]),
    ("macro", [
        "Federal Reserve OR #inflation OR CPI OR interest rates lang:en -is:retweet",
        "#oil OR #gold OR #commodities OR $CL_F OR $GC_F lang:en -is:retweet",
    ]),
    ("small_account_alpha", [
        "($SMCI OR $PLTR OR $MSTR OR $ARM OR $RKLB) lang:en -is:retweet has:cashtags",
        "(\"due diligence\" OR \"DD thread\" OR \"deep dive\") lang:en -is:retweet has:cashtags",
    ]),
    ("unusual_volume", [
        "(\"unusual volume\" OR \"options sweep\" OR \"call sweep\" OR \"put sweep\") lang:en -is:retweet",
        "(\"dark pool\" OR \"block trade\" OR \"whale alert\") lang:en -is:retweet",
    ]),
    ("corporate_insider", [
        "(CEO OR CFO OR \"chief executive\") (\"stepping down\" OR \"appointed\" OR resignation) lang:en -is:retweet",
        "(\"insider buying\" OR \"insider purchase\" OR \"Form 4\") lang:en -is:retweet",
    ]),
    ("sec_filing", [
        "(\"13F filing\" OR \"13D filing\" OR \"S-1 filing\" OR \"10-K\") lang:en -is:retweet",
        "(\"SEC investigation\" OR \"SEC charges\" OR \"SEC settlement\") lang:en -is:retweet",
    ]),
    ("crypto_whale", [
        "$BTC OR $ETH OR #crypto lang:en -is:retweet",
        "(\"whale transfer\" OR \"whale wallet\") (BTC OR ETH) lang:en -is:retweet",
    ]),
    ("political_regulatory", [
        "(antitrust OR regulation OR \"executive order\") (tech OR AI OR crypto) lang:en -is:retweet",
        "(\"trade ban\" OR \"export controls\" OR \"chip ban\") lang:en -is:retweet",
    ]),
    ("employment_signals", [
        "layoffs OR hiring freeze OR restructuring lang:en -is:retweet",
        "(\"mass layoff\" OR \"job cuts\" OR \"workforce reduction\") lang:en -is:retweet has:cashtags",
    ]),
]


class TwitterApiIoScraper(BaseScraper):
    """Twitter scraper using TwitterAPI.io. ~$7-15/month for continuous polling."""

    @property
    def platform(self) -> str:
        return "twitter"

    def __init__(self, interval: int = 120) -> None:
        super().__init__(interval)
        self._api_key = get_settings().twitterapiio_api_key
        self._seen_ids: set[str] = set()
        self._cycle_index: int = 0

    async def _search(
        self,
        client: httpx.AsyncClient,
        query: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Run a single search query. Returns up to 20 tweets per page."""
        resp = await client.get(
            _SEARCH_ENDPOINT,
            headers={"X-API-Key": self._api_key},
            params={"query": query, "queryType": "Latest"},
            timeout=30,
        )

        if resp.status_code == 429:
            logger.warning("[twitter/tapi] rate limited, will retry next cycle")
            return []

        if resp.status_code != 200:
            logger.warning("[twitter/tapi] search failed (%d): %s", resp.status_code, query[:60])
            return []

        data = resp.json()
        posts: list[dict[str, Any]] = []

        for tweet in data.get("tweets", []):
            tid = tweet.get("id", "")
            if not tid or tid in self._seen_ids:
                continue
            self._seen_ids.add(tid)

            # Cap seen_ids to prevent unbounded memory growth
            if len(self._seen_ids) > 50_000:
                self._seen_ids = set(list(self._seen_ids)[-25_000:])

            author = tweet.get("author", {})
            username = author.get("userName", "unknown")

            posts.append(self._make_post(
                source_id=str(tid),
                author=f"@{username}",
                content=(tweet.get("text", "") or "")[:2000],
                url=tweet.get("url") or f"https://x.com/{username}/status/{tid}",
                raw_metadata={
                    "author_name": author.get("name", ""),
                    "author_followers": author.get("followers", 0),
                    "author_verified": author.get("isBlueVerified", False),
                    "retweet_count": tweet.get("retweetCount", 0),
                    "like_count": tweet.get("likeCount", 0),
                    "reply_count": tweet.get("replyCount", 0),
                    "quote_count": tweet.get("quoteCount", 0),
                    "view_count": tweet.get("viewCount", 0),
                    "created_at": tweet.get("createdAt"),
                    "lang": tweet.get("lang", ""),
                    "search_query": query,
                    "category": category,
                    "source": "twitterapiio",
                },
                source_published_at=tweet.get("createdAt"),
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape 2 categories per cycle, rotating through all 9."""
        n = len(_QUERY_CATEGORIES)
        idx1 = self._cycle_index % n
        idx2 = (self._cycle_index + 1) % n
        self._cycle_index = (self._cycle_index + 2) % n

        categories = [_QUERY_CATEGORIES[idx1], _QUERY_CATEGORIES[idx2]]

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for cat_name, queries in categories:
                for query in queries:
                    try:
                        posts = await self._search(client, query, cat_name)
                        all_posts.extend(posts)
                    except Exception:
                        logger.warning("[twitter/tapi] error: %s", query[:60], exc_info=True)

        logger.info(
            "[twitter/tapi] scraped %d tweets from: %s",
            len(all_posts),
            ", ".join(c[0] for c in categories),
        )
        return all_posts
