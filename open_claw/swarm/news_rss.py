"""News RSS feed scraper — monitors 20+ financial/business news feeds."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from typing import Any

import feedparser

from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

RSS_FEEDS: dict[str, str] = {
    "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
    "reuters_markets": "https://feeds.reuters.com/reuters/marketsNews",
    "ap_business": "https://feeds.apnews.com/rss/apf-business",
    "cnbc_top": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "cnbc_world": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    "bbc_business": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "aljazeera_economy": "https://www.aljazeera.com/xml/rss/all.xml",
    "techcrunch": "https://techcrunch.com/feed/",
    "seekingalpha_market": "https://seekingalpha.com/market_currents.xml",
    "seekingalpha_news": "https://seekingalpha.com/feed.xml",
    "marketwatch_top": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "marketwatch_markets": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "theblock": "https://www.theblock.co/rss.xml",
    "politico": "https://rss.politico.com/economy.xml",
    "thehill": "https://thehill.com/feed/",
    "ft": "https://www.ft.com/rss/home",
    "economist": "https://www.economist.com/finance-and-economics/rss.xml",
    "scmp_economy": "https://www.scmp.com/rss/91/feed",
    "nikkei": "https://asia.nikkei.com/rss",
    "barrons": "https://www.barrons.com/feed",
}


class NewsRSSScraper(BaseScraper):
    """Fetch and deduplicate entries from financial RSS feeds."""

    @property
    def platform(self) -> str:
        return "news_rss"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._seen_urls: set[str] = set()

    @staticmethod
    def _hash_url(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def _parse_feed(self, feed_name: str, feed_url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:15]:
                link = getattr(entry, "link", "") or ""
                if not link or link in self._seen_urls:
                    continue
                self._seen_urls.add(link)
                title = getattr(entry, "title", "No title")
                summary = getattr(entry, "summary", "")[:2000]
                published = getattr(entry, "published", "")
                posts.append(self._make_post(
                    source_id=self._hash_url(link),
                    author=feed_name,
                    content=f"{title}\n\n{summary}",
                    url=link,
                    raw_metadata={
                        "feed": feed_name,
                        "feed_url": feed_url,
                        "title": title,
                        "summary": summary[:500],
                        "published": published,
                        "tags": [t.get("term", "") for t in getattr(entry, "tags", [])],
                    },
                ))
        except Exception:
            logger.warning("[news_rss] failed to parse %s", feed_name, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for name, url in RSS_FEEDS.items():
            posts = await loop.run_in_executor(None, self._parse_feed, name, url)
            all_posts.extend(posts)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_HEADLINES = [
    ("reuters_business", "Fed signals patience on rate cuts amid sticky inflation data"),
    ("reuters_markets", "S&P 500 hits record high on AI optimism, Nvidia leads gains"),
    ("ap_business", "US job growth surges past expectations; unemployment falls to 3.5%"),
    ("cnbc_top", "Tesla recalls 2M vehicles over Autopilot safety concerns"),
    ("cnbc_world", "China cuts reserve requirement ratio to boost flagging economy"),
    ("bbc_business", "UK enters technical recession as GDP contracts 0.3% in Q4"),
    ("aljazeera_economy", "OPEC+ agrees to extend production cuts through Q2 2025"),
    ("techcrunch", "OpenAI valued at $80B in latest funding round; revenue tripling"),
    ("seekingalpha_market", "Apple suppliers warn of weakening iPhone demand in China"),
    ("seekingalpha_news", "Goldman Sachs upgrades semiconductor sector to Overweight"),
    ("marketwatch_top", "10-year Treasury yield surges past 4.5% on hot CPI data"),
    ("marketwatch_markets", "Gold hits all-time high above $2,200 on geopolitical risks"),
    ("coindesk", "Bitcoin ETF sees record $1.2B inflow; BTC breaks $60,000"),
    ("theblock", "Ethereum layer-2 activity hits new high; gas fees plunge"),
    ("politico", "Congress passes $95B aid package with implications for defense stocks"),
    ("thehill", "White House announces new tariffs on Chinese EVs and semiconductors"),
    ("ft", "European banks face $50B in unrealized losses on commercial property"),
    ("economist", "Global supply chains rewiring: winners and losers of friendshoring"),
    ("scmp_economy", "China property crisis deepens as Country Garden misses bond payment"),
    ("nikkei", "Japan chip investment boom: $30B in new fab construction announced"),
    ("barrons", "Why the market rally is broadening beyond tech megacaps"),
    ("reuters_business", "Boeing CEO steps down amid ongoing quality and safety crises"),
    ("cnbc_top", "JPMorgan Q4 earnings smash estimates; NII guidance raised"),
    ("bbc_business", "Shell exits North Sea wind projects citing poor returns"),
    ("techcrunch", "Anthropic raises $2B from Google as AI race intensifies"),
]


class MockNewsScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "news_rss"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(20, 30)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            feed, headline = random.choice(_MOCK_HEADLINES)
            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}",
                author=feed,
                content=headline,
                url=f"https://news.example.com/article/{self._generate_id()}",
                raw_metadata={
                    "feed": feed,
                    "title": headline,
                    "summary": f"Full article text for: {headline}",
                    "published": "2025-01-15T14:30:00Z",
                    "tags": random.sample(["markets", "economy", "fed", "earnings", "tech", "energy", "crypto"], 2),
                },
            ))
        return posts
