"""Google Trends scraper — detects search volume spikes for financial keywords."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_KEYWORDS_BATCHES = [
    ["stock market crash", "recession", "fed rate cut", "inflation", "layoffs"],
    ["bitcoin", "ethereum", "crypto crash", "bitcoin ETF", "SEC crypto"],
    ["nvidia stock", "tesla stock", "apple stock", "amazon stock", "meta stock"],
    ["oil price", "gold price", "natural gas", "wheat price", "copper price"],
    ["bank run", "bank failure", "credit crisis", "mortgage rates", "housing crash"],
    ["AI stocks", "semiconductor shortage", "chip war", "TSMC", "ASML"],
    ["OPEC", "sanctions", "tariffs", "trade war", "supply chain"],
]


class GoogleTrendsScraper(BaseScraper):
    """Uses pytrends to detect search interest spikes for financial keywords."""

    @property
    def platform(self) -> str:
        return "google_trends"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)

    def _fetch_batch(self, keywords: list[str]) -> list[dict[str, Any]]:
        from pytrends.request import TrendReq
        posts: list[dict[str, Any]] = []
        try:
            pytrends = TrendReq(hl="en-US", tz=360)
            pytrends.build_payload(keywords, cat=0, timeframe="now 1-d", geo="US")
            interest = pytrends.interest_over_time()
            if interest.empty:
                return posts

            for kw in keywords:
                if kw not in interest.columns:
                    continue
                series = interest[kw]
                current_val = int(series.iloc[-1])
                avg_val = float(series.mean())
                if avg_val > 0 and current_val > avg_val * 1.5:
                    spike_ratio = round(current_val / avg_val, 2)
                    posts.append(self._make_post(
                        source_id=f"trend_{kw.replace(' ', '_')}_{series.index[-1].isoformat()}",
                        author="google_trends",
                        content=f"Search spike detected: '{kw}' — current interest {current_val} vs avg {avg_val:.0f} (spike ratio: {spike_ratio}x)",
                        url=f"https://trends.google.com/trends/explore?q={kw.replace(' ', '+')}&geo=US",
                        raw_metadata={
                            "keyword": kw,
                            "current_interest": current_val,
                            "average_interest": round(avg_val, 1),
                            "spike_ratio": spike_ratio,
                            "timeframe": "now 1-d",
                            "geo": "US",
                        },
                    ))

            trending = pytrends.trending_searches(pn="united_states")
            for _, row in trending.head(10).iterrows():
                term = str(row.iloc[0])
                posts.append(self._make_post(
                    source_id=f"trending_{term.replace(' ', '_')}_{self._generate_id()}",
                    author="google_trends",
                    content=f"Trending search: '{term}'",
                    url=f"https://trends.google.com/trends/explore?q={term.replace(' ', '+')}",
                    raw_metadata={"keyword": term, "type": "trending_search"},
                ))
        except Exception:
            logger.warning("[google_trends] failed to fetch batch %s", keywords, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for batch in _KEYWORDS_BATCHES:
            posts = await loop.run_in_executor(None, self._fetch_batch, batch)
            all_posts.extend(posts)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_SPIKES = [
    ("stock market crash", 85, 32, 2.66),
    ("nvidia stock", 92, 45, 2.04),
    ("fed rate cut", 78, 28, 2.79),
    ("bitcoin ETF", 95, 40, 2.38),
    ("oil price", 70, 35, 2.0),
    ("layoffs tech", 88, 30, 2.93),
    ("bank failure", 65, 25, 2.6),
    ("housing crash", 72, 38, 1.89),
    ("copper price", 60, 28, 2.14),
    ("OPEC meeting", 55, 22, 2.5),
]

_MOCK_TRENDING = [
    "Tesla recall", "Boeing hearing", "Nvidia earnings", "Fed meeting",
    "Apple Vision Pro", "TikTok ban", "Student loans", "Gold record high",
    "China stimulus", "Gaza ceasefire",
]


class MockGoogleTrendsScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "google_trends"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        spike_count = random.randint(3, 8)
        for kw, current, avg, ratio in random.sample(_MOCK_SPIKES, min(spike_count, len(_MOCK_SPIKES))):
            posts.append(self._make_post(
                source_id=f"trend_{kw.replace(' ', '_')}_{self._generate_id()}",
                author="google_trends",
                content=f"Search spike detected: '{kw}' — current interest {current} vs avg {avg} (spike ratio: {ratio}x)",
                url=f"https://trends.google.com/trends/explore?q={kw.replace(' ', '+')}",
                raw_metadata={
                    "keyword": kw,
                    "current_interest": current,
                    "average_interest": avg,
                    "spike_ratio": ratio,
                    "type": "spike",
                },
            ))
        trending_count = random.randint(3, 7)
        for term in random.sample(_MOCK_TRENDING, min(trending_count, len(_MOCK_TRENDING))):
            posts.append(self._make_post(
                source_id=f"trending_{term.replace(' ', '_')}_{self._generate_id()}",
                author="google_trends",
                content=f"Trending search: '{term}'",
                url=f"https://trends.google.com/trends/explore?q={term.replace(' ', '+')}",
                raw_metadata={"keyword": term, "type": "trending_search"},
            ))
        return posts
