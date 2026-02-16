"""Google Trends scraper — detects search volume spikes for financial keywords."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_KEYWORDS_BATCHES = [
    # Reduced to 3 keywords per batch (Google rate limit)
    ["stock market crash", "recession", "fed rate cut"],
    ["inflation", "layoffs", "unemployment"],
    ["bitcoin", "ethereum", "crypto crash"],
    ["bitcoin ETF", "SEC crypto", "stablecoin"],
    ["nvidia stock", "tesla stock", "apple stock"],
    ["amazon stock", "meta stock", "microsoft stock"],
    ["oil price", "gold price", "natural gas"],
    ["wheat price", "copper price", "silver price"],
    ["bank run", "bank failure", "credit crisis"],
    ["mortgage rates", "housing crash", "housing bubble"],
    ["AI stocks", "semiconductor shortage", "chip war"],
    ["TSMC", "ASML", "chip shortage"],
    ["OPEC", "sanctions", "tariffs"],
    ["trade war", "supply chain", "supply chain crisis"],
    # Individual major tickers
    ["AAPL", "TSLA", "NVDA"],
    ["MSFT", "AMZN", "GOOG"],
    ["META", "AMD", "AVGO"],
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

        max_retries = 3
        backoff_secs = 30.0

        for attempt in range(max_retries + 1):
            try:
                pytrends = TrendReq(hl="en-US", tz=360)

                # Fetch current interest (last 24 hours)
                pytrends.build_payload(keywords, cat=0, timeframe="now 1-d", geo="US")
                interest = pytrends.interest_over_time()
                if interest.empty:
                    return posts

                # Fetch 7-day-ago interest for breakout comparison
                interest_7d = None
                try:
                    pytrends.build_payload(keywords, cat=0, timeframe="now 7-d", geo="US")
                    interest_7d = pytrends.interest_over_time()
                except Exception:
                    logger.debug("[google_trends] failed to fetch 7d comparison for %s", keywords)

                for kw in keywords:
                    if kw not in interest.columns:
                        continue
                    series = interest[kw]
                    current_val = int(series.iloc[-1])
                    avg_val = float(series.mean())

                    # 7-day comparison for breakout detection
                    avg_7d = None
                    breakout_detected = False
                    if interest_7d is not None and kw in interest_7d.columns:
                        avg_7d = float(interest_7d[kw].mean())
                        if avg_7d > 0 and current_val > avg_7d * 2.0:
                            breakout_detected = True

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
                                "average_interest_7d": round(avg_7d, 1) if avg_7d is not None else None,
                                "breakout_detected": breakout_detected,
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

                # Success — break out of retry loop
                break

            except Exception as exc:
                exc_str = str(exc).lower()
                is_rate_limit = "429" in exc_str or "too many" in exc_str or "rate" in exc_str

                if is_rate_limit and attempt < max_retries:
                    wait = backoff_secs * (2 ** attempt)
                    logger.warning(
                        "[google_trends] 429 rate limit for batch %s, retrying in %.0fs (attempt %d/%d)",
                        keywords, wait, attempt + 1, max_retries,
                    )
                    time.sleep(wait)
                    continue
                else:
                    logger.warning("[google_trends] failed to fetch batch %s", keywords, exc_info=True)
                    break

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
    ("stock market crash", 85, 32, 2.66, 28, True),
    ("nvidia stock", 92, 45, 2.04, 50, False),
    ("fed rate cut", 78, 28, 2.79, 25, True),
    ("bitcoin ETF", 95, 40, 2.38, 35, True),
    ("oil price", 70, 35, 2.0, 38, False),
    ("layoffs", 88, 30, 2.93, 26, True),
    ("bank failure", 65, 25, 2.6, 20, True),
    ("housing crash", 72, 38, 1.89, 40, False),
    ("copper price", 60, 28, 2.14, 30, False),
    ("OPEC", 55, 22, 2.5, 18, True),
    ("AAPL", 80, 42, 1.90, 45, False),
    ("TSLA", 90, 38, 2.37, 30, True),
    ("NVDA", 98, 50, 1.96, 55, False),
    ("chip shortage", 75, 30, 2.50, 22, True),
    ("supply chain crisis", 68, 28, 2.43, 24, True),
]

_MOCK_TRENDING = [
    "Tesla recall", "Boeing hearing", "Nvidia earnings", "Fed meeting",
    "Apple Vision Pro", "TikTok ban", "Student loans", "Gold record high",
    "China stimulus", "Gaza ceasefire", "AAPL stock split", "bank run news",
    "housing market crash", "AMD earnings", "OPEC production cut",
]


class MockGoogleTrendsScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "google_trends"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        spike_count = random.randint(3, 8)
        for kw, current, avg, ratio, avg_7d, breakout in random.sample(_MOCK_SPIKES, min(spike_count, len(_MOCK_SPIKES))):
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
                    "average_interest_7d": avg_7d,
                    "breakout_detected": breakout,
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
