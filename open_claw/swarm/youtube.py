"""YouTube scraper — financial video search via YouTube Data API v3."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_YT_SEARCH = "https://www.googleapis.com/youtube/v3/search"
_YT_VIDEOS = "https://www.googleapis.com/youtube/v3/videos"

_SEARCH_QUERIES = [
    "stock market analysis today",
    "earnings report reaction",
    "Federal Reserve interest rates",
    "AI stocks to buy",
    "oil price forecast",
    "cryptocurrency market update",
    "real estate market crash",
    "gold price prediction",
    "supply chain disruption",
    "economic recession warning",
]


class YouTubeScraper(BaseScraper):
    """Search YouTube Data API v3 for recent financial videos."""

    @property
    def platform(self) -> str:
        return "youtube"

    def __init__(self, interval: int = 180) -> None:
        super().__init__(interval)
        self._api_key = get_settings().youtube_api_key
        self._seen_ids: set[str] = set()

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20) as client:
            for query in _SEARCH_QUERIES:
                try:
                    resp = await client.get(_YT_SEARCH, params={
                        "part": "snippet",
                        "q": query,
                        "type": "video",
                        "order": "date",
                        "maxResults": 5,
                        "publishedAfter": "2024-01-01T00:00:00Z",
                        "key": self._api_key,
                    })
                    if resp.status_code != 200:
                        logger.warning("[youtube] search failed (%d): %s", resp.status_code, query)
                        continue
                    data = resp.json()
                    video_ids = []
                    snippets: dict[str, dict] = {}
                    for item in data.get("items", []):
                        vid = item["id"].get("videoId", "")
                        if vid and vid not in self._seen_ids:
                            self._seen_ids.add(vid)
                            video_ids.append(vid)
                            snippets[vid] = item["snippet"]

                    if video_ids:
                        stats_resp = await client.get(_YT_VIDEOS, params={
                            "part": "statistics",
                            "id": ",".join(video_ids),
                            "key": self._api_key,
                        })
                        stats_map: dict[str, dict] = {}
                        if stats_resp.status_code == 200:
                            for v in stats_resp.json().get("items", []):
                                stats_map[v["id"]] = v.get("statistics", {})

                    for vid in video_ids:
                        snip = snippets[vid]
                        stats = stats_map.get(vid, {})
                        all_posts.append(self._make_post(
                            source_id=vid,
                            author=snip.get("channelTitle", "Unknown"),
                            content=f"{snip.get('title', '')}\n\n{snip.get('description', '')[:1000]}",
                            url=f"https://www.youtube.com/watch?v={vid}",
                            raw_metadata={
                                "channel": snip.get("channelTitle"),
                                "channel_id": snip.get("channelId"),
                                "published_at": snip.get("publishedAt"),
                                "search_query": query,
                                "view_count": int(stats.get("viewCount", 0)),
                                "like_count": int(stats.get("likeCount", 0)),
                                "comment_count": int(stats.get("commentCount", 0)),
                            },
                        ))
                except Exception:
                    logger.warning("[youtube] error searching '%s'", query, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_VIDEOS = [
    ("Stock Market Crash Incoming? Here's What Smart Money Is Doing", "Graham Stephan", 1_500_000),
    ("NVIDIA Earnings Breakdown — Why This Changes Everything", "Meet Kevin", 890_000),
    ("The Fed Just Made a HUGE Mistake (Rates Update)", "Andrei Jikh", 620_000),
    ("Why Warren Buffett Is Sitting on $157B Cash", "The Swedish Investor", 340_000),
    ("Oil Price to $120? OPEC Just Changed the Game", "Commodity Culture", 210_000),
    ("Bitcoin ETF Approval — What Happens Next for Crypto", "Coin Bureau", 1_200_000),
    ("Tesla FSD V12 Review — This Changes the Autopilot Thesis", "Solving The Money Problem", 450_000),
    ("Japan's Economy Is Collapsing — Here's Why It Matters", "Economics Explained", 780_000),
    ("Best Dividend Stocks for 2025 (High Yield Portfolio)", "Dividend Bull", 290_000),
    ("BREAKING: China Stimulus Package — Markets React", "Bloomberg TV", 2_100_000),
    ("Commercial Real Estate Crisis Explained in 10 Minutes", "How Money Works", 1_800_000),
    ("Copper: The Most Important Metal for the Next Decade", "Real Vision", 560_000),
    ("Why Insurance Companies Are Leaving Florida", "CNBC", 980_000),
    ("Boeing's Quality Problems Are Worse Than You Think", "Wendover Productions", 3_200_000),
    ("AI Chip War: NVIDIA vs AMD vs Intel — Who Wins?", "Coldfusion", 1_100_000),
]


class MockYouTubeScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "youtube"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            title, channel, views = random.choice(_MOCK_VIDEOS)
            vid = self._generate_id()
            posts.append(self._make_post(
                source_id=vid,
                author=channel,
                content=title,
                url=f"https://www.youtube.com/watch?v={vid}",
                raw_metadata={
                    "channel": channel,
                    "search_query": random.choice(_SEARCH_QUERIES),
                    "view_count": views + random.randint(-50_000, 200_000),
                    "like_count": random.randint(5_000, 80_000),
                    "comment_count": random.randint(200, 5_000),
                    "published_at": "2025-01-15T10:00:00Z",
                },
            ))
        return posts
