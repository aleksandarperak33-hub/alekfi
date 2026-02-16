"""Hacker News scraper — top/new/best stories + top comments via public API."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_HN_API = "https://hacker-news.firebaseio.com/v0"


class HackerNewsScraper(BaseScraper):
    """Fetches stories from the Hacker News Firebase API (no auth required)."""

    @property
    def platform(self) -> str:
        return "hackernews"

    def __init__(self, interval: int = 90) -> None:
        super().__init__(interval)
        self._seen_ids: set[int] = set()

    async def _fetch_item(self, client: httpx.AsyncClient, item_id: int) -> dict[str, Any] | None:
        try:
            resp = await client.get(f"{_HN_API}/item/{item_id}.json")
            if resp.status_code == 200:
                return resp.json()
        except Exception:
            logger.debug("[hackernews] failed to fetch item %d", item_id)
        return None

    async def _fetch_comments(self, client: httpx.AsyncClient, kid_ids: list[int], limit: int = 5) -> list[dict[str, Any]]:
        comments: list[dict[str, Any]] = []
        for kid_id in kid_ids[:limit]:
            item = await self._fetch_item(client, kid_id)
            if item and item.get("type") == "comment" and item.get("text"):
                comments.append({
                    "author": item.get("by", "anon"),
                    "text": item["text"][:1000],
                    "id": kid_id,
                })
        return comments

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20) as client:
            for endpoint in ("topstories", "newstories", "beststories"):
                try:
                    resp = await client.get(f"{_HN_API}/{endpoint}.json")
                    if resp.status_code != 200:
                        continue
                    story_ids = resp.json()[:30]
                except Exception:
                    logger.warning("[hackernews] failed to fetch %s", endpoint, exc_info=True)
                    continue

                for sid in story_ids:
                    if sid in self._seen_ids:
                        continue
                    self._seen_ids.add(sid)
                    story = await self._fetch_item(client, sid)
                    if not story or story.get("type") != "story":
                        continue
                    title = story.get("title", "")
                    url = story.get("url", "")
                    text = story.get("text", "")
                    kids = story.get("kids", [])
                    comments = await self._fetch_comments(client, kids)
                    all_posts.append(self._make_post(
                        source_id=str(sid),
                        author=story.get("by", "anon"),
                        content=f"{title}\n\n{text}" if text else title,
                        url=url or f"https://news.ycombinator.com/item?id={sid}",
                        raw_metadata={
                            "hn_id": sid,
                            "endpoint": endpoint,
                            "score": story.get("score", 0),
                            "descendants": story.get("descendants", 0),
                            "time": story.get("time"),
                            "top_comments": comments,
                        },
                    ))
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_STORIES = [
    ("Show HN: Open-source Bloomberg terminal built with Python", "dang", "https://github.com/example/openbb"),
    ("Nvidia surpasses Apple as world's most valuable company", "pg", "https://reuters.com/nvidia-surpasses"),
    ("Google DeepMind achieves breakthrough in protein-drug interaction prediction", "minimaxir", "https://deepmind.google/protein-drugs"),
    ("Amazon layoffs: 18,000 jobs cut in largest tech layoff of the year", "tptacek", "https://nytimes.com/amazon-layoffs"),
    ("The semiconductor shortage is over — now there's a glut", "dang", "https://ft.com/chip-glut"),
    ("Apple Vision Pro teardown reveals $1,500 in components", "ifixit", "https://ifixit.com/avp-teardown"),
    ("Why JPMorgan is betting big on AI for trading", "jacquesm", "https://bloomberg.com/jpm-ai"),
    ("YC W24 batch includes 5 nuclear energy startups", "garry", "https://ycombinator.com/w24-nuclear"),
    ("PostgreSQL 17 benchmarks show 2x performance improvement", "cperciva", "https://postgresql.org/17-benchmarks"),
    ("SpaceX Starship completes first successful orbital flight", "elonmusk", "https://spacex.com/starship-orbital"),
    ("Stripe processes $1T in payments; files confidentially for IPO", "patio11", "https://stripe.com/annual-report"),
    ("The hidden costs of cloud computing: a $100B problem", "acolyer", "https://blog.example.com/cloud-costs"),
    ("Anthropic publishes new research on AI safety interpretability", "dang", "https://anthropic.com/research/interp"),
    ("Intel foundry losing $7B/year — can Pat Gelsinger turn it around?", "luu", "https://anandtech.com/intel-foundry"),
    ("Boeing whistleblower testifies before Congress about safety defects", "throwaway123", "https://cnn.com/boeing-testimony"),
    ("TSMC Arizona fab delayed again; costs balloon to $65B", "chipguy", "https://reuters.com/tsmc-arizona"),
    ("Rust adoption in Linux kernel accelerating; 50K lines merged", "burntsushi", "https://lwn.net/rust-kernel"),
    ("Red Sea shipping crisis could reignite inflation, economists warn", "econbuff", "https://economist.com/red-sea"),
    ("Meta releases Llama 3 — open weights, 400B parameters", "gwern", "https://ai.meta.com/llama3"),
    ("The great commercial real estate crash is here", "tlb", "https://wsj.com/cre-crash"),
]


class MockHNScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "hackernews"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(15, 25)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            title, author, url = random.choice(_MOCK_STORIES)
            sid = random.randint(30000000, 40000000)
            posts.append(self._make_post(
                source_id=str(sid),
                author=author,
                content=title,
                url=url,
                raw_metadata={
                    "hn_id": sid,
                    "endpoint": random.choice(["topstories", "newstories", "beststories"]),
                    "score": random.randint(50, 2500),
                    "descendants": random.randint(10, 800),
                    "top_comments": [
                        {"author": "commenter", "text": "Fascinating development. This could reshape the industry.", "id": sid + 1},
                    ],
                },
            ))
        return posts
