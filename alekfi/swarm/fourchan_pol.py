"""4chan /pol/ scraper — geopolitical threads filtered for market relevance."""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_POL_API = "https://a.4cdn.org/pol"

# ONLY keep threads containing these keywords (case-insensitive)
_GEOPOLITICAL_KEYWORDS = frozenset({
    "sanction", "tariff", "trade war", "embargo", "military", "invasion", "coup",
    "election", "impeach", "fed", "rates", "treasury", "dollar", "yuan", "ruble",
    "oil", "opec", "nato", "taiwan", "china", "russia", "ukraine", "iran",
    "israel", "saudi", "inflation", "recession", "default", "debt ceiling",
    "shutdown", "executive order", "regulation", "deregulation", "antitrust",
    "merger", "ban", "restrict", "export control", "chip", "semiconductor",
    "rare earth", "nuclear", "missile", "bomb", "attack", "ceasefire",
    "peace deal", "summit", "g7", "g20", "brics", "imf", "world bank",
})

# Compiled regex for fast matching
_KEYWORD_RE = re.compile(
    "|".join(re.escape(k) for k in _GEOPOLITICAL_KEYWORDS),
    re.IGNORECASE,
)

# Simple regex to strip HTML tags from 4chan comments
_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Remove HTML tags from a string."""
    return _HTML_TAG_RE.sub("", text)


def _extract_matched_keywords(text: str) -> list[str]:
    """Return deduplicated list of geopolitical keywords found in text."""
    return sorted({m.group(0).lower() for m in _KEYWORD_RE.finditer(text)})


class FourChanPolScraper(BaseScraper):
    """Fetches threads from the 4chan /pol/ catalog filtered for geopolitical market signals."""

    @property
    def platform(self) -> str:
        return "4chan_pol"

    def __init__(self, interval: int = 180) -> None:
        super().__init__(interval)
        self._seen_ids: set[int] = set()

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20) as client:
            try:
                resp = await client.get(f"{_POL_API}/catalog.json")
                if resp.status_code != 200:
                    logger.warning("[4chan_pol] catalog returned %d", resp.status_code)
                    return []
                pages = resp.json()
            except Exception:
                logger.warning("[4chan_pol] failed to fetch catalog", exc_info=True)
                return []

            for page in pages:
                for thread in page.get("threads", []):
                    thread_no = thread.get("no")
                    if thread_no is None or thread_no in self._seen_ids:
                        continue

                    subject = thread.get("sub", "")
                    raw_comment = thread.get("com", "")
                    author = thread.get("name", "Anonymous")
                    timestamp = thread.get("time")

                    # Strip HTML from comment
                    comment = _strip_html(raw_comment)

                    # Combine subject + comment for keyword matching
                    combined_text = f"{subject} {comment}"

                    # ONLY keep threads that match geopolitical keywords
                    matched_keywords = _extract_matched_keywords(combined_text)
                    if not matched_keywords:
                        continue

                    self._seen_ids.add(thread_no)

                    content_parts = []
                    if subject:
                        content_parts.append(_strip_html(subject))
                    if comment:
                        content_parts.append(comment)
                    content = "\n\n".join(content_parts)

                    if not content.strip():
                        continue

                    source_published_at = None
                    if timestamp:
                        source_published_at = datetime.fromtimestamp(
                            timestamp, tz=timezone.utc
                        ).isoformat()

                    replies_count = thread.get("replies", 0)
                    thread_velocity = 0.0
                    if timestamp:
                        now_ts = datetime.now(timezone.utc).timestamp()
                        age_minutes = max((now_ts - timestamp) / 60.0, 1.0)
                        thread_velocity = round(replies_count / age_minutes, 3)

                    all_posts.append(self._make_post(
                        source_id=str(thread_no),
                        author=author,
                        content=content[:3000],
                        url=f"https://boards.4chan.org/pol/thread/{thread_no}",
                        source_published_at=source_published_at,
                        raw_metadata={
                            "thread_no": thread_no,
                            "subject": _strip_html(subject),
                            "replies_count": replies_count,
                            "images_count": thread.get("images", 0),
                            "sticky": thread.get("sticky", 0),
                            "closed": thread.get("closed", 0),
                            "page": page.get("page"),
                            "matched_keywords": matched_keywords,
                            "thread_velocity": thread_velocity,
                        },
                    ))

        # Cap seen IDs to prevent unbounded memory growth
        if len(self._seen_ids) > 10000:
            self._seen_ids = set(list(self._seen_ids)[-5000:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_POL_THREADS = [
    (
        "Anonymous",
        "New US sanctions on Russia target energy sector",
        "Biden admin announces sweeping sanctions targeting Russian oil exports. "
        "European allies scrambling to find alternative suppliers. "
        "Oil futures spiking on the news. This will accelerate de-dollarization.",
    ),
    (
        "Anonymous",
        "BREAKING: China announces retaliatory tariffs on US goods",
        "Beijing hits back with 25% tariff on US agricultural exports. "
        "Trade war escalation continues. Soybean and corn futures dumping. "
        "Farmers in Iowa absolutely seething right now.",
    ),
    (
        "Anonymous",
        "NATO troops deploying to Baltic states",
        "Military buildup accelerating along the Russian border. "
        "30,000 NATO troops stationed in Lithuania, Latvia, Estonia. "
        "Defense stocks like LMT, RTX, NOC are going to moon.",
    ),
    (
        "Anonymous",
        "Taiwan strait crisis: China military drills intensifying",
        "PLA conducting live-fire exercises within 12nm of Taiwan. "
        "TSMC shares tanking. Semiconductor supply chain at risk. "
        "Pentagon moving carrier strike group to the region.",
    ),
    (
        "Anonymous",
        "Fed minutes leaked: Rate cuts delayed until Q4",
        "Sources inside the Federal Reserve say inflation is too sticky. "
        "Rates staying higher for longer. Treasury yields spiking. "
        "The dollar is strengthening and EM currencies are getting crushed.",
    ),
    (
        "Anonymous",
        "Iran nuclear deal talks collapse",
        "Negotiations in Vienna have broken down completely. "
        "Iran enriching uranium to 60%. Oil prices surging past $95/barrel. "
        "Israel threatening preemptive strike. OPEC emergency meeting called.",
    ),
    (
        "Anonymous",
        "BRICS summit: New reserve currency announced",
        "BRICS nations agree on gold-backed trade settlement currency. "
        "Dollar hegemony under direct threat. G7 in emergency session. "
        "IMF and World Bank scrambling to respond.",
    ),
    (
        "Anonymous",
        "US debt ceiling crisis: Government shutdown imminent",
        "Congress fails to reach deal. Treasury running out of cash. "
        "Default risk rising. Credit agencies issuing warnings. "
        "Bond market in turmoil. This is worse than 2011.",
    ),
    (
        "Anonymous",
        "Executive order banning Chinese chip imports",
        "White House announces comprehensive ban on semiconductor imports "
        "from China. Export control expanded to AI accelerators. "
        "NVDA, AMD shares dropping. TSMC caught in the crossfire.",
    ),
    (
        "Anonymous",
        "Saudi Arabia threatens oil embargo over Gaza",
        "Saudi crown prince warns of OPEC production cuts if US doesn't "
        "pressure Israel for ceasefire. Oil heading to $120. "
        "Energy stocks pumping. Airlines getting destroyed.",
    ),
    (
        "Anonymous",
        "Russia-Ukraine ceasefire deal on the table",
        "Leaked peace deal terms: Ukraine gives up Crimea, Russia withdraws "
        "from other territories. NATO membership off table for 20 years. "
        "European defense stocks dumping on the news.",
    ),
    (
        "Anonymous",
        "Rare earth export restrictions from China expanding",
        "Beijing tightening rare earth export controls in response to chip ban. "
        "Gallium and germanium supplies cut off. Western defense industry panicking. "
        "Mining stocks going parabolic. MP Materials up 15%.",
    ),
    (
        "Anonymous",
        "G20 summit: Global minimum tax agreement falling apart",
        "Major economies backing away from the 15% global minimum corporate tax. "
        "Ireland and Singapore refusing compliance. Tech companies routing profits "
        "through new structures. Regulation efforts failing.",
    ),
    (
        "Anonymous",
        "French election chaos: Markets in turmoil",
        "Snap election results causing panic in European markets. "
        "Euro dumping against the dollar. French bond yields spiking. "
        "ECB considering emergency intervention. Recession fears growing.",
    ),
    (
        "Anonymous",
        "North Korea missile test triggers Asian market selloff",
        "ICBM launched over Japan. Nikkei down 3%. Won crashing. "
        "Military tensions at highest level since Korean War. "
        "Defense spending across Asia set to surge. Nuclear escalation risk.",
    ),
    (
        "Anonymous",
        "Coup attempt in major oil-producing nation",
        "Military forces seizing control of government in Nigeria. "
        "Oil production facilities at risk. Brent crude surging. "
        "African political instability spreading. IMF loans in jeopardy.",
    ),
    (
        "Anonymous",
        "US antitrust suit against major tech merger",
        "DOJ blocking the proposed $50B merger between two tech giants. "
        "Antitrust enforcement at highest level in decades. "
        "The deregulation era is officially over. Shares of both companies tanking.",
    ),
    (
        "Anonymous",
        "Yuan devaluation: China letting currency slide",
        "PBOC setting fix at weakest level since 2008. "
        "Yuan breaking through 7.30 against the dollar. Capital outflows accelerating. "
        "Asian currencies under pressure. Ruble also weakening in sympathy.",
    ),
]


class MockFourChanPolScraper(BaseScraper):
    """Mock scraper that generates realistic geopolitical /pol/ threads."""

    @property
    def platform(self) -> str:
        return "4chan_pol"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        selected = random.sample(
            _MOCK_POL_THREADS, min(count, len(_MOCK_POL_THREADS))
        )
        for author, subject, comment in selected:
            thread_no = random.randint(400000000, 500000000)
            combined_text = f"{subject} {comment}"
            matched_keywords = _extract_matched_keywords(combined_text)
            replies_count = random.randint(10, 500)
            age_minutes = random.uniform(5.0, 600.0)
            thread_velocity = round(replies_count / age_minutes, 3)

            posts.append(self._make_post(
                source_id=str(thread_no),
                author=author,
                content=f"{subject}\n\n{comment}",
                url=f"https://boards.4chan.org/pol/thread/{thread_no}",
                raw_metadata={
                    "thread_no": thread_no,
                    "subject": subject,
                    "replies_count": replies_count,
                    "images_count": random.randint(0, 80),
                    "sticky": 0,
                    "closed": 0,
                    "page": random.randint(1, 10),
                    "matched_keywords": matched_keywords,
                    "thread_velocity": thread_velocity,
                },
            ))
        return posts
