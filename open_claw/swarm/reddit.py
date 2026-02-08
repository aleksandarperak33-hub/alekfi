"""Reddit scraper — hot/new/rising posts + top comments from 30 subreddits."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import praw

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

SUBREDDITS = [
    "wallstreetbets", "stocks", "investing", "economics", "technology",
    "worldnews", "politics", "news", "business", "finance",
    "stockmarket", "options", "SecurityAnalysis", "geopolitics", "energy",
    "RealEstate", "supplychain", "semiconductor", "biotech", "pharmaceuticals",
    "electricvehicles", "aerospace", "banking", "insurance", "retailnews",
    "manufacturing", "mining", "shipping", "agriculture", "CryptoCurrency",
]


class RedditScraper(BaseScraper):
    """Scrapes Reddit using PRAW (sync, run in executor)."""

    @property
    def platform(self) -> str:
        return "reddit"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        s = get_settings()
        self._reddit = praw.Reddit(
            client_id=s.reddit_client_id,
            client_secret=s.reddit_client_secret,
            user_agent=s.reddit_user_agent,
        )

    def _fetch_subreddit(self, name: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        sub = self._reddit.subreddit(name)
        for sort_method in ("hot", "new", "rising"):
            listing = getattr(sub, sort_method)(limit=10)
            for submission in listing:
                top_comments = []
                submission.comment_sort = "best"
                submission.comments.replace_more(limit=0)
                for comment in submission.comments[:5]:
                    top_comments.append({
                        "author": str(comment.author) if comment.author else "[deleted]",
                        "body": comment.body[:2000],
                        "score": comment.score,
                    })
                posts.append(self._make_post(
                    source_id=submission.id,
                    author=str(submission.author) if submission.author else "[deleted]",
                    content=f"{submission.title}\n\n{(submission.selftext or '')[:3000]}",
                    url=f"https://reddit.com{submission.permalink}",
                    raw_metadata={
                        "subreddit": name,
                        "sort": sort_method,
                        "score": submission.score,
                        "num_comments": submission.num_comments,
                        "upvote_ratio": submission.upvote_ratio,
                        "flair": submission.link_flair_text,
                        "created_utc": submission.created_utc,
                        "top_comments": top_comments,
                    },
                ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for sub_name in SUBREDDITS:
            try:
                posts = await loop.run_in_executor(None, self._fetch_subreddit, sub_name)
                all_posts.extend(posts)
            except Exception:
                logger.warning("[reddit] failed to scrape r/%s", sub_name, exc_info=True)
        seen: set[str] = set()
        deduped = []
        for p in all_posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                deduped.append(p)
        return deduped


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TEMPLATES = [
    ("wallstreetbets", "u/YOLO_King99", "$TSLA earnings beat, shorts getting squeezed. This is going to $400 by Friday. Diamond hands."),
    ("wallstreetbets", "u/TendieFactory", "Just YOLOed my entire savings into $NVDA calls. Either lambo or food stamps. 🚀"),
    ("stocks", "u/ValueInvestor42", "Deep dive on $AAPL: Services revenue up 18% YoY, wearables segment showing strength. PT $210."),
    ("stocks", "u/DividendDave", "$JNJ spinning off consumer health division — this is bullish for the pharma core business."),
    ("investing", "u/BogleHead2024", "Anyone else concerned about concentration risk in S&P 500? Top 10 stocks are 35% of the index now."),
    ("investing", "u/MacroMaven", "Inverted yield curve persisting for 14 months. Historically this leads to recession within 6-18 months."),
    ("economics", "u/EconPhD", "CPI came in hot at 3.4%. Core services ex-housing still sticky. Fed is not cutting in March."),
    ("economics", "u/DataNerd88", "PMI data shows manufacturing contraction for 5th straight month. Services holding up but slowing."),
    ("technology", "u/TechWatcher", "TSMC earnings call: AI chip demand exceeding capacity. Raising capex guidance by $5B."),
    ("technology", "u/SiliconInsider", "Intel losing another major data center contract to AMD. Market share down to 37%."),
    ("worldnews", "u/GlobalWatch", "OPEC+ emergency meeting called — Saudi Arabia pushing for 2M bbl/day production cut."),
    ("worldnews", "u/GeoPolitics101", "EU approving new sanctions package targeting Russian LNG exports. Energy prices spiking."),
    ("business", "u/CorpWatcher", "Boeing 737 MAX production halted again after new quality defect found. $BA down 8% premarket."),
    ("business", "u/RetailTracker", "Walmart Q3 blowout: same-store sales +7.4%. Stealing market share from everyone."),
    ("finance", "u/FinAnalyst", "JPMorgan net interest income forecast raised to $91B. Banking sector earnings surprising to the upside."),
    ("finance", "u/CreditWatch", "High-yield spreads widening to 450bps. Stress emerging in CCC-rated debt."),
    ("stockmarket", "u/ChartMaster", "SPX hitting resistance at 4800. RSI divergence on daily. Classic distribution pattern."),
    ("options", "u/GreekGod", "Unusual options activity: someone bought 50K $AMZN 200C expiring next month. $8M premium."),
    ("SecurityAnalysis", "u/DCF_Master", "My $MSFT model shows 15% upside from here. Azure growth reaccelerating to 31%."),
    ("geopolitics", "u/RiskAnalyst", "Taiwan strait tensions escalating. China conducting military exercises. Semiconductor supply chain at risk."),
    ("energy", "u/OilTrader", "Natural gas inventories way below 5-year average. If winter is cold, $UNG could double."),
    ("biotech", "u/PharmaDD", "Novo Nordisk GLP-1 data blowing away expectations. Ozempic/Wegovy TAM expanding to $100B+."),
    ("semiconductors", "u/ChipHead", "ASML order backlog at record levels. EUV demand from every major fab."),
    ("CryptoCurrency", "u/CryptoWhale", "Bitcoin ETF inflows hit $500M today. Institutional adoption accelerating ahead of halving."),
    ("RealEstate", "u/PropInvestor", "Commercial real estate distress spreading. Office vacancy rates hit 20% in major metros."),
    ("supplychain", "u/LogisticsGuru", "Red Sea disruptions adding 10-14 days to Asia-Europe shipping. Container rates up 300%."),
    ("electricvehicles", "u/EVFanatic", "BYD outselling Tesla globally for first time. Chinese EVs flooding European market."),
    ("aerospace", "u/AeroAnalyst", "Airbus order backlog at 8,500 aircraft. Industry capacity constrained for next decade."),
    ("banking", "u/BankWatcher", "Regional bank CRE exposure is the next shoe to drop. $NYCB down 40% this week."),
    ("insurance", "u/ActuaryAnon", "Climate-related insurance losses up 40% YoY. Florida homeowners market in crisis."),
    ("retailnews", "u/ShopperInsights", "Target announcing 200 store closures. Shrinkage costs up $1.2B this year."),
    ("manufacturing", "u/FactoryFloor", "US reshoring boom: 350K manufacturing jobs announced this year. Biggest wave since 1950s."),
    ("mining", "u/CopperBull", "Copper supply deficit widening. Chile production down 8%. Green transition needs 3x current output."),
    ("shipping", "u/FreightWatch", "Baltic Dry Index surging on China stimulus hopes. Bulk carrier rates up 50% this month."),
    ("agriculture", "u/FarmReport", "USDA crop report: corn yields below expectations. Drought in Midwest worsening."),
    ("pharmaceuticals", "u/RxAnalyst", "Pfizer writedown on COVID assets. Pipeline restructuring. Activist investor building stake."),
]


class MockRedditScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "reddit"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(30, 50)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            sub, author, content = random.choice(_MOCK_TEMPLATES)
            noise = random.randint(1000, 9999)
            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}_{noise}",
                author=author,
                content=content,
                url=f"https://reddit.com/r/{sub}/comments/{self._generate_id()}",
                raw_metadata={
                    "subreddit": sub,
                    "sort": random.choice(["hot", "new", "rising"]),
                    "score": random.randint(1, 15000),
                    "num_comments": random.randint(5, 800),
                    "upvote_ratio": round(random.uniform(0.55, 0.99), 2),
                    "flair": random.choice(["DD", "Discussion", "News", "YOLO", "Earnings", None]),
                    "top_comments": [
                        {"author": "u/commenter1", "body": "Bullish on this thesis", "score": random.randint(1, 500)},
                        {"author": "u/commenter2", "body": "Source? This seems too good.", "score": random.randint(1, 200)},
                    ],
                },
            ))
        return posts
