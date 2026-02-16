"""Reddit scraper — hot/new/rising posts + top comments from tiered subreddits.

Subreddit tiers control scrape frequency:
  TIER 1 — every cycle (core finance: WSB, stocks, investing, options)
  TIER 2 — every other cycle (sophisticated retail DD)
  TIER 3 — every other cycle (consumer distress signals)
  TIER 4 — every other cycle (tech employment)
  TIER 5 — every third cycle (housing)
  TIER 6 — every third cycle (degen sentiment for flow prediction)
  TIER 7 — every third cycle (company-specific)
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import praw

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# ── Tiered subreddits ─────────────────────────────────────────────────

_TIER_1: list[str] = [
    "wallstreetbets", "stocks", "investing", "options",
]

_TIER_2: list[str] = [
    "thetagang", "vitards", "maxjustrisk",
]

_TIER_3: list[str] = [
    "personalfinance", "povertyfinance",
]

_TIER_4: list[str] = [
    "cscareerquestions", "recruitinghell",
]

_TIER_5: list[str] = [
    "realestate", "REBubble",
]

_TIER_6: list[str] = [
    "Superstonk", "cryptocurrency", "ethtrader",
]

_TIER_7: list[str] = [
    "tesla", "apple", "amazon", "nvidia",
]

# Original subreddits kept as additional coverage (scraped every cycle)
_ORIGINAL_EXTRA: list[str] = [
    "economics", "technology", "worldnews", "politics", "news",
    "business", "finance", "stockmarket", "SecurityAnalysis",
    "geopolitics", "energy", "supplychain", "semiconductor",
    "biotech", "pharmaceuticals", "electricvehicles", "aerospace",
    "banking", "insurance", "retailnews", "manufacturing", "mining",
    "shipping", "agriculture", "CryptoCurrency",
]

# Map tiers to their lists and names for metadata tagging
_TIER_MAP: dict[str, tuple[int, list[str]]] = {}
for _sub in _TIER_1:
    _TIER_MAP[_sub.lower()] = (1, _TIER_1)
for _sub in _TIER_2:
    _TIER_MAP[_sub.lower()] = (2, _TIER_2)
for _sub in _TIER_3:
    _TIER_MAP[_sub.lower()] = (3, _TIER_3)
for _sub in _TIER_4:
    _TIER_MAP[_sub.lower()] = (4, _TIER_4)
for _sub in _TIER_5:
    _TIER_MAP[_sub.lower()] = (5, _TIER_5)
for _sub in _TIER_6:
    _TIER_MAP[_sub.lower()] = (6, _TIER_6)
for _sub in _TIER_7:
    _TIER_MAP[_sub.lower()] = (7, _TIER_7)
for _sub in _ORIGINAL_EXTRA:
    if _sub.lower() not in _TIER_MAP:
        _TIER_MAP[_sub.lower()] = (0, _ORIGINAL_EXTRA)

# Backward-compatible flat list (union of all unique subreddits)
SUBREDDITS = list(dict.fromkeys(
    _TIER_1 + _TIER_2 + _TIER_3 + _TIER_4 + _TIER_5 + _TIER_6 + _TIER_7 + _ORIGINAL_EXTRA
))


def _get_tier(subreddit_name: str) -> int:
    """Return the tier number for a subreddit (0 = original/untiered)."""
    return _TIER_MAP.get(subreddit_name.lower(), (0, []))[0]


def _subreddits_for_cycle(cycle: int) -> list[str]:
    """Return which subreddits should be scraped on a given cycle number.

    - Tier 1 + original extras: every cycle
    - Tier 2-4: every other cycle (cycle % 2 == 0)
    - Tier 5-7: every third cycle (cycle % 3 == 0)
    """
    subs: list[str] = []

    # Always scrape tier 1 and original extras
    subs.extend(_TIER_1)
    subs.extend(_ORIGINAL_EXTRA)

    # Every other cycle: tier 2, 3, 4
    if cycle % 2 == 0:
        subs.extend(_TIER_2)
        subs.extend(_TIER_3)
        subs.extend(_TIER_4)

    # Every third cycle: tier 5, 6, 7
    if cycle % 3 == 0:
        subs.extend(_TIER_5)
        subs.extend(_TIER_6)
        subs.extend(_TIER_7)

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for s in subs:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            result.append(s)
    return result


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
        self._cycle: int = 0

    def _fetch_subreddit(self, name: str) -> list[dict[str, Any]]:
        tier = _get_tier(name)
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
                        "tier": tier,
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
        cycle = self._cycle
        self._cycle += 1

        subs_this_cycle = _subreddits_for_cycle(cycle)
        logger.info(
            "[reddit] cycle %d — scraping %d subreddits (tiers active: 1%s%s)",
            cycle,
            len(subs_this_cycle),
            "+2-4" if cycle % 2 == 0 else "",
            "+5-7" if cycle % 3 == 0 else "",
        )

        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for sub_name in subs_this_cycle:
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
    # Tier 1 — core finance
    ("wallstreetbets", 1, "u/YOLO_King99", "$TSLA earnings beat, shorts getting squeezed. This is going to $400 by Friday. Diamond hands."),
    ("wallstreetbets", 1, "u/TendieFactory", "Just YOLOed my entire savings into $NVDA calls. Either lambo or food stamps."),
    ("stocks", 1, "u/ValueInvestor42", "Deep dive on $AAPL: Services revenue up 18% YoY, wearables segment showing strength. PT $210."),
    ("stocks", 1, "u/DividendDave", "$JNJ spinning off consumer health division -- this is bullish for the pharma core business."),
    ("investing", 1, "u/BogleHead2024", "Anyone else concerned about concentration risk in S&P 500? Top 10 stocks are 35% of the index now."),
    ("investing", 1, "u/MacroMaven", "Inverted yield curve persisting for 14 months. Historically this leads to recession within 6-18 months."),
    ("options", 1, "u/GreekGod", "Unusual options activity: someone bought 50K $AMZN 200C expiring next month. $8M premium."),

    # Tier 2 — sophisticated retail DD
    ("thetagang", 2, "u/ThetaDecay", "Selling 45 DTE puts on $MSFT at the 380 strike. IV rank at 85th percentile. Easy premium."),
    ("thetagang", 2, "u/WheelStrategy", "My wheel on $AAPL has generated $14K in premium this year. 22% annualized return."),
    ("vitards", 2, "u/SteelMagnate", "Cleveland-Cliffs earnings preview: HRC prices up 30% from trough. $CLF undervalued at 4x forward earnings."),
    ("vitards", 2, "u/CommodityKing", "Copper thesis update: Chile production down 8%. Green transition demand growing 15% YoY. Supply deficit widening."),
    ("maxjustrisk", 2, "u/MaxJustRisk_DD", "Gamma exposure analysis on $GME: dealer hedging could force a squeeze above $30. Market maker positioning is extreme."),

    # Tier 3 — consumer distress
    ("personalfinance", 3, "u/BudgetingPro", "Just found out my health insurance premium is going up 25% next year. How is anyone supposed to budget for this?"),
    ("personalfinance", 3, "u/DebtFreeGoal", "Paid off $87K in student loans in 3 years. Here's my exact plan and budget breakdown."),
    ("povertyfinance", 3, "u/StruggleBus2024", "My rent went from $1,200 to $1,800 in one year. Same apartment. Same everything. Just greed."),
    ("povertyfinance", 3, "u/MinWageLife", "Working 60 hours a week at two jobs and still can't afford groceries for the month. Something is fundamentally broken."),

    # Tier 4 — tech employment
    ("cscareerquestions", 4, "u/NewGradDev", "Applied to 500+ jobs in 4 months. 3 interviews. 0 offers. Is the market really this bad for new grads?"),
    ("cscareerquestions", 4, "u/SeniorSWE", "Got laid off from Google L6 position. 15 YoE. Market is brutal even for senior engineers."),
    ("recruitinghell", 4, "u/GhostJobVictim", "Company had me do 6 rounds of interviews over 2 months, then said they filled the role internally. Should be illegal."),
    ("recruitinghell", 4, "u/ResumeBlackHole", "Applied to 200 jobs through company websites. 0 responses. Applied to 10 via referral. 8 interviews. The system is broken."),

    # Tier 5 — housing
    ("realestate", 5, "u/PropInvestor", "Commercial real estate distress spreading. Office vacancy rates hit 20% in major metros."),
    ("realestate", 5, "u/FirstTimeBuyer", "Offered $50K over asking, waived inspection, still lost the bid. Is it even possible to buy a house anymore?"),
    ("REBubble", 5, "u/BubbleWatcher", "Median home price to median income ratio at 7.2x. Historic average is 3.5x. This is not sustainable."),
    ("REBubble", 5, "u/CrashPrediction", "New listings up 40% in Phoenix, Austin, and Boise. Price cuts accelerating. The correction is here."),

    # Tier 6 — degen sentiment
    ("Superstonk", 6, "u/DiamondHands420", "DRS numbers keep climbing. 76M shares direct registered. Float is getting locked. Hedgies are trapped."),
    ("Superstonk", 6, "u/ApeStrong", "New SEC filing shows short interest at 140% again. We've seen this movie before. Buckle up."),
    ("cryptocurrency", 6, "u/CryptoWhale", "Bitcoin ETF inflows hit $500M today. Institutional adoption accelerating ahead of halving."),
    ("cryptocurrency", 6, "u/AltcoinHunter", "Solana TVL just passed $5B. The ecosystem is exploding. ETH maxis in shambles."),
    ("ethtrader", 6, "u/ETHMaxi", "Ethereum staking yield at 5.2% with MEV. Better returns than most bonds and it's deflationary. No brainer."),

    # Tier 7 — company-specific
    ("tesla", 7, "u/TeslaFanboy", "FSD v12.5 is insane. Drove from SF to LA with zero interventions. Robotaxi is closer than anyone thinks."),
    ("tesla", 7, "u/TeslaBear", "Tesla deliveries missed by 20K units. Margins compressing. Competition from BYD intensifying. $TSLA overvalued."),
    ("apple", 7, "u/AppleInvestor", "Apple Intelligence is the most underrated catalyst. 1.2B devices getting AI upgrades = massive services revenue."),
    ("apple", 7, "u/iPhoneFan", "Vision Pro returns reportedly at 50%. But iPhone 16 demand is strong. Services carrying the stock."),
    ("amazon", 7, "u/AWSBull", "AWS revenue reaccelerating to 17% growth. AI workloads driving new capacity builds. $AMZN at 15x forward."),
    ("nvidia", 7, "u/JensenFan", "Blackwell GPU orders are 12 months backlogged. Every hyperscaler wants more. $NVDA data center TAM is $300B+."),
    ("nvidia", 7, "u/ChipSkeptic", "NVDA at 40x forward earnings with competitors emerging. AMD MI300X gaining traction. Multiple compression coming."),

    # Original extras (tier 0)
    ("economics", 0, "u/EconPhD", "CPI came in hot at 3.4%. Core services ex-housing still sticky. Fed is not cutting in March."),
    ("economics", 0, "u/DataNerd88", "PMI data shows manufacturing contraction for 5th straight month. Services holding up but slowing."),
    ("technology", 0, "u/TechWatcher", "TSMC earnings call: AI chip demand exceeding capacity. Raising capex guidance by $5B."),
    ("technology", 0, "u/SiliconInsider", "Intel losing another major data center contract to AMD. Market share down to 37%."),
    ("worldnews", 0, "u/GlobalWatch", "OPEC+ emergency meeting called -- Saudi Arabia pushing for 2M bbl/day production cut."),
    ("worldnews", 0, "u/GeoPolitics101", "EU approving new sanctions package targeting Russian LNG exports. Energy prices spiking."),
    ("business", 0, "u/CorpWatcher", "Boeing 737 MAX production halted again after new quality defect found. $BA down 8% premarket."),
    ("business", 0, "u/RetailTracker", "Walmart Q3 blowout: same-store sales +7.4%. Stealing market share from everyone."),
    ("finance", 0, "u/FinAnalyst", "JPMorgan net interest income forecast raised to $91B. Banking sector earnings surprising to the upside."),
    ("finance", 0, "u/CreditWatch", "High-yield spreads widening to 450bps. Stress emerging in CCC-rated debt."),
    ("stockmarket", 0, "u/ChartMaster", "SPX hitting resistance at 4800. RSI divergence on daily. Classic distribution pattern."),
    ("SecurityAnalysis", 0, "u/DCF_Master", "My $MSFT model shows 15% upside from here. Azure growth reaccelerating to 31%."),
    ("geopolitics", 0, "u/RiskAnalyst", "Taiwan strait tensions escalating. China conducting military exercises. Semiconductor supply chain at risk."),
    ("energy", 0, "u/OilTrader", "Natural gas inventories way below 5-year average. If winter is cold, $UNG could double."),
    ("biotech", 0, "u/PharmaDD", "Novo Nordisk GLP-1 data blowing away expectations. Ozempic/Wegovy TAM expanding to $100B+."),
    ("semiconductor", 0, "u/ChipHead", "ASML order backlog at record levels. EUV demand from every major fab."),
    ("CryptoCurrency", 0, "u/CryptoWhale", "Bitcoin ETF inflows hit $500M today. Institutional adoption accelerating ahead of halving."),
    ("supplychain", 0, "u/LogisticsGuru", "Red Sea disruptions adding 10-14 days to Asia-Europe shipping. Container rates up 300%."),
    ("electricvehicles", 0, "u/EVFanatic", "BYD outselling Tesla globally for first time. Chinese EVs flooding European market."),
    ("aerospace", 0, "u/AeroAnalyst", "Airbus order backlog at 8,500 aircraft. Industry capacity constrained for next decade."),
    ("banking", 0, "u/BankWatcher", "Regional bank CRE exposure is the next shoe to drop. $NYCB down 40% this week."),
    ("insurance", 0, "u/ActuaryAnon", "Climate-related insurance losses up 40% YoY. Florida homeowners market in crisis."),
    ("retailnews", 0, "u/ShopperInsights", "Target announcing 200 store closures. Shrinkage costs up $1.2B this year."),
    ("manufacturing", 0, "u/FactoryFloor", "US reshoring boom: 350K manufacturing jobs announced this year. Biggest wave since 1950s."),
    ("mining", 0, "u/CopperBull", "Copper supply deficit widening. Chile production down 8%. Green transition needs 3x current output."),
    ("shipping", 0, "u/FreightWatch", "Baltic Dry Index surging on China stimulus hopes. Bulk carrier rates up 50% this month."),
    ("agriculture", 0, "u/FarmReport", "USDA crop report: corn yields below expectations. Drought in Midwest worsening."),
    ("pharmaceuticals", 0, "u/RxAnalyst", "Pfizer writedown on COVID assets. Pipeline restructuring. Activist investor building stake."),
]


class MockRedditScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "reddit"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._cycle: int = 0

    async def scrape(self) -> list[dict[str, Any]]:
        cycle = self._cycle
        self._cycle += 1

        # Filter mock templates to match the tier schedule
        active_subs = {s.lower() for s in _subreddits_for_cycle(cycle)}
        eligible = [t for t in _MOCK_TEMPLATES if t[0].lower() in active_subs]

        count = random.randint(30, 50)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            sub, tier, author, content = random.choice(eligible)
            noise = random.randint(1000, 9999)
            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}_{noise}",
                author=author,
                content=content,
                url=f"https://reddit.com/r/{sub}/comments/{self._generate_id()}",
                raw_metadata={
                    "subreddit": sub,
                    "tier": tier,
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
