"""Reddit web scraper — uses public JSON endpoints (no API key required).

Fetches /new.json from tiered subreddits, calculates upvote velocity,
and flags fast-rising posts.  Rate-limit friendly with 1-second delays
between requests and exponential back-off on 429s.

Subreddit tiers control scrape frequency:
  TIER 1 — every cycle  (core finance + crypto)
  TIER 2 — every 2nd cycle  (sophisticated retail DD)
  TIER 3 — every 3rd cycle  (consumer/distress/housing)
  TIER 4 — every 3rd cycle  (company-specific tickers)
  TIER 5 — every 4th cycle  (sector-specific)
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# ── User-Agent for Reddit public JSON endpoints ──────────────────────

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
_REQUEST_TIMEOUT = 15

# ── Tiered subreddits ────────────────────────────────────────────────

_TIER_1: list[str] = [
    "wallstreetbets", "stocks", "options", "investing",
    "cryptocurrency", "StockMarket", "Daytrading",
]

_TIER_2: list[str] = [
    "thetagang", "vitards", "maxjustrisk", "FluentInFinance",
    "SecurityAnalysis", "ValueInvesting", "dividends", "Bogleheads",
    "ethtrader", "defi", "CryptoMoonShots",
]

_TIER_3: list[str] = [
    "personalfinance", "povertyfinance", "frugal",
    "cscareerquestions", "recruitinghell", "antiwork", "workreform",
    "realestate", "REBubble", "FirstTimeHomeBuyer",
    "Superstonk", "FIRE", "fatFIRE",
]

_TIER_4: list[str] = [
    "tesla", "TSLA", "apple", "amazon", "nvidia", "AMD", "intel",
    "google", "microsoft", "meta", "netflix", "disney", "costco",
    "Rivian", "palantir", "sofi", "shopify", "coinbase", "robinhood",
]

_TIER_5: list[str] = [
    "semiconductors", "biotech", "weedstocks", "uranium", "lithium",
    "solar", "electricvehicles", "SpaceX", "defense", "energy",
    "commercialrealestate", "insurance",
]

# Original extras kept for broad coverage (scraped every cycle)
_ORIGINAL_EXTRA: list[str] = [
    "economics", "technology", "worldnews", "politics", "news",
    "business", "finance", "stockmarket", "geopolitics",
    "supplychain", "semiconductor", "pharmaceuticals", "aerospace",
    "banking", "retailnews", "manufacturing", "mining",
    "shipping", "agriculture",
]


# ── Tier bookkeeping ─────────────────────────────────────────────────

_TIER_MAP: dict[str, int] = {}
for _sub in _TIER_1:
    _TIER_MAP[_sub.lower()] = 1
for _sub in _TIER_2:
    _TIER_MAP[_sub.lower()] = 2
for _sub in _TIER_3:
    _TIER_MAP[_sub.lower()] = 3
for _sub in _TIER_4:
    _TIER_MAP[_sub.lower()] = 4
for _sub in _TIER_5:
    _TIER_MAP[_sub.lower()] = 5
for _sub in _ORIGINAL_EXTRA:
    if _sub.lower() not in _TIER_MAP:
        _TIER_MAP[_sub.lower()] = 0


def _get_tier(subreddit_name: str) -> int:
    """Return the tier number for a subreddit (0 = original/untiered)."""
    return _TIER_MAP.get(subreddit_name.lower(), 0)


def _subreddits_for_cycle(cycle: int) -> list[str]:
    """Return which subreddits should be scraped on a given cycle.

    - Tier 1 + original extras: every cycle
    - Tier 2: every 2nd cycle
    - Tier 3 + 4: every 3rd cycle
    - Tier 5: every 4th cycle
    """
    subs: list[str] = []

    # Always scrape tier 1 and original extras
    subs.extend(_TIER_1)
    subs.extend(_ORIGINAL_EXTRA)

    # Every 2nd cycle
    if cycle % 2 == 0:
        subs.extend(_TIER_2)

    # Every 3rd cycle
    if cycle % 3 == 0:
        subs.extend(_TIER_3)
        subs.extend(_TIER_4)

    # Every 4th cycle
    if cycle % 4 == 0:
        subs.extend(_TIER_5)

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for s in subs:
        key = s.lower()
        if key not in seen:
            seen.add(key)
            result.append(s)
    return result


# Backward-compatible flat list (union of all unique subreddits)
SUBREDDITS = list(dict.fromkeys(
    _TIER_1 + _TIER_2 + _TIER_3 + _TIER_4 + _TIER_5 + _ORIGINAL_EXTRA
))

# ── Upvote-velocity threshold for fast-rising flag ───────────────────

_FAST_RISING_THRESHOLD = 5.0


class RedditWebScraper(BaseScraper):
    """Scrapes Reddit via public JSON endpoints (no API key required).

    Uses ``https://www.reddit.com/r/{sub}/new.json`` with a well-behaved
    user-agent.  Includes 1-second inter-request delays and exponential
    back-off on 429 responses.
    """

    @property
    def platform(self) -> str:
        return "reddit"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._cycle: int = 0
        self._seen_ids: set[str] = set()

    async def _fetch_subreddit(
        self,
        client: httpx.AsyncClient,
        name: str,
    ) -> list[dict[str, Any]]:
        """Fetch /new.json for a single subreddit with back-off on 429."""
        tier = _get_tier(name)
        url = f"https://old.reddit.com/r/{name}/new.json?limit=25"
        posts: list[dict[str, Any]] = []

        # Exponential back-off: 5 s, 10 s, 20 s
        backoff_delays = [5, 10, 20]

        for attempt in range(len(backoff_delays) + 1):
            try:
                resp = await client.get(url)

                if resp.status_code == 429:
                    if attempt < len(backoff_delays):
                        wait = backoff_delays[attempt]
                        logger.warning(
                            "[reddit] 429 on r/%s — backing off %ds (attempt %d)",
                            name, wait, attempt + 1,
                        )
                        await asyncio.sleep(wait)
                        continue
                    else:
                        logger.warning("[reddit] 429 on r/%s — giving up after %d attempts", name, attempt + 1)
                        return posts

                resp.raise_for_status()
                data = resp.json()
                break

            except httpx.HTTPStatusError:
                logger.warning("[reddit] HTTP error on r/%s", name, exc_info=True)
                return posts
            except Exception:
                logger.warning("[reddit] failed to fetch r/%s", name, exc_info=True)
                return posts
        else:
            return posts

        now = time.time()

        for child in data.get("data", {}).get("children", []):
            post_data = child.get("data", {})
            post_id = post_data.get("id", "")

            if not post_id or post_id in self._seen_ids:
                continue
            self._seen_ids.add(post_id)

            title = post_data.get("title", "")
            selftext = (post_data.get("selftext") or "")[:3000]
            score = post_data.get("score", 0)
            num_comments = post_data.get("num_comments", 0)
            upvote_ratio = post_data.get("upvote_ratio", 0.0)
            created_utc = post_data.get("created_utc", now)
            author = post_data.get("author", "[deleted]")
            permalink = post_data.get("permalink", "")
            subreddit = post_data.get("subreddit", name)

            # Calculate upvote velocity
            age_minutes = max(1, (now - created_utc) / 60)
            upvote_velocity = score / age_minutes
            fast_rising = upvote_velocity > _FAST_RISING_THRESHOLD

            posts.append(self._make_post(
                source_id=post_id,
                author=author,
                content=f"{title}\n\n{selftext}",
                url=f"https://reddit.com{permalink}",
                raw_metadata={
                    "subreddit": subreddit,
                    "tier": tier,
                    "sort": "new",
                    "score": score,
                    "num_comments": num_comments,
                    "upvote_ratio": upvote_ratio,
                    "created_utc": created_utc,
                    "upvote_velocity": round(upvote_velocity, 2),
                    "fast_rising": fast_rising,
                    "permalink": permalink,
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        cycle = self._cycle
        self._cycle += 1

        subs_this_cycle = _subreddits_for_cycle(cycle)
        logger.info(
            "[reddit] cycle %d — scraping %d subreddits (tiers active: 1%s%s%s)",
            cycle,
            len(subs_this_cycle),
            "+2" if cycle % 2 == 0 else "",
            "+3-4" if cycle % 3 == 0 else "",
            "+5" if cycle % 4 == 0 else "",
        )

        all_posts: list[dict[str, Any]] = []

        async with httpx.AsyncClient(
            headers={"User-Agent": _USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
            follow_redirects=True,
        ) as client:
            for sub_name in subs_this_cycle:
                try:
                    posts = await self._fetch_subreddit(client, sub_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[reddit] failed to scrape r/%s", sub_name, exc_info=True)

                # 1-second delay between subreddit fetches to be polite
                await asyncio.sleep(1)

        # Final deduplication (belt-and-suspenders with _seen_ids)
        seen: set[str] = set()
        deduped: list[dict[str, Any]] = []
        for p in all_posts:
            if p["id"] not in seen:
                seen.add(p["id"])
                deduped.append(p)

        logger.info("[reddit] cycle %d — collected %d posts", cycle, len(deduped))
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
    ("cryptocurrency", 1, "u/CryptoWhale", "Bitcoin ETF inflows hit $500M today. Institutional adoption accelerating ahead of halving."),
    ("StockMarket", 1, "u/ChartMaster", "SPX hitting resistance at 5200. RSI divergence on daily. Classic distribution pattern forming."),
    ("Daytrading", 1, "u/ScalpKing", "Caught the $SPY breakout at open for a clean 2R trade. Level-to-level is the only way."),

    # Tier 2 — sophisticated retail DD
    ("thetagang", 2, "u/ThetaDecay", "Selling 45 DTE puts on $MSFT at the 380 strike. IV rank at 85th percentile. Easy premium."),
    ("thetagang", 2, "u/WheelStrategy", "My wheel on $AAPL has generated $14K in premium this year. 22% annualized return."),
    ("vitards", 2, "u/SteelMagnate", "Cleveland-Cliffs earnings preview: HRC prices up 30% from trough. $CLF undervalued at 4x forward earnings."),
    ("vitards", 2, "u/CommodityKing", "Copper thesis update: Chile production down 8%. Green transition demand growing 15% YoY. Supply deficit widening."),
    ("maxjustrisk", 2, "u/MaxJustRisk_DD", "Gamma exposure analysis on $GME: dealer hedging could force a squeeze above $30. Market maker positioning is extreme."),
    ("FluentInFinance", 2, "u/FinLiterate", "The US national debt just passed $34T. Interest payments now exceed defense spending. This matters."),
    ("SecurityAnalysis", 2, "u/DCF_Master", "My $MSFT model shows 15% upside from here. Azure growth reaccelerating to 31%."),
    ("ValueInvesting", 2, "u/GrahamFollower", "Deep value in Japanese small caps: trading at 0.5x book with pristine balance sheets."),
    ("dividends", 2, "u/DividendGrowth", "My portfolio yields 4.2% with 8% annual dividend growth. 15 years to financial freedom."),
    ("Bogleheads", 2, "u/IndexFundFan", "Your reminder that 90% of active managers underperform VTI over 15 years. Stay the course."),
    ("ethtrader", 2, "u/ETHMaxi", "Ethereum staking yield at 5.2% with MEV. Better returns than most bonds and it's deflationary."),
    ("defi", 2, "u/YieldFarmer", "Aave V3 on Arbitrum offering 8% on stables. Risk-adjusted this beats most TradFi."),
    ("CryptoMoonShots", 2, "u/GemHunter", "Found a new L2 project with $2M TVL and growing. Dev team is ex-Paradigm. NFA."),

    # Tier 3 — consumer distress / housing / memes
    ("personalfinance", 3, "u/BudgetingPro", "Just found out my health insurance premium is going up 25% next year. How is anyone supposed to budget for this?"),
    ("povertyfinance", 3, "u/StruggleBus2024", "My rent went from $1,200 to $1,800 in one year. Same apartment. Same everything. Just greed."),
    ("frugal", 3, "u/SaveEveryPenny", "Our family of 4 eats for $400/month. Here's our exact meal plan and shopping strategy."),
    ("cscareerquestions", 3, "u/NewGradDev", "Applied to 500+ jobs in 4 months. 3 interviews. 0 offers. Is the market really this bad for new grads?"),
    ("recruitinghell", 3, "u/GhostJobVictim", "Company had me do 6 rounds of interviews over 2 months, then said they filled the role internally."),
    ("antiwork", 3, "u/EnoughIsEnough", "CEO made $47M last year. Company just announced layoffs and a hiring freeze. Make it make sense."),
    ("workreform", 3, "u/FairWages", "Productivity is up 60% since 2000. Wages up 16%. Where did all the value go? We know where."),
    ("realestate", 3, "u/PropInvestor", "Commercial real estate distress spreading. Office vacancy rates hit 20% in major metros."),
    ("REBubble", 3, "u/BubbleWatcher", "Median home price to median income ratio at 7.2x. Historic average is 3.5x. This is not sustainable."),
    ("FirstTimeHomeBuyer", 3, "u/SavedForYears", "Finally got pre-approved at 6.8%. Houses in our range need $50K+ in repairs. Is this really it?"),
    ("Superstonk", 3, "u/DiamondHands420", "DRS numbers keep climbing. 76M shares direct registered. Float is getting locked."),
    ("FIRE", 3, "u/LeanFIRE", "Hit $1.2M invested at 38. Planning to pull the trigger at $1.5M. 3.5% SWR in a LCOL area."),
    ("fatFIRE", 3, "u/GoldenHandcuffs", "NW $8M at 42. TC $1.2M. Can't pull the trigger because lifestyle inflation keeps creeping."),

    # Tier 4 — company-specific
    ("tesla", 4, "u/TeslaFanboy", "FSD v12.5 is insane. Drove from SF to LA with zero interventions. Robotaxi is closer than anyone thinks."),
    ("tesla", 4, "u/TeslaBear", "Tesla deliveries missed by 20K units. Margins compressing. Competition from BYD intensifying."),
    ("nvidia", 4, "u/JensenFan", "Blackwell GPU orders are 12 months backlogged. Every hyperscaler wants more. $NVDA data center TAM is $300B+."),
    ("nvidia", 4, "u/ChipSkeptic", "NVDA at 40x forward earnings with competitors emerging. AMD MI300X gaining traction."),
    ("apple", 4, "u/AppleInvestor", "Apple Intelligence is the most underrated catalyst. 1.2B devices getting AI upgrades = massive services revenue."),
    ("amazon", 4, "u/AWSBull", "AWS revenue reaccelerating to 17% growth. AI workloads driving new capacity builds. $AMZN at 15x forward."),
    ("AMD", 4, "u/SuBae", "MI300X beating Nvidia H100 on inference benchmarks. AMD is taking real data center share for the first time."),
    ("intel", 4, "u/PatGelsingerFan", "Intel 18A test chips yielding well. If foundry business takes off, this is a $50 stock."),
    ("google", 4, "u/AlphabetBull", "Gemini 2.0 benchmarks crushing GPT-4. Google Cloud growing 29%. Still trading at a discount to MSFT."),
    ("microsoft", 4, "u/NadellaFan", "Copilot revenue run rate approaching $10B. This is just the beginning of the AI monetization cycle."),
    ("meta", 4, "u/MetaLong", "Threads hitting 200M MAU. Instagram Reels now profitable. Reality Labs losses narrowing."),
    ("palantir", 4, "u/PLTRBull", "Palantir AIP bootcamps converting at 65%. Government + commercial revenue both accelerating."),
    ("sofi", 4, "u/FinTechBull", "SoFi deposits grew 40% QoQ. Student loan payments resuming is a tailwind. Bank charter paying off."),
    ("coinbase", 4, "u/CoinbaseLong", "Coinbase Q4 revenue beat. Derivatives exchange growing 300% YoY. Base L2 gaining traction."),
    ("robinhood", 4, "u/HOODTrader", "Robinhood crypto revenue up 200%. Gold subscriptions hit 2M. This is not a meme stock anymore."),

    # Tier 5 — sector-specific
    ("semiconductors", 5, "u/ChipHead", "ASML order backlog at record levels. EUV demand from every major fab."),
    ("biotech", 5, "u/PharmaDD", "Novo Nordisk GLP-1 data blowing away expectations. Ozempic/Wegovy TAM expanding to $100B+."),
    ("weedstocks", 5, "u/CannabisInvestor", "German legalization driving EU cannabis market. Tilray positioned for first-mover advantage."),
    ("uranium", 5, "u/NuclearBull", "Uranium spot at $90/lb. Utilities scrambling to secure supply. $CCJ printing money."),
    ("lithium", 5, "u/BatteryMetals", "Lithium oversupply narrative is overblown. EV demand curve still exponential long term."),
    ("solar", 5, "u/SolarBull", "Utility-scale solar now cheapest electricity source in 90% of the world. $ENPH and $SEDG oversold."),
    ("electricvehicles", 5, "u/EVFanatic", "BYD outselling Tesla globally for first time. Chinese EVs flooding European market."),
    ("SpaceX", 5, "u/RocketWatcher", "Starship flight 4 success rate improving. Starlink revenue reportedly $6B annualized."),
    ("defense", 5, "u/DefenseBull", "Global defense spending up 12% YoY. $LMT, $RTX, $NOC backlogs at record levels."),
    ("energy", 5, "u/OilTrader", "Natural gas inventories way below 5-year average. If winter is cold, $UNG could double."),
    ("commercialrealestate", 5, "u/CREAnalyst", "Office CMBS delinquency rate hits 7.5%. Regional bank exposure is the next shoe to drop."),
    ("insurance", 5, "u/ActuaryAnon", "Climate-related insurance losses up 40% YoY. Florida homeowners market in crisis."),

    # Original extras (tier 0)
    ("economics", 0, "u/EconPhD", "CPI came in hot at 3.4%. Core services ex-housing still sticky. Fed is not cutting in March."),
    ("technology", 0, "u/TechWatcher", "TSMC earnings call: AI chip demand exceeding capacity. Raising capex guidance by $5B."),
    ("worldnews", 0, "u/GlobalWatch", "OPEC+ emergency meeting called -- Saudi Arabia pushing for 2M bbl/day production cut."),
    ("business", 0, "u/CorpWatcher", "Boeing 737 MAX production halted again after new quality defect found. $BA down 8% premarket."),
    ("finance", 0, "u/FinAnalyst", "JPMorgan net interest income forecast raised to $91B. Banking sector earnings surprising to the upside."),
    ("geopolitics", 0, "u/RiskAnalyst", "Taiwan strait tensions escalating. China conducting military exercises. Semiconductor supply chain at risk."),
    ("supplychain", 0, "u/LogisticsGuru", "Red Sea disruptions adding 10-14 days to Asia-Europe shipping. Container rates up 300%."),
    ("manufacturing", 0, "u/FactoryFloor", "US reshoring boom: 350K manufacturing jobs announced this year. Biggest wave since 1950s."),
    ("mining", 0, "u/CopperBull", "Copper supply deficit widening. Chile production down 8%. Green transition needs 3x current output."),
    ("shipping", 0, "u/FreightWatch", "Baltic Dry Index surging on China stimulus hopes. Bulk carrier rates up 50% this month."),
    ("agriculture", 0, "u/FarmReport", "USDA crop report: corn yields below expectations. Drought in Midwest worsening."),
]


class MockRedditWebScraper(BaseScraper):
    """Mock scraper that generates realistic Reddit-style posts for testing."""

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
        now = time.time()

        for _ in range(count):
            sub, tier, author, content = random.choice(eligible)
            noise = random.randint(1000, 9999)
            score = random.randint(1, 15000)
            age_minutes = random.randint(5, 600)
            upvote_velocity = round(score / max(1, age_minutes), 2)
            fast_rising = upvote_velocity > _FAST_RISING_THRESHOLD

            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}_{noise}",
                author=author,
                content=content,
                url=f"https://reddit.com/r/{sub}/comments/{self._generate_id()}",
                raw_metadata={
                    "subreddit": sub,
                    "tier": tier,
                    "sort": "new",
                    "score": score,
                    "num_comments": random.randint(5, 800),
                    "upvote_ratio": round(random.uniform(0.55, 0.99), 2),
                    "created_utc": now - (age_minutes * 60),
                    "upvote_velocity": upvote_velocity,
                    "fast_rising": fast_rising,
                    "permalink": f"/r/{sub}/comments/{self._generate_id()}/mock_post/",
                },
            ))
        return posts
