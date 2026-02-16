"""News RSS feed scraper — monitors 55+ financial/business news feeds.

Feeds are organized by sector and scraped in rotating batches of 3 to
avoid hammering all endpoints simultaneously.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from typing import Any

import feedparser

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# ── Feed catalogue (55+ feeds, organised by sector) ──────────────────

RSS_FEEDS: dict[str, str] = {
    # ── FINANCIAL NEWS (general) ──────────────────────────────────────
    "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
    "reuters_markets": "https://feeds.reuters.com/reuters/marketsNews",
    "ap_business": "https://feeds.apnews.com/rss/apf-business",
    "cnbc_top": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100003114",
    "cnbc_world": "https://search.cnbc.com/rs/search/combinedcms/view.xml?partnerId=wrss01&id=100727362",
    "marketwatch_top": "https://feeds.marketwatch.com/marketwatch/topstories/",
    "marketwatch_markets": "https://feeds.marketwatch.com/marketwatch/marketpulse/",
    "seekingalpha_market": "https://seekingalpha.com/market_currents.xml",
    "seekingalpha_news": "https://seekingalpha.com/feed.xml",
    "barrons": "https://www.barrons.com/feed",
    "zerohedge": "https://www.zerohedge.com/fullrss2.xml",
    "wolfstreet": "https://wolfstreet.com/feed/",
    "nakedcapitalism": "https://www.nakedcapitalism.com/feed",
    "calculatedrisk": "https://www.calculatedriskblog.com/feeds/posts/default?alt=rss",
    "motleyfool": "https://feeds.fool.com/usmf/",
    "investopedia": "https://www.investopedia.com/feedbuilder/feed/getfeed?feedName=rss_headline",
    "yahoo_finance": "https://feeds.finance.yahoo.com/rss/2.0/headline",

    # ── TECH ──────────────────────────────────────────────────────────
    "techcrunch": "https://techcrunch.com/feed/",
    "arstechnica": "https://feeds.arstechnica.com/arstechnica/index",
    "theverge": "https://www.theverge.com/rss/index.xml",
    "wired": "https://www.wired.com/feed/rss",
    "siliconangle": "https://siliconangle.com/feed/",
    "venturebeat": "https://venturebeat.com/feed/",
    "9to5mac": "https://9to5mac.com/feed/",
    "9to5google": "https://9to5google.com/feed/",
    "electrek": "https://electrek.co/feed/",

    # ── HEALTHCARE / PHARMA ───────────────────────────────────────────
    "fiercepharma": "https://www.fiercepharma.com/rss/xml",
    "statnews": "https://www.statnews.com/feed/",
    "biopharmadive": "https://www.biopharmadive.com/feeds/news/",
    "endpts": "https://endpts.com/feed/",

    # ── ENERGY / COMMODITIES ──────────────────────────────────────────
    "oilprice": "https://oilprice.com/rss/main",
    "mining": "https://www.mining.com/feed/",
    "rigzone": "https://www.rigzone.com/news/rss/rigzone_latest.aspx",

    # ── SUPPLY CHAIN ──────────────────────────────────────────────────
    "supplychaindive": "https://www.supplychaindive.com/feeds/news/",
    "freightwaves": "https://www.freightwaves.com/feed",

    # ── REAL ESTATE ───────────────────────────────────────────────────
    "housingwire": "https://www.housingwire.com/feed/",
    "therealdeal": "https://therealdeal.com/feed/",

    # ── CONSUMER / RETAIL ─────────────────────────────────────────────
    "retaildive": "https://www.retaildive.com/feeds/news/",
    "grocerydive": "https://www.grocerydive.com/feeds/news/",
    "fooddive": "https://www.fooddive.com/feeds/news/",
    "restaurantdive": "https://www.restaurantdive.com/feeds/news/",

    # ── CRYPTO ────────────────────────────────────────────────────────
    "cointelegraph": "https://cointelegraph.com/rss",
    "coindesk": "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "thedefiant": "https://thedefiant.io/feed",
    "decrypt": "https://decrypt.co/feed",
    "theblock": "https://www.theblock.co/rss.xml",

    # ── MACRO / POLICY ────────────────────────────────────────────────
    "fed_press": "https://www.federalreserve.gov/feeds/press_all.xml",
    "politico": "https://rss.politico.com/economy.xml",
    "thehill": "https://thehill.com/feed/",

    # ── INTERNATIONAL ─────────────────────────────────────────────────
    "bbc_business": "https://feeds.bbci.co.uk/news/business/rss.xml",
    "aljazeera": "https://www.aljazeera.com/xml/rss/all.xml",
    "ft": "https://www.ft.com/rss/home",
    "economist": "https://www.economist.com/finance-and-economics/rss.xml",
    "scmp_economy": "https://www.scmp.com/rss/91/feed",
    "nikkei": "https://asia.nikkei.com/rss",
    "economictimes_india": "https://economictimes.indiatimes.com/rssfeedstopstories.cms",
}

# Number of batches the feed list is split into for rotation
_NUM_BATCHES = 3


def _split_into_batches(
    feeds: dict[str, str], num_batches: int
) -> list[list[tuple[str, str]]]:
    """Deterministically split *feeds* into *num_batches* roughly equal batches."""
    items = list(feeds.items())
    batches: list[list[tuple[str, str]]] = [[] for _ in range(num_batches)]
    for idx, item in enumerate(items):
        batches[idx % num_batches].append(item)
    return batches


_FEED_BATCHES = _split_into_batches(RSS_FEEDS, _NUM_BATCHES)


class NewsRSSScraper(BaseScraper):
    """Fetch and deduplicate entries from financial RSS feeds.

    Feeds are rotated across 3 batches so that each cycle only hits ~1/3 of
    all endpoints, reducing overall request volume and latency.
    """

    @property
    def platform(self) -> str:
        return "news_rss"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._seen_urls: set[str] = set()
        self._batch_index: int = 0

    @staticmethod
    def _hash_url(url: str) -> str:
        return hashlib.sha256(url.encode()).hexdigest()[:16]

    def _parse_feed(self, feed_name: str, feed_url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:20]:
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
        batch = _FEED_BATCHES[self._batch_index % _NUM_BATCHES]
        self._batch_index += 1

        logger.info(
            "[news_rss] batch %d/%d — scraping %d feeds",
            (self._batch_index - 1) % _NUM_BATCHES + 1,
            _NUM_BATCHES,
            len(batch),
        )

        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for name, url in batch:
            posts = await loop.run_in_executor(None, self._parse_feed, name, url)
            all_posts.extend(posts)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_HEADLINES = [
    # Financial news (general)
    ("reuters_business", "Fed signals patience on rate cuts amid sticky inflation data"),
    ("reuters_markets", "S&P 500 hits record high on AI optimism, Nvidia leads gains"),
    ("ap_business", "US job growth surges past expectations; unemployment falls to 3.5%"),
    ("cnbc_top", "Tesla recalls 2M vehicles over Autopilot safety concerns"),
    ("cnbc_world", "China cuts reserve requirement ratio to boost flagging economy"),
    ("marketwatch_top", "10-year Treasury yield surges past 4.5% on hot CPI data"),
    ("marketwatch_markets", "Gold hits all-time high above $2,200 on geopolitical risks"),
    ("seekingalpha_market", "Apple suppliers warn of weakening iPhone demand in China"),
    ("seekingalpha_news", "Goldman Sachs upgrades semiconductor sector to Overweight"),
    ("barrons", "Why the market rally is broadening beyond tech megacaps"),
    ("zerohedge", "Commercial real estate losses accelerating at regional banks"),
    ("wolfstreet", "Auto loan delinquencies surge to highest level since 2010"),
    ("nakedcapitalism", "Shadow banking risks resurface as private credit market balloons to $1.7T"),
    ("calculatedrisk", "Housing starts disappoint again as builder confidence fades"),
    ("motleyfool", "3 dividend stocks that could double your money in 5 years"),
    ("investopedia", "What the inverted yield curve is telling us about the economy now"),
    ("yahoo_finance", "Magnificent Seven earnings season kicks off with mixed results"),

    # Tech
    ("techcrunch", "OpenAI valued at $80B in latest funding round; revenue tripling"),
    ("arstechnica", "Intel Lunar Lake benchmarks show 50% efficiency gains over Raptor Lake"),
    ("theverge", "Apple Vision Pro sales slow after initial launch burst"),
    ("wired", "Inside the race to build AI chips that rival Nvidia"),
    ("siliconangle", "Snowflake partners with Anthropic to bring AI to enterprise data lakes"),
    ("venturebeat", "Mistral AI closes $600M round at $6B valuation"),
    ("9to5mac", "iOS 18 adoption rate outpacing iOS 17 by 40% in first month"),
    ("9to5google", "Google Pixel 9 Pro leaks reveal custom Tensor G4 chip benchmarks"),
    ("electrek", "Tesla Megapack installations surge 200% as grid storage demand explodes"),

    # Healthcare / Pharma
    ("fiercepharma", "Novo Nordisk GLP-1 supply shortages expected to persist through 2025"),
    ("statnews", "FDA approves first CRISPR gene therapy in historic decision"),
    ("biopharmadive", "Pfizer slashes 500 jobs as it restructures post-COVID portfolio"),
    ("endpts", "Moderna pivots to combination vaccines as standalone COVID shots decline"),

    # Energy / Commodities
    ("oilprice", "OPEC+ extends production cuts as oil demand growth slows"),
    ("mining", "Lithium prices collapse 70% from peak; miners suspending operations"),
    ("rigzone", "US shale production plateaus as drillers prioritize returns over growth"),

    # Supply chain
    ("supplychaindive", "Red Sea disruptions add $2,000 per container to Asia-Europe shipping"),
    ("freightwaves", "Trucking spot rates hit two-year low amid freight recession"),

    # Real estate
    ("housingwire", "Mortgage rates climb back above 7% crushing refi demand"),
    ("therealdeal", "Manhattan office vacancy hits record 22% as remote work persists"),

    # Consumer / Retail
    ("retaildive", "Target announces 200 store closures citing urban shrinkage losses"),
    ("grocerydive", "Grocery inflation moderates but consumers still trading down to store brands"),
    ("fooddive", "Nestle restructures North American operations cutting 2,000 jobs"),
    ("restaurantdive", "Red Lobster files for bankruptcy amid $1B in lease liabilities"),

    # Crypto
    ("cointelegraph", "Bitcoin halving countdown: miners brace for revenue impact"),
    ("coindesk", "Bitcoin ETF sees record $1.2B inflow; BTC breaks $60,000"),
    ("thedefiant", "Ethereum restaking TVL surpasses $10B as EigenLayer launches mainnet"),
    ("decrypt", "Solana DeFi volume overtakes Ethereum for first time"),
    ("theblock", "Ethereum layer-2 activity hits new high; gas fees plunge"),

    # Macro / Policy
    ("fed_press", "Federal Reserve holds rates steady, signals two cuts possible in 2025"),
    ("politico", "Congress passes $95B aid package with implications for defense stocks"),
    ("thehill", "White House announces new tariffs on Chinese EVs and semiconductors"),

    # International
    ("bbc_business", "UK enters technical recession as GDP contracts 0.3% in Q4"),
    ("aljazeera", "OPEC+ agrees to extend production cuts through Q2 2025"),
    ("ft", "European banks face $50B in unrealized losses on commercial property"),
    ("economist", "Global supply chains rewiring: winners and losers of friendshoring"),
    ("scmp_economy", "China property crisis deepens as Country Garden misses bond payment"),
    ("nikkei", "Japan chip investment boom: $30B in new fab construction announced"),
    ("economictimes_india", "India GDP growth accelerates to 8.4% making it fastest growing major economy"),

    # Extra general headlines
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
                    "tags": random.sample(
                        ["markets", "economy", "fed", "earnings", "tech",
                         "energy", "crypto", "pharma", "realestate", "supply_chain",
                         "consumer", "commodities"],
                        2,
                    ),
                },
            ))
        return posts
