"""Twitter/X scraper — uses API v2 when bearer token is set, Apify fallback otherwise.

Uses Twitter API v2 recent search endpoint via httpx (primary mode).
Falls back to Apify pay-per-result actor ($0.25/1K tweets) when no bearer token.

Query categories:
  - MEGA_CAP: Major tickers and earnings
  - MACRO: Fed, inflation, rates, commodities
  - SMALL_ACCOUNT_ALPHA: Low-follower accounts discussing specific tickers
  - UNUSUAL_VOLUME: Trending tickers and unusual options activity
  - CORPORATE_INSIDER: C-suite of public companies
  - SEC_FILING_BOTS: SEC filing and Edgar bot accounts
  - CRYPTO_WHALE: Whale alerts and on-chain analytics
  - POLITICAL_REGULATORY: Political and regulatory accounts
  - EMPLOYMENT_SIGNALS: Layoffs, hiring, restructuring
"""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_TWITTER_SEARCH = "https://api.twitter.com/2/tweets/search/recent"

# ── Query categories ──────────────────────────────────────────────────

_QUERIES_MEGA_CAP = [
    "$AAPL OR $TSLA OR $NVDA OR $MSFT OR $AMZN lang:en -is:retweet",
    "$GOOG OR $META OR $NFLX OR $BRK.B OR $JPM lang:en -is:retweet",
    "$SPY OR $QQQ OR #stockmarket OR #earnings lang:en -is:retweet",
]

_QUERIES_MACRO = [
    "Federal Reserve OR #inflation OR CPI OR interest rates lang:en -is:retweet",
    "#oil OR #gold OR #commodities OR $CL_F OR $GC_F lang:en -is:retweet",
    "OPEC OR sanctions OR tariffs OR trade war lang:en -is:retweet",
    "Treasury yield OR bond market OR credit spread lang:en -is:retweet",
]

_QUERIES_SMALL_ACCOUNT_ALPHA = [
    # Small accounts (under 5K followers) often post DD before big moves
    "($SMCI OR $PLTR OR $MSTR OR $ARM OR $RKLB) lang:en -is:retweet has:cashtags",
    "($IONQ OR $RGTI OR $QBTS OR $QUBT) lang:en -is:retweet has:cashtags",
    "($LUNR OR $ASTS OR $ASTR OR $RDW) lang:en -is:retweet has:cashtags",
    "(\"due diligence\" OR \"DD thread\" OR \"deep dive\") ($SPY OR $QQQ) lang:en -is:retweet",
    "(\"price target\" OR \"bull case\" OR \"bear case\") lang:en -is:retweet has:cashtags",
]

_QUERIES_UNUSUAL_VOLUME = [
    # Options flow and unusual volume detection
    "(\"unusual volume\" OR \"options sweep\" OR \"call sweep\" OR \"put sweep\") lang:en -is:retweet",
    "(\"dark pool\" OR \"block trade\" OR \"whale alert\") (stock OR options) lang:en -is:retweet",
    "(\"short interest\" OR \"short squeeze\" OR \"days to cover\") lang:en -is:retweet has:cashtags",
    "(\"insider buying\" OR \"insider purchase\" OR \"Form 4\") lang:en -is:retweet",
    "(\"gap up\" OR \"gap down\" OR \"breaking out\" OR \"breaking down\") lang:en -is:retweet has:cashtags",
]

_QUERIES_CORPORATE_INSIDER = [
    # C-suite and corporate accounts
    "(from:elonmusk OR from:sataborsky OR from:timaborsky) lang:en -is:retweet",
    "(from:sataborsky OR from:sundaborsky) (AI OR revenue OR growth OR earnings) lang:en -is:retweet",
    "(CEO OR CFO OR \"chief executive\") (\"stepping down\" OR \"appointed\" OR resignation) lang:en -is:retweet",
    "(\"board of directors\" OR \"shareholder letter\" OR \"guidance raised\" OR \"guidance lowered\") lang:en -is:retweet",
]

_QUERIES_SEC_FILING = [
    # SEC filing bots and EDGAR watchers
    "(from:SECGov OR from:sec_enforcement) lang:en -is:retweet",
    "(\"13F filing\" OR \"13D filing\" OR \"S-1 filing\" OR \"10-K\" OR \"10-Q\") lang:en -is:retweet",
    "(\"SEC investigation\" OR \"SEC charges\" OR \"SEC settlement\") lang:en -is:retweet",
    "(\"proxy statement\" OR \"activist investor\" OR \"poison pill\" OR \"tender offer\") lang:en -is:retweet",
]

_QUERIES_CRYPTO_WHALE = [
    # Crypto whale alerts and on-chain analytics
    "$BTC OR $ETH OR #crypto OR #bitcoin lang:en -is:retweet",
    "(from:whale_alert OR from:lookonchain OR from:ai_9684xtpa) lang:en -is:retweet",
    "(\"whale transfer\" OR \"whale wallet\" OR \"whale accumulation\") (BTC OR ETH) lang:en -is:retweet",
    "(\"Bitcoin ETF\" OR \"Ethereum ETF\") (inflow OR outflow OR approval) lang:en -is:retweet",
    "(\"on-chain\" OR \"DeFi\" OR \"TVL\") (surge OR crash OR record) lang:en -is:retweet",
]

_QUERIES_POLITICAL_REGULATORY = [
    # Political and regulatory accounts affecting markets
    "(from:WhiteHouse OR from:ABORSKYCFPB OR from:FTC) (economy OR consumer OR regulation) lang:en -is:retweet",
    "(antitrust OR regulation OR \"executive order\") (tech OR AI OR crypto) lang:en -is:retweet",
    "(\"debt ceiling\" OR \"government shutdown\" OR \"continuing resolution\") lang:en -is:retweet",
    "(\"trade ban\" OR \"export controls\" OR \"chip ban\" OR \"entity list\") (China OR semiconductor) lang:en -is:retweet",
    "Boeing OR Tesla recall OR FDA approval lang:en -is:retweet",
]

_QUERIES_EMPLOYMENT = [
    "layoffs OR hiring freeze OR restructuring lang:en -is:retweet",
    "(\"mass layoff\" OR \"job cuts\" OR \"workforce reduction\") lang:en -is:retweet has:cashtags",
    "(\"return to office\" OR \"remote work mandate\" OR \"hiring surge\") tech lang:en -is:retweet",
]

# All query categories with labels
_QUERY_CATEGORIES: list[tuple[str, list[str]]] = [
    ("mega_cap", _QUERIES_MEGA_CAP),
    ("macro", _QUERIES_MACRO),
    ("small_account_alpha", _QUERIES_SMALL_ACCOUNT_ALPHA),
    ("unusual_volume", _QUERIES_UNUSUAL_VOLUME),
    ("corporate_insider", _QUERIES_CORPORATE_INSIDER),
    ("sec_filing", _QUERIES_SEC_FILING),
    ("crypto_whale", _QUERIES_CRYPTO_WHALE),
    ("political_regulatory", _QUERIES_POLITICAL_REGULATORY),
    ("employment_signals", _QUERIES_EMPLOYMENT),
]

# Flat list for backward compatibility (used by mock scraper)
_SEARCH_QUERIES = [q for _, queries in _QUERY_CATEGORIES for q in queries]

_TWEET_FIELDS = "author_id,created_at,public_metrics,entities,source"
_USER_FIELDS = "name,username,verified,public_metrics"
_MAX_RESULTS = 100


class TwitterScraper(BaseScraper):
    """Searches Twitter API v2 for financial content. Handles rate limits and pagination."""

    @property
    def platform(self) -> str:
        return "twitter"

    def __init__(self, interval: int = 60) -> None:
        super().__init__(interval)
        self._token = get_settings().twitter_bearer_token
        self._seen_ids: set[str] = set()

    async def _search(
        self,
        client: httpx.AsyncClient,
        query: str,
        category: str,
    ) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        next_token: str | None = None

        for _ in range(3):
            params: dict[str, Any] = {
                "query": query,
                "max_results": min(_MAX_RESULTS, 100),
                "tweet.fields": _TWEET_FIELDS,
                "expansions": "author_id",
                "user.fields": _USER_FIELDS,
            }
            if next_token:
                params["next_token"] = next_token

            resp = await client.get(
                _TWITTER_SEARCH,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
            )

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "60"))
                logger.warning("[twitter] rate limited, backing off %ds", retry_after)
                import asyncio
                await asyncio.sleep(retry_after)
                continue

            if resp.status_code != 200:
                logger.warning("[twitter] search failed (%d): %s", resp.status_code, query)
                break

            data = resp.json()
            users_map: dict[str, dict] = {}
            for user in data.get("includes", {}).get("users", []):
                users_map[user["id"]] = user

            for tweet in data.get("data", []):
                tid = tweet["id"]
                if tid in self._seen_ids:
                    continue
                self._seen_ids.add(tid)

                author_id = tweet.get("author_id", "")
                user = users_map.get(author_id, {})
                metrics = tweet.get("public_metrics", {})

                posts.append(self._make_post(
                    source_id=tid,
                    author=f"@{user.get('username', 'unknown')}",
                    content=tweet.get("text", ""),
                    url=f"https://twitter.com/{user.get('username', 'i')}/status/{tid}",
                    raw_metadata={
                        "author_name": user.get("name", ""),
                        "author_followers": user.get("public_metrics", {}).get("followers_count", 0),
                        "author_verified": user.get("verified", False),
                        "retweet_count": metrics.get("retweet_count", 0),
                        "like_count": metrics.get("like_count", 0),
                        "reply_count": metrics.get("reply_count", 0),
                        "quote_count": metrics.get("quote_count", 0),
                        "created_at": tweet.get("created_at"),
                        "search_query": query,
                        "category": category,
                        "entities": tweet.get("entities", {}),
                    },
                ))

            next_token = data.get("meta", {}).get("next_token")
            if not next_token:
                break

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for category_name, queries in _QUERY_CATEGORIES:
                for query in queries:
                    try:
                        posts = await self._search(client, query, category_name)
                        all_posts.extend(posts)
                    except Exception:
                        logger.warning("[twitter] error for query: %s", query, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TWEETS_MEGA_CAP = [
    ("@jimcramer", "Jim Cramer", 2_100_000, "$AAPL looking strong here. Services segment is the story. Buy buy buy!"),
    ("@profgalloway", "Scott Galloway", 950_000, "Nvidia's market cap just passed Amazon. One company makes AI chips. The other delivers everything. What a time."),
    ("@BobPisani", "Bob Pisani", 520_000, "Magnificent 7 now represents 33% of S&P 500 market cap. Market breadth remains a concern. $SPY"),
    ("@lisaabramowicz1", "Lisa Abramowicz", 670_000, "10-year Treasury yield hits 4.6%. Highest since November. Mortgage rates heading to 8%. Housing freeze continues."),
]

_MOCK_TWEETS_MACRO = [
    ("@DeItaone", "Walter Bloomberg", 890_000, "FED'S POWELL: WE NEED GREATER CONFIDENCE THAT INFLATION IS MOVING SUSTAINABLY TO 2%"),
    ("@elerianm", "Mohamed El-Erian", 1_400_000, "The Fed is behind the curve -- again. Inflation persistence is structural, not transitory. June cut is off the table."),
    ("@PeterSchiff", "Peter Schiff", 980_000, "Gold just hit $2,250. Central banks dumping dollars and buying gold. The de-dollarization trade is real."),
    ("@leadlagreport", "Michael Gayed", 290_000, "Lumber is CRASHING. Down 30% in 2 months. This is historically a leading indicator for housing and the economy."),
    ("@mcaborsky", "Michael Aborsky", 210_000, "Copper at $4.20/lb and climbing. Chile production falling. EV demand surging. Best commodity trade of 2025."),
]

_MOCK_TWEETS_SMALL_ALPHA = [
    ("@deep_dd_anon", "DeepDD", 2_800, "$SMCI insiders just bought $1.2M in shares after the 40% pullback. They know something. Thread:"),
    ("@microcap_hunter", "MicroCap Alpha", 4_100, "$RKLB about to announce their Neutron rocket test. If successful, this is a $50B+ TAM company trading at $8B."),
    ("@quant_retail", "QuantRetail", 1_200, "My DCF model on $PLTR shows 80% upside. Government contracts + commercial AI growth = undervalued."),
    ("@pennydd_king", "PennyDD", 3_500, "$IONQ just signed a deal with the DOD. Quantum computing moat is real. Nobody is talking about this."),
    ("@dd_undercover", "DD Undercover", 890, "Did a deep dive on $ARM's licensing model. They get paid on EVERY chip. 50% margins and accelerating growth."),
]

_MOCK_TWEETS_UNUSUAL_VOLUME = [
    ("@unusual_whales", "unusual_whales", 1_500_000, "ALERT: $NVDA call sweep -- 10,000 contracts of the $800 strike expiring March. $15M in premium."),
    ("@darkpool_track", "DarkPool Tracker", 85_000, "DARK POOL: $AAPL massive block trade -- 2.1M shares at $185.50. Largest single print in 3 months."),
    ("@flow_algo", "FlowAlgo", 120_000, "Unusual put sweep in $XLF. 25K contracts $38P exp Feb. Someone betting on bank weakness."),
    ("@squeeze_alert", "SqueezeAlert", 45_000, "$GME short interest back above 25%. Days to cover at 4.2. Deja vu? Cost to borrow spiking."),
    ("@insider_track", "InsiderTracker", 67_000, "Form 4 ALERT: $CRM CEO just bought 50K shares at $270. Largest insider purchase in 2 years."),
]

_MOCK_TWEETS_CORPORATE = [
    ("@zaborowska", "Karolina Zaborowska", 320_000, "Tesla just recalled 2.2M vehicles in the US. That's basically every Tesla on American roads. $TSLA"),
    ("@GRDecter", "Gary Decter", 150_000, "Shell abandoning North Sea wind projects. Another blow to offshore wind. $SHEL moving to LNG."),
    ("@SaijUppal", "Saij Uppal", 85_000, "Samsung foundry yields reportedly at 20% for 3nm. TSMC at 80%. The gap is widening. $TSM $005930.KS"),
    ("@corp_moves", "CorpMoves", 28_000, "BREAKING: Intel CEO Pat Gelsinger stepping down effective immediately. Stock halted. $INTC"),
    ("@ceo_watch", "CEO Watch", 42_000, "Boeing CEO just sold $4.5M in stock before the latest quality report. Timing is suspicious."),
]

_MOCK_TWEETS_SEC = [
    ("@sec_filings_bot", "SEC Filings Bot", 156_000, "NEW 13F: Berkshire Hathaway Q4 filing shows NEW position in $SU (Suncor Energy). 25M shares."),
    ("@edgar_online", "Edgar Watcher", 89_000, "ALERT: Activist investor Carl Icahn files 13D on $JNJ. 2.1% stake. Pushing for consumer health spinoff acceleration."),
    ("@sec_enforce", "SEC Enforcement Watch", 34_000, "SEC charges three former executives of crypto exchange for fraud. $2.1B in customer funds affected."),
    ("@proxy_monitor", "Proxy Monitor", 21_000, "Proxy fight brewing at $DIS. Nelson Peltz seeking 3 board seats. Management fighting back with poison pill."),
]

_MOCK_TWEETS_CRYPTO = [
    ("@EricBalchunas", "Eric Balchunas", 340_000, "Bitcoin ETF update: IBIT saw $520M inflow today alone. BlackRock's BTC ETF is now larger than their silver ETF."),
    ("@WatcherGuru", "Watcher.Guru", 2_800_000, "JUST IN: MicroStrategy buys another 12,000 Bitcoin worth $800M. Total holdings now 205,000 BTC."),
    ("@whale_alert_", "Whale Alert", 1_200_000, "WHALE: 5,000 BTC ($310M) transferred from unknown wallet to Coinbase. Potential sell pressure incoming."),
    ("@onchain_data", "OnChain Analytics", 78_000, "ETH staking TVL just crossed $60B. 28% of all ETH is now staked. Supply shock thesis playing out."),
    ("@defi_tracker", "DeFi Pulse", 145_000, "Aave V3 TVL surges 40% this week to $12B. Institutional DeFi adoption accelerating."),
]

_MOCK_TWEETS_POLITICAL = [
    ("@elikisos", "Eli Kisos", 450_000, "BREAKING: Boeing halts 737 MAX deliveries after new door plug concern. $BA tanking after hours."),
    ("@MattGoldstein26", "Matt Goldstein", 180_000, "JUST IN: NYCB stock halted. Reports of additional CRE losses. Regional bank contagion fears rising."),
    ("@policy_alpha", "PolicyAlpha", 55_000, "New executive order restricting AI chip exports to China. $NVDA, $AMD, $INTC all under pressure."),
    ("@trade_hawk", "TradeHawk", 67_000, "EU slapping 45% tariffs on Chinese EVs. BYD, NIO, XPEV all affected. European auto stocks rallying."),
    ("@reg_watch", "RegWatch", 38_000, "FTC blocking $CPRI-$TPR merger (Capri/Tapestry). Luxury handbag market concentration cited."),
]

_MOCK_TWEETS_EMPLOYMENT = [
    ("@jessefelder", "Jesse Felder", 210_000, "Semiconductor equipment stocks rolling over. ASML, KLAC, AMAT all breaking down. Leading indicator for chip sector."),
    ("@layoff_tracker", "LayoffTracker", 95_000, "Meta laying off 10,000 in 'Year of Efficiency' Phase 2. Engineering teams hardest hit. $META"),
    ("@tech_layoffs", "TechLayoffs", 180_000, "Amazon cutting 18,000 positions in AWS and advertising. Largest tech layoff of 2025 so far. $AMZN"),
    ("@hiring_pulse", "HiringPulse", 42_000, "AI hiring surge: OpenAI, Anthropic, Google DeepMind collectively posted 2,000+ openings this week. Talent war escalating."),
]

_MOCK_TWEETS_ALL = [
    ("mega_cap", _MOCK_TWEETS_MEGA_CAP),
    ("macro", _MOCK_TWEETS_MACRO),
    ("small_account_alpha", _MOCK_TWEETS_SMALL_ALPHA),
    ("unusual_volume", _MOCK_TWEETS_UNUSUAL_VOLUME),
    ("corporate_insider", _MOCK_TWEETS_CORPORATE),
    ("sec_filing", _MOCK_TWEETS_SEC),
    ("crypto_whale", _MOCK_TWEETS_CRYPTO),
    ("political_regulatory", _MOCK_TWEETS_POLITICAL),
    ("employment_signals", _MOCK_TWEETS_EMPLOYMENT),
]


# ── Apify fallback (no bearer token needed) ─────────────────────────

_APIFY_API = "https://api.apify.com/v2"
_TWITTER_APIFY_ACTOR = "kaitoeasyapi~twitter-x-data-tweet-scraper-pay-per-result-cheapest"

# Simplified search terms for Apify (cashtags + keywords, rotated per cycle)
_APIFY_SEARCH_TERMS: list[tuple[str, list[str]]] = [
    ("mega_cap", ["$NVDA $TSLA $AAPL earnings", "$MSFT $AMZN $GOOG stock"]),
    ("macro", ["Federal Reserve inflation rates", "OPEC oil gold commodities"]),
    ("small_account_alpha", ["$SMCI $PLTR $ARM due diligence", "$IONQ $RKLB quantum space"]),
    ("unusual_volume", ["unusual options sweep call put", "dark pool block trade whale"]),
    ("corporate_insider", ["CEO stepping down appointed resignation", "insider buying Form 4 purchase"]),
    ("sec_filing", ["13F filing SEC 13D activist investor", "SEC investigation charges fraud"]),
    ("crypto_whale", ["Bitcoin ETF inflow outflow", "whale transfer BTC ETH accumulation"]),
    ("political_regulatory", ["tariffs trade ban export controls", "antitrust regulation executive order"]),
    ("employment_signals", ["layoffs hiring freeze tech", "mass layoff workforce reduction"]),
]

_APIFY_MAX_PER_QUERY = 20


class TwitterApifyScraper(BaseScraper):
    """Fallback Twitter/X scraper using Apify when no bearer token is available.

    Uses the pay-per-result actor ($0.25/1K tweets). Budget-conscious:
    runs 1 category (2 queries) per cycle with 300s interval.
    """

    @property
    def platform(self) -> str:
        return "twitter"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key
        self._cycle_index: int = 0

    async def _run_search(
        self,
        client: httpx.AsyncClient,
        search_term: str,
        category: str,
    ) -> list[dict[str, Any]]:
        """Run a single Apify search and return standardised posts."""
        run_resp = await client.post(
            f"{_APIFY_API}/acts/{_TWITTER_APIFY_ACTOR}/runs",
            params={"token": self._api_key, "waitForFinish": 120},
            json={
                "searchTerms": [search_term],
                "maxItems": _APIFY_MAX_PER_QUERY,
                "sort": "Latest",
            },
            timeout=150,
        )
        if run_resp.status_code not in (200, 201):
            logger.warning("[twitter/apify] actor start failed (%d) for '%s'", run_resp.status_code, search_term)
            return []

        run_data = run_resp.json().get("data", {})
        status = run_data.get("status")
        if status != "SUCCEEDED":
            logger.warning("[twitter/apify] run status=%s for '%s'", status, search_term)
            return []

        dataset_id = run_data.get("defaultDatasetId")
        if not dataset_id:
            return []
        items_resp = await client.get(
            f"{_APIFY_API}/datasets/{dataset_id}/items",
            params={"token": self._api_key, "format": "json"},
            timeout=20,
        )
        if items_resp.status_code != 200:
            return []

        posts: list[dict[str, Any]] = []
        for item in items_resp.json():
            # Skip demo/empty items
            if item.get("demo") or item.get("noResults"):
                continue

            tid = str(item.get("id", ""))
            if not tid:
                continue

            author_data = item.get("author", {})
            username = author_data.get("userName", "unknown")
            posts.append(self._make_post(
                source_id=tid,
                author=f"@{username}",
                content=(item.get("text", "") or "")[:2000],
                url=item.get("url") or f"https://x.com/{username}/status/{tid}",
                raw_metadata={
                    "author_name": author_data.get("name", ""),
                    "author_followers": author_data.get("followers", 0),
                    "author_verified": author_data.get("isBlueVerified", False),
                    "retweet_count": item.get("retweetCount", 0),
                    "like_count": item.get("likeCount", 0),
                    "reply_count": item.get("replyCount", 0),
                    "quote_count": item.get("quoteCount", 0),
                    "view_count": item.get("viewCount", 0),
                    "created_at": item.get("createdAt"),
                    "lang": item.get("lang", ""),
                    "search_query": search_term,
                    "category": category,
                    "cashtags": [s.get("text", "") for s in item.get("entities", {}).get("symbols", [])],
                    "hashtags": [h.get("text", "") for h in item.get("entities", {}).get("hashtags", [])],
                    "source": "apify",
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape one category per cycle, rotating through all nine."""
        category_name, terms = _APIFY_SEARCH_TERMS[self._cycle_index % len(_APIFY_SEARCH_TERMS)]
        self._cycle_index += 1

        logger.info("[twitter/apify] scraping category '%s' (%d terms)", category_name, len(terms))

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for term in terms:
                try:
                    posts = await self._run_search(client, term, category_name)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[twitter/apify] error for term: %s", term, exc_info=True)

        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

class MockTwitterScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "twitter"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(20, 40)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            # Pick a random category, then a random tweet from that category
            category_name, mock_list = random.choice(_MOCK_TWEETS_ALL)
            handle, name, followers, text = random.choice(mock_list)
            tid = str(random.randint(1_700_000_000_000_000_000, 1_800_000_000_000_000_000))
            posts.append(self._make_post(
                source_id=tid,
                author=handle,
                content=text,
                url=f"https://twitter.com/{handle.lstrip('@')}/status/{tid}",
                raw_metadata={
                    "author_name": name,
                    "author_followers": followers,
                    "author_verified": followers > 500_000,
                    "retweet_count": random.randint(50, 5_000),
                    "like_count": random.randint(100, 20_000),
                    "reply_count": random.randint(10, 2_000),
                    "quote_count": random.randint(5, 500),
                    "category": category_name,
                    "search_query": random.choice(_SEARCH_QUERIES),
                },
            ))
        return posts
