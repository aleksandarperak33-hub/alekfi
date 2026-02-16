"""4chan /biz/ scraper — threads and replies via the free JSON API."""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timezone
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_FOURCHAN_API = "https://a.4cdn.org/biz"

# Regex to extract ticker mentions: $AAPL, $BTC, or bare all-caps 2-5 letter tickers
_TICKER_RE = re.compile(r'\$([A-Z]{1,5})\b')
_BARE_TICKER_RE = re.compile(r'\b([A-Z]{2,5})\b')

# Common English words that look like tickers but aren't
_TICKER_BLACKLIST = frozenset({
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HER",
    "WAS", "ONE", "OUR", "OUT", "HAS", "HIS", "HOW", "MAN", "NEW", "NOW",
    "OLD", "SEE", "WAY", "WHO", "BOY", "DID", "GET", "HIM", "LET", "SAY",
    "SHE", "TOO", "USE", "DAD", "MOM", "ITS", "SAY", "ANY", "FEW", "GOT",
    "HAD", "HAS", "HIM", "HOW", "ITS", "MAY", "OWN", "SAY", "TWO", "WAY",
    "WHY", "BIG", "END", "FAR", "FEW", "GOT", "RUN", "SET", "TRY", "ASK",
    "MEN", "RAN", "TOP", "RED", "YES", "YET", "WILL", "JUST", "LIKE",
    "THIS", "THAT", "WITH", "FROM", "WHAT", "WHEN", "YOUR", "THAN", "THEM",
    "THEN", "BEEN", "HAVE", "SAID", "EACH", "SOME", "WELL", "VERY", "MUCH",
    "ALSO", "BACK", "LONG", "MADE", "MOST", "GOOD", "OVER", "SUCH", "TAKE",
    "ONLY", "COME", "MAKE", "FIND", "HERE", "KNOW", "LAST", "LOOK", "TELL",
    "DOES", "INTO", "YEAR", "EVEN", "KEEP", "GIVE", "CALL", "WORK", "STILL",
    "DOWN", "BEST", "DONE", "WENT", "HIGH", "TIME", "REAL", "SAME", "FREE",
    "NEXT", "HOLD", "PUMP", "DUMP", "HODL", "NGMI", "WAGMI", "COPE", "LARP",
    "BULL", "BEAR", "LONG", "DROP", "REKT", "MOON", "FOMO", "DYOR", "IMHO",
    "TRUE", "HUGE", "SELL", "SOLD", "CASH", "BOND", "FUND", "RATE", "LOSS",
    "GAIN", "RISE", "FELL", "DEBT", "LOAN", "RICH", "POOR", "LMAO", "EVER",
    "POST", "PAGE", "BASED", "ANON", "KEK",
})

# Well-known tickers to always accept even if short
_KNOWN_TICKERS = frozenset({
    "BTC", "ETH", "SOL", "XRP", "ADA", "DOT", "AVAX", "MATIC", "LINK",
    "DOGE", "SHIB", "PEPE", "WIF", "BONK", "UNI", "AAVE", "CRV", "MKR",
    "AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOG", "GOOGL", "META",
    "AMD", "INTC", "NFLX", "PLTR", "GME", "AMC", "BBBY", "COIN",
    "SPY", "QQQ", "IWM", "DIA", "VIX", "TLT", "GLD", "SLV", "USO",
    "JPM", "GS", "BAC", "WFC", "C", "MS", "V", "MA",
    "BA", "LMT", "RTX", "NOC", "GD",
    "PFE", "JNJ", "MRK", "LLY", "ABBV", "BMY", "UNH",
    "XOM", "CVX", "COP", "OXY", "SLB",
    "DIS", "CMCSA", "T", "VZ", "TMUS",
    "HD", "LOW", "WMT", "COST", "TGT",
    "CRM", "ORCL", "ADBE", "NOW", "SNOW", "CRWD", "PANW",
    "SMCI", "ARM", "AVGO", "QCOM", "MU", "ASML", "TSM",
})


def _extract_tickers(text: str) -> list[str]:
    """Extract likely ticker symbols from text."""
    tickers: set[str] = set()

    # First: explicit $TICKER mentions (high confidence)
    for match in _TICKER_RE.finditer(text):
        ticker = match.group(1)
        if ticker not in _TICKER_BLACKLIST:
            tickers.add(ticker)

    # Second: bare all-caps words, only if they're known tickers
    for match in _BARE_TICKER_RE.finditer(text):
        ticker = match.group(1)
        if ticker in _KNOWN_TICKERS:
            tickers.add(ticker)

    return sorted(tickers)


class FourChanBizScraper(BaseScraper):
    """Fetches threads from the 4chan /biz/ catalog (no auth required)."""

    @property
    def platform(self) -> str:
        return "4chan_biz"

    def __init__(self, interval: int = 120) -> None:
        super().__init__(interval)
        self._seen_ids: set[int] = set()
        self._seen_tickers: set[str] = set()

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20) as client:
            try:
                resp = await client.get(f"{_FOURCHAN_API}/catalog.json")
                if resp.status_code != 200:
                    logger.warning("[4chan_biz] catalog returned %d", resp.status_code)
                    return []
                pages = resp.json()
            except Exception:
                logger.warning("[4chan_biz] failed to fetch catalog", exc_info=True)
                return []

            for page in pages:
                for thread in page.get("threads", []):
                    thread_no = thread.get("no")
                    if thread_no is None or thread_no in self._seen_ids:
                        continue
                    self._seen_ids.add(thread_no)

                    subject = thread.get("sub", "")
                    comment = thread.get("com", "")
                    author = thread.get("name", "Anonymous")
                    timestamp = thread.get("time")

                    content_parts = []
                    if subject:
                        content_parts.append(subject)
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

                    # Fetch thread replies
                    replies = await self._fetch_replies(client, thread_no)

                    # Extract tickers from thread content + replies
                    full_text = content
                    for r in replies:
                        full_text += " " + r.get("text", "")
                    extracted_tickers = _extract_tickers(full_text)

                    # Detect new tickers (not seen in previous scrapes)
                    new_tickers = [t for t in extracted_tickers if t not in self._seen_tickers]
                    is_new_ticker = len(new_tickers) > 0
                    self._seen_tickers.update(extracted_tickers)

                    # Compute thread velocity: replies_per_minute
                    replies_count = thread.get("replies", 0)
                    thread_velocity = 0.0
                    if timestamp:
                        now_ts = datetime.now(timezone.utc).timestamp()
                        age_minutes = max((now_ts - timestamp) / 60.0, 1.0)
                        thread_velocity = round(replies_count / age_minutes, 3)

                    all_posts.append(self._make_post(
                        source_id=str(thread_no),
                        author=author,
                        content=content,
                        url=f"https://boards.4chan.org/biz/thread/{thread_no}",
                        source_published_at=source_published_at,
                        raw_metadata={
                            "thread_no": thread_no,
                            "subject": subject,
                            "replies_count": replies_count,
                            "images_count": thread.get("images", 0),
                            "sticky": thread.get("sticky", 0),
                            "closed": thread.get("closed", 0),
                            "page": page.get("page"),
                            "top_replies": replies,
                            "extracted_tickers": extracted_tickers,
                            "thread_velocity": thread_velocity,
                            "is_new_ticker": is_new_ticker,
                        },
                    ))

        # Cap seen IDs to prevent unbounded memory growth
        if len(self._seen_ids) > 10000:
            self._seen_ids = set(list(self._seen_ids)[-5000:])

        # Cap seen tickers (keep last 2000 unique tickers)
        if len(self._seen_tickers) > 5000:
            self._seen_tickers = set(list(self._seen_tickers)[-2000:])

        return all_posts

    async def _fetch_replies(
        self, client: httpx.AsyncClient, thread_no: int, limit: int = 5
    ) -> list[dict[str, Any]]:
        """Fetch top replies from a thread."""
        replies: list[dict[str, Any]] = []
        try:
            resp = await client.get(f"{_FOURCHAN_API}/thread/{thread_no}.json")
            if resp.status_code != 200:
                return []
            data = resp.json()
            posts = data.get("posts", [])
            # Skip the OP (first post), take up to `limit` replies
            for post in posts[1 : limit + 1]:
                comment = post.get("com", "")
                if not comment:
                    continue
                replies.append({
                    "author": post.get("name", "Anonymous"),
                    "text": comment[:1000],
                    "no": post.get("no"),
                })
        except Exception:
            logger.debug("[4chan_biz] failed to fetch thread %d replies", thread_no)
        return replies


# -- Mock ---------------------------------------------------------------

_MOCK_THREADS = [
    ("Anonymous", "LINK $1000 EOY", "Chainlink marines assemble. The oracle problem is solved and normies still don't get it. $LINK will flip $ETH. Accumulate before it's too late."),
    ("Anonymous", "Is $ETH actually going to flip $BTC?", "The merge happened, L2s are scaling, and institutional money is flowing in. ETH/BTC ratio looking bullish."),
    ("Anonymous", "/smg/ - Stock Market General", "S&P futures up 0.3% premarket. $NVDA earnings after close today. Who's holding calls through earnings?"),
    ("Anonymous", "Just lost 80% of my portfolio on memecoins", "Bought the top on every single Solana memecoin. $BONK, $WIF, $PEPE all dumped. My wife is going to kill me. Is there any recovery play?"),
    ("Anonymous", "$BTC halving pump when?", "Every cycle the halving catalyzes a bull run 6-12 months later. We're right on schedule. $150k by Q4."),
    ("Anonymous", "Gold bugs get in here", "Central banks buying record amounts of gold. De-dollarization is real. $GLD $3000/oz this year."),
    ("Anonymous", "Wagmi or ngmi?", "Seriously considering quitting my job to trade full time. I made $50k last month on leverage. Am I delusional?"),
    ("Anonymous", "The real estate market is about to implode", "Commercial real estate vacancy rates at all time highs. Banks are sitting on massive unrealized losses. 2008 part 2."),
    ("Anonymous", "/cmmg/ - Crypto Memecoin General", "What's the next 100x? $PEPE already did its run. Need a fresh meta. AI agent coins? DeSci? $TAO?"),
    ("Anonymous", "Why aren't you buying Japanese stocks?", "Nikkei breaking out of a 34 year range. Buffett loading up on trading houses. Yen weakness makes exports moon."),
    ("Anonymous", "$TSLA is uninvestable at this valuation", "50x forward earnings for a car company that's losing market share. Robotaxi is vaporware. Short it."),
    ("Anonymous", "The Fed is trapped", "Cut rates and inflation rips. Keep rates high and the economy breaks. There's no soft landing. Buy hard assets."),
    ("Anonymous", "$SOL vs $ETH debate thread", "SOL processes 4000 TPS for pennies. ETH can barely do 15 TPS and costs $5 per swap. It's over for ETH."),
    ("Anonymous", "I just inherited $200k what do", "26 years old, no debt, stable job. Should I dump it all into $SPY or is there a better play right now?"),
    ("Anonymous", "$PLTR is going to $100", "Palantir's AIP platform is being adopted by every Fortune 500 company. Government contracts expanding. This is the next $MSFT."),
    ("Anonymous", "Bank run incoming?", "Credit default swaps on regional banks spiking. $JPM $BAC deposit outflows accelerating. Nobody learned from SVB."),
    ("Anonymous", "Silver is the most undervalued asset on earth", "$SLV Gold/silver ratio at 85. Industrial demand for solar panels exploding. Supply deficit for 3rd year straight."),
    ("Anonymous", "AI bubble is going to pop harder than dotcom", "Every company adding AI to their name. $NVDA $AMD $AVGO 99% of these startups have no revenue. We've seen this movie before."),
    ("Anonymous", "China is dumping US treasuries", "PBOC reduced holdings by $50B last quarter. If they accelerate, yields will spike and the housing market collapses."),
    ("Anonymous", "$DOGE to $1 is unironically happening", "Elon running DOGE department. Memetic energy is off the charts. The prophecy will be fulfilled."),
    ("Anonymous", "What's the best dividend stock right now?", "Looking for 4%+ yield with growth. $O, SCHD, or individual picks? Need passive income to escape the wagecage."),
    ("Anonymous", "Uranium supercycle confirmed", "Nuclear renaissance is here. $CCJ Cameco, Kazatomprom can't keep up with demand. Spot price heading to $150/lb."),
    ("Anonymous", "/pmg/ - Precious Metals General", "Just stacked another 100oz of silver. $SLV $GLD When the dollar collapses you'll wish you held real money."),
    ("Anonymous", "Insider selling at all time highs", "CEO after CEO dumping shares. $AAPL $MSFT $NVDA They know something. The smart money is exiting."),
    ("Anonymous", "DeFi yield farming is back", "New protocols offering 50% APY on stablecoin pairs. $AAVE $CRV Is this sustainable or are we the yield?"),
    # New: ticker-heavy threads
    ("Anonymous", "New $ONDO listing on Coinbase", "RWA tokenization is the next big narrative. $ONDO just listed on Coinbase. Blackrock backing. This is going to $5."),
    ("Anonymous", "$MSTR buying more $BTC again", "MicroStrategy bought another 12,000 BTC at $61K avg. Saylor is absolutely unhinged. $MSTR is a leveraged BTC play."),
    ("Anonymous", "Semiconductor rotation: $NVDA -> $AMD -> $AVGO", "Money rotating from expensive semis to cheaper ones. $AMD at 30x earnings vs $NVDA at 60x. No brainer."),
]

_MOCK_REPLIES = [
    "Based and checked.",
    "Cope. It's going to zero.",
    "This is financial advice and I'm taking it.",
    "Ngmi with this level of delusion.",
    "Checked. The digits confirm it.",
    "Source: my crystal ball.",
    "Just buy the dip bro.",
    "Imagine not being all in on $BTC right now.",
    "You're going to make it anon.",
    "This aged poorly.",
    "Trust the plan.",
    "Literally can't go tits up.",
    "Have fun staying poor.",
    "Sir this is a Wendy's.",
    "Priced in.",
    "$SOL is going to flip $ETH mark my words.",
    "Unironically $PLTR is the play here.",
    "Buy $NVDA calls and thank me later.",
]


class MockFourChanBizScraper(BaseScraper):
    _mock_seen_tickers: set[str] = set()

    @property
    def platform(self) -> str:
        return "4chan_biz"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(20, 40)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            author, subject, comment = random.choice(_MOCK_THREADS)
            thread_no = random.randint(50000000, 60000000)
            num_replies = random.randint(2, 5)
            mock_replies = [
                {
                    "author": "Anonymous",
                    "text": random.choice(_MOCK_REPLIES),
                    "no": thread_no + i + 1,
                }
                for i in range(num_replies)
            ]

            # Extract tickers from mock content
            full_text = f"{subject} {comment}"
            for r in mock_replies:
                full_text += " " + r["text"]
            extracted_tickers = _extract_tickers(full_text)

            new_tickers = [t for t in extracted_tickers if t not in self._mock_seen_tickers]
            is_new_ticker = len(new_tickers) > 0
            self.__class__._mock_seen_tickers.update(extracted_tickers)

            replies_count = random.randint(5, 300)
            age_minutes = random.uniform(5.0, 600.0)
            thread_velocity = round(replies_count / age_minutes, 3)

            posts.append(self._make_post(
                source_id=str(thread_no),
                author=author,
                content=f"{subject}\n\n{comment}",
                url=f"https://boards.4chan.org/biz/thread/{thread_no}",
                raw_metadata={
                    "thread_no": thread_no,
                    "subject": subject,
                    "replies_count": replies_count,
                    "images_count": random.randint(0, 50),
                    "sticky": 0,
                    "closed": 0,
                    "page": random.randint(1, 10),
                    "top_replies": mock_replies,
                    "extracted_tickers": extracted_tickers,
                    "thread_velocity": thread_velocity,
                    "is_new_ticker": is_new_ticker,
                },
            ))
        return posts
