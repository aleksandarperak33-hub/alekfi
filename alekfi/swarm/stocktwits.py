"""StockTwits scraper — real-time retail sentiment tracker via public API."""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any

import aiohttp

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_ST_TRENDING_URL = "https://api.stocktwits.com/api/2/streams/trending.json"
_ST_SYMBOL_URL = "https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json"
_ST_TRENDING_SYMBOLS_URL = "https://api.stocktwits.com/api/2/trending/symbols.json"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# High-interest tickers to track individually
_WATCHLIST = [
    "NVDA", "TSLA", "AAPL", "AMD", "AMZN", "META", "MSFT", "PLTR",
    "COIN", "GME", "AMC", "SPY", "QQQ", "SMCI", "MARA", "SOFI",
]


class StockTwitsScraper(BaseScraper):
    """Tracks trending messages and per-symbol sentiment from StockTwits."""

    @property
    def platform(self) -> str:
        return "stocktwits"

    def __init__(self, interval: int = 180) -> None:
        super().__init__(interval)
        self._seen_ids: set[int] = set()

    def _parse_message(self, msg: dict[str, Any], source_label: str) -> dict[str, Any] | None:
        """Convert a StockTwits message dict into a standardised post."""
        msg_id = msg.get("id")
        if not msg_id or msg_id in self._seen_ids:
            return None
        self._seen_ids.add(msg_id)

        body = msg.get("body", "")
        user = msg.get("user", {})
        author = user.get("username", "anonymous")
        created_at = msg.get("created_at")

        # Sentiment from StockTwits
        sentiment_obj = msg.get("entities", {}).get("sentiment", {})
        if not sentiment_obj:
            sentiment_obj = msg.get("sentiment") or {}
        sentiment_basic = sentiment_obj.get("basic") if isinstance(sentiment_obj, dict) else None

        # Symbols mentioned
        symbols = []
        for sym in msg.get("symbols", []):
            if isinstance(sym, dict):
                symbols.append(sym.get("symbol", ""))
            elif isinstance(sym, str):
                symbols.append(sym)

        likes = msg.get("likes", {})
        like_count = likes.get("total", 0) if isinstance(likes, dict) else 0

        return self._make_post(
            source_id=str(msg_id),
            author=author,
            content=body,
            url=f"https://stocktwits.com/{author}/message/{msg_id}",
            source_published_at=created_at,
            raw_metadata={
                "st_message_id": msg_id,
                "sentiment": sentiment_basic,
                "symbols": symbols,
                "like_count": like_count,
                "followers": user.get("followers", 0),
                "source_label": source_label,
                "user_join_date": user.get("join_date"),
            },
        )

    async def _fetch_trending_messages(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch the global trending stream."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _ST_TRENDING_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.warning("[stocktwits] trending stream returned %d", resp.status)
                    return posts
                data = await resp.json(content_type=None)
        except Exception:
            logger.warning("[stocktwits] failed to fetch trending stream", exc_info=True)
            return posts

        for msg in data.get("messages", []):
            post = self._parse_message(msg, "trending")
            if post:
                posts.append(post)

        return posts

    async def _fetch_symbol_stream(
        self, session: aiohttp.ClientSession, symbol: str
    ) -> list[dict[str, Any]]:
        """Fetch the message stream for a specific symbol."""
        posts: list[dict[str, Any]] = []
        url = _ST_SYMBOL_URL.format(symbol=symbol)
        try:
            async with session.get(
                url, headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    logger.debug("[stocktwits] symbol %s returned %d", symbol, resp.status)
                    return posts
                data = await resp.json(content_type=None)
        except Exception:
            logger.debug("[stocktwits] failed to fetch symbol %s", symbol, exc_info=True)
            return posts

        for msg in data.get("messages", []):
            post = self._parse_message(msg, f"symbol_{symbol}")
            if post:
                posts.append(post)

        return posts

    async def _fetch_trending_symbols(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch the currently trending symbols and emit a summary post."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _ST_TRENDING_SYMBOLS_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.debug("[stocktwits] trending symbols returned %d", resp.status)
                    return posts
                data = await resp.json(content_type=None)
        except Exception:
            logger.debug("[stocktwits] failed to fetch trending symbols", exc_info=True)
            return posts

        symbols = data.get("symbols", [])
        if symbols:
            symbol_list = []
            for s in symbols[:20]:
                if isinstance(s, dict):
                    symbol_list.append(s.get("symbol", "???"))
                elif isinstance(s, str):
                    symbol_list.append(s)

            content = (
                f"[StockTwits Trending Symbols] "
                f"Top {len(symbol_list)}: {', '.join(f'${s}' for s in symbol_list)}"
            )
            posts.append(self._make_post(
                source_id=f"trending_syms_{self._generate_id()}",
                author="StockTwits",
                content=content,
                url="https://stocktwits.com/trending",
                raw_metadata={
                    "trending_symbols": symbol_list,
                    "category": "trending_symbols",
                    "count": len(symbol_list),
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        async with aiohttp.ClientSession() as session:
            # 1. Global trending messages
            trending = await self._fetch_trending_messages(session)
            all_posts.extend(trending)

            # 2. Trending symbols list
            sym_posts = await self._fetch_trending_symbols(session)
            all_posts.extend(sym_posts)

            # 3. Per-symbol streams for our watchlist (rotate 4 per cycle to avoid rate limits)
            symbols_this_cycle = random.sample(_WATCHLIST, min(4, len(_WATCHLIST)))
            for symbol in symbols_this_cycle:
                sym_stream = await self._fetch_symbol_stream(session, symbol)
                all_posts.extend(sym_stream)

        # Aggregate sentiment stats for posts in this batch
        bullish = sum(
            1 for p in all_posts
            if p.get("raw_metadata", {}).get("sentiment") == "Bullish"
        )
        bearish = sum(
            1 for p in all_posts
            if p.get("raw_metadata", {}).get("sentiment") == "Bearish"
        )
        logger.info(
            "[stocktwits] batch: %d posts, %d bullish, %d bearish",
            len(all_posts), bullish, bearish,
        )

        # Cap seen set
        if len(self._seen_ids) > 50000:
            self._seen_ids = set(list(self._seen_ids)[-25000:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_MESSAGES = [
    ("$NVDA to the moon! Earnings were insane, data center revenue is unstoppable.", "ChipTrader99", "Bullish", ["NVDA"], 142),
    ("$TSLA looking weak here. Demand is slowing, margins compressing. Short.", "BearishBets", "Bearish", ["TSLA"], 87),
    ("$AAPL quietly building their AI moat. Next iPhone cycle will be huge.", "LongTermLarry", "Bullish", ["AAPL"], 56),
    ("$AMD MI300X is gaining real enterprise traction. Competition for NVDA heating up.", "SemiWatcher", "Bullish", ["AMD", "NVDA"], 203),
    ("$SPY breaking out to all-time highs. Breadth expanding. Stay long.", "IndexAlpha", "Bullish", ["SPY"], 315),
    ("$PLTR just landed another huge government contract. $1B+ backlog growing fast.", "GovTechBull", "Bullish", ["PLTR"], 178),
    ("$GME the squeeze hasn't squoze yet! Diamond hands!", "APE_4_LIFE", "Bullish", ["GME"], 24),
    ("$META ad revenue is incredible but the metaverse spending is concerning.", "FundyTrader", "Bearish", ["META"], 93),
    ("$COIN if Bitcoin breaks $80K, this goes parabolic. Loading up.", "CryptoKing", "Bullish", ["COIN"], 67),
    ("$MSFT Copilot adoption is slower than expected. Overpriced at 35x earnings.", "ValueHunter", "Bearish", ["MSFT"], 44),
    ("$SMCI the run is overdone. Classic pump. Taking profits here.", "SwingMaster", "Bearish", ["SMCI"], 112),
    ("$AMZN AWS growth reaccelerating. E-commerce margins expanding. Best mega-cap to own.", "CloudBull22", "Bullish", ["AMZN"], 156),
    ("$SOFI turning profitable and growing 30%+ YoY. Fintech winner emerging.", "BankDisruptor", "Bullish", ["SOFI"], 89),
    ("Buying $QQQ puts here. Tech too extended, VIX too low.", "OptionsFlow", "Bearish", ["QQQ"], 71),
    ("$MARA if BTC rips, miners are 3x leveraged plays. Accumulating.", "MinerBull", "Bullish", ["MARA"], 38),
]

_MOCK_TRENDING_SYMBOLS = [
    "NVDA", "TSLA", "AAPL", "AMD", "PLTR", "GME", "SMCI", "COIN",
    "META", "AMZN", "SPY", "QQQ", "SOFI", "MARA", "AMC",
]


class MockStockTwitsScraper(BaseScraper):
    """Mock scraper generating realistic StockTwits sentiment data."""

    @property
    def platform(self) -> str:
        return "stocktwits"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []

        # Generate message posts
        msg_count = random.randint(10, 15)
        selected = random.sample(_MOCK_MESSAGES, min(msg_count, len(_MOCK_MESSAGES)))
        for body, author, sentiment, symbols, likes in selected:
            msg_id = random.randint(500000000, 600000000)
            posts.append(self._make_post(
                source_id=str(msg_id),
                author=author,
                content=body,
                url=f"https://stocktwits.com/{author}/message/{msg_id}",
                source_published_at=datetime.now(timezone.utc).isoformat(),
                raw_metadata={
                    "st_message_id": msg_id,
                    "sentiment": sentiment,
                    "symbols": symbols,
                    "like_count": likes,
                    "followers": random.randint(50, 15000),
                    "source_label": random.choice(["trending", "symbol_NVDA", "symbol_TSLA"]),
                },
            ))

        # Generate trending symbols summary
        trending = random.sample(_MOCK_TRENDING_SYMBOLS, random.randint(8, 12))
        posts.append(self._make_post(
            source_id=f"trending_syms_{self._generate_id()}",
            author="StockTwits",
            content=f"[StockTwits Trending Symbols] Top {len(trending)}: {', '.join(f'${s}' for s in trending)}",
            url="https://stocktwits.com/trending",
            raw_metadata={
                "trending_symbols": trending,
                "category": "trending_symbols",
                "count": len(trending),
            },
        ))

        return posts
