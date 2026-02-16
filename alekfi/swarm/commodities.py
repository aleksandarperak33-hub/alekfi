"""Commodities scraper — tracks futures prices via MarketDataGateway."""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any

import redis.asyncio as aioredis

from alekfi.config import get_settings
from alekfi.marketdata import MarketDataGateway
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# Commodity futures symbols on Yahoo Finance
_COMMODITY_SYMBOLS = {
    "CL=F": {"name": "Crude Oil (WTI)", "code": "CL", "unit": "$/barrel"},
    "GC=F": {"name": "Gold", "code": "GC", "unit": "$/oz"},
    "SI=F": {"name": "Silver", "code": "SI", "unit": "$/oz"},
    "NG=F": {"name": "Natural Gas", "code": "NG", "unit": "$/MMBtu"},
    "HG=F": {"name": "Copper", "code": "HG", "unit": "$/lb"},
    "PL=F": {"name": "Platinum", "code": "PL", "unit": "$/oz"},
    "ZW=F": {"name": "Wheat", "code": "ZW", "unit": "cents/bushel"},
    "ZC=F": {"name": "Corn", "code": "ZC", "unit": "cents/bushel"},
}

# ETF proxies as fallback
_ETF_SYMBOLS = {
    "USO": {"name": "United States Oil Fund", "commodity": "Crude Oil", "code": "CL"},
    "GLD": {"name": "SPDR Gold Trust", "commodity": "Gold", "code": "GC"},
    "SLV": {"name": "iShares Silver Trust", "commodity": "Silver", "code": "SI"},
    "UNG": {"name": "United States Natural Gas Fund", "commodity": "Natural Gas", "code": "NG"},
    "CPER": {"name": "United States Copper Index Fund", "commodity": "Copper", "code": "HG"},
}




def _format_change(change: float, change_pct: float) -> str:
    """Format a price change with sign and percentage."""
    sign = "+" if change >= 0 else ""
    return f"{sign}{change:.2f} ({sign}{change_pct:.2f}%)"


class CommoditiesScraper(BaseScraper):
    """Tracks commodity futures prices, daily changes, and volume via gateway providers."""

    @property
    def platform(self) -> str:
        return "commodities"

    def __init__(self, interval: int = 600) -> None:
        super().__init__(interval)
        self._prev_prices: dict[str, float] = {}
        self._gateway: MarketDataGateway | None = None

    async def _get_gateway(self) -> MarketDataGateway:
        if self._gateway is None:
            settings = get_settings()
            redis = aioredis.from_url(settings.redis_url, decode_responses=True)
            self._gateway = MarketDataGateway(redis_client=redis)
        return self._gateway

    async def _fetch_quote(
        self, symbol: str
    ) -> dict[str, Any] | None:
        """Fetch quote data via the canonical market-data gateway."""
        try:
            gw = await self._get_gateway()
            q = await gw.get_quote(symbol, fail_closed=False)
            if not q or q.get("price") is None:
                return None
            price = float(q["price"])
            prev_close = float(q.get("previous_close") or price)
            change = price - prev_close if prev_close else 0.0
            change_pct = (change / prev_close * 100.0) if prev_close else 0.0
            return {
                "symbol": symbol,
                "price": price,
                "previous_close": prev_close,
                "change": round(change, 4),
                "change_pct": round(change_pct, 4),
                "volume": float(q.get("volume") or 0.0),
                "currency": "USD",
                "provider_used": (q.get("meta") or {}).get("provider_used"),
                "quality_flags": (q.get("meta") or {}).get("quality_flags", []),
            }
        except Exception:
            logger.debug("[commodities] failed to fetch %s", symbol, exc_info=True)
            return None

    async def _fetch_futures(self) -> list[dict[str, Any]]:
        """Fetch all commodity futures quotes."""
        posts: list[dict[str, Any]] = []

        for symbol, info in _COMMODITY_SYMBOLS.items():
            quote = await self._fetch_quote(symbol)
            if not quote or not quote["price"]:
                continue

            price = quote["price"]
            change = quote["change"]
            change_pct = quote["change_pct"]
            volume = quote["volume"]

            # Check for significant price moves
            significance = "medium"
            abs_pct = abs(change_pct)
            if abs_pct >= 5:
                significance = "critical"
            elif abs_pct >= 2:
                significance = "high"

            # Detect notable shifts from previous scrape
            prev = self._prev_prices.get(symbol)
            momentum_note = ""
            if prev and prev != 0:
                session_change = ((price - prev) / prev) * 100
                if abs(session_change) >= 1:
                    direction = "accelerating" if session_change > 0 else "declining"
                    momentum_note = f" | Intra-session: {direction} ({session_change:+.2f}%)"

            self._prev_prices[symbol] = price

            change_str = _format_change(change, change_pct)
            content = (
                f"[Commodity] {info['name']} ({info['code']})\n"
                f"Price: {price:.2f} {info['unit']} | Change: {change_str}\n"
                f"Volume: {volume:,}{momentum_note}"
            )

            posts.append(self._make_post(
                source_id=f"cmdty_{info['code']}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{symbol}/",
                raw_metadata={
                    "commodity": info["name"],
                    "futures_symbol": symbol,
                    "code": info["code"],
                    "price": price,
                    "previous_close": quote["previous_close"],
                    "change": change,
                    "change_pct": change_pct,
                    "volume": volume,
                    "unit": info["unit"],
                    "significance": significance,
                    "category": "futures",
                },
            ))

        return posts

    async def _fetch_etf_proxies(self) -> list[dict[str, Any]]:
        """Fetch commodity ETF prices as supplementary data."""
        posts: list[dict[str, Any]] = []

        for symbol, info in _ETF_SYMBOLS.items():
            quote = await self._fetch_quote(symbol)
            if not quote or not quote["price"]:
                continue

            price = quote["price"]
            change = quote["change"]
            change_pct = quote["change_pct"]
            volume = quote["volume"]

            change_str = _format_change(change, change_pct)
            content = (
                f"[Commodity ETF] {info['name']} ({symbol}) — {info['commodity']} proxy\n"
                f"Price: ${price:.2f} | Change: {change_str}\n"
                f"Volume: {volume:,}"
            )

            posts.append(self._make_post(
                source_id=f"cmdty_etf_{symbol}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M')}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{symbol}/",
                raw_metadata={
                    "etf_symbol": symbol,
                    "etf_name": info["name"],
                    "commodity": info["commodity"],
                    "code": info["code"],
                    "price": price,
                    "change": change,
                    "change_pct": change_pct,
                    "volume": volume,
                    "significance": "low",
                    "category": "etf_proxy",
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        # Primary: commodity futures
        futures_posts = await self._fetch_futures()
        all_posts.extend(futures_posts)

        # Secondary: ETF proxies (always fetch for cross-reference)
        etf_posts = await self._fetch_etf_proxies()
        all_posts.extend(etf_posts)

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_COMMODITIES = [
    # (name, code, futures_sym, price, prev_close, volume, unit)
    ("Crude Oil (WTI)", "CL", "CL=F", 72.45, 71.80, 485_230, "$/barrel"),
    ("Gold", "GC", "GC=F", 2_385.60, 2_372.30, 218_450, "$/oz"),
    ("Silver", "SI", "SI=F", 28.75, 28.20, 125_800, "$/oz"),
    ("Natural Gas", "NG", "NG=F", 2.18, 2.25, 342_100, "$/MMBtu"),
    ("Copper", "HG", "HG=F", 4.52, 4.48, 98_700, "$/lb"),
    ("Platinum", "PL", "PL=F", 1_025.40, 1_018.60, 42_300, "$/oz"),
    ("Wheat", "ZW", "ZW=F", 585.25, 578.50, 72_400, "cents/bushel"),
    ("Corn", "ZC", "ZC=F", 445.75, 442.00, 156_800, "cents/bushel"),
]

_MOCK_ETFS = [
    ("United States Oil Fund", "USO", "Crude Oil", "CL", 72.30, 71.65, 8_520_000),
    ("SPDR Gold Trust", "GLD", "Gold", "GC", 220.15, 218.90, 6_210_000),
    ("iShares Silver Trust", "SLV", "Silver", "SI", 26.40, 25.95, 12_450_000),
    ("United States Natural Gas Fund", "UNG", "Natural Gas", "NG", 6.85, 7.05, 18_900_000),
    ("United States Copper Index Fund", "CPER", "Copper", "HG", 28.90, 28.60, 245_000),
]


class MockCommoditiesScraper(BaseScraper):
    """Mock scraper generating realistic commodity futures and ETF data."""

    @property
    def platform(self) -> str:
        return "commodities"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        now_str = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")

        # Generate futures posts
        for name, code, futures_sym, base_price, base_prev, base_vol, unit in _MOCK_COMMODITIES:
            # Add realistic jitter
            price = base_price * random.uniform(0.97, 1.03)
            prev_close = base_prev * random.uniform(0.99, 1.01)
            volume = int(base_vol * random.uniform(0.7, 1.3))
            change = price - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0

            change_str = _format_change(change, change_pct)

            significance = "medium"
            if abs(change_pct) >= 5:
                significance = "critical"
            elif abs(change_pct) >= 2:
                significance = "high"

            content = (
                f"[Commodity] {name} ({code})\n"
                f"Price: {price:.2f} {unit} | Change: {change_str}\n"
                f"Volume: {volume:,}"
            )

            posts.append(self._make_post(
                source_id=f"cmdty_{code}_{now_str}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{futures_sym}/",
                raw_metadata={
                    "commodity": name,
                    "futures_symbol": futures_sym,
                    "code": code,
                    "price": round(price, 2),
                    "previous_close": round(prev_close, 2),
                    "change": round(change, 4),
                    "change_pct": round(change_pct, 4),
                    "volume": volume,
                    "unit": unit,
                    "significance": significance,
                    "category": "futures",
                },
            ))

        # Generate ETF proxy posts
        for etf_name, symbol, commodity, code, base_price, base_prev, base_vol in _MOCK_ETFS:
            price = base_price * random.uniform(0.97, 1.03)
            prev_close = base_prev * random.uniform(0.99, 1.01)
            volume = int(base_vol * random.uniform(0.7, 1.3))
            change = price - prev_close
            change_pct = (change / prev_close * 100) if prev_close else 0

            change_str = _format_change(change, change_pct)
            content = (
                f"[Commodity ETF] {etf_name} ({symbol}) — {commodity} proxy\n"
                f"Price: ${price:.2f} | Change: {change_str}\n"
                f"Volume: {volume:,}"
            )

            posts.append(self._make_post(
                source_id=f"cmdty_etf_{symbol}_{now_str}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{symbol}/",
                raw_metadata={
                    "etf_symbol": symbol,
                    "etf_name": etf_name,
                    "commodity": commodity,
                    "code": code,
                    "price": round(price, 2),
                    "change": round(change, 4),
                    "change_pct": round(change_pct, 4),
                    "volume": volume,
                    "significance": "low",
                    "category": "etf_proxy",
                },
            ))

        return posts
