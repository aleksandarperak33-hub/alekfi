"""Market Context Worker — 6 data layers for institutional-grade context.

Free Finnhub layers:
  1. Analyst Recommendations — consensus shifts (Finnhub free)
  2. Earnings Calendar + Surprises — proximity + beat/miss (Finnhub free)
  5. Company News — volume and headlines (Finnhub free)

Gateway-based layers (Finnhub premium replacements):
  3. Technical Indicators — RSI, SMA crossovers from daily candles (yfinance)
  4. Price vs 52-week range — current positioning (yfinance)
  6. Support & Resistance — recent highs/lows as S/R levels (yfinance)

Rate limiting: ~40 Finnhub calls/min (1.5s between calls, 65s pause every 35).
All OHLCV requests route through the canonical MarketDataGateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
import numpy as np

from alekfi.config import get_settings
from alekfi.marketdata import MarketDataGateway

logger = logging.getLogger(__name__)

FINNHUB_BASE = "https://finnhub.io/api/v1"

# Scan schedules (seconds)
MEDIUM_INTERVAL = 14400   # 4h    — analyst
SLOW_INTERVAL = 86400     # 24h   — earnings calendar


class MarketContextWorker:
    """Fetches 6 market context layers and feeds into Sigma Engine."""

    def __init__(self, api_key: str | None = None, interval: int = 900) -> None:
        self._api_key = api_key or get_settings().finnhub_api_key
        self._interval = interval
        self._call_count = 0
        self._last_slow_run: datetime | None = None
        self._last_medium_run: datetime | None = None
        self._gateway: MarketDataGateway | None = None
        self._stats: dict[str, int] = {
            "cycles": 0, "api_calls": 0, "errors": 0,
            "analyst_updates": 0, "earnings_updates": 0,
            "technical_updates": 0, "price_range_updates": 0,
            "news_updates": 0, "sr_updates": 0,
        }

    # ── Rate-limited Finnhub API call wrapper ─────────────────────────

    async def _api_call(
        self, client: httpx.AsyncClient, endpoint: str, params: dict
    ) -> dict | list | None:
        """Make a rate-limited Finnhub API call."""
        params["token"] = self._api_key
        try:
            resp = await client.get(
                f"{FINNHUB_BASE}/{endpoint}", params=params, timeout=15
            )
            self._call_count += 1
            self._stats["api_calls"] += 1

            if resp.status_code == 429:
                logger.warning("[market-ctx] rate limited, backing off 10s")
                await asyncio.sleep(10)
                return None
            if resp.status_code == 200:
                data = resp.json()
                # Skip "access denied" responses from premium endpoints
                if isinstance(data, dict) and "error" in data:
                    return None
                return data
            return None
        except Exception:
            self._stats["errors"] += 1
            return None
        finally:
            await asyncio.sleep(1.5)
            if self._call_count % 35 == 0:
                logger.debug("[market-ctx] rate limit pause (35 calls)")
                await asyncio.sleep(65)

    async def _get_gateway(self) -> MarketDataGateway:
        if self._gateway is None:
            import redis.asyncio as aioredis
            s = get_settings()
            r = aioredis.from_url(s.redis_url, decode_responses=True)
            self._gateway = MarketDataGateway(redis_client=r)
        return self._gateway

    # ── Layer 1: Analyst Recommendations (Finnhub free) ───────────────

    async def _scan_analyst_recommendations(
        self, client: httpx.AsyncClient, tickers: list[str]
    ) -> None:
        """Fetch analyst recommendation trends and consensus shifts."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            for ticker in tickers:
                trends = await self._api_call(
                    client, "stock/recommendation", {"symbol": ticker}
                )
                if not trends or not isinstance(trends, list) or len(trends) == 0:
                    continue

                latest = trends[0]
                prior = trends[1] if len(trends) > 1 else None

                result = {
                    "ticker": ticker,
                    "period": latest.get("period"),
                    "strong_buy": latest.get("strongBuy", 0),
                    "buy": latest.get("buy", 0),
                    "hold": latest.get("hold", 0),
                    "sell": latest.get("sell", 0),
                    "strong_sell": latest.get("strongSell", 0),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                total = (result["strong_buy"] + result["buy"] + result["hold"]
                         + result["sell"] + result["strong_sell"])
                if total > 0:
                    consensus = (
                        result["strong_buy"] * 2 + result["buy"] * 1
                        - result["sell"] * 1 - result["strong_sell"] * 2
                    ) / total
                    result["consensus_score"] = round(consensus, 3)
                else:
                    result["consensus_score"] = 0.0

                if prior:
                    prior_total = (prior.get("strongBuy", 0) + prior.get("buy", 0)
                                   + prior.get("hold", 0) + prior.get("sell", 0)
                                   + prior.get("strongSell", 0))
                    if prior_total > 0:
                        prior_consensus = (
                            prior.get("strongBuy", 0) * 2 + prior.get("buy", 0)
                            - prior.get("sell", 0) - prior.get("strongSell", 0) * 2
                        ) / prior_total
                        result["consensus_change"] = round(
                            result["consensus_score"] - prior_consensus, 3
                        )
                    else:
                        result["consensus_change"] = 0.0
                else:
                    result["consensus_change"] = 0.0

                if abs(result["consensus_change"]) > 0:
                    await self._record_sigma(
                        "analyst_rating_change", ticker, result["consensus_change"]
                    )

                await r.hset(
                    "alekfi:market_context:analyst", ticker,
                    json.dumps(result, default=str)
                )
                await r.expire("alekfi:market_context:analyst", 14400)
                self._stats["analyst_updates"] += 1
        finally:
            await r.aclose()

    # ── Layer 2: Earnings Calendar + Surprises (Finnhub free) ─────────

    async def _scan_earnings(
        self, client: httpx.AsyncClient, tickers: list[str]
    ) -> None:
        """Fetch earnings calendar and historical surprises."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            today = datetime.now(timezone.utc).date()
            cal = await self._api_call(
                client, "calendar/earnings",
                {"from": today.isoformat(), "to": (today + timedelta(days=30)).isoformat()}
            )
            upcoming: dict[str, dict] = {}
            if cal and isinstance(cal, dict):
                for item in cal.get("earningsCalendar", []):
                    sym = item.get("symbol", "")
                    if sym:
                        upcoming[sym] = {
                            "date": item.get("date"),
                            "hour": item.get("hour", ""),
                            "eps_estimate": item.get("epsEstimate"),
                            "revenue_estimate": item.get("revenueEstimate"),
                        }

            if upcoming:
                await r.set(
                    "alekfi:market_context:earnings_calendar",
                    json.dumps(upcoming, default=str), ex=86400,
                )

            for ticker in tickers:
                result: dict[str, Any] = {"ticker": ticker}

                if ticker in upcoming:
                    earn_date_str = upcoming[ticker].get("date")
                    if earn_date_str:
                        earn_date = datetime.strptime(earn_date_str, "%Y-%m-%d").date()
                        days_to_earnings = (earn_date - today).days
                        result["next_earnings_date"] = earn_date_str
                        result["days_to_earnings"] = days_to_earnings
                        result["earnings_hour"] = upcoming[ticker].get("hour", "")
                        result["eps_estimate"] = upcoming[ticker].get("epsEstimate")

                        if 0 < days_to_earnings <= 14:
                            proximity_score = max(0, 15 - days_to_earnings)
                            await self._record_sigma(
                                "earnings_proximity", ticker, float(proximity_score)
                            )

                earnings = await self._api_call(
                    client, "stock/earnings", {"symbol": ticker, "limit": 4}
                )
                if earnings and isinstance(earnings, list) and len(earnings) > 0:
                    latest_earn = earnings[0]
                    result["last_eps_actual"] = latest_earn.get("actual")
                    result["last_eps_estimate"] = latest_earn.get("estimate")
                    if latest_earn.get("actual") and latest_earn.get("estimate"):
                        surprise = latest_earn["actual"] - latest_earn["estimate"]
                        surprise_pct = (surprise / abs(latest_earn["estimate"]) * 100
                                        if latest_earn["estimate"] != 0 else 0)
                        result["last_surprise"] = round(surprise, 4)
                        result["last_surprise_pct"] = round(surprise_pct, 2)
                        await self._record_sigma(
                            "earnings_surprise", ticker, surprise_pct
                        )

                    beats = sum(1 for e in earnings
                                if e.get("actual") and e.get("estimate")
                                and e["actual"] > e["estimate"])
                    result["beat_streak"] = beats
                    result["quarters_reported"] = len(earnings)

                result["timestamp"] = datetime.now(timezone.utc).isoformat()
                await r.hset(
                    "alekfi:market_context:earnings", ticker,
                    json.dumps(result, default=str)
                )
                await r.expire("alekfi:market_context:earnings", 86400)
                self._stats["earnings_updates"] += 1
        finally:
            await r.aclose()

    # ── Layer 3: Technical Indicators (yfinance — free) ───────────────

    async def _scan_technicals(self, tickers: list[str]) -> None:
        """Compute RSI, SMA crossovers, and trend from gateway daily candles."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            for ticker in tickers:
                try:
                    result = await self._compute_technicals(ticker)
                    if not result:
                        continue

                    await self._record_sigma(
                        "technical_signal_flip", ticker, result["signal_strength"]
                    )

                    await r.hset(
                        "alekfi:market_context:technicals", ticker,
                        json.dumps(result, default=str)
                    )
                    await r.expire("alekfi:market_context:technicals", 3600)
                    self._stats["technical_updates"] += 1
                except Exception:
                    continue
        finally:
            await r.aclose()

    async def _compute_technicals(self, ticker: str) -> dict[str, Any] | None:
        """Compute technical indicators from gateway daily candles."""
        gw = await self._get_gateway()
        data = await gw.get_ohlcv(ticker, period="3mo", interval="1d", min_points=20, fail_closed=False)
        rows = data.get("rows") or []
        if len(rows) < 20:
            return None

        close = np.array([float(r.get("close") or 0.0) for r in rows if r.get("close") is not None], dtype=float)
        if len(close) < 20:
            return None
        n = len(close)

        # RSI (14-period)
        deltas = np.diff(close)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        avg_gain = np.mean(gains[-14:])
        avg_loss = np.mean(losses[-14:])
        rs = avg_gain / max(avg_loss, 1e-10)
        rsi = 100 - (100 / (1 + rs))

        # SMA crossovers
        sma_20 = np.mean(close[-20:])
        sma_50 = np.mean(close[-50:]) if n >= 50 else sma_20
        current = close[-1]

        # Signal: buy/sell/neutral based on RSI + SMA
        buy_signals = 0
        sell_signals = 0

        if rsi < 30:
            buy_signals += 1  # Oversold
        elif rsi > 70:
            sell_signals += 1  # Overbought

        if current > sma_20:
            buy_signals += 1  # Above 20 SMA
        else:
            sell_signals += 1

        if current > sma_50:
            buy_signals += 1  # Above 50 SMA
        else:
            sell_signals += 1

        if sma_20 > sma_50:
            buy_signals += 1  # Golden cross direction
        else:
            sell_signals += 1

        total = buy_signals + sell_signals
        strength = (buy_signals - sell_signals) / max(total, 1)

        if strength > 0.3:
            signal = "buy"
        elif strength < -0.3:
            signal = "sell"
        else:
            signal = "neutral"

        # Trend strength: use price change over 20 days
        pct_change_20d = ((current - close[-20]) / close[-20]) * 100 if n >= 20 else 0
        trending = abs(pct_change_20d) > 5

        return {
            "ticker": ticker,
            "signal": signal,
            "signal_strength": round(strength, 3),
            "rsi": round(float(rsi), 1),
            "sma_20": round(float(sma_20), 2),
            "sma_50": round(float(sma_50), 2),
            "current_price": round(float(current), 2),
            "buy_count": buy_signals,
            "sell_count": sell_signals,
            "pct_change_20d": round(float(pct_change_20d), 2),
            "trending": trending,
            "data_provider": (data.get("meta") or {}).get("provider_used"),
            "quality_flags": (data.get("meta") or {}).get("quality_flags", []),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ── Layer 4: Price vs 52-week range (yfinance — free) ─────────────

    async def _scan_price_range(self, tickers: list[str]) -> None:
        """Compute price positioning within 52-week range."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            for ticker in tickers:
                try:
                    result = await self._compute_price_range(ticker)
                    if not result:
                        continue

                    if result.get("range_position") is not None:
                        await self._record_sigma(
                            "price_target_change", ticker, result["range_position"]
                        )

                    await r.hset(
                        "alekfi:market_context:price_target", ticker,
                        json.dumps(result, default=str)
                    )
                    await r.expire("alekfi:market_context:price_target", 14400)
                    self._stats["price_range_updates"] += 1
                except Exception:
                    continue
        finally:
            await r.aclose()

    async def _compute_price_range(self, ticker: str) -> dict[str, Any] | None:
        """Compute 52-week high/low positioning from gateway data."""
        gw = await self._get_gateway()
        data = await gw.get_ohlcv(ticker, period="1y", interval="1d", min_points=20, fail_closed=False)
        rows = data.get("rows") or []
        if len(rows) < 20:
            return None

        current = float(rows[-1].get("close") or 0.0)
        highs = [float(r.get("high") or 0.0) for r in rows]
        lows = [float(r.get("low") or 0.0) for r in rows]
        high_52w = max(highs) if highs else 0.0
        low_52w = min(lows) if lows else 0.0

        if high_52w <= 0 or low_52w <= 0 or high_52w == low_52w:
            return None

        # Position in range: 0% = at 52w low, 100% = at 52w high
        range_position = ((current - low_52w) / (high_52w - low_52w)) * 100

        # Upside to 52w high
        upside_pct = ((high_52w - current) / current) * 100

        return {
            "ticker": ticker,
            "current_price": round(current, 2),
            "high_52w": round(high_52w, 2),
            "low_52w": round(low_52w, 2),
            "range_position": round(range_position, 1),
            "upside_to_52w_high": round(upside_pct, 2),
            "downside_to_52w_low": round(((current - low_52w) / current) * 100, 2),
            "data_provider": (data.get("meta") or {}).get("provider_used"),
            "quality_flags": (data.get("meta") or {}).get("quality_flags", []),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    # ── Layer 5: Company News (Finnhub free) ──────────────────────────

    async def _scan_news(
        self, client: httpx.AsyncClient, tickers: list[str]
    ) -> None:
        """Fetch recent company news and compute volume signals."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            today = datetime.now(timezone.utc).date()
            week_ago = today - timedelta(days=7)

            for ticker in tickers:
                data = await self._api_call(
                    client, "company-news",
                    {"symbol": ticker, "from": week_ago.isoformat(), "to": today.isoformat()}
                )
                if not data or not isinstance(data, list):
                    continue

                total_articles = len(data)
                headlines: list[dict] = []
                for article in data[:20]:
                    headlines.append({
                        "headline": (article.get("headline", ""))[:200],
                        "source": article.get("source", ""),
                        "datetime": article.get("datetime"),
                        "url": article.get("url", ""),
                    })

                result = {
                    "ticker": ticker,
                    "total_articles_7d": total_articles,
                    "recent_headlines": headlines[:5],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }

                if total_articles > 0:
                    await self._record_sigma(
                        "news_volume", ticker, float(total_articles)
                    )

                await r.hset(
                    "alekfi:market_context:news", ticker,
                    json.dumps(result, default=str)
                )
                await r.expire("alekfi:market_context:news", 3600)
                self._stats["news_updates"] += 1
        finally:
            await r.aclose()

    # ── Layer 6: Support & Resistance (yfinance — free) ───────────────

    async def _scan_support_resistance(self, tickers: list[str]) -> None:
        """Compute support/resistance levels from recent price pivots."""
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)

        try:
            for ticker in tickers:
                try:
                    result = await self._compute_sr_levels(ticker)
                    if not result:
                        continue

                    await r.hset(
                        "alekfi:market_context:sr", ticker,
                        json.dumps(result, default=str)
                    )
                    await r.expire("alekfi:market_context:sr", 86400)
                    self._stats["sr_updates"] += 1
                except Exception:
                    continue
        finally:
            await r.aclose()

    async def _compute_sr_levels(self, ticker: str) -> dict[str, Any] | None:
        """Compute support/resistance from gateway pivot history."""
        gw = await self._get_gateway()
        data = await gw.get_ohlcv(ticker, period="6mo", interval="1d", min_points=30, fail_closed=False)
        rows = data.get("rows") or []
        if len(rows) < 30:
            return None

        highs = np.array([float(r.get("high") or 0.0) for r in rows], dtype=float)
        lows = np.array([float(r.get("low") or 0.0) for r in rows], dtype=float)
        current = float(rows[-1].get("close") or 0.0)

        # Find pivot highs and lows (local extrema with 5-bar lookback)
        support_candidates: list[float] = []
        resistance_candidates: list[float] = []

        for i in range(5, len(lows) - 5):
            # Pivot low (support)
            if lows[i] == min(lows[i - 5:i + 6]):
                support_candidates.append(float(lows[i]))
            # Pivot high (resistance)
            if highs[i] == max(highs[i - 5:i + 6]):
                resistance_candidates.append(float(highs[i]))

        # Filter to levels below/above current price
        support_levels = sorted([s for s in support_candidates if s < current])
        resistance_levels = sorted([r for r in resistance_candidates if r > current])

        result: dict[str, Any] = {
            "ticker": ticker,
            "current_price": round(current, 2),
            "support_levels": [round(s, 2) for s in support_levels[-3:]],
            "resistance_levels": [round(r, 2) for r in resistance_levels[:3]],
            "data_provider": (data.get("meta") or {}).get("provider_used"),
            "quality_flags": (data.get("meta") or {}).get("quality_flags", []),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if support_levels:
            nearest_support = support_levels[-1]
            result["nearest_support"] = round(nearest_support, 2)
            result["support_distance_pct"] = round(
                ((current - nearest_support) / current) * 100, 2
            )
        if resistance_levels:
            nearest_resistance = resistance_levels[0]
            result["nearest_resistance"] = round(nearest_resistance, 2)
            result["resistance_distance_pct"] = round(
                ((nearest_resistance - current) / current) * 100, 2
            )

        return result

    # ── Sigma integration ─────────────────────────────────────────────

    async def _record_sigma(
        self, signal_type: str, ticker: str, value: float
    ) -> dict[str, Any]:
        """Record a market context metric in the Sigma Engine."""
        from alekfi.analysis.sigma_engine import get_sigma_engine
        engine = await get_sigma_engine()
        return await engine.record_and_score(signal_type, ticker, value)

    # ── Ticker selection ──────────────────────────────────────────────

    async def _get_tracked_tickers(self) -> list[str]:
        """Get tickers to scan from recent signals + core watchlist."""
        from alekfi.db.database import get_session
        from sqlalchemy import text

        tickers: set[str] = set()
        gw = await self._get_gateway()
        core = {"SPY", "QQQ", "NVDA", "TSLA", "AAPL", "MSFT", "AMZN", "META", "GOOG", "AMD"}
        for c in core:
            norm = gw.normalize_symbol(c)
            if norm.valid:
                tickers.add(norm.symbol)

        try:
            async with get_session() as session:
                rows = (await session.execute(text("""
                    SELECT DISTINCT ai->>'symbol' AS ticker
                    FROM signals s
                    CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                    WHERE s.created_at >= NOW() - INTERVAL '24 hours'
                      AND ai->>'symbol' IS NOT NULL
                    ORDER BY ticker
                    LIMIT 30
                """))).all()
                for row in rows:
                    if not row.ticker:
                        continue
                    norm = gw.normalize_symbol(str(row.ticker).upper())
                    if norm.valid:
                        tickers.add(norm.symbol)
        except Exception:
            logger.warning("[market-ctx] failed to fetch tracked tickers from DB")

        return sorted(tickers)[:40]

    # ── Main loop ─────────────────────────────────────────────────────

    async def run(self) -> None:
        """Run the market context worker loop."""
        if not self._api_key:
            logger.warning("[market-ctx] no FINNHUB_API_KEY configured, skipping")
            return

        logger.info("[market-ctx] starting (interval=%ds)", self._interval)

        while True:
            try:
                await self._scan_cycle()
            except Exception:
                logger.error("[market-ctx] scan cycle failed", exc_info=True)
                self._stats["errors"] += 1

            await asyncio.sleep(self._interval)

    async def _scan_cycle(self) -> None:
        """Run one scan cycle, choosing which layers to refresh."""
        now = datetime.now(timezone.utc)
        tickers = await self._get_tracked_tickers()
        self._stats["cycles"] += 1
        self._call_count = 0

        logger.info("[market-ctx] cycle %d: scanning %d tickers", self._stats["cycles"], len(tickers))

        async with httpx.AsyncClient() as client:
            # ── FAST layers (every cycle) ──────────────────────────
            # yfinance-based (no Finnhub API calls)
            await self._scan_technicals(tickers)
            await self._scan_price_range(tickers)
            await self._scan_support_resistance(tickers)

            # Finnhub-based
            await self._scan_news(client, tickers)

            # ── MEDIUM layers (every 4h) ──────────────────────────
            if (self._last_medium_run is None
                    or (now - self._last_medium_run).total_seconds() >= MEDIUM_INTERVAL):
                await self._scan_analyst_recommendations(client, tickers)
                self._last_medium_run = now

            # ── SLOW layers (every 24h) ───────────────────────────
            if (self._last_slow_run is None
                    or (now - self._last_slow_run).total_seconds() >= SLOW_INTERVAL):
                await self._scan_earnings(client, tickers)
                self._last_slow_run = now

        logger.info(
            "[market-ctx] cycle done: %d API calls, tech=%d range=%d sr=%d news=%d analyst=%d earn=%d",
            self._stats["api_calls"],
            self._stats["technical_updates"], self._stats["price_range_updates"],
            self._stats["sr_updates"], self._stats["news_updates"],
            self._stats["analyst_updates"], self._stats["earnings_updates"],
        )

    def get_stats(self) -> dict[str, int]:
        return dict(self._stats)
