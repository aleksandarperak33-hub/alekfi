"""Canonical market data gateway with provider fallback, cache, and circuit breakers.

All workers should consume market data through this module.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Any

import httpx
import redis.asyncio as aioredis

from alekfi.config import get_settings

logger = logging.getLogger(__name__)
_YF_LOGGER = logging.getLogger("yfinance")
_YF_LOGGER.setLevel(logging.CRITICAL)

_STOOQ_QUOTE_URL = "https://stooq.com/q/l/"
_STOOQ_HISTORY_URL = "https://stooq.com/q/d/l/"
_FINNHUB_BASE = "https://finnhub.io/api/v1"

_CB_FAIL_THRESHOLD = int(os.environ.get("MD_CB_FAIL_THRESHOLD", "4"))
_CB_COOLDOWN_SECONDS = int(os.environ.get("MD_CB_COOLDOWN_SECONDS", "600"))

_QUOTE_TTL_SECONDS = int(os.environ.get("MD_QUOTE_TTL_SECONDS", "300"))
_OHLCV_TTL_SECONDS = int(os.environ.get("MD_OHLCV_TTL_SECONDS", "1800"))
_SYMBOL_FAIL_THRESHOLD = int(os.environ.get("MD_SYMBOL_FAIL_THRESHOLD", "5"))
_SYMBOL_FAIL_TTL_SECONDS = int(os.environ.get("MD_SYMBOL_FAIL_TTL_SECONDS", "86400"))

_BANNED_SYMBOLS = {
    "AI",
    "API",
    "APP",
    "MARKETS",
    "MARKET",
    "PREDICTION",
    "POSTMAN",
    "FIRMUS",
    "NIKKEI",
    "CAPITAL",
    "GOLUB",
    "GROUP",
    "HOLDINGS",
    "TRUMP.X",
    "U.S.",
    "DXY",
    "IRN",
    "USDT",
    "USDCNY",
    "EURUSD",
    "USDJPY",
    "USDCHF",
    "NQ",
    "NDX",
    "SPX",
    "VIX",
    "UN",
    "NKY",
    # Persistently failing/dead tickers observed in production logs.
    "BBD.B",
    "DN",
    "BDNCE",
}


@dataclass
class NormalizedSymbol:
    symbol: str
    warnings: list[str]
    valid: bool


@dataclass
class MarketDataSnapshot:
    symbol: str
    last_price: float | None
    asof: str
    volume: float | None
    dollar_volume: float | None
    ret_1d: float | None
    ret_5d: float | None
    atr_14: float | None
    spread_bps_est: float
    quality_flags: list[str]
    provider_used: str | None
    fallback_chain: list[str]
    cache_hit: bool
    data_completeness: float


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _to_epoch(ts: datetime) -> int:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return int(ts.timestamp())


class MarketDataGateway:
    """Single entrypoint for market data in AlekFi."""

    def __init__(self, redis_client: aioredis.Redis | None = None) -> None:
        settings = get_settings()
        self._redis = redis_client or aioredis.from_url(settings.redis_url, decode_responses=True)
        self._finnhub_key = (settings.finnhub_api_key or "").strip()

    async def close(self) -> None:
        try:
            await self._redis.aclose()
        except Exception:
            pass

    @staticmethod
    def is_degraded(meta: dict[str, Any] | None) -> bool:
        """Whether metadata flags indicate this datapoint is unsafe for labeling."""
        if not isinstance(meta, dict):
            return True
        flags = {str(f).upper() for f in (meta.get("quality_flags") or [])}
        degraded = {"AUTH_FAIL", "SYMBOL_INVALID", "INCOMPLETE_WINDOW"}
        return bool(flags & degraded)

    @staticmethod
    def label_skip_reason(meta: dict[str, Any] | None) -> str | None:
        """Return a normalized skip reason for degraded data, else None."""
        if not isinstance(meta, dict):
            return "MISSING_META"
        flags = {str(f).upper() for f in (meta.get("quality_flags") or [])}
        if "AUTH_FAIL" in flags:
            return "AUTH_FAIL"
        if "SYMBOL_INVALID" in flags:
            return "SYMBOL_INVALID"
        if "INCOMPLETE_WINDOW" in flags:
            return "INCOMPLETE_WINDOW"
        return None

    def normalize_symbol(self, raw_ticker: str) -> NormalizedSymbol:
        warnings: list[str] = []
        import re

        symbol = (raw_ticker or "").strip().upper().replace("$", "")

        rename_map = {
            "SQ": "XYZ",
            "FB": "META",
            "TWTR": "X",
            "BTC": "BTC-USD",
            "ETH": "ETH-USD",
            "SOL": "SOL-USD",
            "XRP": "XRP-USD",
            "DOGE": "DOGE-USD",
            "ADA": "ADA-USD",
            "AVAX": "AVAX-USD",
            "LINK": "LINK-USD",
            "DOT": "DOT-USD",
            "MATIC": "MATIC-USD",
            "SHIB": "SHIB-USD",
            "BNB": "BNB-USD",
            "WIF": "WIF-USD",
        }
        symbol = rename_map.get(symbol, symbol)

        if not symbol:
            return NormalizedSymbol(symbol="", warnings=["empty_symbol"], valid=False)

        if symbol in _BANNED_SYMBOLS:
            return NormalizedSymbol(symbol=symbol, warnings=["SYMBOL_INVALID", "symbol_banned"], valid=False)

        if "/" in symbol:
            return NormalizedSymbol(symbol=symbol, warnings=["symbol_contains_slash", "SYMBOL_INVALID"], valid=False)

        # Allow common non-equity formats used in this codebase.
        allowed_special = (
            symbol.startswith("^")
            or bool(re.match(r"^[A-Z]{1,4}=F$", symbol))
            or symbol.endswith("-USD")
        )

        if not allowed_special:
            # Accept typical equities/ETFs and dotted class shares.
            if len(symbol) == 1:
                return NormalizedSymbol(symbol=symbol, warnings=["SYMBOL_INVALID", "symbol_too_short"], valid=False)
            if not re.match(r"^[A-Z0-9]{1,5}(\.[AB])?$", symbol):
                return NormalizedSymbol(symbol=symbol, warnings=["SYMBOL_INVALID", "symbol_format_unusual"], valid=False)

        return NormalizedSymbol(symbol=symbol, warnings=warnings, valid=True)

    async def get_provider_health(self) -> dict[str, Any]:
        providers = ["finnhub", "stooq", "yfinance"]
        out: dict[str, Any] = {"timestamp": _utcnow().isoformat(), "providers": {}}
        now = time.time()

        for p in providers:
            state = await self._get_cb_state(p)
            try:
                open_until = float(state.get("open_until", 0) or 0)
            except Exception:
                open_until = 0.0
            out["providers"][p] = {
                "open": bool(open_until > now),
                "open_until": open_until,
                "fail_count": int(state.get("fail_count", 0)),
                "last_error": state.get("last_error"),
                "last_error_at": state.get("last_error_at"),
            }
        return out

    async def get_quotes_batch(
        self,
        symbols: list[str],
        *,
        fail_closed: bool = False,
        concurrency: int = 8,
    ) -> dict[str, dict[str, Any]]:
        """Fetch a batch of quotes with bounded concurrency."""
        if not symbols:
            return {}

        sem = asyncio.Semaphore(max(1, concurrency))
        out: dict[str, dict[str, Any]] = {}

        async def _one(sym: str) -> None:
            async with sem:
                q = await self.get_quote(sym, fail_closed=fail_closed)
                if q:
                    out[sym] = q

        await asyncio.gather(*[_one(s) for s in symbols])
        return out

    async def get_market_data(
        self,
        symbol: str,
        *,
        fail_closed: bool = False,
    ) -> MarketDataSnapshot:
        """Canonical market-data contract for scoring, tradability, and labeling.

        Fields:
        - last_price, asof, volume, dollar_volume
        - ret_1d, ret_5d, atr_14
        - spread_bps_est
        - quality flags + provider metadata
        """
        quote = await self.get_quote(symbol, fail_closed=fail_closed)
        if not quote:
            now_iso = _utcnow().isoformat()
            return MarketDataSnapshot(
                symbol=symbol,
                last_price=None,
                asof=now_iso,
                volume=None,
                dollar_volume=None,
                ret_1d=None,
                ret_5d=None,
                atr_14=None,
                spread_bps_est=60.0,
                quality_flags=["NO_DATA"],
                provider_used=None,
                fallback_chain=[],
                cache_hit=False,
                data_completeness=0.0,
            )

        meta = quote.get("meta") or {}
        qf = list(meta.get("quality_flags") or [])
        provider = meta.get("provider_used")
        asof = str(meta.get("fetched_at") or _utcnow().isoformat())
        price = self._safe_float(quote.get("price"), default=None)
        volume = self._safe_float(quote.get("volume"), default=None)
        ret_1d = self._safe_float(quote.get("change_1d"), default=None)
        ret_5d = self._safe_float(quote.get("change_5d"), default=None)
        completeness = self._safe_float(meta.get("data_completeness"), default=1.0)

        # Fill missing return fields from daily OHLCV deterministically.
        rows = []
        if ret_1d is None or ret_5d is None or price is None:
            ohlcv = await self.get_ohlcv(symbol, period="1mo", interval="1d", min_points=6, fail_closed=False)
            rows = ohlcv.get("rows") or []
            if rows:
                last_close = self._safe_float(rows[-1].get("close"), default=None)
                prev_close = self._safe_float(rows[-2].get("close"), default=None) if len(rows) >= 2 else None
                close_5 = self._safe_float(rows[-6].get("close"), default=None) if len(rows) >= 6 else None
                if price is None and last_close is not None:
                    price = last_close
                if ret_1d is None and last_close is not None and prev_close and prev_close > 0:
                    ret_1d = round(((last_close - prev_close) / prev_close) * 100.0, 4)
                    qf.append("RET_1D_FROM_OHLCV")
                if ret_5d is None and last_close is not None and close_5 and close_5 > 0:
                    ret_5d = round(((last_close - close_5) / close_5) * 100.0, 4)
                    qf.append("RET_5D_FROM_OHLCV")

        # ATR(14) from daily OHLCV.
        atr_14 = None
        if not rows:
            ohlcv = await self.get_ohlcv(symbol, period="3mo", interval="1d", min_points=15, fail_closed=False)
            rows = ohlcv.get("rows") or []
        atr_14 = self._atr14_from_rows(rows)
        if atr_14 is None:
            qf.append("ATR14_UNAVAILABLE")

        dollar_volume = None
        if price is not None and volume is not None and price > 0 and volume > 0:
            dollar_volume = float(price) * float(volume)

        spread_bps = self._spread_bps_est(price=price, dollar_volume=dollar_volume)

        return MarketDataSnapshot(
            symbol=str(quote.get("symbol") or symbol),
            last_price=round(float(price), 4) if price is not None else None,
            asof=asof,
            volume=float(volume) if volume is not None else None,
            dollar_volume=round(float(dollar_volume), 2) if dollar_volume is not None else None,
            ret_1d=round(float(ret_1d), 4) if ret_1d is not None else None,
            ret_5d=round(float(ret_5d), 4) if ret_5d is not None else None,
            atr_14=round(float(atr_14), 4) if atr_14 is not None else None,
            spread_bps_est=round(float(spread_bps), 2),
            quality_flags=sorted({str(x) for x in qf if x}),
            provider_used=provider,
            fallback_chain=list(meta.get("fallback_chain") or []),
            cache_hit=bool(meta.get("cache_hit", False)),
            data_completeness=round(float(completeness), 4),
        )

    async def get_quote(self, symbol: str, *, fail_closed: bool = False) -> dict[str, Any] | None:
        norm = self.normalize_symbol(symbol)
        if not norm.valid:
            return {
                "symbol": symbol,
                "price": None,
                "previous_close": None,
                "change_1d": None,
                "change_5d": None,
                "volume": None,
                "meta": {
                    "provider_used": None,
                    "fallback_chain": [],
                    "quality_flags": norm.warnings + ["SYMBOL_INVALID"],
                    "cache_hit": False,
                    "fetched_at": _utcnow().isoformat(),
                },
            }

        if await self._symbol_is_quarantined(norm.symbol):
            return {
                "symbol": norm.symbol,
                "price": None,
                "previous_close": None,
                "change_1d": None,
                "change_5d": None,
                "volume": None,
                "meta": {
                    "provider_used": None,
                    "fallback_chain": [],
                    "quality_flags": ["SYMBOL_INVALID", "SYMBOL_QUARANTINED"],
                    "cache_hit": False,
                    "fetched_at": _utcnow().isoformat(),
                },
            }

        cache_key = self._cache_key("quote", norm.symbol)
        cached = await self._cache_get(cache_key)
        if cached:
            meta = cached.get("meta", {})
            meta["cache_hit"] = True
            cached["meta"] = meta
            return cached

        fallback_chain: list[str] = []
        quality_flags: list[str] = []

        provider_order = ["finnhub", "stooq", "yfinance"]
        for provider in provider_order:
            if not await self._provider_available(provider):
                fallback_chain.append(f"{provider}:open_circuit")
                quality_flags.append("PROVIDER_UNAVAILABLE")
                continue

            try:
                if provider == "finnhub":
                    data = await self._quote_finnhub(norm.symbol)
                elif provider == "stooq":
                    data = await self._quote_stooq(norm.symbol)
                else:
                    data = await self._quote_yfinance(norm.symbol)

                if not data or data.get("price") in (None, 0):
                    await self._provider_fail(provider, "NO_DATA")
                    await self._symbol_fail(norm.symbol)
                    fallback_chain.append(f"{provider}:no_data")
                    continue

                await self._provider_success(provider)
                await self._symbol_success(norm.symbol)
                payload = {
                    "symbol": norm.symbol,
                    "price": round(float(data.get("price", 0.0)), 4),
                    "previous_close": self._safe_float(data.get("previous_close")),
                    "change_1d": self._safe_float(data.get("change_1d")),
                    "change_5d": self._safe_float(data.get("change_5d")),
                    "volume": self._safe_float(data.get("volume")),
                    "meta": {
                        "provider_used": provider,
                        "fallback_chain": fallback_chain,
                        "quality_flags": quality_flags + norm.warnings + data.get("quality_flags", []),
                        "data_completeness": data.get("data_completeness", 1.0),
                        "cache_hit": False,
                        "fetched_at": _utcnow().isoformat(),
                    },
                }
                await self._cache_set(cache_key, payload, _QUOTE_TTL_SECONDS)
                return payload
            except Exception as e:
                err = str(e)
                if "401" in err or "Unauthorized" in err or "Invalid Crumb" in err:
                    code = "AUTH_FAIL"
                else:
                    code = "PROVIDER_ERROR"
                await self._provider_fail(provider, code)
                await self._symbol_fail(norm.symbol)
                fallback_chain.append(f"{provider}:{code}")
                quality_flags.append(code)

        if fail_closed:
            return None

        return {
            "symbol": norm.symbol,
            "price": None,
            "previous_close": None,
            "change_1d": None,
            "change_5d": None,
            "volume": None,
            "meta": {
                "provider_used": None,
                "fallback_chain": fallback_chain,
                "quality_flags": quality_flags + ["NO_DATA"],
                "data_completeness": 0.0,
                "cache_hit": False,
                "fetched_at": _utcnow().isoformat(),
            },
        }

    async def get_ohlcv(
        self,
        symbol: str,
        *,
        start: datetime | None = None,
        end: datetime | None = None,
        interval: str = "1d",
        period: str | None = None,
        min_points: int = 1,
        fail_closed: bool = False,
    ) -> dict[str, Any]:
        norm = self.normalize_symbol(symbol)
        if not norm.valid:
            return {
                "symbol": symbol,
                "rows": [],
                "meta": {
                    "provider_used": None,
                    "fallback_chain": [],
                    "quality_flags": norm.warnings + ["SYMBOL_INVALID"],
                    "data_completeness": 0.0,
                    "cache_hit": False,
                    "fetched_at": _utcnow().isoformat(),
                },
            }

        if await self._symbol_is_quarantined(norm.symbol):
            return {
                "symbol": norm.symbol,
                "rows": [],
                "meta": {
                    "provider_used": None,
                    "fallback_chain": [],
                    "quality_flags": ["SYMBOL_INVALID", "SYMBOL_QUARANTINED"],
                    "data_completeness": 0.0,
                    "cache_hit": False,
                    "fetched_at": _utcnow().isoformat(),
                },
            }

        cache_key = self._cache_key(
            "ohlcv",
            norm.symbol,
            interval,
            period or "",
            start.isoformat() if start else "",
            end.isoformat() if end else "",
        )
        cached = await self._cache_get(cache_key)
        if cached:
            meta = cached.get("meta", {})
            meta["cache_hit"] = True
            cached["meta"] = meta
            return cached

        fallback_chain: list[str] = []
        quality_flags: list[str] = []

        # Intraday provider is intentionally disabled until a non-yfinance
        # intraday source is configured (avoid silent contamination).
        if interval != "1d":
            return {
                "symbol": norm.symbol,
                "rows": [],
                "meta": {
                    "provider_used": None,
                    "fallback_chain": [],
                    "quality_flags": ["INTRADAY_UNSUPPORTED_NO_PROVIDER"],
                    "data_completeness": 0.0,
                    "cache_hit": False,
                    "fetched_at": _utcnow().isoformat(),
                },
            }

        providers = ["finnhub", "stooq", "yfinance"]

        for provider in providers:
            if not await self._provider_available(provider):
                fallback_chain.append(f"{provider}:open_circuit")
                quality_flags.append("PROVIDER_UNAVAILABLE")
                continue
            try:
                if provider == "finnhub":
                    rows = await self._ohlcv_finnhub(norm.symbol, start=start, end=end, interval=interval)
                elif provider == "stooq":
                    rows = await self._ohlcv_stooq(norm.symbol, start=start, end=end, interval=interval, period=period)
                else:
                    rows = await self._ohlcv_yfinance(norm.symbol, start=start, end=end, interval=interval, period=period)

                if not rows:
                    await self._provider_fail(provider, "NO_DATA")
                    await self._symbol_fail(norm.symbol)
                    fallback_chain.append(f"{provider}:no_data")
                    continue

                await self._provider_success(provider)
                await self._symbol_success(norm.symbol)
                completeness = 1.0
                if min_points > 0 and len(rows) < min_points:
                    completeness = len(rows) / float(min_points)
                    quality_flags.append("INCOMPLETE_WINDOW")

                payload = {
                    "symbol": norm.symbol,
                    "rows": rows,
                    "meta": {
                        "provider_used": provider,
                        "fallback_chain": fallback_chain,
                        "quality_flags": quality_flags + norm.warnings,
                        "data_completeness": round(completeness, 4),
                        "cache_hit": False,
                        "fetched_at": _utcnow().isoformat(),
                    },
                }
                await self._cache_set(cache_key, payload, _OHLCV_TTL_SECONDS)
                if fail_closed and "INCOMPLETE_WINDOW" in payload["meta"]["quality_flags"]:
                    payload["rows"] = []
                return payload
            except Exception as e:
                err = str(e)
                if "401" in err or "Unauthorized" in err or "Invalid Crumb" in err:
                    code = "AUTH_FAIL"
                else:
                    code = "PROVIDER_ERROR"
                await self._provider_fail(provider, code)
                await self._symbol_fail(norm.symbol)
                fallback_chain.append(f"{provider}:{code}")
                quality_flags.append(code)

        return {
            "symbol": norm.symbol,
            "rows": [],
            "meta": {
                "provider_used": None,
                "fallback_chain": fallback_chain,
                "quality_flags": quality_flags + ["NO_DATA"],
                "data_completeness": 0.0,
                "cache_hit": False,
                "fetched_at": _utcnow().isoformat(),
            },
        }

    async def get_price_at(
        self,
        symbol: str,
        ts: datetime,
        *,
        tolerance_days: int = 3,
        fail_closed: bool = False,
    ) -> dict[str, Any] | None:
        start = ts - timedelta(days=max(5, tolerance_days + 1))
        end = ts + timedelta(days=max(3, tolerance_days + 1))
        data = await self.get_ohlcv(symbol, start=start, end=end, interval="1d", min_points=1, fail_closed=fail_closed)
        rows = data.get("rows", [])
        if not rows:
            return None

        target_epoch = _to_epoch(ts)
        best = min(rows, key=lambda r: abs(int(r.get("ts", 0)) - target_epoch))

        # If the nearest point is too far, mark incomplete/degraded.
        nearest_delta_days = abs(int(best.get("ts", 0)) - target_epoch) / 86400.0
        meta = data.get("meta", {})
        qf = list(meta.get("quality_flags", []))
        if nearest_delta_days > tolerance_days:
            qf.append("INCOMPLETE_WINDOW")
            meta["quality_flags"] = qf
            if fail_closed:
                return None

        return {
            "symbol": data.get("symbol"),
            "price": self._safe_float(best.get("close")),
            "ts": best.get("ts"),
            "meta": meta,
        }

    async def _quote_finnhub(self, symbol: str) -> dict[str, Any] | None:
        if not self._finnhub_key:
            raise RuntimeError("FINNHUB_KEY_MISSING")

        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get(
                f"{_FINNHUB_BASE}/quote",
                params={"symbol": symbol, "token": self._finnhub_key},
            )
            if resp.status_code != 200:
                raise RuntimeError(f"FINNHUB_HTTP_{resp.status_code}")
            raw = resp.json()

        current = self._safe_float(raw.get("c"))
        prev = self._safe_float(raw.get("pc"))
        if current <= 0:
            return None

        change_1d = ((current - prev) / prev * 100.0) if prev > 0 else 0.0
        return {
            "price": current,
            "previous_close": prev,
            "change_1d": round(change_1d, 4),
            "change_5d": 0.0,
            "volume": 0.0,
            "quality_flags": ["LIMITED_FIELDS"],
            "data_completeness": 0.8,
        }

    async def _quote_stooq(self, symbol: str) -> dict[str, Any] | None:
        stooq_symbol = self._to_stooq_symbol(symbol)
        if not stooq_symbol:
            return None

        async with httpx.AsyncClient(timeout=12) as client:
            resp = await client.get(
                _STOOQ_QUOTE_URL,
                params={"s": stooq_symbol, "f": "sd2t2ohlcv", "h": "1", "e": "csv"},
            )
            resp.raise_for_status()
            text = resp.text.strip()

        reader = csv.DictReader(StringIO(text))
        row = next(reader, None)
        if not row:
            return None

        close = row.get("Close")
        if close in (None, "", "N/D"):
            return None

        price = float(close)
        vol = row.get("Volume") or "0"
        volume = float(vol) if vol not in ("N/D", "", None) else 0.0

        return {
            "price": price,
            "previous_close": price,
            "change_1d": 0.0,
            "change_5d": 0.0,
            "volume": volume,
            "quality_flags": ["LIMITED_FIELDS"],
            "data_completeness": 0.85,
        }

    async def _quote_yfinance(self, symbol: str) -> dict[str, Any] | None:
        def _load() -> dict[str, Any] | None:
            import yfinance as yf

            data = yf.download(
                tickers=symbol,
                period="5d",
                interval="1d",
                progress=False,
                timeout=20,
            )
            if data is None or data.empty:
                return None

            closes = data["Close"].dropna()
            if closes.empty:
                return None

            price = float(closes.iloc[-1])
            prev = float(closes.iloc[-2]) if len(closes) >= 2 else price
            start5 = float(closes.iloc[0]) if len(closes) >= 1 else price
            change_1d = ((price - prev) / prev * 100.0) if prev > 0 else 0.0
            change_5d = ((price - start5) / start5 * 100.0) if start5 > 0 else 0.0

            volume = 0.0
            if "Volume" in data.columns:
                try:
                    volume = float(data["Volume"].dropna().iloc[-1])
                except Exception:
                    volume = 0.0

            return {
                "price": price,
                "previous_close": prev,
                "change_1d": round(change_1d, 4),
                "change_5d": round(change_5d, 4),
                "volume": volume,
                "quality_flags": [],
                "data_completeness": 1.0,
            }

        return await asyncio.to_thread(_load)

    async def _ohlcv_finnhub(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime | None,
        interval: str,
    ) -> list[dict[str, Any]]:
        if not self._finnhub_key:
            raise RuntimeError("FINNHUB_KEY_MISSING")
        if interval != "1d":
            raise RuntimeError("FINNHUB_INTERVAL_UNSUPPORTED")

        now = _utcnow()
        if end is None:
            end = now
        if start is None:
            start = end - timedelta(days=120)

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{_FINNHUB_BASE}/stock/candle",
                params={
                    "symbol": symbol,
                    "resolution": "D",
                    "from": _to_epoch(start),
                    "to": _to_epoch(end),
                    "token": self._finnhub_key,
                },
            )
            if resp.status_code != 200:
                raise RuntimeError(f"FINNHUB_HTTP_{resp.status_code}")
            raw = resp.json()

        if not raw or raw.get("s") != "ok":
            return []

        ts = raw.get("t") or []
        opens = raw.get("o") or []
        highs = raw.get("h") or []
        lows = raw.get("l") or []
        closes = raw.get("c") or []
        vols = raw.get("v") or []

        rows: list[dict[str, Any]] = []
        for i in range(min(len(ts), len(closes))):
            rows.append(
                {
                    "ts": int(ts[i]),
                    "open": self._safe_float(opens[i]),
                    "high": self._safe_float(highs[i]),
                    "low": self._safe_float(lows[i]),
                    "close": self._safe_float(closes[i]),
                    "volume": self._safe_float(vols[i]),
                }
            )
        return rows

    async def _ohlcv_stooq(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime | None,
        interval: str,
        period: str | None,
    ) -> list[dict[str, Any]]:
        if interval != "1d":
            return []
        stooq_symbol = self._to_stooq_symbol(symbol)
        if not stooq_symbol:
            return []

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                _STOOQ_HISTORY_URL,
                params={"s": stooq_symbol, "i": "d"},
            )
            resp.raise_for_status()
            body = resp.text.strip()

        # Stooq history uses semicolon delimiters.
        reader = csv.DictReader(StringIO(body), delimiter=";")
        out: list[dict[str, Any]] = []

        now = _utcnow()
        if end is None:
            end = now
        if start is None and period:
            if period.endswith("mo"):
                months = int(period[:-2])
                start = end - timedelta(days=31 * months)
            elif period.endswith("y"):
                years = int(period[:-1])
                start = end - timedelta(days=366 * years)
            elif period.endswith("d"):
                days = int(period[:-1])
                start = end - timedelta(days=days)
        if start is None:
            start = end - timedelta(days=120)

        for row in reader:
            d = row.get("Date")
            if not d:
                continue
            try:
                dt = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except Exception:
                continue
            if dt < start or dt > end:
                continue

            c = row.get("Close")
            if c in (None, "", "N/D"):
                continue

            out.append(
                {
                    "ts": _to_epoch(dt),
                    "open": self._safe_float(row.get("Open")),
                    "high": self._safe_float(row.get("High")),
                    "low": self._safe_float(row.get("Low")),
                    "close": self._safe_float(c),
                    "volume": self._safe_float(row.get("Volume")),
                }
            )

        return out

    async def _ohlcv_yfinance(
        self,
        symbol: str,
        *,
        start: datetime | None,
        end: datetime | None,
        interval: str,
        period: str | None,
    ) -> list[dict[str, Any]]:
        def _load() -> list[dict[str, Any]]:
            import yfinance as yf

            kwargs: dict[str, Any] = {
                "tickers": symbol,
                "interval": interval,
                "progress": False,
                "timeout": 30,
            }
            if start is not None and end is not None:
                kwargs["start"] = start.strftime("%Y-%m-%d")
                kwargs["end"] = end.strftime("%Y-%m-%d")
            else:
                kwargs["period"] = period or "3mo"

            data = yf.download(**kwargs)
            if data is None or data.empty:
                return []

            rows: list[dict[str, Any]] = []
            for idx, r in data.iterrows():
                try:
                    dt = idx.to_pydatetime()
                except Exception:
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                rows.append(
                    {
                        "ts": _to_epoch(dt),
                        "open": self._safe_float(r.get("Open")),
                        "high": self._safe_float(r.get("High")),
                        "low": self._safe_float(r.get("Low")),
                        "close": self._safe_float(r.get("Close")),
                        "volume": self._safe_float(r.get("Volume")),
                    }
                )
            return rows

        return await asyncio.to_thread(_load)

    def _to_stooq_symbol(self, symbol: str) -> str | None:
        s = (symbol or "").strip().lower()
        if not s:
            return None

        # Stooq equity/ETF convention.
        if s.startswith("^"):
            return None
        if s.endswith("-usd") or s.endswith("=f"):
            return None
        return f"{s}.us"

    async def _provider_available(self, provider: str) -> bool:
        state = await self._get_cb_state(provider)
        now = time.time()
        return float(state.get("open_until", 0)) <= now

    async def _provider_fail(self, provider: str, code: str) -> None:
        state = await self._get_cb_state(provider)
        fail_count = int(state.get("fail_count", 0))
        now = time.time()
        open_until = float(state.get("open_until", 0))

        # Only authentication/systemic failures should open provider-level cooldowns.
        # Per-symbol NO_DATA/PROVIDER_ERROR are common and should not poison all symbols.
        if code == "AUTH_FAIL":
            fail_count += 1
            if fail_count >= _CB_FAIL_THRESHOLD:
                open_until = now + _CB_COOLDOWN_SECONDS
                fail_count = 0
        else:
            # Keep provider available for other symbols.
            fail_count = 0
            open_until = 0

        key = f"alekfi:md:cb:{provider}"
        await self._redis.hset(
            key,
            mapping={
                "fail_count": fail_count,
                "open_until": open_until,
                "last_error": code,
                "last_error_at": _utcnow().isoformat(),
            },
        )
        await self._redis.expire(key, 86400)

    async def _provider_success(self, provider: str) -> None:
        key = f"alekfi:md:cb:{provider}"
        await self._redis.hset(
            key,
            mapping={
                "fail_count": 0,
                "open_until": 0,
                "last_error": "",
            },
        )
        await self._redis.expire(key, 86400)

    async def _get_cb_state(self, provider: str) -> dict[str, Any]:
        key = f"alekfi:md:cb:{provider}"
        raw = await self._redis.hgetall(key)
        return raw or {}

    async def _symbol_is_quarantined(self, symbol: str) -> bool:
        raw = await self._redis.get(f"alekfi:md:symbol_fail:{symbol}")
        try:
            return raw is not None and int(raw) >= _SYMBOL_FAIL_THRESHOLD
        except Exception:
            return False

    async def _symbol_fail(self, symbol: str) -> None:
        key = f"alekfi:md:symbol_fail:{symbol}"
        try:
            v = await self._redis.incr(key)
            if v == 1:
                await self._redis.expire(key, _SYMBOL_FAIL_TTL_SECONDS)
        except Exception:
            pass

    async def _symbol_success(self, symbol: str) -> None:
        try:
            await self._redis.delete(f"alekfi:md:symbol_fail:{symbol}")
        except Exception:
            pass

    def _cache_key(self, kind: str, symbol: str, *parts: str) -> str:
        suffix = ":".join(p.replace(" ", "") for p in parts if p is not None)
        if suffix:
            return f"alekfi:md:cache:{kind}:{symbol}:{suffix}"
        return f"alekfi:md:cache:{kind}:{symbol}"

    async def _cache_get(self, key: str) -> dict[str, Any] | None:
        try:
            raw = await self._redis.get(key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None

    async def _cache_set(self, key: str, payload: dict[str, Any], ttl: int) -> None:
        try:
            await self._redis.setex(key, ttl, json.dumps(payload, default=str))
        except Exception:
            pass

    @staticmethod
    def _atr14_from_rows(rows: list[dict[str, Any]]) -> float | None:
        if not rows or len(rows) < 15:
            return None
        trs: list[float] = []
        prev_close = None
        for r in rows[-60:]:
            high = MarketDataGateway._safe_float(r.get("high"), default=None)
            low = MarketDataGateway._safe_float(r.get("low"), default=None)
            close = MarketDataGateway._safe_float(r.get("close"), default=None)
            if high is None or low is None:
                continue
            if prev_close is None:
                tr = high - low
            else:
                tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            if tr >= 0:
                trs.append(float(tr))
            prev_close = close if close is not None else prev_close
        if len(trs) < 14:
            return None
        return sum(trs[-14:]) / 14.0

    @staticmethod
    def _spread_bps_est(*, price: float | None, dollar_volume: float | None) -> float:
        if price is None or price <= 0:
            return 60.0
        if dollar_volume is None:
            return 40.0
        if dollar_volume >= 250_000_000:
            return 2.0
        if dollar_volume >= 50_000_000:
            return 5.0
        if dollar_volume >= 10_000_000:
            return 12.0
        if dollar_volume >= 3_000_000:
            return 25.0
        return 60.0

    @staticmethod
    def _safe_float(v: Any, default: float = 0.0) -> float:
        try:
            if v is None:
                return default
            return float(v)
        except Exception:
            return default
