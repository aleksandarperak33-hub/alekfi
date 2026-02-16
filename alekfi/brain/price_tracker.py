"""PriceTracker — fetches live prices for signal validation and accuracy tracking."""

from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import select

from alekfi.config import Settings
from alekfi.db.database import get_session
from alekfi.db.models import Signal, SignalFeedback, SignalOutcome, PriceData
from alekfi.marketdata import MarketDataGateway

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────
_REDIS_PREFIX = "alekfi:price:"
_REDIS_TTL = 3600  # 1 hour
_BATCH_SIZE = 20
_MARKET_OPEN_UTC = 14 * 60 + 30   # 14:30 UTC = 9:30 ET
_MARKET_CLOSE_UTC = 21 * 60       # 21:00 UTC = 16:00 ET
_POLL_MARKET_HOURS = 5 * 60       # 5 minutes
_POLL_OFF_HOURS = 30 * 60         # 30 minutes

# ── Ticker validation ─────────────────────────────────────────────────
# Common words the entity extractor mistakes for tickers
_NOT_TICKERS = {
    # Acronyms / titles
    "AI", "CEO", "CFO", "CTO", "COO", "CIO", "IPO", "ETF", "GDP", "CPI",
    "FED", "SEC", "FDA", "EPA", "DOJ", "DOD", "NATO", "OPEC", "IMF",
    "UN", "EU", "UK", "US", "USA", "UAE", "WHO", "WTO", "ECB", "BOJ",
    # Currencies (not equity tickers)
    "USD", "EUR", "GBP", "JPY", "CNY", "CHF", "CAD", "AUD", "NZD",
    "BTC", "ETH", "USDT", "USDC", "SOL", "XRP",
    # Market jargon
    "MARKETS", "MARKET", "STOCK", "STOCKS", "CRYPTO", "BOND", "BONDS",
    "BULL", "BEAR", "LONG", "SHORT", "BUY", "SELL", "HOLD", "CALL", "PUT",
    "PREDICTION", "CAPITAL", "GROWTH", "VALUE", "RISK", "TRADE", "TRADING",
    "EARNINGS", "REVENUE", "PROFIT", "LOSS", "DEBT", "CASH", "RATE",
    "RATES", "INFLATION", "RECESSION", "RALLY", "CRASH", "DIP", "PUMP",
    "DUMP", "MOON", "HODL", "FOMO", "FUD", "YOLO", "DD", "TA", "FA",
    "MACD", "RSI", "EMA", "SMA", "ATH", "ATL", "ROI", "PE", "EPS",
    "YOY", "QOQ", "MOM", "TTM", "EBITDA", "FCF", "NAV", "AUM",
    # Fake tickers from entity extractor
    "POSTMAN", "APIDOG", "GOLUB", "FIRMUS", "NATWEST", "NIKKEI",
    "HYPERLIQUID", "INFINI", "VIX", "BDNCE", "DXY", "IRN", "NKY",
    "RDFN", "ZB", "ZI",  # Delisted/invalid tickers
    # Common words mistaken for tickers
    "THE", "AND", "FOR", "NOT", "ARE", "BUT", "ALL", "NEW", "OLD",
    "TOP", "LOW", "HIGH", "BIG", "OIL", "GAS", "WAR", "TAX", "LAW",
    "API", "APP", "WEB", "DNA", "RNA", "HIV", "ICE", "FTC",
    "FCC", "IRS", "FBI", "CIA", "NSA", "DEA", "TSA", "CBP",
    # Country codes / non-equity
    "RUS", "CHN", "IND", "BRA", "MEX", "KOR", "TWN", "SAU",
    # Extended: index/futures, foreign, false positives, delisted (auto-added)
    "AAVE", "ADA", "AMES", "ATOM", "AVAX", "BANK", "BNB", "BOO.L",
    "BTC.X", "CC", "CL", "CORP", "CS", "CT", "DBS", "DOGE",
    "DOT", "EDBI", "EQUITY", "ES", "EURUSD", "FUND", "FUTURES", "GBPUSD",
    "GC", "GLOBAL", "GROUP", "HG", "HOLDINGS", "HWAEF", "HYMTF", "INC",
    "INDEX", "INFRA", "JCP", "JJN", "KC", "LINK", "LLC", "LSEG",
    "LTD", "LUPIN", "MATIC", "MSE", "NCDEX", "NDX", "NG", "NGFHF",
    "NQ", "OPTIONS", "PA", "PL", "PLC", "RTY", "SB", "SHARES",
    "SHIB", "SI", "SPACE", "SPX", "TRUMP.X", "TRUST", "TWE", "UNI",
    "USDCHF", "USDCNY", "USDJPY", "XTB", "YM", "ZF", "ZN", "ZT",
    # Additional false positives (auto-added v2)
    "ABOUT", "ALSO", "ASX", "AUDCAD", "AUDJPY", "BOVESPA", "CHAT", "CLOSE", "CODE", "CORE", "COULD", "DATA", "DAX", "DEAL", "EACH", "EDGE", "EURCAD", "EURJPY", "EVERY", "FAST", "FIRM", "FLOW", "FREE", "FTSE", "FULL", "GBPJPY", "GOOD", "HALF", "HANG", "IBOV", "JUST", "KOSPI", "LAST", "LIKE", "LIVE", "MOST", "MOVE", "MUCH", "NEXT", "NZDUSD", "OPEN", "OTHER", "PARA", "PART", "PLAN", "PLAY", "REAL", "SAFE", "SAME", "SENG", "SENSEX", "SQ", "STILL", "STOXX", "TEAM", "TECH", "TERM", "TOPIX", "TOTAL", "TSX", "UNIT", "VERY", "WIRE", "WOULD",

}

# Ticker rename mapping (companies that changed ticker symbols)
_TICKER_RENAMES = {
    "SQ": "XYZ",      # Block Inc renamed from SQ to XYZ
    "FB": "META",      # Facebook renamed to Meta
    "TWTR": "X",       # Twitter (delisted but mapped)
}

# Regex: valid tickers are 1-5 uppercase letters, optionally with a dot (BRK.B)
_TICKER_RE = re.compile(r"^[A-Z]{1,5}(\.[AB])?$")


def _is_valid_ticker(symbol: str) -> bool:
    """Check if a symbol looks like a real US equity ticker."""
    if not symbol or len(symbol) > 6:
        return False
    s = symbol.upper().strip()
    if s in _NOT_TICKERS:
        return False
    if not _TICKER_RE.match(s):
        return False
    # Reject single-character "tickers" (too ambiguous)
    if len(s) == 1:
        return False
    # Reject if it looks like a currency pair (6+ chars, all letters)
    if len(s) >= 6 and s.isalpha():
        return False
    # Reject symbols with periods in content (like "U.S.")
    if "." in symbol and not re.match(r"^[A-Z]{1,4}\.[AB]$", symbol):
        return False
    return True


def _resolve_ticker(symbol: str) -> str:
    """Resolve renamed tickers to their current symbol."""
    return _TICKER_RENAMES.get(symbol.upper(), symbol.upper())


_SIGNAL_LOOKBACK_HOURS = 48
_VALIDATION_MIN_AGE_HOURS = 24


def _is_market_hours() -> bool:
    """Check if current UTC time is within US market hours (14:30-21:00 UTC)."""
    now = datetime.now(timezone.utc)
    minutes_since_midnight = now.hour * 60 + now.minute
    weekday = now.weekday()
    # Monday=0 ... Friday=4; weekend = no market hours
    if weekday >= 5:
        return False
    return _MARKET_OPEN_UTC <= minutes_since_midnight < _MARKET_CLOSE_UTC


class PriceTracker:
    """Fetches live prices via market-data gateway, caches in Redis, and validates signal accuracy.

    The tracker runs as a continuous async loop:
    - During market hours (9:30-16:00 ET / 14:30-21:00 UTC): polls every 5 minutes
    - Outside market hours: polls every 30 minutes
    - Fetches prices for all tickers referenced in signals from the last 48 hours
    - Stores price data in Redis with 1-hour TTL
    - Periodically validates signals older than 24h against actual price moves
    """

    def __init__(self, config: Settings) -> None:
        self._config = config
        self._redis: aioredis.Redis = aioredis.from_url(
            config.redis_url, decode_responses=True
        )
        self._gateway = MarketDataGateway(redis_client=self._redis)

        # Stats
        self.prices_fetched = 0
        self.validations_run = 0
        self.signals_validated = 0
        self.last_fetch_at: str | None = None
        self.last_validation_at: str | None = None

    # ── Core: fetch prices ────────────────────────────────────────────

    async def fetch_prices(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch current prices for a list of tickers via MarketDataGateway and store in Redis.

        Fetches in batches to avoid API bursts and preserve deterministic fallback behavior.
        Returns a dict of {ticker: price_data} for successfully fetched tickers.
        """
        if not tickers:
            return {}

        results: dict[str, dict[str, Any]] = {}

        # Process in batches
        for i in range(0, len(tickers), _BATCH_SIZE):
            batch = tickers[i : i + _BATCH_SIZE]
            batch_results = await self._fetch_batch(batch)
            results.update(batch_results)

            # Small delay between batches to be respectful
            if i + _BATCH_SIZE < len(tickers):
                await asyncio.sleep(1)

        self.prices_fetched += len(results)
        self.last_fetch_at = datetime.now(timezone.utc).isoformat()

        # Record failures for quarantine (only for batch fetches, not single-ticker validation)
        if len(tickers) > 1:
            failed = set(tickers) - set(results.keys())
            for ft in failed:
                await self._record_ticker_failure(ft)

        logger.info(
            "[price_tracker] fetched %d/%d tickers successfully",
            len(results),
            len(tickers),
        )
        return results

    async def _fetch_batch(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch a single batch of tickers via the canonical gateway and store to Redis."""
        results: dict[str, dict[str, Any]] = {}
        now_iso = datetime.now(timezone.utc).isoformat()
        quotes = await self._gateway.get_quotes_batch(tickers, fail_closed=False, concurrency=6)
        for ticker in tickers:
            q = quotes.get(ticker)
            if not q:
                continue
            price = q.get("price")
            if price is None:
                await self._record_ticker_failure(ticker)
                continue

            meta = q.get("meta") or {}
            price_data: dict[str, Any] = {
                "price": round(float(price), 2),
                "timestamp": now_iso,
                "change_1d": float(q.get("change_1d") or 0.0),
                "change_5d": float(q.get("change_5d") or 0.0),
                "volume": float(q.get("volume") or 0.0),
                "ticker": ticker,
                "source": meta.get("provider_used"),
                "quality_flags": meta.get("quality_flags", []),
                "provider_fallback_chain": meta.get("fallback_chain", []),
                "data_completeness": meta.get("data_completeness"),
            }

            await self._redis.setex(f"{_REDIS_PREFIX}{ticker}", _REDIS_TTL, json.dumps(price_data))

            try:
                async with get_session() as db_session:
                    db_session.add(
                        PriceData(
                            symbol=ticker,
                            price=float(price_data["price"]),
                            volume=float(price_data["volume"]) if price_data.get("volume") else None,
                            change_1h=None,
                            change_24h=float(price_data["change_1d"]),
                            source=str(price_data.get("source") or "gateway"),
                        )
                    )
            except Exception:
                logger.debug("[price_tracker] PriceData persist failed for %s", ticker, exc_info=True)

            results[ticker] = price_data

        return results

    # ── Signal validation ─────────────────────────────────────────────

    async def validate_signals(self) -> list[dict[str, Any]]:
        """Validate signals older than 24h by checking if predicted direction matched actual price move.

        For each qualifying signal:
        - LONG + price went up => accurate=True
        - SHORT + price went down => accurate=True
        - Otherwise => accurate=False

        Creates SignalFeedback records for validated signals.
        Returns list of validation results.
        """
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=_VALIDATION_MIN_AGE_HOURS)
        lookback = now - timedelta(hours=_SIGNAL_LOOKBACK_HOURS * 2)  # go back further for validation

        validations: list[dict[str, Any]] = []

        async with get_session() as session:
            # Find signals that are old enough to validate and don't already have feedback
            stmt = (
                select(Signal)
                .where(Signal.created_at <= cutoff)
                .where(Signal.created_at >= lookback)
                .order_by(Signal.created_at.desc())
                .limit(50)
            )
            result = await session.execute(stmt)
            signals = result.scalars().all()

        if not signals:
            logger.debug("[price_tracker] no signals to validate")
            return validations

        # Collect all tickers from signals for batch price fetching
        ticker_set: set[str] = set()
        for sig in signals:
            instruments = sig.affected_instruments or []
            for inst in instruments:
                symbol = inst.get("symbol", "")
                if symbol and symbol != "N/A" and "/" not in symbol and _is_valid_ticker(symbol):
                    ticker_set.add(_resolve_ticker(symbol))

        # Fetch current prices for all relevant tickers
        if ticker_set:
            await self.fetch_prices(list(ticker_set))

        # Validate each signal
        for sig in signals:
            # Check if feedback already exists
            async with get_session() as session:
                existing_fb = (await session.execute(
                    select(SignalFeedback).where(SignalFeedback.signal_id == sig.id).limit(1)
                )).scalar_one_or_none()

            if existing_fb is not None:
                continue  # Already validated

            instruments = sig.affected_instruments or []
            if not instruments:
                continue

            # Get the primary instrument
            primary_symbol = instruments[0].get("symbol", "")
            if (
                not primary_symbol
                or primary_symbol == "N/A"
                or "/" in primary_symbol
                or not _is_valid_ticker(primary_symbol)
            ):
                continue
            primary_symbol = _resolve_ticker(primary_symbol)

            # Get current price from Redis
            price_data = await self._get_cached_price(primary_symbol)
            if price_data is None:
                continue

            # Determine if direction was correct
            direction = sig.direction.upper()
            change_1d = price_data.get("change_1d", 0.0)

            if direction == "LONG":
                was_accurate = change_1d > 0
            elif direction == "SHORT":
                was_accurate = change_1d < 0
            elif direction == "HEDGE":
                # Hedge signals are considered accurate if abs move > 1%
                was_accurate = abs(change_1d) > 1.0
            else:
                was_accurate = False

            # Store validation result
            validation_result: dict[str, Any] = {
                "signal_id": str(sig.id),
                "ticker": primary_symbol,
                "direction": direction,
                "conviction": sig.conviction,
                "signal_type": sig.signal_type,
                "price_at_validation": price_data.get("price"),
                "change_1d": change_1d,
                "change_5d": price_data.get("change_5d", 0.0),
                "was_accurate": was_accurate,
                "validated_at": now.isoformat(),
            }

            # Create SignalFeedback record
            async with get_session() as session:
                feedback = SignalFeedback(
                    id=uuid.uuid4(),
                    signal_id=sig.id,
                    was_accurate=was_accurate,
                    feedback_type="auto_price_validation",
                    notes=json.dumps({
                        "ticker": primary_symbol,
                        "change_1d": change_1d,
                        "change_5d": price_data.get("change_5d", 0.0),
                        "price": price_data.get("price"),
                        "direction": direction,
                        "method": "price_tracker_auto",
                    }),
                )
                session.add(feedback)

            validations.append(validation_result)
            self.signals_validated += 1

        self.validations_run += 1
        self.last_validation_at = now.isoformat()

        accurate_count = sum(1 for v in validations if v["was_accurate"])
        logger.info(
            "[price_tracker] validated %d signals: %d accurate, %d inaccurate (%.0f%%)",
            len(validations),
            accurate_count,
            len(validations) - accurate_count,
            (accurate_count / max(len(validations), 1)) * 100,
        )
        return validations

    # ── Signal Outcome creation ──────────────────────────────────────

    async def _create_signal_outcomes(self) -> int:
        """Create SignalOutcome records for new signals that don't have one yet."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=2)  # Signals at least 2h old
        lookback = datetime.now(timezone.utc) - timedelta(hours=48)
        created = 0

        async with get_session() as session:
            # Find signals without outcomes
            stmt = select(Signal).where(
                Signal.created_at >= lookback,
                Signal.created_at <= cutoff,
            ).limit(50)
            result = await session.execute(stmt)
            signals = result.scalars().all()

        for sig in signals:
            # Check if outcome already exists
            async with get_session() as session:
                existing = (await session.execute(
                    select(SignalOutcome).where(SignalOutcome.signal_id == sig.id).limit(1)
                )).scalar_one_or_none()

            if existing:
                continue

            instruments = sig.affected_instruments or []
            tickers = []
            for inst in instruments:
                symbol = (inst or {}).get("symbol", "")
                if (
                    symbol
                    and symbol != "N/A"
                    and "/" not in symbol
                    and _is_valid_ticker(symbol)
                ):
                    tickers.append(_resolve_ticker(symbol))
            # Always include SPY for a simple market baseline when computing abnormal returns.
            if "SPY" not in tickers:
                tickers.append("SPY")
            if not tickers:
                continue

            # Fetch creation-time prices
            price_at_creation = {}
            for ticker in tickers:
                cached = await self._get_cached_price(ticker)
                if cached:
                    price_at_creation[ticker] = cached.get("price", 0)

            if not price_at_creation:
                continue

            meta = sig.metadata_ or {}
            source_platforms = []
            for sp in (sig.source_posts or []):
                if isinstance(sp, dict):
                    source_platforms.append(sp.get("platform", "unknown"))

            async with get_session() as session:
                outcome = SignalOutcome(
                    id=uuid.uuid4(),
                    signal_id=sig.id,
                    instruments=tickers,
                    predicted_direction=sig.direction,
                    intelligence_score=meta.get("total_intelligence_score", meta.get("priority_score")),
                    signal_type=sig.signal_type,
                    exclusivity_edge=meta.get("exclusivity_edge"),
                    source_platforms=source_platforms,
                    actionability=meta.get("actionability"),
                    price_at_creation=price_at_creation,
                )
                session.add(outcome)
                created += 1

        if created:
            logger.info("[price_tracker] created %d signal outcomes", created)
        return created

    # ── Multi-checkpoint validation ────────────────────────────────────

    async def validate_signal_outcomes(self) -> int:
        """Check signal outcomes at 1h, 4h, 24h, 48h, 7d checkpoints."""
        now = datetime.now(timezone.utc)
        checkpoints = [
            ("1h", timedelta(hours=1), "price_at_1h", "correct_at_1h"),
            ("4h", timedelta(hours=4), "price_at_4h", "correct_at_4h"),
            ("24h", timedelta(hours=24), "price_at_24h", "correct_at_24h"),
            ("48h", timedelta(hours=48), "price_at_48h", "correct_at_48h"),
            ("7d", timedelta(days=7), "price_at_7d", "correct_at_7d"),
        ]

        validated = 0
        for label, delta, price_col, correct_col in checkpoints:
            async with get_session() as session:
                # Find outcomes where this checkpoint is due but not yet filled
                stmt = (
                    select(SignalOutcome)
                    .where(
                        getattr(SignalOutcome, price_col) == None,  # noqa: E711
                        SignalOutcome.created_at <= now - delta,
                    )
                    .limit(50)
                )
                result = await session.execute(stmt)
                outcomes = result.scalars().all()

            for outcome in outcomes:
                tickers_raw = outcome.instruments or []
                tickers = []
                if isinstance(tickers_raw, dict):
                    tickers_raw = list(tickers_raw)
                for t in tickers_raw:
                    if isinstance(t, str) and _is_valid_ticker(t):
                        tickers.append(_resolve_ticker(t))

                prices = {}
                quality_flags_by_ticker: dict[str, list[str]] = {}
                for ticker in tickers:
                    cached = await self._get_cached_price(ticker)
                    if cached:
                        prices[ticker] = cached.get("price", 0)
                        quality_flags_by_ticker[ticker] = list(cached.get("quality_flags") or [])
                    else:
                        # Try fetching fresh
                        fetched = await self.fetch_prices([ticker])
                        if ticker in fetched:
                            prices[ticker] = fetched[ticker].get("price", 0)
                            quality_flags_by_ticker[ticker] = list(fetched[ticker].get("quality_flags") or [])

                if not prices:
                    continue

                # Determine correctness
                correct = self._evaluate_direction(
                    outcome.predicted_direction or "LONG",
                    outcome.price_at_creation or {},
                    prices,
                )

                # Update max favorable/adverse moves
                max_fav = outcome.max_favorable_move_pct or 0.0
                max_adv = outcome.max_adverse_move_pct or 0.0
                for ticker in (outcome.price_at_creation or {}):
                    if ticker in prices:
                        then_price = outcome.price_at_creation[ticker]
                        now_price = prices[ticker]
                        if then_price and then_price > 0:
                            pct = (now_price - then_price) / then_price * 100
                            direction = (outcome.predicted_direction or "LONG").upper()
                            if direction == "LONG":
                                if pct > 0:
                                    max_fav = max(max_fav, pct)
                                else:
                                    max_adv = max(max_adv, abs(pct))
                            elif direction == "SHORT":
                                if pct < 0:
                                    max_fav = max(max_fav, abs(pct))
                                else:
                                    max_adv = max(max_adv, pct)


                labels = (outcome.labels or {}) if hasattr(outcome, "labels") else {}
                labels["updated_at"] = now.isoformat()

                degraded = False
                degraded_reason = None
                for _t, _flags in quality_flags_by_ticker.items():
                    upper = {str(f).upper() for f in (_flags or [])}
                    for bad in ("AUTH_FAIL", "SYMBOL_INVALID", "INCOMPLETE_WINDOW"):
                        if bad in upper:
                            degraded = True
                            degraded_reason = bad
                            break
                    if degraded:
                        break

                if degraded:
                    labels["label_skipped_reason"] = degraded_reason or "DEGRADED_MARKET_DATA"
                    labels["labeling_version"] = "v2_fail_closed"
                    labels["label_confidence"] = 0.0
                    labels["quality_flags"] = quality_flags_by_ticker
                else:
                    # Ground-truth labels for this checkpoint: returns, abnormal returns vs SPY, and timing hints.
                    base_prices = outcome.price_at_creation or {}
                    returns_pct = {}
                    for t, then_price in base_prices.items():
                        try:
                            now_price = prices.get(t)
                            if then_price and then_price > 0 and now_price is not None:
                                returns_pct[t] = round(((now_price - then_price) / then_price) * 100.0, 3)
                        except Exception:
                            continue

                    spy_ret = returns_pct.get("SPY")
                    abnormal_pct = {}
                    for t, r in returns_pct.items():
                        if t == "SPY" or spy_ret is None:
                            continue
                        abnormal_pct[t] = round(r - spy_ret, 3)

                    non_spy = [t for t in returns_pct.keys() if t != "SPY"]
                    avg_ret = round(sum(returns_pct[t] for t in non_spy) / max(len(non_spy), 1), 3) if non_spy else None
                    avg_abn = round(sum(abnormal_pct.values()) / max(len(abnormal_pct), 1), 3) if abnormal_pct else None

                    dir_label = None
                    if avg_abn is not None:
                        if avg_abn > 0.5:
                            dir_label = "up"
                        elif avg_abn < -0.5:
                            dir_label = "down"
                        else:
                            dir_label = "flat"

                    cps = labels.get("checkpoints") or {}
                    cps[label] = {
                        "returns_pct": returns_pct,
                        "spy_return_pct": spy_ret,
                        "abnormal_returns_pct": abnormal_pct,
                        "avg_return_pct": avg_ret,
                        "avg_abnormal_return_pct": avg_abn,
                        "direction_label": dir_label,
                        "filled_at": now.isoformat(),
                    }
                    labels["checkpoints"] = cps

                    if labels.get("time_to_move") is None and avg_abn is not None and abs(avg_abn) >= 1.0:
                        labels["time_to_move"] = label
                    labels["labeling_version"] = "v2_fail_closed"
                    labels["label_confidence"] = 1.0
                    labels["price_source"] = {k: (quality_flags_by_ticker.get(k) or []) for k in prices.keys()}

                async with get_session() as session:
                    from sqlalchemy import update as sql_update
                    await session.execute(
                        sql_update(SignalOutcome)
                        .where(SignalOutcome.id == outcome.id)
                        .values(**{
                            price_col: prices,
                            correct_col: correct,
                            "max_favorable_move_pct": round(max_fav, 2),
                            "max_adverse_move_pct": round(max_adv, 2),
                            "labels": labels,
                        })
                    )
                validated += 1

        if validated:
            logger.info("[price_tracker] validated %d signal outcome checkpoints", validated)
        return validated

    @staticmethod
    def _evaluate_direction(direction: str, prices_then: dict, prices_now: dict) -> bool | None:
        """Did the price move in the predicted direction?"""
        moves = []
        for ticker in prices_then:
            if ticker in prices_now and prices_then[ticker] and prices_then[ticker] > 0:
                then = prices_then[ticker]
                now = prices_now[ticker]
                pct_change = (now - then) / then * 100
                if direction.upper() == "LONG":
                    moves.append(pct_change > 0.5)
                elif direction.upper() == "SHORT":
                    moves.append(pct_change < -0.5)
                else:  # HEDGE
                    moves.append(abs(pct_change) < 2.0)
        return all(moves) if moves else None

    # ── Price context for dashboard ───────────────────────────────────

    async def get_price_context(self, tickers: list[str]) -> dict[str, dict[str, Any]]:
        """Return cached price data for a list of tickers.

        Used by the dashboard API to enrich signal displays with current prices.
        Falls back to fetching fresh data if not cached.

        Returns:
            Dict mapping ticker to {price, change_1d, change_5d, volume, timestamp}
        """
        result: dict[str, dict[str, Any]] = {}
        missing: list[str] = []

        for ticker in tickers:
            cached = await self._get_cached_price(ticker)
            if cached is not None:
                result[ticker] = {
                    "price": cached.get("price"),
                    "change_1d": cached.get("change_1d", 0.0),
                    "change_5d": cached.get("change_5d", 0.0),
                    "volume": cached.get("volume", 0.0),
                    "timestamp": cached.get("timestamp"),
                }
            else:
                missing.append(ticker)

        # Fetch any missing tickers
        if missing:
            fetched = await self.fetch_prices(missing)
            for ticker, data in fetched.items():
                result[ticker] = {
                    "price": data.get("price"),
                    "change_1d": data.get("change_1d", 0.0),
                    "change_5d": data.get("change_5d", 0.0),
                    "volume": data.get("volume", 0.0),
                    "timestamp": data.get("timestamp"),
                }

        return result

    # ── Redis helpers ─────────────────────────────────────────────────

    async def _get_cached_price(self, ticker: str) -> dict[str, Any] | None:
        """Get cached price data from Redis."""
        try:
            raw = await self._redis.get(f"{_REDIS_PREFIX}{ticker}")
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug("[price_tracker] Redis read failed for %s", ticker)
        return None

    # ── Ticker quarantine (avoid re-fetching known-bad tickers) ──────

    _QUARANTINE_PREFIX = "alekfi:ticker_quarantine:"
    _QUARANTINE_TTL = 86400      # 24 hours
    _QUARANTINE_THRESHOLD = 3    # failures before quarantine

    async def _is_quarantined(self, ticker: str) -> bool:
        """Check if ticker is quarantined due to repeated yfinance failures."""
        try:
            val = await self._redis.get(f"{self._QUARANTINE_PREFIX}{ticker}")
            if val and int(val) >= self._QUARANTINE_THRESHOLD:
                return True
        except Exception:
            pass
        return False

    async def _record_ticker_failure(self, ticker: str) -> None:
        """Increment failure count for a ticker; quarantine after threshold."""
        try:
            key = f"{self._QUARANTINE_PREFIX}{ticker}"
            count = await self._redis.incr(key)
            if count == 1:
                await self._redis.expire(key, self._QUARANTINE_TTL)
            if count == self._QUARANTINE_THRESHOLD:
                logger.info("[price_tracker] quarantined ticker %s for 24h after %d failures", ticker, count)
        except Exception:
            pass

    async def _filter_quarantined(self, tickers: list[str]) -> list[str]:
        """Remove quarantined tickers from list."""
        if not tickers:
            return tickers
        clean = []
        quarantined = []
        for t in tickers:
            if await self._is_quarantined(t):
                quarantined.append(t)
            else:
                clean.append(t)
        if quarantined:
            logger.debug("[price_tracker] skipping %d quarantined tickers", len(quarantined))
        return clean


    # ── Ticker discovery ──────────────────────────────────────────────

    async def _get_active_tickers(self) -> list[str]:
        """Get all unique tickers from signals created in the last 48 hours."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=_SIGNAL_LOOKBACK_HOURS)
        ticker_set: set[str] = set()

        async with get_session() as session:
            stmt = (
                select(Signal.affected_instruments)
                .where(Signal.created_at >= cutoff)
            )
            result = await session.execute(stmt)
            rows = result.scalars().all()

        for instruments in rows:
            if not instruments:
                continue
            for inst in instruments:
                symbol = inst.get("symbol", "")
                if symbol and symbol != "N/A" and "/" not in symbol and _is_valid_ticker(symbol):
                    ticker_set.add(_resolve_ticker(symbol))

        return sorted(ticker_set)

    # ── Main run loop ─────────────────────────────────────────────────

    async def run(self) -> None:
        """Continuous price tracking loop.

        - Every 5 minutes during market hours (14:30-21:00 UTC)
        - Every 30 minutes outside market hours
        - Validates signals on every cycle
        """
        logger.info("[price_tracker] starting continuous price tracking loop")

        while True:
            try:
                # Discover active tickers
                tickers = await self._get_active_tickers()

                if tickers:
                    # Filter quarantined tickers before fetching
                    tickers = await self._filter_quarantined(tickers)
                    logger.info("[price_tracker] tracking %d active tickers", len(tickers))
                    await self.fetch_prices(tickers)
                else:
                    logger.debug("[price_tracker] no active tickers to track")

                # Run signal validation (legacy feedback)
                await self.validate_signals()

                # Create signal outcomes for new signals
                await self._create_signal_outcomes()

                # Multi-checkpoint validation
                await self.validate_signal_outcomes()

            except Exception:
                logger.warning("[price_tracker] cycle error", exc_info=True)

            # Determine sleep interval
            if _is_market_hours():
                sleep_seconds = _POLL_MARKET_HOURS
                logger.debug("[price_tracker] market hours — sleeping %ds", sleep_seconds)
            else:
                sleep_seconds = _POLL_OFF_HOURS
                logger.debug("[price_tracker] off hours — sleeping %ds", sleep_seconds)

            await asyncio.sleep(sleep_seconds)

    # ── Stats ─────────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Return tracker statistics."""
        return {
            "prices_fetched": self.prices_fetched,
            "validations_run": self.validations_run,
            "signals_validated": self.signals_validated,
            "last_fetch_at": self.last_fetch_at,
            "last_validation_at": self.last_validation_at,
            "is_market_hours": _is_market_hours(),
        }
