"""SignalDecayWorker — fills price checkpoints (t1, t3, t7, t30) and sets outcome for signals.

Runs every 10 minutes. For each signal missing price checkpoints, fetches
the current price via MarketDataGateway (with Redis/DB fast paths) and populates
price_t1, price_t3, price_t7, price_t30, return_t1, return_t3, return_t7, return_t30,
and the outcome column ('win', 'loss', 'pending', 'expired').
"""

from __future__ import annotations

import asyncio
import json
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from alekfi.db.database import get_session
from alekfi.marketdata import MarketDataGateway

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────
_INTERVAL_SECONDS = 600  # 10 minutes
_BATCH_LIMIT = 200
_MIN_AGE_HOURS = 1  # start processing after 1 hour
_WIN_LOSS_THRESHOLD_PCT = 1.0  # 1% threshold for win/loss determination
_EXPIRY_DAYS = 7  # signals older than 7d without clear win/loss => expired

# Feature flag: prefer Redis/DB over provider calls (Phase 1)
_USE_DB_PRICES = os.environ.get("DECAY_USE_DB", "1") == "1"
_REDIS_PRICE_TTL_MINUTES = 30  # accept Redis prices within this window

# Checkpoint definitions: (column_suffix, timedelta for eligibility)
_CHECKPOINTS = [
    (1, timedelta(days=1)),
    (3, timedelta(days=3)),
    (7, timedelta(days=7)),
    (30, timedelta(days=30)),
]

# Market hours (US Eastern via UTC)
_MARKET_OPEN_UTC = 14 * 60 + 30   # 14:30 UTC = 9:30 ET
_MARKET_CLOSE_UTC = 21 * 60       # 21:00 UTC = 16:00 ET


def _is_market_hours() -> bool:
    """Check if current UTC time is within US equity market hours (Mon-Fri 14:30-21:00 UTC)."""
    now = datetime.now(timezone.utc)
    weekday = now.weekday()
    if weekday >= 5:  # Saturday=5, Sunday=6
        return False
    minutes_since_midnight = now.hour * 60 + now.minute
    return _MARKET_OPEN_UTC <= minutes_since_midnight < _MARKET_CLOSE_UTC


class SignalDecayWorker:
    """Fills price decay checkpoints on the signals table and determines outcomes."""

    def __init__(self) -> None:
        self.signals_processed: int = 0
        self.prices_fetched: int = 0
        self.outcomes_set: int = 0
        self.errors: int = 0
        self.cycles: int = 0
        self.last_cycle_at: str | None = None
        self._redis = None  # lazy init
        self._gateway: MarketDataGateway | None = None

    # ── Price fetching ────────────────────────────────────────────────

    async def _ensure_redis(self):
        """Lazy-init reusable Redis connection."""
        if self._redis is None:
            try:
                import redis.asyncio as aioredis
                from alekfi.config import get_settings
                settings = get_settings()
                self._redis = aioredis.from_url(settings.redis_url, decode_responses=True)
            except Exception:
                logger.warning("[signal_decay] Redis connection failed")
        if self._gateway is None and self._redis is not None:
            self._gateway = MarketDataGateway(redis_client=self._redis)
        return self._redis

    async def _get_price(self, ticker: str) -> float | None:
        """Get current price: Redis → price_data → gateway."""
        lookup_keys = [ticker]
        if not ticker.endswith("-USD") and ticker in ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "MATIC"):
            lookup_keys.append(f"{ticker}-USD")

        # 1) Redis cache (fastest, <1ms)
        if _USE_DB_PRICES:
            r = await self._ensure_redis()
            if r:
                for key in lookup_keys:
                    try:
                        raw = await r.get(f"alekfi:price:{key}")
                        if raw:
                            data = json.loads(raw)
                            price = data.get("price")
                            if price and float(price) > 0:
                                self.prices_fetched += 1
                                return round(float(price), 4)
                    except Exception:
                        pass

        # 2) price_data table (fast, <10ms)
        if _USE_DB_PRICES:
            for key in lookup_keys:
                try:
                    async with get_session() as session:
                        result = await session.execute(
                            text(
                                "SELECT price FROM price_data "
                                "WHERE symbol = :sym AND fetched_at >= NOW() - INTERVAL '30 minutes' "
                                "ORDER BY fetched_at DESC LIMIT 1"
                            ),
                            {"sym": key},
                        )
                        row = result.fetchone()
                        if row and row.price and float(row.price) > 0:
                            self.prices_fetched += 1
                            return round(float(row.price), 4)
                except Exception:
                    pass

        # 3) Canonical market-data gateway
        await self._ensure_redis()
        gw = self._gateway
        if gw:
            for key in lookup_keys:
                try:
                    quote = await gw.get_quote(key, fail_closed=False)
                    if quote and quote.get("price") and float(quote["price"]) > 0:
                        self.prices_fetched += 1
                        return round(float(quote["price"]), 4)
                except Exception:
                    logger.debug("[signal_decay] gateway quote failed for %s", key, exc_info=True)

        return None

    async def _get_historical_price(self, ticker: str, target_date: datetime) -> float | None:
        """Get the closing price for a ticker on or near a specific date.

        Tries price_data table first, then gateway as fallback.
        """
        lookup_keys = [ticker]
        if not ticker.endswith("-USD") and ticker in ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "MATIC"):
            lookup_keys.append(f"{ticker}-USD")

        # 1) price_data table — nearest price within ±2 hours of target
        if _USE_DB_PRICES:
            for key in lookup_keys:
                try:
                    async with get_session() as session:
                        result = await session.execute(
                            text(
                                "SELECT price, fetched_at FROM price_data "
                                "WHERE symbol = :sym "
                                "  AND fetched_at BETWEEN :t_start AND :t_end "
                                "ORDER BY ABS(EXTRACT(EPOCH FROM (fetched_at - :target))) "
                                "LIMIT 1"
                            ),
                            {
                                "sym": key,
                                "t_start": target_date - timedelta(hours=2),
                                "t_end": target_date + timedelta(hours=2),
                                "target": target_date,
                            },
                        )
                        row = result.fetchone()
                        if row and row.price and float(row.price) > 0:
                            self.prices_fetched += 1
                            return round(float(row.price), 4)
                except Exception:
                    pass

        # 2) Canonical market-data gateway lookup around target timestamp
        await self._ensure_redis()
        gw = self._gateway
        if gw:
            for key in lookup_keys:
                try:
                    snap = await gw.get_price_at(key, target_date, tolerance_days=3, fail_closed=False)
                    if snap and snap.get("price") and float(snap["price"]) > 0:
                        self.prices_fetched += 1
                        return round(float(snap["price"]), 4)
                except Exception:
                    logger.debug("[signal_decay] gateway historical price failed for %s @ %s", key, target_date, exc_info=True)

        return None

    async def _get_price_from_redis(self, ticker: str) -> float | None:
        """Get current price from Redis cache (uses shared connection)."""
        r = await self._ensure_redis()
        if not r:
            return None
        try:
            raw = await r.get(f"alekfi:price:{ticker}")
            if raw:
                data = json.loads(raw)
                price = data.get("price")
                if price and float(price) > 0:
                    return float(price)
        except Exception:
            pass
        return None

    # ── Outcome determination ─────────────────────────────────────────

    @staticmethod
    def _determine_outcome(
        direction: str | None,
        return_t1: float | None,
        return_t7: float | None,
        signal_age_days: float,
    ) -> str:
        """Determine the outcome of a signal.

        Rules:
        - 'win'     if return in signal direction > 1% within 24h (return_t1)
        - 'loss'    if return against signal direction > 1% within 24h (return_t1)
        - 'expired' if signal older than 7d with no clear win/loss
        - 'pending' otherwise
        """
        if return_t1 is not None and direction:
            d = direction.upper()
            if d == "LONG":
                if return_t1 >= _WIN_LOSS_THRESHOLD_PCT:
                    return "win"
                if return_t1 <= -_WIN_LOSS_THRESHOLD_PCT:
                    return "loss"
            elif d == "SHORT":
                # For short signals, negative return = win
                if return_t1 <= -_WIN_LOSS_THRESHOLD_PCT:
                    return "win"
                if return_t1 >= _WIN_LOSS_THRESHOLD_PCT:
                    return "loss"
            elif d == "HEDGE":
                # Hedge wins if absolute move > 1%
                if abs(return_t1) >= _WIN_LOSS_THRESHOLD_PCT:
                    return "win"

        # If we have 7-day data but no win/loss yet, check for expiry
        if signal_age_days >= _EXPIRY_DAYS:
            # Signal is old enough, check if we ever got a clear result
            if return_t7 is not None and direction:
                d = direction.upper()
                if d == "LONG" and return_t7 >= _WIN_LOSS_THRESHOLD_PCT:
                    return "win"
                if d == "LONG" and return_t7 <= -_WIN_LOSS_THRESHOLD_PCT:
                    return "loss"
                if d == "SHORT" and return_t7 <= -_WIN_LOSS_THRESHOLD_PCT:
                    return "win"
                if d == "SHORT" and return_t7 >= _WIN_LOSS_THRESHOLD_PCT:
                    return "loss"
            return "expired"

        return "pending"

    # ── Core processing ───────────────────────────────────────────────

    async def _check_and_update_signals(self) -> int:
        """Find signals needing price checkpoint updates and process them.

        Returns the number of signals processed in this cycle.
        """
        now = datetime.now(timezone.utc)
        processed = 0

        # Query signals that still have unfilled checkpoints (price_t30 IS NULL)
        # and are at least 1 hour old
        query = text("""
            SELECT id, created_at, direction,
                   price_at_signal,
                   price_t1, price_t3, price_t7, price_t30,
                   return_t1, return_t3, return_t7, return_t30,
                   outcome
            FROM signals
            WHERE price_t30 IS NULL
              AND created_at <= NOW() - INTERVAL '1 hour'
            ORDER BY created_at ASC
            LIMIT :limit
        """)

        async with get_session() as session:
            result = await session.execute(query, {"limit": _BATCH_LIMIT})
            rows = result.fetchall()

        if not rows:
            logger.debug("[signal_decay] no signals to process")
            return 0

        logger.info("[signal_decay] found %d signals to check", len(rows))

        for row in rows:
            try:
                signal_id = row.id
                created_at = row.created_at
                direction = row.direction
                price_at_signal = row.price_at_signal
                current_price_t1 = row.price_t1
                current_price_t3 = row.price_t3
                current_price_t7 = row.price_t7
                current_price_t30 = row.price_t30
                current_return_t1 = row.return_t1
                current_return_t3 = row.return_t3
                current_return_t7 = row.return_t7
                current_return_t30 = row.return_t30
                current_outcome = row.outcome

                # Ensure created_at is timezone-aware
                if created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)

                signal_age = now - created_at
                signal_age_days = signal_age.total_seconds() / 86400

                # Extract primary ticker from affected_instruments
                ticker = await self._get_primary_ticker(signal_id)
                if not ticker:
                    logger.debug("[signal_decay] no ticker for signal %s, skipping", signal_id)
                    continue

                # If price_at_signal is missing, try to populate it
                if price_at_signal is None or price_at_signal <= 0:
                    hist_price = await self._get_historical_price(ticker, created_at)
                    if hist_price:
                        price_at_signal = hist_price
                        async with get_session() as session:
                            await session.execute(
                                text("UPDATE signals SET price_at_signal = :p WHERE id = :id"),
                                {"p": price_at_signal, "id": signal_id},
                            )
                        logger.info(
                            "[signal_decay] backfilled price_at_signal=%.4f for %s (%s)",
                            price_at_signal, signal_id, ticker,
                        )

                if not price_at_signal or price_at_signal <= 0:
                    logger.debug("[signal_decay] no base price for signal %s, skipping", signal_id)
                    continue

                # Build the update dict for this signal
                updates: dict[str, Any] = {}
                new_return_t1 = current_return_t1
                new_return_t7 = current_return_t7

                for day_num, delta in _CHECKPOINTS:
                    price_col = f"price_t{day_num}"
                    return_col = f"return_t{day_num}"

                    # Skip if already filled
                    current_price_val = {
                        1: current_price_t1,
                        3: current_price_t3,
                        7: current_price_t7,
                        30: current_price_t30,
                    }.get(day_num)

                    if current_price_val is not None:
                        continue

                    # Check if signal is old enough for this checkpoint
                    if signal_age < delta:
                        continue

                    # Get the price at the checkpoint time
                    checkpoint_time = created_at + delta
                    checkpoint_price = await self._get_historical_price(ticker, checkpoint_time)

                    if checkpoint_price is None:
                        # If the checkpoint is recent (within last 2 days), try current price
                        if (now - checkpoint_time) < timedelta(days=2):
                            checkpoint_price = await self._get_price(ticker)

                    if checkpoint_price is None:
                        continue

                    # Calculate return percentage
                    return_pct = round(
                        ((checkpoint_price - price_at_signal) / price_at_signal) * 100, 4
                    )

                    updates[price_col] = checkpoint_price
                    updates[return_col] = return_pct

                    # Track return_t1 and return_t7 for outcome determination
                    if day_num == 1:
                        new_return_t1 = return_pct
                    elif day_num == 7:
                        new_return_t7 = return_pct

                    logger.info(
                        "[signal_decay] signal %s: %s=%.4f %s=%.4f%% (ticker=%s)",
                        signal_id, price_col, checkpoint_price, return_col, return_pct, ticker,
                    )

                # Determine outcome
                new_outcome = self._determine_outcome(
                    direction, new_return_t1, new_return_t7, signal_age_days
                )

                if new_outcome != current_outcome:
                    updates["outcome"] = new_outcome
                    logger.info(
                        "[signal_decay] signal %s: outcome %s -> %s (dir=%s, ret_t1=%s)",
                        signal_id, current_outcome, new_outcome, direction, new_return_t1,
                    )

                # Apply updates
                if updates:
                    set_clauses = ", ".join(f"{k} = :{k}" for k in updates)
                    update_query = text(f"UPDATE signals SET {set_clauses} WHERE id = :id")
                    params = {**updates, "id": signal_id}

                    async with get_session() as session:
                        await session.execute(update_query, params)

                    processed += 1
                    self.signals_processed += 1

                    if "outcome" in updates and updates["outcome"] in ("win", "loss"):
                        self.outcomes_set += 1

            except Exception:
                self.errors += 1
                logger.warning(
                    "[signal_decay] error processing signal %s",
                    row.id if hasattr(row, "id") else "unknown",
                    exc_info=True,
                )

        return processed

    async def _get_primary_ticker(self, signal_id: Any) -> str | None:
        """Extract the primary ticker from a signal's affected_instruments."""
        try:
            async with get_session() as session:
                result = await session.execute(
                    text("SELECT affected_instruments FROM signals WHERE id = :id"),
                    {"id": signal_id},
                )
                row = result.fetchone()

            if not row or not row.affected_instruments:
                return None

            instruments = row.affected_instruments
            if isinstance(instruments, list) and instruments:
                symbol = instruments[0].get("symbol", "")
                if symbol and symbol != "N/A" and "/" not in symbol:
                    return symbol
            elif isinstance(instruments, dict):
                # Handle case where it's a single dict
                symbol = instruments.get("symbol", "")
                if symbol and symbol != "N/A" and "/" not in symbol:
                    return symbol

        except Exception:
            logger.debug("[signal_decay] failed to get ticker for signal %s", signal_id)

        return None

    # ── Main run loop ─────────────────────────────────────────────────

    async def run(self) -> None:
        """Continuous loop: check and update signals every 10 minutes."""
        logger.info("[signal_decay] starting signal decay worker (interval=%ds)", _INTERVAL_SECONDS)

        while True:
            try:
                cycle_start = datetime.now(timezone.utc)
                self.cycles += 1

                logger.info(
                    "[HEARTBEAT] signal_decay_worker cycle=%d starting at %s (market_hours=%s)",
                    self.cycles, cycle_start.isoformat(), _is_market_hours(),
                )

                processed = await self._check_and_update_signals()

                cycle_end = datetime.now(timezone.utc)
                elapsed = (cycle_end - cycle_start).total_seconds()
                self.last_cycle_at = cycle_end.isoformat()

                logger.info(
                    "[HEARTBEAT] signal_decay_worker cycle=%d done: processed=%d elapsed=%.1fs "
                    "total_processed=%d total_outcomes=%d total_errors=%d",
                    self.cycles, processed, elapsed,
                    self.signals_processed, self.outcomes_set, self.errors,
                )

            except Exception:
                logger.error("[signal_decay] fatal cycle error", exc_info=True)

            # Sleep for the configured interval
            # During off-market hours on weekends, we could sleep longer,
            # but 10 min is fine since the worker is lightweight
            await asyncio.sleep(_INTERVAL_SECONDS)

    # ── Stats ─────────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        """Return worker statistics."""
        return {
            "cycles": self.cycles,
            "signals_processed": self.signals_processed,
            "prices_fetched": self.prices_fetched,
            "outcomes_set": self.outcomes_set,
            "errors": self.errors,
            "last_cycle_at": self.last_cycle_at,
            "is_market_hours": _is_market_hours(),
            "interval_seconds": _INTERVAL_SECONDS,
        }
