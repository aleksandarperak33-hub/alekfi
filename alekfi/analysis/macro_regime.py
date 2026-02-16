"""Macro Regime Detection — VIX-based market regime classification.

Regimes:
  CALM     — VIX < 15   (tight sigma thresholds, most signals are noise)
  NORMAL   — VIX 15-25  (standard thresholds)
  ELEVATED — VIX 25-35  (loose thresholds, more signals pass)
  PANIC    — VIX > 35   (only extreme outliers matter)

Uses MarketDataGateway for VIX data (provider fallback + quality flags).
Stored in Redis at `alekfi:macro:regime` with 30-min TTL.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


REGIME_THRESHOLDS = {
    "CALM": {"vix_max": 15, "sigma_elevated": 1.0, "sigma_significant": 1.5, "sigma_critical": 2.5},
    "NORMAL": {"vix_max": 25, "sigma_elevated": 1.0, "sigma_significant": 2.0, "sigma_critical": 3.0},
    "ELEVATED": {"vix_max": 35, "sigma_elevated": 1.5, "sigma_significant": 2.5, "sigma_critical": 3.5},
    "PANIC": {"vix_max": 999, "sigma_elevated": 2.0, "sigma_significant": 3.0, "sigma_critical": 4.0},
}


def classify_regime(vix: float) -> str:
    """Classify the current macro regime based on VIX."""
    if vix < 15:
        return "CALM"
    if vix < 25:
        return "NORMAL"
    if vix < 35:
        return "ELEVATED"
    return "PANIC"


def get_regime_thresholds(regime: str) -> dict[str, float]:
    """Get sigma thresholds for a given regime."""
    return REGIME_THRESHOLDS.get(regime, REGIME_THRESHOLDS["NORMAL"])


async def update_macro_regime() -> dict[str, Any]:
    """Fetch VIX via gateway, classify regime, store in Redis."""
    import redis.asyncio as aioredis
    from alekfi.config import get_settings
    from alekfi.marketdata import MarketDataGateway

    s = get_settings()
    result: dict[str, Any] = {
        "regime": "NORMAL",
        "vix": None,
        "thresholds": REGIME_THRESHOLDS["NORMAL"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        r = aioredis.from_url(s.redis_url, decode_responses=True)
        try:
            gw = MarketDataGateway(redis_client=r)
            ohlcv = await gw.get_ohlcv("^VIX", period="5d", interval="1d", min_points=2, fail_closed=False)
            rows = ohlcv.get("rows") or []
            if len(rows) >= 1:
                last = rows[-1]
                prev = rows[-2] if len(rows) >= 2 else last
                data = {
                    "c": float(last.get("close") or 0.0),
                    "pc": float(prev.get("close") or 0.0),
                    "h": float(last.get("high") or 0.0),
                    "l": float(last.get("low") or 0.0),
                    "o": float(last.get("open") or 0.0),
                    "meta": ohlcv.get("meta") or {},
                }
            else:
                data = None
        finally:
            await r.aclose()

        if data and data["c"] > 0:
            vix = data["c"]
            regime = classify_regime(vix)
            change_pct = ((vix - data["pc"]) / max(data["pc"], 0.01)) * 100
            result = {
                "regime": regime,
                "vix": round(vix, 2),
                "vix_open": round(data["o"], 2),
                "vix_high": round(data["h"], 2),
                "vix_low": round(data["l"], 2),
                "vix_prev_close": round(data["pc"], 2),
                "vix_change_pct": round(change_pct, 2),
                "thresholds": get_regime_thresholds(regime),
                "quality_flags": data.get("meta", {}).get("quality_flags", []),
                "provider_used": data.get("meta", {}).get("provider_used"),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            logger.info("[macro] VIX=%.2f regime=%s", vix, regime)
    except Exception:
        logger.warning("[macro] failed to fetch VIX, defaulting to NORMAL", exc_info=True)

    # Store in Redis
    try:
        r = aioredis.from_url(s.redis_url, decode_responses=True)
        try:
            await r.set("alekfi:macro:regime", json.dumps(result, default=str), ex=1800)
        finally:
            await r.aclose()
    except Exception:
        logger.warning("[macro] failed to store regime in Redis")

    return result


async def get_current_regime() -> dict[str, Any]:
    """Get the current macro regime from Redis (or compute fresh)."""
    import redis.asyncio as aioredis
    from alekfi.config import get_settings

    s = get_settings()
    try:
        r = aioredis.from_url(s.redis_url, decode_responses=True)
        try:
            raw = await r.get("alekfi:macro:regime")
            if raw:
                return json.loads(raw)
        finally:
            await r.aclose()
    except Exception:
        pass

    return await update_macro_regime()
