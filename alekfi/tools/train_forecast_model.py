"""Train lightweight empirical forecast priors from historical signal outcomes.

Outputs a versioned JSON artifact and optionally stores it in Redis for online inference.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import text

from alekfi.config import get_settings
from alekfi.db.database import get_session

HORIZONS = ("1h", "4h", "1d", "3d", "7d")


def _safe_float(v: Any, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _signed_move(direction: str, ret_pct: float) -> tuple[float, float]:
    d = (direction or "LONG").upper()
    if d == "SHORT":
        favorable = max(-ret_pct, 0.0)
        adverse = max(ret_pct, 0.0)
    elif d == "HEDGE":
        favorable = abs(ret_pct)
        adverse = 0.0
    else:
        favorable = max(ret_pct, 0.0)
        adverse = max(-ret_pct, 0.0)
    return favorable, adverse


async def train_priors(*, since_days: int = 90, min_samples: int = 20) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(days=since_days)
    async with get_session() as session:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT s.signal_type, s.direction, so.labels
                    FROM signals s
                    JOIN signal_outcomes so ON so.signal_id = s.id
                    WHERE so.labels IS NOT NULL
                      AND s.created_at >= :since
                    """
                ),
                {"since": since},
            )
        ).all()

    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: defaultdict(lambda: {
        "n": 0,
        "correct": 0,
        "favorable_sum": 0.0,
        "adverse_sum": 0.0,
    }))

    total_rows = 0
    for r in rows:
        labels = r.labels or {}
        horizons = (labels.get("horizons") or {})
        if not isinstance(horizons, dict):
            continue
        total_rows += 1
        sig_type = str(r.signal_type or "unknown")
        pred_dir = str(r.direction or "LONG").upper()

        for h in HORIZONS:
            hdat = horizons.get(h) or {}
            if not isinstance(hdat, dict):
                continue
            actual_dir = str(hdat.get("direction") or "").lower()
            avg_abn = _safe_float(hdat.get("avg_abnormal_return_pct"), default=None)
            if avg_abn is None:
                avg_abn = _safe_float(hdat.get("avg_return_pct"), default=None)
            if avg_abn is None:
                continue

            b = buckets[sig_type][h]
            b["n"] += 1

            is_correct = 0
            if pred_dir == "LONG" and actual_dir == "up":
                is_correct = 1
            elif pred_dir == "SHORT" and actual_dir == "down":
                is_correct = 1
            elif pred_dir == "HEDGE" and actual_dir == "flat":
                is_correct = 1
            b["correct"] += is_correct

            fav, adv = _signed_move(pred_dir, float(avg_abn))
            b["favorable_sum"] += fav
            b["adverse_sum"] += adv

    by_signal_type: dict[str, Any] = {}
    for sig_type, by_h in buckets.items():
        out_h = {}
        for h, b in by_h.items():
            n = int(b["n"])
            if n < min_samples:
                continue
            p_correct = b["correct"] / n
            out_h[h] = {
                "samples": n,
                "p_correct": round(float(p_correct), 4),
                "exp_favorable_move_pct": round(float(b["favorable_sum"] / n), 4),
                "exp_adverse_move_pct": round(float(b["adverse_sum"] / n), 4),
            }
        if out_h:
            by_signal_type[sig_type] = {"horizons": out_h}

    # Global fallback prior.
    global_by_h = {}
    for h in HORIZONS:
        agg_n = agg_correct = 0
        fav_sum = adv_sum = 0.0
        for by_h in buckets.values():
            b = by_h.get(h)
            if not b:
                continue
            n = int(b["n"])
            if n <= 0:
                continue
            agg_n += n
            agg_correct += int(b["correct"])
            fav_sum += float(b["favorable_sum"])
            adv_sum += float(b["adverse_sum"])
        if agg_n >= min_samples:
            global_by_h[h] = {
                "samples": agg_n,
                "p_correct": round(agg_correct / agg_n, 4),
                "exp_favorable_move_pct": round(fav_sum / agg_n, 4),
                "exp_adverse_move_pct": round(adv_sum / agg_n, 4),
            }

    return {
        "model_name": "v1_empirical_priors",
        "trained_at": datetime.now(timezone.utc).isoformat(),
        "since": since.isoformat(),
        "total_rows": total_rows,
        "min_samples": min_samples,
        "global": {"horizons": global_by_h},
        "by_signal_type": by_signal_type,
    }


async def _store_redis(payload: dict[str, Any]) -> None:
    try:
        import redis.asyncio as aioredis
        s = get_settings()
        r = aioredis.from_url(s.redis_url, decode_responses=True)
        await r.set("alekfi:forecast:model:v1", json.dumps(payload, default=str))
        await r.set("alekfi:forecast:model:v1:updated_at", payload.get("trained_at", ""))
        await r.aclose()
    except Exception:
        pass


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Train empirical forecast priors")
    p.add_argument("--since-days", type=int, default=90)
    p.add_argument("--min-samples", type=int, default=20)
    p.add_argument("--output", type=str, default="reports/forecast_model_v1.json")
    p.add_argument("--no-redis", action="store_true")
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()
    payload = await train_priors(since_days=args.since_days, min_samples=args.min_samples)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")

    if not args.no_redis:
        await _store_redis(payload)

    print(json.dumps({
        "model_name": payload.get("model_name"),
        "total_rows": payload.get("total_rows"),
        "signal_types": len(payload.get("by_signal_type") or {}),
        "output": str(out),
        "redis_written": not args.no_redis,
    }, indent=2))


if __name__ == "__main__":
    import asyncio

    asyncio.run(_amain())
