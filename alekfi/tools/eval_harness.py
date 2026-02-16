"""Evaluation harness for strict/non-strict forecast quality."""

from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from alekfi.db.database import get_session

HORIZONS = ("1h", "4h", "1d", "3d", "7d")


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _strict_eligible(research: dict[str, Any]) -> bool:
    trad = research.get("tradability") or {}
    ev = research.get("evidence") or {}
    controls = research.get("controls") or {}
    verified_exception = bool(controls.get("verified_single_modality_ok"))
    if not trad.get("pass", False):
        return False
    ind = _safe_float(ev.get("independence_score"), 0.0)
    up = int(ev.get("unique_platforms") or 0)
    if ind >= 0.45 and up >= 2:
        return True
    return verified_exception


def _actual_hit(direction: str, outcome_h: dict[str, Any]) -> int | None:
    if not isinstance(outcome_h, dict):
        return None
    label = (outcome_h.get("direction") or outcome_h.get("direction_label") or "").lower()
    if label not in {"up", "down", "flat"}:
        return None
    d = (direction or "LONG").upper()
    if d == "LONG":
        return 1 if label == "up" else 0
    if d == "SHORT":
        return 1 if label == "down" else 0
    if d == "HEDGE":
        return 1 if label == "flat" else 0
    return 0


def _pred_prob(direction: str, forecast_h: dict[str, Any]) -> float | None:
    if not isinstance(forecast_h, dict):
        return None
    d = (direction or "LONG").upper()
    if d == "LONG":
        return _safe_float(forecast_h.get("p_up"), 0.0)
    if d == "SHORT":
        return _safe_float(forecast_h.get("p_down"), 0.0)
    if d == "HEDGE":
        return _safe_float(forecast_h.get("p_flat"), 0.0)
    return None


def _expected_pnl(research: dict[str, Any], horizon: str) -> float:
    learned = research.get("learned") or {}
    by_h = learned.get("expected_pnl_pct_by_horizon") or {}
    if horizon in by_h:
        return _safe_float(by_h.get(horizon), -999.0)
    fh = ((research.get("forecast") or {}).get("by_horizon") or {}).get(horizon) or {}
    trad = research.get("tradability") or {}
    p_up = _safe_float(fh.get("p_up"), 0.0)
    p_down = _safe_float(fh.get("p_down"), 0.0)
    exp = _safe_float(fh.get("expected_move_pct"), 0.0)
    adv = _safe_float(fh.get("expected_adverse_pct"), 0.0)
    cost_pct = (_safe_float(trad.get("spread_bps_est"), 25.0) * 1.5 / 10000.0) * 100.0
    return p_up * exp - p_down * adv - cost_pct


async def run_eval(*, since_days: int = 14, topk: list[int] | None = None) -> dict[str, Any]:
    topk = topk or [5, 10, 20]
    since = datetime.now(timezone.utc) - timedelta(days=since_days)

    async with get_session() as session:
        rows = (
            await session.execute(
                text(
                    """
                    SELECT s.id, s.created_at, s.direction, s.metadata, so.labels
                    FROM signals s
                    JOIN signal_outcomes so ON so.signal_id = s.id
                    WHERE s.created_at >= :since
                      AND s.metadata IS NOT NULL
                      AND so.labels IS NOT NULL
                    ORDER BY s.created_at DESC
                    """
                ),
                {"since": since},
            )
        ).mappings().all()

    if not rows:
        return {"since": since.isoformat(), "rows": 0, "message": "no labeled rows in window"}

    metrics: dict[str, Any] = {
        "since": since.isoformat(),
        "rows": len(rows),
        "horizons": {},
        "strict_vs_nonstrict": {},
        "ablation": {},
    }

    for h in HORIZONS:
        records = []
        for r in rows:
            meta = r["metadata"] or {}
            research = (meta.get("research") or {}) if isinstance(meta, dict) else {}
            if not research:
                continue
            labels = r["labels"] or {}
            if labels.get("label_skipped_reason"):
                continue
            outcome_h = ((labels.get("horizons") or {}).get(h) or ((labels.get("checkpoints") or {}).get(h)))
            fh = ((research.get("forecast") or {}).get("by_horizon") or {}).get(h)
            p = _pred_prob(r["direction"], fh)
            y = _actual_hit(r["direction"], outcome_h)
            if p is None or y is None:
                continue
            records.append(
                {
                    "signal_id": str(r["id"]),
                    "strict": _strict_eligible(research),
                    "p": p,
                    "y": y,
                    "ev": _expected_pnl(research, h),
                    "has_verification": 1 if ((research.get("evidence") or {}).get("verification_hits") or []) else 0,
                }
            )

        if not records:
            metrics["horizons"][h] = {"count": 0}
            continue

        brier = sum((rec["p"] - rec["y"]) ** 2 for rec in records) / len(records)
        overall_hit = sum(rec["y"] for rec in records) / len(records)
        ranked = sorted(records, key=lambda x: x["ev"], reverse=True)

        precision_at_k = {}
        for k in topk:
            top = ranked[:k]
            if not top:
                precision_at_k[f"p@{k}"] = None
            else:
                precision_at_k[f"p@{k}"] = round(sum(x["y"] for x in top) / len(top), 4)

        strict = [x for x in records if x["strict"]]
        nonstrict = [x for x in records if not x["strict"]]
        strict_ranked = sorted(strict, key=lambda x: x["ev"], reverse=True)
        non_ranked = sorted(nonstrict, key=lambda x: x["ev"], reverse=True)
        strict_p10 = round(sum(x["y"] for x in strict_ranked[:10]) / max(len(strict_ranked[:10]), 1), 4) if strict_ranked else None
        non_p10 = round(sum(x["y"] for x in non_ranked[:10]) / max(len(non_ranked[:10]), 1), 4) if non_ranked else None

        with_verif = [x for x in records if x["has_verification"] == 1]
        without_verif = [x for x in records if x["has_verification"] == 0]
        abl_with = round(sum(x["y"] for x in with_verif) / len(with_verif), 4) if with_verif else None
        abl_without = round(sum(x["y"] for x in without_verif) / len(without_verif), 4) if without_verif else None

        metrics["horizons"][h] = {
            "count": len(records),
            "hit_rate": round(overall_hit, 4),
            "brier": round(brier, 5),
            "precision_at_k": precision_at_k,
            "strict_count": len(strict),
            "non_strict_count": len(nonstrict),
            "strict_p@10": strict_p10,
            "non_strict_p@10": non_p10,
            "ablation_verification_hit_rate": {
                "with_verification": abl_with,
                "without_verification": abl_without,
            },
        }

    return metrics


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run AlekFi forecast evaluation harness")
    p.add_argument("--since-days", type=int, default=14)
    p.add_argument("--topk", type=str, default="5,10,20")
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()
    topk = [int(x.strip()) for x in args.topk.split(",") if x.strip()]
    result = await run_eval(since_days=args.since_days, topk=topk)
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    import asyncio

    asyncio.run(_amain())
