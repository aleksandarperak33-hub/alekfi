"""Backfill and nightly label generation for signal outcomes.

Fail-closed semantics:
- If market-data quality is degraded (auth fail, invalid symbol, incomplete window),
  labels are skipped with explicit reasons.
"""

from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text

from alekfi.db.database import get_session
from alekfi.marketdata import MarketDataGateway

HORIZONS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
}

BAD_FLAGS = {"AUTH_FAIL", "SYMBOL_INVALID", "INCOMPLETE_WINDOW"}


@dataclass
class LabelResult:
    labels: dict[str, Any]
    skipped: bool
    skip_reason: str | None


def _labels_from_existing_checkpoints(
    *,
    row: dict[str, Any],
    is_null_sample: bool = False,
) -> LabelResult | None:
    """Prefer existing checkpoint prices to densify labels without extra market-data fetches."""
    c0 = row.get("price_at_creation")
    if not isinstance(c0, dict) or not c0:
        return None

    horizons_src = {
        "1h": row.get("price_at_1h"),
        "4h": row.get("price_at_4h"),
        "1d": row.get("price_at_24h"),
        "3d": row.get("price_at_48h"),  # nearest available checkpoint
        "7d": row.get("price_at_7d"),
    }

    horizons: dict[str, Any] = {}
    for h, pmap in horizons_src.items():
        if not isinstance(pmap, dict) or not pmap:
            continue
        returns_pct: dict[str, float] = {}
        for t, p0 in c0.items():
            try:
                p1 = pmap.get(t)
                if p0 is None or p1 is None:
                    continue
                p0f = float(p0)
                p1f = float(p1)
                if p0f > 0:
                    returns_pct[str(t)] = round(((p1f - p0f) / p0f) * 100.0, 4)
            except Exception:
                continue
        if not returns_pct:
            continue
        spy = returns_pct.get("SPY")
        abnormal = {t: round(r - spy, 4) for t, r in returns_pct.items() if t != "SPY" and spy is not None}
        avg_abn = round(sum(abnormal.values()) / len(abnormal), 4) if abnormal else None
        avg_ret_vals = [v for k, v in returns_pct.items() if k != "SPY"]
        avg_ret = round(sum(avg_ret_vals) / len(avg_ret_vals), 4) if avg_ret_vals else None
        label = "flat"
        if avg_abn is not None:
            if avg_abn > 0.5:
                label = "up"
            elif avg_abn < -0.5:
                label = "down"
        horizons[h] = {
            "direction": label,
            "returns_pct": returns_pct,
            "abnormal_returns_pct": abnormal,
            "avg_return_pct": avg_ret,
            "avg_abnormal_return_pct": avg_abn,
            "derived_from": "signal_outcomes_checkpoint_prices",
            "approximate_horizon": "48h_proxy_for_3d" if h == "3d" else None,
        }

    if not horizons:
        return None

    mfe = row.get("max_favorable_move_pct")
    mae = row.get("max_adverse_move_pct")
    mfe = float(mfe) if mfe is not None else None
    mae = float(mae) if mae is not None else None

    return LabelResult(
        labels={
            "labeling_version": "v3_backfill_checkpoint",
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "signal_type": row.get("signal_type"),
            "predicted_direction": row.get("predicted_direction"),
            "is_null_sample": is_null_sample,
            "horizons": horizons,
            "mfe_pct": round(mfe, 4) if mfe is not None else None,
            "mae_pct": round(mae, 4) if mae is not None else None,
            "time_to_peak_horizon": max(horizons.items(), key=lambda kv: abs((kv[1] or {}).get("avg_abnormal_return_pct") or 0))[0],
            "price_source": {"mode": "checkpoint_prices"},
            "quality_flags": {"mode": ["CHECKPOINT_SOURCE"]},
            "tradability_pass": True,
            "label_confidence": 0.85,
        },
        skipped=False,
        skip_reason=None,
    )


def _to_dt(value: str | datetime | None) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    return datetime.now(timezone.utc)


def _extract_tickers(instruments: Any) -> list[str]:
    out: list[str] = []
    if isinstance(instruments, list):
        for item in instruments:
            if isinstance(item, str):
                out.append(item.upper())
            elif isinstance(item, dict) and item.get("symbol"):
                out.append(str(item["symbol"]).upper())
    elif isinstance(instruments, dict):
        for k in instruments:
            if isinstance(k, str):
                out.append(k.upper())
    out = [x for x in out if x and "/" not in x and len(x) <= 12]
    if "SPY" not in out:
        out.append("SPY")
    return list(dict.fromkeys(out))


def _is_degraded(flags: list[str]) -> bool:
    return bool({str(f).upper() for f in (flags or [])} & BAD_FLAGS)


async def _price_at(gw: MarketDataGateway, symbol: str, ts: datetime) -> tuple[float | None, dict[str, Any], list[str]]:
    snap = await gw.get_price_at(symbol, ts, tolerance_days=3, fail_closed=False)
    if not snap:
        return None, {}, ["NO_DATA"]
    meta = snap.get("meta") or {}
    flags = list(meta.get("quality_flags") or [])
    try:
        px = float(snap.get("price"))
    except Exception:
        px = None
    return px, meta, flags


async def _build_labels_for_outcome(
    gw: MarketDataGateway,
    *,
    instruments: Any,
    created_at: datetime,
    direction: str,
    signal_type: str | None,
    is_null_sample: bool = False,
) -> LabelResult:
    tickers = _extract_tickers(instruments)
    if not tickers:
        return LabelResult(
            labels={
                "label_skipped_reason": "NO_TICKERS",
                "labeling_version": "v3_backfill",
                "label_confidence": 0.0,
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "is_null_sample": is_null_sample,
            },
            skipped=True,
            skip_reason="NO_TICKERS",
        )

    created_at = _to_dt(created_at)

    normalized = []
    for t in tickers:
        n = gw.normalize_symbol(t)
        if n.valid:
            normalized.append(n.symbol)
    tickers = list(dict.fromkeys(normalized))
    if "SPY" not in tickers:
        tickers.append("SPY")
    if len([t for t in tickers if t != "SPY"]) == 0:
        return LabelResult(
            labels={
                "label_skipped_reason": "NO_VALID_TICKERS",
                "labeling_version": "v3_backfill",
                "label_confidence": 0.0,
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "is_null_sample": is_null_sample,
            },
            skipped=True,
            skip_reason="NO_VALID_TICKERS",
        )

    base_prices: dict[str, float] = {}
    quality_flags: dict[str, list[str]] = {}
    price_source: dict[str, Any] = {}
    dropped_tickers: list[str] = []

    for ticker in tickers:
        px, meta, flags = await _price_at(gw, ticker, created_at)
        quality_flags[ticker] = flags
        price_source[ticker] = {
            "provider_used": meta.get("provider_used"),
            "fallback_chain": meta.get("fallback_chain", []),
            "quality_flags": flags,
        }
        if px is None or px <= 0 or _is_degraded(flags):
            # Drop bad non-benchmark tickers; fail only if benchmark or all instruments fail.
            if ticker == "SPY":
                reason = gw.label_skip_reason(meta) or "DEGRADED_MARKET_DATA"
                return LabelResult(
                    labels={
                        "label_skipped_reason": reason,
                        "labeling_version": "v3_backfill",
                        "label_confidence": 0.0,
                        "quality_flags": quality_flags,
                        "price_source": price_source,
                        "computed_at": datetime.now(timezone.utc).isoformat(),
                        "is_null_sample": is_null_sample,
                    },
                    skipped=True,
                    skip_reason=reason,
                )
            dropped_tickers.append(ticker)
            continue
        base_prices[ticker] = px

    if len([t for t in base_prices if t != "SPY"]) == 0:
        return LabelResult(
            labels={
                "label_skipped_reason": "NO_PRICEABLE_INSTRUMENTS",
                "labeling_version": "v3_backfill",
                "label_confidence": 0.0,
                "quality_flags": quality_flags,
                "price_source": price_source,
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "is_null_sample": is_null_sample,
            },
            skipped=True,
            skip_reason="NO_PRICEABLE_INSTRUMENTS",
        )

    horizons: dict[str, Any] = {}
    horizon_scores: list[tuple[str, float]] = []
    for h, delta in HORIZONS.items():
        horizon_ts = created_at + delta
        returns_pct: dict[str, float] = {}
        local_quality = {}
        active_tickers = list(base_prices.keys())
        for ticker in active_tickers:
            px_h, meta_h, flags_h = await _price_at(gw, ticker, horizon_ts)
            local_quality[ticker] = flags_h
            if px_h is None or px_h <= 0 or _is_degraded(flags_h):
                if ticker == "SPY":
                    reason = gw.label_skip_reason(meta_h) or "DEGRADED_MARKET_DATA"
                    return LabelResult(
                        labels={
                            "label_skipped_reason": reason,
                            "labeling_version": "v3_backfill",
                            "label_confidence": 0.0,
                            "quality_flags": quality_flags | local_quality,
                            "price_source": price_source,
                            "computed_at": datetime.now(timezone.utc).isoformat(),
                            "is_null_sample": is_null_sample,
                        },
                        skipped=True,
                        skip_reason=reason,
                    )
                dropped_tickers.append(ticker)
                continue
            b = base_prices[ticker]
            returns_pct[ticker] = round(((px_h - b) / b) * 100.0, 4)

        if len([t for t in returns_pct if t != "SPY"]) == 0:
            continue

        spy_ret = returns_pct.get("SPY")
        abnormal: dict[str, float] = {}
        for t, r in returns_pct.items():
            if t == "SPY" or spy_ret is None:
                continue
            abnormal[t] = round(r - spy_ret, 4)

        abn_vals = list(abnormal.values())
        avg_abn = round(sum(abn_vals) / len(abn_vals), 4) if abn_vals else None
        avg_ret_vals = [v for k, v in returns_pct.items() if k != "SPY"]
        avg_ret = round(sum(avg_ret_vals) / len(avg_ret_vals), 4) if avg_ret_vals else None

        direction_label = "flat"
        if avg_abn is not None:
            if avg_abn > 0.5:
                direction_label = "up"
            elif avg_abn < -0.5:
                direction_label = "down"
        horizon_scores.append((h, abs(avg_abn or 0.0)))

        horizons[h] = {
            "direction": direction_label,
            "returns_pct": returns_pct,
            "abnormal_returns_pct": abnormal,
            "avg_return_pct": avg_ret,
            "avg_abnormal_return_pct": avg_abn,
            "time_to_horizon_minutes": int(delta.total_seconds() / 60),
        }

    # MFE/MAE on 7d window (daily bars)
    if not horizons:
        return LabelResult(
            labels={
                "label_skipped_reason": "NO_HORIZON_LABELS",
                "labeling_version": "v3_backfill",
                "label_confidence": 0.0,
                "quality_flags": quality_flags,
                "price_source": price_source,
                "computed_at": datetime.now(timezone.utc).isoformat(),
                "is_null_sample": is_null_sample,
            },
            skipped=True,
            skip_reason="NO_HORIZON_LABELS",
        )

    # MFE/MAE on 7d window (daily bars)
    mfe = 0.0
    mae = 0.0
    end_7d = created_at + HORIZONS["7d"]
    for t in [x for x in tickers if x != "SPY"]:
        px0 = base_prices.get(t)
        if not px0:
            continue
        ohlcv = await gw.get_ohlcv(t, start=created_at, end=end_7d, interval="1d", min_points=2, fail_closed=False)
        rows = ohlcv.get("rows") or []
        if not rows:
            continue
        rel = [((float(r.get("close") or px0) - px0) / px0) * 100.0 for r in rows if r.get("close") is not None]
        if rel:
            mfe = max(mfe, max(rel))
            mae = min(mae, min(rel))

    # Time-to-peak proxy = first horizon with largest absolute abnormal return.
    time_to_peak = None
    if horizon_scores:
        best = max(horizon_scores, key=lambda x: x[1])
        time_to_peak = best[0]

    primary = next((t for t in tickers if t != "SPY"), tickers[0])
    quote = await gw.get_quote(primary, fail_closed=False)
    quote_meta = (quote or {}).get("meta") or {}
    tradability_pass = not _is_degraded(list(quote_meta.get("quality_flags") or []))
    if quote and quote.get("volume") is not None and quote.get("price") is not None:
        tradability_pass = tradability_pass and float(quote.get("volume") or 0) * float(quote.get("price") or 0) >= 3_000_000

    return LabelResult(
        labels={
            "labeling_version": "v3_backfill",
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "signal_type": signal_type,
            "predicted_direction": direction,
            "is_null_sample": is_null_sample,
            "horizons": horizons,
            "mfe_pct": round(mfe, 4),
            "mae_pct": round(mae, 4),
            "time_to_peak_horizon": time_to_peak,
            "price_source": price_source,
            "quality_flags": quality_flags,
            "dropped_tickers": sorted(set(dropped_tickers)),
            "tradability_pass": bool(tradability_pass),
            "label_confidence": 1.0,
        },
        skipped=False,
        skip_reason=None,
    )


async def _ensure_null_table() -> None:
    async with get_session() as session:
        await session.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS signal_null_outcomes (
                    id UUID PRIMARY KEY,
                    symbol VARCHAR(32) NOT NULL,
                    sample_ts TIMESTAMPTZ NOT NULL,
                    labels JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        )
        await session.execute(
            text(
                """
                CREATE INDEX IF NOT EXISTS idx_signal_null_outcomes_symbol_ts
                ON signal_null_outcomes (symbol, sample_ts DESC)
                """
            )
        )


async def run_backfill_labels(
    *,
    since: datetime,
    limit: int = 5000,
    include_null_samples: bool = True,
    null_sample_limit: int = 1000,
    force: bool = False,
) -> dict[str, Any]:
    """Backfill labels for signal_outcomes since `since`."""
    gw = MarketDataGateway()
    await _ensure_null_table()

    query = """
        SELECT
            so.id, so.signal_id, so.instruments, so.predicted_direction, so.created_at, so.labels, so.signal_type,
            so.price_at_creation, so.price_at_1h, so.price_at_4h, so.price_at_24h, so.price_at_48h, so.price_at_7d,
            so.max_favorable_move_pct, so.max_adverse_move_pct
        FROM signal_outcomes so
        WHERE so.created_at >= :since
    """
    if not force:
        query += " AND (so.labels IS NULL OR so.labels->>'labeling_version' <> 'v3_backfill')"
    query += " ORDER BY so.created_at ASC LIMIT :limit"

    async with get_session() as session:
        rows = (
            await session.execute(
                text(query),
                {"since": since, "limit": limit},
            )
        ).mappings().all()

    labeled = 0
    skipped = 0
    skip_reasons: dict[str, int] = {}
    null_samples = 0

    processed_good_rows: list[dict[str, Any]] = []
    for row in rows:
        result = _labels_from_existing_checkpoints(row=row, is_null_sample=False)
        if result is None:
            result = await _build_labels_for_outcome(
                gw,
                instruments=row["instruments"],
                created_at=row["created_at"],
                direction=(row.get("predicted_direction") or "LONG"),
                signal_type=row.get("signal_type"),
                is_null_sample=False,
            )
        async with get_session() as session:
            await session.execute(
                text("UPDATE signal_outcomes SET labels = CAST(:labels AS jsonb) WHERE id = :id"),
                {"id": str(row["id"]), "labels": json.dumps(result.labels, default=str)},
            )
        if result.skipped:
            skipped += 1
            reason = result.skip_reason or "UNKNOWN"
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
        else:
            labeled += 1
            processed_good_rows.append(row)

    # Null cohort sampling: random matched ticker/timestamp profiles.
    if include_null_samples and processed_good_rows and null_sample_limit > 0:
        for row in random.sample(processed_good_rows, min(len(processed_good_rows), null_sample_limit)):
            tickers = _extract_tickers(row["instruments"])
            primary = next((t for t in tickers if t != "SPY"), None)
            if not primary:
                continue
            sample_ts = _to_dt(row["created_at"]) - timedelta(minutes=random.randint(30, 480))
            null_result = await _build_labels_for_outcome(
                gw,
                instruments=[primary, "SPY"],
                created_at=sample_ts,
                direction=(row.get("predicted_direction") or "LONG"),
                signal_type=row.get("signal_type"),
                is_null_sample=True,
            )
            if null_result.skipped:
                continue
            async with get_session() as session:
                await session.execute(
                    text(
                        """
                        INSERT INTO signal_null_outcomes (id, symbol, sample_ts, labels)
                        VALUES (:id, :symbol, :sample_ts, CAST(:labels AS jsonb))
                        """
                    ),
                    {
                        "id": str(uuid.uuid4()),
                        "symbol": primary,
                        "sample_ts": sample_ts,
                        "labels": json.dumps(null_result.labels, default=str),
                    },
                )
            null_samples += 1

    await gw.close()
    return {
        "since": since.isoformat(),
        "processed": len(rows),
        "labeled": labeled,
        "skipped": skipped,
        "skip_reasons": skip_reasons,
        "null_samples_inserted": null_samples,
        "labeling_version": "v3_backfill",
    }


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Backfill signal outcome labels")
    p.add_argument("--since", type=str, default="2026-01-01")
    p.add_argument("--limit", type=int, default=5000)
    p.add_argument("--null-samples", action="store_true", default=False)
    p.add_argument("--null-sample-limit", type=int, default=1000)
    p.add_argument("--force", action="store_true", default=False)
    return p.parse_args()


async def _amain() -> None:
    args = _parse_args()
    since = _to_dt(args.since)
    summary = await run_backfill_labels(
        since=since,
        limit=args.limit,
        include_null_samples=bool(args.null_samples),
        null_sample_limit=args.null_sample_limit,
        force=args.force,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    import asyncio

    asyncio.run(_amain())
