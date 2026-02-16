"""Signal endpoints — list, detail, summary, feedback, accuracy, rankings, sources."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from alekfi.config import get_settings

router = APIRouter(tags=["signals"])


@router.get("/signals/summary")
async def signals_summary():
    settings = get_settings()
    if settings.mock_mode:
        from alekfi.api.mock_data import get_mock_provider
        return get_mock_provider().get_signals_summary()

    from alekfi.db.database import get_session
    from alekfi.db.models import Signal
    from sqlalchemy import select, func
    async with get_session() as session:
        total = (await session.execute(select(func.count(Signal.id)))).scalar() or 0
        rows = (await session.execute(
            select(Signal.signal_type, func.count()).group_by(Signal.signal_type)
        )).all()
        by_type = {r[0]: r[1] for r in rows}
        d_rows = (await session.execute(
            select(Signal.direction, func.count()).group_by(Signal.direction)
        )).all()
        by_dir = {r[0]: r[1] for r in d_rows}
        avg_c = (await session.execute(select(func.avg(Signal.conviction)))).scalar() or 0
    return {
        "total": total,
        "by_type": by_type,
        "by_direction": by_dir,
        "avg_conviction": round(float(avg_c), 3),
        "top_instruments": [],
    }


@router.get("/signals")
async def list_signals(
    limit: int = Query(50, ge=1, le=200),
    signal_type: str | None = None,
    direction: str | None = None,
    min_conviction: float | None = None,
    since: str | None = None,
):
    settings = get_settings()
    if settings.mock_mode:
        from alekfi.api.mock_data import get_mock_provider
        return get_mock_provider().get_signals(limit, signal_type, direction, min_conviction, since)

    from alekfi.db.database import get_session
    from alekfi.db.models import Signal
    from alekfi.utils import parse_iso
    from sqlalchemy import select
    async with get_session() as session:
        stmt = select(Signal).order_by(Signal.created_at.desc()).limit(limit)
        if signal_type:
            stmt = stmt.where(Signal.signal_type == signal_type)
        if direction:
            stmt = stmt.where(Signal.direction == direction)
        if min_conviction is not None:
            stmt = stmt.where(Signal.conviction >= min_conviction)
        if since:
            stmt = stmt.where(Signal.created_at >= parse_iso(since))
        result = await session.execute(stmt)
        signals = result.scalars().all()
    return [
        {
            "id": str(s.id), "signal_type": s.signal_type,
            "affected_instruments": s.affected_instruments,
            "direction": s.direction, "conviction": s.conviction,
            "time_horizon": s.time_horizon, "thesis": s.thesis,
            "source_posts": s.source_posts, "metadata": s.metadata_,
            "created_at": s.created_at.isoformat(), "expires_at": s.expires_at.isoformat() if s.expires_at else None,
        }
        for s in signals
    ]


# ── Accuracy (must be before {signal_id} to avoid path conflict) ──

@router.get("/signals/accuracy/stats")
async def accuracy_stats():
    settings = get_settings()
    if settings.mock_mode:
        return {"total_feedback": 0, "accuracy_rate": 0, "by_signal_type": {}, "by_platform": {}, "by_ticker": {}, "by_direction": {}, "by_priority_range": {}}

    from alekfi.db.database import get_session
    from alekfi.db.models import Signal, SignalFeedback
    from sqlalchemy import select, func, case, text
    async with get_session() as session:
        total = (await session.execute(select(func.count(SignalFeedback.id)))).scalar() or 0
        accurate = (await session.execute(
            select(func.count(SignalFeedback.id)).where(SignalFeedback.was_accurate == True)  # noqa
        )).scalar() or 0

        # By signal_type
        type_rows = (await session.execute(
            select(
                Signal.signal_type,
                func.count(SignalFeedback.id).label("total"),
                func.sum(case((SignalFeedback.was_accurate == True, 1), else_=0)).label("accurate"),  # noqa
            )
            .join(Signal, SignalFeedback.signal_id == Signal.id)
            .group_by(Signal.signal_type)
        )).all()
        by_signal_type = {
            r.signal_type: {"total": r.total, "accurate": int(r.accurate or 0), "rate": round((r.accurate or 0) / max(r.total, 1), 3)}
            for r in type_rows
        }

        # By direction
        dir_rows = (await session.execute(
            select(
                Signal.direction,
                func.count(SignalFeedback.id).label("total"),
                func.sum(case((SignalFeedback.was_accurate == True, 1), else_=0)).label("accurate"),  # noqa
            )
            .join(Signal, SignalFeedback.signal_id == Signal.id)
            .group_by(Signal.direction)
        )).all()
        by_direction = {
            r.direction: {"total": r.total, "accurate": int(r.accurate or 0), "rate": round((r.accurate or 0) / max(r.total, 1), 3)}
            for r in dir_rows
        }

        # By platform — unpack source_posts JSONB (own session)
        by_platform = {}
        try:
            async with get_session() as plat_session:
                plat_rows = (await plat_session.execute(text("""
                    SELECT sp->>'platform' AS platform,
                           COUNT(*) AS total,
                           SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                    FROM signal_feedback sf
                    JOIN signals s ON sf.signal_id = s.id
                    CROSS JOIN LATERAL jsonb_array_elements(s.source_posts) AS sp
                    WHERE sp->>'platform' IS NOT NULL
                    GROUP BY sp->>'platform'
                    ORDER BY total DESC
                """))).all()
                by_platform = {
                    r.platform: {"total": r.total, "accurate": int(r.accurate or 0), "rate": round((r.accurate or 0) / max(r.total, 1), 3)}
                    for r in plat_rows
                }
        except Exception:
            pass

        # By ticker — unpack affected_instruments JSONB (own session)
        by_ticker = {}
        try:
            async with get_session() as ticker_session:
                ticker_rows = (await ticker_session.execute(text("""
                    SELECT ai->>'symbol' AS symbol,
                           COUNT(*) AS total,
                           SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                    FROM signal_feedback sf
                    JOIN signals s ON sf.signal_id = s.id
                    CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                    WHERE ai->>'symbol' IS NOT NULL
                    GROUP BY ai->>'symbol'
                    ORDER BY total DESC
                """))).all()
                by_ticker = {
                    r.symbol: {"total": r.total, "accurate": int(r.accurate or 0), "rate": round((r.accurate or 0) / max(r.total, 1), 3)}
                    for r in ticker_rows
                }
        except Exception:
            pass

        # By priority range (own session)
        by_priority_range = {}
        try:
            async with get_session() as prio_session:
                prio_rows = (await prio_session.execute(text("""
                    SELECT
                        CASE
                            WHEN (s.metadata->>'priority_score')::int >= 80 THEN 'CRITICAL'
                            WHEN (s.metadata->>'priority_score')::int >= 60 THEN 'HIGH'
                            WHEN (s.metadata->>'priority_score')::int >= 40 THEN 'MODERATE'
                            ELSE 'LOW'
                        END AS prio_range,
                        COUNT(*) AS total,
                        SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                    FROM signal_feedback sf
                    JOIN signals s ON sf.signal_id = s.id
                    WHERE s.metadata->>'priority_score' IS NOT NULL
                    GROUP BY prio_range
                    ORDER BY total DESC
                """))).all()
                by_priority_range = {
                    r.prio_range: {"total": r.total, "accurate": int(r.accurate or 0), "rate": round((r.accurate or 0) / max(r.total, 1), 3)}
                    for r in prio_rows
                }
        except Exception:
            pass

    return {
        "total_feedback": total,
        "accuracy_rate": round(accurate / max(total, 1), 3),
        "by_signal_type": by_signal_type,
        "by_platform": by_platform,
        "by_ticker": by_ticker,
        "by_direction": by_direction,
        "by_priority_range": by_priority_range,
    }




@router.get("/signals/forecasts")
async def list_forecasts(
    horizon: str = Query("1d", pattern="^(1h|4h|1d|3d|7d)$"),
    limit: int = Query(25, ge=1, le=200),
    min_tier: str | None = Query(None, pattern="^(CRITICAL|HIGH|MODERATE|LOW)$"),
    strict: bool = Query(True),
):
    """Ranked, trade-ready forecasts.

    Uses the per-signal `metadata.research` bundle if present.
    """
    settings = get_settings()
    if settings.mock_mode:
        return []

    from alekfi.db.database import get_session
    from alekfi.db.models import Signal
    from sqlalchemy import select

    def tier_rank(t: str) -> int:
        return {"CRITICAL": 4, "HIGH": 3, "MODERATE": 2, "LOW": 1}.get((t or "").upper(), 0)

    def ev_pct_from_bundle(bundle: dict) -> float:
        try:
            learned = bundle.get("learned") or {}
            by_h = learned.get("expected_pnl_pct_by_horizon") or {}
            if horizon in by_h:
                return float(by_h.get(horizon) or -999.0)
            fc = (bundle.get("forecast") or {}).get("by_horizon") or {}
            fh = fc.get(horizon) or {}
            p_up = float(fh.get("p_up") or 0)
            p_down = float(fh.get("p_down") or 0)
            exp = float(fh.get("expected_move_pct") or 0)
            adv = float(fh.get("expected_adverse_pct") or 0)
            trad = bundle.get("tradability") or {}
            cost_bps = float(trad.get("spread_bps_est") or 25.0) * 1.5
            cost_pct = (cost_bps / 10000.0) * 100.0
            risk_penalty = 0.0
            if not trad.get("pass", False):
                risk_penalty += 0.4
            if trad.get("reasons") and "missing_market_data" in trad.get("reasons"):
                risk_penalty += 0.35
            return p_up * exp - p_down * adv - cost_pct - risk_penalty
        except Exception:
            return -999.0

    async with get_session() as session:
        # Pull more than we need; we'll filter + sort in-process.
        stmt = select(Signal).order_by(Signal.created_at.desc()).limit(500)
        rows = (await session.execute(stmt)).scalars().all()

    out = []
    for s in rows:
        meta = s.metadata_ or {}
        bundle = (meta.get("research") or {}) if isinstance(meta, dict) else {}
        if not bundle:
            continue

        # Hard controls: only trade-ready signals
        controls = bundle.get("controls") or {}
        if not controls.get("pass", True):
            continue

        if strict:
            trad = bundle.get('tradability') or {}
            ev = bundle.get('evidence') or {}
            controls = bundle.get("controls") or {}
            verified_exception = bool(controls.get("verified_single_modality_ok"))
            if not trad.get('pass', False):
                continue
            if float(ev.get('independence_score') or 0.0) < 0.45 and not verified_exception:
                continue
            if int(ev.get('unique_platforms') or 0) < 2 and not verified_exception:
                continue

        tier = meta.get("intelligence_tier") or meta.get("priority_label") or ""
        if min_tier and tier_rank(tier) < tier_rank(min_tier):
            continue

        ev_pct = ev_pct_from_bundle(bundle)
        forecast_h = (((bundle.get("forecast") or {}).get("by_horizon") or {}).get(horizon) or {})
        out.append(
            {
                "id": str(s.id),
                "created_at": s.created_at.isoformat(),
                "tier": tier,
                "signal_type": s.signal_type,
                "direction": s.direction,
                "conviction": float(s.conviction),
                "claim": bundle.get("claim"),
                "evidence": {
                    k: (bundle.get("evidence") or {}).get(k)
                    for k in (
                        "independence_score",
                        "unique_platforms",
                        "unique_domains",
                        "unique_authors",
                        "echo_ratio",
                        "origin_time",
                        "independence_breakdown",
                        "verification_hits",
                        "source_credibility",
                    )
                },
                "forecast": forecast_h,
                "tradability": bundle.get("tradability"),
                "learned": bundle.get("learned"),
                "controls": bundle.get("controls"),
                "expected_pnl_pct": round(ev_pct, 4),
                "calibrated_prob": round(float(forecast_h.get("p_up") or 0.0), 4),
                "expected_return_pct": round(float(forecast_h.get("expected_move_pct") or 0.0), 4),
                "expected_time_bucket": forecast_h.get("expected_time_to_move_hours"),
                "thesis": s.thesis,
            }
        )

    out.sort(key=lambda r: r.get("expected_pnl_pct", -999.0), reverse=True)
    return out[:limit]
# ── Detail + Feedback (path param routes last) ──────────────────────

@router.get("/signals/{signal_id}")
async def get_signal(signal_id: str):
    settings = get_settings()
    if settings.mock_mode:
        from alekfi.api.mock_data import get_mock_provider
        sig = get_mock_provider().get_signal(signal_id)
        if not sig:
            raise HTTPException(404, "Signal not found")
        return sig

    import uuid as _uuid
    from alekfi.db.database import get_session
    from alekfi.db.models import Signal
    async with get_session() as session:
        sig = await session.get(Signal, _uuid.UUID(signal_id))
        if not sig:
            raise HTTPException(404, "Signal not found")
    return {
        "id": str(sig.id), "signal_type": sig.signal_type,
        "affected_instruments": sig.affected_instruments,
        "direction": sig.direction, "conviction": sig.conviction,
        "time_horizon": sig.time_horizon, "thesis": sig.thesis,
        "source_posts": sig.source_posts, "metadata": sig.metadata_,
        "created_at": sig.created_at.isoformat(), "expires_at": sig.expires_at.isoformat() if sig.expires_at else None,
    }


class FeedbackRequest(BaseModel):
    type: str  # "thumbs_up" | "thumbs_down"
    notes: str | None = None


@router.post("/signals/{signal_id}/feedback")
async def submit_feedback(signal_id: str, body: FeedbackRequest):
    settings = get_settings()
    if settings.mock_mode:
        return {"status": "ok", "signal_id": signal_id, "feedback": body.type}

    import uuid as _uuid
    from alekfi.db.database import get_session
    from alekfi.db.models import Signal, SignalFeedback
    async with get_session() as session:
        sig = await session.get(Signal, _uuid.UUID(signal_id))
        if not sig:
            raise HTTPException(404, "Signal not found")
        fb = SignalFeedback(
            id=_uuid.uuid4(),
            signal_id=sig.id,
            was_accurate=body.type == "thumbs_up",
            feedback_type=body.type,
            notes=body.notes,
        )
        session.add(fb)
    return {"status": "ok", "signal_id": signal_id, "feedback": body.type}


# ── Company Rankings ────────────────────────────────────────────────

@router.get("/companies/rankings")
async def company_rankings(
    limit: int = Query(30, ge=1, le=100),
    hours: int = Query(24, ge=1, le=168),
):
    """Companies ranked by composite signal strength."""
    settings = get_settings()
    if settings.mock_mode:
        return []

    from datetime import datetime, timedelta, timezone
    from alekfi.db.database import get_session
    from sqlalchemy import text
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    async with get_session() as session:
        rows = (await session.execute(text("""
            WITH ticker_signals AS (
                SELECT
                    ai->>'symbol' AS ticker,
                    s.id AS signal_id,
                    s.signal_type,
                    s.direction,
                    s.conviction,
                    s.thesis,
                    s.time_horizon,
                    s.created_at,
                    COALESCE((s.metadata->>'priority_score')::float, 50) AS priority_score,
                    COALESCE(s.metadata->>'suggested_action', 'WATCH') AS suggested_action
                FROM signals s
                CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                WHERE s.created_at >= :cutoff
                  AND ai->>'symbol' IS NOT NULL
            ),
            ticker_agg AS (
                SELECT
                    ticker,
                    COUNT(*) AS signal_count,
                    ROUND(AVG(priority_score)::numeric, 1) AS avg_priority,
                    MAX(priority_score) AS max_priority,
                    ROUND(AVG(conviction)::numeric, 3) AS avg_conviction,
                    MODE() WITHIN GROUP (ORDER BY direction) AS dominant_direction,
                    MODE() WITHIN GROUP (ORDER BY suggested_action) AS suggested_action
                FROM ticker_signals
                GROUP BY ticker
            ),
            ticker_accuracy AS (
                SELECT
                    ai->>'symbol' AS ticker,
                    COUNT(*) AS fb_total,
                    SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS fb_accurate
                FROM signal_feedback sf
                JOIN signals s ON sf.signal_id = s.id
                CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                WHERE ai->>'symbol' IS NOT NULL
                GROUP BY ai->>'symbol'
            )
            SELECT
                ta.ticker,
                ta.signal_count,
                ta.avg_priority,
                ta.max_priority,
                ta.avg_conviction,
                ta.dominant_direction,
                ta.suggested_action,
                COALESCE(ROUND((tac.fb_accurate::numeric / NULLIF(tac.fb_total, 0)), 3), NULL) AS accuracy_rate,
                tac.fb_total AS feedback_count
            FROM ticker_agg ta
            LEFT JOIN ticker_accuracy tac ON ta.ticker = tac.ticker
            ORDER BY (ta.avg_priority * ta.avg_conviction) DESC
            LIMIT :lim
        """), {"cutoff": cutoff, "lim": limit})).all()

        # Fetch recent signals for each ticker to include thesis + summaries
        result = []
        for r in rows:
            sig_rows = (await session.execute(text("""
                SELECT s.thesis, s.signal_type, s.direction, s.conviction,
                       COALESCE((s.metadata->>'priority_score')::int, 50) AS priority_score,
                       s.created_at
                FROM signals s
                CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                WHERE ai->>'symbol' = :ticker AND s.created_at >= :cutoff
                ORDER BY s.created_at DESC
                LIMIT 5
            """), {"ticker": r.ticker, "cutoff": cutoff})).all()

            signals_summary = [
                {
                    "signal_type": sr.signal_type,
                    "direction": sr.direction,
                    "conviction": float(sr.conviction),
                    "priority_score": sr.priority_score,
                    "created_at": sr.created_at.isoformat(),
                }
                for sr in sig_rows
            ]

            result.append({
                "ticker": r.ticker,
                "signal_count": r.signal_count,
                "avg_priority": float(r.avg_priority),
                "max_priority": float(r.max_priority),
                "avg_conviction": float(r.avg_conviction),
                "dominant_direction": r.dominant_direction,
                "suggested_action": r.suggested_action,
                "accuracy_rate": float(r.accuracy_rate) if r.accuracy_rate is not None else None,
                "feedback_count": r.feedback_count or 0,
                "latest_thesis": sig_rows[0].thesis if sig_rows else "",
                "signals": signals_summary,
            })

    return result


# ── Sources Feed ───────────────────────────────────────────────────

@router.get("/sources/feed")
async def source_feed(limit: int = Query(30, ge=1, le=100)):
    """Recent source posts grouped by type (news, social, official)."""
    settings = get_settings()
    if settings.mock_mode:
        return {"news": [], "social": [], "official": []}

    from datetime import datetime, timedelta, timezone
    from alekfi.db.database import get_session
    from sqlalchemy import text
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    async with get_session() as session:
        rows = (await session.execute(text("""
            SELECT
                sp->>'platform' AS platform,
                sp->>'headline' AS headline,
                sp->>'url' AS url,
                sp->>'author' AS author,
                sp->>'post_id' AS post_id,
                sp->>'relevance' AS relevance,
                s.id::text AS signal_id,
                s.created_at
            FROM signals s
            CROSS JOIN LATERAL jsonb_array_elements(s.source_posts) AS sp
            WHERE s.created_at >= :cutoff
              AND sp->>'platform' IS NOT NULL
            ORDER BY s.created_at DESC
            LIMIT :lim
        """), {"cutoff": cutoff, "lim": limit * 3})).all()

    news_platforms = {"news_rss", "sec_edgar", "patent", "news"}
    social_platforms = {"reddit", "twitter", "hackernews", "discord", "telegram", "youtube", "x"}
    official_platforms = {"sec_edgar", "patent", "government"}

    news, social, official = [], [], []
    seen = set()
    for r in rows:
        key = (r.platform, r.headline or r.post_id or r.url)
        if key in seen:
            continue
        seen.add(key)
        item = {
            "platform": r.platform,
            "headline": r.headline,
            "url": r.url,
            "author": r.author,
            "signal_id": r.signal_id,
            "created_at": r.created_at.isoformat(),
        }
        plat = (r.platform or "").lower()
        if plat in official_platforms:
            if len(official) < limit:
                official.append(item)
        if plat in news_platforms or "rss" in plat or "news" in plat:
            if len(news) < limit:
                news.append(item)
        elif plat in social_platforms:
            if len(social) < limit:
                social.append(item)
        else:
            if len(news) < limit:
                news.append(item)

    return {"news": news, "social": social, "official": official}
