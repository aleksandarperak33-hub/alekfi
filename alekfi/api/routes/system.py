"""System endpoints — health, stats, config."""

from __future__ import annotations

from fastapi import APIRouter
from alekfi import __version__
from alekfi.config import get_settings

router = APIRouter(tags=["system"])


@router.get("/health")
async def health():
    from alekfi.api.app import get_uptime
    settings = get_settings()

    db_ok = False
    redis_ok = False

    if not settings.mock_mode:
        try:
            from alekfi.db.database import get_session
            from sqlalchemy import text
            async with get_session() as session:
                await session.execute(text("SELECT 1"))
            db_ok = True
        except Exception:
            pass
        try:
            import redis.asyncio as aioredis
            r = aioredis.from_url(settings.redis_url)
            await r.ping()
            await r.aclose()
            redis_ok = True
        except Exception:
            pass
    else:
        db_ok = True
        redis_ok = True

    market_data = await _market_data_health()

    return {
        "status": "ok",
        "version": __version__,
        "uptime_seconds": round(get_uptime(), 1),
        "mock_mode": settings.mock_mode,
        "components": {"db": db_ok, "redis": redis_ok, "market_data": market_data.get("healthy", False)},
        "market_data_health": market_data,
    }


@router.get("/agentos/health")
async def agentos_health():
    """Extended ops health for agent runtime (includes market-data provider status)."""
    base = await health()
    return {
        "status": base.get("status"),
        "version": base.get("version"),
        "components": base.get("components"),
        "market_data_health": base.get("market_data_health"),
        "uptime_seconds": base.get("uptime_seconds"),
    }


@router.get("/stats")
async def stats():
    settings = get_settings()
    if settings.mock_mode:
        from alekfi.api.mock_data import get_mock_provider
        return get_mock_provider().get_stats()

    from datetime import datetime, timedelta, timezone
    from alekfi.db.database import get_session
    from alekfi.db.models import Entity, FilteredPost, RawPost, SentimentScore, Signal
    from sqlalchemy import select, func

    async with get_session() as session:
        raw_total = (await session.execute(select(func.count(RawPost.id)))).scalar() or 0
        one_hour = datetime.now(timezone.utc) - timedelta(hours=1)
        raw_hour = (await session.execute(
            select(func.count(RawPost.id)).where(RawPost.scraped_at >= one_hour)
        )).scalar() or 0
        by_plat = (await session.execute(
            select(RawPost.platform, func.count()).group_by(RawPost.platform)
        )).all()

        filt_total = (await session.execute(select(func.count(FilteredPost.id)))).scalar() or 0
        filt_analyzed = (await session.execute(
            select(func.count(FilteredPost.id)).where(FilteredPost.analyzed == True)  # noqa
        )).scalar() or 0

        ent_total = (await session.execute(select(func.count(Entity.id)))).scalar() or 0
        sent_total = (await session.execute(select(func.count(SentimentScore.id)))).scalar() or 0
        sig_total = (await session.execute(select(func.count(Signal.id)))).scalar() or 0

    kill_rate = round(1 - (filt_total / max(raw_total, 1)), 3) if raw_total else 0

    return {
        "swarm": {
            "total_posts": raw_total,
            "posts_per_minute": round(raw_hour / 60, 1),
            "by_platform": {r[0]: r[1] for r in by_plat},
        },
        "gatekeeper": {
            "total_processed": raw_total,
            "total_kept": filt_total,
            "total_killed": raw_total - filt_total,
            "kill_rate": kill_rate,
        },
        "brain": {
            "posts_analyzed": filt_analyzed,
            "entities_extracted": ent_total,
            "sentiments_scored": sent_total,
            "signals_generated": sig_total,
        },
        "signals": {"total": sig_total},
    }


@router.get("/config")
async def config():
    s = get_settings()
    return {
        "gatekeeper_provider": s.gatekeeper_provider,
        "gatekeeper_model": s.gatekeeper_model,
        "brain_provider": s.brain_provider,
        "brain_model": s.brain_model,
        "scrape_interval_seconds": s.scrape_interval_seconds,
        "gatekeeper_batch_size": s.gatekeeper_batch_size,
        "brain_batch_size": s.brain_batch_size,
        "brain_synthesis_interval": s.brain_synthesis_interval,
        "mock_mode": s.mock_mode,
        "log_level": s.log_level,
    }


@router.get("/metrics/signal_quality")
async def metrics_signal_quality(since_days: int = 14):
    """Tiered precision + horizon metrics from eval harness."""
    settings = get_settings()
    if settings.mock_mode:
        return {"rows": 0, "message": "mock_mode"}
    from alekfi.tools.eval_harness import run_eval
    result = await run_eval(since_days=since_days, topk=[5, 10, 20])
    horizons = result.get("horizons") or {}
    summary = {}
    for h, m in horizons.items():
        if not isinstance(m, dict):
            continue
        summary[h] = {
            "count": m.get("count", 0),
            "hit_rate": m.get("hit_rate"),
            "strict_p@10": m.get("strict_p@10"),
            "non_strict_p@10": m.get("non_strict_p@10"),
        }
    return {"since_days": since_days, "rows": result.get("rows", 0), "summary": summary, "raw": result}


@router.get("/metrics/forecast_calibration")
async def metrics_forecast_calibration(since_days: int = 14):
    """Calibration-focused view (Brier + strict/non-strict split by horizon)."""
    settings = get_settings()
    if settings.mock_mode:
        return {"rows": 0, "message": "mock_mode"}
    from alekfi.tools.eval_harness import run_eval
    result = await run_eval(since_days=since_days, topk=[10])
    horizons = result.get("horizons") or {}
    calibration = {}
    for h, m in horizons.items():
        if not isinstance(m, dict):
            continue
        calibration[h] = {
            "count": m.get("count", 0),
            "brier": m.get("brier"),
            "hit_rate": m.get("hit_rate"),
            "strict_count": m.get("strict_count"),
            "non_strict_count": m.get("non_strict_count"),
        }
    return {"since_days": since_days, "rows": result.get("rows", 0), "calibration": calibration}


async def _market_data_health() -> dict:
    try:
        from alekfi.marketdata import MarketDataGateway

        gw = MarketDataGateway()
        health = await gw.get_provider_health()
        await gw.close()
        providers = health.get("providers") or {}
        any_open = any(bool((v or {}).get("open")) for v in providers.values())
        auth_issues = [
            p for p, v in providers.items()
            if str((v or {}).get("last_error") or "").upper() == "AUTH_FAIL"
        ]
        return {
            "healthy": not any_open,
            "providers": providers,
            "auth_issues": auth_issues,
            "open_circuits": [p for p, v in providers.items() if (v or {}).get("open")],
        }
    except Exception as exc:
        return {"healthy": False, "error": str(exc)}
