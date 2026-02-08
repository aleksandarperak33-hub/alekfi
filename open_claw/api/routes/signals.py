"""Signal endpoints — list, detail, and summary."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from open_claw.config import get_settings

router = APIRouter(tags=["signals"])


@router.get("/signals/summary")
async def signals_summary():
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_signals_summary()

    from open_claw.db.database import get_session
    from open_claw.db.models import Signal
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
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_signals(limit, signal_type, direction, min_conviction, since)

    from open_claw.db.database import get_session
    from open_claw.db.models import Signal
    from open_claw.utils import parse_iso
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


@router.get("/signals/{signal_id}")
async def get_signal(signal_id: str):
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        sig = get_mock_provider().get_signal(signal_id)
        if not sig:
            raise HTTPException(404, "Signal not found")
        return sig

    import uuid as _uuid
    from open_claw.db.database import get_session
    from open_claw.db.models import Signal
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
