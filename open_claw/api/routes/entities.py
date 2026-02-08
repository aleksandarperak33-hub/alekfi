"""Entity endpoints — list, detail, and sentiment time series."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from open_claw.config import get_settings

router = APIRouter(tags=["entities"])


@router.get("/entities")
async def list_entities(
    limit: int = Query(50, ge=1, le=200),
    entity_type: str | None = None,
    search: str | None = None,
):
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_entities(limit, entity_type, search)

    from open_claw.db.database import get_session
    from open_claw.db.models import Entity, SentimentScore
    from sqlalchemy import select, func
    async with get_session() as session:
        stmt = select(Entity).order_by(Entity.created_at.desc()).limit(limit)
        if entity_type:
            stmt = stmt.where(Entity.entity_type == entity_type)
        if search:
            stmt = stmt.where(Entity.name.ilike(f"%{search}%"))
        result = await session.execute(stmt)
        entities = result.scalars().all()

        out = []
        for e in entities:
            latest_stmt = (
                select(SentimentScore)
                .where(SentimentScore.entity_id == e.id)
                .order_by(SentimentScore.scored_at.desc())
                .limit(1)
            )
            latest = (await session.execute(latest_stmt)).scalar_one_or_none()
            count_stmt = select(func.count(SentimentScore.id)).where(SentimentScore.entity_id == e.id)
            mention_count = (await session.execute(count_stmt)).scalar() or 0

            out.append({
                "id": str(e.id), "name": e.name, "entity_type": e.entity_type,
                "ticker": e.ticker, "related_tickers": e.related_tickers,
                "created_at": e.created_at.isoformat(),
                "latest_sentiment": latest.sentiment if latest else None,
                "sentiment_confidence": latest.confidence if latest else None,
                "mention_count": mention_count,
            })
    return out


@router.get("/entities/{entity_id}")
async def get_entity(entity_id: str):
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        ent = get_mock_provider().get_entity(entity_id)
        if not ent:
            raise HTTPException(404, "Entity not found")
        return ent

    import uuid as _uuid
    from open_claw.db.database import get_session
    from open_claw.db.models import Entity, SentimentScore
    from sqlalchemy import select
    async with get_session() as session:
        ent = await session.get(Entity, _uuid.UUID(entity_id))
        if not ent:
            raise HTTPException(404, "Entity not found")
        scores_stmt = (
            select(SentimentScore).where(SentimentScore.entity_id == ent.id)
            .order_by(SentimentScore.scored_at.desc()).limit(100)
        )
        scores = (await session.execute(scores_stmt)).scalars().all()
    return {
        "id": str(ent.id), "name": ent.name, "entity_type": ent.entity_type,
        "ticker": ent.ticker, "related_tickers": ent.related_tickers,
        "metadata": ent.metadata_, "created_at": ent.created_at.isoformat(),
        "sentiment_scores": [
            {"id": str(s.id), "sentiment": s.sentiment, "confidence": s.confidence,
             "urgency": s.urgency, "reasoning": s.reasoning, "themes": s.themes,
             "mechanism": s.mechanism, "scored_at": s.scored_at.isoformat()}
            for s in scores
        ],
    }


@router.get("/entities/{entity_id}/sentiment")
async def entity_sentiment(entity_id: str):
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_entity_sentiment(entity_id)

    import uuid as _uuid
    from open_claw.db.database import get_session
    from open_claw.db.models import SentimentScore
    from sqlalchemy import select
    async with get_session() as session:
        stmt = (
            select(SentimentScore).where(SentimentScore.entity_id == _uuid.UUID(entity_id))
            .order_by(SentimentScore.scored_at.asc()).limit(200)
        )
        scores = (await session.execute(stmt)).scalars().all()
    return [
        {"scored_at": s.scored_at.isoformat(), "sentiment": s.sentiment, "confidence": s.confidence}
        for s in scores
    ]
