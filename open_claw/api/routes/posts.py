"""Post endpoints — filtered posts and raw post stats."""

from __future__ import annotations

from fastapi import APIRouter, Query
from open_claw.config import get_settings

router = APIRouter(tags=["posts"])


@router.get("/posts/filtered")
async def list_filtered_posts(
    limit: int = Query(50, ge=1, le=200),
    urgency: str | None = None,
    category: str | None = None,
    since: str | None = None,
):
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_filtered_posts(limit, urgency, category, since)

    from open_claw.db.database import get_session
    from open_claw.db.models import FilteredPost, RawPost
    from open_claw.utils import parse_iso
    from sqlalchemy import select
    async with get_session() as session:
        stmt = select(FilteredPost).order_by(FilteredPost.filtered_at.desc()).limit(limit)
        if urgency:
            stmt = stmt.where(FilteredPost.urgency == urgency)
        if category:
            stmt = stmt.where(FilteredPost.category == category)
        if since:
            stmt = stmt.where(FilteredPost.filtered_at >= parse_iso(since))
        result = await session.execute(stmt)
        posts = result.scalars().all()

        out = []
        for fp in posts:
            raw = await session.get(RawPost, fp.raw_post_id)
            out.append({
                "id": str(fp.id), "raw_post_id": str(fp.raw_post_id),
                "platform": raw.platform if raw else "unknown",
                "author": raw.author if raw else "unknown",
                "content": raw.content[:300] if raw else "",
                "relevance_score": fp.relevance_score, "urgency": fp.urgency,
                "category": fp.category, "gatekeeper_reasoning": fp.gatekeeper_reasoning,
                "filtered_at": fp.filtered_at.isoformat(), "analyzed": fp.analyzed,
            })
    return out


@router.get("/posts/raw/stats")
async def raw_post_stats():
    settings = get_settings()
    if settings.mock_mode:
        from open_claw.api.mock_data import get_mock_provider
        return get_mock_provider().get_raw_stats()

    from datetime import datetime, timedelta, timezone
    from open_claw.db.database import get_session
    from open_claw.db.models import RawPost
    from sqlalchemy import select, func
    async with get_session() as session:
        total = (await session.execute(select(func.count(RawPost.id)))).scalar() or 0
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        last_hour = (await session.execute(
            select(func.count(RawPost.id)).where(RawPost.scraped_at >= one_hour_ago)
        )).scalar() or 0
        by_platform = (await session.execute(
            select(RawPost.platform, func.count()).group_by(RawPost.platform)
        )).all()
    return {
        "total_raw": total,
        "by_platform": {r[0]: r[1] for r in by_platform},
        "posts_per_hour": last_hour,
        "last_hour_count": last_hour,
    }
