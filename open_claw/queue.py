"""Redis-backed async queue for passing data between pipeline tiers."""

from __future__ import annotations

import json
import logging
from typing import Any

import redis.asyncio as aioredis

from open_claw.config import get_settings

logger = logging.getLogger(__name__)

_KEY_RAW = "openclaw:raw"
_KEY_FILTERED = "openclaw:filtered"
_KEY_STATS_PUSHED = "openclaw:stats:pushed"
_KEY_STATS_POPPED = "openclaw:stats:popped"


class RedisQueue:
    """Lightweight async wrapper around Redis lists for the scrape pipeline."""

    def __init__(self, redis_url: str | None = None) -> None:
        url = redis_url or get_settings().redis_url
        self._redis = aioredis.from_url(url, decode_responses=True)

    # ── raw post queue ─────────────────────────────────────────────────

    async def push_raw_posts(self, posts: list[dict[str, Any]]) -> int:
        """RPUSH serialised posts onto the raw queue. Returns count pushed."""
        if not posts:
            return 0
        pipe = self._redis.pipeline()
        for post in posts:
            pipe.rpush(_KEY_RAW, json.dumps(post, default=str))
        pipe.incrby(_KEY_STATS_PUSHED, len(posts))
        await pipe.execute()
        logger.debug("Pushed %d raw posts to queue", len(posts))
        return len(posts)

    async def pop_raw_posts(self, batch_size: int = 100) -> list[dict[str, Any]]:
        """LPOP up to *batch_size* items from the raw queue."""
        pipe = self._redis.pipeline()
        for _ in range(batch_size):
            pipe.lpop(_KEY_RAW)
        results = await pipe.execute()
        posts = [json.loads(r) for r in results if r is not None]
        if posts:
            await self._redis.incrby(_KEY_STATS_POPPED, len(posts))
        logger.debug("Popped %d raw posts from queue", len(posts))
        return posts

    # ── filtered overflow / backup queue ───────────────────────────────

    async def push_filtered(self, posts: list[dict[str, Any]]) -> int:
        """Push filtered posts to a backup queue."""
        if not posts:
            return 0
        pipe = self._redis.pipeline()
        for post in posts:
            pipe.rpush(_KEY_FILTERED, json.dumps(post, default=str))
        await pipe.execute()
        return len(posts)

    # ── stats ──────────────────────────────────────────────────────────

    async def get_stats(self) -> dict[str, Any]:
        """Return queue lengths and throughput counters."""
        pipe = self._redis.pipeline()
        pipe.llen(_KEY_RAW)
        pipe.llen(_KEY_FILTERED)
        pipe.get(_KEY_STATS_PUSHED)
        pipe.get(_KEY_STATS_POPPED)
        raw_len, filtered_len, pushed, popped = await pipe.execute()
        return {
            "raw_queue_length": raw_len,
            "filtered_queue_length": filtered_len,
            "total_pushed": int(pushed or 0),
            "total_popped": int(popped or 0),
        }

    # ── lifecycle ──────────────────────────────────────────────────────

    async def close(self) -> None:
        await self._redis.aclose()
