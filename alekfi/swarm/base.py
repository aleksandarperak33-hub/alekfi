"""Abstract base scraper that all Swarm agents inherit from."""

from __future__ import annotations

import abc
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from alekfi.queue import RedisQueue

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    """Every Tier-1 scraper inherits from this.

    Subclasses must implement ``platform`` and ``scrape()``.
    The ``run_loop`` method handles the scrape → queue → sleep cycle.
    """

    def __init__(self, interval: int = 60) -> None:
        self.interval = interval
        self._error_count = 0
        self._total_posts = 0
        self._dupes_skipped = 0

    # ── abstract interface ─────────────────────────────────────────────

    @property
    @abc.abstractmethod
    def platform(self) -> str:
        """Short platform identifier, e.g. 'reddit', 'twitter'."""

    @abc.abstractmethod
    async def scrape(self) -> list[dict[str, Any]]:
        """Fetch raw posts and return them as dicts.

        Each dict must have the keys:
            id, platform, author, content, url, raw_metadata, scraped_at
        """

    # ── helpers ────────────────────────────────────────────────────────

    def _make_post(
        self,
        source_id: str,
        author: str,
        content: str,
        url: str | None = None,
        raw_metadata: dict[str, Any] | None = None,
        source_published_at: str | None = None,
    ) -> dict[str, Any]:
        """Build a standardised post dict."""
        return {
            "id": f"{self.platform}_{source_id}",
            "platform": self.platform,
            "author": author,
            "content": content,
            "url": url,
            "raw_metadata": raw_metadata or {},
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "source_published_at": source_published_at,
        }

    @staticmethod
    def _generate_id() -> str:
        return uuid.uuid4().hex[:12]

    # ── deduplication ─────────────────────────────────────────────────

    async def _dedup_posts(self, posts: list[dict[str, Any]], redis_client) -> list[dict[str, Any]]:
        """Filter out posts we've already seen using a Redis SET with 24h TTL."""
        if not posts or redis_client is None:
            return posts
        key = f"alekfi:seen:{self.platform}"
        new_posts = []
        for post in posts:
            post_id = post.get("id", "")
            if not post_id:
                new_posts.append(post)
                continue
            try:
                added = await redis_client.sadd(key, post_id)
                if added:
                    new_posts.append(post)
                else:
                    self._dupes_skipped += 1
            except Exception:
                new_posts.append(post)  # on error, let it through
        # Refresh TTL
        try:
            await redis_client.expire(key, 86400)
        except Exception:
            pass
        return new_posts

    # ── run loop ───────────────────────────────────────────────────────

    async def run_once(self, queue: RedisQueue) -> int:
        """Execute a single scrape pass, dedup, and push results to queue."""
        try:
            posts = await self.scrape()
            # Dedup via Redis before pushing to queue
            redis_client = getattr(queue, '_redis', None)
            original_count = len(posts)
            if posts and redis_client:
                posts = await self._dedup_posts(posts, redis_client)
            duped = original_count - len(posts)
            if posts:
                await queue.push_raw_posts(posts)
                self._total_posts += len(posts)
            logger.info(
                "[%s] scraped %d items, %d dupes skipped, %d new pushed (total: %d)",
                self.platform, original_count, duped, len(posts), self._total_posts,
            )
            return len(posts)
        except Exception:
            self._error_count += 1
            logger.exception("[%s] scrape error (#%d)", self.platform, self._error_count)
            return 0

    async def run_loop(self, queue: RedisQueue, once: bool = False) -> None:
        """Continuous scrape loop: scrape → push → sleep → repeat."""
        logger.info("[%s] starting scraper (interval=%ds)", self.platform, self.interval)
        while True:
            await self.run_once(queue)
            if once:
                return
            await asyncio.sleep(self.interval)

    # ── stats ──────────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "total_posts": self._total_posts,
            "error_count": self._error_count,
            "dupes_skipped": self._dupes_skipped,
            "interval": self.interval,
        }
