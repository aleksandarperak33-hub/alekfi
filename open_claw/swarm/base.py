"""Abstract base scraper that all Swarm agents inherit from."""

from __future__ import annotations

import abc
import asyncio
import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from open_claw.queue import RedisQueue

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
        }

    @staticmethod
    def _generate_id() -> str:
        return uuid.uuid4().hex[:12]

    # ── run loop ───────────────────────────────────────────────────────

    async def run_once(self, queue: RedisQueue) -> int:
        """Execute a single scrape pass and push results to queue."""
        try:
            posts = await self.scrape()
            if posts:
                await queue.push_raw_posts(posts)
                self._total_posts += len(posts)
            logger.info(
                "[%s] scraped %d posts (total: %d)",
                self.platform,
                len(posts),
                self._total_posts,
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
            "interval": self.interval,
        }
