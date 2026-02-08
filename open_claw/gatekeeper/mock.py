"""MockGatekeeper — simulates the Tier 2 filter without LLM calls.

Randomly keeps ~10-15% of posts with plausible urgency/category assignments.
Works with both RedisQueue and _MockQueue.
"""

from __future__ import annotations

import logging
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from open_claw.config import Settings

logger = logging.getLogger(__name__)

VALID_CATEGORIES = [
    "earnings", "macro", "geopolitical", "consumer", "supply_chain",
    "regulatory", "labor", "commodity", "technology", "sentiment_shift",
    "insider_activity", "corporate_action", "competitor", "product_launch",
    "legal", "environmental", "infrastructure", "crypto", "real_estate",
    "healthcare",
]

# Weight categories so the distribution is realistic
_CATEGORY_WEIGHTS = [
    12, 10, 8, 9, 6, 7, 5, 8, 11, 4,
    3, 5, 4, 6, 3, 2, 2, 7, 4, 5,
]

_MOCK_REASONING = [
    "Mentions specific company earnings with revenue figures",
    "Contains material insider trade data from SEC filing",
    "Describes supply chain disruption affecting major industry",
    "Geopolitical event with direct commodity price implications",
    "Consumer sentiment shift with evidence of brand damage",
    "Regulatory action affecting publicly traded company",
    "Central bank policy signal with rate implications",
    "Unusual options activity suggesting institutional positioning",
    "Product quality issue at scale — potential recall risk",
    "Cross-border trade policy change affecting sector",
    "Macroeconomic data release diverging from consensus",
    "Corporate restructuring announcement affecting workforce",
    "Technology disruption threatening incumbent business model",
    "Emerging market crisis with contagion potential",
    "Commodity supply deficit with price pressure building",
]


class MockGatekeeper:
    """Drop-in replacement for GatekeeperProcessor. No LLM, no database."""

    def __init__(self, config: Settings, queue: Any) -> None:
        self._config = config
        self._queue = queue
        self._batch_size = config.gatekeeper_batch_size
        self._keep_rate = random.uniform(0.10, 0.15)

        # stats
        self.total_processed = 0
        self.total_kept = 0
        self.total_killed = 0
        self.batches_processed = 0
        self.total_batch_time = 0.0

        # output buffer for downstream (Brain)
        self.filtered_posts: list[dict[str, Any]] = []

    async def process_batch(self) -> dict[str, int]:
        """Pop a batch, randomly filter, store survivors."""
        posts = await self._queue.pop_raw_posts(self._batch_size)
        if not posts:
            return {"total": 0, "kept": 0, "killed": 0, "errors": 0}

        t0 = time.monotonic()
        kept = 0
        killed = 0

        for post in posts:
            if random.random() < self._keep_rate:
                urgency = random.choices(
                    ["HIGH", "MEDIUM", "LOW"],
                    weights=[15, 35, 50],
                    k=1,
                )[0]
                category = random.choices(VALID_CATEGORIES, weights=_CATEGORY_WEIGHTS, k=1)[0]
                relevance_score = round(random.uniform(0.5, 1.0), 2)
                reasoning = random.choice(_MOCK_REASONING)

                filtered = {
                    "id": str(uuid.uuid4()),
                    "raw_post_id": post.get("id", str(uuid.uuid4())),
                    "platform": post.get("platform", "unknown"),
                    "author": post.get("author", "unknown"),
                    "content": post.get("content", ""),
                    "url": post.get("url"),
                    "raw_metadata": post.get("raw_metadata", {}),
                    "relevance_score": relevance_score,
                    "urgency": urgency,
                    "category": category,
                    "gatekeeper_reasoning": reasoning,
                    "filtered_at": datetime.now(timezone.utc).isoformat(),
                    "analyzed": False,
                }
                self.filtered_posts.append(filtered)
                kept += 1
            else:
                killed += 1

        elapsed = time.monotonic() - t0
        self.total_batch_time += elapsed
        self.batches_processed += 1
        self.total_processed += len(posts)
        self.total_kept += kept
        self.total_killed += killed

        logger.info(
            "[gatekeeper-mock] batch %d: %d posts → %d kept, %d killed (%.3fs)",
            self.batches_processed, len(posts), kept, killed, elapsed,
        )
        return {"total": len(posts), "kept": kept, "killed": killed, "errors": 0}

    async def run(self, once: bool = False) -> None:
        """Process all available posts in the queue."""
        logger.info("[gatekeeper-mock] starting (batch_size=%d, keep_rate=%.0f%%)", self._batch_size, self._keep_rate * 100)
        while True:
            stats = await self.process_batch()
            if once:
                return
            if stats["total"] == 0:
                await __import__("asyncio").sleep(2)
            else:
                await __import__("asyncio").sleep(0.5)

    def get_stats(self) -> dict[str, Any]:
        avg_time = (self.total_batch_time / self.batches_processed) if self.batches_processed else 0
        return {
            "total_processed": self.total_processed,
            "total_kept": self.total_kept,
            "total_killed": self.total_killed,
            "batches_processed": self.batches_processed,
            "avg_batch_time": round(avg_time, 4),
            "kill_rate": round(self.total_killed / max(self.total_processed, 1), 3),
        }
