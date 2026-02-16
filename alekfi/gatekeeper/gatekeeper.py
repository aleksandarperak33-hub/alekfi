"""GatekeeperProcessor — Tier 2 LLM-powered relevance filter.

Pulls raw posts from Redis, evaluates them via LLM in batches,
saves RawPost records to Postgres, and writes survivors as FilteredPosts.
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from alekfi.config import Settings
from alekfi.db.database import get_session
from alekfi.db.models import FilteredPost, RawPost
from alekfi.llm_client import LLMClient
from alekfi.queue import RedisQueue
from alekfi.utils import parse_iso

logger = logging.getLogger(__name__)

VALID_URGENCIES = {"HIGH", "MEDIUM", "LOW"}
VALID_CATEGORIES = {
    "earnings", "macro", "geopolitical", "consumer", "supply_chain",
    "regulatory", "labor", "commodity", "technology", "sentiment_shift",
    "insider_activity", "corporate_action", "competitor", "product_launch",
    "legal", "environmental", "infrastructure", "crypto", "real_estate",
    "healthcare",
}

SYSTEM_PROMPT = """You are a financial relevance filter for a hedge fund intelligence system. Your job is to decide which social media posts contain actionable financial intelligence and which are noise.

For EACH numbered post, output a JSON object. Return a JSON array of objects, one per post, in order.

Each object must have:
- "relevant": boolean — true if the post contains actionable financial intelligence
- "relevance_score": float 0-1 — how relevant/important this is
- "urgency": "HIGH" | "MEDIUM" | "LOW" — how time-sensitive
- "relevance_tier": one of:
  - "breaking": Major market-moving event. M&A, earnings surprise, insider trade, regulatory action, geopolitical crisis. Needs IMMEDIATE analysis.
  - "significant": Clearly market-relevant. Company news, industry trend, consumer sentiment shift.
  - "contextual": Background info useful for patterns but not urgent.
  - "noise": Not relevant despite passing keyword filters.
- "category": one of: earnings, macro, geopolitical, consumer, supply_chain, regulatory, labor, commodity, technology, sentiment_shift, insider_activity, corporate_action, competitor, product_launch, legal, environmental, infrastructure, crypto, real_estate, healthcare
- "reasoning": string — brief explanation of your decision

KILL (relevant=false) anything that is:
- Personal opinion without substance or data
- Memes with no financial signal
- Generic financial advice ("diversify your portfolio")
- Spam or self-promotion
- Old news (>48 hours old) being reposted
- Vague speculation without specific claims

KEEP (relevant=true) anything mentioning:
- Specific companies, earnings, revenue numbers, guidance
- Layoffs, hiring freezes, restructuring
- Product quality issues, recalls, safety concerns
- Regulatory actions, FDA approvals, SEC filings
- Insider trades, institutional holdings changes
- Supply chain disruptions, shipping delays
- Geopolitical events affecting markets
- Consumer sentiment shifts, boycotts, viral complaints
- Price movements, unusual trading volume
- Central bank actions, interest rate changes, inflation data

Be aggressive — kill 85-90% of posts. Only keep genuine signal."""


VALID_RELEVANCE_TIERS = {"breaking", "significant", "contextual", "noise"}


class GatekeeperProcessor:
    """Pulls batches from the queue, runs LLM filter, writes survivors to DB."""

    def __init__(
        self,
        config: Settings,
        llm_client: LLMClient,
        queue: RedisQueue,
    ) -> None:
        self._config = config
        self._llm = llm_client
        self._queue = queue
        self._batch_size = config.gatekeeper_batch_size
        self._learning_instruction: str = ""

        # Redis for learning instructions
        try:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(config.redis_url, decode_responses=True)
        except Exception:
            self._redis = None

        # stats
        self.total_processed = 0
        self.total_kept = 0
        self.total_killed = 0
        self.batches_processed = 0
        self.total_batch_time = 0.0

    # ── main batch processing ──────────────────────────────────────────

    async def _fetch_learning_instruction(self) -> None:
        """Periodically fetch gatekeeper learning instructions from Redis."""
        if not self._redis:
            return
        try:
            instruction = await self._redis.get("alekfi:gatekeeper_instruction")
            if instruction:
                self._learning_instruction = instruction
        except Exception:
            pass

    async def _evaluate_sub_batch(self, posts: list[dict]) -> list[dict]:
        """Send a sub-batch to the LLM for evaluation."""
        prompt_lines = ["Evaluate these posts:\n"]
        for i, post in enumerate(posts, 1):
            platform = post.get("platform", "unknown")
            author = post.get("author", "unknown")
            content = post.get("content", "")[:1500]
            prompt_lines.append(f"[{i}] Platform: {platform} | Author: {author}\n{content}\n")
        user_prompt = "\n".join(prompt_lines)
        # Inject learning instructions if available
        system_prompt = SYSTEM_PROMPT
        if self._learning_instruction:
            system_prompt += f"\n\nCALIBRATION NOTE: {self._learning_instruction}"
        raw_response = await self._llm.complete(system_prompt, user_prompt, json_mode=True)
        return self._parse_response(raw_response, len(posts))

    async def process_batch(self) -> dict[str, int]:
        """Pull a batch from the queue, filter via LLM in parallel sub-batches, persist results."""
        posts = await self._queue.pop_raw_posts(self._batch_size)
        if not posts:
            return {"total": 0, "kept": 0, "killed": 0, "errors": 0}

        t0 = time.monotonic()

        # Split into 2 parallel sub-batches for throughput
        mid = len(posts) // 2
        sub1, sub2 = posts[:mid], posts[mid:]

        import asyncio
        try:
            if sub2:
                results = await asyncio.gather(
                    self._evaluate_sub_batch(sub1),
                    self._evaluate_sub_batch(sub2),
                    return_exceptions=True,
                )
                evals1 = results[0] if not isinstance(results[0], Exception) else [{} for _ in sub1]
                evals2 = results[1] if not isinstance(results[1], Exception) else [{} for _ in sub2]
                if isinstance(results[0], Exception):
                    logger.warning("[gatekeeper] sub-batch 1 failed: %s", results[0])
                if isinstance(results[1], Exception):
                    logger.warning("[gatekeeper] sub-batch 2 failed: %s", results[1])
                evaluations = evals1 + evals2
            else:
                evaluations = await self._evaluate_sub_batch(sub1)
        except Exception:
            logger.exception("[gatekeeper] LLM call failed for batch of %d", len(posts))
            return {"total": len(posts), "kept": 0, "killed": 0, "errors": len(posts)}

        # Persist to database
        kept = 0
        killed = 0
        errors = 0
        dupes = 0

        for post, evaluation in zip(posts, evaluations):
            try:
                async with get_session() as session:
                    # Save RawPost
                    source_pub = None
                    if post.get("source_published_at"):
                        try:
                            source_pub = parse_iso(post["source_published_at"])
                        except (ValueError, TypeError):
                            pass
                    raw = RawPost(
                        id=uuid.uuid4(),
                        platform=post.get("platform", "unknown"),
                        source_id=post.get("id", uuid.uuid4().hex),
                        author=post.get("author", "unknown"),
                        content=post.get("content", ""),
                        url=post.get("url"),
                        raw_metadata=post.get("raw_metadata", {}),
                        scraped_at=parse_iso(post["scraped_at"]) if post.get("scraped_at") else datetime.now(timezone.utc),
                        source_published_at=source_pub,
                        processed=True,
                    )
                    session.add(raw)
                    await session.flush()

                    if evaluation.get("relevant", False):
                        urgency = evaluation.get("urgency", "LOW").upper()
                        if urgency not in VALID_URGENCIES:
                            urgency = "LOW"
                        category = evaluation.get("category", "technology").lower()
                        if category not in VALID_CATEGORIES:
                            category = "technology"
                        relevance_tier = evaluation.get("relevance_tier", "contextual").lower()
                        if relevance_tier not in VALID_RELEVANCE_TIERS:
                            # Derive from urgency
                            relevance_tier = {"HIGH": "significant", "MEDIUM": "contextual", "LOW": "contextual"}.get(urgency, "contextual")

                        filtered = FilteredPost(
                            id=uuid.uuid4(),
                            raw_post_id=raw.id,
                            relevance_score=float(evaluation.get("relevance_score", 0.5)),
                            urgency=urgency,
                            category=category,
                            relevance_tier=relevance_tier,
                            gatekeeper_reasoning=evaluation.get("reasoning", "No reasoning provided"),
                            analyzed=False,
                        )
                        session.add(filtered)
                        kept += 1
                    else:
                        killed += 1
            except Exception as exc:
                exc_str = str(exc).lower()
                if "unique" in exc_str or "duplicate" in exc_str or "uniqueviolation" in exc_str:
                    dupes += 1
                else:
                    logger.warning("[gatekeeper] failed to persist post %s", post.get("id"), exc_info=True)
                    errors += 1

        elapsed = time.monotonic() - t0
        self.total_batch_time += elapsed
        self.batches_processed += 1
        self.total_processed += len(posts)
        self.total_kept += kept
        self.total_killed += killed

        ppm = len(posts) / max(elapsed, 0.1) * 60
        logger.info(
            "[gatekeeper] batch %d: %d posts → %d kept, %d killed, %d dupes, %d errors (%.1fs, %.0f posts/min)",
            self.batches_processed, len(posts), kept, killed, dupes, errors, elapsed, ppm,
        )
        return {"total": len(posts), "kept": kept, "killed": killed, "dupes": dupes, "errors": errors}

    # ── response parsing ───────────────────────────────────────────────

    def _parse_response(self, raw: str, expected_count: int) -> list[dict[str, Any]]:
        """Parse the LLM JSON response. Handle malformed output gracefully."""
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return self._pad_evaluations(data, expected_count)
            if isinstance(data, dict):
                # Sometimes LLM wraps array in an object
                for key in ("results", "evaluations", "posts", "data"):
                    if key in data and isinstance(data[key], list):
                        return self._pad_evaluations(data[key], expected_count)
                return self._pad_evaluations([data], expected_count)
        except json.JSONDecodeError:
            pass

        # Fallback: try to find JSON array via regex
        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group())
                if isinstance(data, list):
                    return self._pad_evaluations(data, expected_count)
            except json.JSONDecodeError:
                pass

        logger.warning("[gatekeeper] could not parse LLM response, killing entire batch")
        return [{"relevant": False, "reasoning": "Parse error"} for _ in range(expected_count)]

    @staticmethod
    def _pad_evaluations(evals: list[dict], expected: int) -> list[dict]:
        """Ensure we have exactly `expected` evaluations."""
        while len(evals) < expected:
            evals.append({"relevant": False, "reasoning": "Missing from LLM response"})
        return evals[:expected]

    # ── run loop ───────────────────────────────────────────────────────

    async def run(self, once: bool = False) -> None:
        """Continuous processing loop."""
        logger.info("[gatekeeper] starting (batch_size=%d)", self._batch_size)
        cycle_count = 0
        while True:
            # Fetch learning instructions every 20 batches
            if cycle_count % 20 == 0:
                await self._fetch_learning_instruction()
            cycle_count += 1

            stats = await self.process_batch()
            if once:
                return
            if stats["total"] == 0:
                await __import__("asyncio").sleep(2)
            else:
                await __import__("asyncio").sleep(0.5)

    # ── stats ──────────────────────────────────────────────────────────

    def get_stats(self) -> dict[str, Any]:
        avg_time = (self.total_batch_time / self.batches_processed) if self.batches_processed else 0
        return {
            "total_processed": self.total_processed,
            "total_kept": self.total_kept,
            "total_killed": self.total_killed,
            "batches_processed": self.batches_processed,
            "avg_batch_time": round(avg_time, 2),
            "kill_rate": round(self.total_killed / max(self.total_processed, 1), 3),
            "llm_tokens": self._llm.token_usage,
        }
