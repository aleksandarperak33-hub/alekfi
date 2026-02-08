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

from open_claw.config import Settings
from open_claw.db.database import get_session
from open_claw.db.models import FilteredPost, RawPost
from open_claw.llm_client import LLMClient
from open_claw.queue import RedisQueue
from open_claw.utils import parse_iso

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

        # stats
        self.total_processed = 0
        self.total_kept = 0
        self.total_killed = 0
        self.batches_processed = 0
        self.total_batch_time = 0.0

    # ── main batch processing ──────────────────────────────────────────

    async def process_batch(self) -> dict[str, int]:
        """Pull a batch from the queue, filter via LLM, persist results."""
        posts = await self._queue.pop_raw_posts(self._batch_size)
        if not posts:
            return {"total": 0, "kept": 0, "killed": 0, "errors": 0}

        t0 = time.monotonic()

        # Build numbered prompt
        prompt_lines = ["Evaluate these posts:\n"]
        for i, post in enumerate(posts, 1):
            platform = post.get("platform", "unknown")
            author = post.get("author", "unknown")
            content = post.get("content", "")[:1500]
            prompt_lines.append(f"[{i}] Platform: {platform} | Author: {author}\n{content}\n")

        user_prompt = "\n".join(prompt_lines)

        # Call LLM
        try:
            raw_response = await self._llm.complete(SYSTEM_PROMPT, user_prompt, json_mode=True)
            evaluations = self._parse_response(raw_response, len(posts))
        except Exception:
            logger.exception("[gatekeeper] LLM call failed for batch of %d", len(posts))
            return {"total": len(posts), "kept": 0, "killed": 0, "errors": len(posts)}

        # Persist to database
        kept = 0
        killed = 0
        errors = 0

        async with get_session() as session:
            for post, evaluation in zip(posts, evaluations):
                try:
                    # Save RawPost
                    raw = RawPost(
                        id=uuid.uuid4(),
                        platform=post.get("platform", "unknown"),
                        source_id=post.get("id", uuid.uuid4().hex),
                        author=post.get("author", "unknown"),
                        content=post.get("content", ""),
                        url=post.get("url"),
                        raw_metadata=post.get("raw_metadata", {}),
                        scraped_at=parse_iso(post["scraped_at"]) if post.get("scraped_at") else datetime.now(timezone.utc),
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

                        filtered = FilteredPost(
                            id=uuid.uuid4(),
                            raw_post_id=raw.id,
                            relevance_score=float(evaluation.get("relevance_score", 0.5)),
                            urgency=urgency,
                            category=category,
                            gatekeeper_reasoning=evaluation.get("reasoning", "No reasoning provided"),
                            analyzed=False,
                        )
                        session.add(filtered)
                        kept += 1
                    else:
                        killed += 1
                except Exception:
                    logger.warning("[gatekeeper] failed to persist post %s", post.get("id"), exc_info=True)
                    errors += 1

        elapsed = time.monotonic() - t0
        self.total_batch_time += elapsed
        self.batches_processed += 1
        self.total_processed += len(posts)
        self.total_kept += kept
        self.total_killed += killed

        logger.info(
            "[gatekeeper] batch %d: %d posts → %d kept, %d killed, %d errors (%.1fs)",
            self.batches_processed, len(posts), kept, killed, errors, elapsed,
        )
        return {"total": len(posts), "kept": kept, "killed": killed, "errors": errors}

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
        while True:
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
