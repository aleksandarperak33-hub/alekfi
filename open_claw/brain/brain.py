"""BrainAnalyzer — Tier 3 deep analysis engine.

Entity extraction, sentiment scoring, cross-post synthesis, and signal generation.
All four methods use dedicated LLM prompts and persist results to Postgres.
"""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import select, update

from open_claw.config import Settings
from open_claw.db.database import get_session
from open_claw.db.models import Entity, FilteredPost, RawPost, SentimentScore, Signal
from open_claw.brain.correlation import CorrelationEngine
from open_claw.llm_client import LLMClient

logger = logging.getLogger(__name__)

# ── System prompts ─────────────────────────────────────────────────────

ENTITY_PROMPT = """You are a financial entity extraction engine. Extract ALL entities mentioned or implied in the post.

For each entity, output a JSON object with:
- "name": canonical name (e.g. "Apple Inc." not "AAPL" or "apple")
- "entity_type": one of COMPANY, COMMODITY, COUNTRY, SECTOR, PERSON, PRODUCT, LEGISLATION, CRYPTO
- "ticker": primary ticker symbol if applicable (e.g. "AAPL"), null if not a traded instrument
- "related_tickers": list of related instruments (ETFs, competitors, suppliers affected)
- "second_order_effects": list of objects [{entity: str, relationship: str, direction: "positive"|"negative"}]

Think about second-order effects:
- If TSMC has supply issues → Apple, Nvidia, AMD, Qualcomm affected (negative), Samsung may benefit (positive)
- If oil prices spike → airlines negative, oil companies positive, consumer spending negative
- If FDA approves a drug → company positive, competitors negative, patients positive

Return a JSON array of entity objects. Be thorough — extract every entity, including implied ones."""

SENTIMENT_PROMPT = """You are a financial sentiment analysis engine. For each entity in this post, score the sentiment impact.

Output a JSON array of objects, one per entity:
- "entity_name": exact name from the entity list
- "sentiment": float from -1.0 (very bearish) to 1.0 (very bullish)
- "confidence": float 0-1, how confident you are in this assessment
- "urgency": float 0-1, how time-sensitive is this information
- "mechanism": specific explanation of WHY this affects the entity (be precise, e.g. "Revenue miss indicates weakening demand in consumer electronics segment")
- "themes": list of theme strings (e.g. ["supply_chain_disruption", "china_risk", "AI_demand"])

Be nuanced:
- "Apple supplier faces delays" → Apple: -0.3 (indirect), Supplier: -0.7 (direct)
- "Fed raises rates" → Banks: +0.2 (NII benefit), REITs: -0.4 (higher cap rates), Growth stocks: -0.3 (DCF discount)
- "Boeing safety issue" → Boeing: -0.6, Airbus: +0.3 (competitor benefit), Airlines: -0.1 (fleet concerns)"""

SYNTHESIS_PROMPT = """You are synthesizing the last {window} minutes of filtered financial intelligence for a hedge fund.

Analyze ALL the posts and sentiment data below. Look for:

1. CONVERGENCE — Multiple independent sources reporting the same thing (strongest signal)
2. DIVERGENCE — Conflicting signals from different platforms (needs resolution)
3. MOMENTUM SHIFTS — Sentiment changing direction on an entity
4. EMERGING NARRATIVES — New themes appearing across multiple posts
5. VOLUME ANOMALIES — Unusual discussion volume on any entity
6. CROSS-PLATFORM PATTERNS — Same entity discussed on Reddit, Twitter, and news simultaneously

Output JSON:
{{
  "themes": [
    {{"theme": str, "entities_involved": [str], "signal_strength": float 0-1, "evidence_count": int, "platforms": [str]}}
  ],
  "alerts": [
    {{"entity": str, "alert_type": "convergence"|"divergence"|"momentum_shift"|"volume_spike"|"narrative_shift", "description": str, "urgency": "HIGH"|"MEDIUM"|"LOW"}}
  ],
  "narrative_shifts": [
    {{"entity": str, "old_narrative": str, "new_narrative": str, "evidence": str}}
  ]
}}"""

SIGNAL_PROMPT = """You are a trading signal generator for a hedge fund. Based on this intelligence synthesis, generate actionable trading signals.

Signal types:
- sentiment_momentum: Sustained directional sentiment building over multiple posts
- volume_anomaly: Unusual discussion volume on an entity (potential catalyst incoming)
- narrative_shift: The market story is changing for an entity
- cross_platform_divergence: Different platforms saying different things (information asymmetry)
- source_convergence: Multiple independent sources aligning (high conviction)
- insider_signal: Insider activity aligned with sentiment direction

For each signal, output JSON:
{{
  "signal_type": str,
  "affected_instruments": [{{"symbol": str, "asset_class": "equity"|"commodity"|"fx"|"crypto"|"etf"|"index"|"bond", "direction": "LONG"|"SHORT"|"HEDGE"}}],
  "direction": "LONG"|"SHORT"|"HEDGE",
  "conviction": float 0-1,
  "time_horizon": "MINUTES"|"HOURS"|"DAYS"|"WEEKS",
  "thesis": str (2-3 sentences explaining the trade),
  "evidence": [str] (list of key evidence points),
  "expires_in_hours": int
}}

Return a JSON array of signal objects. Be selective — only generate signals with conviction > 0.4. Quality over quantity."""


class BrainAnalyzer:
    """Tier 3 deep analysis: entities → sentiment → synthesis → signals."""

    def __init__(self, config: Settings, llm_client: LLMClient) -> None:
        self._config = config
        self._llm = llm_client
        self._correlation = CorrelationEngine()
        self._synthesis_interval = config.brain_synthesis_interval

        # stats
        self.posts_analyzed = 0
        self.entities_extracted = 0
        self.sentiments_scored = 0
        self.signals_generated = 0
        self.syntheses_run = 0

    # ── 1. Entity extraction ───────────────────────────────────────────

    async def analyze_entities(self, filtered_post: FilteredPost) -> list[Entity]:
        """Extract entities from a filtered post and upsert them into the DB."""
        content = ""
        async with get_session() as session:
            raw = await session.get(RawPost, filtered_post.raw_post_id)
            if raw:
                content = raw.content
        if not content:
            content = "(no content)"

        user_prompt = f"Platform: {filtered_post.category}\nContent:\n{content}"

        try:
            raw_response = await self._llm.complete(ENTITY_PROMPT, user_prompt, json_mode=True)
            entity_dicts = self._parse_json_array(raw_response)
        except Exception:
            logger.warning("[brain] entity extraction failed for post %s", filtered_post.id, exc_info=True)
            return []

        entities: list[Entity] = []
        async with get_session() as session:
            for ed in entity_dicts:
                name = ed.get("name", "Unknown")
                entity_type = ed.get("entity_type", "COMPANY").upper()
                ticker = ed.get("ticker") or self._correlation.resolve_ticker(name, entity_type)
                related = ed.get("related_tickers", [])
                if not related and ticker:
                    related = [r["symbol"] for r in self._correlation.get_related_instruments(ticker)]

                # Upsert
                stmt = select(Entity).where(Entity.name == name, Entity.entity_type == entity_type)
                result = await session.execute(stmt)
                entity = result.scalar_one_or_none()

                if entity is None:
                    entity = Entity(
                        id=uuid.uuid4(),
                        name=name,
                        entity_type=entity_type,
                        ticker=ticker,
                        related_tickers=related,
                        metadata_=ed.get("second_order_effects"),
                    )
                    session.add(entity)
                else:
                    entity.ticker = ticker or entity.ticker
                    if related:
                        entity.related_tickers = related
                await session.flush()
                entities.append(entity)

        self.entities_extracted += len(entities)
        return entities

    # ── 2. Sentiment scoring ───────────────────────────────────────────

    async def analyze_sentiment(
        self, filtered_post: FilteredPost, entities: list[Entity]
    ) -> list[SentimentScore]:
        """Score sentiment for each entity mentioned in the post."""
        content = ""
        async with get_session() as session:
            raw = await session.get(RawPost, filtered_post.raw_post_id)
            if raw:
                content = raw.content

        entity_names = [e.name for e in entities]
        user_prompt = (
            f"Post (urgency={filtered_post.urgency}, category={filtered_post.category}):\n{content}\n\n"
            f"Entities to score: {', '.join(entity_names)}"
        )

        try:
            raw_response = await self._llm.complete(SENTIMENT_PROMPT, user_prompt, json_mode=True)
            scores_dicts = self._parse_json_array(raw_response)
        except Exception:
            logger.warning("[brain] sentiment analysis failed for post %s", filtered_post.id, exc_info=True)
            return []

        entity_map = {e.name.lower(): e for e in entities}
        scores: list[SentimentScore] = []

        async with get_session() as session:
            for sd in scores_dicts:
                ename = sd.get("entity_name", "").lower()
                entity = entity_map.get(ename)
                if entity is None:
                    continue

                score = SentimentScore(
                    id=uuid.uuid4(),
                    filtered_post_id=filtered_post.id,
                    entity_id=entity.id,
                    sentiment=max(-1.0, min(1.0, float(sd.get("sentiment", 0)))),
                    confidence=max(0.0, min(1.0, float(sd.get("confidence", 0.5)))),
                    urgency=max(0.0, min(1.0, float(sd.get("urgency", 0.3)))),
                    reasoning=sd.get("mechanism", "No mechanism provided"),
                    themes=sd.get("themes"),
                    mechanism=sd.get("mechanism"),
                )
                session.add(score)
                scores.append(score)

        self.sentiments_scored += len(scores)
        return scores

    # ── 3. Synthesis ───────────────────────────────────────────────────

    async def synthesize(self, window_minutes: int = 15) -> dict[str, Any]:
        """Synthesize recent intelligence into themes, alerts, and narrative shifts."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)

        posts_data: list[dict] = []
        async with get_session() as session:
            stmt = (
                select(FilteredPost)
                .where(FilteredPost.filtered_at >= cutoff)
                .order_by(FilteredPost.filtered_at.desc())
                .limit(50)
            )
            result = await session.execute(stmt)
            filtered_posts = result.scalars().all()

            for fp in filtered_posts:
                raw = await session.get(RawPost, fp.raw_post_id)
                content = raw.content if raw else "(no content)"

                score_stmt = select(SentimentScore).where(SentimentScore.filtered_post_id == fp.id)
                score_result = await session.execute(score_stmt)
                sentiments = score_result.scalars().all()

                posts_data.append({
                    "post_id": str(fp.id),
                    "platform": raw.platform if raw else "unknown",
                    "urgency": fp.urgency,
                    "category": fp.category,
                    "content_snippet": content[:500],
                    "sentiments": [
                        {"entity": s.reasoning[:50], "score": s.sentiment, "confidence": s.confidence}
                        for s in sentiments
                    ],
                })

        if not posts_data:
            logger.info("[brain] no recent posts for synthesis (window=%dm)", window_minutes)
            return {"themes": [], "alerts": [], "narrative_shifts": []}

        prompt = SYNTHESIS_PROMPT.format(window=window_minutes)
        user_prompt = f"Intelligence feed ({len(posts_data)} posts):\n\n{json.dumps(posts_data, indent=2, default=str)}"

        try:
            raw_response = await self._llm.complete(prompt, user_prompt, json_mode=True)
            synthesis = json.loads(raw_response)
        except Exception:
            logger.warning("[brain] synthesis failed", exc_info=True)
            synthesis = {"themes": [], "alerts": [], "narrative_shifts": []}

        self.syntheses_run += 1
        logger.info(
            "[brain] synthesis complete: %d themes, %d alerts, %d narrative shifts",
            len(synthesis.get("themes", [])),
            len(synthesis.get("alerts", [])),
            len(synthesis.get("narrative_shifts", [])),
        )
        return synthesis

    # ── 4. Signal generation ───────────────────────────────────────────

    async def generate_signals(self, synthesis: dict[str, Any]) -> list[Signal]:
        """Convert synthesis into actionable trading signals."""
        if not synthesis.get("themes") and not synthesis.get("alerts"):
            return []

        user_prompt = f"Intelligence synthesis:\n{json.dumps(synthesis, indent=2, default=str)}"

        try:
            raw_response = await self._llm.complete(SIGNAL_PROMPT, user_prompt, json_mode=True)
            signal_dicts = self._parse_json_array(raw_response)
        except Exception:
            logger.warning("[brain] signal generation failed", exc_info=True)
            return []

        signals: list[Signal] = []
        async with get_session() as session:
            for sd in signal_dicts:
                conviction = float(sd.get("conviction", 0))
                if conviction < 0.4:
                    continue

                expires_hours = int(sd.get("expires_in_hours", 24))
                signal = Signal(
                    id=uuid.uuid4(),
                    signal_type=sd.get("signal_type", "sentiment_momentum"),
                    affected_instruments=sd.get("affected_instruments", []),
                    direction=sd.get("direction", "LONG").upper(),
                    conviction=conviction,
                    time_horizon=sd.get("time_horizon", "DAYS"),
                    thesis=sd.get("thesis", ""),
                    source_posts=sd.get("evidence"),
                    metadata_={"synthesis_themes": [t.get("theme") for t in synthesis.get("themes", [])]},
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=expires_hours),
                )
                session.add(signal)
                signals.append(signal)

        self.signals_generated += len(signals)
        logger.info("[brain] generated %d signals", len(signals))
        return signals

    # ── main run loop ──────────────────────────────────────────────────

    async def run(self, once: bool = False) -> None:
        """Process unanalyzed posts, periodically run synthesis."""
        import asyncio

        logger.info("[brain] starting (synthesis_interval=%dm)", self._synthesis_interval)
        last_synthesis = datetime.now(timezone.utc) - timedelta(minutes=self._synthesis_interval)

        while True:
            # Process unanalyzed FilteredPosts
            async with get_session() as session:
                stmt = (
                    select(FilteredPost)
                    .where(FilteredPost.analyzed == False)  # noqa: E712
                    .order_by(FilteredPost.filtered_at.asc())
                    .limit(self._config.brain_batch_size)
                )
                result = await session.execute(stmt)
                pending = result.scalars().all()

            for fp in pending:
                try:
                    entities = await self.analyze_entities(fp)
                    if entities:
                        await self.analyze_sentiment(fp, entities)
                    async with get_session() as session:
                        await session.execute(
                            update(FilteredPost).where(FilteredPost.id == fp.id).values(analyzed=True)
                        )
                    self.posts_analyzed += 1
                except Exception:
                    logger.warning("[brain] failed to analyze post %s", fp.id, exc_info=True)

            # Synthesis check
            now = datetime.now(timezone.utc)
            if (now - last_synthesis).total_seconds() >= self._synthesis_interval * 60:
                synthesis = await self.synthesize(window_minutes=self._synthesis_interval)
                await self.generate_signals(synthesis)
                last_synthesis = now

            if once:
                # Run one final synthesis
                synthesis = await self.synthesize(window_minutes=60)
                await self.generate_signals(synthesis)
                return

            await asyncio.sleep(5)

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _parse_json_array(raw: str) -> list[dict[str, Any]]:
        """Parse a JSON array from LLM response, with fallbacks."""
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                for key in ("entities", "results", "scores", "signals", "data"):
                    if key in data and isinstance(data[key], list):
                        return data[key]
                return [data]
        except json.JSONDecodeError:
            pass

        match = re.search(r'\[.*\]', raw, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

        logger.warning("[brain] could not parse JSON array from LLM response")
        return []

    def get_stats(self) -> dict[str, Any]:
        return {
            "posts_analyzed": self.posts_analyzed,
            "entities_extracted": self.entities_extracted,
            "sentiments_scored": self.sentiments_scored,
            "signals_generated": self.signals_generated,
            "syntheses_run": self.syntheses_run,
            "llm_tokens": self._llm.token_usage,
        }
