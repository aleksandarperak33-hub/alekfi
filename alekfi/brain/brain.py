"""BrainAnalyzer — Tier 3 deep analysis engine.

Entity extraction, sentiment scoring, cross-post synthesis, signal generation,
priority scoring, freshness tracking, source traceability, chain-of-thought
reasoning, and adversarial review.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import select, update, func, case, text

from alekfi.config import Settings
from alekfi.db.database import get_session
from alekfi.db.models import Entity, FilteredPost, RawPost, SentimentScore, Signal, SignalFeedback
from alekfi.brain.correlation import CorrelationEngine
from alekfi.llm_client import LLMClient

logger = logging.getLogger(__name__)

# ── Source exclusivity weighting ──────────────────────────────────────────
# How exclusively does AlekFi monitor this source vs institutional algos?
# 0.0 = every terminal/algo has it, 1.0 = almost nobody monitors systematically
SOURCE_EXCLUSIVITY = {
    # HIGH — institutions don't monitor these systematically
    "tiktok": 0.95,
    "instagram": 0.90,
    "glassdoor": 0.85,
    "blind": 0.90,
    "app_store": 0.80,
    "appstore": 0.80,
    "google_play": 0.80,
    "amazon_reviews": 0.85,
    "telegram": 0.80,
    "discord": 0.75,
    "4chan_biz": 0.85,
    # MEDIUM — some quant funds monitor but not deeply
    "reddit": 0.60,
    "youtube": 0.55,
    "google_trends": 0.50,
    "patent_filings": 0.65,
    "patents": 0.65,
    "github_trending": 0.70,
    "linkedin": 0.60,
    # LOW — every terminal and algo already has this
    "news_rss": 0.15,
    "hackernews": 0.30,
    "sec_edgar": 0.20,
    "federal_register": 0.25,
    "fda": 0.30,
    "twitter": 0.25,
    # New scrapers
    "4chan_pol": 0.90,
    "clinical_trials": 0.40,
    "earnings_calendar": 0.10,
    # High-frequency scrapers
    "finviz_news": 0.20,
    "stocktwits": 0.55,
    "options_flow": 0.70,
    "whale_tracker": 0.75,
    "commodities": 0.15,
}
EXCLUSIVITY_HIGH_THRESHOLD = 0.65
EXCLUSIVITY_LOW_THRESHOLD = 0.35

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
7. VELOCITY SPIKES — Track how quickly mentions of each entity are accelerating. If an entity went from 0 mentions to 5+ in the last 30 minutes across different platforms, flag it as a velocity spike.

Output JSON:
{{
  "themes": [
    {{"theme": str, "entities_involved": [str], "signal_strength": float 0-1, "evidence_count": int, "platforms": [str]}}
  ],
  "alerts": [
    {{"entity": str, "alert_type": "convergence"|"divergence"|"momentum_shift"|"volume_spike"|"narrative_shift"|"velocity_spike", "description": str, "urgency": "HIGH"|"MEDIUM"|"LOW"}}
  ],
  "narrative_shifts": [
    {{"entity": str, "old_narrative": str, "new_narrative": str, "evidence": str}}
  ],
  "velocity_spikes": [
    {{"entity": str, "mentions_count": int, "time_window_minutes": int, "platforms": [str], "acceleration": "rapid"|"moderate"}}
  ]
}}"""

SIGNAL_PROMPT = """You are a trading signal generator for a hedge fund. Every signal must be ACTIONABLE — a trader reading this should know exactly what to buy/sell/hedge and why. Vague signals like "market sentiment is mixed" are worthless. Be specific: name the ticker, the direction, the catalyst, the timeframe, and the expected magnitude.

Before scoring each signal, reason step-by-step:
1. What is the core information driving this signal?
2. Is this likely already priced in by the market?
3. What is the strongest counter-argument against this trade?
4. Based on similar historical signals, what typically happened?

Signal types:
- sentiment_momentum: Sustained directional sentiment building over multiple posts
- volume_anomaly: Unusual discussion volume on an entity (potential catalyst incoming)
- narrative_shift: The market story is changing for an entity
- cross_platform_divergence: Different platforms saying different things (information asymmetry)
- source_convergence: Multiple independent sources aligning (high conviction)
- insider_signal: Insider activity aligned with sentiment direction

For each signal, output JSON with ALL of the following fields. Every field is REQUIRED:
{{
  "signal_type": str,
  "headline": str (REQUIRED — a punchy 5-10 word summary, e.g. "NVDA supply chain disruption signals short-term weakness"),
  "affected_instruments": [{{"symbol": str, "asset_class": "equity"|"commodity"|"fx"|"crypto"|"etf"|"index"|"bond", "direction": "LONG"|"SHORT"|"HEDGE"}}],
  "direction": "LONG"|"SHORT"|"HEDGE",
  "conviction": float 0-1,
  "time_horizon": "MINUTES"|"HOURS"|"DAYS"|"WEEKS",
  "thesis": str (2-3 sentences explaining the trade),
  "evidence": [str] (list of key evidence points),
  "expires_in_hours": int,
  "reasoning_chain": str (your step-by-step reasoning from the 4 questions above),
  "counter_argument": str (strongest argument against this trade),
  "priced_in_assessment": "likely_priced_in"|"partially_priced_in"|"likely_not_priced_in",
  "suggested_action": "BUY"|"SELL"|"WATCH"|"HEDGE"|"IGNORE",
  "price_implication": {{
    "direction": "up"|"down"|"neutral",
    "estimated_move_pct": float (e.g. 2.5 for +2.5%, -3.1 for -3.1%),
    "timeframe": "1d"|"1w"|"1m",
    "reasoning": str (why you expect this move)
  }},
  "priced_in": "not_priced_in"|"partially_priced_in"|"fully_priced_in",
  "consensus_position": "contrarian"|"with_consensus"|"neutral",
  "actionability": "immediate_trade"|"watchlist"|"research_needed"|"noise",

  *** CRITICAL — SCORE EACH INTELLIGENCE DIMENSION 1-20. DO NOT SKIP. ***
  "intelligence_scores": {{
    "earnings_impact": int 1-20,
    "information_asymmetry": int 1-20,
    "time_sensitivity": int 1-20,
    "market_cap_exposure": int 1-20,
    "catalyst_clarity": int 1-20
  }},
  "total_intelligence_score": int (MUST equal sum of 5 intelligence_scores, range 5-100),

  "sources": [{{"post_id": str, "platform": str, "author": str, "url": str, "relevance": "primary"|"supporting", "headline": str}}]
}}

{INTELLIGENCE_FRAMEWORK}

EXAMPLE of a properly scored signal:
{{
  "signal_type": "narrative_shift",
  "headline": "TSMC earnings miss sparks chip sector rotation fears",
  "intelligence_scores": {{"earnings_impact": 16, "information_asymmetry": 8, "time_sensitivity": 16, "market_cap_exposure": 18, "catalyst_clarity": 14}},
  "total_intelligence_score": 72,
  "priced_in": "partially_priced_in",
  "consensus_position": "with_consensus",
  "actionability": "immediate_trade",
  "price_implication": {{"direction": "down", "estimated_move_pct": -3.2, "timeframe": "1w", "reasoning": "Earnings miss typically causes 2-5% selloff in semis within a week"}},
  ...
}}

Scoring guidelines:
- A routine earnings beat with mainstream coverage: earnings_impact ~6, information_asymmetry ~3, catalyst_clarity ~6 → low score (~30-40)
- Breaking insider activity from exclusive source: earnings_impact ~16, information_asymmetry ~17, time_sensitivity ~18 → high score (~75-85)
- Signals MUST have varied scores. Not everything is 50. Differentiate aggressively.
- CONTRARIAN signals with strong evidence deserve a 10-point bonus. The best trades are ones the market hasn't figured out yet.

Penalize signals based on stale information. If most source posts are >24 hours old, reduce total_intelligence_score by 10-20 points.

If historical accuracy data is provided below, use it to calibrate your confidence:
- Boost conviction for signal types with >70% accuracy
- Reduce conviction by 0.1-0.2 for signal types with <40% accuracy
- Discount platforms with <50% accuracy as supporting evidence only, not primary
- If a ticker has <30% historical accuracy, require stronger evidence before generating a signal

Return a JSON array of signal objects. Be selective — only generate signals with conviction > 0.4. Quality over quantity."""

REVIEW_PROMPT = """You are an adversarial reviewer for a hedge fund's trading signal pipeline. Your job is to stress-test signals and remove weak ones.

For each signal below, critically evaluate:
1. WEAKNESSES: What could go wrong? What assumptions are being made?
2. PRICED IN: Is this information likely already reflected in the market price? Consider: how widely reported is this, how long has it been known, is it from a mainstream source?
3. RE-SCORE: Based on your critique, adjust the priority_score. Be harsh — most signals should lose 5-15 points. Only exceptional signals should keep or gain points.
4. SURVIVE: Does this signal survive scrutiny? If the thesis is weak, the information is stale, or the trade is too obvious, remove it.

Return a JSON array of ONLY the surviving signals with these updated fields:
{{
  "signal_index": int (0-based index from the input array),
  "priority_score": int (re-scored after review),
  "review_notes": str (brief explanation of your assessment),
  "weaknesses": [str],
  "survives": bool
}}

Be aggressive — remove at least 20-30% of signals. The fund only wants high-quality, non-obvious trades."""

BATCH_ENTITY_PROMPT = """You are a financial entity extraction engine processing multiple posts in one call.
For EACH numbered post, extract ALL financial entities and score sentiment/relevance.

Return ONLY a JSON array with exactly {count} objects in order. No markdown, no explanation.
Each object:
{{
  "post_index": int (1-based),
  "entities": [
    {{"name": str, "type": "company"|"person"|"product"|"ticker"|"sector"|"country"|"commodity"|"crypto"|"legislation",
      "ticker": str or null, "related_tickers": [str]}}
  ],
  "sentiment": float -1.0 to 1.0 (overall financial sentiment),
  "relevance": float 0.0 to 1.0 (how relevant to tradeable financial markets),
  "key_claims": [str] (specific factual claims, not opinions),
  "urgency": "breaking"|"significant"|"contextual"|"noise",
  "themes": [str] (e.g. "supply_chain_disruption", "earnings_surprise", "insider_trading")
}}

Be thorough with entities — include second-order effects (if TSMC has issues → AAPL, NVDA, AMD affected).
Be harsh on relevance — most social media posts are noise (relevance < 0.3)."""

INTELLIGENCE_FRAMEWORK = """
Score this signal on EACH dimension (1-20 points each). Be specific and honest.

1. EARNINGS_IMPACT (1-20): Does this change anyone's earnings estimate?
   1-5: No direct earnings impact, general sentiment
   6-10: Could indirectly affect earnings (consumer trend, competitive shift)
   11-15: Likely affects next quarter (supply chain issue, demand shift, cost change)
   16-20: Directly changes estimate (guidance revision, contract win/loss, FDA decision, insider buying)

2. INFORMATION_ASYMMETRY (1-20): Who else knows this?
   1-5: On Bloomberg, Reuters, CNBC — every terminal has it, fully priced in
   6-10: In niche financial media, FinTwit — sophisticated investors know
   11-15: On Reddit/YouTube/forums — retail knows, institutions may not track
   16-20: On TikTok/Instagram/Glassdoor/4chan/employee forums — almost nobody in finance sees this

3. TIME_SENSITIVITY (1-20): How fast must a trader act?
   1-5: Multi-month trend, no urgency
   6-10: Matters in coming weeks
   11-15: Moves markets in 1-3 days, act soon
   16-20: Moves markets today/tomorrow, act NOW (earnings tonight, breaking event, FDA pending)

4. MARKET_CAP_EXPOSURE (1-20): How big is the affected market?
   1-5: Micro/small cap <$1B, niche
   6-10: Mid cap $1-10B, small sector
   11-15: Large cap $10-100B, major sector, index impact
   16-20: Mega cap >$100B, macro event, moves SPY/QQQ (AAPL, NVDA, rates, oil)

5. CATALYST_CLARITY (1-20): How clear is signal → price movement?
   1-5: Vague sentiment, unclear mechanism
   6-10: Reasonable thesis but multiple steps to price impact
   11-15: Clear mechanism ("product recall → revenue hit → stock drops")
   16-20: Direct and immediate ("CEO bought $5M stock" or "FDA rejected drug")

ALSO DETERMINE:
- priced_in: "not_priced_in" | "partially_priced_in" | "fully_priced_in"
- consensus_position: "contrarian" | "with_consensus" | "neutral" — CONTRARIAN signals that pass quality bar are highest-alpha
- actionability: "immediate_trade" | "watchlist" | "research_needed" | "noise" — be ruthlessly honest, most signals are noise
"""

# Mega-cap tickers for market_cap_exposure scoring
MEGA_CAPS = {
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "BRK.B", "BRK",
    "JPM", "V", "MA", "JNJ", "UNH", "XOM", "PG", "HD", "AVGO", "LLY",
    "SPY", "QQQ", "DIA", "IWM",
}


class BrainAnalyzer:
    """Tier 3 deep analysis: entities → sentiment → synthesis → signals."""

    def __init__(self, config: Settings, llm_client: LLMClient) -> None:
        self._config = config
        self._llm = llm_client
        self._correlation = CorrelationEngine()
        self._synthesis_interval = config.brain_synthesis_interval
        self._entity_batch_size = int(os.environ.get("BRAIN_ENTITY_BATCH_SIZE", str(config.brain_entity_batch_size)))
        self._parallel_batches = int(os.environ.get("BRAIN_PARALLEL_BATCHES", str(config.brain_parallel_batches)))
        self._redis = aioredis.from_url(config.redis_url, decode_responses=True)

        # stats
        self.posts_analyzed = 0
        self.entities_extracted = 0
        self.sentiments_scored = 0
        self.signals_generated = 0
        self.signals_reviewed_out = 0
        self.syntheses_run = 0
        self.signals_noise_filtered = 0

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

    # ── 1b. BATCH entity + sentiment extraction (10x throughput) ──────

    async def _analyze_entity_batch(
        self, posts: list[tuple[FilteredPost, str]]
    ) -> list[dict | None]:
        """Analyze up to N posts in a single LLM call. Returns per-post analysis dicts."""
        numbered = "\n\n".join(
            f"[POST {i+1}] Platform: {fp.category} | Urgency: {fp.urgency} | "
            f"Time: {fp.filtered_at.isoformat() if fp.filtered_at else 'unknown'}\n{content[:600]}"
            for i, (fp, content) in enumerate(posts)
        )
        prompt = BATCH_ENTITY_PROMPT.format(count=len(posts))
        try:
            raw_response = await self._llm.complete(prompt, numbered, json_mode=True)
            parsed = self._parse_json_array(raw_response)
            if isinstance(parsed, list) and len(parsed) >= len(posts):
                return parsed[:len(posts)]
            elif isinstance(parsed, list) and parsed:
                # Pad if LLM returned fewer than expected
                while len(parsed) < len(posts):
                    parsed.append(None)
                return parsed
        except Exception as e:
            logger.warning("[brain] batch entity analysis failed: %s", e)
        return [None] * len(posts)

    async def _process_batch_results(
        self, posts: list[tuple[FilteredPost, str]], results: list[dict | None]
    ) -> int:
        """Process batch analysis results: upsert entities, create sentiments, mark analyzed."""
        count = 0
        for (fp, content), analysis in zip(posts, results):
            if analysis is None:
                # Fallback: try individual analysis
                try:
                    entities = await self.analyze_entities(fp)
                    if entities:
                        await self.analyze_sentiment(fp, entities)
                    async with get_session() as session:
                        await session.execute(
                            update(FilteredPost).where(FilteredPost.id == fp.id).values(analyzed=True)
                        )
                    self.posts_analyzed += 1
                    count += 1
                except Exception:
                    logger.warning("[brain] fallback analysis failed for %s", fp.id)
                continue

            entities_data = analysis.get("entities", [])
            sentiment = analysis.get("sentiment", 0.0)
            relevance = analysis.get("relevance", 0.5)
            urgency = analysis.get("urgency", "contextual")
            themes = analysis.get("themes", [])
            key_claims = analysis.get("key_claims", [])

            # Upsert entities
            entities: list[Entity] = []
            async with get_session() as session:
                for ed in entities_data:
                    name = ed.get("name", "Unknown")
                    entity_type = ed.get("type", "company").upper()
                    if entity_type not in ("COMPANY", "COMMODITY", "COUNTRY", "SECTOR", "PERSON", "PRODUCT", "LEGISLATION", "CRYPTO", "TICKER"):
                        entity_type = "COMPANY"
                    ticker = ed.get("ticker") or self._correlation.resolve_ticker(name, entity_type)
                    related = ed.get("related_tickers", [])
                    if not related and ticker:
                        related = [r["symbol"] for r in self._correlation.get_related_instruments(ticker)]

                    stmt = select(Entity).where(Entity.name == name, Entity.entity_type == entity_type)
                    result = await session.execute(stmt)
                    entity = result.scalar_one_or_none()
                    if entity is None:
                        entity = Entity(
                            id=uuid.uuid4(), name=name, entity_type=entity_type,
                            ticker=ticker, related_tickers=related,
                        )
                        session.add(entity)
                    else:
                        entity.ticker = ticker or entity.ticker
                        if related:
                            entity.related_tickers = related
                    await session.flush()
                    entities.append(entity)
                self.entities_extracted += len(entities)

                # Create sentiment scores from batch result
                for entity in entities:
                    score = SentimentScore(
                        id=uuid.uuid4(),
                        filtered_post_id=fp.id,
                        entity_id=entity.id,
                        sentiment=max(-1.0, min(1.0, float(sentiment))),
                        confidence=max(0.0, min(1.0, float(relevance))),
                        urgency=max(0.0, min(1.0, 0.9 if urgency == "breaking" else 0.6 if urgency == "significant" else 0.3)),
                        reasoning="; ".join(key_claims[:3]) if key_claims else "Batch extraction",
                        themes=themes,
                        mechanism="; ".join(key_claims[:2]) if key_claims else None,
                    )
                    session.add(score)
                self.sentiments_scored += len(entities)

                # Mark analyzed and update relevance_tier
                await session.execute(
                    update(FilteredPost).where(FilteredPost.id == fp.id).values(
                        analyzed=True, relevance_tier=urgency,
                    )
                )

            self.posts_analyzed += 1
            count += 1
        return count

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
        """Synthesize recent intelligence into themes, alerts, narrative shifts, and velocity spikes."""
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
                    "author": raw.author if raw else "unknown",
                    "url": raw.url if raw else None,
                    "source_published_at": raw.source_published_at.isoformat() if raw and raw.source_published_at else None,
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
            return {"themes": [], "alerts": [], "narrative_shifts": [], "velocity_spikes": []}

        prompt = SYNTHESIS_PROMPT.format(window=window_minutes)
        user_prompt = f"Intelligence feed ({len(posts_data)} posts):\n\n{json.dumps(posts_data, indent=2, default=str)}"

        try:
            raw_response = await self._llm.complete(prompt, user_prompt, json_mode=True)
            synthesis = json.loads(raw_response)
        except Exception:
            logger.warning("[brain] synthesis failed", exc_info=True)
            synthesis = {"themes": [], "alerts": [], "narrative_shifts": [], "velocity_spikes": []}

        # Attach source data for signal generation
        synthesis["_source_posts"] = posts_data

        self.syntheses_run += 1
        logger.info(
            "[brain] synthesis complete: %d themes, %d alerts, %d narrative shifts, %d velocity spikes",
            len(synthesis.get("themes", [])),
            len(synthesis.get("alerts", [])),
            len(synthesis.get("narrative_shifts", [])),
            len(synthesis.get("velocity_spikes", [])),
        )
        return synthesis

    # ── 3b. Historical accuracy context ─────────────────────────────────

    async def _get_accuracy_context(self) -> str:
        """Query historical feedback stats for injection into signal prompt."""
        async with get_session() as session:
            total = (await session.execute(
                select(func.count(SignalFeedback.id))
            )).scalar() or 0

            if total < 10:
                return ""

            accurate_total = (await session.execute(
                select(func.count(SignalFeedback.id)).where(SignalFeedback.was_accurate == True)  # noqa: E712
            )).scalar() or 0

            lines = [
                f"## Historical Accuracy Data ({total} feedback records, {accurate_total/total:.0%} overall)",
                "",
            ]

            # By signal_type
            type_rows = (await session.execute(
                select(
                    Signal.signal_type,
                    func.count(SignalFeedback.id).label("total"),
                    func.sum(case((SignalFeedback.was_accurate == True, 1), else_=0)).label("accurate"),  # noqa: E712
                )
                .join(Signal, SignalFeedback.signal_id == Signal.id)
                .group_by(Signal.signal_type)
            )).all()
            if type_rows:
                lines.append("Signal type accuracy:")
                for row in type_rows:
                    rate = (row.accurate or 0) / max(row.total, 1)
                    lines.append(f"  - {row.signal_type}: {rate:.0%} ({row.accurate}/{row.total})")
                lines.append("")

            # By platform — unpack source_posts JSONB
            plat_rows = (await session.execute(text("""
                SELECT sp->>'platform' AS platform,
                       COUNT(*) AS total,
                       SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                FROM signal_feedback sf
                JOIN signals s ON sf.signal_id = s.id
                CROSS JOIN LATERAL jsonb_array_elements(s.source_posts) AS sp
                WHERE sp->>'platform' IS NOT NULL
                GROUP BY sp->>'platform'
                ORDER BY total DESC
                LIMIT 10
            """))).all()
            if plat_rows:
                lines.append("Platform accuracy:")
                for row in plat_rows:
                    rate = (row.accurate or 0) / max(row.total, 1)
                    lines.append(f"  - {row.platform}: {rate:.0%} ({row.accurate}/{row.total})")
                lines.append("")

            # By ticker — unpack affected_instruments JSONB
            ticker_rows = (await session.execute(text("""
                SELECT ai->>'symbol' AS symbol,
                       COUNT(*) AS total,
                       SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                FROM signal_feedback sf
                JOIN signals s ON sf.signal_id = s.id
                CROSS JOIN LATERAL jsonb_array_elements(s.affected_instruments) AS ai
                WHERE ai->>'symbol' IS NOT NULL
                GROUP BY ai->>'symbol'
                ORDER BY total DESC
                LIMIT 20
            """))).all()
            if ticker_rows:
                lines.append("Ticker accuracy (top 20):")
                for row in ticker_rows:
                    rate = (row.accurate or 0) / max(row.total, 1)
                    lines.append(f"  - {row.symbol}: {rate:.0%} ({row.accurate}/{row.total})")
                lines.append("")

        return "\n".join(lines)

    # ── 3c. Source exclusivity analysis ─────────────────────────────────

    def _compute_exclusivity_edge(self, source_posts: list[dict]) -> dict[str, Any]:
        """Analyze source platforms to determine exclusivity edge.

        Returns dict with: edge_type, avg_exclusivity, high_sources, low_sources,
        and whether this is a pre-institutional signal.
        """
        platforms = [sp.get("platform", "unknown") for sp in source_posts if sp.get("platform")]
        if not platforms:
            return {"edge_type": "unknown", "avg_exclusivity": 0.5, "high_sources": [], "low_sources": []}

        scores = [SOURCE_EXCLUSIVITY.get(p, 0.5) for p in platforms]
        avg = sum(scores) / len(scores)
        high = [p for p in set(platforms) if SOURCE_EXCLUSIVITY.get(p, 0.5) >= EXCLUSIVITY_HIGH_THRESHOLD]
        low = [p for p in set(platforms) if SOURCE_EXCLUSIVITY.get(p, 0.5) <= EXCLUSIVITY_LOW_THRESHOLD]

        if high and not low:
            edge_type = "retail_only"
        elif high and low:
            edge_type = "cross_platform"
        elif low and not high:
            edge_type = "commodity"
        else:
            edge_type = "unknown"

        return {
            "edge_type": edge_type,
            "avg_exclusivity": round(avg, 3),
            "high_sources": high,
            "low_sources": low,
        }

    async def _check_pre_institutional(self, entity_names: list[str], source_posts: list[dict]) -> dict[str, Any]:
        """Check if signal appeared on high-exclusivity sources BEFORE low-exclusivity sources.

        This is the ultimate social arbitrage signal.
        """
        high_platforms = [sp for sp in source_posts
                         if SOURCE_EXCLUSIVITY.get(sp.get("platform", ""), 0.5) >= EXCLUSIVITY_HIGH_THRESHOLD]
        low_platforms = [sp for sp in source_posts
                        if SOURCE_EXCLUSIVITY.get(sp.get("platform", ""), 0.5) <= EXCLUSIVITY_LOW_THRESHOLD]

        if not high_platforms:
            return {"is_pre_institutional": False}

        # Check time-to-mainstream for related entities
        lead_times: list[dict] = []
        for name in entity_names[:5]:
            ttm = await self._get_time_to_mainstream(name)
            if ttm and ttm.get("lead_time_hours", 0) > 0:
                lead_times.append(ttm)

        # If high-exclusivity sources present but no low-exclusivity, potentially pre-institutional
        if high_platforms and not low_platforms:
            return {
                "is_pre_institutional": True,
                "high_source_count": len(high_platforms),
                "lead_times": lead_times,
            }

        return {"is_pre_institutional": False, "lead_times": lead_times}

    # ── 3d. Time-to-mainstream tracking ───────────────────────────────────

    async def _track_first_seen(self, entity_name: str, platform: str, post_id: str) -> None:
        """Record first sighting of an entity on a specific platform tier."""
        key = f"alekfi:first_seen:{entity_name.lower().replace(' ', '_')}"
        existing = await self._redis.get(key)
        if existing:
            return  # Already tracked
        value = json.dumps({
            "platform": platform,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "post_id": post_id,
            "exclusivity": SOURCE_EXCLUSIVITY.get(platform, 0.5),
        })
        await self._redis.setex(key, 48 * 3600, value)  # TTL: 48 hours

    async def _get_time_to_mainstream(self, entity_name: str) -> dict[str, Any] | None:
        """Check if entity appeared on high-exclusivity source first, compute lead time."""
        key = f"alekfi:first_seen:{entity_name.lower().replace(' ', '_')}"
        raw = await self._redis.get(key)
        if not raw:
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None

        first_platform = data.get("platform", "")
        first_exclusivity = data.get("exclusivity", 0.5)
        first_time = data.get("timestamp", "")

        if first_exclusivity < EXCLUSIVITY_HIGH_THRESHOLD:
            return None  # First seen on low-exclusivity source, no lead

        # Check if it has since appeared on low-exclusivity sources
        try:
            first_dt = datetime.fromisoformat(first_time)
            if first_dt.tzinfo is None:
                first_dt = first_dt.replace(tzinfo=timezone.utc)
            hours_since = (datetime.now(timezone.utc) - first_dt).total_seconds() / 3600
        except (ValueError, TypeError):
            hours_since = 0

        return {
            "first_platform": first_platform,
            "first_exclusivity": first_exclusivity,
            "first_seen": first_time,
            "lead_time_hours": round(hours_since, 1),
            "entity": entity_name,
        }

    # ── 3e. Velocity detection ────────────────────────────────────────────

    async def _compute_entity_velocity(self, window_minutes: int = 30) -> dict[str, dict]:
        """Compute mention velocity for entities — detect acceleration spikes."""
        now = datetime.now(timezone.utc)
        recent_cutoff = now - timedelta(minutes=window_minutes)
        prior_cutoff = now - timedelta(minutes=window_minutes * 2)

        async with get_session() as session:
            # Recent window mentions by entity
            recent_rows = (await session.execute(text("""
                SELECT e.name, e.ticker, COUNT(*) as mentions,
                       array_agg(DISTINCT r.platform) as platforms
                FROM sentiment_scores ss
                JOIN entities e ON ss.entity_id = e.id
                JOIN filtered_posts fp ON ss.filtered_post_id = fp.id
                JOIN raw_posts r ON fp.raw_post_id = r.id
                WHERE fp.filtered_at >= :cutoff
                GROUP BY e.name, e.ticker
                HAVING COUNT(*) >= 2
            """), {"cutoff": recent_cutoff})).all()

            # Prior window mentions by entity
            prior_rows = (await session.execute(text("""
                SELECT e.name, COUNT(*) as mentions
                FROM sentiment_scores ss
                JOIN entities e ON ss.entity_id = e.id
                JOIN filtered_posts fp ON ss.filtered_post_id = fp.id
                WHERE fp.filtered_at >= :prior AND fp.filtered_at < :recent
                GROUP BY e.name
            """), {"prior": prior_cutoff, "recent": recent_cutoff})).all()

        prior_map = {r.name: r.mentions for r in prior_rows}
        velocities: dict[str, dict] = {}

        for r in recent_rows:
            prior = prior_map.get(r.name, 0)
            ratio = r.mentions / max(prior, 1)
            platforms = r.platforms if r.platforms else []
            # Clean platforms list (postgres array_agg can include None)
            platforms = [p for p in platforms if p]

            # Novel entity surge: was zero mentions, now appearing on 3+ platforms
            is_novel_surge = (prior == 0 and r.mentions >= 2 and len(platforms) >= 3)

            if ratio >= 3.0 or (r.mentions >= 3 and len(platforms) >= 2) or is_novel_surge:
                velocities[r.name] = {
                    "ticker": r.ticker,
                    "recent_mentions": r.mentions,
                    "prior_mentions": prior,
                    "velocity_ratio": round(ratio, 1),
                    "platforms": platforms,
                    "cross_platform": len(platforms) >= 3,
                    "is_spike": ratio >= 3.0,
                    "is_novel_surge": is_novel_surge,
                }

        if velocities:
            logger.info(
                "[brain] velocity spikes detected: %s",
                ", ".join(f"{k} ({v['velocity_ratio']}x on {len(v['platforms'])} platforms)" for k, v in velocities.items()),
            )
        return velocities

    # ── 4. Signal generation with priority scoring + adversarial review ─

    async def generate_signals(self, synthesis: dict[str, Any]) -> list[Signal]:
        """Convert synthesis into actionable trading signals with intelligence scoring and adversarial review."""
        if not synthesis.get("themes") and not synthesis.get("alerts"):
            return []

        # Build user prompt with source data for traceability
        synth_for_prompt = {k: v for k, v in synthesis.items() if not k.startswith("_")}
        source_posts = synthesis.get("_source_posts", [])
        accuracy_ctx = await self._get_accuracy_context()
        learning_ctx = await self._get_learning_context()
        playbook_ctx = await self._get_playbook_context()
        adaptive_thresholds = await self._get_adaptive_thresholds()

        # Compute entity velocity for injection into prompt
        velocity_data = await self._compute_entity_velocity(window_minutes=30)
        velocity_ctx = ""
        if velocity_data:
            velocity_ctx = "\n\n## Entity Velocity Spikes (last 30 min)\n"
            for name, v in velocity_data.items():
                velocity_ctx += (
                    f"- {name} ({v.get('ticker', '?')}): {v['velocity_ratio']}x acceleration, "
                    f"{v['recent_mentions']} mentions on {', '.join(v['platforms'])}. "
                )
                if v.get("cross_platform"):
                    velocity_ctx += "CROSS-PLATFORM SPIKE. "
                velocity_ctx += "\n"
            velocity_ctx += "\nVelocity spikes with ratio > 3x should be CRITICAL priority. Cross-platform spikes (3+ platforms) = maximum urgency.\n"

        # Detect cross-platform convergence
        convergence = await self._detect_convergence(window_minutes=60)
        convergence_ctx = ""
        if convergence:
            convergence_ctx = "\n\n## CONVERGENCE ALERTS (auto-detected, HIGH PRIORITY)\n"
            for c in convergence:
                convergence_ctx += (
                    f"- {c['entity']} ({c.get('ticker', '?')}): appearing on {c['platform_count']} platforms "
                    f"({', '.join(c['platforms'])}) — {c['mention_count']} mentions. "
                    f"This entity was NOT mentioned 2+ hours ago. Generate a CRITICAL signal.\n"
                )

        user_prompt = (
            f"Intelligence synthesis:\n{json.dumps(synth_for_prompt, indent=2, default=str)}\n\n"
            f"Available source posts for citation:\n{json.dumps(source_posts, indent=2, default=str)}"
        )
        if accuracy_ctx:
            user_prompt += f"\n\n{accuracy_ctx}"
        if learning_ctx:
            user_prompt += learning_ctx
        if playbook_ctx:
            user_prompt += playbook_ctx
        if velocity_ctx:
            user_prompt += velocity_ctx
        if convergence_ctx:
            user_prompt += convergence_ctx

        try:
            raw_response = await self._llm.complete(SIGNAL_PROMPT, user_prompt, json_mode=True)
            signal_dicts = self._parse_json_array(raw_response)
        except Exception:
            logger.warning("[brain] signal generation failed", exc_info=True)
            return []

        # ── Adversarial review pass ─────────────────────────────────────
        if signal_dicts:
            signal_dicts = await self._adversarial_review(signal_dicts)

        # ── Compute exclusivity for source posts ────────────────────────
        exclusivity_info = self._compute_exclusivity_edge(source_posts)

        # ── Track first seen for entities in source posts ───────────────
        for sp in source_posts:
            platform = sp.get("platform", "unknown")
            post_id = sp.get("post_id", "")
            for theme in synthesis.get("themes", []):
                for ent_name in theme.get("entities_involved", []):
                    try:
                        await self._track_first_seen(ent_name, platform, post_id)
                    except Exception:
                        pass

        # ── Check for earnings catalysts via Redis ────────────────────
        earnings_tickers: set[str] = set()
        try:
            keys = await self._redis.keys("alekfi:earnings:*")
            for k in keys:
                ticker = k.split(":")[-1]
                earnings_tickers.add(ticker.upper())
        except Exception:
            pass

        noise_threshold = adaptive_thresholds.get("noise_score_threshold", 35)
        min_conv_by_type = adaptive_thresholds.get("min_conviction_by_type", {})
        require_corroboration = adaptive_thresholds.get("require_corroboration", [])

        # ── Persist surviving signals (with dedup + noise filtering) ───
        signals: list[Signal] = []
        async with get_session() as session:
            for sd in signal_dicts:
                conviction = float(sd.get("conviction", 0))
                sig_type = sd.get("signal_type", "sentiment_momentum")

                # Adaptive conviction threshold
                min_conv = min_conv_by_type.get(sig_type, 0.4)
                if conviction < min_conv:
                    continue

                # ── Signal dedup: merge with existing if same instrument + type in last 8h ──
                primary_sym = ""
                for inst in sd.get("affected_instruments", []):
                    if inst.get("symbol"):
                        primary_sym = inst["symbol"]
                        break
                if primary_sym:
                    dedup_cutoff = datetime.now(timezone.utc) - timedelta(hours=8)
                    existing_stmt = (
                        select(Signal)
                        .where(Signal.signal_type == sig_type)
                        .where(Signal.created_at >= dedup_cutoff)
                    )
                    existing_result = await session.execute(existing_stmt)
                    existing_signals = existing_result.scalars().all()
                    duplicate = None
                    for es in existing_signals:
                        es_syms = [i.get("symbol", "") for i in (es.affected_instruments or [])]
                        if primary_sym in es_syms:
                            duplicate = es
                            break
                    if duplicate:
                        old_meta = duplicate.metadata_ or {}
                        old_score = old_meta.get("total_intelligence_score", old_meta.get("priority_score", 0))
                        new_score = sd.get("total_intelligence_score", 0)
                        merged_score = max(old_score, new_score)
                        old_sources = duplicate.source_posts or []
                        new_sources = sd.get("sources", sd.get("evidence", []))
                        if isinstance(new_sources, list) and isinstance(old_sources, list):
                            merged_sources = old_sources + [s for s in new_sources if s not in old_sources]
                        else:
                            merged_sources = old_sources
                        old_meta["total_intelligence_score"] = merged_score
                        old_meta["priority_score"] = merged_score
                        old_meta["priority_label"] = self._intelligence_tier(merged_score)
                        old_meta["dedup_merged"] = True
                        duplicate.metadata_ = old_meta
                        duplicate.source_posts = merged_sources[:20]
                        duplicate.conviction = max(duplicate.conviction or 0, conviction)
                        if sd.get("thesis"):
                            duplicate.thesis = sd["thesis"]
                        logger.info("[brain] dedup merged signal %s %s (score=%d)", primary_sym, sig_type, merged_score)
                        continue

                # ── Intelligence scoring ──────────────────────────────
                total_score, intelligence_scores = self._compute_intelligence_score(sd, source_posts, synthesis)

                # Use LLM score if provided and reasonable
                llm_score = sd.get("total_intelligence_score")
                if llm_score and isinstance(llm_score, (int, float)) and 5 <= llm_score <= 100:
                    total_score = max(total_score, int(llm_score))

                score_source = "intelligence_framework"

                # ── Exclusivity boost ──────────────────────────────────
                sig_sources = sd.get("sources", sd.get("evidence", []))
                sig_exclusivity = self._compute_exclusivity_edge(
                    sig_sources if isinstance(sig_sources, list) and sig_sources and isinstance(sig_sources[0], dict)
                    else source_posts
                )
                edge_type = sig_exclusivity.get("edge_type", "unknown")

                entity_names = [i.get("symbol", "") for i in sd.get("affected_instruments", []) if i.get("symbol")]
                pre_inst = await self._check_pre_institutional(entity_names, source_posts)

                if pre_inst.get("is_pre_institutional"):
                    edge_type = "pre_institutional"
                    total_score = max(total_score, 80)
                    score_source += "+pre_institutional"
                    logger.info("[brain] PRE-INSTITUTIONAL signal detected: %s", ", ".join(entity_names))
                elif edge_type == "cross_platform":
                    total_score = min(100, total_score + 25)
                    score_source += "+cross_platform"
                elif edge_type == "retail_only" and sig_exclusivity["avg_exclusivity"] >= 0.75:
                    total_score = min(100, total_score + 20)
                    score_source += "+high_exclusivity"

                # ── Earnings catalyst boost ───────────────────────────
                has_earnings_catalyst = False
                for ent_name in entity_names:
                    if ent_name.upper() in earnings_tickers:
                        has_earnings_catalyst = True
                        total_score = min(100, total_score + 15)
                        score_source += "+earnings_catalyst"
                        break

                # ── Velocity boost ─────────────────────────────────────
                velocity_match = None
                for ent_name in entity_names:
                    if ent_name in velocity_data:
                        velocity_match = velocity_data[ent_name]
                        break
                if velocity_match:
                    if velocity_match.get("cross_platform"):
                        total_score = max(total_score, 85)
                        score_source += "+cross_plat_velocity"
                    elif velocity_match.get("is_spike"):
                        total_score = min(100, total_score + 15)
                        score_source += "+velocity_spike"

                # ── Convergence boost ──────────────────────────────────
                convergence_match = None
                for c in convergence:
                    if c.get("ticker") and c["ticker"] in entity_names:
                        convergence_match = c
                        break
                    if c.get("entity") and any(c["entity"].lower() in en.lower() for en in entity_names):
                        convergence_match = c
                        break
                if convergence_match:
                    total_score = max(total_score, 85)
                    intelligence_scores["information_asymmetry"] = max(intelligence_scores.get("information_asymmetry", 10), 18)
                    intelligence_scores["time_sensitivity"] = max(intelligence_scores.get("time_sensitivity", 10), 16)
                    score_source += "+convergence"

                # ── Corroboration check ────────────────────────────────
                if sig_type in require_corroboration:
                    sig_platforms = set()
                    for sp in (sig_sources if isinstance(sig_sources, list) else source_posts):
                        if isinstance(sp, dict):
                            sig_platforms.add(sp.get("platform", ""))
                    if len(sig_platforms) < 2:
                        total_score = max(5, total_score - 15)
                        score_source += "+single_source_penalty"

                total_score = max(5, min(100, total_score))
                tier = self._intelligence_tier(total_score)

                # ── NOISE FILTER — don't persist low-quality signals ──
                if total_score < noise_threshold:
                    self.signals_noise_filtered += 1
                    logger.debug("[brain] filtered NOISE signal %s %s (score=%d < %d)",
                                 primary_sym, sig_type, total_score, noise_threshold)
                    continue

                time_to_mainstream = None
                for ent_name in entity_names:
                    ttm = await self._get_time_to_mainstream(ent_name)
                    if ttm and ttm.get("lead_time_hours", 0) > 0.5:
                        time_to_mainstream = ttm
                        break

                syms = ", ".join(i.get("symbol", "?") for i in sd.get("affected_instruments", []))
                logger.info(
                    "[brain] signal %s %s score=%d (%s) tier=%s conv=%.2f action=%s edge=%s",
                    syms, sig_type, total_score, score_source, tier,
                    conviction, sd.get("actionability", "?"), edge_type,
                )

                expires_hours = int(sd.get("expires_in_hours", 24))
                freshness_label = self._compute_freshness(source_posts)

                signal = Signal(
                    id=uuid.uuid4(),
                    signal_type=sig_type,
                    affected_instruments=sd.get("affected_instruments", []),
                    direction=sd.get("direction", "LONG").upper(),
                    conviction=conviction,
                    time_horizon=sd.get("time_horizon", "DAYS"),
                    thesis=sd.get("thesis", ""),
                    source_posts=sd.get("sources", sd.get("evidence")),
                    metadata_={
                        "synthesis_themes": [t.get("theme") for t in synthesis.get("themes", [])],
                        "category": self._derive_category(sd, synthesis),
                        "headline": sd.get("headline", ""),
                        "total_intelligence_score": total_score,
                        "intelligence_scores": intelligence_scores,
                        "intelligence_tier": tier,
                        "priority_score": total_score,  # backward compat
                        "priority_label": tier,
                        "priority_score_source": score_source,
                        "suggested_action": sd.get("suggested_action", "WATCH"),
                        "actionability": sd.get("actionability", "research_needed"),
                        "price_implication": sd.get("price_implication"),
                        "priced_in": sd.get("priced_in", sd.get("priced_in_assessment", "partially_priced_in")),
                        "consensus_position": sd.get("consensus_position", "neutral"),
                        "freshness_label": freshness_label,
                        "reasoning_chain": sd.get("reasoning_chain", ""),
                        "counter_argument": sd.get("counter_argument", ""),
                        "review_notes": sd.get("review_notes", ""),
                        "exclusivity_edge": edge_type,
                        "exclusivity_detail": sig_exclusivity,
                        "time_to_mainstream": time_to_mainstream,
                        "velocity_match": velocity_match,
                        "convergence_match": convergence_match,
                        "earnings_catalyst": has_earnings_catalyst,
                    },
                    expires_at=datetime.now(timezone.utc) + timedelta(hours=expires_hours),
                )
                session.add(signal)
                signals.append(signal)

        self.signals_generated += len(signals)
        logger.info(
            "[brain] generated %d signals (%d noise-filtered, %d review-killed)",
            len(signals), self.signals_noise_filtered, self.signals_reviewed_out,
        )
        return signals

    async def _adversarial_review(self, signal_dicts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Run adversarial review pass on generated signals."""
        review_prompt = f"Review these {len(signal_dicts)} signals:\n{json.dumps(signal_dicts, indent=2, default=str)}"

        try:
            raw_response = await self._llm.complete(REVIEW_PROMPT, review_prompt, json_mode=True)
            reviews = self._parse_json_array(raw_response)
        except Exception:
            logger.warning("[brain] adversarial review failed, using original signals", exc_info=True)
            return signal_dicts

        # Apply review results
        surviving: list[dict[str, Any]] = []
        for review in reviews:
            if not review.get("survives", True):
                self.signals_reviewed_out += 1
                continue
            idx = review.get("signal_index", -1)
            if 0 <= idx < len(signal_dicts):
                sig = signal_dicts[idx]
                # Use reviewer's score if provided; otherwise keep original (computed properly upstream)
                review_score = review.get("priority_score")
                if review_score is not None:
                    sig["priority_score"] = int(review_score)
                # If original signal also lacks a score, it will be computed during persistence
                sig["review_notes"] = review.get("review_notes", "")
                surviving.append(sig)

        # If review returned no valid indices, fall back to originals
        if not surviving and signal_dicts:
            logger.warning("[brain] adversarial review removed all signals, keeping originals")
            return signal_dicts

        removed = len(signal_dicts) - len(surviving)
        if removed > 0:
            logger.info("[brain] adversarial review removed %d/%d signals", removed, len(signal_dicts))
        return surviving

    # ── Convergence detection ──────────────────────────────────────────

    async def _detect_convergence(self, window_minutes: int = 60) -> list[dict]:
        """Detect entities appearing on 4+ platforms in the window that weren't mentioned earlier."""
        now = datetime.now(timezone.utc)
        recent = now - timedelta(minutes=window_minutes)
        prior = now - timedelta(minutes=window_minutes + 120)  # 2h before window

        async with get_session() as session:
            # Recent: entities with 4+ distinct platforms
            recent_rows = (await session.execute(text("""
                SELECT e.name, e.ticker, COUNT(DISTINCT r.platform) as platform_count,
                       COUNT(*) as mention_count,
                       array_agg(DISTINCT r.platform) as platforms
                FROM sentiment_scores ss
                JOIN entities e ON ss.entity_id = e.id
                JOIN filtered_posts fp ON ss.filtered_post_id = fp.id
                JOIN raw_posts r ON fp.raw_post_id = r.id
                WHERE fp.filtered_at >= :recent
                GROUP BY e.name, e.ticker
                HAVING COUNT(DISTINCT r.platform) >= 4
            """), {"recent": recent})).all()

            if not recent_rows:
                return []

            # Prior: check if these entities were mentioned 2+ hours ago
            prior_names = [r.name for r in recent_rows]
            prior_rows = (await session.execute(text("""
                SELECT DISTINCT e.name
                FROM sentiment_scores ss
                JOIN entities e ON ss.entity_id = e.id
                JOIN filtered_posts fp ON ss.filtered_post_id = fp.id
                WHERE fp.filtered_at >= :prior AND fp.filtered_at < :recent
                  AND e.name = ANY(:names)
            """), {"prior": prior, "recent": recent, "names": prior_names})).all()

        prior_set = {r.name for r in prior_rows}
        convergence_signals = []
        for r in recent_rows:
            if r.name not in prior_set:  # NEW convergence — wasn't mentioned before
                convergence_signals.append({
                    "entity": r.name,
                    "ticker": r.ticker,
                    "platform_count": r.platform_count,
                    "mention_count": r.mention_count,
                    "platforms": [p for p in (r.platforms or []) if p],
                })
                logger.info(
                    "[brain] CONVERGENCE ALERT: %s on %d platforms in %dmin — %s",
                    r.name, r.platform_count, window_minutes,
                    ", ".join(p for p in (r.platforms or []) if p),
                )
        return convergence_signals

    # ── Learning context injection ─────────────────────────────────────

    async def _get_learning_context(self) -> str:
        """Fetch the latest learning report from Redis."""
        try:
            report = await self._redis.get("alekfi:learning_report")
            if report:
                return f"\n\n=== SYSTEM LEARNING (auto-generated from historical accuracy) ===\n{report}\n=== END LEARNING ===\n"
        except Exception:
            pass
        return ""

    async def _get_adaptive_thresholds(self) -> dict:
        """Fetch adaptive thresholds from Redis."""
        try:
            raw = await self._redis.get("alekfi:adaptive_thresholds")
            if raw:
                return json.loads(raw)
        except Exception:
            pass
        return {"noise_score_threshold": 35, "min_conviction_by_type": {}, "require_corroboration": []}

    async def _get_playbook_context(self) -> str:
        """Fetch the winning patterns playbook from Redis."""
        try:
            raw = await self._redis.get("alekfi:playbook")
            if raw:
                patterns = json.loads(raw)
                if patterns:
                    lines = ["\n=== WINNING PATTERNS (from historical data, replicate these) ==="]
                    for p in patterns[:10]:
                        lines.append(f"- {p.get('description', 'unknown pattern')}")
                    lines.append("When you see a signal matching these patterns, score it HIGH.")
                    lines.append("=== END PATTERNS ===\n")
                    return "\n".join(lines)
        except Exception:
            pass
        return ""

    # ── Intelligence scoring helpers ───────────────────────────────────

    def _compute_intelligence_score(self, sd: dict, source_posts: list[dict], synthesis: dict) -> tuple[int, dict]:
        """Compute intelligence score from LLM output or fallback."""
        scores = sd.get("intelligence_scores", {})

        # Earnings impact
        ei = int(scores.get("earnings_impact", 8))
        if sd.get("earnings_catalyst"):
            ei = max(ei, 15)
        if sd.get("signal_type") == "insider_signal":
            ei = max(ei, 16)
        if sd.get("signal_type") in ("narrative_shift", "sentiment_momentum"):
            ei = min(ei, max(ei, 6))  # don't cap if LLM scored high

        # Information asymmetry from source exclusivity
        ia = int(scores.get("information_asymmetry", 10))
        platforms = [sp.get("platform", "") for sp in source_posts]
        if platforms:
            avg_excl = sum(SOURCE_EXCLUSIVITY.get(p, 0.5) for p in platforms) / len(platforms)
            ia = max(ia, int(avg_excl * 20))

        # Time sensitivity from horizon
        ts = int(scores.get("time_sensitivity", 10))
        horizon_map = {"MINUTES": 18, "HOURS": 14, "DAYS": 10, "WEEKS": 6, "MONTHS": 3}
        ts = max(ts, horizon_map.get(sd.get("time_horizon", "DAYS"), 10))

        # Market cap exposure
        mc = int(scores.get("market_cap_exposure", 10))
        instruments = [i.get("symbol", "") for i in sd.get("affected_instruments", [])]
        if any(t in MEGA_CAPS for t in instruments):
            mc = max(mc, 16)

        # Catalyst clarity from conviction
        cc = int(scores.get("catalyst_clarity", 10))
        cc = max(cc, int(float(sd.get("conviction", 0.5)) * 20))

        total = ei + ia + ts + mc + cc

        # Contrarian boost
        if sd.get("consensus_position") == "contrarian" and total >= 50:
            total += 10

        total = max(5, min(100, total))

        return total, {
            "earnings_impact": ei, "information_asymmetry": ia,
            "time_sensitivity": ts, "market_cap_exposure": mc, "catalyst_clarity": cc,
        }

    @staticmethod
    def _intelligence_tier(score: int) -> str:
        if score >= 80:
            return "CRITICAL"
        if score >= 65:
            return "HIGH"
        if score >= 50:
            return "MODERATE"
        if score >= 35:
            return "LOW"
        return "NOISE"

    # ── main run loop ──────────────────────────────────────────────────

    async def run(self, once: bool = False) -> None:
        """Process unanalyzed posts with batched parallel analysis, periodic synthesis."""
        synthesis_interval_sec = int(os.environ.get(
            "BRAIN_SYNTHESIS_INTERVAL", str(self._synthesis_interval)
        )) * 60
        logger.info(
            "[brain] starting (synthesis=%ds, batch=%d, parallel=%d)",
            synthesis_interval_sec, self._entity_batch_size, self._parallel_batches,
        )
        last_synthesis = datetime.now(timezone.utc) - timedelta(seconds=synthesis_interval_sec)

        while True:
            cycle_start = time.monotonic()

            # Fetch pending posts with PRIORITY QUEUE ordering
            async with get_session() as session:
                stmt = (
                    select(FilteredPost)
                    .where(FilteredPost.analyzed == False)  # noqa: E712
                    .order_by(
                        # Priority: breaking first, then significant, then by urgency, then recency
                        case(
                            (FilteredPost.relevance_tier == "breaking", 1),
                            (FilteredPost.relevance_tier == "significant", 2),
                            (FilteredPost.relevance_tier == "contextual", 3),
                            else_=4,
                        ),
                        case(
                            (FilteredPost.urgency == "HIGH", 1),
                            (FilteredPost.urgency == "MEDIUM", 2),
                            else_=3,
                        ),
                        FilteredPost.filtered_at.desc(),
                    )
                    .limit(self._config.brain_batch_size)
                )
                result = await session.execute(stmt)
                pending = result.scalars().all()

            # Count total backlog
            pending_count = 0
            async with get_session() as session:
                pending_count = (await session.execute(
                    select(func.count(FilteredPost.id)).where(FilteredPost.analyzed == False)  # noqa: E712
                )).scalar() or 0

            if not pending:
                # No posts to process, check synthesis
                now = datetime.now(timezone.utc)
                if (now - last_synthesis).total_seconds() >= synthesis_interval_sec:
                    synthesis = await self.synthesize(window_minutes=max(self._synthesis_interval, 5))
                    await self.generate_signals(synthesis)
                    last_synthesis = now

                if once:
                    synthesis = await self.synthesize(window_minutes=60)
                    await self.generate_signals(synthesis)
                    return
                await asyncio.sleep(5)
                continue

            # Fetch content for all pending posts
            post_contents: list[tuple[FilteredPost, str]] = []
            async with get_session() as session:
                for fp in pending:
                    raw = await session.get(RawPost, fp.raw_post_id)
                    content = raw.content if raw else "(no content)"
                    post_contents.append((fp, content))

            # Split into batches of entity_batch_size
            batches = [
                post_contents[i:i + self._entity_batch_size]
                for i in range(0, len(post_contents), self._entity_batch_size)
            ]

            # Process batches in parallel groups
            total_analyzed = 0
            new_entities = 0
            entities_before = self.entities_extracted

            for i in range(0, len(batches), self._parallel_batches):
                group = batches[i:i + self._parallel_batches]
                # Launch parallel batch analysis
                batch_results = await asyncio.gather(
                    *[self._analyze_entity_batch(b) for b in group],
                    return_exceptions=True,
                )
                # Process results
                for batch_posts, batch_result in zip(group, batch_results):
                    if isinstance(batch_result, Exception):
                        logger.error("[brain] parallel batch failed: %s", batch_result)
                        # Fallback: process individually
                        for fp, content in batch_posts:
                            try:
                                entities = await self.analyze_entities(fp)
                                if entities:
                                    await self.analyze_sentiment(fp, entities)
                                async with get_session() as session:
                                    await session.execute(
                                        update(FilteredPost).where(FilteredPost.id == fp.id).values(analyzed=True)
                                    )
                                self.posts_analyzed += 1
                                total_analyzed += 1
                            except Exception:
                                logger.warning("[brain] individual fallback failed for %s", fp.id)
                    else:
                        count = await self._process_batch_results(batch_posts, batch_result)
                        total_analyzed += count

            new_entities = self.entities_extracted - entities_before

            # Synthesis check
            now = datetime.now(timezone.utc)
            new_signals = 0
            if (now - last_synthesis).total_seconds() >= synthesis_interval_sec:
                signals_before = self.signals_generated
                synthesis = await self.synthesize(window_minutes=max(self._synthesis_interval, 5))
                await self.generate_signals(synthesis)
                last_synthesis = now
                new_signals = self.signals_generated - signals_before

            # ── THROUGHPUT LOGGING ──────────────────────────────────────
            elapsed = time.monotonic() - cycle_start
            rate = total_analyzed / max(elapsed, 0.1) * 60
            logger.info(
                "[brain] cycle: analyzed %d in %.1fs (%.1f/min) | backlog=%d | entities=%d | signals=%d",
                total_analyzed, elapsed, rate, pending_count - total_analyzed, new_entities, new_signals,
            )

            if once:
                synthesis = await self.synthesize(window_minutes=60)
                await self.generate_signals(synthesis)
                return

            await asyncio.sleep(2)

    # ── helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _priority_label(score: int) -> str:
        """Map priority score to label (backward compat alias)."""
        if score >= 80:
            return "CRITICAL"
        if score >= 65:
            return "HIGH"
        if score >= 50:
            return "MODERATE"
        if score >= 35:
            return "LOW"
        return "NOISE"

    @staticmethod
    def _compute_freshness(source_posts: list[dict]) -> str:
        """Compute freshness label from source post timestamps."""
        now = datetime.now(timezone.utc)
        ages_minutes: list[float] = []
        for sp in source_posts:
            pub = sp.get("source_published_at")
            if pub:
                try:
                    dt = datetime.fromisoformat(pub)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    ages_minutes.append((now - dt).total_seconds() / 60)
                except (ValueError, TypeError):
                    pass
        if not ages_minutes:
            return "RECENT"
        avg_age = sum(ages_minutes) / len(ages_minutes)
        if avg_age < 30:
            return "BREAKING"
        if avg_age < 120:
            return "FRESH"
        if avg_age < 1440:
            return "RECENT"
        return "DATED"

    @staticmethod
    def _derive_category(signal_dict: dict, synthesis: dict) -> str:
        """Derive a category from signal type and synthesis themes."""
        sig_type = signal_dict.get("signal_type", "")
        if "insider" in sig_type:
            return "insider_activity"
        if "convergence" in sig_type:
            return "source_convergence"
        themes = synthesis.get("themes", [])
        if themes:
            for t in themes:
                theme = t.get("theme", "").lower()
                if "earnings" in theme:
                    return "earnings"
                if "macro" in theme or "fed" in theme or "rate" in theme:
                    return "macro"
                if "geopolit" in theme:
                    return "geopolitical"
                if "crypto" in theme:
                    return "crypto"
                if "supply" in theme:
                    return "supply_chain"
                if "tech" in theme or "ai" in theme:
                    return "technology"
                if "consumer" in theme:
                    return "consumer"
                if "energy" in theme or "oil" in theme:
                    return "commodity"
        return "technology"

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
            "signals_reviewed_out": self.signals_reviewed_out,
            "signals_noise_filtered": self.signals_noise_filtered,
            "syntheses_run": self.syntheses_run,
            "entity_batch_size": self._entity_batch_size,
            "parallel_batches": self._parallel_batches,
            "llm_tokens": self._llm.token_usage,
        }
