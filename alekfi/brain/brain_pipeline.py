"""BrainPipeline — 5-stage intelligence pipeline replacing single-pass synthesis.

Stage 1: Deterministic entity extraction (no LLM)
Stage 2: Deduplication & novelty detection (Redis clustering)
Stage 3: Source convergence scoring (weighted multi-source)
Stage 4: LLM synthesis (only for high-convergence clusters)
Stage 5: Risk-adjusted scoring (historical performance, sector concentration)
"""

from __future__ import annotations

import hashlib
import json
import os
import logging
import re
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, TYPE_CHECKING

import redis.asyncio as aioredis
from sqlalchemy import select, text, func

from alekfi.db.database import get_session
from alekfi.db.models import Entity, FilteredPost, RawPost, SentimentScore, Signal, SignalFeedback, SignalSourceItem
from alekfi.brain.research_bundle import build_research_bundle

if TYPE_CHECKING:
    from alekfi.brain.brain import BrainAnalyzer

logger = logging.getLogger(__name__)

_SIGNAL_GEN_TOP_K = int(os.environ.get("SIGNAL_GEN_TOP_K", "8"))

# ── Ticker extraction regex ──────────────────────────────────────────
# Matches $AAPL, $BTC, cashtag format
_CASHTAG_RE = re.compile(r'\$([A-Z]{1,5})\b')
# Matches standalone all-caps 1-5 letter words that look like tickers
_TICKER_RE = re.compile(r'\b([A-Z]{2,5})\b')

# Common words to exclude from ticker extraction
_TICKER_STOPWORDS = {
    "THE", "AND", "FOR", "ARE", "BUT", "NOT", "YOU", "ALL", "CAN", "HER",
    "WAS", "ONE", "OUR", "OUT", "HAS", "HIM", "HOW", "MAN", "NEW", "NOW",
    "OLD", "SEE", "WAY", "WHO", "BOY", "DID", "GET", "HIS", "LET", "SAY",
    "SHE", "TOO", "USE", "DAD", "MOM", "ITS", "BIG", "CEO", "CFO", "COO",
    "IPO", "ETF", "GDP", "CPI", "FED", "SEC", "FDA", "IMF", "ECB", "BOJ",
    "ATH", "ATL", "BPS", "DCF", "EPS", "ROI", "ROE", "YOY", "MOM", "QOQ",
    "API", "AWS", "AI", "CEO", "CT", "US", "UK", "EU", "JUST", "LIKE",
    "THAN", "THEM", "THEN", "THEY", "THIS", "THAT", "WHAT", "WHEN",
    "WILL", "WITH", "BEEN", "FROM", "HAVE", "MUCH", "VERY", "SOME",
    "ALSO", "BACK", "OVER", "ONLY", "EVEN", "MOST", "MADE", "AFTER",
    "LONG", "SHORT", "BULL", "BEAR", "PUMP", "DUMP", "HOLD", "SELL",
    "BUY", "CALL", "PUT", "RISK", "GAIN", "LOSS", "HIGH", "LOW",
    "SELL", "OPEN", "CLOSE", "WEEK", "YEAR", "NEWS", "POST", "SAYS",
    "GOES", "HUGE", "MORE", "MANY", "GOOD", "DOWN", "WELL", "REAL",
    "FREE", "LAST", "NEXT", "BEST", "LOOK", "TAKE", "COME", "MAKE",
    "KNOW", "TIME", "NEED", "HERE", "KEEP", "FIND", "GIVE", "TELL",
    "HELP", "SHOW", "TURN", "MOVE", "LIVE", "FEEL", "FACT", "PART",
    "EACH", "SAME", "SURE", "DOES", "STILL", "STOCK", "TRADE", "ALERT",
    "UPDATE", "WATCH", "GREEN", "DAILY", "PRICE", "SHARE", "THINK",
    "ABOUT", "COULD", "WOULD", "THEIR", "WHICH", "THESE", "THOSE",
    "OTHER", "FIRST", "EVERY", "NEVER", "UNDER", "MONEY", "GOING",
    "TODAY", "RIGHT", "GREAT", "WORLD", "WHERE", "BEING", "POINT",
}

# Known valid tickers (mega caps + key names) to always accept
_KNOWN_TICKERS = {
    "AAPL", "MSFT", "NVDA", "GOOGL", "GOOG", "AMZN", "META", "TSLA", "BRK",
    "JPM", "V", "MA", "JNJ", "UNH", "XOM", "PG", "HD", "AVGO", "LLY",
    "SPY", "QQQ", "DIA", "IWM", "AMD", "INTC", "QCOM", "CRM", "ADBE",
    "ORCL", "NFLX", "DIS", "COST", "WMT", "NKE", "BA", "CAT", "GS",
    "MS", "BAC", "WFC", "C", "BLK", "COIN", "SQ", "PYPL", "UBER",
    "ABNB", "SHOP", "SNOW", "PLTR", "CRWD", "PANW", "NET", "DDOG",
    "MU", "MRVL", "ARM", "ASML", "TXN", "AMAT", "LRCX", "KLAC",
    "PFE", "ABBV", "MRK", "BMY", "AMGN", "GILD", "ISRG", "TMO",
    "XLF", "XLE", "XLV", "XLY", "XLI", "XLU", "XLB", "XLRE", "XLC",
    "BTC", "ETH", "SOL", "DOGE", "XRP", "ADA", "AVAX", "MATIC", "LINK",
    "MSTR", "MARA", "RIOT", "HOOD", "SOFI", "SMCI", "DELL", "HPQ",
    "T", "VZ", "TMUS", "CMCSA", "KO", "PEP", "PM", "MO",
    "GE", "HON", "UPS", "RTX", "LMT", "DE",
}

# ── Event type classification (deterministic) ────────────────────────
_EVENT_PATTERNS: list[tuple[list[str], str]] = [
    (["earnings", "revenue", "eps", "beat expectations", "miss expectations",
      "quarterly results", "guidance", "q1", "q2", "q3", "q4", "10-q", "10q"], "earnings"),
    (["insider", "form 4", "bought shares", "sold shares", "insider trading",
      "insider buying", "insider selling", "officer purchase"], "insider"),
    (["fda", "approval", "clinical", "trial", "drug", "phase 3", "phase 2",
      "biotech", "pdufa", "nda"], "biomedical"),
    (["sec", "investigation", "lawsuit", "regulatory", "fine", "penalty",
      "compliance", "subpoena", "antitrust", "probe"], "regulatory"),
    (["launch", "product", "announced", "unveil", "new feature",
      "partnership", "acquisition", "merger", "m&a", "deal"], "corporate_action"),
    (["tariff", "trade war", "sanctions", "geopolitical", "war", "conflict",
      "embargo", "fed", "rate cut", "rate hike", "fomc", "inflation",
      "cpi", "gdp", "recession", "treasury"], "macro"),
    (["upgrade", "downgrade", "price target", "analyst", "rating",
      "outperform", "underperform", "initiate"], "analyst_action"),
    (["short squeeze", "gamma squeeze", "short interest", "dark pool",
      "unusual volume", "options flow", "calls sweep", "puts sweep"], "flow_anomaly"),
    (["prediction market", "polymarket", "kalshi", "probability",
      "prediction", "betting odds", "forecast"], "prediction_market"),
]

# ── Pipeline Redis keys ──────────────────────────────────────────────
_PIPELINE_DEDUP_PREFIX = "alekfi:pipeline:dedup:"
_CROSS_DEDUP_ENABLED = os.environ.get("CROSS_DEDUP_ENABLED", "1") == "1"
_CROSS_DEDUP_TTL = int(os.environ.get("CROSS_DEDUP_TTL", "3600"))  # 1 hour
_CROSS_DEDUP_SIM_THRESHOLD = float(os.environ.get("CROSS_DEDUP_SIM_THRESHOLD", "0.60"))  # Jaccard similarity threshold for thesis dedup
_DIRECTION_BALANCE_ENABLED = os.environ.get("DIRECTION_BALANCE_ENABLED", "1") == "1"
_DIRECTION_LONG_CAP = float(os.environ.get("DIRECTION_LONG_CAP", "0.85"))
_QUALITY_SCORE_ENABLED = os.environ.get("QUALITY_SCORE_ENABLED", "1") == "1"
_QUALITY_GATE_MIN = int(os.environ.get("QUALITY_GATE_MIN", "30"))
_SHADOW_PERSISTENCE_ENABLED = os.environ.get("SHADOW_PERSISTENCE_ENABLED", "1") == "1"
_MIN_PERSIST_PER_CYCLE = int(os.environ.get("MIN_PERSIST_PER_CYCLE", "0"))
_PIPELINE_DEDUP_TTL = 8 * 3600  # 8 hours


class PipelineCluster:
    """A cluster of posts about the same entity + event type."""

    __slots__ = (
        "primary_ticker", "event_type", "posts", "platforms",
        "entity_names", "convergence_score", "is_novel",
    )

    def __init__(self, primary_ticker: str, event_type: str) -> None:
        self.primary_ticker = primary_ticker
        self.event_type = event_type
        self.posts: list[dict[str, Any]] = []
        self.platforms: set[str] = set()
        self.entity_names: set[str] = set()
        self.convergence_score: float = 0.0
        self.is_novel: bool = True

    def add_post(self, post: dict[str, Any]) -> None:
        self.posts.append(post)
        self.platforms.add(post.get("platform", "unknown"))
        for ent in post.get("_extracted_entities", []):
            self.entity_names.add(ent)

    @property
    def post_count(self) -> int:
        return len(self.posts)

    @property
    def platform_count(self) -> int:
        return len(self.platforms)

    def to_dict(self) -> dict[str, Any]:
        return {
            "primary_ticker": self.primary_ticker,
            "event_type": self.event_type,
            "post_count": self.post_count,
            "platform_count": self.platform_count,
            "platforms": list(self.platforms),
            "entity_names": list(self.entity_names),
            "convergence_score": self.convergence_score,
            "is_novel": self.is_novel,
        }


class BrainPipeline:
    """5-stage intelligence pipeline for the brain."""

    def __init__(self, brain: BrainAnalyzer) -> None:
        self._brain = brain
        self._redis = brain._redis

        # Quality audit (lazy init)
        self._audit = None
        try:
            from alekfi.quality_audit import QualityAudit
            self._audit = QualityAudit(llm_client=brain._llm, redis_client=self._redis)
            logger.info("[pipeline] quality audit initialized")
        except Exception:
            logger.warning("[pipeline] quality audit not available", exc_info=True)

        self._llm = brain._llm
        self._config = brain._config

        # Import what we need from brain module
        from alekfi.brain.brain import SOURCE_EXCLUSIVITY
        self._source_exclusivity = SOURCE_EXCLUSIVITY

    async def _get_current_price(self, ticker: str) -> float | None:
        """Get current price from Redis cache or price_data table.

        Returns float price or None. Never raises.
        """
        if not ticker or ticker == "N/A":
            return None

        # Normalize ticker variants
        lookup_keys = [ticker]
        if not ticker.endswith("-USD") and ticker in ("BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "DOT", "MATIC"):
            lookup_keys.append(f"{ticker}-USD")

        # 1) Redis cache (fastest)
        for key in lookup_keys:
            try:
                raw = await self._redis.get(f"alekfi:price:{key}")
                if raw:
                    import json as _json
                    data = _json.loads(raw)
                    price = data.get("price")
                    if price and float(price) > 0:
                        logger.debug("[pipeline] price from Redis: %s = %.4f", key, float(price))
                        return round(float(price), 4)
            except Exception:
                pass

        # 2) price_data table (latest within 30 min)
        for key in lookup_keys:
            try:
                async with get_session() as price_session:
                    result = await price_session.execute(
                        text(
                            "SELECT price FROM price_data "
                            "WHERE symbol = :sym AND fetched_at >= NOW() - INTERVAL '30 minutes' "
                            "ORDER BY fetched_at DESC LIMIT 1"
                        ),
                        {"sym": key},
                    )
                    row = result.fetchone()
                    if row and row.price and float(row.price) > 0:
                        logger.debug("[pipeline] price from DB: %s = %.4f", key, float(row.price))
                        return round(float(row.price), 4)
            except Exception:
                pass

        logger.debug("[pipeline] no price found for %s", ticker)
        return None


    @staticmethod
    def _compute_signal_fingerprint(direction: str, instruments: list, thesis: str, time_horizon: str = "") -> str:
        """Compute SHA1 fingerprint for signal dedup.

        Normalizes: direction + sorted instrument symbols + thesis keywords.
        """
        # Normalize direction
        d = direction.upper().strip()

        # Sort instrument symbols
        syms = sorted(set(
            inst.get("symbol", "").upper().strip()
            for inst in (instruments or [])
            if inst.get("symbol")
        ))

        # Normalize thesis: lowercase, keep only alphanumeric + spaces, remove stop words
        import re as _re
        _STOP_WORDS = {
            "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could",
            "should", "may", "might", "can", "shall", "to", "of", "in", "for",
            "on", "with", "at", "by", "from", "as", "into", "through", "during",
            "before", "after", "above", "below", "between", "this", "that", "these",
            "those", "it", "its", "and", "but", "or", "nor", "not", "so", "yet",
            "both", "either", "neither", "each", "every", "all", "any", "few",
            "more", "most", "other", "some", "such", "no", "only", "own", "same",
            "than", "too", "very", "just", "also", "about", "which", "who", "whom",
            "what", "when", "where", "why", "how", "while", "if", "then", "because",
            "shows", "indicates", "suggests", "across", "multiple", "strong",
        }
        thesis_clean = _re.sub(r"[^a-z0-9 ]", " ", thesis.lower())
        thesis_words = [w for w in thesis_clean.split() if w and w not in _STOP_WORDS and len(w) > 2]
        # Sort for deterministic hash
        thesis_key = " ".join(sorted(set(thesis_words)))

        th = time_horizon.upper().strip() if time_horizon else ""
        raw = f"{d}|{'|'.join(syms)}|{th}|{thesis_key}"
        return hashlib.sha1(raw.encode()).hexdigest()[:16]

    @staticmethod
    def _thesis_similarity(thesis1: str, thesis2: str) -> float:
        """Compute Jaccard similarity between two thesis strings."""
        import re as _re
        _STOP = {"the", "a", "an", "is", "are", "was", "were", "be", "to", "of", "in",
                 "for", "on", "with", "at", "by", "from", "as", "and", "or", "not",
                 "this", "that", "it", "its", "has", "have", "had", "which", "who"}

        def _tokenize(t):
            words = _re.sub(r"[^a-z0-9 ]", " ", t.lower()).split()
            return set(w for w in words if w not in _STOP and len(w) > 2)

        s1 = _tokenize(thesis1)
        s2 = _tokenize(thesis2)
        if not s1 or not s2:
            return 0.0
        intersection = len(s1 & s2)
        union = len(s1 | s2)
        return intersection / union if union > 0 else 0.0

    # ══════════════════════════════════════════════════════════════════
    # MAIN PIPELINE ENTRY POINT
    # ══════════════════════════════════════════════════════════════════

    async def run(self, window_minutes: int = 15) -> list[Signal]:
        """Execute the full 5-stage pipeline. Returns persisted Signal objects."""
        t0 = time.monotonic()

        # Fetch recent posts
        posts = await self._fetch_recent_posts(window_minutes)
        if not posts:
            logger.info("[pipeline] no recent posts (window=%dm)", window_minutes)
            logger.info(json.dumps({
                "event": "brain_cycle_summary", "ts": datetime.now(timezone.utc).isoformat(),
                "pipeline_posts": 0, "pipeline_clusters": 0, "pipeline_novel_clusters": 0,
                "pipeline_high_conv": 0, "pipeline_signal_dicts": 0, "pipeline_signals_persisted": 0,
                "pipeline_noise_filtered": 0, "total_signals_generated": self._brain.signals_generated,
                "timing_total": 0, "exit_reason": "no_posts",
            }))
            return []

        # Stage 1: Deterministic entity extraction
        enriched_posts = self._stage1_extract_entities(posts)
        stage1_time = time.monotonic() - t0

        # Stage 2: Deduplication & novelty detection
        t1 = time.monotonic()
        clusters = await self._stage2_deduplicate(enriched_posts)
        novel_clusters = [c for c in clusters if c.is_novel]
        stage2_time = time.monotonic() - t1

        if not novel_clusters:
            logger.info(
                "[pipeline] all %d clusters are duplicates, skipping synthesis",
                len(clusters),
            )
            return []

        # Stage 3: Source convergence scoring
        t2 = time.monotonic()
        scored_clusters = self._stage3_score_convergence(novel_clusters)
        high_conv = await self._select_high_convergence_clusters(scored_clusters)
        stage3_time = time.monotonic() - t2

        if not high_conv:
            logger.info(
                "[pipeline] no clusters above convergence threshold (best=%.2f from %d clusters)",
                max((c.convergence_score for c in scored_clusters), default=0),
                len(scored_clusters),
            )
            logger.info(json.dumps({
                "event": "brain_cycle_summary", "ts": datetime.now(timezone.utc).isoformat(),
                "pipeline_posts": len(posts), "pipeline_clusters": len(clusters),
                "pipeline_novel_clusters": len(novel_clusters), "pipeline_high_conv": 0,
                "pipeline_signal_dicts": 0, "pipeline_signals_persisted": 0,
                "pipeline_noise_filtered": 0, "total_signals_generated": self._brain.signals_generated,
                "timing_total": round(time.monotonic() - t0, 2), "exit_reason": "no_high_convergence",
            }))
            return []

        # Stage 4: LLM synthesis (only for high-convergence clusters)
        t3 = time.monotonic()
        signal_dicts = await self._stage4_synthesize(high_conv, posts)
        stage4_time = time.monotonic() - t3

        if not signal_dicts:
            logger.info(json.dumps({
                "event": "brain_cycle_summary", "ts": datetime.now(timezone.utc).isoformat(),
                "pipeline_posts": len(posts), "pipeline_clusters": len(clusters),
                "pipeline_novel_clusters": len(novel_clusters), "pipeline_high_conv": len(high_conv),
                "pipeline_signal_dicts": 0, "pipeline_signals_persisted": 0,
                "pipeline_noise_filtered": 0, "total_signals_generated": self._brain.signals_generated,
                "timing_total": round(time.monotonic() - t0, 2), "exit_reason": "llm_no_signals",
            }))
            return []

        # Stage 5: Risk-adjusted scoring & persistence
        t4 = time.monotonic()
        signals = await self._stage5_risk_adjust(signal_dicts, posts, high_conv)
        stage5_time = time.monotonic() - t4

        total_time = time.monotonic() - t0
        logger.info(
            "[pipeline] complete: %d posts → %d clusters → %d novel → "
            "%d high-conv → %d signal_dicts → %d signals | "
            "timing: S1=%.1fs S2=%.1fs S3=%.1fs S4=%.1fs S5=%.1fs total=%.1fs",
            len(posts), len(clusters), len(novel_clusters),
            len(high_conv), len(signal_dicts), len(signals),
            stage1_time, stage2_time, stage3_time, stage4_time, stage5_time, total_time,
        )

        # ── Structured cycle summary ─────────────────────────────────
        cycle_summary = {
            "event": "brain_cycle_summary",
            "ts": datetime.now(timezone.utc).isoformat(),
            "pipeline_posts": len(posts),
            "pipeline_clusters": len(clusters),
            "pipeline_novel_clusters": len(novel_clusters),
            "pipeline_high_conv": len(high_conv),
            "pipeline_signal_dicts": len(signal_dicts),
            "pipeline_signals_persisted": len(signals),
            "pipeline_noise_filtered": self._brain.signals_noise_filtered,
            "total_signals_generated": self._brain.signals_generated,
            "timing_s1": round(stage1_time, 2),
            "timing_s4_llm": round(stage4_time, 2),
            "timing_total": round(total_time, 2),
        }
        logger.info(json.dumps(cycle_summary))

        # Run quality audit (every N cycles)
        if self._audit:
            try:
                await self._audit.maybe_run()
            except Exception:
                logger.warning("[pipeline] audit failed", exc_info=True)

        return signals

    # ══════════════════════════════════════════════════════════════════
    # DATA FETCHING
    # ══════════════════════════════════════════════════════════════════

    async def _fetch_recent_posts(self, window_minutes: int) -> list[dict[str, Any]]:
        """Fetch recent FilteredPosts with content and sentiments."""
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=window_minutes)
        posts_data: list[dict] = []

        async with get_session() as session:
            stmt = (
                select(FilteredPost)
                .where(FilteredPost.filtered_at >= cutoff)
                .order_by(FilteredPost.filtered_at.desc())
                .limit(100)
            )
            result = await session.execute(stmt)
            filtered_posts = result.scalars().all()

            for fp in filtered_posts:
                raw = await session.get(RawPost, fp.raw_post_id)
                content = raw.content if raw else "(no content)"

                score_stmt = select(SentimentScore).where(
                    SentimentScore.filtered_post_id == fp.id,
                )
                score_result = await session.execute(score_stmt)
                sentiments = score_result.scalars().all()

                posts_data.append({
                    "post_id": str(fp.id),
                    "raw_post_id": str(fp.raw_post_id),
                    "platform": raw.platform if raw else "unknown",
                    "author": raw.author if raw else "unknown",
                    "url": raw.url if raw else None,
                    "source_published_at": (
                        raw.source_published_at.isoformat()
                        if raw and raw.source_published_at else None
                    ),
                    "urgency": fp.urgency,
                    "category": fp.category,
                    "content": content,
                    "content_snippet": content[:500],
                    "sentiments": [
                        {
                            "entity": s.reasoning[:50] if s.reasoning else "",
                            "score": s.sentiment,
                            "confidence": s.confidence,
                        }
                        for s in sentiments
                    ],
                })

        return posts_data

    # ══════════════════════════════════════════════════════════════════
    # STAGE 1: DETERMINISTIC ENTITY EXTRACTION (no LLM)
    # ══════════════════════════════════════════════════════════════════

    def _stage1_extract_entities(
        self, posts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Extract tickers and event types from post content using regex + lookup.

        Enriches each post dict with:
        - _tickers: set of extracted ticker symbols
        - _event_types: set of classified event types
        - _extracted_entities: set of entity name strings
        """
        from alekfi.brain.correlation import ENTITY_TICKER_MAP

        for post in posts:
            content = post.get("content", "")
            content_lower = content.lower()

            # Extract cashtags ($AAPL)
            cashtags = set(_CASHTAG_RE.findall(content))

            # Extract standalone uppercase words that match known tickers
            uppercase_words = set(_TICKER_RE.findall(content))
            tickers_from_words = {
                w for w in uppercase_words
                if w in _KNOWN_TICKERS and w not in _TICKER_STOPWORDS
            }

            # Resolve company names from entity ticker map
            resolved_tickers: set[str] = set()
            entity_names: set[str] = set()
            _strict_match = os.environ.get("ENTITY_STRICT_MATCH", "1") == "1"
            for name, ticker in ENTITY_TICKER_MAP.items():
                if _strict_match and len(name) <= 4:
                    # Short names require word-boundary match to avoid substring false positives
                    # e.g., "ge" should not match "general", "em" should not match "them"
                    if re.search(r'\b' + re.escape(name) + r'\b', content_lower):
                        resolved_tickers.add(ticker)
                        entity_names.add(name.title())
                else:
                    if name in content_lower:
                        resolved_tickers.add(ticker)
                        entity_names.add(name.title())

            # Normalize all tickers: uppercase, strip $, remove whitespace
            all_tickers = set()
            for t in (cashtags | tickers_from_words | resolved_tickers):
                normalized = t.strip().upper().replace("$", "")
                if len(normalized) >= 1 and normalized.isalpha():
                    all_tickers.add(normalized)
                elif "-" in normalized:  # e.g., BTC-USD
                    all_tickers.add(normalized)

            # Classify event types
            event_types: set[str] = set()
            for keywords, event_type in _EVENT_PATTERNS:
                if any(kw in content_lower for kw in keywords):
                    event_types.add(event_type)

            if not event_types:
                event_types.add("sentiment")

            # Add ticker names to entity names (skip fragments)
            for t in all_tickers:
                entity_names.add(t)
            # Remove very short entity names that are likely fragments
            entity_names = {e for e in entity_names if len(e) >= 2}

            post["_tickers"] = all_tickers
            post["_event_types"] = event_types
            post["_extracted_entities"] = entity_names

        extracted_count = sum(len(p.get("_tickers", set())) for p in posts)
        logger.info(
            "[pipeline:S1] extracted %d tickers from %d posts (deterministic)",
            extracted_count, len(posts),
        )
        return posts

    # ══════════════════════════════════════════════════════════════════
    # STAGE 2: DEDUPLICATION & NOVELTY DETECTION
    # ══════════════════════════════════════════════════════════════════

    async def _stage2_deduplicate(
        self, posts: list[dict[str, Any]],
    ) -> list[PipelineCluster]:
        """Cluster posts by (ticker, event_type) and check Redis for recent signals."""
        # Build clusters
        cluster_map: dict[tuple[str, str], PipelineCluster] = {}

        for post in posts:
            tickers = post.get("_tickers", set())
            event_types = post.get("_event_types", {"sentiment"})

            if not tickers:
                # Posts with no tickers go into a general "MARKET" cluster
                for et in event_types:
                    key = ("MARKET", et)
                    if key not in cluster_map:
                        cluster_map[key] = PipelineCluster("MARKET", et)
                    cluster_map[key].add_post(post)
                continue

            for ticker in tickers:
                for et in event_types:
                    key = (ticker, et)
                    if key not in cluster_map:
                        cluster_map[key] = PipelineCluster(ticker, et)
                    cluster_map[key].add_post(post)

        clusters = list(cluster_map.values())

        # Check Redis for recent signals on each cluster (dedup)
        pipe = self._redis.pipeline()
        for cluster in clusters:
            dedup_key = f"{_PIPELINE_DEDUP_PREFIX}{cluster.primary_ticker}:{cluster.event_type}"
            pipe.get(dedup_key)

        try:
            results = await pipe.execute()
            for cluster, existing in zip(clusters, results):
                if existing:
                    cluster.is_novel = False
        except Exception:
            logger.warning("[pipeline:S2] Redis dedup check failed, treating all as novel")

        novel = sum(1 for c in clusters if c.is_novel)
        dupes = len(clusters) - novel
        logger.info(
            "[pipeline:S2] %d clusters (%d novel, %d duplicate)",
            len(clusters), novel, dupes,
        )
        return clusters

    # ══════════════════════════════════════════════════════════════════
    # STAGE 3: SOURCE CONVERGENCE SCORING
    # ══════════════════════════════════════════════════════════════════

    def _stage3_score_convergence(
        self, clusters: list[PipelineCluster],
    ) -> list[PipelineCluster]:
        """Score each cluster for multi-source corroboration.

        Convergence score factors:
        - Number of distinct platforms (weighted by exclusivity)
        - Number of posts
        - Diversity of authors
        """
        for cluster in clusters:
            if cluster.post_count == 0:
                cluster.convergence_score = 0.0
                continue

            # Platform diversity with exclusivity weighting
            platform_weights: list[float] = []
            for platform in cluster.platforms:
                weight = self._source_exclusivity.get(platform, 0.5)
                platform_weights.append(weight)

            # Base score: weighted platform count normalized
            # 1 platform = 0.1-0.3, 2 platforms = 0.3-0.5, 3+ = 0.5-1.0
            if not platform_weights:
                cluster.convergence_score = 0.0
                continue

            avg_weight = sum(platform_weights) / len(platform_weights)
            platform_factor = min(1.0, len(platform_weights) / 4.0)

            # Post volume factor (more posts = more conviction, diminishing returns)
            volume_factor = min(1.0, cluster.post_count / 8.0)

            # Author diversity
            authors = {p.get("author", "unknown") for p in cluster.posts}
            author_factor = min(1.0, len(authors) / 4.0)

            # Has both high and low exclusivity sources = cross-platform confirmation
            has_high = any(w >= 0.65 for w in platform_weights)
            has_low = any(w <= 0.35 for w in platform_weights)
            cross_platform_bonus = 0.15 if (has_high and has_low) else 0.0

            convergence = (
                platform_factor * 0.40
                + volume_factor * 0.25
                + author_factor * 0.15
                + avg_weight * 0.20
                + cross_platform_bonus
            )

            cluster.convergence_score = round(min(1.0, convergence), 3)

        # Sort by convergence score descending
        clusters.sort(key=lambda c: c.convergence_score, reverse=True)

        if clusters:
            logger.info(
                "[pipeline:S3] convergence scores: %s",
                ", ".join(
                    f"{c.primary_ticker}/{c.event_type}={c.convergence_score:.2f}"
                    for c in clusters[:10]
                ),
            )

        return clusters

    async def _select_high_convergence_clusters(
        self,
        scored_clusters: list[PipelineCluster],
    ) -> list[PipelineCluster]:
        """Apply hysteresis + candidate persistence around convergence thresholds."""
        t_high = float(os.environ.get("PIPELINE_CONVERGENCE_T_HIGH", os.environ.get("PIPELINE_CONVERGENCE_THRESHOLD", "0.30")))
        t_low = float(os.environ.get("PIPELINE_CONVERGENCE_T_LOW", "0.25"))
        if t_low >= t_high:
            t_low = max(0.0, t_high - 0.05)
        candidate_min_minutes = int(os.environ.get("PIPELINE_CANDIDATE_MINUTES", "20"))
        now = datetime.now(timezone.utc)

        # Fallback to simple threshold if Redis is unavailable.
        if self._redis is None:
            return [c for c in scored_clusters if c.convergence_score >= t_high]

        selected: list[PipelineCluster] = []
        for c in scored_clusters:
            cluster_id = f"{(c.primary_ticker or 'UNK').upper()}:{(c.event_type or 'unknown').lower()}"
            state_key = f"alekfi:pipeline:cluster_state:{cluster_id}"
            state = await self._redis.hgetall(state_key)
            prev_state = (state.get("state") or "").upper()
            prev_first_seen = state.get("first_seen")
            first_seen = prev_first_seen or now.isoformat()

            try:
                first_seen_dt = datetime.fromisoformat(first_seen.replace("Z", "+00:00"))
            except Exception:
                first_seen_dt = now
                first_seen = now.isoformat()

            dwell_minutes = max(0.0, (now - first_seen_dt).total_seconds() / 60.0)
            score = float(c.convergence_score or 0.0)

            # Hysteresis: promote on T_high, hold active while >= T_low.
            if score >= t_high or (prev_state == "ACTIVE" and score >= t_low):
                selected.append(c)

                if prev_state == "CANDIDATE":
                    await self._redis.hincrby("alekfi:pipeline:metrics", "candidate_promotions", 1)
                    await self._redis.hincrbyfloat("alekfi:pipeline:metrics", "candidate_minutes_total", dwell_minutes)
                elif prev_state != "ACTIVE":
                    await self._redis.hincrby("alekfi:pipeline:metrics", "direct_promotions", 1)

                await self._redis.hset(
                    state_key,
                    mapping={
                        "state": "ACTIVE",
                        "score": round(score, 3),
                        "first_seen": first_seen,
                        "last_seen": now.isoformat(),
                        "dwell_minutes": round(dwell_minutes, 2),
                    },
                )
                await self._redis.expire(state_key, 7200)
                continue

            if t_low <= score < t_high:
                # Persist near-threshold candidates and queue targeted corroboration.
                await self._redis.hset(
                    state_key,
                    mapping={
                        "state": "CANDIDATE",
                        "score": round(score, 3),
                        "first_seen": first_seen,
                        "last_seen": now.isoformat(),
                        "dwell_minutes": round(dwell_minutes, 2),
                    },
                )
                await self._redis.expire(state_key, 7200)
                await self._redis.hincrby("alekfi:pipeline:metrics", "candidate_seen", 1)
                if dwell_minutes >= candidate_min_minutes:
                    await self._redis.hincrby("alekfi:pipeline:metrics", "clusters_stalled_count", 1)

                try:
                    payload = {
                        "cluster_id": cluster_id,
                        "ticker": c.primary_ticker,
                        "event_type": c.event_type,
                        "score": round(score, 3),
                        "dwell_minutes": round(dwell_minutes, 2),
                        "platforms": list(c.platforms),
                        "post_count": c.post_count,
                        "triggered_at": now.isoformat(),
                    }
                    await self._redis.lpush("alekfi:corroboration:queue", json.dumps(payload, default=str))
                    await self._redis.ltrim("alekfi:corroboration:queue", 0, 499)
                except Exception:
                    logger.debug("[pipeline:S3] failed to enqueue corroboration candidate %s", cluster_id, exc_info=True)
                continue

            # Below low threshold: clear cluster state.
            if prev_state:
                await self._redis.delete(state_key)

        # Export snapshot metrics for monitoring.
        await self._redis.hset(
            "alekfi:pipeline:metrics",
            mapping={
                "last_selected_count": len(selected),
                "last_scored_count": len(scored_clusters),
                "t_low": t_low,
                "t_high": t_high,
                "updated_at": now.isoformat(),
            },
        )
        return selected

    # ══════════════════════════════════════════════════════════════════
    # STAGE 4: LLM SYNTHESIS (only for high-convergence clusters)
    # ══════════════════════════════════════════════════════════════════

    async def _stage4_synthesize(
        self,
        clusters: list[PipelineCluster],
        all_posts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Run LLM synthesis only for clusters that passed convergence threshold.

        Builds rich context injection and calls LLM with SIGNAL_PROMPT.
        """
        from alekfi.brain.brain import SIGNAL_PROMPT, INTELLIGENCE_FRAMEWORK

        # Build cluster summary for the LLM
        cluster_summaries = []
        source_posts_for_prompt = []
        for cluster in clusters:
            cluster_summaries.append({
                "ticker": cluster.primary_ticker,
                "event_type": cluster.event_type,
                "convergence_score": cluster.convergence_score,
                "platforms": list(cluster.platforms),
                "post_count": cluster.post_count,
                "entity_names": list(cluster.entity_names)[:10],
            })
            # Top-K posts per cluster (sorted by urgency/weight) to limit prompt size
            _cluster_posts = cluster.posts[:_SIGNAL_GEN_TOP_K]
            for post in _cluster_posts:
                sp = {
                    "id": post["post_id"],
                    "p": post["platform"],
                    "a": post["author"],
                    "s": post.get("content_snippet", "")[:200],
                }
                # Only include non-empty optional fields
                if post.get("url"):
                    sp["u"] = post["url"]
                if post.get("urgency"):
                    sp["ug"] = post["urgency"]
                if post.get("raw_post_id"):
                    sp["rid"] = post["raw_post_id"]
                # Avoid duplicates in source posts
                if not any(
                    existing["id"] == sp["id"]
                    for existing in source_posts_for_prompt
                ):
                    source_posts_for_prompt.append(sp)

        # Gather context injections (reuse brain's methods)
        accuracy_ctx = await self._brain._get_accuracy_context()
        learning_ctx = await self._brain._get_learning_context()
        playbook_ctx = await self._brain._get_playbook_context()
        calibration_ctx = await self._brain._get_calibration_context()
        velocity_data = await self._brain._compute_entity_velocity(window_minutes=30)
        convergence_alerts = await self._brain._detect_convergence(window_minutes=60)

        # Velocity context
        velocity_ctx = ""
        if velocity_data:
            velocity_ctx = "\n\n## Entity Velocity Spikes (last 30 min)\n"
            for name, v in list(velocity_data.items())[:5]:  # Top 5 only
                velocity_ctx += (
                    f"- {name} ({v.get('ticker', '?')}): {v['velocity_ratio']}x acceleration, "
                    f"{v['recent_mentions']} mentions on {', '.join(v['platforms'])}. "
                )
                if v.get("cross_platform"):
                    velocity_ctx += "CROSS-PLATFORM SPIKE. "
                velocity_ctx += "\n"

        # Convergence context
        convergence_ctx = ""
        if convergence_alerts:
            convergence_ctx = "\n\n## CONVERGENCE ALERTS (auto-detected)\n"
            for c in convergence_alerts[:5]:  # Top 5 only
                convergence_ctx += (
                    f"- {c['entity']} ({c.get('ticker', '?')}): {c['platform_count']} platforms "
                    f"({', '.join(c['platforms'])}) — {c['mention_count']} mentions.\n"
                )

        # Regime context
        regime_ctx = ""
        try:
            regime_raw = await self._redis.get("alekfi:market_regime")
            if regime_raw:
                regime = json.loads(regime_raw)
                regime_ctx = (
                    f"\n\n## Market Regime\n"
                    f"- Regime: {regime.get('regime', 'UNKNOWN')}\n"
                    f"- VIX: {regime.get('vix', '?')}\n"
                    f"- SPY 5d: {regime.get('spy_change_5d', '?')}%\n"
                )
        except Exception:
            pass

        # Build the prompt
        _compact_sep = (",", ":")
        _clusters_json = json.dumps(cluster_summaries, separators=_compact_sep, default=str)
        _posts_json = json.dumps(source_posts_for_prompt[:30], separators=_compact_sep, default=str)

        pipeline_context = (
            f"## Pipeline Intelligence (5-stage filtered)\n"
            f"The following {len(clusters)} clusters passed convergence filtering "
            f"(score > PIPELINE_CONVERGENCE_THRESHOLD). Each represents corroborated intelligence from multiple sources.\n\n"
            f"Clusters:\n{_clusters_json}\n"
        )

        user_prompt = (
            f"{pipeline_context}\n\n"
            f"Available source posts for citation:\n"
            f"{_posts_json}"
        )

        if accuracy_ctx:
            user_prompt += f"\n\n{accuracy_ctx}"
        if learning_ctx:
            user_prompt += learning_ctx
        if playbook_ctx:
            user_prompt += playbook_ctx
        if calibration_ctx:
            user_prompt += calibration_ctx
        if velocity_ctx:
            user_prompt += velocity_ctx
        if convergence_ctx:
            user_prompt += convergence_ctx
        if regime_ctx:
            user_prompt += regime_ctx

        # Entity and narrative context
        try:
            if self._brain._entity_memory:
                tickers_for_ctx = [c.primary_ticker for c in clusters if c.primary_ticker != "MARKET"][:10]
                if tickers_for_ctx:
                    entity_contexts = await self._brain._entity_memory.get_entity_contexts_batch(tickers_for_ctx)
                    if entity_contexts:
                        user_prompt += "\n\n## Entity Memory\n"
                        for tk, ectx in entity_contexts.items():
                            state = ectx.get("state")
                            if state:
                                user_prompt += (
                                    f"- {tk}: sentiment_24h={state.get('sentiment_24h', 0):.2f}, "
                                    f"momentum={state.get('momentum', 0):.2f}, "
                                    f"mentions_24h={state.get('mention_count_24h', 0)}\n"
                                )
        except Exception:
            pass

        try:
            if self._brain._narrative_tracker:
                tickers_for_narr = [c.primary_ticker for c in clusters if c.primary_ticker != "MARKET"][:10]
                if tickers_for_narr:
                    narr_ctx = await self._brain._narrative_tracker.get_narrative_context_for_signal(
                        tickers_for_narr, [],
                    )
                    if narr_ctx:
                        user_prompt += f"\n\n{narr_ctx}"
        except Exception:
            pass

        # Call LLM
        try:
            # ── Direction balance warning (Phase 3) ──
            if _DIRECTION_BALANCE_ENABLED:
                try:
                    async with get_session() as dir_session:
                        dir_result = await dir_session.execute(
                            text(
                                "SELECT direction, COUNT(*) as cnt "
                                "FROM signals WHERE created_at >= NOW() - INTERVAL '12 hours' "
                                "GROUP BY direction"
                            )
                        )
                        dir_rows = dir_result.fetchall()
                    total_dir = sum(r.cnt for r in dir_rows)
                    if total_dir >= 5:
                        long_pct = next((r.cnt for r in dir_rows if r.direction == "LONG"), 0) / total_dir
                        if long_pct > _DIRECTION_LONG_CAP:
                            user_prompt += (
                                f"\n\n*** DIRECTIONAL BALANCE WARNING: {long_pct:.0%} of recent signals are LONG. "
                                "Actively consider SHORT opportunities for bearish catalysts. "
                                "Generate at least one SHORT or NO_TRADE signal if evidence supports it. ***"
                            )
                            logger.info("[pipeline:S4] direction balance warning: %.0f%% LONG", long_pct * 100)
                except Exception:
                    pass

            raw_response = await self._llm.complete(SIGNAL_PROMPT, user_prompt, json_mode=True, route="pipeline_signal_gen")
            signal_dicts = self._brain._parse_json_array(raw_response)
        except Exception:
            logger.warning("[pipeline:S4] LLM synthesis failed", exc_info=True)
            return []

        # Adversarial review
        if signal_dicts:
            signal_dicts = await self._brain._adversarial_review(signal_dicts)

        logger.info(
            "[pipeline:S4] LLM produced %d signals from %d clusters",
            len(signal_dicts), len(clusters),
        )

        # Tag each signal dict with cluster info for downstream
        for sd in signal_dicts:
            # Try to match signal to a cluster
            instruments = sd.get("affected_instruments", [])
            primary_sym = ""
            for inst in instruments:
                if inst.get("symbol"):
                    primary_sym = inst["symbol"]
                    break
            sd["_pipeline_source_posts"] = source_posts_for_prompt
            sd["_pipeline_clusters"] = [c.to_dict() for c in clusters]
            sd["_pipeline_primary_sym"] = primary_sym

        return signal_dicts

    # ══════════════════════════════════════════════════════════════════
    # STAGE 5: RISK-ADJUSTED SCORING & PERSISTENCE
    # ══════════════════════════════════════════════════════════════════

    async def _store_dropped(self, reason: str, sd: dict, scores: dict | None = None, fingerprint: str = "") -> None:
        """Store a dropped signal candidate for observability (Phase B)."""
        if not _SHADOW_PERSISTENCE_ENABLED:
            return
        try:
            primary_sym = sd.get("_pipeline_primary_sym", "")
            payload = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "reason": reason,
                "ticker": primary_sym,
                "direction": sd.get("direction", "LONG"),
                "thesis_short": (sd.get("thesis", "") or "")[:200],
                "signal_type": sd.get("signal_type", ""),
                "conviction": float(sd.get("conviction", 0)),
                "scores": scores or {},
                "fingerprint": fingerprint,
            }
            payload_json = json.dumps(payload, default=str)
            # Redis list (cap 500)
            if self._redis:
                await self._redis.lpush("alekfi:signals:dropped", payload_json)
                await self._redis.ltrim("alekfi:signals:dropped", 0, 499)
            # Postgres shadow table
            try:
                async with get_session() as shadow_session:
                    await shadow_session.execute(
                        text(
                            "INSERT INTO signals_shadow (id, created_at, reason, payload) "
                            "VALUES (gen_random_uuid(), NOW(), :reason, CAST(:payload AS jsonb))"
                        ),
                        {"reason": reason, "payload": payload_json},
                    )
            except Exception:
                pass  # Table may not exist yet — non-fatal
        except Exception as e:
            logger.debug("[pipeline:S5] shadow store failed: %s", e)

    async def _stage5_risk_adjust(
        self,
        signal_dicts: list[dict[str, Any]],
        all_posts: list[dict[str, Any]],
        clusters: list[PipelineCluster],
    ) -> list[Signal]:
        from alekfi.brain.brain import SOURCE_EXCLUSIVITY, MEGA_CAPS

        adaptive_thresholds = await self._brain._get_adaptive_thresholds()
        noise_threshold = adaptive_thresholds.get("noise_score_threshold", 35)

        signal_type_accuracy = await self._get_signal_type_accuracy()

        source_posts = signal_dicts[0].get("_pipeline_source_posts", all_posts) if signal_dicts else all_posts
        velocity_data = await self._brain._compute_entity_velocity(window_minutes=30)
        convergence_alerts = await self._brain._detect_convergence(window_minutes=60)

        signals: list[Signal] = []
        noise_filtered = 0
        _drop_counts: dict[str, int] = {
            "no_trade": 0, "low_conviction": 0, "low_convergence": 0,
            "same_type_dedup": 0, "cross_type_dedup": 0,
            "noise_threshold": 0, "quality_gate": 0, "conviction_engine": 0,
        }
        _drop_examples: list[dict] = []
        _best_dropped = None  # Phase D: best dropped candidate for guardrail
        _best_dropped_score = -1

        # ── Pre-fetch existing signals for dedup (read-only session) ──
        existing_signal_map: dict[tuple[str, str], Signal] = {}
        try:
            async with get_session() as dedup_session:
                dedup_cutoff = datetime.now(timezone.utc) - timedelta(hours=8)
                existing_stmt = (
                    select(Signal)
                    .where(Signal.created_at >= dedup_cutoff)
                )
                existing_result = await dedup_session.execute(existing_stmt)
                for es in existing_result.scalars().all():
                    es_syms = [i.get("symbol", "") for i in (es.affected_instruments or [])]
                    for sym in es_syms:
                        if sym:
                            existing_signal_map[(sym, es.signal_type)] = es
        except Exception:
            logger.warning("[pipeline:S5] dedup pre-fetch failed, continuing without dedup", exc_info=True)

        for sd in signal_dicts:
            try:
                conviction = float(sd.get("conviction", 0))
                sig_type = sd.get("signal_type", "sentiment_momentum")
                primary_sym = sd.get("_pipeline_primary_sym", "")

                if not primary_sym:
                    for inst in sd.get("affected_instruments", []):
                        if inst.get("symbol"):
                            primary_sym = inst["symbol"]
                            break

                # Filter NO_TRADE signals (log but don't persist)
                direction_raw = sd.get("direction", "LONG").upper()
                if direction_raw == "NO_TRADE":
                    logger.info("[pipeline:S5] NO_TRADE signal for %s: %s", primary_sym, sd.get("thesis", "")[:80])
                    noise_filtered += 1
                    _drop_counts["no_trade"] += 1
                    if len(_drop_examples) < 10:
                        _drop_examples.append({"reason": "no_trade", "ticker": primary_sym, "thesis": (sd.get("thesis", "") or "")[:100]})
                    await self._store_dropped("no_trade", sd)
                    continue

                # Min confidence filter
                if conviction < 0.4:
                    noise_filtered += 1
                    _drop_counts["low_conviction"] += 1
                    if len(_drop_examples) < 10:
                        _drop_examples.append({"reason": "low_conviction", "ticker": primary_sym, "conviction": conviction})
                    await self._store_dropped("low_conviction", sd, {"conviction": conviction})
                    continue

                # ── Source-quality score ceiling (non-negotiable) ─────
                NOISY_PLATFORMS = {"stocktwits", "4chan", "4chan_pol", "4chan_biz", "reddit_wsb", "telegram"}
                HIGH_TRUST_PLATFORMS = {"congressional_trades", "prediction_markets", "sec_edgar", "sec_filings"}

                # Find matching cluster for this signal
                _matched_cluster = None
                for _cl in sd.get("_pipeline_clusters", []):
                    if _cl.get("primary_ticker") == primary_sym:
                        if _matched_cluster is None or _cl.get("convergence_score", 0) > _matched_cluster.get("convergence_score", 0):
                            _matched_cluster = _cl
                if _matched_cluster is None and sd.get("_pipeline_clusters"):
                    _matched_cluster = max(sd["_pipeline_clusters"], key=lambda c: c.get("convergence_score", 0))

                _cluster_source_count = _matched_cluster.get("post_count", 1) if _matched_cluster else 1
                _cluster_platform_count = _matched_cluster.get("platform_count", 1) if _matched_cluster else 1
                _cluster_platforms = set(_matched_cluster.get("platforms", [])) if _matched_cluster else set()
                _cluster_convergence = _matched_cluster.get("convergence_score", 0.2) if _matched_cluster else 0.2

                # Minimum convergence threshold — drop garbage
                if _cluster_convergence < 0.2:
                    noise_filtered += 1
                    _drop_counts["low_convergence"] += 1
                    if len(_drop_examples) < 10:
                        _drop_examples.append({"reason": "low_convergence", "ticker": primary_sym, "convergence": round(_cluster_convergence, 3)})
                    await self._store_dropped("low_convergence", sd, {"convergence": _cluster_convergence})
                    logger.debug(
                        "[pipeline:S5] dropped %s %s: convergence %.2f < 0.2 threshold",
                        primary_sym, sig_type, _cluster_convergence,
                    )
                    continue

                # Hard ceiling based on number of independent sources
                if _cluster_platform_count == 1 and _cluster_source_count <= 2:
                    _max_score = 40  # Single platform, few posts → never exceed 40
                elif _cluster_platform_count == 2 or _cluster_source_count <= 4:
                    _max_score = 65  # Two platforms cap at 65
                else:
                    _max_score = 100  # 3+ independent platforms can hit 100

                # Further reduce ceiling for known-noisy platforms when they're the only source
                if _cluster_platform_count == 1 and _cluster_platforms & NOISY_PLATFORMS:
                    _max_score = min(_max_score, 25)

                # HIGH-TRUST single sources get a slightly higher ceiling
                if _cluster_platform_count == 1 and _cluster_platforms & HIGH_TRUST_PLATFORMS:
                    _max_score = max(_max_score, 55)

                # Signal dedup: merge with existing if same instrument + type in last 8h
                if primary_sym:
                    dedup_key_tuple = (primary_sym, sig_type)
                    duplicate = existing_signal_map.get(dedup_key_tuple)
                    if duplicate:
                        try:
                            async with get_session() as merge_session:
                                dup_stmt = select(Signal).where(Signal.id == duplicate.id)
                                dup_result = await merge_session.execute(dup_stmt)
                                dup_obj = dup_result.scalar_one_or_none()
                                if dup_obj:
                                    old_meta = dup_obj.metadata_ or {}
                                    new_score = sd.get("total_intelligence_score", 0)
                                    old_score = old_meta.get("total_intelligence_score", 0)
                                    merged_score = max(old_score, new_score)
                                    old_meta["total_intelligence_score"] = merged_score
                                    old_meta["priority_score"] = merged_score
                                    old_meta["priority_label"] = self._brain._intelligence_tier(merged_score)
                                    old_meta["dedup_merged"] = True
                                    old_meta["pipeline_version"] = "v2"
                                    dup_obj.metadata_ = old_meta
                                    dup_obj.conviction = max(dup_obj.conviction or 0, conviction)
                                    logger.info("[pipeline:S5] dedup merged %s %s (score=%d)", primary_sym, sig_type, merged_score)
                        except Exception:
                            logger.warning("[pipeline:S5] dedup merge failed for %s %s", primary_sym, sig_type, exc_info=True)
                        _drop_counts["same_type_dedup"] += 1
                        if len(_drop_examples) < 10:
                            _drop_examples.append({"reason": "same_type_dedup", "ticker": primary_sym, "type": sig_type})
                        await self._store_dropped("same_type_dedup", sd)
                        continue

                # ── Cross-type dedup (Phase 2): same ticker, any type, similar thesis ──
                if _CROSS_DEDUP_ENABLED and primary_sym:
                    thesis_text = sd.get("thesis", "")
                    fp = self._compute_signal_fingerprint(
                        sd.get("direction", "LONG"),
                        sd.get("affected_instruments", []),
                        thesis_text,
                        sd.get("time_horizon", ""),
                    )

                    # Check Redis fingerprint cache
                    fp_key = f"alekfi:signal:fp:{fp}"
                    try:
                        existing_fp = await self._redis.get(fp_key)
                        if existing_fp:
                            logger.info("[pipeline:S5] cross-dedup: fingerprint hit for %s %s (fp=%s)", primary_sym, sig_type, fp[:8])
                            noise_filtered += 1
                            _drop_counts["cross_type_dedup"] += 1
                            if len(_drop_examples) < 10:
                                _drop_examples.append({"reason": "cross_dedup_fp", "ticker": primary_sym, "fingerprint": fp[:8]})
                            await self._store_dropped("cross_type_dedup", sd, {"fingerprint": fp[:16], "match_type": "fingerprint"})
                            continue
                    except Exception:
                        pass

                    # Check DB for thesis similarity against recent signals (same ticker, any type, last 2h)
                    try:
                        async with get_session() as dedup_session:
                            recent = await dedup_session.execute(
                                text(
                                    "SELECT id, thesis, signal_type FROM signals "
                                    "WHERE affected_instruments->0->>'symbol' = :sym "
                                    "  AND created_at >= NOW() - INTERVAL '2 hours' "
                                    "ORDER BY created_at DESC LIMIT 10"
                                ),
                                {"sym": primary_sym},
                            )
                            recent_rows = recent.fetchall()

                        for rr in recent_rows:
                            sim = self._thesis_similarity(thesis_text, rr.thesis or "")
                            if sim >= _CROSS_DEDUP_SIM_THRESHOLD:
                                logger.info(
                                    "[pipeline:S5] cross-dedup: thesis sim=%.2f for %s (%s vs %s, fp=%s), skipping",
                                    sim, primary_sym, sig_type, rr.signal_type, fp[:8],
                                )
                                noise_filtered += 1
                                _drop_counts["cross_type_dedup"] += 1
                                if len(_drop_examples) < 10:
                                    _drop_examples.append({"reason": "cross_dedup_sim", "ticker": primary_sym, "similarity": round(sim, 2), "fingerprint": fp[:8]})
                                await self._store_dropped("cross_type_dedup", sd, {"similarity": round(sim, 2), "fingerprint": fp[:16], "match_type": "thesis_sim"})
                                # Store fingerprint to prevent future duplicates
                                try:
                                    await self._redis.setex(fp_key, _CROSS_DEDUP_TTL, "1")
                                except Exception:
                                    pass
                                break
                        else:
                            # No similar thesis found — store fingerprint for this signal
                            try:
                                await self._redis.setex(fp_key, _CROSS_DEDUP_TTL, "1")
                            except Exception:
                                pass
                            # Fall through to signal creation
                    except Exception:
                        logger.debug("[pipeline:S5] cross-dedup DB check failed", exc_info=True)

                # Intelligence scoring (no DB needed)
                total_score, intelligence_scores = self._brain._compute_intelligence_score(
                    sd, source_posts, {"themes": [], "alerts": []},
                )

                llm_score = sd.get("total_intelligence_score")
                if llm_score and isinstance(llm_score, (int, float)) and 5 <= llm_score <= 100:
                    total_score = max(total_score, int(llm_score))

                score_source = "pipeline_v2"

                # ── Direction-aware quality gate (v2) ─────────────────
                direction = sd.get("direction", "LONG").upper()
                dir_acc = signal_type_accuracy.get("%s:%s" % (sig_type, direction))
                type_acc = dir_acc if dir_acc is not None else signal_type_accuracy.get(sig_type)

                # HEDGE signals are trivially correct — cap to prevent metric inflation
                if direction == "HEDGE":
                    total_score = min(total_score, 35)
                    conviction = min(conviction, 0.50)
                    score_source += "+hedge_cap"

                if type_acc is not None:
                    if type_acc < 0.10:
                        # Near-zero accuracy — aggressive dampening
                        total_score = max(5, int(total_score * 0.60))
                        conviction = max(0.40, conviction - 0.25)
                        score_source += "+very_low_acc(%.0f%%)" % (type_acc * 100)
                    elif type_acc < 0.25:
                        total_score = max(5, int(total_score * 0.75))
                        conviction = max(0.40, conviction - 0.20)
                        score_source += "+low_acc(%.0f%%)" % (type_acc * 100)
                    elif type_acc < 0.40:
                        total_score = max(5, int(total_score * 0.85))
                        conviction = max(0.40, conviction - 0.15)
                        score_source += "+low_acc_penalty"
                    elif type_acc > 0.70:
                        total_score = min(100, int(total_score * 1.10))
                        score_source += "+high_acc_boost"

                # SHORT signals systematically underperform — extra dampening
                if direction == "SHORT" and type_acc is not None and type_acc < 0.15:
                    total_score = max(5, int(total_score * 0.70))
                    conviction = max(0.40, conviction - 0.10)
                    score_source += "+short_penalty"

                # Exclusivity boost
                exclusivity_info = self._brain._compute_exclusivity_edge(source_posts)
                edge_type = exclusivity_info.get("edge_type", "unknown")

                entity_names = [i.get("symbol", "") for i in sd.get("affected_instruments", []) if i.get("symbol")]
                pre_inst = await self._brain._check_pre_institutional(entity_names, source_posts)

                if pre_inst.get("is_pre_institutional"):
                    edge_type = "pre_institutional"
                    total_score = max(total_score, 80)
                    score_source += "+pre_institutional"
                elif edge_type == "cross_platform":
                    total_score = min(100, total_score + 25)
                    score_source += "+cross_platform"
                elif edge_type == "retail_only" and exclusivity_info["avg_exclusivity"] >= 0.75:
                    total_score = min(100, total_score + 20)
                    score_source += "+high_exclusivity"

                # Velocity boost
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

                # Convergence boost
                convergence_match = None
                for c in convergence_alerts:
                    if c.get("ticker") and c["ticker"] in entity_names:
                        convergence_match = c
                        break
                if convergence_match:
                    total_score = max(total_score, 85)
                    score_source += "+convergence"

                # Sector concentration penalty
                concentration_penalty = 0.0
                if self._brain._correlation_tracker:
                    try:
                        concentration_penalty = await self._brain._correlation_tracker.compute_concentration_penalty(
                            primary_sym, sd.get("direction", "LONG").upper(),
                        )
                        if concentration_penalty > 0:
                            total_score = max(5, int(total_score * (1 - concentration_penalty)))
                            score_source += f"+concentration({concentration_penalty:.1f})"
                    except Exception:
                        pass

                # Conviction Engine
                conviction_data = None
                if self._brain._conviction_engine:
                    try:
                        conviction_data = await self._brain._conviction_engine.score(
                            signal_type=sig_type,
                            direction=sd.get("direction", "LONG").upper(),
                            contributing_posts=source_posts[:10],
                            ticker=primary_sym,
                            llm_confidence=conviction,
                        )
                        if conviction_data and not conviction_data.get("should_emit", True):
                            logger.info("[pipeline:S5] suppressed by conviction engine: %s %s", primary_sym, sig_type)
                            noise_filtered += 1
                            _drop_counts["conviction_engine"] += 1
                            if len(_drop_examples) < 10:
                                _drop_examples.append({"reason": "conviction_engine", "ticker": primary_sym, "type": sig_type})
                            await self._store_dropped("conviction_engine", sd)
                            continue
                    except Exception:
                        pass

                total_score = max(5, min(100, total_score))

                # ── Apply hard source-quality ceiling ─────────────────
                _raw_score = total_score
                total_score = min(total_score, _max_score)
                if _raw_score > _max_score:
                    score_source += f"+capped({_max_score})"
                    logger.info(
                        "[pipeline:S5] SCORE CAPPED: %s %s raw=%d capped=%d sources=%d platforms=%s conv=%.2f",
                        primary_sym, sig_type, _raw_score, total_score,
                        _cluster_source_count, list(_cluster_platforms), _cluster_convergence,
                    )

                tier = self._brain._intelligence_tier(total_score)

                # Noise filter
                if total_score < noise_threshold:
                    noise_filtered += 1
                    _drop_counts["noise_threshold"] += 1
                    if len(_drop_examples) < 10:
                        _drop_examples.append({"reason": "noise_threshold", "ticker": primary_sym, "score": total_score, "threshold": noise_threshold})
                    await self._store_dropped("noise_threshold", sd, {"total_score": total_score, "threshold": noise_threshold})
                    # Phase D: track best dropped for guardrail
                    if total_score > _best_dropped_score:
                        _best_dropped_score = total_score
                        _best_dropped = {**sd, "_total_score": total_score, "_conviction": conviction,
                                         "_cluster_platform_count": _cluster_platform_count, "_cluster_convergence": _cluster_convergence}
                    continue

                # Mark cluster as emitted in Redis
                try:
                    dedup_key = f"{_PIPELINE_DEDUP_PREFIX}{primary_sym}:{sig_type}"
                    await self._redis.setex(dedup_key, _PIPELINE_DEDUP_TTL, "1")
                except Exception:
                    pass

                # Freshness & latency
                freshness_label = self._brain._compute_freshness(source_posts)
                source_event_time = None
                time_to_signal_seconds = None
                try:
                    earliest = None
                    for sp in source_posts:
                        pub = sp.get("source_published_at")
                        if pub:
                            dt = datetime.fromisoformat(pub)
                            if dt.tzinfo is None:
                                dt = dt.replace(tzinfo=timezone.utc)
                            if earliest is None or dt < earliest:
                                earliest = dt
                    if earliest:
                        source_event_time = earliest
                        time_to_signal_seconds = int(
                            (datetime.now(timezone.utc) - earliest).total_seconds()
                        )
                except Exception:
                    pass

                # Regime context
                regime_data = None
                try:
                    regime_raw = await self._redis.get("alekfi:market_regime")
                    if regime_raw:
                        regime_data = json.loads(regime_raw)
                except Exception:
                    pass

                # ── Persist signal in its OWN isolated session ──
                # ── Trade-ready bundle (claim/evidence/forecast/tradability) ──
                try:
                    _type_acc = signal_type_accuracy.get(sig_type) if isinstance(signal_type_accuracy, dict) else None
                except Exception:
                    _type_acc = None
                sd['_type_accuracy'] = _type_acc

                bundle = await build_research_bundle(
                    redis=self._redis,
                    signal_dict=sd,
                    source_posts=(sd.get('sources') or source_posts or []),
                    clusters=sd.get('_pipeline_clusters'),
                    tier=tier,
                    total_score=total_score,
                    sig_type=sig_type,
                    primary_sym=primary_sym,
                    direction=(sd.get('direction', 'LONG') or 'LONG').upper(),
                    conviction=conviction,
                    cluster_convergence=_cluster_convergence,
                    source_count=_cluster_source_count,
                    platform_count=_cluster_platform_count,
                    source_event_time=source_event_time,
                )

                # Hard controls for trade-ready ranking are recorded in bundle['controls'].
                # We still persist the signal; the /signals/forecasts endpoint filters on controls.pass.
                expires_hours = int(sd.get("expires_in_hours", 24))

                async with get_session() as persist_session:
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
                            "headline": sd.get("headline", ""),
                            "total_intelligence_score": total_score,
                            "intelligence_scores": intelligence_scores,
                            "intelligence_tier": tier,
                            "priority_score": total_score,
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
                            "exclusivity_detail": exclusivity_info,
                            "velocity_match": velocity_match,
                            "convergence_match": convergence_match,
                            "conviction_score": conviction_data["conviction_score"] if conviction_data else None,
                            "conviction_grade": conviction_data["conviction_grade"] if conviction_data else None,
                            "concentration_penalty": concentration_penalty if concentration_penalty > 0 else None,
                            "source_count": _cluster_source_count,
                            "platform_count": _cluster_platform_count,
                            "source_platforms": list(_cluster_platforms),
                            "cluster_convergence": _cluster_convergence,
                            "score_ceiling": _max_score,
                            "raw_score_before_cap": _raw_score,
                            "pipeline_version": "v2",
                            "pipeline_clusters": [c.to_dict() for c in clusters],
                            "regime": regime_data.get("regime") if regime_data else None,
                            "source_event_time": source_event_time.isoformat() if source_event_time else None,
                            "time_to_signal_seconds": time_to_signal_seconds,
                            "research": bundle,
                        },
                        expires_at=datetime.now(timezone.utc) + timedelta(hours=expires_hours),
                    )

                    # ── Compute real quality score BEFORE weight engine (Phase C) ──
                    _computed_quality = None
                    if _QUALITY_SCORE_ENABLED:
                        _qs_intel = min(total_score, 100)
                        _qs_conv = min(int(conviction * 100), 100)
                        _qs_plat = min(_cluster_platform_count * 25, 100)
                        _qs_convergence = min(int(_cluster_convergence * 100), 100)
                        _computed_quality = int(
                            0.40 * _qs_intel +
                            0.20 * _qs_conv +
                            0.20 * _qs_plat +
                            0.20 * _qs_convergence
                        )
                        _computed_quality = max(5, min(100, _computed_quality))
                        signal.quality_score = _computed_quality

                    # Signal Weight Engine (with real quality override)
                    if self._brain._signal_weight_engine:
                        try:
                            _platform = source_posts[0].get("platform", "unknown") if source_posts else "unknown"
                            _platform_map = {
                                "reddit": "SOCIAL_REDDIT", "twitter": "SOCIAL_TWITTER",
                                "stocktwits": "SOCIAL_STOCKTWITS", "tiktok": "SOCIAL_TIKTOK",
                                "news_rss": "NEWS_ARTICLE", "finviz_news": "NEWS_ARTICLE",
                            }
                            _weight_result = self._brain._signal_weight_engine.compute_weight_sync(
                                signal_metadata={
                                    "source_type": _platform_map.get(_platform, "NEWS_ARTICLE"),
                                    "ticker": primary_sym,
                                    "timestamp": datetime.now(timezone.utc).isoformat(),
                                },
                                sigma_score=0,
                                quality_override=_computed_quality,
                                content=sd.get("thesis", ""),
                            )
                            signal.weight_score = _weight_result["final_weight"]
                            signal.weight_tier = _weight_result["weight_tier"]
                            signal.credibility_score = _weight_result["component_scores"]["credibility"]
                            if _computed_quality is None:
                                signal.quality_score = _weight_result["component_scores"]["quality"]
                            signal.influence_score = _weight_result["component_scores"]["influence"]
                            signal.weight_data = _weight_result
                        except Exception:
                            pass

                    # ── Quality gate (Phase 5) ──
                    if _QUALITY_GATE_MIN > 0 and signal.quality_score is not None:
                        if signal.quality_score < _QUALITY_GATE_MIN:
                            logger.info(
                                "[pipeline:S5] quality gate: %s %s quality=%d < %d, filtering",
                                primary_sym, sig_type, signal.quality_score, _QUALITY_GATE_MIN,
                            )
                            noise_filtered += 1
                            _drop_counts["quality_gate"] += 1
                            if len(_drop_examples) < 10:
                                _drop_examples.append({"reason": "quality_gate", "ticker": primary_sym, "quality": signal.quality_score, "threshold": _QUALITY_GATE_MIN})
                            await self._store_dropped("quality_gate", sd, {"quality_score": signal.quality_score, "threshold": _QUALITY_GATE_MIN})
                            # Phase D: track best dropped for guardrail
                            if signal.quality_score > _best_dropped_score:
                                _best_dropped_score = signal.quality_score
                                _best_dropped = {**sd, "_total_score": total_score, "_quality_score": signal.quality_score,
                                                 "_conviction": conviction, "_cluster_platform_count": _cluster_platform_count,
                                                 "_cluster_convergence": _cluster_convergence}
                            continue

                    # ── Signal fingerprint (Phase 2) ──
                    try:
                        _fp = self._compute_signal_fingerprint(
                            sd.get("direction", "LONG"),
                            sd.get("affected_instruments", []),
                            sd.get("thesis", ""),
                            sd.get("time_horizon", ""),
                        )
                        signal.signal_fingerprint = _fp
                    except Exception:
                        pass

                    # ── Price at signal (Phase 1) ──
                    if os.environ.get("PRICE_ATTACH_ENABLED", "1") == "1" and primary_sym:
                        try:
                            _pas = await self._get_current_price(primary_sym)
                            if _pas:
                                signal.price_at_signal = _pas
                                logger.info("[pipeline:S5] price_at_signal=%.4f for %s", _pas, primary_sym)
                        except Exception:
                            logger.warning("[pipeline:S5] price attach failed for %s", primary_sym, exc_info=True)

                    persist_session.add(signal)
                    await persist_session.flush()  # ensure INSERT succeeds before anything else

                # Session committed — signal persisted

                # ── Signal source traceability ───────────────────────
                try:
                    trace_items = []
                    for sp in source_posts:
                        fp_id = sp.get("raw_post_id") or sp.get("post_id") or sp.get("filtered_post_id") or sp.get("id")
                        if fp_id:
                            trace_items.append(SignalSourceItem(
                                signal_id=signal.id,
                                raw_post_id=fp_id if isinstance(fp_id, uuid.UUID) else uuid.UUID(str(fp_id)),
                                contribution_weight=1.0 / max(len(source_posts), 1),
                                platform=sp.get("platform", "unknown"),
                            ))
                    if trace_items:
                        async with get_session() as trace_session:
                            trace_session.add_all(trace_items)
                        logger.info("[pipeline:S5] traced %d source posts for signal %s", len(trace_items), primary_sym)
                except Exception:
                    logger.warning("[pipeline:S5] traceability insert FAILED for %s", primary_sym, exc_info=True)

                # Wire signal_id to post_outcomes (separate session to avoid corruption)
                try:
                    contributing_ids = []
                    for sp in (sd.get("sources", []) or []):
                        if isinstance(sp, dict) and sp.get("post_id"):
                            contributing_ids.append(sp["post_id"])
                    if contributing_ids:
                        async with get_session() as outcomes_session:
                            await outcomes_session.execute(text("""
                                UPDATE post_outcomes
                                SET signal_id = :sig_id
                                WHERE post_id = ANY(:post_ids)
                                  AND signal_id IS NULL
                            """), {"sig_id": str(signal.id), "post_ids": contributing_ids})
                except Exception:
                    pass
                signals.append(signal)

                logger.info(
                    "[pipeline:S5] signal %s %s score=%d tier=%s conv=%.2f edge=%s",
                    primary_sym, sig_type, total_score, tier, conviction, edge_type,
                )

            except Exception:
                logger.warning(
                    "[pipeline:S5] failed to process signal for %s",
                    sd.get("_pipeline_primary_sym", "?"), exc_info=True,
                )
                continue

        # ── Phase D: Anti-zero persistence guardrail ──
        if _MIN_PERSIST_PER_CYCLE >= 1 and len(signals) == 0 and _best_dropped and len(signal_dicts) > 0:
            bd = _best_dropped
            bd_direction = bd.get("direction", "LONG").upper()
            bd_sym = bd.get("_pipeline_primary_sym", "")
            if bd_direction != "NO_TRADE":
                try:
                    async with get_session() as guard_session:
                        guard_signal = Signal(
                            id=uuid.uuid4(),
                            signal_type=bd.get("signal_type", "sentiment_momentum"),
                            affected_instruments=bd.get("affected_instruments", []),
                            direction=bd_direction,
                            conviction=float(bd.get("_conviction", 0)),
                            time_horizon=bd.get("time_horizon", "DAYS"),
                            thesis=bd.get("thesis", ""),
                            source_posts=bd.get("sources", bd.get("evidence")),
                            metadata_={
                                "forced_persist": 1,
                                "weight_tier": "shadow_low_priority",
                                "total_intelligence_score": bd.get("_total_score", 0),
                                "pipeline_version": "v2",
                                "guardrail_reason": "min_persist_per_cycle",
                            },
                            expires_at=datetime.now(timezone.utc) + timedelta(hours=6),
                        )
                        guard_signal.quality_score = bd.get("_quality_score", bd.get("_total_score", 0))
                        guard_session.add(guard_signal)
                        await guard_session.flush()
                    signals.append(guard_signal)
                    logger.info(
                        "[pipeline:S5] Phase D guardrail: force-persisted %s as shadow_low_priority (score=%d)",
                        bd_sym, bd.get("_total_score", 0),
                    )
                except Exception:
                    logger.warning("[pipeline:S5] Phase D guardrail persist failed", exc_info=True)

        # ── Phase A: Structured cycle outcome log ──
        _cycle_outcome = {
            "event": "pipeline_cycle_outcome",
            "candidates_total": len(signal_dicts),
            "persisted": len(signals),
            "persisted_ids": [str(s.id) for s in signals],
            "noise_filtered_total": noise_filtered,
            "drop_counts": _drop_counts,
            "top_drop_reasons": sorted(
                [(k, v) for k, v in _drop_counts.items() if v > 0],
                key=lambda x: -x[1],
            )[:3],
            "drop_examples": _drop_examples[:5],
        }
        logger.info("[pipeline:S5] %s", json.dumps(_cycle_outcome, default=str))

        self._brain.signals_generated += len(signals)
        self._brain.signals_noise_filtered += noise_filtered

        logger.info(
            "[pipeline:S5] persisted %d signals (%d noise-filtered)",
            len(signals), noise_filtered,
        )
        return signals

    # ── Helpers ───────────────────────────────────────────────────────

    async def _get_signal_type_accuracy(self) -> dict[str, float]:
        """Query historical accuracy by signal type + direction using signal_outcomes."""
        try:
            async with get_session() as session:
                # PRIMARY: signal_outcomes (objective price data)
                rows = (await session.execute(text("""
                    SELECT s.signal_type, s.direction,
                           COUNT(*) as total,
                           SUM(CASE WHEN o.correct_at_1h THEN 1 ELSE 0 END) as correct_1h
                    FROM signal_outcomes o
                    JOIN signals s ON o.signal_id = s.id
                    WHERE o.correct_at_1h IS NOT NULL
                    GROUP BY s.signal_type, s.direction
                    HAVING COUNT(*) >= 3
                """))).all()

            result = {}
            type_agg = {}  # aggregate per type
            for row in rows:
                # direction-specific: "source_convergence:LONG"
                dir_key = "%s:%s" % (row.signal_type, row.direction)
                result[dir_key] = (row.correct_1h or 0) / max(row.total, 1)
                # aggregate by type
                if row.signal_type not in type_agg:
                    type_agg[row.signal_type] = [0, 0]
                type_agg[row.signal_type][0] += row.correct_1h or 0
                type_agg[row.signal_type][1] += row.total
            for t, (c, n) in type_agg.items():
                result[t] = c / max(n, 1)

            if result:
                logger.info("[pipeline] accuracy cache: %s", {k: round(v, 3) for k, v in result.items()})
            return result
        except Exception:
            return {}
