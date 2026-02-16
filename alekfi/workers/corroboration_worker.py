"""Corroboration worker.

Enriches near-threshold signal evidence with independent corroboration so strict feed
eligibility improves without relaxing guardrails.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import select, text

from alekfi.config import get_settings
from alekfi.db.database import get_session
from alekfi.db.models import RawPost, Signal

logger = logging.getLogger(__name__)


class CorroborationWorker:
    """Enrichment pass for near-strict clusters and high-tier signals."""

    def __init__(self, interval_seconds: int = 180) -> None:
        self._interval = interval_seconds
        self._settings = get_settings()
        self._redis = aioredis.from_url(self._settings.redis_url, decode_responses=True)
        self._processed = 0
        self._enriched = 0
        self._errors = 0

    async def run(self) -> None:
        logger.info("[corroboration] worker started (interval=%ds)", self._interval)
        while True:
            try:
                await self._cycle()
            except Exception:
                self._errors += 1
                logger.warning("[corroboration] cycle failed", exc_info=True)
            await asyncio.sleep(self._interval)

    async def _cycle(self) -> None:
        # Priority 1: queued near-threshold candidates from convergence stage.
        queue_items = []
        for _ in range(50):
            raw = await self._redis.rpop("alekfi:corroboration:queue")
            if not raw:
                break
            try:
                queue_items.append(json.loads(raw))
            except Exception:
                continue

        # Priority 2: strict-near-miss signals from DB.
        signals = await self._fetch_near_strict_signals(limit=30)
        if not signals and not queue_items:
            return

        for sig in signals:
            self._processed += 1
            try:
                changed = await self._enrich_signal(sig)
                if changed:
                    self._enriched += 1
            except Exception:
                self._errors += 1
                logger.warning("[corroboration] signal enrichment failed: %s", sig.id, exc_info=True)

        if queue_items:
            await self._redis.hset(
                "alekfi:corroboration:stats",
                mapping={
                    "last_queue_items": len(queue_items),
                    "processed_total": self._processed,
                    "enriched_total": self._enriched,
                    "errors_total": self._errors,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
            )

    async def _fetch_near_strict_signals(self, limit: int = 30) -> list[Signal]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=2)
        out: list[Signal] = []
        async with get_session() as session:
            rows = (
                await session.execute(
                    select(Signal)
                    .where(Signal.created_at >= cutoff)
                    .order_by(Signal.created_at.desc())
                    .limit(250)
                )
            ).scalars().all()

        for s in rows:
            meta = s.metadata_ or {}
            research = (meta.get("research") or {}) if isinstance(meta, dict) else {}
            if not research:
                continue
            evidence = research.get("evidence") or {}
            tier = (meta.get("priority_label") or meta.get("intelligence_tier") or "").upper()
            ind = float(evidence.get("independence_score") or 0.0)
            uniq = int(evidence.get("unique_platforms") or 0)
            should = (
                0.35 <= ind < 0.45
                or uniq == 1
                or tier in {"CRITICAL", "HIGH"}
            )
            if should:
                out.append(s)
            if len(out) >= limit:
                break
        return out

    async def _enrich_signal(self, signal: Signal) -> bool:
        meta = signal.metadata_ or {}
        research = (meta.get("research") or {}) if isinstance(meta, dict) else {}
        if not research:
            return False

        ticker = ""
        for inst in signal.affected_instruments or []:
            if isinstance(inst, dict) and inst.get("symbol"):
                ticker = str(inst["symbol"]).upper()
                break
        if not ticker:
            return False

        existing_sources = self._source_posts_from_signal(signal, research)
        candidates = await self._find_candidate_posts(ticker, signal.thesis, existing_sources)
        if not candidates:
            return False

        merged_sources = existing_sources + candidates
        evidence = self._compute_evidence(merged_sources)
        trad = research.get("tradability") or {}
        controls = self._compute_controls(
            tier=(meta.get("priority_label") or meta.get("intelligence_tier") or "").upper(),
            evidence=evidence,
            tradability=trad,
            source_count=len(merged_sources),
            platform_count=int(evidence.get("unique_platforms") or 0),
            novelty=float(((research.get("forecast") or {}).get("novelty_score") or 0.6)),
        )

        research["evidence"] = evidence
        research["controls"] = controls
        research.setdefault("corroboration", {})
        research["corroboration"]["updated_at"] = datetime.now(timezone.utc).isoformat()
        research["corroboration"]["added_sources"] = len(candidates)
        research["corroboration"]["added_platforms"] = sorted({c.get("platform") for c in candidates if c.get("platform")})
        meta["research"] = research

        async with get_session() as session:
            await session.execute(
                text("UPDATE signals SET metadata = CAST(:meta AS jsonb) WHERE id = :id"),
                {"id": str(signal.id), "meta": json.dumps(meta, default=str)},
            )

        logger.info(
            "[corroboration] enriched signal %s %s (+%d sources, ind=%.3f)",
            signal.id,
            ticker,
            len(candidates),
            float(evidence.get("independence_score") or 0.0),
        )
        return True

    def _source_posts_from_signal(self, signal: Signal, research: dict[str, Any]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for src in (signal.source_posts or []):
            if isinstance(src, dict):
                out.append(
                    {
                        "id": src.get("id") or src.get("post_id"),
                        "platform": (src.get("platform") or src.get("p") or "unknown").lower(),
                        "author": src.get("author") or src.get("a") or "unknown",
                        "url": src.get("url") or src.get("u"),
                        "content_snippet": src.get("content_snippet") or src.get("s") or "",
                        "source_published_at": src.get("source_published_at") or src.get("published_at"),
                    }
                )

        # Include evidence nodes when source_posts are sparse.
        nodes = ((research.get("evidence") or {}).get("nodes") or [])
        for node in nodes:
            if not isinstance(node, dict):
                continue
            out.append(
                {
                    "id": node.get("id"),
                    "platform": node.get("platform"),
                    "author": node.get("author"),
                    "url": node.get("url"),
                    "content_snippet": node.get("snippet") or "",
                    "source_published_at": node.get("published_at"),
                }
            )
        return out

    async def _find_candidate_posts(
        self,
        ticker: str,
        thesis: str,
        existing_sources: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        existing_platforms = {(s.get("platform") or "").lower() for s in existing_sources}
        existing_authors = {(s.get("author") or "").lower() for s in existing_sources}
        tokens = [t for t in re.findall(r"[A-Za-z]{3,}", thesis or "") if t.upper() != ticker][:8]
        token_query = " | ".join(tokens) if tokens else ticker

        async with get_session() as session:
            rows = (
                await session.execute(
                    text(
                        """
                        SELECT id, platform, author, url, content, source_published_at
                        FROM raw_posts
                        WHERE scraped_at >= NOW() - INTERVAL '72 hours'
                          AND (
                            content ILIKE :ticker_q
                            OR to_tsvector('english', content) @@ plainto_tsquery('english', :token_q)
                          )
                        ORDER BY source_published_at DESC NULLS LAST, scraped_at DESC
                        LIMIT 200
                        """
                    ),
                    {"ticker_q": f"%{ticker}%", "token_q": token_query},
                )
            ).all()

        scored = []
        thesis_terms = {t.lower() for t in re.findall(r"[A-Za-z]{3,}", thesis or "")}
        for r in rows:
            platform = (r.platform or "unknown").lower()
            author = (r.author or "unknown").strip()
            if platform in existing_platforms and author.lower() in existing_authors:
                continue
            content = (r.content or "")[:400]
            terms = {t.lower() for t in re.findall(r"[A-Za-z]{3,}", content)}
            overlap = len(thesis_terms & terms) / max(len(thesis_terms | terms), 1)
            # Reward cross-platform evidence.
            score = overlap + (0.20 if platform not in existing_platforms else 0.0)
            scored.append(
                (
                    score,
                    {
                        "id": str(r.id),
                        "platform": platform,
                        "author": author,
                        "url": r.url,
                        "content_snippet": content,
                        "source_published_at": r.source_published_at.isoformat() if r.source_published_at else None,
                    },
                )
            )

        scored.sort(key=lambda x: x[0], reverse=True)
        picked: list[dict[str, Any]] = []
        seen_platforms = set(existing_platforms)
        for _, cand in scored:
            plat = cand.get("platform")
            # Prefer platform diversification.
            if plat in seen_platforms and len(picked) >= 2:
                continue
            seen_platforms.add(plat)
            picked.append(cand)
            if len(picked) >= int(os.environ.get("CORROBORATION_MAX_SOURCES", "5")):
                break
        return picked

    def _compute_evidence(self, source_posts: list[dict[str, Any]]) -> dict[str, Any]:
        # Keep this aligned with research_bundle scoring.
        platforms = {(s.get("platform") or "").lower() for s in source_posts if s.get("platform")}
        domains = set()
        authors = {(s.get("author") or "").lower() for s in source_posts if s.get("author")}
        for s in source_posts:
            u = (s.get("url") or "").lower()
            if "//" in u:
                try:
                    dom = u.split("//", 1)[1].split("/", 1)[0].replace("www.", "")
                    if dom:
                        domains.add(dom)
                except Exception:
                    pass

        uniq_platforms = len(platforms)
        uniq_domains = len(domains)
        uniq_authors = len(authors)
        n = max(len(source_posts), 1)
        echo_ratio = 1.0 - min(1.0, (uniq_domains + uniq_authors) / (2.0 * n))
        origin_div = min(1.0, uniq_domains / max(n, 1) * 2.0)
        modality_bonus = 0.12 * min(max(len({p.split("_", 1)[0] for p in platforms}) - 1, 0), 3)
        echo_penalty = 0.45 * echo_ratio
        independence = max(0.0, min(1.0, 0.30 * min(1.0, uniq_platforms / 3.0) + 0.24 * origin_div + 0.18 * min(1.0, uniq_authors / n * 2.0) + modality_bonus - echo_penalty))

        verification_hits = []
        for s in source_posts:
            u = (s.get("url") or "").lower()
            p = (s.get("platform") or "").lower()
            sid = s.get("id")
            if "sec.gov" in u or p in {"sec_edgar", "sec_filings"}:
                verification_hits.append({"type": "regulatory_filing", "evidence_id": sid, "strength": "high"})
            elif "status" in u or "downdetector" in u:
                verification_hits.append({"type": "outage_signal", "evidence_id": sid, "strength": "medium"})
            elif p in {"news_rss", "finviz_news"}:
                verification_hits.append({"type": "independent_news", "evidence_id": sid, "strength": "medium"})

        source_credibility = min(1.0, 0.25 + 0.22 * min(uniq_domains, 4) / 4.0 + 0.20 * min(uniq_platforms, 3) / 3.0 + (0.2 if any(v["strength"] == "high" for v in verification_hits) else 0.0))

        return {
            "independence_score": round(independence, 3),
            "unique_platforms": uniq_platforms,
            "unique_domains": uniq_domains,
            "unique_authors": uniq_authors,
            "echo_ratio": round(echo_ratio, 3),
            "source_credibility": round(source_credibility, 3),
            "verification_hits": verification_hits[:8],
            "independence_breakdown": {
                "echo_penalty": round(echo_penalty, 3),
                "origin_diversity": round(origin_div, 3),
                "modality_bonus": round(modality_bonus, 3),
                "time_separation_bonus": 0.0,
                "same_origin_ratio": round(echo_ratio, 3),
            },
            "nodes": source_posts[:25],
        }

    def _compute_controls(
        self,
        *,
        tier: str,
        evidence: dict[str, Any],
        tradability: dict[str, Any],
        source_count: int,
        platform_count: int,
        novelty: float,
    ) -> dict[str, Any]:
        ind = float(evidence.get("independence_score") or 0.0)
        verification_hits = evidence.get("verification_hits") or []
        src_cred = float(evidence.get("source_credibility") or 0.0)
        verified_single_ok = len(verification_hits) >= 1 and src_cred >= 0.70 and novelty >= 0.60

        hard_fail = []
        if tier in {"CRITICAL", "HIGH"}:
            if ind < 0.45 and not verified_single_ok:
                hard_fail.append("low_independence")
            if not bool(tradability.get("pass")):
                hard_fail.append("untradable")
            if source_count < 2:
                hard_fail.append("too_few_sources")
            if platform_count < 2 and not verified_single_ok:
                hard_fail.append("not_cross_platform")

        return {
            "hard_fail": hard_fail,
            "pass": len(hard_fail) == 0,
            "min_independence_for_top": 0.45,
            "verified_single_modality_ok": verified_single_ok,
        }

