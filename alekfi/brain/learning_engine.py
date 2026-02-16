"""LearningEngine -- self-evolving intelligence calibration system.

Periodically analyzes signal outcomes, computes adaptive thresholds,
detects accuracy drift, and generates calibration instructions that feed
back into the LLM signal-generation prompts.

Run via:
    engine = LearningEngine(config)
    await engine.run_loop(interval=21600)   # every 6 hours
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import select, func, case, text

from alekfi.config import Settings
from alekfi.db.database import get_session
from alekfi.db.models import FilteredPost, Signal, SignalFeedback

# SignalOutcome may not exist yet -- import defensively
try:
    from alekfi.db.models import SignalOutcome  # type: ignore[attr-defined]
except ImportError:
    SignalOutcome = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)

# ── Score bucket boundaries ──────────────────────────────────────────────
SCORE_BUCKETS = [
    ("CRITICAL", 80, 101),
    ("HIGH", 65, 80),
    ("MODERATE", 50, 65),
    ("LOW", 35, 50),
    ("NOISE", 0, 35),
]

# Redis keys
REDIS_THRESHOLDS = "alekfi:adaptive_thresholds"
REDIS_PLAYBOOK = "alekfi:playbook"
REDIS_DRIFT_ALERT = "alekfi:drift_alert"
REDIS_GATEKEEPER_INSTRUCTION = "alekfi:gatekeeper_instruction"
REDIS_LEARNING_REPORT = "alekfi:learning_report"

# TTLs (seconds)
REPORT_TTL = 86400  # 24 hours
DRIFT_TTL = 86400
THRESHOLDS_TTL = 86400 * 7  # 7 days -- refreshed every cycle anyway
PLAYBOOK_TTL = 86400 * 7
GATEKEEPER_TTL = 86400


class LearningEngine:
    """Self-evolving intelligence system that calibrates AlekFi's signal pipeline."""

    def __init__(self, config: Settings) -> None:
        self._config = config
        self._redis: aioredis.Redis = aioredis.from_url(
            config.redis_url, decode_responses=True
        )

    # ── Public entry points ──────────────────────────────────────────────

    async def run_loop(self, interval: int = 21600) -> None:
        """Run the learning cycle every *interval* seconds (default 6 h)."""
        logger.info(
            "[learning] starting learning loop (interval=%ds / %.1fh)",
            interval,
            interval / 3600,
        )
        while True:
            try:
                report = await self.generate_learning_report()
                logger.info(
                    "[learning] report generated (%d chars)", len(report)
                )
            except Exception:
                logger.exception("[learning] report generation failed")
            await asyncio.sleep(interval)

    # ── Core report generation ───────────────────────────────────────────

    async def generate_learning_report(self) -> str:
        """Analyse signal outcomes and produce a full calibration report.

        Steps:
        1. Accuracy by signal_type
        2. Accuracy by source platform
        3. Accuracy by exclusivity edge
        4. Accuracy by intelligence-score range
        5. Accuracy by actionability
        6. Best / worst signal patterns
        7. Average favourable moves (from signal_outcomes)
        8. Calibration instructions
        9. Adaptive thresholds -> Redis
        10. Playbook -> Redis
        11. Drift detection
        12. Gatekeeper stats
        13. Store final report -> Redis
        """
        lines: list[str] = [
            "=" * 60,
            f"  ALEKFI LEARNING REPORT  --  {datetime.now(timezone.utc).isoformat()}",
            "=" * 60,
            "",
        ]

        # ── 0. Check minimum data ──────────────────────────────────────
        total_feedback = await self._total_feedback_count()
        if total_feedback < 10:
            msg = (
                f"Insufficient feedback data ({total_feedback} records, need 10+). "
                "Skipping calibration -- using defaults."
            )
            logger.info("[learning] %s", msg)
            lines.append(msg)
            report_text = "\n".join(lines)
            await self._store_report(report_text)
            await self._store_default_thresholds()
            return report_text

        lines.append(f"Total feedback records: {total_feedback}")
        lines.append("")

        # ── 1. Accuracy by signal_type ──────────────────────────────────
        type_acc = await self._accuracy_by_signal_type()
        lines.append("## Accuracy by signal_type")
        for row in type_acc:
            lines.append(
                f"  {row['signal_type']:30s}  "
                f"{row['accuracy']:.1%}  ({row['accurate']}/{row['total']})"
            )
        lines.append("")

        # ── 2. Accuracy by source platform ──────────────────────────────
        plat_acc = await self._accuracy_by_platform()
        lines.append("## Accuracy by source platform")
        for row in plat_acc:
            lines.append(
                f"  {row['platform']:30s}  "
                f"{row['accuracy']:.1%}  ({row['accurate']}/{row['total']})"
            )
        lines.append("")

        # ── 3. Accuracy by exclusivity edge ─────────────────────────────
        edge_acc = await self._accuracy_by_exclusivity_edge()
        lines.append("## Accuracy by exclusivity edge")
        for row in edge_acc:
            lines.append(
                f"  {row['edge']:30s}  "
                f"{row['accuracy']:.1%}  ({row['accurate']}/{row['total']})"
            )
        lines.append("")

        # ── 4. Accuracy by intelligence score range ─────────────────────
        bucket_acc = await self._accuracy_by_score_bucket()
        lines.append("## Accuracy by intelligence score bucket")
        for row in bucket_acc:
            lines.append(
                f"  {row['bucket']:30s}  "
                f"{row['accuracy']:.1%}  ({row['accurate']}/{row['total']})"
            )
        lines.append("")

        # ── 5. Accuracy by actionability ────────────────────────────────
        action_acc = await self._accuracy_by_actionability()
        lines.append("## Accuracy by actionability")
        for row in action_acc:
            lines.append(
                f"  {row['actionability']:30s}  "
                f"{row['accuracy']:.1%}  ({row['accurate']}/{row['total']})"
            )
        lines.append("")

        # ── 6. Best / worst patterns ────────────────────────────────────
        best, worst = await self._best_worst_patterns()
        lines.append("## Best signal patterns (sample >= 5)")
        for p in best:
            lines.append(
                f"  {p['signal_type']} | {p['platform']} | {p['edge']}  "
                f"=> {p['accuracy']:.1%} ({p['total']} samples)"
            )
        lines.append("")
        lines.append("## Worst signal patterns (sample >= 5)")
        for p in worst:
            lines.append(
                f"  {p['signal_type']} | {p['platform']} | {p['edge']}  "
                f"=> {p['accuracy']:.1%} ({p['total']} samples)"
            )
        lines.append("")

        # ── 7. Average favourable moves ─────────────────────────────────
        move_stats = await self._average_favorable_moves()
        if move_stats:
            lines.append("## Average favourable moves (signal_outcomes)")
            for row in move_stats:
                lines.append(
                    f"  {row['signal_type']:30s}  "
                    f"avg_move={row['avg_move_pct']:+.2f}%  "
                    f"({row['total']} outcomes)"
                )
            lines.append("")

        # ── 8. Calibration instructions ─────────────────────────────────
        cal_instructions = self._generate_calibration_instructions(
            type_acc, plat_acc, edge_acc
        )
        lines.append("## Calibration Instructions")
        for ci in cal_instructions:
            lines.append(f"  - {ci}")
        lines.append("")

        # ── 9. Adaptive thresholds -> Redis ─────────────────────────────
        overall_accuracy = await self._overall_accuracy()
        thresholds = self._compute_adaptive_thresholds(type_acc, overall_accuracy)
        await self._store_thresholds(thresholds)
        lines.append("## Adaptive Thresholds (stored in Redis)")
        lines.append(f"  {json.dumps(thresholds, indent=4)}")
        lines.append("")

        # ── 10. Playbook -> Redis ───────────────────────────────────────
        playbook = await self._build_playbook(best, move_stats)
        await self._store_playbook(playbook)
        lines.append(f"## Playbook ({len(playbook)} winning patterns stored in Redis)")
        lines.append("")

        # ── 11. Drift detection ─────────────────────────────────────────
        drift = await self.detect_drift()
        if drift:
            lines.append("## DRIFT ALERT")
            lines.append(f"  {json.dumps(drift, indent=4)}")
            lines.append("")

        # ── 12. Gatekeeper stats ────────────────────────────────────────
        gk_stats = await self.compute_gatekeeper_stats()
        lines.append("## Gatekeeper Stats")
        lines.append(f"  {json.dumps(gk_stats, indent=4)}")
        lines.append("")

        # ── Finalize ────────────────────────────────────────────────────
        lines.append("=" * 60)
        lines.append("  END OF REPORT")
        lines.append("=" * 60)

        report_text = "\n".join(lines)
        await self._store_report(report_text)
        return report_text

    # ── Data queries ─────────────────────────────────────────────────────

    async def _total_feedback_count(self) -> int:
        async with get_session() as session:
            result = await session.execute(
                select(func.count(SignalFeedback.id))
            )
            return result.scalar() or 0

    async def _overall_accuracy(self) -> float:
        async with get_session() as session:
            total = (
                await session.execute(select(func.count(SignalFeedback.id)))
            ).scalar() or 0
            if total == 0:
                return 0.0
            accurate = (
                await session.execute(
                    select(func.count(SignalFeedback.id)).where(
                        SignalFeedback.was_accurate == True  # noqa: E712
                    )
                )
            ).scalar() or 0
            return accurate / total

    async def _accuracy_by_signal_type(self) -> list[dict[str, Any]]:
        async with get_session() as session:
            rows = (
                await session.execute(
                    select(
                        Signal.signal_type,
                        func.count(SignalFeedback.id).label("total"),
                        func.sum(
                            case(
                                (SignalFeedback.was_accurate == True, 1),  # noqa: E712
                                else_=0,
                            )
                        ).label("accurate"),
                    )
                    .join(Signal, SignalFeedback.signal_id == Signal.id)
                    .group_by(Signal.signal_type)
                    .order_by(func.count(SignalFeedback.id).desc())
                )
            ).all()

        return [
            {
                "signal_type": r.signal_type,
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

    async def _accuracy_by_platform(self) -> list[dict[str, Any]]:
        """Unpack signals.source_posts JSONB to extract platform accuracy."""
        async with get_session() as session:
            try:
                rows = (
                    await session.execute(
                        text("""
                            SELECT sp->>'platform' AS platform,
                                   COUNT(*)         AS total,
                                   SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                            FROM signal_feedback sf
                            JOIN signals s ON sf.signal_id = s.id
                            CROSS JOIN LATERAL jsonb_array_elements(s.source_posts) AS sp
                            WHERE sp->>'platform' IS NOT NULL
                            GROUP BY sp->>'platform'
                            ORDER BY total DESC
                            LIMIT 20
                        """)
                    )
                ).all()
            except Exception:
                logger.warning("[learning] platform accuracy query failed", exc_info=True)
                return []

        return [
            {
                "platform": r.platform,
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

    async def _accuracy_by_exclusivity_edge(self) -> list[dict[str, Any]]:
        """Extract metadata->>'exclusivity_edge' and compute accuracy."""
        async with get_session() as session:
            try:
                rows = (
                    await session.execute(
                        text("""
                            SELECT s.metadata->>'exclusivity_edge' AS edge,
                                   COUNT(*)                         AS total,
                                   SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                            FROM signal_feedback sf
                            JOIN signals s ON sf.signal_id = s.id
                            WHERE s.metadata->>'exclusivity_edge' IS NOT NULL
                            GROUP BY s.metadata->>'exclusivity_edge'
                            ORDER BY total DESC
                        """)
                    )
                ).all()
            except Exception:
                logger.warning("[learning] exclusivity edge accuracy query failed", exc_info=True)
                return []

        return [
            {
                "edge": r.edge,
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

    async def _accuracy_by_score_bucket(self) -> list[dict[str, Any]]:
        """Bucket signals by priority_score into CRITICAL/HIGH/MODERATE/LOW/NOISE."""
        async with get_session() as session:
            try:
                rows = (
                    await session.execute(
                        text("""
                            SELECT
                                CASE
                                    WHEN (s.metadata->>'priority_score')::int >= 80 THEN 'CRITICAL'
                                    WHEN (s.metadata->>'priority_score')::int >= 65 THEN 'HIGH'
                                    WHEN (s.metadata->>'priority_score')::int >= 50 THEN 'MODERATE'
                                    WHEN (s.metadata->>'priority_score')::int >= 35 THEN 'LOW'
                                    ELSE 'NOISE'
                                END AS bucket,
                                COUNT(*) AS total,
                                SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                            FROM signal_feedback sf
                            JOIN signals s ON sf.signal_id = s.id
                            WHERE s.metadata->>'priority_score' IS NOT NULL
                            GROUP BY bucket
                            ORDER BY MIN((s.metadata->>'priority_score')::int) DESC
                        """)
                    )
                ).all()
            except Exception:
                logger.warning("[learning] score bucket accuracy query failed", exc_info=True)
                return []

        return [
            {
                "bucket": r.bucket,
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

    async def _accuracy_by_actionability(self) -> list[dict[str, Any]]:
        """Extract metadata->>'suggested_action' (actionability) and compute accuracy."""
        async with get_session() as session:
            try:
                rows = (
                    await session.execute(
                        text("""
                            SELECT s.metadata->>'suggested_action' AS actionability,
                                   COUNT(*)                         AS total,
                                   SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                            FROM signal_feedback sf
                            JOIN signals s ON sf.signal_id = s.id
                            WHERE s.metadata->>'suggested_action' IS NOT NULL
                            GROUP BY s.metadata->>'suggested_action'
                            ORDER BY total DESC
                        """)
                    )
                ).all()
            except Exception:
                logger.warning("[learning] actionability accuracy query failed", exc_info=True)
                return []

        return [
            {
                "actionability": r.actionability,
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

    async def _best_worst_patterns(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Find (signal_type, platform, exclusivity_edge) combos with best/worst accuracy.

        Only includes combos with at least 5 samples.
        """
        async with get_session() as session:
            try:
                rows = (
                    await session.execute(
                        text("""
                            SELECT
                                s.signal_type,
                                sp->>'platform'                     AS platform,
                                s.metadata->>'exclusivity_edge'     AS edge,
                                COUNT(*)                             AS total,
                                SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END) AS accurate
                            FROM signal_feedback sf
                            JOIN signals s ON sf.signal_id = s.id
                            CROSS JOIN LATERAL jsonb_array_elements(s.source_posts) AS sp
                            WHERE sp->>'platform' IS NOT NULL
                            GROUP BY s.signal_type,
                                     sp->>'platform',
                                     s.metadata->>'exclusivity_edge'
                            HAVING COUNT(*) >= 5
                            ORDER BY
                                (SUM(CASE WHEN sf.was_accurate THEN 1 ELSE 0 END)::float / COUNT(*)) DESC
                        """)
                    )
                ).all()
            except Exception:
                logger.warning("[learning] best/worst patterns query failed", exc_info=True)
                return [], []

        patterns = [
            {
                "signal_type": r.signal_type,
                "platform": r.platform or "unknown",
                "edge": r.edge or "unknown",
                "total": r.total,
                "accurate": r.accurate or 0,
                "accuracy": (r.accurate or 0) / max(r.total, 1),
            }
            for r in rows
        ]

        # Best = top 10 by accuracy, worst = bottom 10
        best = sorted(patterns, key=lambda p: p["accuracy"], reverse=True)[:10]
        worst = sorted(patterns, key=lambda p: p["accuracy"])[:10]
        return best, worst

    async def _average_favorable_moves(self) -> list[dict[str, Any]]:
        """Query signal_outcomes for average move percentages.

        The signal_outcomes table may not exist yet -- handle gracefully.
        """
        if SignalOutcome is None:
            logger.debug("[learning] SignalOutcome model not available, skipping move stats")
            return []

        try:
            async with get_session() as session:
                rows = (
                    await session.execute(
                        text("""
                            SELECT
                                s.signal_type,
                                COUNT(*)                          AS total,
                                AVG(so.move_pct)                  AS avg_move_pct,
                                MAX(so.move_pct)                  AS max_move_pct,
                                MIN(so.move_pct)                  AS min_move_pct
                            FROM signal_outcomes so
                            JOIN signals s ON so.signal_id = s.id
                            GROUP BY s.signal_type
                            ORDER BY AVG(so.move_pct) DESC
                        """)
                    )
                ).all()

            return [
                {
                    "signal_type": r.signal_type,
                    "total": r.total,
                    "avg_move_pct": float(r.avg_move_pct or 0),
                    "max_move_pct": float(r.max_move_pct or 0),
                    "min_move_pct": float(r.min_move_pct or 0),
                }
                for r in rows
            ]
        except Exception:
            logger.debug(
                "[learning] signal_outcomes query failed (table may not exist yet)",
                exc_info=True,
            )
            return []

    # ── Calibration instructions ─────────────────────────────────────────

    def _generate_calibration_instructions(
        self,
        type_acc: list[dict[str, Any]],
        plat_acc: list[dict[str, Any]],
        edge_acc: list[dict[str, Any]],
    ) -> list[str]:
        """Turn accuracy data into specific text instructions for the LLM."""
        instructions: list[str] = []

        # Signal type thresholds
        for row in type_acc:
            if row["total"] < 5:
                continue
            st = row["signal_type"]
            acc = row["accuracy"]
            if acc < 0.40:
                instructions.append(
                    f"[{st}] accuracy={acc:.0%} (<40%). "
                    f"RAISE conviction threshold to 0.7+ for this signal type. "
                    f"Require additional corroboration before generating."
                )
            elif acc > 0.70:
                instructions.append(
                    f"[{st}] accuracy={acc:.0%} (>70%). "
                    f"These are reliable. Trust them. "
                    f"Can lower conviction threshold to 0.35."
                )

        # Platform calibration
        for row in plat_acc:
            if row["total"] < 5:
                continue
            plat = row["platform"]
            acc = row["accuracy"]
            if acc > 0.65:
                instructions.append(
                    f"[platform:{plat}] accuracy={acc:.0%} (>65%). "
                    f"Boost information_asymmetry score for signals sourced from {plat}."
                )
            elif acc < 0.35:
                instructions.append(
                    f"[platform:{plat}] accuracy={acc:.0%} (<35%). "
                    f"Require corroboration from a second platform before trusting {plat} signals."
                )

        # Exclusivity edge calibration
        for row in edge_acc:
            edge = row["edge"]
            acc = row["accuracy"]
            if row["total"] < 5:
                continue
            if edge == "pre_institutional" and acc > 0.60:
                instructions.append(
                    f"[edge:pre_institutional] accuracy={acc:.0%} (>60%). "
                    f"The social arbitrage edge IS working. Prioritize aggressively."
                )
            elif edge in ("retail_only", "cross_platform") and acc > 0.60:
                instructions.append(
                    f"[edge:{edge}] accuracy={acc:.0%} (>60%). "
                    f"The {edge} edge is generating alpha. Continue prioritizing."
                )
            elif edge == "commodity" and acc < 0.45:
                instructions.append(
                    f"[edge:commodity] accuracy={acc:.0%} (<45%). "
                    f"Already priced in. Be very conservative with commodity-edge signals."
                )

        if not instructions:
            instructions.append(
                "No strong calibration signals yet. Continue collecting feedback."
            )

        return instructions

    # ── Adaptive thresholds ──────────────────────────────────────────────

    def _compute_adaptive_thresholds(
        self,
        type_acc: list[dict[str, Any]],
        overall_accuracy: float,
    ) -> dict[str, Any]:
        """Compute dynamic thresholds based on historical accuracy."""
        min_conviction_by_type: dict[str, float] = {}
        require_corroboration: list[str] = []
        noise_threshold = 35
        critical_threshold = 80

        for row in type_acc:
            st = row["signal_type"]
            acc = row["accuracy"]
            n = row["total"]

            if n >= 10 and acc < 0.40:
                min_conviction_by_type[st] = 0.7
                require_corroboration.append(st)
            elif n >= 10 and acc < 0.50:
                min_conviction_by_type[st] = 0.6
            elif acc > 0.70:
                min_conviction_by_type[st] = 0.35

        # Global accuracy adjustments
        if overall_accuracy < 0.50:
            noise_threshold = 40
        elif overall_accuracy > 0.70:
            noise_threshold = 30

        return {
            "min_conviction_by_type": min_conviction_by_type,
            "noise_score_threshold": noise_threshold,
            "critical_score_threshold": critical_threshold,
            "require_corroboration": require_corroboration,
            "overall_accuracy": round(overall_accuracy, 4),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _store_default_thresholds(self) -> None:
        """Write default thresholds when there is not enough data."""
        defaults = {
            "min_conviction_by_type": {},
            "noise_score_threshold": 35,
            "critical_score_threshold": 80,
            "require_corroboration": [],
            "overall_accuracy": 0.0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        await self._store_thresholds(defaults)

    async def _store_thresholds(self, thresholds: dict[str, Any]) -> None:
        try:
            await self._redis.setex(
                REDIS_THRESHOLDS, THRESHOLDS_TTL, json.dumps(thresholds)
            )
            logger.info("[learning] adaptive thresholds stored in Redis")
        except Exception:
            logger.warning("[learning] failed to store thresholds in Redis", exc_info=True)

    # ── Playbook ─────────────────────────────────────────────────────────

    async def update_playbook(self) -> list[dict[str, Any]]:
        """Build and store the top-20 winning patterns playbook."""
        best, _ = await self._best_worst_patterns()
        move_stats = await self._average_favorable_moves()
        playbook = await self._build_playbook(best, move_stats)
        await self._store_playbook(playbook)
        return playbook

    async def _build_playbook(
        self,
        best_patterns: list[dict[str, Any]],
        move_stats: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Construct the playbook list from best patterns + move stats."""
        move_map: dict[str, float] = {
            m["signal_type"]: m["avg_move_pct"] for m in (move_stats or [])
        }

        playbook: list[dict[str, Any]] = []
        for p in best_patterns[:20]:
            entry = {
                "signal_type": p["signal_type"],
                "source_platforms": [p["platform"]],
                "exclusivity_edge": p["edge"],
                "win_rate": round(p["accuracy"], 4),
                "avg_move_pct": round(move_map.get(p["signal_type"], 0.0), 4),
                "description": (
                    f"{p['signal_type']} signals from {p['platform']} "
                    f"with {p['edge']} edge: "
                    f"{p['accuracy']:.0%} win rate over {p['total']} samples"
                ),
            }
            playbook.append(entry)

        return playbook

    async def _store_playbook(self, playbook: list[dict[str, Any]]) -> None:
        try:
            await self._redis.setex(
                REDIS_PLAYBOOK, PLAYBOOK_TTL, json.dumps(playbook)
            )
            logger.info("[learning] playbook stored in Redis (%d entries)", len(playbook))
        except Exception:
            logger.warning("[learning] failed to store playbook in Redis", exc_info=True)

    # ── Drift detection ──────────────────────────────────────────────────

    async def detect_drift(self) -> dict[str, Any] | None:
        """Compare recent (48 h) vs historical (30 d) accuracy.

        If recent accuracy drops >10 % with 20+ samples, trigger a drift alert
        and auto-tighten the noise threshold.
        """
        recent_cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        historical_cutoff = datetime.now(timezone.utc) - timedelta(days=30)

        async with get_session() as session:
            # Recent accuracy
            recent_total = (
                await session.execute(
                    select(func.count(SignalFeedback.id)).where(
                        SignalFeedback.created_at >= recent_cutoff
                    )
                )
            ).scalar() or 0

            recent_accurate = (
                await session.execute(
                    select(func.count(SignalFeedback.id)).where(
                        SignalFeedback.created_at >= recent_cutoff,
                        SignalFeedback.was_accurate == True,  # noqa: E712
                    )
                )
            ).scalar() or 0

            # Historical accuracy (last 30 days, excluding last 48h for clean comparison)
            hist_total = (
                await session.execute(
                    select(func.count(SignalFeedback.id)).where(
                        SignalFeedback.created_at >= historical_cutoff,
                        SignalFeedback.created_at < recent_cutoff,
                    )
                )
            ).scalar() or 0

            hist_accurate = (
                await session.execute(
                    select(func.count(SignalFeedback.id)).where(
                        SignalFeedback.created_at >= historical_cutoff,
                        SignalFeedback.created_at < recent_cutoff,
                        SignalFeedback.was_accurate == True,  # noqa: E712
                    )
                )
            ).scalar() or 0

        if recent_total < 20 or hist_total < 20:
            logger.debug(
                "[learning] drift check skipped: insufficient samples "
                "(recent=%d, historical=%d)",
                recent_total,
                hist_total,
            )
            return None

        recent_acc = recent_accurate / recent_total
        hist_acc = hist_accurate / hist_total
        drop = hist_acc - recent_acc

        if drop <= 0.10:
            logger.info(
                "[learning] no drift detected (recent=%.1f%%, historical=%.1f%%, drop=%.1f%%)",
                recent_acc * 100,
                hist_acc * 100,
                drop * 100,
            )
            return None

        # Drift detected
        logger.warning(
            "[learning] DRIFT DETECTED: accuracy dropped %.1f%% "
            "(recent=%.1f%% [%d samples], historical=%.1f%% [%d samples])",
            drop * 100,
            recent_acc * 100,
            recent_total,
            hist_acc * 100,
            hist_total,
        )

        # Auto-tighten noise threshold
        await self._auto_tighten_noise_threshold(increase=5)

        drift_alert = {
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "recent_accuracy": round(recent_acc, 4),
            "recent_samples": recent_total,
            "historical_accuracy": round(hist_acc, 4),
            "historical_samples": hist_total,
            "accuracy_drop": round(drop, 4),
            "action_taken": "noise_threshold increased by 5",
        }

        try:
            await self._redis.setex(
                REDIS_DRIFT_ALERT, DRIFT_TTL, json.dumps(drift_alert)
            )
        except Exception:
            logger.warning("[learning] failed to store drift alert in Redis", exc_info=True)

        return drift_alert

    async def _auto_tighten_noise_threshold(self, increase: int = 5) -> None:
        """Read current thresholds from Redis and bump noise_score_threshold."""
        try:
            raw = await self._redis.get(REDIS_THRESHOLDS)
            if raw:
                thresholds = json.loads(raw)
            else:
                thresholds = {
                    "min_conviction_by_type": {},
                    "noise_score_threshold": 35,
                    "critical_score_threshold": 80,
                    "require_corroboration": [],
                }
            old_val = thresholds.get("noise_score_threshold", 35)
            thresholds["noise_score_threshold"] = min(old_val + increase, 60)
            thresholds["drift_tightened_at"] = datetime.now(timezone.utc).isoformat()
            await self._redis.setex(
                REDIS_THRESHOLDS, THRESHOLDS_TTL, json.dumps(thresholds)
            )
            logger.info(
                "[learning] noise threshold tightened: %d -> %d",
                old_val,
                thresholds["noise_score_threshold"],
            )
        except Exception:
            logger.warning("[learning] failed to auto-tighten noise threshold", exc_info=True)

    # ── Gatekeeper stats ─────────────────────────────────────────────────

    async def compute_gatekeeper_stats(self) -> dict[str, Any]:
        """Compute what percentage of gatekeeper-kept posts eventually
        became part of successful signals.

        Stores an instruction in Redis for the gatekeeper LLM.
        """
        async with get_session() as session:
            # Total filtered posts (kept by gatekeeper)
            total_kept = (
                await session.execute(
                    select(func.count()).select_from(
                        select(FilteredPost.id).subquery()  # type: ignore[arg-type]
                    )
                )
            ).scalar() or 0

            if total_kept == 0:
                result = {
                    "total_kept": 0,
                    "contributed_to_signals": 0,
                    "contributed_to_accurate_signals": 0,
                    "hit_rate": 0.0,
                    "accuracy_hit_rate": 0.0,
                }
                await self._store_gatekeeper_instruction(result)
                return result

            # Posts that contributed to any signal (via source_posts JSONB)
            try:
                contributed_total = (
                    await session.execute(
                        text("""
                            SELECT COUNT(DISTINCT fp.id)
                            FROM filtered_posts fp
                            JOIN raw_posts rp ON fp.raw_post_id = rp.id
                            JOIN signals s ON s.source_posts @> jsonb_build_array(
                                jsonb_build_object('post_id', rp.id::text)
                            )
                        """)
                    )
                ).scalar() or 0
            except Exception:
                # Fallback: simpler approach using text matching
                logger.debug(
                    "[learning] optimized gatekeeper query failed, using fallback",
                    exc_info=True,
                )
                contributed_total = 0

            # Posts that contributed to ACCURATE signals
            try:
                contributed_accurate = (
                    await session.execute(
                        text("""
                            SELECT COUNT(DISTINCT fp.id)
                            FROM filtered_posts fp
                            JOIN raw_posts rp ON fp.raw_post_id = rp.id
                            JOIN signals s ON s.source_posts @> jsonb_build_array(
                                jsonb_build_object('post_id', rp.id::text)
                            )
                            JOIN signal_feedback sf ON sf.signal_id = s.id
                            WHERE sf.was_accurate = true
                        """)
                    )
                ).scalar() or 0
            except Exception:
                logger.debug(
                    "[learning] accurate gatekeeper query failed", exc_info=True
                )
                contributed_accurate = 0

        hit_rate = contributed_total / max(total_kept, 1)
        accuracy_hit_rate = contributed_accurate / max(total_kept, 1)

        result = {
            "total_kept": total_kept,
            "contributed_to_signals": contributed_total,
            "contributed_to_accurate_signals": contributed_accurate,
            "hit_rate": round(hit_rate, 4),
            "accuracy_hit_rate": round(accuracy_hit_rate, 4),
        }

        await self._store_gatekeeper_instruction(result)
        return result

    async def _store_gatekeeper_instruction(self, stats: dict[str, Any]) -> None:
        """Generate and store gatekeeper calibration instruction."""
        hit_rate = stats.get("hit_rate", 0)
        accuracy_hit = stats.get("accuracy_hit_rate", 0)
        total = stats.get("total_kept", 0)

        if total < 50:
            instruction = (
                "Insufficient gatekeeper data for calibration. "
                "Continue with default filtering thresholds."
            )
        elif hit_rate < 0.05:
            instruction = (
                f"Only {hit_rate:.1%} of kept posts contributed to signals. "
                "The gatekeeper is too permissive. RAISE the relevance_score threshold. "
                "Be stricter -- reject more low-quality posts."
            )
        elif hit_rate > 0.30:
            instruction = (
                f"{hit_rate:.1%} of kept posts contributed to signals -- excellent selectivity. "
                "Gatekeeper thresholds are well-calibrated. Maintain current standards."
            )
        elif accuracy_hit > 0.15:
            instruction = (
                f"{accuracy_hit:.1%} of kept posts contributed to ACCURATE signals. "
                "The gatekeeper is identifying genuine alpha. Keep prioritizing "
                "high-urgency, cross-platform intelligence."
            )
        else:
            instruction = (
                f"Hit rate: {hit_rate:.1%}, accuracy hit rate: {accuracy_hit:.1%}. "
                "Consider tightening gatekeeper filters slightly. Focus on posts with "
                "higher urgency and from higher-exclusivity platforms."
            )

        try:
            payload = {
                "instruction": instruction,
                "stats": stats,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            await self._redis.setex(
                REDIS_GATEKEEPER_INSTRUCTION,
                GATEKEEPER_TTL,
                json.dumps(payload),
            )
            logger.info("[learning] gatekeeper instruction stored in Redis")
        except Exception:
            logger.warning(
                "[learning] failed to store gatekeeper instruction in Redis",
                exc_info=True,
            )

    # ── Report storage ───────────────────────────────────────────────────

    async def _store_report(self, report_text: str) -> None:
        """Store the complete text report in Redis with 24 h TTL."""
        try:
            await self._redis.setex(REDIS_LEARNING_REPORT, REPORT_TTL, report_text)
            logger.info("[learning] report stored in Redis (%d chars)", len(report_text))
        except Exception:
            logger.warning("[learning] failed to store report in Redis", exc_info=True)

    # ── Convenience: get stored artifacts ────────────────────────────────

    async def get_thresholds(self) -> dict[str, Any]:
        """Read current adaptive thresholds from Redis."""
        try:
            raw = await self._redis.get(REDIS_THRESHOLDS)
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug("[learning] failed to read thresholds from Redis", exc_info=True)
        return {
            "min_conviction_by_type": {},
            "noise_score_threshold": 35,
            "critical_score_threshold": 80,
            "require_corroboration": [],
        }

    async def get_playbook(self) -> list[dict[str, Any]]:
        """Read current playbook from Redis."""
        try:
            raw = await self._redis.get(REDIS_PLAYBOOK)
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug("[learning] failed to read playbook from Redis", exc_info=True)
        return []

    async def get_drift_alert(self) -> dict[str, Any] | None:
        """Read current drift alert from Redis (None if no active alert)."""
        try:
            raw = await self._redis.get(REDIS_DRIFT_ALERT)
            if raw:
                return json.loads(raw)
        except Exception:
            logger.debug("[learning] failed to read drift alert from Redis", exc_info=True)
        return None

    async def get_report(self) -> str | None:
        """Read the latest learning report from Redis."""
        try:
            return await self._redis.get(REDIS_LEARNING_REPORT)
        except Exception:
            logger.debug("[learning] failed to read report from Redis", exc_info=True)
        return None
