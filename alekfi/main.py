"""AlekFi — CLI entrypoint and orchestrator.

Run individual tiers or the full pipeline::

    python -m alekfi.main --swarm
    python -m alekfi.main --gatekeeper
    python -m alekfi.main --brain
    python -m alekfi.main --server
    python -m alekfi.main --all        # default
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from typing import Any

from alekfi import __version__
from alekfi.config import get_settings
from alekfi.utils import setup_logging

logger = logging.getLogger("alekfi")

BANNER = rf"""
     _    _      _    _____ _
    / \  | | ___| | _|  ___(_)
   / _ \ | |/ _ \ |/ / |_  | |
  / ___ \| |  __/   <|  _| | |
 /_/   \_\_|\___|_|\_\_|   |_|  v{__version__}
  Social-media intelligence for hedge funds
"""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="alekfi",
        description="AlekFi — social media intelligence pipeline",
    )
    group = parser.add_argument_group("components")
    group.add_argument("--swarm", action="store_true", help="Run Tier-1 scrapers")
    group.add_argument("--gatekeeper", action="store_true", help="Run Tier-2 relevance filter")
    group.add_argument("--brain", action="store_true", help="Run Tier-3 deep analysis")
    group.add_argument("--signals", action="store_true", help="Run signal aggregation")
    group.add_argument("--server", action="store_true", help="Run FastAPI server on :8000")
    group.add_argument("--all", action="store_true", default=False, help="Run everything (default)")

    parser.add_argument("--mock", action="store_true", help="Enable mock mode (no real API calls)")
    parser.add_argument("--once", action="store_true", help="Run a single pass then exit")
    return parser


async def _run(args: argparse.Namespace) -> None:
    settings = get_settings()

    mock = args.mock or settings.mock_mode
    if args.mock:
        settings.__dict__["mock_mode"] = True

    # Determine which components to start
    components: list[str] = []
    if args.swarm:
        components.append("swarm")
    if args.gatekeeper:
        components.append("gatekeeper")
    if args.brain:
        components.append("brain")
    if args.signals:
        components.append("signals")
    if args.server:
        components.append("server")
    if args.all or not components:
        components = ["swarm", "gatekeeper", "brain", "signals", "server"]

    logger.info("Starting components: %s", ", ".join(components))

    # Initialise database (skip in mock+once mode)
    skip_db = mock and args.once
    if not skip_db:
        try:
            from alekfi.db.database import init_db
            await init_db()
            logger.info("Database ready")
        except Exception as exc:
            logger.warning("Database init skipped: %s", exc)

    background_tasks: list[asyncio.Task] = []
    mock_queue: _MockQueue | None = None
    mock_gatekeeper = None

    # ── Tier 1: Swarm ──────────────────────────────────────────────────

    if "swarm" in components:
        from alekfi.swarm.manager import SwarmManager

        if mock:
            mock_queue = _MockQueue()
            queue = mock_queue
        else:
            from alekfi.queue import RedisQueue
            queue = RedisQueue()

        swarm = SwarmManager(config=settings, queue=queue, mock=mock)

        if args.once:
            await swarm.run(once=True)
            status = swarm.get_status()
            logger.info(
                "Swarm single-pass: %d posts from %d scrapers",
                status["total_posts"], status["active_count"],
            )
            for name, stats in status["scrapers"].items():
                logger.info("  %-20s %4d posts  %d errors", name, stats["total_posts"], stats["error_count"])
        else:
            task = asyncio.create_task(swarm.run(once=False), name="swarm-manager")
            background_tasks.append(task)

    # ── Tier 2: Gatekeeper ─────────────────────────────────────────────

    if "gatekeeper" in components:
        if mock:
            from alekfi.gatekeeper.mock import MockGatekeeper
            if mock_queue is None:
                mock_queue = _MockQueue()
            mock_gatekeeper = MockGatekeeper(config=settings, queue=mock_queue)

            if args.once:
                # Process all posts in batches
                while len(mock_queue._posts) > 0:
                    await mock_gatekeeper.process_batch()
                gk_stats = mock_gatekeeper.get_stats()
                logger.info(
                    "Gatekeeper single-pass: %d processed → %d kept (%d killed, %.0f%% kill rate)",
                    gk_stats["total_processed"], gk_stats["total_kept"],
                    gk_stats["total_killed"], gk_stats["kill_rate"] * 100,
                )
            else:
                task = asyncio.create_task(mock_gatekeeper.run(once=False), name="gatekeeper")
                background_tasks.append(task)
        else:
            from alekfi.gatekeeper.gatekeeper import GatekeeperProcessor
            from alekfi.llm_client import get_gatekeeper_client
            from alekfi.queue import RedisQueue

            gk_queue = RedisQueue()
            gk = GatekeeperProcessor(
                config=settings,
                llm_client=get_gatekeeper_client(),
                queue=gk_queue,
            )
            if args.once:
                await gk.run(once=True)
                gk_stats = gk.get_stats()
                logger.info(
                    "Gatekeeper single-pass: %d processed → %d kept (%d killed)",
                    gk_stats["total_processed"], gk_stats["total_kept"], gk_stats["total_killed"],
                )
            else:
                task = asyncio.create_task(gk.run(once=False), name="gatekeeper")
                background_tasks.append(task)

    # ── Tier 3: Brain ──────────────────────────────────────────────────

    if "brain" in components:
        if mock:
            from alekfi.brain.mock import MockBrain
            brain = MockBrain(config=settings)

            if args.once:
                # Get filtered posts from mock gatekeeper
                filtered = mock_gatekeeper.filtered_posts if mock_gatekeeper else []
                if filtered:
                    await brain.run_on_filtered(filtered)
                else:
                    logger.info("[brain-mock] no filtered posts to analyze")

                brain_stats = brain.get_stats()
                logger.info(
                    "Brain single-pass: %d analyzed → %d entities, %d sentiments → %d signals",
                    brain_stats["posts_analyzed"], brain_stats["entities_extracted"],
                    brain_stats["sentiments_scored"], brain_stats["signals_generated"],
                )
                # Print signal details
                for sig in brain._signals:
                    instruments = sig.get("affected_instruments", [])
                    syms = ", ".join(i["symbol"] for i in instruments)
                    logger.info(
                        "  SIGNAL: %s %s [%s] conviction=%.2f horizon=%s",
                        sig["direction"], syms, sig["signal_type"],
                        sig["conviction"], sig["time_horizon"],
                    )
            else:
                # In continuous mock mode, periodically process new filtered posts
                async def _mock_brain_loop():
                    while True:
                        if mock_gatekeeper and mock_gatekeeper.filtered_posts:
                            batch = list(mock_gatekeeper.filtered_posts)
                            mock_gatekeeper.filtered_posts.clear()
                            await brain.run_on_filtered(batch)
                        await asyncio.sleep(5)
                task = asyncio.create_task(_mock_brain_loop(), name="brain")
                background_tasks.append(task)
        else:
            from alekfi.brain.brain import BrainAnalyzer
            from alekfi.llm_client import get_brain_client

            brain = BrainAnalyzer(config=settings, llm_client=get_brain_client())
            if args.once:
                await brain.run(once=True)
                brain_stats = brain.get_stats()
                logger.info(
                    "Brain single-pass: %d analyzed → %d entities, %d sentiments → %d signals",
                    brain_stats["posts_analyzed"], brain_stats["entities_extracted"],
                    brain_stats["sentiments_scored"], brain_stats["signals_generated"],
                )
            else:
                task = asyncio.create_task(brain.run(once=False), name="brain")
                background_tasks.append(task)

                # Start price tracker alongside brain (real mode, continuous)
                try:
                    from alekfi.brain.price_tracker import PriceTracker
                    price_tracker = PriceTracker(config=settings)
                    pt_task = asyncio.create_task(price_tracker.run(), name="price_tracker")
                    background_tasks.append(pt_task)
                    logger.info("Price tracker started (signal validation + price caching)")
                except ImportError:
                    logger.warning("Price tracker not available")

                # Start learning engine (self-evolving intelligence system)
                try:
                    from alekfi.brain.learning_engine import LearningEngine
                    learning = LearningEngine(config=settings)
                    le_task = asyncio.create_task(
                        learning.run_loop(interval=settings.learning_interval),
                        name="learning_engine",
                    )
                    background_tasks.append(le_task)
                    logger.info("Learning engine started (interval=%ds)", settings.learning_interval)
                except ImportError:
                    logger.warning("Learning engine not available")

                # Start calibration analyst (post-level outcome tracking + rule generation)
                try:
                    from alekfi.brain.calibration_analyst import CalibrationAnalyst
                    calibration_analyst = CalibrationAnalyst(config=settings)
                    # Attach to brain so it can call log_post_outcome
                    brain._calibration_analyst = calibration_analyst
                    ca_task = asyncio.create_task(
                        calibration_analyst.run(), name="calibration_analyst",
                    )
                    background_tasks.append(ca_task)
                    logger.info("Calibration analyst started (post-level outcome tracking)")
                except Exception:
                    logger.warning("Calibration analyst not available", exc_info=True)

                # ── Phase 2: Intelligence Foundation Components ──────────
                try:
                    import redis.asyncio as _phase2_redis
                    from alekfi.db.database import get_session as _p2_get_session
                    _p2_redis = _phase2_redis.from_url(settings.redis_url, decode_responses=True)

                    # ConvictionEngine
                    try:
                        from alekfi.brain.conviction_engine import ConvictionEngine
                        conviction_engine = ConvictionEngine(_p2_redis, _p2_get_session)
                        await conviction_engine.initialize()
                        brain._conviction_engine = conviction_engine
                        logger.info("ConvictionEngine initialized")
                    except Exception:
                        logger.warning("ConvictionEngine not available", exc_info=True)

                    # EntityMemory
                    try:
                        from alekfi.brain.entity_memory import EntityMemory
                        entity_memory = EntityMemory(_p2_redis, _p2_get_session)
                        brain._entity_memory = entity_memory
                        logger.info("EntityMemory initialized")
                    except Exception:
                        logger.warning("EntityMemory not available", exc_info=True)

                    # PatternLibrary
                    try:
                        from alekfi.brain.pattern_library import PatternLibrary
                        pattern_library = PatternLibrary(_p2_redis, _p2_get_session)
                        logger.info("PatternLibrary initialized")
                    except Exception:
                        pattern_library = None
                        logger.warning("PatternLibrary not available", exc_info=True)

                    # CorrelationTracker
                    try:
                        from alekfi.brain.correlation_tracker_v2 import CorrelationTracker
                        correlation_tracker = CorrelationTracker(_p2_redis, _p2_get_session)
                        brain._correlation_tracker = correlation_tracker
                        logger.info("CorrelationTracker initialized")
                    except Exception:
                        logger.warning("CorrelationTracker not available", exc_info=True)

                    # NarrativeTracker
                    try:
                        from alekfi.brain.narrative_tracker import NarrativeTracker
                        from alekfi.llm_client import get_brain_client as _get_narr_client
                        narrative_tracker = NarrativeTracker(_p2_redis, _p2_get_session, _get_narr_client())
                        brain._narrative_tracker = narrative_tracker
                        narr_task = asyncio.create_task(narrative_tracker.run(), name="narrative_tracker")
                        background_tasks.append(narr_task)
                        logger.info("NarrativeTracker started")
                    except Exception:
                        logger.warning("NarrativeTracker not available", exc_info=True)

                    # OutcomeAttribution
                    try:
                        from alekfi.brain.outcome_attribution import OutcomeAttribution
                        outcome_attribution = OutcomeAttribution(_p2_redis, _p2_get_session)
                        logger.info("OutcomeAttribution initialized")
                    except Exception:
                        outcome_attribution = None
                        logger.warning("OutcomeAttribution not available", exc_info=True)

                    # IntelligenceBrief
                    try:
                        from alekfi.brain.intelligence_brief import IntelligenceBrief
                        from alekfi.llm_client import get_brain_client as _get_brief_client
                        intelligence_brief = IntelligenceBrief(_p2_redis, _p2_get_session, _get_brief_client())
                        brief_task = asyncio.create_task(intelligence_brief.run(), name="intelligence_brief")
                        background_tasks.append(brief_task)
                        logger.info("IntelligenceBrief started")
                    except Exception:
                        logger.warning("IntelligenceBrief not available", exc_info=True)

                    # Phase 2 periodic tasks (calibration cycle additions)
                    async def _phase2_calibration_loop():
                        """Runs every 4 hours alongside calibration analyst."""
                        import asyncio as _p2_aio
                        await _p2_aio.sleep(120)  # Wait 2 min after startup
                        while True:
                            try:
                                # Entity state updates
                                if entity_memory:
                                    try:
                                        await entity_memory.update_all_active_entities()
                                    except Exception:
                                        logger.warning("[phase2] entity state update failed", exc_info=True)

                                # Pattern library updates
                                if pattern_library:
                                    try:
                                        await pattern_library.update_patterns_from_outcomes()
                                    except Exception:
                                        logger.warning("[phase2] pattern update failed", exc_info=True)

                                # Conviction weight learning
                                if conviction_engine:
                                    try:
                                        await conviction_engine.learn_weights()
                                        await conviction_engine.refresh_caches()
                                    except Exception:
                                        logger.warning("[phase2] conviction learning failed", exc_info=True)

                                # Outcome attribution
                                if outcome_attribution:
                                    try:
                                        await outcome_attribution.attribute_recent_outcomes()
                                    except Exception:
                                        logger.warning("[phase2] attribution failed", exc_info=True)

                                logger.info("[HEARTBEAT][phase2] calibration cycle complete")
                            except Exception:
                                logger.warning("[phase2] calibration loop error", exc_info=True)
                            await _p2_aio.sleep(14400)  # 4 hours

                    p2_cal_task = asyncio.create_task(_phase2_calibration_loop(), name="phase2_calibration")
                    background_tasks.append(p2_cal_task)
                    logger.info("Phase 2 Intelligence Foundation started (7 components)")

                except Exception:
                    logger.warning("Phase 2 components not fully available", exc_info=True)

                # Initialize Signal Weight Engine
                try:
                    import redis.asyncio as _aioredis
                    from alekfi.analysis.credibility_scorer import SourceCredibilityScorer
                    from alekfi.analysis.quality_evaluator import ArgumentQualityEvaluator
                    from alekfi.analysis.influence_estimator import InfluenceEstimator
                    from alekfi.analysis.signal_weight_engine import SignalWeightEngine
                    from alekfi.analysis.weight_tuner import WeightTuner
                    from alekfi.db.database import get_session as _get_session
                    from alekfi.llm_client import get_gatekeeper_client

                    _weight_redis = _aioredis.from_url(settings.redis_url, decode_responses=True)
                    _cred_scorer = SourceCredibilityScorer()  # no redis needed for sync use
                    _qual_eval = ArgumentQualityEvaluator(_weight_redis, get_gatekeeper_client())
                    _infl_est = InfluenceEstimator()
                    _weight_engine = SignalWeightEngine(_weight_redis, _cred_scorer, _qual_eval, _infl_est)
                    brain._signal_weight_engine = _weight_engine

                    # Set shadow mode on by default
                    await _weight_redis.set('alekfi:weights:shadow_mode', 'true')

                    logger.info('Signal Weight Engine initialized (shadow mode)')

                    # Start weekly weight tuner
                    _weight_tuner = WeightTuner(_weight_redis, _get_session)

                    async def _weekly_weight_tuning():
                        await asyncio.sleep(300)  # Wait 5 min on startup
                        while True:
                            try:
                                result = await _weight_tuner.tune()
                                logger.info('Weekly weight tuning: %s', result.get('status'))
                            except Exception as _te:
                                logger.error('Weight tuning error: %s', _te)
                            await asyncio.sleep(86400 * 7)  # Weekly

                    wt_task = asyncio.create_task(_weekly_weight_tuning(), name='weight_tuner')
                    background_tasks.append(wt_task)
                    logger.info('Weight tuner scheduled (weekly)')
                except Exception:
                    logger.warning('Signal Weight Engine not available', exc_info=True)

                # Start intelligence workers (options flow, short interest, institutional)
                try:
                    from alekfi.workers.options_flow_worker import OptionsFlowWorker
                    options_worker = OptionsFlowWorker()
                    ow_task = asyncio.create_task(options_worker.run(), name="options_flow_worker")
                    background_tasks.append(ow_task)
                    logger.info("Options flow worker started")
                except Exception:
                    logger.warning("Options flow worker not available", exc_info=True)

                try:
                    from alekfi.workers.short_interest_worker import ShortInterestWorker
                    si_worker = ShortInterestWorker()
                    si_task = asyncio.create_task(si_worker.run(), name="short_interest_worker")
                    background_tasks.append(si_task)
                    logger.info("Short interest worker started")
                except Exception:
                    logger.warning("Short interest worker not available", exc_info=True)

                try:
                    from alekfi.workers.institutional_tracker import InstitutionalTracker
                    inst_tracker = InstitutionalTracker()
                    it_task = asyncio.create_task(inst_tracker.run(), name="institutional_tracker")
                    background_tasks.append(it_task)
                    logger.info("Institutional tracker started")
                except Exception:
                    logger.warning("Institutional tracker not available", exc_info=True)

                # v3: Market context worker (6 Finnhub data layers)
                try:
                    from alekfi.workers.market_context_worker import MarketContextWorker
                    mctx_worker = MarketContextWorker()
                    mctx_task = asyncio.create_task(mctx_worker.run(), name="market_context_worker")
                    background_tasks.append(mctx_task)
                    logger.info("Market context worker started")
                except Exception:
                    logger.warning("Market context worker not available", exc_info=True)

                # v3: Signal decay worker (price outcome tracking)
                try:
                    from alekfi.workers.signal_decay_worker import SignalDecayWorker
                    decay_worker = SignalDecayWorker()
                    decay_task = asyncio.create_task(decay_worker.run(), name="signal_decay_worker")
                    background_tasks.append(decay_task)
                    logger.info("Signal decay worker started")
                except Exception:
                    logger.warning("Signal decay worker not available", exc_info=True)

                # v3: Corroboration worker (strict feed enrichment)
                try:
                    from alekfi.workers.corroboration_worker import CorroborationWorker
                    corr_interval = int(os.environ.get("CORROBORATION_INTERVAL_SECONDS", "180"))
                    corr_worker = CorroborationWorker(interval_seconds=corr_interval)
                    corr_task = asyncio.create_task(corr_worker.run(), name="corroboration_worker")
                    background_tasks.append(corr_task)
                    logger.info("Corroboration worker started (interval=%ds)", corr_interval)
                except Exception:
                    logger.warning("Corroboration worker not available", exc_info=True)

                # v3: Nightly label densification job (fail-closed on degraded data)
                async def _nightly_label_loop():
                    import asyncio as _aio
                    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
                    from alekfi.tools.backfill_labels import run_backfill_labels as _run_backfill

                    await _aio.sleep(120)
                    while True:
                        try:
                            since = _dt.now(_tz.utc) - _td(days=2)
                            summary = await _run_backfill(
                                since=since,
                                limit=int(os.environ.get("NIGHTLY_LABEL_LIMIT", "1200")),
                                include_null_samples=True,
                                null_sample_limit=int(os.environ.get("NIGHTLY_NULL_SAMPLE_LIMIT", "600")),
                                force=False,
                            )
                            logger.info("[labels] nightly job complete: %s", summary)
                        except Exception:
                            logger.warning("[labels] nightly job failed", exc_info=True)
                        await _aio.sleep(86400)

                try:
                    labels_task = asyncio.create_task(_nightly_label_loop(), name="nightly_labels")
                    background_tasks.append(labels_task)
                    logger.info("Nightly label job started")
                except Exception:
                    logger.warning("Nightly label job not available", exc_info=True)

                # v3: Daily forecast-priors training (feeds research_bundle model priors)
                async def _forecast_priors_loop():
                    import asyncio as _aio
                    from alekfi.tools.train_forecast_model import train_priors as _train_priors, _store_redis as _store_priors

                    await _aio.sleep(180)
                    while True:
                        try:
                            payload = await _train_priors(
                                since_days=int(os.environ.get("FORECAST_TRAIN_SINCE_DAYS", "180")),
                                min_samples=int(os.environ.get("FORECAST_TRAIN_MIN_SAMPLES", "20")),
                            )
                            await _store_priors(payload)
                            logger.info(
                                "[forecast] priors trained: rows=%s signal_types=%s",
                                payload.get("total_rows"),
                                len((payload.get("by_signal_type") or {})),
                            )
                        except Exception:
                            logger.warning("[forecast] priors training failed", exc_info=True)
                        await _aio.sleep(86400)

                try:
                    priors_task = asyncio.create_task(_forecast_priors_loop(), name="forecast_priors")
                    background_tasks.append(priors_task)
                    logger.info("Forecast priors training loop started")
                except Exception:
                    logger.warning("Forecast priors training not available", exc_info=True)

                # v3: Macro regime updater (runs every 30 min)
                async def _macro_regime_loop():
                    import asyncio as _aio
                    while True:
                        try:
                            from alekfi.analysis.macro_regime import update_macro_regime
                            await update_macro_regime()
                        except Exception:
                            logger.warning("Macro regime update failed", exc_info=True)
                        await _aio.sleep(1800)

                try:
                    macro_task = asyncio.create_task(_macro_regime_loop(), name="macro_regime_updater")
                    background_tasks.append(macro_task)
                    logger.info("Macro regime updater started")
                except Exception:
                    logger.warning("Macro regime updater not available", exc_info=True)

                # v4: RegimeDetector (SPY/VIX regime classification, every 30 min)
                async def _regime_detector_loop():
                    import asyncio as _aio
                    while True:
                        try:
                            from alekfi.brain.regime_detector import RegimeDetector
                            detector = RegimeDetector()
                            result = await detector.detect_regime()
                            logger.info("[regime] detected: %s (VIX=%.1f)", result.get("regime", "?"), result.get("vix", 0))
                        except Exception:
                            logger.warning("Regime detector update failed", exc_info=True)
                        await _aio.sleep(1800)

                try:
                    regime_det_task = asyncio.create_task(_regime_detector_loop(), name="regime_detector")
                    background_tasks.append(regime_det_task)
                    logger.info("Regime detector started (30m interval)")
                except Exception:
                    logger.warning("Regime detector not available", exc_info=True)

                # v4: Set pipeline feature flag default
                try:
                    import redis.asyncio as _pipeline_redis
                    _pr = _pipeline_redis.from_url(settings.redis_url, decode_responses=True)
                    _existing = await _pr.get("alekfi:config:use_pipeline")
                    if _existing is None:
                        await _pr.set("alekfi:config:use_pipeline", "true")
                        logger.info("Pipeline feature flag set to true (default)")
                    else:
                        logger.info("Pipeline feature flag: %s", _existing)
                    await _pr.aclose()
                except Exception:
                    logger.warning("Could not set pipeline feature flag", exc_info=True)


    # ── Done with --once mode ──────────────────────────────────────────

    if args.once:
        logger.info("All components finished (--once mode)")
        return

    # ── Server or keep-alive ───────────────────────────────────────────

    if "server" in components:
        import uvicorn
        from alekfi.api.app import create_app

        app = create_app()
        config = uvicorn.Config(
            app,
            host=settings.api_host,
            port=settings.api_port,
            log_level=settings.log_level.lower(),
        )
        server = uvicorn.Server(config)
        await server.serve()
    else:
        logger.info("Pipeline running — press Ctrl+C to stop")
        stop = asyncio.Event()
        try:
            await stop.wait()
        except asyncio.CancelledError:
            logger.info("Shutdown requested")
            for t in background_tasks:
                t.cancel()


# ── Mock helpers ───────────────────────────────────────────────────────

class _MockQueue:
    """In-memory queue stand-in for mock/test runs (no Redis needed)."""

    def __init__(self) -> None:
        self._posts: list[dict[str, Any]] = []
        self._total = 0

    async def push_raw_posts(self, posts: list[dict[str, Any]]) -> int:
        self._posts.extend(posts)
        self._total += len(posts)
        return len(posts)

    async def pop_raw_posts(self, batch_size: int = 100) -> list[dict[str, Any]]:
        batch = self._posts[:batch_size]
        self._posts = self._posts[batch_size:]
        return batch

    async def push_filtered(self, posts: list[dict[str, Any]]) -> int:
        return len(posts)

    async def get_stats(self) -> dict[str, Any]:
        return {"raw_queue_length": len(self._posts), "total_pushed": self._total}

    async def close(self) -> None:
        self._posts.clear()


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    settings = get_settings()
    setup_logging(settings.log_level)

    print(BANNER, file=sys.stderr)

    try:
        asyncio.run(_run(args))
    except KeyboardInterrupt:
        logger.info("Interrupted — shutting down")


if __name__ == "__main__":
    main()
