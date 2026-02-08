"""Open Claw — CLI entrypoint and orchestrator.

Run individual tiers or the full pipeline::

    python -m open_claw.main --swarm
    python -m open_claw.main --gatekeeper
    python -m open_claw.main --brain
    python -m open_claw.main --server
    python -m open_claw.main --all        # default
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from typing import Any

from open_claw import __version__
from open_claw.config import get_settings
from open_claw.utils import setup_logging

logger = logging.getLogger("open_claw")

BANNER = rf"""
   ___                    ____ _
  / _ \ _ __   ___ _ __  / ___| | __ ___      __
 | | | | '_ \ / _ \ '_ \| |   | |/ _` \ \ /\ / /
 | |_| | |_) |  __/ | | | |___| | (_| |\ V  V /
  \___/| .__/ \___|_| |_|\____|_|\__,_| \_/\_/
       |_|                           v{__version__}
  Social-media intelligence for hedge funds
"""


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="open_claw",
        description="Open Claw — social media intelligence pipeline",
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
            from open_claw.db.database import init_db
            await init_db()
            logger.info("Database ready")
        except Exception as exc:
            logger.warning("Database init skipped: %s", exc)

    background_tasks: list[asyncio.Task] = []
    mock_queue: _MockQueue | None = None
    mock_gatekeeper = None

    # ── Tier 1: Swarm ──────────────────────────────────────────────────

    if "swarm" in components:
        from open_claw.swarm.manager import SwarmManager

        if mock:
            mock_queue = _MockQueue()
            queue = mock_queue
        else:
            from open_claw.queue import RedisQueue
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
            from open_claw.gatekeeper.mock import MockGatekeeper
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
            from open_claw.gatekeeper.gatekeeper import GatekeeperProcessor
            from open_claw.llm_client import get_gatekeeper_client
            from open_claw.queue import RedisQueue

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
            from open_claw.brain.mock import MockBrain
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
            from open_claw.brain.brain import BrainAnalyzer
            from open_claw.llm_client import get_brain_client

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

    # ── Done with --once mode ──────────────────────────────────────────

    if args.once:
        logger.info("All components finished (--once mode)")
        return

    # ── Server or keep-alive ───────────────────────────────────────────

    if "server" in components:
        import uvicorn
        from open_claw.api.app import create_app

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
