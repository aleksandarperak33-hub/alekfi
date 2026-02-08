"""FastAPI application factory with lifespan, CORS, static files, and routers."""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from open_claw import __version__
from open_claw.config import get_settings

logger = logging.getLogger(__name__)

_start_time: float = 0.0


def get_uptime() -> float:
    return time.time() - _start_time if _start_time else 0.0


@asynccontextmanager
async def _lifespan(app: FastAPI):
    global _start_time
    _start_time = time.time()

    settings = get_settings()
    if not settings.mock_mode:
        try:
            from open_claw.db.database import init_db
            await init_db()
            logger.info("Database initialised")
        except Exception as exc:
            logger.warning("Database init failed (running in degraded mode): %s", exc)

    logger.info("Open Claw API v%s starting", __version__)
    yield
    logger.info("Open Claw API shutting down")


def create_app() -> FastAPI:
    """Build and return the fully-configured FastAPI application."""
    settings = get_settings()

    app = FastAPI(
        title="Open Claw",
        description="Social media intelligence platform for hedge funds",
        version=__version__,
        lifespan=_lifespan,
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    from open_claw.api.routes import signals, entities, posts, system
    app.include_router(signals.router, prefix="/api")
    app.include_router(entities.router, prefix="/api")
    app.include_router(posts.router, prefix="/api")
    app.include_router(system.router, prefix="/api")

    # Static files (dashboard)
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app
