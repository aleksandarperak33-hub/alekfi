"""Centralized configuration via pydantic-settings, loaded from .env."""

from __future__ import annotations

import functools
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ── LLM · Gatekeeper (Tier 2) ──────────────────────────────────────
    gatekeeper_provider: str = "deepseek"
    gatekeeper_api_key: str = "sk-xxx"
    gatekeeper_base_url: str = "https://api.deepseek.com/v1"
    gatekeeper_model: str = "deepseek-chat"

    # ── LLM · Brain (Tier 3) ───────────────────────────────────────────
    brain_provider: str = "deepseek"
    brain_api_key: str = "sk-xxx"
    brain_base_url: str = "https://api.deepseek.com/v1"
    brain_model: str = "deepseek-chat"

    # ── Infrastructure ─────────────────────────────────────────────────
    database_url: str = "postgresql+asyncpg://openclaw:openclaw@localhost:5432/openclaw"
    redis_url: str = "redis://localhost:6379/0"

    # ── Data Sources ───────────────────────────────────────────────────
    twitter_bearer_token: str = ""
    reddit_client_id: str = ""
    reddit_client_secret: str = ""
    reddit_user_agent: str = "OpenClaw/1.0"
    youtube_api_key: str = ""
    discord_bot_token: str = ""
    telegram_api_id: str = ""
    telegram_api_hash: str = ""
    apify_api_key: str = ""

    # ── API Server ────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000

    # ── Operational Settings ───────────────────────────────────────────
    scrape_interval_seconds: int = 60
    gatekeeper_batch_size: int = 20
    brain_batch_size: int = 10
    brain_synthesis_interval: int = 15  # minutes between synthesis runs
    brain_entity_confidence_threshold: float = 0.5
    log_level: str = "INFO"
    mock_mode: bool = False

    # ── Computed helpers ───────────────────────────────────────────────
    @property
    def async_database_url(self) -> str:
        """Return the database URL, ensuring the asyncpg driver."""
        url = self.database_url
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url

    @property
    def sync_database_url(self) -> str:
        """Return a synchronous-friendly database URL (for Alembic)."""
        return self.database_url.replace("+asyncpg", "")

    @property
    def redis_host(self) -> str:
        """Extract host from redis URL."""
        from urllib.parse import urlparse
        return urlparse(self.redis_url).hostname or "localhost"

    @property
    def redis_port(self) -> int:
        """Extract port from redis URL."""
        from urllib.parse import urlparse
        return urlparse(self.redis_url).port or 6379

    @property
    def redis_db(self) -> int:
        """Extract db number from redis URL."""
        from urllib.parse import urlparse
        path = urlparse(self.redis_url).path
        return int(path.lstrip("/") or "0")


@functools.lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Singleton accessor for the global settings."""
    return Settings()
