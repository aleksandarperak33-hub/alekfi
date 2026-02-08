"""SwarmManager — orchestrates all Tier-1 scrapers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from open_claw.config import Settings
from open_claw.queue import RedisQueue
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)


class SwarmManager:
    """Instantiates, activates, and runs all Tier-1 scrapers.

    In mock mode every scraper uses its Mock* counterpart.
    In real mode scrapers are only activated if their credentials are configured.
    """

    def __init__(self, config: Settings, queue: RedisQueue, mock: bool = False) -> None:
        self._config = config
        self._queue = queue
        self._mock = mock
        self._scrapers: list[BaseScraper] = []
        self._dormant: list[str] = []
        self._register_scrapers()

    # ── registration ───────────────────────────────────────────────────

    def _register_scrapers(self) -> None:
        if self._mock:
            self._register_mock_scrapers()
        else:
            self._register_real_scrapers()

    def _register_mock_scrapers(self) -> None:
        from open_claw.swarm.reddit import MockRedditScraper
        from open_claw.swarm.news_rss import MockNewsScraper
        from open_claw.swarm.sec_edgar import MockEdgarScraper
        from open_claw.swarm.hackernews import MockHNScraper
        from open_claw.swarm.youtube import MockYouTubeScraper
        from open_claw.swarm.google_trends import MockGoogleTrendsScraper
        from open_claw.swarm.tiktok import MockTikTokScraper
        from open_claw.swarm.instagram import MockInstagramScraper
        from open_claw.swarm.discord_scraper import MockDiscordScraper
        from open_claw.swarm.telegram_scraper import MockTelegramScraper
        from open_claw.swarm.glassdoor import MockGlassdoorScraper
        from open_claw.swarm.amazon_reviews import MockAmazonReviewScraper
        from open_claw.swarm.appstore import MockAppStoreScraper
        from open_claw.swarm.twitter import MockTwitterScraper

        interval = self._config.scrape_interval_seconds
        self._scrapers = [
            MockRedditScraper(interval),
            MockNewsScraper(interval),
            MockEdgarScraper(interval),
            MockHNScraper(interval),
            MockYouTubeScraper(interval),
            MockGoogleTrendsScraper(interval),
            MockTikTokScraper(interval),
            MockInstagramScraper(interval),
            MockDiscordScraper(interval),
            MockTelegramScraper(interval),
            MockGlassdoorScraper(interval),
            MockAmazonReviewScraper(interval),
            MockAppStoreScraper(interval),
            MockTwitterScraper(interval),
        ]
        # No dormant scrapers in mock mode — Twitter mock is always active
        logger.info("Mock mode: registered %d scrapers (all active)", len(self._scrapers))

    def _register_real_scrapers(self) -> None:
        c = self._config
        interval = c.scrape_interval_seconds

        # ── Always-on scrapers (no API key required) ───────────────────
        from open_claw.swarm.news_rss import NewsRSSScraper
        from open_claw.swarm.sec_edgar import SECEdgarScraper
        from open_claw.swarm.hackernews import HackerNewsScraper
        from open_claw.swarm.appstore import AppStoreScraper
        from open_claw.swarm.glassdoor import GlassdoorScraper
        from open_claw.swarm.amazon_reviews import AmazonReviewScraper

        self._scrapers.extend([
            NewsRSSScraper(interval),
            SECEdgarScraper(interval * 2),
            HackerNewsScraper(interval),
            AppStoreScraper(interval * 30),
            GlassdoorScraper(interval * 60),
            AmazonReviewScraper(interval * 60),
        ])

        # ── Reddit (needs client_id + client_secret) ──────────────────
        if c.reddit_client_id and c.reddit_client_secret:
            from open_claw.swarm.reddit import RedditScraper
            self._scrapers.append(RedditScraper(interval))
        else:
            logger.warning("Reddit scraper SKIPPED — REDDIT_CLIENT_ID/SECRET not set")
            self._dormant.append("reddit")

        # ── YouTube (needs API key) ────────────────────────────────────
        if c.youtube_api_key:
            from open_claw.swarm.youtube import YouTubeScraper
            self._scrapers.append(YouTubeScraper(interval * 3))
        else:
            logger.warning("YouTube scraper SKIPPED — YOUTUBE_API_KEY not set")
            self._dormant.append("youtube")

        # ── Google Trends (no key but may hit rate limits) ─────────────
        from open_claw.swarm.google_trends import GoogleTrendsScraper
        self._scrapers.append(GoogleTrendsScraper(interval * 5))

        # ── TikTok (needs Apify key) ──────────────────────────────────
        if c.apify_api_key:
            from open_claw.swarm.tiktok import TikTokScraper
            self._scrapers.append(TikTokScraper(interval * 5))
        else:
            logger.warning("TikTok scraper SKIPPED — APIFY_API_KEY not set")
            self._dormant.append("tiktok")

        # ── Instagram (needs Apify key) ────────────────────────────────
        if c.apify_api_key:
            from open_claw.swarm.instagram import InstagramScraper
            self._scrapers.append(InstagramScraper(interval * 5))
        else:
            logger.warning("Instagram scraper SKIPPED — APIFY_API_KEY not set")
            self._dormant.append("instagram")

        # ── Discord (needs bot token) ─────────────────────────────────
        if c.discord_bot_token:
            from open_claw.swarm.discord_scraper import DiscordScraper
            self._scrapers.append(DiscordScraper(interval // 2))
        else:
            logger.warning("Discord scraper SKIPPED — DISCORD_BOT_TOKEN not set")
            self._dormant.append("discord")

        # ── Telegram (needs api_id + api_hash) ────────────────────────
        if c.telegram_api_id and c.telegram_api_hash:
            from open_claw.swarm.telegram_scraper import TelegramScraper
            self._scrapers.append(TelegramScraper(interval // 2))
        else:
            logger.warning("Telegram scraper SKIPPED — TELEGRAM_API_ID/HASH not set")
            self._dormant.append("telegram")

        # ── Twitter (DORMANT unless token set) ─────────────────────────
        if c.twitter_bearer_token:
            from open_claw.swarm.twitter import TwitterScraper
            self._scrapers.append(TwitterScraper(interval))
        else:
            logger.warning("Twitter scraper DORMANT — TWITTER_BEARER_TOKEN not set")
            self._dormant.append("twitter")

        active = len(self._scrapers)
        dormant = len(self._dormant)
        logger.info("Real mode: %d active scrapers, %d dormant (%s)", active, dormant, ", ".join(self._dormant) or "none")

    # ── running ────────────────────────────────────────────────────────

    async def run(self, once: bool = False) -> None:
        """Launch all scrapers concurrently."""
        if not self._scrapers:
            logger.error("No scrapers registered — nothing to run")
            return

        logger.info(
            "Swarm starting: %d scrapers [%s]",
            len(self._scrapers),
            ", ".join(s.platform for s in self._scrapers),
        )

        if once:
            results = await asyncio.gather(
                *(s.run_once(self._queue) for s in self._scrapers),
                return_exceptions=True,
            )
            total = 0
            for scraper, result in zip(self._scrapers, results):
                if isinstance(result, Exception):
                    logger.error("[%s] failed: %s", scraper.platform, result)
                else:
                    total += result
            logger.info("Single pass complete: %d total posts from %d scrapers", total, len(self._scrapers))
            return

        tasks = [
            asyncio.create_task(s.run_loop(self._queue), name=f"scraper-{s.platform}")
            for s in self._scrapers
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            logger.info("Swarm shutting down, cancelling scrapers")
            for t in tasks:
                t.cancel()

    # ── status ─────────────────────────────────────────────────────────

    @property
    def active_scrapers(self) -> list[str]:
        return [s.platform for s in self._scrapers]

    @property
    def dormant_scrapers(self) -> list[str]:
        return list(self._dormant)

    def get_status(self) -> dict[str, Any]:
        scraper_stats = {s.platform: s.get_stats() for s in self._scrapers}
        total_posts = sum(s.get_stats()["total_posts"] for s in self._scrapers)
        total_errors = sum(s.get_stats()["error_count"] for s in self._scrapers)
        return {
            "active": self.active_scrapers,
            "dormant": self.dormant_scrapers,
            "active_count": len(self._scrapers),
            "dormant_count": len(self._dormant),
            "total_posts": total_posts,
            "total_errors": total_errors,
            "scrapers": scraper_stats,
        }
