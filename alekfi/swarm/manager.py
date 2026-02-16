"""SwarmManager — orchestrates all Tier-1 scrapers."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from alekfi.config import Settings
from alekfi.queue import RedisQueue
from alekfi.swarm.base import BaseScraper

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
        from alekfi.swarm.reddit import MockRedditScraper
        from alekfi.swarm.news_rss import MockNewsScraper
        from alekfi.swarm.sec_edgar import MockEdgarScraper
        from alekfi.swarm.hackernews import MockHNScraper
        from alekfi.swarm.youtube import MockYouTubeScraper
        from alekfi.swarm.google_trends import MockGoogleTrendsScraper
        from alekfi.swarm.tiktok import MockTikTokScraper
        from alekfi.swarm.instagram import MockInstagramScraper
        from alekfi.swarm.discord_scraper import MockDiscordScraper
        from alekfi.swarm.telegram_scraper import MockTelegramScraper
        from alekfi.swarm.glassdoor import MockGlassdoorScraper
        from alekfi.swarm.amazon_reviews import MockAmazonReviewScraper
        from alekfi.swarm.appstore import MockAppStoreScraper
        from alekfi.swarm.twitter import MockTwitterScraper
        from alekfi.swarm.fourchan import MockFourChanBizScraper
        from alekfi.swarm.github_trending import MockGitHubTrendingScraper
        from alekfi.swarm.linkedin import MockLinkedInScraper
        from alekfi.swarm.patents import MockPatentScraper
        from alekfi.swarm.blind import MockBlindScraper
        from alekfi.swarm.federal_register import MockFederalRegisterScraper
        from alekfi.swarm.fda import MockFDAScraper
        from alekfi.swarm.google_play import MockGooglePlayScraper
        from alekfi.swarm.facebook_sc import MockFacebookScraper

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
            MockFourChanBizScraper(interval),
            MockGitHubTrendingScraper(interval),
            MockLinkedInScraper(interval),
            MockPatentScraper(interval),
            MockBlindScraper(interval),
            MockFederalRegisterScraper(interval),
            MockFDAScraper(interval),
            MockGooglePlayScraper(interval),
            MockFacebookScraper(interval),
        ]
        # Try to add new scrapers (may not exist yet)
        for mod, cls_name, intv in [
            ("alekfi.swarm.fourchan_pol", "MockFourChanPolScraper", interval),
            ("alekfi.swarm.clinical_trials", "MockClinicalTrialsScraper", interval),
            ("alekfi.swarm.earnings_calendar", "MockEarningsCalendarScraper", interval),
            ("alekfi.swarm.reddit_web", "MockRedditWebScraper", interval),
            ("alekfi.swarm.finviz_news", "MockFinvizNewsScraper", interval),
            ("alekfi.swarm.stocktwits", "MockStockTwitsScraper", interval),
            ("alekfi.swarm.options_flow", "MockOptionsFlowScraper", interval),
            ("alekfi.swarm.whale_tracker", "MockWhaleTrackerScraper", interval),
            ("alekfi.swarm.commodities", "MockCommoditiesScraper", interval),
            ("alekfi.swarm.prediction_markets", "MockPredictionMarketsScraper", interval),
        ]:
            try:
                import importlib
                m = importlib.import_module(mod)
                self._scrapers.append(getattr(m, cls_name)(intv))
            except (ImportError, AttributeError):
                pass
        logger.info("Mock mode: registered %d scrapers (all active)", len(self._scrapers))

    def _register_real_scrapers(self) -> None:
        c = self._config

        # ── Always-on scrapers (no API key required) ───────────────────
        from alekfi.swarm.news_rss import NewsRSSScraper
        from alekfi.swarm.hackernews import HackerNewsScraper
        from alekfi.swarm.appstore import AppStoreScraper
        from alekfi.swarm.glassdoor import GlassdoorScraper
        from alekfi.swarm.amazon_reviews import AmazonReviewScraper
        from alekfi.swarm.fourchan import FourChanBizScraper
        from alekfi.swarm.github_trending import GitHubTrendingScraper
        from alekfi.swarm.patents import PatentScraper
        from alekfi.swarm.blind import BlindScraper
        from alekfi.swarm.federal_register import FederalRegisterScraper
        from alekfi.swarm.fda import FDAScraper
        from alekfi.swarm.google_play import GooglePlayScraper

        # SEC EDGAR: prefer v2 (enhanced Form 4 + multi-form), fall back to v1
        try:
            from alekfi.swarm.sec_edgar_v2 import SECEdgarScraperV2
            sec_scraper = SECEdgarScraperV2(60)
            logger.info("Using SEC EDGAR v2 (enhanced multi-form scraper)")
        except ImportError:
            from alekfi.swarm.sec_edgar import SECEdgarScraper
            sec_scraper = SECEdgarScraper(60)
            logger.info("Using SEC EDGAR v1 (fallback)")

        # ── AGGRESSIVE INTERVALS for maximum throughput ────────────────
        self._scrapers.extend([
            NewsRSSScraper(15),              # REAL-TIME: 56 feeds, batched rotation
            sec_scraper,                     # SEC EDGAR: every 45s (Form 4 insider trades)
            HackerNewsScraper(15),           # HN: /newest + /top + /best every 15s
            FourChanBizScraper(60),          # 4chan /biz/: full catalog every 60s
            AppStoreScraper(300),            # app store: every 5min
            GlassdoorScraper(600),           # glassdoor: every 10min
            AmazonReviewScraper(600),        # amazon: every 10min
            GitHubTrendingScraper(300),      # github: every 5min
            PatentScraper(600),              # patents: every 10min
            BlindScraper(600),               # blind: every 10min
            FederalRegisterScraper(600),     # federal register: every 10min
            FDAScraper(600),                 # FDA: every 10min
            GooglePlayScraper(300),          # google play: every 5min
        ])

        # Update SEC EDGAR interval
        if hasattr(sec_scraper, '_interval'):
            sec_scraper._interval = 45

        # ── New always-on scrapers ───────────────────────────────────
        for mod, cls_name, intv, desc in [
            ("alekfi.swarm.fourchan_pol", "FourChanPolScraper", 60, "4chan /pol/ geopolitical"),
            ("alekfi.swarm.clinical_trials", "ClinicalTrialsScraper", 300, "clinical trials"),
            ("alekfi.swarm.earnings_calendar", "EarningsCalendarScraper", 1800, "earnings calendar"),
            ("alekfi.swarm.finviz_news", "FinvizNewsScraper", 30, "Finviz news"),
            ("alekfi.swarm.stocktwits", "StockTwitsScraper", 30, "StockTwits"),
            ("alekfi.swarm.options_flow", "OptionsFlowScraper", 60, "options flow"),
            ("alekfi.swarm.whale_tracker", "WhaleTrackerScraper", 60, "whale tracker"),
            ("alekfi.swarm.commodities", "CommoditiesScraper", 300, "commodities (yfinance)"),
            # ── Phase 2B: High-alpha data sources ────────────────────
            ("alekfi.swarm.congressional_trades", "CongressionalTradesScraper", 3600, "congressional trades"),
            ("alekfi.swarm.sec_13f", "Sec13FScraper", 3600, "SEC 13F filings"),
            ("alekfi.swarm.fomc", "FomcScraper", 1800, "FOMC/Fed press"),
            ("alekfi.swarm.cftc_cot", "CftcCotScraper", 86400, "CFTC COT positions"),
            ("alekfi.swarm.polymarket", "PolymarketScraper", 900, "Polymarket predictions"),
            ("alekfi.swarm.lobbyist", "LobbyistScraper", 3600, "lobbyist disclosures"),
            ("alekfi.swarm.dark_pool", "DarkPoolScraper", 86400, "FINRA dark pool ATS"),
            ("alekfi.swarm.crypto_onchain", "CryptoOnchainScraper", 300, "crypto on-chain whales"),
            ("alekfi.swarm.prediction_markets", "PredictionMarketsScraper", 300, "prediction markets velocity"),
        ]:
            try:
                import importlib
                m = importlib.import_module(mod)
                self._scrapers.append(getattr(m, cls_name)(intv))
                logger.info("%s scraper activated (interval=%ds)", desc, intv)
            except (ImportError, AttributeError) as e:
                logger.warning("%s scraper not available: %s", desc, e)

        # ── Reddit: prefer web scraper (no API key), fall back to PRAW ─
        try:
            from alekfi.swarm.reddit_web import RedditWebScraper
            self._scrapers.append(RedditWebScraper(300))  # reddit RSS: every 5min (rate limit friendly)
            logger.info("Reddit web scraper activated (no API key needed)")
        except ImportError:
            if c.reddit_client_id and c.reddit_client_secret:
                from alekfi.swarm.reddit import RedditScraper
                self._scrapers.append(RedditScraper(30))
            else:
                logger.warning("Reddit scraper SKIPPED — no web scraper and no API keys")
                self._dormant.append("reddit")

        # ── YouTube (needs API key) ────────────────────────────────────
        if c.youtube_api_key:
            from alekfi.swarm.youtube import YouTubeScraper
            self._scrapers.append(YouTubeScraper(180))  # youtube: every 3min
        else:
            logger.warning("YouTube scraper SKIPPED — YOUTUBE_API_KEY not set")
            self._dormant.append("youtube")

        # ── Google Trends (no key but may hit rate limits) ─────────────
        from alekfi.swarm.google_trends import GoogleTrendsScraper
        self._scrapers.append(GoogleTrendsScraper(3600))  # google trends: every 60min (server IP rate-limited by Google)

        # ── Apify quota check (uses limits API, zero cost) ────────────
        _apify_available = False
        if c.apify_api_key:
            try:
                import httpx as _httpx
                _r = _httpx.get(
                    "https://api.apify.com/v2/users/me/limits",
                    params={"token": c.apify_api_key},
                    timeout=15,
                )
                if _r.status_code == 200:
                    _limits = _r.json().get("data", {})
                    _used = _limits.get("current", {}).get("monthlyUsageUsd", 0)
                    _max = _limits.get("limits", {}).get("maxMonthlyUsageUsd", 5)
                    if _used < _max * 0.95:
                        _apify_available = True
                        logger.info("Apify quota OK — $%.2f/$%.2f used (%.0f%%)", _used, _max, _used / _max * 100)
                    else:
                        logger.warning("Apify quota nearly exhausted: $%.2f/$%.2f — disabling", _used, _max)
                else:
                    logger.warning("Apify limits check returned %d — disabling", _r.status_code)
            except Exception as _e:
                logger.warning("Apify API check failed: %s — disabling", _e)
        else:
            logger.warning("APIFY_API_KEY not set — all Apify scrapers skipped")

        # ── TikTok (ScrapeCreators — 1 credit per keyword search)
        if c.scrapecreators_api_key:
            from alekfi.swarm.tiktok_sc import TikTokScrapeCreatorsScraper
            self._scrapers.append(TikTokScrapeCreatorsScraper(600))
            logger.info("TikTok scraper activated (ScrapeCreators, interval=600s)")
        else:
            logger.warning("TikTok scraper DORMANT — no SCRAPECREATORS_API_KEY")
            self._dormant.append("tiktok")

        # ── Instagram (ScrapeCreators — 1 credit per profile scrape)
        if c.scrapecreators_api_key:
            from alekfi.swarm.instagram_sc import InstagramScrapeCreatorsScraper
            self._scrapers.append(InstagramScrapeCreatorsScraper(600))
            logger.info("Instagram scraper activated (ScrapeCreators, interval=600s)")
        else:
            logger.warning("Instagram scraper DORMANT — no SCRAPECREATORS_API_KEY")
            self._dormant.append("instagram")




        # ── LinkedIn (ScrapeCreators — 1 credit per company page)
        if c.scrapecreators_api_key:
            from alekfi.swarm.linkedin_sc import LinkedInScrapeCreatorsScraper
            self._scrapers.append(LinkedInScrapeCreatorsScraper(900))
            logger.info("LinkedIn scraper activated (ScrapeCreators, interval=900s)")
        else:
            logger.warning("LinkedIn scraper DORMANT — no SCRAPECREATORS_API_KEY")
            self._dormant.append("linkedin")

        # ── Facebook (ScrapeCreators — 1 credit per page scrape)
        if c.scrapecreators_api_key:
            from alekfi.swarm.facebook_sc import FacebookScrapeCreatorsScraper
            self._scrapers.append(FacebookScrapeCreatorsScraper(900))
            logger.info("Facebook scraper activated (ScrapeCreators, interval=900s)")
        else:
            self._dormant.append("facebook")

        # ── Discord (needs bot token) ─────────────────────────────────
        if c.discord_bot_token:
            from alekfi.swarm.discord_scraper import DiscordScraper
            self._scrapers.append(DiscordScraper(30))  # discord: every 30s
        else:
            logger.warning("Discord scraper SKIPPED — DISCORD_BOT_TOKEN not set")
            self._dormant.append("discord")

        # ── Telegram (needs api_id + api_hash) ────────────────────────
        if c.telegram_api_id and c.telegram_api_hash:
            from alekfi.swarm.telegram_scraper import TelegramScraper
            self._scrapers.append(TelegramScraper(30))  # telegram: every 30s
        else:
            logger.warning("Telegram scraper SKIPPED — TELEGRAM_API_ID/HASH not set")
            self._dormant.append("telegram")

        # ── Twitter (prefer TwitterAPI.io, fall back to official API v2)
        if c.twitterapiio_api_key:
            from alekfi.swarm.twitter_tapi import TwitterApiIoScraper
            self._scrapers.append(TwitterApiIoScraper(120))
            logger.info("Twitter scraper activated (TwitterAPI.io, interval=120s)")
        elif c.twitter_bearer_token:
            from alekfi.swarm.twitter import TwitterScraper
            self._scrapers.append(TwitterScraper(120))
            logger.info("Twitter scraper activated (API v2, interval=120s)")
        else:
            logger.warning("Twitter scraper DORMANT — no TWITTERAPIIO_API_KEY or TWITTER_BEARER_TOKEN")
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
