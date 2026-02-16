"""App Store scraper — rankings and reviews via Apple RSS/iTunes API."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_RSS_BASE = "https://rss.applemarketingtools.com/api/v2/us/apps"
_ITUNES_LOOKUP = "https://itunes.apple.com/lookup"
_ITUNES_REVIEWS = "https://itunes.apple.com/us/rss/customerreviews/id={app_id}/sortBy=mostRecent/json"

_CATEGORIES = {
    "top-free/25/apps.json": "Top Free",
    "top-paid/25/apps.json": "Top Paid",
    "top-grossing/25/apps.json": "Top Grossing",
}

_WATCHED_APPS = {
    "310633997": ("WhatsApp", "META"),
    "333903271": ("Twitter/X", "PRIVATE"),
    "454638411": ("Robinhood", "HOOD"),
    "1477376905": ("Coinbase", "COIN"),
    "544007664": ("YouTube", "GOOG"),
    "835599320": ("Cash App", "SQ"),
    "1094591345": ("Venmo", "PYPL"),
    "1477942215": ("TikTok", "PRIVATE"),
    "284882215": ("Facebook", "META"),
    "389801252": ("Instagram", "META"),
    "1516805437": ("Temu", "PDD"),
    "878672512": ("Shein", "PRIVATE"),
    "297606951": ("Amazon Shopping", "AMZN"),
    "6443640247": ("Threads", "META"),
    "1464446820": ("Revolut", "PRIVATE"),
    "938839644": ("CashApp", "SQ"),
    "284815942": ("PayPal", "PYPL"),
    "1326768853": ("Stripe Dashboard", "PRIVATE"),
    "1087300232": ("Cash App (Square)", "SQ"),
    "1490561916": ("Chime", "PRIVATE"),
    "1191985736": ("SoFi", "SOFI"),
}


class AppStoreScraper(BaseScraper):
    """Monitors App Store rankings and fetches recent app reviews."""

    @property
    def platform(self) -> str:
        return "appstore"

    def __init__(self, interval: int = 1800) -> None:
        super().__init__(interval)
        self._previous_rankings: dict[str, list[str]] = {}

    async def _fetch_rankings(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        for path, label in _CATEGORIES.items():
            try:
                resp = await client.get(f"{_RSS_BASE}/{path}")
                if resp.status_code != 200:
                    continue
                data = resp.json()
                results = data.get("feed", {}).get("results", [])
                current_ids = [r.get("id", "") for r in results]
                prev_ids = self._previous_rankings.get(label, [])

                for rank, app in enumerate(results, 1):
                    app_id = app.get("id", "")
                    app_name = app.get("name", "Unknown")
                    old_rank = (prev_ids.index(app_id) + 1) if app_id in prev_ids else None
                    change = (old_rank - rank) if old_rank else None

                    if change and abs(change) >= 3:
                        direction = "UP" if change > 0 else "DOWN"
                        posts.append(self._make_post(
                            source_id=f"ranking_{label}_{app_id}_{self._generate_id()}",
                            author="app_store",
                            content=f"[App Store {label}] {app_name} moved {direction} {abs(change)} places to #{rank}",
                            url=app.get("url", f"https://apps.apple.com/app/id{app_id}"),
                            raw_metadata={
                                "category": label,
                                "app_name": app_name,
                                "app_id": app_id,
                                "current_rank": rank,
                                "previous_rank": old_rank,
                                "rank_change": change,
                                "artist_name": app.get("artistName", ""),
                            },
                        ))

                self._previous_rankings[label] = current_ids
            except Exception:
                logger.warning("[appstore] failed to fetch %s rankings", label, exc_info=True)
        return posts

    async def _fetch_reviews(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        for app_id, (app_name, ticker) in _WATCHED_APPS.items():
            try:
                url = _ITUNES_REVIEWS.format(app_id=app_id)
                resp = await client.get(url)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                entries = data.get("feed", {}).get("entry", [])
                if isinstance(entries, dict):
                    entries = [entries]
                for entry in entries[:5]:
                    if isinstance(entry, dict) and "content" in entry:
                        review_text = entry.get("content", {}).get("label", "")
                        title = entry.get("title", {}).get("label", "")
                        rating = entry.get("im:rating", {}).get("label", "0")
                        author = entry.get("author", {}).get("name", {}).get("label", "App Store User")
                        review_id = entry.get("id", {}).get("label", self._generate_id())
                        posts.append(self._make_post(
                            source_id=f"apprev_{review_id}",
                            author=author,
                            content=f"[{app_name}] {title} ({rating}/5 stars)\n{review_text[:1500]}",
                            url=f"https://apps.apple.com/app/id{app_id}",
                            raw_metadata={
                                "app_name": app_name,
                                "app_id": app_id,
                                "ticker": ticker,
                                "rating": int(rating),
                                "title": title,
                            },
                        ))
            except Exception:
                logger.debug("[appstore] failed to fetch reviews for %s", app_name, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20) as client:
            rankings = await self._fetch_rankings(client)
            all_posts.extend(rankings)
            reviews = await self._fetch_reviews(client)
            all_posts.extend(reviews)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_RANKING_CHANGES = [
    ("Top Free", "Temu", "PDD", 1, 5, 4),
    ("Top Free", "TikTok", "PRIVATE", 3, 1, -2),
    ("Top Free", "Cash App", "SQ", 8, 15, 7),
    ("Top Free", "Threads", "META", 2, 12, 10),
    ("Top Grossing", "Robinhood", "HOOD", 5, 2, -3),
    ("Top Grossing", "Coinbase", "COIN", 3, 9, 6),
    ("Top Grossing", "YouTube", "GOOG", 1, 1, 0),
]

_MOCK_APP_REVIEWS = [
    ("Robinhood", "HOOD", 1, "Lost everything in the outage", "App crashed during market open. Couldn't sell my puts. Lost $15K. Lawsuit incoming."),
    ("Robinhood", "HOOD", 5, "Best trading app", "Simple interface. Free options trading. Gold membership is worth it."),
    ("Coinbase", "COIN", 2, "Fees are ridiculous", "They charge 1.5% per trade. Moved to Binance. Much cheaper."),
    ("TikTok", "PRIVATE", 1, "App is getting worse", "Algorithm only shows me ads now. Content quality has dropped significantly."),
    ("Temu", "PDD", 1, "Scam products", "Everything I ordered was counterfeit junk. Took 3 weeks to arrive. Disputing charges."),
    ("Temu", "PDD", 4, "Incredible deals", "Got a $200 jacket for $20. Quality is decent. Shipping improved a lot."),
    ("Cash App", "SQ", 3, "Bitcoin buying is nice", "Easy to buy BTC. But customer service is non-existent when things go wrong."),
    ("Instagram", "META", 2, "Too many ads and reels", "Used to be a photo app. Now it's TikTok clone full of sponsored content."),
    ("WhatsApp", "META", 4, "Reliable messaging", "Works everywhere. Group features are good. Privacy concerns though."),
    ("Revolut", "PRIVATE", 5, "Best banking app", "Multi-currency, crypto, stocks all in one. Traditional banks are dinosaurs."),
    ("PayPal", "PYPL", 2, "Account frozen", "Froze my account for 180 days with $2K in it. No explanation. Terrible company."),
    ("SoFi", "SOFI", 5, "All-in-one finance app", "Banking, investing, loans all in one place. 4.6% APY on savings. Love it."),
    ("SoFi", "SOFI", 1, "Loan servicing is terrible", "Student loan refinance was a nightmare. Customer service doesn't exist."),
    ("Chime", "PRIVATE", 4, "Best no-fee banking", "No overdraft fees. Early direct deposit. SpotMe feature is clutch."),
    ("Chime", "PRIVATE", 1, "Direct deposit failed", "My paycheck disappeared for 3 days. No phone support. Had to borrow money for rent."),
]


class MockAppStoreScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "appstore"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        ranking_count = random.randint(2, 5)
        for category, app, ticker, new_rank, old_rank, change in random.sample(_MOCK_RANKING_CHANGES, min(ranking_count, len(_MOCK_RANKING_CHANGES))):
            direction = "UP" if change > 0 else "DOWN"
            posts.append(self._make_post(
                source_id=f"ranking_mock_{app}_{self._generate_id()}",
                author="app_store",
                content=f"[App Store {category}] {app} moved {direction} {abs(change)} places to #{new_rank}",
                url=f"https://apps.apple.com/app/{app.lower().replace(' ', '-')}",
                raw_metadata={
                    "category": category,
                    "app_name": app,
                    "ticker": ticker,
                    "current_rank": new_rank,
                    "previous_rank": old_rank,
                    "rank_change": change,
                },
            ))

        review_count = random.randint(3, 10)
        for _ in range(review_count):
            app, ticker, rating, title, body = random.choice(_MOCK_APP_REVIEWS)
            posts.append(self._make_post(
                source_id=f"apprev_mock_{self._generate_id()}",
                author="App Store User",
                content=f"[{app}] {title} ({rating}/5 stars)\n{body}",
                url=f"https://apps.apple.com/app/{app.lower().replace(' ', '-')}",
                raw_metadata={
                    "app_name": app,
                    "ticker": ticker,
                    "rating": rating,
                    "title": title,
                },
            ))
        return posts
