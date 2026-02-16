"""Google Play Store scraper — app reviews and ratings for public company apps."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx
from bs4 import BeautifulSoup

from alekfi.utils import RateLimiter
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

# Tracked apps: app_id -> (app_name, company, ticker)
_TRACKED_APPS = {
    # Google
    "com.google.android.apps.maps": ("Google Maps", "Google", "GOOG"),
    "com.google.android.youtube": ("YouTube", "Google", "GOOG"),
    "com.google.android.gm": ("Gmail", "Google", "GOOG"),
    "com.google.android.apps.docs": ("Google Docs", "Google", "GOOG"),
    # Meta
    "com.facebook.katana": ("Facebook", "Meta", "META"),
    "com.instagram.android": ("Instagram", "Meta", "META"),
    "com.whatsapp": ("WhatsApp", "Meta", "META"),
    "com.facebook.orca": ("Messenger", "Meta", "META"),
    # Apple
    "com.apple.android.music": ("Apple Music", "Apple", "AAPL"),
    # Amazon
    "com.amazon.mShop.android.shopping": ("Amazon Shopping", "Amazon", "AMZN"),
    "com.amazon.kindle": ("Kindle", "Amazon", "AMZN"),
    # Microsoft
    "com.microsoft.office.outlook": ("Outlook", "Microsoft", "MSFT"),
    "com.microsoft.teams": ("Microsoft Teams", "Microsoft", "MSFT"),
    "com.microsoft.office.officehubrow": ("Microsoft Office", "Microsoft", "MSFT"),
    # Netflix
    "com.netflix.mediaclient": ("Netflix", "Netflix", "NFLX"),
    # Spotify
    "com.spotify.music": ("Spotify", "Spotify", "SPOT"),
    # Uber
    "com.ubercab": ("Uber", "Uber", "UBER"),
    # Lyft
    "com.lyft.android": ("Lyft", "Lyft", "LYFT"),
    # PayPal
    "com.paypal.android.p2pmobile": ("PayPal", "PayPal", "PYPL"),
    # Venmo
    "com.venmo": ("Venmo", "PayPal", "PYPL"),
    # Cash App
    "com.squareup.cash": ("Cash App", "Block", "SQ"),
    # Robinhood
    "com.robinhood.android": ("Robinhood", "Robinhood", "HOOD"),
    # Coinbase
    "com.coinbase.android": ("Coinbase", "Coinbase", "COIN"),
    # TikTok
    "com.zhiliaoapp.musically": ("TikTok", "ByteDance", "PRIVATE"),
}

_PLAY_STORE_BASE = "https://play.google.com/store/apps/details"


class GooglePlayScraper(BaseScraper):
    """Scrapes Google Play Store reviews for public company apps."""

    @property
    def platform(self) -> str:
        return "google_play"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=5, period=60)
        self._seen_review_ids: set[str] = set()

    def _parse_reviews_page(
        self, html: str, app_id: str, app_name: str, company: str, ticker: str
    ) -> list[dict[str, Any]]:
        """Parse Google Play app page and extract recent reviews."""
        posts: list[dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")

        # Google Play review selectors (adapt to current DOM)
        review_blocks = soup.select(
            "[class*='review'], [class*='RHo1pe'], "
            "[jscontroller*='review'], div[data-reviewid]"
        )

        for block in review_blocks[:15]:
            # Extract reviewer name
            author_el = block.select_one(
                "[class*='author'], [class*='X5PpBb'], "
                "span[class*='bp9Aid'], [data-testid='reviewer-name']"
            )
            author = author_el.get_text(strip=True) if author_el else "Google Play User"

            # Extract rating
            rating_el = block.select_one(
                "[class*='rating'], [aria-label*='star'], "
                "[class*='iXRFPc'], div[role='img']"
            )
            rating_text = ""
            if rating_el:
                aria = rating_el.get("aria-label", "")
                if aria:
                    # e.g. "Rated 4 stars out of five"
                    for part in aria.split():
                        if part.isdigit():
                            rating_text = part
                            break
                if not rating_text:
                    rating_text = rating_el.get_text(strip=True)
            rating = int(rating_text) if rating_text.isdigit() else 0

            # Extract review text
            body_el = block.select_one(
                "[class*='review-body'], [jsname='bN97Pc'], "
                "[class*='h3YV2d'], [data-testid='review-body']"
            )
            body = body_el.get_text(strip=True) if body_el else ""

            # Extract date
            date_el = block.select_one(
                "[class*='review-date'], [class*='bp9Aid'], "
                "[class*='CrZEYe'], [data-testid='review-date']"
            )
            review_date = date_el.get_text(strip=True) if date_el else ""

            # Get review ID from data attribute or generate
            review_id = block.get("data-reviewid", "") or block.get("id", "")
            if not review_id:
                review_id = self._generate_id()

            full_id = f"{app_id}_{review_id}"
            if full_id in self._seen_review_ids:
                continue
            self._seen_review_ids.add(full_id)

            if not body:
                continue

            # Focus on 1-star and 5-star reviews for strongest signals
            if rating not in (0, 1, 2, 5):
                continue

            content = f"[{app_name} - Google Play] ({rating}/5 stars)\n{body}"
            posts.append(self._make_post(
                source_id=f"gplay_{app_id}_{review_id}",
                author=author,
                content=content[:3000],
                url=f"{_PLAY_STORE_BASE}?id={app_id}",
                raw_metadata={
                    "app_name": app_name,
                    "app_id": app_id,
                    "company": company,
                    "ticker": ticker,
                    "rating": rating,
                    "review_date": review_date,
                    "review_id": review_id,
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for app_id, (app_name, company, ticker) in _TRACKED_APPS.items():
                async with self._rate_limiter:
                    try:
                        url = f"{_PLAY_STORE_BASE}?id={app_id}&hl=en_US&gl=US"
                        resp = await client.get(url, headers={
                            "User-Agent": random.choice(_USER_AGENTS),
                            "Accept": "text/html,application/xhtml+xml",
                            "Accept-Language": "en-US,en;q=0.9",
                        })
                        if resp.status_code == 200:
                            posts = await asyncio.get_running_loop().run_in_executor(
                                None, self._parse_reviews_page,
                                resp.text, app_id, app_name, company, ticker,
                            )
                            all_posts.extend(posts)
                        else:
                            logger.debug(
                                "[google_play] %s (%s) returned %d",
                                app_name, app_id, resp.status_code,
                            )
                    except Exception:
                        logger.warning(
                            "[google_play] failed to scrape %s (%s)",
                            app_name, app_id, exc_info=True,
                        )
        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_GOOGLE_PLAY_REVIEWS = [
    ("Facebook", "com.facebook.katana", "Meta", "META", 1, "John D.", "App is spyware", "Battery drain is insane since the last update. App runs 24/7 in the background tracking everything. Uninstalling."),
    ("Facebook", "com.facebook.katana", "Meta", "META", 1, "Sarah M.", "Nothing but ads", "Every other post is a sponsored ad now. Can't even see my friends' posts. Facebook is dead."),
    ("Instagram", "com.instagram.android", "Meta", "META", 1, "Mike R.", "Algorithm is broken", "Only shows me reels I don't care about. Can't see posts from people I follow in chronological order. Terrible."),
    ("Instagram", "com.instagram.android", "Meta", "META", 5, "Emily K.", "Best social media app", "Love the stories, reels, and shopping features. Keeps getting better. The new AI filters are amazing."),
    ("YouTube", "com.google.android.youtube", "Google", "GOOG", 1, "Alex T.", "Too many ads", "15-second unskippable ads every 2 minutes. Premium is $14/month. Google is getting greedy. Switching to alternatives."),
    ("YouTube", "com.google.android.youtube", "Google", "GOOG", 5, "Chris P.", "Can't live without it", "Best video platform period. Premium family plan is worth every penny for ad-free + YouTube Music."),
    ("Gmail", "com.google.android.gm", "Google", "GOOG", 2, "David L.", "Inbox is a mess", "AI categorization keeps putting important emails in spam. Missed a job offer because of this. Fix the algorithm."),
    ("Netflix", "com.netflix.mediaclient", "Netflix", "NFLX", 1, "Lisa W.", "Content quality dropped", "They cancelled all the good shows. Raised prices again. Password sharing crackdown was the last straw. Cancelled."),
    ("Netflix", "com.netflix.mediaclient", "Netflix", "NFLX", 5, "Tom B.", "Best streaming service", "The new ad tier is actually great value. Content library is huge. Streaming quality is best in class."),
    ("Spotify", "com.spotify.music", "Spotify", "SPOT", 2, "Rachel G.", "Podcasts ruined it", "I pay for music, not Joe Rogan. The app is bloated with podcast recommendations. Just let me listen to music."),
    ("Spotify", "com.spotify.music", "Spotify", "SPOT", 5, "James H.", "Discover Weekly is magic", "The recommendation algorithm is incredible. Finding new music I love every week. Wrapped is the best marketing ever."),
    ("Robinhood", "com.robinhood.android", "Robinhood", "HOOD", 1, "Kevin S.", "Lost money due to outage", "App went down during market crash. Couldn't close my positions. Lost $8,000. Class action lawsuit needed."),
    ("Robinhood", "com.robinhood.android", "Robinhood", "HOOD", 5, "Amy Z.", "Perfect for beginners", "Clean interface. Free trades. Crypto and options all in one place. Gold card is amazing."),
    ("Coinbase", "com.coinbase.android", "Coinbase", "COIN", 1, "Mark F.", "Fees are highway robbery", "Charged $15 to buy $200 of Bitcoin. And the spread markup is hidden. Use a real exchange instead."),
    ("Coinbase", "com.coinbase.android", "Coinbase", "COIN", 2, "Nina P.", "Account locked for no reason", "Been a customer for 5 years. Account randomly locked. Support takes weeks to respond. Holding my money hostage."),
    ("Cash App", "com.squareup.cash", "Block", "SQ", 1, "Brian J.", "Scam haven", "Someone hacked my account and stole $500. Customer support is non-existent. No phone number to call. Avoid."),
    ("Cash App", "com.squareup.cash", "Block", "SQ", 5, "Diana C.", "Best P2P app", "Instant transfers, Bitcoin buying, boost discounts. Way better than Venmo. Cash Card is underrated."),
    ("Uber", "com.ubercab", "Uber", "UBER", 1, "Steve W.", "Price gouging is real", "Surge pricing 4x during rain. A 10-minute ride cost $45. Switched to Lyft permanently."),
    ("Uber", "com.ubercab", "Uber", "UBER", 5, "Karen L.", "Reliable and fast", "Always a car nearby. Uber One membership saves money. Uber Eats integration is convenient."),
    ("Microsoft Teams", "com.microsoft.teams", "Microsoft", "MSFT", 1, "Paul R.", "Worst app ever made", "Drains battery, crashes constantly, notifications don't work half the time. Forces itself to start on boot. Bloatware."),
    ("Microsoft Teams", "com.microsoft.teams", "Microsoft", "MSFT", 2, "Sandra G.", "Desktop app is fine, mobile is awful", "Can't share screen properly on mobile. Audio cuts out in meetings. Misses notifications. How is this a Microsoft product?"),
    ("TikTok", "com.zhiliaoapp.musically", "ByteDance", "PRIVATE", 5, "Zoe A.", "Most addictive app ever", "The algorithm knows me better than I know myself. Can't stop scrolling. Content quality is amazing."),
    ("TikTok", "com.zhiliaoapp.musically", "ByteDance", "PRIVATE", 1, "Robert M.", "Privacy nightmare", "Chinese government has access to all your data. Ban can't come soon enough. Deleted after reading the report."),
    ("PayPal", "com.paypal.android.p2pmobile", "PayPal", "PYPL", 2, "Jennifer T.", "Account frozen without explanation", "Had $3,000 frozen for 180 days. No explanation. No appeal process. Switched to everything else."),
]


class MockGooglePlayScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "google_play"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            app_name, app_id, company, ticker, rating, reviewer, title, body = random.choice(
                _MOCK_GOOGLE_PLAY_REVIEWS
            )
            content = f"[{app_name} - Google Play] {title} ({rating}/5 stars)\n{body}"
            review_id = self._generate_id()
            posts.append(self._make_post(
                source_id=f"gplay_{app_id}_{review_id}",
                author=reviewer,
                content=content,
                url=f"https://play.google.com/store/apps/details?id={app_id}",
                raw_metadata={
                    "app_name": app_name,
                    "app_id": app_id,
                    "company": company,
                    "ticker": ticker,
                    "rating": rating,
                    "review_date": "2025-01-15",
                    "review_id": review_id,
                },
            ))
        return posts
