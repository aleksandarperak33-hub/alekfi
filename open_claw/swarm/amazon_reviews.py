"""Amazon product review scraper — consumer sentiment as a leading indicator."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx
from bs4 import BeautifulSoup

from open_claw.utils import RateLimiter
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

_PRODUCT_ASINS = {
    "B0BSHF7WHW": ("Apple iPhone 15 Pro Max", "AAPL"),
    "B0CS5QCLLS": ("Apple MacBook Pro M3", "AAPL"),
    "B0D1XD1ZV3": ("Apple AirPods Pro 2", "AAPL"),
    "B0CDQSMP76": ("Samsung Galaxy S24 Ultra", "005930.KS"),
    "B0C8PSRLGW": ("Sony PlayStation 5 Slim", "SONY"),
    "B0BT2V9TYN": ("Meta Quest 3", "META"),
    "B09V3KXJPB": ("Nintendo Switch OLED", "NTDOY"),
    "B0BDHWDR12": ("Bose QuietComfort Ultra", "BSE.DE"),
    "B0CP7BY7N8": ("Nvidia Shield TV Pro", "NVDA"),
    "B0C5C5WRK1": ("Dyson V15 Detect", "DYSON"),
    "B0BN9CMLFT": ("Peloton Bike+", "PTON"),
    "B0BTXNLG5F": ("KitchenAid Stand Mixer", "WHR"),
    "B08N5WRWNW": ("Tesla Wall Connector", "TSLA"),
    "B0C1H26C46": ("Lululemon Yoga Mat", "LULU"),
    "B0CXLNK4G3": ("Nike Air Max Dn", "NKE"),
}


class AmazonReviewScraper(BaseScraper):
    """Scrapes Amazon product reviews for sentiment signals on major consumer brands."""

    @property
    def platform(self) -> str:
        return "amazon_reviews"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=3, period=60)
        self._seen_ids: set[str] = set()

    def _parse_reviews(self, html: str, asin: str, product: str, ticker: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")
        reviews = soup.select("[data-hook='review']")
        for review in reviews[:10]:
            title_el = review.select_one("[data-hook='review-title'] span:last-child, [data-hook='review-title']")
            title = title_el.get_text(strip=True) if title_el else "No title"
            body_el = review.select_one("[data-hook='review-body'] span")
            body = body_el.get_text(strip=True) if body_el else ""
            rating_el = review.select_one("[data-hook='review-star-rating'] span, .review-rating span")
            rating_text = rating_el.get_text(strip=True) if rating_el else "0"
            rating = float(rating_text.split(" ")[0]) if rating_text else 0.0
            review_id_attr = review.get("id", self._generate_id())

            if review_id_attr in self._seen_ids:
                continue
            self._seen_ids.add(review_id_attr)

            posts.append(self._make_post(
                source_id=f"amz_{review_id_attr}",
                author="Amazon Customer",
                content=f"[{product}] {title} ({rating}/5 stars)\n{body[:2000]}",
                url=f"https://www.amazon.com/dp/{asin}",
                raw_metadata={
                    "asin": asin,
                    "product": product,
                    "ticker": ticker,
                    "rating": rating,
                    "title": title,
                    "review_text": body[:1000],
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for asin, (product, ticker) in _PRODUCT_ASINS.items():
                async with self._rate_limiter:
                    try:
                        url = f"https://www.amazon.com/product-reviews/{asin}/ref=cm_cr_dp_d_show_all_btm?ie=UTF8&reviewerType=all_reviews&sortBy=recent"
                        resp = await client.get(url, headers={
                            "User-Agent": random.choice(_USER_AGENTS),
                            "Accept": "text/html",
                            "Accept-Language": "en-US,en;q=0.9",
                        })
                        if resp.status_code == 200:
                            posts = await asyncio.get_running_loop().run_in_executor(
                                None, self._parse_reviews, resp.text, asin, product, ticker
                            )
                            all_posts.extend(posts)
                        else:
                            logger.debug("[amazon_reviews] %s returned %d", asin, resp.status_code)
                    except Exception:
                        logger.warning("[amazon_reviews] failed for %s", asin, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_AMAZON = [
    ("Apple iPhone 15 Pro Max", "AAPL", 5.0, "Best phone I've ever owned", "Camera is incredible. Battery lasts all day. USB-C finally. Worth every penny."),
    ("Apple iPhone 15 Pro Max", "AAPL", 1.0, "Overheating disaster", "Phone gets burning hot within 10 minutes of use. Apple needs to fix this ASAP. Returning."),
    ("Apple MacBook Pro M3", "AAPL", 5.0, "Developer's dream machine", "Compiles code in half the time. Battery lasts 18 hours. Best laptop ever made."),
    ("Samsung Galaxy S24 Ultra", "005930.KS", 4.0, "Great camera, questionable AI features", "Hardware is top-notch. Galaxy AI feels gimmicky though. S-Pen still useful."),
    ("Samsung Galaxy S24 Ultra", "005930.KS", 2.0, "Quality control issues", "Screen had dead pixels out of box. Second unit had battery drain. Samsung QC is slipping."),
    ("Meta Quest 3", "META", 3.0, "Good hardware, lacking content", "VR experience is smooth but there's not enough compelling content. Gathering dust."),
    ("Meta Quest 3", "META", 1.0, "Privacy nightmare", "Requires Meta account. Tracks everything. Kids got targeted ads after using it. Returned."),
    ("Sony PlayStation 5 Slim", "SONY", 4.0, "Finally got one", "Smaller design is nice. Game library is excellent. PS Plus value is great."),
    ("Tesla Wall Connector", "TSLA", 2.0, "Charging issues", "Keeps disconnecting mid-charge. Customer service is non-existent. $500 for this??"),
    ("Tesla Wall Connector", "TSLA", 5.0, "Perfect home charging solution", "Charges my Model Y to full overnight. Easy install. Sleek design."),
    ("Nike Air Max Dn", "NKE", 2.0, "Quality has gone way downhill", "Sole separated after 2 weeks. This is a $180 shoe! Nike quality is not what it used to be."),
    ("Nike Air Max Dn", "NKE", 4.0, "Comfortable daily wear", "Great cushioning for all-day wear. Looks great. Size runs a bit small."),
    ("Peloton Bike+", "PTON", 1.0, "Expensive clothes rack", "Used it for 3 months then it became a $2500 clothes hanger. Subscription is a rip-off."),
    ("Dyson V15 Detect", "DYSON", 5.0, "Worth every penny", "The laser shows you exactly where the dirt is. Battery improved. Best vacuum I've owned."),
    ("Lululemon Yoga Mat", "LULU", 4.0, "Premium quality", "Best grip of any mat I've tried. Heavy though. You get what you pay for."),
    ("Bose QuietComfort Ultra", "BSE.DE", 5.0, "Noise cancelling king", "Best ANC on the market. Spatial audio is impressive. Comfort is unmatched."),
    ("KitchenAid Stand Mixer", "WHR", 3.0, "Not built like they used to be", "Gear stripped after 6 months. Old ones lasted decades. Moving manufacturing overseas shows."),
    ("Nvidia Shield TV Pro", "NVDA", 4.0, "Best streaming box", "4K upscaling is magic. Runs everything. Plex server built in. Worth the premium."),
    ("Nintendo Switch OLED", "NTDOY", 4.0, "Best handheld console", "OLED screen is gorgeous. Perfect for commuting. Joy-Con drift still an issue though."),
]


class MockAmazonReviewScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "amazon_reviews"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            product, ticker, rating, title, body = random.choice(_MOCK_AMAZON)
            posts.append(self._make_post(
                source_id=f"amz_mock_{self._generate_id()}",
                author="Amazon Customer",
                content=f"[{product}] {title} ({rating}/5 stars)\n{body}",
                url=f"https://www.amazon.com/dp/MOCK{self._generate_id()[:6]}",
                raw_metadata={
                    "product": product,
                    "ticker": ticker,
                    "rating": rating,
                    "title": title,
                    "review_text": body,
                },
            ))
        return posts
