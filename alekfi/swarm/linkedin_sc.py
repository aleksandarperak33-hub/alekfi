"""LinkedIn scraper using ScrapeCreators API — 1 credit per company/profile page.

Monitors major company LinkedIn pages for posts about executive changes,
layoffs, hiring surges, restructuring, and other market-moving signals.
Budget: ~1 credit per cycle x every 900s = ~96 credits/day.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.scrapecreators.com"
_COMPANY_ENDPOINT = f"{_BASE_URL}/v1/linkedin/company"

# Major companies to monitor — posts from these pages contain hiring,
# layoff, restructuring, and executive change signals
_COMPANIES: list[tuple[str, str]] = [
    ("Google", "https://www.linkedin.com/company/google"),
    ("Apple", "https://www.linkedin.com/company/apple"),
    ("Microsoft", "https://www.linkedin.com/company/microsoft"),
    ("Amazon", "https://www.linkedin.com/company/amazon"),
    ("Meta", "https://www.linkedin.com/company/meta"),
    ("Tesla", "https://www.linkedin.com/company/tesla-motors"),
    ("Netflix", "https://www.linkedin.com/company/netflix"),
    ("Nvidia", "https://www.linkedin.com/company/nvidia"),
    ("JPMorgan Chase", "https://www.linkedin.com/company/jpmorgan"),
    ("Goldman Sachs", "https://www.linkedin.com/company/goldman-sachs"),
    ("Morgan Stanley", "https://www.linkedin.com/company/morgan-stanley"),
    ("Salesforce", "https://www.linkedin.com/company/salesforce"),
    ("Uber", "https://www.linkedin.com/company/uber-com"),
    ("Airbnb", "https://www.linkedin.com/company/airbnb"),
    ("Stripe", "https://www.linkedin.com/company/stripe"),
    ("Boeing", "https://www.linkedin.com/company/boeing"),
    ("Intel", "https://www.linkedin.com/company/intel-corporation"),
    ("AMD", "https://www.linkedin.com/company/amd"),
    ("BlackRock", "https://www.linkedin.com/company/blackrock"),
    ("Palantir", "https://www.linkedin.com/company/palantir-technologies"),
    ("Coinbase", "https://www.linkedin.com/company/coinbase"),
    ("Shopify", "https://www.linkedin.com/company/shopify"),
    ("Block (Square)", "https://www.linkedin.com/company/joinsquare"),
    ("Rivian", "https://www.linkedin.com/company/rivian"),
    ("SpaceX", "https://www.linkedin.com/company/spacex"),
]


class LinkedInScrapeCreatorsScraper(BaseScraper):
    """LinkedIn company page scraper via ScrapeCreators. 1 credit per company."""

    @property
    def platform(self) -> str:
        return "linkedin"

    def __init__(self, interval: int = 900) -> None:
        super().__init__(interval)
        self._api_key = get_settings().scrapecreators_api_key
        self._cycle_index: int = 0

    async def _scrape_company(
        self,
        client: httpx.AsyncClient,
        company_name: str,
        company_url: str,
    ) -> list[dict[str, Any]]:
        """Scrape a company LinkedIn page for posts. 1 credit."""
        resp = await client.get(
            _COMPANY_ENDPOINT,
            headers={"x-api-key": self._api_key},
            params={"url": company_url},
            timeout=30,
        )
        if resp.status_code != 200:
            logger.warning(
                "[linkedin/sc] company scrape failed (%d) for %s",
                resp.status_code, company_name,
            )
            return []

        data = resp.json()
        if not data.get("success", True):
            return []

        employee_count = data.get("employeeCount", 0)
        industry = data.get("industry", "")
        company_size = data.get("size", "")

        posts: list[dict[str, Any]] = []
        for post in data.get("posts", []):
            text = post.get("text", "") or post.get("description", "") or ""
            if not text:
                continue

            post_url = post.get("url", "") or post.get("link", "")
            post_id = str(post.get("id", "") or self._generate_id())
            likes = post.get("likeCount", 0) or post.get("likes", 0)
            comments = post.get("commentCount", 0) or post.get("comments", 0)

            posts.append(self._make_post(
                source_id=post_id,
                author=company_name,
                content=text[:2000],
                url=post_url or company_url,
                raw_metadata={
                    "company": company_name,
                    "company_url": company_url,
                    "employee_count": employee_count,
                    "industry": industry,
                    "company_size": company_size,
                    "likes": likes,
                    "comments": comments,
                    "published_at": post.get("datePublished") or post.get("postedAt"),
                    "source": "scrapecreators",
                },
                source_published_at=post.get("datePublished") or post.get("postedAt"),
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape 1 company per cycle, rotating through the watchlist."""
        company_name, company_url = _COMPANIES[self._cycle_index % len(_COMPANIES)]
        self._cycle_index += 1

        logger.info("[linkedin/sc] scraping company: %s", company_name)

        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            try:
                posts = await self._scrape_company(client, company_name, company_url)
                all_posts.extend(posts)
            except Exception:
                logger.warning("[linkedin/sc] error for %s", company_name, exc_info=True)

        return all_posts
