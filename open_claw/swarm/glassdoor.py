"""Glassdoor scraper — employee reviews as leading indicators of company health."""

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
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

_TOP_COMPANIES = {
    "Apple": "https://www.glassdoor.com/Reviews/Apple-Reviews-E1138.htm",
    "Google": "https://www.glassdoor.com/Reviews/Google-Reviews-E9079.htm",
    "Meta": "https://www.glassdoor.com/Reviews/Meta-Reviews-E40772.htm",
    "Amazon": "https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036.htm",
    "Microsoft": "https://www.glassdoor.com/Reviews/Microsoft-Reviews-E1651.htm",
    "Goldman Sachs": "https://www.glassdoor.com/Reviews/Goldman-Sachs-Reviews-E2800.htm",
    "JPMorgan": "https://www.glassdoor.com/Reviews/JPMorgan-Chase-Reviews-E145.htm",
    "Boeing": "https://www.glassdoor.com/Reviews/Boeing-Reviews-E102.htm",
    "Tesla": "https://www.glassdoor.com/Reviews/Tesla-Reviews-E43129.htm",
    "Nvidia": "https://www.glassdoor.com/Reviews/NVIDIA-Reviews-E7633.htm",
    "Netflix": "https://www.glassdoor.com/Reviews/Netflix-Reviews-E11891.htm",
    "Walmart": "https://www.glassdoor.com/Reviews/Walmart-Reviews-E715.htm",
    "Pfizer": "https://www.glassdoor.com/Reviews/Pfizer-Reviews-E525.htm",
    "Disney": "https://www.glassdoor.com/Reviews/Walt-Disney-Company-Reviews-E717.htm",
    "Intel": "https://www.glassdoor.com/Reviews/Intel-Corporation-Reviews-E1519.htm",
}


class GlassdoorScraper(BaseScraper):
    """Scrapes Glassdoor company reviews pages for top companies."""

    @property
    def platform(self) -> str:
        return "glassdoor"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=5, period=60)
        self._seen_review_ids: set[str] = set()

    def _parse_reviews_page(self, html: str, company: str, url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")
        review_blocks = soup.select("[data-test='reviewsList'] li, .review-list .review")
        for block in review_blocks[:10]:
            title_el = block.select_one(".review-details__title, [data-test='review-title']")
            title = title_el.get_text(strip=True) if title_el else "Untitled Review"
            rating_el = block.select_one(".star-rating .rating, [data-test='rating']")
            rating = rating_el.get_text(strip=True) if rating_el else "N/A"
            pros_el = block.select_one("[data-test='pros'], .review-details__pros span")
            pros = pros_el.get_text(strip=True) if pros_el else ""
            cons_el = block.select_one("[data-test='cons'], .review-details__cons span")
            cons = cons_el.get_text(strip=True) if cons_el else ""
            job_el = block.select_one(".review-details__job-title, [data-test='job-title']")
            job_title = job_el.get_text(strip=True) if job_el else "Unknown"

            review_id = f"{company}_{title[:30]}_{rating}"
            if review_id in self._seen_review_ids:
                continue
            self._seen_review_ids.add(review_id)

            content = f"[{company}] {title} (Rating: {rating}/5)\nPros: {pros}\nCons: {cons}"
            posts.append(self._make_post(
                source_id=f"gd_{self._generate_id()}",
                author=f"Glassdoor Employee ({job_title})",
                content=content[:3000],
                url=url,
                raw_metadata={
                    "company": company,
                    "rating": rating,
                    "title": title,
                    "pros": pros[:500],
                    "cons": cons[:500],
                    "job_title": job_title,
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for company, url in _TOP_COMPANIES.items():
                async with self._rate_limiter:
                    try:
                        resp = await client.get(url, headers={
                            "User-Agent": random.choice(_USER_AGENTS),
                            "Accept": "text/html,application/xhtml+xml",
                            "Accept-Language": "en-US,en;q=0.9",
                        })
                        if resp.status_code == 200:
                            posts = await asyncio.get_running_loop().run_in_executor(
                                None, self._parse_reviews_page, resp.text, company, url
                            )
                            all_posts.extend(posts)
                        else:
                            logger.debug("[glassdoor] %s returned %d", company, resp.status_code)
                    except Exception:
                        logger.warning("[glassdoor] failed to scrape %s", company, exc_info=True)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_REVIEWS = [
    ("Apple", "Software Engineer", 4.0, "Amazing products, smart colleagues, great benefits", "Work-life balance is brutal. Secrecy culture can be isolating."),
    ("Apple", "Retail Specialist", 3.0, "Employee discount, flexible schedule", "Low pay for the Bay Area. Limited career growth in retail."),
    ("Google", "SRE", 4.5, "Best perks in tech. Free food, 20% time, incredible engineering culture", "Getting bloated. Too many meetings. Promotion politics."),
    ("Google", "Product Manager", 3.5, "Compensation is top-notch. Brand recognition opens doors", "Layoffs killed morale. Constant reorgs. Projects get cancelled."),
    ("Meta", "ML Engineer", 2.5, "High pay, cutting-edge AI work, fast-paced", "Toxic culture post-layoffs. Surveillance of employees. Metaverse pivot is failing."),
    ("Meta", "Content Moderator", 1.5, "It's a job, I guess", "PTSD-inducing content. Contractor abuse. Mark doesn't care about us."),
    ("Amazon", "SDE II", 3.0, "Learn a lot, high bar, good AWS tech", "PIP culture is real. 60+ hour weeks expected. Stack ranking destroys teams."),
    ("Amazon", "Warehouse Associate", 2.0, "Consistent hours, benefits after 90 days", "Grueling physical work. Bathroom breaks monitored. High injury rates."),
    ("Boeing", "Aerospace Engineer", 2.0, "Great pension, engineering heritage", "Safety culture has collapsed. Management overrides engineering. Whistleblowers punished."),
    ("Boeing", "Quality Inspector", 1.5, "Union protection, decent base pay", "Dangerous shortcuts on 737 MAX. Management ignores quality reports. I fear for passengers."),
    ("Tesla", "Autopilot Engineer", 3.5, "Elon's vision is inspiring. Move fast. Impact is real", "Burnout factory. 80-hour weeks. No work-life balance. Get fired via email."),
    ("Goldman Sachs", "Investment Banking Analyst", 2.5, "Best name on resume. Unmatched deal flow and training", "100-hour weeks aren't a meme — they're real. Mental health crisis among juniors."),
    ("Goldman Sachs", "VP Trading", 4.0, "Compensation is incredible. Intellectual stimulation", "Cutthroat culture. Office politics at senior levels. Layoffs every December."),
    ("Nvidia", "ASIC Engineer", 5.0, "Best stock performance. Jensen is a visionary. AI work is groundbreaking", "Hard to get promoted. Very competitive internally. Silicon Valley COL."),
    ("Nvidia", "Data Scientist", 4.5, "Company is on fire. Stock went 10x. Incredible culture of innovation", "Expectations are sky-high. Imposter syndrome is common. Not diverse enough."),
    ("Pfizer", "Clinical Research Associate", 3.0, "Good work-life balance. Meaningful work on vaccines", "Bureaucratic. Slow decision-making. COVID revenue cliff is scary."),
    ("Intel", "Process Engineer", 2.0, "Legacy and resources are impressive", "Company is lost. Falling behind TSMC. Morale is at rock bottom. Layoffs constant."),
    ("Disney", "Theme Park Cast Member", 2.5, "Magical environment. Free park access", "Pay is below living wage. Housing unaffordable near parks. Benefits cut."),
    ("Walmart", "Store Manager", 3.0, "Stable company. Good supply chain training", "Corporate pressure on margins. Understaffed stores. Burnout."),
    ("Netflix", "Senior SWE", 4.0, "Keeper test keeps team quality high. Freedom and responsibility culture", "No job security. Can be let go anytime. Stress of 'adequate performance = fired'."),
]


class MockGlassdoorScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "glassdoor"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            company, job, rating, pros, cons = random.choice(_MOCK_REVIEWS)
            content = f"[{company}] Review by {job} (Rating: {rating}/5)\nPros: {pros}\nCons: {cons}"
            posts.append(self._make_post(
                source_id=f"gd_mock_{self._generate_id()}",
                author=f"Glassdoor Employee ({job})",
                content=content,
                url=f"https://www.glassdoor.com/Reviews/{company.replace(' ', '-')}-Reviews.htm",
                raw_metadata={
                    "company": company,
                    "rating": rating,
                    "job_title": job,
                    "pros": pros,
                    "cons": cons,
                },
            ))
        return posts
