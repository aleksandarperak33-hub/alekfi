"""Glassdoor scraper — employee reviews as leading indicators of company health."""

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

_KEYWORDS = [
    "layoffs", "restructuring", "acquisition", "new CEO",
    "toxic", "leaving", "PIP",
]

_TOP_COMPANIES = {
    "Apple": "https://www.glassdoor.com/Reviews/Apple-Reviews-E1138.htm",
    "Google": "https://www.glassdoor.com/Reviews/Google-Reviews-E9079.htm",
    "Meta": "https://www.glassdoor.com/Reviews/Meta-Reviews-E40772.htm",
    "Amazon": "https://www.glassdoor.com/Reviews/Amazon-Reviews-E6036.htm",
    "Microsoft": "https://www.glassdoor.com/Reviews/Microsoft-Reviews-E1651.htm",
    "Goldman Sachs": "https://www.glassdoor.com/Reviews/Goldman-Sachs-Reviews-E2800.htm",
    "JPMorgan": "https://www.glassdoor.com/Reviews/JPMorgan-Chase-Reviews-E145.htm",
    "Morgan Stanley": "https://www.glassdoor.com/Reviews/Morgan-Stanley-Reviews-E2282.htm",
    "Citadel": "https://www.glassdoor.com/Reviews/Citadel-Reviews-E14937.htm",
    "Bridgewater": "https://www.glassdoor.com/Reviews/Bridgewater-Associates-Reviews-E14849.htm",
    "Boeing": "https://www.glassdoor.com/Reviews/Boeing-Reviews-E102.htm",
    "Tesla": "https://www.glassdoor.com/Reviews/Tesla-Reviews-E43129.htm",
    "Nvidia": "https://www.glassdoor.com/Reviews/NVIDIA-Reviews-E7633.htm",
    "Netflix": "https://www.glassdoor.com/Reviews/Netflix-Reviews-E11891.htm",
    "Walmart": "https://www.glassdoor.com/Reviews/Walmart-Reviews-E715.htm",
    "Pfizer": "https://www.glassdoor.com/Reviews/Pfizer-Reviews-E525.htm",
    "Disney": "https://www.glassdoor.com/Reviews/Walt-Disney-Company-Reviews-E717.htm",
    "Intel": "https://www.glassdoor.com/Reviews/Intel-Corporation-Reviews-E1519.htm",
    "Palantir": "https://www.glassdoor.com/Reviews/Palantir-Technologies-Reviews-E236375.htm",
    "Snowflake": "https://www.glassdoor.com/Reviews/Snowflake-Reviews-E1064452.htm",
    "CrowdStrike": "https://www.glassdoor.com/Reviews/CrowdStrike-Reviews-E795976.htm",
    "Datadog": "https://www.glassdoor.com/Reviews/Datadog-Reviews-E1064888.htm",
}


def _match_keywords(text: str) -> list[str]:
    """Return matching keywords found in the text (case-insensitive)."""
    lower_text = text.lower()
    return [kw for kw in _KEYWORDS if kw.lower() in lower_text]


class GlassdoorScraper(BaseScraper):
    """Scrapes Glassdoor company reviews pages for top companies."""

    @property
    def platform(self) -> str:
        return "glassdoor"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=5, period=60)
        self._seen_review_ids: set[str] = set()
        self._review_counts: dict[str, int] = {}

    def _parse_reviews_page(self, html: str, company: str, url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")
        review_blocks = soup.select("[data-test='reviewsList'] li, .review-list .review")

        # Track review velocity per company
        current_count = len(review_blocks)
        prev_count = self._review_counts.get(company, 0)
        self._review_counts[company] = current_count

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

            full_text = f"{title} {pros} {cons}"
            matched_kw = _match_keywords(full_text)

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
                    "matched_keywords": matched_kw,
                    "review_velocity": {
                        "current_page_count": current_count,
                        "previous_page_count": prev_count,
                    },
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
    ("Apple", "Software Engineer", 4.0, "Amazing products, smart colleagues, great benefits", "Work-life balance is brutal. Secrecy culture can be isolating.", []),
    ("Apple", "Retail Specialist", 3.0, "Employee discount, flexible schedule", "Low pay for the Bay Area. Limited career growth in retail.", []),
    ("Google", "SRE", 4.5, "Best perks in tech. Free food, 20% time, incredible engineering culture", "Getting bloated. Too many meetings. Promotion politics.", []),
    ("Google", "Product Manager", 3.5, "Compensation is top-notch. Brand recognition opens doors", "Layoffs killed morale. Constant reorgs. Projects get cancelled.", ["layoffs", "restructuring"]),
    ("Meta", "ML Engineer", 2.5, "High pay, cutting-edge AI work, fast-paced", "Toxic culture post-layoffs. Surveillance of employees. Metaverse pivot is failing.", ["toxic", "layoffs"]),
    ("Meta", "Content Moderator", 1.5, "It's a job, I guess", "PTSD-inducing content. Contractor abuse. Mark doesn't care about us.", ["toxic"]),
    ("Amazon", "SDE II", 3.0, "Learn a lot, high bar, good AWS tech", "PIP culture is real. 60+ hour weeks expected. Stack ranking destroys teams.", ["PIP"]),
    ("Amazon", "Warehouse Associate", 2.0, "Consistent hours, benefits after 90 days", "Grueling physical work. Bathroom breaks monitored. High injury rates.", []),
    ("Boeing", "Aerospace Engineer", 2.0, "Great pension, engineering heritage", "Safety culture has collapsed. Management overrides engineering. Whistleblowers punished.", []),
    ("Boeing", "Quality Inspector", 1.5, "Union protection, decent base pay", "Dangerous shortcuts on 737 MAX. Management ignores quality reports. I fear for passengers.", []),
    ("Tesla", "Autopilot Engineer", 3.5, "Elon's vision is inspiring. Move fast. Impact is real", "Burnout factory. 80-hour weeks. No work-life balance. Get fired via email.", ["leaving"]),
    ("Goldman Sachs", "Investment Banking Analyst", 2.5, "Best name on resume. Unmatched deal flow and training", "100-hour weeks aren't a meme — they're real. Mental health crisis among juniors.", ["leaving"]),
    ("Goldman Sachs", "VP Trading", 4.0, "Compensation is incredible. Intellectual stimulation", "Cutthroat culture. Office politics at senior levels. Layoffs every December.", ["layoffs"]),
    ("Nvidia", "ASIC Engineer", 5.0, "Best stock performance. Jensen is a visionary. AI work is groundbreaking", "Hard to get promoted. Very competitive internally. Silicon Valley COL.", []),
    ("Nvidia", "Data Scientist", 4.5, "Company is on fire. Stock went 10x. Incredible culture of innovation", "Expectations are sky-high. Imposter syndrome is common. Not diverse enough.", []),
    ("Pfizer", "Clinical Research Associate", 3.0, "Good work-life balance. Meaningful work on vaccines", "Bureaucratic. Slow decision-making. COVID revenue cliff is scary.", []),
    ("Intel", "Process Engineer", 2.0, "Legacy and resources are impressive", "Company is lost. Falling behind TSMC. Morale is at rock bottom. Layoffs constant.", ["layoffs"]),
    ("Disney", "Theme Park Cast Member", 2.5, "Magical environment. Free park access", "Pay is below living wage. Housing unaffordable near parks. Benefits cut.", []),
    ("Walmart", "Store Manager", 3.0, "Stable company. Good supply chain training", "Corporate pressure on margins. Understaffed stores. Burnout.", []),
    ("Netflix", "Senior SWE", 4.0, "Keeper test keeps team quality high. Freedom and responsibility culture", "No job security. Can be let go anytime. Stress of 'adequate performance = fired'.", []),
    ("Morgan Stanley", "Associate", 3.0, "Prestigious brand. Good training program", "Work-life balance is non-existent. Layoffs in tech division are constant. Leaving for buy-side.", ["layoffs", "leaving"]),
    ("JPMorgan", "VP Technology", 3.5, "Massive resources. Good benefits", "Bureaucratic nightmare. Restructuring every quarter. New CEO changes everything.", ["restructuring", "new CEO"]),
    ("Citadel", "Quantitative Researcher", 4.0, "Compensation is unmatched. Smartest people in the room", "Pressure is extreme. PIP culture for bottom performers. Toxic competitiveness.", ["PIP", "toxic"]),
    ("Bridgewater", "Investment Associate", 2.0, "Radical transparency is interesting concept", "Toxic radical transparency. Constant dot ratings. People leaving in droves.", ["toxic", "leaving"]),
    ("Palantir", "Forward Deployed Engineer", 3.5, "Interesting government contracts. Mission-driven work", "Cult-like culture. Acquisition of smaller competitors squeezes teams.", ["acquisition"]),
    ("Snowflake", "Senior Engineer", 4.0, "Great tech. Cloud data warehousing leader", "Post-IPO restructuring. New CEO has different vision. Engineering layoffs.", ["restructuring", "new CEO", "layoffs"]),
    ("CrowdStrike", "Threat Analyst", 4.5, "Cybersecurity market is booming. Great leadership", "After the big outage incident, morale dropped. Restructuring security processes.", ["restructuring"]),
    ("Datadog", "SRE", 4.0, "Best observability platform. Strong engineering culture", "Acquisition strategy is aggressive. Integration of acquired teams is messy.", ["acquisition"]),
]


class MockGlassdoorScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "glassdoor"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            company, job, rating, pros, cons, keywords = random.choice(_MOCK_REVIEWS)
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
                    "matched_keywords": keywords,
                    "review_velocity": {
                        "current_page_count": random.randint(8, 15),
                        "previous_page_count": random.randint(5, 12),
                    },
                },
            ))
        return posts
