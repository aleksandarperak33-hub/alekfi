"""Blind (Teamblind.com) scraper — anonymous employee posts from FAANG/tech companies."""

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

_COMPANY_FEEDS = {
    "Apple": "https://www.teamblind.com/company/Apple/posts",
    "Google": "https://www.teamblind.com/company/Google/posts",
    "Meta": "https://www.teamblind.com/company/Meta/posts",
    "Amazon": "https://www.teamblind.com/company/Amazon/posts",
    "Microsoft": "https://www.teamblind.com/company/Microsoft/posts",
    "Netflix": "https://www.teamblind.com/company/Netflix/posts",
    "Nvidia": "https://www.teamblind.com/company/NVIDIA/posts",
    "Tesla": "https://www.teamblind.com/company/Tesla/posts",
    "Salesforce": "https://www.teamblind.com/company/Salesforce/posts",
    "Adobe": "https://www.teamblind.com/company/Adobe/posts",
    "Uber": "https://www.teamblind.com/company/Uber/posts",
    "Lyft": "https://www.teamblind.com/company/Lyft/posts",
    "Airbnb": "https://www.teamblind.com/company/Airbnb/posts",
    "Snap": "https://www.teamblind.com/company/Snap/posts",
    "Twitter/X": "https://www.teamblind.com/company/Twitter/posts",
    "Oracle": "https://www.teamblind.com/company/Oracle/posts",
    "Intel": "https://www.teamblind.com/company/Intel/posts",
    "AMD": "https://www.teamblind.com/company/AMD/posts",
    "Qualcomm": "https://www.teamblind.com/company/Qualcomm/posts",
    "Broadcom": "https://www.teamblind.com/company/Broadcom/posts",
}

_KEYWORDS = [
    "layoffs", "restructuring", "acquisition", "new CEO", "toxic",
    "leaving", "PIP", "reorg", "RIF", "hiring freeze",
    "return to office", "stock", "RSU", "compensation",
]


def _match_keywords(text: str) -> list[str]:
    """Return list of matching keywords found in the text (case-insensitive)."""
    lower_text = text.lower()
    return [kw for kw in _KEYWORDS if kw.lower() in lower_text]


class BlindScraper(BaseScraper):
    """Scrapes Teamblind.com company feeds for anonymous employee posts."""

    @property
    def platform(self) -> str:
        return "blind"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=5, period=60)
        self._seen_post_ids: set[str] = set()

    def _parse_company_page(self, html: str, company: str, url: str) -> list[dict[str, Any]]:
        """Parse Blind company feed page and extract posts with keyword filtering."""
        posts: list[dict[str, Any]] = []
        soup = BeautifulSoup(html, "lxml")

        # Blind post list selectors (adapt to current DOM structure)
        post_blocks = soup.select(
            "article, .post-list-item, [class*='PostList'] > div, "
            ".content-list li, [data-testid='post-item']"
        )

        for block in post_blocks[:20]:
            # Extract title
            title_el = block.select_one(
                "h2, h3, .post-title, [class*='Title'], "
                "[data-testid='post-title'], a[class*='title']"
            )
            title = title_el.get_text(strip=True) if title_el else ""

            # Extract body/preview
            body_el = block.select_one(
                "p, .post-body, .post-preview, [class*='Body'], "
                "[class*='Preview'], [data-testid='post-body']"
            )
            body = body_el.get_text(strip=True) if body_el else ""

            # Extract link
            link_el = block.select_one("a[href*='/post/']") or (title_el if title_el and title_el.name == "a" else None)
            post_url = ""
            if link_el and link_el.get("href"):
                href = link_el["href"]
                post_url = href if href.startswith("http") else f"https://www.teamblind.com{href}"

            # Extract upvote/comment count if available
            votes_el = block.select_one("[class*='vote'], [class*='like'], [data-testid='upvote-count']")
            votes = votes_el.get_text(strip=True) if votes_el else "0"
            comments_el = block.select_one("[class*='comment-count'], [class*='Comment'], [data-testid='comment-count']")
            comment_count = comments_el.get_text(strip=True) if comments_el else "0"

            full_text = f"{title} {body}"
            if not full_text.strip():
                continue

            matched_kw = _match_keywords(full_text)
            if not matched_kw:
                continue

            post_id = f"{company}_{title[:40]}_{body[:20]}"
            if post_id in self._seen_post_ids:
                continue
            self._seen_post_ids.add(post_id)

            content = f"[{company} - Blind] {title}\n{body}"
            posts.append(self._make_post(
                source_id=f"blind_{self._generate_id()}",
                author="anonymous",
                content=content[:3000],
                url=post_url or url,
                raw_metadata={
                    "company": company,
                    "title": title[:500],
                    "body_preview": body[:500],
                    "matched_keywords": matched_kw,
                    "votes": votes,
                    "comment_count": comment_count,
                },
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            for company, url in _COMPANY_FEEDS.items():
                async with self._rate_limiter:
                    try:
                        resp = await client.get(url, headers={
                            "User-Agent": random.choice(_USER_AGENTS),
                            "Accept": "text/html,application/xhtml+xml",
                            "Accept-Language": "en-US,en;q=0.9",
                        })
                        if resp.status_code == 200:
                            posts = await asyncio.get_running_loop().run_in_executor(
                                None, self._parse_company_page, resp.text, company, url
                            )
                            all_posts.extend(posts)
                        else:
                            logger.debug("[blind] %s returned %d", company, resp.status_code)
                    except Exception:
                        logger.warning("[blind] failed to scrape %s", company, exc_info=True)
        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_BLIND_POSTS = [
    ("Apple", "PIP season is here", "Manager told 3 people on my team they're on PIP. All senior engineers. Morale is destroyed. Anyone else seeing this?", ["PIP"]),
    ("Apple", "Return to office mandate killing innovation", "Forcing everyone back 5 days. Best engineers are leaving for remote-first companies. Tim Cook is out of touch.", ["return to office", "leaving"]),
    ("Google", "Massive layoffs coming Q2", "My VP told us in a private meeting to 'prepare for changes'. Usually means 15-20% cut in our org. Start interviewing now.", ["layoffs"]),
    ("Google", "RSU refresh is pathetic", "Got my annual refresh — 40% less than last year for exceeding expectations. Total comp down 25% from when I joined. Time to leave.", ["RSU", "compensation", "leaving"]),
    ("Meta", "Reorg #47 this year", "Another reorg announced today. My skip-level changed for the 3rd time in 6 months. Nobody knows who reports to whom. It's chaos.", ["reorg"]),
    ("Meta", "Toxic culture under new Reality Labs lead", "New VP is a micromanager who sends Slack messages at 2am and expects immediate responses. Three people on my team resigned this month.", ["toxic", "leaving"]),
    ("Amazon", "PIP factories are real", "Just got put on Pivot (Amazon PIP). Never got negative feedback before. Manager needed to meet his URA quota. This company is broken.", ["PIP"]),
    ("Amazon", "Hiring freeze across AWS", "All open reqs in my org frozen. Backfills not approved. They're saying 'efficiency' but it means layoffs are next.", ["hiring freeze", "layoffs"]),
    ("Microsoft", "Acquisition rumors — who are we buying?", "Leadership briefing mentioned a 'transformative acquisition' in the AI space. NDA but it's huge. Anyone else heard this?", ["acquisition"]),
    ("Microsoft", "Stock price concerns after earnings", "Missed cloud revenue estimates. Stock down 5% AH. My RSUs are underwater from grant price. Thinking about jumping ship.", ["stock", "RSU", "leaving"]),
    ("Netflix", "Layoffs in gaming division", "Entire mobile gaming team (60+ people) got walked out today. No warning. No severance negotiation. Keeper test is ruthless.", ["layoffs"]),
    ("Netflix", "New CEO direction is confusing", "Since the co-CEO transition, strategy changes every quarter. Ad tier, gaming, live sports — we're spreading too thin.", ["new CEO"]),
    ("Nvidia", "Compensation is insane right now", "TC went from $400K to $1.2M in 2 years just from stock appreciation. But the expectations are brutal. 70hr weeks minimum.", ["compensation", "stock"]),
    ("Tesla", "RIF happening next week", "Elon sent company-wide email about 'right-sizing'. My director said expect 10% reduction in force across all orgs.", ["RIF", "layoffs"]),
    ("Tesla", "Toxic management culture", "Senior director screams at engineers in meetings. HR does nothing. Multiple people filed complaints — all ignored.", ["toxic"]),
    ("Salesforce", "Restructuring Slack teams", "Post-acquisition integration is a mess. Slack teams being restructured again. Half the original Slack engineers have left.", ["restructuring", "leaving"]),
    ("Intel", "Morale at all-time low", "Stock is down 50% from peak. Layoffs every quarter. Pat left. New CEO has no plan. TSMC is eating our lunch.", ["layoffs", "stock", "new CEO"]),
    ("Uber", "Return to office backlash", "Dara announced 3 days in office minimum. Engineers are furious. Top performers already interviewing at remote-first companies.", ["return to office", "leaving"]),
    ("Lyft", "Another round of layoffs", "Just got the calendar invite for 'Team Restructuring Discussion'. Last time this happened, 30% of my org was let go.", ["layoffs", "restructuring"]),
    ("Snap", "Hiring freeze extended indefinitely", "All hiring frozen since Q3. No backfills. Remaining engineers doing 2x the work. Evan doesn't care about employee wellbeing.", ["hiring freeze"]),
]


class MockBlindScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "blind"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            company, title, body, keywords = random.choice(_MOCK_BLIND_POSTS)
            content = f"[{company} - Blind] {title}\n{body}"
            posts.append(self._make_post(
                source_id=f"blind_mock_{self._generate_id()}",
                author="anonymous",
                content=content,
                url=f"https://www.teamblind.com/post/{title.lower().replace(' ', '-')}-{self._generate_id()[:8]}",
                raw_metadata={
                    "company": company,
                    "title": title,
                    "body_preview": body[:500],
                    "matched_keywords": keywords,
                    "votes": str(random.randint(5, 500)),
                    "comment_count": str(random.randint(3, 150)),
                },
            ))
        return posts
