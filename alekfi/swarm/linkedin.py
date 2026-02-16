"""LinkedIn scraper — uses Apify API to scrape job postings and executive changes."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_APIFY_API = "https://api.apify.com/v2"
_LINKEDIN_ACTOR = "anchor~linkedin-jobs-scraper"

_SEARCH_QUERIES = [
    "CEO appointed",
    "CFO hired",
    "CTO appointed",
    "executive layoffs",
    "mass layoffs tech",
    "hiring surge",
    "VP of Engineering hired",
    "Chief AI Officer",
    "restructuring announcement",
    "head of product hired",
    # New queries
    "restructuring",
    "acquisition announcement",
    "IPO filing",
    "board of directors change",
    "supply chain disruption",
    "plant closure",
    "new product launch",
]

_MAJOR_COMPANIES = [
    "Google", "Apple", "Microsoft", "Amazon", "Meta",
    "Tesla", "Netflix", "Nvidia", "Salesforce", "Goldman Sachs",
    "JPMorgan", "Morgan Stanley", "Uber", "Airbnb", "Stripe",
]


class LinkedInScraper(BaseScraper):
    """Runs an Apify LinkedIn scraper actor and collects job/executive results."""

    @property
    def platform(self) -> str:
        return "linkedin"

    def __init__(self, interval: int = 600) -> None:
        super().__init__(interval)
        self._api_key = get_settings().apify_api_key

    async def _run_actor(self, client: httpx.AsyncClient, query: str) -> list[dict[str, Any]]:
        run_resp = await client.post(
            f"{_APIFY_API}/acts/{_LINKEDIN_ACTOR}/runs",
            params={"token": self._api_key},
            json={
                "searchUrl": f"https://www.linkedin.com/jobs/search/?keywords={query}",
                "maxResults": 15,
                "scrapeCompany": True,
            },
            timeout=30,
        )
        if run_resp.status_code not in (200, 201):
            logger.warning("[linkedin] actor start failed (%d) for '%s'", run_resp.status_code, query)
            return []

        run_data = run_resp.json().get("data", {})
        run_id = run_data.get("id")
        if not run_id:
            return []

        for _ in range(30):
            await asyncio.sleep(5)
            status_resp = await client.get(
                f"{_APIFY_API}/actor-runs/{run_id}",
                params={"token": self._api_key},
                timeout=15,
            )
            if status_resp.status_code != 200:
                continue
            status = status_resp.json().get("data", {}).get("status")
            if status == "SUCCEEDED":
                break
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                logger.warning("[linkedin] actor run %s ended with %s", run_id, status)
                return []
        else:
            logger.warning("[linkedin] actor run %s timed out waiting", run_id)
            return []

        dataset_id = run_data.get("defaultDatasetId")
        if not dataset_id:
            return []
        items_resp = await client.get(
            f"{_APIFY_API}/datasets/{dataset_id}/items",
            params={"token": self._api_key, "format": "json"},
            timeout=20,
        )
        if items_resp.status_code != 200:
            return []

        posts: list[dict[str, Any]] = []
        for item in items_resp.json():
            job_id = str(item.get("id", self._generate_id()))
            title = item.get("title", "") or ""
            company = item.get("companyName", "") or item.get("company", "")
            location = item.get("location", "") or ""
            description = item.get("description", "") or ""
            content = f"{title} at {company}"
            if location:
                content += f" ({location})"
            if description:
                content += f"\n\n{description[:1500]}"
            posts.append(self._make_post(
                source_id=job_id,
                author=company or "unknown",
                content=content[:2000],
                url=item.get("url") or item.get("link") or f"https://www.linkedin.com/jobs/view/{job_id}/",
                raw_metadata={
                    "search_query": query,
                    "title": title,
                    "company": company,
                    "location": location,
                    "employment_type": item.get("employmentType", ""),
                    "seniority_level": item.get("seniorityLevel", ""),
                    "posted_at": item.get("postedAt", ""),
                    "applicants": item.get("applicantsCount", 0),
                },
                source_published_at=item.get("postedAt"),
            ))
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        if not self._api_key:
            logger.debug("[linkedin] skipping — no apify_api_key configured")
            return []
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient() as client:
            for query in _SEARCH_QUERIES[:5]:
                try:
                    posts = await self._run_actor(client, query)
                    all_posts.extend(posts)
                except Exception:
                    logger.warning("[linkedin] error scraping '%s'", query, exc_info=True)
        return all_posts


# -- Mock ------------------------------------------------------------------

_MOCK_LINKEDIN_POSTS = [
    (
        "Google",
        "Google appoints new Chief AI Officer amid restructuring push. Former DeepMind VP Lila Patel to lead unified AI division. "
        "The move signals Google's commitment to consolidating its AI efforts under a single executive. "
        "#google #AI #executive #leadership",
        "Chief AI Officer",
        "Mountain View, CA",
        1_240,
    ),
    (
        "Meta",
        "Meta announces 4,000 layoffs across Reality Labs and Instagram divisions. "
        "CEO Zuckerberg says company is 'refocusing on core AI investments'. Third round of cuts in 18 months. "
        "#meta #layoffs #tech",
        "Restructuring Announcement",
        "Menlo Park, CA",
        3_870,
    ),
    (
        "JPMorgan Chase",
        "JPMorgan hiring 2,000 AI and machine learning engineers over the next year. "
        "CFO states 'We will spend $2B on AI infrastructure in 2026.' Largest tech hiring push in the bank's history. "
        "#jpmorgan #hiring #AI #fintech",
        "AI Engineering Hiring Surge",
        "New York, NY",
        2_150,
    ),
    (
        "Tesla",
        "Tesla names new CFO Vaibhav Taneja as outgoing CFO Zachary Kirkhorn departs. "
        "Market reacts with 3% dip. Analysts question timing ahead of Q3 earnings. "
        "#tesla #CFO #executive",
        "CFO Transition",
        "Austin, TX",
        5_400,
    ),
    (
        "Amazon",
        "Amazon Web Services lays off 1,200 in sales and marketing division. "
        "Company pivoting resources to Bedrock AI platform and custom chip development. "
        "#amazon #AWS #layoffs #cloud",
        "Division Layoffs",
        "Seattle, WA",
        2_890,
    ),
    (
        "Nvidia",
        "Nvidia hires former Intel exec Pat Gelsinger as SVP of Enterprise AI Solutions. "
        "Move seen as aggressive play into enterprise GPU-as-a-service market. "
        "#nvidia #hiring #executive #AI",
        "SVP of Enterprise AI",
        "Santa Clara, CA",
        4_100,
    ),
    (
        "Goldman Sachs",
        "Goldman Sachs cuts 300 managing directors in annual performance review. "
        "Bank simultaneously opens 500 junior analyst positions in technology division. "
        "#goldmansachs #wallstreet #layoffs #hiring",
        "Annual Review Cuts",
        "New York, NY",
        1_780,
    ),
    (
        "Microsoft",
        "Microsoft appoints new Head of AI Safety after public pressure over Copilot issues. "
        "Dr. Sarah Chen joins from Anthropic to lead 200-person responsible AI team. "
        "#microsoft #AI #safety #executive",
        "Head of AI Safety",
        "Redmond, WA",
        3_200,
    ),
    (
        "Apple",
        "Apple quietly hiring 500+ engineers for 'secret' robotics division. "
        "Postings mention autonomous systems, humanoid robotics, and home AI. "
        "Could this be the next big Apple product line? "
        "#apple #robotics #hiring #tech",
        "Robotics Division Hiring",
        "Cupertino, CA",
        6_300,
    ),
    (
        "Stripe",
        "Stripe promotes COO Claire Hughes Johnson to co-CEO alongside Patrick Collison. "
        "Company reportedly eyeing 2026 IPO at $100B+ valuation. "
        "#stripe #fintech #executive #IPO",
        "Co-CEO Appointment",
        "San Francisco, CA",
        2_450,
    ),
    (
        "Salesforce",
        "Salesforce eliminates 700 roles in Slack and Tableau teams following integration completion. "
        "CEO Marc Benioff promises 'no further cuts this fiscal year.' "
        "#salesforce #layoffs #slack #tableau",
        "Post-Integration Layoffs",
        "San Francisco, CA",
        1_960,
    ),
    (
        "Uber",
        "Uber appoints new CTO from Waymo as company accelerates autonomous vehicle program. "
        "Move marks return to self-driving ambitions after 2020 unit sale. "
        "#uber #CTO #autonomous #executive",
        "CTO Appointment",
        "San Francisco, CA",
        1_580,
    ),
    (
        "Netflix",
        "Netflix opens massive gaming studio in Helsinki, hiring 300 game developers. "
        "Company doubles down on gaming after subscriber growth stalls. "
        "#netflix #gaming #hiring #tech",
        "Gaming Studio Launch",
        "Los Gatos, CA",
        2_730,
    ),
    (
        "Morgan Stanley",
        "Morgan Stanley wealth management division lays off 500 financial advisors. "
        "Bank shifting to AI-powered robo-advisory for clients under $1M. "
        "#morganstanley #layoffs #fintech #AI",
        "Wealth Management Cuts",
        "New York, NY",
        3_400,
    ),
    (
        "Airbnb",
        "Airbnb hires new VP of Trust & Safety from Meta. Company faces regulatory pressure in EU. "
        "Role will oversee 1,500-person global trust team. "
        "#airbnb #executive #hiring #safety",
        "VP of Trust & Safety",
        "San Francisco, CA",
        1_120,
    ),
    # New mock entries for expanded queries
    (
        "Intel",
        "Intel announces $20B fab acquisition in Germany. Board of directors approves largest overseas investment in company history. "
        "Move aims to compete with TSMC's European expansion. "
        "#intel #acquisition #semiconductor",
        "Acquisition Announcement",
        "Santa Clara, CA",
        4_500,
    ),
    (
        "Rivian",
        "Rivian files S-1 amendment for secondary offering of 50M shares. "
        "Company needs capital to fund R2 platform development. Stock drops 8% on dilution fears. "
        "#rivian #IPO #EV #offering",
        "IPO Filing / Secondary",
        "Irvine, CA",
        3_200,
    ),
    (
        "Boeing",
        "Boeing board of directors change: 3 new independent directors appointed amid safety crisis. "
        "FAA oversight intensifying. New directors include former NTSB chair. "
        "#boeing #board #governance",
        "Board of Directors Change",
        "Arlington, VA",
        5_100,
    ),
    (
        "Ford",
        "Ford announces closure of 2 assembly plants in Michigan and Ohio. 3,500 jobs affected. "
        "Company shifting production to EV-focused facilities in Tennessee and Kentucky. "
        "#ford #plantclosure #restructuring #EV",
        "Plant Closure",
        "Dearborn, MI",
        4_800,
    ),
    (
        "Samsung",
        "Samsung launches new product line: AI-powered semiconductor design tools for enterprise customers. "
        "Direct competition with Synopsys and Cadence. Priced 40% below incumbents. "
        "#samsung #productlaunch #semiconductor #AI",
        "New Product Launch",
        "Seoul, South Korea",
        2_900,
    ),
    (
        "Toyota",
        "Toyota reports major supply chain disruption: key battery supplier factory fire halts production of 5 EV models. "
        "Estimated 3-month delay on deliveries. Stock down 4%. "
        "#toyota #supplychain #disruption #EV",
        "Supply Chain Disruption",
        "Toyota City, Japan",
        3_600,
    ),
]


class MockLinkedInScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "linkedin"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(10, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            company, content, title, location, applicants = random.choice(_MOCK_LINKEDIN_POSTS)
            pid = self._generate_id()
            posts.append(self._make_post(
                source_id=f"mock_{pid}",
                author=company,
                content=content,
                url=f"https://www.linkedin.com/jobs/view/{pid}/",
                raw_metadata={
                    "search_query": random.choice(_SEARCH_QUERIES),
                    "title": title,
                    "company": company,
                    "location": location,
                    "employment_type": random.choice(["Full-time", "Contract", "Part-time"]),
                    "seniority_level": random.choice(["Executive", "Director", "Vice President", "Mid-Senior level"]),
                    "applicants": applicants + random.randint(-200, 500),
                },
            ))
        return posts
