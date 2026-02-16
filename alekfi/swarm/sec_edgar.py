"""SEC EDGAR scraper — 8-K filings, Form 4 insider trades, 13F holdings, S-1, SC 13D/G, DEFA14A."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_EDGAR_BASE = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_RSS_BASE = "https://www.sec.gov/cgi-bin/browse-edgar"
_EDGAR_FULL_TEXT = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_SUBMISSIONS = "https://data.sec.gov/submissions"
_EDGAR_FILINGS_RSS = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type={form_type}&dateb=&owner=include&count=40&search_text=&start=0&output=atom"
_HEADERS = {
    "User-Agent": "AlekFi/1.0 (contact@openclaw.dev)",
    "Accept": "application/json",
}

# Significance levels for filing types
_SIGNIFICANCE_MAP: dict[str, str] = {
    "8-K": "critical",
    "4": "critical",
    "13F-HR": "high",
    "S-1": "high",
    "SC 13D": "high",
    "SC 13D/A": "high",
    "SC 13G": "high",
    "SC 13G/A": "high",
    "DEFA14A": "high",
}

# All form types to monitor
_FORM_TYPES = ["8-K", "4", "13F-HR", "S-1", "SC 13D", "SC 13G", "DEFA14A"]


def _get_significance(form_type: str) -> str:
    """Return the significance level for a given form type."""
    return _SIGNIFICANCE_MAP.get(form_type, "medium")


class SECEdgarScraper(BaseScraper):
    """Monitors SEC EDGAR for 8-K, Form 4, 13F, S-1, SC 13D/G, and DEFA14A filings."""

    @property
    def platform(self) -> str:
        return "sec_edgar"

    def __init__(self, interval: int = 120) -> None:
        super().__init__(interval)
        self._seen_accessions: set[str] = set()

    async def _fetch_full_text_search(self, client: httpx.AsyncClient, query: str, form_type: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        try:
            resp = await client.get(
                "https://efts.sec.gov/LATEST/search-index",
                params={
                    "q": query,
                    "dateRange": "custom",
                    "startdt": "2024-01-01",
                    "forms": form_type,
                },
                headers=_HEADERS,
            )
            if resp.status_code == 200:
                data = resp.json()
                for hit in data.get("hits", {}).get("hits", [])[:20]:
                    src = hit.get("_source", {})
                    accession = src.get("file_num", self._generate_id())
                    if isinstance(accession, list):
                        accession = accession[0] if accession else self._generate_id()
                    if accession in self._seen_accessions:
                        continue
                    self._seen_accessions.add(accession)
                    significance = _get_significance(form_type)
                    posts.append(self._make_post(
                        source_id=accession,
                        author=src.get("display_names", ["SEC"])[0] if src.get("display_names") else "SEC",
                        content=f"[{form_type}] {src.get('display_names', ['Unknown'])[0]}: {src.get('file_description', 'Filing')}",
                        url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={accession}",
                        raw_metadata={
                            "form_type": form_type,
                            "file_date": src.get("file_date"),
                            "company": src.get("display_names", []),
                            "file_description": src.get("file_description"),
                            "significance_level": significance,
                        },
                    ))
        except Exception:
            logger.warning("[sec_edgar] full-text search failed for %s/%s", form_type, query, exc_info=True)
        return posts

    async def _fetch_recent_filings(self, client: httpx.AsyncClient, form_type: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        try:
            url = f"https://efts.sec.gov/LATEST/search-index?forms={form_type}&dateRange=custom&startdt=2024-01-01"
            resp = await client.get(url, headers=_HEADERS)
            if resp.status_code == 200:
                import feedparser
                feed = feedparser.parse(resp.text)
                for entry in feed.entries[:20]:
                    acc = getattr(entry, "id", self._generate_id())
                    if acc in self._seen_accessions:
                        continue
                    self._seen_accessions.add(acc)
                    significance = _get_significance(form_type)
                    posts.append(self._make_post(
                        source_id=acc,
                        author=getattr(entry, "author", "SEC"),
                        content=f"[{form_type}] {getattr(entry, 'title', 'Filing')}",
                        url=getattr(entry, "link", None),
                        raw_metadata={
                            "form_type": form_type,
                            "published": getattr(entry, "published", ""),
                            "summary": getattr(entry, "summary", "")[:1000],
                            "significance_level": significance,
                        },
                    ))
        except Exception:
            logger.warning("[sec_edgar] RSS fetch failed for %s", form_type, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30) as client:
            for form_type in _FORM_TYPES:
                posts = await self._fetch_recent_filings(client, form_type)
                all_posts.extend(posts)
                # Use targeted search queries depending on form type
                if form_type in ("8-K", "4"):
                    text_posts = await self._fetch_full_text_search(
                        client, "material event OR acquisition OR restructuring", form_type
                    )
                elif form_type == "S-1":
                    text_posts = await self._fetch_full_text_search(
                        client, "initial public offering OR IPO OR registration statement", form_type
                    )
                elif form_type in ("SC 13D", "SC 13G"):
                    text_posts = await self._fetch_full_text_search(
                        client, "beneficial ownership OR activist OR stake", form_type
                    )
                elif form_type == "DEFA14A":
                    text_posts = await self._fetch_full_text_search(
                        client, "proxy OR solicitation OR shareholder vote OR board election", form_type
                    )
                else:
                    text_posts = await self._fetch_full_text_search(
                        client, "material event OR acquisition OR restructuring", form_type
                    )
                all_posts.extend(text_posts)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_FILINGS = [
    # 8-K: critical
    ("8-K", "Apple Inc.", "AAPL", "Material Event: Company announces $100B share buyback program"),
    ("8-K", "Boeing Co.", "BA", "Material Event: CEO resignation effective immediately due to quality concerns"),
    ("8-K", "Tesla Inc.", "TSLA", "Material Event: Entry into new market — launching energy storage division in India"),
    ("8-K", "JPMorgan Chase", "JPM", "Material Event: Acquisition of regional bank for $2.3B"),
    ("8-K", "Pfizer Inc.", "PFE", "Material Event: FDA approval for new oncology drug; projected $5B peak sales"),
    # Form 4: critical
    ("4", "Elon Musk", "TSLA", "Form 4: CEO sold 4.4M shares at $245.50. Proceeds: $1.08B"),
    ("4", "Tim Cook", "AAPL", "Form 4: CEO exercised options and sold 100K shares at $192.30"),
    ("4", "Jamie Dimon", "JPM", "Form 4: CEO sold $150M in shares — first sale in 18 years"),
    ("4", "Lisa Su", "AMD", "Form 4: CEO purchased 50K shares at $155.20 on open market"),
    ("4", "Jensen Huang", "NVDA", "Form 4: CEO sold 600K shares at $725 under 10b5-1 plan"),
    # 13F-HR: high
    ("13F-HR", "Berkshire Hathaway", "BRK.B", "13F: New position in homebuilder NVR Inc. worth $800M"),
    ("13F-HR", "Bridgewater Associates", "N/A", "13F: Increased emerging market positions by 40%; reduced US tech exposure"),
    ("13F-HR", "Citadel Advisors", "N/A", "13F: Massive increase in NVDA position; new AI infrastructure bets"),
    ("13F-HR", "Soros Fund Management", "N/A", "13F: New $500M position in copper miners; exiting Chinese tech"),
    # S-1: high
    ("S-1", "Databricks Inc.", "N/A", "S-1: IPO filing for Databricks. Revenue $1.6B, growing 55% YoY. Targeting $40B+ valuation."),
    ("S-1", "Stripe Inc.", "N/A", "S-1: Stripe files for IPO. 2024 revenue $18B. Profitable for 2nd consecutive year."),
    ("S-1", "Shein Group", "N/A", "S-1: Fast fashion giant Shein files for US IPO. Revenue $30B. Controversial supply chain under scrutiny."),
    # SC 13D: high (activist stakes)
    ("SC 13D", "Elliott Management", "SWK", "SC 13D: Elliott discloses 8.2% activist stake in Stanley Black & Decker. Demanding board seats and cost cuts."),
    ("SC 13D", "Carl Icahn", "IEP", "SC 13D: Icahn increases stake in Southwest Gas to 12.5%. Pushing for asset sales and management change."),
    ("SC 13D", "Starboard Value", "PFGC", "SC 13D: Starboard acquires 6.7% stake in Performance Food Group. Seeking operational improvements."),
    # SC 13G: high (passive large stakes)
    ("SC 13G", "Vanguard Group", "AAPL", "SC 13G: Vanguard increases passive stake in Apple to 8.9%. Largest institutional holder."),
    ("SC 13G", "BlackRock Inc.", "MSFT", "SC 13G: BlackRock reports 7.2% ownership of Microsoft through index funds."),
    # DEFA14A: high (proxy fights)
    ("DEFA14A", "Disney Co.", "DIS", "DEFA14A: Trian Partners files proxy solicitation for Disney board seats. Nelson Peltz nominated for director."),
    ("DEFA14A", "Salesforce Inc.", "CRM", "DEFA14A: Activist investor launches proxy fight at Salesforce. Demanding CEO accountability on M&A spending."),
]


class MockEdgarScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "sec_edgar"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(5, 15)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            form_type, company, ticker, content = random.choice(_MOCK_FILINGS)
            significance = _get_significance(form_type)
            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}",
                author=company,
                content=content,
                url=f"https://www.sec.gov/cgi-bin/browse-edgar?company={company.replace(' ', '+')}&CIK=&type={form_type}",
                raw_metadata={
                    "form_type": form_type,
                    "company": company,
                    "ticker": ticker,
                    "file_date": "2025-01-15",
                    "significance_level": significance,
                },
            ))
        return posts
