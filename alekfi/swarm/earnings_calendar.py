"""Earnings calendar scraper — companies reporting this week."""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_YAHOO_EARNINGS_URL = "https://finance.yahoo.com/calendar/earnings"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# Major tickers to monitor for earnings
_MAJOR_TICKERS = [
    # Tech mega-caps
    "AAPL", "MSFT", "GOOG", "GOOGL", "AMZN", "META", "NVDA", "TSLA",
    # Semiconductors
    "AMD", "INTC", "AVGO", "QCOM", "MU", "ASML", "TSM", "ARM", "SMCI",
    # Software / Cloud
    "CRM", "ORCL", "ADBE", "NOW", "SNOW", "CRWD", "PANW", "PLTR",
    # Banks / Finance
    "JPM", "BAC", "WFC", "GS", "MS", "C", "SCHW", "BLK", "AXP",
    # Payments
    "V", "MA", "PYPL", "SQ", "COIN",
    # Healthcare / Pharma
    "JNJ", "PFE", "UNH", "LLY", "MRK", "ABBV", "BMY", "AMGN", "GILD",
    "REGN", "VRTX", "MRNA", "BIIB", "NVO",
    # Energy
    "XOM", "CVX", "COP", "OXY", "SLB",
    # Defense / Aerospace
    "BA", "LMT", "RTX", "NOC", "GD",
    # Consumer / Retail
    "WMT", "COST", "HD", "TGT", "LOW", "NKE",
    # Food / Beverage
    "PG", "KO", "PEP", "MCD", "SBUX",
    # Media / Entertainment
    "DIS", "NFLX", "CMCSA",
    # Telecom
    "T", "VZ", "TMUS",
    # Industrials
    "CAT", "DE", "UPS", "FDX", "HON", "MMM", "GE",
    # Autos
    "F", "GM", "RIVN", "LCID",
]

# Simple regex to extract table rows from Yahoo Finance earnings page
_TABLE_ROW_RE = re.compile(
    r'data-symbol="([A-Z.]+)".*?'
    r'<td[^>]*>([^<]*)</td>.*?'  # company name
    r'<td[^>]*>([^<]*)</td>.*?'  # earnings date
    r'<td[^>]*>([^<]*)</td>.*?'  # EPS estimate
    r'<td[^>]*>([^<]*)</td>',    # revenue estimate
    re.DOTALL,
)


def _get_week_range() -> tuple[str, str]:
    """Return (start_date, end_date) for the current week (Monday to Sunday)."""
    today = datetime.now(timezone.utc).date()
    # Go to the Monday of this week
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    return monday.isoformat(), sunday.isoformat()


def _parse_earnings_html(html: str) -> list[dict[str, Any]]:
    """Extract earnings data from Yahoo Finance HTML.

    Falls back to regex-based parsing since BeautifulSoup may not be installed.
    """
    entries: list[dict[str, Any]] = []

    # Try parsing with BeautifulSoup if available
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if table:
            rows = table.find_all("tr")[1:]  # skip header
            for row in rows:
                cells = row.find_all("td")
                if len(cells) >= 5:
                    ticker = cells[0].get_text(strip=True)
                    company = cells[1].get_text(strip=True)
                    earnings_date = cells[2].get_text(strip=True)
                    eps_estimate = cells[3].get_text(strip=True)
                    revenue_estimate = cells[4].get_text(strip=True)

                    if ticker:
                        entries.append({
                            "ticker": ticker,
                            "company": company,
                            "earnings_date": earnings_date,
                            "eps_estimate": eps_estimate or "N/A",
                            "revenue_estimate": revenue_estimate or "N/A",
                        })
            return entries
    except ImportError:
        pass

    # Fallback: regex-based extraction
    for match in _TABLE_ROW_RE.finditer(html):
        ticker = match.group(1)
        company = match.group(2).strip()
        earnings_date = match.group(3).strip()
        eps_estimate = match.group(4).strip() or "N/A"
        revenue_estimate = match.group(5).strip() or "N/A"

        if ticker:
            entries.append({
                "ticker": ticker,
                "company": company,
                "earnings_date": earnings_date,
                "eps_estimate": eps_estimate,
                "revenue_estimate": revenue_estimate,
            })

    return entries


class EarningsCalendarScraper(BaseScraper):
    """Scrapes Yahoo Finance earnings calendar for upcoming reports from major companies."""

    @property
    def platform(self) -> str:
        return "earnings_calendar"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._seen_earnings: set[str] = set()

    async def _fetch_yahoo_calendar(
        self, client: httpx.AsyncClient
    ) -> list[dict[str, Any]]:
        """Fetch and parse the Yahoo Finance earnings calendar page."""
        entries: list[dict[str, Any]] = []
        start_date, end_date = _get_week_range()
        try:
            resp = await client.get(
                _YAHOO_EARNINGS_URL,
                params={"from": start_date, "to": end_date},
                headers=_HEADERS,
            )
            if resp.status_code != 200:
                logger.warning(
                    "[earnings_calendar] Yahoo returned %d", resp.status_code
                )
                return entries

            entries = _parse_earnings_html(resp.text)
            logger.info(
                "[earnings_calendar] parsed %d entries from Yahoo", len(entries)
            )
        except Exception:
            logger.warning(
                "[earnings_calendar] failed to fetch Yahoo calendar", exc_info=True
            )
        return entries

    async def _fetch_via_finnhub(self) -> list[dict[str, Any]]:
        """Fallback: use Finnhub earnings calendar API for the current week."""
        entries: list[dict[str, Any]] = []
        settings = get_settings()
        if not settings.finnhub_api_key:
            logger.debug("[earnings_calendar] FINNHUB_API_KEY missing, skipping fallback")
            return entries

        today = datetime.now(timezone.utc).date().isoformat()
        week_end = (datetime.now(timezone.utc).date() + timedelta(days=7)).isoformat()
        major = set(_MAJOR_TICKERS)

        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
                resp = await client.get(
                    "https://finnhub.io/api/v1/calendar/earnings",
                    params={"from": today, "to": week_end, "token": settings.finnhub_api_key},
                )
                if resp.status_code != 200:
                    return entries
                payload = resp.json() if resp.text else {}
        except Exception:
            logger.debug("[earnings_calendar] finnhub fallback failed", exc_info=True)
            return entries

        for item in (payload or {}).get("earningsCalendar", []):
            ticker_symbol = (item.get("symbol") or "").upper()
            if not ticker_symbol or ticker_symbol not in major:
                continue
            entries.append(
                {
                    "ticker": ticker_symbol,
                    "company": ticker_symbol,
                    "earnings_date": item.get("date", ""),
                    "eps_estimate": item.get("epsEstimate") or "N/A",
                    "revenue_estimate": item.get("revenueEstimate") or "N/A",
                }
            )

        return entries

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        # Primary: Yahoo Finance calendar page
        async with httpx.AsyncClient(
            timeout=20, follow_redirects=True
        ) as client:
            entries = await self._fetch_yahoo_calendar(client)

        # Fallback if Yahoo scraping returns nothing
        if not entries:
            entries = await self._fetch_via_finnhub()

        # Filter to major tickers we care about
        major_set = set(_MAJOR_TICKERS)

        for entry in entries:
            ticker = entry.get("ticker", "")
            # Only create posts for tickers in our watchlist (or all if from Yahoo)
            if ticker not in major_set and len(entries) > 50:
                continue

            dedup_key = f"{ticker}_{entry.get('earnings_date', '')}"
            if dedup_key in self._seen_earnings:
                continue
            self._seen_earnings.add(dedup_key)

            company = entry.get("company", ticker)
            earnings_date = entry.get("earnings_date", "TBD")
            eps_estimate = entry.get("eps_estimate", "N/A")
            revenue_estimate = entry.get("revenue_estimate", "N/A")

            content = (
                f"[Earnings Report Upcoming] {company} (${ticker})\n"
                f"Reporting Date: {earnings_date}\n"
                f"EPS Estimate: {eps_estimate}\n"
                f"Revenue Estimate: {revenue_estimate}"
            )

            all_posts.append(self._make_post(
                source_id=f"earnings_{dedup_key}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{ticker}/",
                raw_metadata={
                    "ticker": ticker,
                    "company": company,
                    "earnings_date": earnings_date,
                    "eps_estimate": eps_estimate,
                    "revenue_estimate": revenue_estimate,
                    "significance": "high",
                },
            ))

        # Cap seen set
        if len(self._seen_earnings) > 5000:
            self._seen_earnings = set(list(self._seen_earnings)[-2500:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_EARNINGS = [
    ("AAPL", "Apple Inc.", "2026-02-10", "2.35", "124.3B"),
    ("MSFT", "Microsoft Corporation", "2026-02-11", "3.12", "65.7B"),
    ("NVDA", "NVIDIA Corporation", "2026-02-12", "5.40", "38.2B"),
    ("GOOG", "Alphabet Inc.", "2026-02-11", "1.92", "88.3B"),
    ("AMZN", "Amazon.com Inc.", "2026-02-13", "1.28", "170.5B"),
    ("META", "Meta Platforms Inc.", "2026-02-12", "5.50", "42.1B"),
    ("JPM", "JPMorgan Chase & Co.", "2026-02-10", "4.85", "42.8B"),
    ("XOM", "Exxon Mobil Corporation", "2026-02-10", "2.15", "90.3B"),
    ("LLY", "Eli Lilly and Company", "2026-02-13", "3.25", "11.8B"),
    ("TSLA", "Tesla Inc.", "2026-02-12", "0.72", "25.6B"),
    ("BA", "Boeing Co.", "2026-02-11", "-0.45", "18.2B"),
    ("DIS", "The Walt Disney Company", "2026-02-13", "1.45", "23.7B"),
]


class MockEarningsCalendarScraper(BaseScraper):
    """Mock scraper generating realistic upcoming earnings announcements."""

    @property
    def platform(self) -> str:
        return "earnings_calendar"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(6, 10)
        posts: list[dict[str, Any]] = []
        selected = random.sample(_MOCK_EARNINGS, min(count, len(_MOCK_EARNINGS)))
        for ticker, company, earnings_date, eps_est, rev_est in selected:
            content = (
                f"[Earnings Report Upcoming] {company} (${ticker})\n"
                f"Reporting Date: {earnings_date}\n"
                f"EPS Estimate: {eps_est}\n"
                f"Revenue Estimate: {rev_est}"
            )

            posts.append(self._make_post(
                source_id=f"earnings_{ticker}_{earnings_date}",
                author="Yahoo Finance",
                content=content,
                url=f"https://finance.yahoo.com/quote/{ticker}/",
                raw_metadata={
                    "ticker": ticker,
                    "company": company,
                    "earnings_date": earnings_date,
                    "eps_estimate": eps_est,
                    "revenue_estimate": rev_est,
                    "significance": "high",
                },
            ))
        return posts
