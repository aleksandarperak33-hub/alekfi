"""Finviz financial news aggregator — headlines, sources, and unusual volume stocks."""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone
from typing import Any

import aiohttp
from bs4 import BeautifulSoup

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_FINVIZ_NEWS_URL = "https://finviz.com/news.ashx"
_FINVIZ_UNUSUAL_VOL_URL = "https://finviz.com/screener.ashx?s=ta_unusualvolume"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://finviz.com/",
}

# Regex-free ticker extraction: common tickers mentioned in financial headlines
_TICKER_PATTERN_PREFIXES = ("$", "NYSE:", "NASDAQ:")


def _extract_tickers_from_text(text: str) -> list[str]:
    """Extract likely stock tickers from headline text."""
    tickers: list[str] = []
    words = text.split()
    for word in words:
        clean = word.strip("().,;:!?")
        # $AAPL style
        if clean.startswith("$") and len(clean) > 1 and clean[1:].isalpha():
            tickers.append(clean[1:].upper())
        # All-caps 1-5 letter words that look like tickers
        elif clean.isupper() and 1 <= len(clean) <= 5 and clean.isalpha():
            # Exclude common English words
            if clean not in {
                "A", "I", "AN", "AT", "BY", "DO", "GO", "IF", "IN", "IS",
                "IT", "MY", "NO", "OF", "ON", "OR", "SO", "TO", "UP", "US",
                "WE", "AM", "AS", "BE", "HE", "ME", "OK", "PM", "THE",
                "AND", "BUT", "FOR", "HAS", "HOW", "ITS", "MAY", "NEW",
                "NOT", "NOW", "OLD", "OUR", "OUT", "OWN", "SAY", "SHE",
                "TOO", "TWO", "WAY", "WHO", "ALL", "ARE", "BIG", "CAN",
                "CEO", "CFO", "COO", "CTO", "GDP", "IPO", "ETF", "SEC",
                "FDA", "FED", "NYSE", "API", "AI", "EV", "EVS", "VS",
            }:
                tickers.append(clean)
    return tickers


class FinvizNewsScraper(BaseScraper):
    """Scrapes Finviz for financial news headlines and unusual volume stocks."""

    @property
    def platform(self) -> str:
        return "finviz_news"

    def __init__(self, interval: int = 120) -> None:
        super().__init__(interval)
        self._seen_headlines: set[str] = set()

    async def _fetch_news(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch and parse the Finviz news page for headlines."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _FINVIZ_NEWS_URL, headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=20)
            ) as resp:
                if resp.status != 200:
                    logger.warning("[finviz_news] news page returned %d", resp.status)
                    return posts
                html = await resp.text()
        except Exception:
            logger.warning("[finviz_news] failed to fetch news page", exc_info=True)
            return posts

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Finviz news page has tables with class "styled-table-new"
            # or news items in divs / table rows with links
            news_tables = soup.find_all("table", class_="styled-table-new")
            if not news_tables:
                # Fallback: look for the news table by structure
                news_tables = soup.find_all("table", {"id": "news-table"})
            if not news_tables:
                # Another fallback: find all tables and look for the one with news links
                for table in soup.find_all("table"):
                    rows = table.find_all("tr")
                    if len(rows) > 10:
                        link_count = sum(1 for r in rows if r.find("a", class_="tab-link-nw") or r.find("a", href=True))
                        if link_count > 5:
                            news_tables = [table]
                            break

            for table in news_tables:
                rows = table.find_all("tr")
                current_date = ""
                for row in rows:
                    # Date cell
                    date_cell = row.find("td", {"width": "130"}) or row.find("td", align="right")
                    if date_cell:
                        date_text = date_cell.get_text(strip=True)
                        if date_text and len(date_text) > 5:
                            current_date = date_text

                    # Headline link
                    link = row.find("a", class_="tab-link-nw")
                    if not link:
                        link = row.find("a", href=True)
                    if not link:
                        continue

                    headline = link.get_text(strip=True)
                    url = link.get("href", "")
                    if not headline or len(headline) < 10:
                        continue

                    # Source badge
                    source_span = row.find("span", class_="tab-link-nw-source")
                    source = source_span.get_text(strip=True) if source_span else "Finviz"

                    # Dedup
                    dedup_key = headline[:80].lower()
                    if dedup_key in self._seen_headlines:
                        continue
                    self._seen_headlines.add(dedup_key)

                    tickers = _extract_tickers_from_text(headline)

                    posts.append(self._make_post(
                        source_id=f"fvnews_{self._generate_id()}",
                        author=source,
                        content=headline,
                        url=url if url.startswith("http") else f"https://finviz.com{url}",
                        source_published_at=current_date if current_date else None,
                        raw_metadata={
                            "source": source,
                            "tickers_mentioned": tickers,
                            "headline_date": current_date,
                            "category": "news",
                        },
                    ))
        except Exception:
            logger.warning("[finviz_news] failed to parse news HTML", exc_info=True)

        return posts

    async def _fetch_unusual_volume(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Scrape the Finviz unusual volume screener page."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _FINVIZ_UNUSUAL_VOL_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.warning("[finviz_news] unusual volume page returned %d", resp.status)
                    return posts
                html = await resp.text()
        except Exception:
            logger.warning("[finviz_news] failed to fetch unusual volume page", exc_info=True)
            return posts

        try:
            soup = BeautifulSoup(html, "html.parser")

            # The screener table contains rows with stock data
            table = soup.find("table", class_="screener_table") or soup.find(
                "table", {"id": "screener-views-table"}
            )
            if not table:
                # Fallback: find a table with many columns (the screener table)
                for t in soup.find_all("table"):
                    header_row = t.find("tr")
                    if header_row and len(header_row.find_all("td")) > 8:
                        table = t
                        break

            if not table:
                logger.debug("[finviz_news] could not find screener table")
                return posts

            rows = table.find_all("tr")[1:]  # skip header
            for row in rows[:50]:  # cap at 50 stocks
                cells = row.find_all("td")
                if len(cells) < 8:
                    continue

                # Typical Finviz screener columns: No, Ticker, Company, Sector, Industry, Country, Market Cap, ...
                ticker_cell = cells[1] if len(cells) > 1 else None
                if not ticker_cell:
                    continue

                ticker_link = ticker_cell.find("a")
                ticker = ticker_link.get_text(strip=True) if ticker_link else cells[1].get_text(strip=True)
                company = cells[2].get_text(strip=True) if len(cells) > 2 else ticker
                sector = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                market_cap = cells[6].get_text(strip=True) if len(cells) > 6 else ""
                price = cells[8].get_text(strip=True) if len(cells) > 8 else ""
                change = cells[9].get_text(strip=True) if len(cells) > 9 else ""
                volume = cells[10].get_text(strip=True) if len(cells) > 10 else ""

                if not ticker or len(ticker) > 6:
                    continue

                dedup_key = f"uvol_{ticker}".lower()
                if dedup_key in self._seen_headlines:
                    continue
                self._seen_headlines.add(dedup_key)

                content = (
                    f"[Unusual Volume] ${ticker} ({company})\n"
                    f"Sector: {sector} | Market Cap: {market_cap}\n"
                    f"Price: {price} | Change: {change} | Volume: {volume}"
                )

                posts.append(self._make_post(
                    source_id=f"fvuvol_{ticker}_{self._generate_id()}",
                    author="Finviz Screener",
                    content=content,
                    url=f"https://finviz.com/quote.ashx?t={ticker}",
                    raw_metadata={
                        "ticker": ticker,
                        "company": company,
                        "sector": sector,
                        "market_cap": market_cap,
                        "price": price,
                        "change_pct": change,
                        "volume": volume,
                        "category": "unusual_volume",
                    },
                ))
        except Exception:
            logger.warning("[finviz_news] failed to parse unusual volume HTML", exc_info=True)

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with aiohttp.ClientSession() as session:
            news_posts = await self._fetch_news(session)
            all_posts.extend(news_posts)

            vol_posts = await self._fetch_unusual_volume(session)
            all_posts.extend(vol_posts)

        # Cap seen set to prevent unbounded growth
        if len(self._seen_headlines) > 10000:
            self._seen_headlines = set(list(self._seen_headlines)[-5000:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_NEWS_HEADLINES = [
    ("NVDA rallies 8% after Q4 earnings crush expectations; data center revenue up 400% YoY", "Reuters", ["NVDA"]),
    ("Fed holds rates steady, signals potential cut in September meeting", "CNBC", []),
    ("Apple announces $110B share buyback — largest in US corporate history", "Bloomberg", ["AAPL"]),
    ("Tesla recalls 2M vehicles over Autopilot safety concerns; stock drops 5%", "WSJ", ["TSLA"]),
    ("Microsoft Azure revenue grows 29%, beating cloud estimates", "TechCrunch", ["MSFT"]),
    ("Gold hits record $2,450/oz as geopolitical tensions escalate", "MarketWatch", []),
    ("Bitcoin surges past $72K as ETF inflows top $1B in single day", "CoinDesk", []),
    ("Eli Lilly obesity drug Zepbound sales hit $1.2B in first quarter", "FiercePharma", ["LLY"]),
    ("JPMorgan CEO Jamie Dimon warns of stagflation risk in H2 outlook", "Financial Times", ["JPM"]),
    ("Amazon AWS introduces custom AI chips, challenging NVDA dominance", "The Information", ["AMZN", "NVDA"]),
    ("Boeing 737 MAX deliveries halted again by FAA quality audit", "Reuters", ["BA"]),
    ("Palantir lands $480M Army contract for AI battlefield analytics", "Defense News", ["PLTR"]),
    ("Costco same-store sales up 9.4%, beating estimates by wide margin", "Barrons", ["COST"]),
    ("Chinese EV maker BYD outsells Tesla globally for second straight quarter", "Bloomberg", ["TSLA"]),
    ("CrowdStrike shares jump 12% after major Pentagon cybersecurity contract win", "CNBC", ["CRWD"]),
    ("Oil drops below $70 as OPEC+ signals production increase for Q3", "Reuters", []),
    ("Meta launches next-gen AI glasses; AR hardware revenue could hit $5B by 2026", "The Verge", ["META"]),
    ("AMD MI300X AI chip gains traction; Microsoft and Meta are major buyers", "AnandTech", ["AMD", "MSFT", "META"]),
    ("Broadcom acquires VMware integration complete; cross-selling drives 15% revenue beat", "SeekingAlpha", ["AVGO"]),
    ("Regional bank stocks plunge as commercial real estate loan defaults spike 40%", "WSJ", []),
    ("UnitedHealth under DOJ antitrust probe for pharmacy benefit practices", "Politico", ["UNH"]),
    ("Semiconductor Equipment stocks surge on TSMC capex guidance raise to $36B", "Nikkei", ["TSM"]),
]

_MOCK_UNUSUAL_VOLUME = [
    ("SMCI", "Super Micro Computer", "Technology", "45.2B", "892.50", "+14.3%", "28.5M"),
    ("MARA", "Marathon Digital", "Financial", "6.8B", "22.15", "+18.7%", "95.2M"),
    ("RIVN", "Rivian Automotive", "Consumer Cyclical", "15.1B", "16.80", "+9.2%", "42.3M"),
    ("IONQ", "IonQ Inc.", "Technology", "8.2B", "14.50", "+22.1%", "35.7M"),
    ("SOFI", "SoFi Technologies", "Financial", "9.4B", "8.95", "+7.8%", "68.1M"),
    ("PLTR", "Palantir Technologies", "Technology", "52.3B", "23.40", "+6.5%", "85.6M"),
    ("SNAP", "Snap Inc.", "Communication Services", "18.7B", "12.30", "-8.4%", "55.2M"),
    ("PLUG", "Plug Power", "Industrials", "3.2B", "5.45", "-12.6%", "72.8M"),
    ("RBLX", "Roblox Corp.", "Technology", "28.5B", "48.20", "+5.1%", "22.4M"),
    ("COIN", "Coinbase Global", "Financial", "42.1B", "175.30", "+11.3%", "18.9M"),
]


class MockFinvizNewsScraper(BaseScraper):
    """Mock scraper generating realistic Finviz news and unusual volume data."""

    @property
    def platform(self) -> str:
        return "finviz_news"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        now_str = datetime.now(timezone.utc).strftime("%b-%d-%y %I:%M%p")

        # Generate news headlines
        news_count = random.randint(12, 20)
        selected_news = random.sample(
            _MOCK_NEWS_HEADLINES, min(news_count, len(_MOCK_NEWS_HEADLINES))
        )
        for headline, source, tickers in selected_news:
            posts.append(self._make_post(
                source_id=f"fvnews_{self._generate_id()}",
                author=source,
                content=headline,
                url=f"https://finviz.com/news.ashx#{self._generate_id()}",
                source_published_at=now_str,
                raw_metadata={
                    "source": source,
                    "tickers_mentioned": tickers,
                    "headline_date": now_str,
                    "category": "news",
                },
            ))

        # Generate unusual volume items
        vol_count = random.randint(4, 8)
        selected_vol = random.sample(
            _MOCK_UNUSUAL_VOLUME, min(vol_count, len(_MOCK_UNUSUAL_VOLUME))
        )
        for ticker, company, sector, mcap, price, change, volume in selected_vol:
            content = (
                f"[Unusual Volume] ${ticker} ({company})\n"
                f"Sector: {sector} | Market Cap: {mcap}\n"
                f"Price: {price} | Change: {change} | Volume: {volume}"
            )
            posts.append(self._make_post(
                source_id=f"fvuvol_{ticker}_{self._generate_id()}",
                author="Finviz Screener",
                content=content,
                url=f"https://finviz.com/quote.ashx?t={ticker}",
                raw_metadata={
                    "ticker": ticker,
                    "company": company,
                    "sector": sector,
                    "market_cap": mcap,
                    "price": price,
                    "change_pct": change,
                    "volume": volume,
                    "category": "unusual_volume",
                },
            ))

        return posts
