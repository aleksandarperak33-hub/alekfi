"""Options flow scraper — unusual options activity detector via public sources."""

from __future__ import annotations

import logging
import random
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp
from bs4 import BeautifulSoup

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# Public sources for unusual options activity
_BARCHART_UNUSUAL_URL = "https://www.barchart.com/options/unusual-activity/stocks"
_FINVIZ_OPTIONS_URL = "https://finviz.com/screener.ashx?v=171&s=ta_unusualvolume&o=-optionvolume"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
}

# Regex for parsing volume/OI strings like "1.2K", "45.3M"
_VOLUME_RE = re.compile(r"([\d,.]+)\s*([KMB])?", re.IGNORECASE)


def _parse_volume(text: str) -> int:
    """Parse a human-readable volume string like '1.2K' or '45.3M' into an integer."""
    text = text.strip()
    match = _VOLUME_RE.match(text)
    if not match:
        try:
            return int(text.replace(",", ""))
        except (ValueError, TypeError):
            return 0

    num_str = match.group(1).replace(",", "")
    suffix = (match.group(2) or "").upper()
    try:
        num = float(num_str)
    except ValueError:
        return 0

    multipliers = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}
    return int(num * multipliers.get(suffix, 1))


class OptionsFlowScraper(BaseScraper):
    """Detects unusual options activity by scraping public options data sources."""

    @property
    def platform(self) -> str:
        return "options_flow"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._seen_keys: set[str] = set()

    async def _fetch_barchart_unusual(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Scrape Barchart's unusual options activity page."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _BARCHART_UNUSUAL_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=25),
            ) as resp:
                if resp.status != 200:
                    logger.warning("[options_flow] barchart returned %d", resp.status)
                    return posts
                html = await resp.text()
        except Exception:
            logger.warning("[options_flow] failed to fetch barchart", exc_info=True)
            return posts

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Barchart uses a data table for unusual options
            table = soup.find("table", class_="bc-table-scrollable-inner")
            if not table:
                # Fallback: find the main data table
                for t in soup.find_all("table"):
                    headers = [th.get_text(strip=True).lower() for th in t.find_all("th")]
                    if "symbol" in headers and ("volume" in headers or "vol" in headers):
                        table = t
                        break

            if not table:
                logger.debug("[options_flow] could not find barchart options table")
                return posts

            rows = table.find_all("tr")[1:]  # skip header
            for row in rows[:40]:
                cells = row.find_all("td")
                if len(cells) < 6:
                    continue

                ticker = cells[0].get_text(strip=True).upper()
                if not ticker or len(ticker) > 6:
                    continue

                # Typical columns: Symbol, Type(C/P), Strike, Expiration, Volume, Open Interest, Vol/OI
                option_type = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                strike = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                expiration = cells[3].get_text(strip=True) if len(cells) > 3 else ""
                volume = cells[4].get_text(strip=True) if len(cells) > 4 else "0"
                open_interest = cells[5].get_text(strip=True) if len(cells) > 5 else "0"
                vol_oi_ratio = cells[6].get_text(strip=True) if len(cells) > 6 else ""

                dedup_key = f"{ticker}_{option_type}_{strike}_{expiration}"
                if dedup_key in self._seen_keys:
                    continue
                self._seen_keys.add(dedup_key)

                vol_int = _parse_volume(volume)
                oi_int = _parse_volume(open_interest)

                # Determine significance based on volume/OI
                significance = "medium"
                if vol_int > 10000 or (oi_int > 0 and vol_int / max(oi_int, 1) > 3):
                    significance = "high"
                if vol_int > 50000:
                    significance = "critical"

                opt_label = "Call" if option_type.upper().startswith("C") else "Put" if option_type.upper().startswith("P") else option_type
                content = (
                    f"[Unusual Options] ${ticker} {opt_label} ${strike} exp {expiration}\n"
                    f"Volume: {volume} | OI: {open_interest} | Vol/OI: {vol_oi_ratio}"
                )

                posts.append(self._make_post(
                    source_id=f"opt_{dedup_key}_{self._generate_id()}",
                    author="Barchart",
                    content=content,
                    url=f"https://www.barchart.com/stocks/quotes/{ticker}/options",
                    raw_metadata={
                        "ticker": ticker,
                        "option_type": opt_label,
                        "strike": strike,
                        "expiration": expiration,
                        "volume": vol_int,
                        "open_interest": oi_int,
                        "vol_oi_ratio": vol_oi_ratio,
                        "significance": significance,
                        "source": "barchart",
                    },
                ))
        except Exception:
            logger.warning("[options_flow] failed to parse barchart HTML", exc_info=True)

        return posts

    async def _fetch_finviz_options(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch stocks with high options volume from Finviz screener."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _FINVIZ_OPTIONS_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.debug("[options_flow] finviz options returned %d", resp.status)
                    return posts
                html = await resp.text()
        except Exception:
            logger.debug("[options_flow] failed to fetch finviz options page", exc_info=True)
            return posts

        try:
            soup = BeautifulSoup(html, "html.parser")

            # Find screener results table
            table = None
            for t in soup.find_all("table"):
                header_row = t.find("tr")
                if header_row:
                    headers_text = header_row.get_text(strip=True).lower()
                    if "ticker" in headers_text or "symbol" in headers_text:
                        table = t
                        break

            if not table:
                return posts

            rows = table.find_all("tr")[1:]
            for row in rows[:30]:
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue

                ticker = cells[1].get_text(strip=True).upper() if len(cells) > 1 else ""
                company = cells[2].get_text(strip=True) if len(cells) > 2 else ""
                if not ticker or len(ticker) > 6:
                    continue

                # Extract options volume if available
                opt_vol = ""
                for i, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if text and ("K" in text or "M" in text) and i > 5:
                        opt_vol = text
                        break

                dedup_key = f"fv_opt_{ticker}"
                if dedup_key in self._seen_keys:
                    continue
                self._seen_keys.add(dedup_key)

                content = (
                    f"[High Options Volume] ${ticker} ({company})\n"
                    f"Options Volume: {opt_vol if opt_vol else 'Elevated'}"
                )

                posts.append(self._make_post(
                    source_id=f"fvopt_{ticker}_{self._generate_id()}",
                    author="Finviz",
                    content=content,
                    url=f"https://finviz.com/quote.ashx?t={ticker}",
                    raw_metadata={
                        "ticker": ticker,
                        "company": company,
                        "options_volume": opt_vol,
                        "significance": "medium",
                        "source": "finviz",
                    },
                ))
        except Exception:
            logger.debug("[options_flow] failed to parse finviz options HTML", exc_info=True)

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        async with aiohttp.ClientSession() as session:
            barchart_posts = await self._fetch_barchart_unusual(session)
            all_posts.extend(barchart_posts)

            finviz_posts = await self._fetch_finviz_options(session)
            all_posts.extend(finviz_posts)

        # Cap seen set
        if len(self._seen_keys) > 10000:
            self._seen_keys = set(list(self._seen_keys)[-5000:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_OPTIONS_FLOW = [
    # (ticker, type, strike, expiration, volume, OI, vol/oi, significance)
    ("NVDA", "Call", "950", "2026-03-21", 45200, 12800, "3.53", "critical"),
    ("NVDA", "Call", "1000", "2026-06-20", 28700, 5400, "5.31", "critical"),
    ("TSLA", "Put", "180", "2026-02-21", 38500, 15200, "2.53", "high"),
    ("TSLA", "Put", "150", "2026-03-21", 22100, 8900, "2.48", "high"),
    ("AAPL", "Call", "200", "2026-03-21", 52000, 28000, "1.86", "critical"),
    ("AMD", "Call", "180", "2026-04-17", 31200, 9800, "3.18", "high"),
    ("SPY", "Put", "480", "2026-02-14", 89500, 42000, "2.13", "critical"),
    ("META", "Call", "550", "2026-03-21", 18900, 4200, "4.50", "high"),
    ("COIN", "Call", "250", "2026-03-21", 15600, 3100, "5.03", "high"),
    ("PLTR", "Call", "30", "2026-04-17", 42800, 18500, "2.31", "high"),
    ("SMCI", "Call", "1200", "2026-02-21", 12400, 1800, "6.89", "critical"),
    ("BA", "Put", "180", "2026-03-21", 19800, 7200, "2.75", "high"),
    ("XOM", "Put", "95", "2026-04-17", 14200, 6800, "2.09", "medium"),
    ("MARA", "Call", "30", "2026-03-21", 35600, 12400, "2.87", "high"),
    ("LLY", "Call", "850", "2026-06-20", 8900, 2100, "4.24", "high"),
]


class MockOptionsFlowScraper(BaseScraper):
    """Mock scraper generating realistic unusual options activity data."""

    @property
    def platform(self) -> str:
        return "options_flow"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []

        count = random.randint(8, 12)
        selected = random.sample(_MOCK_OPTIONS_FLOW, min(count, len(_MOCK_OPTIONS_FLOW)))

        for ticker, opt_type, strike, expiration, volume, oi, vol_oi, significance in selected:
            # Randomise volume slightly for variety
            vol_jitter = int(volume * random.uniform(0.85, 1.15))
            oi_jitter = int(oi * random.uniform(0.9, 1.1))
            ratio = f"{vol_jitter / max(oi_jitter, 1):.2f}"

            content = (
                f"[Unusual Options] ${ticker} {opt_type} ${strike} exp {expiration}\n"
                f"Volume: {vol_jitter:,} | OI: {oi_jitter:,} | Vol/OI: {ratio}"
            )

            posts.append(self._make_post(
                source_id=f"opt_{ticker}_{opt_type}_{strike}_{self._generate_id()}",
                author="Barchart",
                content=content,
                url=f"https://www.barchart.com/stocks/quotes/{ticker}/options",
                raw_metadata={
                    "ticker": ticker,
                    "option_type": opt_type,
                    "strike": strike,
                    "expiration": expiration,
                    "volume": vol_jitter,
                    "open_interest": oi_jitter,
                    "vol_oi_ratio": ratio,
                    "significance": significance,
                    "source": "barchart",
                },
            ))

        return posts
