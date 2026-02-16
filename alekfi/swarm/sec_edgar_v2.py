"""SEC EDGAR v2 scraper — enhanced filing monitor using EFTS full-text search."""

from __future__ import annotations

import asyncio
import logging
import random
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# ── EDGAR API endpoints ──────────────────────────────────────────────────
_EFTS_SEARCH = "https://efts.sec.gov/LATEST/search-index"
_EDGAR_RSS = (
    "https://www.sec.gov/cgi-bin/browse-edgar"
    "?action=getcurrent&type={form_type}&dateb=&owner=include"
    "&count=40&search_text=&start=0&output=atom"
)
_EDGAR_FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}"

_HEADERS = {
    "User-Agent": "AlekFi/2.0 (contact@openclaw.dev)",
    "Accept": "application/json",
}
_HEADERS_XML = {
    "User-Agent": "AlekFi/2.0 (contact@openclaw.dev)",
    "Accept": "application/atom+xml, application/xml, text/xml",
}

# ── Rate-limiter: max 10 req/s per SEC fair-access policy ────────────────
_REQUEST_INTERVAL = 0.12  # ~8.3 req/s to stay safely under 10

# ── Form type configuration ──────────────────────────────────────────────
_FORM_CONFIG: list[dict[str, Any]] = [
    {
        "form": "4",
        "significance": "critical",
        "search_query": "insider purchase OR insider sale OR Form 4",
        "description": "Insider Trading",
    },
    {
        "form": "8-K",
        "significance": "critical",
        "search_query": "material event OR acquisition OR restructuring OR CEO resignation",
        "description": "Material Events",
    },
    {
        "form": "SC 13D",
        "significance": "high",
        "search_query": "beneficial ownership OR activist OR stake acquisition",
        "description": "Activist Stakes (>5%)",
    },
    {
        "form": "SC 13G",
        "significance": "medium",
        "search_query": "beneficial ownership OR passive stake",
        "description": "Large Passive Stakes",
    },
    {
        "form": "S-1",
        "significance": "high",
        "search_query": "initial public offering OR IPO OR registration statement",
        "description": "IPO Filings",
    },
    {
        "form": "13F-HR",
        "significance": "medium",
        "search_query": "institutional holdings OR quarterly report",
        "description": "Quarterly Institutional Holdings",
    },
    {
        "form": "DEFA14A",
        "significance": "high",
        "search_query": "proxy solicitation OR shareholder vote OR board election OR proxy fight",
        "description": "Proxy Fights",
    },
]

_FORM_TYPES = [cfg["form"] for cfg in _FORM_CONFIG]

_SIGNIFICANCE_MAP: dict[str, str] = {cfg["form"]: cfg["significance"] for cfg in _FORM_CONFIG}
# Include amendment variants
_SIGNIFICANCE_MAP.update({
    "SC 13D/A": "high",
    "SC 13G/A": "medium",
})

# ── Insider transaction codes (Form 4) ───────────────────────────────────
_TRANSACTION_CODES: dict[str, str] = {
    "P": "purchase",
    "S": "sale",
    "A": "grant/award",
    "D": "disposition",
    "F": "tax withholding",
    "M": "option exercise",
    "G": "gift",
    "J": "other acquisition",
    "K": "equity swap",
    "C": "conversion",
    "E": "expiration",
    "I": "discretionary",
    "U": "tender of shares",
    "W": "acquired from issuer",
    "X": "option exercise (expired)",
    "Z": "deposit into trust",
}

# ── C-suite titles for flagging high-value insider trades ────────────────
_CSUITE_PATTERNS = re.compile(
    r"\b(CEO|CFO|COO|CTO|CIO|CISO|Chief\s+Executive|Chief\s+Financial|Chief\s+Operating|"
    r"Chief\s+Technology|President|Chairman|Director)\b",
    re.IGNORECASE,
)


def _get_significance(form_type: str) -> str:
    """Return the significance level for a given form type."""
    return _SIGNIFICANCE_MAP.get(form_type, "medium")


def _date_range_last_24h() -> tuple[str, str]:
    """Return (start_date, end_date) for the last 24 hours in YYYY-MM-DD format."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=24)
    return start.strftime("%Y-%m-%d"), now.strftime("%Y-%m-%d")


def _extract_accession_number(text: str) -> str | None:
    """Extract an accession number (e.g. 0001234567-24-012345) from text."""
    match = re.search(r"\d{10}-\d{2}-\d{6}", text)
    return match.group() if match else None


class SECEdgarScraperV2(BaseScraper):
    """Enhanced SEC EDGAR filing monitor using both EFTS search and RSS feeds.

    Improvements over v1:
    - Uses dynamic 24h date range instead of hardcoded dates
    - Dual-source: EFTS full-text search + RSS Atom feeds for reliability
    - Better Form 4 parsing with insider role detection
    - Rate limiting (max 10 req/s per SEC policy)
    - Proper deduplication via accession numbers
    """

    @property
    def platform(self) -> str:
        return "sec_edgar"

    def __init__(self, interval: int = 120) -> None:
        super().__init__(interval)
        self._seen_accessions: set[str] = set()

    # ── Rate-limited request helper ───────────────────────────────────

    async def _rate_limited_get(
        self,
        client: httpx.AsyncClient,
        url: str,
        *,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response | None:
        """Make a GET request with rate limiting. Returns None on failure."""
        await asyncio.sleep(_REQUEST_INTERVAL)
        try:
            resp = await client.get(url, params=params, headers=headers or _HEADERS)
            if resp.status_code == 429:
                logger.warning("[sec_edgar_v2] rate limited, backing off 5s")
                await asyncio.sleep(5)
                resp = await client.get(url, params=params, headers=headers or _HEADERS)
            return resp if resp.status_code == 200 else None
        except Exception:
            logger.warning("[sec_edgar_v2] request failed: %s", url, exc_info=True)
            return None

    # ── EFTS full-text search ─────────────────────────────────────────

    async def _fetch_efts_search(
        self, client: httpx.AsyncClient, form_cfg: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Query the EDGAR EFTS full-text search API for recent filings."""
        start_dt, end_dt = _date_range_last_24h()
        form_type = form_cfg["form"]
        query = form_cfg["search_query"]

        params = {
            "q": query,
            "forms": form_type,
            "dateRange": "custom",
            "startdt": start_dt,
            "enddt": end_dt,
        }

        resp = await self._rate_limited_get(client, _EFTS_SEARCH, params=params)
        if resp is None:
            return []

        posts: list[dict[str, Any]] = []
        try:
            data = resp.json()
            hits = data.get("hits", {}).get("hits", [])
        except Exception:
            logger.warning("[sec_edgar_v2] EFTS response parse failed for %s", form_type)
            return []

        for hit in hits[:25]:
            src = hit.get("_source", {})

            # Build accession number for dedup
            file_num = src.get("file_num", "")
            if isinstance(file_num, list):
                file_num = file_num[0] if file_num else ""
            accession = _extract_accession_number(hit.get("_id", "")) or file_num or self._generate_id()

            if accession in self._seen_accessions:
                continue
            self._seen_accessions.add(accession)

            display_names = src.get("display_names", [])
            company_name = display_names[0] if display_names else "Unknown"
            file_date = src.get("file_date", "")
            file_desc = src.get("file_description", "Filing")

            # Build content string
            content = f"[{form_type}] {company_name}: {file_desc}"

            # Build URL
            filing_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&filenum={file_num}&type={form_type}&dateb=&owner=include&count=10&action=getcompany"

            # Extra metadata for Form 4
            raw_meta: dict[str, Any] = {
                "form_type": form_type,
                "form_description": form_cfg["description"],
                "file_date": file_date,
                "company": company_name,
                "display_names": display_names,
                "file_description": file_desc,
                "significance_level": form_cfg["significance"],
                "source": "efts_search",
                "accession_number": accession,
            }

            # Form 4 specific: try to extract insider info from description
            if form_type == "4":
                raw_meta.update(self._parse_form4_metadata(file_desc, company_name))

            posts.append(self._make_post(
                source_id=accession,
                author=company_name,
                content=content,
                url=filing_url,
                raw_metadata=raw_meta,
                source_published_at=file_date if file_date else None,
            ))

        logger.debug("[sec_edgar_v2] EFTS returned %d new posts for %s", len(posts), form_type)
        return posts

    # ── RSS Atom feed ─────────────────────────────────────────────────

    async def _fetch_rss_feed(
        self, client: httpx.AsyncClient, form_cfg: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Fetch recent filings from the EDGAR RSS (Atom) feed.

        Uses feedparser if available, falls back to ElementTree.
        """
        form_type = form_cfg["form"]
        url = _EDGAR_RSS.format(form_type=form_type)

        resp = await self._rate_limited_get(client, url, headers=_HEADERS_XML)
        if resp is None:
            return []

        posts: list[dict[str, Any]] = []

        # Try feedparser first
        try:
            import feedparser
            feed = feedparser.parse(resp.text)
            entries = feed.entries[:25]
        except ImportError:
            entries = self._parse_atom_fallback(resp.text)
        except Exception:
            logger.warning("[sec_edgar_v2] feedparser failed for %s, trying fallback", form_type)
            entries = self._parse_atom_fallback(resp.text)

        for entry in entries:
            # feedparser objects or dicts from fallback
            if isinstance(entry, dict):
                entry_id = entry.get("id", "")
                title = entry.get("title", "Filing")
                link = entry.get("link", "")
                published = entry.get("published", "")
                summary = entry.get("summary", "")
                author = entry.get("author", "SEC")
            else:
                entry_id = getattr(entry, "id", "")
                title = getattr(entry, "title", "Filing")
                link = getattr(entry, "link", "")
                published = getattr(entry, "published", "")
                summary = getattr(entry, "summary", "")[:1000]
                author = getattr(entry, "author", "SEC")

            # Try to extract accession number from the entry ID or link
            accession = _extract_accession_number(entry_id) or _extract_accession_number(link) or self._generate_id()

            if accession in self._seen_accessions:
                continue
            self._seen_accessions.add(accession)

            significance = form_cfg["significance"]
            content = f"[{form_type}] {title}"

            raw_meta: dict[str, Any] = {
                "form_type": form_type,
                "form_description": form_cfg["description"],
                "published": published,
                "summary": summary[:500],
                "significance_level": significance,
                "source": "rss_feed",
                "accession_number": accession,
            }

            # Form 4: parse insider details from title/summary
            if form_type == "4":
                raw_meta.update(self._parse_form4_metadata(title + " " + summary, author))

            posts.append(self._make_post(
                source_id=accession,
                author=author,
                content=content,
                url=link or None,
                raw_metadata=raw_meta,
                source_published_at=published if published else None,
            ))

        logger.debug("[sec_edgar_v2] RSS returned %d new posts for %s", len(posts), form_type)
        return posts

    # ── Atom XML fallback parser ──────────────────────────────────────

    @staticmethod
    def _parse_atom_fallback(xml_text: str) -> list[dict[str, Any]]:
        """Parse Atom XML without feedparser using stdlib ElementTree."""
        entries: list[dict[str, Any]] = []
        try:
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            root = ElementTree.fromstring(xml_text)
            for entry_el in root.findall("atom:entry", ns)[:25]:
                entry: dict[str, Any] = {}
                title_el = entry_el.find("atom:title", ns)
                entry["title"] = title_el.text if title_el is not None and title_el.text else "Filing"

                id_el = entry_el.find("atom:id", ns)
                entry["id"] = id_el.text if id_el is not None and id_el.text else ""

                link_el = entry_el.find("atom:link", ns)
                entry["link"] = link_el.get("href", "") if link_el is not None else ""

                updated_el = entry_el.find("atom:updated", ns)
                entry["published"] = updated_el.text if updated_el is not None and updated_el.text else ""

                summary_el = entry_el.find("atom:summary", ns)
                entry["summary"] = summary_el.text[:1000] if summary_el is not None and summary_el.text else ""

                author_el = entry_el.find("atom:author/atom:name", ns)
                entry["author"] = author_el.text if author_el is not None and author_el.text else "SEC"

                entries.append(entry)
        except ElementTree.ParseError:
            logger.warning("[sec_edgar_v2] Atom XML parse failed")
        return entries

    # ── Form 4 metadata extraction ────────────────────────────────────

    @staticmethod
    def _parse_form4_metadata(text: str, filer_name: str = "") -> dict[str, Any]:
        """Extract Form 4 insider trading details from filing text/description.

        Tries to identify: reporting person, transaction type, shares, value,
        and whether it is a C-suite purchase exceeding $100K.
        """
        meta: dict[str, Any] = {}

        # Detect C-suite role
        csuite_match = _CSUITE_PATTERNS.search(text)
        if csuite_match:
            meta["insider_role"] = csuite_match.group()
        elif filer_name:
            csuite_filer = _CSUITE_PATTERNS.search(filer_name)
            if csuite_filer:
                meta["insider_role"] = csuite_filer.group()

        # Detect transaction type
        text_lower = text.lower()
        if "purchase" in text_lower or "bought" in text_lower or "acquired" in text_lower:
            meta["transaction_type"] = "purchase"
        elif "sale" in text_lower or "sold" in text_lower or "disposed" in text_lower:
            meta["transaction_type"] = "sale"
        elif "option" in text_lower or "exercise" in text_lower:
            meta["transaction_type"] = "option_exercise"
        elif "grant" in text_lower or "award" in text_lower:
            meta["transaction_type"] = "grant"

        # Try to extract share count
        shares_match = re.search(r"([\d,]+(?:\.\d+)?)\s*(?:shares|shs)", text, re.IGNORECASE)
        if shares_match:
            try:
                meta["shares"] = float(shares_match.group(1).replace(",", ""))
            except ValueError:
                pass

        # Try to extract dollar value
        value_match = re.search(r"\$\s*([\d,.]+)\s*([BMKbmk](?:illion|illion)?)?", text)
        if value_match:
            try:
                raw_val = float(value_match.group(1).replace(",", ""))
                multiplier_str = (value_match.group(2) or "").upper()
                if multiplier_str.startswith("B"):
                    raw_val *= 1_000_000_000
                elif multiplier_str.startswith("M"):
                    raw_val *= 1_000_000
                elif multiplier_str.startswith("K"):
                    raw_val *= 1_000
                meta["value_usd"] = raw_val
            except ValueError:
                pass

        # Flag critical C-suite purchases > $100K
        is_csuite = "insider_role" in meta
        is_purchase = meta.get("transaction_type") == "purchase"
        value = meta.get("value_usd", 0)
        if is_csuite and is_purchase and value > 100_000:
            meta["csuite_significant_purchase"] = True
            meta["override_significance"] = "critical"

        return meta

    # ── Main scrape method ────────────────────────────────────────────

    async def scrape(self) -> list[dict[str, Any]]:
        """Scrape all monitored form types via both EFTS search and RSS feeds."""
        all_posts: list[dict[str, Any]] = []

        async with httpx.AsyncClient(timeout=30) as client:
            for form_cfg in _FORM_CONFIG:
                # Fetch from both sources for better coverage
                rss_posts = await self._fetch_rss_feed(client, form_cfg)
                all_posts.extend(rss_posts)

                efts_posts = await self._fetch_efts_search(client, form_cfg)
                all_posts.extend(efts_posts)

        # Apply significance overrides (e.g. C-suite purchases)
        for post in all_posts:
            override = post.get("raw_metadata", {}).get("override_significance")
            if override:
                post["raw_metadata"]["significance_level"] = override

        logger.info(
            "[sec_edgar_v2] scrape complete: %d total filings (%d unique accessions tracked)",
            len(all_posts),
            len(self._seen_accessions),
        )
        return all_posts


# ── Mock ──────────────────────────────────────────────────────────────────

_MOCK_FILINGS: list[tuple[str, str, str, str, dict[str, Any]]] = [
    # Form 4 - Insider Trading (THE HIGHEST ALPHA)
    ("4", "Elon Musk", "TSLA", "Form 4: CEO sold 4.4M shares at $245.50. Proceeds: $1.08B", {
        "insider_role": "CEO", "transaction_type": "sale", "shares": 4_400_000, "value_usd": 1_080_000_000,
    }),
    ("4", "Tim Cook", "AAPL", "Form 4: CEO exercised options and sold 100K shares at $192.30", {
        "insider_role": "CEO", "transaction_type": "sale", "shares": 100_000, "value_usd": 19_230_000,
    }),
    ("4", "Jamie Dimon", "JPM", "Form 4: CEO sold $150M in shares — first sale in 18 years", {
        "insider_role": "CEO", "transaction_type": "sale", "shares": 862_000, "value_usd": 150_000_000,
    }),
    ("4", "Lisa Su", "AMD", "Form 4: CEO purchased 50K shares at $155.20 on open market", {
        "insider_role": "CEO", "transaction_type": "purchase", "shares": 50_000, "value_usd": 7_760_000,
        "csuite_significant_purchase": True, "override_significance": "critical",
    }),
    ("4", "Jensen Huang", "NVDA", "Form 4: CEO sold 600K shares at $725 under 10b5-1 plan", {
        "insider_role": "CEO", "transaction_type": "sale", "shares": 600_000, "value_usd": 435_000_000,
    }),
    ("4", "Satya Nadella", "MSFT", "Form 4: CEO sold 50% of holdings — 838K shares at $420", {
        "insider_role": "CEO", "transaction_type": "sale", "shares": 838_000, "value_usd": 351_960_000,
    }),
    ("4", "Andy Jassy", "AMZN", "Form 4: CEO purchased 10K shares at $185 on open market", {
        "insider_role": "CEO", "transaction_type": "purchase", "shares": 10_000, "value_usd": 1_850_000,
        "csuite_significant_purchase": True, "override_significance": "critical",
    }),
    ("4", "Mary Barra", "GM", "Form 4: CEO purchased 25K shares at $38.50", {
        "insider_role": "CEO", "transaction_type": "purchase", "shares": 25_000, "value_usd": 962_500,
        "csuite_significant_purchase": True, "override_significance": "critical",
    }),
    # 8-K - Material Events
    ("8-K", "Apple Inc.", "AAPL", "8-K: Company announces $100B share buyback program", {}),
    ("8-K", "Boeing Co.", "BA", "8-K: CEO resignation effective immediately due to quality concerns", {}),
    ("8-K", "Tesla Inc.", "TSLA", "8-K: Entry into new market — launching energy storage division in India", {}),
    ("8-K", "JPMorgan Chase", "JPM", "8-K: Acquisition of regional bank for $2.3B", {}),
    ("8-K", "Pfizer Inc.", "PFE", "8-K: FDA approval for new oncology drug; projected $5B peak sales", {}),
    ("8-K", "Intel Corp.", "INTC", "8-K: Announces $20B foundry investment with government subsidies", {}),
    # SC 13D - Activist Stakes
    ("SC 13D", "Elliott Management", "SWK", "SC 13D: Elliott discloses 8.2% activist stake in Stanley Black & Decker. Demanding board seats.", {
        "beneficial_ownership_pct": 8.2,
    }),
    ("SC 13D", "Carl Icahn", "IEP", "SC 13D: Icahn increases stake in Southwest Gas to 12.5%. Pushing for asset sales.", {
        "beneficial_ownership_pct": 12.5,
    }),
    ("SC 13D", "Starboard Value", "PFGC", "SC 13D: Starboard acquires 6.7% stake in Performance Food Group.", {
        "beneficial_ownership_pct": 6.7,
    }),
    ("SC 13D", "Third Point LLC", "INTC", "SC 13D: Dan Loeb takes 5.1% stake in Intel. Demands foundry spinoff.", {
        "beneficial_ownership_pct": 5.1,
    }),
    # SC 13G - Large Passive Stakes
    ("SC 13G", "Vanguard Group", "AAPL", "SC 13G: Vanguard increases passive stake in Apple to 8.9%. Largest institutional holder.", {
        "beneficial_ownership_pct": 8.9,
    }),
    ("SC 13G", "BlackRock Inc.", "MSFT", "SC 13G: BlackRock reports 7.2% ownership of Microsoft through index funds.", {
        "beneficial_ownership_pct": 7.2,
    }),
    # S-1 - IPO Filings
    ("S-1", "Databricks Inc.", "N/A", "S-1: IPO filing for Databricks. Revenue $1.6B, growing 55% YoY. Targeting $40B+ valuation.", {}),
    ("S-1", "Stripe Inc.", "N/A", "S-1: Stripe files for IPO. 2024 revenue $18B. Profitable for 2nd consecutive year.", {}),
    ("S-1", "Shein Group", "N/A", "S-1: Fast fashion giant Shein files for US IPO. Revenue $30B.", {}),
    # 13F-HR - Quarterly Institutional Holdings
    ("13F-HR", "Berkshire Hathaway", "BRK.B", "13F: New position in homebuilder NVR Inc. worth $800M", {}),
    ("13F-HR", "Bridgewater Associates", "N/A", "13F: Increased emerging market positions by 40%; reduced US tech exposure", {}),
    ("13F-HR", "Citadel Advisors", "N/A", "13F: Massive increase in NVDA position; new AI infrastructure bets", {}),
    ("13F-HR", "Soros Fund Management", "N/A", "13F: New $500M position in copper miners; exiting Chinese tech", {}),
    # DEFA14A - Proxy Fights
    ("DEFA14A", "Disney Co.", "DIS", "DEFA14A: Trian Partners files proxy solicitation for Disney board seats.", {}),
    ("DEFA14A", "Salesforce Inc.", "CRM", "DEFA14A: Activist investor launches proxy fight at Salesforce.", {}),
]


class MockEdgarScraperV2(BaseScraper):
    """Mock SEC EDGAR scraper with realistic filing data for development/testing."""

    @property
    def platform(self) -> str:
        return "sec_edgar"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(5, 15)
        posts: list[dict[str, Any]] = []

        for _ in range(count):
            form_type, company, ticker, content, extra_meta = random.choice(_MOCK_FILINGS)
            significance = _get_significance(form_type)

            # Apply override if present
            if extra_meta.get("override_significance"):
                significance = extra_meta["override_significance"]

            raw_metadata: dict[str, Any] = {
                "form_type": form_type,
                "form_description": next(
                    (cfg["description"] for cfg in _FORM_CONFIG if cfg["form"] == form_type),
                    form_type,
                ),
                "company": company,
                "ticker": ticker,
                "file_date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                "significance_level": significance,
                "source": "mock",
                "accession_number": f"0001234567-{random.randint(20, 26):02d}-{random.randint(100000, 999999)}",
            }
            raw_metadata.update(extra_meta)

            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}",
                author=company,
                content=content,
                url=f"https://www.sec.gov/cgi-bin/browse-edgar?company={company.replace(' ', '+')}&CIK=&type={form_type}",
                raw_metadata=raw_metadata,
            ))

        return posts
