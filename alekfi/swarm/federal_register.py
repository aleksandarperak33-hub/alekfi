"""Federal Register scraper — tracks proposed rules and regulations affecting public companies."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.utils import RateLimiter
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_API_BASE = "https://www.federalregister.gov/api/v1"

_DOCUMENT_TYPES = ["RULE", "PRORULE"]  # RULE and PROPOSED RULE

_TRACKED_AGENCIES = [
    "securities-and-exchange-commission",        # SEC
    "federal-trade-commission",                  # FTC
    "food-and-drug-administration",              # FDA
    "environmental-protection-agency",           # EPA
    "justice-department",                        # DOJ
    "federal-communications-commission",         # FCC
    "consumer-financial-protection-bureau",      # CFPB
    "transportation-department",                 # DOT
    "energy-department",                         # DOE
]

_AGENCY_LABELS = {
    "securities-and-exchange-commission": "SEC",
    "federal-trade-commission": "FTC",
    "food-and-drug-administration": "FDA",
    "environmental-protection-agency": "EPA",
    "justice-department": "DOJ",
    "federal-communications-commission": "FCC",
    "consumer-financial-protection-bureau": "CFPB",
    "transportation-department": "DOT",
    "energy-department": "DOE",
}

_KEYWORDS = [
    "financial", "securities", "banking", "pharmaceutical",
    "technology", "energy", "telecommunications", "antitrust",
    "merger", "acquisition",
]


def _match_keywords(text: str) -> list[str]:
    """Return matching keywords found in text (case-insensitive)."""
    lower_text = text.lower()
    return [kw for kw in _KEYWORDS if kw.lower() in lower_text]


def _extract_significance(doc: dict[str, Any]) -> str:
    """Determine document significance level from metadata."""
    if doc.get("significant"):
        return "significant"
    doc_type = doc.get("type", "")
    if doc_type == "Rule":
        return "final_rule"
    if doc_type == "Proposed Rule":
        return "proposed_rule"
    return "standard"


class FederalRegisterScraper(BaseScraper):
    """Fetches new rules and proposed rules from the Federal Register API."""

    @property
    def platform(self) -> str:
        return "federal_register"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=10, period=60)
        self._seen_doc_numbers: set[str] = set()

    async def _fetch_documents_by_type(
        self, client: httpx.AsyncClient, doc_type: str
    ) -> list[dict[str, Any]]:
        """Fetch documents of a specific type from the Federal Register API."""
        posts: list[dict[str, Any]] = []
        try:
            params = {
                "conditions[type][]": doc_type,
                "per_page": 20,
                "order": "newest",
                "fields[]": [
                    "document_number", "title", "abstract", "type",
                    "agencies", "html_url", "publication_date",
                    "docket_ids", "significant",
                ],
            }
            async with self._rate_limiter:
                resp = await client.get(f"{_API_BASE}/documents.json", params=params)
            if resp.status_code != 200:
                logger.debug("[federal_register] type=%s returned %d", doc_type, resp.status_code)
                return posts

            data = resp.json()
            results = data.get("results", [])

            for doc in results:
                doc_number = doc.get("document_number", "")
                if not doc_number or doc_number in self._seen_doc_numbers:
                    continue

                title = doc.get("title", "Untitled")
                abstract = doc.get("abstract", "") or ""
                full_text = f"{title} {abstract}"
                matched_kw = _match_keywords(full_text)

                if not matched_kw:
                    continue

                self._seen_doc_numbers.add(doc_number)

                agencies = doc.get("agencies", [])
                agency_names = [a.get("name", "Unknown") for a in agencies] if agencies else ["Unknown"]
                agency_str = ", ".join(agency_names)
                html_url = doc.get("html_url", f"https://www.federalregister.gov/d/{doc_number}")
                pub_date = doc.get("publication_date", "")
                docket_ids = doc.get("docket_ids", []) or []
                significance = _extract_significance(doc)

                content = f"[Federal Register - {agency_str}] {title}"
                if abstract:
                    content += f"\n\n{abstract[:2000]}"

                posts.append(self._make_post(
                    source_id=f"fr_{doc_number}",
                    author=agency_str,
                    content=content[:3000],
                    url=html_url,
                    source_published_at=pub_date,
                    raw_metadata={
                        "agency": agency_str,
                        "agency_slugs": [a.get("slug", "") for a in agencies],
                        "document_number": doc_number,
                        "document_type": doc.get("type", ""),
                        "publication_date": pub_date,
                        "docket_ids": docket_ids,
                        "significance": significance,
                        "matched_keywords": matched_kw,
                        "title": title,
                        "abstract": abstract[:1000],
                    },
                ))
        except Exception:
            logger.warning("[federal_register] failed to fetch type=%s", doc_type, exc_info=True)
        return posts

    async def _fetch_agency_documents(
        self, client: httpx.AsyncClient, agency_slug: str
    ) -> list[dict[str, Any]]:
        """Fetch recent documents from a specific agency."""
        posts: list[dict[str, Any]] = []
        try:
            params = {
                "conditions[agencies][]": agency_slug,
                "per_page": 10,
                "order": "newest",
                "fields[]": [
                    "document_number", "title", "abstract", "type",
                    "agencies", "html_url", "publication_date",
                    "docket_ids", "significant",
                ],
            }
            async with self._rate_limiter:
                resp = await client.get(f"{_API_BASE}/documents.json", params=params)
            if resp.status_code != 200:
                return posts

            data = resp.json()
            results = data.get("results", [])

            for doc in results:
                doc_number = doc.get("document_number", "")
                if not doc_number or doc_number in self._seen_doc_numbers:
                    continue

                title = doc.get("title", "Untitled")
                abstract = doc.get("abstract", "") or ""
                full_text = f"{title} {abstract}"
                matched_kw = _match_keywords(full_text)

                if not matched_kw:
                    continue

                self._seen_doc_numbers.add(doc_number)

                agencies = doc.get("agencies", [])
                agency_names = [a.get("name", "Unknown") for a in agencies] if agencies else ["Unknown"]
                agency_str = ", ".join(agency_names)
                agency_label = _AGENCY_LABELS.get(agency_slug, agency_slug.upper())
                html_url = doc.get("html_url", f"https://www.federalregister.gov/d/{doc_number}")
                pub_date = doc.get("publication_date", "")
                docket_ids = doc.get("docket_ids", []) or []
                significance = _extract_significance(doc)

                content = f"[Federal Register - {agency_label}] {title}"
                if abstract:
                    content += f"\n\n{abstract[:2000]}"

                posts.append(self._make_post(
                    source_id=f"fr_{doc_number}",
                    author=agency_str,
                    content=content[:3000],
                    url=html_url,
                    source_published_at=pub_date,
                    raw_metadata={
                        "agency": agency_str,
                        "agency_label": agency_label,
                        "agency_slug": agency_slug,
                        "document_number": doc_number,
                        "document_type": doc.get("type", ""),
                        "publication_date": pub_date,
                        "docket_ids": docket_ids,
                        "significance": significance,
                        "matched_keywords": matched_kw,
                        "title": title,
                        "abstract": abstract[:1000],
                    },
                ))
        except Exception:
            logger.warning("[federal_register] failed for agency %s", agency_slug, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            # Fetch by document type (RULE, PROPOSED RULE)
            for doc_type in _DOCUMENT_TYPES:
                posts = await self._fetch_documents_by_type(client, doc_type)
                all_posts.extend(posts)

            # Fetch by tracked agency
            for agency_slug in _TRACKED_AGENCIES:
                posts = await self._fetch_agency_documents(client, agency_slug)
                all_posts.extend(posts)

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_FEDERAL_DOCS = [
    (
        "2025-01234",
        "SEC",
        "Securities and Exchange Commission",
        "Rule",
        "Enhanced Disclosure Requirements for AI-Driven Trading Systems",
        "The SEC is proposing amendments to Regulation NMS to require broker-dealers using artificial intelligence and machine learning algorithms for securities trading to provide enhanced disclosures to customers and regulators about the nature, scope, and risks of such systems.",
        ["securities", "technology"],
        "significant",
        ["SEC-2025-0042"],
    ),
    (
        "2025-01567",
        "FTC",
        "Federal Trade Commission",
        "Proposed Rule",
        "Antitrust Guidelines for Technology Platform Mergers and Acquisitions",
        "The FTC proposes updated merger guidelines specifically addressing acquisitions by dominant technology platforms, establishing new thresholds for review and presumptions regarding competitive harm in digital markets.",
        ["antitrust", "merger", "acquisition", "technology"],
        "significant",
        ["FTC-2025-0018"],
    ),
    (
        "2025-02345",
        "FDA",
        "Food and Drug Administration",
        "Rule",
        "Accelerated Approval Pathway for Gene Therapy Products",
        "FDA finalizes rule establishing expedited review process for gene therapy products targeting rare diseases with unmet medical needs, including revised pharmaceutical manufacturing standards.",
        ["pharmaceutical"],
        "final_rule",
        ["FDA-2025-N-0033"],
    ),
    (
        "2025-03456",
        "EPA",
        "Environmental Protection Agency",
        "Proposed Rule",
        "Revised Emissions Standards for Natural Gas Power Generation Facilities",
        "EPA proposes stricter methane and CO2 emissions limits for natural gas-fired power plants, with significant implications for energy sector capital expenditure requirements.",
        ["energy"],
        "proposed_rule",
        ["EPA-HQ-OAR-2025-0012"],
    ),
    (
        "2025-04567",
        "CFPB",
        "Consumer Financial Protection Bureau",
        "Rule",
        "Open Banking Data Sharing Requirements for Financial Institutions",
        "CFPB finalizes rules requiring banks and financial institutions to provide consumers with standardized, machine-readable access to their financial data, enabling seamless account portability.",
        ["financial", "banking"],
        "significant",
        ["CFPB-2025-0007"],
    ),
    (
        "2025-05678",
        "FCC",
        "Federal Communications Commission",
        "Proposed Rule",
        "Spectrum Allocation Framework for Next-Generation Telecommunications",
        "FCC proposes reallocation of mid-band spectrum to support 6G research and deployment, with implications for existing telecommunications providers and technology companies.",
        ["telecommunications", "technology"],
        "proposed_rule",
        ["FCC-25-42"],
    ),
    (
        "2025-06789",
        "DOJ",
        "Department of Justice",
        "Rule",
        "Revised Merger Review Procedures for Banking Sector Consolidation",
        "DOJ updates its bank merger review framework, establishing stricter scrutiny for acquisitions that would create institutions with assets exceeding $100 billion.",
        ["banking", "merger", "acquisition"],
        "significant",
        ["DOJ-AG-2025-0003"],
    ),
    (
        "2025-07890",
        "SEC",
        "Securities and Exchange Commission",
        "Proposed Rule",
        "Climate-Related Financial Risk Disclosure for Public Companies",
        "SEC proposes mandatory climate risk disclosure requirements for publicly traded companies, including Scope 1, 2, and 3 greenhouse gas emissions reporting and financial impact assessments.",
        ["securities", "financial"],
        "significant",
        ["SEC-2025-0098"],
    ),
    (
        "2025-08901",
        "DOE",
        "Department of Energy",
        "Rule",
        "Energy Efficiency Standards for Commercial Data Center Equipment",
        "DOE establishes minimum energy efficiency standards for servers, storage systems, and cooling equipment used in commercial data centers, affecting major technology companies.",
        ["energy", "technology"],
        "final_rule",
        ["DOE-EE-2025-0015"],
    ),
    (
        "2025-09012",
        "DOT",
        "Department of Transportation",
        "Proposed Rule",
        "Autonomous Vehicle Safety Standards and Certification Requirements",
        "DOT proposes comprehensive safety certification framework for Level 4 and Level 5 autonomous vehicles, with new testing requirements and liability standards affecting technology and automotive companies.",
        ["technology"],
        "proposed_rule",
        ["DOT-NHTSA-2025-0044"],
    ),
]


class MockFederalRegisterScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "federal_register"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(5, 10)
        posts: list[dict[str, Any]] = []
        selected = random.sample(_MOCK_FEDERAL_DOCS, min(count, len(_MOCK_FEDERAL_DOCS)))
        for doc_number, agency_label, agency_name, doc_type, title, abstract, keywords, significance, docket_ids in selected:
            content = f"[Federal Register - {agency_label}] {title}\n\n{abstract}"
            posts.append(self._make_post(
                source_id=f"fr_{doc_number}",
                author=agency_name,
                content=content,
                url=f"https://www.federalregister.gov/d/{doc_number}",
                source_published_at="2025-01-15",
                raw_metadata={
                    "agency": agency_name,
                    "agency_label": agency_label,
                    "document_number": doc_number,
                    "document_type": doc_type,
                    "publication_date": "2025-01-15",
                    "docket_ids": docket_ids,
                    "significance": significance,
                    "matched_keywords": keywords,
                    "title": title,
                    "abstract": abstract,
                },
            ))
        return posts
