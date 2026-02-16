"""ClinicalTrials.gov scraper — completed drug trials from free REST API."""

from __future__ import annotations

import logging
import random
from typing import Any

import httpx

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_CLINICAL_TRIALS_API = "https://clinicaltrials.gov/api/v2/studies"

# Map sponsor companies to tickers for market-relevant tagging
_PHARMA_TICKERS: dict[str, str] = {
    "pfizer": "PFE",
    "johnson & johnson": "JNJ",
    "j&j": "JNJ",
    "janssen": "JNJ",
    "eli lilly": "LLY",
    "lilly": "LLY",
    "merck": "MRK",
    "abbvie": "ABBV",
    "bristol-myers squibb": "BMY",
    "bristol myers": "BMY",
    "amgen": "AMGN",
    "gilead": "GILD",
    "regeneron": "REGN",
    "vertex": "VRTX",
    "moderna": "MRNA",
    "biogen": "BIIB",
    "novartis": "NVS",
    "roche": "RHHBY",
    "astrazeneca": "AZN",
    "sanofi": "SNY",
    "novo nordisk": "NVO",
    "gsk": "GSK",
    "glaxosmithkline": "GSK",
    "bayer": "BAYRY",
    "takeda": "TAK",
    "daiichi sankyo": "DSNKY",
    "incyte": "INCY",
    "alnylam": "ALNY",
    "seagen": "SGEN",
    "biomarin": "BMRN",
    "jazz": "JAZZ",
    "jazz pharmaceuticals": "JAZZ",
    "alexion": "AZN",
    "catalent": "CTLT",
    "crispr": "CRSP",
    "crispr therapeutics": "CRSP",
    "intellia": "NTLA",
    "intellia therapeutics": "NTLA",
}


def _sponsor_to_ticker(sponsor_name: str) -> str:
    """Try to map a sponsor name to a stock ticker. Returns empty string if not found."""
    lower_name = sponsor_name.lower()
    for company, ticker in _PHARMA_TICKERS.items():
        if company in lower_name:
            return ticker
    return ""


def _determine_significance(phases: list[str]) -> str:
    """Assign significance based on trial phase.

    Phase 3 completions are critical (market-moving).
    Phase 2 completions are high (pipeline signal).
    Everything else is medium.
    """
    for phase in phases:
        upper = phase.upper()
        if "3" in upper:
            return "critical"
    for phase in phases:
        upper = phase.upper()
        if "2" in upper:
            return "high"
    return "medium"


class ClinicalTrialsScraper(BaseScraper):
    """Monitors ClinicalTrials.gov for completed and near-completion drug trials."""

    @property
    def platform(self) -> str:
        return "clinical_trials"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._seen_nct_ids: set[str] = set()

    async def _fetch_studies(
        self,
        client: httpx.AsyncClient,
        status: str,
        page_size: int,
    ) -> list[dict[str, Any]]:
        """Fetch studies from ClinicalTrials.gov API v2 with the given status filter."""
        posts: list[dict[str, Any]] = []
        try:
            params = {
                "filter.overallStatus": status,
                "sort": "LastUpdatePostDate:desc",
                "pageSize": page_size,
                "format": "json",
            }
            resp = await client.get(_CLINICAL_TRIALS_API, params=params)
            if resp.status_code != 200:
                logger.warning(
                    "[clinical_trials] API returned %d for status=%s",
                    resp.status_code,
                    status,
                )
                return posts

            data = resp.json()
            studies = data.get("studies", [])

            for study in studies:
                protocol = study.get("protocolSection", {})

                # Identification
                id_module = protocol.get("identificationModule", {})
                nct_id = id_module.get("nctId", "")
                if not nct_id or nct_id in self._seen_nct_ids:
                    continue
                self._seen_nct_ids.add(nct_id)

                official_title = id_module.get("officialTitle", "")
                brief_title = id_module.get("briefTitle", "Untitled Study")
                title = official_title or brief_title

                # Sponsor
                sponsor_module = protocol.get("sponsorCollaboratorsModule", {})
                lead_sponsor = sponsor_module.get("leadSponsor", {})
                sponsor_name = lead_sponsor.get("name", "Unknown Sponsor")
                ticker = _sponsor_to_ticker(sponsor_name)

                # Conditions
                conditions_module = protocol.get("conditionsModule", {})
                conditions = conditions_module.get("conditions", [])

                # Design & Phase
                design_module = protocol.get("designModule", {})
                phases = design_module.get("phases", [])
                phase_str = ", ".join(phases) if phases else "N/A"

                # Enrollment
                enrollment_info = design_module.get("enrollmentInfo", {})
                enrollment = enrollment_info.get("count", "N/A")

                # Status dates
                status_module = protocol.get("statusModule", {})
                last_update = (
                    status_module.get("lastUpdatePostDateStruct", {}).get("date", "")
                )

                # Brief summary
                description_module = protocol.get("descriptionModule", {})
                brief_summary = description_module.get("briefSummary", "")

                significance = _determine_significance(phases)

                status_label = "Completed" if status == "COMPLETED" else "Nearing Completion"
                content = (
                    f"[Clinical Trial {status_label}] {title}\n"
                    f"Sponsor: {sponsor_name}"
                )
                if ticker:
                    content += f" (${ticker})"
                content += (
                    f"\nPhase: {phase_str} | Enrollment: {enrollment}\n"
                    f"Conditions: {', '.join(conditions[:3])}"
                )
                if brief_summary:
                    content += f"\n\n{brief_summary[:500]}"

                posts.append(self._make_post(
                    source_id=nct_id,
                    author=sponsor_name,
                    content=content[:3000],
                    url=f"https://clinicaltrials.gov/study/{nct_id}",
                    source_published_at=last_update,
                    raw_metadata={
                        "nct_id": nct_id,
                        "title": title[:500],
                        "brief_title": brief_title,
                        "sponsor": sponsor_name,
                        "ticker": ticker,
                        "phase": phase_str,
                        "phases_raw": phases,
                        "enrollment": enrollment,
                        "conditions": conditions[:5],
                        "overall_status": status,
                        "last_update": last_update,
                        "significance": significance,
                    },
                ))

        except Exception:
            logger.warning(
                "[clinical_trials] failed to fetch studies (status=%s)",
                status,
                exc_info=True,
            )
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            # Completed trials (primary interest)
            completed = await self._fetch_studies(client, "COMPLETED", page_size=20)
            all_posts.extend(completed)

            # Active but not recruiting (nearing completion — pipeline signal)
            nearing = await self._fetch_studies(
                client, "ACTIVE_NOT_RECRUITING", page_size=10
            )
            all_posts.extend(nearing)

        # Cap seen IDs to prevent unbounded memory growth
        if len(self._seen_nct_ids) > 5000:
            self._seen_nct_ids = set(list(self._seen_nct_ids)[-2500:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_TRIALS = [
    (
        "NCT05012345",
        "A Phase 3, Randomized Study of Donanemab in Early Alzheimer's Disease",
        "Eli Lilly and Company",
        "LLY",
        ["PHASE3"],
        "Alzheimer's Disease",
        1736,
        "critical",
        "COMPLETED",
    ),
    (
        "NCT05023456",
        "Phase 2 Study of mRNA-4157 Combined With Pembrolizumab in Melanoma",
        "Moderna, Inc.",
        "MRNA",
        ["PHASE2"],
        "Melanoma",
        157,
        "high",
        "COMPLETED",
    ),
    (
        "NCT05034567",
        "KEYNOTE-789: Pembrolizumab Plus Chemotherapy in Non-Small Cell Lung Cancer",
        "Merck Sharp & Dohme LLC",
        "MRK",
        ["PHASE3"],
        "Non-Small Cell Lung Cancer",
        920,
        "critical",
        "COMPLETED",
    ),
    (
        "NCT05045678",
        "Phase 3 Study of Dupixent in Chronic Obstructive Pulmonary Disease",
        "Regeneron Pharmaceuticals",
        "REGN",
        ["PHASE3"],
        "COPD",
        935,
        "critical",
        "ACTIVE_NOT_RECRUITING",
    ),
    (
        "NCT05056789",
        "Phase 2 Study of Oral GLP-1 Receptor Agonist in Type 2 Diabetes",
        "Novo Nordisk A/S",
        "NVO",
        ["PHASE2"],
        "Type 2 Diabetes Mellitus",
        480,
        "high",
        "COMPLETED",
    ),
    (
        "NCT05067890",
        "CRISPR-Based Gene Editing for Sickle Cell Disease (CLIMB-131)",
        "CRISPR Therapeutics AG",
        "CRSP",
        ["PHASE3"],
        "Sickle Cell Disease",
        45,
        "critical",
        "COMPLETED",
    ),
    (
        "NCT05078901",
        "Phase 2/3 Study of Lecanemab in Early Alzheimer's Disease — CLARITY AD Extension",
        "Biogen",
        "BIIB",
        ["PHASE2", "PHASE3"],
        "Alzheimer's Disease",
        1795,
        "critical",
        "ACTIVE_NOT_RECRUITING",
    ),
    (
        "NCT05089012",
        "Phase 3 Trial of Resmetirom for NASH (MAESTRO-NASH)",
        "Gilead Sciences",
        "GILD",
        ["PHASE3"],
        "Nonalcoholic Steatohepatitis",
        966,
        "critical",
        "COMPLETED",
    ),
    (
        "NCT05090123",
        "Phase 2 Study of ADC Targeting HER2-Low Breast Cancer",
        "AstraZeneca",
        "AZN",
        ["PHASE2"],
        "Breast Cancer",
        557,
        "high",
        "COMPLETED",
    ),
    (
        "NCT05101234",
        "Phase 3 Study of RSV Vaccine in Older Adults (AREXVY)",
        "GlaxoSmithKline",
        "GSK",
        ["PHASE3"],
        "Respiratory Syncytial Virus",
        24966,
        "critical",
        "COMPLETED",
    ),
    (
        "NCT05112345",
        "Phase 2 Trial of Bispecific Antibody in Multiple Myeloma",
        "Amgen Inc.",
        "AMGN",
        ["PHASE2"],
        "Multiple Myeloma",
        295,
        "high",
        "ACTIVE_NOT_RECRUITING",
    ),
    (
        "NCT05123456",
        "Phase 3 Study of Oral PCSK9 Inhibitor for Hypercholesterolemia",
        "Pfizer Inc.",
        "PFE",
        ["PHASE3"],
        "Hypercholesterolemia",
        1250,
        "critical",
        "COMPLETED",
    ),
]


class MockClinicalTrialsScraper(BaseScraper):
    """Mock scraper generating realistic clinical trial completion data."""

    @property
    def platform(self) -> str:
        return "clinical_trials"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(6, 12)
        posts: list[dict[str, Any]] = []
        selected = random.sample(_MOCK_TRIALS, min(count, len(_MOCK_TRIALS)))
        for (
            nct_id,
            title,
            sponsor,
            ticker,
            phases,
            condition,
            enrollment,
            significance,
            status,
        ) in selected:
            phase_str = ", ".join(phases)
            status_label = "Completed" if status == "COMPLETED" else "Nearing Completion"
            content = (
                f"[Clinical Trial {status_label}] {title}\n"
                f"Sponsor: {sponsor}"
            )
            if ticker:
                content += f" (${ticker})"
            content += (
                f"\nPhase: {phase_str} | Enrollment: {enrollment}\n"
                f"Conditions: {condition}"
            )

            posts.append(self._make_post(
                source_id=nct_id,
                author=sponsor,
                content=content,
                url=f"https://clinicaltrials.gov/study/{nct_id}",
                raw_metadata={
                    "nct_id": nct_id,
                    "title": title,
                    "brief_title": title.split(" — ")[0] if " — " in title else title,
                    "sponsor": sponsor,
                    "ticker": ticker,
                    "phase": phase_str,
                    "phases_raw": phases,
                    "enrollment": enrollment,
                    "conditions": [condition],
                    "overall_status": status,
                    "last_update": "2026-02-05",
                    "significance": significance,
                },
            ))
        return posts
