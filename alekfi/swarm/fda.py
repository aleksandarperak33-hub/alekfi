"""FDA scraper — drug approvals, warning letters, adverse events, and clinical trial results."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from typing import Any

import feedparser
import httpx

from alekfi.utils import RateLimiter
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# FDA RSS feeds for drug approvals and safety communications
_FDA_RSS_FEEDS = {
    "drug_approvals": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drugs/rss.xml",
    "drug_safety": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/drug-safety/rss.xml",
    "medical_devices": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/medical-devices/rss.xml",
    "press_releases": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/press-releases/rss.xml",
    "recalls": "https://www.fda.gov/about-fda/contact-fda/stay-informed/rss-feeds/recalls/rss.xml",
}

# FDA adverse events API
_ADVERSE_EVENTS_URL = "https://api.fda.gov/drug/event.json"

# ClinicalTrials.gov API v2
_CLINICAL_TRIALS_URL = "https://clinicaltrials.gov/api/v2/studies"

# Known pharma/biotech companies for tagging
_KNOWN_COMPANIES = {
    "pfizer": "PFE",
    "johnson & johnson": "JNJ",
    "j&j": "JNJ",
    "merck": "MRK",
    "abbvie": "ABBV",
    "eli lilly": "LLY",
    "lilly": "LLY",
    "bristol-myers squibb": "BMY",
    "bristol myers": "BMY",
    "amgen": "AMGN",
    "gilead": "GILD",
    "regeneron": "REGN",
    "moderna": "MRNA",
    "biontech": "BNTX",
    "novartis": "NVS",
    "roche": "RHHBY",
    "astrazeneca": "AZN",
    "sanofi": "SNY",
    "gsk": "GSK",
    "glaxosmithkline": "GSK",
    "novo nordisk": "NVO",
    "vertex": "VRTX",
    "biogen": "BIIB",
    "illumina": "ILMN",
    "intuitive surgical": "ISRG",
    "edwards lifesciences": "EW",
    "danaher": "DHR",
    "thermo fisher": "TMO",
    "abbott": "ABT",
}


def _identify_company(text: str) -> tuple[str, str]:
    """Try to identify a pharma company from text. Returns (company_name, ticker)."""
    lower_text = text.lower()
    for company, ticker in _KNOWN_COMPANIES.items():
        if company in lower_text:
            return company.title(), ticker
    return "", ""


def _hash_text(text: str) -> str:
    """Generate a short hash from text for deduplication."""
    return hashlib.sha256(text.encode()).hexdigest()[:12]


class FDAScraper(BaseScraper):
    """Monitors FDA drug approvals, adverse events, and clinical trial results."""

    @property
    def platform(self) -> str:
        return "fda"

    def __init__(self, interval: int = 3600) -> None:
        super().__init__(interval)
        self._rate_limiter = RateLimiter(max_calls=10, period=60)
        self._seen_ids: set[str] = set()

    def _parse_rss_feed(self, feed_name: str, feed_url: str) -> list[dict[str, Any]]:
        """Parse an FDA RSS feed and extract posts."""
        posts: list[dict[str, Any]] = []
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:15]:
                link = getattr(entry, "link", "") or ""
                title = getattr(entry, "title", "No title")
                summary = getattr(entry, "summary", "")[:2000]
                published = getattr(entry, "published", "")

                entry_id = _hash_text(link or title)
                if entry_id in self._seen_ids:
                    continue
                self._seen_ids.add(entry_id)

                full_text = f"{title} {summary}"
                company_name, ticker = _identify_company(full_text)

                # Determine event type from feed name
                event_type_map = {
                    "drug_approvals": "approval",
                    "drug_safety": "safety_communication",
                    "medical_devices": "device_action",
                    "press_releases": "press_release",
                    "recalls": "recall",
                }
                event_type = event_type_map.get(feed_name, "general")

                content = f"[FDA - {event_type.replace('_', ' ').title()}] {title}"
                if summary:
                    content += f"\n\n{summary}"

                posts.append(self._make_post(
                    source_id=f"fda_rss_{entry_id}",
                    author="FDA",
                    content=content[:3000],
                    url=link or f"https://www.fda.gov",
                    source_published_at=published,
                    raw_metadata={
                        "event_type": event_type,
                        "feed": feed_name,
                        "title": title,
                        "summary": summary[:500],
                        "drug_name": "",
                        "company": company_name,
                        "ticker": ticker,
                        "significance": "high" if event_type in ("approval", "recall") else "medium",
                    },
                ))
        except Exception:
            logger.warning("[fda] failed to parse RSS feed %s", feed_name, exc_info=True)
        return posts

    async def _fetch_adverse_events(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        """Fetch recent adverse events from FDA drug event API."""
        posts: list[dict[str, Any]] = []
        try:
            params = {
                "limit": 20,
                "sort": "receivedate:desc",
            }
            async with self._rate_limiter:
                resp = await client.get(_ADVERSE_EVENTS_URL, params=params)
            if resp.status_code != 200:
                logger.debug("[fda] adverse events API returned %d", resp.status_code)
                return posts

            data = resp.json()
            results = data.get("results", [])

            for event in results:
                safety_report_id = event.get("safetyreportid", self._generate_id())
                if safety_report_id in self._seen_ids:
                    continue
                self._seen_ids.add(safety_report_id)

                # Extract drug info
                drugs = event.get("patient", {}).get("drug", [])
                drug_names = []
                for drug in drugs[:3]:
                    name = drug.get("medicinalproduct", "")
                    if name:
                        drug_names.append(name)

                # Extract reactions
                reactions = event.get("patient", {}).get("reaction", [])
                reaction_texts = [r.get("reactionmeddrapt", "") for r in reactions[:5] if r.get("reactionmeddrapt")]

                # Extract outcome
                serious = event.get("serious", "0")
                receive_date = event.get("receivedate", "")

                drug_str = ", ".join(drug_names) if drug_names else "Unknown Drug"
                reaction_str = ", ".join(reaction_texts) if reaction_texts else "Unknown Reaction"

                full_text = f"{drug_str} {reaction_str}"
                company_name, ticker = _identify_company(full_text)

                content = f"[FDA Adverse Event] Drug(s): {drug_str}\nReaction(s): {reaction_str}\nSerious: {'Yes' if serious == '1' else 'No'}"

                posts.append(self._make_post(
                    source_id=f"fda_ae_{safety_report_id}",
                    author="FDA",
                    content=content[:3000],
                    url=f"https://api.fda.gov/drug/event.json?search=safetyreportid:{safety_report_id}",
                    source_published_at=receive_date,
                    raw_metadata={
                        "event_type": "adverse_event",
                        "safety_report_id": safety_report_id,
                        "drug_names": drug_names,
                        "reactions": reaction_texts,
                        "serious": serious == "1",
                        "receive_date": receive_date,
                        "company": company_name,
                        "ticker": ticker,
                        "significance": "high" if serious == "1" else "medium",
                    },
                ))
        except Exception:
            logger.warning("[fda] failed to fetch adverse events", exc_info=True)
        return posts

    async def _fetch_clinical_trials(self, client: httpx.AsyncClient) -> list[dict[str, Any]]:
        """Fetch recently completed clinical trials from ClinicalTrials.gov."""
        posts: list[dict[str, Any]] = []
        try:
            params = {
                "filter.overallStatus": "COMPLETED",
                "sort": "LastUpdatePostDate:desc",
                "pageSize": 20,
                "format": "json",
            }
            async with self._rate_limiter:
                resp = await client.get(_CLINICAL_TRIALS_URL, params=params)
            if resp.status_code != 200:
                logger.debug("[fda] clinical trials API returned %d", resp.status_code)
                return posts

            data = resp.json()
            studies = data.get("studies", [])

            for study in studies:
                protocol = study.get("protocolSection", {})
                id_module = protocol.get("identificationModule", {})
                nct_id = id_module.get("nctId", "")
                if not nct_id or nct_id in self._seen_ids:
                    continue
                self._seen_ids.add(nct_id)

                title = id_module.get("officialTitle", "") or id_module.get("briefTitle", "Untitled Study")
                brief_summary = protocol.get("descriptionModule", {}).get("briefSummary", "")

                # Extract sponsor
                sponsor_module = protocol.get("sponsorCollaboratorsModule", {})
                lead_sponsor = sponsor_module.get("leadSponsor", {})
                sponsor_name = lead_sponsor.get("name", "Unknown Sponsor")

                # Extract conditions and interventions
                conditions_module = protocol.get("conditionsModule", {})
                conditions = conditions_module.get("conditions", [])
                interventions_module = protocol.get("armsInterventionsModule", {})
                interventions = interventions_module.get("interventions", [])
                intervention_names = [i.get("name", "") for i in interventions[:3] if i.get("name")]

                # Extract phase
                design_module = protocol.get("designModule", {})
                phases = design_module.get("phases", [])
                phase_str = ", ".join(phases) if phases else "N/A"

                # Extract enrollment
                enrollment_info = design_module.get("enrollmentInfo", {})
                enrollment = enrollment_info.get("count", "N/A")

                # Extract status dates
                status_module = protocol.get("statusModule", {})
                last_update = status_module.get("lastUpdatePostDateStruct", {}).get("date", "")

                full_text = f"{title} {brief_summary} {sponsor_name}"
                company_name, ticker = _identify_company(full_text)
                if not company_name:
                    company_name = sponsor_name

                content = (
                    f"[Clinical Trial Completed] {title}\n"
                    f"Sponsor: {sponsor_name}\n"
                    f"Phase: {phase_str} | Enrollment: {enrollment}\n"
                    f"Conditions: {', '.join(conditions[:3])}\n"
                    f"Interventions: {', '.join(intervention_names)}"
                )

                posts.append(self._make_post(
                    source_id=f"fda_ct_{nct_id}",
                    author="FDA",
                    content=content[:3000],
                    url=f"https://clinicaltrials.gov/study/{nct_id}",
                    source_published_at=last_update,
                    raw_metadata={
                        "event_type": "trial_result",
                        "nct_id": nct_id,
                        "title": title[:500],
                        "sponsor": sponsor_name,
                        "phase": phase_str,
                        "enrollment": enrollment,
                        "conditions": conditions[:5],
                        "interventions": intervention_names,
                        "drug_name": intervention_names[0] if intervention_names else "",
                        "company": company_name,
                        "ticker": ticker,
                        "significance": "high" if "PHASE3" in phase_str.upper() or "PHASE 3" in phase_str.upper() else "medium",
                    },
                ))
        except Exception:
            logger.warning("[fda] failed to fetch clinical trials", exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        # Parse RSS feeds (synchronous feedparser, run in executor)
        loop = asyncio.get_running_loop()
        for feed_name, feed_url in _FDA_RSS_FEEDS.items():
            posts = await loop.run_in_executor(None, self._parse_rss_feed, feed_name, feed_url)
            all_posts.extend(posts)

        # Fetch from APIs
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            adverse_posts = await self._fetch_adverse_events(client)
            all_posts.extend(adverse_posts)

            trial_posts = await self._fetch_clinical_trials(client)
            all_posts.extend(trial_posts)

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_FDA_ACTIONS = [
    (
        "approval",
        "FDA Approves Eli Lilly's Donanemab for Early Alzheimer's Disease",
        "The FDA has granted full approval to Eli Lilly's donanemab (Kisunla) for the treatment of early symptomatic Alzheimer's disease, making it the second anti-amyloid therapy to receive full approval.",
        "Donanemab (Kisunla)",
        "Eli Lilly",
        "LLY",
        "high",
    ),
    (
        "approval",
        "FDA Grants Accelerated Approval to Pfizer's KRAZATI for KRAS G12C NSCLC",
        "Pfizer receives accelerated approval for adagrasib (KRAZATI) for adult patients with KRAS G12C-mutated locally advanced or metastatic non-small cell lung cancer.",
        "Adagrasib (KRAZATI)",
        "Pfizer",
        "PFE",
        "high",
    ),
    (
        "safety_communication",
        "FDA Issues Warning on Novo Nordisk's Ozempic: Rare Intestinal Blockage Risk",
        "FDA updates labeling for semaglutide (Ozempic, Wegovy) to include warning about risk of intestinal obstruction (ileus) based on post-marketing adverse event reports.",
        "Semaglutide (Ozempic/Wegovy)",
        "Novo Nordisk",
        "NVO",
        "high",
    ),
    (
        "trial_result",
        "Moderna's mRNA Cancer Vaccine Shows 44% Reduction in Melanoma Recurrence in Phase 3",
        "Phase 3 KEYNOTE-942 trial demonstrates that Moderna's personalized cancer vaccine mRNA-4157 (V940) combined with Keytruda significantly reduces risk of melanoma recurrence or death.",
        "mRNA-4157 (V940)",
        "Moderna",
        "MRNA",
        "high",
    ),
    (
        "adverse_event",
        "Spike in Adverse Event Reports for AbbVie's Rinvoq in Rheumatoid Arthritis Patients",
        "FDA notes a significant increase in serious adverse event reports for upadacitinib (Rinvoq), including cardiovascular events and malignancies, prompting additional safety review.",
        "Upadacitinib (Rinvoq)",
        "AbbVie",
        "ABBV",
        "high",
    ),
    (
        "recall",
        "FDA Recalls Contaminated Batch of Generic Metformin from Sun Pharmaceutical",
        "Voluntary recall initiated for metformin HCl extended-release tablets due to NDMA impurity levels exceeding acceptable daily intake limits.",
        "Metformin HCl ER",
        "Sun Pharmaceutical",
        "",
        "medium",
    ),
    (
        "approval",
        "FDA Approves Vertex's CASGEVY — First CRISPR Gene Therapy for Sickle Cell Disease",
        "Historic FDA approval for exagamglogene autotemcel (CASGEVY), the first CRISPR-based gene editing therapy, for treatment of sickle cell disease in patients 12 and older.",
        "Exagamglogene autotemcel (CASGEVY)",
        "Vertex Pharmaceuticals",
        "VRTX",
        "high",
    ),
    (
        "trial_result",
        "Bristol Myers Squibb's Opdivo + Yervoy Combination Improves Overall Survival in Mesothelioma",
        "Updated Phase 3 data shows nivolumab plus ipilimumab significantly extends overall survival versus chemotherapy in unresectable malignant pleural mesothelioma.",
        "Nivolumab + Ipilimumab",
        "Bristol-Myers Squibb",
        "BMY",
        "medium",
    ),
    (
        "safety_communication",
        "FDA Strengthens Warning on Johnson & Johnson's Xarelto Bleeding Risks",
        "FDA mandates updated boxed warning for rivaroxaban (Xarelto) regarding increased risk of fatal bleeding events, particularly in elderly patients with renal impairment.",
        "Rivaroxaban (Xarelto)",
        "Johnson & Johnson",
        "JNJ",
        "high",
    ),
    (
        "approval",
        "FDA Approves AstraZeneca's Dato-DXd for Previously Treated HR+/HER2-Low Breast Cancer",
        "AstraZeneca and Daiichi Sankyo receive approval for datopotamab deruxtecan (Dato-DXd) as third-line treatment for hormone receptor-positive, HER2-low metastatic breast cancer.",
        "Datopotamab Deruxtecan (Dato-DXd)",
        "AstraZeneca",
        "AZN",
        "high",
    ),
]


class MockFDAScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "fda"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(5, 10)
        posts: list[dict[str, Any]] = []
        selected = random.sample(_MOCK_FDA_ACTIONS, min(count, len(_MOCK_FDA_ACTIONS)))
        for event_type, title, description, drug_name, company, ticker, significance in selected:
            content = f"[FDA - {event_type.replace('_', ' ').title()}] {title}\n\n{description}"
            posts.append(self._make_post(
                source_id=f"fda_mock_{self._generate_id()}",
                author="FDA",
                content=content,
                url=f"https://www.fda.gov/news-events/{self._generate_id()[:8]}",
                raw_metadata={
                    "event_type": event_type,
                    "title": title,
                    "drug_name": drug_name,
                    "company": company,
                    "ticker": ticker,
                    "significance": significance,
                },
            ))
        return posts
