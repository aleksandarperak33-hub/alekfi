"""USPTO patent filing scraper — monitors patent application RSS feeds."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import random
from typing import Any

import feedparser

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# USPTO publishes patent applications weekly via RSS.
# These feeds cover recent patent application publications (PAIR/AppFT).
_USPTO_FEEDS: dict[str, str] = {
    "uspto_applications": "https://www.uspto.gov/patents/search/rss/applications.xml",
    "uspto_grants": "https://www.uspto.gov/patents/search/rss/grants.xml",
}

# Companies in the financial universe we track for patent filings.
_TRACKED_COMPANIES: set[str] = {
    # Big Tech
    "apple", "google", "alphabet", "microsoft", "amazon", "meta",
    "nvidia", "amd", "intel", "qualcomm", "broadcom", "tsmc",
    "samsung", "ibm", "oracle", "cisco", "salesforce", "adobe",
    "tesla", "openai", "anthropic",
    # Pharma / Biotech
    "pfizer", "moderna", "johnson & johnson", "merck", "eli lilly",
    "abbvie", "amgen", "gilead", "regeneron", "novartis", "roche",
    "astrazeneca", "novo nordisk", "bristol-myers", "bms",
    # Semiconductors / Hardware
    "arm", "asml", "applied materials", "lam research", "synopsys",
    "cadence", "micron", "western digital", "seagate",
    # Autonomous / EV
    "waymo", "cruise", "rivian", "lucid", "nio", "byd",
    # Defense / Aerospace
    "lockheed martin", "raytheon", "northrop grumman", "boeing",
    "spacex", "palantir",
    # Finance / Fintech
    "jpmorgan", "goldman sachs", "visa", "mastercard", "stripe",
    "block", "square", "paypal", "coinbase",
}


def _matches_tracked_company(text: str) -> str | None:
    """Return the matched company name if *text* mentions a tracked company."""
    lower = text.lower()
    for company in _TRACKED_COMPANIES:
        if company in lower:
            return company
    return None


class PatentScraper(BaseScraper):
    """Fetch USPTO patent RSS feeds and filter for tracked companies."""

    @property
    def platform(self) -> str:
        return "patents"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._seen_ids: set[str] = set()

    @staticmethod
    def _hash_id(value: str) -> str:
        return hashlib.sha256(value.encode()).hexdigest()[:16]

    def _parse_feed(self, feed_name: str, feed_url: str) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []
        try:
            parsed = feedparser.parse(feed_url)
            for entry in parsed.entries[:50]:
                link = getattr(entry, "link", "") or ""
                title = getattr(entry, "title", "")
                summary = getattr(entry, "summary", "")[:3000]
                combined_text = f"{title} {summary}"

                matched = _matches_tracked_company(combined_text)
                if not matched:
                    continue

                entry_id = link or title
                hashed = self._hash_id(entry_id)
                if hashed in self._seen_ids:
                    continue
                self._seen_ids.add(hashed)

                published = getattr(entry, "published", "")
                posts.append(self._make_post(
                    source_id=hashed,
                    author=matched,
                    content=f"[Patent] {title}\n\n{summary}",
                    url=link or None,
                    raw_metadata={
                        "feed": feed_name,
                        "feed_url": feed_url,
                        "title": title,
                        "summary": summary[:500],
                        "published": published,
                        "matched_company": matched,
                        "patent_id": getattr(entry, "id", ""),
                        "categories": [t.get("term", "") for t in getattr(entry, "tags", [])],
                    },
                    source_published_at=published or None,
                ))
        except Exception:
            logger.warning("[patents] failed to parse %s", feed_name, exc_info=True)
        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        all_posts: list[dict[str, Any]] = []
        for name, url in _USPTO_FEEDS.items():
            posts = await loop.run_in_executor(None, self._parse_feed, name, url)
            all_posts.extend(posts)
        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_PATENTS = [
    # AI / Machine Learning
    ("Apple Inc.", "AAPL", "System and method for on-device large language model inference with dynamic memory allocation"),
    ("Google LLC", "GOOGL", "Transformer architecture optimization for real-time multimodal reasoning tasks"),
    ("Microsoft Corp.", "MSFT", "Federated learning framework for enterprise data with differential privacy guarantees"),
    ("Nvidia Corp.", "NVDA", "Hardware-accelerated sparse attention mechanism for trillion-parameter models"),
    ("Meta Platforms", "META", "Neural codec for ultra-low-bitrate video compression using diffusion models"),
    ("IBM Corp.", "IBM", "Quantum-classical hybrid optimization for large-scale combinatorial problems"),
    ("OpenAI Inc.", "N/A", "Reinforcement learning from human feedback with constitutional AI constraints"),
    # Semiconductors
    ("Intel Corp.", "INTC", "3D chiplet interconnect architecture with photonic data links"),
    ("AMD Inc.", "AMD", "Adaptive voltage-frequency scaling for heterogeneous compute tiles"),
    ("TSMC Ltd.", "TSM", "Sub-2nm gate-all-around transistor fabrication with extreme ultraviolet patterning"),
    ("Qualcomm Inc.", "QCOM", "Unified AI-radio processor for 6G millimeter-wave communications"),
    ("Broadcom Inc.", "AVGO", "Silicon photonics transceiver for 1.6 terabit data center interconnects"),
    ("ASML Holdings", "ASML", "High-NA EUV lithography system with improved overlay accuracy"),
    # Biotech / Pharma
    ("Pfizer Inc.", "PFE", "mRNA lipid nanoparticle formulation for targeted pancreatic cancer immunotherapy"),
    ("Moderna Inc.", "MRNA", "Self-amplifying RNA platform for rapid-response pandemic vaccine production"),
    ("Eli Lilly", "LLY", "GLP-1/GIP dual agonist with extended half-life for obesity and NASH treatment"),
    ("AbbVie Inc.", "ABBV", "Bispecific antibody construct for simultaneous TNF and IL-23 blockade"),
    ("Regeneron", "REGN", "CRISPR-Cas13 platform for in vivo RNA editing of hepatic targets"),
    ("Novo Nordisk", "NVO", "Oral semaglutide formulation with enhanced gastrointestinal absorption"),
    # Autonomous Vehicles / Robotics
    ("Tesla Inc.", "TSLA", "End-to-end neural network for urban autonomous driving without HD maps"),
    ("Waymo LLC", "GOOGL", "Multi-agent prediction and planning for dense pedestrian intersection scenarios"),
    ("Cruise LLC", "GM", "LiDAR-free perception stack using surround-view camera fusion with radar"),
    ("Nvidia Corp.", "NVDA", "Digital twin simulation platform for autonomous vehicle validation at scale"),
    # Defense / Aerospace
    ("Lockheed Martin", "LMT", "Hypersonic scramjet thermal protection system with active cooling channels"),
    ("Boeing Co.", "BA", "Autonomous aerial refueling system for unmanned combat aircraft"),
    ("Palantir Technologies", "PLTR", "Distributed sensor fusion framework for real-time battlefield awareness"),
    # Fintech
    ("Visa Inc.", "V", "Privacy-preserving biometric payment authentication using homomorphic encryption"),
    ("Mastercard Inc.", "MA", "Graph neural network for real-time cross-border transaction fraud detection"),
    ("JPMorgan Chase", "JPM", "Blockchain-based tokenization platform for illiquid alternative assets"),
]


class MockPatentScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "patents"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(8, 20)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            company, ticker, title = random.choice(_MOCK_PATENTS)
            posts.append(self._make_post(
                source_id=f"mock_{self._generate_id()}",
                author=company,
                content=f"[Patent] {title}",
                url=f"https://patents.google.com/patent/US{random.randint(11000000, 12999999)}B2",
                raw_metadata={
                    "feed": "uspto_applications",
                    "title": title,
                    "company": company,
                    "ticker": ticker,
                    "published": "2025-01-15T08:00:00Z",
                    "matched_company": company.lower().split()[0],
                    "patent_id": f"US20250{random.randint(100000, 999999)}A1",
                    "categories": random.sample(
                        ["artificial-intelligence", "semiconductor", "biotech", "autonomous-vehicles",
                         "machine-learning", "drug-discovery", "5g-6g", "quantum-computing",
                         "robotics", "blockchain", "cybersecurity", "photonics"],
                        k=random.randint(1, 3),
                    ),
                },
            ))
        return posts
