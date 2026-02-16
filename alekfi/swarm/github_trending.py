"""GitHub Trending scraper — monitors trending repos for fintech/AI/crypto signals."""

from __future__ import annotations

import logging
import random
import re
from typing import Any

import httpx
from bs4 import BeautifulSoup

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_GITHUB_TRENDING_URL = "https://github.com/trending"

_RELEVANT_KEYWORDS = re.compile(
    r"(financ|trading|trade|quant|portfolio|hedge|stock|market|"
    r"fintech|payment|banking|ledger|invoice|"
    r"ai|artificial.intelligence|machine.learn|deep.learn|llm|gpt|"
    r"neural|transformer|diffusion|embedding|"
    r"blockchain|crypto|bitcoin|ethereum|solana|defi|web3|nft|token|"
    r"wallet|smart.contract|dex|dao)",
    re.IGNORECASE,
)


class GitHubTrendingScraper(BaseScraper):
    """Scrapes github.com/trending HTML page for repos related to finance, trading, AI, and blockchain."""

    @property
    def platform(self) -> str:
        return "github_trending"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._seen_repos: set[str] = set()

    @staticmethod
    def _is_relevant(name: str, description: str) -> bool:
        """Return True if repo name or description matches finance/AI/crypto keywords."""
        text = f"{name} {description}"
        return bool(_RELEVANT_KEYWORDS.search(text))

    @staticmethod
    def _parse_stars_today(text: str) -> int:
        """Extract 'N stars today' integer from the trailing text."""
        match = re.search(r"([\d,]+)\s+stars?\s+today", text, re.IGNORECASE)
        if match:
            return int(match.group(1).replace(",", ""))
        return 0

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []
        spoken_languages = ["", "python", "javascript", "typescript", "rust", "go"]

        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
            for lang in spoken_languages:
                url = f"{_GITHUB_TRENDING_URL}/{lang}" if lang else _GITHUB_TRENDING_URL
                try:
                    resp = await client.get(url, headers={"Accept": "text/html"})
                    if resp.status_code != 200:
                        logger.warning("[github_trending] %s returned %d", url, resp.status_code)
                        continue
                except Exception:
                    logger.warning("[github_trending] failed to fetch %s", url, exc_info=True)
                    continue

                soup = BeautifulSoup(resp.text, "html.parser")
                articles = soup.select("article.Box-row")

                for article in articles:
                    # Repo name: h2 > a with href like /owner/repo
                    h2 = article.select_one("h2 a")
                    if not h2:
                        continue
                    href = h2.get("href", "").strip()
                    if not href or href.count("/") < 2:
                        continue

                    repo_full_name = href.lstrip("/")  # "owner/repo"
                    if repo_full_name in self._seen_repos:
                        continue

                    # Description
                    desc_tag = article.select_one("p")
                    description = desc_tag.get_text(strip=True) if desc_tag else ""

                    # Filter: only relevant repos
                    if not self._is_relevant(repo_full_name, description):
                        continue

                    self._seen_repos.add(repo_full_name)

                    # Language
                    lang_tag = article.select_one("[itemprop='programmingLanguage']")
                    language = lang_tag.get_text(strip=True) if lang_tag else ""

                    # Stars today
                    stars_today_tag = article.select_one("span.d-inline-block.float-sm-right")
                    stars_today = 0
                    if stars_today_tag:
                        stars_today = self._parse_stars_today(stars_today_tag.get_text())

                    # Total stars
                    star_links = article.select("a.Link--muted")
                    total_stars = 0
                    forks = 0
                    for link in star_links:
                        link_href = link.get("href", "")
                        link_text = link.get_text(strip=True).replace(",", "")
                        if "/stargazers" in link_href:
                            total_stars = int(link_text) if link_text.isdigit() else 0
                        elif "/forks" in link_href:
                            forks = int(link_text) if link_text.isdigit() else 0

                    repo_url = f"https://github.com/{repo_full_name}"
                    content = f"{repo_full_name}: {description}" if description else repo_full_name

                    all_posts.append(self._make_post(
                        source_id=repo_full_name.replace("/", "_"),
                        author=repo_full_name.split("/")[0],
                        content=content,
                        url=repo_url,
                        raw_metadata={
                            "repo": repo_full_name,
                            "description": description,
                            "language": language,
                            "stars_today": stars_today,
                            "total_stars": total_stars,
                            "forks": forks,
                            "trending_language_filter": lang or "all",
                        },
                    ))

        return all_posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_REPOS = [
    ("openbb-finance/OpenBB", "Investment research for everyone, everywhere.", "Python", 412, 28500),
    ("ranaroussi/yfinance", "Download market data from Yahoo! Finance's API.", "Python", 285, 12800),
    ("freqtrade/freqtrade", "Free, open source crypto trading bot.", "Python", 198, 27400),
    ("ccxt/ccxt", "A JavaScript / TypeScript / Python cryptocurrency trading API.", "TypeScript", 167, 32500),
    ("stefan-jansen/machine-learning-for-trading", "Code for Machine Learning for Algorithmic Trading.", "Jupyter Notebook", 145, 13200),
    ("AI4Finance-Foundation/FinRL", "Deep reinforcement learning framework for quantitative finance.", "Python", 320, 9800),
    ("tensortrade-org/tensortrade", "An open source reinforcement learning framework for trading.", "Python", 89, 4500),
    ("langchain-ai/langchain", "Building applications with LLMs through composability.", "Python", 580, 95000),
    ("run-llama/llama_index", "LlamaIndex: data framework for LLM applications.", "Python", 445, 36000),
    ("paradigmxyz/reth", "Modular, contributor-friendly and blazing-fast Ethereum execution client.", "Rust", 378, 3900),
    ("foundry-rs/foundry", "Foundry is a blazing fast portable and modular toolkit for Ethereum.", "Rust", 256, 8200),
    ("aave/aave-v3-core", "Aave Protocol V3 core smart contracts.", "Solidity", 134, 820),
    ("Uniswap/v4-core", "Core smart contracts of Uniswap v4.", "Solidity", 210, 2100),
    ("microsoft/autogen", "A programming framework for agentic AI.", "Python", 498, 32000),
    ("crewAIInc/crewAI", "Framework for orchestrating role-playing autonomous AI agents.", "Python", 612, 21000),
    ("qdrant/qdrant", "High-performance open-source vector similarity search engine.", "Rust", 189, 20500),
    ("bluesky-social/atproto", "Social networking technology created by Bluesky.", "TypeScript", 310, 6200),
    ("goldmansachs/gs-quant", "Python toolkit for quantitative finance by Goldman Sachs.", "Python", 76, 3200),
    ("robinhood/faust", "Python stream processing library for real-time trading signals.", "Python", 52, 6800),
    ("solana-labs/solana-program-library", "A collection of Solana programs maintained by Solana Labs.", "Rust", 165, 3400),
]


class MockGitHubTrendingScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "github_trending"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(8, 16)
        posts: list[dict[str, Any]] = []
        selected = random.sample(_MOCK_REPOS, min(count, len(_MOCK_REPOS)))
        for repo, description, language, stars_today, total_stars in selected:
            content = f"{repo}: {description}"
            posts.append(self._make_post(
                source_id=repo.replace("/", "_"),
                author=repo.split("/")[0],
                content=content,
                url=f"https://github.com/{repo}",
                raw_metadata={
                    "repo": repo,
                    "description": description,
                    "language": language,
                    "stars_today": stars_today + random.randint(-50, 50),
                    "total_stars": total_stars + random.randint(-200, 200),
                    "forks": random.randint(100, 5000),
                    "trending_language_filter": random.choice(["all", "python", "rust", "typescript"]),
                },
            ))
        return posts
