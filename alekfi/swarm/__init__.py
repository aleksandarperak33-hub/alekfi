"""Tier 1 — Swarm scrapers. Dumb collectors that push raw posts to Redis."""

from alekfi.swarm.manager import SwarmManager
from alekfi.swarm.fourchan import FourChanBizScraper, MockFourChanBizScraper
from alekfi.swarm.github_trending import GitHubTrendingScraper, MockGitHubTrendingScraper
from alekfi.swarm.linkedin import LinkedInScraper, MockLinkedInScraper
from alekfi.swarm.patents import PatentScraper, MockPatentScraper
from alekfi.swarm.blind import BlindScraper, MockBlindScraper
from alekfi.swarm.federal_register import FederalRegisterScraper, MockFederalRegisterScraper
from alekfi.swarm.fda import FDAScraper, MockFDAScraper
from alekfi.swarm.google_play import GooglePlayScraper, MockGooglePlayScraper
from alekfi.swarm.fourchan_pol import FourChanPolScraper, MockFourChanPolScraper
from alekfi.swarm.clinical_trials import ClinicalTrialsScraper, MockClinicalTrialsScraper
from alekfi.swarm.earnings_calendar import EarningsCalendarScraper, MockEarningsCalendarScraper
from alekfi.swarm.finviz_news import FinvizNewsScraper, MockFinvizNewsScraper
from alekfi.swarm.stocktwits import StockTwitsScraper, MockStockTwitsScraper
from alekfi.swarm.options_flow import OptionsFlowScraper, MockOptionsFlowScraper
from alekfi.swarm.whale_tracker import WhaleTrackerScraper, MockWhaleTrackerScraper
from alekfi.swarm.commodities import CommoditiesScraper, MockCommoditiesScraper

__all__ = [
    "SwarmManager",
    "FourChanBizScraper",
    "MockFourChanBizScraper",
    "GitHubTrendingScraper",
    "MockGitHubTrendingScraper",
    "LinkedInScraper",
    "MockLinkedInScraper",
    "PatentScraper",
    "MockPatentScraper",
    "BlindScraper",
    "MockBlindScraper",
    "FederalRegisterScraper",
    "MockFederalRegisterScraper",
    "FDAScraper",
    "MockFDAScraper",
    "GooglePlayScraper",
    "MockGooglePlayScraper",
    "FourChanPolScraper",
    "MockFourChanPolScraper",
    "ClinicalTrialsScraper",
    "MockClinicalTrialsScraper",
    "EarningsCalendarScraper",
    "MockEarningsCalendarScraper",
    "FinvizNewsScraper",
    "MockFinvizNewsScraper",
    "StockTwitsScraper",
    "MockStockTwitsScraper",
    "OptionsFlowScraper",
    "MockOptionsFlowScraper",
    "WhaleTrackerScraper",
    "MockWhaleTrackerScraper",
    "CommoditiesScraper",
    "MockCommoditiesScraper",
]
