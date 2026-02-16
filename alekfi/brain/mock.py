"""MockBrain — simulates Tier 3 analysis without LLM calls.

Generates plausible entities, sentiment scores, and signals from filtered posts.
Works entirely in-memory when passed filtered post dicts from MockGatekeeper.
"""

from __future__ import annotations

import logging
import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from alekfi.brain.correlation import CorrelationEngine

logger = logging.getLogger(__name__)

_ENTITY_POOL = [
    ("Apple Inc.", "COMPANY", "AAPL"),
    ("Tesla Inc.", "COMPANY", "TSLA"),
    ("Nvidia Corp.", "COMPANY", "NVDA"),
    ("Microsoft Corp.", "COMPANY", "MSFT"),
    ("Amazon.com", "COMPANY", "AMZN"),
    ("Meta Platforms", "COMPANY", "META"),
    ("Google/Alphabet", "COMPANY", "GOOG"),
    ("Boeing Co.", "COMPANY", "BA"),
    ("JPMorgan Chase", "COMPANY", "JPM"),
    ("Pfizer Inc.", "COMPANY", "PFE"),
    ("Nike Inc.", "COMPANY", "NKE"),
    ("Walmart Inc.", "COMPANY", "WMT"),
    ("Goldman Sachs", "COMPANY", "GS"),
    ("Intel Corp.", "COMPANY", "INTC"),
    ("TSMC", "COMPANY", "TSM"),
    ("Samsung", "COMPANY", "005930.KS"),
    ("Shell plc", "COMPANY", "SHEL"),
    ("Toyota Motor", "COMPANY", "TM"),
    ("LVMH", "COMPANY", "MC.PA"),
    ("BYD Company", "COMPANY", "BYDDF"),
    ("Crude Oil", "COMMODITY", "CL"),
    ("Gold", "COMMODITY", "GC"),
    ("Copper", "COMMODITY", "HG"),
    ("Natural Gas", "COMMODITY", "NG"),
    ("Wheat", "COMMODITY", "ZW"),
    ("Bitcoin", "CRYPTO", "BTC"),
    ("Ethereum", "CRYPTO", "ETH"),
    ("Solana", "CRYPTO", "SOL"),
    ("United States", "COUNTRY", "SPY"),
    ("China", "COUNTRY", "FXI"),
    ("Japan", "COUNTRY", "EWJ"),
    ("European Union", "COUNTRY", "VGK"),
    ("Technology Sector", "SECTOR", "XLK"),
    ("Energy Sector", "SECTOR", "XLE"),
    ("Financials Sector", "SECTOR", "XLF"),
    ("Healthcare Sector", "SECTOR", "XLV"),
    ("Semiconductors", "SECTOR", "SMH"),
    ("Elon Musk", "PERSON", "TSLA"),
    ("Jerome Powell", "PERSON", None),
    ("Jensen Huang", "PERSON", "NVDA"),
]

_THEMES = [
    "AI_demand", "supply_chain_disruption", "rate_sensitivity",
    "consumer_weakness", "geopolitical_risk", "earnings_surprise",
    "insider_activity", "regulatory_pressure", "ESG_risk",
    "commodity_squeeze", "tech_competition", "china_exposure",
    "labor_market", "inflation_persistence", "housing_weakness",
    "crypto_adoption", "energy_transition", "defense_spending",
]

_MECHANISMS = [
    "Revenue growth exceeding consensus expectations driven by new product cycle",
    "Supply chain disruption reducing manufacturing throughput and margin pressure",
    "Regulatory investigation creating legal overhang and uncertainty premium",
    "Consumer sentiment shift indicating brand damage and potential revenue decline",
    "Insider selling pattern suggesting management lacks confidence in near-term outlook",
    "Geopolitical tensions disrupting trade flows and creating supply uncertainty",
    "Central bank policy shift affecting discount rates and sector valuations",
    "Competitive pressure from new entrant eroding market share and pricing power",
    "Product quality issues at scale creating recall risk and liability exposure",
    "Commodity price spike compressing margins for downstream consumers",
    "Technology disruption threatening incumbent business model viability",
    "Cross-border sanctions restricting market access and revenue streams",
]

_SIGNAL_TYPES = [
    "sentiment_momentum", "volume_anomaly", "narrative_shift",
    "cross_platform_divergence", "source_convergence", "insider_signal",
]

_SIGNAL_THESES = [
    "Multiple independent sources reporting {entity} supply disruption. Cross-platform convergence from Reddit, news, and industry insiders suggests material impact within 1-2 weeks.",
    "Sentiment on {entity} shifted from neutral to strongly negative over 6 hours. Glassdoor reviews + social media indicate internal problems not yet priced in.",
    "{entity} seeing unusual discussion volume spike (3x normal). Historically this precedes material news by 24-48 hours. Positioning before catalyst.",
    "Insider buying at {entity} aligned with improving social sentiment. Smart money and crowd both bullish — high conviction directional play.",
    "Divergence detected: institutional platforms bearish on {entity} while retail heavily bullish. Information asymmetry suggests fade the crowd.",
    "Narrative shift on {entity}: 'growth story' → 'value trap'. Multiple analyst downgrades + deteriorating consumer sentiment. Momentum reversal likely.",
]


class MockBrain:
    """Drop-in replacement for BrainAnalyzer. No LLM, no database."""

    def __init__(self, config: Any) -> None:
        self._config = config
        self._correlation = CorrelationEngine()

        # stats
        self.posts_analyzed = 0
        self.entities_extracted = 0
        self.sentiments_scored = 0
        self.signals_generated = 0
        self.syntheses_run = 0

        # internal state
        self._entities: list[dict[str, Any]] = []
        self._sentiments: list[dict[str, Any]] = []
        self._signals: list[dict[str, Any]] = []

    def analyze_post(self, post: dict[str, Any]) -> dict[str, Any]:
        """Analyze a single filtered post. Returns analysis result."""
        # Extract 1-4 random entities
        num_entities = random.randint(1, 4)
        selected_entities = random.sample(_ENTITY_POOL, min(num_entities, len(_ENTITY_POOL)))

        entities_out: list[dict[str, Any]] = []
        sentiments_out: list[dict[str, Any]] = []

        for name, etype, ticker in selected_entities:
            entity = {
                "id": str(uuid.uuid4()),
                "name": name,
                "entity_type": etype,
                "ticker": ticker,
                "related_tickers": [r["symbol"] for r in self._correlation.get_related_instruments(ticker)] if ticker else [],
            }
            entities_out.append(entity)
            self._entities.append(entity)

            # Score sentiment
            sentiment_val = round(random.uniform(-0.8, 0.8), 2)
            confidence = round(random.uniform(0.4, 0.95), 2)
            urgency_val = round(random.uniform(0.1, 0.9), 2)

            sentiment = {
                "id": str(uuid.uuid4()),
                "filtered_post_id": post.get("id"),
                "entity_id": entity["id"],
                "entity_name": name,
                "sentiment": sentiment_val,
                "confidence": confidence,
                "urgency": urgency_val,
                "mechanism": random.choice(_MECHANISMS),
                "themes": random.sample(_THEMES, random.randint(1, 3)),
                "scored_at": datetime.now(timezone.utc).isoformat(),
            }
            sentiments_out.append(sentiment)
            self._sentiments.append(sentiment)

        self.entities_extracted += len(entities_out)
        self.sentiments_scored += len(sentiments_out)
        self.posts_analyzed += 1

        return {
            "post_id": post.get("id"),
            "entities": entities_out,
            "sentiments": sentiments_out,
        }

    def synthesize(self) -> dict[str, Any]:
        """Generate a mock synthesis from accumulated sentiment data."""
        # Build themes from entity mentions
        entity_counts: dict[str, int] = {}
        entity_sentiments: dict[str, list[float]] = {}
        for s in self._sentiments:
            name = s["entity_name"]
            entity_counts[name] = entity_counts.get(name, 0) + 1
            entity_sentiments.setdefault(name, []).append(s["sentiment"])

        themes: list[dict] = []
        for name, count in sorted(entity_counts.items(), key=lambda x: -x[1])[:5]:
            avg_sent = sum(entity_sentiments[name]) / len(entity_sentiments[name])
            themes.append({
                "theme": f"{'Bullish' if avg_sent > 0 else 'Bearish'} pressure on {name}",
                "entities_involved": [name],
                "signal_strength": round(min(abs(avg_sent) * 1.5, 1.0), 2),
                "evidence_count": count,
                "platforms": random.sample(["reddit", "twitter", "news_rss", "discord", "telegram", "hackernews"], min(3, count)),
            })

        alerts: list[dict] = []
        for name, sents in entity_sentiments.items():
            if len(sents) >= 2:
                avg = sum(sents) / len(sents)
                if abs(avg) > 0.4:
                    alerts.append({
                        "entity": name,
                        "alert_type": random.choice(["convergence", "momentum_shift", "volume_spike"]),
                        "description": f"{'Strong bullish' if avg > 0 else 'Strong bearish'} sentiment ({avg:+.2f}) across {len(sents)} mentions",
                        "urgency": "HIGH" if abs(avg) > 0.6 else "MEDIUM",
                    })

        self.syntheses_run += 1
        synthesis = {
            "themes": themes,
            "alerts": alerts[:5],
            "narrative_shifts": [],
        }

        logger.info(
            "[brain-mock] synthesis: %d themes, %d alerts",
            len(themes), len(alerts),
        )
        return synthesis

    def generate_signals(self, synthesis: dict[str, Any]) -> list[dict[str, Any]]:
        """Generate mock trading signals from synthesis."""
        signals: list[dict[str, Any]] = []

        for theme in synthesis.get("themes", [])[:3]:
            entity_name = theme["entities_involved"][0] if theme["entities_involved"] else "SPY"
            ticker = None
            for n, t, tk in _ENTITY_POOL:
                if n == entity_name:
                    ticker = tk
                    break
            if not ticker:
                ticker = "SPY"

            conviction = round(random.uniform(0.45, 0.9), 2)
            direction = "LONG" if theme["signal_strength"] > 0.5 and random.random() > 0.4 else "SHORT"
            thesis_template = random.choice(_SIGNAL_THESES)

            signal = {
                "id": str(uuid.uuid4()),
                "signal_type": random.choice(_SIGNAL_TYPES),
                "affected_instruments": [
                    {"symbol": ticker, "asset_class": "equity", "direction": direction},
                ],
                "direction": direction,
                "conviction": conviction,
                "time_horizon": random.choice(["HOURS", "DAYS", "WEEKS"]),
                "thesis": thesis_template.format(entity=entity_name),
                "evidence": [f"Theme: {theme['theme']}", f"Evidence count: {theme['evidence_count']}"],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": (datetime.now(timezone.utc) + timedelta(hours=random.randint(4, 72))).isoformat(),
            }
            signals.append(signal)
            self._signals.append(signal)

        for alert in synthesis.get("alerts", [])[:2]:
            entity_name = alert["entity"]
            ticker = None
            for n, t, tk in _ENTITY_POOL:
                if n == entity_name:
                    ticker = tk
                    break
            if not ticker:
                continue

            direction = "SHORT" if "bearish" in alert["description"].lower() else "LONG"
            signal = {
                "id": str(uuid.uuid4()),
                "signal_type": alert["alert_type"].replace("_", "_"),
                "affected_instruments": [
                    {"symbol": ticker, "asset_class": "equity", "direction": direction},
                ],
                "direction": direction,
                "conviction": round(random.uniform(0.5, 0.85), 2),
                "time_horizon": "DAYS",
                "thesis": f"Alert-driven signal: {alert['description']}",
                "evidence": [alert["description"]],
                "created_at": datetime.now(timezone.utc).isoformat(),
                "expires_at": (datetime.now(timezone.utc) + timedelta(hours=24)).isoformat(),
            }
            signals.append(signal)
            self._signals.append(signal)

        self.signals_generated += len(signals)
        logger.info("[brain-mock] generated %d signals", len(signals))
        return signals

    async def run_on_filtered(self, filtered_posts: list[dict[str, Any]]) -> None:
        """Process a list of filtered post dicts (from MockGatekeeper)."""
        logger.info("[brain-mock] starting analysis of %d filtered posts", len(filtered_posts))

        for post in filtered_posts:
            self.analyze_post(post)

        synthesis = self.synthesize()
        self.generate_signals(synthesis)

    def get_stats(self) -> dict[str, Any]:
        return {
            "posts_analyzed": self.posts_analyzed,
            "entities_extracted": self.entities_extracted,
            "sentiments_scored": self.sentiments_scored,
            "signals_generated": self.signals_generated,
            "syntheses_run": self.syntheses_run,
        }
