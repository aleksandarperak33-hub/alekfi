"""MockDataProvider — generates realistic fake API responses when no DB is available."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


def _utc() -> datetime:
    return datetime.now(timezone.utc)


def _ago(minutes: int) -> str:
    return (_utc() - timedelta(minutes=minutes)).isoformat()


def _uuid_str() -> str:
    return str(uuid.uuid4())


_SIGNAL_TYPES = [
    "sentiment_momentum", "volume_anomaly", "narrative_shift",
    "cross_platform_divergence", "source_convergence", "insider_signal",
]

_INSTRUMENTS = [
    ("NVDA", "equity"), ("AAPL", "equity"), ("TSLA", "equity"),
    ("BA", "equity"), ("JPM", "equity"), ("META", "equity"),
    ("AMZN", "equity"), ("MSFT", "equity"), ("PFE", "equity"),
    ("NKE", "equity"), ("GS", "equity"), ("INTC", "equity"),
    ("CL", "commodity"), ("GC", "commodity"), ("HG", "commodity"),
    ("BTC", "crypto"), ("ETH", "crypto"), ("SOL", "crypto"),
    ("XLF", "etf"), ("XLE", "etf"), ("SMH", "etf"),
    ("SPY", "etf"), ("QQQ", "etf"), ("TLT", "bond"),
]

_THESES = [
    "Multiple Reddit and Twitter sources confirm supply chain disruptions at key suppliers. Cross-platform convergence with 85% bearish sentiment across 12 posts in 2 hours.",
    "Unusual options flow detected: 50K call contracts purchased at $800 strike. Dark pool activity confirms institutional accumulation. Momentum building.",
    "Employee sentiment on Glassdoor dropped 40% in last month. Combined with insider selling (3 Form 4 filings), suggests internal issues not yet reflected in price.",
    "Narrative shifting from 'growth at all costs' to 'profitability focus'. Analyst downgrades accelerating. TikTok consumer complaints going viral.",
    "Earnings whisper numbers circulating on Discord significantly above consensus. Options IV at 52% suggests big move expected. Smart money positioning long.",
    "Geopolitical risk escalation detected across 4 news sources. Commodity exposure through futures and sector ETFs recommended as hedge.",
    "Consumer brand sentiment collapsing on TikTok and Instagram. Product quality complaints viral with 5M+ views. Revenue impact likely within 1-2 quarters.",
    "Fed language analysis from multiple financial Telegram channels suggests hawkish pivot. Bond traders repositioning. Rate-sensitive sectors vulnerable.",
]

_ENTITY_POOL = [
    ("Apple Inc.", "COMPANY", "AAPL"), ("Nvidia Corp.", "COMPANY", "NVDA"),
    ("Tesla Inc.", "COMPANY", "TSLA"), ("Boeing Co.", "COMPANY", "BA"),
    ("JPMorgan Chase", "COMPANY", "JPM"), ("Meta Platforms", "COMPANY", "META"),
    ("Amazon.com", "COMPANY", "AMZN"), ("Microsoft Corp.", "COMPANY", "MSFT"),
    ("Pfizer Inc.", "COMPANY", "PFE"), ("Nike Inc.", "COMPANY", "NKE"),
    ("Goldman Sachs", "COMPANY", "GS"), ("Intel Corp.", "COMPANY", "INTC"),
    ("TSMC", "COMPANY", "TSM"), ("Shell plc", "COMPANY", "SHEL"),
    ("Crude Oil", "COMMODITY", "CL"), ("Gold", "COMMODITY", "GC"),
    ("Copper", "COMMODITY", "HG"), ("Bitcoin", "CRYPTO", "BTC"),
    ("Ethereum", "CRYPTO", "ETH"), ("Technology Sector", "SECTOR", "XLK"),
    ("Energy Sector", "SECTOR", "XLE"), ("Semiconductors", "SECTOR", "SMH"),
    ("United States", "COUNTRY", "SPY"), ("China", "COUNTRY", "FXI"),
    ("Jerome Powell", "PERSON", None), ("Elon Musk", "PERSON", None),
]

_CATEGORIES = [
    "earnings", "macro", "geopolitical", "consumer", "supply_chain",
    "regulatory", "technology", "sentiment_shift", "insider_activity",
    "corporate_action", "commodity", "crypto",
]

_PLATFORMS = [
    "reddit", "news_rss", "twitter", "hackernews", "discord",
    "telegram", "tiktok", "instagram", "youtube", "sec_edgar",
    "glassdoor", "amazon_reviews", "appstore", "google_trends",
]


class MockDataProvider:
    """Generates all mock data in memory at construction time."""

    def __init__(self) -> None:
        self._signals = self._gen_signals(35)
        self._entities = self._gen_entities()
        self._filtered_posts = self._gen_filtered_posts(80)

    # ── Signals ────────────────────────────────────────────────────────

    def _gen_signals(self, count: int) -> list[dict[str, Any]]:
        signals = []
        for i in range(count):
            sym, ac = random.choice(_INSTRUMENTS)
            direction = random.choice(["LONG", "SHORT", "HEDGE"])
            signals.append({
                "id": _uuid_str(),
                "signal_type": random.choice(_SIGNAL_TYPES),
                "affected_instruments": [{"symbol": sym, "asset_class": ac, "direction": direction}],
                "direction": direction,
                "conviction": round(random.uniform(0.42, 0.95), 2),
                "time_horizon": random.choice(["MINUTES", "HOURS", "DAYS", "WEEKS"]),
                "thesis": random.choice(_THESES),
                "source_posts": [_uuid_str() for _ in range(random.randint(2, 6))],
                "metadata": {},
                "created_at": _ago(i * 8 + random.randint(0, 5)),
                "expires_at": (_utc() + timedelta(hours=random.randint(4, 72))).isoformat(),
            })
        return signals

    def get_signals(self, limit=50, signal_type=None, direction=None, min_conviction=None, since=None):
        out = list(self._signals)
        if signal_type:
            out = [s for s in out if s["signal_type"] == signal_type]
        if direction:
            out = [s for s in out if s["direction"] == direction]
        if min_conviction is not None:
            out = [s for s in out if s["conviction"] >= min_conviction]
        if since:
            out = [s for s in out if s["created_at"] >= since]
        return out[:limit]

    def get_signal(self, signal_id: str):
        for s in self._signals:
            if s["id"] == signal_id:
                return s
        return None

    def get_signals_summary(self):
        by_type: dict[str, int] = {}
        by_dir: dict[str, int] = {}
        convictions = []
        instruments: dict[str, int] = {}
        for s in self._signals:
            by_type[s["signal_type"]] = by_type.get(s["signal_type"], 0) + 1
            by_dir[s["direction"]] = by_dir.get(s["direction"], 0) + 1
            convictions.append(s["conviction"])
            for inst in s["affected_instruments"]:
                sym = inst["symbol"]
                instruments[sym] = instruments.get(sym, 0) + 1
        top = sorted(instruments.items(), key=lambda x: -x[1])[:10]
        return {
            "total": len(self._signals),
            "by_type": by_type,
            "by_direction": by_dir,
            "avg_conviction": round(sum(convictions) / max(len(convictions), 1), 3),
            "top_instruments": [{"symbol": s, "count": c} for s, c in top],
        }

    # ── Entities ───────────────────────────────────────────────────────

    def _gen_entities(self) -> list[dict[str, Any]]:
        entities = []
        for name, etype, ticker in _ENTITY_POOL:
            sent = round(random.uniform(-0.7, 0.7), 2)
            entities.append({
                "id": _uuid_str(),
                "name": name,
                "entity_type": etype,
                "ticker": ticker,
                "related_tickers": [],
                "metadata": {},
                "created_at": _ago(random.randint(60, 600)),
                "latest_sentiment": sent,
                "sentiment_confidence": round(random.uniform(0.4, 0.9), 2),
                "mention_count": random.randint(2, 30),
                "sentiment_history": [
                    {"scored_at": _ago(i * 15), "sentiment": round(sent + random.uniform(-0.2, 0.2), 2), "confidence": round(random.uniform(0.4, 0.9), 2)}
                    for i in range(20)
                ],
            })
        return entities

    def get_entities(self, limit=50, entity_type=None, search=None):
        out = list(self._entities)
        if entity_type:
            out = [e for e in out if e["entity_type"] == entity_type]
        if search:
            q = search.lower()
            out = [e for e in out if q in e["name"].lower()]
        return out[:limit]

    def get_entity(self, entity_id: str):
        for e in self._entities:
            if e["id"] == entity_id:
                return e
        return None

    def get_entity_sentiment(self, entity_id: str):
        e = self.get_entity(entity_id)
        if e:
            return e.get("sentiment_history", [])
        return []

    # ── Filtered Posts ─────────────────────────────────────────────────

    def _gen_filtered_posts(self, count: int) -> list[dict[str, Any]]:
        posts = []
        for i in range(count):
            posts.append({
                "id": _uuid_str(),
                "raw_post_id": _uuid_str(),
                "platform": random.choice(_PLATFORMS),
                "author": f"user_{random.randint(100,999)}",
                "content": random.choice(_THESES)[:200],
                "relevance_score": round(random.uniform(0.5, 1.0), 2),
                "urgency": random.choice(["HIGH", "MEDIUM", "LOW"]),
                "category": random.choice(_CATEGORIES),
                "gatekeeper_reasoning": "Identified actionable financial intelligence with specific entity mentions",
                "filtered_at": _ago(i * 4 + random.randint(0, 3)),
                "analyzed": random.choice([True, True, True, False]),
            })
        return posts

    def get_filtered_posts(self, limit=50, urgency=None, category=None, since=None):
        out = list(self._filtered_posts)
        if urgency:
            out = [p for p in out if p["urgency"] == urgency]
        if category:
            out = [p for p in out if p["category"] == category]
        if since:
            out = [p for p in out if p["filtered_at"] >= since]
        return out[:limit]

    def get_raw_stats(self):
        platform_counts = {}
        for p in _PLATFORMS:
            platform_counts[p] = random.randint(80, 500)
        return {
            "total_raw": sum(platform_counts.values()),
            "by_platform": platform_counts,
            "posts_per_hour": sum(platform_counts.values()) // 4,
            "last_hour_count": random.randint(200, 600),
        }

    # ── System ─────────────────────────────────────────────────────────

    def get_stats(self):
        return {
            "swarm": {
                "total_posts": random.randint(3000, 8000),
                "active_scrapers": 14,
                "dormant_scrapers": 0,
                "posts_per_minute": round(random.uniform(8, 25), 1),
                "by_platform": {p: random.randint(50, 400) for p in _PLATFORMS},
            },
            "gatekeeper": {
                "total_processed": random.randint(3000, 8000),
                "total_kept": random.randint(300, 800),
                "total_killed": random.randint(2700, 7200),
                "kill_rate": round(random.uniform(0.87, 0.93), 3),
                "batches_processed": random.randint(150, 400),
            },
            "brain": {
                "posts_analyzed": random.randint(200, 600),
                "entities_extracted": random.randint(500, 1500),
                "sentiments_scored": random.randint(500, 1500),
                "signals_generated": len(self._signals),
                "syntheses_run": random.randint(10, 30),
            },
            "signals": {
                "total": len(self._signals),
                "active": len([s for s in self._signals if s["expires_at"] > _utc().isoformat()]),
                "by_direction": {"LONG": 0, "SHORT": 0, "HEDGE": 0},
            },
        }


_provider: MockDataProvider | None = None


def get_mock_provider() -> MockDataProvider:
    global _provider
    if _provider is None:
        _provider = MockDataProvider()
    return _provider
