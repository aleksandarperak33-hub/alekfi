"""Whale tracker — monitors large cryptocurrency transactions on-chain."""

from __future__ import annotations

import logging
import os
import random
from datetime import datetime, timezone
from typing import Any

import aiohttp

from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# Whale Alert API (free tier: 10 requests/min, last 3600s window)
_WHALE_ALERT_URL = "https://api.whale-alert.io/v1/transactions"
# Fallback: blockchain.info for BTC large transactions
_BLOCKCHAIN_INFO_URL = "https://blockchain.info/unconfirmed-transactions?format=json"
_BLOCKCHAIN_TX_URL = "https://blockchain.info/rawtx/{txhash}"

# Minimum USD value to consider a "whale" transaction
_MIN_USD_VALUE = 1_000_000  # $1M

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}

# Known exchange wallet labels (partial matching)
_EXCHANGE_LABELS = {
    "binance", "coinbase", "kraken", "bitfinex", "okx", "bybit",
    "huobi", "kucoin", "gemini", "bitstamp", "ftx", "crypto.com",
    "gate.io", "upbit", "bitmex", "deribit", "bitget",
}


def _classify_address(owner: str, owner_type: str) -> str:
    """Classify an address as 'exchange', 'unknown', or a named entity."""
    if not owner and not owner_type:
        return "unknown"
    combined = f"{owner} {owner_type}".lower()
    for exchange in _EXCHANGE_LABELS:
        if exchange in combined:
            return f"exchange:{exchange}"
    if owner_type and owner_type.lower() == "exchange":
        return f"exchange:{owner or 'unknown'}"
    return owner or owner_type or "unknown"


def _determine_flow(from_class: str, to_class: str) -> str:
    """Determine if a transaction is an exchange inflow, outflow, internal, or external."""
    from_is_exchange = from_class.startswith("exchange:")
    to_is_exchange = to_class.startswith("exchange:")
    if from_is_exchange and to_is_exchange:
        return "exchange_internal"
    elif to_is_exchange:
        return "exchange_inflow"
    elif from_is_exchange:
        return "exchange_outflow"
    return "external_transfer"


class WhaleTrackerScraper(BaseScraper):
    """Tracks large cryptocurrency transactions via Whale Alert API and blockchain.info fallback."""

    @property
    def platform(self) -> str:
        return "whale_tracker"

    def __init__(self, interval: int = 300) -> None:
        super().__init__(interval)
        self._seen_txs: set[str] = set()
        self._api_key: str = os.environ.get("WHALE_ALERT_API_KEY", "")

    async def _fetch_whale_alert(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch recent large transactions from Whale Alert API."""
        posts: list[dict[str, Any]] = []
        if not self._api_key:
            logger.debug("[whale_tracker] no WHALE_ALERT_API_KEY set, skipping whale alert")
            return posts

        try:
            # Free tier: transactions from last 3600 seconds, min value $500K
            params = {
                "api_key": self._api_key,
                "min_value": _MIN_USD_VALUE,
                "start": int(datetime.now(timezone.utc).timestamp()) - 3600,
            }
            async with session.get(
                _WHALE_ALERT_URL,
                params=params,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status == 401:
                    logger.warning("[whale_tracker] whale alert API key invalid or expired")
                    return posts
                if resp.status == 429:
                    logger.warning("[whale_tracker] whale alert rate limit hit")
                    return posts
                if resp.status != 200:
                    logger.warning("[whale_tracker] whale alert returned %d", resp.status)
                    return posts
                data = await resp.json(content_type=None)
        except Exception:
            logger.warning("[whale_tracker] failed to fetch whale alert", exc_info=True)
            return posts

        transactions = data.get("transactions", [])
        for tx in transactions:
            tx_hash = tx.get("hash", "")
            tx_id = tx.get("id", tx_hash or self._generate_id())
            if tx_id in self._seen_txs:
                continue
            self._seen_txs.add(tx_id)

            blockchain = tx.get("blockchain", "unknown")
            symbol = tx.get("symbol", "").upper()
            amount = tx.get("amount", 0)
            amount_usd = tx.get("amount_usd", 0)

            from_info = tx.get("from", {})
            to_info = tx.get("to", {})
            from_owner = from_info.get("owner", "")
            from_type = from_info.get("owner_type", "")
            to_owner = to_info.get("owner", "")
            to_type = to_info.get("owner_type", "")

            from_class = _classify_address(from_owner, from_type)
            to_class = _classify_address(to_owner, to_type)
            flow_type = _determine_flow(from_class, to_class)

            # Determine significance
            significance = "medium"
            if amount_usd >= 50_000_000:
                significance = "critical"
            elif amount_usd >= 10_000_000:
                significance = "high"

            content = (
                f"[Whale Transfer] {amount:,.2f} {symbol} (${amount_usd:,.0f})\n"
                f"From: {from_class} -> To: {to_class}\n"
                f"Flow: {flow_type} | Chain: {blockchain}"
            )

            posts.append(self._make_post(
                source_id=str(tx_id),
                author="Whale Alert",
                content=content,
                url=f"https://whale-alert.io/transaction/{blockchain}/{tx_hash}" if tx_hash else None,
                source_published_at=datetime.fromtimestamp(
                    tx.get("timestamp", 0), tz=timezone.utc
                ).isoformat() if tx.get("timestamp") else None,
                raw_metadata={
                    "blockchain": blockchain,
                    "symbol": symbol,
                    "amount": amount,
                    "amount_usd": amount_usd,
                    "from_owner": from_class,
                    "to_owner": to_class,
                    "flow_type": flow_type,
                    "tx_hash": tx_hash,
                    "significance": significance,
                },
            ))

        return posts

    async def _fetch_blockchain_info(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fallback: fetch recent large BTC transactions from blockchain.info."""
        posts: list[dict[str, Any]] = []
        try:
            async with session.get(
                _BLOCKCHAIN_INFO_URL,
                headers=_HEADERS,
                timeout=aiohttp.ClientTimeout(total=20),
            ) as resp:
                if resp.status != 200:
                    logger.debug("[whale_tracker] blockchain.info returned %d", resp.status)
                    return posts
                data = await resp.json(content_type=None)
        except Exception:
            logger.debug("[whale_tracker] failed to fetch blockchain.info", exc_info=True)
            return posts

        txs = data.get("txs", [])
        for tx in txs:
            tx_hash = tx.get("hash", "")
            if tx_hash in self._seen_txs:
                continue

            # Sum outputs to get total BTC value
            total_satoshi = sum(out.get("value", 0) for out in tx.get("out", []))
            total_btc = total_satoshi / 1e8

            # Rough USD estimate (we don't have live price here, use ~$65,000 as approximation)
            # In production you'd fetch the live price, but this is a reasonable floor
            estimated_usd = total_btc * 65000

            if estimated_usd < _MIN_USD_VALUE:
                continue

            self._seen_txs.add(tx_hash)

            # Count inputs/outputs for flow analysis
            input_count = len(tx.get("inputs", []))
            output_count = len(tx.get("out", []))

            significance = "medium"
            if estimated_usd >= 50_000_000:
                significance = "critical"
            elif estimated_usd >= 10_000_000:
                significance = "high"

            content = (
                f"[BTC Whale Transfer] {total_btc:,.4f} BTC (~${estimated_usd:,.0f})\n"
                f"Inputs: {input_count} | Outputs: {output_count}\n"
                f"Chain: bitcoin | Hash: {tx_hash[:16]}..."
            )

            posts.append(self._make_post(
                source_id=tx_hash[:24],
                author="blockchain.info",
                content=content,
                url=f"https://blockchain.info/tx/{tx_hash}",
                source_published_at=datetime.fromtimestamp(
                    tx.get("time", 0), tz=timezone.utc
                ).isoformat() if tx.get("time") else None,
                raw_metadata={
                    "blockchain": "bitcoin",
                    "symbol": "BTC",
                    "amount": total_btc,
                    "amount_usd": estimated_usd,
                    "tx_hash": tx_hash,
                    "input_count": input_count,
                    "output_count": output_count,
                    "flow_type": "external_transfer",
                    "significance": significance,
                    "price_estimate": 65000,
                },
            ))

        return posts

    async def scrape(self) -> list[dict[str, Any]]:
        all_posts: list[dict[str, Any]] = []

        async with aiohttp.ClientSession() as session:
            # Primary: Whale Alert API
            whale_posts = await self._fetch_whale_alert(session)
            all_posts.extend(whale_posts)

            # Fallback: blockchain.info if whale alert yielded few results
            if len(whale_posts) < 3:
                btc_posts = await self._fetch_blockchain_info(session)
                all_posts.extend(btc_posts)

        # Cap seen set
        if len(self._seen_txs) > 20000:
            self._seen_txs = set(list(self._seen_txs)[-10000:])

        return all_posts


# -- Mock -------------------------------------------------------------------

_MOCK_WHALE_TRANSFERS = [
    # (blockchain, symbol, amount, amount_usd, from_class, to_class, flow_type, significance)
    ("bitcoin", "BTC", 1200.0, 78_000_000, "unknown", "exchange:binance", "exchange_inflow", "critical"),
    ("bitcoin", "BTC", 850.5, 55_282_500, "exchange:coinbase", "unknown", "exchange_outflow", "critical"),
    ("bitcoin", "BTC", 500.0, 32_500_000, "unknown", "unknown", "external_transfer", "high"),
    ("ethereum", "ETH", 25000.0, 87_500_000, "exchange:kraken", "unknown", "exchange_outflow", "critical"),
    ("ethereum", "ETH", 15000.0, 52_500_000, "unknown", "exchange:binance", "exchange_inflow", "critical"),
    ("ethereum", "USDT", 150_000_000, 150_000_000, "exchange:bitfinex", "exchange:binance", "exchange_internal", "critical"),
    ("ethereum", "USDT", 50_000_000, 50_000_000, "unknown", "exchange:coinbase", "exchange_inflow", "critical"),
    ("tron", "USDT", 80_000_000, 80_000_000, "exchange:okx", "unknown", "exchange_outflow", "critical"),
    ("bitcoin", "BTC", 200.0, 13_000_000, "exchange:gemini", "unknown", "exchange_outflow", "high"),
    ("ethereum", "ETH", 5000.0, 17_500_000, "unknown", "exchange:kraken", "exchange_inflow", "high"),
    ("bitcoin", "BTC", 75.0, 4_875_000, "unknown", "exchange:bitstamp", "exchange_inflow", "medium"),
    ("ethereum", "USDC", 25_000_000, 25_000_000, "exchange:coinbase", "unknown", "exchange_outflow", "high"),
]


class MockWhaleTrackerScraper(BaseScraper):
    """Mock scraper generating realistic crypto whale transaction data."""

    @property
    def platform(self) -> str:
        return "whale_tracker"

    async def scrape(self) -> list[dict[str, Any]]:
        posts: list[dict[str, Any]] = []

        count = random.randint(6, 10)
        selected = random.sample(_MOCK_WHALE_TRANSFERS, min(count, len(_MOCK_WHALE_TRANSFERS)))

        for blockchain, symbol, amount, amount_usd, from_class, to_class, flow_type, significance in selected:
            # Add some variation
            amount_jitter = amount * random.uniform(0.8, 1.2)
            usd_jitter = amount_usd * random.uniform(0.85, 1.15)
            tx_hash = self._generate_id() + self._generate_id() + self._generate_id()

            content = (
                f"[Whale Transfer] {amount_jitter:,.2f} {symbol} (${usd_jitter:,.0f})\n"
                f"From: {from_class} -> To: {to_class}\n"
                f"Flow: {flow_type} | Chain: {blockchain}"
            )

            posts.append(self._make_post(
                source_id=f"whale_{tx_hash[:16]}",
                author="Whale Alert",
                content=content,
                url=f"https://whale-alert.io/transaction/{blockchain}/{tx_hash}",
                source_published_at=datetime.now(timezone.utc).isoformat(),
                raw_metadata={
                    "blockchain": blockchain,
                    "symbol": symbol,
                    "amount": round(amount_jitter, 4),
                    "amount_usd": round(usd_jitter, 0),
                    "from_owner": from_class,
                    "to_owner": to_class,
                    "flow_type": flow_type,
                    "tx_hash": tx_hash,
                    "significance": significance,
                },
            ))

        return posts
