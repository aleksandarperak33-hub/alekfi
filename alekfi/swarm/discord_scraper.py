"""Discord scraper — monitors trading/investing servers via discord.py bot."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import defaultdict
from typing import Any

from alekfi.config import get_settings
from alekfi.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

# Channel name -> channel_type mapping for classification
_CHANNEL_TYPE_MAP: dict[str, str] = {
    "options-flow": "options",
    "options-alerts": "options",
    "theta-gang": "options",
    "0dte-plays": "options",
    "earnings-plays": "options",
    "crypto-trading": "crypto",
    "crypto-signals": "crypto",
    "crypto-analysis": "crypto",
    "defi-alpha": "crypto",
    "nft-talk": "crypto",
    "altcoin-calls": "crypto",
    "whale-alerts": "signals",
    "trade-signals": "signals",
    "scalp-alerts": "signals",
    "dark-pool-prints": "signals",
    "unusual-activity": "signals",
    "sector-rotation": "general",
    "general-market": "general",
    "dd-discussion": "general",
    "commodities": "general",
    "macro-talk": "general",
    "tech-stocks": "general",
    "small-caps": "general",
    "meme-stocks": "general",
}


def _classify_channel(channel_name: str) -> str:
    """Return channel type from map, or 'general' as default."""
    return _CHANNEL_TYPE_MAP.get(channel_name, "general")


class DiscordScraper(BaseScraper):
    """Connects a discord.py bot to configured channels and collects messages."""

    @property
    def platform(self) -> str:
        return "discord"

    def __init__(self, interval: int = 30) -> None:
        super().__init__(interval)
        self._token = get_settings().discord_bot_token
        self._buffer: list[dict[str, Any]] = []
        self._bot_started = False
        # Message velocity tracking: "server:channel" -> list of timestamps
        self._velocity_windows: dict[str, list[float]] = defaultdict(list)
        self._velocity_window_secs = 300  # 5-minute window

    def _record_velocity(self, server: str, channel: str) -> float:
        """Record a message timestamp and return current messages-per-5-min for this channel."""
        key = f"{server}:{channel}"
        now = time.monotonic()
        window = self._velocity_windows[key]
        window.append(now)
        cutoff = now - self._velocity_window_secs
        self._velocity_windows[key] = [t for t in window if t >= cutoff]
        return float(len(self._velocity_windows[key]))

    async def _start_bot(self) -> None:
        import discord

        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            logger.info("[discord] bot connected as %s (guilds: %d)", client.user, len(client.guilds))
            self._bot_started = True

        @client.event
        async def on_message(message):
            if message.author.bot:
                return
            if not message.content:
                return
            reactions = [
                {"emoji": str(r.emoji), "count": r.count}
                for r in message.reactions
            ] if message.reactions else []
            server_name = message.guild.name if message.guild else "DM"
            channel_name = message.channel.name if hasattr(message.channel, "name") else "unknown"
            channel_type = _classify_channel(channel_name)
            velocity = self._record_velocity(server_name, channel_name)
            self._buffer.append(self._make_post(
                source_id=str(message.id),
                author=f"{message.author.name}#{message.author.discriminator}",
                content=message.content[:3000],
                url=message.jump_url,
                raw_metadata={
                    "server": server_name,
                    "channel": channel_name,
                    "channel_id": message.channel.id,
                    "channel_type": channel_type,
                    "reactions": reactions,
                    "attachments": len(message.attachments),
                    "msg_velocity_5m": velocity,
                },
            ))

        asyncio.create_task(client.start(self._token))

    async def scrape(self) -> list[dict[str, Any]]:
        if not self._bot_started:
            await self._start_bot()
            await asyncio.sleep(5)

        posts = list(self._buffer)
        self._buffer.clear()
        return posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_DISCORD_MESSAGES = [
    # Options
    ("options-flow", "TradingElite", "Unusual flow alert: $AAPL 200C 02/21 — 25K contracts, $4.5M premium. Someone knows something."),
    ("options-flow", "FlowTracker", "Massive put sweep on $SPY 470P weeklies. 50K contracts in 2 min. Hedge or conviction?"),
    ("0dte-plays", "ZeroDTE_King", "0DTE $SPX 4820C scalp. Entered at $2.50, riding to $5. VWAP reclaim on the 1-min. LFG."),
    ("0dte-plays", "GammaQueen", "0DTE gamma squeeze incoming on $QQQ. Call wall at 410, if we break through, dealers forced to buy."),
    ("earnings-plays", "EarningsWhisper", "$AMZN earnings tonight. Whisper number is $1.15 vs consensus $0.98. IV at 45%. Playing straddle."),
    ("earnings-plays", "ThetaGang_Boss", "$MSFT beat on cloud revenue. Azure +31% YoY. Going to gap up 5%+ tomorrow. Sold puts at $400."),
    # Signals
    ("whale-alerts", "WhaleWatcher", "Insider purchase alert: $LLY CEO just bought $5M in shares on open market. Bullish signal."),
    ("whale-alerts", "DarkPoolSpy", "Dark pool print: $NVDA 500K shares at $720. That's $360M. Institutions accumulating aggressively."),
    ("trade-signals", "AlgoTrader_X", "SIGNAL: Long $GOOGL at $155. Target $168. Stop $149. Momentum breakout on daily + positive earnings whisper."),
    ("trade-signals", "SwingKing", "SIGNAL: Short $COIN at $220. Target $185. Stop $232. Crypto exchange volume declining, regulatory overhang."),
    ("dark-pool-prints", "DPScanner", "Large dark pool block: $META 800K shares at $485. Total notional $388M. 4th large print this week."),
    ("scalp-alerts", "ScalpMaster", "SCALP: Long $TSLA 250C 0DTE at $1.20. Target $3.00. Just bounced off VWAP with volume surge."),
    # General
    ("sector-rotation", "MacroChad", "Rotation into small caps is REAL. IWM breaking out of 2-year range. This is the start of a major move."),
    ("sector-rotation", "SectorSurfer", "Energy leading again today. XLE up 3% on OPEC news. Loading $CVX and $XOM calls."),
    ("general-market", "DayTraderMike", "SPX rejected at 4800 resistance for the 3rd time. We're going to retest 4650 before any breakout."),
    ("general-market", "BullMarketBro", "VIX at 13. Market is WAY too complacent. Buying some VIX calls as portfolio insurance."),
    ("dd-discussion", "FundamentalGuy", "Deep dive on $CRWD: 38% revenue growth, 78% gross margins, net retention 125%. This is best-in-class SaaS."),
    ("dd-discussion", "ValueHunter_", "$PFE looks like deep value here. Trading at 10x earnings, 5.8% yield. Market hates it but pipeline is solid."),
    ("commodities", "GoldBug2024", "Central banks bought 1,037 tonnes of gold last year. De-dollarization is real. $GLD to $250."),
    ("commodities", "OilTrdr", "EIA crude draw of 5.2M barrels vs expected 1.5M build. $WTI ripping. $85 incoming."),
    ("macro-talk", "BondKing", "10Y yield at 4.5% and rising. This is going to break something. CRE is the weakest link."),
    ("macro-talk", "FedWatcher", "CME FedWatch now showing 70% chance of June cut. Down from 90% last week. Hawkish repricing."),
    ("tech-stocks", "AIInvestor", "Every major cloud provider is increasing AI capex by 50%+. The picks-and-shovels play is $NVDA and $AVGO."),
    ("tech-stocks", "ChipAnalyst", "Intel losing market share in every segment. Data center, client, foundry — all declining. Avoid."),
    # Crypto
    ("crypto-trading", "CryptoAlpha", "$BTC holding 60K like a champ. ETF flows positive 12 days straight. Next stop 70K."),
    ("crypto-trading", "AltSeason_", "$SOL breaking out. TVL surging, NFT volume on Solana overtaking Ethereum. $200 target."),
    ("defi-alpha", "YieldFarmer", "New Pendle pool on $stETH giving 22% APY. Leverage loop via Aave brings it to 45%. Degen approved."),
    ("defi-alpha", "MEV_Hunter", "Huge MEV opportunity on Arbitrum. Sandwich bot pulling $50K/day on GMX liquidations. Alpha is leaking."),
    ("altcoin-calls", "GemFinder", "$TIA breaking out of accumulation range. Celestia modular narrative + airdrop farming = explosive move to $25."),
    ("altcoin-calls", "MicroCapKing", "Low cap gem: $ONDO at $0.80. RWA tokenization narrative. Blackrock connection. 50x potential. DYOR."),
    # Small caps and memes
    ("small-caps", "PennyStockPro", "$SMCI breaking out on AI server demand. Small cap playing in the big leagues now. $500 target."),
    ("meme-stocks", "DiamondHands", "$GME gamma ramp building again. 150C open interest massive. If we break $25, market makers forced to hedge."),
]

_MOCK_SERVERS = [
    "Alpha Trading",
    "WallStreetBets Discord",
    "Macro Trading Hub",
    "Options Flow Club",
    "Crypto Whales",
    "DeFi Degen Academy",
    "Stock Market Signals",
    "Theta Gang HQ",
]


class MockDiscordScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "discord"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(20, 40)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            channel, author, content = random.choice(_MOCK_DISCORD_MESSAGES)
            mid = random.randint(1_100_000_000_000_000_000, 1_200_000_000_000_000_000)
            server = random.choice(_MOCK_SERVERS)
            channel_type = _classify_channel(channel)
            posts.append(self._make_post(
                source_id=str(mid),
                author=author,
                content=content,
                url=f"https://discord.com/channels/{random.randint(100000,999999)}/{random.randint(100000,999999)}/{mid}",
                raw_metadata={
                    "server": server,
                    "channel": channel,
                    "channel_type": channel_type,
                    "reactions": [
                        {"emoji": random.choice(["fire", "rocket", "bear", "bull", "money", "chart_up", "chart_down", "100"]), "count": random.randint(1, 50)}
                    ],
                    "msg_velocity_5m": random.randint(1, 60),
                },
            ))
        return posts
