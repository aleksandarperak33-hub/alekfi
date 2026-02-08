"""Discord scraper — monitors trading/investing servers via discord.py bot."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)


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
            self._buffer.append(self._make_post(
                source_id=str(message.id),
                author=f"{message.author.name}#{message.author.discriminator}",
                content=message.content[:3000],
                url=message.jump_url,
                raw_metadata={
                    "server": message.guild.name if message.guild else "DM",
                    "channel": message.channel.name if hasattr(message.channel, "name") else "unknown",
                    "channel_id": message.channel.id,
                    "reactions": reactions,
                    "attachments": len(message.attachments),
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
    ("options-flow", "TradingElite", "Unusual flow alert: $AAPL 200C 02/21 — 25K contracts, $4.5M premium. Someone knows something."),
    ("options-flow", "FlowTracker", "Massive put sweep on $SPY 470P weeklies. 50K contracts in 2 min. Hedge or conviction?"),
    ("earnings-plays", "EarningsWhisper", "$AMZN earnings tonight. Whisper number is $1.15 vs consensus $0.98. IV at 45%. Playing straddle."),
    ("earnings-plays", "ThetaGang_Boss", "$MSFT beat on cloud revenue. Azure +31% YoY. Going to gap up 5%+ tomorrow. Sold puts at $400."),
    ("sector-rotation", "MacroChad", "Rotation into small caps is REAL. IWM breaking out of 2-year range. This is the start of a major move."),
    ("sector-rotation", "SectorSurfer", "Energy leading again today. XLE up 3% on OPEC news. Loading $CVX and $XOM calls."),
    ("whale-alerts", "WhaleWatcher", "Insider purchase alert: $LLY CEO just bought $5M in shares on open market. Bullish signal."),
    ("whale-alerts", "DarkPoolSpy", "Dark pool print: $NVDA 500K shares at $720. That's $360M. Institutions accumulating aggressively."),
    ("general-market", "DayTraderMike", "SPX rejected at 4800 resistance for the 3rd time. We're going to retest 4650 before any breakout."),
    ("general-market", "BullMarketBro", "VIX at 13. Market is WAY too complacent. Buying some VIX calls as portfolio insurance."),
    ("crypto-trading", "CryptoAlpha", "$BTC holding 60K like a champ. ETF flows positive 12 days straight. Next stop 70K."),
    ("crypto-trading", "AltSeason_", "$SOL breaking out. TVL surging, NFT volume on Solana overtaking Ethereum. $200 target."),
    ("dd-discussion", "FundamentalGuy", "Deep dive on $CRWD: 38% revenue growth, 78% gross margins, net retention 125%. This is best-in-class SaaS."),
    ("dd-discussion", "ValueHunter_", "$PFE looks like deep value here. Trading at 10x earnings, 5.8% yield. Market hates it but pipeline is solid."),
    ("commodities", "GoldBug2024", "Central banks bought 1,037 tonnes of gold last year. De-dollarization is real. $GLD to $250."),
    ("commodities", "OilTrdr", "EIA crude draw of 5.2M barrels vs expected 1.5M build. $WTI ripping. $85 incoming."),
    ("macro-talk", "BondKing", "10Y yield at 4.5% and rising. This is going to break something. CRE is the weakest link."),
    ("macro-talk", "FedWatcher", "CME FedWatch now showing 70% chance of June cut. Down from 90% last week. Hawkish repricing."),
    ("tech-stocks", "AIInvestor", "Every major cloud provider is increasing AI capex by 50%+. The picks-and-shovels play is $NVDA and $AVGO."),
    ("tech-stocks", "ChipAnalyst", "Intel losing market share in every segment. Data center, client, foundry — all declining. Avoid."),
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
            server = random.choice(["Alpha Trading", "WallStreetBets Discord", "Macro Trading Hub", "Options Flow Club"])
            posts.append(self._make_post(
                source_id=str(mid),
                author=author,
                content=content,
                url=f"https://discord.com/channels/{random.randint(100000,999999)}/{random.randint(100000,999999)}/{mid}",
                raw_metadata={
                    "server": server,
                    "channel": channel,
                    "reactions": [
                        {"emoji": random.choice(["🚀", "🐂", "🐻", "💰", "📈", "📉", "🔥"]), "count": random.randint(1, 50)}
                    ],
                },
            ))
        return posts
