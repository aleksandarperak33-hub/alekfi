"""Telegram scraper — monitors financial channels/groups via Telethon."""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

from open_claw.config import get_settings
from open_claw.swarm.base import BaseScraper

logger = logging.getLogger(__name__)

_DEFAULT_CHANNELS = [
    "BloombergMarkets", "FinancialTimes", "WSJMarkets",
    "CoinDeskGlobal", "bitcoinchannel", "CryptoNewsAlerts",
    "tradingsignals_global", "ForexSignalsOfficial",
    "MacroAlf", "unusual_whales",
]


class TelegramScraper(BaseScraper):
    """Uses Telethon async client to monitor configured Telegram channels."""

    @property
    def platform(self) -> str:
        return "telegram"

    def __init__(self, interval: int = 30) -> None:
        super().__init__(interval)
        s = get_settings()
        self._api_id = int(s.telegram_api_id) if s.telegram_api_id else 0
        self._api_hash = s.telegram_api_hash
        self._client = None
        self._buffer: list[dict[str, Any]] = []
        self._started = False

    async def _start_client(self) -> None:
        from telethon import TelegramClient, events

        self._client = TelegramClient("openclaw_session", self._api_id, self._api_hash)
        await self._client.start()

        @self._client.on(events.NewMessage(chats=_DEFAULT_CHANNELS))
        async def handler(event):
            msg = event.message
            chat = await event.get_chat()
            chat_title = getattr(chat, "title", getattr(chat, "username", "unknown"))
            self._buffer.append(self._make_post(
                source_id=f"{chat.id}_{msg.id}",
                author=chat_title,
                content=(msg.text or "")[:3000],
                url=f"https://t.me/{getattr(chat, 'username', 'c/' + str(chat.id))}/{msg.id}",
                raw_metadata={
                    "channel": chat_title,
                    "channel_id": chat.id,
                    "views": msg.views or 0,
                    "forwards": msg.forwards or 0,
                    "reply_to": msg.reply_to_msg_id,
                    "has_media": msg.media is not None,
                },
            ))

        self._started = True
        logger.info("[telegram] client connected, monitoring %d channels", len(_DEFAULT_CHANNELS))

    async def scrape(self) -> list[dict[str, Any]]:
        if not self._started:
            await self._start_client()
            await asyncio.sleep(3)

        posts = list(self._buffer)
        self._buffer.clear()
        return posts


# ── Mock ───────────────────────────────────────────────────────────────

_MOCK_TELEGRAM = [
    ("Bloomberg Markets", "BREAKING: US CPI comes in at 3.4% YoY, above expectations of 3.2%. Core CPI at 3.9%. Dollar surging, equities selling off."),
    ("Bloomberg Markets", "JUST IN: ECB cuts rates by 25bps to 3.75%, first cut since 2019. Euro weakens against dollar."),
    ("Financial Times", "EXCLUSIVE: Saudi Aramco considering $50B acquisition of major LNG producer to diversify beyond crude oil."),
    ("Financial Times", "Chinese property developers face $230B in offshore bond maturities over next 18 months. Restructuring wave expected."),
    ("WSJ Markets", "Nvidia becomes most valuable company globally at $2.1T. Jensen Huang says AI demand 'far exceeds supply.'"),
    ("WSJ Markets", "Commercial real estate distress: US office vacancy hits record 20.1%. CMBS delinquencies rising sharply."),
    ("CoinDesk Global", "Bitcoin ETF cumulative inflows cross $10B mark. BlackRock's IBIT alone holds 170K BTC worth $10.2B."),
    ("CoinDesk Global", "Ethereum Dencun upgrade goes live, reducing L2 fees by 90%. $ETH price stable around $3,200."),
    ("Crypto Signals", "SIGNAL: BTC forming inverse head and shoulders on 4H. Breakout above 62K targets 68K. Invalidation: 58K."),
    ("Crypto Signals", "ALERT: Whale just moved 5,000 BTC ($300M) from Coinbase to cold storage. Bullish accumulation signal."),
    ("Forex Signals", "TRADE: Long EUR/JPY at 163.50. Target 166.00. Stop 162.00. BOJ dovish hold + ECB hawkish = divergence play."),
    ("Forex Signals", "USD/CNH breaking above 7.30. PBOC fixing weaker yuan. Capital outflows accelerating from China."),
    ("MacroAlf", "Global liquidity conditions improving: China easing, ECB cutting, BOJ still dovish. This is bullish for risk assets."),
    ("MacroAlf", "US fiscal deficit at 7% of GDP in a non-recession year. This is unprecedented. Bond vigilantes will eventually show up."),
    ("Unusual Whales", "ALERT: Senator [REDACTED] purchased $1M in $LMT calls 2 days before defense appropriations vote. Investigate."),
    ("Unusual Whales", "Massive dark pool activity in $PANW. 2M shares printed at $315, 3% above market. Institutions accumulating."),
    ("Trading Signals", "Gold breakout confirmed above $2,180. Next resistance at $2,250. Central bank buying + rate cut expectations driving momentum."),
    ("Trading Signals", "Japanese yen at 34-year low against dollar. BOJ intervention risk HIGH above 155. Watch for surprise rate hike."),
]


class MockTelegramScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "telegram"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(15, 30)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            channel, text = random.choice(_MOCK_TELEGRAM)
            msg_id = random.randint(10000, 99999)
            posts.append(self._make_post(
                source_id=f"mock_{channel.replace(' ', '')}_{msg_id}",
                author=channel,
                content=text,
                url=f"https://t.me/{channel.replace(' ', '')}/{msg_id}",
                raw_metadata={
                    "channel": channel,
                    "views": random.randint(5_000, 500_000),
                    "forwards": random.randint(50, 5_000),
                    "has_media": random.choice([True, False]),
                },
            ))
        return posts
