"""Telegram scraper — monitors financial channels/groups via Telethon."""

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

# Channel definitions with type tags
_CHANNEL_REGISTRY: list[tuple[str, str]] = [
    # News
    ("BloombergMarkets", "news"),
    ("FinancialTimes", "news"),
    ("WSJMarkets", "news"),
    # Crypto
    ("CoinDeskGlobal", "crypto"),
    ("bitcoinchannel", "crypto"),
    ("CryptoNewsAlerts", "crypto"),
    ("WhalePanda", "crypto"),
    ("CryptoCapo", "crypto"),
    ("EmperorBTC", "crypto"),
    ("AltcoinDaily", "crypto"),
    ("CryptoWizardd", "crypto"),
    ("LunarCrush", "crypto"),
    ("CoinGecko", "crypto"),
    ("Messari", "crypto"),
    ("DeFiPulse", "crypto"),
    ("TokenTerminal", "crypto"),
    # Analysis / signals
    ("tradingsignals_global", "analysis"),
    ("ForexSignalsOfficial", "analysis"),
    ("MacroAlf", "analysis"),
    ("unusual_whales", "analysis"),
    # Macro
    ("ZeroHedge", "macro"),
    ("FinancialJuice", "macro"),
    ("DeItaone", "macro"),  # Walter Bloomberg
]

_DEFAULT_CHANNELS = [ch for ch, _ in _CHANNEL_REGISTRY]

# Build a lookup: channel_name -> channel_type
_CHANNEL_TYPE_MAP: dict[str, str] = {ch: ct for ch, ct in _CHANNEL_REGISTRY}


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
        # Message velocity tracking: channel -> list of timestamps
        self._velocity_windows: dict[str, list[float]] = defaultdict(list)
        self._velocity_window_secs = 300  # 5-minute window

    def _record_velocity(self, channel_name: str) -> float:
        """Record a message timestamp and return current messages-per-5-min for this channel."""
        now = time.monotonic()
        window = self._velocity_windows[channel_name]
        window.append(now)
        # Prune entries older than the window
        cutoff = now - self._velocity_window_secs
        self._velocity_windows[channel_name] = [t for t in window if t >= cutoff]
        return float(len(self._velocity_windows[channel_name]))

    async def _start_client(self) -> None:
        from telethon import TelegramClient, events

        self._client = TelegramClient("openclaw_session", self._api_id, self._api_hash)
        await self._client.start()

        @self._client.on(events.NewMessage(chats=_DEFAULT_CHANNELS))
        async def handler(event):
            msg = event.message
            chat = await event.get_chat()
            chat_title = getattr(chat, "title", getattr(chat, "username", "unknown"))
            chat_username = getattr(chat, "username", "") or ""
            channel_type = _CHANNEL_TYPE_MAP.get(chat_username, "unknown")
            velocity = self._record_velocity(chat_username or chat_title)
            self._buffer.append(self._make_post(
                source_id=f"{chat.id}_{msg.id}",
                author=chat_title,
                content=(msg.text or "")[:3000],
                url=f"https://t.me/{getattr(chat, 'username', 'c/' + str(chat.id))}/{msg.id}",
                raw_metadata={
                    "channel": chat_title,
                    "channel_id": chat.id,
                    "channel_type": channel_type,
                    "views": msg.views or 0,
                    "forwards": msg.forwards or 0,
                    "reply_to": msg.reply_to_msg_id,
                    "has_media": msg.media is not None,
                    "msg_velocity_5m": velocity,
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
    # News
    ("Bloomberg Markets", "news", "BREAKING: US CPI comes in at 3.4% YoY, above expectations of 3.2%. Core CPI at 3.9%. Dollar surging, equities selling off."),
    ("Bloomberg Markets", "news", "JUST IN: ECB cuts rates by 25bps to 3.75%, first cut since 2019. Euro weakens against dollar."),
    ("Financial Times", "news", "EXCLUSIVE: Saudi Aramco considering $50B acquisition of major LNG producer to diversify beyond crude oil."),
    ("Financial Times", "news", "Chinese property developers face $230B in offshore bond maturities over next 18 months. Restructuring wave expected."),
    ("WSJ Markets", "news", "Nvidia becomes most valuable company globally at $2.1T. Jensen Huang says AI demand 'far exceeds supply.'"),
    ("WSJ Markets", "news", "Commercial real estate distress: US office vacancy hits record 20.1%. CMBS delinquencies rising sharply."),
    # Crypto
    ("CoinDesk Global", "crypto", "Bitcoin ETF cumulative inflows cross $10B mark. BlackRock's IBIT alone holds 170K BTC worth $10.2B."),
    ("CoinDesk Global", "crypto", "Ethereum Dencun upgrade goes live, reducing L2 fees by 90%. $ETH price stable around $3,200."),
    ("WhalePanda", "crypto", "Massive BTC accumulation by whales in the $58-60K range. On-chain data shows long-term holders not selling. Bullish."),
    ("CryptoCapo", "crypto", "ALT SEASON signal: BTC dominance dropping below 50%. Historically this precedes a 2-3 month altcoin rally."),
    ("EmperorBTC", "crypto", "Technical setup: $SOL forming ascending triangle on daily chart. Breakout above $185 targets $230. Volume increasing."),
    ("AltcoinDaily", "crypto", "Top 5 altcoins to watch this week: $LINK, $AVAX, $INJ, $TIA, $ONDO. All showing institutional accumulation on-chain."),
    ("CryptoWizardd", "crypto", "DeFi TVL just crossed $100B again. Major protocols seeing inflow surge: Aave +$2B, Lido +$1.5B this week."),
    ("LunarCrush", "crypto", "Social engagement spike: $PEPE mentions up 340% in 24h. $WIF up 280%. Meme coin season heating up."),
    ("CoinGecko", "crypto", "Market update: Total crypto market cap $2.4T. BTC $62K, ETH $3.2K, SOL $180. 24h volume $85B."),
    ("Messari", "crypto", "Research: L2 transaction volume now exceeds Ethereum mainnet by 5x. Base and Arbitrum leading. Gas fees at 18-month lows."),
    ("DeFiPulse", "crypto", "New DeFi protocol alert: EigenLayer TVL surges past $15B. Restaking narrative driving massive capital inflows."),
    ("TokenTerminal", "crypto", "Revenue leaders this week: Uniswap $12M, Aave $8M, Lido $15M, MakerDAO $6M. DeFi revenue hitting new highs."),
    # Analysis / signals
    ("Crypto Signals", "analysis", "SIGNAL: BTC forming inverse head and shoulders on 4H. Breakout above 62K targets 68K. Invalidation: 58K."),
    ("Crypto Signals", "analysis", "ALERT: Whale just moved 5,000 BTC ($300M) from Coinbase to cold storage. Bullish accumulation signal."),
    ("Forex Signals", "analysis", "TRADE: Long EUR/JPY at 163.50. Target 166.00. Stop 162.00. BOJ dovish hold + ECB hawkish = divergence play."),
    ("Forex Signals", "analysis", "USD/CNH breaking above 7.30. PBOC fixing weaker yuan. Capital outflows accelerating from China."),
    ("MacroAlf", "analysis", "Global liquidity conditions improving: China easing, ECB cutting, BOJ still dovish. This is bullish for risk assets."),
    ("MacroAlf", "analysis", "US fiscal deficit at 7% of GDP in a non-recession year. This is unprecedented. Bond vigilantes will eventually show up."),
    ("Unusual Whales", "analysis", "ALERT: Senator [REDACTED] purchased $1M in $LMT calls 2 days before defense appropriations vote. Investigate."),
    ("Unusual Whales", "analysis", "Massive dark pool activity in $PANW. 2M shares printed at $315, 3% above market. Institutions accumulating."),
    ("Trading Signals", "analysis", "Gold breakout confirmed above $2,180. Next resistance at $2,250. Central bank buying + rate cut expectations driving momentum."),
    ("Trading Signals", "analysis", "Japanese yen at 34-year low against dollar. BOJ intervention risk HIGH above 155. Watch for surprise rate hike."),
    # Macro
    ("ZeroHedge", "macro", "US 10Y yield spikes to 4.7% after hot jobs data. Rate cut expectations collapsing. Mortgage rates back above 7.5%."),
    ("ZeroHedge", "macro", "BREAKING: Major US regional bank halts withdrawals citing 'liquidity management.' Stock halted, down 35% pre-halt."),
    ("FinancialJuice", "macro", "FED'S WALLER: WE NEED TO SEE MORE PROGRESS ON INFLATION BEFORE CUTTING. MARKET PRICING SHIFTS TO SEPTEMBER."),
    ("FinancialJuice", "macro", "CHINA PBOC CUTS RRR BY 50BPS, INJECTS $140B INTO BANKING SYSTEM. BIGGEST EASING MOVE IN 2 YEARS."),
    ("DeItaone", "macro", "WALTER BLOOMBERG: APPLE REPORTEDLY IN TALKS TO ACQUIRE OPENAI STAKE AT $100B VALUATION."),
    ("DeItaone", "macro", "WALTER BLOOMBERG: US INITIAL JOBLESS CLAIMS 245K VS 230K EXPECTED. CONTINUING CLAIMS AT 18-MONTH HIGH."),
]


class MockTelegramScraper(BaseScraper):
    @property
    def platform(self) -> str:
        return "telegram"

    async def scrape(self) -> list[dict[str, Any]]:
        count = random.randint(15, 30)
        posts: list[dict[str, Any]] = []
        for _ in range(count):
            channel, channel_type, text = random.choice(_MOCK_TELEGRAM)
            msg_id = random.randint(10000, 99999)
            posts.append(self._make_post(
                source_id=f"mock_{channel.replace(' ', '')}_{msg_id}",
                author=channel,
                content=text,
                url=f"https://t.me/{channel.replace(' ', '')}/{msg_id}",
                raw_metadata={
                    "channel": channel,
                    "channel_type": channel_type,
                    "views": random.randint(5_000, 500_000),
                    "forwards": random.randint(50, 5_000),
                    "has_media": random.choice([True, False]),
                    "msg_velocity_5m": random.randint(1, 45),
                },
            ))
        return posts
