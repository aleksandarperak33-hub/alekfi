"""CLI: print market-data provider health (circuit breakers + last errors)."""

from __future__ import annotations

import asyncio
import json

from alekfi.marketdata import MarketDataGateway


async def _amain() -> None:
    gw = MarketDataGateway()
    try:
        health = await gw.get_provider_health()
        print(json.dumps(health, indent=2, default=str))
    finally:
        await gw.close()


if __name__ == "__main__":
    asyncio.run(_amain())
