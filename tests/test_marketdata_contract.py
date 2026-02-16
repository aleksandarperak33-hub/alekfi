from __future__ import annotations

import types

import pytest

from alekfi.marketdata.gateway import MarketDataGateway, NormalizedSymbol


@pytest.mark.asyncio
async def test_market_data_contract_has_required_fields() -> None:
    gw = object.__new__(MarketDataGateway)

    async def _quote(self, symbol, fail_closed=False):  # noqa: ANN001
        return {
            "symbol": symbol,
            "price": 101.0,
            "previous_close": 100.0,
            "change_1d": 1.0,
            "change_5d": 2.5,
            "volume": 5_000_000.0,
            "meta": {
                "provider_used": "finnhub",
                "fallback_chain": [],
                "quality_flags": [],
                "cache_hit": False,
                "fetched_at": "2026-02-16T00:00:00+00:00",
                "data_completeness": 1.0,
            },
        }

    async def _ohlcv(self, symbol, **kwargs):  # noqa: ANN001
        rows = []
        price = 90.0
        for i in range(20):
            rows.append(
                {
                    "ts": 1700000000 + i * 86400,
                    "open": price,
                    "high": price + 2.0,
                    "low": price - 2.0,
                    "close": price + 0.5,
                    "volume": 1_000_000.0,
                }
            )
            price += 0.5
        return {"symbol": symbol, "rows": rows, "meta": {"quality_flags": []}}

    gw.get_quote = types.MethodType(_quote, gw)
    gw.get_ohlcv = types.MethodType(_ohlcv, gw)

    md = await MarketDataGateway.get_market_data(gw, "AAPL")
    assert md.last_price is not None
    assert md.asof
    assert md.volume is not None
    assert md.dollar_volume is not None
    assert md.ret_1d is not None
    assert md.ret_5d is not None
    assert md.atr_14 is not None
    assert md.spread_bps_est > 0


@pytest.mark.asyncio
async def test_intraday_ohlcv_is_disabled_without_non_yfinance_provider() -> None:
    gw = object.__new__(MarketDataGateway)

    async def _symbol_q(self, symbol):  # noqa: ANN001
        return False

    async def _cache_get(self, key):  # noqa: ANN001
        return None

    gw.normalize_symbol = lambda raw: NormalizedSymbol(symbol=raw, warnings=[], valid=True)
    gw._symbol_is_quarantined = types.MethodType(_symbol_q, gw)
    gw._cache_get = types.MethodType(_cache_get, gw)

    out = await MarketDataGateway.get_ohlcv(gw, "AAPL", interval="1h")
    assert out["rows"] == []
    assert "INTRADAY_UNSUPPORTED_NO_PROVIDER" in (out.get("meta") or {}).get("quality_flags", [])
