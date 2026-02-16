from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from alekfi.marketdata.gateway import MarketDataGateway
from alekfi.tools.backfill_labels import _build_labels_for_outcome


def test_no_direct_yfinance_imports_outside_gateway() -> None:
    root = Path(__file__).resolve().parents[1] / "alekfi"
    allow = {
        (root / "marketdata" / "gateway.py").resolve(),
    }
    offenders: list[str] = []
    for p in root.rglob("*.py"):
        rp = p.resolve()
        if rp in allow:
            continue
        if any(part.startswith("__pycache__") for part in p.parts):
            continue
        if p.suffix != ".py" or p.name.endswith(".bak.py") or ".bak" in p.name:
            continue
        text = p.read_text(encoding="utf-8", errors="ignore")
        if "import yfinance" in text:
            offenders.append(str(p.relative_to(root)))
    assert offenders == [], f"Direct yfinance imports found outside gateway: {offenders}"


def test_normalize_symbol_blocks_known_bad_tickers() -> None:
    gw = MarketDataGateway()
    for raw in ("BBD.B", "DN", "BDNCE"):
        norm = gw.normalize_symbol(raw)
        assert norm.valid is False
        assert "SYMBOL_INVALID" in norm.warnings


@pytest.mark.asyncio
async def test_labeler_fail_closed_on_degraded_data() -> None:
    class FakeGateway:
        def normalize_symbol(self, raw_ticker):  # noqa: ANN001
            return SimpleNamespace(symbol=raw_ticker, warnings=[], valid=True)

        async def get_price_at(self, symbol, ts, tolerance_days=3, fail_closed=False):  # noqa: ANN001
            return {
                "symbol": symbol,
                "price": 100.0,
                "meta": {"quality_flags": ["AUTH_FAIL"], "provider_used": "yfinance"},
            }

        async def get_quote(self, symbol, fail_closed=False):  # noqa: ANN001
            return {
                "symbol": symbol,
                "price": 100.0,
                "volume": 100_000.0,
                "meta": {"quality_flags": ["AUTH_FAIL"], "provider_used": "yfinance"},
            }

        @staticmethod
        def label_skip_reason(meta):  # noqa: ANN001
            return "AUTH_FAIL"

    result = await _build_labels_for_outcome(
        FakeGateway(),
        instruments=["AAPL", "SPY"],
        created_at=datetime.now(timezone.utc),
        direction="LONG",
        signal_type="sentiment_momentum",
    )
    assert result.skipped is True
    assert result.skip_reason == "AUTH_FAIL"
    assert result.labels.get("label_confidence") == 0.0
