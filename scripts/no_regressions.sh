#!/usr/bin/env bash
set -euo pipefail

ROOT="/docker/alekfi"
cd "$ROOT"

echo "[1/4] Guardrail: no direct yfinance imports outside marketdata gateway"
python3 - <<'PY'
from pathlib import Path
import sys

root = Path('/docker/alekfi/alekfi')
allow = {(root / 'marketdata' / 'gateway.py').resolve()}
offenders = []
for p in root.rglob('*.py'):
    rp = p.resolve()
    if rp in allow:
        continue
    if '__pycache__' in p.parts:
        continue
    text = p.read_text(encoding='utf-8', errors='ignore')
    if 'import yfinance' in text:
        offenders.append(str(p.relative_to(root)))
if offenders:
    print('FAIL: direct yfinance imports found:', offenders)
    sys.exit(1)
print('PASS')
PY

echo "[2/4] Guardrail: fail-closed label behavior smoke test"
docker exec alekfi-brain-1 /bin/sh -lc "python - <<'PY'
import asyncio
from datetime import datetime, timezone
from alekfi.tools.backfill_labels import _build_labels_for_outcome

class FakeGateway:
    def normalize_symbol(self, raw_ticker):
        class Norm:
            symbol = raw_ticker
            warnings = []
            valid = True
        return Norm()

    async def get_price_at(self, symbol, ts, tolerance_days=3, fail_closed=False):
        return {'symbol': symbol, 'price': 100.0, 'meta': {'quality_flags': ['AUTH_FAIL'], 'provider_used': 'yfinance'}}
    async def get_quote(self, symbol, fail_closed=False):
        return {'symbol': symbol, 'price': 100.0, 'volume': 123456.0, 'meta': {'quality_flags': ['AUTH_FAIL'], 'provider_used': 'yfinance'}}
    @staticmethod
    def label_skip_reason(meta):
        return 'AUTH_FAIL'

async def main():
    out = await _build_labels_for_outcome(
        FakeGateway(),
        instruments=['AAPL', 'SPY'],
        created_at=datetime.now(timezone.utc),
        direction='LONG',
        signal_type='sentiment_momentum',
    )
    assert out.skipped is True
    assert out.skip_reason == 'AUTH_FAIL'
    assert out.labels.get('label_confidence') == 0.0

asyncio.run(main())
print('PASS')
PY"

echo "[3/4] Optional pytest guardrails"
if docker exec alekfi-brain-1 /bin/sh -lc "python - <<'PY'
import importlib.util
print('yes' if importlib.util.find_spec('pytest') else 'no')
PY" | grep -q yes; then
  docker exec alekfi-brain-1 /bin/sh -lc "cd /app && python -m pytest -q tests/test_marketdata_guardrails.py tests/test_marketdata_contract.py tests/test_signal_decay_schema_guard.py tests/test_corroboration_worker.py tests/test_forecast_contract.py tests/test_api_routes_contract.py"
else
  echo "SKIP: pytest unavailable in container"
fi

echo "[4/4] Metrics snapshot"
python3 /docker/alekfi/scripts/metrics_snapshot.py

echo "Done"
