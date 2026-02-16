# AlekFi Quality Pipeline Runbook

## 0) Bring Stack Up (Local)
If default ports are busy, use override envs.

```bash
POSTGRES_PORT=15432 REDIS_PORT=16379 API_PORT_HOST=18000 docker compose up -d --build
```

Check status:

```bash
POSTGRES_PORT=15432 REDIS_PORT=16379 API_PORT_HOST=18000 docker compose ps
```

## 1) Baseline / Metrics Snapshot
One command for strict/non-strict counts, independence/platform averages, labels, provider health, yfinance errors, and candidate/stall stats:

```bash
python3 scripts/metrics_snapshot.py
```

## 2) Market Data Provider Health + Cache
API health endpoints:

```bash
curl -s http://localhost:8000/api/health
curl -s http://localhost:8000/api/agentos/health
```

Redis circuit-breaker state:

```bash
docker exec alekfi-redis-1 /bin/sh -lc "redis-cli --no-auth-warning KEYS 'alekfi:md:cb:*'"
```

## 3) Backfill Labels (Dense Ground Truth)
Backfill outcomes + null samples:

```bash
python3 -m alekfi.tools.backfill_labels --since 2026-01-01 --limit 3000 --include-null-samples --null-sample-limit 1500
```

Incremental backfill:

```bash
python3 -m alekfi.tools.backfill_labels --since 2026-02-01 --limit 1000
```

## 4) Nightly Label Job
Nightly loop is started in `alekfi/main.py` for brain runtime.

Manual run:

```bash
python3 -m alekfi.tools.nightly_labels --since-hours 48 --limit 1200
```

## 5) Forecast Priors Training
Manual training + Redis publish:

```bash
python3 -m alekfi.tools.train_forecast_model --since-days 180 --min-samples 20
```

Artifact output:
- `reports/forecast_model_v1.json`

## 6) Evaluation Harness
Run full quality eval:

```bash
python3 -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20
```

API views:

```bash
curl -s "http://localhost:8000/api/metrics/signal_quality?since_days=30"
curl -s "http://localhost:8000/api/metrics/forecast_calibration?since_days=30"
```

## 7) Corroboration Worker Control
Corroboration starts from `alekfi/main.py` (brain runtime).

Temporary disable paths:
- Set long interval: `CORROBORATION_INTERVAL_SECONDS=86400`
- Or comment/env-gate worker startup at `alekfi/main.py` corroboration block.

Restart brain only:

```bash
docker compose up -d --build brain
```

## 8) Strict Feed Verification
Strict vs non-strict sanity check:

```bash
curl -s "http://localhost:8000/api/signals/forecasts?strict=true&limit=20"
curl -s "http://localhost:8000/api/signals/forecasts?strict=false&limit=20"
```

## 9) Do-Not-Regress Checklist
Run guardrails:

```bash
scripts/no_regressions.sh
```

This enforces:
- no direct `import yfinance` outside gateway
- fail-closed label smoke behavior
- key unit tests for market data, corroboration, forecast contract, and API route contract
- metrics snapshot generation

## 10) Deployment Smoke Checks (VPS)
```bash
ssh root@147.93.116.107 "cd /docker/alekfi && docker compose up -d --build"
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
```
