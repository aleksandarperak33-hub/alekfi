# AlekFi Quality Pipeline Runbook

## 1) Baseline / Current Metrics Snapshot
```bash
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
```
Prints:
- strict/non-strict counts
- avg independence + unique platforms
- label counts + skip reasons
- provider health + yfinance error rate
- convergence + candidate metrics

## 2) Provider Health + Cache Inspection
Gateway health:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.marketdata_health'"
```

Redis provider breaker states:
```bash
ssh root@147.93.116.107 "docker exec alekfi-redis-1 /bin/sh -lc 'redis-cli -a \"$REDIS_PASSWORD\" --no-auth-warning KEYS alekfi:md:cb:*'"
```

## 3) Backfill Labels (dense outcomes)
Full backfill:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.backfill_labels --since 2026-01-01 --limit 3000 --force'"
```

Incremental backfill (recent window):
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.backfill_labels --since 2026-02-01 --limit 1000'"
```

## 4) Nightly Label Job
Manual run:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.nightly_labels --since-hours 48 --limit 1200'"
```

Service-managed loop starts from `alekfi/main.py` on brain startup.

## 5) Evaluation Harness
Run:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
```

Save report artifact:
```bash
ssh root@147.93.116.107 "mkdir -p /docker/alekfi/reports && docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20' > /docker/alekfi/reports/eval_harness_$(date +%Y%m%d_%H%M).json"
```

## 6) Corroboration Worker Control
Currently started by brain process startup in `alekfi/main.py`.

Restart brain/swarm with latest code:
```bash
ssh root@147.93.116.107 "cd /docker/alekfi && docker compose up -d --build brain swarm"
```

Disable corroboration quickly (hotfix path):
- Comment out or env-gate corroboration worker startup in `alekfi/main.py`
- Rebuild/restart brain

## 7) Do-Not-Regress Checks
Run all guardrails:
```bash
ssh root@147.93.116.107 "/docker/alekfi/scripts/no_regressions.sh"
```

Includes:
- no direct yfinance imports outside gateway
- fail-closed label smoke test
- optional pytest test file
- metrics snapshot

## 8) Troubleshooting
If strict feed is empty:
1. Check strict/non-strict counts and independence distribution via metrics snapshot.
2. Verify corroboration worker logs in `alekfi-brain-1`.
3. Verify provider health open-circuit state.
4. Verify label skip reasons are not dominated by degraded data.

If labels stop updating:
1. Run nightly labels manually.
2. Inspect skip reasons (`DEGRADED_MARKET_DATA`, `SYMBOL_INVALID`, etc.).
3. Inspect market data provider health and Redis cache connectivity.
