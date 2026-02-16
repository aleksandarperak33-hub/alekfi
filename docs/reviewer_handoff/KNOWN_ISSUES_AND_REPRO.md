# Known Issues + Repro Steps

## 1) Scraper Provider 402 Failures (External data vendors)
Observed in swarm logs (Instagram/TikTok/Facebook/LinkedIn scrapecreator-backed paths).

Repro:
```bash
ssh root@147.93.116.107 "docker logs --since 2h alekfi-swarm-1 2>&1 | grep -Ei 'failed \(402\)|instagram_sc|tiktok_sc|facebook_sc|linkedin_sc' | tail -n 80"
```

Impact:
- Reduced source coverage for some corroboration scenarios.

## 2) Longer-Horizon Eval Coverage Sparse (1d/3d/7d)
Current `eval_harness` output is dense for short horizons and sparse for longer windows in recent period.

Repro:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
```

Impact:
- Calibration and precision confidence strongest in 1h/4h for current window.

## 3) Candidate Queue Volume High
Convergence candidate count is elevated and should be monitored against promotion rate.

Repro:
```bash
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
```
Check: `candidate_cluster_count`, `pipeline_metrics_raw`.

Impact:
- Potential backlog and delayed enrichment if ingestion spikes.

## 4) Provider Last-Error Telemetry Not Always Empty
`provider_health` may report last error strings even when breaker is closed/open_until=0.

Repro:
```bash
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
```

Impact:
- Telemetry can look noisy; use `open_until` + fail counts as primary availability signal.

## 5) Verify strict feed payload contract completeness
Strict output should include guardrail explanation and forecast fields.

Repro:
```bash
ssh root@147.93.116.107 "python3 - <<'PY'
import requests, json
rows=requests.get('http://localhost:8000/api/signals/forecasts?strict=true&limit=5', timeout=20).json()
print('count',len(rows))
print(json.dumps(rows[0] if rows else {}, indent=2)[:2000])
PY"
```

Impact:
- Contract consistency affects downstream execution/UI consumers.
