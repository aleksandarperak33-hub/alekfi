# AlekFi

Institutional signal-research and execution-intent pipeline for public-equity event claims.

## Institutional Overview

AlekFi converts noisy claims into trade-ranked forecasts with hard controls:

1. `Swarm` ingests raw cross-platform posts/news/feeds.
2. `Gatekeeper` filters relevance and market impact.
3. `Brain` + `BrainPipeline` clusters claims, computes convergence, and synthesizes signals.
4. `ResearchBundle` compiles canonical claim, evidence graph, forecast by horizon, tradability profile, and EV score.
5. `Postgres` stores signals/outcomes; `Redis` powers queueing/cache/health/circuit-breakers.
6. `API` serves strict/non-strict forecasts, quality metrics, and health endpoints.

## Quality Controls

- Canonical market data via `alekfi/marketdata/gateway.py` with provider fallback, cache, circuit breaker, and quality flags.
- Fail-closed labeling in `alekfi/tools/backfill_labels.py` (degraded data never creates training labels).
- Strict-feed guardrails in `alekfi/api/routes/signals.py`:
  - independence threshold
  - platform diversity
  - tradability pass
  - verified single-modality exception path for high-credibility evidence.
- Convergence hysteresis + candidate persistence in `alekfi/brain/brain_pipeline.py`.

## Evaluation Methodology

- Label horizons: `1h`, `4h`, `1d`, `3d`, `7d` with abnormal-return context.
- Ranking target: expected PnL net cost/risk penalties.
- Evaluation harness: `alekfi/tools/eval_harness.py`
  - Precision@K
  - Brier score (calibration)
  - strict vs non-strict splits
  - verification ablations

## Quick Commands

Run unit tests:

```bash
python3 -m pytest -q
```

Run no-regression guardrails:

```bash
scripts/no_regressions.sh
```

Run metrics snapshot (host with Docker access):

```bash
python3 scripts/metrics_snapshot.py
```

Detailed audit and runbook:
- `docs/QUALITY_RELIABILITY_UPGRADE_REPORT.md`
- `docs/RUNBOOK_QUALITY_PIPELINE.md`
