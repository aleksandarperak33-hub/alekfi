# AlekFi Quality & Reliability Upgrade Report
Date: 2026-02-16
Scope: institutional-grade signal quality, strict trade-readiness, forecast rigor, and robustness.

## Executive Summary
This upgrade hardens AlekFi around the quality-critical path:
- One canonical market-data path with fallback/circuit-breakers/quality flags.
- Fail-closed label generation to stop degraded-data poisoning.
- Strict-feed enrichment via corroboration and explainable independence scoring.
- Convergence hysteresis + candidate persistence to prevent near-threshold stalls.
- Forecast ranking shifted to expected-value semantics with model-backed priors.
- API-level observability for signal quality and forecast calibration.

## Architecture Map (Data Lineage)
`Swarm` -> `Gatekeeper` -> `Brain` -> `BrainPipeline` -> `ResearchBundle` -> `Postgres/Redis` -> `API`.

Key flow points:
- Ingestion: raw claims/posts from connectors.
- Aggregation: cluster + convergence + synthesis.
- Research bundle: normalized claim, evidence graph, tradability, forecast, EV score.
- Outcomes: signal_outcomes labels for training/eval.
- Serving: strict/non-strict forecasts and quality/calibration metrics endpoints.

## Top 10 Issues (Impact-Ranked)
1. **Market data contamination risk from provider instability**
   - Fixed by canonical gateway contract + failover + quality flags.
   - Refs: `alekfi/marketdata/gateway.py:111`, `alekfi/marketdata/gateway.py:242`, `alekfi/marketdata/gateway.py:936`
2. **Direct provider coupling in workers (quality drift + inconsistency)**
   - Mitigated by routing all quotes/OHLCV via gateway and adding guardrails.
   - Refs: `alekfi/workers/signal_decay_worker.py:267`, `tests/test_marketdata_guardrails.py:13`
3. **Label poisoning from degraded data**
   - Fail-closed labeling with explicit skip reasons + confidence.
   - Refs: `alekfi/tools/backfill_labels.py:182`, `alekfi/tools/backfill_labels.py:394`, `tests/test_marketdata_guardrails.py:42`
4. **Strict feed starvation despite promising near-threshold signals**
   - Added corroboration worker + queue-driven enrichment + provenance fields.
   - Refs: `alekfi/workers/corroboration_worker.py:48`, `alekfi/workers/corroboration_worker.py:93`, `alekfi/workers/corroboration_worker.py:253`
5. **Weak independence semantics (echo-chamber false positives)**
   - Added explainable independence breakdown and verification-hit logic.
   - Refs: `alekfi/brain/research_bundle.py:179`, `alekfi/workers/corroboration_worker.py:327`
6. **Convergence stalling near threshold**
   - Added hysteresis and candidate persistence with measurable stall metrics.
   - Refs: `alekfi/brain/brain_pipeline.py:679`, `alekfi/brain/brain_pipeline.py:751`
7. **Forecast quality constrained by heuristics only**
   - Added empirical priors trainer + inference blending (type-specific + global fallback).
   - Refs: `alekfi/tools/train_forecast_model.py:46`, `alekfi/brain/research_bundle.py:402`
8. **Strict ranking not fully aligned to directional calibrated probability**
   - Fixed API output to directional `calibrated_prob` by signal direction.
   - Refs: `alekfi/api/routes/signals.py:216`
9. **Insufficient quality observability for diligence**
   - Added quality/calibration endpoints and provider health exposure.
   - Refs: `alekfi/api/routes/system.py:54`, `alekfi/api/routes/system.py:139`, `alekfi/api/routes/system.py:161`
10. **Schema/bootstrap fragility across concurrent service startup**
   - Added advisory lock around DB bootstrap and legacy-schema guard for decay worker.
   - Refs: `alekfi/db/database.py:50`, `alekfi/workers/signal_decay_worker.py:453`

## Baseline -> Current Metrics
### Known baseline from production findings
- strict forecasts: `0`
- non-strict forecasts: `5`
- labeled outcomes: `11 / 565`
- repeated yfinance auth/crumb failures in worker logs
- convergence candidate stalling around threshold

### Local validation snapshot in this run
- `python3 -m pytest -q` -> `14 passed`
- API/compose runtime re-check was partially blocked because Docker daemon became unavailable mid-run.
- Metric capture scaffolding is in place (`scripts/metrics_snapshot.py`) and should be rerun on active Docker host to refresh before/after table.

## What Changed (Phase-Aligned)
### P0 / Phase 1: Market Data Reliability
- Canonical market-data contract `MarketDataSnapshot` fields:
  - `last_price`, `asof`, `volume`, `dollar_volume`, `ret_1d`, `ret_5d`, `atr_14`, `spread_bps_est`, `quality_flags`.
- Deterministic daily fallback chain and provider circuit-breakers.
- Intraday explicitly fail-closed when no non-yfinance intraday provider is configured.
- Added market-data provider health API payload.
- Refs:
  - `alekfi/marketdata/gateway.py:242`
  - `alekfi/marketdata/gateway.py:455`
  - `alekfi/api/routes/system.py:13`

### P0 / Phase 2: Densify Labels + Integrity
- Backfill/labeling supports multi-horizon outcomes and abnormal return context.
- Degraded data writes skip labels with machine-readable reasons.
- Refs:
  - `alekfi/tools/backfill_labels.py:44`
  - `alekfi/tools/backfill_labels.py:182`
  - `alekfi/tools/backfill_labels.py:270`

### P0 / Phase 3: Strict Feed Starvation
- Corroboration worker now consumes near-threshold queue + ticker-matched queue candidates.
- Provenance fields added to enrichment artifacts (`retrieval_time`, `source_type`).
- Strict exceptions remain constrained (verified single-modality only).
- Refs:
  - `alekfi/workers/corroboration_worker.py:48`
  - `alekfi/workers/corroboration_worker.py:93`
  - `alekfi/workers/corroboration_worker.py:384`

### P1 / Phase 4: Convergence Robustness
- Hysteresis promotion/hold with `T_high/T_low`.
- Candidate persistence and corroboration queueing when dwell-time exceeds threshold.
- Refs:
  - `alekfi/brain/brain_pipeline.py:679`

### P0/P1 / Phase 5: Forecast Quality + EV Ranking
- Forecast prior trainer stores empirical priors in Redis and versioned artifact.
- Forecast construction blends signal priors + global priors.
- Forecast serving ranks by expected PnL net costs/risk and now exposes directional calibrated probability.
- Refs:
  - `alekfi/tools/train_forecast_model.py:46`
  - `alekfi/brain/research_bundle.py:402`
  - `alekfi/api/routes/signals.py:216`

### P1 / Phase 5: Evaluation + Metrics
- Evaluation harness with tiered Precision@K, Brier, strict-vs-non-strict splits, verification ablation.
- API metrics endpoints for easy ops/diligence consumption.
- Refs:
  - `alekfi/tools/eval_harness.py:85`
  - `alekfi/api/routes/system.py:139`
  - `alekfi/api/routes/system.py:161`

## Acceptance Tests Added/Updated
- `tests/test_marketdata_guardrails.py`
- `tests/test_marketdata_contract.py`
- `tests/test_signal_decay_schema_guard.py`
- `tests/test_corroboration_worker.py`
- `tests/test_forecast_contract.py`
- `tests/test_api_routes_contract.py`
- `scripts/no_regressions.sh`

Result in this run:
- `python3 -m pytest -q` -> `14 passed`.

## How to Verify
1. Unit tests
```bash
python3 -m pytest -q
```
2. No-regression guardrails
```bash
scripts/no_regressions.sh
```
3. Baseline/current metrics snapshot (Docker host)
```bash
python3 scripts/metrics_snapshot.py
```
4. Forecast eval harness
```bash
python3 -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20
```

## Rollback Plan
1. Revert last commit set and redeploy prior image set.
2. Disable corroboration/priors loops via startup toggles in `alekfi/main.py`.
3. Keep strict feed enabled but rely on existing persisted bundles until retraining stabilizes.
4. Re-run `scripts/no_regressions.sh` and confirm label skip rates normalize.

## Prioritized Roadmap
### P0 (Immediate)
- Bring Docker runtime back and capture fresh before/after metrics from `scripts/metrics_snapshot.py` and eval harness.
- Add a non-yfinance intraday provider adapter (Polygon/IEX/Alpaca/TwelveData) to re-enable intraday features safely.
- Move forecast priors from aggregate priors toward time-split calibrated models (isotonic/Platt by horizon).

### P1 (Next)
- Expand verification recipes by event type (outage/layoff/regulatory/app-quality).
- Add null-cohort negative samples into dedicated training table and include in model features.
- Add latency-aware paper backtest and capacity constraints per signal family.

### P2 (Hardening)
- CI integration for no-regression + eval smoke metrics.
- Feature store formalization for training/inference parity.
- Dashboarding for strict funnel, calibration drift, and provider health trends.

## Known Limitations
- Current learned layer is empirical-prior blending, not yet a full supervised calibrated model stack.
- Intraday features are intentionally disabled until a non-yfinance intraday adapter is wired.
- Latest runtime endpoint validation is pending Docker daemon availability in this environment.
