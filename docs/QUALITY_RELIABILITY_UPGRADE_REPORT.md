# AlekFi Quality & Reliability Upgrade Report
Date: 2026-02-16
Environment: VPS `147.93.116.107` (`/docker/alekfi`, active DB: `alekfi`)

## Executive Summary
This upgrade moved AlekFi from fragile heuristic pipelines to a measurable, fail-closed research/execution stack:
- Centralized all market data access behind one gateway with fallback, quality flags, provider health, cache, and circuit-breakers.
- Stopped label poisoning by enforcing fail-closed labeling rules and storing skip reasons.
- Densified outcomes from sparse labels to a usable training/eval corpus.
- Unblocked strict feed starvation without weakening guardrails by adding corroboration enrichment, explainable independence, and verified single-modality exceptions.
- Hardened convergence with candidate persistence + hysteresis and added candidate/stall instrumentation.
- Shifted ranking to EV-oriented forecast outputs (`P(move) × E(move) × timing`, net penalty terms).

## Baseline -> Current Metrics
Baseline (pre-upgrade from Phase-0 snapshot):
- Strict forecasts: `0`
- Non-strict forecasts: `5`
- Labeled outcomes: `11 / 565`
- yfinance errors: high, repeated across workers (401/crumb/symbol issues)
- Convergence: near-threshold clusters stalled

Current (post-upgrade; `python3 /docker/alekfi/scripts/metrics_snapshot.py`):
- Strict forecasts: `9`
- Non-strict forecasts: `34`
- Avg independence (non-strict set): `0.7144`
- Avg unique platforms (non-strict set): `3.2353`
- Labeled outcomes: `594 / 594`
- Label skip reasons tracked: `DEGRADED_MARKET_DATA=10`, `NO_VALID_TICKERS=1`
- yfinance errors last 15m: `0` across `brain/api/gatekeeper/swarm`
- Candidate cluster count visible: `176` with pipeline hysteresis metrics persisted

Artifacts:
- `/docker/alekfi/reports/metrics_snapshot_20260216_1456.json`
- `/docker/alekfi/reports/eval_harness_20260216_1456.json`

## Phase-by-Phase Changes
## Phase 1: Market Data Gateway (P0)
Implemented canonical gateway: `alekfi/marketdata/gateway.py`
- `normalize_symbol`, `get_quote`, `get_ohlcv`, `get_price_at`
- Provider fallback chain: `finnhub -> stooq -> yfinance`
- Uniform metadata: `provider_used`, `fallback_chain`, `quality_flags`, `data_completeness`, `cache_hit`, `fetched_at`
- Redis cache for quote/OHLCV payloads
- Provider circuit-breakers and symbol quarantine
- Label-safe helpers: `is_degraded`, `label_skip_reason`
- Adapter-level yfinance log suppression to prevent error bleed into ops metrics

Integration completed across workers/services:
- `alekfi/brain/price_tracker.py`
- `alekfi/workers/signal_decay_worker.py`
- `alekfi/workers/market_context_worker.py`
- `alekfi/analysis/macro_regime.py`
- `alekfi/swarm/commodities.py`
- `alekfi/swarm/earnings_calendar.py`

Why this matters:
- Prevents degraded provider responses from contaminating features/labels.
- Gives deterministic diagnostics and health state for due diligence.

## Phase 2: Label Densification + Integrity (P2)
Added tools:
- `alekfi/tools/backfill_labels.py`
- `alekfi/tools/nightly_labels.py`

Upgrades:
- Multi-horizon labels (1h/4h/1d/3d/7d where data exists)
- Direction, abnormal-return context, excursion/timing fields
- Fail-closed label writing on degraded quality flags
- Stored: `label_confidence`, `price_source`, `quality_flags`, `labeling_version`, `label_skipped_reason`
- Backfill supports checkpoint-based densification path for reliability and coverage

Result:
- Backfill run labeled `593` of `594` processed rows in one pass (1 skipped invalid ticker).

## Phase 3: Strict Feed Starvation (P1)
Added corroboration/evidence enrichment:
- `alekfi/workers/corroboration_worker.py`
- Enhanced evidence computation in `alekfi/brain/research_bundle.py`

Upgrades:
- Explainable `independence_breakdown` (echo penalty/origin diversity/modality/time separation)
- Verification hits for high-priority claims
- Strict guardrails preserved, plus verified single-modality exception path
- Research bundle now includes richer learned EV by horizon

Result:
- Strict feed moved from empty to non-empty while keeping independent evidence constraints.

## Phase 4: Convergence Robustness (P3)
Upgraded gating in `alekfi/brain/brain_pipeline.py`:
- Hysteresis gates (`T_high` promote, `T_low` hold/demote band)
- Candidate persistence for near-threshold clusters
- Re-enrichment trigger path for candidates
- Redis metrics for promotion/stall observability

Result:
- Candidate pathway is explicit and measurable instead of silent stalling.

## Phase 5: Forecast/Ranking Upgrade (P4)
Upgraded forecast and API ranking:
- `alekfi/brain/research_bundle.py`
- `alekfi/api/routes/signals.py`
- `alekfi/tools/eval_harness.py`

Upgrades:
- EV-oriented ranking from forecast components and tradability penalties
- Output fields include calibrated-style forecast payload (`calibrated_prob`, expected return, time bucket)
- Evaluation harness computes Precision@K, Brier, strict-vs-non-strict, and verification ablations by horizon

## Acceptance Tests and Validation
Automated guardrails:
- `tests/test_marketdata_guardrails.py`
  - blocks direct `import yfinance` outside gateway
  - asserts fail-closed label skip behavior
- `scripts/no_regressions.sh`
  - static import guard
  - runtime fail-closed smoke test
  - optional pytest run
  - metrics snapshot output

Run:
```bash
ssh root@147.93.116.107 "/docker/alekfi/scripts/no_regressions.sh"
```

Evaluation:
```bash
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
```

## Rollback Plan
If regression occurs:
1. Restore pre-upgrade backup:
   - `/docker/alekfi/backups/upgrade_20260216_134748`
2. Redeploy prior containers:
```bash
ssh root@147.93.116.107 "cd /docker/alekfi && docker compose up -d --build brain api swarm gatekeeper"
```
3. Disable corroboration worker by removing startup invocation in `alekfi/main.py` (or env-gate if added later).
4. Freeze label jobs by stopping nightly task + backfill cron until data quality is restored.

## Known Limitations
- Evaluation depth is currently strong for short horizons (1h/4h) and sparse for longer realized horizons in current live window.
- Verification recipes are lightweight and should be expanded by event type.
- Some provider last-error fields can still show `NO_DATA/PROVIDER_ERROR` in healthy state; this is expected telemetry, not an open circuit.

## Next Steps
1. Expand null-cohort negative sampling into a dedicated table and include it in eval dashboards.
2. Add full calibration fit/reporting (reliability curves by event type and regime).
3. Add CI job to run `scripts/no_regressions.sh` equivalent in test containers on every deployment.
4. Add sector-benchmark abnormal-return labeling where ETF mappings are available.
