# AlekFi Architecture Quick Map

## Runtime Services
- `api`: serves signal/forecast endpoints.
- `brain`: clustering, convergence, research bundle construction, label jobs.
- `gatekeeper`: filtering/classification layer before high-cost processing.
- `swarm`: multi-source ingestion workers.
- `postgres`: persistent signals/outcomes/metadata store.
- `redis`: queueing, provider health, cache, pipeline metrics.

## Core Data Path
1. Swarm ingests noisy claims.
2. Gatekeeper filters/ranks claims.
3. Brain clusters and computes convergence.
4. Research bundle enriches evidence + forecast + tradability.
5. API exposes strict/non-strict forecasts.
6. Outcomes are labeled and fed into evaluation harness.

## Reliability-Critical Modules
- Market data canonical gateway:
  - `alekfi/marketdata/gateway.py`
- Labeling / backfill / nightly:
  - `alekfi/tools/backfill_labels.py`
  - `alekfi/tools/nightly_labels.py`
- Convergence + candidate persistence:
  - `alekfi/brain/brain_pipeline.py`
- Corroboration enrichment:
  - `alekfi/workers/corroboration_worker.py`
- Strict ranking endpoint:
  - `alekfi/api/routes/signals.py`
- Evaluation harness:
  - `alekfi/tools/eval_harness.py`

## Guardrails
- No direct `yfinance` imports outside gateway.
- Fail-closed label writing on degraded data flags.
- Strict feed requires independence/platform controls, with explicit verified exception path.

## Operational Metrics
- `scripts/metrics_snapshot.py` reports:
  - strict/non-strict counts
  - evidence distributions
  - labeling coverage + skip reasons
  - provider health + recent error rates
  - convergence/candidate metrics
