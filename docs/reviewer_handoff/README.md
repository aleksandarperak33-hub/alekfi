# Reviewer Handoff Bundle (AlekFi)

This folder is a complete handoff package for external code/reliability analysis.

## What To Review First
1. `../QUALITY_RELIABILITY_UPGRADE_REPORT.md`
2. `../RUNBOOK_QUALITY_PIPELINE.md`
3. `ARCHITECTURE_QUICKMAP.md`
4. `KNOWN_ISSUES_AND_REPRO.md`
5. `artifacts/metrics_snapshot.json`
6. `artifacts/eval_harness.json`

## Included Artifacts
- Runtime quality metrics:
  - `artifacts/metrics_snapshot.json`
  - `artifacts/eval_harness.json`
- DB introspection + samples:
  - `artifacts/db_schema.sql`
  - `artifacts/signals_columns.txt`
  - `artifacts/signal_outcomes_columns.txt`
  - `artifacts/sample_signals_50.jsonl`
  - `artifacts/sample_signal_outcomes_50.jsonl`
- Log snapshots (last 2h when generated):
  - `artifacts/alekfi-brain-1_2h.log`
  - `artifacts/alekfi-brain-1_2h_errwarn.log`
  - `artifacts/alekfi-swarm-1_2h.log`
  - `artifacts/alekfi-swarm-1_2h_errwarn.log`
  - `artifacts/alekfi-api-1_2h.log`
  - `artifacts/alekfi-api-1_2h_errwarn.log`
  - `artifacts/alekfi-gatekeeper-1_2h.log`
  - `artifacts/alekfi-gatekeeper-1_2h_errwarn.log`

## Environment + Access
- Sanitized env template: `ENV_SANITIZED.example`
- Access guidance: `VPS_ACCESS_CHECKLIST.md`

## Regeneration Commands
From local machine:
```bash
ssh root@147.93.116.107 "python3 /docker/alekfi/scripts/metrics_snapshot.py"
ssh root@147.93.116.107 "docker exec alekfi-brain-1 /bin/sh -lc 'python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20'"
ssh root@147.93.116.107 "/docker/alekfi/scripts/no_regressions.sh"
```

## Scope Note
This repo review is strongest when combined with live-system artifacts and runbook steps. Static GitHub review alone is not sufficient for infra/data quality failures.
