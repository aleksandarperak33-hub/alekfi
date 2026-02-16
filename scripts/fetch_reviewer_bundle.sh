#!/usr/bin/env bash
set -euo pipefail

HOST="${1:-root@147.93.116.107}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="$REPO_ROOT/docs/reviewer_handoff/artifacts"
TS="$(date -u +%Y%m%d_%H%M%S)"
REMOTE_DIR="/docker/alekfi/reports/reviewer_bundle_${TS}"

mkdir -p "$OUT_DIR"

ssh "$HOST" "TS=$TS bash -s" <<'EOF'
set -euo pipefail
B="/docker/alekfi/reports/reviewer_bundle_${TS}"
mkdir -p "$B"

python3 /docker/alekfi/scripts/metrics_snapshot.py > "$B/metrics_snapshot.json"
docker exec alekfi-brain-1 /bin/sh -lc "python -m alekfi.tools.eval_harness --since-days 30 --topk 5,10,20" > "$B/eval_harness.json"

for c in alekfi-brain-1 alekfi-api-1 alekfi-gatekeeper-1 alekfi-swarm-1; do
  docker logs --since 2h "$c" > "$B/${c}_2h.log" 2>&1 || true
  docker logs --since 2h "$c" 2>&1 | grep -Ei "ERROR|WARNING|AUTH_FAIL|INCOMPLETE_WINDOW|yfinance|finnhub|stooq|traceback" > "$B/${c}_2h_errwarn.log" || true
done

PUSER=$(grep -E '^POSTGRES_USER=' /docker/alekfi/.env | cut -d= -f2- || true)
PDB=$(grep -E '^POSTGRES_DB=' /docker/alekfi/.env | cut -d= -f2- || true)
PPASS=$(grep -E '^POSTGRES_PASSWORD=' /docker/alekfi/.env | cut -d= -f2- || true)
PUSER=${PUSER:-alekfi_db}
PDB=${PDB:-alekfi}

docker exec alekfi-postgres-1 /bin/sh -lc "PGPASSWORD='$PPASS' pg_dump -U '$PUSER' -d '$PDB' -s" > "$B/db_schema.sql"
docker exec alekfi-postgres-1 /bin/sh -lc "PGPASSWORD='$PPASS' psql -U '$PUSER' -d '$PDB' -t -A -F '|' -c \"SELECT column_name FROM information_schema.columns WHERE table_name='signals' ORDER BY ordinal_position;\"" > "$B/signals_columns.txt"
docker exec alekfi-postgres-1 /bin/sh -lc "PGPASSWORD='$PPASS' psql -U '$PUSER' -d '$PDB' -t -A -F '|' -c \"SELECT column_name FROM information_schema.columns WHERE table_name='signal_outcomes' ORDER BY ordinal_position;\"" > "$B/signal_outcomes_columns.txt"
docker exec alekfi-postgres-1 /bin/sh -lc "PGPASSWORD='$PPASS' psql -U '$PUSER' -d '$PDB' -t -A -c \"SELECT row_to_json(t)::text FROM (SELECT * FROM signals ORDER BY created_at DESC LIMIT 50) t;\"" > "$B/sample_signals_50.jsonl"
docker exec alekfi-postgres-1 /bin/sh -lc "PGPASSWORD='$PPASS' psql -U '$PUSER' -d '$PDB' -t -A -c \"SELECT row_to_json(t)::text FROM (SELECT * FROM signal_outcomes ORDER BY created_at DESC LIMIT 50) t;\"" > "$B/sample_signal_outcomes_50.jsonl"

echo "$B"
EOF

scp -r "$HOST:$REMOTE_DIR/." "$OUT_DIR/"

cat <<MSG
Reviewer bundle fetched.
Remote: $REMOTE_DIR
Local : $OUT_DIR
MSG
