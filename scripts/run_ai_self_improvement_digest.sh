#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MEMORY_FILE="${1:-$REPO_ROOT/memory/ai-digest-posted.json}"
OUT_FILE="${2:-$REPO_ROOT/memory/ai-digest-latest.md}"

mkdir -p "$(dirname "$MEMORY_FILE")"
if [[ ! -f "$MEMORY_FILE" ]]; then
  cp "$REPO_ROOT/skills/ai-self-improvement-digest/references/ai-digest-posted.template.json" "$MEMORY_FILE"
fi

cmd=(
  python3
  "$REPO_ROOT/skills/ai-self-improvement-digest/scripts/generate_digest.py"
  --memory "$MEMORY_FILE"
  --output "$OUT_FILE"
)
if [[ -n "${AI_DIGEST_OFFLINE_JSON:-}" ]]; then
  cmd+=(--offline-json "$AI_DIGEST_OFFLINE_JSON")
fi

"${cmd[@]}"

echo "Digest written to: $OUT_FILE"
