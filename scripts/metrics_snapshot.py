#!/usr/bin/env python3
"""One-shot AlekFi quality baseline/after snapshot.

Runs on the VPS host (where docker CLI is available).
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path
from typing import Any

ROOT = Path("/docker/alekfi")
ENV_FILE = ROOT / ".env"


def _run(cmd: str) -> tuple[int, str]:
    p = subprocess.run(cmd, shell=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return p.returncode, p.stdout.strip()


def _env_map() -> dict[str, str]:
    out: dict[str, str] = {}
    if not ENV_FILE.exists():
        return out
    for line in ENV_FILE.read_text().splitlines():
        if not line or line.strip().startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def _docker_psql(sql: str, env: dict[str, str]) -> str:
    user = env.get("POSTGRES_USER", "alekfi_db")
    db = env.get("POSTGRES_DB", "alekfi")
    pwd = env.get("POSTGRES_PASSWORD", "")
    cmd = (
        "docker exec alekfi-postgres-1 /bin/sh -lc "
        + shlex.quote(f"PGPASSWORD={pwd} psql -U {user} -d {db} -t -A -F '|' -c {shlex.quote(sql)}")
    )
    _, out = _run(cmd)
    return out


def _fetch_json(url: str) -> Any:
    rc, out = _run(f"curl -s {shlex.quote(url)}")
    if rc != 0 or not out:
        return None
    try:
        return json.loads(out)
    except Exception:
        return None


def _provider_health(env: dict[str, str]) -> dict[str, Any]:
    redis_pwd = env.get("REDIS_PASSWORD", "")
    auth = f"-a {shlex.quote(redis_pwd)} " if redis_pwd else ""
    rc, out = _run(
        f"docker exec alekfi-redis-1 /bin/sh -lc \"redis-cli {auth} --no-auth-warning KEYS 'alekfi:md:cb:*'\""
    )
    if rc != 0:
        return {"error": out}
    keys = [k.strip() for k in out.splitlines() if k.strip()]
    result = {}
    for key in keys:
        rc2, raw = _run(
            f"docker exec alekfi-redis-1 /bin/sh -lc "
            + shlex.quote(f"redis-cli {auth} --no-auth-warning HGETALL {shlex.quote(key)}")
        )
        if rc2 != 0:
            continue
        tokens = raw.splitlines()
        if len(tokens) % 2 == 1:
            tokens.append("")
        vals = {}
        for i in range(0, len(tokens) - 1, 2):
            vals[tokens[i]] = tokens[i + 1]
        result[key] = vals
    return result


def _yfinance_errors() -> dict[str, int]:
    out: dict[str, int] = {}
    for container in ("alekfi-brain-1", "alekfi-api-1", "alekfi-gatekeeper-1", "alekfi-swarm-1"):
        rc, raw = _run(
            "docker logs --since 15m "
            f"{container} 2>&1 | "
            "grep -Eic '\"logger\": \"yfinance\".*\"level\": \"ERROR\"|Invalid Crumb|HTTP Error 401|possibly delisted|Failed download|Quote not found' || true"
        )
        try:
            out[container] = int(raw.strip().splitlines()[-1]) if raw.strip() else 0
        except Exception:
            out[container] = 0
    return out


def main() -> None:
    env = _env_map()
    strict = _fetch_json("http://localhost:8000/api/signals/forecasts?strict=true&limit=200") or []
    nonstrict = _fetch_json("http://localhost:8000/api/signals/forecasts?strict=false&limit=200") or []
    if not isinstance(strict, list):
        strict = []
    if not isinstance(nonstrict, list):
        nonstrict = []

    ind_vals = [float(((x.get("evidence") or {}).get("independence_score") or 0.0)) for x in nonstrict]
    plat_vals = [int(((x.get("evidence") or {}).get("unique_platforms") or 0)) for x in nonstrict]

    labels_counts = _docker_psql(
        "SELECT count(*) AS total, count(labels) AS labeled FROM signal_outcomes;",
        env,
    )
    skip_counts = _docker_psql(
        "SELECT COALESCE(labels->>'label_skipped_reason','none') AS reason, count(*) "
        "FROM signal_outcomes GROUP BY 1 ORDER BY 2 DESC LIMIT 10;",
        env,
    )
    conv_dist = _docker_psql(
        "SELECT "
        "round(COALESCE((metadata->>'cluster_convergence')::numeric,0), 3) AS conv, "
        "count(*) FROM signals "
        "WHERE created_at >= NOW() - INTERVAL '24 hours' "
        "GROUP BY 1 ORDER BY 1 DESC LIMIT 20;",
        env,
    )
    stall = _provider_health(env)  # includes md circuit-breaker states

    # Candidate/stall metrics from Redis.
    redis_pwd = env.get("REDIS_PASSWORD", "")
    auth = f"-a {shlex.quote(redis_pwd)} " if redis_pwd else ""
    _, pipe_metrics = _run(
        "docker exec alekfi-redis-1 /bin/sh -lc "
        + shlex.quote(f"redis-cli {auth} --no-auth-warning HGETALL alekfi:pipeline:metrics")
    )
    _, candidate_keys = _run(
        "docker exec alekfi-redis-1 /bin/sh -lc "
        + shlex.quote(f"redis-cli {auth} --no-auth-warning KEYS 'alekfi:pipeline:cluster_state:*'")
    )
    candidate_count = len([x for x in candidate_keys.splitlines() if x.strip()])

    payload = {
        "strict_count": len(strict),
        "non_strict_count": len(nonstrict),
        "avg_independence": round(sum(ind_vals) / len(ind_vals), 4) if ind_vals else 0.0,
        "avg_unique_platforms": round(sum(plat_vals) / len(plat_vals), 4) if plat_vals else 0.0,
        "label_counts_raw": labels_counts,
        "label_skip_reasons_raw": skip_counts,
        "convergence_distribution_raw": conv_dist,
        "provider_health": stall,
        "yfinance_errors_last_15m": _yfinance_errors(),
        "pipeline_metrics_raw": pipe_metrics,
        "candidate_cluster_count": candidate_count,
    }
    print(json.dumps(payload, indent=2, default=str))


if __name__ == "__main__":
    main()
