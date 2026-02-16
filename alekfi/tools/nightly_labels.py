"""Nightly label job wrapper."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone

from alekfi.tools.backfill_labels import run_backfill_labels


async def _amain() -> None:
    since = datetime.now(timezone.utc) - timedelta(days=2)
    summary = await run_backfill_labels(
        since=since,
        limit=1200,
        include_null_samples=True,
        null_sample_limit=600,
        force=False,
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_amain())
