from __future__ import annotations

import pytest

from alekfi.workers.signal_decay_worker import SignalDecayWorker


@pytest.mark.asyncio
async def test_decay_worker_skips_when_legacy_checkpoint_columns_missing() -> None:
    worker = SignalDecayWorker()

    async def _unsupported():
        return False

    worker._supports_legacy_signal_checkpoints = _unsupported  # type: ignore[method-assign]

    processed = await worker._check_and_update_signals()
    assert processed == 0
    assert worker._legacy_signal_schema is False
