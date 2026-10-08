"""
Incarnation persistence degradation — the anti-swallow contract.

The recovery-fault campaign traced the defect live: under sustained
ENOSPC the store's saves failed-and-were-absorbed on every bump
(traced virtual 65.25 -> 87.02) while the LIVE incarnation kept
advancing — the persisted value went stale with nothing reported to
any caller or diagnostic surface, silently eroding the restart
zombie-guard margin this store exists to provide.

These tests pin the repaired contract: acceptance (the monotonicity
verdict) is unchanged and never waits on storage; persistence truth is
tracked separately — ``persistence_degraded`` flips on failure, every
failure is counted into ``get_stats``, the next successful save heals
the flag, and the healed value is what a restarted generation loads.
"""

from pathlib import Path

import pytest

from hyperscale.distributed.swim.detection.incarnation_store import (
    IncarnationStore,
)
from tests.simulation.harness.sim import SimFilesystem


async def _initialized_store(
    filesystem: SimFilesystem,
) -> tuple[IncarnationStore, int]:
    store = IncarnationStore(
        storage_directory=Path("/node/incarnation"),
        node_address="sim-node:9000",
        filesystem=filesystem,
    )
    initial_incarnation = await store.initialize()
    return store, initial_incarnation


@pytest.mark.asyncio
async def test_save_failure_flips_degraded_but_accepts_the_bump() -> None:
    """Protocol monotonicity cannot wait on storage: the bump is
    accepted and the live value advances — but the failed save is
    LOUD state, never a silent True."""
    filesystem = SimFilesystem()
    store, initial_incarnation = await _initialized_store(filesystem)
    assert store.persistence_degraded is False

    filesystem.set_disk_full(0)
    accepted = await store.update_incarnation(initial_incarnation + 5)
    assert accepted is True
    assert await store.get_incarnation() == initial_incarnation + 5
    assert store.persistence_degraded is True

    stats = store.get_stats()
    assert stats["persistence_degraded"] is True
    assert stats["persist_failure_count"] == 1


@pytest.mark.asyncio
async def test_repeated_failures_are_counted_not_absorbed() -> None:
    filesystem = SimFilesystem()
    store, initial_incarnation = await _initialized_store(filesystem)

    filesystem.set_disk_full(0)
    for bump in range(1, 4):
        assert await store.update_incarnation(initial_incarnation + bump)
    assert store.get_stats()["persist_failure_count"] == 3
    assert store.persistence_degraded is True


@pytest.mark.asyncio
async def test_next_successful_save_heals_and_persists_the_live_value() -> None:
    """When the disk recovers, the very next accepted bump writes the
    CURRENT live value — a rebooted generation then loads the healed
    record, restoring the zombie-guard margin."""
    filesystem = SimFilesystem()
    store, initial_incarnation = await _initialized_store(filesystem)

    filesystem.set_disk_full(0)
    assert await store.update_incarnation(initial_incarnation + 5)
    assert store.persistence_degraded is True

    filesystem.clear_disk_full()
    assert await store.update_incarnation(initial_incarnation + 6)
    assert store.persistence_degraded is False

    rebooted = IncarnationStore(
        storage_directory=Path("/node/incarnation"),
        node_address="sim-node:9000",
        filesystem=filesystem,
    )
    rebooted_incarnation = await rebooted.initialize()
    assert rebooted_incarnation == (
        initial_incarnation + 6 + rebooted.restart_incarnation_bump
    )


@pytest.mark.asyncio
async def test_monotonicity_rejection_is_not_a_persistence_event() -> None:
    filesystem = SimFilesystem()
    store, initial_incarnation = await _initialized_store(filesystem)

    accepted = await store.update_incarnation(initial_incarnation)
    assert accepted is False
    assert store.persistence_degraded is False
    assert store.get_stats()["persist_failure_count"] == 0
