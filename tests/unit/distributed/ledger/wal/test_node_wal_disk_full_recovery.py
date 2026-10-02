"""
The node WAL survives a full disk: a failed append is rolled back and
forgotten, and later appends -- once space frees -- are durable.

One failed group commit used to latch the WAL writer forever (a single
ENOSPC wedged the job ledger until restart). Continuing after a failure
needs two things this pins: the torn prefix the failed append left is
cut back (recovery stops at the first invalid frame, so a torn record
left in place would hide every later record), and the failed entry is
not left pending in memory as if it were durable.

Driven through the real NodeWAL over the SIM filesystem, whose full
device writes the bytes that fit and then raises ENOSPC (POSIX short
write semantics).

* A failed append fails its caller, leaves no pending entry, and the
  WAL keeps appending once space frees; a restart recovers exactly the
  acknowledged entries.
* Seeded runs of appends interleaved with disk-full windows of random
  budgets and restarts: after every restart, recovery yields exactly
  the acknowledged appends, in order.
"""

import random
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.wal.node_wal import NodeWAL
from hyperscale.distributed.ledger.wal.wal_entry import JobEventType
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/node.wal")
SEEDS = range(20)
CYCLES = 6
MAXIMUM_APPENDS_PER_CYCLE = 6
MAXIMUM_DISK_FULL_BUDGET_BYTES = 256


async def open_wal(filesystem: SimFilesystem) -> NodeWAL:
    return await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), filesystem=filesystem)


async def recovered_payloads(filesystem: SimFilesystem) -> list[bytes]:
    wal = await open_wal(filesystem)
    payloads = [entry.payload async for entry in wal.iter_from(0)]
    await wal.close()
    return payloads


async def append_acknowledged(wal: NodeWAL, payload: bytes) -> bool:
    try:
        await wal.append(JobEventType.JOB_CREATED, payload)
    except OSError:
        return False
    return True


@pytest.mark.asyncio
async def test_a_failed_append_is_forgotten_and_later_appends_are_durable() -> None:
    filesystem = SimFilesystem()
    wal = await open_wal(filesystem)
    assert await append_acknowledged(wal, b"first")
    assert await append_acknowledged(wal, b"second")
    pending_before_failure = wal.pending_count

    filesystem.set_disk_full(3)
    assert not await append_acknowledged(wal, b"lost-to-a-full-disk")
    assert wal.pending_count == pending_before_failure

    filesystem.clear_disk_full()
    assert await append_acknowledged(wal, b"third")
    await wal.close()

    assert await recovered_payloads(filesystem) == [b"first", b"second", b"third"]


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_recovery_yields_exactly_the_acknowledged_appends(seed: int) -> None:
    rng = random.Random(seed)
    filesystem = SimFilesystem()
    acknowledged: list[bytes] = []

    for cycle in range(CYCLES):
        wal = await open_wal(filesystem)
        for index in range(rng.randint(1, MAXIMUM_APPENDS_PER_CYCLE)):
            if rng.random() < 0.3:
                filesystem.set_disk_full(rng.randint(0, MAXIMUM_DISK_FULL_BUDGET_BYTES))
            elif rng.random() < 0.5:
                filesystem.clear_disk_full()
            payload = f"seed-{seed}-cycle-{cycle}-entry-{index}".encode()
            if await append_acknowledged(wal, payload):
                acknowledged.append(payload)
        await wal.close()
        filesystem.clear_disk_full()

        assert await recovered_payloads(filesystem) == acknowledged, (seed, cycle)
