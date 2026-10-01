"""
Phase 7 storage faults: the SimFilesystem knobs and the production
recovery paths they exercise.

Knob semantics (all deterministic):
* slow_disk — operations cost VIRTUAL time (awaits the injected clock
  inside the op: the reason the seam is async).
* disk_full — a byte budget; exceeding writes raise OSError(ENOSPC).
* fsync_reorder — crash keeps a SEEDED SUBSET of un-fsynced segments
  and tears the last survivor: out-of-order persistence, not a clean
  suffix.

Production scenarios: NodeWAL and RaftWAL group-commit entries through
the seam, survive power loss (fsynced = durable by construction), and
recover TRUNCATION-SAFELY past crash debris; the idempotency ledger
does the same end-to-end under SIM.
"""

import asyncio
import struct
from pathlib import Path

import pytest

from hyperscale.distributed.hlc import HLCTimestamp
from hyperscale.distributed.idempotency.idempotency_config import (
    IdempotencyConfig,
)
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.idempotency.idempotency_status import (
    IdempotencyStatus,
)
from hyperscale.distributed.idempotency.manager_ledger import (
    ManagerIdempotencyLedger,
)
from hyperscale.distributed.ledger.wal.node_wal import NodeWAL
from hyperscale.distributed.ledger.wal.wal_entry import JobEventType
from hyperscale.distributed.raft.models import RaftLogEntry
from hyperscale.distributed.raft.raft_wal import RaftWAL
from tests.simulation.harness.sim import (
    SimFilesystem,
    SimulationLoop,
    VirtualClock,
)
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock


class _StubRunner:
    def run(self, *args, **kwargs):
        return None

    async def cancel(self, token: str) -> None:
        return None


class _RecordingLogger:
    def __init__(self) -> None:
        self.messages: list[str] = []

    async def log(self, model) -> None:
        self.messages.append(model.message)


def _run_virtual(coroutine_factory):
    """Run a scenario coroutine on a SimulationLoop with VirtualClock;
    returns (result, final_virtual_time)."""
    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    try:
        clock = VirtualClock(loop)
        result = loop.run_until_complete(coroutine_factory(clock))
        return result, loop.time()
    finally:
        loop.close()
        asyncio.set_event_loop(None)


# -- knob semantics -------------------------------------------------------


def test_slow_disk_costs_virtual_time():
    async def scenario(clock):
        filesystem = SimFilesystem(clock=clock)
        filesystem.set_slow_disk(0.25)
        await filesystem.append_fsync("/wal/events.wal", b"entry")
        await filesystem.read_bytes("/wal/events.wal")
        filesystem.clear_slow_disk()
        await filesystem.append_fsync("/wal/events.wal", b"free")
        return None

    _result, elapsed = _run_virtual(scenario)
    # Two charged operations at 0.25 virtual seconds each; the third is
    # free after clear_slow_disk.
    assert elapsed == pytest.approx(0.5, abs=1e-9)


def test_slow_disk_without_clock_is_rejected():
    filesystem = SimFilesystem()
    with pytest.raises(ValueError):
        filesystem.set_slow_disk(0.1)


@pytest.mark.asyncio
async def test_disk_full_budget_raises_enospc():
    filesystem = SimFilesystem()
    filesystem.set_disk_full(10)

    await filesystem.append_fsync("/data/a.bin", b"12345")  # 5 of 10
    with pytest.raises(OSError) as exception_info:
        await filesystem.append_fsync("/data/a.bin", b"6789012345")  # > 5 left
    assert exception_info.value.errno == 28

    # The failed write consumed nothing; the remaining budget still fits.
    await filesystem.append_fsync("/data/a.bin", b"67890")

    filesystem.clear_disk_full()
    await filesystem.append_fsync("/data/a.bin", b"unbounded again")


@pytest.mark.asyncio
async def test_fsync_reorder_crash_keeps_seeded_subset_with_torn_tail():
    async def build(seed: int) -> bytes:
        filesystem = SimFilesystem()
        handle = await filesystem.open("/data/log.bin", "ab")
        for index in range(8):
            await handle.write(f"segment-{index}|".encode())
        filesystem.set_fsync_reorder(seed)
        filesystem.crash()
        return await filesystem.read_bytes("/data/log.bin")

    survivors_first = await build(11)
    survivors_second = await build(11)
    survivors_other_seed = await build(12)
    all_segments = b"".join(f"segment-{index}|".encode() for index in range(8))

    # Deterministic: same seed, same surviving junk; different seed
    # differs; and the survivors are a strict subset (out-of-order
    # persistence, not everything and not a clean nothing).
    assert survivors_first == survivors_second
    assert survivors_first != survivors_other_seed
    assert survivors_first != all_segments


@pytest.mark.asyncio
async def test_durable_content_survives_reorder_crash_untouched():
    filesystem = SimFilesystem()
    await filesystem.append_fsync("/data/wal.bin", b"DURABLE|")
    handle = await filesystem.open("/data/wal.bin", "ab")
    await handle.write(b"volatile-1|")
    await handle.write(b"volatile-2|")

    filesystem.set_fsync_reorder(3)
    filesystem.crash()

    content = await filesystem.read_bytes("/data/wal.bin")
    assert content.startswith(b"DURABLE|")


# -- production recovery paths under SIM ----------------------------------


@pytest.mark.asyncio
async def test_node_wal_survives_crash_and_tolerates_torn_tail():
    filesystem = SimFilesystem()
    clock = new_hybrid_logical_clock()
    wal_path = Path("/ledger/node.wal")

    wal = await NodeWAL.open(wal_path, clock, filesystem=filesystem)
    await wal.append(JobEventType.JOB_CREATED, b"job-1")
    await wal.append(JobEventType.JOB_ACCEPTED, b"job-1")
    await wal.close()

    # Power loss (group commits fsynced -> durable), plus crash debris:
    # a torn frame promising more bytes than exist.
    filesystem.crash()
    await filesystem.append_fsync(wal_path, struct.pack(">I", 0) + b"\xff\xff")

    recovered = await NodeWAL.open(
        wal_path, new_hybrid_logical_clock(), filesystem=filesystem
    )
    recovered_entries = [entry async for entry in recovered.iter_from(0)]
    assert len(recovered_entries) == 2
    assert [entry.payload for entry in recovered_entries] == [
        b"job-1",
        b"job-1",
    ]
    await recovered.close()


@pytest.mark.asyncio
async def test_raft_wal_survives_crash_and_stops_at_corruption():
    filesystem = SimFilesystem()
    wal_path = Path("/raft/raft.wal")

    def _entry(term: int, index: int, command: bytes) -> RaftLogEntry:
        return RaftLogEntry(
            term=term,
            index=index,
            command=command,
            command_type="stats_update",
            job_id="job-1",
            hlc=HLCTimestamp(wall_ms=index, logical=0, node_id=1),
        )

    wal = RaftWAL(wal_path, _RecordingLogger(), filesystem=filesystem)
    await wal.open()
    assert await wal.append(_entry(1, 1, b"cmd-1"))
    assert await wal.append(_entry(1, 2, b"cmd-2"))
    await wal.close()

    filesystem.crash()
    # Crash debris after the durable entries: garbage that fails CRC.
    await filesystem.append_fsync(wal_path, b"\x00" * 24)

    recovered_wal = RaftWAL(
        wal_path, _RecordingLogger(), filesystem=filesystem
    )
    await recovered_wal.open()
    entries = await recovered_wal.recover()
    assert [(entry.term, entry.index) for entry in entries] == [(1, 1), (1, 2)]
    await recovered_wal.close()


@pytest.mark.asyncio
async def test_idempotency_ledger_crash_recovery_under_sim():
    filesystem = SimFilesystem()
    wal_path = Path("/idempotency/ledger.wal")
    key = IdempotencyKey(client_id="client", sequence=1, nonce="feedface")

    ledger = ManagerIdempotencyLedger(
        IdempotencyConfig(),
        wal_path,
        _StubRunner(),
        _RecordingLogger(),
        filesystem=filesystem,
    )
    await ledger.start()
    await ledger.check_or_reserve(key, "job-1")
    await ledger.commit(key, b"result-1")
    await ledger.close()

    filesystem.crash()

    recovered = ManagerIdempotencyLedger(
        IdempotencyConfig(),
        wal_path,
        _StubRunner(),
        _RecordingLogger(),
        filesystem=filesystem,
    )
    await recovered.start()
    entry = recovered.get_by_key(key)
    assert entry is not None
    assert entry.status == IdempotencyStatus.COMMITTED
    assert entry.result_serialized == b"result-1"
    await recovered.close()
