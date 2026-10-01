"""
NodeWAL recovery cuts the file back to its last recoverable entry.

Recovery stops at the first torn or corrupt frame. Left in place, that
frame hid every entry appended after the restart from the NEXT recovery:
measured, an append made after recovering a crash's torn tail was gone
after the following restart. Pinned:

* Torn tail (a crash mid-append), then appends, then a restart: every
  entry -- before and after the crash -- is recovered, in order.
* A corrupt frame mid-file: recovery keeps the entries before it, and
  appends after the restart survive the next one.
* The discarded bytes are preserved beside the WAL (never destroyed) and
  the discard is reported.
* Seeded crash cycles (random appends, power loss, random torn debris,
  repeated): every entry made durable before a crash is recovered, in
  order, after every later restart.
"""

import random
import struct
from pathlib import Path

import pytest

from hyperscale.distributed.ledger.wal.node_wal import WAL_FORMAT, NodeWAL
from hyperscale.distributed.ledger.wal.wal_entry import JobEventType
from hyperscale.logging.hyperscale_logging_models import WALTailDiscarded
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/node.wal")
TORN_FRAME = struct.pack(">I", 0) + b"\xff\xff"
CRASH_CYCLE_SEEDS = range(20)
CRASH_CYCLES = 6
MAXIMUM_APPENDS_PER_CYCLE = 5


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


async def _append_all(filesystem: SimFilesystem, payloads: list[bytes], logger: RecordingLogger | None = None) -> None:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), logger=logger, filesystem=filesystem)
    for payload in payloads:
        await wal.append(JobEventType.JOB_CREATED, payload)
    await wal.close()


async def _recovered(filesystem: SimFilesystem, logger: RecordingLogger | None = None) -> list[bytes]:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), logger=logger, filesystem=filesystem)
    payloads = [entry.payload async for entry in wal.iter_from(0)]
    await wal.close()
    return payloads


@pytest.mark.asyncio
async def test_appends_after_recovering_a_torn_tail_survive_the_next_restart() -> None:
    filesystem = SimFilesystem()
    await _append_all(filesystem, [b"before-1", b"before-2"])
    filesystem.crash()
    await filesystem.append_fsync(WAL_PATH, TORN_FRAME)
    logger = RecordingLogger()

    await _append_all(filesystem, [b"after-1"], logger)

    assert await _recovered(filesystem) == [b"before-1", b"before-2", b"after-1"]
    (report,) = [entry for entry in logger.entries if isinstance(entry, WALTailDiscarded)]
    assert (report.discarded_bytes, report.recovered_entries) == (len(TORN_FRAME), 2)
    assert await filesystem.read_bytes(Path(report.preserved_path)) == TORN_FRAME


@pytest.mark.asyncio
async def test_a_corrupt_frame_mid_file_is_cut_with_everything_after_it_preserved() -> None:
    filesystem = SimFilesystem()
    await _append_all(filesystem, [b"first", b"second", b"third"])
    intact = await filesystem.read_bytes(WAL_PATH)
    (first_frame_length,) = struct.unpack_from(">I", intact, WAL_FORMAT.header_size + 4)
    first_frame_end = WAL_FORMAT.header_size + first_frame_length
    damaged = bytearray(intact)
    damaged[first_frame_end + 10] ^= 0xFF  # inside the second frame
    await filesystem.atomic_write(WAL_PATH, bytes(damaged))
    logger = RecordingLogger()

    assert await _recovered(filesystem, logger) == [b"first"]
    (report,) = [entry for entry in logger.entries if isinstance(entry, WALTailDiscarded)]
    assert await filesystem.read_bytes(Path(report.preserved_path)) == bytes(damaged[first_frame_end:])

    await _append_all(filesystem, [b"after"])
    assert await _recovered(filesystem) == [b"first", b"after"]


@pytest.mark.asyncio
async def test_without_damage_recovery_discards_nothing() -> None:
    filesystem = SimFilesystem()
    await _append_all(filesystem, [b"one", b"two"])
    logger = RecordingLogger()

    assert await _recovered(filesystem, logger) == [b"one", b"two"]
    assert logger.entries == []
    assert await filesystem.list_directory(WAL_PATH.parent, "*.discarded-*") == []


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", CRASH_CYCLE_SEEDS)
async def test_durable_entries_survive_every_crash_cycle(seed: int) -> None:
    random_source = random.Random(seed)
    filesystem = SimFilesystem()
    durable: list[bytes] = []

    for cycle in range(CRASH_CYCLES):
        appended = [
            f"seed-{seed}-cycle-{cycle}-entry-{entry}".encode()
            for entry in range(random_source.randint(0, MAXIMUM_APPENDS_PER_CYCLE))
        ]
        await _append_all(filesystem, appended)
        durable.extend(appended)
        filesystem.crash()
        debris_length = random_source.randint(0, len(TORN_FRAME) + 40)
        if debris_length:
            await filesystem.append_fsync(WAL_PATH, random_source.randbytes(debris_length))

        assert await _recovered(filesystem) == durable, (seed, cycle)
