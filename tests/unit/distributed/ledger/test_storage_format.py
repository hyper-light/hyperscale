"""
Persisted ledger files carry a format header (magic + version), and a
node never interprets bytes it did not write in its own format -- it
cannot assume what a disk holds.

Pinned, over the simulated filesystem:

* A WAL, checkpoint or archive record in a foreign format, another
  format version, or with any header byte damaged is SET ASIDE: its
  bytes preserved under ``<name>.unrecognized-<n>``, its path freed, the
  event reported -- and the store keeps working (new writes land and
  recover).
* A damaged archive record (valid header, undecodable contents) is set
  aside too, so the job's next archival lands instead of colliding with
  the damaged file forever.
* A WAL whose header was torn by a crash while the file was created
  (any strict prefix, including empty) is rewritten, not set aside.
* Without a logger to report through, an unreadable file is refused
  (raised), never silently skipped, and left untouched.
* Set-asides never overwrite one another.
* A read path that flips bytes (the disk intact) never causes an intact
  file to be removed: the verdict is re-read first, and reads that
  disagree raise ``UnstableStorageReadError`` with nothing moved.
"""

from pathlib import Path

import msgspec
import pytest

from hyperscale.distributed.hlc import HLCTimestamp
from hyperscale.distributed.ledger.archive.job_archive_store import (
    ARCHIVE_FORMAT,
    JobArchiveStore,
)
from hyperscale.distributed.ledger.checkpoint.checkpoint import (
    Checkpoint,
    CheckpointManager,
)
from hyperscale.distributed.ledger.job_state import JobState
from hyperscale.distributed.ledger.storage_format import (
    UnrecognizedStorageFormatError,
    UnstableStorageReadError,
)
from hyperscale.distributed.ledger.wal.node_wal import WAL_FORMAT, NodeWAL
from hyperscale.distributed.ledger.wal.wal_entry import JobEventType
from hyperscale.logging.hyperscale_logging_models import StorageFormatUnrecognized
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/wal")
CHECKPOINT_DIR = Path("/node/ledger/checkpoints")
ARCHIVE_DIR = Path("/node/ledger/archive")
ARCHIVED_JOB_ID = "east-1700000000123-abc"
FOREIGN_BYTES = b"#!/bin/sh\necho this is not a ledger file\n"
READ_FAULT_SEEDS = range(8)


class RecordingLogger:
    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)

    @property
    def set_asides(self) -> list[StorageFormatUnrecognized]:
        return [entry for entry in self.entries if isinstance(entry, StorageFormatUnrecognized)]


async def _plant(filesystem: SimFilesystem, path: Path, data: bytes) -> None:
    await filesystem.mkdir(path.parent, parents=True, exist_ok=True)
    await filesystem.atomic_write(path, data)


async def _wal_with_one_entry(filesystem: SimFilesystem) -> bytes:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), filesystem=filesystem)
    await wal.append(JobEventType.JOB_CREATED, b"job-1")
    await wal.close()
    return await filesystem.read_bytes(WAL_PATH)


async def _recovered_payloads(filesystem: SimFilesystem, logger: RecordingLogger | None) -> list[bytes]:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), logger=logger, filesystem=filesystem)
    payloads = [entry.payload async for entry in wal.iter_from(0)]
    await wal.close()
    return payloads


async def _assert_set_aside(
    filesystem: SimFilesystem, logger: RecordingLogger, path: Path, original: bytes
) -> StorageFormatUnrecognized:
    (report,) = logger.set_asides
    assert report.path == str(path)
    assert report.set_aside_path == str(path.with_name(f"{path.name}.unrecognized-0"))
    assert await filesystem.read_bytes(Path(report.set_aside_path)) == original
    return report


async def _assert_wal_usable(filesystem: SimFilesystem, logger: RecordingLogger) -> None:
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), logger=logger, filesystem=filesystem)
    await wal.append(JobEventType.JOB_CREATED, b"after")
    await wal.close()
    assert await _recovered_payloads(filesystem, logger) == [b"after"]


# --------------------------------------------------------------------- WAL


@pytest.mark.asyncio
async def test_wal_in_a_foreign_format_is_set_aside_and_the_wal_starts_empty() -> None:
    filesystem = SimFilesystem()
    await _plant(filesystem, WAL_PATH, FOREIGN_BYTES)
    logger = RecordingLogger()

    assert await _recovered_payloads(filesystem, logger) == []
    await _assert_set_aside(filesystem, logger, WAL_PATH, FOREIGN_BYTES)
    await _assert_wal_usable(filesystem, logger)


@pytest.mark.asyncio
async def test_wal_of_another_format_version_is_set_aside() -> None:
    filesystem = SimFilesystem()
    original = await _wal_with_one_entry(filesystem)
    other_version = original[:4] + (int.from_bytes(original[4:8], "big") + 1).to_bytes(4, "big") + original[8:]
    await filesystem.atomic_write(WAL_PATH, other_version)
    logger = RecordingLogger()

    assert await _recovered_payloads(filesystem, logger) == []
    report = await _assert_set_aside(filesystem, logger, WAL_PATH, other_version)
    assert "version" in report.reason


@pytest.mark.asyncio
@pytest.mark.parametrize("header_offset", range(WAL_FORMAT.header_size))
async def test_wal_with_any_header_byte_damaged_is_never_read(header_offset: int) -> None:
    filesystem = SimFilesystem()
    original = await _wal_with_one_entry(filesystem)
    damaged = bytearray(original)
    damaged[header_offset] ^= 0x01
    await filesystem.atomic_write(WAL_PATH, bytes(damaged))
    logger = RecordingLogger()

    assert await _recovered_payloads(filesystem, logger) == []
    await _assert_set_aside(filesystem, logger, WAL_PATH, bytes(damaged))


@pytest.mark.asyncio
@pytest.mark.parametrize("prefix_length", range(WAL_FORMAT.header_size))
async def test_wal_header_torn_at_creation_is_rewritten_not_set_aside(prefix_length: int) -> None:
    filesystem = SimFilesystem()
    await _plant(filesystem, WAL_PATH, WAL_FORMAT.header[:prefix_length])
    logger = RecordingLogger()

    assert await _recovered_payloads(filesystem, logger) == []
    assert logger.entries == []
    assert await filesystem.read_bytes(WAL_PATH) == WAL_FORMAT.header
    await _assert_wal_usable(filesystem, logger)


@pytest.mark.asyncio
async def test_without_a_logger_an_unrecognized_wal_is_refused_and_left_untouched() -> None:
    filesystem = SimFilesystem()
    await _plant(filesystem, WAL_PATH, FOREIGN_BYTES)

    with pytest.raises(UnrecognizedStorageFormatError):
        await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), filesystem=filesystem)

    assert await filesystem.read_bytes(WAL_PATH) == FOREIGN_BYTES
    assert await filesystem.list_directory(WAL_PATH.parent, "*.unrecognized-*") == []


@pytest.mark.asyncio
async def test_set_asides_never_overwrite_one_another() -> None:
    filesystem = SimFilesystem()
    logger = RecordingLogger()
    for generation in range(3):
        await _plant(filesystem, WAL_PATH, FOREIGN_BYTES + bytes([generation]))
        assert await _recovered_payloads(filesystem, logger) == []

    assert [report.set_aside_path for report in logger.set_asides] == [
        str(WAL_PATH.with_name(f"{WAL_PATH.name}.unrecognized-{generation}")) for generation in range(3)
    ]
    for generation in range(3):
        aside = WAL_PATH.with_name(f"{WAL_PATH.name}.unrecognized-{generation}")
        assert await filesystem.read_bytes(aside) == FOREIGN_BYTES + bytes([generation])


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", READ_FAULT_SEEDS)
async def test_a_faulty_read_path_never_removes_an_intact_wal(seed: int) -> None:
    """Every read of the WAL flips one byte; the disk is intact. Whatever
    the flips hit -- the header at open, the header on a later re-read, a
    frame -- the node either refuses loudly or recovers a prefix of the
    entries, and the intact file is still there afterwards."""
    filesystem = SimFilesystem()
    original = await _wal_with_one_entry(filesystem)
    filesystem.set_read_corruption(seed=seed, probability=1.0, path_glob=WAL_PATH.name)
    logger = RecordingLogger()

    try:
        payloads = await _recovered_payloads(filesystem, logger)
    except (UnstableStorageReadError, UnrecognizedStorageFormatError):
        payloads = None
    filesystem.clear_read_corruption()

    assert payloads in (None, [], [b"job-1"])
    assert logger.set_asides == []
    assert (await filesystem.read_bytes(WAL_PATH)).startswith(original)


# -------------------------------------------------------------- checkpoint


def _checkpoint(created_at_ms: int) -> Checkpoint:
    return Checkpoint(
        local_lsn=1,
        regional_lsn=0,
        global_lsn=0,
        job_states={},
        created_at_ms=created_at_ms,
        hlc=HLCTimestamp(wall_ms=created_at_ms, logical=0, node_id=1),
    )


@pytest.mark.asyncio
async def test_unrecognized_newest_checkpoint_is_set_aside_for_an_older_valid_one() -> None:
    filesystem = SimFilesystem()
    manager = CheckpointManager(CHECKPOINT_DIR, filesystem=filesystem)
    await manager.initialize()
    await manager.save(_checkpoint(created_at_ms=1000))
    foreign_path = CHECKPOINT_DIR / "checkpoint_9999.bin"
    await _plant(filesystem, foreign_path, FOREIGN_BYTES)
    logger = RecordingLogger()

    recovered = CheckpointManager(CHECKPOINT_DIR, filesystem=filesystem, logger=logger)
    await recovered.initialize()

    assert recovered.latest.created_at_ms == 1000
    await _assert_set_aside(filesystem, logger, foreign_path, FOREIGN_BYTES)
    assert not await filesystem.exists(foreign_path)


# ----------------------------------------------------------------- archive


def _terminal_job_state() -> JobState:
    hlc = HLCTimestamp(wall_ms=1000, logical=0, node_id=1)
    return JobState.create(
        job_id=ARCHIVED_JOB_ID,
        fence_token=1,
        assigned_datacenters=("dc-east",),
        created_hlc=hlc,
    ).with_completion("completed", total_completed=2, total_failed=0, hlc=hlc)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "unreadable_record",
    [
        FOREIGN_BYTES,
        ARCHIVE_FORMAT.encode(b"\xc1 not msgpack"),
        ARCHIVE_FORMAT.encode(msgspec.msgpack.encode({"unexpected": "shape"})),
    ],
    ids=["foreign-format", "undecodable-contents", "wrong-shape"],
)
async def test_unreadable_archive_record_is_set_aside_so_the_next_archival_lands(
    unreadable_record: bytes,
) -> None:
    filesystem = SimFilesystem()
    logger = RecordingLogger()
    store = JobArchiveStore(ARCHIVE_DIR, filesystem=filesystem, logger=logger)
    await store.initialize()
    await store.write_if_absent(_terminal_job_state())
    (record_path,) = [
        path
        for region in await filesystem.list_subdirectories(ARCHIVE_DIR)
        for shard in await filesystem.list_subdirectories(region)
        for path in await filesystem.list_directory(shard, "*.bin")
    ]
    await filesystem.atomic_write(record_path, unreadable_record)

    assert await store.read(ARCHIVED_JOB_ID) is None
    await _assert_set_aside(filesystem, logger, record_path, unreadable_record)

    await store.write_if_absent(_terminal_job_state())
    assert await store.read(ARCHIVED_JOB_ID) == _terminal_job_state()
