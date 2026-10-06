"""
WAL disk reclamation (AD-38): checkpoints bound the log on disk, and
every crash recovers exactly what was acknowledged.

A checkpoint used to compact only memory: the file kept every frame ever
written, and boot read all of it. Now each checkpoint cuts the log to what
the oldest retained checkpoint still needs -- that one is the fallback for
a damaged newest, so its replay must stay possible.

Driven VOPR-style: a seeded workload of job creations, acceptances and
completions with checkpoints between, on a simulated disk that raises
seeded I/O errors and loses its un-synced tail on each crash. After every
crash the reopened ledger holds every acknowledged job in its
acknowledged state; after every checkpoint the log holds no frame a
retained checkpoint already covers; a terminal job whose archive write
failed survives a checkpoint and a restart and is archived after.
"""

import asyncio
import random
import struct
from pathlib import Path

import pytest

from hyperscale.distributed.ledger import job_ledger as job_ledger_module
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.ledger.wal.node_wal import WAL_FORMAT
from hyperscale.distributed.ledger.wal.wal_entry import HEADER_SIZE
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/wal")
CHECKPOINT_DIR = Path("/node/ledger/checkpoints")
ARCHIVE_DIR = Path("/node/ledger/archive")
RETENTION = 3
SEEDS = range(12)
OPERATIONS_PER_SEED = 400
IO_ERROR_PROBABILITY = 0.02


class SteppingWallClock(RealClock):
    """Real monotonic time and sleeps; a wall reading that advances one
    millisecond per read -- checkpoints are named by it, so every run names
    them alike (and never two alike)."""

    def __init__(self) -> None:
        super().__init__()
        self._now = 1_000_000.0
        self.time = self._advance

    def _advance(self) -> float:
        self._now += 0.001
        return self._now


@pytest.fixture(autouse=True)
def reproducible_checkpoint_names(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(job_ledger_module, "_DEFAULT_CLOCK", SteppingWallClock())


class RecordingLogger:
    """Takes the ledger's reports, as a node's logger would."""

    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


async def open_ledger(
    filesystem: SimFilesystem, logger: RecordingLogger | None = None, retention: int = RETENTION
) -> JobLedger:
    return await JobLedger.open(
        wal_path=WAL_PATH,
        checkpoint_dir=CHECKPOINT_DIR,
        archive_dir=ARCHIVE_DIR,
        region_code="dc-east",
        gate_id="gate-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
        checkpoint_retention_count=retention,
        logger=logger,
    )


async def wal_frame_lsns(filesystem: SimFilesystem) -> list[int]:
    frames = WAL_FORMAT.decode(await filesystem.read_bytes(WAL_PATH))
    frame_lsns: list[int] = []
    offset = 0
    while offset + HEADER_SIZE <= len(frames):
        frame_length, frame_lsn = struct.unpack(">IQ", frames[offset + 4 : offset + 16])
        frame_lsns.append(frame_lsn)
        offset += frame_length
    return frame_lsns


async def held_state(ledger: JobLedger, job_id: str) -> str | None:
    if (job := ledger.get_job(job_id)) is not None:
        return job.status
    archived = await ledger.get_archived_job(job_id)
    return None if archived is None else archived.status


async def assert_recovers_what_was_acknowledged(
    filesystem: SimFilesystem, acknowledged: dict[str, str], logger: RecordingLogger | None = None
) -> JobLedger:
    """Reopen on a quiet disk and check every acknowledged job."""
    filesystem.clear_io_error()
    ledger = await open_ledger(filesystem, logger)
    for job_id, status in acknowledged.items():
        held = await held_state(ledger, job_id)
        if status == "completed":
            assert held == "completed", (job_id, held)
        else:
            assert held is not None and held != "completed", (job_id, held)
    return ledger


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_crashes_recover_every_acknowledged_job_and_checkpoints_bound_the_log(seed: int) -> None:
    draw = random.Random(seed)
    filesystem = SimFilesystem()
    ledger = await open_ledger(filesystem)
    acknowledged: dict[str, str] = {}
    created_order: list[str] = []
    filesystem.set_io_error(seed=seed, probability=IO_ERROR_PROBABILITY)

    for operation_index in range(OPERATIONS_PER_SEED):
        choice = draw.random()
        try:
            if choice < 0.35 or not created_order:
                job_id = f"job-{seed}-{operation_index}"
                _created_id, result = await ledger.create_job(
                    spec_hash=b"spec",
                    assigned_datacenters=("dc-east",),
                    requestor_id="client-1",
                    durability=DurabilityLevel.LOCAL,
                    job_id=job_id,
                )
                if result.success:
                    acknowledged[job_id] = "created"
                    created_order.append(job_id)
            elif choice < 0.55:
                job_id = draw.choice(created_order)
                result = await ledger.accept_job(
                    job_id, datacenter_id="dc-east", worker_count=1, durability=DurabilityLevel.LOCAL
                )
                if result is not None and result.success and acknowledged[job_id] != "completed":
                    acknowledged[job_id] = "accepted"
            elif choice < 0.8:
                job_id = draw.choice(created_order)
                result = await ledger.complete_job(
                    job_id,
                    final_status="completed",
                    total_completed=1,
                    total_failed=0,
                    duration_ms=1,
                    durability=DurabilityLevel.LOCAL,
                )
                if result is not None and result.success:
                    acknowledged[job_id] = "completed"
            elif choice < 0.95:
                await ledger.checkpoint()
                # Nothing a retained checkpoint covers is left in the log.
                covered_through = await ledger._checkpoint_manager.lowest_retained_local_lsn()
                assert all(frame_lsn > covered_through for frame_lsn in await wal_frame_lsns(filesystem))
            else:
                filesystem.crash()
                ledger = await assert_recovers_what_was_acknowledged(filesystem, acknowledged)
                filesystem.set_io_error(seed=seed + operation_index, probability=IO_ERROR_PROBABILITY)
        except OSError:
            # The simulated device refused: the operation was not
            # acknowledged. A latched writer surfaces as RuntimeError on
            # the next append -- the node restarts.
            continue
        except RuntimeError:
            filesystem.crash()
            ledger = await assert_recovers_what_was_acknowledged(filesystem, acknowledged)
            filesystem.set_io_error(seed=seed + operation_index, probability=IO_ERROR_PROBABILITY)

    filesystem.crash()
    ledger = await assert_recovers_what_was_acknowledged(filesystem, acknowledged)

    # Quiesced: finish every job.
    for job_id in created_order:
        if acknowledged[job_id] != "completed":
            await ledger.complete_job(
                job_id,
                final_status="completed",
                total_completed=1,
                total_failed=0,
                duration_ms=1,
                durability=DurabilityLevel.LOCAL,
            )
            acknowledged[job_id] = "completed"
    # Then one short job per checkpoint, past retention: the log holds
    # only what follows the oldest retained checkpoint -- the frames of
    # the jobs after it, two apiece -- however long the run before was.
    for round_index in range(RETENTION):
        job_id = f"job-{seed}-tail-{round_index}"
        _created_id, result = await ledger.create_job(
            spec_hash=b"spec",
            assigned_datacenters=("dc-east",),
            requestor_id="client-1",
            durability=DurabilityLevel.LOCAL,
            job_id=job_id,
        )
        assert result.success
        await ledger.complete_job(
            job_id,
            final_status="completed",
            total_completed=1,
            total_failed=0,
            duration_ms=1,
            durability=DurabilityLevel.LOCAL,
        )
        acknowledged[job_id] = "completed"
        await ledger.checkpoint()
    assert len(await wal_frame_lsns(filesystem)) <= 2 * (RETENTION - 1)

    await ledger.close()
    ledger = await assert_recovers_what_was_acknowledged(filesystem, acknowledged)
    await ledger.close()


@pytest.mark.asyncio
async def test_a_damaged_newest_checkpoint_falls_back_through_the_kept_log() -> None:
    filesystem = SimFilesystem()
    ledger = await open_ledger(filesystem)
    acknowledged: dict[str, str] = {}
    for checkpoint_index in range(RETENTION):
        for job_index in range(4):
            job_id = f"job-{checkpoint_index}-{job_index}"
            _created_id, result = await ledger.create_job(
                spec_hash=b"spec",
                assigned_datacenters=("dc-east",),
                requestor_id="client-1",
                durability=DurabilityLevel.LOCAL,
                job_id=job_id,
            )
            assert result.success
            acknowledged[job_id] = "created"
        await ledger.checkpoint()
    await ledger.close()

    # Newest by the LSN its name carries -- never by the name as text,
    # which puts LSN 11 before 3 when checkpoints share a millisecond.
    newest = max(
        await filesystem.list_directory(CHECKPOINT_DIR, "checkpoint_*.bin"),
        key=lambda checkpoint_file: int(Path(checkpoint_file).stem.split("_")[2]),
    )
    damaged = bytearray(await filesystem.read_bytes(newest))
    damaged[-1] ^= 0xFF
    await filesystem.atomic_write(newest, bytes(damaged))

    # The damaged newest is set aside -- reported -- and its predecessor
    # replays from its own LSN through the frames the cut kept for it.
    logger = RecordingLogger()
    ledger = await assert_recovers_what_was_acknowledged(filesystem, acknowledged, logger)
    assert logger.entries
    await ledger.close()


class FailingArchiveFilesystem(SimFilesystem):
    """A disk whose archive directory refuses writes while ``failing``."""

    def __init__(self) -> None:
        super().__init__()
        self.failing = True

    async def atomic_write(self, path, data: bytes) -> None:
        if self.failing and str(path).startswith(str(ARCHIVE_DIR)):
            raise OSError(5, "Input/output error")
        await super().atomic_write(path, data)


@pytest.mark.asyncio
async def test_an_owed_archive_survives_the_checkpoint_that_cuts_its_frames() -> None:
    filesystem = FailingArchiveFilesystem()
    ledger = await open_ledger(filesystem)
    _created_id, result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="owed",
    )
    assert result.success
    completed = await ledger.complete_job(
        "owed",
        final_status="completed",
        total_completed=1,
        total_failed=0,
        duration_ms=1,
        durability=DurabilityLevel.LOCAL,
    )
    assert completed is not None and completed.success
    assert ledger.pending_archive_count == 1

    # Checkpoints past retention, each over one more frame, so the oldest
    # retained one lies past the owed job's frames.
    for round_index in range(RETENTION):
        _created_id, result = await ledger.create_job(
            spec_hash=b"spec",
            assigned_datacenters=("dc-east",),
            requestor_id="client-1",
            durability=DurabilityLevel.LOCAL,
            job_id=f"later-{round_index}",
        )
        assert result.success
        await ledger.checkpoint()
    # The owed job's frames (LSNs 0 and 1) are gone from the log ...
    assert min(await wal_frame_lsns(filesystem)) > 1
    await ledger.close()

    # ... yet the restarted ledger still owes, and now lands, its archive.
    filesystem.failing = False
    ledger = await open_ledger(filesystem)
    archived = await ledger.get_archived_job("owed")
    assert archived is not None and archived.status == "completed"
    assert ledger.pending_archive_count == 0
    await ledger.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", SEEDS)
async def test_appends_racing_a_cut_are_all_kept(seed: int) -> None:
    """A cut reads the log, drops its prefix and renames the result into
    place; an append landing between the read and the rename would be
    lost with the old file. Every append acknowledged around concurrent
    cuts must be in the log after them."""
    from hyperscale.distributed.ledger.events.event_type import JobEventType
    from hyperscale.distributed.ledger.wal.node_wal import NodeWAL

    draw = random.Random(seed)
    filesystem = SimFilesystem()
    wal = await NodeWAL.open(WAL_PATH, new_hybrid_logical_clock(), filesystem=filesystem)
    appended = [await wal.append(JobEventType.JOB_CREATED, b"before") for _ in range(8)]

    async def append_one(index: int) -> int:
        result = await wal.append(JobEventType.JOB_CREATED, f"racing-{index}".encode())
        return result.entry.lsn

    cut_through = appended[draw.randrange(len(appended))].entry.lsn
    operations = [append_one(index) for index in range(32)]
    cut_position = draw.randrange(len(operations) + 1)
    operations.insert(cut_position, wal.discard_through(cut_through))
    outcomes = await asyncio.gather(*operations)
    racing_lsns = {outcome for index, outcome in enumerate(outcomes) if index != cut_position}
    await wal.close()

    kept = set(await wal_frame_lsns(filesystem))
    assert racing_lsns <= kept
    assert all(frame_lsn > cut_through for frame_lsn in kept)


@pytest.mark.asyncio
async def test_writes_after_a_restart_on_an_emptied_log_survive_the_next_restart() -> None:
    """A checkpoint can leave the log no frames at all. Recovered so, the
    log numbered its next entries from 0 again -- at or below the
    checkpoint's LSN -- and the next recovery, replaying only past it,
    skipped them: a job acknowledged after the first restart was gone
    after the second."""
    # One checkpoint retained: the one taken last holds through the last
    # frame, so the cut empties the log.
    filesystem = SimFilesystem()
    ledger = await open_ledger(filesystem, retention=1)
    for round_index in range(RETENTION + 1):
        _created_id, result = await ledger.create_job(
            spec_hash=b"spec",
            assigned_datacenters=("dc-east",),
            requestor_id="client-1",
            durability=DurabilityLevel.LOCAL,
            job_id=f"before-{round_index}",
        )
        assert result.success
        await ledger.complete_job(
            f"before-{round_index}",
            final_status="completed",
            total_completed=1,
            total_failed=0,
            duration_ms=1,
            durability=DurabilityLevel.LOCAL,
        )
        await ledger.checkpoint()
    assert await wal_frame_lsns(filesystem) == []
    await ledger.close()

    ledger = await open_ledger(filesystem, retention=1)
    _created_id, result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="after-restart",
    )
    assert result.success
    await ledger.close()

    ledger = await open_ledger(filesystem, retention=1)
    assert ledger.get_job("after-restart") is not None
    await ledger.close()
