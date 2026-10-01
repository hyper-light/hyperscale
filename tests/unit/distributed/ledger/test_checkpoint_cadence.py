"""
The AD-38 compaction cadence — checkpoints actually get taken, and
taking them bounds both the WAL and the checkpoint directory.

``JobLedger.checkpoint()`` was written, tested, and never called: zero
production call sites. Compaction only removes WAL entries at or below
a checkpoint's LSN, so with no checkpoint ever taken the entry map grew
for the whole process lifetime and every recovery replayed from LSN 0
— violating architecture.md's own control-plane criteria, "Compaction:
WAL size bounded to 2x active job state" and "Recovery: <30 seconds
from crash to serving requests".

``CheckpointManager.cleanup()`` had zero call sites for the same
reason, which makes the cadence and the retention one fix rather than
two: checkpointing on a cadence without pruning would only trade an
unbounded WAL for an unbounded checkpoint directory.

These tests pin the policy from both directions — the triggers that
must fire (entry ratio, entry floor, age bound), the trigger that must
NOT fire (an idle ledger writing checkpoints it would re-derive on
recovery), the bound the whole thing exists to hold, and the disk
failure that must not take down the loop that drives it.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.checkpoint.checkpoint import (
    Checkpoint,
    CheckpointManager,
)
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.wal.node_wal import NodeWAL
from hyperscale.distributed.runtime import RealClock
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/wal")
CHECKPOINT_DIR = Path("/node/ledger/checkpoints")
ARCHIVE_DIR = Path("/node/ledger/archive")


class _ManualClock(RealClock):
    """A ``RealClock`` whose wall reading is under test control.

    ``time`` is a slot on ``RealClock`` holding a callable, so
    rebinding it steers the wall axis — the one the checkpoint age
    bound reads — without disturbing monotonic, sleep, or wait_for.
    """

    def __init__(self, start_seconds: float = 1_000_000.0) -> None:
        super().__init__()
        self._now = start_seconds
        self.time = lambda: self._now

    def advance(self, seconds: float) -> None:
        self._now += seconds


class _FailingCheckpointManager(CheckpointManager):
    """A real manager whose ``save`` raises the disk failure a
    checkpoint can plausibly hit (ENOSPC on a full volume)."""

    async def save(self, checkpoint: Checkpoint) -> Path:
        raise OSError(28, "No space left on device")


async def _open_ledger(
    filesystem: SimFilesystem,
    **checkpoint_policy: float | int,
) -> JobLedger:
    return await JobLedger.open(
        wal_path=WAL_PATH,
        checkpoint_dir=CHECKPOINT_DIR,
        archive_dir=ARCHIVE_DIR,
        region_code="dc-east",
        gate_id="gate-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
        **checkpoint_policy,
    )


async def _create_job(ledger: JobLedger, job_id: str) -> None:
    """One WAL append, one newly-active job."""
    created_id, create_result = await ledger.create_job(
        spec_hash=b"spec-hash",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id=job_id,
    )
    assert create_result.success and created_id == job_id


async def _accept_job(ledger: JobLedger, job_id: str) -> None:
    """One WAL append against an already-active job."""
    accept_result = await ledger.accept_job(
        job_id,
        datacenter_id="dc-east",
        worker_count=2,
        durability=DurabilityLevel.LOCAL,
    )
    assert accept_result is not None and accept_result.success


async def _complete_job(ledger: JobLedger, job_id: str) -> None:
    """One WAL append that also makes the job terminal, so it leaves
    the active set the ratio is measured against."""
    complete_result = await ledger.complete_job(
        job_id,
        final_status="completed",
        total_completed=10,
        total_failed=0,
        duration_ms=1500,
        durability=DurabilityLevel.LOCAL,
    )
    assert complete_result is not None and complete_result.success


async def _checkpoint_files(filesystem: SimFilesystem) -> list[str]:
    return sorted(
        path.name
        for path in await filesystem.list_directory(
            CHECKPOINT_DIR, "checkpoint_*.bin"
        )
    )


@pytest.mark.asyncio
async def test_idle_ledger_never_checkpoints() -> None:
    """An idle node must not write checkpoints on its loop's tick.

    With nothing pending there is nothing to compact, and a checkpoint
    would only persist state recovery already re-derives — a periodic
    disk write bought for no bound at all.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem)

    for _ in range(5):
        assert await ledger.maybe_checkpoint() is None

    assert await _checkpoint_files(filesystem) == []
    await ledger.close()


@pytest.mark.asyncio
async def test_wal_past_the_floor_checkpoints_and_compacts() -> None:
    """The floor is what covers a node whose jobs have all finished:
    active state is zero, so the 2x ratio is zero, and without a floor
    the WAL would never compact again no matter how many entries the
    completed work left behind.

    One job through its whole life is three appends against zero
    surviving active state.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=3)

    await _create_job(ledger, "job-0")
    await _accept_job(ledger, "job-0")
    await _complete_job(ledger, "job-0")

    assert ledger.pending_wal_entries == 3
    assert ledger.active_job_count == 0, "terminal job stayed in the active set"

    checkpoint_path = await ledger.maybe_checkpoint()

    assert checkpoint_path is not None, (
        "no active jobs means a 2x bound of zero — only the floor can "
        "trigger here, and without it the WAL never compacts again"
    )
    assert ledger.pending_wal_entries == 0, "checkpoint did not compact the WAL"
    assert await _checkpoint_files(filesystem) == [checkpoint_path.name]
    await ledger.close()


@pytest.mark.asyncio
async def test_threshold_tracks_two_times_active_job_state() -> None:
    """architecture.md's criterion, stated as a test: the trigger is
    ``2x active job state``, so three active jobs hold six pending
    entries and checkpoint on the sixth.

    The floor is set to 1 here so the RATIO is what's under test — at
    the default floor of 64 this whole scenario sits below the trigger.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(
        filesystem, min_checkpoint_wal_entries=1, checkpoint_wal_ratio=2
    )

    for index in range(3):
        await _create_job(ledger, f"job-{index}")

    assert ledger.active_job_count == 3
    assert ledger.pending_wal_entries == 3
    assert await ledger.maybe_checkpoint() is None, (
        "checkpointed at 3 pending entries against 3 active jobs — below "
        "the 2x bound, so this is a checkpoint the criterion did not ask for"
    )

    for index in range(2):
        await _accept_job(ledger, f"job-{index}")

    assert ledger.pending_wal_entries == 5
    assert await ledger.maybe_checkpoint() is None

    await _accept_job(ledger, "job-2")

    assert ledger.pending_wal_entries == 6
    assert await ledger.maybe_checkpoint() is not None, (
        "6 pending entries against 3 active jobs is exactly the 2x bound "
        "and must checkpoint"
    )
    assert ledger.pending_wal_entries == 0
    await ledger.close()


@pytest.mark.asyncio
async def test_age_bound_checkpoints_a_low_rate_wal(monkeypatch) -> None:
    """A node whose WAL never reaches the entry threshold still has to
    checkpoint, or its recovery cost climbs with uptime forever.

    One append, a threshold it cannot reach, and only the clock moves.
    """
    manual_clock = _ManualClock()
    monkeypatch.setattr(
        "hyperscale.distributed.ledger.job_ledger._DEFAULT_CLOCK", manual_clock
    )

    filesystem = SimFilesystem()
    ledger = await _open_ledger(
        filesystem,
        min_checkpoint_wal_entries=1_000,
        checkpoint_max_interval_seconds=60.0,
    )

    await _create_job(ledger, "the-only-job")

    assert await ledger.maybe_checkpoint() is None, "age-triggered early"

    manual_clock.advance(59.0)
    assert await ledger.maybe_checkpoint() is None, "age-triggered early"

    manual_clock.advance(2.0)
    assert await ledger.maybe_checkpoint() is not None, (
        "a WAL older than the interval never checkpointed — recovery cost "
        "grows with uptime on any low-rate node"
    )

    # The interval restarts from the checkpoint, not from boot.
    await _create_job(ledger, "a-second-job")
    manual_clock.advance(59.0)
    assert await ledger.maybe_checkpoint() is None
    await ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_bounds_recovery_replay(monkeypatch) -> None:
    """The point of the cadence: a restarted node resumes from the
    checkpoint instead of replaying its whole history.

    ``iter_from``'s start LSN is the direct evidence — it is 0 for a
    ledger that never checkpointed and ``checkpoint_lsn + 1`` for one
    that did.
    """
    replayed_from: list[int] = []
    original_iter_from = NodeWAL.iter_from

    def recording_iter_from(self: NodeWAL, start_lsn: int):
        replayed_from.append(start_lsn)
        return original_iter_from(self, start_lsn)

    monkeypatch.setattr(NodeWAL, "iter_from", recording_iter_from)

    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=4)

    # Four active jobs put the 2x bound at eight entries, which the
    # create plus accept of each reaches exactly.
    for index in range(4):
        await _create_job(ledger, f"job-{index}")
    for index in range(4):
        await _accept_job(ledger, f"job-{index}")

    assert ledger.pending_wal_entries == 8
    assert await ledger.maybe_checkpoint() is not None
    checkpoint_lsn = ledger._checkpoint_manager.latest.local_lsn
    await ledger.close()

    replayed_from.clear()
    recovered_ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=4)

    assert replayed_from == [checkpoint_lsn + 1], (
        f"recovery replayed from {replayed_from} instead of resuming at "
        f"the checkpoint LSN {checkpoint_lsn}"
    )
    assert replayed_from[0] > 0, "replayed from the beginning of the WAL"
    assert recovered_ledger.active_job_count == 4, (
        "checkpoint recovery lost active job state"
    )
    for index in range(4):
        assert recovered_ledger.get_job(f"job-{index}") is not None

    await recovered_ledger.close()


@pytest.mark.asyncio
async def test_recovery_applies_entries_written_after_the_checkpoint() -> None:
    """The realistic restart: a checkpoint, more work, then a crash.

    Resuming at ``checkpoint_lsn + 1`` is only safe if the boundary is
    exact — one off in either direction and the node either replays an
    entry it already has or, far worse, skips a durably committed job
    on the way back up.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=2)

    await _create_job(ledger, "before-checkpoint")
    await _accept_job(ledger, "before-checkpoint")
    assert await ledger.maybe_checkpoint() is not None

    await _create_job(ledger, "after-checkpoint")
    await ledger.close()

    recovered_ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=2)

    assert recovered_ledger.get_job("before-checkpoint") is not None, (
        "job snapshotted into the checkpoint was lost on recovery"
    )
    assert recovered_ledger.get_job("after-checkpoint") is not None, (
        "job committed after the checkpoint was skipped by replay — the "
        "resume boundary is off by one"
    )
    assert recovered_ledger.active_job_count == 2
    await recovered_ledger.close()


@pytest.mark.asyncio
async def test_retention_keeps_only_the_newest_checkpoints(monkeypatch) -> None:
    """Checkpoints on a cadence means checkpoint FILES on a cadence.

    Retention stays above one on purpose: recovery walks the files
    newest-first and skips undecodable ones, so the older copies are
    the fallback for a checkpoint torn by a crash mid-write.
    """
    manual_clock = _ManualClock()
    monkeypatch.setattr(
        "hyperscale.distributed.ledger.job_ledger._DEFAULT_CLOCK", manual_clock
    )

    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, checkpoint_retention_count=3)

    written: list[str] = []
    for index in range(6):
        await _create_job(ledger, f"job-{index}")
        # Checkpoint filenames are keyed by millisecond, so distinct
        # files require distinct instants.
        manual_clock.advance(1.0)
        written.append((await ledger.checkpoint()).name)

    assert len(set(written)) == 6, "checkpoints collided on one filename"
    assert await _checkpoint_files(filesystem) == sorted(written[-3:]), (
        "checkpoint directory grew past its retention count"
    )
    await ledger.close()


@pytest.mark.asyncio
async def test_checkpoint_disk_failure_never_breaks_the_cadence() -> None:
    """A checkpoint is pure optimization — every entry it would have
    compacted is still durable in the WAL — so a failing disk must not
    propagate into the periodic loop that drives it.

    The failure is contained and reported, the pending entries stay,
    and the next tick retries.
    """
    filesystem = SimFilesystem()
    ledger = await _open_ledger(filesystem, min_checkpoint_wal_entries=2)
    ledger._checkpoint_manager = _FailingCheckpointManager(
        checkpoint_dir=CHECKPOINT_DIR, filesystem=filesystem
    )

    for index in range(3):
        await _create_job(ledger, f"job-{index}")

    assert await ledger.maybe_checkpoint() is None
    assert ledger.pending_wal_entries == 3, (
        "a failed checkpoint compacted the WAL anyway"
    )
    assert ledger.active_job_count == 3

    # Still due on the next tick, and still contained.
    assert await ledger.maybe_checkpoint() is None
    assert ledger.pending_wal_entries == 3
    await ledger.close()
