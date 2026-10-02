"""
Archive-write isolation — the durable-terminal-implies-delivery
contract, proven at the JobLedger level under SIM.

The recovery-fault campaign measured the defect live: a completing
job's 86-byte JobCompleted WAL append FIT the exhausted disk but the
236-byte archive copy raised ENOSPC — and that exception aborted the
completed-cache put, the snapshot publish, ``mark_applied``, and
propagated out of the manager's completion handler, so the tier-1
client push and the gate notification never ran: durably-COMPLETED
work was reported to the client as a timeout. Recovery's terminal
sweep carried the same unprotected write, so a full disk at boot could
wedge the owning server's ``start()``.

These tests pin the isolation contract: once the WAL terminal commits,
``complete_job`` succeeds and reads flip to the terminal REGARDLESS of
archive health; the owed record is parked (``pending_archive_count``),
healed by the next successful archive write or a targeted
``get_archived_job``; and recovery re-attempts unarchived terminals
without ever wedging ``open()``.
"""

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.archive.job_archive_store import (
    JobArchiveStore,
)
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.ledger.job_state import JobState
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock


class FlakyArchiveStore(JobArchiveStore):
    """A real ``JobArchiveStore`` whose ``write_if_absent`` raises
    ENOSPC while ``failures_remaining > 0`` — the archive-leg fault the
    live campaign measured, injected without disturbing WAL/checkpoint
    byte budgets."""

    __slots__ = ("failures_remaining", "write_attempts")

    def __init__(self, archive_dir: Path, filesystem: SimFilesystem) -> None:
        super().__init__(archive_dir=archive_dir, filesystem=filesystem)
        self.failures_remaining = 0
        self.write_attempts = 0

    async def write_if_absent(self, job_state: JobState) -> None:
        self.write_attempts += 1
        if self.failures_remaining > 0:
            self.failures_remaining -= 1
            raise OSError(28, "No space left on device")
        await super().write_if_absent(job_state)


async def _open_ledger_with_archive(
    filesystem: SimFilesystem,
    archive_failures_at_open: int = 0,
) -> tuple[JobLedger, FlakyArchiveStore]:
    """Mirror ``JobLedger.open``'s wiring with the flaky archive store
    injected through the public constructor.

    ``archive_failures_at_open`` arms the store BEFORE ``_recover()``
    runs, so recovery's terminal sweep itself exercises the isolated
    path — the wedged-``start()`` regression case."""
    from hyperscale.distributed.ledger.checkpoint.checkpoint import (
        CheckpointManager,
    )
    from hyperscale.distributed.ledger.job_id import JobIdGenerator
    from hyperscale.distributed.ledger.pipeline.commit_pipeline import (
        CommitPipeline,
    )
    from hyperscale.distributed.ledger.wal.node_wal import NodeWAL

    from hyperscale.distributed.ledger.storage_health import StorageHealth

    clock = new_hybrid_logical_clock()
    storage_health = StorageHealth()
    wal = await NodeWAL.open(
        path=Path("/manager/ledger/wal"),
        clock=clock,
        filesystem=filesystem,
        storage_health=storage_health,
    )
    pipeline = CommitPipeline(wal=wal)
    checkpoint_manager = CheckpointManager(
        checkpoint_dir=Path("/manager/ledger/checkpoints"),
        filesystem=filesystem,
    )
    await checkpoint_manager.initialize()
    archive_store = FlakyArchiveStore(
        archive_dir=Path("/manager/ledger/archive"), filesystem=filesystem
    )
    await archive_store.initialize()
    archive_store.failures_remaining = archive_failures_at_open

    ledger = JobLedger(
        clock=clock,
        wal=wal,
        pipeline=pipeline,
        checkpoint_manager=checkpoint_manager,
        job_id_generator=JobIdGenerator(region_code="dc-east", gate_id="mgr-1"),
        archive_store=archive_store,
        storage_health=storage_health,
    )
    await ledger._recover()
    return ledger, archive_store


async def _create_and_accept(ledger: JobLedger, job_id: str) -> None:
    created_id, create_result = await ledger.create_job(
        spec_hash=b"spec-hash",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id=job_id,
    )
    assert create_result.success and created_id == job_id
    accept_result = await ledger.accept_job(
        job_id,
        datacenter_id="dc-east",
        worker_count=2,
        durability=DurabilityLevel.LOCAL,
    )
    assert accept_result is not None and accept_result.success


async def _complete(ledger: JobLedger, job_id: str) -> None:
    complete_result = await ledger.complete_job(
        job_id,
        final_status="completed",
        total_completed=10,
        total_failed=0,
        duration_ms=1500,
        durability=DurabilityLevel.LOCAL,
    )
    assert complete_result is not None and complete_result.success


@pytest.mark.asyncio
async def test_archive_failure_never_aborts_completion() -> None:
    """The core contract: with the archive leg raising ENOSPC,
    ``complete_job`` still succeeds, reads flip to the terminal
    immediately, and the owed record is parked — not raised."""
    filesystem = SimFilesystem()
    ledger, archive_store = await _open_ledger_with_archive(filesystem)
    await _create_and_accept(ledger, "job-archive-0001")

    archive_store.failures_remaining = 10**6  # sustained exhaustion
    await _complete(ledger, "job-archive-0001")

    job_state = ledger.get_job("job-archive-0001")
    assert job_state is not None and job_state.is_terminal
    assert job_state.status == "completed"
    assert "job-archive-0001" not in ledger.get_all_jobs()
    assert ledger.pending_archive_count == 1


@pytest.mark.asyncio
async def test_next_successful_archive_write_heals_the_backlog() -> None:
    """A later completion whose archive write lands is evidence the
    disk recovered — the parked backlog heals in the same call."""
    filesystem = SimFilesystem()
    ledger, archive_store = await _open_ledger_with_archive(filesystem)
    await _create_and_accept(ledger, "job-archive-0002")
    await _create_and_accept(ledger, "job-archive-0003")

    archive_store.failures_remaining = 1  # exactly the first write fails
    await _complete(ledger, "job-archive-0002")
    assert ledger.pending_archive_count == 1

    await _complete(ledger, "job-archive-0003")
    assert ledger.pending_archive_count == 0

    healed = await ledger.get_archived_job("job-archive-0002")
    assert healed is not None and healed.status == "completed"


@pytest.mark.asyncio
async def test_targeted_read_heals_the_owed_record() -> None:
    filesystem = SimFilesystem()
    ledger, archive_store = await _open_ledger_with_archive(filesystem)
    await _create_and_accept(ledger, "job-archive-0004")

    archive_store.failures_remaining = 1
    await _complete(ledger, "job-archive-0004")
    assert ledger.pending_archive_count == 1

    read_back = await ledger.get_archived_job("job-archive-0004")
    assert read_back is not None and read_back.is_terminal
    assert ledger.pending_archive_count == 0


@pytest.mark.asyncio
async def test_recovery_reattempts_unarchived_terminal_and_never_wedges() -> None:
    """The park is process-lifetime only, and that is SOUND: a reboot's
    recovery sweep finds the unarchived terminal in the WAL and
    re-attempts its record — and when the disk is STILL bad at boot,
    ``open()`` completes anyway (pre-fix it wedged the owning server's
    ``start()``), serving the terminal from the WAL-rebuilt cache."""
    filesystem = SimFilesystem()
    ledger, archive_store = await _open_ledger_with_archive(filesystem)
    await _create_and_accept(ledger, "job-archive-0005")
    archive_store.failures_remaining = 10**6
    await _complete(ledger, "job-archive-0005")
    assert ledger.pending_archive_count == 1
    await ledger.close()

    # Generation 2: the archive disk is STILL failing when recovery's
    # terminal sweep runs inside open — it must park, not wedge, and
    # the terminal must serve from the WAL-rebuilt cache.
    recovered, recovered_store = await _open_ledger_with_archive(
        filesystem, archive_failures_at_open=10**6
    )
    assert recovered_store.write_attempts >= 1
    assert recovered.pending_archive_count == 1
    job_state = recovered.get_job("job-archive-0005")
    assert job_state is not None and job_state.is_terminal
    await recovered.close()

    # Generation 3: disk healed — recovery's sweep writes the record.
    third_generation, _healthy_store = await _open_ledger_with_archive(
        filesystem
    )
    archived = await third_generation.get_archived_job("job-archive-0005")
    assert archived is not None and archived.status == "completed"
    await third_generation.close()
