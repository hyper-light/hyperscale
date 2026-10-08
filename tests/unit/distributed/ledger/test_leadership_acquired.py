"""
AD-38 audit trail: a job's leadership changes are journaled.

A ledger recorded the job giving up its record (``JobRelinquished``) but
never the other side -- a node taking a job over left no trace in the
job's record of who led it from then on. ``JobLeadershipAcquired`` is
written by the taking node right after it adopts the job's replicated
history: who leads, from whom, under which lease fence. It survives a
restart through WAL replay and through a checkpoint, and a job that has
ended takes no further leadership.
"""

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.events.event_type import JobEventType
from hyperscale.distributed.ledger.job_ledger import JobLedger
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

WAL_PATH = Path("/node/ledger/wal")
CHECKPOINT_DIR = Path("/node/ledger/checkpoints")
ARCHIVE_DIR = Path("/node/ledger/archive")
JOB_ID = "job-taken-over"


async def open_ledger(filesystem: SimFilesystem) -> JobLedger:
    return await JobLedger.open(
        wal_path=WAL_PATH,
        checkpoint_dir=CHECKPOINT_DIR,
        archive_dir=ARCHIVE_DIR,
        region_code="dc-east",
        gate_id="gate-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
    )


async def create(ledger: JobLedger) -> None:
    _created_id, result = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id=JOB_ID,
    )
    assert result.success


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint_before_restart", [False, True])
async def test_a_takeover_is_journaled_and_survives_a_restart(checkpoint_before_restart: bool) -> None:
    filesystem = SimFilesystem()
    ledger = await open_ledger(filesystem)
    await create(ledger)

    result = await ledger.record_leadership_acquired(JOB_ID, "manager-b", "manager-a", lease_fence_token=7)

    assert result is not None and result.success
    assert ledger.get_job(JOB_ID).leader_id == "manager-b"
    if checkpoint_before_restart:
        await ledger.checkpoint()
    await ledger.close()

    recovered = await open_ledger(filesystem)
    assert recovered.get_job(JOB_ID).leader_id == "manager-b"
    if not checkpoint_before_restart:
        entries = [entry async for entry in recovered._wal.iter_from(0)]
        assert [entry.event_type for entry in entries] == [
            JobEventType.JOB_CREATED,
            JobEventType.JOB_LEADERSHIP_ACQUIRED,
        ]
    await recovered.close()


@pytest.mark.asyncio
async def test_an_ended_job_takes_no_leadership() -> None:
    ledger = await open_ledger(SimFilesystem())
    await create(ledger)
    await ledger.complete_job(
        JOB_ID,
        final_status="completed",
        total_completed=1,
        total_failed=0,
        duration_ms=1,
        durability=DurabilityLevel.LOCAL,
    )

    assert await ledger.record_leadership_acquired(JOB_ID, "manager-b", "manager-a", lease_fence_token=7) is None
    assert await ledger.record_leadership_acquired("job-unknown", "manager-b", None, lease_fence_token=1) is None
    await ledger.close()
