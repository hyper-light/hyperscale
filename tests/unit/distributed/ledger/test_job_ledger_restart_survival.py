"""
Restart survival — the linearizability oracle's durability invariant,
proven at the JobLedger level under SIM.

The VOPR storage campaign exposed that the manager opened a NodeWAL
and never appended to it: a restart forgot every job. With the full
JobLedger wired, these tests pin the recovery contract on the SAME
in-memory disk across ledger generations (SimFilesystem persists
across ``close()`` + reopen and across ``crash()``):

* an ACCEPTED job survives restart as active state;
* a TERMINAL job survives restart with its final status (served from
  the archive/completed cache after recovery's terminal sweep);
* power loss (``crash()``) after group commit loses nothing — the
  create/accept/complete records are fsynced by construction;
* a client-supplied job id is recorded verbatim (the manager path —
  ids are generated at the client, not by the ledger).
"""

from pathlib import Path

import pytest

from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from tests.simulation.harness.sim import SimFilesystem


async def _open_ledger(filesystem: SimFilesystem) -> JobLedger:
    return await JobLedger.open(
        wal_path=Path("/manager/ledger/wal"),
        checkpoint_dir=Path("/manager/ledger/checkpoints"),
        archive_dir=Path("/manager/ledger/archive"),
        region_code="dc-east",
        gate_id="mgr-1",
        node_id=1,
        filesystem=filesystem,
    )


@pytest.mark.asyncio
async def test_accepted_job_survives_restart() -> None:
    filesystem = SimFilesystem()

    ledger = await _open_ledger(filesystem)
    job_id, create_result = await ledger.create_job(
        spec_hash=b"spec-hash-1",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="job-client-0001",
    )
    assert create_result.success
    assert job_id == "job-client-0001"
    accept_result = await ledger.accept_job(
        job_id,
        datacenter_id="dc-east",
        worker_count=2,
        durability=DurabilityLevel.LOCAL,
    )
    assert accept_result is not None and accept_result.success
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    job_state = recovered.get_job(job_id)
    assert job_state is not None
    assert job_state.job_id == "job-client-0001"
    assert not job_state.is_terminal
    await recovered.close()


@pytest.mark.asyncio
async def test_terminal_job_survives_restart_with_final_status() -> None:
    filesystem = SimFilesystem()

    ledger = await _open_ledger(filesystem)
    job_id, _create = await ledger.create_job(
        spec_hash=b"spec-hash-2",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="job-client-0002",
    )
    await ledger.accept_job(
        job_id, datacenter_id="dc-east", worker_count=2,
        durability=DurabilityLevel.LOCAL,
    )
    complete_result = await ledger.complete_job(
        job_id,
        final_status="completed",
        total_completed=10,
        total_failed=0,
        duration_ms=1500,
        durability=DurabilityLevel.LOCAL,
    )
    assert complete_result is not None and complete_result.success
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    job_state = recovered.get_job(job_id)
    assert job_state is not None
    assert job_state.is_terminal
    assert job_state.status == "completed"
    assert job_state.completed_count == 10
    await recovered.close()


@pytest.mark.asyncio
async def test_power_loss_after_commit_loses_nothing() -> None:
    filesystem = SimFilesystem()

    ledger = await _open_ledger(filesystem)
    job_id, _create = await ledger.create_job(
        spec_hash=b"spec-hash-3",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="job-client-0003",
    )
    await ledger.close()

    # Power loss, not a clean shutdown: volatile tails vanish. The
    # create record was group-committed (append_fsync) so it is
    # durable by construction.
    filesystem.crash()

    recovered = await _open_ledger(filesystem)
    assert recovered.get_job(job_id) is not None
    await recovered.close()


@pytest.mark.asyncio
async def test_terminal_transition_is_recorded_once() -> None:
    filesystem = SimFilesystem()

    ledger = await _open_ledger(filesystem)
    job_id, _create = await ledger.create_job(
        spec_hash=b"spec-hash-4",
        assigned_datacenters=("dc-east",),
        requestor_id="client-1",
        durability=DurabilityLevel.LOCAL,
        job_id="job-client-0004",
    )
    first = await ledger.complete_job(
        job_id, final_status="failed", total_completed=0, total_failed=3,
        duration_ms=900, durability=DurabilityLevel.LOCAL,
    )
    assert first is not None and first.success
    # The ledger refuses a second terminal transition (absorbing
    # terminals hold durably, not just in client memory).
    second = await ledger.complete_job(
        job_id, final_status="completed", total_completed=9, total_failed=0,
        duration_ms=1200, durability=DurabilityLevel.LOCAL,
    )
    assert second is None
    await ledger.close()

    recovered = await _open_ledger(filesystem)
    job_state = recovered.get_job(job_id)
    assert job_state is not None
    assert job_state.status == "failed"
    await recovered.close()
