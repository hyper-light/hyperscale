"""
A gate recovering a job from its ledger awaits the job's results where it
runs now -- after a mid-run move off a lost datacenter too (AD-36, AD-38).

The failover's placement change committed to the gate tier in the job's
replica, which lives in the gates' memory: restarted, every gate lost it,
and one recovering the job from its ledger awaited the lost datacenter --
dropping the replacement's results as a stranger's and taking the lost
datacenter's cancelled final result for the job's. The move is now in the
job's durable record (``JobDatacenterReassigned``), and recovery restores
the job's datacenters, result slots and released datacenters from it.

A real ``JobLedger`` on a simulated filesystem, written and reopened as a
restarted gate does, recovered by a real ``GateServer`` (never started).
"""

from pathlib import Path

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.ledger import DatacenterReassignment, JobLedger
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.models import DatacenterSubstitution
from hyperscale.distributed.nodes.gate.server import GateServer
from tests.simulation.harness.sim import SimFilesystem
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

JOB_ID = "job-1"
HOST = "127.0.0.1"
REASSIGNMENT = DatacenterReassignment(
    lost_datacenter="dc-b",
    replacement_datacenter="dc-c",
    completed_workflow_ids=("wf-login",),
    total_completed=40,
    total_failed=2,
)


async def open_ledger(filesystem: SimFilesystem) -> JobLedger:
    return await JobLedger.open(
        wal_path=Path("/gate/ledger/wal"),
        checkpoint_dir=Path("/gate/ledger/checkpoints"),
        archive_dir=Path("/gate/ledger/archive"),
        region_code="global",
        gate_id="gate-1",
        clock=new_hybrid_logical_clock(),
        filesystem=filesystem,
    )


@pytest.mark.asyncio
async def test_a_recovered_job_runs_where_its_failover_moved_it() -> None:
    filesystem = SimFilesystem()
    ledger = await open_ledger(filesystem)
    _job_id, created = await ledger.create_job(
        spec_hash=b"spec",
        assigned_datacenters=("dc-a", "dc-b"),
        requestor_id=f"{HOST}:19500",
        durability=DurabilityLevel.LOCAL,
        job_id=JOB_ID,
        timeout_seconds=600.0,
    )
    reassigned = await ledger.reassign_datacenter(
        JOB_ID, REASSIGNMENT, durability=DurabilityLevel.LOCAL
    )
    await ledger.close()
    filesystem.crash()

    gate = GateServer(
        host=HOST,
        tcp_port=19481,
        udp_port=19482,
        env=Env(MERCURY_SYNC_AUTH_SECRET="recovers-job-placement-secret-012345"),
        datacenter_managers={"dc-a": [(HOST, 19581)], "dc-b": [(HOST, 19681)], "dc-c": [(HOST, 19781)]},
        datacenter_manager_udp={"dc-a": [(HOST, 19582)], "dc-b": [(HOST, 19682)], "dc-c": [(HOST, 19782)]},
    )
    gate._job_ledger = await open_ledger(filesystem)
    await gate._recover_durable_jobs()

    tracked = gate._job_timeout_tracker._tracked_jobs.get(JOB_ID)
    assert created.success and reassigned.success
    assert gate._job_manager.get_target_dcs(JOB_ID) == {"dc-a", "dc-c"}
    assert gate._job_manager.get_datacenter_substitutions(JOB_ID) == [
        DatacenterSubstitution(
            lost_datacenter="dc-b",
            replacement_datacenter="dc-c",
            completed_workflow_ids=["wf-login"],
            total_completed=40,
            total_failed=2,
        )
    ]
    assert gate._job_manager.get_released_datacenters(JOB_ID) == {"dc-b"}
    # The workflow dc-b delivered keeps its slot there; the rest are dc-c's.
    assert gate._job_manager.expected_workflow_datacenters(JOB_ID, "wf-login") == {"dc-a", "dc-b"}
    assert gate._job_manager.expected_workflow_datacenters(JOB_ID, "wf-browse") == {"dc-a", "dc-c"}
    assert tracked is not None and sorted(tracked.target_datacenters) == ["dc-a", "dc-c"]
    await gate._job_ledger.close()
