"""
The gate expires prepared replicas whose leader died mid-prepare.

GateJobReplicationCoordinator.reap_expired_prepared was written for a
gate maintenance loop that never called it. A peer keeps a prepared
replica until a commit or abort arrives -- when the leader dies between
prepare and commit, neither ever does, so the prepared replica (and
every expired commit-rollback record of a job this gate never sees
finish) stayed forever. The job-cleanup loop now reaps them each pass.

Driven through the real ``_job_cleanup_loop`` (one pass) over a real
replication coordinator.
"""

from types import SimpleNamespace

import pytest

from hyperscale.distributed.models import GateJobReplica
from hyperscale.distributed.raft.store.volatile_raft_storage import VolatileRaftStorage
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.nodes.gate.replication_coordinator import GateJobReplicationCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer

EXPIRED_JOB = "job-leader-died-mid-prepare"
LIVE_JOB = "job-prepare-in-flight"
LONG_TTL_SECONDS = 3600.0


def make_coordinator() -> GateJobReplicationCoordinator:
    return GateJobReplicationCoordinator(
        clock=RealClock(),
        logger=SimpleNamespace(log=None),
        task_runner=SimpleNamespace(run=None),
        get_node_id=lambda: SimpleNamespace(full="gate-b"),
        get_node_addr=lambda: ("10.0.0.2", 9000),
        send_tcp=None,
        apply_committed=None,
        drop_committed=None,
        storage=VolatileRaftStorage(),
        prepared_ttl_seconds=LONG_TTL_SECONDS,
    )


def make_replica(job_id: str) -> GateJobReplica:
    return GateJobReplica(
        job_id=job_id,
        sequence=1,
        fence_token=1,
        leader_id="gate-a",
        leader_addr=("10.0.0.1", 9000),
        origin_gate_addr=("10.0.0.1", 9000),
        callback_addr=None,
        target_dcs=["dc-a"],
        target_dc_count=1,
        status_seed="SUBMITTED",
        submitted_wall_time=0.0,
        raft_voters=["gate-a", "gate-b"],
    )


class OnePassGate:
    """The loop's own sleep ends the loop after one pass."""

    def __init__(self, coordinator: GateJobReplicationCoordinator) -> None:
        self.gate = object.__new__(GateServer)
        self.gate._running = True
        self.gate._job_cleanup_interval = 0.0
        self.gate._replication_coordinator = coordinator
        self.gate._get_expired_terminal_jobs = lambda now: []
        self.gate._clock = SimpleNamespace(sleep=self.sleep, monotonic=lambda: 0.0)
        self.sleeps = 0

    async def sleep(self, seconds: float) -> None:
        self.sleeps += 1
        if self.sleeps > 1:
            self.gate._running = False


@pytest.mark.asyncio
async def test_the_cleanup_loop_reaps_expired_prepared_and_rollback_entries() -> None:
    coordinator = make_coordinator()
    coordinator._prepared[EXPIRED_JOB] = make_replica(EXPIRED_JOB)
    coordinator._prepared_expires_at[EXPIRED_JOB] = 0.0
    # Rollback records are keyed by the job and the commit's exact version.
    coordinator._commit_rollback_replicas[EXPIRED_JOB] = {(1, 1): None}
    coordinator._commit_rollback_expires_at[(EXPIRED_JOB, 1, 1)] = 0.0
    await coordinator._record_prepare(make_replica(LIVE_JOB))
    one_pass = OnePassGate(coordinator)

    await GateServer._job_cleanup_loop(one_pass.gate)

    assert EXPIRED_JOB not in coordinator._prepared
    assert EXPIRED_JOB not in coordinator._commit_rollback_replicas
    assert (EXPIRED_JOB, 1, 1) not in coordinator._commit_rollback_expires_at
    assert LIVE_JOB in coordinator._prepared, "an unexpired prepare is kept"
