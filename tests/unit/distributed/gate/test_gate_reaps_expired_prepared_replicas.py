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

from hyperscale.distributed.nodes.gate.replication_coordinator import GateJobReplicationCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer

EXPIRED_JOB = "job-leader-died-mid-prepare"
LIVE_JOB = "job-prepare-in-flight"
LONG_TTL_SECONDS = 3600.0


def make_coordinator() -> GateJobReplicationCoordinator:
    return GateJobReplicationCoordinator(
        logger=SimpleNamespace(log=None),
        task_runner=SimpleNamespace(run=None),
        get_node_id=lambda: SimpleNamespace(full="gate-b"),
        get_node_addr=lambda: ("10.0.0.2", 9000),
        send_tcp=None,
        apply_committed=None,
        drop_committed=None,
        prepared_ttl_seconds=LONG_TTL_SECONDS,
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
    coordinator._prepared[EXPIRED_JOB] = object()
    coordinator._prepared_expires_at[EXPIRED_JOB] = 0.0
    coordinator._commit_rollback_replicas[(EXPIRED_JOB, 1)] = None
    coordinator._commit_rollback_expires_at[(EXPIRED_JOB, 1)] = 0.0
    await coordinator._record_prepare(SimpleNamespace(job_id=LIVE_JOB, sequence=1))
    one_pass = OnePassGate(coordinator)

    await GateServer._job_cleanup_loop(one_pass.gate)

    assert EXPIRED_JOB not in coordinator._prepared
    assert (EXPIRED_JOB, 1) not in coordinator._commit_rollback_replicas
    assert LIVE_JOB in coordinator._prepared, "an unexpired prepare is kept"
