"""
A worker's workflow is no longer orphaned once a manager claims its job's
leadership -- also when that manager is the leader it already had.

SWIM can declare a live manager dead (a blip) and see it rejoin. Its
workers mark the workflows it leads orphaned, and cancel them when the
orphan grace period ends without a leader transfer. A heartbeat in which a
manager claims the job's leadership cleared the mark only if it named a
DIFFERENT address -- the same leader, rejoined, never cleared it, so the
worker cancelled work its live leader still led. Driven through the real
worker state and the server's leadership-claim handler.
"""

from types import SimpleNamespace

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.distributed.nodes.worker.state import WorkerState

LEADER = ("10.0.0.1", 9000)
OTHER_MANAGER = ("10.0.0.2", 9000)


def worker_with_orphaned_workflow() -> SimpleNamespace:
    worker_state = WorkerState(
        SimpleNamespace(total_cores=4, available_cores=4),
        throughput_interval_seconds=Env().WORKER_THROUGHPUT_INTERVAL_SECONDS,
        completion_times_max_samples=Env().WORKER_COMPLETION_TIMES_MAX_SAMPLES,
    )
    worker_state.mark_workflow_orphaned("workflow-1")
    return SimpleNamespace(
        _active_workflows={"workflow-1": SimpleNamespace(job_id="job-1")},
        _workflow_job_leader={"workflow-1": LEADER},
        _worker_state=worker_state,
        _udp_logger=SimpleNamespace(log=None),
    )


def claim(worker: SimpleNamespace, claimant: tuple[str, int], jobs: list[str]) -> None:
    WorkerServer._on_job_leadership_update(
        worker, jobs, claimant, "127.0.0.1", 8000, "worker-1", lambda *args, **kwargs: None
    )


def test_the_same_leader_claiming_the_job_clears_the_orphan() -> None:
    worker = worker_with_orphaned_workflow()

    claim(worker, LEADER, ["job-1"])

    assert not worker._worker_state.is_workflow_orphaned("workflow-1")
    assert worker._workflow_job_leader["workflow-1"] == LEADER


def test_a_new_leader_claiming_the_job_clears_the_orphan_and_takes_it() -> None:
    worker = worker_with_orphaned_workflow()

    claim(worker, OTHER_MANAGER, ["job-1"])

    assert not worker._worker_state.is_workflow_orphaned("workflow-1")
    assert worker._workflow_job_leader["workflow-1"] == OTHER_MANAGER


def test_a_claim_of_another_job_leaves_the_orphan() -> None:
    worker = worker_with_orphaned_workflow()

    claim(worker, LEADER, ["job-2"])

    assert worker._worker_state.is_workflow_orphaned("workflow-1")
