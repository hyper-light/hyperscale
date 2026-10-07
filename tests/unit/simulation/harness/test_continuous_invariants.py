"""
Mutation checks for the continuous invariant catalog
(``tests/simulation/harness/invariants.py`` + ``invariant_checks/``).

Every invariant is shown both ways on node doubles that carry the real
production state objects: it holds on a healthy cluster, and it fails --
naming the breach -- once the violation is injected into that state
(a fence token forced backwards, a token run twice, a PID shared, a
lock leaked, cores held past the bound, views split past a
dissemination, a foreign member gossiped in). The last test runs the
real ``InvariantChecker`` loop to show a violation injected mid-run is
caught on the next tick and surfaced, not swallowed.
"""

import asyncio
import math

from hyperscale.distributed.models import JobStatus, WorkerStatus, WorkflowProgress
from hyperscale.distributed.models.jobs import JobInfo, SubWorkflowInfo, TrackingToken, WorkflowInfo
from hyperscale.distributed.workflow.workflow_state import WorkflowState

from tests.simulation.harness.invariants import (
    InvariantChecker,
    InvariantViolation,
    SafetyInvariant,
    cancelled_jobs_free_cores,
    cluster_id_isolation,
    leaked_locks_bounded,
    monotonic_fence_tokens,
    no_orphan_workflows,
    resource_counter_consistency,
    unique_sub_workflow_tokens,
    worker_subprocess_attribution,
)
from tests.simulation.harness.invariant_checks.cancelled_core_release import CancelledCoreRelease
from tests.simulation.harness.invariant_checks.job_progress import JobProgressWatch
from tests.simulation.harness.invariant_checks.member_count_convergence import MemberCountConvergence
from tests.simulation.harness.server_handle import ServerHandle
from tests.unit.simulation.harness.fake_cluster import FakeCluster
from tests.unit.simulation.harness.fake_nodes import HOST, gate_handle, manager_handle, worker_handle

DATACENTER = "dc-a"
JOB_ID = "job-1"
WORKFLOW_ID = "workflow-1"


class ManualClock:
    """A clock a test advances by hand."""

    def __init__(self) -> None:
        self.now: float = 1000.0

    def __call__(self) -> float:
        return self.now


def _cluster() -> tuple[FakeCluster, ServerHandle, ServerHandle, ServerHandle]:
    cluster = FakeCluster([DATACENTER])
    manager = manager_handle(DATACENTER, 0, 9000)
    worker_a = worker_handle(DATACENTER, 0, 9100)
    worker_b = worker_handle(DATACENTER, 1, 9200)
    cluster.handles.extend([manager, worker_a, worker_b])
    return cluster, manager, worker_a, worker_b


def _sub_token(manager: ServerHandle, worker: ServerHandle) -> str:
    return str(
        TrackingToken.for_sub_workflow(
            DATACENTER, manager.instance._node_id.full, JOB_ID, WORKFLOW_ID, worker.instance._node_id.full
        )
    )


def _progress(token: str, completed_count: int) -> WorkflowProgress:
    return WorkflowProgress(
        job_id=JOB_ID,
        workflow_id=token,
        workflow_name="Workflow",
        status="running",
        completed_count=completed_count,
        failed_count=0,
        rate_per_second=0.0,
        elapsed_seconds=0.0,
    )


def _run_on_worker(manager: ServerHandle, worker: ServerHandle, token: str, cores: int) -> None:
    worker.instance._worker_state.add_active_workflow(
        token, _progress(token, 0), (manager.host, manager.tcp_port)
    )
    asyncio.run(worker.instance._core_allocator.allocate(token, cores))


def _led_job(manager: ServerHandle, worker: ServerHandle, status: str) -> JobInfo:
    """Install a job the manager leads, one workflow DISPATCHED to ``worker``."""
    manager_id = manager.instance._node_id.full
    job = JobInfo(token=TrackingToken.for_job(DATACENTER, manager_id, JOB_ID), submission=None, status=status)
    workflow_token = TrackingToken.for_workflow(DATACENTER, manager_id, JOB_ID, WORKFLOW_ID)
    sub_token = workflow_token.to_sub_workflow_token(worker.instance._node_id.full)
    job.workflows[str(workflow_token)] = WorkflowInfo(
        token=workflow_token, name="Workflow", sub_workflow_tokens=[str(sub_token)]
    )
    job.sub_workflows[str(sub_token)] = SubWorkflowInfo(
        token=sub_token, parent_token=workflow_token, cores_allocated=2, progress=_progress(str(sub_token), 0)
    )
    manager.instance._job_manager._jobs[JOB_ID] = job
    manager.instance._job_manager.workflow_lifecycle.install_state(
        JOB_ID, WORKFLOW_ID, WorkflowState.DISPATCHED, 0, "test"
    )
    manager.instance._manager_state.apply_job_leadership(
        JOB_ID, manager_id, (manager.host, manager.tcp_port), 1
    )
    return job


def _holds(invariant: SafetyInvariant, cluster: FakeCluster) -> bool:
    return invariant.evaluate(cluster).holds


def _breach(invariant: SafetyInvariant, cluster: FakeCluster) -> str:
    result = invariant.evaluate(cluster)
    assert not result.holds, f"{invariant.name} missed the injected violation"
    return result.detail


# --- MonotonicFenceTokens -------------------------------------------------


def test_fence_token_forced_backwards_is_caught_on_every_source() -> None:
    cluster, manager, worker, _ = _cluster()
    gate = gate_handle(0, 9300, active_peer_count=0)
    cluster.handles.append(gate)
    invariant = monotonic_fence_tokens()
    sources = [
        manager.instance._manager_state._job_fencing_tokens,
        manager.instance._job_manager._job_fence_tokens,
        worker.instance._worker_state._job_fence_tokens,
        gate.instance._job_manager._job_fence_tokens,
    ]
    _set_every_token(sources, 5)
    assert _holds(invariant, cluster)
    _set_every_token(sources, 6)
    assert _holds(invariant, cluster)

    for tokens in sources:
        tokens[JOB_ID] = 4
        assert "went backwards: 6 -> 4" in _breach(invariant, cluster)
        tokens[JOB_ID] = 6


def _set_every_token(sources: list[dict[str, int]], token: int) -> None:
    for tokens in sources:
        tokens[JOB_ID] = token


def test_fence_token_forgotten_then_relearned_older_is_caught() -> None:
    cluster, _, worker, _ = _cluster()
    invariant = monotonic_fence_tokens()
    tokens = worker.instance._worker_state._job_fence_tokens
    tokens[JOB_ID] = 7
    assert _holds(invariant, cluster)
    del tokens[JOB_ID]
    assert _holds(invariant, cluster)

    tokens[JOB_ID] = 3
    assert "went backwards: 7 -> 3" in _breach(invariant, cluster)


def test_restarted_node_starts_a_fresh_fence_history() -> None:
    cluster, manager, _, _ = _cluster()
    invariant = monotonic_fence_tokens()
    manager.instance._manager_state._job_fencing_tokens[JOB_ID] = 9
    assert _holds(invariant, cluster)

    manager.instance = manager_handle(DATACENTER, 0, 9000).instance
    manager.instance._manager_state._job_fencing_tokens[JOB_ID] = 1
    assert _holds(invariant, cluster)


# --- UniqueSubWorkflowTokens ----------------------------------------------


def test_sub_workflow_token_run_on_two_workers_is_caught() -> None:
    cluster, manager, worker_a, worker_b = _cluster()
    invariant = unique_sub_workflow_tokens()
    token = _sub_token(manager, worker_a)
    _run_on_worker(manager, worker_a, token, 1)
    assert _holds(invariant, cluster)

    _run_on_worker(manager, worker_b, token, 1)
    assert "runs on both" in _breach(invariant, cluster)


def test_sub_workflow_token_naming_another_worker_is_caught() -> None:
    cluster, manager, worker_a, worker_b = _cluster()
    _run_on_worker(manager, worker_b, _sub_token(manager, worker_a), 1)

    assert "that names worker" in _breach(unique_sub_workflow_tokens(), cluster)


def test_sub_workflow_token_listed_twice_in_a_job_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    invariant = unique_sub_workflow_tokens()
    job = _led_job(manager, worker, JobStatus.RUNNING.value)
    assert _holds(invariant, cluster)

    workflow = next(iter(job.workflows.values()))
    workflow.sub_workflow_tokens.append(workflow.sub_workflow_tokens[0])
    assert "more than once" in _breach(invariant, cluster)


# --- WorkerSubprocessAttribution ------------------------------------------


def test_executor_pid_in_two_worker_pools_is_caught() -> None:
    cluster, _, worker_a, worker_b = _cluster()
    invariant = worker_subprocess_attribution()
    pool_of = {
        handle.node_id: handle.instance._lifecycle_manager._server_pool._executor._processes
        for handle in (worker_a, worker_b)
    }
    pool_of[worker_a.node_id][4101] = object()
    pool_of[worker_b.node_id][4102] = object()
    assert _holds(invariant, cluster)

    pool_of[worker_b.node_id][4101] = object()
    assert "executor PID 4101" in _breach(invariant, cluster)


# --- NoOrphanWorkflows ----------------------------------------------------


def test_workflow_without_a_known_job_leader_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    invariant = no_orphan_workflows()
    token = _sub_token(manager, worker)
    _run_on_worker(manager, worker, token, 1)
    assert _holds(invariant, cluster)

    del worker.instance._worker_state._workflow_job_leader[token]
    assert "no known job leader" in _breach(invariant, cluster)


def test_workflow_led_by_a_non_manager_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    token = _sub_token(manager, worker)
    _run_on_worker(manager, worker, token, 1)
    worker.instance._worker_state._workflow_job_leader[token] = (HOST, 1)

    assert "no manager of this cluster" in _breach(no_orphan_workflows(), cluster)


# --- LeakedLocksBounded ---------------------------------------------------


def test_peer_lock_outliving_its_peer_is_caught() -> None:
    cluster, manager, _, _ = _cluster()
    invariant = leaked_locks_bounded()
    state = manager.instance._manager_state
    dead_peer = (HOST, 9400)
    state._peer_state_locks[dead_peer] = asyncio.Lock()
    state.add_dead_manager(dead_peer, 0.0)
    assert _holds(invariant, cluster)

    state.remove_dead_manager(dead_peer)
    assert str(dead_peer) in _breach(invariant, cluster)


def test_gate_lock_outliving_its_gate_is_caught() -> None:
    cluster, manager, _, _ = _cluster()
    manager.instance._manager_state._gate_state_locks["gone-gate"] = asyncio.Lock()

    assert "gone-gate" in _breach(leaked_locks_bounded(), cluster)


# --- JobMakesProgress -----------------------------------------------------


def test_in_flight_job_stalled_past_the_stuck_bound_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    clock = ManualClock()
    watch = JobProgressWatch(clock=clock)
    job = _led_job(manager, worker, JobStatus.RUNNING.value)
    env = manager.instance.env
    stuck_bound = env.JOB_STUCK_THRESHOLD + env.JOB_TIMEOUT_CHECK_INTERVAL
    assert watch.evaluate(cluster).holds

    clock.now += stuck_bound
    assert watch.evaluate(cluster).holds
    clock.now += 1.0
    result = watch.evaluate(cluster)
    assert not result.holds and "made no progress" in result.detail

    next(iter(job.sub_workflows.values())).progress = _progress("progressed", 5)
    assert watch.evaluate(cluster).holds


def test_job_progress_watch_ignores_jobs_out_of_flight_and_paused_leaders() -> None:
    cluster, manager, worker, _ = _cluster()
    clock = ManualClock()
    watch = JobProgressWatch(clock=clock)
    _led_job(manager, worker, JobStatus.RUNNING.value)
    watch.evaluate(cluster)

    cluster.faults.paused_node_ids.add(manager.node_id)
    clock.now += 10_000.0
    assert watch.evaluate(cluster).holds
    cluster.faults.paused_node_ids.clear()
    assert watch.evaluate(cluster).holds

    manager.instance._job_manager.workflow_lifecycle.install_state(
        JOB_ID, WORKFLOW_ID, WorkflowState.COMPLETED, 0, "test"
    )
    clock.now += 10_000.0
    assert watch.evaluate(cluster).holds


# --- CancelledJobsFreeCores -----------------------------------------------


def test_cancelled_job_holding_cores_past_the_worker_bound_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    clock = ManualClock()
    release = CancelledCoreRelease(clock=clock)
    _led_job(manager, worker, JobStatus.CANCELLED.value)
    _run_on_worker(manager, worker, _sub_token(manager, worker), 2)
    config = worker.instance._config
    release_bound = (
        config.cancellation_poll_interval_seconds
        + config.tcp_timeout_short_seconds
        + config.workflow_cancel_timeout_seconds
        + config.execution_update_wait_seconds
    )
    assert release.evaluate(cluster).holds

    clock.now += release_bound
    assert release.evaluate(cluster).holds
    clock.now += 1.0
    result = release.evaluate(cluster)
    assert not result.holds and "still holds cores" in result.detail

    asyncio.run(worker.instance._core_allocator.free(_sub_token(manager, worker)))
    assert release.evaluate(cluster).holds


def test_cancelled_core_clock_restarts_after_a_disruption() -> None:
    cluster, manager, worker, _ = _cluster()
    clock = ManualClock()
    release = CancelledCoreRelease(clock=clock)
    _led_job(manager, worker, JobStatus.CANCELLED.value)
    _run_on_worker(manager, worker, _sub_token(manager, worker), 2)
    release.evaluate(cluster)

    cluster.faults.disrupted = True
    clock.now += 10_000.0
    assert release.evaluate(cluster).holds
    cluster.faults.disrupted = False
    assert release.evaluate(cluster).holds


def test_running_job_holding_cores_owes_no_release() -> None:
    cluster, manager, worker, _ = _cluster()
    clock = ManualClock()
    invariant = cancelled_jobs_free_cores()
    _led_job(manager, worker, JobStatus.RUNNING.value)
    _run_on_worker(manager, worker, _sub_token(manager, worker), 2)

    assert _holds(invariant, cluster)
    clock.now += 10_000.0
    assert CancelledCoreRelease(clock=clock).evaluate(cluster).holds


# --- ResourceCounterConsistency -------------------------------------------


def test_leaked_worker_core_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    invariant = resource_counter_consistency()
    _run_on_worker(manager, worker, _sub_token(manager, worker), 2)
    assert _holds(invariant, cluster)

    worker.instance._core_allocator._available_cores -= 1
    assert "core counters disagree" in _breach(invariant, cluster)


def test_manager_reserving_more_cores_than_a_worker_has_is_caught() -> None:
    cluster, manager, _, _ = _cluster()
    invariant = resource_counter_consistency()
    worker_status = WorkerStatus(worker_id="dc-a-worker-0", state="healthy", available_cores=4, total_cores=4)
    manager.instance._worker_pool._workers[worker_status.worker_id] = worker_status
    worker_status.reserved_cores = 4
    assert _holds(invariant, cluster)

    worker_status.reserved_cores = 5
    assert "out-of-range cores" in _breach(invariant, cluster)


def test_manager_recording_a_total_other_than_the_workers_own_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    invariant = resource_counter_consistency()
    worker_total = worker.instance._core_allocator.total_cores
    worker_status = WorkerStatus(
        worker_id=worker.instance._node_id.full, state="healthy", available_cores=0, total_cores=worker_total
    )
    manager.instance._worker_pool._workers[worker_status.worker_id] = worker_status
    assert _holds(invariant, cluster)

    # In range, yet not the worker's: a total derived from the free count.
    worker_status.total_cores = worker_total - 1
    assert "but the worker's allocator holds" in _breach(invariant, cluster)


# --- MemberCountConvergence -----------------------------------------------


def _manager_pair() -> tuple[FakeCluster, ServerHandle, ServerHandle]:
    cluster = FakeCluster([DATACENTER])
    manager_a = manager_handle(DATACENTER, 0, 9000)
    manager_b = manager_handle(DATACENTER, 1, 9010)
    cluster.handles.extend([manager_a, manager_b])
    for observer, peer in ((manager_a, manager_b), (manager_b, manager_a)):
        observer.instance._manager_state._active_manager_peers.add((peer.host, peer.tcp_port))
    return cluster, manager_a, manager_b


def test_member_counts_split_past_one_dissemination_are_caught() -> None:
    cluster, manager_a, manager_b = _manager_pair()
    clock = ManualClock()
    convergence = MemberCountConvergence(clock=clock)
    env = manager_a.instance.env
    observer_count = 2
    rebroadcast_rounds = max(
        1, int(manager_a.instance._gossip_buffer.broadcast_multiplier * math.log(observer_count + 1))
    )
    dissemination_bound = (rebroadcast_rounds + 1) * env.SWIM_UDP_POLL_INTERVAL + env.SWIM_MAX_PROBE_TIMEOUT
    assert convergence.evaluate(cluster).holds

    manager_a.instance._manager_state._active_manager_peers.clear()
    assert convergence.evaluate(cluster).holds
    clock.now += dissemination_bound
    assert convergence.evaluate(cluster).holds
    clock.now += 1.0
    result = convergence.evaluate(cluster)
    assert not result.holds and "disagree on member count" in result.detail

    manager_b.instance._manager_state._active_manager_peers.clear()
    assert convergence.evaluate(cluster).holds


def test_member_counts_owe_nothing_before_stability_or_under_disruption() -> None:
    cluster, manager_a, _ = _manager_pair()
    clock = ManualClock()
    convergence = MemberCountConvergence(clock=clock)
    manager_a.instance._manager_state._active_manager_peers.clear()

    for unstable, disrupted in ((True, False), (False, True)):
        cluster.stabilized = not unstable
        cluster.faults.disrupted = disrupted
        convergence.evaluate(cluster)
        clock.now += 10_000.0
        assert convergence.evaluate(cluster).holds


def test_gate_member_counts_split_are_caught() -> None:
    cluster = FakeCluster([])
    cluster.handles.extend([gate_handle(0, 9300, active_peer_count=1), gate_handle(1, 9310, active_peer_count=0)])
    clock = ManualClock()
    convergence = MemberCountConvergence(clock=clock)
    convergence.evaluate(cluster)
    clock.now += 10_000.0

    assert "gates observers disagree" in convergence.evaluate(cluster).detail


# --- ClusterIdIsolation ---------------------------------------------------


def test_foreign_swim_member_is_caught() -> None:
    cluster, manager, worker, _ = _cluster()
    invariant = cluster_id_isolation()
    tracker = manager.instance._incarnation_tracker
    asyncio.run(tracker.update_node((worker.host, worker.udp_port), b"OK", 1, 0.0))
    assert _holds(invariant, cluster)

    asyncio.run(tracker.update_node((HOST, 1), b"OK", 1, 0.0))
    assert "no node of this cluster" in _breach(invariant, cluster)


def test_mixed_cluster_ids_are_caught() -> None:
    cluster, _, worker, _ = _cluster()
    worker.instance.env = worker.instance.env.model_copy(update={"CLUSTER_ID": "other-cluster"})

    assert "more than one cluster id" in _breach(cluster_id_isolation(), cluster)


# --- The checker loop -----------------------------------------------------


def test_checker_catches_a_violation_injected_mid_run() -> None:
    cluster, manager, _, _ = _cluster()

    async def run_checker() -> InvariantViolation | None:
        checker = InvariantChecker(harness=cluster, poll_interval=0.01)
        checker.add_safety(monotonic_fence_tokens())
        manager.instance._manager_state._job_fencing_tokens[JOB_ID] = 4
        await checker.start()
        await asyncio.sleep(0.05)
        assert checker.violation is None
        manager.instance._manager_state._job_fencing_tokens[JOB_ID] = 2
        await asyncio.sleep(0.05)
        await checker.stop()
        return checker.violation

    violation = asyncio.run(run_checker())
    assert violation is not None and "MonotonicFenceTokens" in str(violation)
