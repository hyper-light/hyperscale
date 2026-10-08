import asyncio

import pytest

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.jobs.workers.stage_priority import StagePriority
from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.jobs.workflow_dispatcher import WorkflowDispatcher
from hyperscale.distributed.models import (
    JobSubmission,
    NodeInfo,
    PendingWorkflow,
    TrackingToken,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
    WorkflowDispatch,
)
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.taskex import TaskRunner


def _registration(
    worker_id: str = "worker-1",
    port: int = 10_001,
    cores: int = 2,
) -> WorkerRegistration:
    return WorkerRegistration(
        node=NodeInfo(
            node_id=worker_id,
            role="worker",
            host="127.0.0.1",
            port=port,
            datacenter="local",
            udp_port=port + 1,
        ),
        total_cores=cores,
        available_cores=cores,
        memory_mb=1024,
    )


def _heartbeat(
    *,
    worker_id: str = "worker-1",
    state: WorkerState = WorkerState.HEALTHY,
    available_cores: int = 2,
    version: int = 1,
    accepting_work: bool = True,
) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        node_id=worker_id,
        state=state.value,
        available_cores=available_cores,
        total_cores=2,
        queue_depth=0,
        cpu_percent=0.0,
        memory_percent=0.0,
        version=version,
        health_accepting_work=accepting_work,
    )


def _pending_workflow(workflow_id: str, vus: int) -> PendingWorkflow:
    return PendingWorkflow(
        job_id="job-1",
        workflow_id=workflow_id,
        workflow_name=workflow_id,
        workflow=Workflow(),
        vus=vus,
        priority=StagePriority.AUTO,
        is_test=False,
        dependencies=set(),
    )


async def _ignore_dispatch_exhaustion(job_id: str, workflow_id: str, reason: str) -> None:
    """These tests exercise allocation and fan-out; none spends a budget."""


async def _ignore_plans_to_stop(job_id: str, plans: list[tuple[str, str]]) -> None:
    """These tests exercise allocation and fan-out; none cancels a job."""


def _dispatcher(
    max_concurrent_dispatches: int = 16,
) -> WorkflowDispatcher:
    async def send_dispatch(
        worker_id: str,
        dispatch: WorkflowDispatch,
    ) -> tuple[DispatchOutcome, str]:
        return DispatchOutcome.ACCEPTED, ""

    return WorkflowDispatcher(
        job_manager=None,
        worker_pool=WorkerPool(),
        send_dispatch=send_dispatch,
        datacenter="local",
        manager_id="manager-1",
        task_runner=TaskRunner(),
        on_dispatch_exhausted=_ignore_dispatch_exhaustion,
        stop_dispatched_plans=_ignore_plans_to_stop,
        max_concurrent_dispatches=max_concurrent_dispatches,
    )


@pytest.mark.asyncio
async def test_auto_allocation_caps_single_vu_to_one_core() -> None:
    dispatcher = _dispatcher()

    allocations = dispatcher._calculate_allocations(
        [_pending_workflow("workflow-1", vus=1)],
        total_cores=50,
    )

    assert [(pending.workflow_id, cores) for pending, cores in allocations] == [
        ("workflow-1", 1)
    ]


@pytest.mark.asyncio
async def test_auto_allocation_keeps_high_vu_parallelism() -> None:
    dispatcher = _dispatcher()

    allocations = dispatcher._calculate_allocations(
        [_pending_workflow("workflow-1", vus=1000)],
        total_cores=50,
    )

    assert [(pending.workflow_id, cores) for pending, cores in allocations] == [
        ("workflow-1", 50)
    ]


@pytest.mark.asyncio
async def test_dispatch_transport_failure_uses_routing_cooldown_not_health() -> None:
    worker_pool = WorkerPool(
        get_swim_status=lambda _addr: "OK",
        dispatch_failure_base_cooldown_seconds=30.0,
        dispatch_failure_max_cooldown_seconds=30.0,
    )
    await worker_pool.register_worker(_registration())

    assert worker_pool.is_worker_healthy("worker-1")
    assert worker_pool.record_dispatch_transport_failure("worker-1", "refused")

    worker = worker_pool.get_worker("worker-1")
    assert worker is not None
    assert worker.health == WorkerState.HEALTHY
    assert not worker_pool.is_worker_dispatch_routable("worker-1")
    assert not worker_pool.is_worker_healthy("worker-1")

    snapshot = worker_pool.get_worker_dispatch_routing_snapshot("worker-1")
    assert snapshot is not None
    assert snapshot["consecutive_failures"] == 1
    assert snapshot["last_error"] == "refused"

    assert worker_pool.record_dispatch_success("worker-1")
    assert worker_pool.is_worker_dispatch_routable("worker-1")
    assert worker_pool.is_worker_healthy("worker-1")


@pytest.mark.asyncio
async def test_equal_version_heartbeat_refreshes_lifecycle_state() -> None:
    worker_pool = WorkerPool(get_swim_status=lambda _addr: "OK")
    await worker_pool.register_worker(_registration())

    await worker_pool.process_heartbeat(
        "worker-1",
        _heartbeat(
            state=WorkerState.DRAINING,
            available_cores=0,
            version=1,
            accepting_work=False,
        ),
    )
    assert not worker_pool.is_worker_healthy("worker-1")

    await worker_pool.process_heartbeat(
        "worker-1",
        _heartbeat(
            state=WorkerState.HEALTHY,
            available_cores=2,
            version=1,
            accepting_work=True,
        ),
    )
    assert worker_pool.is_worker_healthy("worker-1")


@pytest.mark.asyncio
async def test_manager_drain_intent_survives_healthy_heartbeat() -> None:
    worker_pool = WorkerPool(get_swim_status=lambda _addr: "OK")
    await worker_pool.register_worker(_registration())

    marked = await worker_pool.mark_workers_draining(
        {"worker-1"},
        reason="planned_scale_down",
    )

    assert marked == {"worker-1"}
    assert not worker_pool.is_worker_healthy("worker-1")

    await worker_pool.process_heartbeat(
        "worker-1",
        _heartbeat(
            state=WorkerState.HEALTHY,
            available_cores=2,
            version=1,
            accepting_work=True,
        ),
    )

    worker = worker_pool.get_worker("worker-1")
    assert worker is not None
    assert worker.health == WorkerState.DRAINING
    assert not worker_pool.is_worker_healthy("worker-1")


@pytest.mark.asyncio
async def test_allocation_excludes_reassignment_workers() -> None:
    worker_pool = WorkerPool(get_swim_status=lambda _addr: "OK")
    await worker_pool.register_worker(_registration("worker-1", port=10_001))
    await worker_pool.register_worker(_registration("worker-2", port=10_011))

    allocations = await worker_pool.allocate_cores(
        1,
        excluded_worker_ids={"worker-1"},
        job_id="job-1",
        dispatch_token_for=lambda worker_id: f"job-1:workflow-1:{worker_id}",
    )

    assert allocations == [("worker-2", 1)]


async def _claimed_workflow_job_manager() -> JobManager:
    """A JobManager holding ``workflow-1`` of ``job-1`` DISPATCHED: a
    dispatch's plans go out only while their workflow is."""
    job_manager = JobManager(
        datacenter="local",
        manager_id="manager-1",
        clock=RealClock(),
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )
    await job_manager.create_job(
        JobSubmission(job_id="job-1", workflows=b"", vus=5, timeout_seconds=60)
    )
    await job_manager.register_workflow(
        "job-1",
        "workflow-1",
        "workflow-1",
        Workflow(),
        dependency_workflow_ids=frozenset(),
        is_test=False,
    )
    assert await job_manager.claim_workflow_for_dispatch("job-1", "workflow-1")
    return job_manager


def _dispatch_plans(worker_count: int) -> list:
    workflow_token = TrackingToken.for_workflow("local", "manager-1", "job-1", "workflow-1")
    return [
        (
            f"worker-{worker_index}",
            1,
            workflow_token.to_sub_workflow_token(f"worker-{worker_index}"),
            WorkflowDispatch(job_id="job-1", workflow_id=f"workflow-{worker_index}"),
        )
        for worker_index in range(worker_count)
    ]


@pytest.mark.asyncio
async def test_nothing_is_sent_for_a_workflow_that_left_the_dispatch() -> None:
    sent: list[str] = []

    async def send_dispatch(
        worker_id: str,
        dispatch: WorkflowDispatch,
    ) -> tuple[DispatchOutcome, str]:
        sent.append(worker_id)
        return DispatchOutcome.ACCEPTED, ""

    job_manager = await _claimed_workflow_job_manager()
    dispatcher = WorkflowDispatcher(
        job_manager=job_manager,
        worker_pool=WorkerPool(),
        send_dispatch=send_dispatch,
        datacenter="local",
        manager_id="manager-1",
        task_runner=TaskRunner(),
        on_dispatch_exhausted=_ignore_dispatch_exhaustion,
        stop_dispatched_plans=_ignore_plans_to_stop,
    )
    # A cancellation lands between the claim and the sends.
    assert await job_manager.cancel_workflows("job-1", None, "test") == ([], ["workflow-1"])

    results = await dispatcher._send_dispatch_plans(
        _pending_workflow("workflow-1", vus=5), _dispatch_plans(3)
    )

    assert sent == []
    assert [result[3] for result in results] == [DispatchOutcome.WITHHELD] * 3


@pytest.mark.asyncio
async def test_dispatch_plan_fanout_is_bounded() -> None:
    active_dispatches = 0
    max_active_dispatches = 0

    async def send_dispatch(
        worker_id: str,
        dispatch: WorkflowDispatch,
    ) -> tuple[DispatchOutcome, str]:
        nonlocal active_dispatches
        nonlocal max_active_dispatches
        active_dispatches += 1
        max_active_dispatches = max(max_active_dispatches, active_dispatches)
        await asyncio.sleep(0.01)
        active_dispatches -= 1
        return DispatchOutcome.ACCEPTED, ""

    dispatcher = WorkflowDispatcher(
        job_manager=await _claimed_workflow_job_manager(),
        worker_pool=WorkerPool(),
        send_dispatch=send_dispatch,
        datacenter="local",
        manager_id="manager-1",
        task_runner=TaskRunner(),
        on_dispatch_exhausted=_ignore_dispatch_exhaustion,
        stop_dispatched_plans=_ignore_plans_to_stop,
        max_concurrent_dispatches=2,
    )
    pending = _pending_workflow("workflow-1", vus=5)
    dispatch_plans = _dispatch_plans(5)

    results = await dispatcher._send_dispatch_plans(pending, dispatch_plans)

    assert [result[3] for result in results] == [DispatchOutcome.ACCEPTED] * len(dispatch_plans)
    assert max_active_dispatches == 2
