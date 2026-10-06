"""
A dispatch pass never waits for capacity, and its dispatch loop waits for
capacity to CHANGE -- probed, then pinned, on virtual time.

THE BUGS THESE PIN (each traced on the pre-fix code):

* Head-of-line blocking. Every pass held one dispatcher-wide lock, and
  inside it waited in ``WorkerPool.allocate_cores`` -- up to 30s per
  round -- for cores its workflow could use, then waited out each
  worker's answer to the dispatch. One unplaceable workflow, or one
  worker slow to answer, stalled EVERY job's dispatch behind it: a second
  job whose workflow fit at once dispatched only when the first one's
  wait ended. Allocation is now one attempt, the job's dispatch loop
  waits between passes, and passes hold no lock (claims make each
  workflow's dispatch exactly-once).
* A level-triggered capacity wait. The loop waited for "any healthy worker
  has a free core", which holds at once over cores the workflow cannot
  use -- with a one-attempt allocator, a frozen-instant livelock (SIM) or
  a 100%-CPU spin (real host). It now waits for ``capacity_generation`` to
  move since its pass, so the unplaceable workflow re-checks only when
  something changes, or on the loop's periodic re-check.
* Unattainable shares. The dispatcher sized shares against a total that
  counted an overloaded worker's free cores, which allocation never
  selects, and allocation held out for the whole share: the workflow
  starved while the cores it could use sat idle. The total now counts
  only selectable workers, and an allocation takes what it can, up to
  the share.

Real ``WorkerPool``, ``JobManager``, ``WorkflowDispatcher`` and
``TaskRunner`` on a ``SimulationLoop``, every ``hyperscale.distributed``
clock swapped to its virtual time; ``run_window``'s runaway guard turns
any same-instant spin into a loud failure. The one stand-in is each
worker's answer to a dispatch (it takes it), recorded with the virtual
instant it was sent.
"""

import asyncio
import contextvars
from typing import Any, Callable, Coroutine, TypeVar

from hyperscale.core.graph.workflow import Workflow
from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.jobs.worker_pool import WorkerPool
from hyperscale.distributed.jobs.workflow_dispatcher import WorkflowDispatcher
from hyperscale.distributed.models import (
    JobSubmission,
    NodeInfo,
    TrackingToken,
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
    WorkflowDispatch,
)
from hyperscale.distributed.runtime import (
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.distributed.taskex import TaskRunner
from hyperscale.distributed.workflow import WorkflowState
from hyperscale.logging import LoggingConfig
from tests.simulation.harness.sim import SimulationLoop, VirtualClock


ScenarioResult = TypeVar("ScenarioResult")

_DATACENTER = "local"
_MANAGER_ID = "manager-1"

# The dispatch loop re-checks every 5s from 0.0. Observation ends between
# two re-checks, sharing no instant with one, and before a worker's liveness
# window (30s) lapses -- workers here heartbeat only when a scenario says so.
_OBSERVE_UNTIL = 27.5

# Between the dispatch loop's periodic re-checks (every 5s from 0.0):
# a wake at this instant can only come from the capacity change itself.
_CORES_FREED_AT = 7.25

# A worker this slow to answer a dispatch holds the pass sending to it for
# that long; well inside the observation window.
_SLOW_ANSWER_SECONDS = 3.0


def _simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh ``SimulationLoop`` through virtual
    ``until``, every ``hyperscale.distributed`` clock on the loop's virtual
    time and logging off (its stream setup uses I/O the loop bans) in a
    context of its own. ``run_window`` raises if anything spins at one
    virtual instant."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(until)
        assert scenario_task.done(), f"the scenario was still running at virtual {until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def _registration(worker_id: str, port: int, cores: int) -> WorkerRegistration:
    return WorkerRegistration(
        node=NodeInfo(
            node_id=worker_id,
            role="worker",
            host="127.0.0.1",
            port=port,
            datacenter=_DATACENTER,
            udp_port=port + 1,
        ),
        total_cores=cores,
        available_cores=cores,
        memory_mb=1024,
    )


def _heartbeat(
    worker_id: str,
    available_cores: int,
    version: int,
    overload_state: str,
    cores_version: int = 0,
) -> WorkerHeartbeat:
    return WorkerHeartbeat(
        node_id=worker_id,
        state=WorkerState.HEALTHY.value,
        available_cores=available_cores,
        queue_depth=0,
        cpu_percent=0.0,
        memory_percent=0.0,
        version=version,
        total_cores=available_cores,
        cores_version=cores_version,
        health_accepting_work=True,
        health_overload_state=overload_state,
    )


def _record_allocation_attempts(
    worker_pool: WorkerPool,
    attempts: list[tuple[float, frozenset[str]]],
) -> None:
    """Record the virtual instant, and the excluded workers, of every
    allocation the dispatcher attempts on the real pool."""
    allocate_cores = worker_pool.allocate_cores

    async def recording_allocate_cores(
        cores_needed: int,
        excluded_worker_ids: set[str] | None = None,
        **reservation,
    ) -> list[tuple[str, int]] | None:
        attempts.append(
            (asyncio.get_running_loop().time(), frozenset(excluded_worker_ids or ()))
        )
        return await allocate_cores(cores_needed, excluded_worker_ids=excluded_worker_ids, **reservation)

    worker_pool.allocate_cores = recording_allocate_cores


def _build_job_manager(clock: VirtualClock) -> JobManager:
    return JobManager(
        datacenter=_DATACENTER,
        manager_id=_MANAGER_ID,
        clock=clock,
        max_budgeted_retries=Env().RETRY_BUDGET_PER_WORKFLOW_MAX,
    )


def _build_dispatcher(
    job_manager: JobManager,
    worker_pool: WorkerPool,
    task_runner: TaskRunner,
    sends: list[tuple[float, str, str, int]],
    terminal_calls: list[str],
    answer_delay_by_worker: dict[str, float],
) -> WorkflowDispatcher:
    # Each worker's core availability version, bumped as it allocates a
    # dispatch's cores -- the version its ack carries, which the manager's
    # dispatch coordinator records on the pool.
    worker_cores_versions: dict[str, int] = {}

    async def take_dispatch(
        worker_id: str,
        dispatch: WorkflowDispatch,
    ) -> tuple[DispatchOutcome, str]:
        sends.append(
            (asyncio.get_running_loop().time(), worker_id, dispatch.job_id, dispatch.cores)
        )
        if (answer_delay := answer_delay_by_worker.get(worker_id)) is not None:
            await asyncio.sleep(answer_delay)
        worker_cores_versions[worker_id] = worker_cores_versions.get(worker_id, 0) + 1
        await worker_pool.record_dispatch_taken(
            worker_id, dispatch.workflow_id, worker_cores_versions[worker_id]
        )
        return DispatchOutcome.ACCEPTED, ""

    async def record_exhaustion(job_id: str, workflow_id: str, reason: str) -> None:
        terminal_calls.append(f"exhausted {job_id}/{workflow_id}: {reason}")

    async def record_stopped_plans(job_id: str, plans: list[tuple[str, str]]) -> None:
        terminal_calls.append(f"stopped plans of {job_id}: {plans}")

    return WorkflowDispatcher(
        job_manager=job_manager,
        worker_pool=worker_pool,
        send_dispatch=take_dispatch,
        datacenter=_DATACENTER,
        manager_id=_MANAGER_ID,
        task_runner=task_runner,
        on_dispatch_exhausted=record_exhaustion,
        stop_dispatched_plans=record_stopped_plans,
    )


async def _submit_job(
    job_manager: JobManager,
    dispatcher: WorkflowDispatcher,
    job_id: str,
    workflow_id: str,
    vus: int,
) -> JobSubmission:
    """Create the job, register its one workflow, and leave it queued."""
    submission = JobSubmission(job_id=job_id, workflows=b"", vus=vus, timeout_seconds=300.0)
    await job_manager.create_job(submission)
    workflow = Workflow()
    workflow.vus = vus
    assert await dispatcher.register_workflows(submission, [(workflow_id, [], workflow)])
    return submission


async def _exclude_from_workflow(
    dispatcher: WorkflowDispatcher,
    job_id: str,
    workflow_id: str,
    worker_id: str,
) -> None:
    """Leave the workflow as worker loss leaves it: requeued, excluding the
    worker that lost it -- which then re-registered under the same id (a
    false SWIM death, an eviction), so its cores are in the pool but never
    the workflow's."""
    lost_sub_workflow_token = TrackingToken.for_workflow(
        _DATACENTER, _MANAGER_ID, job_id, workflow_id
    ).to_sub_workflow_token(worker_id)
    assert await dispatcher.requeue_workflow(
        str(lost_sub_workflow_token), excluded_worker_id=worker_id
    )


def test_an_unplaceable_workflow_stalls_no_other_job() -> None:
    """Job A's workflow may not use the cluster's one worker; job B's fits.

    Pre-fix, A's pass held the dispatch lock through a 30s allocation wait,
    so B -- queued behind it -- had not dispatched by the time the worker's
    liveness lapsed. B now dispatches at 0.0, and A, still PENDING, re-checks
    once per capacity event or periodic re-check -- never in a loop.
    """

    async def scenario(
        clock: VirtualClock,
    ) -> tuple[
        list[tuple[float, str, str, int]],
        list[float],
        WorkflowState | None,
        list[str],
    ]:
        sends: list[tuple[float, str, str, int]] = []
        attempts: list[tuple[float, frozenset[str]]] = []
        terminal_calls: list[str] = []
        task_runner = TaskRunner()
        worker_pool = WorkerPool()
        _record_allocation_attempts(worker_pool, attempts)
        job_manager = _build_job_manager(clock)
        dispatcher = _build_dispatcher(
            job_manager, worker_pool, task_runner, sends, terminal_calls, answer_delay_by_worker={}
        )
        try:
            await worker_pool.register_worker(_registration("worker-1", 10_001, cores=2))
            job_a = await _submit_job(job_manager, dispatcher, "job-a", "workflow-a", vus=1)
            await _exclude_from_workflow(dispatcher, "job-a", "workflow-a", "worker-1")
            await dispatcher.start_job_dispatch("job-a", job_a)
            job_b = await _submit_job(job_manager, dispatcher, "job-b", "workflow-b", vus=1)
            await dispatcher.start_job_dispatch("job-b", job_b)

            await clock.sleep(_OBSERVE_UNTIL)
            return (
                sends,
                [attempted_at for attempted_at, excluded in attempts if excluded],
                job_manager.workflow_lifecycle.get_state("job-a", "workflow-a"),
                terminal_calls,
            )
        finally:
            await dispatcher.shutdown()
            await task_runner.shutdown()

    sends, job_a_attempts, job_a_state, terminal_calls = _simulate(scenario, _OBSERVE_UNTIL)

    assert sends == [(0.0, "worker-1", "job-b", 1)]
    assert job_a_state == WorkflowState.PENDING
    # At 0.0: A's first pass; the wake its requeue signalled (ready event and
    # dispatch trigger); the dispatch trigger B's taken dispatch set. Then
    # one re-check per periodic wake -- the pool never changed again.
    assert job_a_attempts == [0.0, 0.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0]
    assert terminal_calls == []


def test_freed_cores_wake_a_waiting_workflow_at_once() -> None:
    """The cluster's one core runs job A's workflow; job B's waits for it.

    When the worker reports the core free -- between the loop's periodic
    re-checks -- B dispatches at that instant: the capacity change ends the
    wait it began after its pass, so a change is never lost to the window
    between the pass and the wait.
    """

    async def scenario(
        clock: VirtualClock,
    ) -> tuple[list[tuple[float, str, str, int]], list[str]]:
        sends: list[tuple[float, str, str, int]] = []
        terminal_calls: list[str] = []
        task_runner = TaskRunner()
        worker_pool = WorkerPool()
        job_manager = _build_job_manager(clock)
        dispatcher = _build_dispatcher(
            job_manager, worker_pool, task_runner, sends, terminal_calls, answer_delay_by_worker={}
        )
        try:
            await worker_pool.register_worker(_registration("worker-1", 10_001, cores=1))
            job_a = await _submit_job(job_manager, dispatcher, "job-a", "workflow-a", vus=1)
            await dispatcher.start_job_dispatch("job-a", job_a)
            job_b = await _submit_job(job_manager, dispatcher, "job-b", "workflow-b", vus=1)
            await dispatcher.start_job_dispatch("job-b", job_b)

            await clock.sleep(_CORES_FREED_AT)
            await worker_pool.process_heartbeat(
                "worker-1",
                # Job A's core: allocated at version 1, freed at 2.
                _heartbeat("worker-1", available_cores=1, version=1, overload_state="healthy", cores_version=2),
            )
            await clock.sleep(_OBSERVE_UNTIL - _CORES_FREED_AT)
            return sends, terminal_calls
        finally:
            await dispatcher.shutdown()
            await task_runner.shutdown()

    sends, terminal_calls = _simulate(scenario, _OBSERVE_UNTIL)

    assert sends == [
        (0.0, "worker-1", "job-a", 1),
        (_CORES_FREED_AT, "worker-1", "job-b", 1),
    ]
    assert terminal_calls == []


def test_an_overloaded_workers_cores_are_not_offered() -> None:
    """One worker is overloaded, one healthy; the workflow wants four cores.

    Allocation never selects an overloaded worker, so its free cores are not
    the dispatcher's to share out: the workflow is sized to, and dispatched
    on, the healthy worker's two at 0.0. Pre-fix its share counted all four,
    allocation held out for them, and it never dispatched.
    """

    async def scenario(
        clock: VirtualClock,
    ) -> tuple[list[tuple[float, str, str, int]], list[str]]:
        sends: list[tuple[float, str, str, int]] = []
        terminal_calls: list[str] = []
        task_runner = TaskRunner()
        worker_pool = WorkerPool()
        job_manager = _build_job_manager(clock)
        dispatcher = _build_dispatcher(
            job_manager, worker_pool, task_runner, sends, terminal_calls, answer_delay_by_worker={}
        )
        try:
            await worker_pool.register_worker(_registration("worker-1", 10_001, cores=2))
            await worker_pool.register_worker(_registration("worker-2", 10_011, cores=2))
            await worker_pool.process_heartbeat(
                "worker-1",
                _heartbeat("worker-1", available_cores=2, version=1, overload_state="overloaded"),
            )
            assert worker_pool.get_total_available_cores() == 2

            job = await _submit_job(job_manager, dispatcher, "job-a", "workflow-a", vus=4)
            await dispatcher.start_job_dispatch("job-a", job)

            await clock.sleep(_OBSERVE_UNTIL)
            return sends, terminal_calls
        finally:
            await dispatcher.shutdown()
            await task_runner.shutdown()

    sends, terminal_calls = _simulate(scenario, _OBSERVE_UNTIL)

    assert sends == [(0.0, "worker-2", "job-a", 2)]
    assert terminal_calls == []


def test_an_excluded_worker_shrinks_the_share_instead_of_starving_it() -> None:
    """Two workers, two cores each; the workflow wants four but may not use
    the worker that lost it.

    Its share is sized against both workers' cores; it takes the two it may
    use, at 0.0. Pre-fix, allocation held out for all four and the workflow
    never dispatched while the worker it could use sat idle.
    """

    async def scenario(
        clock: VirtualClock,
    ) -> tuple[list[tuple[float, str, str, int]], list[str]]:
        sends: list[tuple[float, str, str, int]] = []
        terminal_calls: list[str] = []
        task_runner = TaskRunner()
        worker_pool = WorkerPool()
        job_manager = _build_job_manager(clock)
        dispatcher = _build_dispatcher(
            job_manager, worker_pool, task_runner, sends, terminal_calls, answer_delay_by_worker={}
        )
        try:
            await worker_pool.register_worker(_registration("worker-1", 10_001, cores=2))
            await worker_pool.register_worker(_registration("worker-2", 10_011, cores=2))
            job = await _submit_job(job_manager, dispatcher, "job-a", "workflow-a", vus=4)
            await _exclude_from_workflow(dispatcher, "job-a", "workflow-a", "worker-1")
            await dispatcher.start_job_dispatch("job-a", job)

            await clock.sleep(_OBSERVE_UNTIL)
            return sends, terminal_calls
        finally:
            await dispatcher.shutdown()
            await task_runner.shutdown()

    sends, terminal_calls = _simulate(scenario, _OBSERVE_UNTIL)

    assert sends == [(0.0, "worker-2", "job-a", 2)]
    assert terminal_calls == []


def test_a_slow_worker_answer_stalls_no_other_job() -> None:
    """Job A's workflow goes to a worker slow to answer; job B's to another.

    Pre-fix, every pass held the dispatcher's lock through its sends, so B's
    dispatch waited out A's slow answer. B now goes at 0.0, beside A's.
    """

    async def scenario(
        clock: VirtualClock,
    ) -> tuple[list[tuple[float, str, str, int]], list[str]]:
        sends: list[tuple[float, str, str, int]] = []
        terminal_calls: list[str] = []
        task_runner = TaskRunner()
        worker_pool = WorkerPool()
        job_manager = _build_job_manager(clock)
        dispatcher = _build_dispatcher(
            job_manager,
            worker_pool,
            task_runner,
            sends,
            terminal_calls,
            answer_delay_by_worker={"worker-2": _SLOW_ANSWER_SECONDS},
        )
        try:
            # Allocation prefers the worker with the most free cores: B's
            # workflow lands on worker-1; A's, kept off worker-1, on worker-2.
            await worker_pool.register_worker(_registration("worker-1", 10_001, cores=4))
            await worker_pool.register_worker(_registration("worker-2", 10_011, cores=1))
            job_a = await _submit_job(job_manager, dispatcher, "job-a", "workflow-a", vus=1)
            await _exclude_from_workflow(dispatcher, "job-a", "workflow-a", "worker-1")
            await dispatcher.start_job_dispatch("job-a", job_a)
            job_b = await _submit_job(job_manager, dispatcher, "job-b", "workflow-b", vus=1)
            await dispatcher.start_job_dispatch("job-b", job_b)

            await clock.sleep(_OBSERVE_UNTIL)
            return sends, terminal_calls
        finally:
            await dispatcher.shutdown()
            await task_runner.shutdown()

    sends, terminal_calls = _simulate(scenario, _OBSERVE_UNTIL)

    assert sends == [
        (0.0, "worker-2", "job-a", 1),
        (0.0, "worker-1", "job-b", 1),
    ]
    assert terminal_calls == []
