"""
Adversarial workflows (SCENARIOS §7) -- the picklable child entries of the
adversarial-workload scenarios: a workflow whose step raises, one whose
step never ends, and one whose memory grows without bound.

``adversarial_client_entry`` submits one adversary to a gateless manager,
awaits its terminal, then submits a plain ``SimPingWorkflow`` job
(``follow-up``) -- the cluster must run the next job normally once the
adversary is gone. ``observed_worker_entry`` records the worker's active
workflows and free cores on change; ``guarded_manager_entry`` records
every AD-41 kill and throttle the manager orders.

The never-ending step swallows every cancellation: under cooperative
scheduling that is how a step stuck in a loop behaves -- no in-process
cancel stops it -- so only a bound outside the step (the job's AD-34
timeout) can end the job.

SIM has no memory to exhaust: the executors' resource monitors do not run
on the ``SimulationLoop`` (``WorkflowRunner(monitors_enabled=False)`` under
an injected transport), so every executor reports zero. The worker entry
therefore SCRIPTS the hog's memory, as ``ScriptedResourceMonitor`` scripts
a host's CPU: each status update of the hog workflow reports
``growth_bytes_per_second`` times the virtual seconds since the worker
first saw it. Everything downstream -- the worker's Kalman estimate, the
progress report, the manager's ``ResourceEnforcer`` and its kill through
the cancel path -- is the production code.

Rows carry labels, statuses, booleans, counts and virtual times only --
inside the replay contract (error texts carry per-run ids, so only which
causes they name is recorded).

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname. The workflow classes are built inside
functions, so cloudpickle ships them by value -- the nodes' restricted
unpickler admits no ``tests`` module by reference.
"""

import asyncio
from pathlib import Path

from hyperscale.core.jobs.models import WorkflowStatusUpdate
from hyperscale.distributed.models import ClientJobResult, WorkflowDispatch, WorkflowProgress
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.distributed.resources.resource_budget import ResourceBudget
from hyperscale.distributed.resources.workflow_resource_tracker import BYTES_PER_MEGABYTE
from hyperscale.graph import Workflow, step

from .child_context import ChildContext
from .job_dispatch_demo import SimPingWorkflow
from .workflow_lifecycle_demo import _env

WATCH_INTERVAL_SECONDS = 0.5
RAISED_MARKER = "sim adversary: the step raised"
# What a cancelled run's error names when the worker timed it out
# (``_stuck_workflows_to_cancel``) or the manager killed it for its
# memory (``_kill_workflow_for_resources``).
TIMEOUT_MARKER = "execution_timeout_exceeded"
MEMORY_KILL_MARKER = "resource budget exceeded"
# One VU: the adversary's whole footprint is one executor's work.
ADVERSARY_VUS = 1
HOG_WORKFLOW_NAME = "SimMemoryHogWorkflow"


def build_raising_workflow(duration_seconds: float) -> type[Workflow]:
    """A one-step ACTION workflow whose step raises ``RAISED_MARKER``."""

    class SimRaisingWorkflow(Workflow):
        vus = ADVERSARY_VUS
        duration = f"{duration_seconds:g}s"

        @step()
        async def raising_action(self) -> dict[str, str]:
            await asyncio.sleep(0.0)
            raise RuntimeError(RAISED_MARKER)

    return SimRaisingWorkflow


def build_never_ending_workflow(duration_seconds: float, tick_seconds: float) -> type[Workflow]:
    """A one-step ACTION workflow whose step never returns: it sleeps
    ``tick_seconds`` at a time forever and swallows every cancellation."""

    class SimNeverEndingWorkflow(Workflow):
        vus = ADVERSARY_VUS
        duration = f"{duration_seconds:g}s"

        @step()
        async def endless_action(self) -> dict[str, str]:
            while True:
                try:
                    await asyncio.sleep(tick_seconds)
                except asyncio.CancelledError:
                    continue

    return SimNeverEndingWorkflow


def build_memory_hog_workflow(duration_seconds: float, tick_seconds: float) -> type[Workflow]:
    """A one-step ACTION workflow that never returns on its own; its
    memory -- scripted at the worker (``observed_worker_entry``) -- grows
    for as long as it runs. Unlike the never-ending step it yields to
    cancellation, as an allocating loop that awaits does."""

    class SimMemoryHogWorkflow(Workflow):
        vus = ADVERSARY_VUS
        duration = f"{duration_seconds:g}s"

        @step()
        async def hoarding_action(self) -> dict[str, str]:
            while True:
                await asyncio.sleep(tick_seconds)

    return SimMemoryHogWorkflow


def guarded_manager_entry(
    context: ChildContext,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str,
) -> None:
    """Gateless manager child (AD-41 resource guards on, the default)
    recording ``("manager-started", t)``, and ``("resource-kill", t)`` /
    ``("resource-throttle", t)`` for every AD-41 action it orders."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    kill_workflow = manager._kill_workflow_for_resources
    throttle_workflow = manager._throttle_workflow_for_resources

    async def recorded_kill(*kill_arguments: object) -> bool:
        log.append(("resource-kill", round(context.loop.time(), 6)))
        return await kill_workflow(*kill_arguments)

    async def recorded_throttle(*throttle_arguments: object) -> bool:
        log.append(("resource-throttle", round(context.loop.time(), 6)))
        return await throttle_workflow(*throttle_arguments)

    manager._resource_enforcer._on_kill_workflow = recorded_kill
    manager._resource_enforcer._on_throttle_workflow = recorded_throttle

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def _script_hog_memory(
    context: ChildContext,
    worker: WorkerServer,
    growth_bytes_per_second: float,
) -> None:
    """Report the hog workflow's memory as ``growth_bytes_per_second``
    times the virtual seconds since the worker first saw it; every other
    workflow reports what its executors measured."""
    workflow_executor = worker._workflow_executor
    record_update_resources = workflow_executor._record_update_resources
    first_seen_at: dict[str, float] = {}

    def scripted_update_resources(
        dispatch: WorkflowDispatch,
        progress: WorkflowProgress,
        workflow_status_update: WorkflowStatusUpdate,
    ) -> None:
        if progress.workflow_name == HOG_WORKFLOW_NAME:
            started_at = first_seen_at.setdefault(dispatch.workflow_id, context.loop.time())
            grown_bytes = growth_bytes_per_second * (context.loop.time() - started_at)
            workflow_status_update.total_memory_usage_mb = grown_bytes / BYTES_PER_MEGABYTE
        record_update_resources(dispatch, progress, workflow_status_update)

    workflow_executor._record_update_resources = scripted_update_resources


def observed_worker_entry(
    context: ChildContext,
    host: str,
    tcp_port: int,
    udp_port: int,
    datacenter_id: str,
    seed_manager_address: tuple[str, int],
    total_cores: int,
    hog_growth_bytes_per_second: float,
) -> None:
    """Worker child recording ``("worker-state", active workflows, free
    cores, t)`` whenever either changes, with the hog's memory scripted
    to grow at ``hog_growth_bytes_per_second``."""
    worker = WorkerServer(
        host,
        tcp_port,
        udp_port,
        _env(WORKER_MAX_CORES=total_cores),
        dc_id=datacenter_id,
        seed_managers=[seed_manager_address],
        **context.sim_kwargs(),
        process_spawner=context,
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    _script_hog_memory(context, worker, hog_growth_bytes_per_second)

    async def watch() -> None:
        last_state: tuple[int, int] | None = None
        while True:
            state = (len(worker._active_workflows), worker._core_allocator.available_cores)
            if state != last_state:
                last_state = state
                log.append(("worker-state", *state, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(worker.start())
    context.loop.create_task(watch())


def _adversary_class(adversary_kind: str, duration_seconds: float, tick_seconds: float) -> type[Workflow]:
    builders = {
        "raises": lambda: build_raising_workflow(duration_seconds),
        "never-ends": lambda: build_never_ending_workflow(duration_seconds, tick_seconds),
        "exhausts-memory": lambda: build_memory_hog_workflow(duration_seconds, tick_seconds),
    }
    return builders[adversary_kind]()


def _memory_budget(max_memory_bytes: int | None) -> ResourceBudget | None:
    """The environment's default AD-41 budget with ``max_memory_bytes``
    as its memory limit; None (the manager's default) when not given."""
    if max_memory_bytes is None:
        return None
    default_budget = ResourceBudget.from_env(_env())
    return ResourceBudget(
        max_cpu_percent=default_budget.max_cpu_percent,
        max_memory_bytes=max_memory_bytes,
        warning_threshold=default_budget.warning_threshold,
        throttle_threshold=default_budget.throttle_threshold,
        kill_threshold=default_budget.kill_threshold,
        warning_grace_seconds=default_budget.warning_grace_seconds,
        kill_grace_seconds=default_budget.kill_grace_seconds,
    )


async def _submit_until_accepted(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    label: str,
    workflow: Workflow,
    job_timeout_seconds: float,
    resource_budget: ResourceBudget | None,
) -> str:
    """Submit until the manager accepts; each refusal is logged by type."""
    while True:
        try:
            return await client.submit_job(
                workflows=[([], workflow)],
                vus=workflow.vus,
                timeout_seconds=job_timeout_seconds,
                resource_budget=resource_budget,
            )
        except Exception as submit_error:
            log.append(("submit-rejected", label, type(submit_error).__name__, round(context.loop.time(), 6)))
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)


def _error_texts(result: ClientJobResult) -> str:
    """Every error text the job's result carries, joined."""
    return " | ".join(
        error_text
        for error_text in (
            result.error,
            *(workflow_result.error for workflow_result in result.workflow_results.values()),
        )
        if error_text
    )


async def _run_job(
    context: ChildContext,
    client: HyperscaleClient,
    log: list[tuple[object, ...]],
    label: str,
    workflow: Workflow,
    job_timeout_seconds: float,
    resource_budget: ResourceBudget | None,
) -> None:
    """Submit one job, await its terminal and record it: ``("job-errors",
    label, any error, names RAISED_MARKER, names TIMEOUT_MARKER, names
    MEMORY_KILL_MARKER)``."""
    job_id = await _submit_until_accepted(context, client, log, label, workflow, job_timeout_seconds, resource_budget)
    log.append(("job-submitted", label, round(context.loop.time(), 6)))
    result = await client.wait_for_job(job_id)
    log.append(("job-finished", label, result.status, round(context.loop.time(), 6)))
    errors = _error_texts(result)
    log.append(
        (
            "job-errors",
            label,
            bool(errors),
            RAISED_MARKER in errors,
            TIMEOUT_MARKER in errors,
            MEMORY_KILL_MARKER in errors,
        )
    )


def adversarial_client_entry(
    context: ChildContext,
    host: str,
    port: int,
    manager_tcp_address: tuple[str, int],
    adversary_kind: str,
    duration_seconds: float,
    tick_seconds: float,
    job_timeout_seconds: float,
    max_memory_bytes: int | None,
) -> None:
    """Client child: submit the ``adversary_kind`` workflow (``raises``,
    ``never-ends`` or ``exhausts-memory``; one VU; with an AD-41 memory
    limit of ``max_memory_bytes`` when given), await its terminal, then
    submit a ``SimPingWorkflow`` job (``follow-up``) and await that too.
    Records ``("job-submitted", label, t)``, ``("job-finished", label,
    status, t)`` and the ``job-errors`` row of ``_run_job``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list[tuple[object, ...]] = []
    context.set_result(log)
    adversary_class = _adversary_class(adversary_kind, duration_seconds, tick_seconds)

    async def run() -> None:
        await client.start()
        await _run_job(
            context, client, log, adversary_kind, adversary_class(), job_timeout_seconds,
            _memory_budget(max_memory_bytes),
        )
        await _run_job(context, client, log, "follow-up", SimPingWorkflow(), job_timeout_seconds, None)

    context.loop.create_task(run())
