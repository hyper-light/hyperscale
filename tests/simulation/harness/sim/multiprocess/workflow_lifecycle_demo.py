"""
A ``ManagerServer`` whose AD-54 workflow lifecycle is observed — the
picklable manager entry for scenarios the ``WorkflowLifecycleOracle``
judges.

``observe_workflow_lifecycle`` registers an observer on the manager's
``JobManager.workflow_lifecycle`` that records every published
transition — taken or refused, applied or installed — and a watcher
that records, on each change, how many lifecycle records the manager
holds, how many of them belong to no workflow, how many workflows'
``WorkflowInfo.status`` disagrees with their state's projection, how many
transitions were refused, and the dispatcher's per-job state.

Rows name a workflow by (job ordinal, workflow name) — the order the job
first appeared in this manager's lifecycle, and the class name — never by
job, workflow or node id, which are fresh per run; the log stays inside
the replay contract. Observing adds no suspension point: under SIM the
lifecycle's logs return at once and the observer appends synchronously,
so a transition's row lands in apply order.

``refusing_worker_entry`` is a worker that refuses every workflow it is
dispatched -- the workflow itself, never for capacity -- and
``budgeted_client_entry`` submits a job with a chosen per-workflow retry
budget (AD-44): together they drive a workflow to dispatch exhaustion.
``dag_cancelling_client_entry`` submits the K3 dependent pair and cancels
the job while its dependency runs. ``steady_client_entry`` submits one long
workflow of short actions -- steady progress for its whole duration.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os
from pathlib import Path

from hyperscale.core.engines.client.custom.custom_result import CustomResult
from hyperscale.distributed.env.env import Env
from hyperscale.distributed.models import JobCancellationComplete
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.manager.server import ManagerServer

from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.distributed.workflow import (
    WORKFLOW_STATUS_BY_WORKFLOW_STATE,
    StateTransition,
)
from hyperscale.graph import Workflow, step

from .l2_workload_demo import _build_dag_workflows, _build_sustained_workflow

_AUTH_SECRET = "sim-multiprocess-secret-00000000"
WATCH_INTERVAL_SECONDS = 0.5
# A refusal of the workflow itself: it carries none of the readiness
# markers (``READINESS_REJECTION_MARKERS``) a capacity refusal does.
SIM_REFUSAL = "sim refusal: this worker refuses the workflow itself"


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


def observe_workflow_lifecycle(context, manager: ManagerServer, log: list) -> None:
    """Record ``("lifecycle", job ordinal, workflow name, from, to,
    accepted, installed, t)`` for every transition the manager publishes,
    and ``(tag, count, t)`` whenever one of the watched counts changes:
    ``jobs``, ``lifecycle-records``, ``lifecycle-orphaned-records``,
    ``lifecycle-mismatched``, ``lifecycle-refused``,
    ``dispatcher-pending`` and ``dispatch-loops``."""
    job_manager = manager._job_manager
    lifecycle = job_manager.workflow_lifecycle
    job_ordinals: dict[str, int] = {}

    last_counts: dict[str, int] = {}

    def record_count_changes() -> None:
        workflows_with_records = 0
        mismatched_workflows = 0
        for job in job_manager.iter_jobs():
            for workflow in job.workflows.values():
                state = lifecycle.get_state(job.job_id, workflow.token.workflow_id or "")
                if state is not None:
                    workflows_with_records += 1
                if state is None or WORKFLOW_STATUS_BY_WORKFLOW_STATE[state] != workflow.status:
                    mismatched_workflows += 1

        dispatcher = manager._workflow_dispatcher
        counts = {
            "jobs": job_manager.job_count,
            "lifecycle-records": lifecycle.record_count,
            "lifecycle-orphaned-records": lifecycle.record_count - workflows_with_records,
            "lifecycle-mismatched": mismatched_workflows,
            "lifecycle-refused": lifecycle.rejected_transition_count,
            # Built at start, so absent before it.
            "dispatcher-pending": 0 if dispatcher is None else len(dispatcher._pending),
            "dispatch-loops": 0 if dispatcher is None else len(dispatcher._job_dispatch_tasks),
        }
        for tag, count in counts.items():
            if last_counts.get(tag) != count:
                last_counts[tag] = count
                log.append((tag, count, round(context.loop.time(), 6)))

    async def record_transition(transition: StateTransition) -> None:
        job = job_manager.get_job_by_id(transition.job_id)
        log.append(
            (
                "lifecycle",
                job_ordinals.setdefault(transition.job_id, len(job_ordinals)),
                next(
                    (
                        workflow.name
                        for workflow in (job.workflows.values() if job is not None else ())
                        if workflow.token.workflow_id == transition.workflow_id
                    ),
                    None,
                ),
                None if transition.from_state is None else transition.from_state.value,
                transition.to_state.value,
                transition.accepted,
                transition.installed,
                round(context.loop.time(), 6),
            )
        )
        # Counts change WITH transitions -- a dispatch loop exists while it
        # dispatches -- so they are read at each one too, not only on the
        # timer: a job done inside one watch interval is still seen.
        record_count_changes()

    lifecycle.register_observer(record_transition)

    async def watch_lifecycle() -> None:
        while True:
            record_count_changes()
            await asyncio.sleep(WATCH_INTERVAL_SECONDS)

    context.loop.create_task(watch_lifecycle())


def lifecycle_manager_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    env_overrides=None,
) -> None:
    """Manager child: a gateless (L1/L2) ``ManagerServer`` with its
    workflow lifecycle observed (see ``observe_workflow_lifecycle``), plus
    a ``("manager-started", t)`` milestone. ``env_overrides`` (a dict of
    ``Env`` fields) tunes the manager for the scenario."""
    manager = ManagerServer(
        host,
        tcp_port,
        udp_port,
        _env(**(env_overrides or {})),
        dc_id=datacenter_id,
        wal_data_dir=Path(f"/sim/{host}-{tcp_port}/ledger"),
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await manager.start()
        log.append(("manager-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())
    observe_workflow_lifecycle(context, manager, log)


def refusing_worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_address,
    total_cores,
) -> None:
    """Worker child that refuses every workflow dispatched to it: starting
    the workflow raises ``SIM_REFUSAL``, so the worker frees the cores it
    allocated and answers the dispatch refused with that error. Records
    ``("worker-started", t)`` and ``("dispatch-refused", t)`` per refusal."""
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
    log: list = []
    context.set_result(log)

    async def refuse_to_start(dispatch, address, allocation_result) -> bytes:
        log.append(("dispatch-refused", round(context.loop.time(), 6)))
        raise RuntimeError(SIM_REFUSAL)

    worker._handle_dispatch_execution = refuse_to_start

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def budgeted_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    retry_budget_per_workflow,
    workflow_duration_seconds,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit one ``SimL2SustainedWorkflow`` of
    ``workflow_duration_seconds`` (one VU) with a per-workflow retry budget
    of ``retry_budget_per_workflow`` and await the job's terminal.
    Records ``("job-submitted", t)``, ``("job-finished", status, t)`` and
    ``("failure-cause", spent_budget, refused)`` -- whether the job's
    errors name the spent retry budget and the workers' refusal (booleans:
    the texts carry per-run worker ids)."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    sustained_workflow_class = _build_sustained_workflow(workflow_duration_seconds, 1)

    async def run() -> None:
        await client.start()

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], sustained_workflow_class())],
                    vus=1,
                    timeout_seconds=job_timeout_seconds,
                    retry_budget_per_workflow=retry_budget_per_workflow,
                )
            except Exception as submit_error:
                # The exception TYPE only: rejection texts can embed
                # per-run values, and the log is part of the replay contract.
                log.append(
                    ("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6))
                )
                await asyncio.sleep(1.0)

        log.append(("job-submitted", round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))
        error_texts = " | ".join(
            error_text
            for error_text in (
                result.error,
                *(workflow_result.error for workflow_result in result.workflow_results.values()),
            )
            if error_text
        )
        log.append(("failure-cause", "retry budget" in error_texts, SIM_REFUSAL in error_texts))

    context.loop.create_task(run())


def dag_cancelling_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    long_a_seconds,
    short_b_seconds,
    cancel_after_running_seconds,
    job_timeout_seconds,
    observe_seconds,
) -> None:
    """Client child: submit the K3 dependent pair (``SimDagShortB`` waits on
    ``SimDagLongA``), cancel the job ``cancel_after_running_seconds`` after
    it is first seen running -- while A runs and B waits -- and record
    ``("cancel-response", accepted, t)``, every ``("cancellation-push",
    success, t)`` received and ``("job-finished", status, t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    long_a_class, short_b_class = _build_dag_workflows(long_a_seconds, short_b_seconds, 1)

    completion_handler = client._cancellation_complete_handler
    handle_push = completion_handler.handle

    async def counting_handle(address, data, clock_time):
        completion = JobCancellationComplete.load(data)
        log.append(("cancellation-push", completion.success, round(context.loop.time(), 6)))
        return await handle_push(address, data, clock_time)

    completion_handler.handle = counting_handle

    async def run() -> None:
        await client.start()

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], long_a_class()), (["SimDagLongA"], short_b_class())],
                    vus=1,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(
                    ("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6))
                )
                await asyncio.sleep(1.0)
        log.append(("job-submitted", round(context.loop.time(), 6)))

        while (job_result := client.get_job_status(job_id)) is None or job_result.status != "running":
            await asyncio.sleep(0.1)
        log.append(("running-seen", round(context.loop.time(), 6)))
        await asyncio.sleep(cancel_after_running_seconds)

        response = await client.cancel_job(job_id, reason="sim cancel")
        log.append(("cancel-response", response.success, round(context.loop.time(), 6)))

        result = await client.wait_for_job(job_id, timeout=observe_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))
        await asyncio.sleep(observe_seconds)
        log.append(("observed-until", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def _build_steady_workflow(workflow_duration_seconds: float) -> type[Workflow]:
    """Two VUs repeating a half-second action for the whole duration: the
    workflow completes actions -- makes progress -- every half second.

    The step returns a ``CustomResult``, which makes it a TEST hook: a TEST
    workflow's VUs repeat the step graph until the duration ends, where a
    step returning a plain value is an ACTION, run once. Both classes are
    built here so they are pickled by value -- the nodes' restricted
    unpickler admits no ``tests`` module by reference."""

    class SimLoadStepResult(CustomResult):
        @property
        def successful(self) -> bool:
            return True

    class SimSteadyWorkflow(Workflow):
        vus = 2
        duration = f"{workflow_duration_seconds}s"

        @step()
        async def steady_action(self) -> SimLoadStepResult:
            await asyncio.sleep(0.5)
            return SimLoadStepResult(timings={"total": 0.5})

    return SimSteadyWorkflow


def steady_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    workflow_duration_seconds,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit one ``SimSteadyWorkflow`` of
    ``workflow_duration_seconds`` and await the job's terminal. Records
    ``("job-submitted", t)`` and ``("job-finished", status, t)``."""
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    steady_workflow_class = _build_steady_workflow(workflow_duration_seconds)

    async def run() -> None:
        await client.start()

        job_id: str | None = None
        while job_id is None:
            try:
                job_id = await client.submit_job(
                    workflows=[([], steady_workflow_class())],
                    vus=2,
                    timeout_seconds=job_timeout_seconds,
                )
            except Exception as submit_error:
                log.append(
                    ("submit-rejected", type(submit_error).__name__, round(context.loop.time(), 6))
                )
                await asyncio.sleep(1.0)

        log.append(("job-submitted", round(context.loop.time(), 6)))
        result = await client.wait_for_job(job_id, timeout=wait_timeout_seconds)
        log.append(("job-finished", result.status, round(context.loop.time(), 6)))

    context.loop.create_task(run())
