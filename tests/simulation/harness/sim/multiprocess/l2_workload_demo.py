"""
L2 (gateless client -> manager -> worker) WORKLOAD entries — the
picklable children the K3/K4/L2/L3/L5 scenario families drive.

``job_dispatch_demo``'s entries submit a fixed 2s workflow and observe
only status transitions; the entries here extend that vocabulary for
workload-realism scenarios in the GATELESS topology:

* ``sustained_client_entry`` — submits ONE parameterizable-duration
  ACTION-chain workflow directly to a manager and additionally records
  the OBSERVED STATS trajectory (``total_completed``/``total_failed``),
  so push-stream stress scenarios (K4/L3) can assert the client's view
  is never wound back by late/reordered pushes racing polls.
* ``dag_client_entry`` — submits a DEPENDENT two-workflow DAG
  ``[([], SimDagLongA), (["SimDagLongA"], SimDagShortB)]`` (K3): the
  first SIM coverage of order-under-fault for the submission API's
  dependency surface.
* ``dag_worker_entry`` — ``worker_entry`` extended with PER-WORKFLOW
  execution milestones (workflow names are deterministic class names,
  not ids), so dependency ordering is assertable from the worker's own
  timeline: B must never start before A's terminal instant.

Workflows are deliberately ACTION chains (chained ``@step``s awaiting
parameterized virtual sleeps), NOT duration-governed TEST workflows: the
old ``WorkflowRunner._generate`` busy-waited ``asyncio.sleep(0)`` against
a frozen virtual clock at the duration boundary, so TEST workflows died
with ``SimulationConstraintError`` under SIM (see
``gate_fault_client_demo`` where this constraint was probed). Its
successor ``_run_long_lived_vu`` anchors frozen clocks with a timed
sleep -- not yet re-probed under SIM. The class ``duration`` is set to
match the summed sleeps so worker-side bookkeeping windows agree.

Milestones are ``(tag, value..., virtual_time)`` ONLY — never node
ids, snowflakes, or error text — so identical-seed runs compare equal:

* ``("submit-rejected", ExcName, t)`` — each refused submission
* ``("job-submitted", t)`` — acceptance
* ``("status-seen", status, t)`` — every observed status transition
* ``("stats-seen", total_completed, total_failed, t)`` — every
  observed change of the client's stats view (the wind-back oracle)
* ``("wait-timed-out", t)`` — ``wait_for_job`` deadline expiry; the
  entry keeps waiting UNBOUNDED so a late terminal still lands (LOUD,
  never silent)
* ``("job-finished", status, t)`` — result delivery (exactly once)
* ``("final-stats", total_completed, total_failed, t)`` — the result's
  stats at delivery
* ``("client-error", ExcName, t)`` — unexpected client-flow failure,
  recorded before re-raising (child logging is disabled under SIM)
* worker: ``("workflow-started", name, t)`` when a workflow name
  appears in the active set and ``("workflow-executed", name, t)``
  when it drains (final result sent — outcome-agnostic attempt
  evidence, the exact explicit-attempt vocabulary
  ``ClusterTraceOracle.check_workflow_execution`` consumes), sampled
  at 0.25s, plus the ``worker_entry``-compatible ``worker-started`` /
  ``manager-healthy`` / ``("workflows-active", count, t)`` milestones.
* worker: ``("workflow-deadline-armed", name, armed_at, timeout)``
  logged with a workflow's ``workflow-started`` row when the worker
  armed its local execution deadline — ``armed_at`` is the EXACT
  instant the stuck-workflow enforcement measures elapsed from (not
  the 0.25s-quantized sample instant), so deadline bounds are exact.

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname; the workflow classes are function-local
(cloudpickled by value across the submission path exactly as a user's
script-defined workflow travels).
"""

import asyncio
import os
import sys

import cloudpickle

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.distributed.nodes.worker.server import WorkerServer
from hyperscale.graph import Workflow, step

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


# By-value pickling for everything this module defines — the manager's
# restricted unpickler admits hyperscale.* by reference only, so a
# tests-tree workflow class must travel by value, reproducing the
# production user-script shape.
cloudpickle.register_pickle_by_value(sys.modules[__name__])


def _env(**overrides) -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET, **overrides)


# The sustained workflow's ACTION steps; each completes once, so a run of
# it completes exactly this many actions.
SUSTAINED_ACTION_STEP_COUNT = 2


def _build_sustained_workflow(
    workflow_duration_seconds: float, workflow_vus: int
) -> type[Workflow]:
    """Two chained ACTION steps, each awaiting half the requested
    duration on the virtual clock — the one-shot DAG pass keeps
    executors mid-run for the whole window, so fault windows provably
    intersect live execution (probed: client-visible completion lands
    at dispatch + duration + push latency)."""
    step_sleep_seconds = workflow_duration_seconds / 2.0

    class SimL2SustainedWorkflow(Workflow):
        vus = workflow_vus
        duration = f"{workflow_duration_seconds}s"

        @step()
        async def sustained_step_one(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"stage": "one"}

        @step("sustained_step_one")
        async def sustained_step_two(self) -> dict[str, str]:
            await asyncio.sleep(step_sleep_seconds)
            return {"stage": "two"}

    return SimL2SustainedWorkflow


def _build_dag_workflows(
    long_a_seconds: float, short_b_seconds: float, workflow_vus: int
) -> tuple[type[Workflow], type[Workflow]]:
    """The K3 dependent pair: ``SimDagShortB`` depends on
    ``SimDagLongA`` BY NAME through the submission API (the manager's
    ``WorkflowDispatcher`` resolves dependency names to workflow ids
    and holds dependents pending until the dependency completes).
    Names are fixed class names so replay comparisons and dependency
    resolution are both deterministic."""
    long_step_seconds = long_a_seconds / 2.0
    short_step_seconds = short_b_seconds / 2.0

    class SimDagLongA(Workflow):
        vus = workflow_vus
        duration = f"{long_a_seconds}s"

        @step()
        async def long_leg_one(self) -> dict[str, str]:
            await asyncio.sleep(long_step_seconds)
            return {"leg": "a-one"}

        @step("long_leg_one")
        async def long_leg_two(self) -> dict[str, str]:
            await asyncio.sleep(long_step_seconds)
            return {"leg": "a-two"}

    class SimDagShortB(Workflow):
        vus = workflow_vus
        duration = f"{short_b_seconds}s"

        @step()
        async def short_leg_one(self) -> dict[str, str]:
            await asyncio.sleep(short_step_seconds)
            return {"leg": "b-one"}

        @step("short_leg_one")
        async def short_leg_two(self) -> dict[str, str]:
            await asyncio.sleep(short_step_seconds)
            return {"leg": "b-two"}

    return SimDagLongA, SimDagShortB


def _run_observed_submission(
    context,
    client,
    log: list,
    workflows,
    workflow_vus: int,
    job_timeout_seconds: float,
    wait_timeout_seconds: float,
) -> None:
    """Shared submit -> watch(status+stats) -> await-completion flow.

    Identical await ordering across the client entries so pinned
    schedules stay byte-for-byte per entry. The stats watcher is the
    K4/L3 wind-back oracle's data source: every observed change of
    ``(total_completed, total_failed)`` is a milestone, so a late or
    duplicated push regressing the stats view would surface as a
    non-monotone ``stats-seen`` sequence.
    """

    async def run() -> None:
        try:
            await client.start()

            job_id: str | None = None
            while job_id is None:
                try:
                    job_id = await client.submit_job(
                        workflows=workflows,
                        vus=workflow_vus,
                        timeout_seconds=job_timeout_seconds,
                    )
                except Exception as submit_error:
                    # Production behavior: the manager rejects until it
                    # is leader with registered capacity — retry on
                    # virtual time. Type name only: rejection texts can
                    # embed per-run values.
                    log.append(
                        (
                            "submit-rejected",
                            type(submit_error).__name__,
                            round(context.loop.time(), 6),
                        )
                    )
                    await asyncio.sleep(1.0)

            log.append(("job-submitted", round(context.loop.time(), 6)))

            async def watch_status_and_stats() -> None:
                last_status: str | None = None
                last_stats: tuple[int, int] | None = None
                while True:
                    job_result = client.get_job_status(job_id)
                    if job_result is not None:
                        if job_result.status != last_status:
                            last_status = job_result.status
                            log.append(
                                (
                                    "status-seen",
                                    job_result.status,
                                    round(context.loop.time(), 6),
                                )
                            )
                        observed_stats = (
                            job_result.total_completed,
                            job_result.total_failed,
                        )
                        if observed_stats != last_stats:
                            last_stats = observed_stats
                            log.append(
                                (
                                    "stats-seen",
                                    observed_stats[0],
                                    observed_stats[1],
                                    round(context.loop.time(), 6),
                                )
                            )
                    await asyncio.sleep(0.5)

            status_watcher = context.loop.create_task(watch_status_and_stats())
            try:
                result = await client.wait_for_job(
                    job_id, timeout=wait_timeout_seconds
                )
            except asyncio.TimeoutError:
                # LOUD, then keep waiting: the simulation ceiling
                # bounds the run, and a late terminal must still reach
                # the log (the L2 poll-fallback convergence path).
                log.append(("wait-timed-out", round(context.loop.time(), 6)))
                result = await client.wait_for_job(job_id)
            finally:
                # Guarded: at process teardown the coordinator STOP can
                # close the loop while ``run`` is still parked on the
                # wait; cancelling against a closed loop raises inside
                # generator close (stderr noise, results already
                # collected) — skip it, the process is exiting.
                if not context.loop.is_closed():
                    status_watcher.cancel()
            log.append(
                ("job-finished", result.status, round(context.loop.time(), 6))
            )
            log.append(
                (
                    "final-stats",
                    result.total_completed,
                    result.total_failed,
                    round(context.loop.time(), 6),
                )
            )
        except asyncio.CancelledError:
            raise
        except Exception as client_error:
            # Child logging is disabled under SIM: without this entry
            # an unexpected failure would surface only as an opaque
            # never-retrieved task exception. Type name only (stable
            # across replays), then re-raise — never swallow.
            log.append(
                (
                    "client-error",
                    type(client_error).__name__,
                    round(context.loop.time(), 6),
                )
            )
            raise

    context.loop.create_task(run())


def sustained_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    workflow_duration_seconds,
    workflow_vus,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit one sustained ACTION-chain workflow of
    ``workflow_duration_seconds`` DIRECTLY to a manager (gateless
    L2 topology) and await completion with status + stats watchers.

    ``job_timeout_seconds`` is the explicit AD-34 job timeout the
    manager honors verbatim — sized per scenario so the INTENDED
    outcome (not an accidental timeout) decides the run.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    workflow_class = _build_sustained_workflow(
        workflow_duration_seconds, workflow_vus
    )
    _run_observed_submission(
        context,
        client,
        log,
        [([], workflow_class())],
        workflow_vus,
        job_timeout_seconds,
        wait_timeout_seconds,
    )


def dag_client_entry(
    context,
    host,
    port,
    manager_tcp_address,
    long_a_seconds,
    short_b_seconds,
    workflow_vus,
    job_timeout_seconds,
    wait_timeout_seconds,
) -> None:
    """Client child: submit the K3 dependent DAG
    ``[([], SimDagLongA), (["SimDagLongA"], SimDagShortB)]`` directly
    to a manager and await the WHOLE-JOB terminal.

    The dependency is by workflow NAME (the submission API's
    contract); the manager must hold B pending until A's completion
    signal even when cores for B are free the whole time.
    """
    client = HyperscaleClient(
        host=host,
        port=port,
        env=_env(),
        managers=[manager_tcp_address],
        **context.sim_kwargs(),
    )
    log: list = []
    context.set_result(log)
    long_a_class, short_b_class = _build_dag_workflows(
        long_a_seconds, short_b_seconds, workflow_vus
    )
    _run_observed_submission(
        context,
        client,
        log,
        [([], long_a_class()), (["SimDagLongA"], short_b_class())],
        workflow_vus,
        job_timeout_seconds,
        wait_timeout_seconds,
    )


def dag_worker_entry(
    context,
    host,
    tcp_port,
    udp_port,
    datacenter_id,
    seed_manager_address,
    total_cores,
    start_at=0.0,
) -> None:
    """Worker child: ``worker_entry`` extended with PER-WORKFLOW-NAME
    execution milestones — the K3 ordering oracle's data source.

    Samples the active-workflow set every 0.25s and logs
    ``("workflow-started", name, t)`` when a workflow name appears and
    ``("workflow-executed", name, t)`` when it drains (final result
    sent) — the drain row is outcome-agnostic ATTEMPT evidence, in the
    explicit vocabulary ``ClusterTraceOracle.check_workflow_execution``
    counts. Names come from the dispatch's deterministic workflow
    class name (``_workflow_id_to_name``), never ids, so replay
    comparisons hold. Same-sample transitions log in sorted-name order
    for determinism.
    """
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

    async def run() -> None:
        await worker.start()
        log.append(("worker-started", round(context.loop.time(), 6)))

        while not worker._registry._healthy_manager_ids:
            await asyncio.sleep(0.5)
        log.append(("manager-healthy", round(context.loop.time(), 6)))

    async def watch_workflows() -> None:
        last_active_count = -1
        last_named_active: set[str] = set()
        while True:
            active_count = len(worker._active_workflows)
            if active_count != last_active_count:
                last_active_count = active_count
                log.append(
                    (
                        "workflows-active",
                        active_count,
                        round(context.loop.time(), 6),
                    )
                )

            workflow_ids_by_name = {
                workflow_name: workflow_id
                for workflow_id in worker._active_workflows
                if (
                    workflow_name := (
                        worker._worker_state._workflow_id_to_name.get(
                            workflow_id
                        )
                    )
                )
                is not None
            }
            named_active = set(workflow_ids_by_name)
            for workflow_name in sorted(named_active - last_named_active):
                log.append(
                    (
                        "workflow-started",
                        workflow_name,
                        round(context.loop.time(), 6),
                    )
                )
                started_workflow_id = workflow_ids_by_name[workflow_name]
                if (
                    deadline_armed_at := worker._worker_state._workflow_start_times.get(
                        started_workflow_id
                    )
                ) is not None:
                    log.append(
                        (
                            "workflow-deadline-armed",
                            workflow_name,
                            round(deadline_armed_at, 6),
                            worker._worker_state._workflow_timeout_seconds[
                                started_workflow_id
                            ],
                        )
                    )
            for workflow_name in sorted(last_named_active - named_active):
                log.append(
                    (
                        "workflow-executed",
                        workflow_name,
                        round(context.loop.time(), 6),
                    )
                )
            last_named_active = named_active
            await asyncio.sleep(0.25)

    if start_at > 0.0:
        context.loop.call_at(start_at, lambda: context.loop.create_task(run()))
    else:
        context.loop.create_task(run())
    context.loop.create_task(watch_workflows())
