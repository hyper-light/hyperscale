import asyncio
import copy
import inspect
import math
import warnings
from collections import defaultdict, deque
from types import MethodType
from typing import (
    Any,
    Awaitable,
    Callable,
    Coroutine,
    Dict,
    List,
    Literal,
    Set,
    Tuple,
)

import networkx
import psutil

from hyperscale.core.engines.client import TimeParser
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.models.env import Env
from hyperscale.core.jobs.models.workflow_run_control import WorkflowRunControl
from hyperscale.core.jobs.models.workflow_status import WorkflowStatus
from hyperscale.core.utils.cancel_and_release_task import cancel_and_release_task
from hyperscale.core.monitoring import CPUMonitor, MemoryMonitor
from hyperscale.core.state import Context, ContextHook, StateAction
from hyperscale.core.state.workflow_context import WorkflowContext
from hyperscale.core.testing.models.base import OptimizedArg
from hyperscale.logging import Entry, Logger, LogLevel
from hyperscale.logging.hyperscale_logging_models import (
    RunDebug,
    RunError,
    RunFatal,
    RunInfo,
    RunTrace,
)
from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.reporting.results import Results

from .completion_counter import CompletionCounter

StepStatsType = Literal[
    "total",
    "ok",
    "err",
]

# What a run returns: its run id, results, context, error, and terminal status.
RunOutcome = Tuple[
    int,
    WorkflowStats | Dict[str, Any | Exception] | None,
    WorkflowContext | None,
    Exception | None,
    WorkflowStatus,
]


warnings.simplefilter("ignore")

async def guard_optimize_call(optimize_call: Coroutine[Any, Any, None]):
    try:
        await optimize_call

    except Exception:
        pass


# Frozen-clock anchor tuning for the VUs. The spin threshold is the
# count of CONSECUTIVE iterations of a VU whose ``loop.time()`` reading
# was bit-identical before a timed sleep is injected: a real event loop
# can never hold one reading across ten thousand iterations (each costs
# microseconds against nanosecond-resolution clocks), so on real hosts
# the anchor never fires and the VUs keep their exact maximum-throughput
# hot path; under a virtual (timer-driven) clock the reading is frozen
# by construction whenever no timer is pending, the threshold trips,
# and the 1ms anchor gives the clock a timer to advance on instead of
# spinning at one instant forever.
_FROZEN_CLOCK_SPINS: int = 10_000
_FROZEN_CLOCK_ANCHOR_SECONDS: float = 0.001


class WorkflowRunner:
    def __init__(
        self,
        env: Env,
        worker_id: int,
        node_id: int,
        *,
        monitors_enabled: bool = True,
        deterministic_step_order: bool = False,
    ) -> None:
        # Phase 6 SIM seam. The per-run CPU/memory monitors sample the
        # real host through ``run_in_executor`` — banned on the
        # ``SimulationLoop`` and inherently non-deterministic — so the
        # owning ``RemoteGraphController`` disables them when it runs
        # under a simulation transport (the same treatment logging and
        # the worker-node telemetry get). ``True`` in REAL mode:
        # behavior is byte-identical.
        self._monitors_enabled = monitors_enabled

        # SIM seam. Replay compares serialized results byte for byte, so
        # under a simulation transport each layer's steps run and are
        # consumed in step-name order (see ``_run_long_lived_vu`` and
        # ``_execute_non_test_workflow``). ``False`` in REAL mode, where
        # the order carries no meaning: no per-request sorting.
        self._deterministic_step_order = deterministic_step_order

        self._worker_id = worker_id

        self._logfile = f"hyperscale.worker.{self._worker_id}.log.json"
        if worker_id is None:
            self._logfile = "hyperscale.leader.log.json"

        self._node_id = node_id
        self.run_statuses: Dict[int, Dict[str, WorkflowStatus]] = defaultdict(dict)

        # AD-41 THROTTLE, per workflow: the slots its VUs hold (one per VU
        # in an iteration), the VUs waiting in line for one, and the cap
        # on slots -- the worker's VUs until a throttle cuts it.
        self._active: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._active_waiters: Dict[int, Dict[str, deque[asyncio.Future[None]]]] = defaultdict(dict)
        self._max_active: Dict[int, Dict[str, int]] = defaultdict(dict)
        # AD-41 THROTTLE: the concurrency cap a throttled workflow had
        # before its first throttle (restored on release), and which
        # workflows run concurrency-gated (TEST) VUs at all.
        self._throttle_base_cap: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._concurrency_gated: Dict[int, set[str]] = defaultdict(set)
        self._run_tasks: Dict[int, Dict[str, asyncio.Future]] = defaultdict(dict)
        self._threads = psutil.cpu_count(logical=False)
        self._workflows_sem: asyncio.Semaphore | None = None
        self._workflow_hooks: Dict[int, Dict[str, List[str]]] = defaultdict(dict)
        self._duplicate_job_policy: Literal["reject", "replace"] = (
            env.MERCURY_SYNC_DUPLICATE_JOB_POLICY
        )

        self._workflow_step_stats: Dict[
            int,
            Dict[
                tuple[str, str],
                Dict[
                    StepStatsType,
                    CompletionCounter,
                ],
            ],
        ] = defaultdict(dict)

        self._max_running_workflows = env.MERCURY_SYNC_MAX_RUNNING_WORKFLOWS
        self._max_pending_workflows = env.MERCURY_SYNC_MAX_PENDING_WORKFLOWS
        self._run_check_lock: asyncio.Lock | None = None
        self._completed_counts: Dict[int, Dict[str, CompletionCounter]] = defaultdict(
            dict
        )
        self._failed_counts: Dict[int, Dict[str, CompletionCounter]] = defaultdict(dict)
        self._running_workflows: Dict[int, Dict[str, Workflow]] = defaultdict(dict)

        self._cpu_monitor = CPUMonitor(env)
        self._memory_monitor = MemoryMonitor(env)
        self._logger = Logger()

        # Each registered submission of a workflow's run on this node (see
        # register_run), as cancellation reaches it: by run id and
        # workflow. More than one when a run is resubmitted to the node.
        self._run_controls: Dict[int, Dict[str, set[WorkflowRunControl]]] = defaultdict(dict)

    def setup(self):
        if self._workflows_sem is None:
            self._workflows_sem = asyncio.Semaphore(self._max_running_workflows)

        if self._run_check_lock is None:
            self._run_check_lock = asyncio.Lock()

        self._clear()

    def register_run(self, run_id: int, workflow_name: str) -> WorkflowRunControl:
        """
        Register a submission of the workflow's run on this node -- before
        anything can ask to cancel it -- and return its control, for
        ``run``. Cancellation reaches it by run id and workflow from then
        on, and the run's stats stay readable until ``release_run``.
        """
        control = WorkflowRunControl()
        self._run_controls[run_id].setdefault(workflow_name, set()).add(control)
        return control

    def release_run(self, run_id: int, workflow_name: str, control: WorkflowRunControl) -> None:
        """
        Drop a submission ``register_run`` registered, once nothing reads
        its run's stats. With the workflow's last submission in the run
        gone, so is everything kept for it: its status, counts, step
        stats and hooks, its slots, and its CPU and memory samples.
        """
        run_controls = self._run_controls.get(run_id, {})
        if (controls := run_controls.get(workflow_name)) is None:
            return

        controls.discard(control)
        if controls:
            return

        del run_controls[workflow_name]
        if not run_controls:
            del self._run_controls[run_id]

        for run_states in (
            self.run_statuses,
            self._completed_counts,
            self._failed_counts,
            self._workflow_hooks,
            self._active,
            self._max_active,
            self._run_tasks,
            self._running_workflows,
        ):
            if (workflow_states := run_states.get(run_id)) is not None:
                workflow_states.pop(workflow_name, None)
                if not workflow_states:
                    del run_states[run_id]

        if (step_stats := self._workflow_step_stats.get(run_id)) is not None:
            for stats_key in [stats_key for stats_key in step_stats if stats_key[0] == workflow_name]:
                del step_stats[stats_key]

            if not step_stats:
                del self._workflow_step_stats[run_id]

        self._cpu_monitor.release_run(run_id, workflow_name)
        self._memory_monitor.release_run(run_id, workflow_name)

    async def await_cancellation(self, run_id: int, workflow_name: str) -> None:
        """
        Wait for every registered submission of the workflow's run to end,
        cancelled, failed or finished: at once when none is registered
        (each ended and was released, or none was made).
        """
        await asyncio.gather(
            *[control.ended.wait() for control in self._run_controls.get(run_id, {}).get(workflow_name, ())]
        )

    def request_cancellation(self, run_id: int, workflow_name: str) -> bool:
        """
        Request graceful cancellation of the workflow's run on this node.

        The VUs of each registered submission of it check its control
        before they start an iteration: none starts a new one. Iterations
        already under way complete normally, and the standard cleanup path
        runs without throwing exceptions. Other runs are untouched. False
        when no submission is registered: the run already ended, or none
        was made.

        NOTE: only the duration-governed (TEST) execution path consults
        this flag. ACTION workflows run their step DAG with plain awaits
        and finish at natural length regardless — the graceful phase of
        cancellation is a no-op for them. ``hard_cancel`` is the
        escalation that actually stops in-flight execution when the
        graceful window expires.
        """
        controls = self._run_controls.get(run_id, {}).get(workflow_name, ())
        for control in controls:
            control.running = False

        return bool(controls)

    def hard_cancel(self, run_id: int, workflow_name: str) -> bool:
        """
        Hard-stop a running workflow after its graceful window expired.

        Cancels the run task (CancelledError propagates to whoever is
        awaiting the run — the worker's executor already maps it to the
        CANCELLED terminal — and each executor cancels its VUs or steps
        as it unwinds), cancels the VUs waiting in line on a throttle,
        clears the pacing state (the ``replace`` duplicate-policy
        cleanup, which this mirrors), and — because a run's end is
        normally marked only at the END of natural execution — marks each
        registered submission of it ended, so ``await_cancellation``
        observers converge instead of waiting on an execution that will
        never finish (the run's stop event is set as the cancellation
        unwinds ``run``). Other runs are untouched. Idempotent; returns
        True when a live run task was actually cancelled.

        Without this escalation, "cancelling" an ACTION workflow meant
        waiting for it to complete: a hard-timed-out 100s workflow kept
        its cores busy for the full 100s (measured: 55.5 virtual seconds
        of zombie execution past the client's timeout terminal, with the
        manager re-cancelling on a ~6s cadence the whole way).
        """
        controls = self._run_controls.get(run_id, {}).get(workflow_name, ())
        for control in controls:
            control.running = False

        cancelled_live_task = False
        run_task = self._run_tasks.get(run_id, {}).get(workflow_name)
        if run_task is not None and not run_task.done():
            run_task.cancel()
            cancelled_live_task = True

        if run_id in self._active and workflow_name in self._active[run_id]:
            self._active[run_id][workflow_name] = 0
        if (parked := self._active_waiters.get(run_id, {}).pop(workflow_name, None)) is not None:
            for parked_vu in parked:
                parked_vu.cancel()
            parked.clear()
        if self._running_workflows.get(run_id, {}).get(workflow_name):
            del self._running_workflows[run_id][workflow_name]

        for control in controls:
            control.ended.set()

        return cancelled_live_task

    def throttle_workflow(self, run_id: int, workflow_name: str, scale: float) -> int | None:
        """AD-41 THROTTLE: cut a running workflow's concurrency to ``scale``
        of its current operating point (the lower of its cap and the VUs
        actually in an iteration), never below one; repeated throttles
        compound. A VU in an iteration finishes it, and one starts an
        iteration only while fewer than the cap are under way.

        Returns the new cap, or None when the workflow runs no
        concurrency-gated VUs (not running, or an ACTION workflow, whose
        steps run once) -- there is nothing to throttle.
        """
        if not 0.0 < scale <= 1.0:
            raise ValueError(f"throttle scale must be in (0, 1], got {scale}")
        if workflow_name not in self._concurrency_gated.get(run_id, ()):
            return None
        caps = self._max_active[run_id]
        self._throttle_base_cap[run_id].setdefault(workflow_name, caps[workflow_name])
        in_flight = self._active.get(run_id, {}).get(workflow_name, 0)
        operating_point = min(caps[workflow_name], in_flight) if in_flight > 0 else caps[workflow_name]
        caps[workflow_name] = max(1, math.floor(operating_point * scale))
        return caps[workflow_name]

    def release_workflow_throttle(self, run_id: int, workflow_name: str) -> bool:
        """Restore a throttled workflow's original cap and hand the slots it
        frees to the VUs waiting in line for one. False when it was not
        throttled."""
        base_cap = self._throttle_base_cap.get(run_id, {}).pop(workflow_name, None)
        if base_cap is None:
            return False
        if workflow_name in self._concurrency_gated.get(run_id, ()):
            self._max_active[run_id][workflow_name] = base_cap
        # First come, first served: each freed slot goes to the VU that has
        # waited longest (one cancelled while waiting is passed over), as a
        # VU ending an iteration hands its own on (see _run_long_lived_vu).
        if parked := self._active_waiters.get(run_id, {}).get(workflow_name):
            active = self._active[run_id]
            while parked and active[workflow_name] < base_cap:
                if not (parked_vu := parked.popleft()).done():
                    active[workflow_name] += 1
                    parked_vu.set_result(None)
        return True

    def _end_concurrency_gate(self, run_id: int, workflow_name: str) -> None:
        self._concurrency_gated[run_id].discard(workflow_name)
        if not self._concurrency_gated[run_id]:
            del self._concurrency_gated[run_id]
        self._throttle_base_cap.get(run_id, {}).pop(workflow_name, None)
        if run_id in self._throttle_base_cap and not self._throttle_base_cap[run_id]:
            del self._throttle_base_cap[run_id]
        # The run's VUs have ended or are cancelled: none waits in line, and
        # none hands a slot on.
        if (parked := self._active_waiters.get(run_id, {}).pop(workflow_name, None)) is not None:
            parked.clear()
        if run_id in self._active_waiters and not self._active_waiters[run_id]:
            del self._active_waiters[run_id]

    @property
    def pending(self):
        return len(
            [
                status
                for workflow_statuses in self.run_statuses.values()
                for status in workflow_statuses.values()
                if status == WorkflowStatus.PENDING
            ]
        )

    def get_running_workflow_stats(
        self,
        run_id: int,
        workflow: str,
    ) -> Tuple[WorkflowStatus, int, int, Dict[str, Dict[StepStatsType, int]]] | None:
        """
        The run's status, completed and failed counts and per-step stats;
        None when no submission of it is registered -- once released (see
        ``release_run``), nothing is kept to report.
        """
        if not self._run_controls.get(run_id, {}).get(workflow):
            return None

        status = self.run_statuses.get(
            run_id,
            {},
        ).get(workflow, WorkflowStatus.UNKNOWN)

        workflow_hooks = self._workflow_hooks[run_id].get(workflow, [])

        worklow_hook_stats: Dict[str, Dict[StepStatsType, int]] = {
            hook: {"total": 0, "ok": 0, "err": 0} for hook in workflow_hooks
        }

        completed_counter = self._completed_counts[run_id].get(
            workflow, CompletionCounter()
        )
        completed_count = completed_counter.value()

        failed_counter = self._failed_counts[run_id].get(
            workflow, CompletionCounter()
        )
        failed_count = failed_counter.value()

        for (workflow_name, hook_name), stats in self._workflow_step_stats[
            run_id
        ].items():
            if workflow_name == workflow:
                worklow_hook_stats[hook_name] = {
                    "total": stats["total"].value(),
                    "ok": stats["ok"].value(),
                    "err": stats["err"].value(),
                }

        return (
            status,
            completed_count,
            failed_count,
            worklow_hook_stats,
        )

    def get_system_stats(
        self,
        run_id: int,
        workflow_name: str,
    ):
        return (
            self._cpu_monitor.get_moving_avg(
                run_id,
                workflow_name,
            ),
            self._memory_monitor.get_moving_avg(
                run_id,
                workflow_name,
            ),
        )

    async def run(
        self,
        run_id: int,
        workflow: Workflow,
        workflow_context: Dict[str, Any],
        vus: int,
        await_start: Callable[[], Awaitable[None]] | None = None,
        stop_event: asyncio.Event | None = None,
        control: WorkflowRunControl | None = None,
    ) -> RunOutcome:
        """
        Run the workflow on this node and return its results, context,
        error, and terminal status.

        ``await_start``, when given, is awaited once the run is set up and
        before it executes: the node's start gate, which lets every node
        running the workflow start together however long each one's
        setup took.

        ``stop_event``, when given, is this submission's own stop signal:
        set as the run's load generation ends -- or, however else the
        submission ends (rejected, failed, cancelled), when it does.

        ``control``, when given, is this submission's registration (see
        ``register_run``), made before anything could ask to cancel it;
        its caller releases it. Without one, the run registers itself
        and is released as it ends.
        """
        registered_here = control is None
        if control is None:
            control = self.register_run(run_id, workflow.name)

        try:
            return await self._run(
                run_id,
                workflow,
                workflow_context,
                vus,
                await_start,
                stop_event,
                control,
            )

        finally:
            control.ended.set()
            if stop_event is not None:
                stop_event.set()

            if registered_here:
                self.release_run(run_id, workflow.name, control)

    async def _run(
        self,
        run_id: int,
        workflow: Workflow,
        workflow_context: Dict[str, Any],
        vus: int,
        await_start: Callable[[], Awaitable[None]] | None,
        stop_event: asyncio.Event | None,
        control: WorkflowRunControl,
    ) -> RunOutcome:
        default_config = {
            "node_id": self._node_id,
            "workflow": workflow.name,
            "run_id": run_id,
            "workflow_vus": workflow.vus,
            "duration": workflow.duration,
        }

        workflow_slug = workflow.name.lower()

        self._logger.configure(
            name=f"{workflow_slug}_{run_id}_logger",
            path=self._logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
            models={
                "trace": (RunTrace, default_config),
                "debug": (
                    RunDebug,
                    default_config,
                ),
                "info": (
                    RunInfo,
                    default_config,
                ),
                "error": (
                    RunError,
                    default_config,
                ),
                "fatal": (
                    RunFatal,
                    default_config,
                ),
            },
        )

        async with self._logger.context(
            name=f"{workflow_slug}_{run_id}_logger",
        ) as ctx:
            # Held for exactly the check: however it exits -- a rejection,
            # an error, a cancellation -- the next run's check can begin.
            async with self._run_check_lock:
                await ctx.log_prepared(
                    message=f"Run {run_id} of Workflow {workflow.name} entering pre-pnding execution check",
                    name="info",
                )

                already_running = (
                    self.run_statuses[run_id].get(workflow.name) == WorkflowStatus.RUNNING
                )

                if self.pending >= self._max_pending_workflows:
                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} failed to start due to exceeded max pending workflow limit of {self._max_pending_workflows}",
                        name="error",
                    )

                    return (
                        run_id,
                        None,
                        None,
                        Exception("Err. - Run rejected. Too many pending workflows."),
                        WorkflowStatus.REJECTED,
                    )

                elif already_running and self._duplicate_job_policy == "reject":
                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} failed to start due to workflow already being in running stats with duplicate job policy of REJECT",
                        name="error",
                    )

                    return (
                        run_id,
                        None,
                        None,
                        Exception("Err. - Run rejected. Already running."),
                        WorkflowStatus.REJECTED,
                    )

                elif already_running and self._duplicate_job_policy == "replace":
                    workflow_name = workflow.name

                    self._active[run_id][workflow_name] = 0

                    if self._run_tasks[run_id].get(workflow_name):
                        self._run_tasks[run_id][workflow_name].cancel()
                        await asyncio.sleep(0)

                    if self._active_waiters[run_id].get(workflow_name):
                        del self._active_waiters[run_id][workflow_name]

                    # Absent when the running one is not set up yet.
                    self._max_active[run_id].pop(workflow_name, None)

                    if self._running_workflows[run_id].get(workflow.name):
                        del self._running_workflows[run_id][workflow.name]

                await ctx.log_prepared(
                    message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.PENDING.name} state",
                    name="info",
                )

            self.run_statuses[run_id][workflow.name] = WorkflowStatus.PENDING

            async with self._workflows_sem:
                workflow_name = workflow.name

                await ctx.log_prepared(
                    message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.CREATED.name} state",
                    name="info",
                )

                context = Context()

                run_task: asyncio.Future | None = None
                try:
                    # Inside the try: however the run ends, its monitors
                    # stop (finally, below).
                    if self._monitors_enabled:
                        await self._cpu_monitor.start_background_monitor(
                            run_id, workflow_name
                        )
                        await self._memory_monitor.start_background_monitor(
                            run_id, workflow_name
                        )

                    self.run_statuses[run_id][workflow_name] = WorkflowStatus.CREATED

                    await asyncio.gather(
                        *[
                            context.update(workflow_name, hook_name, value)
                            for hook_name, value in workflow_context.items()
                        ]
                    )

                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.RUNNING.name} state",
                        name="info",
                    )

                    self.run_statuses[run_id][workflow.name] = WorkflowStatus.RUNNING

                    self._running_workflows[run_id][workflow.name] = workflow

                    run_task = asyncio.ensure_future(
                        self._run_workflow(
                            run_id,
                            workflow,
                            context,
                            vus,
                            await_start,
                            stop_event,
                            control,
                        )
                    )
                    self._run_tasks[run_id][workflow_name] = run_task

                    (results, updated_context) = await run_task

                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} successfully halted run",
                        name="info",
                    )

                    await asyncio.gather(
                        *[
                            context.update(workflow_name, hook_name, value)
                            for workflow_name, hook_context in updated_context.iter_workflow_contexts()
                            for hook_name, value in hook_context.items()
                        ]
                    )

                    workflow_name = workflow.name

                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} clearing run context",
                        name="info",
                    )

                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.COMPLETED.name} state",
                        name="info",
                    )

                    self.run_statuses[run_id][workflow_name] = WorkflowStatus.COMPLETED

                    return (
                        run_id,
                        results,
                        updated_context[workflow_name],
                        None,
                        WorkflowStatus.COMPLETED,
                    )

                except asyncio.CancelledError:
                    # Hard-cancelled (hard_cancel cancelled the run task),
                    # or cancelled with whatever awaited the run.
                    self.run_statuses[run_id][workflow_name] = WorkflowStatus.CANCELLED
                    raise

                except Exception as err:
                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} encountered error {str(err)}",
                        name="error",
                    )

                    self.run_statuses[run_id][workflow.name] = WorkflowStatus.FAILED

                    return (
                        run_id,
                        None,
                        context[workflow_name],
                        err,
                        WorkflowStatus.FAILED,
                    )

                finally:
                    # However the run ended -- completed, failed or
                    # cancelled -- its monitors stop: a cancelled run's
                    # used to sample until the runner closed.
                    if self._monitors_enabled:
                        await self._cpu_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )
                        await self._memory_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )

                    # However the run ended (completed, failed, cancelled,
                    # replaced), release it: its workflow and finished task
                    # leave the runner and its engine clients close.
                    # Previously a finished run's workflow stayed in
                    # _running_workflows and its connections stayed open
                    # for the executor's lifetime.
                    try:
                        self._release_workflow(run_id, workflow, run_task)
                    except ExceptionGroup as close_errors:
                        await ctx.log_prepared(
                            message=(
                                f"Run {run_id} of Workflow {workflow.name} failed to "
                                f"close its engine clients: {close_errors!r}"
                            ),
                            name="error",
                        )

    def _release_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        run_task: asyncio.Future | None,
    ) -> None:
        """Drop this run's workflow and run task from the runner -- only
        if they are still THIS run's (a ``replace`` may already have
        registered a successor under the same name) -- and close this
        run's engine clients. Close failures surface as an
        ExceptionGroup after every client was attempted."""
        running_workflows = self._running_workflows.get(run_id)
        if running_workflows is not None and running_workflows.get(workflow.name) is workflow:
            del running_workflows[workflow.name]
        run_tasks = self._run_tasks.get(run_id)
        if run_tasks is not None and run_task is not None and run_tasks.get(workflow.name) is run_task:
            del run_tasks[workflow.name]
        workflow.client.close()

    async def _run_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
        vus: int,
        await_start: Callable[[], Awaitable[None]] | None,
        stop_event: asyncio.Event | None,
        control: WorkflowRunControl,
    ) -> Tuple[
        WorkflowStats
        | Dict[
            str,
            Any | Exception,
        ],
        Context,
    ]:
        workflow_slug = workflow.name.lower()

        async with self._logger.context(
            name=f"{workflow_slug}_{run_id}_logger",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Run {run_id} of Workflow {workflow.name} setting actions and context",
                name="debug",
            )

            state_actions = self._setup_state_actions(workflow)
            context = await self._use_context(
                workflow.name,
                state_actions,
                context,
            )

            await ctx.log_prepared(
                message=f"Run {run_id} of Workflow {workflow.name} creating traversal order and optimizations",
                name="debug",
            )

            (
                workflow,
                hooks,
                traversal_order,
                config,
            ) = await self._setup(
                run_id,
                workflow,
                context,
                vus,
            )

            is_test_workflow = (
                len(
                    [hook for hook in hooks.values() if hook.hook_type == HookType.TEST]
                )
                > 0
            )

            if is_test_workflow:
                await ctx.log_prepared(
                    message=f"Run {run_id} of test Workflow {workflow.name} beginning execution",
                    name="debug",
                )

                self._concurrency_gated[run_id].add(workflow.name)
                # Where its VUs wait in line on a throttle (see
                # _run_long_lived_vu), until the gate ends.
                self._active_waiters[run_id][workflow.name] = deque()
                try:
                    # Inside the concurrency gate: a throttle that arrives
                    # while the run waits to start applies once it does.
                    if await_start is not None:
                        await await_start()

                    results = await self._execute_test_workflow(
                        run_id,
                        workflow,
                        traversal_order,
                        hooks,
                        context,
                        config,
                        stop_event,
                        control,
                    )

                finally:
                    self._end_concurrency_gate(run_id, workflow.name)

            else:
                await ctx.log_prepared(
                    message=f"Run {run_id} of non test Workflow {workflow.name} beginning execution",
                    name="debug",
                )

                if await_start is not None:
                    await await_start()

                results = await self._execute_non_test_workflow(
                    run_id,
                    workflow,
                    traversal_order,
                    context,
                    config,
                    stop_event,
                )

            await ctx.log_prepared(
                message=f"Run {run_id} of Workflow {workflow.name} completed execution and updated context",
                name="debug",
            )

            context = await self._provide_context(
                workflow.name,
                state_actions,
                context,
                results,
            )

            return (results, context)

    def _setup_state_actions(self, workflow: Workflow) -> Dict[str, ContextHook]:
        # This run's own copies of the workflow class's state hooks, read
        # from the class (an instance set up before holds bound calls in
        # their place): the class's are shared by every instance -- and by
        # any run of the class at the same time -- so a run binds and
        # fills only its own.
        state_actions: Dict[str, ContextHook] = {
            name: copy.copy(hook)
            for name, hook in inspect.getmembers(
                type(workflow),
                predicate=lambda member: isinstance(member, ContextHook),
            )
        }

        for action in state_actions.values():
            # Bound to this run's workflow from the unbound function:
            # binding a method already bound to another instance keeps that
            # instance from Python 3.14 (3.12 rebinds).
            action._call = MethodType(
                action._call.__func__ if isinstance(action._call, MethodType) else action._call,
                workflow,
            )
            setattr(workflow, action.name, action._call)

        return state_actions

    async def _use_context(
        self,
        workflow: str,
        state_actions: Dict[str, ContextHook],
        context: Context,
    ):
        use_actions = [
            action
            for action in state_actions.values()
            if action.action_type == StateAction.USE
        ]

        if len(use_actions) < 1:
            return context

        for hook in use_actions:
            hook.context_args = {
                name: value
                for provider in hook.workflows
                for name, value in context[provider].items()
            }

        resolved = await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in use_actions]
        )

        await asyncio.gather(
            *[context[workflow].set(hook_name, value) for hook_name, value in resolved]
        )

        return context

    async def _setup(
        self,
        run_id: int,
        workflow: Workflow,
        context: Context,
        vus: int,
    ) -> Tuple[
        Workflow,
        Dict[str, Hook],
        List[
            Dict[
                str,
                Hook,
            ]
        ],
        Dict[str, Any],
    ]:
        workflow_slug = workflow.name.lower()
        async with self._logger.context(
            name=f"{workflow_slug}_{run_id}_logger",
        ) as ctx:
            await ctx.log_prepared(
                message=f"Run {run_id} of Workflow {workflow.name} executing for {workflow.duration} with {vus} VUs and timeout of {workflow.timeout}",
                name="debug",
            )

            self._active[run_id][workflow.name] = 0
            self._completed_counts[run_id][workflow.name] = CompletionCounter()
            self._failed_counts[run_id][workflow.name] = CompletionCounter()

            # This run's own copies of the workflow class's hooks, read from
            # the class (as the state hooks are): the class's are shared by
            # every instance -- and by any run of the class at the same time
            # -- so a run binds and fills only its own (below).
            hooks: Dict[str, Hook] = {
                name: copy.copy(hook)
                for name, hook in inspect.getmembers(
                    type(workflow),
                    predicate=lambda member: isinstance(member, Hook),
                )
            }

            # Every setting a Workflow may declare, with its default. A
            # Workflow attribute of one of these names overrides it -- only
            # these, so a Workflow's other members never leak in.
            config = {
                "vus": 1000,
                "duration": "1m",
                "threads": self._threads,
                "connect_retries": 3,
                "interval": workflow.interval,
                "cert_path": None,
                "key_path": None,
                "reset_connections": False,
                # Clients check the server's certificate and name (RFC 9110
                # section 4.3.4); a Workflow targeting a self-signed server
                # sets ``verify_tls = False``.
                "verify_tls": True,
            }

            engines_count = len(
                set(
                    [
                        hook.engine_type.name
                        if hook.engine_type != RequestType.CUSTOM
                        else hook.custom_result_type_name
                        for hook in hooks.values()
                        if hook.hook_type == HookType.TEST
                    ]
                )
            )

            vus_per_engine = math.ceil(vus / max(engines_count, 1))

            config.update(
                {
                    name: value
                    for name, value in inspect.getmembers(workflow)
                    # Membership, not truthiness, so False overrides
                    # (verify_tls, reset_connections). The Workflow base
                    # declares only vus, duration and interval among these,
                    # each equal to its default here.
                    if name in config
                }
            )

            config["vus"] = vus_per_engine
            # The worker's VUs: a task each.
            config["worker_vus"] = vus
            config["duration"] = TimeParser(config["duration"]).time

            if (interval := config.get("interval")) and interval is not None:
                config["interval"] = TimeParser(interval).time

            # AD-41 THROTTLE: each of the worker's VUs can be in an
            # iteration at once, until a throttle cuts the cap (see
            # throttle_workflow).
            self._max_active[run_id][workflow.name] = vus

            for client in workflow.client:
                setup_client(
                    client,
                    config.get("vus"),
                    pages=config.get("pages", 1),
                    cert_path=config.get("cert_path"),
                    key_path=config.get("key_path"),
                    reset_connections=config.get("reset_connections"),
                    verify_tls=config["verify_tls"],
                )

            self._workflow_hooks[run_id][workflow.name] = list(hooks.keys())

            step_graph = networkx.DiGraph()
            sources = []

            workflow_context = context[workflow.name]
            optimized_context_args = {
                name: value
                for name, value in workflow_context.items()
                if isinstance(value, OptimizedArg)
            }

            for hook in hooks.values():
                await ctx.log_prepared(
                    message=f"Run {run_id} of Workflow {workflow.name} setting up action {hook.name}",
                    name="debug",
                )

                stats_key = (workflow.name, hook.name)
                self._workflow_step_stats[run_id][stats_key] = {
                    "total": CompletionCounter(),
                    "ok": CompletionCounter(),
                    "err": CompletionCounter(),
                }

                step_graph.add_node(hook.name)

                # Bound to this run's workflow from the unbound callable (as
                # the state actions are), with argument dicts of its own: the
                # copy still shares its class hook's.
                hook.call = MethodType(
                    hook.call.__func__ if isinstance(hook.call, MethodType) else hook.call,
                    workflow,
                )
                hook.context_args = dict(hook.context_args)
                hook.optimized_args = dict(hook.optimized_args)
                setattr(workflow, hook.name, hook.call)

                if hook.hook_type == HookType.TEST:
                    hook.optimized_args.update(
                        {
                            name: value
                            for name, value in optimized_context_args.items()
                            if name in hook.kwarg_names
                        }
                    )

                if len(hook.optimized_args) > 0:
                    for arg in hook.optimized_args.values():
                        arg.call_name = hook.name

                    await asyncio.gather(
                        *[
                            guard_optimize_call(
                                arg.optimize(hook.engine_type),
                            )
                            for arg in hook.optimized_args.values()
                        ]
                    )

                    await asyncio.gather(
                        *[
                            guard_optimize_call(
                                workflow.client[hook.engine_type]._optimize(arg),
                            )
                            for arg in hook.optimized_args.values()
                        ]
                    )

                if len(hook.dependencies) == 0:
                    sources.append(hook.name)

                for dependency in hook.dependencies:
                    step_graph.add_edge(dependency, hook.name)

            traversal_order: List[Dict[str, Hook]] = []

            for traversal_layer in networkx.bfs_layers(step_graph, sources):
                traversal_order.append(
                    {hook_name: hooks.get(hook_name) for hook_name in traversal_layer}
                )

            return (
                workflow,
                hooks,
                traversal_order,
                config,
            )

    async def _execute_test_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
        hooks: Dict[str, Hook],
        context: Context,
        config: Dict[str, Any],
        stop_event: asyncio.Event | None,
        control: WorkflowRunControl,
    ):
        """
        A test workflow run by long-lived VUs: a task per VU for the whole
        run, each running the step graph again until the deadline -- k6's VU
        model. A VU starts its next iteration as soon as the last one
        finishes or, with an interval, one interval after the last one
        started: one iteration per VU per interval. An AD-41 throttle caps
        how many VUs are in an iteration at once. The engines' slots bound
        the requests in flight, and a VU still in an iteration at the
        deadline gets one more second to finish it.
        """
        loop = asyncio.get_event_loop()

        workflow_name = workflow.name

        workflow_context = context[workflow_name].dict()

        start = loop.time()
        deadline = start + config["duration"]

        workflow_results = Results(hooks)
        step_aggregates = workflow_results.create_aggregates(hooks)

        # Where the VUs wait in line on a throttle.
        parked = self._active_waiters[run_id][workflow_name]

        vu_tasks: Set[asyncio.Task] = {
            loop.create_task(
                self._run_long_lived_vu(
                    run_id,
                    workflow_name,
                    traversal_order,
                    workflow_context,
                    workflow_results,
                    step_aggregates,
                    deadline,
                    config.get("interval"),
                    parked,
                    control,
                ),
                name=workflow_name,
            )
            for _ in range(config["worker_vus"])
        }

        try:
            # Load generation ends at the deadline, where the VUs stop
            # starting iterations; one still in an iteration gets one more
            # second to finish it.
            running = vu_tasks
            if vu_tasks:
                _, running = await asyncio.wait(vu_tasks, timeout=max(deadline - loop.time(), 0))

            if self._monitors_enabled:
                await self._cpu_monitor.stop_background_monitor(
                    run_id,
                    workflow_name,
                )

            if running:
                await asyncio.wait(running, timeout=1)

            elapsed = loop.time() - start

        finally:
            # Past the grace, or cancelled: a VU still in an iteration is
            # cancelled, and that iteration does not count.
            for vu_task in vu_tasks:
                if not vu_task.done():
                    cancel_and_release_task(vu_task)

        # This run's load generation is over.
        if stop_event is not None:
            stop_event.set()

        for vu_task in vu_tasks:
            if vu_task.done() and not vu_task.cancelled() and (vu_error := vu_task.exception()) is not None:
                raise vu_error

        return workflow_results.process_aggregates(
            workflow_name,
            step_aggregates,
            elapsed,
        )

    async def _run_long_lived_vu(
        self,
        run_id: int,
        workflow_name: str,
        traversal_order: List[Dict[str, Hook]],
        workflow_context: Dict[str, Any],
        workflow_results: Results,
        step_aggregates: Dict[str, Any],
        deadline: float,
        interval: float | None,
        parked: deque[asyncio.Future[None]],
        control: WorkflowRunControl,
    ) -> None:
        """
        One long-lived VU: the step graph, layer by layer, again and again
        until the deadline, each step counted and its result aggregated.
        Each layer's steps run together, each given the context values its
        keyword arguments name. With an interval, each iteration starts one
        interval after the last one started, or at once if that one ran
        longer. An iteration holds one of the workflow's AD-41 slots: with
        none free, the VU waits in line (``parked``) until a VU hands it
        one. Once its run's ``control`` stops running, it starts no further
        iteration. An iteration under way when the VU is cancelled is
        dropped.
        """
        loop = asyncio.get_event_loop()
        current_task = asyncio.current_task()
        count_completed = self._completed_counts[run_id][workflow_name].increment
        count_failed = self._failed_counts[run_id][workflow_name].increment
        aggregate_result = workflow_results.aggregate_result
        step_stats = self._workflow_step_stats[run_id]
        active = self._active[run_id]
        caps = self._max_active[run_id]

        # What an iteration needs from each layer, looked up once (under
        # simulation in step-name order, so results replay byte for byte):
        # the hooks that take context values, with the keys they take; each
        # step's call and arguments; each step's counters. Before a layer,
        # every iteration's context holds the same keys -- the workflow
        # context's, then each earlier step's name, as every step stores its
        # result -- so the keys a hook's keyword arguments name are matched
        # here once, not each iteration.
        layers = []
        context_keys = dict.fromkeys(workflow_context)
        for hook_set in traversal_order:
            layer_hooks = sorted(hook_set.items()) if self._deterministic_step_order else hook_set.items()
            layers.append(
                (
                    [
                        (hook.context_args, argument_keys)
                        for _, hook in layer_hooks
                        if (argument_keys := [key for key in context_keys if key in hook.kwarg_names])
                    ],
                    [(hook.call, hook.context_args) for _, hook in layer_hooks],
                    [
                        (
                            step_name,
                            step_stats[(workflow_name, step_name)]["total"].increment,
                            step_stats[(workflow_name, step_name)]["ok"].increment,
                            step_stats[(workflow_name, step_name)]["err"].increment,
                        )
                        for step_name, _ in layer_hooks
                    ],
                )
            )
            context_keys.update(dict.fromkeys(step_name for step_name, _ in layer_hooks))

        frozen_clock_spins = 0
        previous_now = None
        # Whether the VU first in line handed this one its slot.
        holds_slot = False
        while control.running and (now := loop.time()) < deadline:
            # An iteration runs in one of the workflow's slots. With all of
            # them taken, or VUs already waiting for one, this VU waits at the
            # back of the line until a VU hands it its slot.
            if holds_slot:
                holds_slot = False

            elif parked or active[workflow_name] >= caps[workflow_name]:
                parked_vu = loop.create_future()
                parked.append(parked_vu)
                await parked_vu
                holds_slot = True
                continue

            else:
                active[workflow_name] += 1

            context = dict(workflow_context)
            # Whether a layer had to wait: an iteration in which none did
            # ends with a yield (below).
            waited = False

            try:
                for layer_arguments, layer_calls, layer_steps in layers:
                    # This iteration's values for the keys each hook takes.
                    for step_arguments, argument_keys in layer_arguments:
                        step_arguments.update({key: context[key] for key in argument_keys})

                    # Each step's task starts at once, in this VU's turn, and
                    # runs until it first waits, instead of a loop pass later.
                    gathered = asyncio.gather(
                        *[
                            asyncio.Task(step_call(**step_arguments), loop=loop, eager_start=True)
                            for step_call, step_arguments in layer_calls
                        ],
                        return_exceptions=True,
                    )
                    if not gathered.done():
                        waited = True

                    results = await gathered
                    if current_task.cancelling():
                        return

                    for (step_name, count_total, count_ok, count_error), result in zip(layer_steps, results):
                        failed = isinstance(result, BaseException)
                        count_total()
                        if failed:
                            count_failed()
                            count_error()

                        else:
                            count_ok()

                        context[step_name] = result
                        count_completed()

                        # A result, or a failure's exception, is aggregated;
                        # nothing, or a CancelledError, is not.
                        if result is not None and (not failed or isinstance(result, Exception)):
                            aggregate_result(step_aggregates, step_name, result)

            except Exception:
                # A VU that fails hands its slot on, as below, before its
                # error ends it: no VU waits in line on it.
                while parked and parked[0].done():
                    parked.popleft()
                if parked and active[workflow_name] <= caps[workflow_name]:
                    parked.popleft().set_result(None)

                else:
                    active[workflow_name] -= 1

                raise

            # The slot goes to the VU first in line -- passing over any
            # cancelled while waiting -- while the cap allows it, and back to
            # the workflow otherwise. A VU cancelled with its run mid-
            # iteration (above) does neither: the run's counts no longer gate
            # anything, and a replacing run may already count its own under
            # the same keys.
            while parked and parked[0].done():
                parked.popleft()
            if parked and active[workflow_name] <= caps[workflow_name]:
                parked.popleft().set_result(None)

            else:
                active[workflow_name] -= 1

            # With an interval, the next iteration starts one interval after
            # this one started -- at once if this one ran longer -- and never
            # past the deadline: one iteration per VU per interval. Its timed
            # sleep gives the loop a turn too.
            pacing_delay = min(now + interval, deadline) - loop.time() if interval else 0.0

            # A step that never suspends would keep this VU from yielding:
            # an iteration that waited on nothing gives the loop a turn. On a
            # frozen (simulated) clock, a timed sleep lets it advance (see
            # _FROZEN_CLOCK_SPINS).
            frozen_clock_spins = frozen_clock_spins + 1 if now == previous_now else 0
            previous_now = now
            if pacing_delay > 0:
                await asyncio.sleep(pacing_delay)

            elif frozen_clock_spins >= _FROZEN_CLOCK_SPINS:
                frozen_clock_spins = 0
                await asyncio.sleep(_FROZEN_CLOCK_ANCHOR_SECONDS)

            elif not waited:
                await asyncio.sleep(0)

        # Handed a slot as the run ended: it goes on, as above, so every VU
        # still in line ends too.
        if holds_slot:
            while parked and parked[0].done():
                parked.popleft()
            if parked and active[workflow_name] <= caps[workflow_name]:
                parked.popleft().set_result(None)

            else:
                active[workflow_name] -= 1

    async def _execute_non_test_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
        context: Context,
        config: Dict[str, Any],
        stop_event: asyncio.Event | None,
    ) -> Dict[str, Any | Exception]:
        """
        An action workflow: its step graph once, layer by layer. Each layer's
        steps run together, each given the context values its keyword
        arguments name, and have the workflow's duration to finish: a step
        still running then is cancelled and has no result, as is every step
        still running when the run is cancelled. Returns each finished
        step's result -- or, once every layer has run, raises the first
        failed step's exception, in graph order.
        """
        workflow_name = workflow.name
        step_stats = self._workflow_step_stats[run_id]
        completed_counter = self._completed_counts[run_id][workflow_name]
        failed_counter = self._failed_counts[run_id][workflow_name]
        layer_timeout = config["duration"]

        step_context: Dict[str, Any] = dict(context[workflow_name].dict())
        step_results: Dict[str, Any | Exception] = {}
        first_error: BaseException | None = None

        for hook_set in traversal_order:
            # Under simulation in step-name order, so results replay byte
            # for byte.
            layer_hooks = sorted(hook_set.items()) if self._deterministic_step_order else hook_set.items()
            for _, hook in layer_hooks:
                hook.context_args.update({key: step_context[key] for key in step_context if key in hook.kwarg_names})

            layer_tasks = [
                asyncio.create_task(hook.call(**hook.context_args), name=step_name)
                for step_name, hook in layer_hooks
            ]
            try:
                await asyncio.wait(layer_tasks, timeout=layer_timeout)

            finally:
                # Out of time, or cancelled with the run: a step still
                # running is cancelled, and has no result.
                for layer_task in layer_tasks:
                    if not layer_task.done():
                        cancel_and_release_task(layer_task)

            for (step_name, _), layer_task in zip(layer_hooks, layer_tasks):
                if not layer_task.done():
                    continue

                stats = step_stats[(workflow_name, step_name)]
                stats["total"].increment()
                try:
                    result = layer_task.result()
                    stats["ok"].increment()

                except (Exception, asyncio.CancelledError) as step_error:
                    # A failed step's exception is its result, which later
                    # steps are given; the first fails the workflow (below).
                    failed_counter.increment()
                    stats["err"].increment()
                    result = step_error
                    if first_error is None:
                        first_error = step_error

                step_context[step_name] = result
                step_results[step_name] = result
                completed_counter.increment()

        # This run's load generation is over.
        if stop_event is not None:
            stop_event.set()

        if first_error is not None:
            raise first_error

        return step_results

    async def _provide_context(
        self,
        workflow: str,
        state_actions: Dict[str, ContextHook],
        context: Context,
        results: Dict[str, Any],
    ):
        provide_actions = [
            action
            for action in state_actions.values()
            if action.action_type == StateAction.PROVIDE
        ]

        if len(provide_actions) < 1:
            return context

        for hook in provide_actions:
            hook.context_args = {
                name: value for name, value in context[workflow].items()
            }

            hook.context_args.update(results)

        provided = await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in provide_actions]
        )

        # A provided value goes to the provider's own namespace -- read by a
        # consumer naming its source, @state('Provider') -- and to every
        # namespace it targets -- read by a consumer of its own namespace,
        # @state(). The value is what the hook returned: ``hook.result`` is
        # never assigned, so every value written from it was None.
        await asyncio.gather(
            *[
                context[namespace].set(hook_name, result)
                for hook, (hook_name, result) in zip(provide_actions, provided)
                for namespace in dict.fromkeys((workflow, *hook.workflows))
            ]
        )

        return context

    def _clear(self):
        self._running_workflows.clear()
        self._failed_counts.clear()
        self._completed_counts.clear()
        self._workflow_step_stats.clear()
        self._workflow_hooks.clear()

        self._active.clear()
        self._active_waiters.clear()
        self._max_active.clear()
        self._throttle_base_cap.clear()
        self._concurrency_gated.clear()
        self._run_controls.clear()

    async def close(self):
        async with self._logger.context(
            name="workflow_manager",
            path=self._logfile,
            template="{timestamp} - {level} - {thread_id} - {filename}:{function_name}.{line_number} - {message}",
        ) as ctx:
            await ctx.log(
                Entry(
                    message=f"Closing Workflow Runner at {self._node_id}",
                    level=LogLevel.INFO,
                )
            )

            for job in self._running_workflows.values():
                for workflow in job.values():
                    workflow.client.close()

            try:
                await self._cpu_monitor.stop_all_background_monitors()
            except Exception:
                pass

            try:
                await self._memory_monitor.stop_all_background_monitors()
            except Exception:
                pass

    def abort(self):
        self._logger.abort()

        for job in self._running_workflows.values():
            for workflow in job.values():
                try:
                    workflow.client.close()

                except Exception:
                    pass

        try:
            self._cpu_monitor.abort_all_background_monitors()

        except Exception:
            pass

        try:
            self._memory_monitor.abort_all_background_monitors()

        except Exception:
            pass
