import asyncio
import inspect
import math
import socket
import warnings
from collections import defaultdict
from typing import Any, AsyncGenerator, Coroutine, Dict, List, Literal, Tuple

import networkx
import psutil

from hyperscale.core.engines.client import TimeParser
from hyperscale.core.engines.client.shared.models import RequestType
from hyperscale.core.engines.client.setup_clients import setup_client
from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import Hook, HookType
from hyperscale.core.jobs.models.env import Env
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


warnings.simplefilter("ignore")


async def guard_optimize_call(optimize_call: Coroutine[Any, Any, None]):
    try:
        await optimize_call

    except Exception:
        pass


def guard_result(result: asyncio.Task):
    try:
        return result.result()

    except Exception as err:
        return err


# Frozen-clock anchor tuning for the VU generator loops. The spin
# threshold is the count of CONSECUTIVE generator iterations whose
# ``loop.time()`` reading was bit-identical before a timed sleep is
# injected: a real event loop can never hold one reading across ten
# thousand yield+spawn iterations (each costs microseconds against
# nanosecond-resolution clocks), so on real hosts the anchor never
# fires and the generators keep their exact maximum-throughput
# sleep(0) hot path; under a virtual (timer-driven) clock the reading
# is frozen by construction whenever no timer is pending, the
# threshold trips, and the 1ms anchor gives the clock a timer to
# advance on instead of spinning at one instant forever.
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
        # under a simulation transport each hook set's finished tasks
        # are consumed in step-name order (see ``_spawn_vu``). ``False``
        # in REAL mode, where the order carries no meaning: no
        # per-request sorting.
        self._deterministic_step_order = deterministic_step_order

        self._worker_id = worker_id

        self._logfile = f"hyperscale.worker.{self._worker_id}.log.json"
        if worker_id is None:
            self._logfile = "hyperscale.leader.log.json"

        self._node_id = node_id
        self.run_statuses: Dict[int, Dict[str, WorkflowStatus]] = defaultdict(dict)

        self._active: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._active_waiters: Dict[int, Dict[str, asyncio.Future]] = defaultdict(dict)
        self._max_active: Dict[int, Dict[str, int]] = defaultdict(dict)
        # AD-41 THROTTLE: the concurrency cap a throttled workflow had
        # before its first throttle (restored on release), and which
        # workflows run a concurrency-gated (TEST) spawn loop at all.
        self._throttle_base_cap: Dict[int, Dict[str, int]] = defaultdict(dict)
        self._concurrency_gated: Dict[int, set[str]] = defaultdict(set)
        self._run_tasks: Dict[int, Dict[str, asyncio.Future]] = defaultdict(dict)
        self._failed: Dict[int, Dict[str, List[asyncio.Task]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._pending: Dict[int, Dict[str, List[asyncio.Task]]] = defaultdict(
            lambda: defaultdict(list)
        )
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
        self._is_cancelled: asyncio.Event = asyncio.Event()
        self._is_stopped: asyncio.Event = asyncio.Event()

        # Cancellation flag - checked by generators to stop spawning new VUs
        self._running: bool = False

    def setup(self):
        if self._workflows_sem is None:
            self._workflows_sem = asyncio.Semaphore(self._max_running_workflows)

        if self._run_check_lock is None:
            self._run_check_lock = asyncio.Lock()

        self._clear()

    async def await_cancellation(self) -> None:
        """
        Wait for the current workflow to finish (by cancellation or completion).

        This event is set when either _execute_test_workflow or
        _execute_non_test_workflow completes, regardless of whether
        the workflow was cancelled or finished normally.
        """
        await self._is_cancelled.wait()

    def request_cancellation(self) -> None:
        """
        Request graceful cancellation of the current workflow.

        This sets a flag that causes the VU generators (_generate, _generate_constant)
        to stop yielding new VUs. Already-spawned tasks complete normally, and the
        standard cleanup path runs without throwing exceptions.

        NOTE: only the duration-governed (TEST) execution path consults
        this flag. ACTION workflows run their step DAG with plain awaits
        and finish at natural length regardless — the graceful phase of
        cancellation is a no-op for them. ``hard_cancel`` is the
        escalation that actually stops in-flight execution when the
        graceful window expires.

        Thread-safe: GIL ensures atomic bool write.
        """
        self._running = False

    def hard_cancel(self, run_id: int, workflow_name: str) -> bool:
        """
        Hard-stop a running workflow after its graceful window expired.

        Cancels the run task (CancelledError propagates to whoever is
        awaiting the run — the worker's executor already maps it to the
        CANCELLED terminal), cancels every pending VU task, clears the
        pacing state (the ``replace`` duplicate-policy cleanup, which
        this mirrors), and — because the completion events are normally
        set only at the END of natural execution — sets ``_is_cancelled``
        and ``_is_stopped`` directly so ``await_cancellation`` observers
        converge instead of waiting on an execution that will never
        finish. Idempotent; returns True when a live run task was
        actually cancelled.

        Without this escalation, "cancelling" an ACTION workflow meant
        waiting for it to complete: a hard-timed-out 100s workflow kept
        its cores busy for the full 100s (measured: 55.5 virtual seconds
        of zombie execution past the client's timeout terminal, with the
        manager re-cancelling on a ~6s cadence the whole way).
        """
        self._running = False

        cancelled_live_task = False
        run_task = self._run_tasks.get(run_id, {}).get(workflow_name)
        if run_task is not None and not run_task.done():
            run_task.cancel()
            cancelled_live_task = True

        for pending_task in self._pending.get(run_id, {}).get(workflow_name, []):
            cancel_and_release_task(pending_task)
        if self._pending.get(run_id, {}).get(workflow_name):
            self._pending[run_id][workflow_name].clear()

        if run_id in self._active and workflow_name in self._active[run_id]:
            self._active[run_id][workflow_name] = 0
        if self._active_waiters.get(run_id, {}).get(workflow_name):
            waiter = self._active_waiters[run_id][workflow_name]
            if waiter is not None and not waiter.done():
                waiter.cancel()
            del self._active_waiters[run_id][workflow_name]
        if self._running_workflows.get(run_id, {}).get(workflow_name):
            del self._running_workflows[run_id][workflow_name]

        if not self._is_stopped.is_set():
            self._is_stopped.set()
        if not self._is_cancelled.is_set():
            self._is_cancelled.set()

        return cancelled_live_task

    def throttle_workflow(self, run_id: int, workflow_name: str, scale: float) -> int | None:
        """AD-41 THROTTLE: cut a running workflow's concurrency to ``scale``
        of its current operating point (the lower of its cap and the steps
        actually in flight), never below one; repeated throttles compound.

        Returns the new cap, or None when the workflow runs no
        concurrency-gated spawn loop (not running, or an ACTION workflow,
        whose steps run once) -- there is nothing to throttle.
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
        """Restore a throttled workflow's original cap and wake its spawn
        loop if it is parked on the throttled one. False when it was not
        throttled."""
        base_cap = self._throttle_base_cap.get(run_id, {}).pop(workflow_name, None)
        if base_cap is None:
            return False
        if workflow_name in self._concurrency_gated.get(run_id, ()):
            self._max_active[run_id][workflow_name] = base_cap
        # Wake it the way _spawn_vu does: resolve, then clear the slot so
        # the spawn loop can park on the restored cap again.
        waiter = self._active_waiters.get(run_id, {}).get(workflow_name)
        if waiter is not None:
            if not waiter.done():
                waiter.set_result(None)
            self._active_waiters[run_id][workflow_name] = None
        return True

    def _end_concurrency_gate(self, run_id: int, workflow_name: str) -> None:
        self._concurrency_gated[run_id].discard(workflow_name)
        if not self._concurrency_gated[run_id]:
            del self._concurrency_gated[run_id]
        self._throttle_base_cap.get(run_id, {}).pop(workflow_name, None)
        if run_id in self._throttle_base_cap and not self._throttle_base_cap[run_id]:
            del self._throttle_base_cap[run_id]

    async def await_stop(self) -> None:
        return await self._is_stopped.wait()
        

    @property
    def is_cancelled(self) -> bool:
        """Check if cancellation has been requested."""
        return self._running is False

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
    ) -> Tuple[WorkflowStatus, int, int, Dict[str, Dict[StepStatsType, int]]]:
        status = self.run_statuses.get(
            run_id,
            {},
        ).get(workflow, WorkflowStatus.UNKNOWN)

        completed_count = 0
        failed_count = 0

        workflow_hooks = self._workflow_hooks[run_id].get(workflow, [])

        worklow_hook_stats: Dict[str, Dict[StepStatsType, int]] = {
            hook: {"total": 0, "ok": 0, "err": 0} for hook in workflow_hooks
        }

        try:
            completed_counter = self._completed_counts[run_id].get(
                workflow, CompletionCounter()
            )
            completed_count = completed_counter.value()

        except Exception:
            pass

        try:
            failed_counter = self._failed_counts[run_id].get(
                workflow, CompletionCounter()
            )
            failed_count = failed_counter.value()

        except Exception:
            pass

        try:
            for (workflow_name, hook_name), stats in self._workflow_step_stats[
                run_id
            ].items():
                if workflow_name == workflow:
                    worklow_hook_stats[hook_name] = {
                        "total": stats["total"].value(),
                        "ok": stats["ok"].value(),
                        "err": stats["err"].value(),
                    }

        except Exception:
            pass

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
    ) -> Tuple[
        int,
        WorkflowStats
        | Dict[
            str,
            Any | Exception,
        ]
        | None,
        WorkflowContext | None,
        Exception | None,
        WorkflowStatus,
    ]:
        # Reset cancellation state for new workflow run
        self._running = True
        self._is_cancelled.clear()

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
            await self._run_check_lock.acquire()

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

                self._run_check_lock.release()

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

                self._run_check_lock.release()

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

                if self._max_active[run_id][workflow_name]:
                    del self._max_active[run_id][workflow_name]

                self._pending[run_id][workflow_name].clear()

                if self._running_workflows[run_id].get(workflow.name):
                    del self._running_workflows[run_id][workflow.name]

            await ctx.log_prepared(
                message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.PENDING.name} state",
                name="info",
            )

            self._run_check_lock.release()

            self.run_statuses[run_id][workflow.name] = WorkflowStatus.PENDING

            async with self._workflows_sem:
                workflow_name = workflow.name

                await ctx.log_prepared(
                    message=f"Run {run_id} of Workflow {workflow.name} successfully entered {WorkflowStatus.CREATED.name} state",
                    name="info",
                )

                if self._monitors_enabled:
                    await self._cpu_monitor.start_background_monitor(
                        run_id, workflow_name
                    )
                    await self._memory_monitor.start_background_monitor(
                        run_id, workflow_name
                    )

                self.run_statuses[run_id][workflow_name] = WorkflowStatus.CREATED

                context = Context()

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

                run_task: asyncio.Future | None = None
                try:
                    run_task = asyncio.ensure_future(
                        self._run_workflow(
                            run_id,
                            workflow,
                            context,
                            vus,
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

                    if self._monitors_enabled:
                        await self._cpu_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )
                        await self._memory_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )

                    return (
                        run_id,
                        results,
                        updated_context[workflow_name],
                        None,
                        WorkflowStatus.COMPLETED,
                    )

                except Exception as err:
                    await ctx.log_prepared(
                        message=f"Run {run_id} of Workflow {workflow.name} encountered error {str(err)}",
                        name="error",
                    )

                    self.run_statuses[run_id][workflow.name] = WorkflowStatus.FAILED
                    if self._monitors_enabled:
                        await self._cpu_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )
                        await self._memory_monitor.stop_background_monitor(
                            run_id,
                            workflow_name,
                        )

                    return (
                        run_id,
                        None,
                        context[workflow_name],
                        err,
                        WorkflowStatus.FAILED,
                    )

                finally:
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
                try:
                    results = await self._execute_test_workflow(
                        run_id,
                        workflow,
                        traversal_order,
                        hooks,
                        context,
                        config,
                    )
                finally:
                    self._end_concurrency_gate(run_id, workflow.name)

            else:
                await ctx.log_prepared(
                    message=f"Run {run_id} of non test Workflow {workflow.name} beginning execution",
                    name="debug",
                )

                results = await self._execute_non_test_workflow(
                    run_id,
                    workflow,
                    traversal_order,
                    context,
                    config,
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
        state_actions: Dict[str, ContextHook] = {
            name: hook
            for name, hook in inspect.getmembers(
                workflow,
                predicate=lambda member: isinstance(member, ContextHook),
            )
        }

        for action in state_actions.values():
            action._call = action._call.__get__(workflow, workflow.__class__)
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

            hooks: Dict[str, Hook] = {
                name: hook
                for name, hook in inspect.getmembers(
                    workflow,
                    predicate=lambda member: isinstance(member, Hook),
                )
            }

            config = {
                "vus": 1000,
                "duration": "1m",
                "threads": self._threads,
                "connect_retries": 3,
                "interval": workflow.interval,
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
                    if config.get(name)
                }
            )

            config["vus"] = vus_per_engine
            config["duration"] = TimeParser(config["duration"]).time

            if (interval := config.get("interval")) and interval is not None:
                config["interval"] = TimeParser(interval).time

            threads = config.get("threads")

            # Floor-based approach - commented out for testing
            # self._max_active[run_id][workflow.name] = vus * 10

            # Original CPU-aware formula: scales with CPU count to account for
            # less powerful individual cores on high-CPU systems
            self._max_active[run_id][workflow.name] = math.ceil(
                (vus * (psutil.cpu_count() ** 2)) / threads
            )

            for client in workflow.client:
                setup_client(
                    client,
                    config.get("vus"),
                    pages=config.get("pages", 1),
                    cert_path=config.get("cert_path"),
                    key_path=config.get("key_path"),
                    reset_connections=config.get("reset_connections"),
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

                hook.call = hook.call.__get__(workflow, workflow.__class__)
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

            self._active_waiters[run_id][workflow.name] = None

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
    ):
        loop = asyncio.get_event_loop()

        workflow_name = workflow.name

        workflow_context = context[workflow_name].dict()

        # Loop time, not wall time: on a real event loop ``loop.time()``
        # IS the monotonic clock, so REAL behavior is unchanged; under
        # SIM it is virtual time, so a workflow's duration and elapsed
        # measurement follow the deterministic schedule instead of
        # host-dependent wall time.
        start = loop.time()

        if config.get("interval") is None:
            completed, pending = await asyncio.wait(
                [
                    loop.create_task(
                        self._spawn_vu(
                            run_id,
                            workflow_name,
                            traversal_order,
                            remaining,
                            workflow_context,
                        ),
                        name=workflow.name,
                    )
                    async for remaining in self._generate(
                        run_id,
                        workflow_name,
                        config,
                    )
                ],
                timeout=1,
            )

        else:
            completed, pending = await asyncio.wait(
                [
                    loop.create_task(
                        self._spawn_vu(
                            run_id,
                            workflow_name,
                            traversal_order,
                            remaining,
                            workflow_context,
                        ),
                        name=workflow.name,
                    )
                    async for remaining in self._generate_constant(
                        run_id,
                        workflow_name,
                        config,
                    )
                ],
                timeout=1,
            )

        elapsed = loop.time() - start

        if not self._is_stopped.set():
            self._is_stopped.set()

        await asyncio.gather(*completed, return_exceptions=True)

        # Cancel and release all pending tasks
        for pend in self._pending[run_id][workflow.name]:
            cancel_and_release_task(pend)
        self._pending[run_id][workflow.name].clear()

        # Cancel tasks from asyncio.wait that didn't complete
        for pend in pending:
            cancel_and_release_task(pend)

        if len(self._failed[run_id][workflow_name]) > 0:
            await asyncio.gather(
                *self._failed[run_id][workflow_name],
                return_exceptions=True,
            )

        workflow_results_set: Dict[str, List[Any]] = {
            hook_name: [] for hook_name in hooks
        }

        [
            workflow_results_set[result.get_name()].append(
                guard_result(result),
            )
            for complete in completed
            for result in complete.result()
            if guard_result(result) is not None
        ]

        workflow_results = Results(hooks)

        processed_results = workflow_results.process(
            workflow_name,
            workflow_results_set,
            elapsed,
        )

        if not self._is_cancelled.is_set():
            self._is_cancelled.set()

        return processed_results

    async def _execute_non_test_workflow(
        self,
        run_id: int,
        workflow: Workflow,
        traversal_order: List[Dict[str, Hook]],
        context: Context,
        config: Dict[str, Any],
    ) -> Dict[str, Any | Exception]:
        workflow_name = workflow.name
        workflow_context = context[workflow_name].dict()

        workflow_duration = config.get(
            "duration",
            TimeParser("1m").time,
        )

        execution_results: List[asyncio.Task] = await self._spawn_vu(
            run_id,
            workflow_name,
            traversal_order,
            workflow_duration,
            workflow_context,
        )

        await asyncio.gather(*execution_results)

        if not self._is_stopped.set():
            self._is_stopped.set()

        # Cancel and release all pending tasks
        for pend in self._pending[run_id][workflow_name]:
            cancel_and_release_task(pend)
        self._pending[run_id][workflow_name].clear()

        if not self._is_cancelled.is_set():
            self._is_cancelled.set()

        return {result.get_name(): guard_result(result) for result in execution_results}

    async def _spawn_vu(
        self,
        run_id: int,
        workflow_name: str,
        traversal_order: List[Dict[str, Hook]],
        remaining: float,
        context: Dict[str, Any],
    ):
        results: List[asyncio.Task] = []
        context: Dict[str, Any] = dict(context)

        for hook_set in traversal_order:
            set_count = len(hook_set)
            self._active[run_id][workflow_name] += set_count

            for hook in hook_set.values():
                hook.context_args.update(
                    {key: context[key] for key in context if key in hook.kwarg_names}
                )

            tasks: Tuple[
                List[asyncio.Task],
                List[asyncio.Task],
            ] = await asyncio.wait(
                [
                    asyncio.create_task(
                        hook.call(**hook.context_args),
                        name=hook_name,
                    )
                    for hook_name, hook in hook_set.items()
                ],
                timeout=remaining,
            )

            # Under simulation only, STEP-NAME order everywhere downstream:
            # ``asyncio.wait`` returns id()-hashed SETS, and both the
            # results list (serialized per-step result mapping — insertion
            # order is wire bytes) and the per-step context dict are built
            # by iterating them. Address order permutes across runs
            # whenever one wait window completes >=2 steps (the chaos
            # VOPR's replay-divergence flake); task names are the hook
            # names — unique per set — so they are a total order.
            completed, pending = tasks
            if self._deterministic_step_order:
                completed = sorted(
                    completed, key=lambda done_task: done_task.get_name()
                )
                pending = sorted(
                    pending, key=lambda pending_task: pending_task.get_name()
                )

            results.extend(completed)

            for complete in completed:
                step_name = complete.get_name()

                self._workflow_step_stats[run_id][(workflow_name, step_name)][
                    "total"
                ].increment()

                try:
                    result = complete.result()
                    self._workflow_step_stats[run_id][(workflow_name, step_name)][
                        "ok"
                    ].increment()

                except (
                    Exception,
                    AssertionError,
                    asyncio.TimeoutError,
                    asyncio.CancelledError,
                    asyncio.InvalidStateError,
                    asyncio.IncompleteReadError,
                ) as err:
                    self._failed_counts[run_id][workflow_name].increment()
                    self._workflow_step_stats[run_id][(workflow_name, step_name)][
                        "err"
                    ].increment()

                    result = err

                    self._failed[run_id][workflow_name].append(complete)

                context[step_name] = result
                self._completed_counts[run_id][workflow_name].increment()

            self._pending[run_id][workflow_name].extend(pending)

            self._active[run_id][workflow_name] -= set_count

            if (
                self._active[run_id][workflow_name]
                <= self._max_active[run_id][workflow_name]
                and self._active_waiters[run_id][workflow_name]
            ):
                try:
                    self._active_waiters[run_id][workflow_name].set_result(None)
                    self._active_waiters[run_id][workflow_name] = None

                except asyncio.InvalidStateError:
                    self._active_waiters[run_id][workflow_name] = None

        return results

    async def _generate(
        self,
        run_id: int,
        workflow_name: str,
        config: Dict[str, Any],
    ) -> AsyncGenerator[float, None]:
        duration = config.get("duration")

        elapsed = 0

        # Loop time, not wall time — identical on a real loop, virtual
        # (deterministic) under SIM. See _execute_test_workflow.
        loop = asyncio.get_event_loop()
        start = loop.time()
        # Frozen-clock anchor (SIM liveness). ``sleep(0)`` re-queues at
        # the SAME loop instant, so when nothing else has a pending
        # timer a virtual clock never advances and this loop spins at
        # one frozen instant forever (the executor SimulationConstraint
        # abort; a real loop crawls past on wall time). Detection is a
        # single int compare per iteration — the hot path gains NO
        # awaits and NO time reads: only after ``_FROZEN_CLOCK_SPINS``
        # CONSECUTIVE iterations with a bit-identical clock reading —
        # impossible on any real clock at these iteration costs — does
        # a 1ms timed sleep run to give the clock a timer to advance
        # on. Throughput characteristics on a real loop are unchanged.
        frozen_clock_spins = 0
        while elapsed < duration and self._running:
            try:
                remaining = duration - elapsed

                yield remaining

                await asyncio.sleep(0)

                if (
                    self._active[run_id][workflow_name]
                    > self._max_active[run_id][workflow_name]
                    and self._active_waiters[run_id][workflow_name] is None
                ):
                    self._active_waiters[run_id][workflow_name] = (
                        loop.create_future()
                    )

                    try:
                        await asyncio.wait_for(
                            self._active_waiters[run_id][workflow_name],
                            timeout=remaining,
                        )
                    except asyncio.TimeoutError:
                        pass

            except Exception:
                pass

            previous_elapsed = elapsed
            elapsed = loop.time() - start
            if elapsed == previous_elapsed:
                frozen_clock_spins += 1
                if frozen_clock_spins >= _FROZEN_CLOCK_SPINS:
                    await asyncio.sleep(_FROZEN_CLOCK_ANCHOR_SECONDS)
                    frozen_clock_spins = 0
                    elapsed = loop.time() - start
            else:
                frozen_clock_spins = 0

        if self._monitors_enabled:
            await self._cpu_monitor.stop_background_monitor(
                run_id,
                workflow_name,
            )

    async def _generate_constant(
        self,
        run_id: int,
        workflow_name: str,
        config: Dict[str, Any],
    ) -> AsyncGenerator[float, None]:
        duration = config.get("duration")
        vus = config.get("vus")
        interval = config.get("interval")

        elapsed = 0
        generated = 0

        # Loop time, not wall time — identical on a real loop, virtual
        # (deterministic) under SIM. See _execute_test_workflow.
        loop = asyncio.get_event_loop()
        start = loop.time()
        # Frozen-clock anchor — same SIM-liveness guard as
        # ``_generate`` (a zero ``interval`` config degenerates this
        # loop to the same sleep(0) spin); see the comment there.
        frozen_clock_spins = 0
        while elapsed < duration and self._running:
            try:
                remaining = duration - elapsed

                yield remaining

                generated += 1

                await asyncio.sleep(0)

                if generated >= vus:
                    await asyncio.sleep(interval)
                    generated = 0

                if (
                    self._active[run_id][workflow_name]
                    > self._max_active[run_id][workflow_name]
                    and self._active_waiters[run_id][workflow_name] is None
                ):
                    self._active_waiters[run_id][workflow_name] = (
                        loop.create_future()
                    )

                    try:
                        await asyncio.wait_for(
                            self._active_waiters[run_id][workflow_name],
                            timeout=remaining,
                        )
                    except asyncio.TimeoutError:
                        pass

            except Exception:
                pass

            previous_elapsed = elapsed
            elapsed = loop.time() - start
            if elapsed == previous_elapsed:
                frozen_clock_spins += 1
                if frozen_clock_spins >= _FROZEN_CLOCK_SPINS:
                    await asyncio.sleep(_FROZEN_CLOCK_ANCHOR_SECONDS)
                    frozen_clock_spins = 0
                    elapsed = loop.time() - start
            else:
                frozen_clock_spins = 0

        if self._monitors_enabled:
            await self._cpu_monitor.stop_background_monitor(
                run_id,
                workflow_name,
            )

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

        await asyncio.gather(
            *[hook.call(**hook.context_args) for hook in provide_actions]
        )

        await asyncio.gather(
            *[
                context[target].set(hook.name, hook.result)
                for hook in provide_actions
                for target in hook.workflows
            ]
        )

        return context

    def _clear(self):
        self._running_workflows.clear()
        self._failed_counts.clear()
        self._completed_counts.clear()
        self._workflow_step_stats.clear()
        self._workflow_hooks.clear()
        self._pending.clear()

        self._active.clear()
        self._active_waiters.clear()
        self._max_active.clear()
        self._throttle_base_cap.clear()
        self._concurrency_gated.clear()

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

            # Cancel and release all pending tasks across all runs/workflows
            for run_id in self._pending:
                for workflow_name in self._pending[run_id]:
                    for pend in self._pending[run_id][workflow_name]:
                        cancel_and_release_task(pend)
                    self._pending[run_id][workflow_name].clear()

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

        # Cancel and release all pending tasks
        for run_id in self._pending:
            for workflow_name in self._pending[run_id]:
                for pend in self._pending[run_id][workflow_name]:
                    cancel_and_release_task(pend)
                self._pending[run_id][workflow_name].clear()

        # Cancel and release all failed tasks
        for run_id in self._failed:
            for workflow_name in self._failed[run_id]:
                for pend in self._failed[run_id][workflow_name]:
                    cancel_and_release_task(pend)
                self._failed[run_id][workflow_name].clear()

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
