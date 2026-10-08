import asyncio
import signal
import time

from hyperscale.core.graph import Workflow
from hyperscale.core.jobs.models import TerminalMode
from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs import WindowedStatsPush
from hyperscale.distributed.models import (
    ClientJobResult,
    JobBatchPush,
    JobStatusPush,
    WorkflowResultPush,
    WorkflowStatus,
)
from hyperscale.distributed.nodes.client import HyperscaleClient
from hyperscale.ui import HyperscaleInterface, InterfaceUpdatesController
from hyperscale.ui.actions import (
    update_active_workflow_message,
    update_workflow_execution_stats,
    update_workflow_executions_counter,
    update_workflow_executions_final_rate,
    update_workflow_executions_rates,
    update_workflow_executions_total_rate,
    update_workflow_progress_seconds,
    update_workflow_run_timer,
)

from .shutdown_signals import ShutdownSignals

# The local run plots one completion-rate point per second; a cluster run
# plots on the same cadence so both modes draw the same chart.
RATE_SAMPLE_INTERVAL_SECONDS = 1.0

# Why a job is cancelled when the operator stops its cluster run.
OPERATOR_CANCEL_REASON = "cancelled by the operator from the CLI"

ClusterPush = WindowedStatsPush | JobBatchPush | WorkflowResultPush | JobStatusPush


class ClusterRunner:
    """
    Run a test's workflows as one job on a running Hyperscale cluster --
    through its gates, or straight to one datacenter's managers -- and render
    the live terminal UI a local run shows, from the progress the cluster
    pushes back.

    Every push lands on one queue that a single updater drains into the UI,
    so updates apply in arrival order. A workflow runs as sub-workflows, one
    per worker (per datacenter) it was dispatched to, each reporting its own
    cumulative counts: the UI shows each workflow's sum of its
    sub-workflows' latest counts.
    """

    def __init__(
        self,
        host: str,
        port: int,
        env: Env,
        gates: list[tuple[str, int]],
        managers: list[tuple[str, int]],
    ) -> None:
        self._updates = InterfaceUpdatesController()
        self._interface = HyperscaleInterface(self._updates)
        self._client = HyperscaleClient(
            host=host,
            port=port,
            env=env,
            managers=managers,
            gates=gates,
        )
        self._pushes: asyncio.Queue[ClusterPush | None] = asyncio.Queue()
        self._terminal_mode: TerminalMode = "disabled"
        self._shutdown_signals: ShutdownSignals | None = None

        # Per workflow (its UI slug): each sub-workflow's latest windowed
        # stats, by sub-workflow token.
        self._sub_workflow_stats: dict[str, dict[str, WindowedStatsPush]] = {}
        self._workflow_started_at: dict[str, float] = {}
        self._workflow_rate_points: dict[str, list[tuple[int, int]]] = {}
        # (when, completed count) of each workflow's last rate point.
        self._workflow_rate_sampled: dict[str, tuple[float, int]] = {}
        self._displayed_workflows: list[str] = []
        self._finished_workflows: set[str] = set()

    async def run(
        self,
        test_name: str,
        workflows: list[tuple[list[str], Workflow]],
        terminal_mode: TerminalMode = "full",
    ) -> ClientJobResult:
        """
        Submit the workflows as one job, render its progress until it ends,
        and return its result. Stopping the run (SIGINT/SIGTERM, or
        cancelling this task) cancels the job on the cluster too: a job left
        running would keep loading its target unseen.
        """
        self._terminal_mode = terminal_mode
        self._interface.initialize(
            [workflow for _, workflow in workflows],
            terminal_mode=terminal_mode,
        )
        if terminal_mode in ("ci", "full"):
            await self._interface.run()

        self._shutdown_signals = ShutdownSignals(asyncio.current_task())
        try:
            with self._shutdown_signals as shutdown_signals:
                await update_active_workflow_message(
                    "initializing",
                    f"Connecting {test_name} to the cluster...",
                )
                await self._client.start()
                # The client's server registers signal handlers of its own.
                shutdown_signals.route()

                await update_active_workflow_message(
                    "initializing",
                    f"Submitting {test_name}...",
                )
                job_id = await self._client.submit_job(
                    workflows=workflows,
                    on_status_update=self._pushes.put_nowait,
                    on_progress_update=self._queue_progress,
                    on_workflow_result=self._pushes.put_nowait,
                )

                updater = asyncio.get_running_loop().create_task(self._apply_pushes())
                try:
                    result = await self._client.wait_for_job(job_id)

                except asyncio.CancelledError:
                    await self._client.cancel_job(job_id, reason=OPERATOR_CANCEL_REASON)
                    raise

                finally:
                    # The updater drains what arrived, then stops.
                    self._pushes.put_nowait(None)
                    await updater

        except BaseException:
            if terminal_mode in ("ci", "full"):
                await self._interface.abort()
            await self._client.stop()
            raise

        if terminal_mode in ("ci", "full"):
            await self._interface.stop()
        await self._client.stop()
        return result

    @property
    def stopped_by(self) -> signal.Signals | None:
        """The signal that stopped the run, if one did."""
        if self._shutdown_signals is None:
            return None
        return self._shutdown_signals.received

    async def _queue_progress(self, push: WindowedStatsPush | JobBatchPush) -> None:
        """The client's progress callback. A coroutine, so the client awaits
        it on the loop rather than handing it to a thread."""
        self._pushes.put_nowait(push)

    async def _apply_pushes(self) -> None:
        """Apply each push to the UI, in arrival order, until the run ends."""
        while (push := await self._pushes.get()) is not None:
            if isinstance(push, WindowedStatsPush):
                workflow_slug = push.workflow_name.lower()
                sub_workflow_stats = self._sub_workflow_stats.setdefault(workflow_slug, {})
                if (
                    previous_stats := sub_workflow_stats.get(push.workflow_id)
                ) is not None and previous_stats.window_end >= push.window_end:
                    # A late window: this sub-workflow already reported newer
                    # cumulative counts.
                    continue
                sub_workflow_stats[push.workflow_id] = push

                now = time.monotonic()
                if workflow_slug not in self._workflow_started_at:
                    self._workflow_started_at[workflow_slug] = now
                    self._workflow_rate_sampled[workflow_slug] = (now, 0)
                    self._workflow_rate_points[workflow_slug] = []
                    # A workflow starting takes the place of those finished.
                    self._displayed_workflows = [
                        displayed
                        for displayed in self._displayed_workflows
                        if displayed not in self._finished_workflows
                    ]
                    self._displayed_workflows.append(workflow_slug)
                    self._updates.update_active_workflows(list(self._displayed_workflows))
                    await update_workflow_run_timer(workflow_slug, True)

                completed_count = sum(
                    stats.completed_count for stats in sub_workflow_stats.values()
                )
                elapsed = now - self._workflow_started_at[workflow_slug]
                await asyncio.gather(
                    update_active_workflow_message(
                        workflow_slug, f"Running - {push.workflow_name}"
                    ),
                    update_workflow_executions_counter(workflow_slug, completed_count),
                    update_workflow_executions_total_rate(
                        workflow_slug, completed_count, True
                    ),
                    update_workflow_progress_seconds(workflow_slug, elapsed),
                )

                sampled_at, sampled_completed = self._workflow_rate_sampled[workflow_slug]
                if (sample_interval := now - sampled_at) > RATE_SAMPLE_INTERVAL_SECONDS:
                    # Each point is the rate over its own sample interval, as
                    # a local run plots it.
                    self._workflow_rate_points[workflow_slug].append(
                        (
                            int(elapsed),
                            int((completed_count - sampled_completed) / sample_interval),
                        )
                    )
                    self._workflow_rate_sampled[workflow_slug] = (now, completed_count)

                    step_totals: dict[str, dict[str, int]] = {}
                    for stats in sub_workflow_stats.values():
                        for step in stats.step_stats:
                            totals = step_totals.setdefault(
                                step.step_name, {"ok": 0, "err": 0, "total": 0}
                            )
                            totals["ok"] += step.completed_count
                            totals["err"] += step.failed_count
                            totals["total"] += step.total_count

                    await update_workflow_executions_rates(
                        workflow_slug, self._workflow_rate_points[workflow_slug]
                    )
                    await update_workflow_execution_stats(workflow_slug, step_totals)

            elif isinstance(push, WorkflowResultPush):
                workflow_slug = push.workflow_name.lower()
                self._finished_workflows.add(workflow_slug)
                # A client-bound result carries the workflow's one aggregated
                # stats set: its final executed count, over the elapsed it was
                # measured across -- the last stream sample misses actions
                # that finished after it, and the time the result arrived
                # adds its transfer, understating the rate (as a local run
                # reports it).
                if (
                    len(push.results) == 1
                    and (final_counts := push.results[0].get("stats")) is not None
                    and (final_elapsed := push.results[0].get("elapsed"))
                ):
                    await update_workflow_executions_counter(
                        workflow_slug, final_counts["executed"]
                    )
                    await update_workflow_executions_final_rate(
                        workflow_slug, final_counts["executed"], final_elapsed
                    )
                await update_workflow_run_timer(workflow_slug, False)
                await update_active_workflow_message(
                    workflow_slug,
                    "Complete"
                    if push.status == WorkflowStatus.COMPLETED.value
                    else f"{push.status.capitalize()} - {push.error or push.workflow_name}",
                )
                # Finished workflows stay on screen until others start; while
                # some still run, only those are shown.
                if running_workflows := [
                    displayed
                    for displayed in self._displayed_workflows
                    if displayed not in self._finished_workflows
                ]:
                    self._displayed_workflows = running_workflows
                    self._updates.update_active_workflows(list(running_workflows))

            elif isinstance(push, JobStatusPush) and not self._workflow_started_at:
                # Until a workflow runs, the job's own status is the message.
                await update_active_workflow_message("initializing", push.message)
