import time
from collections import Counter

from hyperscale.distributed.models import WorkflowProgress
from hyperscale.distributed.nodes import WorkerServer
from hyperscale.ui.components.table.table_config import HeaderOptions

from .counter_rates import CounterRates
from .dashboard_formatting import format_reading, in_use_percent
from .models import NodeDashboardChart, NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader

WORKER_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="worker",
    table_headers={
        "workflow": HeaderOptions(default="none", fixed=True),
        "status": HeaderOptions(default="-"),
        "done": HeaderOptions(default=0),
        "failed": HeaderOptions(default=0),
        "rate": HeaderOptions(default=0, precision_format=".1f"),
        "cores": HeaderOptions(default=0),
    },
    # The workflows it completed and failed are workflows a second: one
    # axis, as on its manager's chart. Its active workflows (a count) and
    # the share of its cores busy (percent) are not, and are listed beside
    # it; the throughput it reports and its backpressure are in its panel.
    chart_unit="wf /s",
    # Declared failures first: where series share a cell the later one is
    # drawn (ScatterPlot), so a run of zero failures never hides the small
    # rates above it, while any nonzero failure rate lands on cells of its
    # own; every series' value is also listed beside the chart.
    charts=(
        NodeDashboardChart("failed", "failed", "hot_pink_3", "x"),
        NodeDashboardChart("completed", "completed", "royal_blue", "circle_toggle"),
    ),
)


def workflow_row(progress: WorkflowProgress) -> TableRow:
    """One running workflow's row in the worker's workflow table."""
    return {
        "workflow": progress.workflow_name,
        "status": progress.status,
        "done": progress.completed_count,
        "failed": progress.failed_count,
        "rate": progress.rate_per_second,
        "cores": len(progress.assigned_cores),
    }


def total_progress(running: list[WorkflowProgress]) -> tuple[int, int, float]:
    """The running workflows' actions together: (completed, failed, rate
    per second)."""
    completed, failed, rate_per_second = 0, 0, 0.0
    for progress in running:
        completed += progress.completed_count
        failed += progress.failed_count
        rate_per_second += progress.rate_per_second

    return completed, failed, rate_per_second


def describe_address(address: tuple[str, int] | None) -> str:
    """``host:port``, or ``none`` when there is no address."""
    if address is None:
        return "none"

    host, port = address
    return f"{host}:{port}"


def describe_heartbeat_age(last_heartbeat: float | None) -> str:
    """How long ago (monotonic) the last heartbeat or ack arrived."""
    if last_heartbeat is None:
        return "never"

    return f"{time.monotonic() - last_heartbeat:.1f}s ago"


class WorkerDashboardReader:
    """Reads a worker's dashboard frame from its own state: its cores, its
    running workflows with their progress and rates, the workflows that
    have ended while the dashboard watched, and the manager it reports to
    -- and the values its chart plots: the workflows it completed and
    failed each second since the last sample; beside the chart, its active
    workflows and the share of its cores busy.

    The worker keeps no count of ended workflows, so the reader counts
    them itself: each workflow that leaves the worker's active set between
    two samples is counted by the terminal status it was left with. It
    holds only the workflows active at the last sample, so its memory is
    bounded by the worker's own.

    Every read is synchronous and local -- no await, no network -- and
    costs O(active workflows + managers).
    """

    layout = WORKER_DASHBOARD_LAYOUT

    def __init__(self, worker: WorkerServer) -> None:
        self._worker = worker
        self._identity = NodeIdentityReader(worker, "worker")
        self._last_active_workflows: dict[str, WorkflowProgress] = {}
        self._ended_statuses: Counter[str] = Counter()
        self._rates = CounterRates(self._ended_counts(), worker._clock.monotonic())

    def read(self) -> NodeDashboardFrame:
        """Sample the worker's state into one dashboard frame."""
        worker = self._worker
        active_workflows = dict(worker._active_workflows)
        self._count_ended_workflows(active_workflows)
        running = list(active_workflows.values())
        sampled_at = worker._clock.monotonic()
        core_allocator = worker._core_allocator
        rates = self._rates.advance(self._ended_counts(), sampled_at)
        return NodeDashboardFrame(
            identity_lines=self._identity.identity_lines(),
            lifecycle_state=worker._get_worker_state().name.lower(),
            uptime_seconds=self._identity.uptime_seconds(),
            cluster_lines=self._manager_lines(),
            summary_lines=self._core_lines(),
            detail_lines=self._workflow_lines(running),
            table_rows=[workflow_row(progress) for progress in running],
            chart_values=[rates["failed"], rates["completed"]],
            value_lines=[
                f"active workflows {len(running)}",
                "cores busy % "
                f"{format_reading(in_use_percent(core_allocator.total_cores, core_allocator.available_cores))}",
            ],
            sampled_at=sampled_at,
        )

    def _ended_counts(self) -> dict[str, int]:
        return {"completed": self._ended_statuses["completed"], "failed": self._ended_statuses["failed"]}

    def _count_ended_workflows(self, active_workflows: dict[str, WorkflowProgress]) -> None:
        ended_workflow_ids = self._last_active_workflows.keys() - active_workflows.keys()
        self._ended_statuses.update(
            self._last_active_workflows[workflow_id].status for workflow_id in ended_workflow_ids
        )
        self._last_active_workflows = active_workflows

    def _manager_lines(self) -> list[str]:
        worker = self._worker
        registry = worker._registry
        primary_manager_id = registry._primary_manager_id
        connection = worker._cluster_connection
        return [
            f"MANAGERS {connection.state.name.lower()}",
            f"primary {describe_address(registry.get_primary_manager_tcp_addr())}",
            f"known {len(registry._known_managers)} healthy {len(registry._healthy_manager_ids)}",
            f"last ack {describe_heartbeat_age(connection._manager_last_heartbeat.get(primary_manager_id))}",
            *self._identity.swim_lines(),
            *self._identity.health_lines(
                worker._backpressure_manager._overload_detector.current_state.name.lower()
            ),
        ]

    def _core_lines(self) -> list[str]:
        worker = self._worker
        core_allocator = worker._core_allocator
        free_cores = core_allocator.available_cores
        return [
            f"CORES {core_allocator.total_cores} free {free_cores}",
            f"allocated {core_allocator.total_cores - free_cores}",
            f"queued workflows {len(worker._pending_workflows)}",
            f"throughput {worker._worker_state._throughput_last_value:.2f} wf/s",
            f"backpressure {worker._backpressure_manager.get_max_backpressure_level().name.lower()}",
        ]

    def _workflow_lines(self, running: list[WorkflowProgress]) -> list[str]:
        ended_statuses = self._ended_statuses
        actions_completed, actions_failed, rate_per_second = total_progress(running)
        return [
            f"WORKFLOWS running {len(running)}",
            f"completed {ended_statuses['completed']}",
            f"failed {ended_statuses['failed']} cancelled {ended_statuses['cancelled']}",
            f"actions {actions_completed}",
            f"errors {actions_failed}",
            f"rate {rate_per_second:.1f}/s",
        ]
