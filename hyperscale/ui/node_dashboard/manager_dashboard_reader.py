from collections import Counter

from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.models import JobInfo, WorkerStatus
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.distributed.slo import LatencyObservation
from hyperscale.ui.components.table.table_config import HeaderOptions

from .counter_rates import CounterRates
from .dashboard_formatting import cluster_lines, count_statuses, format_reading, in_use_percent
from .job_workflow_tally import JobWorkflowTally
from .models import NodeDashboardChart, NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader

MANAGER_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="manager",
    table_headers={
        "worker": HeaderOptions(default="none", fixed=True),
        "state": HeaderOptions(default="-"),
        "cores": HeaderOptions(default=0),
        "free": HeaderOptions(default=0),
        "load": HeaderOptions(default="-"),
        "p95 ms": HeaderOptions(default="-", precision_format=".1f"),
    },
    # Workflows dispatched, completed and failed are all workflows a
    # second: one axis. The share of cores in use (percent) and the
    # dispatch round trip (milliseconds) are not, and are listed beside it.
    chart_unit="wf /s",
    # Declared failures first: where series share a cell the later one is
    # drawn (ScatterPlot), so a run of zero failures never hides the small
    # rates above it, while any nonzero failure rate lands on cells of its
    # own; every series' value is also listed beside the chart.
    charts=(
        NodeDashboardChart("failures", "failed", "hot_pink_3", "x"),
        NodeDashboardChart("completions", "completed", "royal_blue", "circle_toggle"),
        NodeDashboardChart("dispatches", "dispatched", "aquamarine_2", "dot"),
    ),
)

# JobStatus values grouped as the dashboard counts them.
PENDING_JOB_STATUSES = ("submitted", "queued", "dispatching")
RUNNING_JOB_STATUSES = ("running", "completing")
FAILED_JOB_STATUSES = ("failed", "timeout")


def describe_worker(worker: WorkerStatus) -> str:
    """A worker's TCP address when its registration is known, else its id."""
    if (registration := worker.registration) is None:
        return worker.worker_id

    return f"{registration.node.host}:{registration.node.port}"


def worker_row(worker: WorkerStatus, dispatch_latencies: dict[str, LatencyObservation]) -> TableRow:
    """One worker's row in the manager's worker table: its dispatch round
    trip p95 (D-5) is left at the column's default until one is observed."""
    row: TableRow = {
        "worker": describe_worker(worker),
        "state": worker.state,
        "cores": worker.total_cores,
        "free": worker.available_cores - worker.reserved_cores,
        "load": worker.overload_state,
    }
    if (observation := dispatch_latencies.get(worker.worker_id)) is not None:
        row["p95 ms"] = observation.p95_ms

    return row


def core_counts(workers: list[WorkerStatus]) -> tuple[int, int]:
    """The workers' cores: (total, free -- neither in use nor reserved)."""
    total_cores, free_cores = 0, 0
    for worker in workers:
        total_cores += worker.total_cores
        free_cores += worker.available_cores - worker.reserved_cores

    return total_cores, free_cores


def dispatch_counts(manager: ManagerServer, workflow_tally: JobWorkflowTally) -> dict[str, int]:
    """The manager's cumulative counters its rate charts plot: dispatches
    workers accepted, and the workflows its jobs completed and failed."""
    outcome_counts = manager._dispatch.dispatch_outcome_counts()
    return {
        "dispatches": outcome_counts.get(DispatchOutcome.ACCEPTED.value, 0),
        "completions": workflow_tally.completed_total,
        "failures": workflow_tally.failed_total,
    }


def count_workflows(jobs: list[JobInfo]) -> tuple[int, int, int]:
    """The workflows of ``jobs``: (total, completed, failed)."""
    total, completed, failed = 0, 0, 0
    for job in jobs:
        total += job.workflows_total
        completed += job.workflows_completed
        failed += job.workflows_failed

    return total, completed, failed


class ManagerDashboardReader:
    """Reads a manager's dashboard frame from its own state: its workers
    and their cores, its jobs and workflows by status, its dispatch rate,
    the gates it knows and the jobs it leads -- and the values its charts
    plot: dispatches accepted, workflows completed and failed (each per
    second since the last sample), the share of worker cores in use, and
    the datacenter's dispatch round trip p95 over the AD-42 SLO windows
    (D-5, the digest its gates' health and routing read). Each worker's own
    p95 is in its table row.

    Every read is synchronous and local -- no await, no network -- and
    costs O(workers + jobs + SLO windows), the size of what the dashboard
    summarizes.
    """

    layout = MANAGER_DASHBOARD_LAYOUT

    def __init__(self, manager: ManagerServer) -> None:
        self._manager = manager
        self._identity = NodeIdentityReader(manager, "manager")
        self._workflow_tally = JobWorkflowTally()
        self._workflow_tally.advance(manager._job_manager.iter_jobs())
        self._rates = CounterRates(dispatch_counts(manager, self._workflow_tally), manager._clock.monotonic())

    def read(self) -> NodeDashboardFrame:
        """Sample the manager's state into one dashboard frame."""
        manager = self._manager
        sampled_at = manager._clock.monotonic()
        workers = list(manager._worker_pool._workers.values())
        total_cores, free_cores = core_counts(workers)
        jobs = manager._job_manager.iter_jobs()
        dispatch_latencies = manager._manager_state.get_worker_dispatch_latency_observations(sampled_at)
        return NodeDashboardFrame(
            identity_lines=self._identity.identity_lines(),
            lifecycle_state=manager._manager_state.manager_state_enum.name.lower(),
            uptime_seconds=self._identity.uptime_seconds(),
            cluster_lines=cluster_lines(
                manager._cluster_membership,
                [
                    *self._identity.swim_lines(),
                    *self._identity.health_lines(manager._overload_detector.current_state.name.lower()),
                ],
            ),
            summary_lines=self._worker_lines(total_cores, free_cores),
            detail_lines=self._job_lines(jobs),
            table_rows=[worker_row(worker, dispatch_latencies) for worker in workers],
            chart_values=self._chart_values(jobs, sampled_at),
            value_lines=[
                f"cores in use % {format_reading(in_use_percent(total_cores, free_cores))}",
                f"dispatch p95 ms {format_reading(self._datacenter_p95_ms(sampled_at))}",
            ],
            sampled_at=sampled_at,
        )

    def _chart_values(self, jobs: list[JobInfo], sampled_at: float) -> list[float | None]:
        self._workflow_tally.advance(jobs)
        rates = self._rates.advance(dispatch_counts(self._manager, self._workflow_tally), sampled_at)
        return [rates["failures"], rates["completions"], rates["dispatches"]]

    def _datacenter_p95_ms(self, sampled_at: float) -> float | None:
        datacenter_latency = self._manager._manager_state.get_dispatch_latency_observation(sampled_at)
        return None if datacenter_latency is None else datacenter_latency.p95_ms

    def _worker_lines(self, total_cores: int, free_cores: int) -> list[str]:
        manager_state = self._manager._manager_state
        worker_metrics = manager_state.get_worker_metrics()
        gate_metrics = manager_state.get_gate_metrics()
        peer_metrics = manager_state.get_quorum_metrics()
        return [
            f"WORKERS {worker_metrics['worker_count']} unhealthy {worker_metrics['unhealthy_worker_count']}",
            f"cores {total_cores} free {free_cores}",
            f"gates {gate_metrics['known_gate_count']} healthy {gate_metrics['healthy_gate_count']}",
            f"managers {peer_metrics['active_peer_count']} of {peer_metrics['known_peer_count']} up",
        ]

    def _job_lines(self, jobs: list[JobInfo]) -> list[str]:
        manager = self._manager
        status_counts = Counter(job.status for job in jobs)
        workflows_total, workflows_completed, workflows_failed = count_workflows(jobs)
        led_job_count = len(manager._leases.get_led_job_ids())
        known_leader_count = manager._manager_state.get_job_metrics()["job_leader_count"]
        return [
            f"JOBS {len(jobs)} leading {led_job_count}/{known_leader_count}",
            f"pending {count_statuses(status_counts, PENDING_JOB_STATUSES)} "
            f"running {count_statuses(status_counts, RUNNING_JOB_STATUSES)}",
            f"completed {status_counts['completed']} failed {count_statuses(status_counts, FAILED_JOB_STATUSES)}",
            f"cancelled {status_counts['cancelled']}",
            f"workflows {workflows_total} done {workflows_completed}",
            f"workflows failed {workflows_failed}",
            f"dispatch {manager._manager_state._dispatch_throughput_last_value:.1f}/s "
            f"fail {manager._manager_state._dispatch_failure_count}",
        ]
