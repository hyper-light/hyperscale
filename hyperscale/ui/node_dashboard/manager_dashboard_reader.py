from collections import Counter

from hyperscale.distributed.jobs.dispatch_outcome import DispatchOutcome
from hyperscale.distributed.models import JobInfo, WorkerStatus
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.distributed.slo import LatencyObservation
from hyperscale.ui.components.stat_tile import StatTileReading, StatTileSecondary
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.components.table.table_config import HeaderOptions

from .counter_rates import CounterRates
from .dashboard_formatting import (
    cluster_lines,
    cohort_badge,
    cores_meter,
    count_statuses,
    format_milliseconds,
    format_reading,
    in_use_percent,
    leader_badge,
)
from .job_workflow_tally import JobWorkflowTally
from .models import NodeDashboardChart, NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader
from .status_tones import known_ratio_badges, state_badge

MANAGER_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="manager",
    table_headers={
        "worker": HeaderOptions(default="none", fixed=True),
        "state": HeaderOptions(default="-"),
        "cores": HeaderOptions(default="-"),
        "load": HeaderOptions(default="-"),
        "p95 ms": HeaderOptions(default="-", precision_format=".1f"),
    },
    table_empty_message="waiting for workers to register",
    tile_labels=("WORKERS", "CORES", "JOBS", "WORKFLOWS"),
    # Workflows dispatched, completed and failed are all workflows a
    # second: one axis. The share of cores in use is the CORES tile's
    # meter, and the dispatch round trip (milliseconds) the chart's extra
    # reading.
    chart_unit="wf /s",
    chart_reading_unit="/s",
    # Declared failures first: where series share a cell the later one is
    # drawn (ScatterPlot), so a failure never hides the small rates above
    # it; failures plot no zeros, so any failure rate shows on cells of its
    # own and a quiet node draws none. Every series' reading is in the
    # chart's legend.
    charts=(
        NodeDashboardChart("failures", "failed", "indian_red_3", "x", plots_zero=False),
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


def free_cores(worker: WorkerStatus) -> int:
    """A worker's cores neither in use nor reserved."""
    return worker.available_cores - worker.reserved_cores


def worker_row(worker: WorkerStatus, dispatch_latencies: dict[str, LatencyObservation]) -> TableRow:
    """One worker's row in the manager's worker table: its state and load
    as badges, its cores in use of its cores as a meter, and its dispatch
    round trip p95 (D-5) -- left at the column's default until one is
    observed."""
    row: TableRow = {
        "worker": describe_worker(worker),
        "state": state_badge(worker.state),
        "cores": cores_meter(worker.total_cores - free_cores(worker), worker.total_cores),
        "load": state_badge(worker.overload_state),
    }
    if (observation := dispatch_latencies.get(worker.worker_id)) is not None:
        row["p95 ms"] = observation.p95_ms

    return row


def core_counts(workers: list[WorkerStatus]) -> tuple[int, int]:
    """The workers' cores: (total, free -- neither in use nor reserved)."""
    total_cores, total_free_cores = 0, 0
    for worker in workers:
        total_cores += worker.total_cores
        total_free_cores += free_cores(worker)

    return total_cores, total_free_cores


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
        total_cores, free_core_count = core_counts(workers)
        jobs = manager._job_manager.iter_jobs()
        dispatch_latencies = manager._manager_state.get_worker_dispatch_latency_observations(sampled_at)
        datacenter_p95_ms = self._datacenter_p95_ms(sampled_at)
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
            summary_lines=self._worker_lines(total_cores, free_core_count),
            detail_lines=self._job_lines(jobs),
            table_rows=[worker_row(worker, dispatch_latencies) for worker in workers],
            chart_values=self._chart_values(jobs, sampled_at),
            value_lines=[
                f"cores in use % {format_reading(in_use_percent(total_cores, free_core_count))}",
                f"dispatch p95 ms {format_reading(datacenter_p95_ms)}",
            ],
            sampled_at=sampled_at,
            badges=self._badges(),
            tiles=[
                self._worker_tile(),
                StatTileReading(value="", meter=cores_meter(total_cores - free_core_count, total_cores)),
                self._job_tile(jobs),
                self._workflow_tile(jobs),
            ],
            chart_extra_reading=f"dispatch p95 {format_milliseconds(datacenter_p95_ms)}",
        )

    def _badges(self) -> list[StatusBadgeReading]:
        """The manager's status at a glance: the cluster's leader and its
        own membership, its peer managers and gates (where it knows any),
        its SWIM view and its local health."""
        manager = self._manager
        manager_state = manager._manager_state
        peer_metrics = manager_state.get_quorum_metrics()
        gate_metrics = manager_state.get_gate_metrics()
        return [
            leader_badge(manager._cluster_membership),
            cohort_badge(manager._cluster_membership),
            *known_ratio_badges("managers", peer_metrics["active_peer_count"], peer_metrics["known_peer_count"]),
            *known_ratio_badges("gates", gate_metrics["healthy_gate_count"], gate_metrics["known_gate_count"]),
            self._identity.swim_badge(),
            self._identity.load_badge(manager._overload_detector.current_state.name.lower()),
        ]

    def _worker_tile(self) -> StatTileReading:
        worker_metrics = self._manager._manager_state.get_worker_metrics()
        unhealthy_count = worker_metrics["unhealthy_worker_count"]
        return StatTileReading(
            value=f"{worker_metrics['worker_count'] - unhealthy_count} healthy",
            secondaries=(StatTileSecondary(f"{unhealthy_count} unhealthy", unhealthy_count, "failing"),),
        )

    def _job_tile(self, jobs: list[JobInfo]) -> StatTileReading:
        """Jobs running, then -- while nonzero -- failed, queued, done and
        cancelled, and the jobs this manager leads of those it knows."""
        status_counts = Counter(job.status for job in jobs)
        failed_count = count_statuses(status_counts, FAILED_JOB_STATUSES)
        pending_count = count_statuses(status_counts, PENDING_JOB_STATUSES)
        led_job_count = len(self._manager._leases.get_led_job_ids())
        known_leader_count = self._manager._manager_state.get_job_metrics()["job_leader_count"]
        return StatTileReading(
            value=f"{count_statuses(status_counts, RUNNING_JOB_STATUSES)} running",
            secondaries=(
                StatTileSecondary(f"{failed_count} failed", failed_count, "failing"),
                StatTileSecondary(f"{pending_count} queued", pending_count),
                StatTileSecondary(f"{status_counts['completed']} done", status_counts["completed"]),
                StatTileSecondary(f"{status_counts['cancelled']} cancelled", status_counts["cancelled"]),
                StatTileSecondary(f"leading {led_job_count}/{known_leader_count}", led_job_count),
            ),
        )

    def _workflow_tile(self, jobs: list[JobInfo]) -> StatTileReading:
        """Workflows done, then -- while nonzero -- failed, still active,
        failed dispatches and the dispatch rate."""
        manager_state = self._manager._manager_state
        workflows_total, workflows_completed, workflows_failed = count_workflows(jobs)
        active_count = workflows_total - workflows_completed - workflows_failed
        dispatch_failures = manager_state._dispatch_failure_count
        dispatch_rate = manager_state._dispatch_throughput_last_value
        return StatTileReading(
            value=f"{workflows_completed} done",
            secondaries=(
                StatTileSecondary(f"{workflows_failed} failed", workflows_failed, "failing"),
                StatTileSecondary(f"{active_count} active", active_count),
                StatTileSecondary(f"{dispatch_failures} dispatch failed", dispatch_failures, "failing"),
                StatTileSecondary(f"{dispatch_rate:.1f}/s dispatch", dispatch_rate),
            ),
        )

    def _chart_values(self, jobs: list[JobInfo], sampled_at: float) -> list[float | None]:
        self._workflow_tally.advance(jobs)
        rates = self._rates.advance(dispatch_counts(self._manager, self._workflow_tally), sampled_at)
        return [rates["failures"], rates["completions"], rates["dispatches"]]

    def _datacenter_p95_ms(self, sampled_at: float) -> float | None:
        datacenter_latency = self._manager._manager_state.get_dispatch_latency_observation(sampled_at)
        return None if datacenter_latency is None else datacenter_latency.p95_ms

    def _worker_lines(self, total_cores: int, free_core_count: int) -> list[str]:
        manager_state = self._manager._manager_state
        worker_metrics = manager_state.get_worker_metrics()
        gate_metrics = manager_state.get_gate_metrics()
        peer_metrics = manager_state.get_quorum_metrics()
        return [
            f"WORKERS {worker_metrics['worker_count']} unhealthy {worker_metrics['unhealthy_worker_count']}",
            f"cores {total_cores} free {free_core_count}",
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
