from collections import Counter

from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes import GateServer
from hyperscale.ui.components.table.table_config import HeaderOptions

from .counter_rates import CounterRates
from .dashboard_formatting import cluster_lines, count_statuses, format_reading
from .job_outcome_tally import JobOutcomeTally
from .models import NodeDashboardChart, NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader

GATE_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="gate",
    table_headers={
        "datacenter": HeaderOptions(default="none", fixed=True),
        "health": HeaderOptions(default="-"),
        "managers": HeaderOptions(default=0),
        "workers": HeaderOptions(default=0),
        "capacity": HeaderOptions(default=0),
        "p95 ms": HeaderOptions(default="-", precision_format=".1f"),
    },
    # Jobs admitted, completed and failed are all jobs a second: one axis.
    # The datacenters accepting jobs (a count) and the worst datacenter's
    # dispatch round trip (milliseconds) are not, and are listed beside it.
    chart_unit="jobs /s",
    # Declared failures first: where series share a cell the later one is
    # drawn (ScatterPlot), so a run of zero failures never hides the small
    # rates above it, while any nonzero failure rate lands on cells of its
    # own; every series' value is also listed beside the chart.
    charts=(
        NodeDashboardChart("failed", "failed", "hot_pink_3", "x"),
        NodeDashboardChart("completed", "completed", "royal_blue", "circle_toggle"),
        NodeDashboardChart("admitted", "admitted", "aquamarine_2", "dot"),
    ),
)

# DatacenterHealth values a gate routes new jobs to.
ACCEPTING_DATACENTER_HEALTH = ("healthy", "busy")
# JobStatus values grouped as the dashboard counts them.
PENDING_JOB_STATUSES = ("submitted", "queued", "dispatching")
RUNNING_JOB_STATUSES = ("running", "completing")
COMPLETED_JOB_STATUSES = ("completed",)
FAILED_JOB_STATUSES = ("failed", "timeout")


def datacenter_row(status: DatacenterStatus, dispatch_p95_by_datacenter: dict[str, float]) -> TableRow:
    """One datacenter's row in the gate's datacenter table: its dispatch
    round trip p95 (D-5) is left at the column's default until one of its
    managers reports one."""
    row: TableRow = {
        "datacenter": status.dc_id,
        "health": status.health,
        "managers": status.manager_count,
        "workers": status.worker_count,
        "capacity": status.available_capacity,
    }
    if (dispatch_p95_ms := dispatch_p95_by_datacenter.get(status.dc_id)) is not None:
        row["p95 ms"] = dispatch_p95_ms

    return row


def dispatch_p95_by_datacenter(gate: GateServer, datacenter_ids: list[str]) -> dict[str, float]:
    """Each datacenter's dispatch round trip p95 over its AD-42 SLO windows
    (D-5), from its freshest manager heartbeat with samples -- the value
    the gate's health classification and routing read. A datacenter none
    of whose managers has reported samples is left out."""
    runtime_state = gate._modular_state
    return {
        datacenter_id: heartbeat.slo_p95_ms
        for datacenter_id in datacenter_ids
        if (heartbeat := runtime_state.get_dc_slo_heartbeat(datacenter_id)) is not None
    }


def outcome_counts(outcome_tally: JobOutcomeTally) -> dict[str, int]:
    """The gate's cumulative job counters its rate charts plot."""
    return {
        "admitted": outcome_tally.admitted_total,
        "completed": outcome_tally.completed_total,
        "failed": outcome_tally.failed_total,
    }


def job_statuses(gate: GateServer) -> dict[str, str]:
    """Each job the gate holds, by its status."""
    return {job_id: job.status for job_id, job in gate._job_manager.items()}


def sum_metrics_with_prefix(metrics: dict[str, int], prefix: str) -> int:
    """The total of the ``kind:label`` counters of one kind."""
    return sum(count for key, count in metrics.items() if key.startswith(prefix))


class GateDashboardReader:
    """Reads a gate's dashboard frame from its own state: the datacenters
    it routes to and their health, their managers, its jobs by status, its
    forwarding rate and routing decisions, its peer gates and the jobs it
    leads -- and the values its charts plot: jobs admitted, completed and
    failed (each per second since the last sample), the datacenters
    accepting new jobs, and the worst datacenter's dispatch round trip p95
    (D-5). Each datacenter's own p95 is in its table row.

    Every read is synchronous and local -- no await, no network -- and
    costs O(datacenter managers + jobs). A datacenter's health is the
    gate's own heartbeat classification (``DatacenterHealthManager.
    get_datacenter_health``), the one its routing reads.
    """

    layout = GATE_DASHBOARD_LAYOUT

    def __init__(self, gate: GateServer) -> None:
        self._gate = gate
        self._identity = NodeIdentityReader(gate, "gate")
        self._outcome_tally = JobOutcomeTally(COMPLETED_JOB_STATUSES, FAILED_JOB_STATUSES)
        self._outcome_tally.advance(job_statuses(gate))
        self._rates = CounterRates(outcome_counts(self._outcome_tally), gate._clock.monotonic())

    def read(self) -> NodeDashboardFrame:
        """Sample the gate's state into one dashboard frame."""
        gate = self._gate
        sampled_at = gate._clock.monotonic()
        datacenter_statuses = self._datacenter_statuses()
        dispatch_p95s = dispatch_p95_by_datacenter(gate, [status.dc_id for status in datacenter_statuses])
        return NodeDashboardFrame(
            identity_lines=self._identity.identity_lines(),
            lifecycle_state=gate._modular_state.get_gate_state().name.lower(),
            uptime_seconds=self._identity.uptime_seconds(),
            cluster_lines=cluster_lines(
                gate._cluster_membership,
                [
                    *self._identity.swim_lines(),
                    *self._identity.health_lines(gate._overload_detector.current_state.name.lower()),
                ],
            ),
            summary_lines=self._datacenter_lines(datacenter_statuses),
            detail_lines=self._job_lines(),
            table_rows=[datacenter_row(status, dispatch_p95s) for status in datacenter_statuses],
            chart_values=self._chart_values(sampled_at),
            value_lines=self._value_lines(datacenter_statuses, dispatch_p95s),
            sampled_at=sampled_at,
        )

    def _chart_values(self, sampled_at: float) -> list[float | None]:
        self._outcome_tally.advance(job_statuses(self._gate))
        rates = self._rates.advance(outcome_counts(self._outcome_tally), sampled_at)
        return [rates["failed"], rates["completed"], rates["admitted"]]

    def _value_lines(self, datacenter_statuses: list[DatacenterStatus], dispatch_p95s: dict[str, float]) -> list[str]:
        health_counts = Counter(status.health for status in datacenter_statuses)
        return [
            f"DCs accepting {count_statuses(health_counts, ACCEPTING_DATACENTER_HEALTH)}",
            f"worst DC p95 ms {format_reading(max(dispatch_p95s.values(), default=None))}",
        ]

    def _datacenter_statuses(self) -> list[DatacenterStatus]:
        gate = self._gate
        health_manager = gate._dc_health_manager
        datacenter_ids = health_manager.known_datacenters() | gate._datacenter_managers.keys()
        return [health_manager.get_datacenter_health(datacenter_id) for datacenter_id in sorted(datacenter_ids)]

    def _datacenter_lines(self, datacenter_statuses: list[DatacenterStatus]) -> list[str]:
        gate = self._gate
        runtime_state = gate._modular_state
        health_counts = Counter(status.health for status in datacenter_statuses)
        alive_managers = sum(status.manager_count for status in datacenter_statuses)
        return [
            f"DATACENTERS {len(datacenter_statuses)}",
            f"accepting {count_statuses(health_counts, ACCEPTING_DATACENTER_HEALTH)}",
            f"managers alive {alive_managers}",
            f"gates {runtime_state.get_active_peer_count()} of {len(gate._gate_peers)} up",
            f"gates dead {len(runtime_state._dead_gate_peers)}",
            f"orphaned jobs {len(runtime_state._orphaned_jobs)}",
        ]

    def _job_lines(self) -> list[str]:
        gate = self._gate
        status_counts = Counter(job.status for _, job in gate._job_manager.items())
        routing_metrics = gate._job_router.get_metrics()
        leadership_tracker = gate._job_leadership_tracker
        led_job_count = len(leadership_tracker.get_jobs_led_by(gate.node_id.full))
        return [
            f"JOBS {gate._job_manager.job_count()} leading {led_job_count}/{len(leadership_tracker)}",
            f"pending {count_statuses(status_counts, PENDING_JOB_STATUSES)} "
            f"running {count_statuses(status_counts, RUNNING_JOB_STATUSES)}",
            f"completed {status_counts['completed']} failed {count_statuses(status_counts, FAILED_JOB_STATUSES)}",
            f"cancelled {status_counts['cancelled']}",
            f"forward {gate._modular_state._forward_throughput_last_value:.1f}/s",
            f"routed {sum_metrics_with_prefix(routing_metrics, 'decision:')} "
            f"fallback {sum_metrics_with_prefix(routing_metrics, 'fallback:')}",
        ]
