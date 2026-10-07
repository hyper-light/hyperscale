from collections import Counter

from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes import GateServer
from hyperscale.ui.components.meter import MeterReading
from hyperscale.ui.components.stat_tile import StatTileReading, StatTileSecondary
from hyperscale.ui.components.status_badge import StatusBadgeReading
from hyperscale.ui.components.table.table_config import HeaderOptions
from hyperscale.ui.styling.tones import StatusTone

from .counter_rates import CounterRates
from .dashboard_formatting import (
    cluster_lines,
    cores_meter,
    count_statuses,
    format_milliseconds,
    format_reading,
    leader_badge,
)
from .job_outcome_tally import JobOutcomeTally
from .models import NodeDashboardChart, NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader
from .status_tones import joined_label, nonzero_counts, state_badge

GATE_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="gate",
    table_headers={
        "datacenter": HeaderOptions(default="none", fixed=True),
        "health": HeaderOptions(default="-"),
        "managers": HeaderOptions(default=0),
        "workers": HeaderOptions(default=0),
        "cores": HeaderOptions(default="-"),
        "p95 ms": HeaderOptions(default="-", precision_format=".1f"),
    },
    table_empty_message="waiting for datacenters to report",
    tile_labels=("JOBS", "DATACENTERS", "FORWARDING", "ORPHANS"),
    # Jobs admitted, completed and failed are all jobs a second: one axis.
    # The datacenters accepting jobs (a count) are a badge and the worst
    # datacenter's dispatch round trip (milliseconds) the chart's extra
    # reading.
    chart_unit="jobs /s",
    chart_reading_unit="/s",
    # Declared failures first: where series share a cell the later one is
    # drawn (ScatterPlot), so a failure never hides the small rates above
    # it; failures plot no zeros, so any failure rate shows on cells of its
    # own and a quiet node draws none. Every series' reading is in the
    # chart's legend.
    charts=(
        NodeDashboardChart("failed", "failed", "indian_red_3", "x", plots_zero=False),
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


# DatacenterHealth values a gate routes no job to while it has another.
HEALTHY_DATACENTER_HEALTH = ("healthy",)


def datacenter_row(
    status: DatacenterStatus,
    total_cores: int,
    dispatch_p95_by_datacenter: dict[str, float],
) -> TableRow:
    """One datacenter's row in the gate's datacenter table: its health as a
    badge, its cores in use of ``total_cores`` (its freshest manager
    heartbeat's) as a meter -- a datacenter's available capacity is its
    free cores -- and its dispatch round trip p95 (D-5), left at the
    column's default until one of its managers reports one."""
    row: TableRow = {
        "datacenter": status.dc_id,
        "health": state_badge(status.health),
        "managers": status.manager_count,
        "workers": status.worker_count,
        "cores": cores_meter(max(total_cores - status.available_capacity, 0), total_cores),
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


def datacenter_total_cores(gate: GateServer, datacenter_id: str) -> int:
    """A datacenter's cores, from its freshest manager heartbeat (the one
    its health is classified from); 0 before any manager reports."""
    heartbeat, _, _ = gate._dc_health_manager.get_best_manager_heartbeat(datacenter_id)
    return 0 if heartbeat is None else heartbeat.total_cores


def quorum_tone(up_count: int, cluster_size: int) -> StatusTone:
    """A majority of the gates up holds a quorum; fewer do not."""
    return "ok" if 2 * up_count > cluster_size else "failing"


def accepting_tone(accepting_count: int, datacenter_count: int) -> StatusTone:
    """Every datacenter accepting jobs is as expected, some worth a look,
    none in trouble."""
    if accepting_count < 1:
        return "failing"

    return "ok" if accepting_count >= datacenter_count else "degraded"


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
            table_rows=[
                datacenter_row(status, datacenter_total_cores(gate, status.dc_id), dispatch_p95s)
                for status in datacenter_statuses
            ],
            chart_values=self._chart_values(sampled_at),
            value_lines=self._value_lines(datacenter_statuses, dispatch_p95s),
            sampled_at=sampled_at,
            badges=self._badges(datacenter_statuses),
            tiles=[
                self._job_tile(),
                self._datacenter_tile(datacenter_statuses),
                self._forwarding_tile(),
                self._orphan_tile(),
            ],
            chart_extra_reading=f"worst DC p95 {format_milliseconds(max(dispatch_p95s.values(), default=None))}",
        )

    def _badges(self, datacenter_statuses: list[DatacenterStatus]) -> list[StatusBadgeReading]:
        """The gate's status at a glance: the cluster's leader, the gates'
        quorum (the gate cluster's membership), the datacenters accepting
        jobs, its SWIM view and its local health."""
        gate = self._gate
        health_counts = Counter(status.health for status in datacenter_statuses)
        accepting_count = count_statuses(health_counts, ACCEPTING_DATACENTER_HEALTH)
        return [
            leader_badge(gate._cluster_membership),
            self._quorum_badge(),
            StatusBadgeReading(
                f"DCs accepting {accepting_count}/{len(datacenter_statuses)}",
                accepting_tone(accepting_count, len(datacenter_statuses)),
            ),
            self._identity.swim_badge(),
            self._identity.load_badge(gate._overload_detector.current_state.name.lower()),
        ]

    def _quorum_badge(self) -> StatusBadgeReading:
        """The gates up (this one among them) of the gate cluster, and the
        peers it holds dead while any are."""
        gate = self._gate
        runtime_state = gate._modular_state
        cluster_size = len(gate._gate_peers) + 1
        up_count = runtime_state.get_active_peer_count() + 1
        dead_gates = nonzero_counts(((len(runtime_state._dead_gate_peers), "dead"),))
        return StatusBadgeReading(
            joined_label([f"quorum {up_count}/{cluster_size}", *dead_gates]), quorum_tone(up_count, cluster_size)
        )

    def _job_tile(self) -> StatTileReading:
        """Jobs running, then -- while nonzero -- failed, queued, done and
        cancelled, and the jobs this gate leads of those it tracks."""
        gate = self._gate
        status_counts = Counter(job.status for _, job in gate._job_manager.items())
        failed_count = count_statuses(status_counts, FAILED_JOB_STATUSES)
        pending_count = count_statuses(status_counts, PENDING_JOB_STATUSES)
        leadership_tracker = gate._job_leadership_tracker
        led_job_count = len(leadership_tracker.get_jobs_led_by(gate.node_id.full))
        return StatTileReading(
            value=f"{count_statuses(status_counts, RUNNING_JOB_STATUSES)} running",
            secondaries=(
                StatTileSecondary(f"{failed_count} failed", failed_count, "failing"),
                StatTileSecondary(f"{pending_count} queued", pending_count),
                StatTileSecondary(f"{status_counts['completed']} done", status_counts["completed"]),
                StatTileSecondary(f"{status_counts['cancelled']} cancelled", status_counts["cancelled"]),
                StatTileSecondary(f"leading {led_job_count}/{len(leadership_tracker)}", led_job_count),
            ),
        )

    def _datacenter_tile(self, datacenter_statuses: list[DatacenterStatus]) -> StatTileReading:
        """The datacenters healthy of all, as a meter (each one's managers
        alive are in its table row)."""
        health_counts = Counter(status.health for status in datacenter_statuses)
        healthy_count = count_statuses(health_counts, HEALTHY_DATACENTER_HEALTH)
        return StatTileReading(
            value="",
            meter=MeterReading(
                used=healthy_count,
                total=len(datacenter_statuses),
                label=f"{healthy_count}/{len(datacenter_statuses)} healthy",
            ),
        )

    def _forwarding_tile(self) -> StatTileReading:
        """The jobs forwarded each second, then -- while nonzero -- the
        routing decisions made and the fallbacks among them."""
        gate = self._gate
        routing_metrics = gate._job_router.get_metrics()
        routed_count = sum_metrics_with_prefix(routing_metrics, "decision:")
        fallback_count = sum_metrics_with_prefix(routing_metrics, "fallback:")
        return StatTileReading(
            value=f"{gate._modular_state._forward_throughput_last_value:.1f}/s",
            secondaries=(
                StatTileSecondary(f"{fallback_count} fallback", fallback_count, "degraded"),
                StatTileSecondary(f"{routed_count} routed", routed_count),
            ),
        )

    def _orphan_tile(self) -> StatTileReading:
        """The jobs orphaned by a lost peer: in trouble while any are."""
        orphan_count = len(self._gate._modular_state._orphaned_jobs)
        return StatTileReading(value=f"{orphan_count}", value_tone="failing" if orphan_count > 0 else None)

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
