from collections import Counter

from hyperscale.distributed.models import DatacenterStatus
from hyperscale.distributed.nodes import GateServer
from hyperscale.ui.components.table.table_config import HeaderOptions

from .dashboard_formatting import cluster_lines, count_statuses
from .models import NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader

GATE_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="gate",
    table_headers={
        "datacenter": HeaderOptions(default="none", fixed=True),
        "health": HeaderOptions(default="-"),
        "managers": HeaderOptions(default=0),
        "workers": HeaderOptions(default=0),
        "capacity": HeaderOptions(default=0),
    },
)

# DatacenterHealth values a gate routes new jobs to.
ACCEPTING_DATACENTER_HEALTH = ("healthy", "busy")
# JobStatus values grouped as the dashboard counts them.
PENDING_JOB_STATUSES = ("submitted", "queued", "dispatching")
RUNNING_JOB_STATUSES = ("running", "completing")
FAILED_JOB_STATUSES = ("failed", "timeout")


def datacenter_row(status: DatacenterStatus) -> TableRow:
    """One datacenter's row in the gate's datacenter table."""
    return {
        "datacenter": status.dc_id,
        "health": status.health,
        "managers": status.manager_count,
        "workers": status.worker_count,
        "capacity": status.available_capacity,
    }


def sum_metrics_with_prefix(metrics: dict[str, int], prefix: str) -> int:
    """The total of the ``kind:label`` counters of one kind."""
    return sum(count for key, count in metrics.items() if key.startswith(prefix))


class GateDashboardReader:
    """Reads a gate's dashboard frame from its own state: the datacenters
    it routes to and their health, their managers, its jobs by status, its
    forwarding rate and routing decisions, its peer gates and the jobs it
    leads.

    Every read is synchronous and local -- no await, no network -- and
    costs O(datacenter managers + jobs). A datacenter's health is the
    gate's own heartbeat classification (``DatacenterHealthManager.
    get_datacenter_health``), the one its routing reads.
    """

    layout = GATE_DASHBOARD_LAYOUT

    def __init__(self, gate: GateServer) -> None:
        self._gate = gate
        self._identity = NodeIdentityReader(gate, "gate")

    def read(self) -> NodeDashboardFrame:
        """Sample the gate's state into one dashboard frame."""
        gate = self._gate
        datacenter_statuses = self._datacenter_statuses()
        return NodeDashboardFrame(
            identity_lines=self._identity.identity_lines(
                gate._modular_state.get_gate_state().name.lower(),
                gate._overload_detector.current_state.name.lower(),
            ),
            cluster_lines=cluster_lines(gate._cluster_membership, self._identity.swim_lines()),
            summary_lines=self._datacenter_lines(datacenter_statuses),
            detail_lines=self._job_lines(),
            table_rows=[datacenter_row(status) for status in datacenter_statuses],
        )

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
