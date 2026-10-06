from collections import Counter

from hyperscale.distributed.models import JobInfo, WorkerStatus
from hyperscale.distributed.nodes import ManagerServer
from hyperscale.ui.components.table.table_config import HeaderOptions

from .dashboard_formatting import cluster_lines, count_statuses
from .models import NodeDashboardFrame, NodeDashboardLayout, TableRow
from .node_identity_reader import NodeIdentityReader

MANAGER_DASHBOARD_LAYOUT = NodeDashboardLayout(
    role="manager",
    table_headers={
        "worker": HeaderOptions(default="none", fixed=True),
        "state": HeaderOptions(default="-"),
        "cores": HeaderOptions(default=0),
        "free": HeaderOptions(default=0),
        "load": HeaderOptions(default="-"),
    },
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


def worker_row(worker: WorkerStatus) -> TableRow:
    """One worker's row in the manager's worker table."""
    return {
        "worker": describe_worker(worker),
        "state": worker.state,
        "cores": worker.total_cores,
        "free": worker.available_cores - worker.reserved_cores,
        "load": worker.overload_state,
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
    the gates it knows and the jobs it leads.

    Every read is synchronous and local -- no await, no network -- and
    costs O(workers + jobs), the size of what the dashboard summarizes.
    """

    layout = MANAGER_DASHBOARD_LAYOUT

    def __init__(self, manager: ManagerServer) -> None:
        self._manager = manager
        self._identity = NodeIdentityReader(manager, "manager")

    def read(self) -> NodeDashboardFrame:
        """Sample the manager's state into one dashboard frame."""
        manager = self._manager
        workers = list(manager._worker_pool._workers.values())
        return NodeDashboardFrame(
            identity_lines=self._identity.identity_lines(
                manager._manager_state.manager_state_enum.name.lower(),
                manager._overload_detector.current_state.name.lower(),
            ),
            cluster_lines=cluster_lines(manager._cluster_membership, self._identity.swim_lines()),
            summary_lines=self._worker_lines(workers),
            detail_lines=self._job_lines(),
            table_rows=[worker_row(worker) for worker in workers],
        )

    def _worker_lines(self, workers: list[WorkerStatus]) -> list[str]:
        manager_state = self._manager._manager_state
        worker_metrics = manager_state.get_worker_metrics()
        gate_metrics = manager_state.get_gate_metrics()
        peer_metrics = manager_state.get_quorum_metrics()
        total_cores = sum(worker.total_cores for worker in workers)
        free_cores = sum(worker.available_cores - worker.reserved_cores for worker in workers)
        return [
            f"WORKERS {worker_metrics['worker_count']} unhealthy {worker_metrics['unhealthy_worker_count']}",
            f"cores {total_cores} free {free_cores}",
            f"gates {gate_metrics['known_gate_count']} healthy {gate_metrics['healthy_gate_count']}",
            f"managers {peer_metrics['active_peer_count']} of {peer_metrics['known_peer_count']} up",
        ]

    def _job_lines(self) -> list[str]:
        manager = self._manager
        jobs = manager._job_manager.iter_jobs()
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
