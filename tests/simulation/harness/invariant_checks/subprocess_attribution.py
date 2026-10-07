"""
WorkerSubprocessAttribution (simulation_framework.md §12).

Every executor subprocess belongs to exactly one worker: no PID sits in
two live workers' pools at once. A PID in two pools means two workers
share an executor or one holds a dead child whose PID the OS handed to
the other -- either way a workflow's process is attributed to the wrong
worker.

The design's stated form ("every PID in ``_executor._processes`` is in
``tracked_pids``") compares the pool with the supervisor's 1 s copy of
that same pool, so it can only fire on a spawn between two snapshots
and can never catch a misattribution; the disjointness check replaces it.
"""

from typing import TYPE_CHECKING

from tests.simulation.harness.invariant_checks.live_nodes import live_handles
from tests.simulation.harness.server_handle import ServerKind
from tests.simulation.harness.supervisor import Supervisor

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def subprocess_attribution_violation(harness: "ClusterHarness") -> str:
    """The first PID found in two live workers' pools, or ``""``."""
    owners: dict[int, str] = {}
    details = [
        _shared_pid_violation(pid, handle.node_id, owners)
        for handle in live_handles(harness, ServerKind.WORKER)
        for pid in sorted(Supervisor._snapshot_worker_pids(handle))
    ]
    return next(filter(None, details), "")


def _shared_pid_violation(pid: int, node_id: str, owners: dict[int, str]) -> str:
    previous_owner = owners.setdefault(pid, node_id)
    if previous_owner == node_id:
        return ""
    return f"executor PID {pid} is in the pools of both {previous_owner} and {node_id}"
