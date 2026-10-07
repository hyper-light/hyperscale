"""
NoOrphanWorkflows (simulation_framework.md §12): every workflow a live
worker runs has a known job leader, and that leader is a manager of this
cluster.

The worker records a workflow's leader in the same step that makes the
workflow active (``WorkerState.add_active_workflow``) and forgets both
together, so the check is exact at every tick. A leader that has died is
still known: the worker's orphan grace (adaptive, AD-26) owns what
happens next, and ``JobMakesProgress`` bounds the job.
"""

from typing import TYPE_CHECKING

from tests.simulation.harness.invariant_checks.live_nodes import handles_of_kind, live_handles
from tests.simulation.harness.server_handle import ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def orphan_workflow_violation(harness: "ClusterHarness") -> str:
    """The first workflow running without a known cluster job leader, or ``""``."""
    manager_addresses = _manager_tcp_addresses(harness)
    details = [
        _leader_violation(
            handle.node_id,
            workflow_id,
            handle.instance._worker_state.get_workflow_job_leader(workflow_id),
            manager_addresses,
        )
        for handle in live_handles(harness, ServerKind.WORKER)
        for workflow_id in list(handle.instance._worker_state._active_workflows)
    ]
    return next(filter(None, details), "")


def _manager_tcp_addresses(harness: "ClusterHarness") -> set[tuple[str, int]]:
    return {
        (handle.host, handle.tcp_port) for handle in handles_of_kind(harness, ServerKind.MANAGER)
    }


def _leader_violation(
    node_id: str,
    workflow_id: str,
    leader_address: tuple[str, int] | None,
    manager_addresses: set[tuple[str, int]],
) -> str:
    if leader_address in manager_addresses:
        return ""
    if leader_address is None:
        return f"{node_id} runs workflow {workflow_id!r} with no known job leader"
    return (
        f"{node_id} runs workflow {workflow_id!r} under job leader "
        f"{leader_address} that is no manager of this cluster"
    )
