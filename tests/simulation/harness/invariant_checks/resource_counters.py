"""
Resource counter consistency (SCENARIOS.md §11: ``available + reserved <=
total`` for every worker).

On the worker, ``CoreAllocator.available_cores`` caches the count of free
cores, so the stated bound tightens to an identity: free cores plus cores
assigned to workflows equal the total. Fewer means cores leaked; more
means one core counted twice.

On a manager, a worker's ``reserved_cores`` are cores dispatched but not
yet reflected in the worker's reported ``available_cores`` -- still
inside it -- so ``available + reserved`` may legitimately exceed the
total there. What holds is that each counter stays within ``[0, total]``,
and that the total is the worker's own: a manager's record of a live
worker carries that worker's allocator total. Range checks alone passed a
total derived from the reported free count (it moved with the counters it
bounded: ``cores 1 free 0`` on a two-core worker mid-job).
"""

from typing import TYPE_CHECKING

from hyperscale.distributed.jobs.core_allocator import CoreAllocator
from hyperscale.distributed.models import WorkerStatus

from tests.simulation.harness.invariant_checks.live_nodes import live_handles
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness


def resource_counter_violation(harness: "ClusterHarness") -> str:
    """The first inconsistent core counter on a worker or a manager, or ``""``."""
    return _worker_counter_violation(harness) or _manager_counter_violation(harness)


def _worker_counter_violation(harness: "ClusterHarness") -> str:
    details = [_allocator_violation(handle) for handle in live_handles(harness, ServerKind.WORKER)]
    return next(filter(None, details), "")


def _allocator_violation(handle: ServerHandle) -> str:
    allocator: CoreAllocator = handle.instance._core_allocator
    assigned_cores = _assigned_core_count(allocator)
    if allocator.available_cores + assigned_cores == allocator.total_cores:
        return ""
    return (
        f"{handle.node_id} core counters disagree: available {allocator.available_cores} "
        f"+ assigned {assigned_cores} != total {allocator.total_cores}"
    )


def _assigned_core_count(allocator: CoreAllocator) -> int:
    return sum(1 for workflow_id in allocator._core_assignments.values() if workflow_id is not None)


def _manager_counter_violation(harness: "ClusterHarness") -> str:
    worker_totals = _live_worker_totals(harness)
    details = [
        _worker_status_violation(handle.node_id, worker_status, worker_totals)
        for handle in live_handles(harness, ServerKind.MANAGER)
        for worker_status in handle.instance._worker_pool.iter_workers()
    ]
    return next(filter(None, details), "")


def _live_worker_totals(harness: "ClusterHarness") -> dict[str, int]:
    """Each live worker's allocator total, by the id managers record it under."""
    return {
        handle.instance._node_id.full: handle.instance._core_allocator.total_cores
        for handle in live_handles(harness, ServerKind.WORKER)
    }


def _worker_status_violation(node_id: str, worker_status: WorkerStatus, worker_totals: dict[str, int]) -> str:
    total_cores = worker_status.total_cores
    if (worker_total := worker_totals.get(worker_status.worker_id, total_cores)) != total_cores:
        return (
            f"{node_id} records worker {worker_status.worker_id} with total {total_cores}, "
            f"but the worker's allocator holds {worker_total}"
        )
    if 0 <= worker_status.available_cores <= total_cores and 0 <= worker_status.reserved_cores <= total_cores:
        return ""
    return (
        f"{node_id} holds out-of-range cores for worker {worker_status.worker_id}: "
        f"available {worker_status.available_cores}, reserved {worker_status.reserved_cores}, "
        f"total {total_cores}"
    )
