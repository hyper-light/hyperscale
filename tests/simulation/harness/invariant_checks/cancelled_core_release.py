"""
Cancelled jobs free worker cores within budget (SCENARIOS.md §11).

Once a job is cancelled at its leader, every live worker running one of
its dispatches must give the dispatch's cores back within the worker's
own cancellation bound: one cancellation poll interval (the fallback
that finds a cancel whose push was lost), the poll's query timeout, the
wait for the cancelled workflow to stop, and one execution-update wait
(the cadence at which a running workflow notices its cancel event) --
all read from the worker's ``WorkerConfig``.

The clock runs only while the obligation can be met: no network fault,
pause or suspended subprocess is in force (a worker cut off from its
leader learns of the cancel through the orphan path, whose grace is
adaptive), and the job leader the worker knows is a live manager.
"""

from collections.abc import Callable
from typing import TYPE_CHECKING

from hyperscale.distributed.models import JobStatus
from hyperscale.distributed.models.jobs import TrackingToken

from tests.simulation.harness.invariant_checks.live_nodes import live_handles
from tests.simulation.harness.invariant_result import InvariantResult
from tests.simulation.harness.server_handle import ServerHandle, ServerKind

if TYPE_CHECKING:
    from tests.simulation.harness.cluster_harness import ClusterHarness

HoldingKey = tuple[str, str]


class CancelledCoreRelease:
    """Stateful evaluator: since when each cancelled dispatch has held its cores."""

    def __init__(self, clock: Callable[[], float]) -> None:
        self._clock = clock
        self._cancelled_job_ids: set[str] = set()
        self._held_since: dict[HoldingKey, float] = {}

    def evaluate(self, harness: "ClusterHarness") -> InvariantResult:
        """Holds while no cancelled job's dispatch kept its cores past the worker's bound."""
        if harness.faults.has_active_disruption():
            self._held_since.clear()
            return InvariantResult(holds=True)
        self._cancelled_job_ids.update(_cancelled_job_ids(harness))
        holdings = self._cancelled_holdings(harness)
        self._forget_released(_holding_keys(holdings))
        detail = self._first_overdue(holdings, self._clock())
        return InvariantResult(holds=not detail, detail=detail)

    def _forget_released(self, held_keys: set[HoldingKey]) -> None:
        self._held_since = {key: since for key, since in self._held_since.items() if key in held_keys}

    def _first_overdue(self, holdings: list[tuple[ServerHandle, str]], now: float) -> str:
        details = [self._overdue(handle, workflow_id, now) for handle, workflow_id in holdings]
        return next(filter(None, details), "")

    def _cancelled_holdings(self, harness: "ClusterHarness") -> list[tuple[ServerHandle, str]]:
        live_manager_addresses = _live_manager_tcp_addresses(harness)
        return [
            (handle, workflow_id)
            for handle in live_handles(harness, ServerKind.WORKER)
            for workflow_id in self._owed_releases(handle, live_manager_addresses)
        ]

    def _owed_releases(self, handle: ServerHandle, live_manager_addresses: set[tuple[str, int]]) -> list[str]:
        return [
            workflow_id
            for workflow_id in list(handle.instance._core_allocator._workflow_cores)
            if self._release_is_owed(handle, workflow_id, live_manager_addresses)
        ]

    def _release_is_owed(
        self,
        handle: ServerHandle,
        workflow_id: str,
        live_manager_addresses: set[tuple[str, int]],
    ) -> bool:
        leader_address = handle.instance._worker_state.get_workflow_job_leader(workflow_id)
        return _job_id_of(workflow_id) in self._cancelled_job_ids and leader_address in live_manager_addresses

    def _overdue(self, handle: ServerHandle, workflow_id: str, now: float) -> str:
        held_seconds = now - self._held_since.setdefault((handle.node_id, workflow_id), now)
        release_bound = _release_bound(handle)
        if held_seconds <= release_bound:
            return ""
        return (
            f"{handle.node_id} still holds cores for {workflow_id!r} of a cancelled job "
            f"{held_seconds:.1f}s on (worker cancellation bound {release_bound:.1f}s)"
        )


def _cancelled_job_ids(harness: "ClusterHarness") -> list[str]:
    return [
        job_id
        for handle in live_handles(harness, ServerKind.MANAGER)
        for job_id in _cancelled_jobs_on(handle)
    ]


def _cancelled_jobs_on(handle: ServerHandle) -> list[str]:
    return [
        job.job_id
        for job in handle.instance._job_manager.iter_jobs()
        if job.status == JobStatus.CANCELLED.value
    ]


def _live_manager_tcp_addresses(harness: "ClusterHarness") -> set[tuple[str, int]]:
    return {(handle.host, handle.tcp_port) for handle in live_handles(harness, ServerKind.MANAGER)}


def _holding_keys(holdings: list[tuple[ServerHandle, str]]) -> set[HoldingKey]:
    return {(handle.node_id, workflow_id) for handle, workflow_id in holdings}


def _job_id_of(workflow_id: str) -> str:
    try:
        return TrackingToken.parse(workflow_id).job_id
    except ValueError:
        return ""


def _release_bound(handle: ServerHandle) -> float:
    config = handle.instance._config
    return (
        config.cancellation_poll_interval_seconds
        + config.tcp_timeout_short_seconds
        + config.workflow_cancel_timeout_seconds
        + config.execution_update_wait_seconds
    )
