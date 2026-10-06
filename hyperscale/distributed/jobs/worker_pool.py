"""
Worker Pool - Thread-safe worker registration and resource management.

This class encapsulates all worker-related state and operations with proper
synchronization. It provides race-condition safe access to worker data
and core allocation.

Key responsibilities:
- Worker registration and deregistration
- Health tracking (integrates with SWIM and three-signal model AD-19)
- Core availability tracking and allocation
- Worker selection for workflow dispatch
"""

import asyncio
from types import MappingProxyType
from typing import Callable

from hyperscale.distributed.models import (
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
    WorkerStatus,
)
from hyperscale.distributed.models.worker_state import WorkerStateUpdate
from hyperscale.distributed.health import (
    WorkerHealthState,
    WorkerHealthConfig,
    RoutingDecision,
)
from hyperscale.distributed.jobs.worker_dispatch_routing_state import (
    WorkerDispatchRoutingState,
)
from hyperscale.distributed.jobs.worker_drain_intent import WorkerDrainIntent
from hyperscale.distributed.jobs.logging_models import (
    WorkerPoolTrace,
    WorkerPoolDebug,
    WorkerPoolInfo,
    WorkerPoolWarning,
    WorkerPoolError,
    WorkerPoolCritical,
)
from hyperscale.logging import Logger

from hyperscale.distributed.runtime import Clock, RealClock
from hyperscale.distributed.models import NodeInfo


_DEFAULT_CLOCK: Clock = RealClock()


# The bucket a healthy worker's overload state puts it in; a state not
# listed is HEALTHY.
_BUCKET_BY_OVERLOAD_STATE: MappingProxyType[str, str] = MappingProxyType(
    {
        "healthy": "HEALTHY",
        "busy": "BUSY",
        "stressed": "DEGRADED",
        "overloaded": "UNHEALTHY",
    }
)


# Re-export for backwards compatibility
WorkerInfo = WorkerStatus
WorkerHealth = WorkerState


class WorkerPool:
    """
    Thread-safe worker pool management.

    Manages worker registration, health tracking, and core allocation.
    Uses locks to ensure race-condition safe access when multiple
    workflows are being dispatched concurrently.
    """

    def __init__(
        self,
        health_grace_period: float = 30.0,
        get_swim_status: Callable[[tuple[str, int]], str | None] | None = None,
        manager_id: str = "",
        datacenter: str = "",
        dispatch_failure_base_cooldown_seconds: float = 0.25,
        dispatch_failure_max_cooldown_seconds: float = 5.0,
        dispatch_readiness_cooldown_seconds: float = 0.5,
    ):
        """
        Initialize WorkerPool.

        Args:
            health_grace_period: Seconds to consider a worker healthy after registration
                                 before SWIM status is available
            get_swim_status: Optional callback to get SWIM health status for a worker
                            Returns 'OK', 'SUSPECT', 'DEAD', or None
            manager_id: Manager node ID for log context
            datacenter: Datacenter identifier for log context
            dispatch_failure_base_cooldown_seconds: Base routing cooldown for TCP
                                                   dispatch transport failures
            dispatch_failure_max_cooldown_seconds: Maximum routing cooldown for TCP
                                                  dispatch transport failures
            dispatch_readiness_cooldown_seconds: Routing cooldown for worker-side
                                                 readiness rejections
        """
        self._health_grace_period = health_grace_period
        self._get_swim_status = get_swim_status
        self._manager_id = manager_id
        self._datacenter = datacenter
        self._dispatch_failure_base_cooldown_seconds = (
            dispatch_failure_base_cooldown_seconds
        )
        self._dispatch_failure_max_cooldown_seconds = (
            dispatch_failure_max_cooldown_seconds
        )
        self._dispatch_readiness_cooldown_seconds = dispatch_readiness_cooldown_seconds
        self._logger = Logger()

        # Worker storage - node_id -> WorkerStatus
        self._workers: dict[str, WorkerStatus] = {}

        # Cores reserved per dispatch, per worker: dispatch (sub-workflow
        # token) -> (job, cores, the worker's core availability version
        # once it allocated them, or None until its ack says). Every report
        # of a worker's free cores -- heartbeat, progress, result -- carries
        # that version, so a reservation stands exactly until the free
        # count applied here reflects its dispatch: a report at or past the
        # version it was allocated at (running or already finished), or
        # one listing or reporting on the dispatch. Or until the dispatch
        # fails, the worker re-registers or leaves, or its job ends here.
        # ``WorkerStatus.reserved_cores`` is their sum.
        self._dispatch_reservations: dict[str, dict[str, tuple[str, int, int | None]]] = {}
        # Per worker, the version of the free count applied last: a report
        # older than it (reordered in flight) never overwrites it.
        self._applied_cores_versions: dict[str, int] = {}

        # Three-signal health state tracking (AD-19)
        self._worker_health: dict[str, WorkerHealthState] = {}
        self._health_config = WorkerHealthConfig()
        self._dispatch_routing: dict[str, WorkerDispatchRoutingState] = {}
        self._drain_intents: dict[str, WorkerDrainIntent] = {}
        self._drain_epoch: int = 0

        # Quick lookup by address
        self._addr_to_worker: dict[tuple[str, int], str] = {}

        # Remote worker tracking (AD-48)
        self._remote_workers: dict[str, WorkerStatus] = {}
        self._remote_addr_to_worker: dict[tuple[str, int], str] = {}

        # Lock for worker registration/deregistration
        self._registration_lock = asyncio.Lock()

        # Lock for core allocation (separate from registration)
        self._allocation_lock = asyncio.Lock()

        # Condition for waiting on cores (uses allocation lock for atomic wait)
        self._cores_condition = asyncio.Condition(self._allocation_lock)

        # Moves, under the condition, with every notify of it. A dispatcher
        # that reads it before an allocation attempt waits for it to move
        # (``wait_for_capacity_change``), not for cores to exist: a change
        # landing between the attempt and the wait is never lost, and cores
        # the attempt could not use never wake it in a loop.
        self.capacity_generation: int = 0

    # =========================================================================
    # Worker Registration
    # =========================================================================

    async def register_worker(
        self,
        registration: WorkerRegistration,
    ) -> WorkerStatus:
        """
        Register a new worker or update existing registration.

        Thread-safe: uses registration lock.
        """
        async with self._registration_lock:
            node_id = registration.node.node_id

            # Evict any DIFFERENT node_id currently holding this TCP
            # address (mirrors the registry's same-addr eviction): a
            # socket address is a singleton, so the previous entry is a
            # dead generation whose SWIM death detection lagged the
            # restart. Without this the pool accumulates one stale
            # entry per restart cycle — and allocation could select
            # the stale id (its cached cores look free), whose dispatch
            # then dies on the registry miss while the retry loop
            # re-picks it forever (measured: a fresh job accepted
            # around a worker power-cycle stranded to the AD-34
            # timeout against an idle, healthy gen-2).
            new_addr = (registration.node.host, registration.node.port)
            self._evict_stale_worker_at_addr(node_id, new_addr)

            # Check if already registered
            if node_id in self._workers:
                worker = self._reregister_worker(node_id, registration)

            else:
                worker = self._register_new_worker(node_id, registration)

        # Signal outside registration lock to avoid nested lock acquisition
        async with self._cores_condition:
            self.capacity_generation += 1
            self._cores_condition.notify_all()

        return worker

    def _evict_stale_worker_at_addr(self, node_id: str, new_addr: tuple[str, int]) -> None:
        """Under the registration lock: forget a different node id that
        holds the registering worker's address -- a dead generation."""
        stale_node_id = self._addr_to_worker.get(new_addr)
        if stale_node_id is not None and stale_node_id != node_id:
            self._workers.pop(stale_node_id, None)
            self._dispatch_reservations.pop(stale_node_id, None)
            self._applied_cores_versions.pop(stale_node_id, None)
            self._worker_health.pop(stale_node_id, None)
            self._dispatch_routing.pop(stale_node_id, None)
            self._drain_intents.pop(stale_node_id, None)
            self._addr_to_worker.pop(new_addr, None)

    @staticmethod
    def _cores_or_zero(cores: int | None) -> int:
        """A registration's core count, zero when it reports none."""
        return cores or 0

    def _refresh_registration_readiness(
        self,
        health_state: WorkerHealthState,
        drain_intended: bool,
        registration: WorkerRegistration,
    ) -> None:
        """AD-19 readiness from a registration: accepting with its free
        cores, unless a drain is intended."""
        health_state.update_readiness(
            accepting=not drain_intended,
            capacity=(
                0
                if drain_intended
                else self._cores_or_zero(registration.available_cores)
            ),
        )

    def _reregister_worker(self, node_id: str, registration: WorkerRegistration) -> WorkerStatus:
        """Under the registration lock: take a known worker's new
        registration, which reports its cores from scratch."""
        worker = self._workers[node_id]
        drain_intended = self.is_worker_drain_intended(node_id)
        if worker.registration:
            old_addr = (
                worker.registration.node.host,
                worker.registration.node.port,
            )
            self._addr_to_worker.pop(old_addr, None)

        self._reset_worker_from_registration(node_id, worker, registration, drain_intended)

        health_state = self._worker_health.get(node_id)
        if health_state:
            health_state.update_liveness(success=True)
            self._refresh_registration_readiness(health_state, drain_intended, registration)

        self._get_or_create_dispatch_routing_state(node_id).record_success()

        addr = (registration.node.host, registration.node.port)
        self._addr_to_worker[addr] = node_id
        return worker

    def _reset_worker_from_registration(
        self,
        node_id: str,
        worker: WorkerStatus,
        registration: WorkerRegistration,
        drain_intended: bool,
    ) -> None:
        """Under the registration lock: reset a known worker's cores and
        state from its new registration."""
        worker.registration = registration
        worker.last_seen = _DEFAULT_CLOCK.monotonic()
        worker.total_cores = self._cores_or_zero(registration.total_cores)
        worker.available_cores = self._cores_or_zero(registration.available_cores)
        # A (re-)registration reports from scratch: nothing it has
        # not seen is in flight to it any more.
        worker.reserved_cores = 0
        self._dispatch_reservations.pop(node_id, None)
        self._applied_cores_versions.pop(node_id, None)
        worker.health = (
            WorkerState.DRAINING if drain_intended else WorkerState.HEALTHY
        )

    def _register_new_worker(self, node_id: str, registration: WorkerRegistration) -> WorkerStatus:
        """Under the registration lock: track a worker registering for the
        first time, with its AD-19 health state."""
        drain_intended = self.is_worker_drain_intended(node_id)

        # Create new worker status
        worker = WorkerStatus(
            worker_id=node_id,
            state=(
                WorkerState.DRAINING.value
                if drain_intended
                else WorkerState.HEALTHY.value
            ),
            registration=registration,
            last_seen=_DEFAULT_CLOCK.monotonic(),
            total_cores=self._cores_or_zero(registration.total_cores),
            available_cores=self._cores_or_zero(registration.available_cores),
        )

        self._workers[node_id] = worker

        # Initialize three-signal health state (AD-19)
        health_state = WorkerHealthState(
            worker_id=node_id,
            config=self._health_config,
        )
        health_state.update_liveness(success=True)
        self._refresh_registration_readiness(health_state, drain_intended, registration)
        self._worker_health[node_id] = health_state
        self._get_or_create_dispatch_routing_state(node_id).record_success()

        # Add address lookup
        addr = (registration.node.host, registration.node.port)
        self._addr_to_worker[addr] = node_id
        return worker

    async def deregister_worker(self, node_id: str) -> bool:
        """
        Remove a worker from the pool.

        Thread-safe: uses registration lock.
        Returns True if worker was removed, False if not found.
        """
        async with self._registration_lock:
            worker = self._workers.pop(node_id, None)
            if not worker:
                return False

            # Remove health state tracking
            self._dispatch_reservations.pop(node_id, None)
            self._applied_cores_versions.pop(node_id, None)
            self._worker_health.pop(node_id, None)
            self._dispatch_routing.pop(node_id, None)
            self._drain_intents.pop(node_id, None)

            # Remove address lookup
            if worker.registration:
                addr = (worker.registration.node.host, worker.registration.node.port)
                self._addr_to_worker.pop(addr, None)

            return True

    def get_worker(self, node_id: str) -> WorkerStatus | None:
        """Get worker info by node ID."""
        return self._workers.get(node_id)

    def get_worker_by_addr(self, addr: tuple[str, int]) -> WorkerStatus | None:
        """Get worker info by (host, port) address."""
        node_id = self._addr_to_worker.get(addr)
        if node_id:
            return self._workers.get(node_id)
        return None

    def iter_workers(self) -> list[WorkerStatus]:
        """Get a snapshot of all workers."""
        return list(self._workers.values())

    # =========================================================================
    # Health Tracking
    # =========================================================================

    def _get_or_create_dispatch_routing_state(
        self,
        node_id: str,
    ) -> WorkerDispatchRoutingState:
        routing_state = self._dispatch_routing.get(node_id)
        if routing_state is None:
            routing_state = WorkerDispatchRoutingState(
                worker_id=node_id,
                base_cooldown_seconds=self._dispatch_failure_base_cooldown_seconds,
                max_cooldown_seconds=self._dispatch_failure_max_cooldown_seconds,
            )
            self._dispatch_routing[node_id] = routing_state

        return routing_state

    def mark_worker_draining_immediate(self, node_id: str, reason: str) -> bool:
        """Synchronously mark a worker as non-routable."""
        worker = self._workers.get(node_id)
        if worker is None:
            return False

        self._drain_epoch += 1
        self._drain_intents[node_id] = WorkerDrainIntent(
            worker_id=node_id,
            epoch=self._drain_epoch,
            reason=reason,
        )
        worker.health = WorkerState.DRAINING

        if health_state := self._worker_health.get(node_id):
            health_state.update_readiness(accepting=False, capacity=0)

        return True

    async def mark_workers_draining(
        self,
        worker_ids: set[str],
        reason: str,
    ) -> set[str]:
        """Mark workers as draining and wake dispatch waiters."""
        marked_worker_ids: set[str] = set()

        async with self._cores_condition:
            self._drain_epoch += 1
            drain_epoch = self._drain_epoch

            for node_id in worker_ids:
                self._mark_worker_draining(node_id, drain_epoch, reason, marked_worker_ids)

            if marked_worker_ids:
                self.capacity_generation += 1
                self._cores_condition.notify_all()

        return marked_worker_ids

    def _mark_worker_draining(
        self,
        node_id: str,
        drain_epoch: int,
        reason: str,
        marked_worker_ids: set[str],
    ) -> None:
        """Under the allocation lock: record a registered worker's drain
        intent at ``drain_epoch`` and stop routing new work to it."""
        worker = self._workers.get(node_id)
        if worker is None:
            return

        self._drain_intents[node_id] = WorkerDrainIntent(
            worker_id=node_id,
            epoch=drain_epoch,
            reason=reason,
        )
        worker.health = WorkerState.DRAINING

        if health_state := self._worker_health.get(node_id):
            health_state.update_readiness(accepting=False, capacity=0)

        marked_worker_ids.add(node_id)

    def is_worker_drain_intended(self, node_id: str) -> bool:
        """Return whether the manager has explicit drain intent for a worker."""
        return node_id in self._drain_intents

    def record_dispatch_success(self, node_id: str) -> bool:
        """
        Clear dispatch routing cooldown after a successful workflow dispatch.

        This does not mutate SWIM or lifecycle health.
        """
        if node_id not in self._workers:
            return False

        self._get_or_create_dispatch_routing_state(node_id).record_success()
        return True

    def record_dispatch_transport_failure(
        self,
        node_id: str,
        error: str = "",
    ) -> bool:
        """
        Temporarily cool down workflow dispatch routing after a TCP failure.

        This intentionally does not change SWIM membership or worker lifecycle
        state. It only prevents the allocator from repeatedly selecting a
        worker whose dispatch path is currently failing.
        """
        if node_id not in self._workers:
            return False

        self._get_or_create_dispatch_routing_state(node_id).record_failure(
            error=error,
        )
        return True

    def record_dispatch_readiness_rejection(
        self,
        node_id: str,
        error: str = "",
    ) -> bool:
        """
        Temporarily cool down routing after a worker rejects dispatch as not ready.

        This is a readiness/routing signal, not SWIM health.
        """
        if node_id not in self._workers:
            return False

        self._get_or_create_dispatch_routing_state(node_id).record_failure(
            error=error,
            cooldown_seconds=self._dispatch_readiness_cooldown_seconds,
        )
        return True

    def is_worker_dispatch_routable(self, node_id: str) -> bool:
        """Return whether dispatch allocation may currently select this worker."""
        if node_id not in self._workers:
            return False

        routing_state = self._dispatch_routing.get(node_id)
        if routing_state is None:
            return True

        return routing_state.is_routable()

    def get_worker_dispatch_routing_snapshot(
        self,
        node_id: str,
    ) -> dict[str, str | int | float | bool] | None:
        """Return diagnostic dispatch routing state for a worker."""
        routing_state = self._dispatch_routing.get(node_id)
        if routing_state is None:
            return None

        return {
            "worker_id": routing_state.worker_id,
            "routable": routing_state.is_routable(),
            "consecutive_failures": routing_state.consecutive_failures,
            "remaining_cooldown_seconds": routing_state.remaining_cooldown_seconds(),
            "last_failure_at": routing_state.last_failure_at,
            "last_success_at": routing_state.last_success_at,
            "last_error": routing_state.last_error,
        }

    def _next_dispatch_routing_ready_delay(self) -> float | None:
        now = _DEFAULT_CLOCK.monotonic()
        positive_delays = [delay for delay in self._dispatch_routing_cooldown_delays(now) if delay > 0]
        return min(positive_delays, default=None)

    def _dispatch_routing_cooldown_delays(self, now: float) -> list[float]:
        """The remaining dispatch routing cooldown of each registered worker
        cooling down at ``now``."""
        return [
            routing_state.remaining_cooldown_seconds(now)
            for node_id, routing_state in self._dispatch_routing.items()
            if self._is_cooling_down(node_id, routing_state, now)
        ]

    def _is_cooling_down(
        self,
        node_id: str,
        routing_state: WorkerDispatchRoutingState,
        now: float,
    ) -> bool:
        """Whether a registered worker's dispatch routing cools down at ``now``."""
        return node_id in self._workers and not routing_state.is_routable(now)

    def update_health(self, node_id: str, health: WorkerState) -> bool:
        """
        Update worker health status.

        Returns True if worker exists and was updated.
        """
        worker = self._workers.get(node_id)
        if not worker:
            return False

        worker.health = health

        # Update three-signal liveness based on health (AD-19)
        health_state = self._worker_health.get(node_id)
        if health_state:
            is_healthy = health == WorkerState.HEALTHY
            health_state.update_liveness(success=is_healthy)

        return True

    def is_worker_healthy(self, node_id: str) -> bool:
        """
        Check if a worker is considered healthy.

        A worker is healthy if:
        1. Local dispatch routing is not cooling down this worker, AND
        2. Worker lifecycle state allows new work, AND
        3. SWIM reports it as OK, or it has explicit healthy state, or it is
           within the new-registration grace period.
        """
        worker = self._workers.get(node_id)
        if not worker:
            return False

        return self._may_take_new_work(node_id, worker) and self._passes_health_signals(node_id, worker)

    def _may_take_new_work(self, node_id: str, worker: WorkerStatus) -> bool:
        """Whether no drain is intended, dispatch routing is not cooling the
        worker down, and its lifecycle state allows new work."""
        # Lifecycle state is more specific than SWIM membership. A worker can
        # still be visible to UDP/SWIM while it is explicitly draining.
        return (
            not self.is_worker_drain_intended(node_id)
            and self.is_worker_dispatch_routable(node_id)
            and worker.health not in (WorkerState.DRAINING, WorkerState.OFFLINE)
        )

    def _passes_health_signals(self, node_id: str, worker: WorkerStatus) -> bool:
        """Whether AD-19 routing neither drains nor evicts the worker, SWIM
        does not report it down, and it is healthy or newly registered."""
        routing_decision = self.get_worker_routing_decision(node_id)
        if routing_decision in (RoutingDecision.DRAIN, RoutingDecision.EVICT):
            return False

        # SWIM membership as a NEGATIVE gate only. SUSPECT/DEAD is
        # definitive evidence against routing; "OK" is NOT definitive
        # for it — SWIM death detection trails a crash by tens of
        # seconds, so a just-died worker reads OK while its heartbeats
        # have already stopped. Short-circuiting True on OK overrode
        # the staleness/grace checks below and accepted work against
        # down workers (measured: a power-cycled worker's job stranded
        # to the AD-34 timeout because acceptance beat the reboot).
        if self._swim_reports_down(worker):
            return False

        return self._is_healthy_or_in_grace(worker)

    def _is_healthy_or_in_grace(self, worker: WorkerStatus) -> bool:
        """Whether the worker is explicitly HEALTHY, or registered within the
        grace period."""
        # Check explicit health status
        if worker.health == WorkerState.HEALTHY:
            return True

        # Grace period for newly registered workers
        now = _DEFAULT_CLOCK.monotonic()
        return (now - worker.last_seen) < self._health_grace_period

    def counts_toward_capacity(self, node_id: str) -> bool:
        """Whether a worker's cores are the datacenter's to use (AD-41).

        Busy or idle alike -- unlike ``is_worker_healthy``, which asks
        whether it can take new work now -- as long as it is not leaving
        (drain intended, lifecycle DRAINING or OFFLINE), not judged dead or
        stuck (routing EVICT), and not suspected or dead under SWIM. A
        routing DRAIN only means "not ready for new work" (a saturated
        worker reports exactly that), so its cores still count.
        """
        if (worker := self._workers.get(node_id)) is None:
            return False
        return not self._is_leaving(node_id, worker) and self._is_not_evicted_or_down(node_id, worker)

    def _is_leaving(self, node_id: str, worker: WorkerStatus) -> bool:
        """Whether a drain is intended or the worker is DRAINING or OFFLINE."""
        return self.is_worker_drain_intended(node_id) or worker.health in (
            WorkerState.DRAINING,
            WorkerState.OFFLINE,
        )

    def _is_not_evicted_or_down(self, node_id: str, worker: WorkerStatus) -> bool:
        """Whether AD-19 routing does not evict the worker and SWIM does not
        suspect it or hold it dead."""
        return self.get_worker_routing_decision(node_id) != RoutingDecision.EVICT and not self._swim_reports_down(
            worker
        )

    def _swim_reports_down(self, worker: WorkerStatus) -> bool:
        """SWIM's negative verdict on a registered worker: SUSPECT or DEAD."""
        if not (self._get_swim_status and worker.registration):
            return False
        return self._get_swim_status(self._swim_addr(worker)) in ("SUSPECT", "DEAD")

    @staticmethod
    def _swim_addr(worker: WorkerStatus) -> tuple[str, int]:
        """The address SWIM knows a registered worker by: its UDP port, else
        its TCP port."""
        return (
            worker.registration.node.host,
            worker.registration.node.udp_port or worker.registration.node.port,
        )

    def get_healthy_worker_ids(self) -> list[str]:
        return [node_id for node_id in self._workers if self.is_worker_healthy(node_id)]

    def get_worker_health_bucket(self, node_id: str) -> str:
        worker = self._workers.get(node_id)
        if not worker or not self.is_worker_healthy(node_id):
            return "UNHEALTHY"

        return self._healthy_worker_bucket(node_id, worker)

    def _healthy_worker_bucket(self, node_id: str, worker: WorkerStatus) -> str:
        """A healthy worker's bucket: DEGRADED while AD-19 routing says to
        investigate it, else by its overload state (HEALTHY when unknown)."""
        routing_decision = self.get_worker_routing_decision(node_id)
        if routing_decision == RoutingDecision.INVESTIGATE:
            return "DEGRADED"

        return _BUCKET_BY_OVERLOAD_STATE.get(worker.overload_state, "HEALTHY")

    def get_worker_health_state_counts(self) -> dict[str, int]:
        counts = {"healthy": 0, "busy": 0, "stressed": 0, "overloaded": 0}

        for node_id, worker in self._workers.items():
            if self.is_worker_healthy(node_id):
                self._tally_overload_state(counts, worker.overload_state)

        return counts

    @staticmethod
    def _tally_overload_state(counts: dict[str, int], overload_state: str) -> None:
        """Count a healthy worker under its overload state; an unknown state
        counts as healthy."""
        counts[overload_state if overload_state in counts else "healthy"] += 1

    def get_workers_by_health_bucket(self) -> dict[str, list[str]]:
        buckets: dict[str, list[str]] = {
            "HEALTHY": [],
            "BUSY": [],
            "DEGRADED": [],
            "UNHEALTHY": [],
        }

        for node_id in self._workers:
            bucket = self.get_worker_health_bucket(node_id)
            if bucket in buckets:
                buckets[bucket].append(node_id)

        return buckets

    # =========================================================================
    # Three-Signal Health Model (AD-19)
    # =========================================================================

    def get_worker_health_state(self, node_id: str) -> WorkerHealthState | None:
        """Get the three-signal health state for a worker."""
        return self._worker_health.get(node_id)

    def get_worker_routing_decision(self, node_id: str) -> RoutingDecision | None:
        """
        Get routing decision for a worker based on three-signal health.

        Returns:
            RoutingDecision.ROUTE - healthy, send work
            RoutingDecision.DRAIN - not ready, stop new work
            RoutingDecision.INVESTIGATE - degraded, check worker
            RoutingDecision.EVICT - dead or stuck, remove
            None - worker not found
        """
        health_state = self._worker_health.get(node_id)
        if health_state:
            return health_state.get_routing_decision()
        return None

    def update_worker_progress(
        self,
        node_id: str,
        assigned: int,
        completed: int,
        expected_rate: float | None = None,
    ) -> bool:
        """
        Update worker progress signal from completion metrics.

        Called periodically to track workflow completion rates.

        Args:
            node_id: Worker node ID
            assigned: Number of workflows assigned to worker
            completed: Number of completions in the last interval
            expected_rate: Expected completion rate per interval

        Returns:
            True if worker was found and updated
        """
        health_state = self._worker_health.get(node_id)
        if not health_state:
            return False

        health_state.update_progress(
            assigned=assigned,
            completed=completed,
            expected_rate=expected_rate,
        )
        return True

    def get_workers_to_evict(self) -> list[str]:
        """
        Get list of workers that should be evicted based on health signals.

        Returns node IDs where routing decision is EVICT.
        """
        return [
            node_id
            for node_id, health_state in self._worker_health.items()
            if health_state.get_routing_decision() == RoutingDecision.EVICT
        ]

    def get_workers_to_investigate(self) -> list[str]:
        """
        Get list of workers that need investigation based on health signals.

        Returns node IDs where routing decision is INVESTIGATE.
        """
        return [
            node_id
            for node_id, health_state in self._worker_health.items()
            if health_state.get_routing_decision() == RoutingDecision.INVESTIGATE
        ]

    def get_workers_to_drain(self) -> list[str]:
        """
        Get list of workers that should be drained based on health signals.

        Returns node IDs where routing decision is DRAIN.
        """
        return [
            node_id
            for node_id, health_state in self._worker_health.items()
            if health_state.get_routing_decision() == RoutingDecision.DRAIN
        ]

    def get_routable_worker_ids(self) -> list[str]:
        """
        Get list of workers that can receive new work based on health signals.

        Returns node IDs where routing decision is ROUTE.
        """
        return [
            node_id
            for node_id, health_state in self._worker_health.items()
            if health_state.get_routing_decision() == RoutingDecision.ROUTE
        ]

    def get_worker_health_diagnostics(self, node_id: str) -> dict | None:
        """Get diagnostic information for a worker's health state."""
        health_state = self._worker_health.get(node_id)
        if health_state:
            return health_state.get_diagnostics()
        return None

    # =========================================================================
    # Heartbeat Processing
    # =========================================================================

    async def process_heartbeat(
        self,
        node_id: str,
        heartbeat: WorkerHeartbeat,
    ) -> bool:
        """
        Process a heartbeat from a worker.

        Updates available cores and last seen time.
        Thread-safe: uses allocation lock for core updates.

        Returns True if worker exists and was updated.
        """
        worker = self._workers.get(node_id)
        if not worker:
            return False

        async with self._cores_condition:
            if self._is_stale_heartbeat(worker, heartbeat):
                return True

            self._apply_heartbeat(node_id, worker, heartbeat)

        return True

    @staticmethod
    def _is_stale_heartbeat(worker: WorkerStatus, heartbeat: WorkerHeartbeat) -> bool:
        """Whether the heartbeat is older than the one applied last."""
        return (
            worker.heartbeat is not None
            and heartbeat.version < worker.heartbeat.version
        )

    def _apply_heartbeat(
        self,
        node_id: str,
        worker: WorkerStatus,
        heartbeat: WorkerHeartbeat,
    ) -> None:
        """Under the allocation lock: take a worker's heartbeat -- its state,
        free cores and readiness (AD-19) -- and wake dispatch waiters when
        its capacity grew or allocation may now select it."""
        # Allocation selects workers by health bucket, so "may take
        # work" is the bucket, not bare health: an overloaded worker is
        # healthy yet never selected.
        was_selectable = self.get_worker_health_bucket(node_id) != "UNHEALTHY"
        drain_intended = self.is_worker_drain_intended(node_id)
        worker.heartbeat = heartbeat
        worker.last_seen = _DEFAULT_CLOCK.monotonic()
        worker.health = self._heartbeat_health(heartbeat, drain_intended)

        # Against what was free to allocate: the reservations this
        # heartbeat clears free cores too.
        old_unreserved_cores = worker.available_cores - worker.reserved_cores
        worker.total_cores = heartbeat.available_cores + len(
            heartbeat.active_workflows
        )
        self._apply_heartbeat_cores(node_id, worker, heartbeat)

        worker.overload_state = getattr(
            heartbeat, "health_overload_state", "healthy"
        )

        if worker.available_cores - worker.reserved_cores > old_unreserved_cores:
            self.capacity_generation += 1
            self._cores_condition.notify_all()

        self._refresh_heartbeat_health(node_id, worker, heartbeat, drain_intended)

        if self._became_selectable(node_id, was_selectable):
            self.capacity_generation += 1
            self._cores_condition.notify_all()

    @staticmethod
    def _heartbeat_health(heartbeat: WorkerHeartbeat, drain_intended: bool) -> WorkerState:
        """The lifecycle state a heartbeat sets: DRAINING while a drain is
        intended, else the state it reports (DEGRADED when unknown)."""
        if drain_intended:
            return WorkerState.DRAINING

        try:
            return WorkerState(heartbeat.state)
        except ValueError:
            return WorkerState.DEGRADED

    def _apply_heartbeat_cores(
        self,
        node_id: str,
        worker: WorkerStatus,
        heartbeat: WorkerHeartbeat,
    ) -> None:
        """Under the allocation lock: take the heartbeat's free cores unless
        a newer count was applied, and clear the reservations it reflects."""
        reservations = self._dispatch_reservations.get(node_id, {})
        # A heartbeat older than the free count applied last (reordered
        # behind a progress report or result) does not overwrite it.
        if heartbeat.cores_version >= self._applied_cores_versions.get(node_id, 0):
            worker.available_cores = heartbeat.available_cores
            self._applied_cores_versions[node_id] = heartbeat.cores_version
        applied_version = self._applied_cores_versions.get(node_id, 0)
        # Dispatches the applied count reflects: allocated at or before
        # its version, or listed by this heartbeat (allocated before it,
        # so before anything newer too).
        for dispatch_token in self._heartbeat_reflected_tokens(reservations, heartbeat, applied_version):
            del reservations[dispatch_token]
        worker.reserved_cores = self._reserved_cores(reservations)

    @staticmethod
    def _heartbeat_reflected_tokens(
        reservations: dict[str, tuple[str, int, int | None]],
        heartbeat: WorkerHeartbeat,
        applied_version: int,
    ) -> list[str]:
        """The reservations a heartbeat reflects: listed by it, or taken at
        or before the applied free count's version."""
        return [
            dispatch_token
            for dispatch_token, (_job_id, _cores, allocated_at_version) in reservations.items()
            if WorkerPool._heartbeat_reflects(dispatch_token, allocated_at_version, heartbeat, applied_version)
        ]

    @staticmethod
    def _heartbeat_reflects(
        dispatch_token: str,
        allocated_at_version: int | None,
        heartbeat: WorkerHeartbeat,
        applied_version: int,
    ) -> bool:
        """Whether a heartbeat reflects one reservation's dispatch."""
        return dispatch_token in heartbeat.active_workflows or WorkerPool._is_reflected(
            allocated_at_version, applied_version
        )

    def _refresh_heartbeat_health(
        self,
        node_id: str,
        worker: WorkerStatus,
        heartbeat: WorkerHeartbeat,
        drain_intended: bool,
    ) -> None:
        """AD-19: a heartbeat is liveness, and sets readiness from its free
        cores and its say on taking work."""
        health_state = self._worker_health.get(node_id)
        if health_state:
            health_state.update_liveness(success=True)

            self._refresh_readiness(
                health_state, drain_intended, heartbeat.health_accepting_work, worker.available_cores
            )

    # =========================================================================
    # Core Allocation
    # =========================================================================

    def get_total_available_cores(self) -> int:
        """Get the free cores allocation can take now: those of every
        worker in a health bucket it selects from (healthy, busy or
        degraded). A healthy but overloaded worker's free cores are not
        counted -- ``allocate_cores`` never selects it."""
        total = sum(
            worker.available_cores - worker.reserved_cores
            for worker in self._workers.values()
            if self.get_worker_health_bucket(worker.node_id) != "UNHEALTHY"
        )

        return total

    async def allocate_cores(
        self,
        cores_needed: int,
        excluded_worker_ids: set[str] | None = None,
        *,
        job_id: str,
        dispatch_token_for: Callable[[str], str],
    ) -> list[tuple[str, int]] | None:
        """
        Allocate cores from the worker pool, now.

        Selects workers allocation may use -- by health bucket, never one in
        ``excluded_worker_ids`` -- and reserves up to ``cores_needed`` of
        their free cores. Returns list of (worker_node_id, cores_allocated)
        tuples.

        One attempt, never a wait: a caller that got nothing waits for the
        pool to change (``wait_for_capacity_change``) outside whatever it
        holds. Fewer cores than ``cores_needed`` is an allocation, not a
        failure: the caller's share was sized against every selectable
        worker's free cores, which a heartbeat can shrink and an exclusion
        can put out of its reach -- holding out for the share starved it
        while the cores it could use sat idle.

        Thread-safe: uses allocation lock.

        Args:
            cores_needed: Most cores to reserve
            excluded_worker_ids: Workers this allocation must not use
            job_id: The job the dispatches belong to
            dispatch_token_for: The dispatch (sub-workflow token) a worker's
                share will be sent as; each share is reserved under it

        Returns:
            List of (node_id, cores) tuples reserving at least one core, or
            None when no worker it may use has a free core
        """
        async with self._cores_condition:
            allocations = self._select_workers_for_allocation(
                cores_needed,
                excluded_worker_ids=excluded_worker_ids,
            )
            verified_allocations: list[tuple[str, int]] = []

            for node_id, cores in allocations:
                self._reserve_allocation(
                    node_id, cores, job_id, dispatch_token_for, verified_allocations
                )

            return verified_allocations or None

    def _reserve_allocation(
        self,
        node_id: str,
        cores: int,
        job_id: str,
        dispatch_token_for: Callable[[str], str],
        verified_allocations: list[tuple[str, int]],
    ) -> None:
        """Under the allocation lock: reserve a selected worker's share,
        capped at its unreserved cores, under the dispatch it will be sent
        as; a worker gone or with none free is skipped."""
        worker = self._workers.get(node_id)
        if worker is None:
            return

        actual_available = worker.available_cores - worker.reserved_cores
        if actual_available <= 0:
            return

        actual_cores = min(cores, actual_available)
        worker.reserved_cores += actual_cores
        reservations = self._dispatch_reservations.setdefault(node_id, {})
        dispatch_token = dispatch_token_for(node_id)
        _reserved_job_id, already_reserved, _version = reservations.get(dispatch_token, (job_id, 0, None))
        reservations[dispatch_token] = (job_id, already_reserved + actual_cores, None)
        verified_allocations.append((node_id, actual_cores))

    def _select_workers_for_allocation(
        self,
        cores_needed: int,
        excluded_worker_ids: set[str] | None = None,
    ) -> list[tuple[str, int]]:
        allocations: list[tuple[str, int]] = []
        remaining = cores_needed

        bucket_priority = ["HEALTHY", "BUSY", "DEGRADED"]

        workers_by_bucket = self._workers_by_selectable_bucket(bucket_priority, excluded_worker_ids)

        for bucket in bucket_priority:
            if remaining <= 0:
                break

            remaining = self._allocate_from_bucket(workers_by_bucket[bucket], remaining, allocations)

        return allocations

    def _workers_by_selectable_bucket(
        self,
        bucket_priority: list[str],
        excluded_worker_ids: set[str] | None,
    ) -> dict[str, list[tuple[str, WorkerStatus]]]:
        """The workers not excluded, by the health bucket allocation selects
        them from."""
        excluded = excluded_worker_ids or set()

        workers_by_bucket: dict[str, list[tuple[str, WorkerStatus]]] = {
            bucket: [] for bucket in bucket_priority
        }

        self._file_workers_by_bucket(excluded, workers_by_bucket)
        return workers_by_bucket

    def _file_workers_by_bucket(
        self,
        excluded: set[str],
        workers_by_bucket: dict[str, list[tuple[str, WorkerStatus]]],
    ) -> None:
        """File each worker not excluded under its selectable health bucket."""
        for node_id, worker in self._workers.items():
            self._file_worker_by_bucket(node_id, worker, excluded, workers_by_bucket)

    def _file_worker_by_bucket(
        self,
        node_id: str,
        worker: WorkerStatus,
        excluded: set[str],
        workers_by_bucket: dict[str, list[tuple[str, WorkerStatus]]],
    ) -> None:
        """File a worker not excluded under its health bucket, when
        allocation selects from that bucket."""
        if node_id in excluded:
            return

        bucket = self.get_worker_health_bucket(node_id)
        if bucket in workers_by_bucket:
            workers_by_bucket[bucket].append((node_id, worker))

    def _allocate_from_bucket(
        self,
        bucket_workers: list[tuple[str, WorkerStatus]],
        remaining: int,
        allocations: list[tuple[str, int]],
    ) -> int:
        """Take cores from a bucket's workers, most unreserved cores first,
        until ``remaining`` is met; returns what is still needed."""
        bucket_workers.sort(
            key=lambda x: x[1].available_cores - x[1].reserved_cores,
            reverse=True,
        )

        for node_id, worker in bucket_workers:
            if remaining <= 0:
                break

            remaining -= self._take_worker_cores(node_id, worker, remaining, allocations)

        return remaining

    @staticmethod
    def _take_worker_cores(
        node_id: str,
        worker: WorkerStatus,
        remaining: int,
        allocations: list[tuple[str, int]],
    ) -> int:
        """Take up to ``remaining`` of a worker's unreserved cores; returns
        how many were taken."""
        available = worker.available_cores - worker.reserved_cores
        if available <= 0:
            return 0

        to_allocate = min(available, remaining)
        allocations.append((node_id, to_allocate))
        return to_allocate

    async def release_cores(
        self,
        node_id: str,
        dispatch_token: str,
    ) -> bool:
        """
        Release a dispatch's reserved cores back to its worker: the
        dispatch was not taken (or never sent).

        Thread-safe: uses allocation lock.
        """
        async with self._cores_condition:
            worker = self._workers.get(node_id)
            if not worker:
                return False

            _job_id, cores, _version = self._dispatch_reservations.get(node_id, {}).pop(
                dispatch_token, ("", 0, None)
            )
            worker.reserved_cores = max(0, worker.reserved_cores - cores)

            self.capacity_generation += 1
            self._cores_condition.notify_all()

            return True

    async def update_worker_cores_from_progress(
        self,
        node_id: str,
        worker_available_cores: int,
        dispatch_token: str,
        cores_version: int,
    ) -> bool:
        """
        Update worker's available cores from workflow progress report.

        Progress reports and results from workers include their current
        available_cores, which is more recent than heartbeat data. This
        method updates the worker's availability -- clearing the reservation
        of the dispatch the report is for -- and signals if cores became
        available.

        Thread-safe: uses allocation lock.

        Returns True if worker was found and updated.
        """
        async with self._cores_condition:
            worker = self._workers.get(node_id)
            if not worker:
                return False

            was_selectable = self.get_worker_health_bucket(node_id) != "UNHEALTHY"
            # Against what was free to allocate: the reservation this
            # report clears frees cores too.
            old_unreserved_cores = worker.available_cores - worker.reserved_cores
            self._apply_reported_cores(node_id, worker, worker_available_cores, dispatch_token, cores_version)

            self._refresh_reported_readiness(node_id, worker)

            if self._capacity_grew(node_id, worker, old_unreserved_cores, was_selectable):
                self.capacity_generation += 1
                self._cores_condition.notify_all()

            return True

    def _refresh_reported_readiness(self, node_id: str, worker: WorkerStatus) -> None:
        """Under the allocation lock: AD-19 readiness from a progress
        report's free count, for a worker that has heartbeated."""
        # AD-19 readiness follows the free count, as a heartbeat sets it:
        # a worker busy at its last heartbeat read not ready -- never
        # selected -- after a result freed its cores, its cores idle
        # until its next heartbeat reached this manager.
        health_state = self._worker_health.get(node_id)
        if health_state is not None and worker.heartbeat is not None:
            self._refresh_readiness(
                health_state,
                self.is_worker_drain_intended(node_id),
                worker.heartbeat.health_accepting_work,
                worker.available_cores,
            )

    def _capacity_grew(
        self,
        node_id: str,
        worker: WorkerStatus,
        old_unreserved_cores: int,
        was_selectable: bool,
    ) -> bool:
        """Whether the worker has more unreserved cores than before, or
        allocation may select it now and could not before."""
        return worker.available_cores - worker.reserved_cores > old_unreserved_cores or self._became_selectable(
            node_id, was_selectable
        )

    def _became_selectable(self, node_id: str, was_selectable: bool) -> bool:
        """Whether allocation may select the worker now and could not before."""
        return not was_selectable and self.get_worker_health_bucket(node_id) != "UNHEALTHY"

    def _apply_reported_cores(
        self,
        node_id: str,
        worker: "WorkerStatus",
        worker_available_cores: int,
        dispatch_token: str,
        cores_version: int,
    ) -> None:
        """Take a worker's reported free cores and clear the reservations
        its report reflects (held under the allocation lock)."""
        # A report older than the free count applied last (reordered in
        # flight) does not overwrite it.
        if cores_version >= self._applied_cores_versions.get(node_id, 0):
            worker.available_cores = worker_available_cores
            self._applied_cores_versions[node_id] = cores_version
        reservations = self._dispatch_reservations.get(node_id, {})
        # The report's own dispatch is reflected (it reports after its
        # allocation), and so is every dispatch allocated at or before the
        # applied version; the rest are still in flight.
        reservations.pop(dispatch_token, None)
        for reflected_token in self._reflected_reservation_tokens(
            reservations, self._applied_cores_versions.get(node_id, 0)
        ):
            del reservations[reflected_token]
        worker.reserved_cores = self._reserved_cores(reservations)

    @staticmethod
    def _reflected_reservation_tokens(
        reservations: dict[str, tuple[str, int, int | None]],
        applied_version: int,
    ) -> list[str]:
        """The reservations a free count at ``applied_version`` already
        reflects: those taken at or before it (one not yet taken has no
        version)."""
        return [
            reflected_token
            for reflected_token, (_job_id, _cores, allocated_at_version) in reservations.items()
            if WorkerPool._is_reflected(allocated_at_version, applied_version)
        ]

    @staticmethod
    def _is_reflected(allocated_at_version: int | None, applied_version: int) -> bool:
        """Whether a free count at ``applied_version`` reflects a dispatch
        the worker took at ``allocated_at_version`` (None: not taken yet)."""
        return allocated_at_version is not None and allocated_at_version <= applied_version

    @staticmethod
    def _reserved_cores(reservations: dict[str, tuple[str, int, int | None]]) -> int:
        """The cores a worker's outstanding reservations hold."""
        return sum(cores for _job_id, cores, _version in reservations.values())

    @staticmethod
    def _refresh_readiness(
        health_state: "WorkerHealthState",
        drain_intended: bool,
        accepting_work: bool,
        available_cores: int,
    ) -> None:
        """AD-19 readiness from a worker's free cores and its own say on
        taking work: no capacity, so not accepting, while it drains."""
        capacity = 0 if drain_intended else available_cores
        health_state.update_readiness(accepting=accepting_work and capacity > 0, capacity=capacity)

    async def record_dispatch_taken(self, node_id: str, dispatch_token: str, allocated_at_version: int) -> None:
        """The worker took ``dispatch_token``, allocating its cores at core
        availability version ``allocated_at_version``: any free count at
        or past that version reflects them. Its reservation stands until
        the count applied here does -- at once, if it already does."""
        async with self._cores_condition:
            reservations = self._dispatch_reservations.get(node_id, {})
            if (reservation := reservations.get(dispatch_token)) is None:
                return
            reserved_job_id, cores, _version = reservation
            if allocated_at_version <= self._applied_cores_versions.get(node_id, 0):
                del reservations[dispatch_token]
                self._return_reflected_reservation_cores(node_id, cores)
                return
            reservations[dispatch_token] = (reserved_job_id, cores, allocated_at_version)

    def _return_reflected_reservation_cores(self, node_id: str, cores: int) -> None:
        """Under the allocation lock: a reservation the applied free count
        already reflects stops holding the worker's cores."""
        if (worker := self._workers.get(node_id)) is not None:
            worker.reserved_cores = max(0, worker.reserved_cores - cores)
            self.capacity_generation += 1
            self._cores_condition.notify_all()

    async def release_job_reservations(self, job_id: str) -> int:
        """Release every reservation still held for ``job_id``'s dispatches
        once the job has ended here -- one whose dispatch no report from
        this worker ever showed (its result went to another manager, say).
        Returns how many were released."""
        released = 0
        async with self._cores_condition:
            for node_id, reservations in self._dispatch_reservations.items():
                released += self._release_worker_job_reservations(node_id, reservations, job_id)
            if released:
                self.capacity_generation += 1
                self._cores_condition.notify_all()
        return released

    def _release_worker_job_reservations(
        self,
        node_id: str,
        reservations: dict[str, tuple[str, int, int | None]],
        job_id: str,
    ) -> int:
        """Under the allocation lock: drop one worker's reservations for the
        job; returns how many it held."""
        job_tokens = self._job_reservation_tokens(reservations, job_id)
        if job_tokens:
            self._drop_reservations(node_id, reservations, job_tokens)
        return len(job_tokens)

    def _drop_reservations(
        self,
        node_id: str,
        reservations: dict[str, tuple[str, int, int | None]],
        dispatch_tokens: list[str],
    ) -> None:
        """Under the allocation lock: drop the given reservations of a worker
        and recount the cores it has reserved."""
        for dispatch_token in dispatch_tokens:
            del reservations[dispatch_token]
        if (worker := self._workers.get(node_id)) is not None:
            worker.reserved_cores = self._reserved_cores(reservations)

    @staticmethod
    def _job_reservation_tokens(
        reservations: dict[str, tuple[str, int, int | None]],
        job_id: str,
    ) -> list[str]:
        """The dispatch tokens of a worker's reservations held for the job."""
        return [
            token for token, (reserved_job_id, _cores, _version) in reservations.items() if reserved_job_id == job_id
        ]

    # =========================================================================
    # Wait Helpers
    # =========================================================================

    async def wait_for_capacity_change(
        self,
        observed_generation: int,
        timeout: float,
    ) -> None:
        """
        Wait until the pool's capacity may have changed since
        ``capacity_generation`` read ``observed_generation``.

        Returns at once when it has already moved, else on the next change,
        when the soonest dispatch-routing cooldown ends (a worker becomes
        routable again with no notify), or after ``timeout`` -- whichever
        comes first. Read the generation BEFORE the allocation attempt this
        waits out: a change during or after the attempt then ends the wait,
        and none is lost. Unlike waiting for free cores, this never returns
        at once over cores the attempt could not use (excluded or
        unselectable workers), so a dispatch loop built on it cannot spin.
        """
        async with self._cores_condition:
            if self.capacity_generation != observed_generation:
                return

            wait_timeout = self._capacity_wait_timeout(timeout)

            try:
                await _DEFAULT_CLOCK.wait_for(
                    self._cores_condition.wait(),
                    timeout=wait_timeout,
                )
            except asyncio.TimeoutError:
                # The wait is bounded by design: on expiry the caller
                # re-reads the pool, as on a change.
                return

    def _capacity_wait_timeout(self, timeout: float) -> float:
        """The capacity wait's bound: ``timeout``, or sooner when a dispatch
        routing cooldown ends first, never under 1ms."""
        wait_timeout = timeout
        routing_ready_delay = self._next_dispatch_routing_ready_delay()
        if routing_ready_delay is not None:
            wait_timeout = min(wait_timeout, routing_ready_delay)

        # Progress floor. A routing cooldown's remaining time can be a
        # positive sub-quantum float artifact of deadline arithmetic on
        # a quantized clock (observed: 1.6e-11s): waiting on it re-arms
        # a timer at the SAME virtual instant -- a livelock under SIM, a
        # 100%-CPU micro-spin on a real host. Flooring the wait
        # guarantees the clock moves; genuine cooldown waits (>= 0.25s
        # base) are unaffected.
        return max(wait_timeout, 0.001)

    async def notify_cores_available(self) -> None:
        async with self._cores_condition:
            self.capacity_generation += 1
            self._cores_condition.notify_all()

    # =========================================================================
    # Logging Helpers
    # =========================================================================

    def _get_log_context(self) -> dict:
        """Get common context fields for logging."""
        healthy_ids = self.get_healthy_worker_ids()
        return {
            "manager_id": self._manager_id,
            "datacenter": self._datacenter,
            "worker_count": len(self._workers),
            "healthy_worker_count": len(healthy_ids),
            "total_cores": sum(w.total_cores for w in self._workers.values()),
            "available_cores": self.get_total_available_cores(),
        }

    async def _log_trace(self, message: str) -> None:
        """Log a trace-level message."""
        await self._logger.log(
            WorkerPoolTrace(message=message, **self._get_log_context())
        )

    async def _log_debug(self, message: str) -> None:
        """Log a debug-level message."""
        await self._logger.log(
            WorkerPoolDebug(message=message, **self._get_log_context())
        )

    async def _log_info(self, message: str) -> None:
        """Log an info-level message."""
        await self._logger.log(
            WorkerPoolInfo(message=message, **self._get_log_context())
        )

    async def _log_warning(self, message: str) -> None:
        """Log a warning-level message."""
        await self._logger.log(
            WorkerPoolWarning(message=message, **self._get_log_context())
        )

    async def _log_error(self, message: str) -> None:
        """Log an error-level message."""
        await self._logger.log(
            WorkerPoolError(message=message, **self._get_log_context())
        )

    async def _log_critical(self, message: str) -> None:
        await self._logger.log(
            WorkerPoolCritical(message=message, **self._get_log_context())
        )

    async def register_remote_worker(self, update: WorkerStateUpdate) -> bool:
        async with self._registration_lock:
            worker_id = update.worker_id

            if worker_id in self._workers:
                return False

            if worker_id in self._remote_workers:
                self._refresh_remote_worker(self._remote_workers[worker_id], update)
                return True

            self._add_remote_worker(worker_id, update)

            return True

    @staticmethod
    def _remote_worker_health(update: WorkerStateUpdate) -> WorkerState:
        """A remote worker's lifecycle state from its owner's update (AD-48)."""
        return (
            WorkerState.DRAINING
            if update.state == "draining"
            else WorkerState.HEALTHY
        )

    def _refresh_remote_worker(self, existing: WorkerStatus, update: WorkerStateUpdate) -> None:
        """Take a known remote worker's cores and state from its owner's
        update (AD-48)."""
        existing.total_cores = update.total_cores
        existing.available_cores = update.available_cores
        existing.health = self._remote_worker_health(update)
        existing.last_seen = _DEFAULT_CLOCK.monotonic()

    def _add_remote_worker(self, worker_id: str, update: WorkerStateUpdate) -> None:
        """Track a remote worker another manager owns (AD-48)."""
        node_info = NodeInfo(
            node_id=worker_id,
            role="worker",
            host=update.host,
            port=update.tcp_port,
            datacenter=update.datacenter,
            udp_port=update.udp_port,
        )

        registration = WorkerRegistration(
            node=node_info,
            total_cores=update.total_cores,
            available_cores=update.available_cores,
            memory_mb=0,
        )

        worker = WorkerStatus(
            worker_id=worker_id,
            state=self._remote_worker_health(update).value,
            registration=registration,
            last_seen=_DEFAULT_CLOCK.monotonic(),
            total_cores=update.total_cores,
            available_cores=update.available_cores,
            is_remote=True,
            owner_manager_id=update.owner_manager_id,
        )

        self._remote_workers[worker_id] = worker

        addr = (update.host, update.tcp_port)
        self._remote_addr_to_worker[addr] = worker_id

    async def deregister_remote_worker(self, worker_id: str) -> bool:
        async with self._registration_lock:
            worker = self._remote_workers.pop(worker_id, None)
            if not worker:
                return False

            if worker.registration:
                addr = (worker.registration.node.host, worker.registration.node.port)
                self._remote_addr_to_worker.pop(addr, None)

            return True

    def get_remote_worker(self, worker_id: str) -> WorkerStatus | None:
        return self._remote_workers.get(worker_id)

    def is_worker_local(self, worker_id: str) -> bool:
        return worker_id in self._workers

    def is_worker_remote(self, worker_id: str) -> bool:
        return worker_id in self._remote_workers

    def iter_remote_workers(self) -> list[WorkerStatus]:
        return list(self._remote_workers.values())

    def iter_all_workers(self) -> list[WorkerStatus]:
        return list(self._workers.values()) + list(self._remote_workers.values())

    def get_local_worker_count(self) -> int:
        return len(self._workers)

    def get_remote_worker_count(self) -> int:
        return len(self._remote_workers)

    def get_total_worker_count(self) -> int:
        return len(self._workers) + len(self._remote_workers)

    async def cleanup_remote_workers_for_manager(self, manager_id: str) -> int:
        async with self._registration_lock:
            to_remove = self._remote_worker_ids_owned_by(manager_id)

            for worker_id in to_remove:
                self._forget_remote_worker(worker_id)

            return len(to_remove)

    def _remote_worker_ids_owned_by(self, manager_id: str) -> list[str]:
        """The remote workers the given manager owns (AD-48)."""
        return [
            worker_id
            for worker_id, worker in self._remote_workers.items()
            if getattr(worker, "owner_manager_id", None) == manager_id
        ]

    def _forget_remote_worker(self, worker_id: str) -> None:
        """Drop a remote worker and its address lookup (AD-48)."""
        worker = self._remote_workers.pop(worker_id, None)
        if worker and worker.registration:
            addr = (
                worker.registration.node.host,
                worker.registration.node.port,
            )
            self._remote_addr_to_worker.pop(addr, None)
