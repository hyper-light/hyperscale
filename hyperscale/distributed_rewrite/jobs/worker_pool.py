"""
Worker Pool - Thread-safe worker registration and resource management.

This class encapsulates all worker-related state and operations with proper
synchronization. It provides race-condition safe access to worker data
and core allocation.

Key responsibilities:
- Worker registration and deregistration
- Health tracking (integrates with SWIM)
- Core availability tracking and allocation
- Worker selection for workflow dispatch
"""

import asyncio
import time
from typing import Callable

from hyperscale.distributed_rewrite.models import (
    WorkerHeartbeat,
    WorkerRegistration,
    WorkerState,
    WorkerStatus,
)
from hyperscale.distributed_rewrite.jobs.logging_models import (
    WorkerPoolTrace,
    WorkerPoolDebug,
    WorkerPoolInfo,
    WorkerPoolWarning,
    WorkerPoolError,
    WorkerPoolCritical,
)
from hyperscale.logging import Logger


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
        """
        self._health_grace_period = health_grace_period
        self._get_swim_status = get_swim_status
        self._manager_id = manager_id
        self._datacenter = datacenter
        self._logger = Logger()

        # Worker storage - node_id -> WorkerStatus
        self._workers: dict[str, WorkerStatus] = {}

        # Quick lookup by address
        self._addr_to_worker: dict[tuple[str, int], str] = {}  # (host, port) -> node_id

        # Lock for worker registration/deregistration
        self._registration_lock = asyncio.Lock()

        # Lock for core allocation (separate from registration)
        self._allocation_lock = asyncio.Lock()

        # Event signaled when cores become available
        self._cores_available = asyncio.Event()

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

            # Check if already registered
            if node_id in self._workers:
                worker = self._workers[node_id]
                worker.registration = registration
                worker.last_seen = time.monotonic()
                return worker

            # Create new worker status
            worker = WorkerStatus(
                worker_id=node_id,
                state=WorkerState.HEALTHY.value,
                registration=registration,
                last_seen=time.monotonic(),
                total_cores=registration.total_cores or 0,
                available_cores=registration.available_cores or 0,
            )

            self._workers[node_id] = worker

            # Add address lookup
            addr = (registration.node.host, registration.node.port)
            self._addr_to_worker[addr] = node_id

            # Signal that cores may be available
            self._cores_available.set()

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

    def update_health(self, node_id: str, health: WorkerState) -> bool:
        """
        Update worker health status.

        Returns True if worker exists and was updated.
        """
        worker = self._workers.get(node_id)
        if not worker:
            return False

        worker.health = health
        return True

    def is_worker_healthy(self, node_id: str) -> bool:
        """
        Check if a worker is considered healthy.

        A worker is healthy if:
        1. SWIM reports it as OK, OR
        2. It was recently registered (within grace period)
        """
        worker = self._workers.get(node_id)
        if not worker:
            return False

        # Check SWIM status if callback provided
        if self._get_swim_status and worker.registration:
            addr = (worker.registration.node.host,
                    worker.registration.node.udp_port or worker.registration.node.port)
            swim_status = self._get_swim_status(addr)
            if swim_status == 'OK':
                return True
            if swim_status in ('SUSPECT', 'DEAD'):
                return False

        # Check explicit health status
        if worker.health == WorkerState.HEALTHY:
            return True
        if worker.health in (WorkerState.DRAINING, WorkerState.OFFLINE):
            return False

        # Grace period for newly registered workers
        now = time.monotonic()
        if (now - worker.last_seen) < self._health_grace_period:
            return True

        return False

    def get_healthy_worker_ids(self) -> list[str]:
        """Get list of all healthy worker node IDs."""
        return [
            node_id for node_id in self._workers
            if self.is_worker_healthy(node_id)
        ]

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

        async with self._allocation_lock:
            worker.heartbeat = heartbeat
            worker.last_seen = time.monotonic()

            # Update cores from heartbeat (authoritative source)
            old_available = worker.available_cores
            worker.available_cores = heartbeat.available_cores
            worker.total_cores = heartbeat.available_cores + len(heartbeat.active_workflows)

            # Clear any reservations that are now confirmed
            worker.reserved_cores = 0

            # Signal if cores became available
            if worker.available_cores > old_available:
                self._cores_available.set()

        return True

    # =========================================================================
    # Core Allocation
    # =========================================================================

    def get_total_available_cores(self) -> int:
        """Get total available cores across all healthy workers."""
        return sum(
            worker.available_cores - worker.reserved_cores
            for worker in self._workers.values()
            if self.is_worker_healthy(worker.node_id)
        )

    async def allocate_cores(
        self,
        cores_needed: int,
        timeout: float = 30.0,
    ) -> list[tuple[str, int]] | None:
        """
        Allocate cores from the worker pool.

        Selects workers to satisfy the core requirement and reserves
        the cores. Returns list of (worker_node_id, cores_allocated) tuples.

        Thread-safe: uses allocation lock.

        Args:
            cores_needed: Total cores required
            timeout: Max seconds to wait for cores to become available

        Returns:
            List of (node_id, cores) tuples, or None if timeout
        """
        start_time = time.monotonic()

        while True:
            elapsed = time.monotonic() - start_time
            if elapsed >= timeout:
                return None

            # Use a local event for this specific wait to avoid race conditions
            # The pattern is: check inside lock, only wait if not satisfied
            should_wait = False

            async with self._allocation_lock:
                allocations = self._select_workers_for_allocation(cores_needed)
                total_allocated = sum(cores for _, cores in allocations)

                if total_allocated >= cores_needed:
                    # Reserve the cores
                    for node_id, cores in allocations:
                        worker = self._workers.get(node_id)
                        if worker:
                            worker.reserved_cores += cores

                    return allocations

                # Not enough cores - prepare to wait
                # Clear inside lock to avoid missing signals
                self._cores_available.clear()
                should_wait = True

            # Wait for cores to become available (outside lock)
            if should_wait:
                remaining = timeout - elapsed
                try:
                    await asyncio.wait_for(
                        self._cores_available.wait(),
                        timeout=min(5.0, remaining),  # Check every 5s max
                    )
                except asyncio.TimeoutError:
                    pass  # Re-check availability

    def _select_workers_for_allocation(
        self,
        cores_needed: int,
    ) -> list[tuple[str, int]]:
        """
        Select workers to satisfy core requirement.

        Uses a greedy algorithm to pack workflows onto workers
        while respecting available cores.

        Must be called with allocation lock held.
        """
        allocations: list[tuple[str, int]] = []
        remaining = cores_needed

        # Get healthy workers sorted by available cores (descending)
        healthy_workers = [
            (node_id, worker)
            for node_id, worker in self._workers.items()
            if self.is_worker_healthy(node_id)
        ]
        healthy_workers.sort(
            key=lambda x: x[1].available_cores - x[1].reserved_cores,
            reverse=True,
        )

        for node_id, worker in healthy_workers:
            if remaining <= 0:
                break

            available = worker.available_cores - worker.reserved_cores
            if available <= 0:
                continue

            # Allocate as many cores as possible from this worker
            to_allocate = min(available, remaining)
            allocations.append((node_id, to_allocate))
            remaining -= to_allocate

        return allocations

    async def release_cores(
        self,
        node_id: str,
        cores: int,
    ) -> bool:
        """
        Release reserved cores back to a worker.

        Called when a dispatch fails or workflow completes.
        Thread-safe: uses allocation lock.
        """
        async with self._allocation_lock:
            worker = self._workers.get(node_id)
            if not worker:
                return False

            worker.reserved_cores = max(0, worker.reserved_cores - cores)

            # Signal that cores are available
            self._cores_available.set()

            return True

    async def confirm_allocation(
        self,
        node_id: str,
        cores: int,
    ) -> bool:
        """
        Confirm that an allocation was accepted by the worker.

        This converts reserved cores to actually-in-use cores.
        The next heartbeat from the worker will provide authoritative counts.

        Thread-safe: uses allocation lock.
        """
        async with self._allocation_lock:
            worker = self._workers.get(node_id)
            if not worker:
                return False

            # Move from reserved to in-use (reduce available)
            worker.reserved_cores = max(0, worker.reserved_cores - cores)
            worker.available_cores = max(0, worker.available_cores - cores)

            return True

    # =========================================================================
    # Wait Helpers
    # =========================================================================

    async def wait_for_cores(self, timeout: float = 30.0) -> bool:
        """
        Wait for cores to become available.

        Returns True if cores became available, False on timeout.

        Note: This method clears the event inside the allocation lock
        to prevent race conditions where a signal could be missed.
        """
        async with self._allocation_lock:
            # Check if any cores are already available
            total_available = sum(
                worker.available_cores - worker.reserved_cores
                for worker in self._workers.values()
                if self.is_worker_healthy(worker.node_id)
            )
            if total_available > 0:
                return True

            # Clear inside lock to avoid missing signals
            self._cores_available.clear()

        # Wait outside lock
        try:
            await asyncio.wait_for(
                self._cores_available.wait(),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            return False

    def signal_cores_available(self) -> None:
        """Signal that cores have become available."""
        self._cores_available.set()

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
        await self._logger.log(WorkerPoolTrace(message=message, **self._get_log_context()))

    async def _log_debug(self, message: str) -> None:
        """Log a debug-level message."""
        await self._logger.log(WorkerPoolDebug(message=message, **self._get_log_context()))

    async def _log_info(self, message: str) -> None:
        """Log an info-level message."""
        await self._logger.log(WorkerPoolInfo(message=message, **self._get_log_context()))

    async def _log_warning(self, message: str) -> None:
        """Log a warning-level message."""
        await self._logger.log(WorkerPoolWarning(message=message, **self._get_log_context()))

    async def _log_error(self, message: str) -> None:
        """Log an error-level message."""
        await self._logger.log(WorkerPoolError(message=message, **self._get_log_context()))

    async def _log_critical(self, message: str) -> None:
        """Log a critical-level message."""
        await self._logger.log(WorkerPoolCritical(message=message, **self._get_log_context()))
