"""
Core Allocator - Thread-safe core allocation for WorkerServer.

This class encapsulates all core allocation logic with proper locking to
prevent race conditions. It manages the mapping between workflow IDs and
CPU core indices.

Key responsibilities:
- Atomic core allocation and deallocation
- Workflow-to-core mapping maintenance
- Available core tracking
- Partial core release for streaming workflows

Design principles:
- Single lock protects ALL core state for atomicity
- All public methods are async and acquire the lock
- Internal methods assume lock is held
- No TOCTOU races: check-and-allocate is atomic
"""

import asyncio
from dataclasses import dataclass, field

from hyperscale.distributed_rewrite.jobs.logging_models import (
    AllocatorTrace,
    AllocatorDebug,
    AllocatorInfo,
    AllocatorWarning,
    AllocatorError,
    AllocatorCritical,
)
from hyperscale.logging import Logger


@dataclass
class AllocationResult:
    """Result of a core allocation attempt."""

    success: bool
    allocated_cores: list[int] = field(default_factory=list)
    error: str | None = None


class CoreAllocator:
    """
    Thread-safe core allocator for workflow execution.

    Manages the assignment of CPU cores to workflows with proper
    synchronization to prevent race conditions.

    All core state is protected by a single lock to ensure:
    - Atomic check-and-allocate operations
    - Consistent available_cores count
    - No TOCTOU races between availability check and allocation

    Usage:
        allocator = CoreAllocator(total_cores=8)

        # Atomic allocation
        result = await allocator.allocate("workflow-1", cores_needed=4)
        if result.success:
            # Use result.allocated_cores
            ...

        # Release when done
        await allocator.free("workflow-1")
    """

    def __init__(self, total_cores: int, worker_id: str = ""):
        """
        Initialize CoreAllocator.

        Args:
            total_cores: Total number of CPU cores available
            worker_id: Worker node ID for log context
        """
        self._total_cores = total_cores
        self._worker_id = worker_id
        self._logger = Logger()

        # Core assignment tracking
        # Maps core_index -> workflow_id (None if core is free)
        self._core_assignments: dict[int, str | None] = {
            i: None for i in range(total_cores)
        }

        # Reverse mapping: workflow_id -> list of assigned core indices
        self._workflow_cores: dict[str, list[int]] = {}

        # Available core count (cached for fast access)
        self._available_cores = total_cores

        # Single lock protects ALL core state
        self._lock = asyncio.Lock()

        # Event signaled when cores become available
        self._cores_available = asyncio.Event()
        self._cores_available.set()  # Initially all cores are available

    @property
    def total_cores(self) -> int:
        """Get total core count."""
        return self._total_cores

    @property
    def available_cores(self) -> int:
        """
        Get current available core count.

        Note: This is a snapshot and may change. For allocation decisions,
        use the atomic allocate() method instead of checking this first.
        """
        return self._available_cores

    async def allocate(
        self,
        workflow_id: str,
        cores_needed: int,
    ) -> AllocationResult:
        """
        Atomically allocate cores to a workflow.

        This is an atomic operation - it either allocates all requested
        cores or none at all. There is no TOCTOU race between checking
        availability and performing allocation.

        Args:
            workflow_id: Unique identifier for the workflow
            cores_needed: Number of cores to allocate

        Returns:
            AllocationResult with success status and allocated core indices
        """
        if cores_needed <= 0:
            await self._log_warning(
                f"Allocation request with invalid cores_needed={cores_needed}",
                workflow_id=workflow_id,
            )
            return AllocationResult(
                success=False,
                error="cores_needed must be positive",
            )

        if cores_needed > self._total_cores:
            await self._log_warning(
                f"Allocation request for {cores_needed} cores exceeds total {self._total_cores}",
                workflow_id=workflow_id,
            )
            return AllocationResult(
                success=False,
                error=f"Requested {cores_needed} cores but only {self._total_cores} total available",
            )

        async with self._lock:
            # Check if workflow already has cores allocated
            if workflow_id in self._workflow_cores:
                return AllocationResult(
                    success=False,
                    error=f"Workflow {workflow_id} already has cores allocated",
                )

            # Find free cores
            free_cores = [
                i for i, wf_id in self._core_assignments.items()
                if wf_id is None
            ]

            if len(free_cores) < cores_needed:
                return AllocationResult(
                    success=False,
                    error=f"Insufficient cores: need {cores_needed}, have {len(free_cores)}",
                )

            # Allocate cores (atomic with the check above)
            allocated = free_cores[:cores_needed]
            for core_idx in allocated:
                self._core_assignments[core_idx] = workflow_id

            self._workflow_cores[workflow_id] = allocated
            self._available_cores = self._count_free_cores()

            # Update event state
            if self._available_cores == 0:
                self._cores_available.clear()

            return AllocationResult(
                success=True,
                allocated_cores=allocated,
            )

    async def free(self, workflow_id: str) -> list[int]:
        """
        Free all cores allocated to a workflow.

        Args:
            workflow_id: Workflow whose cores should be freed

        Returns:
            List of core indices that were freed
        """
        async with self._lock:
            return self._free_internal(workflow_id)

    async def free_subset(
        self,
        workflow_id: str,
        count: int,
    ) -> list[int]:
        """
        Free a subset of cores allocated to a workflow.

        Used for streaming workflows where some cores complete before others.
        Frees the first `count` cores from the workflow's allocation.

        Args:
            workflow_id: Workflow to partially release cores from
            count: Number of cores to free

        Returns:
            List of core indices that were freed
        """
        if count <= 0:
            return []

        async with self._lock:
            allocated = self._workflow_cores.get(workflow_id, [])
            if not allocated:
                return []

            # Free the first `count` cores
            to_free = allocated[:count]
            for core_idx in to_free:
                if self._core_assignments.get(core_idx) == workflow_id:
                    self._core_assignments[core_idx] = None

            # Update workflow's remaining cores
            self._workflow_cores[workflow_id] = allocated[count:]
            if not self._workflow_cores[workflow_id]:
                del self._workflow_cores[workflow_id]

            self._available_cores = self._count_free_cores()

            # Signal that cores are available
            if to_free:
                self._cores_available.set()

            return to_free

    async def get_workflow_cores(self, workflow_id: str) -> list[int]:
        """
        Get the core indices assigned to a workflow.

        Args:
            workflow_id: Workflow to query

        Returns:
            List of assigned core indices (empty if none)
        """
        async with self._lock:
            return list(self._workflow_cores.get(workflow_id, []))

    async def get_core_assignments(self) -> dict[int, str | None]:
        """
        Get a snapshot of current core assignments.

        Returns:
            Copy of core_index -> workflow_id mapping
        """
        async with self._lock:
            return dict(self._core_assignments)

    async def get_workflows_on_cores(self, core_indices: list[int]) -> set[str]:
        """
        Get workflow IDs running on specific cores.

        Args:
            core_indices: List of core indices to check

        Returns:
            Set of workflow IDs running on those cores
        """
        async with self._lock:
            workflows = set()
            for core_idx in core_indices:
                wf_id = self._core_assignments.get(core_idx)
                if wf_id:
                    workflows.add(wf_id)
            return workflows

    async def wait_for_cores(self, timeout: float = 30.0) -> bool:
        """
        Wait for cores to become available.

        Args:
            timeout: Maximum seconds to wait

        Returns:
            True if cores are available, False on timeout
        """
        try:
            await asyncio.wait_for(
                self._cores_available.wait(),
                timeout=timeout,
            )
            return True
        except asyncio.TimeoutError:
            return False

    async def get_stats(self) -> dict:
        """
        Get allocation statistics.

        Returns:
            Dict with total_cores, available_cores, active_workflows
        """
        async with self._lock:
            return {
                "total_cores": self._total_cores,
                "available_cores": self._available_cores,
                "active_workflows": len(self._workflow_cores),
                "workflow_core_counts": {
                    wf_id: len(cores)
                    for wf_id, cores in self._workflow_cores.items()
                },
            }

    # =========================================================================
    # Internal Methods (must be called with lock held)
    # =========================================================================

    def _free_internal(self, workflow_id: str) -> list[int]:
        """
        Internal free implementation. Must be called with lock held.

        Args:
            workflow_id: Workflow whose cores should be freed

        Returns:
            List of core indices that were freed
        """
        allocated = self._workflow_cores.pop(workflow_id, [])

        for core_idx in allocated:
            if self._core_assignments.get(core_idx) == workflow_id:
                self._core_assignments[core_idx] = None

        self._available_cores = self._count_free_cores()

        # Signal that cores are available
        if allocated:
            self._cores_available.set()

        return allocated

    def _count_free_cores(self) -> int:
        """Count free cores. Must be called with lock held."""
        return sum(1 for wf_id in self._core_assignments.values() if wf_id is None)

    # =========================================================================
    # Logging Helpers
    # =========================================================================

    def _get_log_context(self, workflow_id: str = "") -> dict:
        """Get common context fields for logging."""
        return {
            "worker_id": self._worker_id,
            "workflow_id": workflow_id,
            "total_cores": self._total_cores,
            "available_cores": self._available_cores,
            "active_workflows": len(self._workflow_cores),
        }

    async def _log_trace(self, message: str, workflow_id: str = "") -> None:
        """Log a trace-level message."""
        await self._logger.log(AllocatorTrace(message=message, **self._get_log_context(workflow_id)))

    async def _log_debug(self, message: str, workflow_id: str = "") -> None:
        """Log a debug-level message."""
        await self._logger.log(AllocatorDebug(message=message, **self._get_log_context(workflow_id)))

    async def _log_info(self, message: str, workflow_id: str = "") -> None:
        """Log an info-level message."""
        await self._logger.log(AllocatorInfo(message=message, **self._get_log_context(workflow_id)))

    async def _log_warning(self, message: str, workflow_id: str = "") -> None:
        """Log a warning-level message."""
        await self._logger.log(AllocatorWarning(message=message, **self._get_log_context(workflow_id)))

    async def _log_error(self, message: str, workflow_id: str = "") -> None:
        """Log an error-level message."""
        await self._logger.log(AllocatorError(message=message, **self._get_log_context(workflow_id)))

    async def _log_critical(self, message: str, workflow_id: str = "") -> None:
        """Log a critical-level message."""
        await self._logger.log(AllocatorCritical(message=message, **self._get_log_context(workflow_id)))
