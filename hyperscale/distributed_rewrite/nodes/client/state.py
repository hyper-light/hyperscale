"""
Client runtime state for HyperscaleClient.

Manages all mutable state including job tracking, leadership, cancellations,
callbacks, and metrics.
"""

import asyncio
from typing import Callable

from hyperscale.distributed_rewrite.models import (
    ClientJobResult,
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
    NegotiatedCapabilities,
)


class ClientState:
    """
    Runtime state for HyperscaleClient.

    Centralizes all mutable dictionaries and tracking structures.
    Provides clean separation between configuration (immutable) and
    runtime state (mutable).
    """

    def __init__(self) -> None:
        """Initialize empty state containers."""
        # Job tracking
        self._jobs: dict[str, ClientJobResult] = {}
        self._job_events: dict[str, asyncio.Event] = {}
        self._job_callbacks: dict[str, Callable[[ClientJobResult], None]] = {}
        self._job_targets: dict[str, tuple[str, int]] = {}

        # Cancellation tracking
        self._cancellation_events: dict[str, asyncio.Event] = {}
        self._cancellation_errors: dict[str, list[str]] = {}
        self._cancellation_success: dict[str, bool] = {}

        # Reporter and workflow callbacks
        self._reporter_callbacks: dict[str, Callable] = {}
        self._workflow_callbacks: dict[str, Callable] = {}
        self._job_reporting_configs: dict[str, list] = {}

        # Progress callbacks
        self._progress_callbacks: dict[str, Callable] = {}

        # Protocol negotiation state
        self._server_negotiated_caps: dict[tuple[str, int], NegotiatedCapabilities] = {}

        # Target selection state (round-robin indices)
        self._current_manager_idx: int = 0
        self._current_gate_idx: int = 0

        # Gate leadership tracking
        self._gate_job_leaders: dict[str, GateLeaderInfo] = {}

        # Manager leadership tracking (keyed by (job_id, datacenter_id))
        self._manager_job_leaders: dict[tuple[str, str], ManagerLeaderInfo] = {}

        # Request routing locks (per-job)
        self._request_routing_locks: dict[str, asyncio.Lock] = {}

        # Orphaned job tracking
        self._orphaned_jobs: dict[str, OrphanedJobInfo] = {}

        # Leadership transfer metrics
        self._gate_transfers_received: int = 0
        self._manager_transfers_received: int = 0
        self._requests_rerouted: int = 0
        self._requests_failed_leadership_change: int = 0

        # Gate connection state
        self._gate_connection_state: dict[tuple[str, int], str] = {}

    def initialize_job_tracking(
        self,
        job_id: str,
        initial_result: ClientJobResult,
        callback: Callable[[ClientJobResult], None] | None = None,
    ) -> None:
        """
        Initialize tracking structures for a new job.

        Args:
            job_id: Job identifier
            initial_result: Initial job result (typically SUBMITTED status)
            callback: Optional callback to invoke on status updates
        """
        self._jobs[job_id] = initial_result
        self._job_events[job_id] = asyncio.Event()
        if callback:
            self._job_callbacks[job_id] = callback

    def initialize_cancellation_tracking(self, job_id: str) -> None:
        """
        Initialize tracking structures for job cancellation.

        Args:
            job_id: Job identifier
        """
        self._cancellation_events[job_id] = asyncio.Event()
        self._cancellation_success[job_id] = False
        self._cancellation_errors[job_id] = []

    def mark_job_target(self, job_id: str, target: tuple[str, int]) -> None:
        """
        Mark the target server for a job (for sticky routing).

        Args:
            job_id: Job identifier
            target: (host, port) tuple of target server
        """
        self._job_targets[job_id] = target

    def get_job_target(self, job_id: str) -> tuple[str, int] | None:
        """
        Get the known target for a job.

        Args:
            job_id: Job identifier

        Returns:
            Target (host, port) or None if not known
        """
        return self._job_targets.get(job_id)

    def get_or_create_routing_lock(self, job_id: str) -> asyncio.Lock:
        """
        Get or create a routing lock for a job.

        Args:
            job_id: Job identifier

        Returns:
            asyncio.Lock for this job's routing decisions
        """
        if job_id not in self._request_routing_locks:
            self._request_routing_locks[job_id] = asyncio.Lock()
        return self._request_routing_locks[job_id]

    def mark_job_orphaned(self, job_id: str, orphan_info: OrphanedJobInfo) -> None:
        """
        Mark a job as orphaned.

        Args:
            job_id: Job identifier
            orphan_info: Orphan information
        """
        self._orphaned_jobs[job_id] = orphan_info

    def clear_job_orphaned(self, job_id: str) -> None:
        """
        Clear orphaned status for a job.

        Args:
            job_id: Job identifier
        """
        self._orphaned_jobs.pop(job_id, None)

    def is_job_orphaned(self, job_id: str) -> bool:
        """
        Check if a job is orphaned.

        Args:
            job_id: Job identifier

        Returns:
            True if job is orphaned
        """
        return job_id in self._orphaned_jobs

    def increment_gate_transfers(self) -> None:
        """Increment gate transfer counter."""
        self._gate_transfers_received += 1

    def increment_manager_transfers(self) -> None:
        """Increment manager transfer counter."""
        self._manager_transfers_received += 1

    def increment_rerouted(self) -> None:
        """Increment requests rerouted counter."""
        self._requests_rerouted += 1

    def increment_failed_leadership_change(self) -> None:
        """Increment failed leadership change counter."""
        self._requests_failed_leadership_change += 1

    def get_leadership_metrics(self) -> dict:
        """
        Get leadership and orphan tracking metrics.

        Returns:
            Dict with transfer counts, rerouted requests, failures, and orphan status
        """
        return {
            "gate_transfers_received": self._gate_transfers_received,
            "manager_transfers_received": self._manager_transfers_received,
            "requests_rerouted": self._requests_rerouted,
            "requests_failed_leadership_change": self._requests_failed_leadership_change,
            "orphaned_jobs": len(self._orphaned_jobs),
            "tracked_gate_leaders": len(self._gate_job_leaders),
            "tracked_manager_leaders": len(self._manager_job_leaders),
        }
