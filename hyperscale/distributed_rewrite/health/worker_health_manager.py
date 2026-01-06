"""
Worker Health Manager for Adaptive Healthcheck Extensions (AD-26).

This module provides the WorkerHealthManager class that managers use
to track worker health and handle deadline extension requests.

Key responsibilities:
- Track ExtensionTracker per worker
- Handle extension requests with proper validation
- Reset trackers when workers become healthy
- Coordinate with the three-signal health model (AD-19)
"""

from dataclasses import dataclass, field
import time

from hyperscale.distributed_rewrite.health.extension_tracker import (
    ExtensionTracker,
    ExtensionTrackerConfig,
)
from hyperscale.distributed_rewrite.models import (
    HealthcheckExtensionRequest,
    HealthcheckExtensionResponse,
)


@dataclass(slots=True)
class WorkerHealthManagerConfig:
    """
    Configuration for WorkerHealthManager.

    Attributes:
        base_deadline: Base deadline in seconds for extensions.
        min_grant: Minimum extension grant in seconds.
        max_extensions: Maximum extensions per worker per cycle.
        eviction_threshold: Number of failed extensions before eviction.
    """

    base_deadline: float = 30.0
    min_grant: float = 1.0
    max_extensions: int = 5
    eviction_threshold: int = 3


class WorkerHealthManager:
    """
    Manages worker health and deadline extensions.

    This class is used by managers to:
    1. Track ExtensionTracker instances for each worker
    2. Handle extension requests from workers
    3. Reset trackers when workers become healthy
    4. Determine when workers should be evicted

    Thread Safety:
    - The manager should ensure proper locking when accessing this class
    - Each worker has its own ExtensionTracker instance

    Usage:
        manager = WorkerHealthManager(config)

        # When worker requests extension
        response = manager.handle_extension_request(request, current_deadline)

        # When worker becomes healthy
        manager.on_worker_healthy(worker_id)

        # When checking if worker should be evicted
        should_evict, reason = manager.should_evict_worker(worker_id)
    """

    def __init__(self, config: WorkerHealthManagerConfig | None = None):
        """
        Initialize the WorkerHealthManager.

        Args:
            config: Configuration for extension tracking. Uses defaults if None.
        """
        self._config = config or WorkerHealthManagerConfig()
        self._extension_config = ExtensionTrackerConfig(
            base_deadline=self._config.base_deadline,
            min_grant=self._config.min_grant,
            max_extensions=self._config.max_extensions,
        )

        # Per-worker extension trackers
        self._trackers: dict[str, ExtensionTracker] = {}

        # Track consecutive extension failures for eviction decisions
        self._extension_failures: dict[str, int] = {}

    def _get_tracker(self, worker_id: str) -> ExtensionTracker:
        """Get or create an ExtensionTracker for a worker."""
        if worker_id not in self._trackers:
            self._trackers[worker_id] = self._extension_config.create_tracker(worker_id)
        return self._trackers[worker_id]

    def handle_extension_request(
        self,
        request: HealthcheckExtensionRequest,
        current_deadline: float,
    ) -> HealthcheckExtensionResponse:
        """
        Handle a deadline extension request from a worker.

        Args:
            request: The extension request from the worker.
            current_deadline: The worker's current deadline timestamp.

        Returns:
            HealthcheckExtensionResponse with the decision.
        """
        tracker = self._get_tracker(request.worker_id)

        # Attempt to grant extension
        granted, extension_seconds, denial_reason = tracker.request_extension(
            reason=request.reason,
            current_progress=request.current_progress,
        )

        if granted:
            # Clear extension failure count on successful grant
            self._extension_failures.pop(request.worker_id, None)

            new_deadline = tracker.get_new_deadline(current_deadline, extension_seconds)

            return HealthcheckExtensionResponse(
                granted=True,
                extension_seconds=extension_seconds,
                new_deadline=new_deadline,
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=None,
            )
        else:
            # Track extension failures
            failures = self._extension_failures.get(request.worker_id, 0) + 1
            self._extension_failures[request.worker_id] = failures

            return HealthcheckExtensionResponse(
                granted=False,
                extension_seconds=0.0,
                new_deadline=current_deadline,  # Unchanged
                remaining_extensions=tracker.get_remaining_extensions(),
                denial_reason=denial_reason,
            )

    def on_worker_healthy(self, worker_id: str) -> None:
        """
        Reset extension tracking when a worker becomes healthy.

        Call this when:
        - Worker responds to liveness probe
        - Worker completes a workflow successfully
        - Worker's health signals indicate recovery

        Args:
            worker_id: ID of the worker that became healthy.
        """
        tracker = self._trackers.get(worker_id)
        if tracker:
            tracker.reset()

        # Clear extension failures
        self._extension_failures.pop(worker_id, None)

    def on_worker_removed(self, worker_id: str) -> None:
        """
        Clean up tracking state when a worker is removed.

        Call this when:
        - Worker is evicted
        - Worker leaves the cluster
        - Worker is marked as dead

        Args:
            worker_id: ID of the worker being removed.
        """
        self._trackers.pop(worker_id, None)
        self._extension_failures.pop(worker_id, None)

    def should_evict_worker(self, worker_id: str) -> tuple[bool, str | None]:
        """
        Determine if a worker should be evicted based on extension failures.

        A worker should be evicted if it has exhausted all extensions
        and failed to make progress, indicating it is stuck.

        Args:
            worker_id: ID of the worker to check.

        Returns:
            Tuple of (should_evict, reason).
        """
        failures = self._extension_failures.get(worker_id, 0)

        if failures >= self._config.eviction_threshold:
            return (
                True,
                f"Worker exhausted {failures} extension requests without progress",
            )

        tracker = self._trackers.get(worker_id)
        if tracker and tracker.is_exhausted:
            return (
                True,
                f"Worker exhausted all {self._config.max_extensions} deadline extensions",
            )

        return (False, None)

    def get_worker_extension_state(self, worker_id: str) -> dict:
        """
        Get the extension tracking state for a worker.

        Useful for debugging and observability.

        Args:
            worker_id: ID of the worker.

        Returns:
            Dict with extension tracking information.
        """
        tracker = self._trackers.get(worker_id)
        if not tracker:
            return {
                "worker_id": worker_id,
                "has_tracker": False,
            }

        return {
            "worker_id": worker_id,
            "has_tracker": True,
            "extension_count": tracker.extension_count,
            "remaining_extensions": tracker.get_remaining_extensions(),
            "total_extended": tracker.total_extended,
            "last_progress": tracker.last_progress,
            "is_exhausted": tracker.is_exhausted,
            "extension_failures": self._extension_failures.get(worker_id, 0),
        }

    def get_all_extension_states(self) -> dict[str, dict]:
        """
        Get extension tracking state for all workers.

        Returns:
            Dict mapping worker_id to extension state.
        """
        return {
            worker_id: self.get_worker_extension_state(worker_id)
            for worker_id in self._trackers
        }

    @property
    def tracked_worker_count(self) -> int:
        """Get the number of workers with active extension trackers."""
        return len(self._trackers)
