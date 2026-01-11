"""
Manager load shedding module.

Implements AD-22 priority-based load shedding to protect the system
under overload conditions while ensuring critical operations are never shed.

Uses the centralized AD-37 message classification from the reliability module
to ensure consistent priority handling across all node types.
"""

from typing import TYPE_CHECKING

from hyperscale.distributed_rewrite.reliability import (
    RequestPriority,
    classify_handler_to_priority,
    CONTROL_HANDLERS,
    DISPATCH_HANDLERS,
    DATA_HANDLERS,
    TELEMETRY_HANDLERS,
)
from hyperscale.logging.hyperscale_logging_models import ServerDebug, ServerWarning

if TYPE_CHECKING:
    from hyperscale.distributed_rewrite.nodes.manager.config import ManagerConfig
    from hyperscale.logging import Logger


# Re-export RequestPriority for backwards compatibility
__all__ = ["RequestPriority", "OverloadState", "ManagerLoadShedder"]


class OverloadState:
    """
    Simple overload state tracker.

    Tracks system load for shedding decisions.
    """

    __slots__ = ("_pending_count", "_max_pending", "_state")

    def __init__(self, max_pending: int = 1000) -> None:
        self._pending_count = 0
        self._max_pending = max_pending
        self._state = "healthy"

    def record_request_start(self) -> None:
        """Record start of request processing."""
        self._pending_count += 1
        self._update_state()

    def record_request_end(self) -> None:
        """Record end of request processing."""
        self._pending_count = max(0, self._pending_count - 1)
        self._update_state()

    def _update_state(self) -> None:
        """Update overload state based on pending count."""
        ratio = self._pending_count / self._max_pending
        if ratio < 0.5:
            self._state = "healthy"
        elif ratio < 0.7:
            self._state = "busy"
        elif ratio < 0.9:
            self._state = "stressed"
        else:
            self._state = "overloaded"

    def get_state(self) -> str:
        """Get current overload state."""
        return self._state

    @property
    def pending_count(self) -> int:
        """Get current pending request count."""
        return self._pending_count


class ManagerLoadShedder:
    """
    Determines whether to shed requests based on priority and load (AD-22).

    Shedding thresholds by overload state:
    - healthy: shed nothing (process all)
    - busy: shed LOW priority
    - stressed: shed NORMAL and LOW
    - overloaded: shed HIGH, NORMAL, LOW (only CRITICAL processed)
    """

    def __init__(
        self,
        config: "ManagerConfig",
        logger: "Logger",
        node_id: str,
        task_runner,
        max_pending: int = 1000,
    ) -> None:
        self._config = config
        self._logger = logger
        self._node_id = node_id
        self._task_runner = task_runner
        self._overload = OverloadState(max_pending)

        # Map overload state to minimum priority that gets processed
        # Requests with priority >= min_priority are shed
        self._shed_thresholds: dict[str, int] = {
            "healthy": 4,  # Process all (nothing shed)
            "busy": 3,  # Shed LOW
            "stressed": 2,  # Shed NORMAL and LOW
            "overloaded": 1,  # Only CRITICAL (shed HIGH, NORMAL, LOW)
        }

        # Message type to priority classification
        self._priority_map: dict[str, RequestPriority] = {}
        self._init_priority_map()

        # Metrics
        self._shed_count: dict[str, int] = {
            "CRITICAL": 0,
            "HIGH": 0,
            "NORMAL": 0,
            "LOW": 0,
        }
        self._total_processed: int = 0

    def _init_priority_map(self) -> None:
        """Initialize message type to priority mapping."""
        # CRITICAL - Never shed
        critical_types = {
            "ping",
            "pong",
            "swim_probe",
            "swim_ack",
            "cancel_job",
            "cancel_workflow",
            "final_result",
            "job_complete",
            "leadership_transfer",
            "leadership_claim",
        }

        # HIGH - Shed under severe overload
        high_types = {
            "job_submit",
            "workflow_dispatch",
            "state_sync_request",
            "state_sync_response",
            "provision_request",
            "provision_confirm",
            "worker_registration",
        }

        # NORMAL - Shed under moderate overload
        normal_types = {
            "progress_update",
            "stats_query",
            "heartbeat",
            "worker_heartbeat",
            "register_callback",
            "reconnect",
        }

        for msg_type in critical_types:
            self._priority_map[msg_type] = RequestPriority.CRITICAL
        for msg_type in high_types:
            self._priority_map[msg_type] = RequestPriority.HIGH
        for msg_type in normal_types:
            self._priority_map[msg_type] = RequestPriority.NORMAL
        # Everything else defaults to LOW

    def classify_request(self, message_type: str) -> RequestPriority:
        """
        Classify request by message type.

        Args:
            message_type: Type of message being processed

        Returns:
            RequestPriority for the message
        """
        return self._priority_map.get(message_type, RequestPriority.LOW)

    def should_shed(self, priority: RequestPriority) -> bool:
        """
        Check if request should be shed based on priority and load.

        Args:
            priority: Priority of the request

        Returns:
            True if request should be shed (rejected)
        """
        state = self._overload.get_state()
        min_priority_processed = self._shed_thresholds.get(state, 4)

        # Shed if priority.value >= threshold (lower value = higher priority)
        should_shed = priority.value >= min_priority_processed

        if should_shed:
            self._shed_count[priority.name] += 1

        return should_shed

    def should_shed_message(self, message_type: str) -> bool:
        """
        Check if message should be shed.

        Convenience method that classifies and checks in one call.

        Args:
            message_type: Type of message

        Returns:
            True if message should be shed
        """
        priority = self.classify_request(message_type)
        return self.should_shed(priority)

    def on_request_start(self) -> None:
        """Called when request processing starts."""
        self._overload.record_request_start()
        self._total_processed += 1

    def on_request_end(self) -> None:
        """Called when request processing ends."""
        self._overload.record_request_end()

    def get_overload_state(self) -> str:
        """Get current overload state."""
        return self._overload.get_state()

    def get_metrics(self) -> dict:
        """Get load shedding metrics."""
        return {
            "overload_state": self._overload.get_state(),
            "pending_count": self._overload.pending_count,
            "total_processed": self._total_processed,
            "shed_critical": self._shed_count["CRITICAL"],
            "shed_high": self._shed_count["HIGH"],
            "shed_normal": self._shed_count["NORMAL"],
            "shed_low": self._shed_count["LOW"],
            "total_shed": sum(self._shed_count.values()),
        }
