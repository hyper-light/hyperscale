"""
Load Shedding with Priority Queues (AD-22, AD-37).

Provides graceful degradation under load by shedding low-priority
requests based on current overload state.

Uses unified MessageClass classification from AD-37:
- CONTROL (CRITICAL): SWIM probes/acks, cancellation, leadership - never shed
- DISPATCH (HIGH): Job submissions, workflow dispatch, state sync
- DATA (NORMAL): Progress updates, stats queries
- TELEMETRY (LOW): Debug stats, detailed metrics - shed first

Shedding Behavior by State:
- healthy: Accept all requests
- busy: Shed TELEMETRY (LOW) only
- stressed: Shed DATA (NORMAL) and TELEMETRY (LOW)
- overloaded: Shed all except CONTROL (CRITICAL)
"""

from dataclasses import dataclass, field
from enum import IntEnum

from hyperscale.distributed_rewrite.reliability.overload import (
    HybridOverloadDetector,
    OverloadState,
)
from hyperscale.distributed_rewrite.reliability.message_class import (
    MessageClass,
    classify_handler,
)


class RequestPriority(IntEnum):
    """Priority levels for request classification.

    Lower values indicate higher priority.
    Maps directly to AD-37 MessageClass via MESSAGE_CLASS_TO_PRIORITY.
    """

    CRITICAL = 0  # CONTROL: SWIM probes/acks, cancellation, leadership - never shed
    HIGH = 1  # DISPATCH: Job submissions, workflow dispatch, state sync
    NORMAL = 2  # DATA: Progress updates, stats queries
    LOW = 3  # TELEMETRY: Debug stats, detailed metrics


# Mapping from MessageClass to RequestPriority (AD-37 compliance)
MESSAGE_CLASS_TO_REQUEST_PRIORITY: dict[MessageClass, RequestPriority] = {
    MessageClass.CONTROL: RequestPriority.CRITICAL,
    MessageClass.DISPATCH: RequestPriority.HIGH,
    MessageClass.DATA: RequestPriority.NORMAL,
    MessageClass.TELEMETRY: RequestPriority.LOW,
}


@dataclass(slots=True)
class LoadShedderConfig:
    """Configuration for LoadShedder behavior."""

    # Mapping of overload state to minimum priority that gets shed
    # Requests with priority >= this threshold are shed
    shed_thresholds: dict[OverloadState, RequestPriority | None] = field(
        default_factory=lambda: {
            OverloadState.HEALTHY: None,  # Accept all
            OverloadState.BUSY: RequestPriority.LOW,  # Shed TELEMETRY only
            OverloadState.STRESSED: RequestPriority.NORMAL,  # Shed DATA and TELEMETRY
            OverloadState.OVERLOADED: RequestPriority.HIGH,  # Shed all except CONTROL
        }
    )


# Legacy message type to priority mapping for backwards compatibility
# New code should use classify_handler_to_priority() which uses AD-37 MessageClass
DEFAULT_MESSAGE_PRIORITIES: dict[str, RequestPriority] = {
    # CRITICAL/CONTROL priority - never shed
    "Ping": RequestPriority.CRITICAL,
    "Ack": RequestPriority.CRITICAL,
    "Nack": RequestPriority.CRITICAL,
    "PingReq": RequestPriority.CRITICAL,
    "Suspect": RequestPriority.CRITICAL,
    "Alive": RequestPriority.CRITICAL,
    "Dead": RequestPriority.CRITICAL,
    "Join": RequestPriority.CRITICAL,
    "JoinAck": RequestPriority.CRITICAL,
    "Leave": RequestPriority.CRITICAL,
    "JobCancelRequest": RequestPriority.CRITICAL,
    "JobCancelResponse": RequestPriority.CRITICAL,
    "JobFinalResult": RequestPriority.CRITICAL,
    "Heartbeat": RequestPriority.CRITICAL,
    "HealthCheck": RequestPriority.CRITICAL,
    # HIGH/DISPATCH priority
    "SubmitJob": RequestPriority.HIGH,
    "SubmitJobResponse": RequestPriority.HIGH,
    "JobAssignment": RequestPriority.HIGH,
    "WorkflowDispatch": RequestPriority.HIGH,
    "WorkflowComplete": RequestPriority.HIGH,
    "StateSync": RequestPriority.HIGH,
    "StateSyncRequest": RequestPriority.HIGH,
    "StateSyncResponse": RequestPriority.HIGH,
    "AntiEntropyRequest": RequestPriority.HIGH,
    "AntiEntropyResponse": RequestPriority.HIGH,
    "JobLeaderGateTransfer": RequestPriority.HIGH,
    "JobLeaderGateTransferAck": RequestPriority.HIGH,
    # NORMAL/DATA priority
    "JobProgress": RequestPriority.NORMAL,
    "JobStatusRequest": RequestPriority.NORMAL,
    "JobStatusResponse": RequestPriority.NORMAL,
    "JobStatusPush": RequestPriority.NORMAL,
    "RegisterCallback": RequestPriority.NORMAL,
    "RegisterCallbackResponse": RequestPriority.NORMAL,
    "StatsUpdate": RequestPriority.NORMAL,
    "StatsQuery": RequestPriority.NORMAL,
    # LOW/TELEMETRY priority - shed first
    "DetailedStatsRequest": RequestPriority.LOW,
    "DetailedStatsResponse": RequestPriority.LOW,
    "DebugRequest": RequestPriority.LOW,
    "DebugResponse": RequestPriority.LOW,
    "DiagnosticsRequest": RequestPriority.LOW,
    "DiagnosticsResponse": RequestPriority.LOW,
}


def classify_handler_to_priority(handler_name: str) -> RequestPriority:
    """
    Classify a handler using AD-37 MessageClass and return RequestPriority.

    This is the preferred classification method that uses the unified
    AD-37 message classification system.

    Args:
        handler_name: Name of the handler (e.g., "receive_workflow_progress")

    Returns:
        RequestPriority based on AD-37 MessageClass
    """
    message_class = classify_handler(handler_name)
    return MESSAGE_CLASS_TO_REQUEST_PRIORITY[message_class]


class LoadShedder:
    """
    Load shedder that drops requests based on priority and overload state.

    Uses HybridOverloadDetector to determine current load and decides
    whether to accept or shed incoming requests based on their priority.

    Example usage:
        detector = HybridOverloadDetector()
        shedder = LoadShedder(detector)

        # Record latencies from processing
        detector.record_latency(50.0)

        # Check if request should be processed
        message_type = "StatsUpdate"
        if shedder.should_shed(message_type):
            # Return 503 or similar
            return ServiceUnavailableResponse()
        else:
            # Process the request
            handle_stats_update()
    """

    def __init__(
        self,
        overload_detector: HybridOverloadDetector,
        config: LoadShedderConfig | None = None,
        message_priorities: dict[str, RequestPriority] | None = None,
    ):
        """
        Initialize LoadShedder.

        Args:
            overload_detector: Detector for current system load state
            config: Configuration for shedding behavior
            message_priorities: Custom message type to priority mapping
        """
        self._detector = overload_detector
        self._config = config or LoadShedderConfig()
        self._message_priorities = message_priorities or DEFAULT_MESSAGE_PRIORITIES.copy()

        # Metrics
        self._total_requests = 0
        self._shed_requests = 0
        self._shed_by_priority: dict[RequestPriority, int] = {p: 0 for p in RequestPriority}

    def classify_request(self, message_type: str) -> RequestPriority:
        """
        Classify a request by message type to determine its priority.

        Args:
            message_type: The type of message/request

        Returns:
            RequestPriority for the message type, defaults to NORMAL if unknown
        """
        return self._message_priorities.get(message_type, RequestPriority.NORMAL)

    def should_shed(
        self,
        message_type: str,
        cpu_percent: float | None = None,
        memory_percent: float | None = None,
    ) -> bool:
        """
        Determine if a request should be shed based on current load.

        Uses legacy message type mapping. For AD-37 compliant classification,
        use should_shed_handler() instead.

        Args:
            message_type: The type of message/request
            cpu_percent: Current CPU utilization (0-100), optional
            memory_percent: Current memory utilization (0-100), optional

        Returns:
            True if request should be shed, False if it should be processed
        """
        self._total_requests += 1

        priority = self.classify_request(message_type)
        return self.should_shed_priority(priority, cpu_percent, memory_percent)

    def should_shed_handler(
        self,
        handler_name: str,
        cpu_percent: float | None = None,
        memory_percent: float | None = None,
    ) -> bool:
        """
        Determine if a request should be shed using AD-37 MessageClass classification.

        This is the preferred method for AD-37 compliant load shedding.
        Uses classify_handler() to determine MessageClass and maps to RequestPriority.

        Args:
            handler_name: Name of the handler (e.g., "receive_workflow_progress")
            cpu_percent: Current CPU utilization (0-100), optional
            memory_percent: Current memory utilization (0-100), optional

        Returns:
            True if request should be shed, False if it should be processed
        """
        self._total_requests += 1

        priority = classify_handler_to_priority(handler_name)
        return self.should_shed_priority(priority, cpu_percent, memory_percent)

    def should_shed_priority(
        self,
        priority: RequestPriority,
        cpu_percent: float | None = None,
        memory_percent: float | None = None,
    ) -> bool:
        """
        Determine if a request with given priority should be shed.

        Args:
            priority: The priority of the request
            cpu_percent: Current CPU utilization (0-100), optional
            memory_percent: Current memory utilization (0-100), optional

        Returns:
            True if request should be shed, False if it should be processed
        """
        # Default None to 0.0 for detector
        cpu = cpu_percent if cpu_percent is not None else 0.0
        memory = memory_percent if memory_percent is not None else 0.0
        state = self._detector.get_state(cpu, memory)
        threshold = self._config.shed_thresholds.get(state)

        # No threshold means accept all requests
        if threshold is None:
            return False

        # Shed if priority is at or below threshold (higher number = lower priority)
        should_shed = priority >= threshold

        if should_shed:
            self._shed_requests += 1
            self._shed_by_priority[priority] += 1

        return should_shed

    def get_current_state(
        self,
        cpu_percent: float | None = None,
        memory_percent: float | None = None,
    ) -> OverloadState:
        """
        Get the current overload state.

        Args:
            cpu_percent: Current CPU utilization (0-100), optional
            memory_percent: Current memory utilization (0-100), optional

        Returns:
            Current OverloadState
        """
        # Default None to 0.0 for detector
        cpu = cpu_percent if cpu_percent is not None else 0.0
        memory = memory_percent if memory_percent is not None else 0.0
        return self._detector.get_state(cpu, memory)

    def register_message_priority(
        self,
        message_type: str,
        priority: RequestPriority,
    ) -> None:
        """
        Register or update priority for a message type.

        Args:
            message_type: The type of message
            priority: The priority to assign
        """
        self._message_priorities[message_type] = priority

    def get_metrics(self) -> dict:
        """
        Get shedding metrics.

        Returns:
            Dictionary with shedding statistics
        """
        shed_rate = (
            self._shed_requests / self._total_requests
            if self._total_requests > 0
            else 0.0
        )

        return {
            "total_requests": self._total_requests,
            "shed_requests": self._shed_requests,
            "shed_rate": shed_rate,
            "shed_by_priority": {
                priority.name: count
                for priority, count in self._shed_by_priority.items()
            },
        }

    def reset_metrics(self) -> None:
        """Reset all metrics counters."""
        self._total_requests = 0
        self._shed_requests = 0
        self._shed_by_priority = {p: 0 for p in RequestPriority}
