"""``LoadShedder`` -- pickled under the namespace
``hyperscale.distributed.reliability.load_shedding`` (see that module)."""

from hyperscale.distributed.reliability.overload import HybridOverloadDetector, OverloadState
from hyperscale.distributed.reliability.priority import RequestPriority
from hyperscale.distributed.reliability.message_class import MessageClass, classify_handler

from .load_shedder_config import LoadShedderConfig

# Mapping from MessageClass to RequestPriority (AD-37 compliance)
MESSAGE_CLASS_TO_REQUEST_PRIORITY: dict[MessageClass, RequestPriority] = {
    MessageClass.CONTROL: RequestPriority.CRITICAL,
    MessageClass.DISPATCH: RequestPriority.HIGH,
    MessageClass.DATA: RequestPriority.NORMAL,
    MessageClass.TELEMETRY: RequestPriority.LOW,
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
        if shedder.should_shed_handler("workflow_progress"):
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
        detector_sampled_externally: bool = False,
    ):
        """
        Initialize LoadShedder.

        Args:
            overload_detector: Detector for current system load state
            config: Configuration for shedding behavior
            detector_sampled_externally: True when another owner (a
                node's resource sampler) feeds the detector its CPU and
                memory samples: checks without readings then read the
                state that sampler settled on instead of sampling zeros
        """
        self._detector = overload_detector
        self._detector_sampled_externally = detector_sampled_externally
        self._config = config or LoadShedderConfig()

        # Metrics
        self._total_requests = 0
        self._shed_requests = 0
        self._shed_by_priority: dict[RequestPriority, int] = {
            p: 0 for p in RequestPriority
        }

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
        state = self._overload_state(cpu_percent, memory_percent)
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
        return self._overload_state(cpu_percent, memory_percent)

    def _overload_state(self, cpu_percent: float | None, memory_percent: float | None) -> OverloadState:
        """Sample the detector with the caller's resource readings. With
        none and an external sampler, read the state that sampler settled
        on: a per-request sample of zero CPU and memory would count toward
        the detector's de-escalation hysteresis, so a burst of requests
        talked a CPU-overloaded node out of shedding."""
        if cpu_percent is None and memory_percent is None and self._detector_sampled_externally:
            return self._detector.current_state
        return self._detector.get_state(cpu_percent or 0.0, memory_percent or 0.0)

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
