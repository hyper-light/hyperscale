"""
Execution metrics for worker performance tracking.

Tracks workflow execution statistics, completion times,
and throughput for health signal calculation.
"""

from dataclasses import dataclass, field


@dataclass(slots=True)
class ExecutionMetrics:
    """
    Execution metrics for worker performance tracking.

    Used for AD-19 Three-Signal Health Model throughput calculation
    and general performance monitoring.
    """

    workflows_executed: int = 0
    workflows_completed: int = 0
    workflows_failed: int = 0
    workflows_cancelled: int = 0
    total_cores_allocated: int = 0
    total_execution_time_seconds: float = 0.0
    throughput_completions: int = 0
    throughput_interval_start: float = 0.0
    throughput_last_value: float = 0.0


@dataclass(slots=True)
class CompletionTimeTracker:
    """
    Tracks recent completion times for expected throughput calculation.

    Maintains a sliding window of completion times to estimate
    expected throughput for health signal reporting.
    """

    max_samples: int = 50
    completion_times: list[float] = field(default_factory=list)

    def add_completion_time(self, duration_seconds: float) -> None:
        """Add a completion time, maintaining max samples."""
        self.completion_times.append(duration_seconds)
        if len(self.completion_times) > self.max_samples:
            self.completion_times.pop(0)

    def get_average_completion_time(self) -> float:
        """Get average completion time, or 0.0 if no samples."""
        if not self.completion_times:
            return 0.0
        return sum(self.completion_times) / len(self.completion_times)
