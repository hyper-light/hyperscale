"""
TCP handlers for windowed stats and job results.

Handles:
- WindowedStatsPush: Aggregated stats from managers
- JobFinalResult: Final job result from manager

Dependencies:
- Windowed stats collector
- Job manager
- Progress callbacks
- Forwarding tracker

TODO: Extract from gate.py:
- windowed_stats_push() (lines 7650+)
- job_final_result() (lines 6173-6257)
"""

from typing import Protocol


class StatsDependencies(Protocol):
    """Protocol defining dependencies for stats handlers."""

    def aggregate_stats(self, job_id: str, datacenter_id: str, stats) -> None:
        """Aggregate stats from a datacenter."""
        ...

    def push_stats_to_client(self, job_id: str) -> None:
        """Push aggregated stats to client callback."""
        ...

    def record_final_result(
        self, job_id: str, datacenter_id: str, result
    ) -> None:
        """Record final result from a datacenter."""
        ...


# Placeholder for full handler implementation
# The handlers will be extracted when the composition root is refactored

__all__ = ["StatsDependencies"]
