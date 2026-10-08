"""
Time-Aligned Results Aggregation.

This module provides time-aware aggregation for WorkflowStats across multiple
workers and datacenters, reporting how far apart in time the sources' stats
were collected.

Time Alignment Strategy:
- Each WorkflowStats or progress update includes a `collected_at` Unix timestamp
- Merged stats are Results.merge_results() of the sources: when a source's
  stats were collected changes neither its samples nor its elapsed time
- Concurrent sources' rates add; counts can be interpolated to a common
  reference time

Usage:
    from hyperscale.reporting.time_aligned_results import TimeAlignedResults

    aggregator = TimeAlignedResults()

    # Aggregate WorkflowStats with time awareness
    aligned_stats = aggregator.merge_with_time_alignment(
        workflow_stats_list,
        reference_time=time.time(),  # Align to this timestamp
    )
"""

import math
import operator
import time
from typing import Dict, List, Optional

from hyperscale.reporting.common.results_types import WorkflowStats
from hyperscale.reporting.results import Results

from .time_alignment_metadata import TimeAlignmentMetadata as TimeAlignmentMetadata
from .timestamped_stats import TimestampedStats as TimestampedStats

# A progress update's rate; an update without one contributes none.
RATE_PER_SECOND = operator.methodcaller("get", "rate_per_second", 0.0)


class TimeAlignedResults(Results):
    """
    Time-aware results aggregator that accounts for collection time differences.

    Extends the base Results class to provide time-aligned aggregation,
    which is important for accurate rate calculations when aggregating
    data from multiple workers or datacenters with network latency.

    Beyond merge_results():
    - Time skew reporting: Reports the time spread across sources
    - Reference time alignment: Can align progress counts to a specific timestamp
    """

    def __init__(
        self,
        precision: int = 8,
        max_time_skew_warning_seconds: float = 5.0,
    ) -> None:
        """
        Initialize the time-aligned results aggregator.

        Args:
            precision: Decimal precision for calculations
            max_time_skew_warning_seconds: Log warning if time skew exceeds this
        """
        super().__init__(precision=precision)
        self._max_time_skew_warning = max_time_skew_warning_seconds

    def merge_with_time_alignment(
        self,
        timestamped_stats: List[TimestampedStats],
        reference_time: Optional[float] = None,
    ) -> tuple[WorkflowStats, TimeAlignmentMetadata]:
        """
        Merge WorkflowStats with time alignment.

        The merged stats are merge_results() of the sources; the metadata
        reports their collection timestamps and time skew.

        Args:
            timestamped_stats: List of stats with collection timestamps
            reference_time: Optional reference time to align to (defaults to max collected_at)

        Returns:
            Tuple of (merged WorkflowStats, alignment metadata)
        """
        if not timestamped_stats:
            raise ValueError("Cannot merge empty stats list")

        # Extract raw stats for base merge
        workflow_stats_list = [ts.stats for ts in timestamped_stats]

        # Calculate time alignment metadata
        collection_times = [ts.collected_at for ts in timestamped_stats]
        min_collected = min(collection_times)
        max_collected = max(collection_times)
        time_spread = max_collected - min_collected

        if reference_time is None:
            reference_time = max_collected

        sources = [ts.source for ts in timestamped_stats if ts.source]

        metadata = TimeAlignmentMetadata(
            reference_time=reference_time,
            min_collected_at=min_collected,
            max_collected_at=max_collected,
            time_spread_seconds=time_spread,
            sources_count=len(timestamped_stats),
            sources=sources,
        )

        # The sources ran concurrently: the merged rate is every action over
        # the longest elapsed, whenever each source's stats were collected.
        return self.merge_results(workflow_stats_list), metadata

    def aggregate_progress_stats(
        self,
        progress_updates: List[Dict[str, float]],
        reference_time: Optional[float] = None,
    ) -> Dict[str, float]:
        """
        Aggregate progress statistics with time alignment.

        Used for aggregating WorkflowProgress or JobProgress updates
        from multiple workers/datacenters.

        Args:
            progress_updates: List of progress dicts, each containing:
                - collected_at: Unix timestamp
                - completed_count: Total completed
                - failed_count: Total failed
                - rate_per_second: Current rate
                - elapsed_seconds: Time since start
            reference_time: Optional reference time (defaults to now)

        Returns:
            Aggregated progress dict with time-aligned metrics
        """
        if not progress_updates:
            return {
                "completed_count": 0,
                "failed_count": 0,
                "rate_per_second": 0.0,
                "elapsed_seconds": 0.0,
                "collected_at": time.time(),
            }

        if reference_time is None:
            reference_time = time.time()

        # Extract collection times
        collection_times = [
            p.get("collected_at", reference_time)
            for p in progress_updates
        ]
        min_collected = min(collection_times)
        max_collected = max(collection_times)

        # Sum counts (these are cumulative, not rates)
        total_completed = sum(p.get("completed_count", 0) for p in progress_updates)
        total_failed = sum(p.get("failed_count", 0) for p in progress_updates)

        # The sources run concurrently: their rates add (fsum: correctly
        # rounded, whatever order the updates arrived in).
        total_rate = math.fsum(map(RATE_PER_SECOND, progress_updates))

        # Use maximum elapsed as the reference (all sources started around same time)
        max_elapsed = max(p.get("elapsed_seconds", 0.0) for p in progress_updates)

        return {
            "completed_count": total_completed,
            "failed_count": total_failed,
            "rate_per_second": total_rate,
            "elapsed_seconds": max_elapsed,
            "collected_at": reference_time,
            "time_spread_seconds": max_collected - min_collected,
            "sources_count": len(progress_updates),
        }

    def interpolate_to_reference_time(
        self,
        progress_updates: List[Dict[str, float]],
        reference_time: float,
    ) -> Dict[str, float]:
        """
        Interpolate progress values to a common reference time.

        Uses linear interpolation based on rate to estimate what the
        counts would be at the reference time.

        Args:
            progress_updates: List of progress dicts
            reference_time: Target time to interpolate to

        Returns:
            Interpolated progress dict
        """
        if not progress_updates:
            return {
                "completed_count": 0,
                "failed_count": 0,
                "rate_per_second": 0.0,
                "elapsed_seconds": 0.0,
                "collected_at": reference_time,
            }

        interpolated_completed = 0
        interpolated_failed = 0

        for progress in progress_updates:
            collected_at = progress.get("collected_at", reference_time)
            rate = progress.get("rate_per_second", 0.0)
            completed = progress.get("completed_count", 0)
            failed = progress.get("failed_count", 0)

            # Calculate time delta
            time_delta = reference_time - collected_at

            if time_delta > 0 and rate > 0:
                # Extrapolate forward: estimate additional completions
                estimated_additional = int(rate * time_delta)
                interpolated_completed += completed + estimated_additional
            elif time_delta < 0 and rate > 0:
                # Interpolate backward: estimate fewer completions
                estimated_reduction = int(rate * abs(time_delta))
                interpolated_completed += max(0, completed - estimated_reduction)
            else:
                interpolated_completed += completed

            # Failed counts typically don't change with rate extrapolation
            interpolated_failed += failed

        # Recalculate rate as sum of individual rates
        total_rate = math.fsum(map(RATE_PER_SECOND, progress_updates))

        # Use max elapsed
        max_elapsed = max(p.get("elapsed_seconds", 0.0) for p in progress_updates)

        return {
            "completed_count": interpolated_completed,
            "failed_count": interpolated_failed,
            "rate_per_second": total_rate,
            "elapsed_seconds": max_elapsed,
            "collected_at": reference_time,
            "interpolated": True,
        }
