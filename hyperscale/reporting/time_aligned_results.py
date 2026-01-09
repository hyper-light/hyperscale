"""
Time-Aligned Results Aggregation.

This module provides time-aware aggregation for WorkflowStats across multiple
workers and datacenters. Unlike the basic Results.merge_results(), this class
accounts for collection time differences to provide more accurate rate metrics.

Time Alignment Strategy:
- Each WorkflowStats or progress update includes a `collected_at` Unix timestamp
- When aggregating, we interpolate or align values to a common reference time
- Rate metrics are adjusted based on the time window they represent
- This prevents misleading aggregations when data arrives with network latency

Usage:
    from hyperscale.reporting.time_aligned_results import TimeAlignedResults

    aggregator = TimeAlignedResults()

    # Aggregate WorkflowStats with time awareness
    aligned_stats = aggregator.merge_with_time_alignment(
        workflow_stats_list,
        reference_time=time.time(),  # Align to this timestamp
    )
"""

import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import numpy as np

from hyperscale.reporting.common.results_types import (
    CheckSet,
    ContextCount,
    CountResults,
    MetricsSet,
    QuantileSet,
    ResultSet,
    StatsResults,
    WorkflowStats,
)
from hyperscale.reporting.results import Results


@dataclass
class TimestampedStats:
    """WorkflowStats with associated collection timestamp."""
    stats: WorkflowStats
    collected_at: float  # Unix timestamp when stats were collected
    source: str = ""     # Identifier for source (worker_id, datacenter, etc.)


@dataclass
class TimeAlignmentMetadata:
    """Metadata about the time alignment performed during aggregation."""
    reference_time: float       # The target alignment timestamp
    min_collected_at: float     # Earliest collection time
    max_collected_at: float     # Latest collection time
    time_spread_seconds: float  # Spread between earliest and latest
    sources_count: int          # Number of sources aggregated
    sources: list[str]          # Source identifiers


class TimeAlignedResults(Results):
    """
    Time-aware results aggregator that accounts for collection time differences.

    Extends the base Results class to provide time-aligned aggregation,
    which is important for accurate rate calculations when aggregating
    data from multiple workers or datacenters with network latency.

    Key improvements over basic merge_results():
    - Rate interpolation: Adjusts rates based on actual time windows
    - Time skew reporting: Reports the time spread across sources
    - Reference time alignment: Can align all stats to a specific timestamp
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

        Unlike the base merge_results(), this method:
        1. Tracks collection timestamps from each source
        2. Calculates time-adjusted rates
        3. Reports time skew metadata

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

        # Perform base merge
        merged = self.merge_results(workflow_stats_list)

        # Adjust rate metrics with time awareness
        merged = self._adjust_rate_for_time_alignment(
            merged,
            timestamped_stats,
            reference_time,
        )

        return merged, metadata

    def _adjust_rate_for_time_alignment(
        self,
        merged: WorkflowStats,
        timestamped_stats: List[TimestampedStats],
        reference_time: float,
    ) -> WorkflowStats:
        """
        Adjust rate metrics based on time alignment.

        For rate calculations (aps - actions per second), we need to account
        for the fact that different sources may have collected data at different
        times. Simply summing rates can be misleading.

        Strategy:
        - Calculate weighted average rate based on each source's contribution
        - Account for the time window each rate represents
        - Use the most recent elapsed time as the reference
        """
        if not timestamped_stats:
            return merged

        # Calculate time-weighted rate
        total_executed = 0
        weighted_elapsed_sum = 0.0
        weights_sum = 0.0

        for ts in timestamped_stats:
            stats = ts.stats
            executed = stats.get("stats", {}).get("executed", 0)
            elapsed = stats.get("elapsed", 0.0)

            if elapsed > 0:
                # Weight by recency - more recent data gets higher weight
                time_delta = reference_time - ts.collected_at
                # Decay weight for older data (half-life of 1 second)
                weight = np.exp(-time_delta / 1.0) if time_delta > 0 else 1.0

                total_executed += executed
                weighted_elapsed_sum += elapsed * weight
                weights_sum += weight

        # Calculate time-adjusted rate
        if weights_sum > 0 and weighted_elapsed_sum > 0:
            weighted_elapsed = weighted_elapsed_sum / weights_sum
            if weighted_elapsed > 0:
                merged["aps"] = total_executed / weighted_elapsed

        return merged

    def aggregate_progress_stats(
        self,
        progress_updates: List[Dict[str, Any]],
        reference_time: Optional[float] = None,
    ) -> Dict[str, Any]:
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

        # Calculate time-weighted rate
        weighted_rate = self._calculate_time_weighted_rate(
            progress_updates,
            reference_time,
        )

        # Use maximum elapsed as the reference (all sources started around same time)
        max_elapsed = max(p.get("elapsed_seconds", 0.0) for p in progress_updates)

        return {
            "completed_count": total_completed,
            "failed_count": total_failed,
            "rate_per_second": weighted_rate,
            "elapsed_seconds": max_elapsed,
            "collected_at": reference_time,
            "time_spread_seconds": max_collected - min_collected,
            "sources_count": len(progress_updates),
        }

    def _calculate_time_weighted_rate(
        self,
        progress_updates: List[Dict[str, Any]],
        reference_time: float,
    ) -> float:
        """
        Calculate time-weighted rate from multiple progress updates.

        More recent rates are weighted more heavily to account for
        network latency causing some updates to arrive later.

        Args:
            progress_updates: List of progress dicts with rate_per_second and collected_at
            reference_time: Reference time for weight calculation

        Returns:
            Time-weighted average rate
        """
        if not progress_updates:
            return 0.0

        weighted_sum = 0.0
        weights_sum = 0.0

        for progress in progress_updates:
            rate = progress.get("rate_per_second", 0.0)
            collected_at = progress.get("collected_at", reference_time)

            if rate >= 0:  # Include zero rates
                # Calculate time delta from reference
                time_delta = reference_time - collected_at

                # Apply exponential decay weight
                # Half-life of 2 seconds - recent data is more relevant
                if time_delta >= 0:
                    weight = np.exp(-time_delta / 2.0)
                else:
                    # Future timestamp (clock skew) - use full weight
                    weight = 1.0

                weighted_sum += rate * weight
                weights_sum += weight

        if weights_sum > 0:
            return weighted_sum / weights_sum

        return 0.0

    def interpolate_to_reference_time(
        self,
        progress_updates: List[Dict[str, Any]],
        reference_time: float,
    ) -> Dict[str, Any]:
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
        total_rate = sum(p.get("rate_per_second", 0.0) for p in progress_updates)

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
