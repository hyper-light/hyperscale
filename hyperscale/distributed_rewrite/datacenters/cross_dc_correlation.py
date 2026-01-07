"""
Cross-DC Correlation Detection for Eviction Decisions (Phase 7).

Detects when multiple datacenters are experiencing failures simultaneously,
which typically indicates a network partition or gateway issue rather than
actual datacenter failures. This prevents cascade evictions when the problem
is network connectivity rather than individual DC health.

Key scenarios:
1. Network partition between gate and DCs → multiple DCs appear unhealthy
2. Gateway failure → all DCs unreachable simultaneously
3. Cascading failures → genuine but correlated failures

When correlation is detected, the gate should:
- Delay eviction decisions
- Investigate connectivity (OOB probes, peer gates)
- Avoid marking DCs as permanently unhealthy

See tracker.py for within-DC correlation (workers within a manager).
"""

import time
from dataclasses import dataclass, field
from enum import Enum


class CorrelationSeverity(Enum):
    """Severity level for correlated failures."""

    NONE = "none"  # No correlation detected
    LOW = "low"  # Some correlation, may be coincidence
    MEDIUM = "medium"  # Likely correlated, investigate
    HIGH = "high"  # Strong correlation, likely network issue


@dataclass
class CorrelationDecision:
    """Result of correlation analysis."""

    severity: CorrelationSeverity
    reason: str
    affected_datacenters: list[str] = field(default_factory=list)
    recommendation: str = ""

    @property
    def should_delay_eviction(self) -> bool:
        """Check if eviction should be delayed due to correlation."""
        return self.severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH)


@dataclass
class CrossDCCorrelationConfig:
    """Configuration for cross-DC correlation detection."""

    # Time window for detecting simultaneous failures (seconds)
    correlation_window_seconds: float = 30.0

    # Minimum DCs failing within window to trigger LOW correlation
    low_threshold: int = 2

    # Minimum DCs failing within window to trigger MEDIUM correlation
    medium_threshold: int = 3

    # Minimum fraction of known DCs failing to trigger HIGH correlation
    high_threshold_fraction: float = 0.5

    # Backoff duration after correlation detected (seconds)
    correlation_backoff_seconds: float = 60.0

    # Maximum failures to track per DC before cleanup
    max_failures_per_dc: int = 100


@dataclass(slots=True)
class DCFailureRecord:
    """Record of a datacenter failure event."""

    datacenter_id: str
    timestamp: float
    failure_type: str  # "unhealthy", "timeout", "unreachable", etc.
    manager_count_affected: int = 0


class CrossDCCorrelationDetector:
    """
    Detects correlated failures across multiple datacenters.

    Used by gates to avoid cascade evictions when network issues cause
    multiple DCs to appear unhealthy simultaneously.

    Algorithm:
    1. Record failure events as they occur
    2. When evaluating eviction, check recent failures across all DCs
    3. If multiple DCs failed within the correlation window, flag correlation
    4. Severity based on count and fraction of affected DCs

    Example usage:
        detector = CrossDCCorrelationDetector()

        # Record failures as they occur
        detector.record_failure("dc-west", "unhealthy", manager_count=3)
        detector.record_failure("dc-east", "timeout", manager_count=2)

        # Check for correlation before eviction
        decision = detector.check_correlation("dc-west")
        if decision.should_delay_eviction:
            # Investigate rather than evict
            pass

        # After successful recovery
        detector.record_recovery("dc-west")
    """

    def __init__(self, config: CrossDCCorrelationConfig | None = None):
        """
        Initialize the correlation detector.

        Args:
            config: Configuration for correlation detection.
        """
        self._config = config or CrossDCCorrelationConfig()

        # Recent failures: dc_id -> list of failure timestamps
        self._failure_records: dict[str, list[DCFailureRecord]] = {}

        # Known datacenters for fraction calculation
        self._known_datacenters: set[str] = set()

        # Last correlation backoff timestamp
        self._last_correlation_time: float = 0.0

        # Statistics
        self._total_failures_recorded: int = 0
        self._correlation_events_detected: int = 0

    def add_datacenter(self, datacenter_id: str) -> None:
        """
        Register a datacenter for tracking.

        Args:
            datacenter_id: The datacenter ID to track.
        """
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._failure_records:
            self._failure_records[datacenter_id] = []

    def remove_datacenter(self, datacenter_id: str) -> None:
        """
        Remove a datacenter from tracking.

        Args:
            datacenter_id: The datacenter ID to remove.
        """
        self._known_datacenters.discard(datacenter_id)
        self._failure_records.pop(datacenter_id, None)

    def record_failure(
        self,
        datacenter_id: str,
        failure_type: str = "unhealthy",
        manager_count_affected: int = 0,
    ) -> None:
        """
        Record a datacenter failure event.

        Args:
            datacenter_id: The failing datacenter.
            failure_type: Type of failure (unhealthy, timeout, unreachable).
            manager_count_affected: Number of managers affected.
        """
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._failure_records:
            self._failure_records[datacenter_id] = []

        record = DCFailureRecord(
            datacenter_id=datacenter_id,
            timestamp=time.monotonic(),
            failure_type=failure_type,
            manager_count_affected=manager_count_affected,
        )

        self._failure_records[datacenter_id].append(record)
        self._total_failures_recorded += 1

        # Enforce max failures per DC
        if len(self._failure_records[datacenter_id]) > self._config.max_failures_per_dc:
            self._failure_records[datacenter_id] = self._failure_records[datacenter_id][
                -self._config.max_failures_per_dc :
            ]

    def record_recovery(self, datacenter_id: str) -> None:
        """
        Record that a datacenter has recovered.

        Clears failure history for the DC.

        Args:
            datacenter_id: The recovered datacenter.
        """
        self._failure_records[datacenter_id] = []

    def check_correlation(self, datacenter_id: str) -> CorrelationDecision:
        """
        Check if a datacenter's failures are correlated with other DCs.

        Should be called before making eviction decisions to detect
        network-wide issues.

        Args:
            datacenter_id: The datacenter being evaluated for eviction.

        Returns:
            CorrelationDecision with severity and recommendation.
        """
        now = time.monotonic()
        window_start = now - self._config.correlation_window_seconds

        # Check if we're still in backoff from previous correlation
        if (now - self._last_correlation_time) < self._config.correlation_backoff_seconds:
            if self._last_correlation_time > 0:
                return CorrelationDecision(
                    severity=CorrelationSeverity.MEDIUM,
                    reason="Within correlation backoff period",
                    affected_datacenters=self._get_recent_failing_dcs(window_start),
                    recommendation="Wait for backoff to expire before evicting",
                )

        # Count DCs with recent failures (within window)
        recent_failing_dcs = self._get_recent_failing_dcs(window_start)
        failure_count = len(recent_failing_dcs)

        # No correlation if only one DC failing
        if failure_count <= 1:
            return CorrelationDecision(
                severity=CorrelationSeverity.NONE,
                reason="No correlated failures detected",
                affected_datacenters=recent_failing_dcs,
                recommendation="Safe to proceed with eviction",
            )

        # Calculate fraction of known DCs failing
        known_dc_count = len(self._known_datacenters)
        if known_dc_count == 0:
            known_dc_count = 1  # Avoid division by zero

        failure_fraction = failure_count / known_dc_count

        # Determine severity based on thresholds
        severity: CorrelationSeverity
        reason: str
        recommendation: str

        if failure_fraction >= self._config.high_threshold_fraction:
            severity = CorrelationSeverity.HIGH
            reason = (
                f"{failure_count}/{known_dc_count} DCs ({failure_fraction:.0%}) "
                f"failing within {self._config.correlation_window_seconds}s window"
            )
            recommendation = (
                "High correlation detected - likely network issue. "
                "Investigate connectivity before evicting any DC."
            )
            self._last_correlation_time = now
            self._correlation_events_detected += 1

        elif failure_count >= self._config.medium_threshold:
            severity = CorrelationSeverity.MEDIUM
            reason = (
                f"{failure_count} DCs failing within "
                f"{self._config.correlation_window_seconds}s window"
            )
            recommendation = (
                "Medium correlation detected. "
                "Delay eviction and investigate cross-DC connectivity."
            )
            self._last_correlation_time = now
            self._correlation_events_detected += 1

        elif failure_count >= self._config.low_threshold:
            severity = CorrelationSeverity.LOW
            reason = (
                f"{failure_count} DCs failing within "
                f"{self._config.correlation_window_seconds}s window"
            )
            recommendation = (
                "Low correlation detected. "
                "Consider investigating before evicting, but may proceed cautiously."
            )

        else:
            severity = CorrelationSeverity.NONE
            reason = "Failure count below correlation thresholds"
            recommendation = "Safe to proceed with eviction"

        return CorrelationDecision(
            severity=severity,
            reason=reason,
            affected_datacenters=recent_failing_dcs,
            recommendation=recommendation,
        )

    def _get_recent_failing_dcs(self, since: float) -> list[str]:
        """
        Get list of DCs with failures since the given timestamp.

        Args:
            since: Timestamp (monotonic) to filter from.

        Returns:
            List of datacenter IDs with recent failures.
        """
        failing_dcs: list[str] = []
        for dc_id, records in self._failure_records.items():
            for record in records:
                if record.timestamp >= since:
                    failing_dcs.append(dc_id)
                    break  # Only count each DC once
        return failing_dcs

    def get_recent_failure_count(self, datacenter_id: str) -> int:
        """
        Get count of recent failures for a specific datacenter.

        Args:
            datacenter_id: The datacenter to check.

        Returns:
            Number of failures within the correlation window.
        """
        window_start = time.monotonic() - self._config.correlation_window_seconds
        records = self._failure_records.get(datacenter_id, [])
        return sum(1 for record in records if record.timestamp >= window_start)

    def cleanup_old_records(self) -> int:
        """
        Remove failure records older than the correlation window.

        Returns:
            Number of records removed.
        """
        window_start = time.monotonic() - self._config.correlation_window_seconds
        removed = 0

        for dc_id in list(self._failure_records.keys()):
            old_records = self._failure_records[dc_id]
            new_records = [r for r in old_records if r.timestamp >= window_start]
            removed += len(old_records) - len(new_records)
            self._failure_records[dc_id] = new_records

        return removed

    def clear_all(self) -> None:
        """Clear all failure records and reset state."""
        self._failure_records.clear()
        self._last_correlation_time = 0.0

    def get_stats(self) -> dict:
        """
        Get statistics about correlation detection.

        Returns:
            Dictionary with statistics.
        """
        window_start = time.monotonic() - self._config.correlation_window_seconds
        recent_failing = self._get_recent_failing_dcs(window_start)

        return {
            "known_datacenters": len(self._known_datacenters),
            "datacenters_with_failures": len(
                [dc for dc, records in self._failure_records.items() if records]
            ),
            "recent_failing_count": len(recent_failing),
            "recent_failing_dcs": recent_failing,
            "total_failures_recorded": self._total_failures_recorded,
            "correlation_events_detected": self._correlation_events_detected,
            "in_backoff": (
                time.monotonic() - self._last_correlation_time
            ) < self._config.correlation_backoff_seconds,
            "config": {
                "correlation_window_seconds": self._config.correlation_window_seconds,
                "low_threshold": self._config.low_threshold,
                "medium_threshold": self._config.medium_threshold,
                "high_threshold_fraction": self._config.high_threshold_fraction,
                "correlation_backoff_seconds": self._config.correlation_backoff_seconds,
            },
        }
