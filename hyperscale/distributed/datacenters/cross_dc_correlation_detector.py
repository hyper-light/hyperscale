"""``CrossDCCorrelationDetector`` -- pickled under the namespace
``hyperscale.distributed.datacenters.cross_dc_correlation`` (see that module)."""

import sys
import time
from itertools import filterfalse
from typing import Callable

from .cross_dc_correlation_shared import _DEFAULT_CLOCK
from .correlation_decision import CorrelationDecision
from .correlation_severity import CorrelationSeverity
from .cross_dc_correlation_config import CrossDCCorrelationConfig
from .dc_failure_record import DCFailureRecord
from .dc_health_state import DCHealthState
from .dc_state_info import DCStateInfo
from .extension_record import ExtensionRecord
from .latency_sample import LatencySample


class CrossDCCorrelationDetector:
    """
    Detects correlated failures across multiple datacenters.

    Used by gates to avoid cascade evictions when network issues cause
    multiple DCs to appear unhealthy simultaneously.

    Key features:
    1. Per-DC state machine with hysteresis
    2. Failure confirmation (debouncing)
    3. Recovery confirmation (sustained health required)
    4. Flap detection for unstable DCs
    5. Weighted recency for failure importance

    Algorithm:
    1. Record failure/recovery events as they occur
    2. Apply debouncing - transient failures are filtered
    3. Track state transitions with hysteresis
    4. Detect flapping DCs and treat them specially
    5. When evaluating eviction, count confirmed failures
    6. Severity based on confirmed count and fraction

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

    def __init__(
        self,
        config: CrossDCCorrelationConfig | None = None,
        on_callback_error: Callable[[str, list[str], Exception], None] | None = None,
    ):
        """
        Initialize the correlation detector.

        Args:
            config: Configuration for correlation detection.
            on_callback_error: Called when partition callbacks fail.
                Receives (event_type, affected_dcs, exception).
        """
        self._config = config or CrossDCCorrelationConfig()

        self._failure_records: dict[str, list[DCFailureRecord]] = {}
        self._dc_states: dict[str, DCStateInfo] = {}
        self._extension_records: dict[str, list[ExtensionRecord]] = {}
        self._known_datacenters: set[str] = set()
        self._last_correlation_time: float = 0.0

        self._total_failures_recorded: int = 0
        self._correlation_events_detected: int = 0
        self._flap_events_detected: int = 0
        self._latency_correlation_events: int = 0
        self._extension_correlation_events: int = 0
        self._lhm_correlation_events: int = 0

        self._partition_healed_callbacks: list[Callable[[list[str], float], None]] = []
        self._partition_detected_callbacks: list[
            Callable[[list[str], float], None]
        ] = []
        self._on_callback_error = on_callback_error
        self._partition_healed_count: int = 0
        self._last_partition_healed_time: float = 0.0
        self._was_in_partition: bool = False

    def add_datacenter(self, datacenter_id: str) -> None:
        """
        Register a datacenter for tracking.

        Args:
            datacenter_id: The datacenter ID to track.
        """
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._failure_records:
            self._failure_records[datacenter_id] = []
        if datacenter_id not in self._dc_states:
            self._dc_states[datacenter_id] = DCStateInfo(
                datacenter_id=datacenter_id,
                state_entered_at=_DEFAULT_CLOCK.monotonic(),
            )

    def remove_datacenter(self, datacenter_id: str) -> None:
        """
        Remove a datacenter from tracking.

        Args:
            datacenter_id: The datacenter ID to remove.
        """
        self._known_datacenters.discard(datacenter_id)
        self._failure_records.pop(datacenter_id, None)
        self._dc_states.pop(datacenter_id, None)
        self._extension_records.pop(datacenter_id, None)

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
        now = _DEFAULT_CLOCK.monotonic()

        # Ensure DC is tracked
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._failure_records:
            self._failure_records[datacenter_id] = []
        self._ensure_dc_state(datacenter_id, now)

        # Record the failure
        record = DCFailureRecord(
            datacenter_id=datacenter_id,
            timestamp=now,
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

        # Update state machine
        state = self._dc_states[datacenter_id]
        state.last_failure_at = now
        state.consecutive_failures += 1
        state.consecutive_recoveries = 0

        # Count failures in flap detection window
        window_start = now - self._config.flap_detection_window_seconds
        state.failure_count_in_window = self._count_records_since(
            self._failure_records[datacenter_id], window_start
        )

        # State transitions
        self._apply_failure_transition(state, now)

    def _ensure_dc_state(self, datacenter_id: str, now: float) -> None:
        """Create the DC's state-machine entry, entered at ``now``, if it is not tracked yet."""
        if datacenter_id not in self._dc_states:
            self._dc_states[datacenter_id] = DCStateInfo(
                datacenter_id=datacenter_id,
                state_entered_at=now,
            )

    @staticmethod
    def _count_records_since(records: list[DCFailureRecord], window_start: float) -> int:
        """Count the failure records stamped at or after ``window_start``."""
        return sum(1 for r in records if r.timestamp >= window_start)

    def _apply_failure_transition(self, state: DCStateInfo, now: float) -> None:
        """Advance the DC state machine on a failure (hysteresis; FLAPPING stays put)."""
        if state.current_state == DCHealthState.HEALTHY:
            state.current_state = DCHealthState.FAILING
            state.state_entered_at = now
            return
        if state.current_state == DCHealthState.RECOVERING:
            self._fail_while_recovering(state, now)
            return
        # Already flapping: stay in that state (the check below skips FLAPPING).
        self._confirm_failure(state, now)

    def _fail_while_recovering(self, state: DCStateInfo, now: float) -> None:
        """A recovering DC failed again: enter FLAPPING when it oscillates, else FAILING."""
        # Was recovering but failed again - check for flapping
        if state.is_flapping(
            self._config.flap_threshold,
            self._config.flap_detection_window_seconds,
        ):
            state.current_state = DCHealthState.FLAPPING
            state.state_entered_at = now
            self._flap_events_detected += 1
        else:
            state.current_state = DCHealthState.FAILING
            state.state_entered_at = now

    def _confirm_failure(self, state: DCStateInfo, now: float) -> None:
        """Already failing/failed: check whether the failure is confirmed (debouncing)."""
        if state.current_state in (
            DCHealthState.FAILING,
            DCHealthState.FAILED,
        ) and state.is_confirmed_failed(self._config.failure_confirmation_seconds):
            self._promote_to_failed(state, now)

    @staticmethod
    def _promote_to_failed(state: DCStateInfo, now: float) -> None:
        """Upgrade a confirmed failure to FAILED unless it already is."""
        if state.current_state != DCHealthState.FAILED:
            state.current_state = DCHealthState.FAILED
            state.state_entered_at = now

    def record_recovery(self, datacenter_id: str) -> None:
        """
        Record that a datacenter is showing signs of recovery.

        Does NOT immediately clear failure history. Recovery must be
        sustained for recovery_confirmation_seconds before DC is
        considered healthy again.

        Args:
            datacenter_id: The recovering datacenter.
        """
        now = _DEFAULT_CLOCK.monotonic()

        if datacenter_id not in self._dc_states:
            return

        state = self._dc_states[datacenter_id]
        state.last_recovery_at = now
        state.consecutive_recoveries += 1
        state.consecutive_failures = 0

        # Count recoveries in flap detection window
        state.recovery_count_in_window += 1

        # State transitions
        self._apply_recovery_transition(datacenter_id, state, now)

    def _apply_recovery_transition(self, datacenter_id: str, state: DCStateInfo, now: float) -> None:
        """Advance the DC state machine on a recovery signal (already HEALTHY: nothing to do)."""
        if state.current_state == DCHealthState.FLAPPING:
            self._exit_flapping_after_cooldown(state, now)
            return
        if state.current_state in (DCHealthState.FAILING, DCHealthState.FAILED):
            # Start recovery process
            state.current_state = DCHealthState.RECOVERING
            state.state_entered_at = now
            return
        self._confirm_recovery(datacenter_id, state, now)

    def _exit_flapping_after_cooldown(self, state: DCStateInfo, now: float) -> None:
        """Leave FLAPPING for RECOVERING only once the flap cooldown has elapsed."""
        # Need cooldown period before exiting flapping
        if (now - state.state_entered_at) >= self._config.flap_cooldown_seconds:
            state.current_state = DCHealthState.RECOVERING
            state.state_entered_at = now
        # Otherwise stay in FLAPPING

    def _confirm_recovery(self, datacenter_id: str, state: DCStateInfo, now: float) -> None:
        """A RECOVERING DC whose recovery is sustained becomes HEALTHY and drops its history."""
        # Check if recovery is confirmed
        if state.current_state == DCHealthState.RECOVERING and state.is_confirmed_recovered(
            self._config.recovery_confirmation_seconds
        ):
            state.current_state = DCHealthState.HEALTHY
            state.state_entered_at = now
            # Clear failure records on confirmed recovery
            self._failure_records[datacenter_id] = []
            state.failure_count_in_window = 0
            state.recovery_count_in_window = 0

    def record_latency(
        self,
        datacenter_id: str,
        latency_ms: float,
        probe_type: str = "health",
    ) -> None:
        """
        Record a latency measurement for a datacenter.

        High latency across multiple DCs indicates network degradation rather
        than individual DC failure. This signal is used to distinguish network
        partitions from actual DC failures.

        Args:
            datacenter_id: The datacenter being probed.
            latency_ms: Measured latency in milliseconds.
            probe_type: Type of probe ("health", "oob", "ping").
        """
        if not self._config.enable_latency_correlation:
            return

        now = _DEFAULT_CLOCK.monotonic()

        # Ensure DC is tracked
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._dc_states:
            self._dc_states[datacenter_id] = DCStateInfo(
                datacenter_id=datacenter_id,
                state_entered_at=now,
            )

        state = self._dc_states[datacenter_id]

        # Add sample
        sample = LatencySample(
            timestamp=now, latency_ms=latency_ms, probe_type=probe_type
        )
        state.latency_samples.append(sample)

        # Trim old samples outside the window
        window_start = now - self._config.latency_sample_window_seconds
        state.latency_samples = [
            s for s in state.latency_samples if s.timestamp >= window_start
        ]

        # Update computed metrics
        if len(state.latency_samples) >= self._config.min_latency_samples:
            latencies = [s.latency_ms for s in state.latency_samples]
            state.avg_latency_ms = sum(latencies) / len(latencies)
            state.max_latency_ms = max(latencies)
            state.latency_elevated = (
                state.avg_latency_ms >= self._config.latency_elevated_threshold_ms
            )
        else:
            # Not enough samples yet
            state.avg_latency_ms = latency_ms
            state.max_latency_ms = latency_ms
            state.latency_elevated = False

    def record_extension(
        self,
        datacenter_id: str,
        worker_id: str,
        extension_count: int,
        reason: str = "",
    ) -> None:
        """
        Record an extension request from a worker in a datacenter.

        When workers request extensions (more time to complete work), it often
        indicates load rather than failure. If multiple DCs have high extension
        activity, this suggests a load spike rather than health issues.

        Args:
            datacenter_id: The datacenter of the worker.
            worker_id: The worker requesting the extension.
            extension_count: Total extensions this worker has requested.
            reason: Reason for the extension request.
        """
        if not self._config.enable_extension_correlation:
            return

        now = _DEFAULT_CLOCK.monotonic()

        # Ensure DC is tracked
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._extension_records:
            self._extension_records[datacenter_id] = []
        if datacenter_id not in self._dc_states:
            self._dc_states[datacenter_id] = DCStateInfo(
                datacenter_id=datacenter_id,
                state_entered_at=now,
            )

        # Add record
        record = ExtensionRecord(
            timestamp=now,
            worker_id=worker_id,
            extension_count=extension_count,
            reason=reason,
        )
        self._extension_records[datacenter_id].append(record)

        # Trim old records
        window_start = now - self._config.extension_window_seconds
        self._extension_records[datacenter_id] = [
            r
            for r in self._extension_records[datacenter_id]
            if r.timestamp >= window_start
        ]

        # Count unique workers with extensions in this DC
        unique_workers = set(
            r.worker_id for r in self._extension_records[datacenter_id]
        )
        state = self._dc_states[datacenter_id]
        state.active_extensions = len(unique_workers)

    def record_lhm_score(
        self,
        datacenter_id: str,
        lhm_score: int,
        node_type: str = "manager",
    ) -> None:
        """
        Record a Local Health Multiplier (LHM) score for a datacenter
        from a particular node tier.

        High LHM scores indicate nodes are experiencing resource pressure
        (event loop lag, missed probes, etc.). If multiple DCs report high
        LHM across multiple tiers, the pattern is systemic load rather
        than isolated failures and eviction should be deferred (AD-33
        cross-DC correlation rationale).

        AD-19 addendum (Phase D): all three tiers (manager / worker /
        gate) report ``lhm_score`` uniformly via heartbeat. This
        method tracks each tier's most-recent score per DC and
        retains a DC-level summary as the **max** across tiers so
        existing ``check_correlation`` code paths see "the
        most-stressed tier in this DC."

        Args:
            datacenter_id: The datacenter reporting.
            lhm_score: Current LHM score (0-8, higher = more stressed).
            node_type: Reporting tier — one of ``"manager"``,
                ``"worker"``, ``"gate"``. Defaults to ``"manager"``
                for back-compat with callers that pre-date Phase D.
        """
        if not self._config.enable_lhm_correlation:
            return

        now = _DEFAULT_CLOCK.monotonic()

        # Ensure DC is tracked
        self._known_datacenters.add(datacenter_id)
        if datacenter_id not in self._dc_states:
            self._dc_states[datacenter_id] = DCStateInfo(
                datacenter_id=datacenter_id,
                state_entered_at=now,
            )

        state = self._dc_states[datacenter_id]
        state.per_tier_lhm_scores[node_type] = lhm_score
        # DC-level summary is the max across tiers — any tier saturating
        # raises the DC-wide signal. Preserves the historic
        # ``current_lhm_score`` semantics for downstream consumers
        # (correlation thresholding, metrics) without forcing them to
        # become per-tier-aware.
        state.current_lhm_score = max(state.per_tier_lhm_scores.values())
        state.lhm_stressed = (
            state.current_lhm_score >= self._config.lhm_stressed_threshold
        )

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
        now = _DEFAULT_CLOCK.monotonic()
        window_start = now - self._config.correlation_window_seconds

        # Check if we're still in backoff from previous correlation
        if (
            now - self._last_correlation_time
        ) < self._config.correlation_backoff_seconds and self._last_correlation_time > 0:
            return CorrelationDecision(
                severity=CorrelationSeverity.MEDIUM,
                reason="Within correlation backoff period",
                affected_datacenters=self._get_confirmed_failing_dcs(),
                recommendation="Wait for backoff to expire before evicting",
                flapping_datacenters=self._get_flapping_dcs(),
            )

        return self._evaluate_correlation(now, window_start)

    def _evaluate_correlation(self, now: float, window_start: float) -> CorrelationDecision:
        """Weigh confirmed, flapping and recent failures into a correlation decision (outside backoff)."""
        # Count DCs with CONFIRMED failures (not just transient)
        confirmed_failing_dcs = self._get_confirmed_failing_dcs()
        flapping_dcs = self._get_flapping_dcs()
        recent_failing_dcs = self._get_recent_failing_dcs(window_start)

        # For correlation, we count confirmed failures + flapping
        # Flapping DCs are treated as failing for correlation purposes
        effective_failure_count = len(confirmed_failing_dcs) + len(flapping_dcs)

        # But also consider recent unconfirmed failures if they're clustered
        # This helps detect rapidly developing situations
        unconfirmed_recent = list(
            filterfalse(
                set(confirmed_failing_dcs).union(flapping_dcs).__contains__,
                recent_failing_dcs,
            )
        )

        # If we have many unconfirmed failures clustered together,
        # weight them partially (they might be a developing partition)
        weighted_unconfirmed = len(unconfirmed_recent) * 0.5
        total_weighted_failures = effective_failure_count + weighted_unconfirmed

        # No correlation if count is too low
        if total_weighted_failures < self._config.low_threshold:
            return CorrelationDecision(
                severity=CorrelationSeverity.NONE,
                reason="No correlated failures detected",
                affected_datacenters=recent_failing_dcs,
                recommendation="Safe to proceed with eviction",
                flapping_datacenters=flapping_dcs,
            )

        # Calculate fraction of known DCs failing
        # (at least one known DC, to avoid division by zero)
        known_dc_count = max(len(self._known_datacenters), 1)

        failure_fraction = effective_failure_count / known_dc_count

        # Determine severity based on thresholds
        severity, reason, recommendation = self._classify_severity(
            now,
            effective_failure_count,
            known_dc_count,
            failure_fraction,
            total_weighted_failures,
            len(unconfirmed_recent),
            flapping_dcs,
        )

        # Compute secondary correlation signals
        latency_metrics = self._compute_latency_correlation()
        extension_metrics = self._compute_extension_correlation()
        lhm_metrics = self._compute_lhm_correlation()

        # Track correlation events for statistics
        self._latency_correlation_events += latency_metrics["correlated"]
        self._extension_correlation_events += extension_metrics["correlated"]
        self._lhm_correlation_events += lhm_metrics["correlated"]

        # Enhance recommendation if secondary signals suggest network issue
        recommendation = self._latency_recommendation(
            recommendation, severity, latency_metrics["correlated"]
        )
        recommendation = self._load_recommendation(
            recommendation, extension_metrics["correlated"], lhm_metrics["correlated"]
        )

        affected = confirmed_failing_dcs + flapping_dcs
        if severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH):
            self.mark_partition_detected(affected)

        return CorrelationDecision(
            severity=severity,
            reason=reason,
            affected_datacenters=affected,
            recommendation=recommendation,
            flapping_datacenters=flapping_dcs,
            latency_correlated=latency_metrics["correlated"],
            extension_correlated=extension_metrics["correlated"],
            lhm_correlated=lhm_metrics["correlated"],
            avg_latency_ms=latency_metrics["avg_latency_ms"],
            dcs_with_elevated_latency=latency_metrics["dcs_elevated"],
            dcs_with_extensions=extension_metrics["dcs_with_extensions"],
            dcs_with_elevated_lhm=lhm_metrics["dcs_stressed"],
        )

    def _classify_severity(
        self,
        now: float,
        effective_failure_count: int,
        known_dc_count: int,
        failure_fraction: float,
        total_weighted_failures: float,
        unconfirmed_count: int,
        flapping_dcs: list[str],
    ) -> tuple[CorrelationSeverity, str, str]:
        """Map failure counts to (severity, reason, recommendation); HIGH needs fraction AND count."""
        # HIGH: Both fraction AND high count threshold must be met
        is_high_fraction = failure_fraction >= self._config.high_threshold_fraction
        is_high_count = effective_failure_count >= self._config.high_count_threshold

        if is_high_fraction and is_high_count:
            reason = (
                f"{effective_failure_count}/{known_dc_count} DCs ({failure_fraction:.0%}) "
                f"confirmed failing within {self._config.correlation_window_seconds}s window"
            ) + self._flapping_suffix(flapping_dcs)
            recommendation = (
                "High correlation detected - likely network issue. "
                "Investigate connectivity before evicting any DC."
            )
            self._last_correlation_time = now
            self._correlation_events_detected += 1
            return CorrelationSeverity.HIGH, reason, recommendation

        return self._classify_below_high_severity(
            now,
            effective_failure_count,
            total_weighted_failures,
            unconfirmed_count,
            flapping_dcs,
        )

    def _classify_below_high_severity(
        self,
        now: float,
        effective_failure_count: int,
        total_weighted_failures: float,
        unconfirmed_count: int,
        flapping_dcs: list[str],
    ) -> tuple[CorrelationSeverity, str, str]:
        """Classify MEDIUM / LOW / NONE once HIGH correlation has been ruled out."""
        if effective_failure_count >= self._config.medium_threshold:
            reason = (
                f"{effective_failure_count} DCs confirmed failing within "
                f"{self._config.correlation_window_seconds}s window"
            ) + self._flapping_suffix(flapping_dcs)
            recommendation = (
                "Medium correlation detected. "
                "Delay eviction and investigate cross-DC connectivity."
            )
            self._last_correlation_time = now
            self._correlation_events_detected += 1
            return CorrelationSeverity.MEDIUM, reason, recommendation

        if total_weighted_failures >= self._config.low_threshold:
            reason = (
                f"{effective_failure_count} confirmed + {unconfirmed_count} unconfirmed "
                f"DCs failing within {self._config.correlation_window_seconds}s window"
            )
            recommendation = (
                "Low correlation detected. "
                "Consider investigating before evicting, but may proceed cautiously."
            )
            return CorrelationSeverity.LOW, reason, recommendation

        return (
            CorrelationSeverity.NONE,
            "Failure count below correlation thresholds",
            "Safe to proceed with eviction",
        )

    @staticmethod
    def _flapping_suffix(flapping_dcs: list[str]) -> str:
        """The reason suffix naming how many DCs are flapping, or "" when none are."""
        return f" ({len(flapping_dcs)} flapping)" if flapping_dcs else ""

    @staticmethod
    def _latency_recommendation(
        recommendation: str,
        severity: CorrelationSeverity,
        latency_correlated: bool,
    ) -> str:
        """Latency elevated across DCs with no failure correlation points at network degradation."""
        if latency_correlated and severity == CorrelationSeverity.NONE:
            return (
                "Latency elevated across DCs suggests network degradation. "
                "Consider investigating before evicting."
            )
        return recommendation

    @staticmethod
    def _load_recommendation(
        recommendation: str,
        extension_correlated: bool,
        lhm_correlated: bool,
    ) -> str:
        """High extensions AND high LHM across DCs indicate load, not failure (AD-33)."""
        if extension_correlated and lhm_correlated:
            return (
                "High extensions and LHM across DCs indicates load, not failure. "
                "Delay eviction until load subsides."
            )
        return recommendation

    def _get_confirmed_failing_dcs(self) -> list[str]:
        """
        Get list of DCs with confirmed (sustained) failures.

        Returns:
            List of datacenter IDs with confirmed failures.
        """
        return [
            dc_id
            for dc_id, state in self._dc_states.items()
            if self._is_confirmed_failing(state)
        ]

    def _is_confirmed_failing(self, state: DCStateInfo) -> bool:
        """FAILED counts outright; FAILING counts once the failure is confirmed (debounced)."""
        return state.current_state == DCHealthState.FAILED or (
            state.current_state == DCHealthState.FAILING
            and state.is_confirmed_failed(self._config.failure_confirmation_seconds)
        )

    def _get_flapping_dcs(self) -> list[str]:
        """
        Get list of DCs that are flapping.

        Returns:
            List of datacenter IDs that are flapping.
        """
        return [
            dc_id
            for dc_id, state in self._dc_states.items()
            if state.current_state == DCHealthState.FLAPPING
        ]

    def _get_recent_failing_dcs(self, since: float) -> list[str]:
        """
        Get list of DCs with any failures since the given timestamp.

        Args:
            since: Timestamp (monotonic) to filter from.

        Returns:
            List of datacenter IDs with recent failures.
        """
        # Only count each DC once
        return [
            dc_id
            for dc_id, records in self._failure_records.items()
            if self._has_record_since(records, since)
        ]

    @staticmethod
    def _has_record_since(records: list[DCFailureRecord], since: float) -> bool:
        """Whether any failure record is stamped at or after ``since``."""
        return any(record.timestamp >= since for record in records)

    def _secondary_signal_unavailable(self, enabled: bool) -> bool:
        """A secondary correlation signal is skipped when disabled or no DC is known."""
        return not enabled or len(self._known_datacenters) == 0

    def _compute_latency_correlation(self) -> dict:
        """
        Compute latency correlation across DCs.

        Returns:
            Dict with correlated flag and metrics.
        """
        if self._secondary_signal_unavailable(self._config.enable_latency_correlation):
            return {"correlated": False, "avg_latency_ms": 0.0, "dcs_elevated": 0}

        known_dc_count = len(self._known_datacenters)

        # Count DCs with elevated latency
        dcs_with_elevated_latency = sum(
            state.latency_elevated for state in self._dc_states.values()
        )
        avg_latency = self._mean_sampled_latency()
        fraction_elevated = dcs_with_elevated_latency / known_dc_count

        correlated = fraction_elevated >= self._config.latency_correlation_fraction

        return {
            "correlated": correlated,
            "avg_latency_ms": avg_latency,
            "dcs_elevated": dcs_with_elevated_latency,
        }

    def _mean_sampled_latency(self) -> float:
        """Mean of the per-DC average latencies over DCs that have samples (0.0 when none do)."""
        sampled_latencies = self._sampled_average_latencies()
        return (
            sum(sampled_latencies) / len(sampled_latencies) if sampled_latencies else 0.0
        )

    def _sampled_average_latencies(self) -> list[float]:
        """Per-DC average latencies of the DCs that have recorded latency samples."""
        return [
            state.avg_latency_ms
            for state in self._dc_states.values()
            if state.avg_latency_ms > 0
        ]

    def _compute_extension_correlation(self) -> dict:
        """
        Compute extension request correlation across DCs.

        Returns:
            Dict with correlated flag and metrics.
        """
        if self._secondary_signal_unavailable(self._config.enable_extension_correlation):
            return {"correlated": False, "dcs_with_extensions": 0}

        known_dc_count = len(self._known_datacenters)

        # Count DCs with significant extension activity
        dcs_with_extensions = sum(
            state.active_extensions >= self._config.extension_count_threshold
            for state in self._dc_states.values()
        )

        fraction_with_extensions = dcs_with_extensions / known_dc_count
        correlated = (
            fraction_with_extensions >= self._config.extension_correlation_fraction
        )

        return {
            "correlated": correlated,
            "dcs_with_extensions": dcs_with_extensions,
        }

    def _compute_lhm_correlation(self) -> dict:
        """
        Compute LHM (Local Health Multiplier) correlation across DCs.

        Returns:
            Dict with correlated flag and metrics.
        """
        if self._secondary_signal_unavailable(self._config.enable_lhm_correlation):
            return {"correlated": False, "dcs_stressed": 0}

        known_dc_count = len(self._known_datacenters)

        # Count DCs with elevated LHM
        dcs_stressed = sum(state.lhm_stressed for state in self._dc_states.values())

        fraction_stressed = dcs_stressed / known_dc_count
        correlated = fraction_stressed >= self._config.lhm_correlation_fraction

        return {
            "correlated": correlated,
            "dcs_stressed": dcs_stressed,
        }

    def get_dc_state(self, datacenter_id: str) -> DCHealthState | None:
        """
        Get the current state of a specific datacenter.

        Args:
            datacenter_id: The datacenter to check.

        Returns:
            Current DCHealthState or None if not tracked.
        """
        state = self._dc_states.get(datacenter_id)
        return state.current_state if state else None

    def get_recent_failure_count(self, datacenter_id: str) -> int:
        """
        Get count of recent failures for a specific datacenter.

        Args:
            datacenter_id: The datacenter to check.

        Returns:
            Number of failures within the correlation window.
        """
        window_start = _DEFAULT_CLOCK.monotonic() - self._config.correlation_window_seconds
        records = self._failure_records.get(datacenter_id, [])
        return sum(1 for record in records if record.timestamp >= window_start)

    def cleanup_old_records(self) -> int:
        """
        Remove failure records older than the correlation window.

        Returns:
            Number of records removed.
        """
        window_start = _DEFAULT_CLOCK.monotonic() - self._config.correlation_window_seconds
        removed = 0

        for dc_id in list(self._failure_records.keys()):
            old_records = self._failure_records[dc_id]
            new_records = self._records_since(old_records, window_start)
            removed += len(old_records) - len(new_records)
            self._failure_records[dc_id] = new_records

        return removed

    @staticmethod
    def _records_since(records: list[DCFailureRecord], window_start: float) -> list[DCFailureRecord]:
        """The failure records stamped at or after ``window_start``, in order."""
        return [r for r in records if r.timestamp >= window_start]

    def clear_all(self) -> None:
        """Clear all failure records and reset state."""
        self._failure_records.clear()
        self._dc_states.clear()
        self._last_correlation_time = 0.0

    def get_stats(self) -> dict:
        """
        Get statistics about correlation detection.

        Returns:
            Dictionary with statistics.
        """
        window_start = _DEFAULT_CLOCK.monotonic() - self._config.correlation_window_seconds
        recent_failing = self._get_recent_failing_dcs(window_start)
        confirmed_failing = self._get_confirmed_failing_dcs()
        flapping = self._get_flapping_dcs()

        # Count DCs by state
        state_counts: dict[str, int] = {}
        for state in self._dc_states.values():
            state_name = state.current_state.value
            state_counts[state_name] = state_counts.get(state_name, 0) + 1

        # Get secondary correlation metrics
        latency_metrics = self._compute_latency_correlation()
        extension_metrics = self._compute_extension_correlation()
        lhm_metrics = self._compute_lhm_correlation()

        return {
            "known_datacenters": len(self._known_datacenters),
            "datacenters_with_failures": len(
                list(filter(None, self._failure_records.values()))
            ),
            "recent_failing_count": len(recent_failing),
            "confirmed_failing_count": len(confirmed_failing),
            "flapping_count": len(flapping),
            "recent_failing_dcs": recent_failing,
            "confirmed_failing_dcs": confirmed_failing,
            "flapping_dcs": flapping,
            "total_failures_recorded": self._total_failures_recorded,
            "correlation_events_detected": self._correlation_events_detected,
            "flap_events_detected": self._flap_events_detected,
            "latency_correlation_events": self._latency_correlation_events,
            "extension_correlation_events": self._extension_correlation_events,
            "lhm_correlation_events": self._lhm_correlation_events,
            "state_counts": state_counts,
            "in_backoff": (_DEFAULT_CLOCK.monotonic() - self._last_correlation_time)
            < self._config.correlation_backoff_seconds,
            # Secondary correlation current state
            "latency_correlated": latency_metrics["correlated"],
            "avg_latency_ms": latency_metrics["avg_latency_ms"],
            "dcs_with_elevated_latency": latency_metrics["dcs_elevated"],
            "extension_correlated": extension_metrics["correlated"],
            "dcs_with_extensions": extension_metrics["dcs_with_extensions"],
            "lhm_correlated": lhm_metrics["correlated"],
            "dcs_with_elevated_lhm": lhm_metrics["dcs_stressed"],
            "config": {
                "correlation_window_seconds": self._config.correlation_window_seconds,
                "low_threshold": self._config.low_threshold,
                "medium_threshold": self._config.medium_threshold,
                "high_count_threshold": self._config.high_count_threshold,
                "high_threshold_fraction": self._config.high_threshold_fraction,
                "correlation_backoff_seconds": self._config.correlation_backoff_seconds,
                "failure_confirmation_seconds": self._config.failure_confirmation_seconds,
                "recovery_confirmation_seconds": self._config.recovery_confirmation_seconds,
                "flap_threshold": self._config.flap_threshold,
                "flap_detection_window_seconds": self._config.flap_detection_window_seconds,
                "flap_cooldown_seconds": self._config.flap_cooldown_seconds,
                "enable_latency_correlation": self._config.enable_latency_correlation,
                "latency_elevated_threshold_ms": self._config.latency_elevated_threshold_ms,
                "enable_extension_correlation": self._config.enable_extension_correlation,
                "extension_count_threshold": self._config.extension_count_threshold,
                "enable_lhm_correlation": self._config.enable_lhm_correlation,
                "lhm_stressed_threshold": self._config.lhm_stressed_threshold,
            },
            "partition_healed_count": self._partition_healed_count,
            "last_partition_healed_time": self._last_partition_healed_time,
            "was_in_partition": self._was_in_partition,
        }

    def register_partition_healed_callback(
        self,
        callback: Callable[[list[str], float], None],
    ) -> None:
        """Register a callback to be invoked when a partition heals."""
        self._partition_healed_callbacks.append(callback)

    def register_partition_detected_callback(
        self,
        callback: Callable[[list[str], float], None],
    ) -> None:
        """Register a callback to be invoked when a partition is detected."""
        self._partition_detected_callbacks.append(callback)

    def check_partition_healed(self) -> bool:
        """
        Check if a previously detected partition has healed.

        Returns True if:
        1. We were previously in a partition state (MEDIUM or HIGH correlation)
        2. All DCs have recovered to HEALTHY state
        3. No correlation is currently detected

        Returns:
            True if partition has healed, False otherwise
        """
        if not self._was_in_partition or self._partition_still_active():
            return False

        now = _DEFAULT_CLOCK.monotonic()
        self._was_in_partition = False
        self._last_partition_healed_time = now
        self._partition_healed_count += 1

        healed_datacenters = list(self._known_datacenters)
        self._notify_partition_callbacks(
            self._partition_healed_callbacks,
            "partition_healed",
            healed_datacenters,
            now,
        )

        return True

    def _partition_still_active(self) -> bool:
        """Whether any DC is still confirmed failing or flapping, unhealthy, or correlated."""
        confirmed_failing = self._get_confirmed_failing_dcs()
        flapping = self._get_flapping_dcs()

        if confirmed_failing or flapping:
            return True

        return self._unhealthy_or_correlated()

    def _unhealthy_or_correlated(self) -> bool:
        """Whether some DC is not HEALTHY or MEDIUM/HIGH correlation is still detected."""
        if not all(
            state.current_state == DCHealthState.HEALTHY
            for state in self._dc_states.values()
        ):
            return True

        decision = self.check_correlation("")
        return decision.severity in (CorrelationSeverity.MEDIUM, CorrelationSeverity.HIGH)

    def _notify_partition_callbacks(
        self,
        callbacks: list[Callable[[list[str], float], None]],
        event_type: str,
        datacenters: list[str],
        now: float,
    ) -> None:
        """Invoke each partition callback, routing a callback failure to the error handler."""
        for callback in callbacks:
            try:
                callback(datacenters, now)
            except Exception as callback_error:
                self._report_callback_error(event_type, datacenters, callback_error)

    def _report_callback_error(
        self,
        event_type: str,
        datacenters: list[str],
        callback_error: Exception,
    ) -> None:
        """Hand a partition callback failure to on_callback_error; a failing handler goes to stderr."""
        if self._on_callback_error:
            try:
                self._on_callback_error(event_type, datacenters, callback_error)
            except Exception as handler_error:
                print(
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] "
                    f"CRITICAL: {event_type} callback error handler failed: {handler_error}, "
                    f"original_error={callback_error}, "
                    f"datacenters={datacenters}",
                    file=sys.stderr,
                )

    def mark_partition_detected(self, affected_datacenters: list[str]) -> None:
        """
        Mark that a partition has been detected.

        Called when check_correlation returns MEDIUM or HIGH severity.
        This enables partition healed detection.

        Args:
            affected_datacenters: List of datacenter IDs affected by the partition
        """
        was_already_partitioned = self._was_in_partition
        self._was_in_partition = True

        if not was_already_partitioned:
            now = _DEFAULT_CLOCK.monotonic()
            self._notify_partition_callbacks(
                self._partition_detected_callbacks,
                "partition_detected",
                affected_datacenters,
                now,
            )

    def is_in_partition(self) -> bool:
        """Check if we are currently in a partition state."""
        return self._was_in_partition

    def get_time_since_partition_healed(self) -> float | None:
        """
        Get time since the last partition healed.

        Returns:
            Seconds since last partition healed, or None if never healed
        """
        if self._last_partition_healed_time == 0.0:
            return None
        return _DEFAULT_CLOCK.monotonic() - self._last_partition_healed_time
