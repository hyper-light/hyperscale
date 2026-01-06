"""
Gate Health State (AD-19).

Three-signal health model for gates, monitored by peer gates.

Signals:
1. Liveness: Is the gate process alive and responsive?
2. Readiness: Can the gate forward jobs? (has DC connectivity, not overloaded)
3. Progress: Is job forwarding happening at expected rate?

Routing decisions and leader election integration:
- route: All signals healthy, forward jobs
- drain: Not ready but alive, stop forwarding
- investigate: Progress issues, check gate
- evict: Dead or stuck, remove from peer list

Leader Election:
- Unhealthy gates should not participate in leader election
- Gates with overload_state == "overloaded" should yield leadership
"""

import time
from dataclasses import dataclass, field
from enum import Enum

from hyperscale.distributed_rewrite.health.worker_health import (
    ProgressState,
    RoutingDecision,
)


@dataclass
class GateHealthConfig:
    """Configuration for gate health thresholds."""

    # Liveness thresholds
    liveness_timeout_seconds: float = 30.0
    max_consecutive_liveness_failures: int = 3

    # Progress rate thresholds (as fraction of expected)
    normal_rate_threshold: float = 0.8  # >= 80% of expected = normal
    slow_rate_threshold: float = 0.3  # >= 30% of expected = slow
    # Below slow threshold = degraded
    # Zero forwards with jobs = stuck

    # Overload states that indicate not ready
    overload_not_ready_states: tuple[str, ...] = ("stressed", "overloaded")


@dataclass
class GateHealthState:
    """
    Unified health state combining all three signals for a gate.

    Monitored by peer gates to make forwarding decisions and determine
    leader election eligibility.

    Example usage:
        state = GateHealthState(gate_id="gate-1")

        # Update from heartbeat
        state.update_liveness(success=True)

        # Update from gate status
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy"
        )

        # Update from forwarding metrics
        state.update_progress(
            jobs_forwarded=50,
            stats_aggregated=100,
            expected_forward_rate=60.0
        )

        # Get routing decision
        decision = state.get_routing_decision()
        if decision == RoutingDecision.ROUTE:
            # Forward jobs to this gate
            pass

        # Check leader election eligibility
        if state.should_participate_in_election():
            # Gate can be considered for leadership
            pass
    """

    gate_id: str
    config: GateHealthConfig = field(default_factory=GateHealthConfig)

    # Signal 1: Liveness
    last_liveness_response: float = field(default_factory=time.monotonic)
    consecutive_liveness_failures: int = 0

    # Signal 2: Readiness
    has_dc_connectivity: bool = False  # Can reach at least one DC
    connected_dc_count: int = 0
    overload_state: str = "healthy"  # From HybridOverloadDetector

    # Signal 3: Progress
    jobs_forwarded_last_interval: int = 0
    stats_aggregated_last_interval: int = 0
    expected_forward_rate: float = 1.0  # Jobs per interval

    @property
    def liveness(self) -> bool:
        """
        Is the gate process alive and responsive?

        Based on heartbeat/probe responses. A gate is considered live if:
        - Recent response within timeout window
        - Not too many consecutive failures
        """
        time_since_response = time.monotonic() - self.last_liveness_response
        return (
            time_since_response < self.config.liveness_timeout_seconds
            and self.consecutive_liveness_failures < self.config.max_consecutive_liveness_failures
        )

    @property
    def readiness(self) -> bool:
        """
        Can the gate forward jobs?

        Based on DC connectivity and overload state. A gate is ready if:
        - Has connectivity to at least one DC
        - Not in stressed or overloaded state
        """
        return (
            self.has_dc_connectivity
            and self.connected_dc_count > 0
            and self.overload_state not in self.config.overload_not_ready_states
        )

    @property
    def progress_state(self) -> ProgressState:
        """
        Is job forwarding happening at expected rate?

        Detects stuck or degraded gates even when liveness appears healthy.
        """
        if self.jobs_forwarded_last_interval == 0:
            return ProgressState.IDLE

        # Calculate actual rate compared to expected
        actual_rate = self.jobs_forwarded_last_interval

        if actual_rate >= self.expected_forward_rate * self.config.normal_rate_threshold:
            return ProgressState.NORMAL
        elif actual_rate >= self.expected_forward_rate * self.config.slow_rate_threshold:
            return ProgressState.SLOW
        elif actual_rate > 0:
            return ProgressState.DEGRADED
        else:
            return ProgressState.STUCK

    def get_routing_decision(self) -> RoutingDecision:
        """
        Determine action based on combined health signals.

        Decision matrix:
        - EVICT: Not live OR stuck (regardless of other signals)
        - DRAIN: Live but not ready (stop forwarding new jobs)
        - INVESTIGATE: Live and ready but degraded progress
        - ROUTE: All signals healthy
        """
        if not self.liveness:
            return RoutingDecision.EVICT

        progress = self.progress_state
        if progress == ProgressState.STUCK:
            return RoutingDecision.EVICT

        if not self.readiness:
            return RoutingDecision.DRAIN

        if progress == ProgressState.DEGRADED:
            return RoutingDecision.INVESTIGATE

        return RoutingDecision.ROUTE

    def should_participate_in_election(self) -> bool:
        """
        Determine if this gate should participate in leader election.

        A gate should not lead if:
        - Not live (can't respond to requests)
        - Not ready (can't forward jobs)
        - Overloaded (should shed load, not take on leadership)
        - Progress is stuck (something is wrong)
        """
        if not self.liveness:
            return False

        if not self.readiness:
            return False

        if self.overload_state == "overloaded":
            return False

        if self.progress_state == ProgressState.STUCK:
            return False

        return True

    def update_liveness(self, success: bool) -> None:
        """
        Update liveness signal from probe/heartbeat result.

        Args:
            success: Whether the probe succeeded
        """
        if success:
            self.last_liveness_response = time.monotonic()
            self.consecutive_liveness_failures = 0
        else:
            self.consecutive_liveness_failures += 1

    def update_readiness(
        self,
        has_dc_connectivity: bool,
        connected_dc_count: int,
        overload_state: str,
    ) -> None:
        """
        Update readiness signal from gate status.

        Args:
            has_dc_connectivity: Whether gate can reach at least one DC
            connected_dc_count: Number of DCs gate is connected to
            overload_state: Current overload state from detector
        """
        self.has_dc_connectivity = has_dc_connectivity
        self.connected_dc_count = connected_dc_count
        self.overload_state = overload_state

    def update_progress(
        self,
        jobs_forwarded: int,
        stats_aggregated: int,
        expected_forward_rate: float | None = None,
    ) -> None:
        """
        Update progress signal from forwarding metrics.

        Args:
            jobs_forwarded: Number of jobs forwarded in the last interval
            stats_aggregated: Number of stats updates aggregated in the last interval
            expected_forward_rate: Expected job forward rate (per interval)
        """
        self.jobs_forwarded_last_interval = jobs_forwarded
        self.stats_aggregated_last_interval = stats_aggregated
        if expected_forward_rate is not None:
            self.expected_forward_rate = expected_forward_rate

    def get_diagnostics(self) -> dict:
        """
        Get diagnostic information for debugging/monitoring.

        Returns dict with all health signals and computed states.
        """
        return {
            "gate_id": self.gate_id,
            "liveness": self.liveness,
            "readiness": self.readiness,
            "progress_state": self.progress_state.value,
            "routing_decision": self.get_routing_decision().value,
            "should_participate_in_election": self.should_participate_in_election(),
            "last_liveness_response": self.last_liveness_response,
            "consecutive_liveness_failures": self.consecutive_liveness_failures,
            "has_dc_connectivity": self.has_dc_connectivity,
            "connected_dc_count": self.connected_dc_count,
            "overload_state": self.overload_state,
            "jobs_forwarded_last_interval": self.jobs_forwarded_last_interval,
            "stats_aggregated_last_interval": self.stats_aggregated_last_interval,
            "expected_forward_rate": self.expected_forward_rate,
        }
