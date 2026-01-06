"""
Worker Health State (AD-19).

Three-signal health model for workers, monitored by managers.

Signals:
1. Liveness: Is the worker process alive and responsive?
2. Readiness: Can the worker accept new work?
3. Progress: Is work completing at expected rate?

Routing decisions based on combined signals:
- route: All signals healthy, send work
- drain: Not ready but alive, stop new work
- investigate: Progress issues, check worker
- evict: Dead or stuck, remove from pool
"""

import time
from dataclasses import dataclass, field
from enum import Enum


class ProgressState(Enum):
    """Progress signal states."""

    IDLE = "idle"  # No work assigned
    NORMAL = "normal"  # Completing at expected rate
    SLOW = "slow"  # Below expected rate but making progress
    DEGRADED = "degraded"  # Significantly below expected rate
    STUCK = "stuck"  # No completions despite having work


class RoutingDecision(Enum):
    """Routing decisions based on health signals."""

    ROUTE = "route"  # Healthy, send work
    DRAIN = "drain"  # Stop new work, let existing complete
    INVESTIGATE = "investigate"  # Check worker, possible issues
    EVICT = "evict"  # Remove from pool


@dataclass
class WorkerHealthConfig:
    """Configuration for worker health thresholds."""

    # Liveness thresholds
    liveness_timeout_seconds: float = 30.0
    max_consecutive_liveness_failures: int = 3

    # Progress rate thresholds (as fraction of expected)
    normal_rate_threshold: float = 0.8  # >= 80% of expected = normal
    slow_rate_threshold: float = 0.3  # >= 30% of expected = slow
    # Below slow threshold = degraded
    # Zero completions with work = stuck


@dataclass
class WorkerHealthState:
    """
    Unified health state combining all three signals for a worker.

    Monitored by the manager to make routing decisions.

    Example usage:
        state = WorkerHealthState(worker_id="worker-1")

        # Update from heartbeat
        state.update_liveness(success=True)

        # Update from worker status
        state.update_readiness(accepting=True, capacity=5)

        # Update from completion metrics
        state.update_progress(assigned=10, completed=8, expected_rate=1.0)

        # Get routing decision
        decision = state.get_routing_decision()
        if decision == RoutingDecision.ROUTE:
            # Send work to this worker
            pass
    """

    worker_id: str
    config: WorkerHealthConfig = field(default_factory=WorkerHealthConfig)

    # Signal 1: Liveness
    last_liveness_response: float = field(default_factory=time.monotonic)
    consecutive_liveness_failures: int = 0

    # Signal 2: Readiness
    accepting_work: bool = True
    available_capacity: int = 0

    # Signal 3: Progress
    workflows_assigned: int = 0
    completions_last_interval: int = 0
    expected_completion_rate: float = 1.0  # Per interval

    @property
    def liveness(self) -> bool:
        """
        Is the worker process alive and responsive?

        Based on heartbeat/probe responses. A worker is considered live if:
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
        Can the worker accept new work?

        Based on worker's self-reported status. A worker is ready if:
        - Actively accepting work
        - Has available capacity
        """
        return self.accepting_work and self.available_capacity > 0

    @property
    def progress_state(self) -> ProgressState:
        """
        Is work completing at expected rate?

        Detects stuck or degraded workers even when liveness appears healthy.
        """
        if self.workflows_assigned == 0:
            return ProgressState.IDLE

        # Calculate actual rate as fraction of assigned work completed
        actual_rate = self.completions_last_interval / max(self.workflows_assigned, 1)

        if actual_rate >= self.expected_completion_rate * self.config.normal_rate_threshold:
            return ProgressState.NORMAL
        elif actual_rate >= self.expected_completion_rate * self.config.slow_rate_threshold:
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
        - DRAIN: Live but not ready (let existing work complete)
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

    def update_readiness(self, accepting: bool, capacity: int) -> None:
        """
        Update readiness signal from worker status.

        Args:
            accepting: Whether worker is accepting new work
            capacity: Available capacity for new workflows
        """
        self.accepting_work = accepting
        self.available_capacity = capacity

    def update_progress(
        self,
        assigned: int,
        completed: int,
        expected_rate: float | None = None,
    ) -> None:
        """
        Update progress signal from completion metrics.

        Args:
            assigned: Number of workflows currently assigned
            completed: Number of completions in the last interval
            expected_rate: Expected completion rate (per interval)
        """
        self.workflows_assigned = assigned
        self.completions_last_interval = completed
        if expected_rate is not None:
            self.expected_completion_rate = expected_rate

    def get_diagnostics(self) -> dict:
        """
        Get diagnostic information for debugging/monitoring.

        Returns dict with all health signals and computed states.
        """
        return {
            "worker_id": self.worker_id,
            "liveness": self.liveness,
            "readiness": self.readiness,
            "progress_state": self.progress_state.value,
            "routing_decision": self.get_routing_decision().value,
            "last_liveness_response": self.last_liveness_response,
            "consecutive_liveness_failures": self.consecutive_liveness_failures,
            "accepting_work": self.accepting_work,
            "available_capacity": self.available_capacity,
            "workflows_assigned": self.workflows_assigned,
            "completions_last_interval": self.completions_last_interval,
            "expected_completion_rate": self.expected_completion_rate,
        }
