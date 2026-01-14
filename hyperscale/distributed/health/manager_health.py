"""
Manager Health State (AD-19).

Three-signal health model for managers, monitored by gates.

Signals:
1. Liveness: Is the manager process alive and responsive?
2. Readiness: Can the manager accept new jobs? (has quorum, accepting, has workers)
3. Progress: Is work being dispatched at expected rate?

Routing decisions and DC health integration:
- route: All signals healthy, send jobs
- drain: Not ready but alive, stop new jobs
- investigate: Progress issues, check manager
- evict: Dead or stuck, remove from pool

DC Health Classification:
- ALL managers NOT liveness → DC = UNHEALTHY
- MAJORITY managers NOT readiness → DC = DEGRADED
- ANY manager progress == "stuck" → DC = DEGRADED
"""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum

from hyperscale.distributed.health.worker_health import (
    ProgressState,
    RoutingDecision,
)


@dataclass(slots=True)
class ManagerHealthConfig:
    """Configuration for manager health thresholds."""

    # Liveness thresholds
    liveness_timeout_seconds: float = 30.0
    max_consecutive_liveness_failures: int = 3

    # Progress rate thresholds (as fraction of expected)
    normal_rate_threshold: float = 0.8  # >= 80% of expected = normal
    slow_rate_threshold: float = 0.3  # >= 30% of expected = slow
    # Below slow threshold = degraded
    # Zero dispatches with accepted jobs = stuck


@dataclass(slots=True)
class ManagerHealthState:
    """
    Unified health state combining all three signals for a manager.

    Monitored by the gate to make routing decisions and determine DC health.

    Example usage:
        state = ManagerHealthState(
            manager_id="manager-1",
            datacenter_id="dc-east"
        )

        # Update from heartbeat
        state.update_liveness(success=True)

        # Update from manager status
        state.update_readiness(
            has_quorum=True,
            accepting=True,
            worker_count=10
        )

        # Update from throughput metrics
        state.update_progress(
            jobs_accepted=5,
            workflows_dispatched=20,
            expected_throughput=25.0
        )

        # Get routing decision
        decision = state.get_routing_decision()
        if decision == RoutingDecision.ROUTE:
            # Send jobs to this manager
            pass
    """

    manager_id: str
    datacenter_id: str
    config: ManagerHealthConfig = field(default_factory=ManagerHealthConfig)
    _state_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    # Signal 1: Liveness
    last_liveness_response: float = field(default_factory=time.monotonic)
    consecutive_liveness_failures: int = 0

    # Signal 2: Readiness
    has_quorum: bool = False  # Can make authoritative decisions
    accepting_jobs: bool = True  # Self-reported
    active_worker_count: int = 0  # Workers available for dispatch

    # Signal 3: Progress
    jobs_accepted_last_interval: int = 0
    workflows_dispatched_last_interval: int = 0
    expected_throughput: float = 1.0  # Workflows per interval based on worker capacity

    @property
    def liveness(self) -> bool:
        """
        Is the manager process alive and responsive?

        Based on heartbeat/probe responses. A manager is considered live if:
        - Recent response within timeout window
        - Not too many consecutive failures
        """
        time_since_response = time.monotonic() - self.last_liveness_response
        return (
            time_since_response < self.config.liveness_timeout_seconds
            and self.consecutive_liveness_failures
            < self.config.max_consecutive_liveness_failures
        )

    @property
    def readiness(self) -> bool:
        """
        Can the manager accept new jobs?

        Based on quorum status, self-reported acceptance, and worker availability.
        A manager is ready if:
        - Has quorum (can make authoritative decisions)
        - Actively accepting jobs
        - Has workers available for dispatch
        """
        return self.has_quorum and self.accepting_jobs and self.active_worker_count > 0

    @property
    def progress_state(self) -> ProgressState:
        """
        Is work being dispatched at expected rate?

        Detects stuck or degraded managers even when liveness appears healthy.
        """
        if self.jobs_accepted_last_interval == 0:
            return ProgressState.IDLE

        # Calculate actual rate compared to expected throughput
        actual_rate = self.workflows_dispatched_last_interval

        if actual_rate >= self.expected_throughput * self.config.normal_rate_threshold:
            return ProgressState.NORMAL
        elif actual_rate >= self.expected_throughput * self.config.slow_rate_threshold:
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

    def _apply_liveness_update(self, success: bool) -> None:
        if success:
            self.last_liveness_response = time.monotonic()
            self.consecutive_liveness_failures = 0
        else:
            self.consecutive_liveness_failures += 1

    def update_liveness(self, success: bool) -> None:
        """
        Update liveness signal from probe/heartbeat result.

        Args:
            success: Whether the probe succeeded
        """
        self._apply_liveness_update(success)

    async def update_liveness_async(self, success: bool) -> None:
        async with self._state_lock:
            self._apply_liveness_update(success)

    def update_readiness(
        self,
        has_quorum: bool,
        accepting: bool,
        worker_count: int,
    ) -> None:
        """
        Update readiness signal from manager status.

        Args:
            has_quorum: Whether manager has quorum for decisions
            accepting: Whether manager is accepting new jobs
            worker_count: Number of active workers available
        """
        self.has_quorum = has_quorum
        self.accepting_jobs = accepting
        self.active_worker_count = worker_count

    def update_progress(
        self,
        jobs_accepted: int,
        workflows_dispatched: int,
        expected_throughput: float | None = None,
    ) -> None:
        """
        Update progress signal from throughput metrics.

        Args:
            jobs_accepted: Number of jobs accepted in the last interval
            workflows_dispatched: Number of workflows dispatched in the last interval
            expected_throughput: Expected workflow throughput (per interval)
        """
        self.jobs_accepted_last_interval = jobs_accepted
        self.workflows_dispatched_last_interval = workflows_dispatched
        if expected_throughput is not None:
            self.expected_throughput = expected_throughput

    def get_diagnostics(self) -> dict:
        """
        Get diagnostic information for debugging/monitoring.

        Returns dict with all health signals and computed states.
        """
        return {
            "manager_id": self.manager_id,
            "datacenter_id": self.datacenter_id,
            "liveness": self.liveness,
            "readiness": self.readiness,
            "progress_state": self.progress_state.value,
            "routing_decision": self.get_routing_decision().value,
            "last_liveness_response": self.last_liveness_response,
            "consecutive_liveness_failures": self.consecutive_liveness_failures,
            "has_quorum": self.has_quorum,
            "accepting_jobs": self.accepting_jobs,
            "active_worker_count": self.active_worker_count,
            "jobs_accepted_last_interval": self.jobs_accepted_last_interval,
            "workflows_dispatched_last_interval": self.workflows_dispatched_last_interval,
            "expected_throughput": self.expected_throughput,
        }
