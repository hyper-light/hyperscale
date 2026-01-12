from __future__ import annotations

import time
from typing import Generic, Protocol, TypeVar

from hyperscale.distributed.health.worker_health import ProgressState, RoutingDecision


class HealthSignals(Protocol):
    """Three-signal health interface for routing decisions."""

    @property
    def liveness(self) -> bool: ...

    @property
    def readiness(self) -> bool: ...

    @property
    def progress_state(self) -> ProgressState: ...

    def get_routing_decision(self) -> RoutingDecision: ...


T = TypeVar("T", bound=HealthSignals)


class NodeHealthTracker(Generic[T]):
    """Generic health tracker with correlation-aware eviction checks."""

    def __init__(
        self,
        correlation_window_seconds: float = 60.0,
        correlation_threshold: int = 3,
        eviction_backoff_seconds: float = 30.0,
    ) -> None:
        self._correlation_window_seconds = correlation_window_seconds
        self._correlation_threshold = correlation_threshold
        self._eviction_backoff_seconds = eviction_backoff_seconds
        self._states: dict[str, T] = {}
        self._eviction_timestamps: dict[str, float] = {}
        self._failure_timestamps: dict[str, float] = {}

    def update_state(self, node_id: str, state: T) -> None:
        """Update health state for a node."""
        self._states[node_id] = state
        if state.get_routing_decision() == RoutingDecision.EVICT:
            self._failure_timestamps.setdefault(node_id, time.monotonic())
        else:
            self._failure_timestamps.pop(node_id, None)

    def get_routing_decision(self, node_id: str) -> RoutingDecision | None:
        """Return the routing decision for a node, if tracked."""
        state = self._states.get(node_id)
        if state is None:
            return None
        return state.get_routing_decision()

    def should_evict(self, node_id: str) -> tuple[bool, str, bool]:
        """Return (should_evict, reason, correlated_failures)."""
        state = self._states.get(node_id)
        if state is None:
            return False, "Node not tracked", False
        if state.get_routing_decision() != RoutingDecision.EVICT:
            return (
                False,
                f"Routing decision is {state.get_routing_decision().value}, not evict",
                False,
            )
        return self._evaluate_eviction(node_id)

    def mark_evicted(self, node_id: str) -> None:
        """Record eviction timestamp for backoff tracking."""
        self._eviction_timestamps[node_id] = time.monotonic()

    def _evaluate_eviction(self, node_id: str) -> tuple[bool, str, bool]:
        if self._is_backoff_active(node_id):
            return False, "Eviction backoff in effect", False
        if self._has_correlated_failures():
            return False, "Correlated failures detected (possible network issue)", True
        return True, "Node health indicates eviction", False

    def _is_backoff_active(self, node_id: str) -> bool:
        last_eviction = self._eviction_timestamps.get(node_id)
        if last_eviction is None:
            return False
        return (time.monotonic() - last_eviction) < self._eviction_backoff_seconds

    def _has_correlated_failures(self) -> bool:
        window_start = time.monotonic() - self._correlation_window_seconds
        recent_failures = sum(
            1
            for timestamp in self._failure_timestamps.values()
            if timestamp >= window_start
        )
        return recent_failures >= self._correlation_threshold
