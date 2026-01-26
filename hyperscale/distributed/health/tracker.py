"""
Generic Health Tracking Infrastructure (AD-19).

Provides reusable infrastructure for tracking health across any node type:
- HealthSignals: Protocol defining the three-signal interface
- NodeHealthTracker: Generic tracker with routing decisions and eviction logic
- HealthPiggyback: Data structure for SWIM message embedding
"""

import time
from dataclasses import dataclass, field
from typing import Generic, Protocol, TypeVar, Callable

from hyperscale.distributed.health.worker_health import (
    ProgressState,
    RoutingDecision,
)


class HealthSignals(Protocol):
    """
    Protocol defining the three-signal health interface.

    Any health state class (WorkerHealthState, ManagerHealthState, GateHealthState)
    should implement this protocol.
    """

    @property
    def liveness(self) -> bool:
        """Is the node alive and responsive?"""
        ...

    @property
    def readiness(self) -> bool:
        """Can the node accept work?"""
        ...

    @property
    def progress_state(self) -> ProgressState:
        """Is the node making progress?"""
        ...

    def get_routing_decision(self) -> RoutingDecision:
        """Get routing decision based on combined signals."""
        ...


# Type variable for health state implementations
T = TypeVar("T", bound=HealthSignals)


@dataclass(slots=True)
class EvictionDecision:
    """Result of an eviction decision check."""

    should_evict: bool
    reason: str
    correlated_failures: bool = False  # True if multiple nodes failing simultaneously


@dataclass(slots=True)
class NodeHealthTrackerConfig:
    """Configuration for NodeHealthTracker."""

    # Correlation detection
    correlation_window_seconds: float = 60.0  # Time window for correlation detection
    correlation_threshold: int = 3  # Min simultaneous failures to trigger correlation

    # Eviction backoff
    eviction_backoff_seconds: float = 30.0  # Wait time before re-evicting same node


class NodeHealthTracker(Generic[T]):
    """
    Generic health tracker for any node type.

    Provides unified tracking, routing decisions, and eviction logic
    with correlation detection to prevent cascade evictions.

    Example usage:
        tracker = NodeHealthTracker[WorkerHealthState]()

        # Update state
        tracker.update_state("worker-1", worker_health_state)

        # Get routing decision
        decision = tracker.get_routing_decision("worker-1")

        # Get list of healthy nodes
        healthy = tracker.get_healthy_nodes()

        # Check if we should evict (with correlation detection)
        evict_decision = tracker.should_evict("worker-1")
        if evict_decision.should_evict:
            if evict_decision.correlated_failures:
                # Investigate network issue, don't evict
                pass
            else:
                # Safe to evict
                pass
    """

    def __init__(self, config: NodeHealthTrackerConfig | None = None):
        self._config = config or NodeHealthTrackerConfig()
        self._states: dict[str, T] = {}
        self._eviction_timestamps: dict[str, float] = {}  # node_id -> last eviction time
        self._failure_timestamps: dict[str, float] = {}  # node_id -> time when first marked for eviction

    def update_state(self, node_id: str, state: T) -> None:
        """
        Update health state for a node.

        Args:
            node_id: Node identifier
            state: Health state implementing HealthSignals
        """
        self._states[node_id] = state

        # Track when node first enters evictable state
        decision = state.get_routing_decision()
        if decision == RoutingDecision.EVICT:
            if node_id not in self._failure_timestamps:
                self._failure_timestamps[node_id] = time.monotonic()
        else:
            # Node recovered, clear failure tracking
            self._failure_timestamps.pop(node_id, None)

    def remove_state(self, node_id: str) -> bool:
        """
        Remove health state for a node.

        Returns True if node was tracked, False otherwise.
        """
        state = self._states.pop(node_id, None)
        self._failure_timestamps.pop(node_id, None)
        self._eviction_timestamps.pop(node_id, None)
        return state is not None

    def get_state(self, node_id: str) -> T | None:
        """Get health state for a node."""
        return self._states.get(node_id)

    def get_routing_decision(self, node_id: str) -> RoutingDecision | None:
        """
        Get routing decision for a node.

        Returns None if node is not tracked.
        """
        state = self._states.get(node_id)
        if state:
            return state.get_routing_decision()
        return None

    def get_healthy_nodes(self) -> list[str]:
        """
        Get list of nodes that can receive work.

        Returns node IDs where routing decision is ROUTE.
        """
        return [
            node_id
            for node_id, state in self._states.items()
            if state.get_routing_decision() == RoutingDecision.ROUTE
        ]

    def get_nodes_to_investigate(self) -> list[str]:
        """
        Get list of nodes that need investigation.

        Returns node IDs where routing decision is INVESTIGATE.
        """
        return [
            node_id
            for node_id, state in self._states.items()
            if state.get_routing_decision() == RoutingDecision.INVESTIGATE
        ]

    def get_nodes_to_drain(self) -> list[str]:
        """
        Get list of nodes that should be drained.

        Returns node IDs where routing decision is DRAIN.
        """
        return [
            node_id
            for node_id, state in self._states.items()
            if state.get_routing_decision() == RoutingDecision.DRAIN
        ]

    def get_nodes_to_evict(self) -> list[str]:
        """
        Get list of nodes that should be evicted.

        Returns node IDs where routing decision is EVICT.
        Does not check for correlation - use should_evict() for that.
        """
        return [
            node_id
            for node_id, state in self._states.items()
            if state.get_routing_decision() == RoutingDecision.EVICT
        ]

    def should_evict(self, node_id: str) -> EvictionDecision:
        """
        Check if a node should be evicted, with correlation detection.

        Correlation detection prevents cascade evictions when multiple
        nodes fail simultaneously (likely a network issue, not node issue).

        Also implements eviction backoff to prevent repeated eviction
        of the same node.

        Args:
            node_id: Node to check

        Returns:
            EvictionDecision with should_evict, reason, and correlated_failures
        """
        state = self._states.get(node_id)
        if not state:
            return EvictionDecision(
                should_evict=False,
                reason="Node not tracked",
            )

        decision = state.get_routing_decision()
        if decision != RoutingDecision.EVICT:
            return EvictionDecision(
                should_evict=False,
                reason=f"Routing decision is {decision.value}, not evict",
            )

        # Check eviction backoff
        now = time.monotonic()
        last_eviction = self._eviction_timestamps.get(node_id)
        if last_eviction and (now - last_eviction) < self._config.eviction_backoff_seconds:
            return EvictionDecision(
                should_evict=False,
                reason="Eviction backoff in effect",
            )

        # Check for correlated failures
        correlated = self._check_correlation(node_id)
        if correlated:
            return EvictionDecision(
                should_evict=False,
                reason="Correlated failures detected (possible network issue)",
                correlated_failures=True,
            )

        return EvictionDecision(
            should_evict=True,
            reason="Node health indicates eviction",
        )

    def _check_correlation(self, node_id: str) -> bool:
        """
        Check if node failure is correlated with other failures.

        Returns True if multiple nodes entered evictable state
        within the correlation window.
        """
        now = time.monotonic()
        window_start = now - self._config.correlation_window_seconds

        # Count nodes that entered evictable state within the window
        recent_failures = sum(
            1 for timestamp in self._failure_timestamps.values()
            if timestamp >= window_start
        )

        return recent_failures >= self._config.correlation_threshold

    def mark_evicted(self, node_id: str) -> None:
        """
        Mark a node as evicted.

        Records eviction timestamp for backoff tracking.
        """
        self._eviction_timestamps[node_id] = time.monotonic()

    def get_diagnostics(self) -> dict:
        """
        Get diagnostic information about all tracked nodes.
        """
        now = time.monotonic()
        nodes: dict[str, dict] = {}

        for node_id, state in self._states.items():
            nodes[node_id] = {
                "liveness": state.liveness,
                "readiness": state.readiness,
                "progress_state": state.progress_state.value,
                "routing_decision": state.get_routing_decision().value,
                "failure_timestamp": self._failure_timestamps.get(node_id),
                "last_eviction": self._eviction_timestamps.get(node_id),
            }

        return {
            "node_count": len(self._states),
            "healthy_count": len(self.get_healthy_nodes()),
            "evictable_count": len(self.get_nodes_to_evict()),
            "recent_failures": sum(
                1 for ts in self._failure_timestamps.values()
                if ts >= now - self._config.correlation_window_seconds
            ),
            "nodes": nodes,
        }


@dataclass(slots=True)
class HealthPiggyback:
    """
    Health information for SWIM message embedding.

    This data structure is designed to be embedded in SWIM protocol
    messages to propagate health information alongside membership updates.
    """

    node_id: str
    node_type: str  # "worker", "manager", "gate"

    # Liveness signal
    is_alive: bool = True

    # Readiness signals
    accepting_work: bool = True
    capacity: int = 0  # Available capacity (cores, slots, etc.)

    # Progress signals
    throughput: float = 0.0  # Actual throughput
    expected_throughput: float = 0.0  # Expected throughput

    # Overload state (from HybridOverloadDetector)
    overload_state: str = "healthy"

    # Timestamp for staleness detection
    timestamp: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict:
        """Serialize to dictionary for embedding."""
        return {
            "node_id": self.node_id,
            "node_type": self.node_type,
            "is_alive": self.is_alive,
            "accepting_work": self.accepting_work,
            "capacity": self.capacity,
            "throughput": self.throughput,
            "expected_throughput": self.expected_throughput,
            "overload_state": self.overload_state,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "HealthPiggyback":
        """Deserialize from dictionary."""
        return cls(
            node_id=data["node_id"],
            node_type=data["node_type"],
            is_alive=data.get("is_alive", True),
            accepting_work=data.get("accepting_work", True),
            capacity=data.get("capacity", 0),
            throughput=data.get("throughput", 0.0),
            expected_throughput=data.get("expected_throughput", 0.0),
            overload_state=data.get("overload_state", "healthy"),
            timestamp=data.get("timestamp", time.monotonic()),
        )

    def is_stale(self, max_age_seconds: float = 60.0) -> bool:
        """Check if this piggyback data is stale."""
        return (time.monotonic() - self.timestamp) > max_age_seconds
