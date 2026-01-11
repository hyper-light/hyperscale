"""
Integration tests for Generic Health Tracking Infrastructure (AD-19).

Tests:
- NodeHealthTracker with different health state types
- Eviction decisions with correlation detection
- Health piggyback serialization
- HealthSignals protocol compliance
"""

import time
from unittest.mock import patch

import pytest

from hyperscale.distributed_rewrite.health import (
    EvictionDecision,
    GateHealthState,
    HealthPiggyback,
    ManagerHealthState,
    NodeHealthTracker,
    NodeHealthTrackerConfig,
    ProgressState,
    RoutingDecision,
    WorkerHealthState,
)


class TestNodeHealthTrackerWithWorkers:
    """Test NodeHealthTracker with WorkerHealthState."""

    def test_update_and_get_state(self) -> None:
        """Test basic state update and retrieval."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        # expected_rate is the fraction of assigned work expected to complete
        # With 5 assigned and 4 completed, actual_rate = 4/5 = 0.8
        # For NORMAL status, actual_rate >= expected_rate * 0.8
        # So expected_rate=1.0 means: 0.8 >= 1.0 * 0.8 = 0.8 → True (NORMAL)
        state.update_progress(
            assigned=5,
            completed=4,
            expected_rate=1.0,
        )

        tracker.update_state("worker-1", state)

        retrieved = tracker.get_state("worker-1")
        assert retrieved is not None
        assert retrieved.worker_id == "worker-1"
        assert retrieved.liveness is True
        assert retrieved.readiness is True

    def test_get_routing_decision(self) -> None:
        """Test getting routing decision for tracked node."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        state = WorkerHealthState(worker_id="worker-1")
        state.update_liveness(success=True)
        state.update_readiness(accepting=True, capacity=10)
        # expected_rate is the fraction of assigned work expected to complete
        # With 5 assigned and 4 completed, actual_rate = 4/5 = 0.8
        # For NORMAL status, actual_rate >= expected_rate * 0.8
        # So expected_rate=1.0 means: 0.8 >= 1.0 * 0.8 = 0.8 → True (NORMAL)
        state.update_progress(
            assigned=5,
            completed=4,
            expected_rate=1.0,
        )

        tracker.update_state("worker-1", state)

        decision = tracker.get_routing_decision("worker-1")
        assert decision == RoutingDecision.ROUTE

    def test_get_routing_decision_unknown_node(self) -> None:
        """Test routing decision for unknown node returns None."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        decision = tracker.get_routing_decision("unknown-node")
        assert decision is None

    def test_get_healthy_nodes(self) -> None:
        """Test filtering healthy nodes."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Create healthy worker
        healthy = WorkerHealthState(worker_id="worker-healthy")
        healthy.update_liveness(success=True)
        healthy.update_readiness(accepting=True, capacity=10)
        # Use expected_rate=1.0 (fraction) so that 4/5=0.8 >= 1.0*0.8 = NORMAL
        healthy.update_progress(assigned=5, completed=4, expected_rate=1.0)
        tracker.update_state("worker-healthy", healthy)

        # Create unhealthy worker (not accepting work)
        unhealthy = WorkerHealthState(worker_id="worker-unhealthy")
        unhealthy.update_liveness(success=True)
        unhealthy.update_readiness(accepting=False, capacity=0)
        tracker.update_state("worker-unhealthy", unhealthy)

        healthy_nodes = tracker.get_healthy_nodes()
        assert "worker-healthy" in healthy_nodes
        assert "worker-unhealthy" not in healthy_nodes

    def test_get_nodes_to_evict(self) -> None:
        """Test filtering nodes that should be evicted."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Create healthy worker
        healthy = WorkerHealthState(worker_id="worker-healthy")
        healthy.update_liveness(success=True)
        healthy.update_readiness(accepting=True, capacity=10)
        tracker.update_state("worker-healthy", healthy)

        # Create dead worker (liveness timeout)
        dead = WorkerHealthState(worker_id="worker-dead")
        dead.last_liveness_response = time.monotonic() - 60.0  # 60 seconds ago
        dead.consecutive_liveness_failures = 5
        tracker.update_state("worker-dead", dead)

        evictable = tracker.get_nodes_to_evict()
        assert "worker-dead" in evictable
        assert "worker-healthy" not in evictable

    def test_remove_state(self) -> None:
        """Test removing node state."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        state = WorkerHealthState(worker_id="worker-1")
        tracker.update_state("worker-1", state)

        assert tracker.get_state("worker-1") is not None

        removed = tracker.remove_state("worker-1")
        assert removed is True
        assert tracker.get_state("worker-1") is None

        # Removing again returns False
        removed_again = tracker.remove_state("worker-1")
        assert removed_again is False


class TestEvictionWithCorrelation:
    """Test eviction decisions with correlation detection."""

    def test_single_failure_should_evict(self) -> None:
        """Test that single node failure allows eviction."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Create dead worker
        dead = WorkerHealthState(worker_id="worker-dead")
        dead.last_liveness_response = time.monotonic() - 60.0
        dead.consecutive_liveness_failures = 5
        tracker.update_state("worker-dead", dead)

        decision = tracker.should_evict("worker-dead")
        assert decision.should_evict is True
        assert decision.correlated_failures is False

    def test_correlated_failures_prevent_eviction(self) -> None:
        """Test that multiple simultaneous failures prevent eviction."""
        config = NodeHealthTrackerConfig(
            correlation_window_seconds=60.0,
            correlation_threshold=3,
        )
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker(config=config)

        # Create multiple dead workers that failed within the correlation window
        for i in range(4):
            dead = WorkerHealthState(worker_id=f"worker-{i}")
            dead.last_liveness_response = time.monotonic() - 60.0
            dead.consecutive_liveness_failures = 5
            tracker.update_state(f"worker-{i}", dead)

        # Should detect correlation and prevent eviction
        decision = tracker.should_evict("worker-0")
        assert decision.should_evict is False
        assert decision.correlated_failures is True
        assert "correlated" in decision.reason.lower()

    def test_eviction_backoff(self) -> None:
        """Test that eviction backoff prevents repeated eviction."""
        config = NodeHealthTrackerConfig(
            eviction_backoff_seconds=30.0,
        )
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker(config=config)

        # Create dead worker
        dead = WorkerHealthState(worker_id="worker-dead")
        dead.last_liveness_response = time.monotonic() - 60.0
        dead.consecutive_liveness_failures = 5
        tracker.update_state("worker-dead", dead)

        # First eviction should be allowed
        decision1 = tracker.should_evict("worker-dead")
        assert decision1.should_evict is True

        # Mark as evicted
        tracker.mark_evicted("worker-dead")

        # Update state again (simulating node coming back dead)
        tracker.update_state("worker-dead", dead)

        # Second eviction should be blocked by backoff
        decision2 = tracker.should_evict("worker-dead")
        assert decision2.should_evict is False
        assert "backoff" in decision2.reason.lower()

    def test_not_evict_healthy_node(self) -> None:
        """Test that healthy nodes are not evicted."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        healthy = WorkerHealthState(worker_id="worker-healthy")
        healthy.update_liveness(success=True)
        healthy.update_readiness(accepting=True, capacity=10)
        tracker.update_state("worker-healthy", healthy)

        decision = tracker.should_evict("worker-healthy")
        assert decision.should_evict is False
        assert "not evict" in decision.reason.lower() or "route" in decision.reason.lower()

    def test_not_evict_unknown_node(self) -> None:
        """Test that unknown nodes cannot be evicted."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        decision = tracker.should_evict("unknown-node")
        assert decision.should_evict is False
        assert "not tracked" in decision.reason.lower()


class TestNodeHealthTrackerWithManagers:
    """Test NodeHealthTracker with ManagerHealthState."""

    def test_manager_health_tracking(self) -> None:
        """Test tracking manager health states."""
        tracker: NodeHealthTracker[ManagerHealthState] = NodeHealthTracker()

        state = ManagerHealthState(manager_id="manager-1", datacenter_id="dc-east")
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=10)
        state.update_progress(
            jobs_accepted=5,
            workflows_dispatched=20,
            expected_throughput=25.0,
        )

        tracker.update_state("manager-1", state)

        decision = tracker.get_routing_decision("manager-1")
        assert decision == RoutingDecision.ROUTE

    def test_manager_drain_no_workers(self) -> None:
        """Test manager with no workers should drain."""
        tracker: NodeHealthTracker[ManagerHealthState] = NodeHealthTracker()

        state = ManagerHealthState(manager_id="manager-1", datacenter_id="dc-east")
        state.update_liveness(success=True)
        state.update_readiness(has_quorum=True, accepting=True, worker_count=0)

        tracker.update_state("manager-1", state)

        decision = tracker.get_routing_decision("manager-1")
        assert decision == RoutingDecision.DRAIN


class TestNodeHealthTrackerWithGates:
    """Test NodeHealthTracker with GateHealthState."""

    def test_gate_health_tracking(self) -> None:
        """Test tracking gate health states."""
        tracker: NodeHealthTracker[GateHealthState] = NodeHealthTracker()

        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=True,
            connected_dc_count=3,
            overload_state="healthy",
        )
        state.update_progress(
            jobs_forwarded=50,
            stats_aggregated=100,
            expected_forward_rate=60.0,
        )

        tracker.update_state("gate-1", state)

        decision = tracker.get_routing_decision("gate-1")
        assert decision == RoutingDecision.ROUTE

    def test_gate_drain_no_dc_connectivity(self) -> None:
        """Test gate without DC connectivity should drain."""
        tracker: NodeHealthTracker[GateHealthState] = NodeHealthTracker()

        state = GateHealthState(gate_id="gate-1")
        state.update_liveness(success=True)
        state.update_readiness(
            has_dc_connectivity=False,
            connected_dc_count=0,
            overload_state="healthy",
        )

        tracker.update_state("gate-1", state)

        decision = tracker.get_routing_decision("gate-1")
        assert decision == RoutingDecision.DRAIN


class TestHealthPiggyback:
    """Test HealthPiggyback serialization and deserialization."""

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        piggyback = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            is_alive=True,
            accepting_work=True,
            capacity=10,
            throughput=5.0,
            expected_throughput=6.0,
            overload_state="healthy",
        )

        data = piggyback.to_dict()

        assert data["node_id"] == "worker-1"
        assert data["node_type"] == "worker"
        assert data["is_alive"] is True
        assert data["accepting_work"] is True
        assert data["capacity"] == 10
        assert data["throughput"] == 5.0
        assert data["expected_throughput"] == 6.0
        assert data["overload_state"] == "healthy"
        assert "timestamp" in data

    def test_from_dict(self) -> None:
        """Test deserialization from dictionary."""
        data = {
            "node_id": "manager-1",
            "node_type": "manager",
            "is_alive": True,
            "accepting_work": False,
            "capacity": 0,
            "throughput": 10.0,
            "expected_throughput": 15.0,
            "overload_state": "stressed",
            "timestamp": 12345.0,
        }

        piggyback = HealthPiggyback.from_dict(data)

        assert piggyback.node_id == "manager-1"
        assert piggyback.node_type == "manager"
        assert piggyback.is_alive is True
        assert piggyback.accepting_work is False
        assert piggyback.capacity == 0
        assert piggyback.throughput == 10.0
        assert piggyback.expected_throughput == 15.0
        assert piggyback.overload_state == "stressed"
        assert piggyback.timestamp == 12345.0

    def test_roundtrip(self) -> None:
        """Test serialization roundtrip preserves data."""
        original = HealthPiggyback(
            node_id="gate-1",
            node_type="gate",
            is_alive=True,
            accepting_work=True,
            capacity=5,
            throughput=100.0,
            expected_throughput=120.0,
            overload_state="busy",
        )

        data = original.to_dict()
        restored = HealthPiggyback.from_dict(data)

        assert restored.node_id == original.node_id
        assert restored.node_type == original.node_type
        assert restored.is_alive == original.is_alive
        assert restored.accepting_work == original.accepting_work
        assert restored.capacity == original.capacity
        assert restored.throughput == original.throughput
        assert restored.expected_throughput == original.expected_throughput
        assert restored.overload_state == original.overload_state

    def test_is_stale(self) -> None:
        """Test staleness detection."""
        piggyback = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
        )

        # Fresh piggyback should not be stale
        assert piggyback.is_stale(max_age_seconds=60.0) is False

        # Old piggyback should be stale
        piggyback.timestamp = time.monotonic() - 120.0  # 2 minutes ago
        assert piggyback.is_stale(max_age_seconds=60.0) is True

    def test_from_dict_with_defaults(self) -> None:
        """Test deserialization with missing optional fields uses defaults."""
        minimal_data = {
            "node_id": "worker-1",
            "node_type": "worker",
        }

        piggyback = HealthPiggyback.from_dict(minimal_data)

        assert piggyback.node_id == "worker-1"
        assert piggyback.node_type == "worker"
        assert piggyback.is_alive is True  # default
        assert piggyback.accepting_work is True  # default
        assert piggyback.capacity == 0  # default
        assert piggyback.overload_state == "healthy"  # default


class TestDiagnostics:
    """Test diagnostic information retrieval."""

    def test_get_diagnostics(self) -> None:
        """Test getting diagnostic information."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Add healthy worker
        healthy = WorkerHealthState(worker_id="worker-healthy")
        healthy.update_liveness(success=True)
        healthy.update_readiness(accepting=True, capacity=10)
        tracker.update_state("worker-healthy", healthy)

        # Add dead worker
        dead = WorkerHealthState(worker_id="worker-dead")
        dead.last_liveness_response = time.monotonic() - 60.0
        dead.consecutive_liveness_failures = 5
        tracker.update_state("worker-dead", dead)

        diagnostics = tracker.get_diagnostics()

        assert diagnostics["node_count"] == 2
        assert diagnostics["healthy_count"] == 1
        assert diagnostics["evictable_count"] == 1
        assert "worker-healthy" in diagnostics["nodes"]
        assert "worker-dead" in diagnostics["nodes"]
        assert diagnostics["nodes"]["worker-healthy"]["routing_decision"] == "route"
        assert diagnostics["nodes"]["worker-dead"]["routing_decision"] == "evict"


class TestInvestigateAndDrain:
    """Test investigate and drain node filtering."""

    def test_get_nodes_to_investigate(self) -> None:
        """Test filtering nodes that need investigation."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Create degraded worker (live and ready but degraded progress)
        degraded = WorkerHealthState(worker_id="worker-degraded")
        degraded.update_liveness(success=True)
        degraded.update_readiness(accepting=True, capacity=10)
        degraded.workflows_assigned = 10
        degraded.completions_last_interval = 1  # Very low completion
        degraded.expected_completion_rate = 10.0
        tracker.update_state("worker-degraded", degraded)

        # Verify it's in investigate state
        assert degraded.progress_state == ProgressState.DEGRADED

        investigate = tracker.get_nodes_to_investigate()
        assert "worker-degraded" in investigate

    def test_get_nodes_to_drain(self) -> None:
        """Test filtering nodes that should be drained."""
        tracker: NodeHealthTracker[WorkerHealthState] = NodeHealthTracker()

        # Create worker not accepting work (should drain)
        draining = WorkerHealthState(worker_id="worker-draining")
        draining.update_liveness(success=True)
        draining.update_readiness(accepting=False, capacity=0)
        tracker.update_state("worker-draining", draining)

        drain = tracker.get_nodes_to_drain()
        assert "worker-draining" in drain
