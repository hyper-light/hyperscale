"""
Tests for the health state carried on SWIM messages (AD-19):
``HealthPiggyback`` serialization.
"""

import time

from hyperscale.distributed.health import (
    GateHealthState,
    HealthPiggyback,
    ManagerHealthState,
    ProgressState,
    RoutingDecision,
    WorkerHealthState,
)


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

