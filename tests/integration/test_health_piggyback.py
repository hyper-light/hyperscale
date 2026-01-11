"""
Integration tests for Health Piggyback in SWIM Protocol Messages (AD-19).

Tests:
- Health piggyback fields in WorkerHeartbeat
- Health piggyback fields in ManagerHeartbeat
- Health piggyback fields in GateHeartbeat
- StateEmbedder health field population
- HealthPiggyback serialization roundtrip
"""

import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed_rewrite.health.tracker import HealthPiggyback
from hyperscale.distributed_rewrite.models import (
    GateHeartbeat,
    ManagerHeartbeat,
    WorkerHeartbeat,
)
from hyperscale.distributed_rewrite.swim.core.state_embedder import (
    GateStateEmbedder,
    ManagerStateEmbedder,
    WorkerStateEmbedder,
)


class TestWorkerHeartbeatHealthPiggyback:
    """Test health piggyback fields in WorkerHeartbeat."""

    def test_default_health_fields(self) -> None:
        """Test default values for health piggyback fields."""
        heartbeat = WorkerHeartbeat(
            node_id="worker-1",
            state="healthy",
            available_cores=4,
            queue_depth=0,
            cpu_percent=25.0,
            memory_percent=40.0,
            version=1,
        )

        assert heartbeat.health_accepting_work is True
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"

    def test_custom_health_fields(self) -> None:
        """Test custom values for health piggyback fields."""
        heartbeat = WorkerHeartbeat(
            node_id="worker-1",
            state="degraded",
            available_cores=2,
            queue_depth=10,
            cpu_percent=85.0,
            memory_percent=70.0,
            version=5,
            health_accepting_work=False,
            health_throughput=10.5,
            health_expected_throughput=15.0,
            health_overload_state="stressed",
        )

        assert heartbeat.health_accepting_work is False
        assert heartbeat.health_throughput == 10.5
        assert heartbeat.health_expected_throughput == 15.0
        assert heartbeat.health_overload_state == "stressed"

    def test_serialization_roundtrip(self) -> None:
        """Test that health fields survive serialization."""
        original = WorkerHeartbeat(
            node_id="worker-1",
            state="healthy",
            available_cores=4,
            queue_depth=0,
            cpu_percent=25.0,
            memory_percent=40.0,
            version=1,
            health_accepting_work=True,
            health_throughput=5.0,
            health_expected_throughput=8.0,
            health_overload_state="busy",
        )

        # Serialize and deserialize
        data = original.dump()
        restored = WorkerHeartbeat.load(data)

        assert restored.health_accepting_work == original.health_accepting_work
        assert restored.health_throughput == original.health_throughput
        assert restored.health_expected_throughput == original.health_expected_throughput
        assert restored.health_overload_state == original.health_overload_state


class TestManagerHeartbeatHealthPiggyback:
    """Test health piggyback fields in ManagerHeartbeat."""

    def test_default_health_fields(self) -> None:
        """Test default values for health piggyback fields."""
        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=5,
            active_workflows=20,
            worker_count=10,
            healthy_worker_count=10,
            available_cores=40,
            total_cores=80,
        )

        assert heartbeat.health_accepting_jobs is True
        assert heartbeat.health_has_quorum is True
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"

    def test_custom_health_fields(self) -> None:
        """Test custom values for health piggyback fields."""
        heartbeat = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-east",
            is_leader=False,
            term=3,
            version=10,
            active_jobs=15,
            active_workflows=60,
            worker_count=10,
            healthy_worker_count=6,
            available_cores=10,
            total_cores=80,
            health_accepting_jobs=False,
            health_has_quorum=False,
            health_throughput=100.0,
            health_expected_throughput=150.0,
            health_overload_state="overloaded",
        )

        assert heartbeat.health_accepting_jobs is False
        assert heartbeat.health_has_quorum is False
        assert heartbeat.health_throughput == 100.0
        assert heartbeat.health_expected_throughput == 150.0
        assert heartbeat.health_overload_state == "overloaded"

    def test_serialization_roundtrip(self) -> None:
        """Test that health fields survive serialization."""
        original = ManagerHeartbeat(
            node_id="manager-1",
            datacenter="dc-east",
            is_leader=True,
            term=1,
            version=1,
            active_jobs=5,
            active_workflows=20,
            worker_count=10,
            healthy_worker_count=10,
            available_cores=40,
            total_cores=80,
            health_accepting_jobs=True,
            health_has_quorum=True,
            health_throughput=50.0,
            health_expected_throughput=60.0,
            health_overload_state="busy",
        )

        # Serialize and deserialize
        data = original.dump()
        restored = ManagerHeartbeat.load(data)

        assert restored.health_accepting_jobs == original.health_accepting_jobs
        assert restored.health_has_quorum == original.health_has_quorum
        assert restored.health_throughput == original.health_throughput
        assert restored.health_expected_throughput == original.health_expected_throughput
        assert restored.health_overload_state == original.health_overload_state


class TestGateHeartbeatHealthPiggyback:
    """Test health piggyback fields in GateHeartbeat."""

    def test_default_health_fields(self) -> None:
        """Test default values for health piggyback fields."""
        heartbeat = GateHeartbeat(
            node_id="gate-1",
            datacenter="dc-global",
            is_leader=True,
            term=1,
            version=1,
            state="active",
            active_jobs=10,
            active_datacenters=3,
            manager_count=6,
        )

        assert heartbeat.health_has_dc_connectivity is True
        assert heartbeat.health_connected_dc_count == 0
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"

    def test_custom_health_fields(self) -> None:
        """Test custom values for health piggyback fields."""
        heartbeat = GateHeartbeat(
            node_id="gate-1",
            datacenter="dc-global",
            is_leader=False,
            term=2,
            version=5,
            state="degraded",
            active_jobs=50,
            active_datacenters=2,
            manager_count=4,
            health_has_dc_connectivity=False,
            health_connected_dc_count=1,
            health_throughput=200.0,
            health_expected_throughput=300.0,
            health_overload_state="stressed",
        )

        assert heartbeat.health_has_dc_connectivity is False
        assert heartbeat.health_connected_dc_count == 1
        assert heartbeat.health_throughput == 200.0
        assert heartbeat.health_expected_throughput == 300.0
        assert heartbeat.health_overload_state == "stressed"

    def test_serialization_roundtrip(self) -> None:
        """Test that health fields survive serialization."""
        original = GateHeartbeat(
            node_id="gate-1",
            datacenter="dc-global",
            is_leader=True,
            term=1,
            version=1,
            state="active",
            active_jobs=10,
            active_datacenters=3,
            manager_count=6,
            health_has_dc_connectivity=True,
            health_connected_dc_count=3,
            health_throughput=150.0,
            health_expected_throughput=180.0,
            health_overload_state="busy",
        )

        # Serialize and deserialize
        data = original.dump()
        restored = GateHeartbeat.load(data)

        assert restored.health_has_dc_connectivity == original.health_has_dc_connectivity
        assert restored.health_connected_dc_count == original.health_connected_dc_count
        assert restored.health_throughput == original.health_throughput
        assert restored.health_expected_throughput == original.health_expected_throughput
        assert restored.health_overload_state == original.health_overload_state


class TestWorkerStateEmbedderHealthPiggyback:
    """Test WorkerStateEmbedder health piggyback field population."""

    def test_embedder_with_health_callbacks(self) -> None:
        """Test that health callbacks are used in heartbeat."""
        embedder = WorkerStateEmbedder(
            get_node_id=lambda: "worker-1",
            get_worker_state=lambda: "healthy",
            get_available_cores=lambda: 4,
            get_queue_depth=lambda: 2,
            get_cpu_percent=lambda: 30.0,
            get_memory_percent=lambda: 45.0,
            get_state_version=lambda: 1,
            get_active_workflows=lambda: {"wf-1": "running"},
            # Health piggyback callbacks
            get_health_accepting_work=lambda: True,
            get_health_throughput=lambda: 5.0,
            get_health_expected_throughput=lambda: 8.0,
            get_health_overload_state=lambda: "busy",
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = WorkerHeartbeat.load(state_bytes)
        assert heartbeat.health_accepting_work is True
        assert heartbeat.health_throughput == 5.0
        assert heartbeat.health_expected_throughput == 8.0
        assert heartbeat.health_overload_state == "busy"

    def test_embedder_without_health_callbacks(self) -> None:
        """Test that default values are used when no health callbacks."""
        embedder = WorkerStateEmbedder(
            get_node_id=lambda: "worker-1",
            get_worker_state=lambda: "healthy",
            get_available_cores=lambda: 4,
            get_queue_depth=lambda: 0,
            get_cpu_percent=lambda: 20.0,
            get_memory_percent=lambda: 30.0,
            get_state_version=lambda: 1,
            get_active_workflows=lambda: {},
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = WorkerHeartbeat.load(state_bytes)
        # Default values when callbacks not provided
        assert heartbeat.health_accepting_work is True
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"


class TestManagerStateEmbedderHealthPiggyback:
    """Test ManagerStateEmbedder health piggyback field population."""

    def test_embedder_with_health_callbacks(self) -> None:
        """Test that health callbacks are used in heartbeat."""
        embedder = ManagerStateEmbedder(
            get_node_id=lambda: "manager-1",
            get_datacenter=lambda: "dc-east",
            is_leader=lambda: True,
            get_term=lambda: 1,
            get_state_version=lambda: 1,
            get_active_jobs=lambda: 5,
            get_active_workflows=lambda: 20,
            get_worker_count=lambda: 10,
            get_healthy_worker_count=lambda: 10,
            get_available_cores=lambda: 40,
            get_total_cores=lambda: 80,
            on_worker_heartbeat=lambda hb, addr: None,
            # Health piggyback callbacks
            get_health_accepting_jobs=lambda: True,
            get_health_has_quorum=lambda: True,
            get_health_throughput=lambda: 100.0,
            get_health_expected_throughput=lambda: 120.0,
            get_health_overload_state=lambda: "stressed",
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = ManagerHeartbeat.load(state_bytes)
        assert heartbeat.health_accepting_jobs is True
        assert heartbeat.health_has_quorum is True
        assert heartbeat.health_throughput == 100.0
        assert heartbeat.health_expected_throughput == 120.0
        assert heartbeat.health_overload_state == "stressed"

    def test_embedder_without_health_callbacks(self) -> None:
        """Test that default values are used when no health callbacks."""
        embedder = ManagerStateEmbedder(
            get_node_id=lambda: "manager-1",
            get_datacenter=lambda: "dc-east",
            is_leader=lambda: False,
            get_term=lambda: 1,
            get_state_version=lambda: 1,
            get_active_jobs=lambda: 0,
            get_active_workflows=lambda: 0,
            get_worker_count=lambda: 5,
            get_healthy_worker_count=lambda: 5,
            get_available_cores=lambda: 20,
            get_total_cores=lambda: 40,
            on_worker_heartbeat=lambda hb, addr: None,
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = ManagerHeartbeat.load(state_bytes)
        # Default values when callbacks not provided
        assert heartbeat.health_accepting_jobs is True
        assert heartbeat.health_has_quorum is True
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"


class TestGateStateEmbedderHealthPiggyback:
    """Test GateStateEmbedder health piggyback field population."""

    def test_embedder_with_health_callbacks(self) -> None:
        """Test that health callbacks are used in heartbeat."""
        embedder = GateStateEmbedder(
            get_node_id=lambda: "gate-1",
            get_datacenter=lambda: "dc-global",
            is_leader=lambda: True,
            get_term=lambda: 1,
            get_state_version=lambda: 1,
            get_gate_state=lambda: "active",
            get_active_jobs=lambda: 10,
            get_active_datacenters=lambda: 3,
            get_manager_count=lambda: 6,
            on_manager_heartbeat=lambda hb, addr: None,
            # Health piggyback callbacks
            get_health_has_dc_connectivity=lambda: True,
            get_health_connected_dc_count=lambda: 3,
            get_health_throughput=lambda: 200.0,
            get_health_expected_throughput=lambda: 250.0,
            get_health_overload_state=lambda: "busy",
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = GateHeartbeat.load(state_bytes)
        assert heartbeat.health_has_dc_connectivity is True
        assert heartbeat.health_connected_dc_count == 3
        assert heartbeat.health_throughput == 200.0
        assert heartbeat.health_expected_throughput == 250.0
        assert heartbeat.health_overload_state == "busy"

    def test_embedder_without_health_callbacks(self) -> None:
        """Test that default values are used when no health callbacks."""
        embedder = GateStateEmbedder(
            get_node_id=lambda: "gate-1",
            get_datacenter=lambda: "dc-global",
            is_leader=lambda: False,
            get_term=lambda: 1,
            get_state_version=lambda: 1,
            get_gate_state=lambda: "syncing",
            get_active_jobs=lambda: 0,
            get_active_datacenters=lambda: 0,
            get_manager_count=lambda: 0,
            on_manager_heartbeat=lambda hb, addr: None,
        )

        state_bytes = embedder.get_state()
        assert state_bytes is not None

        heartbeat = GateHeartbeat.load(state_bytes)
        # Default values when callbacks not provided
        assert heartbeat.health_has_dc_connectivity is True
        assert heartbeat.health_connected_dc_count == 0
        assert heartbeat.health_throughput == 0.0
        assert heartbeat.health_expected_throughput == 0.0
        assert heartbeat.health_overload_state == "healthy"


class TestHealthPiggybackDataclass:
    """Test HealthPiggyback dataclass operations."""

    def test_create_piggyback(self) -> None:
        """Test creating a health piggyback."""
        piggyback = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            is_alive=True,
            accepting_work=True,
            capacity=4,
            throughput=10.0,
            expected_throughput=15.0,
            overload_state="healthy",
        )

        assert piggyback.node_id == "worker-1"
        assert piggyback.node_type == "worker"
        assert piggyback.is_alive is True
        assert piggyback.accepting_work is True
        assert piggyback.capacity == 4
        assert piggyback.throughput == 10.0
        assert piggyback.expected_throughput == 15.0
        assert piggyback.overload_state == "healthy"

    def test_to_dict_from_dict_roundtrip(self) -> None:
        """Test serialization roundtrip."""
        original = HealthPiggyback(
            node_id="manager-1",
            node_type="manager",
            is_alive=True,
            accepting_work=True,
            capacity=40,
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
        recent = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )

        old = HealthPiggyback(
            node_id="worker-2",
            node_type="worker",
            timestamp=time.monotonic() - 120.0,  # 2 minutes ago
        )

        assert recent.is_stale(max_age_seconds=60.0) is False
        assert old.is_stale(max_age_seconds=60.0) is True

    def test_default_values(self) -> None:
        """Test default values for HealthPiggyback."""
        piggyback = HealthPiggyback(
            node_id="gate-1",
            node_type="gate",
        )

        assert piggyback.is_alive is True
        assert piggyback.accepting_work is True
        assert piggyback.capacity == 0
        assert piggyback.throughput == 0.0
        assert piggyback.expected_throughput == 0.0
        assert piggyback.overload_state == "healthy"
