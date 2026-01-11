"""
Server integration tests for Version Skew Handling (AD-25).

Tests version negotiation in realistic server scenarios with:
- Async connection handling with version validation
- Rolling upgrade simulations across mixed-version clusters
- Feature degradation when older nodes are present
- Connection rejection for incompatible major versions
- Failure paths and edge cases
- Multi-node cluster version compatibility
"""

import asyncio
import pytest
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from hyperscale.distributed.protocol import (
    ProtocolVersion,
    CURRENT_PROTOCOL_VERSION,
    FEATURE_VERSIONS,
    get_all_features,
    get_features_for_version,
    NodeCapabilities,
    NegotiatedCapabilities,
    negotiate_capabilities,
)
from hyperscale.distributed.models import (
    WorkerRegistration,
    ManagerPeerRegistration,
    ManagerPeerRegistrationResponse,
    RegistrationResponse,
    NodeInfo,
    ManagerInfo,
    NodeRole,
)


class ConnectionState(Enum):
    """State of a connection."""

    PENDING = "pending"
    NEGOTIATING = "negotiating"
    CONNECTED = "connected"
    REJECTED = "rejected"
    DISCONNECTED = "disconnected"


@dataclass
class ConnectionInfo:
    """Information about a connection."""

    local_node_id: str
    remote_node_id: str
    state: ConnectionState
    negotiated: NegotiatedCapabilities | None = None
    rejection_reason: str | None = None
    established_at: float | None = None


class SimulatedNode:
    """
    Base simulated node for version negotiation testing.

    Handles connection establishment with version negotiation.
    """

    def __init__(
        self,
        node_id: str,
        role: NodeRole,
        protocol_version: ProtocolVersion | None = None,
    ):
        self._node_id = node_id
        self._role = role
        self._protocol_version = protocol_version or CURRENT_PROTOCOL_VERSION
        self._capabilities = NodeCapabilities(
            protocol_version=self._protocol_version,
            capabilities=get_features_for_version(self._protocol_version),
            node_version=f"hyperscale-{self._protocol_version}",
        )
        self._connections: dict[str, ConnectionInfo] = {}
        self._connection_attempts = 0
        self._rejection_count = 0

    @property
    def node_id(self) -> str:
        return self._node_id

    @property
    def protocol_version(self) -> ProtocolVersion:
        return self._protocol_version

    @property
    def capabilities(self) -> NodeCapabilities:
        return self._capabilities

    async def handle_connection_request(
        self,
        remote_capabilities: NodeCapabilities,
        remote_node_id: str,
    ) -> tuple[bool, NegotiatedCapabilities | None, str | None]:
        """
        Handle incoming connection request with version negotiation.

        Args:
            remote_capabilities: The connecting node's capabilities.
            remote_node_id: The connecting node's ID.

        Returns:
            Tuple of (accepted, negotiated_capabilities, rejection_reason)
        """
        self._connection_attempts += 1

        # Perform negotiation
        result = negotiate_capabilities(self._capabilities, remote_capabilities)

        if not result.compatible:
            self._rejection_count += 1
            rejection_reason = (
                f"Incompatible protocol versions: "
                f"local={self._protocol_version} vs remote={remote_capabilities.protocol_version}"
            )
            self._connections[remote_node_id] = ConnectionInfo(
                local_node_id=self._node_id,
                remote_node_id=remote_node_id,
                state=ConnectionState.REJECTED,
                rejection_reason=rejection_reason,
            )
            return False, None, rejection_reason

        # Accept connection
        self._connections[remote_node_id] = ConnectionInfo(
            local_node_id=self._node_id,
            remote_node_id=remote_node_id,
            state=ConnectionState.CONNECTED,
            negotiated=result,
            established_at=time.time(),
        )
        return True, result, None

    def get_connection(self, remote_node_id: str) -> ConnectionInfo | None:
        """Get connection info for a remote node."""
        return self._connections.get(remote_node_id)

    def get_all_connections(self) -> list[ConnectionInfo]:
        """Get all connections."""
        return list(self._connections.values())

    def supports_feature_with(self, remote_node_id: str, feature: str) -> bool:
        """Check if a feature is supported with a specific connected node."""
        conn = self._connections.get(remote_node_id)
        if conn is None or conn.negotiated is None:
            return False
        return conn.negotiated.supports(feature)


class SimulatedWorker(SimulatedNode):
    """Simulated worker node for version testing."""

    def __init__(
        self,
        node_id: str,
        protocol_version: ProtocolVersion | None = None,
    ):
        super().__init__(node_id, NodeRole.WORKER, protocol_version)
        self._manager_id: str | None = None

    async def register_with_manager(
        self,
        manager: "SimulatedManager",
    ) -> tuple[bool, str | None]:
        """
        Register with a manager node.

        Args:
            manager: The manager to register with.

        Returns:
            Tuple of (success, error_message)
        """
        accepted, negotiated, rejection = await manager.handle_connection_request(
            self._capabilities,
            self._node_id,
        )

        if not accepted:
            return False, rejection

        # Store connection on worker side too
        self._connections[manager.node_id] = ConnectionInfo(
            local_node_id=self._node_id,
            remote_node_id=manager.node_id,
            state=ConnectionState.CONNECTED,
            negotiated=negotiated,
            established_at=time.time(),
        )
        self._manager_id = manager.node_id
        return True, None

    def can_use_rate_limiting(self) -> bool:
        """Check if rate limiting is supported with current manager."""
        if self._manager_id is None:
            return False
        return self.supports_feature_with(self._manager_id, "rate_limiting")

    def can_use_healthcheck_extensions(self) -> bool:
        """Check if healthcheck extensions are supported with current manager."""
        if self._manager_id is None:
            return False
        return self.supports_feature_with(self._manager_id, "healthcheck_extensions")


class SimulatedManager(SimulatedNode):
    """Simulated manager node for version testing."""

    def __init__(
        self,
        node_id: str,
        protocol_version: ProtocolVersion | None = None,
    ):
        super().__init__(node_id, NodeRole.MANAGER, protocol_version)
        self._workers: dict[str, SimulatedWorker] = {}
        self._peer_managers: dict[str, "SimulatedManager"] = {}

    async def register_worker(
        self,
        worker: SimulatedWorker,
    ) -> tuple[bool, str | None]:
        """
        Accept a worker registration.

        Args:
            worker: The worker registering.

        Returns:
            Tuple of (success, error_message)
        """
        success, error = await worker.register_with_manager(self)
        if success:
            self._workers[worker.node_id] = worker
        return success, error

    async def register_peer(
        self,
        peer: "SimulatedManager",
    ) -> tuple[bool, str | None]:
        """
        Register with a peer manager.

        Args:
            peer: The peer manager to connect to.

        Returns:
            Tuple of (success, error_message)
        """
        accepted, negotiated, rejection = await peer.handle_connection_request(
            self._capabilities,
            self._node_id,
        )

        if not accepted:
            return False, rejection

        # Store connection on both sides
        self._connections[peer.node_id] = ConnectionInfo(
            local_node_id=self._node_id,
            remote_node_id=peer.node_id,
            state=ConnectionState.CONNECTED,
            negotiated=negotiated,
            established_at=time.time(),
        )
        self._peer_managers[peer.node_id] = peer
        return True, None

    def get_cluster_minimum_version(self) -> ProtocolVersion:
        """Get the minimum protocol version across all connected nodes."""
        versions = [self._protocol_version]
        for conn in self._connections.values():
            if conn.state == ConnectionState.CONNECTED and conn.negotiated:
                versions.append(conn.negotiated.remote_version)
        return min(versions, key=lambda v: (v.major, v.minor))


class SimulatedGate(SimulatedNode):
    """Simulated gate node for version testing."""

    def __init__(
        self,
        node_id: str,
        protocol_version: ProtocolVersion | None = None,
    ):
        super().__init__(node_id, NodeRole.GATE, protocol_version)
        self._managers: dict[str, SimulatedManager] = {}

    async def connect_to_manager(
        self,
        manager: SimulatedManager,
    ) -> tuple[bool, str | None]:
        """Connect to a manager."""
        accepted, negotiated, rejection = await manager.handle_connection_request(
            self._capabilities,
            self._node_id,
        )

        if not accepted:
            return False, rejection

        self._connections[manager.node_id] = ConnectionInfo(
            local_node_id=self._node_id,
            remote_node_id=manager.node_id,
            state=ConnectionState.CONNECTED,
            negotiated=negotiated,
            established_at=time.time(),
        )
        self._managers[manager.node_id] = manager
        return True, None


class TestVersionNegotiationBasics:
    """Test basic version negotiation scenarios."""

    @pytest.mark.asyncio
    async def test_same_version_connection(self) -> None:
        """Test connection between nodes with same version."""
        worker = SimulatedWorker("worker-1", CURRENT_PROTOCOL_VERSION)
        manager = SimulatedManager("manager-1", CURRENT_PROTOCOL_VERSION)

        success, error = await manager.register_worker(worker)

        assert success is True
        assert error is None

        conn = manager.get_connection("worker-1")
        assert conn is not None
        assert conn.state == ConnectionState.CONNECTED
        assert conn.negotiated is not None
        assert conn.negotiated.compatible is True

        # Should have all current features
        all_features = get_features_for_version(CURRENT_PROTOCOL_VERSION)
        for feature in all_features:
            assert conn.negotiated.supports(feature) is True

    @pytest.mark.asyncio
    async def test_compatible_different_minor_versions(self) -> None:
        """Test connection between nodes with different minor versions."""
        # Worker is newer (1.4), Manager is older (1.2)
        worker = SimulatedWorker("worker-1", ProtocolVersion(1, 4))
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 2))

        success, error = await manager.register_worker(worker)

        assert success is True
        assert error is None

        conn = manager.get_connection("worker-1")
        assert conn is not None
        assert conn.negotiated.compatible is True

        # Should have 1.2 features, not 1.4 features
        assert conn.negotiated.supports("job_submission") is True
        assert conn.negotiated.supports("client_reconnection") is True
        assert conn.negotiated.supports("rate_limiting") is False  # 1.3
        assert conn.negotiated.supports("healthcheck_extensions") is False  # 1.4

    @pytest.mark.asyncio
    async def test_incompatible_major_versions_rejected(self) -> None:
        """Test that incompatible major versions are rejected."""
        worker = SimulatedWorker("worker-1", ProtocolVersion(2, 0))
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        success, error = await manager.register_worker(worker)

        assert success is False
        assert error is not None
        assert "Incompatible" in error

        conn = manager.get_connection("worker-1")
        assert conn is not None
        assert conn.state == ConnectionState.REJECTED


class TestRollingUpgradeScenarios:
    """Test rolling upgrade scenarios."""

    @pytest.mark.asyncio
    async def test_upgrade_workers_first(self) -> None:
        """
        Test rolling upgrade scenario: upgrade workers first.

        1. Start with v1.2 manager and v1.2 workers
        2. Upgrade workers to v1.4
        3. Workers should still work with v1.2 manager using v1.2 features
        """
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 2))

        # Original workers at v1.2
        old_workers = [
            SimulatedWorker(f"worker-{i}", ProtocolVersion(1, 2))
            for i in range(3)
        ]

        for worker in old_workers:
            success, _ = await manager.register_worker(worker)
            assert success is True

        # Simulate upgrade: new workers at v1.4 replace old ones
        new_workers = [
            SimulatedWorker(f"new-worker-{i}", ProtocolVersion(1, 4))
            for i in range(3)
        ]

        for worker in new_workers:
            success, _ = await manager.register_worker(worker)
            assert success is True

        # New workers should work but only use v1.2 features
        for worker in new_workers:
            assert worker.can_use_rate_limiting() is False  # Not available with v1.2 manager
            assert worker.can_use_healthcheck_extensions() is False

    @pytest.mark.asyncio
    async def test_upgrade_manager_after_workers(self) -> None:
        """
        Test rolling upgrade scenario: upgrade manager after workers.

        1. Workers already at v1.4
        2. Manager upgraded from v1.2 to v1.4
        3. All features now available
        """
        # New v1.4 manager
        new_manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        # Workers at v1.4
        workers = [
            SimulatedWorker(f"worker-{i}", ProtocolVersion(1, 4))
            for i in range(3)
        ]

        for worker in workers:
            success, _ = await new_manager.register_worker(worker)
            assert success is True

        # Now all features should be available
        for worker in workers:
            assert worker.can_use_rate_limiting() is True
            assert worker.can_use_healthcheck_extensions() is True

    @pytest.mark.asyncio
    async def test_mixed_version_cluster_during_upgrade(self) -> None:
        """
        Test mixed version cluster during rolling upgrade.

        Cluster has:
        - 1 v1.2 manager (being upgraded last)
        - 1 v1.4 worker (already upgraded)
        - 1 v1.2 worker (not yet upgraded)
        """
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 2))

        old_worker = SimulatedWorker("old-worker", ProtocolVersion(1, 2))
        new_worker = SimulatedWorker("new-worker", ProtocolVersion(1, 4))

        # Both should connect successfully
        success_old, _ = await manager.register_worker(old_worker)
        success_new, _ = await manager.register_worker(new_worker)

        assert success_old is True
        assert success_new is True

        # Both workers limited to v1.2 features due to manager
        assert old_worker.can_use_rate_limiting() is False
        assert new_worker.can_use_rate_limiting() is False

        # Minimum version in cluster is v1.2
        min_version = manager.get_cluster_minimum_version()
        assert min_version == ProtocolVersion(1, 2)


class TestFeatureDegradation:
    """Test feature degradation with older nodes."""

    @pytest.mark.asyncio
    async def test_features_degrade_to_common_denominator(self) -> None:
        """Test that features degrade to lowest common denominator."""
        # Manager at v1.4 with full features
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        # Worker at v1.0 with only base features
        worker = SimulatedWorker("worker-1", ProtocolVersion(1, 0))

        success, _ = await manager.register_worker(worker)
        assert success is True

        conn = manager.get_connection("worker-1")

        # Should only have v1.0 features
        assert conn.negotiated.supports("job_submission") is True
        assert conn.negotiated.supports("heartbeat") is True
        assert conn.negotiated.supports("cancellation") is True

        # Should NOT have newer features
        assert conn.negotiated.supports("batched_stats") is False  # 1.1
        assert conn.negotiated.supports("client_reconnection") is False  # 1.2
        assert conn.negotiated.supports("rate_limiting") is False  # 1.3
        assert conn.negotiated.supports("healthcheck_extensions") is False  # 1.4

    @pytest.mark.asyncio
    async def test_per_connection_feature_availability(self) -> None:
        """Test that feature availability is per-connection."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        # Three workers at different versions
        worker_v10 = SimulatedWorker("worker-v10", ProtocolVersion(1, 0))
        worker_v12 = SimulatedWorker("worker-v12", ProtocolVersion(1, 2))
        worker_v14 = SimulatedWorker("worker-v14", ProtocolVersion(1, 4))

        await manager.register_worker(worker_v10)
        await manager.register_worker(worker_v12)
        await manager.register_worker(worker_v14)

        # Check feature availability per connection
        assert manager.supports_feature_with("worker-v10", "rate_limiting") is False
        assert manager.supports_feature_with("worker-v12", "rate_limiting") is False
        assert manager.supports_feature_with("worker-v14", "rate_limiting") is True

        assert manager.supports_feature_with("worker-v10", "client_reconnection") is False
        assert manager.supports_feature_with("worker-v12", "client_reconnection") is True
        assert manager.supports_feature_with("worker-v14", "client_reconnection") is True


class TestConnectionFailurePaths:
    """Test connection failure paths."""

    @pytest.mark.asyncio
    async def test_rejection_increments_counter(self) -> None:
        """Test that rejected connections increment counter."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 0))
        incompatible_worker = SimulatedWorker("worker-1", ProtocolVersion(2, 0))

        await manager.register_worker(incompatible_worker)

        assert manager._rejection_count == 1
        assert manager._connection_attempts == 1

    @pytest.mark.asyncio
    async def test_multiple_rejections(self) -> None:
        """Test multiple rejected connections."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 0))

        incompatible_workers = [
            SimulatedWorker(f"worker-{i}", ProtocolVersion(2, i))
            for i in range(5)
        ]

        for worker in incompatible_workers:
            await manager.register_worker(worker)

        assert manager._rejection_count == 5
        assert manager._connection_attempts == 5

    @pytest.mark.asyncio
    async def test_connection_info_preserved_after_rejection(self) -> None:
        """Test that connection info is preserved after rejection."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 0))
        worker = SimulatedWorker("rejected-worker", ProtocolVersion(2, 0))

        success, error = await manager.register_worker(worker)

        assert success is False

        conn = manager.get_connection("rejected-worker")
        assert conn is not None
        assert conn.state == ConnectionState.REJECTED
        assert conn.rejection_reason is not None
        assert "Incompatible" in conn.rejection_reason


class TestMultiNodeCluster:
    """Test version handling in multi-node clusters."""

    @pytest.mark.asyncio
    async def test_gate_manager_worker_chain(self) -> None:
        """Test version negotiation through gate -> manager -> worker chain."""
        gate = SimulatedGate("gate-1", ProtocolVersion(1, 4))
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 3))
        worker = SimulatedWorker("worker-1", ProtocolVersion(1, 2))

        # Gate connects to manager (v1.4 <-> v1.3)
        success_gm, _ = await gate.connect_to_manager(manager)
        assert success_gm is True

        # Manager registers worker (v1.3 <-> v1.2)
        success_mw, _ = await manager.register_worker(worker)
        assert success_mw is True

        # Check feature availability at each hop
        gate_manager_conn = gate.get_connection("manager-1")
        manager_worker_conn = manager.get_connection("worker-1")

        # Gate-Manager: v1.3 features (lower of 1.4 and 1.3)
        assert gate_manager_conn.negotiated.supports("rate_limiting") is True
        assert gate_manager_conn.negotiated.supports("healthcheck_extensions") is False

        # Manager-Worker: v1.2 features (lower of 1.3 and 1.2)
        assert manager_worker_conn.negotiated.supports("client_reconnection") is True
        assert manager_worker_conn.negotiated.supports("rate_limiting") is False

    @pytest.mark.asyncio
    async def test_manager_peer_replication(self) -> None:
        """Test version negotiation between manager peers."""
        manager_1 = SimulatedManager("manager-1", ProtocolVersion(1, 4))
        manager_2 = SimulatedManager("manager-2", ProtocolVersion(1, 3))
        manager_3 = SimulatedManager("manager-3", ProtocolVersion(1, 2))

        # All managers connect to each other
        await manager_1.register_peer(manager_2)
        await manager_1.register_peer(manager_3)
        await manager_2.register_peer(manager_3)

        # Check connections
        conn_1_2 = manager_1.get_connection("manager-2")
        conn_1_3 = manager_1.get_connection("manager-3")
        conn_2_3 = manager_2.get_connection("manager-3")

        assert conn_1_2.negotiated.supports("rate_limiting") is True
        assert conn_1_2.negotiated.supports("healthcheck_extensions") is False

        assert conn_1_3.negotiated.supports("client_reconnection") is True
        assert conn_1_3.negotiated.supports("rate_limiting") is False

        assert conn_2_3.negotiated.supports("client_reconnection") is True
        assert conn_2_3.negotiated.supports("rate_limiting") is False

    @pytest.mark.asyncio
    async def test_cluster_minimum_version_tracking(self) -> None:
        """Test tracking minimum version across cluster."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        workers = [
            SimulatedWorker("worker-1", ProtocolVersion(1, 4)),
            SimulatedWorker("worker-2", ProtocolVersion(1, 3)),
            SimulatedWorker("worker-3", ProtocolVersion(1, 1)),
        ]

        for worker in workers:
            await manager.register_worker(worker)

        min_version = manager.get_cluster_minimum_version()
        assert min_version == ProtocolVersion(1, 1)


class TestVersionEdgeCases:
    """Test edge cases in version handling."""

    @pytest.mark.asyncio
    async def test_unknown_feature_not_supported(self) -> None:
        """Test that unknown features return False."""
        worker = SimulatedWorker("worker-1", CURRENT_PROTOCOL_VERSION)
        manager = SimulatedManager("manager-1", CURRENT_PROTOCOL_VERSION)

        await manager.register_worker(worker)

        conn = manager.get_connection("worker-1")
        assert conn.negotiated.supports("nonexistent_feature") is False

    @pytest.mark.asyncio
    async def test_version_1_0_minimum_features(self) -> None:
        """Test that v1.0 has minimum required features."""
        worker = SimulatedWorker("worker-1", ProtocolVersion(1, 0))
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 0))

        await manager.register_worker(worker)

        conn = manager.get_connection("worker-1")

        # Must have base features for system to function
        assert conn.negotiated.supports("job_submission") is True
        assert conn.negotiated.supports("workflow_dispatch") is True
        assert conn.negotiated.supports("heartbeat") is True
        assert conn.negotiated.supports("cancellation") is True

    @pytest.mark.asyncio
    async def test_concurrent_connections_with_different_versions(self) -> None:
        """Test concurrent connections from nodes with different versions."""
        manager = SimulatedManager("manager-1", ProtocolVersion(1, 4))

        workers = [
            SimulatedWorker(f"worker-{i}", ProtocolVersion(1, i))
            for i in range(5)  # v1.0 through v1.4
        ]

        # Connect all concurrently
        results = await asyncio.gather(*[
            manager.register_worker(worker) for worker in workers
        ])

        # All should succeed
        assert all(success for success, _ in results)

        # Each connection should have appropriate features
        for idx, worker in enumerate(workers):
            conn = manager.get_connection(worker.node_id)
            assert conn.state == ConnectionState.CONNECTED

            # Features available should match the worker's version
            expected_features = get_features_for_version(ProtocolVersion(1, idx))
            for feature in expected_features:
                assert conn.negotiated.supports(feature) is True


class TestMessageVersionFields:
    """Test version fields in protocol messages."""

    def test_worker_registration_default_version(self) -> None:
        """Test WorkerRegistration default version fields."""
        reg = WorkerRegistration(
            node=NodeInfo(
                node_id="worker-1",
                role=NodeRole.WORKER.value,
                host="localhost",
                port=8000,
                datacenter="dc-1",
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
        )

        # Should default to v1.0
        assert reg.protocol_version_major == 1
        assert reg.protocol_version_minor == 0
        assert reg.capabilities == ""

    def test_worker_registration_with_version(self) -> None:
        """Test WorkerRegistration with explicit version."""
        reg = WorkerRegistration(
            node=NodeInfo(
                node_id="worker-1",
                role=NodeRole.WORKER.value,
                host="localhost",
                port=8000,
                datacenter="dc-1",
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
            protocol_version_major=1,
            protocol_version_minor=4,
            capabilities="job_submission,rate_limiting,healthcheck_extensions",
        )

        assert reg.protocol_version_major == 1
        assert reg.protocol_version_minor == 4
        assert "rate_limiting" in reg.capabilities

    def test_worker_registration_roundtrip(self) -> None:
        """Test WorkerRegistration serialization preserves version."""
        original = WorkerRegistration(
            node=NodeInfo(
                node_id="worker-1",
                role=NodeRole.WORKER.value,
                host="localhost",
                port=8000,
                datacenter="dc-1",
            ),
            total_cores=4,
            available_cores=4,
            memory_mb=8192,
            available_memory_mb=8192,
            protocol_version_major=1,
            protocol_version_minor=3,
            capabilities="rate_limiting,retry_after",
        )

        serialized = original.dump()
        restored = WorkerRegistration.load(serialized)

        assert restored.protocol_version_major == 1
        assert restored.protocol_version_minor == 3
        assert restored.capabilities == "rate_limiting,retry_after"

    def test_registration_response_negotiated_version(self) -> None:
        """Test RegistrationResponse with negotiated version."""
        resp = RegistrationResponse(
            accepted=True,
            manager_id="manager-1",
            healthy_managers=[],
            protocol_version_major=1,
            protocol_version_minor=2,  # Negotiated down from 1.4
            capabilities="job_submission,cancellation,client_reconnection",
        )

        assert resp.accepted is True
        assert resp.protocol_version_major == 1
        assert resp.protocol_version_minor == 2
        assert "client_reconnection" in resp.capabilities
        assert "rate_limiting" not in resp.capabilities


class TestVersionCompatibilityMatrix:
    """Test version compatibility across all version pairs."""

    @pytest.mark.asyncio
    async def test_all_minor_versions_compatible(self) -> None:
        """Test that all minor versions within same major are compatible."""
        # Test all pairs within major version 1
        for local_minor in range(5):
            for remote_minor in range(5):
                worker = SimulatedWorker(
                    f"worker-{local_minor}-{remote_minor}",
                    ProtocolVersion(1, local_minor),
                )
                manager = SimulatedManager(
                    f"manager-{local_minor}-{remote_minor}",
                    ProtocolVersion(1, remote_minor),
                )

                success, _ = await manager.register_worker(worker)
                assert success is True, (
                    f"v1.{local_minor} should be compatible with v1.{remote_minor}"
                )

    @pytest.mark.asyncio
    async def test_cross_major_versions_incompatible(self) -> None:
        """Test that different major versions are incompatible."""
        major_versions = [1, 2, 3]

        for major_a in major_versions:
            for major_b in major_versions:
                if major_a == major_b:
                    continue

                worker = SimulatedWorker(
                    f"worker-{major_a}-{major_b}",
                    ProtocolVersion(major_a, 0),
                )
                manager = SimulatedManager(
                    f"manager-{major_a}-{major_b}",
                    ProtocolVersion(major_b, 0),
                )

                success, error = await manager.register_worker(worker)
                assert success is False, (
                    f"v{major_a}.0 should be incompatible with v{major_b}.0"
                )
                assert "Incompatible" in error
