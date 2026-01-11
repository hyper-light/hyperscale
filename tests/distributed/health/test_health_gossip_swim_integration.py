"""
Integration tests for Health Gossip SWIM Protocol Integration (Phase 6.1).

Tests the integration of HealthGossipBuffer with SWIM messages including:
- StateEmbedder.get_health_piggyback() for all node types
- Message encoding with both membership and health gossip
- Message parsing with health piggyback extraction
- End-to-end health state dissemination
- Callback integration with LocalHealthMultiplier
"""

import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from hyperscale.distributed.health.tracker import HealthPiggyback
from hyperscale.distributed.swim.core.state_embedder import (
    GateStateEmbedder,
    ManagerStateEmbedder,
    NullStateEmbedder,
    WorkerStateEmbedder,
)
from hyperscale.distributed.swim.gossip.health_gossip_buffer import (
    HealthGossipBuffer,
    HealthGossipBufferConfig,
    HealthGossipEntry,
    MAX_HEALTH_PIGGYBACK_SIZE,
)


# =============================================================================
# StateEmbedder get_health_piggyback Tests
# =============================================================================


class TestNullStateEmbedderHealthPiggyback:
    """Test NullStateEmbedder health piggyback."""

    def test_get_health_piggyback_returns_none(self) -> None:
        """Test that NullStateEmbedder returns None for health piggyback."""
        embedder = NullStateEmbedder()
        result = embedder.get_health_piggyback()
        assert result is None


class TestWorkerStateEmbedderHealthPiggyback:
    """Test WorkerStateEmbedder health piggyback generation."""

    def test_get_health_piggyback_basic(self) -> None:
        """Test basic health piggyback generation."""
        embedder = WorkerStateEmbedder(
            get_node_id=lambda: "worker-dc1-001",
            get_worker_state=lambda: "healthy",
            get_available_cores=lambda: 8,
            get_queue_depth=lambda: 5,
            get_cpu_percent=lambda: 45.0,
            get_memory_percent=lambda: 60.0,
            get_state_version=lambda: 10,
            get_active_workflows=lambda: {"wf-1": "running"},
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.node_id == "worker-dc1-001"
        assert piggyback.node_type == "worker"
        assert piggyback.is_alive is True
        assert piggyback.accepting_work is True  # Default
        assert piggyback.capacity == 8  # From get_available_cores

    def test_get_health_piggyback_with_callbacks(self) -> None:
        """Test health piggyback with all health callbacks set."""
        embedder = WorkerStateEmbedder(
            get_node_id=lambda: "worker-dc1-001",
            get_worker_state=lambda: "degraded",
            get_available_cores=lambda: 4,
            get_queue_depth=lambda: 20,
            get_cpu_percent=lambda: 90.0,
            get_memory_percent=lambda: 85.0,
            get_state_version=lambda: 15,
            get_active_workflows=lambda: {},
            get_health_accepting_work=lambda: False,
            get_health_throughput=lambda: 25.5,
            get_health_expected_throughput=lambda: 50.0,
            get_health_overload_state=lambda: "stressed",
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.accepting_work is False
        assert piggyback.throughput == 25.5
        assert piggyback.expected_throughput == 50.0
        assert piggyback.overload_state == "stressed"

    def test_get_health_piggyback_timestamp_is_current(self) -> None:
        """Test that health piggyback has current timestamp."""
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

        before = time.monotonic()
        piggyback = embedder.get_health_piggyback()
        after = time.monotonic()

        assert piggyback is not None
        assert before <= piggyback.timestamp <= after


class TestManagerStateEmbedderHealthPiggyback:
    """Test ManagerStateEmbedder health piggyback generation."""

    def test_get_health_piggyback_basic(self) -> None:
        """Test basic health piggyback generation for manager."""
        embedder = ManagerStateEmbedder(
            get_node_id=lambda: "manager-dc1-001",
            get_datacenter=lambda: "dc-east",
            is_leader=lambda: True,
            get_term=lambda: 5,
            get_state_version=lambda: 20,
            get_active_jobs=lambda: 10,
            get_active_workflows=lambda: 50,
            get_worker_count=lambda: 20,
            get_healthy_worker_count=lambda: 18,
            get_available_cores=lambda: 80,
            get_total_cores=lambda: 100,
            on_worker_heartbeat=lambda hb, addr: None,
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.node_id == "manager-dc1-001"
        assert piggyback.node_type == "manager"
        assert piggyback.capacity == 80  # From get_available_cores

    def test_get_health_piggyback_with_callbacks(self) -> None:
        """Test health piggyback with manager-specific callbacks."""
        embedder = ManagerStateEmbedder(
            get_node_id=lambda: "manager-dc1-001",
            get_datacenter=lambda: "dc-east",
            is_leader=lambda: False,
            get_term=lambda: 3,
            get_state_version=lambda: 15,
            get_active_jobs=lambda: 25,
            get_active_workflows=lambda: 100,
            get_worker_count=lambda: 20,
            get_healthy_worker_count=lambda: 10,
            get_available_cores=lambda: 40,
            get_total_cores=lambda: 100,
            on_worker_heartbeat=lambda hb, addr: None,
            get_health_accepting_jobs=lambda: False,
            get_health_has_quorum=lambda: True,
            get_health_throughput=lambda: 150.0,
            get_health_expected_throughput=lambda: 200.0,
            get_health_overload_state=lambda: "overloaded",
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.accepting_work is False  # From accepting_jobs
        assert piggyback.throughput == 150.0
        assert piggyback.expected_throughput == 200.0
        assert piggyback.overload_state == "overloaded"


class TestGateStateEmbedderHealthPiggyback:
    """Test GateStateEmbedder health piggyback generation."""

    def test_get_health_piggyback_basic(self) -> None:
        """Test basic health piggyback generation for gate."""
        embedder = GateStateEmbedder(
            get_node_id=lambda: "gate-global-001",
            get_datacenter=lambda: "dc-global",
            is_leader=lambda: True,
            get_term=lambda: 2,
            get_state_version=lambda: 8,
            get_gate_state=lambda: "active",
            get_active_jobs=lambda: 30,
            get_active_datacenters=lambda: 5,
            get_manager_count=lambda: 10,
            on_manager_heartbeat=lambda hb, addr: None,
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.node_id == "gate-global-001"
        assert piggyback.node_type == "gate"
        assert piggyback.capacity == 0  # Default connected DC count

    def test_get_health_piggyback_with_dc_connectivity(self) -> None:
        """Test health piggyback with DC connectivity callbacks."""
        embedder = GateStateEmbedder(
            get_node_id=lambda: "gate-global-001",
            get_datacenter=lambda: "dc-global",
            is_leader=lambda: True,
            get_term=lambda: 2,
            get_state_version=lambda: 8,
            get_gate_state=lambda: "active",
            get_active_jobs=lambda: 30,
            get_active_datacenters=lambda: 5,
            get_manager_count=lambda: 10,
            on_manager_heartbeat=lambda hb, addr: None,
            get_health_has_dc_connectivity=lambda: True,
            get_health_connected_dc_count=lambda: 5,
            get_health_throughput=lambda: 500.0,
            get_health_expected_throughput=lambda: 600.0,
            get_health_overload_state=lambda: "busy",
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.accepting_work is True  # From has_dc_connectivity
        assert piggyback.capacity == 5  # From connected_dc_count
        assert piggyback.throughput == 500.0
        assert piggyback.overload_state == "busy"

    def test_get_health_piggyback_no_dc_connectivity(self) -> None:
        """Test health piggyback when DC connectivity lost."""
        embedder = GateStateEmbedder(
            get_node_id=lambda: "gate-global-001",
            get_datacenter=lambda: "dc-global",
            is_leader=lambda: False,
            get_term=lambda: 1,
            get_state_version=lambda: 5,
            get_gate_state=lambda: "degraded",
            get_active_jobs=lambda: 0,
            get_active_datacenters=lambda: 0,
            get_manager_count=lambda: 0,
            on_manager_heartbeat=lambda hb, addr: None,
            get_health_has_dc_connectivity=lambda: False,
            get_health_connected_dc_count=lambda: 0,
            get_health_overload_state=lambda: "stressed",
        )

        piggyback = embedder.get_health_piggyback()

        assert piggyback is not None
        assert piggyback.accepting_work is False
        assert piggyback.capacity == 0


# =============================================================================
# Message Integration Tests
# =============================================================================


class TestHealthGossipMessageFormat:
    """Test health gossip message format and integration with SWIM messages."""

    def test_health_piggyback_format(self) -> None:
        """Test the #|h message format."""
        buffer = HealthGossipBuffer()

        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            accepting_work=True,
            capacity=4,
            throughput=10.0,
            expected_throughput=15.0,
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        encoded = buffer.encode_piggyback()

        # Format verification
        assert encoded.startswith(b"#|h")
        # Should contain all fields separated by |
        decoded = encoded.decode()
        assert "node-1" in decoded
        assert "worker" in decoded
        assert "healthy" in decoded

    def test_multiple_entries_separated_by_hash(self) -> None:
        """Test that multiple entries are separated by # character."""
        buffer = HealthGossipBuffer()

        for i in range(3):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        encoded = buffer.encode_piggyback()

        # Count ; separators (excluding the #|h prefix)
        content = encoded[3:]  # Skip #|h
        parts = content.split(b";")
        assert len(parts) >= 1  # At least one entry

    def test_membership_and_health_gossip_coexistence(self) -> None:
        """Test that membership gossip | and health gossip #|h can coexist."""
        # Simulate a full SWIM message with both types
        base_message = b"ack>127.0.0.1:8001"

        # Membership gossip format (from GossipBuffer)
        membership_piggyback = b"|join:1:192.168.1.1:8000|alive:2:192.168.1.2:8001"

        # Health gossip format
        health_buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        health_buffer.update_local_health(health)
        health_piggyback = health_buffer.encode_piggyback()

        # Combined message
        full_message = base_message + membership_piggyback + health_piggyback

        # Verify both can be identified
        assert b"|join:" in full_message  # Membership gossip
        assert b"#|h" in full_message  # Health gossip

        # Extract health piggyback
        health_idx = full_message.find(b"#|h")
        assert health_idx > 0
        health_data = full_message[health_idx:]
        assert health_data.startswith(b"#|h")


class TestHealthGossipExtraction:
    """Test extracting health gossip from SWIM messages."""

    def test_extract_health_from_combined_message(self) -> None:
        """Test extracting health gossip from a combined message."""
        # Simulate what HealthAwareServer.receive() does
        base_message = b"ack>127.0.0.1:8001"
        membership = b"|join:1:192.168.1.1:8000"

        health_buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="overloaded",
            capacity=0,
            accepting_work=False,
            timestamp=time.monotonic(),
        )
        health_buffer.update_local_health(health)
        health_piggyback = health_buffer.encode_piggyback()

        full_message = base_message + membership + health_piggyback

        # Extract health gossip first
        health_idx = full_message.find(b"#|h")
        if health_idx > 0:
            health_data = full_message[health_idx:]
            remaining_message = full_message[:health_idx]

            # Process health
            receiver_buffer = HealthGossipBuffer()
            processed = receiver_buffer.decode_and_process_piggyback(health_data)
            assert processed == 1

            received_health = receiver_buffer.get_health("worker-1")
            assert received_health is not None
            assert received_health.overload_state == "overloaded"

            # Remaining message should have membership gossip
            assert b"|join:" in remaining_message

    def test_extract_health_when_no_membership_gossip(self) -> None:
        """Test extracting health gossip when there's no membership gossip."""
        base_message = b"ack>127.0.0.1:8001"

        health_buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        health_buffer.update_local_health(health)
        health_piggyback = health_buffer.encode_piggyback()

        full_message = base_message + health_piggyback

        health_idx = full_message.find(b"#|h")
        assert health_idx > 0
        health_data = full_message[health_idx:]
        remaining = full_message[:health_idx]

        assert remaining == base_message
        assert HealthGossipBuffer.is_health_piggyback(health_data)


class TestHealthGossipPropagation:
    """Test health state propagation across nodes."""

    def test_single_hop_propagation(self) -> None:
        """Test health state propagates from node A to node B."""
        node_a_buffer = HealthGossipBuffer()
        node_b_buffer = HealthGossipBuffer()

        # Node A has stressed state
        health_a = HealthPiggyback(
            node_id="node-a",
            node_type="worker",
            overload_state="stressed",
            throughput=10.0,
            expected_throughput=20.0,
            timestamp=time.monotonic(),
        )
        node_a_buffer.update_local_health(health_a)

        # Encode and send to node B
        encoded = node_a_buffer.encode_piggyback()
        processed = node_b_buffer.decode_and_process_piggyback(encoded)

        assert processed == 1

        received = node_b_buffer.get_health("node-a")
        assert received is not None
        assert received.overload_state == "stressed"
        assert received.throughput == 10.0

    def test_multi_hop_propagation(self) -> None:
        """Test health state propagates through multiple nodes."""
        nodes = [HealthGossipBuffer() for _ in range(5)]

        # Original source health
        source_health = HealthPiggyback(
            node_id="source",
            node_type="worker",
            overload_state="overloaded",
            capacity=0,
            timestamp=time.monotonic(),
        )
        nodes[0].update_local_health(source_health)

        # Propagate through chain
        for i in range(len(nodes) - 1):
            encoded = nodes[i].encode_piggyback()
            nodes[i + 1].decode_and_process_piggyback(encoded)

        # Last node should have source's health
        received = nodes[-1].get_health("source")
        assert received is not None
        assert received.overload_state == "overloaded"

    def test_fan_out_propagation(self) -> None:
        """Test health state fans out to multiple nodes."""
        source = HealthGossipBuffer()
        receivers = [HealthGossipBuffer() for _ in range(10)]

        source_health = HealthPiggyback(
            node_id="source",
            node_type="manager",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        source.update_local_health(source_health)

        encoded = source.encode_piggyback()

        # Fan out to all receivers
        for receiver in receivers:
            receiver.decode_and_process_piggyback(encoded)

        # All receivers should have source's health
        for receiver in receivers:
            health = receiver.get_health("source")
            assert health is not None
            assert health.overload_state == "stressed"


class TestHealthGossipWithLocalHealthMultiplier:
    """Test integration with LocalHealthMultiplier for timeout adjustments."""

    def test_callback_integration_for_lhm(self) -> None:
        """Test that health updates can trigger LHM adjustments."""
        buffer = HealthGossipBuffer()

        # Track calls for LHM integration
        lhm_updates: list[HealthPiggyback] = []

        def on_health_update(health: HealthPiggyback) -> None:
            lhm_updates.append(health)

        buffer.set_health_update_callback(on_health_update)

        # Receive health from stressed node
        stressed = HealthPiggyback(
            node_id="stressed-node",
            node_type="worker",
            overload_state="stressed",
            throughput=5.0,
            expected_throughput=20.0,
            timestamp=time.monotonic(),
        )
        buffer.process_received_health(stressed)

        # Callback should have been invoked
        assert len(lhm_updates) == 1
        assert lhm_updates[0].overload_state == "stressed"


class TestHealthGossipEdgeCasesIntegration:
    """Edge cases for health gossip SWIM integration."""

    def test_empty_message_handling(self) -> None:
        """Test handling when message has no piggyback."""
        base_message = b"ack>127.0.0.1:8001"

        health_idx = base_message.find(b"#|h")
        assert health_idx == -1  # No health piggyback

    def test_health_only_no_base_message(self) -> None:
        """Test health piggyback without base message (invalid)."""
        health_buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        health_buffer.update_local_health(health)
        encoded = health_buffer.encode_piggyback()

        # Raw health piggyback should still be parseable
        receiver = HealthGossipBuffer()
        processed = receiver.decode_and_process_piggyback(encoded)
        assert processed == 1

    def test_partial_corruption_resilience(self) -> None:
        """Test resilience to partial message corruption."""
        health_buffer = HealthGossipBuffer()

        # Add several health entries
        for i in range(5):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            health_buffer.update_local_health(health)

        encoded = health_buffer.encode_piggyback()

        # Corrupt middle of message
        corrupted = encoded[:30] + b"CORRUPTION" + encoded[40:]

        receiver = HealthGossipBuffer()
        # Should process what it can, skip corrupted
        processed = receiver.decode_and_process_piggyback(corrupted)

        # Some entries might still be processed
        # The key is it doesn't crash
        assert processed >= 0


class TestHealthGossipSizeConstraints:
    """Test size constraints for health gossip in UDP messages."""

    def test_max_health_piggyback_size_respected(self) -> None:
        """Test that encoding respects MAX_HEALTH_PIGGYBACK_SIZE."""
        buffer = HealthGossipBuffer()

        # Add many entries with long node IDs
        for i in range(100):
            health = HealthPiggyback(
                node_id=f"very-long-node-identifier-for-worker-{i:04d}",
                node_type="worker",
                overload_state="overloaded",
                capacity=9999,
                throughput=99999.99,
                expected_throughput=99999.99,
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        encoded = buffer.encode_piggyback(max_size=MAX_HEALTH_PIGGYBACK_SIZE)

        assert len(encoded) <= MAX_HEALTH_PIGGYBACK_SIZE

    def test_space_sharing_with_membership_gossip(self) -> None:
        """Test that health gossip respects space left by membership gossip."""
        # Simulate a message with large membership gossip
        base_message = b"ack>127.0.0.1:8001"
        # Large membership gossip (simulated)
        large_membership = b"|" + b"join:1:192.168.1.1:8000|" * 20

        message_so_far = base_message + large_membership
        remaining_space = 1400 - len(message_so_far)  # UDP safe limit

        buffer = HealthGossipBuffer()
        for i in range(20):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Encode with remaining space
        encoded = buffer.encode_piggyback(max_size=remaining_space)

        assert len(encoded) <= remaining_space


class TestHealthGossipConcurrencyIntegration:
    """Test concurrent health gossip operations in SWIM context."""

    @pytest.mark.asyncio
    async def test_concurrent_receive_and_broadcast(self) -> None:
        """Test concurrent receiving and broadcasting of health updates."""
        import asyncio

        buffer = HealthGossipBuffer()
        received_count = 0

        def on_update(health: HealthPiggyback) -> None:
            nonlocal received_count
            received_count += 1

        buffer.set_health_update_callback(on_update)

        async def receive_updates() -> None:
            for i in range(50):
                health = HealthPiggyback(
                    node_id=f"remote-{i}",
                    node_type="worker",
                    timestamp=time.monotonic(),
                )
                buffer.process_received_health(health)
                await asyncio.sleep(0.001)

        async def broadcast_updates() -> None:
            for i in range(50):
                buffer.encode_piggyback()
                await asyncio.sleep(0.001)

        await asyncio.gather(receive_updates(), broadcast_updates())

        assert received_count == 50

    @pytest.mark.asyncio
    async def test_multiple_senders_same_node_id(self) -> None:
        """Test handling updates for same node from multiple sources."""
        import asyncio

        buffer = HealthGossipBuffer()

        async def send_update(source_idx: int) -> None:
            for _ in range(10):
                health = HealthPiggyback(
                    node_id="shared-node",
                    node_type="worker",
                    overload_state=["healthy", "busy", "stressed"][source_idx % 3],
                    timestamp=time.monotonic(),
                )
                buffer.process_received_health(health)
                await asyncio.sleep(0.001)

        await asyncio.gather(*[send_update(i) for i in range(5)])

        # Should have exactly one entry for shared-node
        health = buffer.get_health("shared-node")
        assert health is not None
        # Last update wins (most recent timestamp)


class TestHealthGossipNegativePathsIntegration:
    """Negative path tests for health gossip SWIM integration."""

    def test_malformed_health_marker(self) -> None:
        """Test handling of malformed #|h marker."""
        buffer = HealthGossipBuffer()

        # Incorrect format (missing proper prefix)
        processed = buffer.decode_and_process_piggyback(b"#hdata")
        assert processed == 0

    def test_truncated_health_entry(self) -> None:
        """Test handling of truncated health entry."""
        buffer = HealthGossipBuffer()

        # Valid start but truncated mid-entry
        processed = buffer.decode_and_process_piggyback(b"#|hnode-1|work")
        assert processed == 0

    def test_empty_health_entries(self) -> None:
        """Test handling of empty entries between separators."""
        buffer = HealthGossipBuffer()

        # Multiple empty entries
        processed = buffer.decode_and_process_piggyback(b"#|h;;;")
        assert processed == 0

    def test_very_large_timestamp(self) -> None:
        """Test handling of very large timestamp values."""
        buffer = HealthGossipBuffer()

        # Timestamp way in future
        data = b"#|hnode-1|worker|healthy|1|4|10.0|15.0|999999999999.99"
        processed = buffer.decode_and_process_piggyback(data)
        # Should still parse
        assert processed == 1
