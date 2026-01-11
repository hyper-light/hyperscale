"""
Integration tests for HealthGossipBuffer (Phase 6.1).

Tests O(log n) health state dissemination for SWIM protocol including:
- HealthGossipEntry serialization/deserialization
- HealthGossipBuffer encoding/decoding
- Priority-based broadcast ordering (severity-first)
- Stale entry cleanup and eviction
- Callback integration for health updates
- Concurrency handling with multiple nodes
- Edge cases (empty buffers, oversized entries, malformed data)
- Failure paths (invalid data, corruption)
"""

import asyncio
import time
from dataclasses import dataclass
from typing import Any
from unittest.mock import MagicMock, call

import pytest

from hyperscale.distributed_rewrite.health.tracker import HealthPiggyback
from hyperscale.distributed_rewrite.swim.gossip.health_gossip_buffer import (
    HealthGossipBuffer,
    HealthGossipBufferConfig,
    HealthGossipEntry,
    OverloadSeverity,
    MAX_HEALTH_PIGGYBACK_SIZE,
)


# =============================================================================
# HealthGossipEntry Tests
# =============================================================================


class TestHealthGossipEntrySerialization:
    """Test HealthGossipEntry to_bytes and from_bytes serialization."""

    def test_basic_serialization_roundtrip(self) -> None:
        """Test that serialization roundtrip preserves all fields."""
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            overload_state="healthy",
            accepting_work=True,
            capacity=4,
            throughput=10.5,
            expected_throughput=15.0,
            timestamp=time.monotonic(),
        )
        entry = HealthGossipEntry(health=health, timestamp=time.monotonic())

        serialized = entry.to_bytes()
        restored = HealthGossipEntry.from_bytes(serialized)

        assert restored is not None
        assert restored.health.node_id == health.node_id
        assert restored.health.node_type == health.node_type
        assert restored.health.overload_state == health.overload_state
        assert restored.health.accepting_work == health.accepting_work
        assert restored.health.capacity == health.capacity
        assert abs(restored.health.throughput - health.throughput) < 0.01
        assert abs(restored.health.expected_throughput - health.expected_throughput) < 0.01

    def test_serialization_with_special_characters_in_node_id(self) -> None:
        """Test serialization with node IDs containing special characters."""
        # Node IDs may contain dashes, underscores, dots
        health = HealthPiggyback(
            node_id="worker-dc_east.zone1-001",
            node_type="worker",
            overload_state="stressed",
            accepting_work=False,
            capacity=8,
            throughput=20.0,
            expected_throughput=25.0,
            timestamp=time.monotonic(),
        )
        entry = HealthGossipEntry(health=health, timestamp=time.monotonic())

        serialized = entry.to_bytes()
        restored = HealthGossipEntry.from_bytes(serialized)

        assert restored is not None
        assert restored.health.node_id == "worker-dc_east.zone1-001"

    def test_serialization_all_overload_states(self) -> None:
        """Test serialization with all possible overload states."""
        states = ["healthy", "busy", "stressed", "overloaded"]

        for state in states:
            health = HealthPiggyback(
                node_id=f"node-{state}",
                node_type="manager",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            entry = HealthGossipEntry(health=health, timestamp=time.monotonic())

            serialized = entry.to_bytes()
            restored = HealthGossipEntry.from_bytes(serialized)

            assert restored is not None
            assert restored.health.overload_state == state

    def test_serialization_float_precision(self) -> None:
        """Test that float values maintain sufficient precision."""
        health = HealthPiggyback(
            node_id="worker-1",
            node_type="worker",
            throughput=123.456789,
            expected_throughput=987.654321,
            timestamp=time.monotonic(),
        )
        entry = HealthGossipEntry(health=health, timestamp=time.monotonic())

        serialized = entry.to_bytes()
        restored = HealthGossipEntry.from_bytes(serialized)

        assert restored is not None
        # 2 decimal places preserved in format
        assert abs(restored.health.throughput - 123.46) < 0.01
        assert abs(restored.health.expected_throughput - 987.65) < 0.01


class TestHealthGossipEntryNegativePaths:
    """Test failure paths and invalid data handling for HealthGossipEntry."""

    def test_from_bytes_with_empty_data(self) -> None:
        """Test from_bytes returns None for empty data."""
        result = HealthGossipEntry.from_bytes(b"")
        assert result is None

    def test_from_bytes_with_insufficient_fields(self) -> None:
        """Test from_bytes returns None when not enough fields."""
        # Only 5 fields instead of 8
        result = HealthGossipEntry.from_bytes(b"node-1|worker|healthy|1|4")
        assert result is None

    def test_from_bytes_with_invalid_boolean(self) -> None:
        """Test from_bytes handles invalid boolean gracefully."""
        # Invalid accepting_work value (not 0 or 1)
        # This should parse but treat 'x' as false
        result = HealthGossipEntry.from_bytes(
            b"node-1|worker|healthy|x|4|10.0|15.0|12345.67"
        )
        # The parsing should succeed but accepting_work will be False (x != "1")
        assert result is not None
        assert result.health.accepting_work is False

    def test_from_bytes_with_invalid_integer_capacity(self) -> None:
        """Test from_bytes returns None for non-integer capacity."""
        result = HealthGossipEntry.from_bytes(
            b"node-1|worker|healthy|1|abc|10.0|15.0|12345.67"
        )
        assert result is None

    def test_from_bytes_with_invalid_float_throughput(self) -> None:
        """Test from_bytes returns None for non-float throughput."""
        result = HealthGossipEntry.from_bytes(
            b"node-1|worker|healthy|1|4|not_a_float|15.0|12345.67"
        )
        assert result is None

    def test_from_bytes_with_non_utf8_data(self) -> None:
        """Test from_bytes returns None for non-UTF8 data."""
        # Invalid UTF-8 sequence
        result = HealthGossipEntry.from_bytes(b"\xff\xfe\x00\x01")
        assert result is None

    def test_from_bytes_with_pipe_in_node_id(self) -> None:
        """Test from_bytes handles pipe character in node_id correctly."""
        # Pipe is the delimiter, so this would mess up parsing
        # The split would create more fields than expected
        data = b"node|with|pipes|worker|healthy|1|4|10.0|15.0|12345.67"
        result = HealthGossipEntry.from_bytes(data)
        # This should still work due to maxsplit=7 - anything after 7th | is timestamp
        # Actually with maxsplit=7, it splits into 8 parts max
        # "node", "with", "pipes", "worker", "healthy", "1", "4", "10.0|15.0|12345.67"
        # This would fail because the 8th field is "10.0|15.0|12345.67" not just timestamp
        assert result is None


class TestHealthGossipEntrySeverity:
    """Test severity ordering and prioritization."""

    def test_severity_ordering(self) -> None:
        """Test that severity is ordered correctly."""
        assert OverloadSeverity.HEALTHY < OverloadSeverity.BUSY
        assert OverloadSeverity.BUSY < OverloadSeverity.STRESSED
        assert OverloadSeverity.STRESSED < OverloadSeverity.OVERLOADED

    def test_entry_severity_property(self) -> None:
        """Test that entry severity property works correctly."""
        overloaded = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w", overload_state="overloaded"),
            timestamp=time.monotonic(),
        )
        stressed = HealthGossipEntry(
            health=HealthPiggyback(node_id="n2", node_type="w", overload_state="stressed"),
            timestamp=time.monotonic(),
        )
        busy = HealthGossipEntry(
            health=HealthPiggyback(node_id="n3", node_type="w", overload_state="busy"),
            timestamp=time.monotonic(),
        )
        healthy = HealthGossipEntry(
            health=HealthPiggyback(node_id="n4", node_type="w", overload_state="healthy"),
            timestamp=time.monotonic(),
        )

        assert overloaded.severity == OverloadSeverity.OVERLOADED
        assert stressed.severity == OverloadSeverity.STRESSED
        assert busy.severity == OverloadSeverity.BUSY
        assert healthy.severity == OverloadSeverity.HEALTHY

    def test_unknown_overload_state_severity(self) -> None:
        """Test that unknown overload states are treated as healthy."""
        unknown = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w", overload_state="unknown_state"),
            timestamp=time.monotonic(),
        )
        assert unknown.severity == OverloadSeverity.UNKNOWN
        # UNKNOWN == HEALTHY (value 0)
        assert unknown.severity == OverloadSeverity.HEALTHY


class TestHealthGossipEntryBroadcast:
    """Test broadcast counting and limits."""

    def test_should_broadcast_initially_true(self) -> None:
        """Test that new entries should be broadcast."""
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w"),
            timestamp=time.monotonic(),
            broadcast_count=0,
            max_broadcasts=5,
        )
        assert entry.should_broadcast() is True

    def test_should_broadcast_at_limit(self) -> None:
        """Test that entries at limit should not be broadcast."""
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w"),
            timestamp=time.monotonic(),
            broadcast_count=5,
            max_broadcasts=5,
        )
        assert entry.should_broadcast() is False

    def test_mark_broadcast_increments_count(self) -> None:
        """Test that mark_broadcast increments the count."""
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w"),
            timestamp=time.monotonic(),
            broadcast_count=0,
        )

        assert entry.broadcast_count == 0
        entry.mark_broadcast()
        assert entry.broadcast_count == 1
        entry.mark_broadcast()
        assert entry.broadcast_count == 2


class TestHealthGossipEntryStaleness:
    """Test staleness detection."""

    def test_is_stale_recent_entry(self) -> None:
        """Test that recent entries are not stale."""
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w", timestamp=time.monotonic()),
            timestamp=time.monotonic(),
        )
        assert entry.is_stale(max_age_seconds=30.0) is False

    def test_is_stale_old_entry(self) -> None:
        """Test that old entries are stale."""
        old_time = time.monotonic() - 60.0  # 60 seconds ago
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w", timestamp=old_time),
            timestamp=time.monotonic(),
        )
        assert entry.is_stale(max_age_seconds=30.0) is True

    def test_is_stale_boundary(self) -> None:
        """Test staleness at exact boundary."""
        boundary_time = time.monotonic() - 30.0  # Exactly 30 seconds ago
        entry = HealthGossipEntry(
            health=HealthPiggyback(node_id="n1", node_type="w", timestamp=boundary_time),
            timestamp=time.monotonic(),
        )
        # At boundary should be considered stale (age >= max_age)
        assert entry.is_stale(max_age_seconds=30.0) is True


# =============================================================================
# HealthGossipBuffer Tests
# =============================================================================


class TestHealthGossipBufferBasic:
    """Test basic HealthGossipBuffer operations."""

    def test_update_local_health(self) -> None:
        """Test updating local node's health state."""
        buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="local-node",
            node_type="worker",
            overload_state="healthy",
            capacity=4,
        )

        buffer.update_local_health(health)

        retrieved = buffer.get_health("local-node")
        assert retrieved is not None
        assert retrieved.node_id == "local-node"
        assert retrieved.capacity == 4

    def test_process_received_health_new_entry(self) -> None:
        """Test processing health from a remote node."""
        buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="remote-node",
            node_type="manager",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )

        accepted = buffer.process_received_health(health)
        assert accepted is True

        retrieved = buffer.get_health("remote-node")
        assert retrieved is not None
        assert retrieved.overload_state == "stressed"

    def test_process_received_health_older_rejected(self) -> None:
        """Test that older updates are rejected."""
        buffer = HealthGossipBuffer()

        # Add newer health first
        newer = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        buffer.process_received_health(newer)

        # Try to add older health
        older = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic() - 10.0,  # 10 seconds older
        )
        accepted = buffer.process_received_health(older)
        assert accepted is False

        # Should still have the newer state
        retrieved = buffer.get_health("node-1")
        assert retrieved is not None
        assert retrieved.overload_state == "stressed"

    def test_process_received_health_newer_accepted(self) -> None:
        """Test that newer updates replace older ones."""
        buffer = HealthGossipBuffer()

        # Add older health first
        older = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic() - 10.0,
        )
        buffer.process_received_health(older)

        # Add newer health
        newer = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        accepted = buffer.process_received_health(newer)
        assert accepted is True

        # Should have the newer state
        retrieved = buffer.get_health("node-1")
        assert retrieved is not None
        assert retrieved.overload_state == "stressed"


class TestHealthGossipBufferEncoding:
    """Test piggyback encoding and decoding."""

    def test_encode_piggyback_empty_buffer(self) -> None:
        """Test encoding from empty buffer returns empty bytes."""
        buffer = HealthGossipBuffer()
        encoded = buffer.encode_piggyback()
        assert encoded == b""

    def test_encode_piggyback_single_entry(self) -> None:
        """Test encoding a single entry."""
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

        assert encoded.startswith(b"#|h")
        assert b"node-1" in encoded

    def test_encode_decode_roundtrip(self) -> None:
        """Test encode/decode roundtrip preserves data."""
        buffer1 = HealthGossipBuffer()

        # Add several health entries
        for i in range(3):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                overload_state=["healthy", "busy", "stressed"][i],
                capacity=i + 1,
                timestamp=time.monotonic(),
            )
            buffer1.update_local_health(health)

        encoded = buffer1.encode_piggyback()

        # Decode into a new buffer
        buffer2 = HealthGossipBuffer()
        processed = buffer2.decode_and_process_piggyback(encoded)

        assert processed == 3

        # Verify all entries received
        for i in range(3):
            health = buffer2.get_health(f"node-{i}")
            assert health is not None
            assert health.capacity == i + 1

    def test_encode_respects_max_count(self) -> None:
        """Test that encoding respects max_count parameter."""
        buffer = HealthGossipBuffer()

        # Add 10 entries
        for i in range(10):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Encode with max 3
        encoded = buffer.encode_piggyback(max_count=3)

        # Decode and verify only 3 entries
        buffer2 = HealthGossipBuffer()
        processed = buffer2.decode_and_process_piggyback(encoded)
        assert processed <= 3

    def test_encode_respects_max_size(self) -> None:
        """Test that encoding respects max_size parameter."""
        buffer = HealthGossipBuffer()

        # Add many entries
        for i in range(50):
            health = HealthPiggyback(
                node_id=f"node-with-long-identifier-{i}",
                node_type="worker",
                overload_state="overloaded",
                capacity=1000,
                throughput=9999.99,
                expected_throughput=9999.99,
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Encode with small size limit
        encoded = buffer.encode_piggyback(max_size=200)

        assert len(encoded) <= 200

    def test_is_health_piggyback(self) -> None:
        """Test health piggyback detection."""
        assert HealthGossipBuffer.is_health_piggyback(b"#|hdata") is True
        assert HealthGossipBuffer.is_health_piggyback(b"#|h") is True
        assert HealthGossipBuffer.is_health_piggyback(b"|regular|gossip") is False
        assert HealthGossipBuffer.is_health_piggyback(b"") is False
        assert HealthGossipBuffer.is_health_piggyback(b"#|") is False


class TestHealthGossipBufferPrioritization:
    """Test priority-based broadcast selection."""

    def test_overloaded_prioritized_over_healthy(self) -> None:
        """Test that overloaded nodes are broadcast first."""
        buffer = HealthGossipBuffer()

        # Add healthy node first
        healthy = HealthPiggyback(
            node_id="healthy-node",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(healthy)

        # Add overloaded node second
        overloaded = HealthPiggyback(
            node_id="overloaded-node",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(overloaded)

        # Get entries for piggybacking
        entries = buffer.get_entries_to_piggyback(max_count=1)

        assert len(entries) == 1
        assert entries[0].health.node_id == "overloaded-node"

    def test_severity_order_stressed_then_busy_then_healthy(self) -> None:
        """Test full severity ordering."""
        buffer = HealthGossipBuffer()

        # Add in reverse order (healthy first, overloaded last)
        for state in ["healthy", "busy", "stressed", "overloaded"]:
            health = HealthPiggyback(
                node_id=f"{state}-node",
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Get all entries ordered by priority
        entries = buffer.get_entries_to_piggyback(max_count=4)

        assert len(entries) == 4
        assert entries[0].health.overload_state == "overloaded"
        assert entries[1].health.overload_state == "stressed"
        assert entries[2].health.overload_state == "busy"
        assert entries[3].health.overload_state == "healthy"

    def test_same_severity_lower_broadcast_count_first(self) -> None:
        """Test that within same severity, lower broadcast count is prioritized."""
        buffer = HealthGossipBuffer()

        # Add two stressed nodes
        for i in range(2):
            health = HealthPiggyback(
                node_id=f"stressed-{i}",
                node_type="worker",
                overload_state="stressed",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Manually set different broadcast counts
        buffer._entries["stressed-0"].broadcast_count = 3
        buffer._entries["stressed-1"].broadcast_count = 1

        entries = buffer.get_entries_to_piggyback(max_count=2)

        # stressed-1 should come first (lower broadcast count)
        assert entries[0].health.node_id == "stressed-1"
        assert entries[1].health.node_id == "stressed-0"


class TestHealthGossipBufferNegativePaths:
    """Test failure paths and error handling."""

    def test_decode_non_health_piggyback(self) -> None:
        """Test decoding data that's not health piggyback."""
        buffer = HealthGossipBuffer()

        # Regular membership gossip format
        processed = buffer.decode_and_process_piggyback(b"|join:1:127.0.0.1:8000")
        assert processed == 0

    def test_decode_empty_health_piggyback(self) -> None:
        """Test decoding empty health piggyback."""
        buffer = HealthGossipBuffer()
        processed = buffer.decode_and_process_piggyback(b"#|h")
        assert processed == 0

    def test_decode_malformed_entries(self) -> None:
        """Test decoding with some malformed entries."""
        buffer = HealthGossipBuffer()

        # Mix of valid and invalid entries (using ; as entry separator)
        # Format: #|h + entries separated by ;
        data = (
            b"#|hnode-1|worker|healthy|1|4|10.0|15.0|" + str(time.monotonic()).encode() +
            b";invalid_entry" +
            b";node-2|worker|busy|1|8|20.0|25.0|" + str(time.monotonic()).encode()
        )
        processed = buffer.decode_and_process_piggyback(data)

        # Should process valid entries, skip invalid
        assert processed >= 1
        assert buffer.get_health("node-1") is not None or buffer.get_health("node-2") is not None

    def test_decode_corrupted_utf8(self) -> None:
        """Test handling corrupted UTF-8 in piggyback."""
        buffer = HealthGossipBuffer()
        data = b"#|h\xff\xfe|worker|healthy|1|4|10.0|15.0|12345.0"
        processed = buffer.decode_and_process_piggyback(data)
        # Should handle gracefully without crashing
        assert processed == 0
        assert buffer._malformed_count == 1


class TestHealthGossipBufferCapacity:
    """Test capacity limits and eviction."""

    def test_max_entries_eviction(self) -> None:
        """Test that oldest/least important entries are evicted at capacity."""
        config = HealthGossipBufferConfig(max_entries=5)
        buffer = HealthGossipBuffer(config=config)

        # Add 10 entries (5 over limit)
        for i in range(10):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                overload_state="healthy",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Should have at most max_entries
        assert len(buffer._entries) <= 5

    def test_overloaded_retained_during_eviction(self) -> None:
        """Test that overloaded entries are retained during eviction."""
        config = HealthGossipBufferConfig(max_entries=3)
        buffer = HealthGossipBuffer(config=config)

        # Add one overloaded
        overloaded = HealthPiggyback(
            node_id="overloaded",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(overloaded)

        # Add many healthy (should trigger eviction)
        for i in range(5):
            health = HealthPiggyback(
                node_id=f"healthy-{i}",
                node_type="worker",
                overload_state="healthy",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Overloaded should be retained
        assert buffer.get_health("overloaded") is not None

    def test_cleanup_stale_entries(self) -> None:
        """Test stale entry cleanup."""
        config = HealthGossipBufferConfig(stale_age_seconds=1.0)
        buffer = HealthGossipBuffer(config=config)

        # Add stale entry
        stale = HealthPiggyback(
            node_id="stale-node",
            node_type="worker",
            timestamp=time.monotonic() - 60.0,  # Very old
        )
        buffer.update_local_health(stale)

        # Add fresh entry
        fresh = HealthPiggyback(
            node_id="fresh-node",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(fresh)

        # Run cleanup
        removed = buffer.cleanup_stale()

        assert removed == 1
        assert buffer.get_health("stale-node") is None
        assert buffer.get_health("fresh-node") is not None

    def test_cleanup_broadcast_complete(self) -> None:
        """Test cleanup of entries that have been fully broadcast."""
        buffer = HealthGossipBuffer()

        health = HealthPiggyback(
            node_id="broadcast-done",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        # Mark as fully broadcast
        buffer._entries["broadcast-done"].broadcast_count = 100
        buffer._entries["broadcast-done"].max_broadcasts = 5

        removed = buffer.cleanup_broadcast_complete()

        assert removed == 1
        assert buffer.get_health("broadcast-done") is None


class TestHealthGossipBufferCallback:
    """Test health update callback integration."""

    def test_callback_invoked_on_received_health(self) -> None:
        """Test that callback is invoked when health is received."""
        buffer = HealthGossipBuffer()
        callback = MagicMock()
        buffer.set_health_update_callback(callback)

        health = HealthPiggyback(
            node_id="remote-node",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        buffer.process_received_health(health)

        callback.assert_called_once()
        called_health = callback.call_args[0][0]
        assert called_health.node_id == "remote-node"
        assert called_health.overload_state == "stressed"

    def test_callback_not_invoked_for_rejected_update(self) -> None:
        """Test that callback is not invoked for rejected updates."""
        buffer = HealthGossipBuffer()
        callback = MagicMock()
        buffer.set_health_update_callback(callback)

        # Add newer health first
        newer = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.process_received_health(newer)
        callback.reset_mock()

        # Try to add older (should be rejected)
        older = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic() - 10.0,
        )
        buffer.process_received_health(older)

        callback.assert_not_called()

    def test_callback_exception_does_not_affect_gossip(self) -> None:
        """Test that callback exceptions don't break gossip processing."""
        buffer = HealthGossipBuffer()
        callback = MagicMock(side_effect=Exception("Callback error"))
        buffer.set_health_update_callback(callback)

        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=time.monotonic(),
        )

        # Should not raise despite callback error
        accepted = buffer.process_received_health(health)
        assert accepted is True
        assert buffer.get_health("node-1") is not None


class TestHealthGossipBufferQueries:
    """Test query methods for health state."""

    def test_get_overloaded_nodes(self) -> None:
        """Test getting list of overloaded nodes."""
        buffer = HealthGossipBuffer()

        # Add mix of nodes
        for state in ["healthy", "busy", "overloaded", "stressed", "overloaded"]:
            health = HealthPiggyback(
                node_id=f"node-{state}-{time.monotonic()}",
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        overloaded = buffer.get_overloaded_nodes()
        assert len(overloaded) == 2

    def test_get_stressed_nodes(self) -> None:
        """Test getting list of stressed nodes (includes overloaded)."""
        buffer = HealthGossipBuffer()

        for state in ["healthy", "busy", "stressed", "overloaded"]:
            health = HealthPiggyback(
                node_id=f"node-{state}",
                node_type="worker",
                overload_state=state,
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        stressed = buffer.get_stressed_nodes()
        assert len(stressed) == 2  # stressed + overloaded
        assert "node-stressed" in stressed
        assert "node-overloaded" in stressed

    def test_get_nodes_not_accepting_work(self) -> None:
        """Test getting nodes that are not accepting work."""
        buffer = HealthGossipBuffer()

        # Add accepting node
        accepting = HealthPiggyback(
            node_id="accepting",
            node_type="worker",
            accepting_work=True,
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(accepting)

        # Add not accepting node
        not_accepting = HealthPiggyback(
            node_id="not-accepting",
            node_type="worker",
            accepting_work=False,
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(not_accepting)

        result = buffer.get_nodes_not_accepting_work()
        assert result == ["not-accepting"]


class TestHealthGossipBufferConcurrency:
    """Test concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_updates(self) -> None:
        """Test concurrent health updates from multiple nodes."""
        buffer = HealthGossipBuffer()

        async def update_node(node_idx: int) -> None:
            for update_num in range(10):
                health = HealthPiggyback(
                    node_id=f"node-{node_idx}",
                    node_type="worker",
                    overload_state=["healthy", "busy", "stressed"][update_num % 3],
                    timestamp=time.monotonic(),
                )
                buffer.update_local_health(health)
                await asyncio.sleep(0.001)  # Small delay

        # Run 5 concurrent updaters
        await asyncio.gather(*[update_node(i) for i in range(5)])

        # All nodes should have entries
        for i in range(5):
            assert buffer.get_health(f"node-{i}") is not None

    @pytest.mark.asyncio
    async def test_concurrent_encode_decode(self) -> None:
        """Test concurrent encoding and decoding."""
        buffer1 = HealthGossipBuffer()
        buffer2 = HealthGossipBuffer()

        # Populate buffer1
        for i in range(20):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer1.update_local_health(health)

        async def encode_and_send() -> bytes:
            await asyncio.sleep(0.001)
            return buffer1.encode_piggyback()

        async def receive_and_decode(data: bytes) -> int:
            await asyncio.sleep(0.001)
            return buffer2.decode_and_process_piggyback(data)

        # Run concurrent encode/decode cycles
        for _ in range(10):
            encoded = await encode_and_send()
            if encoded:
                await receive_and_decode(encoded)

        # Should have processed some entries
        assert len(buffer2._entries) > 0


class TestHealthGossipBufferEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_empty_node_id(self) -> None:
        """Test handling empty node ID."""
        buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        # Should be stored (empty string is valid key)
        assert buffer.get_health("") is not None

    def test_very_long_node_id(self) -> None:
        """Test handling very long node ID."""
        buffer = HealthGossipBuffer()
        long_id = "n" * 500  # 500 character node ID
        health = HealthPiggyback(
            node_id=long_id,
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        assert buffer.get_health(long_id) is not None

    def test_negative_capacity(self) -> None:
        """Test handling negative capacity value."""
        buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            capacity=-5,  # Negative (shouldn't happen but test resilience)
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        retrieved = buffer.get_health("node-1")
        assert retrieved is not None
        assert retrieved.capacity == -5

    def test_zero_timestamp(self) -> None:
        """Test handling zero timestamp."""
        buffer = HealthGossipBuffer()
        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=0.0,
        )
        buffer.update_local_health(health)

        # Should be marked very stale
        assert buffer._entries["node-1"].is_stale(max_age_seconds=1.0) is True

    def test_future_timestamp(self) -> None:
        """Test handling timestamp in the future."""
        buffer = HealthGossipBuffer()
        future = time.monotonic() + 3600  # 1 hour in future
        health = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            timestamp=future,
        )
        buffer.update_local_health(health)

        # Should not be stale
        assert buffer._entries["node-1"].is_stale(max_age_seconds=30.0) is False

    def test_clear_buffer(self) -> None:
        """Test clearing all entries."""
        buffer = HealthGossipBuffer()

        for i in range(10):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        assert len(buffer._entries) == 10

        buffer.clear()

        assert len(buffer._entries) == 0

    def test_remove_specific_node(self) -> None:
        """Test removing a specific node."""
        buffer = HealthGossipBuffer()

        health = HealthPiggyback(
            node_id="to-remove",
            node_type="worker",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(health)

        assert buffer.get_health("to-remove") is not None

        removed = buffer.remove_node("to-remove")
        assert removed is True
        assert buffer.get_health("to-remove") is None

        # Removing non-existent node
        removed = buffer.remove_node("not-exists")
        assert removed is False


class TestHealthGossipBufferStatistics:
    """Test statistics tracking."""

    def test_stats_tracking(self) -> None:
        """Test that statistics are properly tracked."""
        config = HealthGossipBufferConfig(max_entries=3)
        buffer = HealthGossipBuffer(config=config)

        # Add entries (will trigger eviction)
        for i in range(5):
            health = HealthPiggyback(
                node_id=f"node-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.update_local_health(health)

        # Process some received health
        for i in range(3):
            health = HealthPiggyback(
                node_id=f"remote-{i}",
                node_type="worker",
                timestamp=time.monotonic(),
            )
            buffer.process_received_health(health)

        stats = buffer.get_stats()

        assert "pending_entries" in stats
        assert "total_updates" in stats
        assert "evicted_count" in stats
        assert stats["total_updates"] == 3  # From process_received_health

    def test_malformed_count_tracking(self) -> None:
        """Test tracking of malformed entries."""
        buffer = HealthGossipBuffer()

        # Send malformed data (using ; as entry separator)
        buffer.decode_and_process_piggyback(b"#|hinvalid1;invalid2;invalid3")

        stats = buffer.get_stats()
        assert stats["malformed_count"] >= 3


class TestHealthGossipBufferBroadcastCountReset:
    """Test broadcast count reset on state changes."""

    def test_broadcast_count_reset_on_state_change(self) -> None:
        """Test that broadcast count resets when overload state changes."""
        buffer = HealthGossipBuffer()

        # Add healthy node
        healthy = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(healthy)

        # Mark as broadcast several times
        buffer._entries["node-1"].broadcast_count = 3

        # Update to stressed (state change)
        stressed = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="stressed",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(stressed)

        # Broadcast count should be reset
        assert buffer._entries["node-1"].broadcast_count == 0

    def test_broadcast_count_preserved_no_state_change(self) -> None:
        """Test that broadcast count preserved when state unchanged."""
        buffer = HealthGossipBuffer()

        # Add healthy node
        healthy1 = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(healthy1)
        buffer._entries["node-1"].broadcast_count = 3

        # Update with same state
        healthy2 = HealthPiggyback(
            node_id="node-1",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic() + 1,
        )
        buffer.update_local_health(healthy2)

        # Broadcast count should be preserved
        assert buffer._entries["node-1"].broadcast_count == 3


class TestHealthGossipBufferMaxBroadcasts:
    """Test max broadcasts based on severity."""

    def test_overloaded_gets_more_broadcasts(self) -> None:
        """Test that overloaded nodes get more broadcast attempts."""
        config = HealthGossipBufferConfig(
            min_broadcasts_healthy=3,
            min_broadcasts_overloaded=8,
        )
        buffer = HealthGossipBuffer(config=config)

        healthy = HealthPiggyback(
            node_id="healthy-node",
            node_type="worker",
            overload_state="healthy",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(healthy)

        overloaded = HealthPiggyback(
            node_id="overloaded-node",
            node_type="worker",
            overload_state="overloaded",
            timestamp=time.monotonic(),
        )
        buffer.update_local_health(overloaded)

        assert buffer._entries["healthy-node"].max_broadcasts == 3
        assert buffer._entries["overloaded-node"].max_broadcasts == 8
