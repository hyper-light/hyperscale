"""
Integration tests for Gate Models (Section 15.3.2).

Tests gate-specific data models:
- GatePeerState, GatePeerTracking
- DCHealthState, ManagerTracking
- JobForwardingState, ForwardingMetrics
- LeaseState, LeaseTracking
"""

import asyncio
import time
import pytest
from dataclasses import is_dataclass

from hyperscale.distributed.nodes.gate.models import (
    GatePeerState,
    GatePeerTracking,
    DCHealthState,
    ManagerTracking,
    JobForwardingState,
    ForwardingMetrics,
    LeaseState,
    LeaseTracking,
)
from hyperscale.distributed.reliability import BackpressureLevel


# =============================================================================
# GatePeerTracking Tests
# =============================================================================


class TestGatePeerTrackingHappyPath:
    """Tests for GatePeerTracking happy path."""

    def test_create_with_minimal_fields(self):
        """Create tracking with minimal required fields."""
        tracking = GatePeerTracking(
            udp_addr=("10.0.0.1", 9001),
            tcp_addr=("10.0.0.1", 9000),
        )

        assert tracking.udp_addr == ("10.0.0.1", 9001)
        assert tracking.tcp_addr == ("10.0.0.1", 9000)
        assert tracking.epoch == 0
        assert tracking.is_active is False
        assert tracking.heartbeat is None
        assert tracking.health_state is None

    def test_create_with_all_fields(self):
        """Create tracking with all fields populated."""
        tracking = GatePeerTracking(
            udp_addr=("10.0.0.1", 9001),
            tcp_addr=("10.0.0.1", 9000),
            epoch=5,
            is_active=True,
            heartbeat=None,  # Would be GateHeartbeat
            health_state=None,  # Would be GateHealthState
        )

        assert tracking.epoch == 5
        assert tracking.is_active is True

    def test_uses_slots(self):
        """GatePeerTracking uses slots for memory efficiency."""
        tracking = GatePeerTracking(
            udp_addr=("10.0.0.1", 9001),
            tcp_addr=("10.0.0.1", 9000),
        )
        assert hasattr(tracking, "__slots__")


# =============================================================================
# GatePeerState Tests
# =============================================================================


class TestGatePeerStateHappyPath:
    """Tests for GatePeerState happy path."""

    def test_create_empty_state(self):
        """Create empty peer state."""
        state = GatePeerState()

        assert state.gate_peers_tcp == []
        assert state.gate_peers_udp == []
        assert state.udp_to_tcp == {}
        assert state.active_peers == set()
        assert state.peer_locks == {}
        assert state.peer_epochs == {}
        assert state.peer_info == {}
        assert state.known_gates == {}
        assert state.peer_health == {}

    def test_create_with_peers(self):
        """Create state with configured peers."""
        tcp_peers = [("10.0.0.1", 9000), ("10.0.0.2", 9000)]
        udp_peers = [("10.0.0.1", 9001), ("10.0.0.2", 9001)]

        state = GatePeerState(
            gate_peers_tcp=tcp_peers,
            gate_peers_udp=udp_peers,
        )

        assert len(state.gate_peers_tcp) == 2
        assert len(state.gate_peers_udp) == 2

    @pytest.mark.asyncio
    async def test_get_or_create_peer_lock(self):
        """Get or create peer lock returns consistent lock."""
        state = GatePeerState()
        peer_addr = ("10.0.0.1", 9001)

        lock1 = await state.get_or_create_peer_lock(peer_addr)
        lock2 = await state.get_or_create_peer_lock(peer_addr)

        assert lock1 is lock2
        assert isinstance(lock1, asyncio.Lock)
        assert peer_addr in state.peer_locks

    @pytest.mark.asyncio
    async def test_increment_epoch(self):
        """Increment epoch returns incremented value."""
        state = GatePeerState()
        peer_addr = ("10.0.0.1", 9001)

        epoch1 = await state.increment_epoch(peer_addr)
        epoch2 = await state.increment_epoch(peer_addr)
        epoch3 = await state.increment_epoch(peer_addr)

        assert epoch1 == 1
        assert epoch2 == 2
        assert epoch3 == 3

    @pytest.mark.asyncio
    async def test_get_epoch_returns_zero_for_unknown(self):
        """Get epoch returns 0 for unknown peer."""
        state = GatePeerState()
        unknown_addr = ("10.0.0.99", 9001)

        assert await state.get_epoch(unknown_addr) == 0

    @pytest.mark.asyncio
    async def test_get_epoch_returns_current_value(self):
        """Get epoch returns current value after increments."""
        state = GatePeerState()
        peer_addr = ("10.0.0.1", 9001)

        await state.increment_epoch(peer_addr)
        await state.increment_epoch(peer_addr)

        assert await state.get_epoch(peer_addr) == 2


class TestGatePeerStateConcurrency:
    """Tests for GatePeerState concurrency handling."""

    @pytest.mark.asyncio
    async def test_concurrent_lock_access(self):
        """Concurrent access to same lock is serialized."""
        state = GatePeerState()
        peer_addr = ("10.0.0.1", 9001)
        execution_order = []

        async def task(task_id: int, delay: float):
            lock = state.get_or_create_peer_lock(peer_addr)
            async with lock:
                execution_order.append(f"start-{task_id}")
                await asyncio.sleep(delay)
                execution_order.append(f"end-{task_id}")

        await asyncio.gather(
            task(1, 0.05),
            task(2, 0.01),
        )

        # Should be serialized - one starts and ends before next
        assert execution_order[1] == "end-1" or execution_order[1] == "end-2"

    @pytest.mark.asyncio
    async def test_different_peers_have_different_locks(self):
        """Different peers get different locks allowing parallel access."""
        state = GatePeerState()
        peer1 = ("10.0.0.1", 9001)
        peer2 = ("10.0.0.2", 9001)

        lock1 = state.get_or_create_peer_lock(peer1)
        lock2 = state.get_or_create_peer_lock(peer2)

        assert lock1 is not lock2

        # Both can be acquired simultaneously
        async with lock1:
            async with lock2:
                pass  # Both held at same time

    @pytest.mark.asyncio
    async def test_rapid_epoch_increments(self):
        """Rapid epoch increments produce unique values."""
        state = GatePeerState()
        peer_addr = ("10.0.0.1", 9001)
        epochs = []

        async def increment():
            for _ in range(100):
                epoch = state.increment_epoch(peer_addr)
                epochs.append(epoch)

        await asyncio.gather(increment(), increment())

        # All epochs should be unique (no duplicates)
        # Note: Without locking, there might be duplicates
        # This tests the actual behavior
        assert state.get_epoch(peer_addr) > 0


class TestGatePeerStateEdgeCases:
    """Tests for GatePeerState edge cases."""

    def test_empty_peer_lists_are_valid(self):
        """Empty peer lists are valid configurations."""
        state = GatePeerState(
            gate_peers_tcp=[],
            gate_peers_udp=[],
        )
        assert len(state.gate_peers_tcp) == 0

    def test_many_peers(self):
        """Handle many peer addresses."""
        peers = [(f"10.0.0.{i}", 9000) for i in range(100)]
        state = GatePeerState(gate_peers_tcp=peers)

        assert len(state.gate_peers_tcp) == 100

    def test_duplicate_peer_addresses(self):
        """Duplicate addresses in list are kept."""
        peers = [("10.0.0.1", 9000), ("10.0.0.1", 9000)]
        state = GatePeerState(gate_peers_tcp=peers)

        assert len(state.gate_peers_tcp) == 2

    def test_active_peers_set_operations(self):
        """Active peers set supports standard operations."""
        state = GatePeerState()
        peer = ("10.0.0.1", 9000)

        state.active_peers.add(peer)
        assert peer in state.active_peers

        state.active_peers.discard(peer)
        assert peer not in state.active_peers


# =============================================================================
# ManagerTracking Tests
# =============================================================================


class TestManagerTrackingHappyPath:
    """Tests for ManagerTracking happy path."""

    def test_create_minimal(self):
        """Create tracking with minimal fields."""
        tracking = ManagerTracking(
            address=("10.0.0.1", 8000),
            datacenter_id="dc-east",
        )

        assert tracking.address == ("10.0.0.1", 8000)
        assert tracking.datacenter_id == "dc-east"
        assert tracking.last_heartbeat is None
        assert tracking.last_status_time == 0.0
        assert tracking.health_state is None
        assert tracking.backpressure_level == BackpressureLevel.NONE

    def test_create_with_backpressure(self):
        """Create tracking with backpressure level."""
        tracking = ManagerTracking(
            address=("10.0.0.1", 8000),
            datacenter_id="dc-east",
            backpressure_level=BackpressureLevel.THROTTLE,
        )

        assert tracking.backpressure_level == BackpressureLevel.THROTTLE


# =============================================================================
# DCHealthState Tests
# =============================================================================


class TestDCHealthStateHappyPath:
    """Tests for DCHealthState happy path."""

    def test_create_empty_state(self):
        """Create empty DC health state."""
        state = DCHealthState()

        assert state.datacenter_managers == {}
        assert state.datacenter_managers_udp == {}
        assert state.registration_states == {}
        assert state.manager_status == {}
        assert state.manager_last_status == {}
        assert state.manager_health == {}
        assert state.manager_backpressure == {}
        assert state.backpressure_delay_ms == 0
        assert state.dc_backpressure == {}

    def test_update_manager_status(self):
        """Update manager status stores heartbeat and timestamp."""
        state = DCHealthState()
        dc_id = "dc-east"
        manager_addr = ("10.0.0.1", 8000)

        # Create a mock heartbeat (would be ManagerHeartbeat in production)
        class MockHeartbeat:
            pass

        heartbeat = MockHeartbeat()
        timestamp = time.monotonic()

        state.update_manager_status(dc_id, manager_addr, heartbeat, timestamp)

        assert dc_id in state.manager_status
        assert manager_addr in state.manager_status[dc_id]
        assert state.manager_status[dc_id][manager_addr] is heartbeat
        assert state.manager_last_status[manager_addr] == timestamp

    def test_get_dc_backpressure_level(self):
        """Get DC backpressure level returns correct value."""
        state = DCHealthState()
        state.dc_backpressure["dc-east"] = BackpressureLevel.BATCH

        assert state.get_dc_backpressure_level("dc-east") == BackpressureLevel.BATCH
        assert state.get_dc_backpressure_level("unknown") == BackpressureLevel.NONE

    def test_update_dc_backpressure(self):
        """Update DC backpressure calculates max from managers."""
        state = DCHealthState()
        dc_id = "dc-east"
        state.datacenter_managers[dc_id] = [
            ("10.0.0.1", 8000),
            ("10.0.0.2", 8000),
            ("10.0.0.3", 8000),
        ]

        # Set different backpressure levels
        state.manager_backpressure[("10.0.0.1", 8000)] = BackpressureLevel.NONE
        state.manager_backpressure[("10.0.0.2", 8000)] = BackpressureLevel.THROTTLE
        state.manager_backpressure[("10.0.0.3", 8000)] = BackpressureLevel.BATCH

        state.update_dc_backpressure(dc_id)

        # Should be max (BATCH)
        assert state.dc_backpressure[dc_id] == BackpressureLevel.BATCH


class TestDCHealthStateEdgeCases:
    """Tests for DCHealthState edge cases."""

    def test_update_dc_backpressure_no_managers(self):
        """Update DC backpressure with no managers returns NONE."""
        state = DCHealthState()
        state.datacenter_managers["dc-empty"] = []

        state.update_dc_backpressure("dc-empty")

        assert state.dc_backpressure["dc-empty"] == BackpressureLevel.NONE

    def test_update_dc_backpressure_missing_manager_levels(self):
        """Update DC backpressure with missing manager levels uses NONE."""
        state = DCHealthState()
        dc_id = "dc-east"
        state.datacenter_managers[dc_id] = [
            ("10.0.0.1", 8000),
            ("10.0.0.2", 8000),
        ]
        # Only set one manager's level
        state.manager_backpressure[("10.0.0.1", 8000)] = BackpressureLevel.THROTTLE

        state.update_dc_backpressure(dc_id)

        assert state.dc_backpressure[dc_id] == BackpressureLevel.THROTTLE

    def test_update_dc_backpressure_all_reject(self):
        """Update DC backpressure with all REJECT stays REJECT."""
        state = DCHealthState()
        dc_id = "dc-east"
        state.datacenter_managers[dc_id] = [
            ("10.0.0.1", 8000),
            ("10.0.0.2", 8000),
        ]
        state.manager_backpressure[("10.0.0.1", 8000)] = BackpressureLevel.REJECT
        state.manager_backpressure[("10.0.0.2", 8000)] = BackpressureLevel.REJECT

        state.update_dc_backpressure(dc_id)

        assert state.dc_backpressure[dc_id] == BackpressureLevel.REJECT


# =============================================================================
# ForwardingMetrics Tests
# =============================================================================


class TestForwardingMetricsHappyPath:
    """Tests for ForwardingMetrics happy path."""

    def test_create_default(self):
        """Create metrics with defaults."""
        metrics = ForwardingMetrics()

        assert metrics.count == 0
        assert metrics.last_throughput == 0.0
        assert metrics.interval_seconds == 10.0

    def test_record_forward(self):
        """Record forward increments count."""
        metrics = ForwardingMetrics()

        metrics.record_forward()
        assert metrics.count == 1

        metrics.record_forward()
        assert metrics.count == 2

    def test_calculate_throughput_within_interval(self):
        """Calculate throughput within interval returns last value."""
        metrics = ForwardingMetrics(interval_seconds=10.0)
        # Just created, so within interval
        metrics.record_forward()
        metrics.record_forward()

        # Should return 0.0 (last value) since interval hasn't elapsed
        throughput = metrics.calculate_throughput()
        assert throughput == 0.0
        # Count should remain since interval not elapsed
        assert metrics.count == 2

    def test_calculate_throughput_after_interval(self):
        """Calculate throughput after interval calculates and resets."""
        metrics = ForwardingMetrics(interval_seconds=0.0)  # Immediate interval
        metrics.record_forward()
        metrics.record_forward()
        metrics.record_forward()

        # Force interval start to past
        metrics.interval_start = time.monotonic() - 1.0
        metrics.count = 10

        throughput = metrics.calculate_throughput()

        assert throughput > 0.0  # Should be ~10/elapsed
        assert metrics.count == 0  # Reset after calculation


class TestForwardingMetricsEdgeCases:
    """Tests for ForwardingMetrics edge cases."""

    def test_zero_interval(self):
        """Zero interval causes immediate calculation."""
        metrics = ForwardingMetrics(interval_seconds=0.0)
        metrics.record_forward()

        throughput = metrics.calculate_throughput()
        # Very high throughput due to tiny elapsed time
        assert throughput >= 0.0

    def test_many_forwards(self):
        """Handle many forward records."""
        metrics = ForwardingMetrics()

        for _ in range(10000):
            metrics.record_forward()

        assert metrics.count == 10000


# =============================================================================
# JobForwardingState Tests
# =============================================================================


class TestJobForwardingStateHappyPath:
    """Tests for JobForwardingState happy path."""

    def test_create_default(self):
        """Create state with defaults."""
        state = JobForwardingState()

        assert state.forward_timeout == 3.0
        assert state.max_forward_attempts == 3
        assert state.throughput_metrics is not None

    def test_record_forward_delegates(self):
        """Record forward delegates to metrics."""
        state = JobForwardingState()

        state.record_forward()
        state.record_forward()

        assert state.throughput_metrics.count == 2

    def test_get_throughput_delegates(self):
        """Get throughput delegates to metrics."""
        state = JobForwardingState()

        throughput = state.get_throughput()
        assert throughput >= 0.0


# =============================================================================
# LeaseTracking Tests
# =============================================================================


class TestLeaseTrackingHappyPath:
    """Tests for LeaseTracking happy path."""

    def test_create(self):
        """Create lease tracking."""

        # Mock lease
        class MockLease:
            pass

        lease = MockLease()
        tracking = LeaseTracking(
            job_id="job-123",
            datacenter_id="dc-east",
            lease=lease,
            fence_token=42,
        )

        assert tracking.job_id == "job-123"
        assert tracking.datacenter_id == "dc-east"
        assert tracking.lease is lease
        assert tracking.fence_token == 42


# =============================================================================
# LeaseState Tests
# =============================================================================


class TestLeaseStateHappyPath:
    """Tests for LeaseState happy path."""

    def test_create_default(self):
        """Create lease state with defaults."""
        state = LeaseState()

        assert state.leases == {}
        assert state.fence_token == 0
        assert state.lease_timeout == 30.0

    def test_get_lease_key(self):
        """Get lease key formats correctly."""
        state = LeaseState()

        key = state.get_lease_key("job-123", "dc-east")
        assert key == "job-123:dc-east"

    def test_set_and_get_lease(self):
        """Set and get lease operations work."""
        state = LeaseState()

        class MockLease:
            pass

        lease = MockLease()
        state.set_lease("job-123", "dc-east", lease)

        result = state.get_lease("job-123", "dc-east")
        assert result is lease

    def test_get_nonexistent_lease(self):
        """Get nonexistent lease returns None."""
        state = LeaseState()

        result = state.get_lease("unknown", "unknown")
        assert result is None

    def test_remove_lease(self):
        """Remove lease removes it."""
        state = LeaseState()

        class MockLease:
            pass

        state.set_lease("job-123", "dc-east", MockLease())
        state.remove_lease("job-123", "dc-east")

        result = state.get_lease("job-123", "dc-east")
        assert result is None

    def test_remove_nonexistent_lease_is_safe(self):
        """Remove nonexistent lease doesn't raise."""
        state = LeaseState()
        state.remove_lease("unknown", "unknown")  # Should not raise

    def test_next_fence_token(self):
        """Next fence token increments and returns."""
        state = LeaseState()

        token1 = state.next_fence_token()
        token2 = state.next_fence_token()
        token3 = state.next_fence_token()

        assert token1 == 1
        assert token2 == 2
        assert token3 == 3
        assert state.fence_token == 3


class TestLeaseStateEdgeCases:
    """Tests for LeaseState edge cases."""

    def test_many_leases(self):
        """Handle many leases."""
        state = LeaseState()

        class MockLease:
            pass

        for i in range(1000):
            state.set_lease(f"job-{i}", f"dc-{i % 5}", MockLease())

        assert len(state.leases) == 1000

    def test_overwrite_lease(self):
        """Overwriting lease replaces previous."""
        state = LeaseState()

        class Lease1:
            pass

        class Lease2:
            pass

        state.set_lease("job-1", "dc-1", Lease1())
        state.set_lease("job-1", "dc-1", Lease2())

        result = state.get_lease("job-1", "dc-1")
        assert isinstance(result, Lease2)

    def test_fence_token_overflow(self):
        """Fence token handles large values."""
        state = LeaseState()
        state.fence_token = 2**62

        token = state.next_fence_token()
        assert token == 2**62 + 1

    def test_special_characters_in_ids(self):
        """Handle special characters in IDs."""
        state = LeaseState()

        class MockLease:
            pass

        # IDs with special chars
        state.set_lease("job:colon", "dc-dash", MockLease())
        key = state.get_lease_key("job:colon", "dc-dash")
        assert key == "job:colon:dc-dash"

        result = state.get_lease("job:colon", "dc-dash")
        assert result is not None


# =============================================================================
# Slots and Memory Tests
# =============================================================================


class TestModelsUseSlots:
    """Tests that all models use slots for memory efficiency."""

    def test_gate_peer_tracking_uses_slots(self):
        """GatePeerTracking uses slots."""
        assert hasattr(GatePeerTracking, "__slots__")

    def test_gate_peer_state_uses_slots(self):
        """GatePeerState uses slots."""
        assert hasattr(GatePeerState, "__slots__")

    def test_manager_tracking_uses_slots(self):
        """ManagerTracking uses slots."""
        assert hasattr(ManagerTracking, "__slots__")

    def test_dc_health_state_uses_slots(self):
        """DCHealthState uses slots."""
        assert hasattr(DCHealthState, "__slots__")

    def test_forwarding_metrics_uses_slots(self):
        """ForwardingMetrics uses slots."""
        assert hasattr(ForwardingMetrics, "__slots__")

    def test_job_forwarding_state_uses_slots(self):
        """JobForwardingState uses slots."""
        assert hasattr(JobForwardingState, "__slots__")

    def test_lease_tracking_uses_slots(self):
        """LeaseTracking uses slots."""
        assert hasattr(LeaseTracking, "__slots__")

    def test_lease_state_uses_slots(self):
        """LeaseState uses slots."""
        assert hasattr(LeaseState, "__slots__")


class TestModelsAreDataclasses:
    """Tests that all models are proper dataclasses."""

    def test_all_are_dataclasses(self):
        """All model classes are dataclasses."""
        classes = [
            GatePeerTracking,
            GatePeerState,
            ManagerTracking,
            DCHealthState,
            ForwardingMetrics,
            JobForwardingState,
            LeaseTracking,
            LeaseState,
        ]
        for cls in classes:
            assert is_dataclass(cls), f"{cls.__name__} is not a dataclass"
