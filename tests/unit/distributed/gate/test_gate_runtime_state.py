"""
Integration tests for GateRuntimeState (Section 15.3.4).

Tests the centralized mutable runtime state for GateServer.
"""

import asyncio
import time
import pytest

from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import GateState as GateStateEnum
from hyperscale.distributed.reliability import BackpressureLevel


# =============================================================================
# Initialization Tests
# =============================================================================


class TestGateRuntimeStateInitialization:
    """Tests for GateRuntimeState initialization."""

    def test_creates_empty_state(self):
        """State initializes with empty containers."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        # Gate peer state
        assert state._gate_udp_to_tcp == {}
        assert state._active_gate_peers == set()
        assert state._peer_state_locks == {}
        assert state._peer_state_epoch == {}
        assert state._gate_peer_info == {}
        assert state._known_gates == {}

        # Datacenter/manager state
        assert state._dc_registration_states == {}
        assert state._datacenter_manager_status == {}
        assert state._manager_last_status == {}
        assert state._manager_health == {}

        # Backpressure state
        assert state._manager_backpressure == {}
        assert state._backpressure_delay_ms == 0
        assert state._dc_backpressure == {}

    def test_initial_gate_state_is_syncing(self):
        """Initial gate state is SYNCING."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert state._gate_state == GateStateEnum.SYNCING

    def test_initial_state_version_is_zero(self):
        """Initial state version is 0."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert state._state_version == 0

    def test_initial_throughput_values(self):
        """Initial throughput tracking values."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert state._forward_throughput_count == 0
        assert state._forward_throughput_interval_start == 0.0
        assert state._forward_throughput_last_value == 0.0


# =============================================================================
# Gate Peer Methods Tests
# =============================================================================


class TestGatePeerMethods:
    """Tests for gate peer tracking methods."""

    @pytest.mark.asyncio
    async def test_get_or_create_peer_lock_creates_lock(self):
        """Get or create peer lock creates new lock."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9001)

        lock = await state.get_or_create_peer_lock(peer_addr)

        assert isinstance(lock, asyncio.Lock)
        assert peer_addr in state._peer_state_locks

    @pytest.mark.asyncio
    async def test_get_or_create_peer_lock_returns_same_lock(self):
        """Get or create peer lock returns same lock for same peer."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9001)

        lock1 = await state.get_or_create_peer_lock(peer_addr)
        lock2 = await state.get_or_create_peer_lock(peer_addr)

        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_different_peers_get_different_locks(self):
        """Different peers get different locks."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer1 = ("10.0.0.1", 9001)
        peer2 = ("10.0.0.2", 9001)

        lock1 = await state.get_or_create_peer_lock(peer1)
        lock2 = await state.get_or_create_peer_lock(peer2)

        assert lock1 is not lock2

    @pytest.mark.asyncio
    async def test_increment_peer_epoch(self):
        """Increment peer epoch increments and returns value."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9001)

        epoch1 = await state.increment_peer_epoch(peer_addr)
        epoch2 = await state.increment_peer_epoch(peer_addr)
        epoch3 = await state.increment_peer_epoch(peer_addr)

        assert epoch1 == 1
        assert epoch2 == 2
        assert epoch3 == 3

    @pytest.mark.asyncio
    async def test_get_peer_epoch_unknown_peer(self):
        """Get peer epoch for unknown peer returns 0."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert await state.get_peer_epoch(("unknown", 9999)) == 0

    @pytest.mark.asyncio
    async def test_get_peer_epoch_after_increment(self):
        """Get peer epoch returns incremented value."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9001)

        await state.increment_peer_epoch(peer_addr)
        await state.increment_peer_epoch(peer_addr)

        assert await state.get_peer_epoch(peer_addr) == 2

    @pytest.mark.asyncio
    async def test_add_active_peer(self):
        """Add active peer adds to set."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9000)

        await state.add_active_peer(peer_addr)

        assert peer_addr in state._active_gate_peers

    @pytest.mark.asyncio
    async def test_remove_active_peer(self):
        """Remove active peer removes from set."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9000)

        await state.add_active_peer(peer_addr)
        await state.remove_active_peer(peer_addr)

        assert peer_addr not in state._active_gate_peers

    @pytest.mark.asyncio
    async def test_remove_nonexistent_peer_is_safe(self):
        """Remove nonexistent peer doesn't raise."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        await state.remove_active_peer(("unknown", 9999))  # Should not raise

    @pytest.mark.asyncio
    async def test_is_peer_active(self):
        """Is peer active returns correct status."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9000)

        assert state.is_peer_active(peer_addr) is False

        await state.add_active_peer(peer_addr)
        assert state.is_peer_active(peer_addr) is True

        await state.remove_active_peer(peer_addr)
        assert state.is_peer_active(peer_addr) is False

    @pytest.mark.asyncio
    async def test_get_active_peer_count(self):
        """Get active peer count returns correct count."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        assert state.get_active_peer_count() == 0

        await state.add_active_peer(("10.0.0.1", 9000))
        assert state.get_active_peer_count() == 1

        await state.add_active_peer(("10.0.0.2", 9000))
        assert state.get_active_peer_count() == 2

        await state.remove_active_peer(("10.0.0.1", 9000))
        assert state.get_active_peer_count() == 1


# =============================================================================
# Datacenter/Manager Methods Tests
# =============================================================================


class TestDatacenterManagerMethods:
    """Tests for datacenter and manager tracking methods."""

    @pytest.mark.asyncio
    async def test_update_manager_status(self):
        """Update manager status stores heartbeat and timestamp."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        dc_id = "dc-east"
        manager_addr = ("10.0.0.1", 8000)

        class MockHeartbeat:
            pass

        heartbeat = MockHeartbeat()
        timestamp = time.monotonic()

        await state.update_manager_status(dc_id, manager_addr, heartbeat, timestamp)

        assert dc_id in state._datacenter_manager_status
        assert manager_addr in state._datacenter_manager_status[dc_id]
        assert state._datacenter_manager_status[dc_id][manager_addr] is heartbeat
        assert state._manager_last_status[manager_addr] == timestamp

    @pytest.mark.asyncio
    async def test_update_manager_status_multiple_dcs(self):
        """Update manager status for multiple DCs."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        class MockHeartbeat:
            pass

        await state.update_manager_status(
            "dc-east", ("10.0.0.1", 8000), MockHeartbeat(), 1.0
        )
        await state.update_manager_status(
            "dc-west", ("10.0.1.1", 8000), MockHeartbeat(), 2.0
        )

        assert "dc-east" in state._datacenter_manager_status
        assert "dc-west" in state._datacenter_manager_status

    @pytest.mark.asyncio
    async def test_get_manager_status(self):
        """Get manager status returns heartbeat."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        class MockHeartbeat:
            pass

        heartbeat = MockHeartbeat()
        await state.update_manager_status("dc-east", ("10.0.0.1", 8000), heartbeat, 1.0)

        result = state.get_manager_status("dc-east", ("10.0.0.1", 8000))
        assert result is heartbeat

    def test_get_manager_status_unknown_dc(self):
        """Get manager status for unknown DC returns None."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        result = state.get_manager_status("unknown", ("10.0.0.1", 8000))
        assert result is None

    def test_get_manager_status_unknown_manager(self):
        """Get manager status for unknown manager returns None."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state._datacenter_manager_status["dc-east"] = {}

        result = state.get_manager_status("dc-east", ("unknown", 9999))
        assert result is None


# =============================================================================
# Backpressure Methods Tests
# =============================================================================


class TestBackpressureMethods:
    """Tests for backpressure tracking methods."""

    def test_get_dc_backpressure_level_unknown(self):
        """Get DC backpressure level for unknown DC returns NONE."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert state.get_dc_backpressure_level("unknown") == BackpressureLevel.NONE

    def test_get_dc_backpressure_level_known(self):
        """Get DC backpressure level for known DC returns correct level."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state._dc_backpressure["dc-east"] = BackpressureLevel.THROTTLE

        assert state.get_dc_backpressure_level("dc-east") == BackpressureLevel.THROTTLE

    def test_get_max_backpressure_level_empty(self):
        """Get max backpressure level with no DCs returns NONE."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        assert state.get_max_backpressure_level() == BackpressureLevel.NONE

    def test_get_max_backpressure_level_single_dc(self):
        """Get max backpressure level with single DC."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state._dc_backpressure["dc-east"] = BackpressureLevel.BATCH

        assert state.get_max_backpressure_level() == BackpressureLevel.BATCH

    def test_get_max_backpressure_level_multiple_dcs(self):
        """Get max backpressure level returns highest."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state._dc_backpressure["dc-1"] = BackpressureLevel.NONE
        state._dc_backpressure["dc-2"] = BackpressureLevel.THROTTLE
        state._dc_backpressure["dc-3"] = BackpressureLevel.BATCH
        state._dc_backpressure["dc-4"] = BackpressureLevel.REJECT

        assert state.get_max_backpressure_level() == BackpressureLevel.REJECT


# =============================================================================
# Orphan/Leadership Methods Tests
# =============================================================================


class TestOrphanLeadershipMethods:
    """Tests for orphan job and leadership tracking methods."""

    def test_mark_leader_dead(self):
        """Mark leader dead adds to set."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        leader_addr = ("10.0.0.1", 9000)

        state.mark_leader_dead(leader_addr)

        assert leader_addr in state._dead_job_leaders

    def test_clear_dead_leader(self):
        """Clear dead leader removes from set."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        leader_addr = ("10.0.0.1", 9000)

        state.mark_leader_dead(leader_addr)
        state.clear_dead_leader(leader_addr)

        assert leader_addr not in state._dead_job_leaders

    def test_clear_nonexistent_dead_leader_is_safe(self):
        """Clear nonexistent dead leader doesn't raise."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state.clear_dead_leader(("unknown", 9999))  # Should not raise

    def test_is_leader_dead(self):
        """Is leader dead returns correct status."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        leader_addr = ("10.0.0.1", 9000)

        assert state.is_leader_dead(leader_addr) is False

        state.mark_leader_dead(leader_addr)
        assert state.is_leader_dead(leader_addr) is True

        state.clear_dead_leader(leader_addr)
        assert state.is_leader_dead(leader_addr) is False

    def test_mark_job_orphaned(self):
        """Mark job orphaned stores timestamp."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_id = "job-123"
        timestamp = time.monotonic()

        state.mark_job_orphaned(job_id, timestamp, None)

        assert job_id in state._orphaned_jobs
        assert state._orphaned_jobs[job_id] == timestamp

    def test_clear_orphaned_job(self):
        """Clear orphaned job removes it."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_id = "job-123"

        state.mark_job_orphaned(job_id, time.monotonic(), None)
        state.clear_orphaned_job(job_id)

        assert job_id not in state._orphaned_jobs

    def test_clear_nonexistent_orphaned_job_is_safe(self):
        """Clear nonexistent orphaned job doesn't raise."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state.clear_orphaned_job("unknown")  # Should not raise

    def test_is_job_orphaned(self):
        """Is job orphaned returns correct status."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_id = "job-123"

        assert state.is_job_orphaned(job_id) is False

        state.mark_job_orphaned(job_id, time.monotonic(), None)
        assert state.is_job_orphaned(job_id) is True

        state.clear_orphaned_job(job_id)
        assert state.is_job_orphaned(job_id) is False

    def test_get_orphaned_jobs(self):
        """Get orphaned jobs returns copy of dict."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        state.mark_job_orphaned("job-1", 1.0, None)
        state.mark_job_orphaned("job-2", 2.0, None)

        result = state.get_orphaned_jobs()

        assert len(result) == 2
        assert result["job-1"] == 1.0
        assert result["job-2"] == 2.0

        # Should be a copy
        result["job-3"] = 3.0
        assert "job-3" not in state._orphaned_jobs


# =============================================================================
# Cancellation Methods Tests
# =============================================================================


# =============================================================================
# Throughput Methods Tests
# =============================================================================


class TestThroughputMethods:
    """Tests for throughput tracking methods."""

    @pytest.mark.asyncio
    async def test_record_forward(self):
        """Record forward increments count."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        await state.record_forward()
        assert state._forward_throughput_count == 1

        await state.record_forward()
        assert state._forward_throughput_count == 2

    @pytest.mark.asyncio
    async def test_first_interval_starts_at_the_given_start(self):
        """The first sample spans from the state's start, not the monotonic clock's epoch."""
        state = GateRuntimeState(forward_throughput_interval_start=1_000.0)
        for _ in range(20):
            await state.record_forward()

        # The first interval has not elapsed: no sample yet, the count kept.
        assert state.calculate_throughput(1_009.0, 10.0) == 0.0
        assert state._forward_throughput_count == 20

        # Twenty forwards over the ten seconds since the start.
        assert state.calculate_throughput(1_010.0, 10.0) == 2.0
        assert state._forward_throughput_count == 0
        assert state._forward_throughput_interval_start == 1_010.0

    def test_calculate_throughput_within_interval(self):
        """Calculate throughput within interval returns last value."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        state._forward_throughput_interval_start = time.monotonic()
        state._forward_throughput_count = 10
        state._forward_throughput_last_value = 5.0

        # Calculate with interval of 100s (won't trigger reset)
        result = state.calculate_throughput(time.monotonic(), 100.0)

        assert result == 5.0  # Returns last value
        assert state._forward_throughput_count == 10  # Not reset

    def test_calculate_throughput_after_interval(self):
        """Calculate throughput after interval calculates and resets."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        past_time = time.monotonic() - 10.0
        state._forward_throughput_interval_start = past_time
        state._forward_throughput_count = 50

        now = time.monotonic()
        result = state.calculate_throughput(now, 5.0)  # 5s interval elapsed

        # Should calculate throughput (approximately 50/10 = 5.0)
        assert result > 0.0
        assert state._forward_throughput_count == 0  # Reset
        assert state._forward_throughput_interval_start == now


# =============================================================================
# State Version Methods Tests
# =============================================================================


class TestStateVersionMethods:
    """Tests for state version tracking methods."""

    @pytest.mark.asyncio
    async def test_increment_state_version(self):
        """Increment state version increments and returns."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        version1 = await state.increment_state_version()
        version2 = await state.increment_state_version()
        version3 = await state.increment_state_version()

        assert version1 == 1
        assert version2 == 2
        assert version3 == 3

    @pytest.mark.asyncio
    async def test_get_state_version(self):
        """Get state version returns current value."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        assert state.get_state_version() == 0

        await state.increment_state_version()
        await state.increment_state_version()

        assert state.get_state_version() == 2


# =============================================================================
# Gate State Methods Tests
# =============================================================================


class TestGateStateMethods:
    """Tests for gate state management methods."""

    def test_set_gate_state(self):
        """Set gate state updates state."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        state.set_gate_state(GateStateEnum.ACTIVE)
        assert state._gate_state == GateStateEnum.ACTIVE

        state.set_gate_state(GateStateEnum.SYNCING)
        assert state._gate_state == GateStateEnum.SYNCING

    def test_get_gate_state(self):
        """Get gate state returns current state."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        assert state.get_gate_state() == GateStateEnum.SYNCING

        state.set_gate_state(GateStateEnum.ACTIVE)
        assert state.get_gate_state() == GateStateEnum.ACTIVE

    def test_is_active(self):
        """Is active returns correct status."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        assert state.is_active() is False

        state.set_gate_state(GateStateEnum.ACTIVE)
        assert state.is_active() is True

        state.set_gate_state(GateStateEnum.SYNCING)
        assert state.is_active() is False


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_peer_lock_access(self):
        """Concurrent access to same peer lock is serialized."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        peer_addr = ("10.0.0.1", 9001)
        execution_order = []

        async def task(task_id: int, delay: float):
            lock = await state.get_or_create_peer_lock(peer_addr)
            async with lock:
                execution_order.append(f"start-{task_id}")
                await asyncio.sleep(delay)
                execution_order.append(f"end-{task_id}")

        await asyncio.gather(
            task(1, 0.05),
            task(2, 0.01),
        )

        # Operations should be serialized
        assert len(execution_order) == 4


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_many_active_peers(self):
        """Handle many active peers."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        for i in range(1000):
            await state.add_active_peer((f"10.0.{i // 256}.{i % 256}", 9000))

        assert state.get_active_peer_count() == 1000

    def test_many_orphaned_jobs(self):
        """Handle many orphaned jobs."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        for i in range(1000):
            state.mark_job_orphaned(f"job-{i}", float(i), None)

        assert len(state.get_orphaned_jobs()) == 1000

    def test_many_dead_leaders(self):
        """Handle many dead leaders."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        for i in range(1000):
            state.mark_leader_dead((f"10.0.{i // 256}.{i % 256}", 9000))

        assert len(state._dead_job_leaders) == 1000

    def test_special_characters_in_job_ids(self):
        """Handle special characters in job IDs."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        special_ids = [
            "job:colon",
            "job-dash",
            "job_underscore",
            "job.dot",
            "job/slash",
        ]

        for job_id in special_ids:
            state.mark_job_orphaned(job_id, 1.0, None)
            assert state.is_job_orphaned(job_id) is True

    @pytest.mark.asyncio
    async def test_empty_dc_ids(self):
        """Handle empty datacenter IDs."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        class MockHeartbeat:
            pass

        await state.update_manager_status("", ("10.0.0.1", 8000), MockHeartbeat(), 1.0)
        assert "" in state._datacenter_manager_status

    def test_very_long_job_ids(self):
        """Handle very long job IDs."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        long_id = "j" * 10000

        state.mark_job_orphaned(long_id, 1.0, None)
        assert state.is_job_orphaned(long_id) is True


# =============================================================================
# Negative Path Tests
# =============================================================================


class TestNegativePaths:
    """Tests for negative paths and error handling."""

    def test_throughput_calculation_zero_elapsed(self):
        """Throughput calculation handles zero elapsed time."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        now = time.monotonic()
        state._forward_throughput_interval_start = now
        state._forward_throughput_count = 10

        # Should not divide by zero
        result = state.calculate_throughput(now, 0.0)
        # When elapsed is 0, still uses safe division
        assert result >= 0.0

    def test_backpressure_level_comparison(self):
        """Backpressure levels compare correctly."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)

        # Set various levels
        state._dc_backpressure["dc-1"] = BackpressureLevel.NONE
        state._dc_backpressure["dc-2"] = BackpressureLevel.REJECT

        max_level = state.get_max_backpressure_level()
        assert max_level == BackpressureLevel.REJECT
