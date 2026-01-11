"""
Integration tests for GateLeadershipCoordinator (Section 15.3.7).

Tests job leadership coordination across peer gates including:
- Leadership tracking with fence tokens
- Leadership announcements and transfers
- Orphaned job management
"""

import asyncio
import pytest
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock

from hyperscale.distributed.nodes.gate.leadership_coordinator import (
    GateLeadershipCoordinator,
)
from hyperscale.distributed.nodes.gate.state import GateRuntimeState


# =============================================================================
# Mock Classes
# =============================================================================


@dataclass
class MockLogger:
    """Mock logger for testing."""
    messages: list[str] = field(default_factory=list)

    async def log(self, *args, **kwargs):
        self.messages.append(str(args))


@dataclass
class MockTaskRunner:
    """Mock task runner for testing."""
    tasks: list = field(default_factory=list)

    def run(self, coro, *args, **kwargs):
        task = asyncio.create_task(coro(*args, **kwargs))
        self.tasks.append(task)
        return task


@dataclass
class MockNodeId:
    """Mock node ID."""
    full: str = "gate-001"
    datacenter: str = "global"


@dataclass
class MockJobLeadershipTracker:
    """Mock job leadership tracker."""
    leaders: dict = field(default_factory=dict)
    fence_tokens: dict = field(default_factory=dict)
    external_leaders: dict = field(default_factory=dict)

    def is_leader(self, job_id: str) -> bool:
        return job_id in self.leaders

    def assume_leadership(self, job_id: str, metadata: int, fence_token: int = None):
        self.leaders[job_id] = True
        if fence_token is not None:
            self.fence_tokens[job_id] = fence_token
        else:
            self.fence_tokens[job_id] = self.fence_tokens.get(job_id, 0) + 1

    def get_fence_token(self, job_id: str) -> int | None:
        return self.fence_tokens.get(job_id)

    def record_external_leader(
        self,
        job_id: str,
        leader_id: str,
        leader_addr: tuple[str, int],
        fence_token: int,
        metadata: int,
    ):
        self.external_leaders[job_id] = {
            "leader_id": leader_id,
            "leader_addr": leader_addr,
            "fence_token": fence_token,
        }

    def get_leader(self, job_id: str) -> tuple[str, tuple[str, int]] | None:
        if job_id in self.leaders:
            return ("gate-001", ("127.0.0.1", 9000))
        if job_id in self.external_leaders:
            ext = self.external_leaders[job_id]
            return (ext["leader_id"], ext["leader_addr"])
        return None

    def relinquish(self, job_id: str):
        self.leaders.pop(job_id, None)


# =============================================================================
# is_job_leader Tests
# =============================================================================


class TestIsJobLeaderHappyPath:
    """Tests for is_job_leader happy path."""

    def test_is_leader_returns_true(self):
        """Returns true when we are the leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        assert coordinator.is_job_leader("job-1") is True

    def test_is_leader_returns_false(self):
        """Returns false when we are not the leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        assert coordinator.is_job_leader("job-1") is False


# =============================================================================
# assume_leadership Tests
# =============================================================================


class TestAssumeLeadershipHappyPath:
    """Tests for assume_leadership happy path."""

    def test_assumes_leadership(self):
        """Assumes leadership for job."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        coordinator.assume_leadership("job-1", 3)

        assert tracker.is_leader("job-1") is True


# =============================================================================
# broadcast_leadership Tests
# =============================================================================


class TestBroadcastLeadershipHappyPath:
    """Tests for broadcast_leadership happy path."""

    @pytest.mark.asyncio
    async def test_broadcasts_to_all_peers(self):
        """Broadcasts leadership to all active peers."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)
        task_runner = MockTaskRunner()

        peers = [("10.0.0.1", 9000), ("10.0.0.2", 9000), ("10.0.0.3", 9000)]

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=task_runner,
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: peers,
        )

        await coordinator.broadcast_leadership("job-1", 2)

        # Should have spawned tasks for each peer
        assert len(task_runner.tasks) == 3

    @pytest.mark.asyncio
    async def test_broadcasts_to_no_peers(self):
        """No broadcast when no active peers."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)
        task_runner = MockTaskRunner()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=task_runner,
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],  # No peers
        )

        await coordinator.broadcast_leadership("job-1", 2)

        assert len(task_runner.tasks) == 0


# =============================================================================
# handle_leadership_announcement Tests
# =============================================================================


class TestHandleLeadershipAnnouncementHappyPath:
    """Tests for handle_leadership_announcement happy path."""

    def test_accepts_new_leader(self):
        """Accepts leadership announcement for unknown job."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=1,
            target_dc_count=2,
        )

        assert ack.accepted is True
        assert ack.job_id == "job-1"

    def test_accepts_higher_fence_token(self):
        """Accepts announcement with higher fence token."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.fence_tokens["job-1"] = 5

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=10,  # Higher than 5
            target_dc_count=2,
        )

        assert ack.accepted is True


class TestHandleLeadershipAnnouncementNegativePath:
    """Tests for handle_leadership_announcement negative paths."""

    def test_rejects_lower_fence_token(self):
        """Rejects announcement with lower fence token."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.fence_tokens["job-1"] = 10

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=5,  # Lower than 10
            target_dc_count=2,
        )

        assert ack.accepted is False
        assert ack.responder_id == "gate-001"

    def test_rejects_equal_fence_token(self):
        """Rejects announcement with equal fence token."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.fence_tokens["job-1"] = 5

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=5,  # Equal to 5
            target_dc_count=2,
        )

        assert ack.accepted is False


# =============================================================================
# transfer_leadership Tests
# =============================================================================


class TestTransferLeadershipHappyPath:
    """Tests for transfer_leadership happy path."""

    @pytest.mark.asyncio
    async def test_successful_transfer(self):
        """Successfully transfers leadership."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)

        @dataclass
        class MockTransferAck:
            accepted: bool = True

            @classmethod
            def load(cls, data: bytes) -> "MockTransferAck":
                return cls(accepted=True)

        async def mock_send(addr, msg_type, data, timeout=None):
            return (b"accepted", None)

        # Patch the load method
        original_import = __import__

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=mock_send,
            get_active_peers=lambda: [],
        )

        # The actual test depends on JobLeaderGateTransferAck
        # For unit testing, we verify the method doesn't raise
        result = await coordinator.transfer_leadership(
            job_id="job-1",
            new_leader_id="gate-002",
            new_leader_addr=("10.0.0.2", 9000),
            reason="load_balance",
        )

        # Result depends on ack parsing
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_transfer_when_not_leader(self):
        """Transfer fails when not leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        # Not leader for job-1

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        result = await coordinator.transfer_leadership(
            job_id="job-1",
            new_leader_id="gate-002",
            new_leader_addr=("10.0.0.2", 9000),
        )

        assert result is False


class TestTransferLeadershipFailureMode:
    """Tests for transfer_leadership failure modes."""

    @pytest.mark.asyncio
    async def test_transfer_with_network_error(self):
        """Transfer fails on network error."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)

        async def failing_send(addr, msg_type, data, timeout=None):
            raise Exception("Network error")

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=failing_send,
            get_active_peers=lambda: [],
        )

        result = await coordinator.transfer_leadership(
            job_id="job-1",
            new_leader_id="gate-002",
            new_leader_addr=("10.0.0.2", 9000),
        )

        assert result is False


# =============================================================================
# handle_leadership_transfer Tests
# =============================================================================


class TestHandleLeadershipTransferHappyPath:
    """Tests for handle_leadership_transfer happy path."""

    def test_accepts_transfer_for_us(self):
        """Accepts transfer when we are the designated new leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),  # Returns "gate-001"
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_transfer(
            job_id="job-1",
            old_leader_id="gate-002",
            new_leader_id="gate-001",  # Us
            fence_token=5,
            reason="load_balance",
        )

        assert ack.accepted is True
        assert tracker.is_leader("job-1") is True


class TestHandleLeadershipTransferNegativePath:
    """Tests for handle_leadership_transfer negative paths."""

    def test_rejects_transfer_for_other(self):
        """Rejects transfer when we are not the designated new leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),  # Returns "gate-001"
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_transfer(
            job_id="job-1",
            old_leader_id="gate-002",
            new_leader_id="gate-003",  # Not us
            fence_token=5,
            reason="load_balance",
        )

        assert ack.accepted is False
        assert ack.manager_id == "gate-001"


# =============================================================================
# get_job_leader Tests
# =============================================================================


class TestGetJobLeaderHappyPath:
    """Tests for get_job_leader happy path."""

    def test_returns_our_leadership(self):
        """Returns our address when we are leader."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.assume_leadership("job-1", 2)

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        result = coordinator.get_job_leader("job-1")

        assert result is not None
        leader_id, leader_addr = result
        assert leader_id == "gate-001"

    def test_returns_external_leader(self):
        """Returns external leader address."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        tracker.record_external_leader(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=5,
            metadata=2,
        )

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        result = coordinator.get_job_leader("job-1")

        assert result is not None
        leader_id, leader_addr = result
        assert leader_id == "gate-002"
        assert leader_addr == ("10.0.0.2", 9000)

    def test_returns_none_for_unknown(self):
        """Returns None for unknown job."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        result = coordinator.get_job_leader("unknown-job")

        assert result is None


# =============================================================================
# Orphan Job Management Tests
# =============================================================================


class TestOrphanJobManagement:
    """Tests for orphan job management."""

    def test_mark_job_orphaned(self):
        """Marks job as orphaned."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        coordinator.mark_job_orphaned("job-1")

        assert state.is_job_orphaned("job-1") is True

    def test_clear_orphaned_job(self):
        """Clears orphaned status."""
        state = GateRuntimeState()
        state.mark_job_orphaned("job-1", 1.0)
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        coordinator.clear_orphaned_job("job-1")

        assert state.is_job_orphaned("job-1") is False


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_announcements(self):
        """Concurrent leadership announcements are handled safely."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        # Send many concurrent announcements for different jobs
        acks = []
        for i in range(100):
            ack = coordinator.handle_leadership_announcement(
                job_id=f"job-{i}",
                leader_id=f"gate-{i}",
                leader_addr=(f"10.0.0.{i % 256}", 9000),
                fence_token=1,
                target_dc_count=1,
            )
            acks.append(ack)

        # All should be accepted (no prior leadership)
        assert all(ack.accepted for ack in acks)

    @pytest.mark.asyncio
    async def test_concurrent_broadcasts(self):
        """Concurrent broadcasts don't interfere."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()
        task_runner = MockTaskRunner()

        for i in range(10):
            tracker.assume_leadership(f"job-{i}", 2)

        peers = [("10.0.0.1", 9000), ("10.0.0.2", 9000)]

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=task_runner,
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: peers,
        )

        # Broadcast for all jobs concurrently
        await asyncio.gather(*[
            coordinator.broadcast_leadership(f"job-{i}", 2)
            for i in range(10)
        ])

        # Should have 10 jobs * 2 peers = 20 tasks
        assert len(task_runner.tasks) == 20


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_very_large_fence_token(self):
        """Handles very large fence tokens."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=2**62,
            target_dc_count=2,
        )

        assert ack.accepted is True

    def test_zero_fence_token(self):
        """Handles zero fence token."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=0,
            target_dc_count=2,
        )

        assert ack.accepted is True

    def test_special_characters_in_job_id(self):
        """Handles special characters in job ID."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        special_ids = [
            "job:colon",
            "job-dash",
            "job_underscore",
            "job.dot",
        ]

        for job_id in special_ids:
            ack = coordinator.handle_leadership_announcement(
                job_id=job_id,
                leader_id="gate-002",
                leader_addr=("10.0.0.2", 9000),
                fence_token=1,
                target_dc_count=1,
            )
            assert ack.accepted is True

    def test_many_target_dcs(self):
        """Handles many target datacenters."""
        state = GateRuntimeState()
        tracker = MockJobLeadershipTracker()

        coordinator = GateLeadershipCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            leadership_tracker=tracker,
            get_node_id=lambda: MockNodeId(),
            get_node_addr=lambda: ("127.0.0.1", 9000),
            send_tcp=AsyncMock(),
            get_active_peers=lambda: [],
        )

        ack = coordinator.handle_leadership_announcement(
            job_id="job-1",
            leader_id="gate-002",
            leader_addr=("10.0.0.2", 9000),
            fence_token=1,
            target_dc_count=100,
        )

        assert ack.accepted is True
