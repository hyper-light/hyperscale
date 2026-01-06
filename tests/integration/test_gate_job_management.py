"""
Integration tests for Gate Job Management (AD-27 Phase 5.1).

Tests:
- GateJobManager per-job locking and state management
- JobForwardingTracker peer management and forwarding
- ConsistentHashRing job-to-gate mapping
"""

import asyncio
import pytest

from hyperscale.distributed_rewrite.jobs.gates import (
    GateJobManager,
    JobForwardingTracker,
    GatePeerInfo,
    ForwardingResult,
    ConsistentHashRing,
    HashRingNode,
)
from hyperscale.distributed_rewrite.models import (
    GlobalJobStatus,
    JobFinalResult,
    JobProgress,
    JobStatus,
)


class TestGateJobManager:
    """Test GateJobManager operations."""

    def test_create_manager(self) -> None:
        """Test creating a GateJobManager."""
        manager = GateJobManager()

        assert manager.job_count() == 0
        assert manager.get_all_job_ids() == []

    def test_set_and_get_job(self) -> None:
        """Test setting and getting job state."""
        manager = GateJobManager()

        job = GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            timestamp=100.0,
        )
        manager.set_job("job-123", job)

        retrieved = manager.get_job("job-123")
        assert retrieved is not None
        assert retrieved.job_id == "job-123"
        assert retrieved.status == JobStatus.RUNNING.value

    def test_has_job(self) -> None:
        """Test checking job existence."""
        manager = GateJobManager()

        assert manager.has_job("job-123") is False

        manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.PENDING.value,
        ))

        assert manager.has_job("job-123") is True
        assert manager.has_job("job-456") is False

    def test_delete_job(self) -> None:
        """Test deleting a job and all associated data."""
        manager = GateJobManager()

        # Set up job with all associated data
        manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
        ))
        manager.set_target_dcs("job-123", {"dc-1", "dc-2"})
        manager.set_callback("job-123", ("10.0.0.1", 8080))
        manager.set_fence_token("job-123", 5)

        # Delete
        deleted = manager.delete_job("job-123")

        assert deleted is not None
        assert deleted.job_id == "job-123"
        assert manager.has_job("job-123") is False
        assert manager.get_target_dcs("job-123") == set()
        assert manager.get_callback("job-123") is None
        assert manager.get_fence_token("job-123") == 0

    def test_target_dc_management(self) -> None:
        """Test target datacenter tracking."""
        manager = GateJobManager()

        manager.set_target_dcs("job-123", {"dc-1", "dc-2"})
        assert manager.get_target_dcs("job-123") == {"dc-1", "dc-2"}

        manager.add_target_dc("job-123", "dc-3")
        assert "dc-3" in manager.get_target_dcs("job-123")

    def test_dc_result_management(self) -> None:
        """Test datacenter result tracking."""
        manager = GateJobManager()
        manager.set_target_dcs("job-123", {"dc-1", "dc-2"})

        result1 = JobFinalResult(
            job_id="job-123",
            datacenter="dc-1",
            status=JobStatus.COMPLETED.value,
        )
        manager.set_dc_result("job-123", "dc-1", result1)

        assert manager.get_completed_dc_count("job-123") == 1
        assert manager.all_dcs_reported("job-123") is False

        result2 = JobFinalResult(
            job_id="job-123",
            datacenter="dc-2",
            status=JobStatus.COMPLETED.value,
        )
        manager.set_dc_result("job-123", "dc-2", result2)

        assert manager.get_completed_dc_count("job-123") == 2
        assert manager.all_dcs_reported("job-123") is True

    def test_callback_management(self) -> None:
        """Test callback registration."""
        manager = GateJobManager()

        assert manager.has_callback("job-123") is False

        manager.set_callback("job-123", ("10.0.0.1", 8080))
        assert manager.has_callback("job-123") is True
        assert manager.get_callback("job-123") == ("10.0.0.1", 8080)

        removed = manager.remove_callback("job-123")
        assert removed == ("10.0.0.1", 8080)
        assert manager.has_callback("job-123") is False

    def test_fence_token_management(self) -> None:
        """Test fence token tracking."""
        manager = GateJobManager()

        assert manager.get_fence_token("job-123") == 0

        manager.set_fence_token("job-123", 5)
        assert manager.get_fence_token("job-123") == 5

        # Update only if higher
        assert manager.update_fence_token_if_higher("job-123", 3) is False
        assert manager.get_fence_token("job-123") == 5

        assert manager.update_fence_token_if_higher("job-123", 10) is True
        assert manager.get_fence_token("job-123") == 10

    @pytest.mark.asyncio
    async def test_job_locking(self) -> None:
        """Test per-job locking for concurrent safety."""
        manager = GateJobManager()
        manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.PENDING.value,
            total_completed=0,
        ))

        results: list[int] = []

        async def increment_job(amount: int) -> None:
            async with manager.lock_job("job-123"):
                job = manager.get_job("job-123")
                assert job is not None
                # Simulate some async work
                await asyncio.sleep(0.01)
                job.total_completed += amount
                manager.set_job("job-123", job)
                results.append(amount)

        # Run concurrent increments
        await asyncio.gather(
            increment_job(1),
            increment_job(2),
            increment_job(3),
        )

        # All increments should have been serialized
        job = manager.get_job("job-123")
        assert job is not None
        assert job.total_completed == 6

    def test_cleanup_old_jobs(self) -> None:
        """Test cleaning up old completed jobs."""
        manager = GateJobManager()

        # Add old completed job
        manager.set_job("job-old", GlobalJobStatus(
            job_id="job-old",
            status=JobStatus.COMPLETED.value,
            timestamp=0.0,  # Very old
        ))

        # Add recent running job
        import time
        manager.set_job("job-new", GlobalJobStatus(
            job_id="job-new",
            status=JobStatus.RUNNING.value,
            timestamp=time.monotonic(),
        ))

        # Cleanup with 1 second max age
        removed = manager.cleanup_old_jobs(max_age_seconds=1.0)

        assert "job-old" in removed
        assert manager.has_job("job-old") is False
        assert manager.has_job("job-new") is True


class TestJobForwardingTracker:
    """Test JobForwardingTracker operations."""

    def test_create_tracker(self) -> None:
        """Test creating a JobForwardingTracker."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        assert tracker.peer_count() == 0

    def test_register_peer(self) -> None:
        """Test registering a peer gate."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        tracker.register_peer("gate-2", "10.0.0.2", 8080)

        assert tracker.peer_count() == 1
        peer = tracker.get_peer("gate-2")
        assert peer is not None
        assert peer.tcp_host == "10.0.0.2"
        assert peer.tcp_port == 8080

    def test_register_self_ignored(self) -> None:
        """Test that registering self is ignored."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        tracker.register_peer("gate-1", "10.0.0.1", 8080)

        assert tracker.peer_count() == 0

    def test_unregister_peer(self) -> None:
        """Test unregistering a peer."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        tracker.register_peer("gate-2", "10.0.0.2", 8080)
        tracker.unregister_peer("gate-2")

        assert tracker.peer_count() == 0

    def test_update_peer_from_heartbeat(self) -> None:
        """Test updating peer info from heartbeat."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        tracker.update_peer_from_heartbeat("gate-2", "10.0.0.2", 8080)
        tracker.update_peer_from_heartbeat("gate-2", "10.0.0.20", 9000)

        peer = tracker.get_peer("gate-2")
        assert peer is not None
        assert peer.tcp_host == "10.0.0.20"
        assert peer.tcp_port == 9000

    @pytest.mark.asyncio
    async def test_forward_with_no_peers(self) -> None:
        """Test forwarding with no peers registered."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> bytes:
            return b"ok"

        result = await tracker.forward_result(
            job_id="job-123",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.forwarded is False
        assert "No peer gates" in (result.error or "")

    @pytest.mark.asyncio
    async def test_forward_success(self) -> None:
        """Test successful forwarding."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")
        tracker.register_peer("gate-2", "10.0.0.2", 8080)

        forwarded_to: list[tuple[str, int]] = []

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> bytes:
            forwarded_to.append(addr)
            return b"ok"

        result = await tracker.forward_result(
            job_id="job-123",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.forwarded is True
        assert result.target_gate_id == "gate-2"
        assert ("10.0.0.2", 8080) in forwarded_to

    @pytest.mark.asyncio
    async def test_forward_with_failure_retry(self) -> None:
        """Test that forwarding retries on failure."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")
        tracker.register_peer("gate-2", "10.0.0.2", 8080)
        tracker.register_peer("gate-3", "10.0.0.3", 8080)

        call_count = 0

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> bytes:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("First peer failed")
            return b"ok"

        result = await tracker.forward_result(
            job_id="job-123",
            data=b"test_data",
            send_tcp=mock_send_tcp,
        )

        assert result.forwarded is True
        assert call_count == 2

    def test_get_stats(self) -> None:
        """Test getting forwarding statistics."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")
        tracker.register_peer("gate-2", "10.0.0.2", 8080)

        stats = tracker.get_stats()

        assert stats["peer_count"] == 1
        assert stats["total_forwards"] == 0
        assert "gate-2" in stats["peers"]

    def test_cleanup_stale_peers(self) -> None:
        """Test cleaning up stale peers."""
        tracker = JobForwardingTracker(local_gate_id="gate-1")

        # Register peer with old last_seen
        tracker.register_peer("gate-2", "10.0.0.2", 8080)
        peer = tracker.get_peer("gate-2")
        assert peer is not None
        peer.last_seen = 0.0  # Very old

        removed = tracker.cleanup_stale_peers(max_age_seconds=1.0)

        assert "gate-2" in removed
        assert tracker.peer_count() == 0


class TestConsistentHashRing:
    """Test ConsistentHashRing operations."""

    def test_create_ring(self) -> None:
        """Test creating an empty ring."""
        ring = ConsistentHashRing()

        assert ring.node_count() == 0
        assert ring.get_node("any-key") is None

    def test_add_node(self) -> None:
        """Test adding a node to the ring."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)

        assert ring.node_count() == 1
        assert ring.has_node("gate-1") is True

        node = ring.get_node_by_id("gate-1")
        assert node is not None
        assert node.tcp_host == "10.0.0.1"
        assert node.tcp_port == 8080

    def test_remove_node(self) -> None:
        """Test removing a node from the ring."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)
        removed = ring.remove_node("gate-1")

        assert removed is not None
        assert removed.node_id == "gate-1"
        assert ring.has_node("gate-1") is False
        assert ring.node_count() == 0

    def test_get_node_for_key(self) -> None:
        """Test getting the responsible node for a key."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)

        # With only one node, all keys map to it
        owner = ring.get_node("job-123")
        assert owner is not None
        assert owner.node_id == "gate-1"

    def test_consistent_mapping(self) -> None:
        """Test that same key always maps to same node."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)
        ring.add_node("gate-3", "10.0.0.3", 8080)

        # Same key should always map to same node
        owner1 = ring.get_owner_id("job-12345")
        owner2 = ring.get_owner_id("job-12345")
        owner3 = ring.get_owner_id("job-12345")

        assert owner1 == owner2 == owner3

    def test_is_owner(self) -> None:
        """Test ownership checking."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)

        assert ring.is_owner("any-job", "gate-1") is True
        assert ring.is_owner("any-job", "gate-2") is False

    def test_get_multiple_nodes(self) -> None:
        """Test getting multiple nodes for replication."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)
        ring.add_node("gate-3", "10.0.0.3", 8080)

        nodes = ring.get_nodes("job-123", count=2)

        assert len(nodes) == 2
        # All returned nodes should be distinct
        node_ids = [n.node_id for n in nodes]
        assert len(set(node_ids)) == 2

    def test_distribution_balance(self) -> None:
        """Test that keys are reasonably balanced across nodes."""
        ring = ConsistentHashRing(replicas=150)

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)
        ring.add_node("gate-3", "10.0.0.3", 8080)

        # Generate sample keys
        sample_keys = [f"job-{i}" for i in range(1000)]
        distribution = ring.get_distribution(sample_keys)

        # Each node should have roughly 333 keys (1000/3)
        # Allow 20% deviation
        for count in distribution.values():
            assert 200 < count < 466, f"Distribution unbalanced: {distribution}"

    def test_minimal_remapping_on_add(self) -> None:
        """Test that adding a node only remaps ~1/N keys."""
        ring = ConsistentHashRing(replicas=150)

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)

        # Record owners before adding third node
        sample_keys = [f"job-{i}" for i in range(1000)]
        owners_before = {key: ring.get_owner_id(key) for key in sample_keys}

        # Add third node
        ring.add_node("gate-3", "10.0.0.3", 8080)

        # Count remapped keys
        remapped = 0
        for key in sample_keys:
            if ring.get_owner_id(key) != owners_before[key]:
                remapped += 1

        # Should remap roughly 1/3 of keys (now 3 nodes instead of 2)
        # Allow generous margin
        assert remapped < 500, f"Too many keys remapped: {remapped}"

    def test_ring_info(self) -> None:
        """Test getting ring information."""
        ring = ConsistentHashRing(replicas=100)

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080, weight=2)

        info = ring.get_ring_info()

        assert info["node_count"] == 2
        assert info["replicas_per_node"] == 100
        # gate-2 has weight 2, so more virtual nodes
        assert info["virtual_node_count"] == 300  # 100 + 200

    def test_weighted_nodes(self) -> None:
        """Test that weighted nodes get proportionally more keys."""
        ring = ConsistentHashRing(replicas=150)

        ring.add_node("gate-1", "10.0.0.1", 8080, weight=1)
        ring.add_node("gate-2", "10.0.0.2", 8080, weight=2)

        sample_keys = [f"job-{i}" for i in range(1000)]
        distribution = ring.get_distribution(sample_keys)

        # gate-2 should have roughly 2x the keys of gate-1
        # Allow significant margin due to hashing variance
        assert distribution["gate-2"] > distribution["gate-1"]

    def test_clear_ring(self) -> None:
        """Test clearing all nodes from the ring."""
        ring = ConsistentHashRing()

        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)

        ring.clear()

        assert ring.node_count() == 0
        assert ring.get_node("any-key") is None


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_job_lifecycle_with_forwarding(self) -> None:
        """
        Test full job lifecycle with forwarding.

        Scenario:
        1. Gate-1 receives job submission
        2. Gate-1 stores job in GateJobManager
        3. Gate-2 receives result for job it doesn't own
        4. Gate-2 forwards to Gate-1
        5. Gate-1 aggregates and completes job
        """
        # Setup
        gate1_manager = GateJobManager()
        gate2_tracker = JobForwardingTracker(local_gate_id="gate-2")
        hash_ring = ConsistentHashRing()

        # Register gates in hash ring
        hash_ring.add_node("gate-1", "10.0.0.1", 8080)
        hash_ring.add_node("gate-2", "10.0.0.2", 8080)

        # Setup forwarding
        gate2_tracker.register_peer("gate-1", "10.0.0.1", 8080)

        # Find a job that maps to gate-1
        test_job_id = "job-for-gate1"
        # Ensure the job maps to gate-1 by checking
        while hash_ring.get_owner_id(test_job_id) != "gate-1":
            test_job_id = f"job-{hash(test_job_id)}"

        # Gate-1 receives and stores job
        job = GlobalJobStatus(
            job_id=test_job_id,
            status=JobStatus.RUNNING.value,
        )
        gate1_manager.set_job(test_job_id, job)
        gate1_manager.set_target_dcs(test_job_id, {"dc-1"})

        # Gate-2 receives result (simulated as not owning the job)
        owner = hash_ring.get_owner_id(test_job_id)
        assert owner == "gate-1"

        # Track forwarded data
        forwarded_data: list[bytes] = []

        async def mock_send_tcp(
            addr: tuple[str, int],
            endpoint: str,
            data: bytes,
            timeout: float = 5.0,
        ) -> bytes:
            forwarded_data.append(data)
            return b"ok"

        # Forward result
        result = JobFinalResult(
            job_id=test_job_id,
            datacenter="dc-1",
            status=JobStatus.COMPLETED.value,
        )

        forward_result = await gate2_tracker.forward_result(
            job_id=test_job_id,
            data=result.dump(),
            send_tcp=mock_send_tcp,
        )

        assert forward_result.forwarded is True
        assert len(forwarded_data) == 1

    def test_hash_ring_with_job_manager(self) -> None:
        """Test using hash ring to determine job ownership."""
        manager = GateJobManager()
        ring = ConsistentHashRing()

        # Setup 3 gates
        ring.add_node("gate-1", "10.0.0.1", 8080)
        ring.add_node("gate-2", "10.0.0.2", 8080)
        ring.add_node("gate-3", "10.0.0.3", 8080)

        # Simulate receiving jobs
        for i in range(100):
            job_id = f"job-{i}"
            owner = ring.get_owner_id(job_id)

            # Only store if we're the owner (simulating gate-1's perspective)
            if owner == "gate-1":
                manager.set_job(job_id, GlobalJobStatus(
                    job_id=job_id,
                    status=JobStatus.RUNNING.value,
                ))

        # Should have roughly 1/3 of jobs
        assert 20 < manager.job_count() < 50
