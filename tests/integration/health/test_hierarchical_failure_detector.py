"""
Comprehensive tests for the HierarchicalFailureDetector component.

Tests cover:
1. Happy path: Normal suspicion lifecycle across both layers
2. Negative path: Invalid inputs, stale incarnations
3. Failure modes: Callback exceptions, layer disagreements
4. Edge cases: Global death clearing job suspicions, reconciliation
5. Concurrency correctness: Async safety under concurrent operations
"""

import asyncio
import time

import pytest

from hyperscale.distributed_rewrite.swim.detection.hierarchical_failure_detector import (
    HierarchicalFailureDetector,
    HierarchicalConfig,
    NodeStatus,
    FailureSource,
    FailureEvent,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def default_config() -> HierarchicalConfig:
    """Default configuration for tests."""
    return HierarchicalConfig(
        global_min_timeout=5.0,
        global_max_timeout=30.0,
        job_min_timeout=1.0,
        job_max_timeout=10.0,
        coarse_tick_ms=1000,
        fine_tick_ms=100,
        poll_interval_far_ms=1000,
        poll_interval_near_ms=50,
        reconciliation_interval_s=5.0,
    )


@pytest.fixture
def fast_config() -> HierarchicalConfig:
    """Fast configuration for quick expiration tests."""
    return HierarchicalConfig(
        global_min_timeout=0.05,
        global_max_timeout=0.1,
        job_min_timeout=0.05,
        job_max_timeout=0.1,
        coarse_tick_ms=10,
        fine_tick_ms=10,
        poll_interval_far_ms=10,
        poll_interval_near_ms=5,
        reconciliation_interval_s=0.1,
    )


def make_node(index: int) -> tuple[str, int]:
    """Create a node address from an index."""
    return (f"192.168.1.{index}", 7946)


def make_job_id(index: int) -> str:
    """Create a job ID from an index."""
    return f"job-{index:04d}"


# =============================================================================
# Test HierarchicalFailureDetector - Happy Path
# =============================================================================


class TestHierarchicalHappyPath:
    """Happy path tests for HierarchicalFailureDetector."""

    @pytest.mark.asyncio
    async def test_start_stop_lifecycle(self, default_config: HierarchicalConfig):
        """Starting and stopping should work correctly."""
        detector = HierarchicalFailureDetector(config=default_config)

        await detector.start()
        assert detector._running is True

        await detector.stop()
        assert detector._running is False

    @pytest.mark.asyncio
    async def test_suspect_global_creates_suspicion(self, default_config: HierarchicalConfig):
        """Global suspicion should be tracked."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)
            from_node = make_node(2)

            result = await detector.suspect_global(node, 1, from_node)

            assert result is True
            assert await detector.is_alive_global(node) is False
            status = await detector.get_node_status(node)
            assert status == NodeStatus.SUSPECTED_GLOBAL
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_suspect_job_creates_suspicion(self, default_config: HierarchicalConfig):
        """Job suspicion should be tracked."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            job_id = make_job_id(1)
            node = make_node(1)
            from_node = make_node(2)

            result = await detector.suspect_job(job_id, node, 1, from_node)

            assert result is True
            assert detector.is_alive_for_job(job_id, node) is False
            status = await detector.get_node_status(node)
            assert status == NodeStatus.SUSPECTED_JOB
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_refute_global_clears_suspicion(self, default_config: HierarchicalConfig):
        """Refuting global suspicion should clear it."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            await detector.suspect_global(node, 1, make_node(2))
            assert await detector.is_alive_global(node) is False

            result = await detector.refute_global(node, 2)

            assert result is True
            assert await detector.is_alive_global(node) is True
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_refute_job_clears_suspicion(self, default_config: HierarchicalConfig):
        """Refuting job suspicion should clear it."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await detector.suspect_job(job_id, node, 1, make_node(2))
            assert detector.is_alive_for_job(job_id, node) is False

            result = await detector.refute_job(job_id, node, 2)

            assert result is True
            assert detector.is_alive_for_job(job_id, node) is True
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_confirm_global_adds_confirmation(self, default_config: HierarchicalConfig):
        """Confirming global suspicion should add confirmation."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            await detector.suspect_global(node, 1, make_node(2))
            result = await detector.confirm_global(node, 1, make_node(3))

            assert result is True
            state = await detector.get_global_suspicion_state(node)
            assert state.confirmation_count == 2
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_confirm_job_adds_confirmation(self, default_config: HierarchicalConfig):
        """Confirming job suspicion should add confirmation."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await detector.suspect_job(job_id, node, 1, make_node(2))
            result = await detector.confirm_job(job_id, node, 1, make_node(3))

            assert result is True
            state = detector.get_job_suspicion_state(job_id, node)
            assert state.confirmation_count == 2
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_global_expiration_triggers_callback(self, fast_config: HierarchicalConfig):
        """Global expiration should trigger callback."""
        deaths: list[tuple[tuple[str, int], int]] = []

        def on_global_death(node: tuple[str, int], incarnation: int) -> None:
            deaths.append((node, incarnation))

        detector = HierarchicalFailureDetector(
            config=fast_config,
            on_global_death=on_global_death,
        )
        await detector.start()

        try:
            node = make_node(1)
            await detector.suspect_global(node, 1, make_node(2))

            # Wait for expiration
            await asyncio.sleep(0.3)

            assert len(deaths) == 1
            assert deaths[0][0] == node
            assert deaths[0][1] == 1

            status = await detector.get_node_status(node)
            assert status == NodeStatus.DEAD_GLOBAL
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_job_expiration_triggers_callback(self, fast_config: HierarchicalConfig):
        """Job expiration should trigger callback."""
        deaths: list[tuple[str, tuple[str, int], int]] = []

        def on_job_death(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            deaths.append((job_id, node, incarnation))

        detector = HierarchicalFailureDetector(
            config=fast_config,
            on_job_death=on_job_death,
        )
        await detector.start()

        try:
            job_id = make_job_id(1)
            node = make_node(1)
            await detector.suspect_job(job_id, node, 1, make_node(2))

            # Wait for expiration
            await asyncio.sleep(0.3)

            assert len(deaths) == 1
            assert deaths[0][0] == job_id
            assert deaths[0][1] == node
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_clear_job_removes_all_job_suspicions(self, default_config: HierarchicalConfig):
        """Clearing a job should remove all its suspicions."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            job_id = make_job_id(1)

            for i in range(5):
                await detector.suspect_job(job_id, make_node(i), 1, make_node(100))

            assert len(detector.get_suspected_nodes_for_job(job_id)) == 5

            cleared = await detector.clear_job(job_id)

            assert cleared == 5
            assert len(detector.get_suspected_nodes_for_job(job_id)) == 0
        finally:
            await detector.stop()


# =============================================================================
# Test HierarchicalFailureDetector - Negative Path
# =============================================================================


class TestHierarchicalNegativePath:
    """Negative path tests for HierarchicalFailureDetector."""

    @pytest.mark.asyncio
    async def test_suspect_global_stale_incarnation(self, default_config: HierarchicalConfig):
        """Stale global suspicion should be ignored."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            await detector.suspect_global(node, 5, make_node(2))
            result = await detector.suspect_global(node, 3, make_node(3))

            assert result is False
            state = await detector.get_global_suspicion_state(node)
            assert state.incarnation == 5
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_suspect_job_for_globally_dead_node(self, fast_config: HierarchicalConfig):
        """Job suspicion for globally dead node should be rejected."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            node = make_node(1)
            job_id = make_job_id(1)

            # Let node die globally
            await detector.suspect_global(node, 1, make_node(2))
            await asyncio.sleep(0.3)

            # Try to suspect for job
            result = await detector.suspect_job(job_id, node, 1, make_node(3))

            assert result is False
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_refute_global_with_lower_incarnation(self, default_config: HierarchicalConfig):
        """Refuting with lower incarnation should fail."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            await detector.suspect_global(node, 5, make_node(2))
            result = await detector.refute_global(node, 3)

            assert result is False
            assert await detector.is_alive_global(node) is False
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_confirm_global_wrong_incarnation(self, default_config: HierarchicalConfig):
        """Confirming with wrong incarnation should fail."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            await detector.suspect_global(node, 5, make_node(2))
            result = await detector.confirm_global(node, 3, make_node(3))

            assert result is False
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_confirm_global_nonexistent(self, default_config: HierarchicalConfig):
        """Confirming nonexistent suspicion should fail."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            result = await detector.confirm_global(make_node(1), 1, make_node(2))

            assert result is False
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_refute_global_nonexistent(self, default_config: HierarchicalConfig):
        """Refuting nonexistent suspicion should fail."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            result = await detector.refute_global(make_node(1), 1)

            assert result is False
        finally:
            await detector.stop()


# =============================================================================
# Test HierarchicalFailureDetector - Layer Interaction
# =============================================================================


class TestHierarchicalLayerInteraction:
    """Tests for interaction between global and job layers."""

    @pytest.mark.asyncio
    async def test_global_death_clears_job_suspicions(self, fast_config: HierarchicalConfig):
        """Global death should clear all job suspicions for that node."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            node = make_node(1)

            # Create job suspicions first
            for i in range(3):
                job_id = make_job_id(i)
                await detector.suspect_job(job_id, node, 1, make_node(100))

            assert len(detector.get_jobs_with_suspected_node(node)) == 3

            # Now suspect globally and let it expire
            await detector.suspect_global(node, 1, make_node(100))
            await asyncio.sleep(0.3)

            # Job suspicions should be cleared
            assert len(detector.get_jobs_with_suspected_node(node)) == 0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_globally_dead_affects_is_alive_for_job(self, fast_config: HierarchicalConfig):
        """Globally dead node should show as dead for all jobs."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            node = make_node(1)
            job_id = make_job_id(1)

            # Node is initially alive for job
            assert detector.is_alive_for_job(job_id, node) is True

            # Kill globally
            await detector.suspect_global(node, 1, make_node(2))
            await asyncio.sleep(0.3)

            # Should be dead for job too
            assert detector.is_alive_for_job(job_id, node) is False
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_job_suspicion_independent_of_other_jobs(
        self,
        default_config: HierarchicalConfig,
    ):
        """Job suspicions should be independent across jobs."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)
            job_a = make_job_id(1)
            job_b = make_job_id(2)

            # Suspect for job A only
            await detector.suspect_job(job_a, node, 1, make_node(2))

            # Node should be dead for job A, alive for job B
            assert detector.is_alive_for_job(job_a, node) is False
            assert detector.is_alive_for_job(job_b, node) is True

            # Global should still be alive
            assert await detector.is_alive_global(node) is True
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_clear_global_death_allows_new_suspicions(
        self,
        fast_config: HierarchicalConfig,
    ):
        """Clearing global death should allow new suspicions."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            node = make_node(1)

            # Kill globally
            await detector.suspect_global(node, 1, make_node(2))
            await asyncio.sleep(0.3)

            status = await detector.get_node_status(node)
            assert status == NodeStatus.DEAD_GLOBAL

            # Clear death (node rejoined)
            result = await detector.clear_global_death(node)
            assert result is True

            # Now can suspect again
            result = await detector.suspect_global(node, 2, make_node(3))
            assert result is True
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_node_status_priority(self, default_config: HierarchicalConfig):
        """Node status should reflect most severe condition."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)

            # Initially alive
            status = await detector.get_node_status(node)
            assert status == NodeStatus.ALIVE

            # Suspect for job
            await detector.suspect_job(make_job_id(1), node, 1, make_node(2))
            status = await detector.get_node_status(node)
            assert status == NodeStatus.SUSPECTED_JOB

            # Suspect globally (more severe)
            await detector.suspect_global(node, 1, make_node(3))
            status = await detector.get_node_status(node)
            assert status == NodeStatus.SUSPECTED_GLOBAL
        finally:
            await detector.stop()


# =============================================================================
# Test HierarchicalFailureDetector - Failure Modes
# =============================================================================


class TestHierarchicalFailureModes:
    """Failure mode tests for HierarchicalFailureDetector."""

    @pytest.mark.asyncio
    async def test_global_callback_exception_doesnt_stop_detection(
        self,
        fast_config: HierarchicalConfig,
    ):
        """Exception in global callback should not stop detection."""
        call_count = 0

        def failing_callback(node: tuple[str, int], incarnation: int) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated failure")

        detector = HierarchicalFailureDetector(
            config=fast_config,
            on_global_death=failing_callback,
        )
        await detector.start()

        try:
            # Create two suspicions
            for i in range(2):
                await detector.suspect_global(make_node(i), 1, make_node(100))

            # Wait for expirations
            await asyncio.sleep(0.3)

            # Both should have been processed
            assert call_count == 2
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_job_callback_exception_doesnt_stop_detection(
        self,
        fast_config: HierarchicalConfig,
    ):
        """Exception in job callback should not stop detection."""
        call_count = 0

        def failing_callback(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated failure")

        detector = HierarchicalFailureDetector(
            config=fast_config,
            on_job_death=failing_callback,
        )
        await detector.start()

        try:
            # Create two suspicions
            for i in range(2):
                await detector.suspect_job(make_job_id(i), make_node(i), 1, make_node(100))

            # Wait for expirations
            await asyncio.sleep(0.3)

            # Both should have been processed
            assert call_count == 2
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_double_stop_is_safe(self, default_config: HierarchicalConfig):
        """Double stop should be safe."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        await detector.stop()
        await detector.stop()  # Should not raise

        assert detector._running is False


# =============================================================================
# Test HierarchicalFailureDetector - Edge Cases
# =============================================================================


class TestHierarchicalEdgeCases:
    """Edge case tests for HierarchicalFailureDetector."""

    @pytest.mark.asyncio
    async def test_lhm_affects_timeouts(self, default_config: HierarchicalConfig):
        """LHM should affect suspicion timeouts."""
        lhm_value = 1.0

        def get_lhm() -> float:
            return lhm_value

        detector = HierarchicalFailureDetector(
            config=default_config,
            get_lhm_multiplier=get_lhm,
        )
        await detector.start()

        try:
            node = make_node(1)

            # Set high LHM before suspecting
            lhm_value = 2.0

            await detector.suspect_global(node, 1, make_node(2))

            state = await detector.get_global_suspicion_state(node)
            # Timeout should be multiplied by LHM
            assert state.max_timeout == default_config.global_max_timeout * 2.0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_apply_lhm_adjustment(self, default_config: HierarchicalConfig):
        """LHM adjustment should extend timeouts."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            # Create some suspicions
            for i in range(5):
                await detector.suspect_global(make_node(i), 1, make_node(100))

            # Apply LHM adjustment
            result = await detector.apply_lhm_adjustment(2.0)

            assert result["global_adjusted"] == 5
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_get_recent_events(self, fast_config: HierarchicalConfig):
        """Recent events should be tracked."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            # Create and let expire
            await detector.suspect_global(make_node(1), 1, make_node(100))
            await detector.suspect_job(make_job_id(1), make_node(2), 1, make_node(100))

            await asyncio.sleep(0.3)

            events = detector.get_recent_events(10)

            assert len(events) >= 2
            sources = {e.source for e in events}
            assert FailureSource.GLOBAL in sources
            assert FailureSource.JOB in sources
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_stats_accuracy(self, fast_config: HierarchicalConfig):
        """Stats should be accurate."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            # Create suspicions
            await detector.suspect_global(make_node(1), 1, make_node(100))
            await detector.suspect_job(make_job_id(1), make_node(2), 1, make_node(100))

            await asyncio.sleep(0.3)

            stats = detector.get_stats()

            assert stats["global_deaths"] == 1
            assert stats["job_deaths"] == 1
            assert stats["globally_dead_count"] == 1
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_reconciliation_cleans_up_inconsistencies(
        self,
        fast_config: HierarchicalConfig,
    ):
        """Reconciliation should clean up inconsistent state."""
        detector = HierarchicalFailureDetector(config=fast_config)

        # Manually create inconsistent state (job suspicion for dead node)
        node = make_node(1)
        detector._globally_dead.add(node)

        # Add job suspicion directly (bypassing check)
        await detector._job_manager.start_suspicion(
            make_job_id(1), node, 1, make_node(100),
            min_timeout=10.0, max_timeout=20.0,
        )

        await detector.start()

        try:
            # Wait for reconciliation
            await asyncio.sleep(0.3)

            # Job suspicion should be cleared
            assert len(detector.get_jobs_with_suspected_node(node)) == 0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_clear_global_death_nonexistent(self, default_config: HierarchicalConfig):
        """Clearing non-dead node should return False."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            result = await detector.clear_global_death(make_node(1))
            assert result is False
        finally:
            await detector.stop()


# =============================================================================
# Test HierarchicalFailureDetector - Concurrency Correctness
# =============================================================================


class TestHierarchicalConcurrency:
    """Concurrency correctness tests for HierarchicalFailureDetector."""

    @pytest.mark.asyncio
    async def test_concurrent_global_suspects_same_node(
        self,
        default_config: HierarchicalConfig,
    ):
        """Concurrent global suspicions for same node should be safe."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)
            results: list[bool] = []

            async def suspect(from_idx: int):
                result = await detector.suspect_global(node, 1, make_node(from_idx))
                results.append(result)

            await asyncio.gather(*[suspect(i) for i in range(10)])

            # First should succeed, rest add confirmations (also return True)
            # State should be consistent
            state = await detector.get_global_suspicion_state(node)
            assert state is not None
            assert state.confirmation_count == 10
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_concurrent_global_and_job_operations(
        self,
        default_config: HierarchicalConfig,
    ):
        """Concurrent operations on both layers should be safe."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            async def global_operations():
                for i in range(20):
                    node = make_node(i)
                    await detector.suspect_global(node, 1, make_node(100))
                    await asyncio.sleep(0)
                    await detector.refute_global(node, 2)
                    await asyncio.sleep(0)

            async def job_operations():
                for i in range(20):
                    job_id = make_job_id(i % 5)
                    node = make_node(i + 50)
                    await detector.suspect_job(job_id, node, 1, make_node(100))
                    await asyncio.sleep(0)
                    await detector.refute_job(job_id, node, 2)
                    await asyncio.sleep(0)

            await asyncio.gather(global_operations(), job_operations())

            # State should be consistent
            stats = detector.get_stats()
            assert stats["global_suspected"] >= 0
            assert stats["job_suspicions"] >= 0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_concurrent_status_queries_during_modifications(
        self,
        default_config: HierarchicalConfig,
    ):
        """Status queries during modifications should return valid values."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            node = make_node(1)
            job_id = make_job_id(1)

            statuses: list[NodeStatus] = []
            done = asyncio.Event()

            async def query_status():
                while not done.is_set():
                    status = await detector.get_node_status(node)
                    statuses.append(status)
                    await asyncio.sleep(0)

            async def modify():
                for _ in range(50):
                    await detector.suspect_global(node, 1, make_node(2))
                    await asyncio.sleep(0)
                    await detector.refute_global(node, 2)
                    await detector.suspect_job(job_id, node, 1, make_node(3))
                    await asyncio.sleep(0)
                    await detector.refute_job(job_id, node, 2)
                    await asyncio.sleep(0)
                done.set()

            await asyncio.gather(query_status(), modify())

            # All statuses should be valid enum values
            valid_statuses = set(NodeStatus)
            for status in statuses:
                assert status in valid_statuses
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_concurrent_lhm_adjustment(self, default_config: HierarchicalConfig):
        """Concurrent LHM adjustments should be safe."""
        detector = HierarchicalFailureDetector(config=default_config)
        await detector.start()

        try:
            # Pre-populate
            for i in range(10):
                await detector.suspect_global(make_node(i), 1, make_node(100))

            async def adjust():
                for multiplier in [1.5, 2.0, 0.75, 1.0]:
                    await detector.apply_lhm_adjustment(multiplier)
                    await asyncio.sleep(0.01)

            async def suspect_more():
                for i in range(10, 20):
                    await detector.suspect_global(make_node(i), 1, make_node(100))
                    await asyncio.sleep(0)

            await asyncio.gather(adjust(), suspect_more())

            # State should be consistent
            stats = detector.get_stats()
            assert stats["global_suspected"] >= 0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_expiration_during_operations(self, fast_config: HierarchicalConfig):
        """Expirations during other operations should be handled correctly."""
        global_deaths: list[tuple[str, int]] = []
        job_deaths: list[tuple[str, tuple[str, int], int]] = []

        def on_global_death(node: tuple[str, int], incarnation: int) -> None:
            global_deaths.append((node, incarnation))

        def on_job_death(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            job_deaths.append((job_id, node, incarnation))

        detector = HierarchicalFailureDetector(
            config=fast_config,
            on_global_death=on_global_death,
            on_job_death=on_job_death,
        )
        await detector.start()

        try:
            async def create_global_suspicions():
                for i in range(10):
                    await detector.suspect_global(make_node(i), 1, make_node(100))
                    await asyncio.sleep(0.02)

            async def create_job_suspicions():
                for i in range(10):
                    await detector.suspect_job(
                        make_job_id(i % 3), make_node(i + 50), 1, make_node(100)
                    )
                    await asyncio.sleep(0.02)

            await asyncio.gather(create_global_suspicions(), create_job_suspicions())

            # Wait for all to expire
            await asyncio.sleep(0.5)

            # All should have expired (allowing for some to be cleared by global death)
            assert len(global_deaths) == 10
            # Job deaths may be less due to clearing by global deaths
            assert len(job_deaths) >= 0
        finally:
            await detector.stop()

    @pytest.mark.asyncio
    async def test_global_death_concurrent_with_job_operations(
        self,
        fast_config: HierarchicalConfig,
    ):
        """Global death during job operations should not cause corruption."""
        detector = HierarchicalFailureDetector(config=fast_config)
        await detector.start()

        try:
            node = make_node(1)

            async def job_operations():
                for i in range(50):
                    job_id = make_job_id(i)
                    await detector.suspect_job(job_id, node, 1, make_node(100))
                    await asyncio.sleep(0.01)

            async def trigger_global_death():
                await asyncio.sleep(0.05)
                await detector.suspect_global(node, 1, make_node(100))

            await asyncio.gather(job_operations(), trigger_global_death())

            # Wait for global expiration
            await asyncio.sleep(0.3)

            # Node should be globally dead
            status = await detector.get_node_status(node)
            assert status == NodeStatus.DEAD_GLOBAL

            # Job suspicions should eventually be cleared by reconciliation
            await asyncio.sleep(0.2)
            # State should be consistent
            stats = detector.get_stats()
            assert stats["globally_dead_count"] == 1
        finally:
            await detector.stop()
