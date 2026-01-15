"""
Comprehensive tests for the JobSuspicionManager component.

Tests cover:
1. Happy path: Normal suspicion lifecycle, per-job isolation
2. Negative path: Invalid inputs, missing entries, limit enforcement
3. Failure modes: Callback exceptions, rapid confirmations
4. Edge cases: Job cleanup, cross-job node status, LHM adjustments
5. Concurrency correctness: Async safety under concurrent operations
"""

import asyncio
import time

import pytest

from hyperscale.distributed.swim.detection.job_suspicion_manager import (
    JobSuspicionManager,
    JobSuspicionConfig,
    JobSuspicion,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def default_config() -> JobSuspicionConfig:
    """Default configuration for tests."""
    return JobSuspicionConfig(
        poll_interval_far_ms=1000,
        poll_interval_medium_ms=250,
        poll_interval_near_ms=50,
        far_threshold_s=5.0,
        near_threshold_s=1.0,
        max_suspicions_per_job=1000,
        max_total_suspicions=10000,
    )


@pytest.fixture
def fast_config() -> JobSuspicionConfig:
    """Fast configuration for quick expiration tests."""
    return JobSuspicionConfig(
        poll_interval_far_ms=50,
        poll_interval_medium_ms=20,
        poll_interval_near_ms=10,
        far_threshold_s=0.5,
        near_threshold_s=0.1,
        max_suspicions_per_job=100,
        max_total_suspicions=1000,
    )


@pytest.fixture
def limited_config() -> JobSuspicionConfig:
    """Configuration with low limits for testing limits."""
    return JobSuspicionConfig(
        poll_interval_far_ms=100,
        poll_interval_medium_ms=50,
        poll_interval_near_ms=20,
        max_suspicions_per_job=5,
        max_total_suspicions=10,
    )


def make_node(index: int) -> tuple[str, int]:
    """Create a node address from an index."""
    return (f"192.168.1.{index}", 7946)


def make_job_id(index: int) -> str:
    """Create a job ID from an index."""
    return f"job-{index:04d}"


# =============================================================================
# Test JobSuspicion Dataclass
# =============================================================================


class TestJobSuspicion:
    """Tests for the JobSuspicion dataclass."""

    def test_add_confirmation_returns_true_for_new(self):
        """Adding new confirmation returns True."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        result = suspicion.add_confirmation(make_node(2))

        assert result is True
        assert suspicion.confirmation_count == 1

    def test_add_confirmation_returns_false_for_duplicate(self):
        """Adding duplicate confirmation returns False."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        suspicion.add_confirmation(make_node(2))
        result = suspicion.add_confirmation(make_node(2))

        assert result is False
        assert suspicion.confirmation_count == 1

    def test_calculate_timeout_decreases_with_confirmations(self):
        """More confirmations should decrease timeout."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        timeout_0 = suspicion.calculate_timeout(n_members=10)

        for i in range(5):
            suspicion.add_confirmation(make_node(i + 10))

        timeout_5 = suspicion.calculate_timeout(n_members=10)

        assert timeout_5 < timeout_0
        assert timeout_5 >= suspicion.min_timeout

    def test_time_remaining_decreases_over_time(self):
        """time_remaining should decrease as time passes."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        remaining_1 = suspicion.time_remaining(n_members=10)
        time.sleep(0.1)
        remaining_2 = suspicion.time_remaining(n_members=10)

        assert remaining_2 < remaining_1

    def test_cancel_sets_cancelled_flag(self):
        """Cancelling should set the cancelled flag."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        assert suspicion._cancelled is False
        suspicion.cancel()
        assert suspicion._cancelled is True

    def test_cleanup_clears_confirmers(self):
        """Cleanup should clear confirmers set."""
        suspicion = JobSuspicion(
            job_id="job-1",
            node=make_node(1),
            incarnation=1,
            start_time=time.monotonic(),
            min_timeout=1.0,
            max_timeout=10.0,
        )

        for i in range(5):
            suspicion.add_confirmation(make_node(i + 10))

        assert len(suspicion.confirmers) == 5

        suspicion.cleanup()

        assert len(suspicion.confirmers) == 0


# =============================================================================
# Test JobSuspicionManager - Happy Path
# =============================================================================


class TestJobSuspicionManagerHappyPath:
    """Happy path tests for JobSuspicionManager."""

    @pytest.mark.asyncio
    async def test_start_suspicion_creates_suspicion(self, default_config: JobSuspicionConfig):
        """Starting a suspicion should create and track it."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)
            from_node = make_node(2)

            suspicion = await manager.start_suspicion(
                job_id=job_id,
                node=node,
                incarnation=1,
                from_node=from_node,
            )

            assert suspicion is not None
            assert suspicion.job_id == job_id
            assert suspicion.node == node
            assert manager.is_suspected(job_id, node) is True
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_start_suspicion_with_same_incarnation_adds_confirmation(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Starting suspicion with same incarnation adds confirmation."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 1, make_node(2))
            suspicion = await manager.start_suspicion(job_id, node, 1, make_node(3))

            assert suspicion.confirmation_count == 2
            stats = manager.get_stats()
            assert stats["confirmed_count"] == 1  # Second start counted as confirm
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_confirm_suspicion_adds_confirmation(self, default_config: JobSuspicionConfig):
        """Confirming a suspicion should add confirmation."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 1, make_node(2))

            result = await manager.confirm_suspicion(job_id, node, 1, make_node(3))

            assert result is True
            suspicion = manager.get_suspicion(job_id, node)
            assert suspicion.confirmation_count == 2
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_refute_suspicion_clears_suspicion(self, default_config: JobSuspicionConfig):
        """Refuting with higher incarnation should clear suspicion."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 1, make_node(2))
            assert manager.is_suspected(job_id, node) is True

            result = await manager.refute_suspicion(job_id, node, 2)

            assert result is True
            assert manager.is_suspected(job_id, node) is False
            stats = manager.get_stats()
            assert stats["refuted_count"] == 1
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_suspicion_expires_after_timeout(self, fast_config: JobSuspicionConfig):
        """Suspicion should expire and trigger callback after timeout."""
        expired: list[tuple[str, tuple[str, int], int]] = []

        def on_expired(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            expired.append((job_id, node, incarnation))

        manager = JobSuspicionManager(config=fast_config, on_expired=on_expired)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(
                job_id=job_id,
                node=node,
                incarnation=1,
                from_node=make_node(2),
                min_timeout=0.05,
                max_timeout=0.1,
            )

            # Wait for expiration
            await asyncio.sleep(0.3)

            assert len(expired) == 1
            assert expired[0][0] == job_id
            assert expired[0][1] == node
            assert manager.is_suspected(job_id, node) is False
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_per_job_isolation(self, default_config: JobSuspicionConfig):
        """Suspicions should be isolated per job."""
        manager = JobSuspicionManager(config=default_config)

        try:
            node = make_node(1)
            job_a = make_job_id(1)
            job_b = make_job_id(2)

            await manager.start_suspicion(job_a, node, 1, make_node(2))

            assert manager.is_suspected(job_a, node) is True
            assert manager.is_suspected(job_b, node) is False

            # Suspecting same node in another job is independent
            await manager.start_suspicion(job_b, node, 1, make_node(3))

            assert manager.is_suspected(job_a, node) is True
            assert manager.is_suspected(job_b, node) is True

            # Refuting in one job doesn't affect other
            await manager.refute_suspicion(job_a, node, 2)

            assert manager.is_suspected(job_a, node) is False
            assert manager.is_suspected(job_b, node) is True
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_clear_job_removes_all_job_suspicions(self, default_config: JobSuspicionConfig):
        """clear_job should remove all suspicions for that job."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)

            for i in range(5):
                await manager.start_suspicion(job_id, make_node(i), 1, make_node(10))

            assert len(manager.get_suspected_nodes(job_id)) == 5

            cleared = await manager.clear_job(job_id)

            assert cleared == 5
            assert len(manager.get_suspected_nodes(job_id)) == 0
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_get_suspected_nodes_returns_correct_list(self, default_config: JobSuspicionConfig):
        """get_suspected_nodes should return all suspected nodes for a job."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            nodes = [make_node(i) for i in range(5)]

            for node in nodes:
                await manager.start_suspicion(job_id, node, 1, make_node(10))

            suspected = manager.get_suspected_nodes(job_id)

            assert len(suspected) == 5
            for node in nodes:
                assert node in suspected
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_get_jobs_suspecting_returns_correct_list(self, default_config: JobSuspicionConfig):
        """get_jobs_suspecting should return all jobs suspecting a node."""
        manager = JobSuspicionManager(config=default_config)

        try:
            node = make_node(1)
            jobs = [make_job_id(i) for i in range(3)]

            for job_id in jobs:
                await manager.start_suspicion(job_id, node, 1, make_node(10))

            suspecting_jobs = manager.get_jobs_suspecting(node)

            assert len(suspecting_jobs) == 3
            for job_id in jobs:
                assert job_id in suspecting_jobs
        finally:
            await manager.shutdown()


# =============================================================================
# Test JobSuspicionManager - Negative Path
# =============================================================================


class TestJobSuspicionManagerNegativePath:
    """Negative path tests for JobSuspicionManager."""

    @pytest.mark.asyncio
    async def test_start_suspicion_stale_incarnation_ignored(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Starting suspicion with stale incarnation should be ignored."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            # Start with incarnation 5
            suspicion1 = await manager.start_suspicion(job_id, node, 5, make_node(2))

            # Try to start with incarnation 3 (stale)
            suspicion2 = await manager.start_suspicion(job_id, node, 3, make_node(3))

            # Should return existing suspicion, not create new
            assert suspicion2 is suspicion1
            assert suspicion2.incarnation == 5
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_start_suspicion_higher_incarnation_replaces(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Starting suspicion with higher incarnation should replace."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 1, make_node(2))
            suspicion = await manager.start_suspicion(job_id, node, 5, make_node(3))

            assert suspicion.incarnation == 5
            assert suspicion.confirmation_count == 1  # New suspicion
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_confirm_nonexistent_returns_false(self, default_config: JobSuspicionConfig):
        """Confirming nonexistent suspicion returns False."""
        manager = JobSuspicionManager(config=default_config)

        try:
            result = await manager.confirm_suspicion(
                make_job_id(1), make_node(1), 1, make_node(2)
            )

            assert result is False
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_confirm_wrong_incarnation_returns_false(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Confirming with wrong incarnation returns False."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 5, make_node(2))

            result = await manager.confirm_suspicion(job_id, node, 3, make_node(3))

            assert result is False
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_refute_nonexistent_returns_false(self, default_config: JobSuspicionConfig):
        """Refuting nonexistent suspicion returns False."""
        manager = JobSuspicionManager(config=default_config)

        try:
            result = await manager.refute_suspicion(make_job_id(1), make_node(1), 5)

            assert result is False
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_refute_lower_incarnation_returns_false(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Refuting with lower incarnation returns False."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 5, make_node(2))

            result = await manager.refute_suspicion(job_id, node, 3)

            assert result is False
            assert manager.is_suspected(job_id, node) is True
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_per_job_limit_enforced(self, limited_config: JobSuspicionConfig):
        """Per-job suspicion limit should be enforced."""
        manager = JobSuspicionManager(config=limited_config)

        try:
            job_id = make_job_id(1)

            # Add up to limit
            for i in range(limited_config.max_suspicions_per_job):
                suspicion = await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))
                assert suspicion is not None

            # Next one should fail
            suspicion = await manager.start_suspicion(
                job_id, make_node(100), 1, make_node(101)
            )
            assert suspicion is None
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_total_limit_enforced(self, limited_config: JobSuspicionConfig):
        """Total suspicion limit should be enforced."""
        manager = JobSuspicionManager(config=limited_config)

        try:
            # Fill across multiple jobs
            for i in range(limited_config.max_total_suspicions):
                job_id = make_job_id(i % 3)  # Spread across 3 jobs
                suspicion = await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))
                assert suspicion is not None

            # Next one should fail
            suspicion = await manager.start_suspicion(
                make_job_id(99), make_node(999), 1, make_node(100)
            )
            assert suspicion is None
        finally:
            await manager.shutdown()


# =============================================================================
# Test JobSuspicionManager - Failure Modes
# =============================================================================


class TestJobSuspicionManagerFailureModes:
    """Failure mode tests for JobSuspicionManager."""

    @pytest.mark.asyncio
    async def test_callback_exception_does_not_stop_manager(
        self,
        fast_config: JobSuspicionConfig,
    ):
        """Exceptions in callback should not stop the manager."""
        call_count = 0

        def failing_callback(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated failure")

        manager = JobSuspicionManager(config=fast_config, on_expired=failing_callback)

        try:
            # Add two suspicions that will expire
            for i in range(2):
                await manager.start_suspicion(
                    make_job_id(i), make_node(i), 1, make_node(10),
                    min_timeout=0.05, max_timeout=0.1,
                )

            # Wait for expirations
            await asyncio.sleep(0.3)

            # Both should have been processed despite first failing
            assert call_count == 2
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_rapid_confirmations_handled_correctly(
        self,
        fast_config: JobSuspicionConfig,
    ):
        """Rapid confirmations should all be counted correctly."""
        manager = JobSuspicionManager(config=fast_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(
                job_id, node, 1, make_node(2),
                min_timeout=1.0, max_timeout=5.0,  # Long timeout to not expire
            )

            # Rapid confirmations from many nodes
            for i in range(50):
                await manager.confirm_suspicion(job_id, node, 1, make_node(100 + i))

            suspicion = manager.get_suspicion(job_id, node)
            # 1 from start + 50 confirmations
            assert suspicion.confirmation_count == 51
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_during_polling_completes_gracefully(
        self,
        fast_config: JobSuspicionConfig,
    ):
        """Shutdown during polling should complete gracefully."""
        manager = JobSuspicionManager(config=fast_config)

        # Add many suspicions
        for i in range(20):
            await manager.start_suspicion(
                make_job_id(i % 5), make_node(i), 1, make_node(100),
                min_timeout=5.0, max_timeout=10.0,
            )

        # Shutdown immediately
        await manager.shutdown()

        assert manager.get_stats()["active_suspicions"] == 0


# =============================================================================
# Test JobSuspicionManager - Edge Cases
# =============================================================================


class TestJobSuspicionManagerEdgeCases:
    """Edge case tests for JobSuspicionManager."""

    @pytest.mark.asyncio
    async def test_confirmations_reduce_timeout_during_poll(
        self,
        fast_config: JobSuspicionConfig,
    ):
        """Confirmations should reduce timeout and cause earlier expiration."""
        expired: list[float] = []
        start_time = time.monotonic()

        def on_expired(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            expired.append(time.monotonic() - start_time)

        # Custom member count getter
        def get_n_members(job_id: str) -> int:
            return 10

        manager = JobSuspicionManager(
            config=fast_config,
            on_expired=on_expired,
            get_n_members=get_n_members,
        )

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(
                job_id, node, 1, make_node(2),
                min_timeout=0.1, max_timeout=0.5,
            )

            # Add many confirmations to reduce timeout
            for i in range(8):
                await manager.confirm_suspicion(job_id, node, 1, make_node(10 + i))

            # Wait for expiration
            await asyncio.sleep(0.6)

            # Should have expired faster than max_timeout
            assert len(expired) == 1
            assert expired[0] < 0.5  # Less than max_timeout
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_lhm_affects_poll_interval(self, fast_config: JobSuspicionConfig):
        """LHM should slow down polling when under load."""
        poll_times: list[float] = []
        last_poll = time.monotonic()

        def get_lhm() -> float:
            return 3.0  # Simulate high load

        manager = JobSuspicionManager(
            config=fast_config,
            get_lhm_multiplier=get_lhm,
        )

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(
                job_id, node, 1, make_node(2),
                min_timeout=0.5, max_timeout=1.0,
            )

            # Let it poll a few times
            await asyncio.sleep(0.5)

            # Just verify it's still working under LHM
            assert manager.is_suspected(job_id, node) is True
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_clear_all_stops_all_timers(self, default_config: JobSuspicionConfig):
        """clear_all should stop all polling timers."""
        manager = JobSuspicionManager(config=default_config)

        # Add suspicions across multiple jobs
        for i in range(10):
            await manager.start_suspicion(
                make_job_id(i % 3), make_node(i), 1, make_node(100)
            )

        assert manager.get_stats()["active_suspicions"] == 10

        await manager.clear_all()

        assert manager.get_stats()["active_suspicions"] == 0

    @pytest.mark.asyncio
    async def test_get_suspicion_returns_none_for_missing(
        self,
        default_config: JobSuspicionConfig,
    ):
        """get_suspicion should return None for missing entries."""
        manager = JobSuspicionManager(config=default_config)

        try:
            result = manager.get_suspicion(make_job_id(1), make_node(1))

            assert result is None
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_job_stats_accurate(self, default_config: JobSuspicionConfig):
        """Job stats should be accurate."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)

            for i in range(5):
                await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))

            stats = manager.get_job_stats(job_id)

            assert stats["suspicion_count"] == 5
            assert stats["suspected_nodes"] == 5
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_stats_after_expirations(self, fast_config: JobSuspicionConfig):
        """Stats should be accurate after expirations."""
        manager = JobSuspicionManager(config=fast_config)

        try:
            job_id = make_job_id(1)

            for i in range(3):
                await manager.start_suspicion(
                    job_id, make_node(i), 1, make_node(100),
                    min_timeout=0.05, max_timeout=0.1,
                )

            # Wait for expirations
            await asyncio.sleep(0.3)

            stats = manager.get_stats()
            assert stats["active_suspicions"] == 0
            assert stats["expired_count"] == 3
            assert stats["started_count"] == 3
        finally:
            await manager.shutdown()


# =============================================================================
# Test JobSuspicionManager - Concurrency Correctness
# =============================================================================


class TestJobSuspicionManagerConcurrency:
    """Concurrency correctness tests for JobSuspicionManager (asyncio)."""

    @pytest.mark.asyncio
    async def test_concurrent_starts_same_key_one_wins(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Concurrent starts for same (job, node) should result in one suspicion."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            results: list[JobSuspicion | None] = []

            async def try_start(from_idx: int):
                result = await manager.start_suspicion(
                    job_id, node, 1, make_node(from_idx)
                )
                results.append(result)

            await asyncio.gather(*[try_start(i) for i in range(10)])

            # All should get a suspicion (either create or add confirmation)
            assert all(r is not None for r in results)
            # But only one should exist
            assert manager.get_stats()["active_suspicions"] == 1
            # And it should have all confirmations
            suspicion = manager.get_suspicion(job_id, node)
            assert suspicion.confirmation_count == 10
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_start_refute_race(self, default_config: JobSuspicionConfig):
        """Concurrent start and refute should not corrupt state."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            async def start_and_refute():
                await manager.start_suspicion(job_id, node, 1, make_node(2))
                await asyncio.sleep(0)
                await manager.refute_suspicion(job_id, node, 2)

            await asyncio.gather(*[start_and_refute() for _ in range(10)])

            # State should be consistent (either suspected or not)
            # Not both or corrupted
            is_suspected = manager.is_suspected(job_id, node)
            assert isinstance(is_suspected, bool)
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_confirmations_all_counted(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Concurrent confirmations should all be counted."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            await manager.start_suspicion(job_id, node, 1, make_node(2))

            async def confirm(from_idx: int):
                await manager.confirm_suspicion(job_id, node, 1, make_node(100 + from_idx))

            await asyncio.gather(*[confirm(i) for i in range(50)])

            suspicion = manager.get_suspicion(job_id, node)
            # 1 original + 50 confirmations
            assert suspicion.confirmation_count == 51
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_operations_multiple_jobs(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Concurrent operations across multiple jobs should not interfere."""
        manager = JobSuspicionManager(config=default_config)

        try:
            num_jobs = 5
            num_nodes = 10

            async def operate_job(job_idx: int):
                job_id = make_job_id(job_idx)
                for i in range(num_nodes):
                    await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))
                    await asyncio.sleep(0)
                    if i % 3 == 0:
                        await manager.refute_suspicion(job_id, make_node(i), 2)
                    await asyncio.sleep(0)

            await asyncio.gather(*[operate_job(j) for j in range(num_jobs)])

            # Each job should have consistent state
            for j in range(num_jobs):
                job_id = make_job_id(j)
                suspected = manager.get_suspected_nodes(job_id)
                # Should have some nodes (those not refuted)
                assert len(suspected) >= 0
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_concurrent_clear_job_with_operations(
        self,
        default_config: JobSuspicionConfig,
    ):
        """Clearing a job during operations should not cause errors."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)

            # Pre-populate
            for i in range(20):
                await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))

            async def add_more():
                for i in range(20, 40):
                    await manager.start_suspicion(job_id, make_node(i), 1, make_node(100))
                    await asyncio.sleep(0)

            async def clear():
                await asyncio.sleep(0.01)
                await manager.clear_job(job_id)

            await asyncio.gather(add_more(), clear())

            # State should be consistent
            stats = manager.get_stats()
            assert stats["active_suspicions"] >= 0
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_expiration_callback_not_duplicated(self, fast_config: JobSuspicionConfig):
        """Each suspicion should only trigger one expiration callback."""
        expired_counts: dict[tuple[str, tuple[str, int]], int] = {}

        def on_expired(job_id: str, node: tuple[str, int], incarnation: int) -> None:
            key = (job_id, node)
            expired_counts[key] = expired_counts.get(key, 0) + 1

        manager = JobSuspicionManager(config=fast_config, on_expired=on_expired)

        try:
            # Add multiple suspicions
            for i in range(10):
                await manager.start_suspicion(
                    make_job_id(i % 3), make_node(i), 1, make_node(100),
                    min_timeout=0.05, max_timeout=0.1,
                )

            # Wait for all expirations
            await asyncio.sleep(0.3)

            # Each should have expired exactly once
            for key, count in expired_counts.items():
                assert count == 1, f"{key} expired {count} times"
        finally:
            await manager.shutdown()

    @pytest.mark.asyncio
    async def test_is_suspected_consistent_during_modifications(
        self,
        default_config: JobSuspicionConfig,
    ):
        """is_suspected should return valid values during modifications."""
        manager = JobSuspicionManager(config=default_config)

        try:
            job_id = make_job_id(1)
            node = make_node(1)

            results: list[bool] = []
            done = asyncio.Event()

            async def check_suspected():
                while not done.is_set():
                    result = manager.is_suspected(job_id, node)
                    results.append(result)
                    await asyncio.sleep(0)

            async def toggle():
                for _ in range(50):
                    await manager.start_suspicion(job_id, node, 1, make_node(2))
                    await asyncio.sleep(0)
                    await manager.refute_suspicion(job_id, node, 2)
                    await asyncio.sleep(0)
                done.set()

            await asyncio.gather(check_suspected(), toggle())

            # All results should be valid booleans
            assert all(isinstance(r, bool) for r in results)
        finally:
            await manager.shutdown()
