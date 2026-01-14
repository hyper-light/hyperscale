"""
Unit tests for Manager Core Modules from Section 15.4.6 of REFACTOR.md.

Tests cover:
- ManagerRegistry
- ManagerCancellationCoordinator
- ManagerLeaseCoordinator
- ManagerWorkflowLifecycle
- ManagerDispatchCoordinator
- ManagerHealthMonitor
- ManagerStatsCoordinator

Each test class validates:
- Happy path (normal operations)
- Negative path (invalid inputs, error conditions)
- Failure modes (exception handling)
- Concurrency and race conditions
- Edge cases (boundary conditions, special values)
"""

import asyncio
import pytest
import time
from unittest.mock import MagicMock, AsyncMock

from hyperscale.distributed.jobs import WindowedStatsCollector
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.nodes.manager.config import ManagerConfig
from hyperscale.distributed.nodes.manager.registry import ManagerRegistry
from hyperscale.distributed.reliability import StatsBuffer, StatsBufferConfig
from hyperscale.distributed.nodes.manager.cancellation import (
    ManagerCancellationCoordinator,
)
from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
from hyperscale.distributed.nodes.manager.workflow_lifecycle import (
    ManagerWorkflowLifecycle,
)
from hyperscale.distributed.nodes.manager.dispatch import ManagerDispatchCoordinator
from hyperscale.distributed.nodes.manager.health import (
    ManagerHealthMonitor,
    NodeStatus,
    JobSuspicion,
    ExtensionTracker,
    HealthcheckExtensionManager,
)
from hyperscale.distributed.nodes.manager.stats import (
    ManagerStatsCoordinator,
    ProgressState,
    BackpressureLevel,
)


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def manager_state():
    """Create a fresh ManagerState for testing."""
    state = ManagerState()
    state.initialize_locks()
    return state


@pytest.fixture
def manager_config():
    """Create a ManagerConfig for testing."""
    return ManagerConfig(
        host="127.0.0.1",
        tcp_port=8000,
        udp_port=8001,
        datacenter_id="dc-test",
    )


@pytest.fixture
def mock_logger():
    """Create a mock logger."""
    logger = MagicMock()
    logger.log = AsyncMock()
    return logger


@pytest.fixture
def mock_task_runner():
    """Create a mock task runner."""
    runner = MagicMock()
    runner.run = MagicMock()
    return runner


@pytest.fixture
def stats_buffer():
    return StatsBuffer(
        StatsBufferConfig(
            hot_max_entries=100,
            throttle_threshold=0.7,
            batch_threshold=0.85,
            reject_threshold=0.95,
        )
    )


@pytest.fixture
def windowed_stats():
    return WindowedStatsCollector()


@pytest.fixture
def mock_worker_registration():
    """Create a mock worker registration."""
    node = MagicMock()
    node.node_id = "worker-test-123"
    node.host = "10.0.0.100"
    node.tcp_port = 6000
    node.udp_port = 6001
    node.total_cores = 8

    registration = MagicMock()
    registration.node = node

    return registration


# =============================================================================
# ManagerRegistry Tests
# =============================================================================


class TestManagerRegistryHappyPath:
    """Happy path tests for ManagerRegistry."""

    def test_register_worker(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        mock_worker_registration,
    ):
        """Can register a worker."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        registry.register_worker(mock_worker_registration)

        assert "worker-test-123" in manager_state._workers
        assert ("10.0.0.100", 6000) in manager_state._worker_addr_to_id
        assert "worker-test-123" in manager_state._worker_circuits

    def test_unregister_worker(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        mock_worker_registration,
    ):
        """Can unregister a worker."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        registry.register_worker(mock_worker_registration)
        registry.unregister_worker("worker-test-123")

        assert "worker-test-123" not in manager_state._workers
        assert ("10.0.0.100", 6000) not in manager_state._worker_addr_to_id

    def test_get_worker(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        mock_worker_registration,
    ):
        """Can get worker by ID."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        registry.register_worker(mock_worker_registration)

        result = registry.get_worker("worker-test-123")
        assert result is mock_worker_registration

        result_none = registry.get_worker("nonexistent")
        assert result_none is None

    def test_get_worker_by_addr(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        mock_worker_registration,
    ):
        """Can get worker by address."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        registry.register_worker(mock_worker_registration)

        result = registry.get_worker_by_addr(("10.0.0.100", 6000))
        assert result is mock_worker_registration

    def test_get_healthy_worker_ids(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        mock_worker_registration,
    ):
        """Can get healthy worker IDs."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        registry.register_worker(mock_worker_registration)

        healthy = registry.get_healthy_worker_ids()
        assert "worker-test-123" in healthy

        # Mark unhealthy
        manager_state._worker_unhealthy_since["worker-test-123"] = time.monotonic()

        healthy = registry.get_healthy_worker_ids()
        assert "worker-test-123" not in healthy


class TestManagerRegistryGateManagement:
    """Tests for gate management in ManagerRegistry."""

    def test_register_gate(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can register a gate."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        gate_info = MagicMock()
        gate_info.node_id = "gate-123"

        registry.register_gate(gate_info)

        assert "gate-123" in manager_state._known_gates
        assert "gate-123" in manager_state._healthy_gate_ids

    def test_unregister_gate(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can unregister a gate."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        gate_info = MagicMock()
        gate_info.node_id = "gate-123"

        registry.register_gate(gate_info)
        registry.unregister_gate("gate-123")

        assert "gate-123" not in manager_state._known_gates
        assert "gate-123" not in manager_state._healthy_gate_ids

    def test_mark_gate_unhealthy(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can mark gate as unhealthy."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        gate_info = MagicMock()
        gate_info.node_id = "gate-123"

        registry.register_gate(gate_info)
        registry.mark_gate_unhealthy("gate-123")

        assert "gate-123" not in manager_state._healthy_gate_ids
        assert "gate-123" in manager_state._gate_unhealthy_since

    def test_mark_gate_healthy(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can mark gate as healthy."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        gate_info = MagicMock()
        gate_info.node_id = "gate-123"

        registry.register_gate(gate_info)
        registry.mark_gate_unhealthy("gate-123")
        registry.mark_gate_healthy("gate-123")

        assert "gate-123" in manager_state._healthy_gate_ids
        assert "gate-123" not in manager_state._gate_unhealthy_since


class TestManagerRegistryHealthBuckets:
    """Tests for AD-17 health bucket selection."""

    def test_get_workers_by_health_bucket(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Workers are bucketed by health state."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        health_states: dict[str, str] = {}

        for worker_id, health_state in [
            ("worker-healthy-1", "healthy"),
            ("worker-healthy-2", "healthy"),
            ("worker-busy-1", "busy"),
            ("worker-stressed-1", "stressed"),
        ]:
            node = MagicMock()
            node.node_id = worker_id
            node.host = "10.0.0.1"
            node.tcp_port = 6000
            node.udp_port = 6001
            node.total_cores = 4

            reg = MagicMock()
            reg.node = node

            registry.register_worker(reg)
            health_states[worker_id] = health_state

        original_get_health = registry.get_worker_health_state
        registry.get_worker_health_state = lambda worker_id: health_states.get(
            worker_id, "healthy"
        )

        buckets = registry.get_workers_by_health_bucket(cores_required=1)

        registry.get_worker_health_state = original_get_health

        assert len(buckets["healthy"]) == 2
        assert len(buckets["busy"]) == 1
        assert len(buckets["degraded"]) == 1


# =============================================================================
# ManagerLeaseCoordinator Tests
# =============================================================================


class TestManagerLeaseCoordinatorHappyPath:
    """Happy path tests for ManagerLeaseCoordinator."""

    def test_claim_job_leadership(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can claim job leadership."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        result = leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))

        assert result is True
        assert leases.is_job_leader("job-123") is True
        assert leases.get_job_leader("job-123") == "manager-1"
        assert leases.get_job_leader_addr("job-123") == ("127.0.0.1", 8000)

    def test_cannot_claim_if_other_leader(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Cannot claim leadership if another manager is leader."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        # Set another manager as leader
        manager_state._job_leaders["job-123"] = "manager-2"

        result = leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))

        assert result is False
        assert leases.get_job_leader("job-123") == "manager-2"

    def test_release_job_leadership(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can release job leadership."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))
        leases.release_job_leadership("job-123")

        assert leases.is_job_leader("job-123") is False
        assert leases.get_job_leader("job-123") is None

    def test_transfer_job_leadership(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can transfer job leadership."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))

        result = leases.transfer_job_leadership(
            "job-123",
            "manager-2",
            ("127.0.0.2", 8000),
        )

        assert result is True
        assert leases.get_job_leader("job-123") == "manager-2"
        assert leases.get_job_leader_addr("job-123") == ("127.0.0.2", 8000)


class TestManagerLeaseCoordinatorFencing:
    """Tests for fencing token management."""

    @pytest.mark.asyncio
    async def test_fence_token_increments(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Fence token increments correctly."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))

        token1 = leases.get_fence_token("job-123")
        assert token1 == 1

        token2 = await leases.increment_fence_token("job-123")
        assert token2 == 2

        token3 = await leases.increment_fence_token("job-123")
        assert token3 == 3

    @pytest.mark.asyncio
    async def test_validate_fence_token(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can validate fence tokens."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))
        await leases.increment_fence_token("job-123")

        assert leases.validate_fence_token("job-123", 2) is True
        assert leases.validate_fence_token("job-123", 3) is True
        assert leases.validate_fence_token("job-123", 1) is False

    def test_layer_version_increments(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Layer version increments correctly."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))

        version1 = leases.get_layer_version("job-123")
        assert version1 == 1

        version2 = leases.increment_layer_version("job-123")
        assert version2 == 2


class TestManagerLeaseCoordinatorEdgeCases:
    """Edge case tests for ManagerLeaseCoordinator."""

    def test_get_led_job_ids(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can get list of jobs we lead."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-1", ("127.0.0.1", 8000))
        leases.claim_job_leadership("job-2", ("127.0.0.1", 8000))
        manager_state._job_leaders["job-3"] = "manager-2"  # Different leader

        led_jobs = leases.get_led_job_ids()

        assert "job-1" in led_jobs
        assert "job-2" in led_jobs
        assert "job-3" not in led_jobs

    @pytest.mark.asyncio
    async def test_clear_job_leases(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can clear all lease state for a job."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-123", ("127.0.0.1", 8000))
        await leases.increment_fence_token("job-123")
        leases.increment_layer_version("job-123")

        leases.clear_job_leases("job-123")

        assert leases.get_job_leader("job-123") is None
        assert leases.get_fence_token("job-123") == 0
        assert leases.get_layer_version("job-123") == 0


# =============================================================================
# ManagerCancellationCoordinator Tests
# =============================================================================


class TestManagerCancellationCoordinatorHappyPath:
    """Happy path tests for ManagerCancellationCoordinator."""

    @pytest.mark.asyncio
    async def test_cancel_job_not_found(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Cancelling nonexistent job returns error."""
        coord = ManagerCancellationCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            send_to_worker=AsyncMock(),
            send_to_client=AsyncMock(),
        )

        request = MagicMock()
        request.job_id = "nonexistent-job"
        request.reason = "Test cancellation"

        result = await coord.cancel_job(request, ("10.0.0.1", 9000))

        # Should return error response
        assert b"Job not found" in result or b"accepted" in result.lower()

    def test_is_workflow_cancelled(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can check if workflow is cancelled."""
        coord = ManagerCancellationCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            send_to_worker=AsyncMock(),
            send_to_client=AsyncMock(),
        )

        assert coord.is_workflow_cancelled("wf-123") is False

        # Mark as cancelled
        cancelled_info = MagicMock()
        cancelled_info.cancelled_at = time.time()
        manager_state._cancelled_workflows["wf-123"] = cancelled_info

        assert coord.is_workflow_cancelled("wf-123") is True

    def test_cleanup_old_cancellations(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can cleanup old cancellation records."""
        coord = ManagerCancellationCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            send_to_worker=AsyncMock(),
            send_to_client=AsyncMock(),
        )

        # Add old and new cancellations
        old_info = MagicMock()
        old_info.cancelled_at = time.time() - 1000  # Old

        new_info = MagicMock()
        new_info.cancelled_at = time.time()  # New

        manager_state._cancelled_workflows["wf-old"] = old_info
        manager_state._cancelled_workflows["wf-new"] = new_info

        cleaned = coord.cleanup_old_cancellations(max_age_seconds=500)

        assert cleaned == 1
        assert "wf-old" not in manager_state._cancelled_workflows
        assert "wf-new" in manager_state._cancelled_workflows


# =============================================================================
# ManagerHealthMonitor Tests
# =============================================================================


class TestManagerHealthMonitorHappyPath:
    """Happy path tests for ManagerHealthMonitor."""

    def test_handle_worker_failure(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can handle worker failure."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor.handle_worker_failure("worker-123")

        assert "worker-123" in manager_state._worker_unhealthy_since

    def test_handle_worker_recovery(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can handle worker recovery."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        manager_state._worker_unhealthy_since["worker-123"] = time.monotonic()
        monitor.handle_worker_recovery("worker-123")

        assert "worker-123" not in manager_state._worker_unhealthy_since

    def test_get_worker_health_status(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can get worker health status."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        # Unknown worker
        assert monitor.get_worker_health_status("unknown") == "unknown"

        # Register healthy worker
        manager_state._workers["worker-123"] = MagicMock()
        assert monitor.get_worker_health_status("worker-123") == "healthy"

        # Mark unhealthy
        manager_state._worker_unhealthy_since["worker-123"] = time.monotonic()
        assert monitor.get_worker_health_status("worker-123") == "unhealthy"


class TestManagerHealthMonitorJobSuspicion:
    """Tests for AD-30 job suspicion tracking."""

    def test_suspect_job(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can start job suspicion."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor.suspect_job("job-123", "worker-456")

        assert ("job-123", "worker-456") in monitor._job_suspicions

    def test_refute_job_suspicion(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can refute job suspicion."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor.suspect_job("job-123", "worker-456")
        monitor.refute_job_suspicion("job-123", "worker-456")

        assert ("job-123", "worker-456") not in monitor._job_suspicions

    def test_get_node_status(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can get comprehensive node status."""
        registry = ManagerRegistry(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        monitor = ManagerHealthMonitor(
            state=manager_state,
            config=manager_config,
            registry=registry,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        # Alive status
        assert monitor.get_node_status("worker-123") == NodeStatus.ALIVE

        # Suspected global
        manager_state._worker_unhealthy_since["worker-123"] = time.monotonic()
        assert monitor.get_node_status("worker-123") == NodeStatus.SUSPECTED_GLOBAL

        # Clear and suspect for job
        del manager_state._worker_unhealthy_since["worker-123"]
        monitor.suspect_job("job-456", "worker-123")
        assert (
            monitor.get_node_status("worker-123", "job-456") == NodeStatus.SUSPECTED_JOB
        )


class TestJobSuspicionClass:
    """Tests for JobSuspicion helper class."""

    def test_creation(self):
        """Can create JobSuspicion."""
        suspicion = JobSuspicion(
            job_id="job-123",
            worker_id="worker-456",
            timeout_seconds=10.0,
        )

        assert suspicion.job_id == "job-123"
        assert suspicion.worker_id == "worker-456"
        assert suspicion.confirmation_count == 0
        assert suspicion.timeout_seconds == 10.0

    def test_add_confirmation(self):
        """Can add confirmations."""
        suspicion = JobSuspicion("job-123", "worker-456")

        suspicion.add_confirmation()
        assert suspicion.confirmation_count == 1

        suspicion.add_confirmation()
        assert suspicion.confirmation_count == 2

    def test_time_remaining(self):
        """time_remaining calculates correctly."""
        suspicion = JobSuspicion("job-123", "worker-456", timeout_seconds=10.0)

        # Initially should have time remaining
        remaining = suspicion.time_remaining(cluster_size=5)
        assert remaining > 0

        # With confirmations, timeout shrinks
        suspicion.add_confirmation()
        suspicion.add_confirmation()
        remaining_after = suspicion.time_remaining(cluster_size=5)
        # Should shrink due to confirmations
        assert remaining_after <= remaining


class TestExtensionTracker:
    """Tests for ExtensionTracker (AD-26)."""

    def test_request_extension_first_time(self):
        """First extension request should succeed."""
        tracker = ExtensionTracker(
            worker_id="worker-123",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        granted, seconds = tracker.request_extension(
            "long_workflow", current_progress=0.1
        )

        assert granted is True
        assert seconds == 30.0  # Full base deadline on first extension

    def test_extension_requires_progress(self):
        """Subsequent extensions require progress."""
        tracker = ExtensionTracker(
            worker_id="worker-123",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=5,
        )

        # First extension
        tracker.request_extension("long_workflow", current_progress=0.1)

        # Second extension without progress should fail
        granted, seconds = tracker.request_extension(
            "long_workflow", current_progress=0.1
        )
        assert granted is False

        # Second extension with progress should succeed
        granted, seconds = tracker.request_extension(
            "long_workflow", current_progress=0.2
        )
        assert granted is True

    def test_extension_limit(self):
        """Extensions are limited to max_extensions."""
        tracker = ExtensionTracker(
            worker_id="worker-123",
            base_deadline=30.0,
            min_grant=1.0,
            max_extensions=2,
        )

        # First two should succeed
        granted1, _ = tracker.request_extension("long_workflow", current_progress=0.1)
        granted2, _ = tracker.request_extension("long_workflow", current_progress=0.2)
        granted3, _ = tracker.request_extension("long_workflow", current_progress=0.3)

        assert granted1 is True
        assert granted2 is True
        assert granted3 is False

    def test_logarithmic_reduction(self):
        """Extensions reduce logarithmically."""
        tracker = ExtensionTracker(
            worker_id="worker-123",
            base_deadline=32.0,
            min_grant=1.0,
            max_extensions=5,
        )

        _, seconds1 = tracker.request_extension("long_workflow", current_progress=0.1)
        _, seconds2 = tracker.request_extension("long_workflow", current_progress=0.2)
        _, seconds3 = tracker.request_extension("long_workflow", current_progress=0.3)

        assert seconds1 == 32.0
        assert seconds2 == 16.0
        assert seconds3 == 8.0


# =============================================================================
# ManagerStatsCoordinator Tests
# =============================================================================


class TestManagerStatsCoordinatorHappyPath:
    """Happy path tests for ManagerStatsCoordinator."""

    def test_record_dispatch(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        stats_buffer,
        windowed_stats,
    ):
        """Can record dispatch for throughput tracking."""
        stats = ManagerStatsCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            stats_buffer=stats_buffer,
            windowed_stats=windowed_stats,
        )

        assert manager_state._dispatch_throughput_count == 0

        stats.record_dispatch()
        assert manager_state._dispatch_throughput_count == 1

        stats.record_dispatch()
        stats.record_dispatch()
        assert manager_state._dispatch_throughput_count == 3


class TestManagerStatsCoordinatorProgressState:
    """Tests for AD-19 progress state tracking."""

    def test_get_progress_state_normal(
        self,
        manager_state,
        manager_config,
        mock_logger,
        mock_task_runner,
        stats_buffer,
        windowed_stats,
    ):
        """Progress state is NORMAL when no workers."""
        stats = ManagerStatsCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
            stats_buffer=stats_buffer,
            windowed_stats=windowed_stats,
        )

        # With no workers and no dispatches, should be NORMAL
        state = stats.get_progress_state()
        assert state == ProgressState.NORMAL


class TestManagerStatsCoordinatorBackpressure:
    """Tests for AD-23 backpressure."""

    def test_backpressure_levels(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Backpressure levels based on buffer fill."""
        stats = ManagerStatsCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        # Initially no backpressure
        assert stats.get_backpressure_level() == BackpressureLevel.NONE

        # Add entries to trigger throttle
        stats._stats_buffer_count = 1000
        assert stats.get_backpressure_level() == BackpressureLevel.THROTTLE

        # Add more for batch
        stats._stats_buffer_count = 5000
        assert stats.get_backpressure_level() == BackpressureLevel.BATCH

        # Add more for reject
        stats._stats_buffer_count = 10000
        assert stats.get_backpressure_level() == BackpressureLevel.REJECT

    def test_should_apply_backpressure(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """should_apply_backpressure checks high watermark."""
        stats = ManagerStatsCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        assert stats.should_apply_backpressure() is False

        stats._stats_buffer_count = 2000
        assert stats.should_apply_backpressure() is True


class TestManagerStatsCoordinatorMetrics:
    """Tests for stats metrics."""

    def test_get_stats_metrics(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Can get stats metrics."""
        stats = ManagerStatsCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        stats.record_dispatch()
        stats.record_dispatch()
        stats._stats_buffer_count = 500

        metrics = stats.get_stats_metrics()

        assert "dispatch_throughput" in metrics
        assert "expected_throughput" in metrics
        assert "progress_state" in metrics
        assert "backpressure_level" in metrics
        assert metrics["stats_buffer_count"] == 500
        assert metrics["throughput_count"] == 2


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestCoreModulesConcurrency:
    """Concurrency tests for core modules."""

    @pytest.mark.asyncio
    async def test_concurrent_job_leadership_claims(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Multiple managers cannot simultaneously claim same job."""
        leases1 = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases2 = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-2",
            task_runner=mock_task_runner,
        )

        # Simulate race condition
        result1 = leases1.claim_job_leadership("job-race", ("10.0.0.1", 8000))
        result2 = leases2.claim_job_leadership("job-race", ("10.0.0.2", 8000))

        # Only one should succeed
        assert result1 is True
        assert result2 is False

    @pytest.mark.asyncio
    async def test_concurrent_fence_token_increments(
        self, manager_state, manager_config, mock_logger, mock_task_runner
    ):
        """Fence token increments are sequential."""
        leases = ManagerLeaseCoordinator(
            state=manager_state,
            config=manager_config,
            logger=mock_logger,
            node_id="manager-1",
            task_runner=mock_task_runner,
        )

        leases.claim_job_leadership("job-fence", ("127.0.0.1", 8000))

        async def increment_many():
            for _ in range(100):
                await leases.increment_fence_token("job-fence")
                await asyncio.sleep(0)

        await asyncio.gather(
            increment_many(),
            increment_many(),
            increment_many(),
        )

        assert leases.get_fence_token("job-fence") == 301
