"""
Unit tests for Manager Configuration and State from Section 15.4.3 and 15.4.4 of REFACTOR.md.

Tests cover:
- ManagerConfig dataclass
- create_manager_config_from_env factory function
- ManagerState class

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
from unittest.mock import MagicMock

from hyperscale.distributed.nodes.manager.config import (
    ManagerConfig,
    create_manager_config_from_env,
)
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.models import ManagerState as ManagerStateEnum


# =============================================================================
# ManagerConfig Tests
# =============================================================================


class TestManagerConfigHappyPath:
    """Happy path tests for ManagerConfig."""

    def test_create_with_required_fields(self):
        """Create ManagerConfig with required fields."""
        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.host == "127.0.0.1"
        assert config.tcp_port == 8000
        assert config.udp_port == 8001

    def test_default_values(self):
        """Check default values for optional fields."""
        config = ManagerConfig(
            host="10.0.0.1",
            tcp_port=9000,
            udp_port=9001,
        )

        # Network
        assert config.datacenter_id == "default"
        assert config.seed_gates == []
        assert config.gate_udp_addrs == []
        assert config.seed_managers == []
        assert config.manager_udp_peers == []

        # Quorum
        assert config.quorum_timeout_seconds == 5.0

        # Workflow
        assert config.max_workflow_retries == 3
        assert config.workflow_timeout_seconds == 300.0

        # Dead node reaping
        assert config.dead_worker_reap_interval_seconds == 60.0
        assert config.dead_peer_reap_interval_seconds == 120.0
        assert config.dead_gate_reap_interval_seconds == 120.0

        # Cluster identity
        assert config.cluster_id == "hyperscale"
        assert config.environment_id == "default"
        assert config.mtls_strict_mode is False

    def test_custom_values(self):
        """Create ManagerConfig with custom values."""
        config = ManagerConfig(
            host="192.168.1.100",
            tcp_port=7000,
            udp_port=7001,
            datacenter_id="dc-east",
            seed_gates=[("gate-1.example.com", 6000)],
            seed_managers=[("manager-2.example.com", 7000)],
            quorum_timeout_seconds=10.0,
            max_workflow_retries=5,
            workflow_timeout_seconds=600.0,
            cluster_id="my-cluster",
            environment_id="production",
            mtls_strict_mode=True,
        )

        assert config.datacenter_id == "dc-east"
        assert config.seed_gates == [("gate-1.example.com", 6000)]
        assert config.seed_managers == [("manager-2.example.com", 7000)]
        assert config.quorum_timeout_seconds == 10.0
        assert config.max_workflow_retries == 5
        assert config.workflow_timeout_seconds == 600.0
        assert config.cluster_id == "my-cluster"
        assert config.environment_id == "production"
        assert config.mtls_strict_mode is True


class TestManagerConfigNegativePath:
    """Negative path tests for ManagerConfig."""

    def test_missing_required_fields_raises_type_error(self):
        """Missing required fields should raise TypeError."""
        with pytest.raises(TypeError):
            ManagerConfig()

        with pytest.raises(TypeError):
            ManagerConfig(host="127.0.0.1")

        with pytest.raises(TypeError):
            ManagerConfig(host="127.0.0.1", tcp_port=8000)


class TestManagerConfigEdgeCases:
    """Edge case tests for ManagerConfig."""

    def test_slots_enforced(self):
        """ManagerConfig uses slots=True."""
        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
        )

        with pytest.raises(AttributeError):
            config.arbitrary_field = "value"

    def test_zero_timeouts(self):
        """Zero timeout values should be allowed."""
        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            quorum_timeout_seconds=0.0,
            workflow_timeout_seconds=0.0,
            tcp_timeout_short_seconds=0.0,
            tcp_timeout_standard_seconds=0.0,
        )

        assert config.quorum_timeout_seconds == 0.0

    def test_very_large_values(self):
        """Very large configuration values should work."""
        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            max_workflow_retries=1_000_000,
            workflow_timeout_seconds=86400.0 * 365,  # One year
            stats_hot_max_entries=10_000_000,
        )

        assert config.max_workflow_retries == 1_000_000
        assert config.stats_hot_max_entries == 10_000_000

    def test_ipv6_host(self):
        """IPv6 host should work."""
        config = ManagerConfig(
            host="::1",
            tcp_port=8000,
            udp_port=8001,
        )

        assert config.host == "::1"

    def test_multiple_seed_addresses(self):
        """Multiple seed addresses should work."""
        gates = [
            ("gate-1.example.com", 6000),
            ("gate-2.example.com", 6001),
            ("gate-3.example.com", 6002),
        ]
        managers = [
            ("manager-1.example.com", 7000),
            ("manager-2.example.com", 7001),
        ]

        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            seed_gates=gates,
            seed_managers=managers,
        )

        assert len(config.seed_gates) == 3
        assert len(config.seed_managers) == 2

    def test_backpressure_thresholds(self):
        """AD-23 backpressure thresholds should be configurable."""
        config = ManagerConfig(
            host="127.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            stats_throttle_threshold=0.5,
            stats_batch_threshold=0.7,
            stats_reject_threshold=0.9,
        )

        assert config.stats_throttle_threshold == 0.5
        assert config.stats_batch_threshold == 0.7
        assert config.stats_reject_threshold == 0.9


class TestCreateManagerConfigFromEnv:
    """Tests for create_manager_config_from_env factory."""

    def test_creates_config_with_env_values(self):
        """Factory creates config from environment values."""
        # Create a mock Env object
        mock_env = MagicMock()
        mock_env.MANAGER_DEAD_WORKER_REAP_INTERVAL = 30.0
        mock_env.MANAGER_DEAD_PEER_REAP_INTERVAL = 60.0
        mock_env.MANAGER_DEAD_GATE_REAP_INTERVAL = 60.0
        mock_env.ORPHAN_SCAN_INTERVAL = 15.0
        mock_env.ORPHAN_SCAN_WORKER_TIMEOUT = 5.0
        mock_env.CANCELLED_WORKFLOW_TTL = 150.0
        mock_env.CANCELLED_WORKFLOW_CLEANUP_INTERVAL = 30.0
        mock_env.RECOVERY_MAX_CONCURRENT = 3
        mock_env.RECOVERY_JITTER_MIN = 0.05
        mock_env.RECOVERY_JITTER_MAX = 0.5
        mock_env.DISPATCH_MAX_CONCURRENT_PER_WORKER = 5
        mock_env.COMPLETED_JOB_MAX_AGE = 1800.0
        mock_env.FAILED_JOB_MAX_AGE = 3600.0
        mock_env.JOB_CLEANUP_INTERVAL = 30.0
        mock_env.MANAGER_DEAD_NODE_CHECK_INTERVAL = 5.0
        mock_env.MANAGER_RATE_LIMIT_CLEANUP_INTERVAL = 150.0
        mock_env.MANAGER_TCP_TIMEOUT_SHORT = 1.0
        mock_env.MANAGER_TCP_TIMEOUT_STANDARD = 3.0
        mock_env.MANAGER_BATCH_PUSH_INTERVAL = 0.5
        mock_env.JOB_RESPONSIVENESS_THRESHOLD = 15.0
        mock_env.JOB_RESPONSIVENESS_CHECK_INTERVAL = 2.5
        mock_env.DISCOVERY_FAILURE_DECAY_INTERVAL = 30.0
        mock_env.STATS_WINDOW_SIZE_MS = 500
        mock_env.STATS_DRIFT_TOLERANCE_MS = 50
        mock_env.STATS_MAX_WINDOW_AGE_MS = 2500
        mock_env.MANAGER_STATS_HOT_MAX_ENTRIES = 5000
        mock_env.MANAGER_STATS_THROTTLE_THRESHOLD = 0.6
        mock_env.MANAGER_STATS_BATCH_THRESHOLD = 0.8
        mock_env.MANAGER_STATS_REJECT_THRESHOLD = 0.9
        mock_env.STATS_PUSH_INTERVAL_MS = 500
        mock_env.MANAGER_STATE_SYNC_RETRIES = 2
        mock_env.MANAGER_STATE_SYNC_TIMEOUT = 5.0
        mock_env.LEADER_ELECTION_JITTER_MAX = 0.25
        mock_env.MANAGER_STARTUP_SYNC_DELAY = 0.5
        mock_env.CLUSTER_STABILIZATION_TIMEOUT = 15.0
        mock_env.CLUSTER_STABILIZATION_POLL_INTERVAL = 0.25
        mock_env.MANAGER_HEARTBEAT_INTERVAL = 2.5
        mock_env.MANAGER_PEER_SYNC_INTERVAL = 15.0
        mock_env.get = MagicMock(side_effect=lambda k, d=None: d)

        config = create_manager_config_from_env(
            host="10.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            env=mock_env,
            datacenter_id="dc-west",
        )

        assert config.host == "10.0.0.1"
        assert config.tcp_port == 8000
        assert config.udp_port == 8001
        assert config.datacenter_id == "dc-west"
        assert config.dead_worker_reap_interval_seconds == 30.0
        assert config.recovery_max_concurrent == 3

    def test_with_seed_addresses(self):
        """Factory accepts seed addresses."""
        mock_env = MagicMock()
        # Set all required attributes
        for attr in [
            "MANAGER_DEAD_WORKER_REAP_INTERVAL",
            "MANAGER_DEAD_PEER_REAP_INTERVAL",
            "MANAGER_DEAD_GATE_REAP_INTERVAL",
            "ORPHAN_SCAN_INTERVAL",
            "ORPHAN_SCAN_WORKER_TIMEOUT",
            "CANCELLED_WORKFLOW_TTL",
            "CANCELLED_WORKFLOW_CLEANUP_INTERVAL",
            "RECOVERY_MAX_CONCURRENT",
            "RECOVERY_JITTER_MIN",
            "RECOVERY_JITTER_MAX",
            "DISPATCH_MAX_CONCURRENT_PER_WORKER",
            "COMPLETED_JOB_MAX_AGE",
            "FAILED_JOB_MAX_AGE",
            "JOB_CLEANUP_INTERVAL",
            "MANAGER_DEAD_NODE_CHECK_INTERVAL",
            "MANAGER_RATE_LIMIT_CLEANUP_INTERVAL",
            "MANAGER_TCP_TIMEOUT_SHORT",
            "MANAGER_TCP_TIMEOUT_STANDARD",
            "MANAGER_BATCH_PUSH_INTERVAL",
            "JOB_RESPONSIVENESS_THRESHOLD",
            "JOB_RESPONSIVENESS_CHECK_INTERVAL",
            "DISCOVERY_FAILURE_DECAY_INTERVAL",
            "STATS_WINDOW_SIZE_MS",
            "STATS_DRIFT_TOLERANCE_MS",
            "STATS_MAX_WINDOW_AGE_MS",
            "MANAGER_STATS_HOT_MAX_ENTRIES",
            "MANAGER_STATS_THROTTLE_THRESHOLD",
            "MANAGER_STATS_BATCH_THRESHOLD",
            "MANAGER_STATS_REJECT_THRESHOLD",
            "STATS_PUSH_INTERVAL_MS",
            "MANAGER_STATE_SYNC_RETRIES",
            "MANAGER_STATE_SYNC_TIMEOUT",
            "LEADER_ELECTION_JITTER_MAX",
            "MANAGER_STARTUP_SYNC_DELAY",
            "CLUSTER_STABILIZATION_TIMEOUT",
            "CLUSTER_STABILIZATION_POLL_INTERVAL",
            "MANAGER_HEARTBEAT_INTERVAL",
            "MANAGER_PEER_SYNC_INTERVAL",
        ]:
            setattr(
                mock_env,
                attr,
                1.0
                if "INTERVAL" in attr or "TIMEOUT" in attr or "THRESHOLD" in attr
                else 1,
            )
        mock_env.get = MagicMock(side_effect=lambda k, d=None: d)

        gates = [("gate-1", 6000), ("gate-2", 6001)]
        managers = [("manager-2", 7000)]

        config = create_manager_config_from_env(
            host="10.0.0.1",
            tcp_port=8000,
            udp_port=8001,
            env=mock_env,
            seed_gates=gates,
            seed_managers=managers,
        )

        assert config.seed_gates == gates
        assert config.seed_managers == managers


# =============================================================================
# ManagerState Tests
# =============================================================================


class TestManagerStateHappyPath:
    """Happy path tests for ManagerState."""

    def test_initialization(self):
        """ManagerState initializes with empty containers."""
        state = ManagerState()

        # Gate tracking
        assert state._known_gates == {}
        assert state._healthy_gate_ids == set()
        assert state._primary_gate_id is None
        assert state._current_gate_leader_id is None

        # Manager peer tracking
        assert state._known_manager_peers == {}
        assert state._active_manager_peers == set()
        assert state._active_manager_peer_ids == set()

        # Worker tracking
        assert state._workers == {}
        assert state._worker_addr_to_id == {}
        assert state._worker_circuits == {}

        # Job tracking
        assert state._job_leaders == {}
        assert state._job_fencing_tokens == {}

        # State versioning
        assert state._fence_token == 0
        assert state._state_version == 0
        assert state._external_incarnation == 0
        assert state._manager_state == ManagerStateEnum.SYNCING

    def test_initialize_locks(self):
        """initialize_locks creates asyncio locks."""
        state = ManagerState()

        assert state._core_allocation_lock is None
        assert state._eager_dispatch_lock is None

        state.initialize_locks()

        assert isinstance(state._core_allocation_lock, asyncio.Lock)
        assert isinstance(state._eager_dispatch_lock, asyncio.Lock)


class TestManagerStateLockManagement:
    """Tests for lock management methods."""

    @pytest.mark.asyncio
    async def test_get_peer_state_lock_creates_new(self):
        """get_peer_state_lock creates lock for new peer."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)

        lock = await state.get_peer_state_lock(peer_addr)

        assert isinstance(lock, asyncio.Lock)
        assert peer_addr in state._peer_state_locks

    @pytest.mark.asyncio
    async def test_get_peer_state_lock_returns_existing(self):
        """get_peer_state_lock returns existing lock."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)

        lock1 = await state.get_peer_state_lock(peer_addr)
        lock2 = await state.get_peer_state_lock(peer_addr)

        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_get_gate_state_lock_creates_new(self):
        """get_gate_state_lock creates lock for new gate."""
        state = ManagerState()
        gate_id = "gate-123"

        lock = await state.get_gate_state_lock(gate_id)

        assert isinstance(lock, asyncio.Lock)
        assert gate_id in state._gate_state_locks

    @pytest.mark.asyncio
    async def test_get_workflow_cancellation_lock(self):
        """get_workflow_cancellation_lock creates/returns lock."""
        state = ManagerState()
        workflow_id = "workflow-123"

        lock1 = await state.get_workflow_cancellation_lock(workflow_id)
        lock2 = await state.get_workflow_cancellation_lock(workflow_id)

        assert isinstance(lock1, asyncio.Lock)
        assert lock1 is lock2

    @pytest.mark.asyncio
    async def test_get_dispatch_semaphore(self):
        """get_dispatch_semaphore creates/returns semaphore."""
        state = ManagerState()
        worker_id = "worker-123"

        sem1 = await state.get_dispatch_semaphore(worker_id, max_concurrent=5)
        sem2 = await state.get_dispatch_semaphore(worker_id, max_concurrent=10)

        assert isinstance(sem1, asyncio.Semaphore)
        assert sem1 is sem2


class TestManagerStateVersioning:
    """Tests for state versioning methods."""

    @pytest.mark.asyncio
    async def test_increment_fence_token(self):
        """increment_fence_token increments and returns value."""
        state = ManagerState()

        assert state._fence_token == 0

        result1 = await state.increment_fence_token()
        assert result1 == 1
        assert state._fence_token == 1

        result2 = await state.increment_fence_token()
        assert result2 == 2
        assert state._fence_token == 2

    @pytest.mark.asyncio
    async def test_increment_state_version(self):
        """increment_state_version increments and returns value."""
        state = ManagerState()

        assert state._state_version == 0

        result = await state.increment_state_version()
        assert result == 1
        assert state._state_version == 1

    @pytest.mark.asyncio
    async def test_increment_external_incarnation(self):
        """increment_external_incarnation increments and returns value."""
        state = ManagerState()

        assert state._external_incarnation == 0

        result = await state.increment_external_incarnation()
        assert result == 1

    @pytest.mark.asyncio
    async def test_increment_context_lamport_clock(self):
        """increment_context_lamport_clock increments and returns value."""
        state = ManagerState()

        assert state._context_lamport_clock == 0

        result = await state.increment_context_lamport_clock()
        assert result == 1


class TestManagerStatePeerManagement:
    """Tests for peer management methods."""

    def test_get_active_peer_count(self):
        """get_active_peer_count returns correct count."""
        state = ManagerState()

        assert state.get_active_peer_count() == 1

        state._active_manager_peers.add(("10.0.0.1", 8000))
        state._active_manager_peers.add(("10.0.0.2", 8000))

        assert state.get_active_peer_count() == 3

    @pytest.mark.asyncio
    async def test_is_peer_active(self):
        """is_peer_active checks peer status."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)

        assert await state.is_peer_active(peer_addr) is False

        state._active_manager_peers.add(peer_addr)

        assert await state.is_peer_active(peer_addr) is True

    @pytest.mark.asyncio
    async def test_add_active_peer(self):
        """add_active_peer adds to both sets."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)
        node_id = "manager-123"

        await state.add_active_peer(peer_addr, node_id)

        assert peer_addr in state._active_manager_peers
        assert node_id in state._active_manager_peer_ids

    @pytest.mark.asyncio
    async def test_remove_active_peer(self):
        """remove_active_peer removes from both sets."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)
        node_id = "manager-123"

        state._active_manager_peers.add(peer_addr)
        state._active_manager_peer_ids.add(node_id)

        await state.remove_active_peer(peer_addr, node_id)

        assert peer_addr not in state._active_manager_peers
        assert node_id not in state._active_manager_peer_ids


class TestManagerStateCancellationCleanup:
    """Tests for cancellation state cleanup."""

    def test_clear_cancellation_state(self):
        """clear_cancellation_state removes all cancellation tracking."""
        state = ManagerState()
        job_id = "job-123"

        # Set up cancellation state
        state._cancellation_pending_workflows[job_id] = {"wf-1", "wf-2"}
        state._cancellation_errors[job_id] = ["error1"]
        state._cancellation_completion_events[job_id] = asyncio.Event()
        state._cancellation_initiated_at[job_id] = time.monotonic()

        state.clear_cancellation_state(job_id)

        assert job_id not in state._cancellation_pending_workflows
        assert job_id not in state._cancellation_errors
        assert job_id not in state._cancellation_completion_events
        assert job_id not in state._cancellation_initiated_at

    def test_clear_cancellation_state_nonexistent_job(self):
        """clear_cancellation_state handles nonexistent job gracefully."""
        state = ManagerState()

        # Should not raise
        state.clear_cancellation_state("nonexistent-job")


class TestManagerStateJobCleanup:
    """Tests for job state cleanup."""

    def test_clear_job_state(self):
        """clear_job_state removes all job-related state."""
        state = ManagerState()
        job_id = "job-cleanup"

        # Set up job state
        state._job_leaders[job_id] = "manager-1"
        state._job_leader_addrs[job_id] = ("10.0.0.1", 8000)
        state._job_fencing_tokens[job_id] = 5
        state._job_layer_version[job_id] = 3
        state._job_callbacks[job_id] = ("10.0.0.2", 9000)
        state._job_submissions[job_id] = MagicMock()
        state._cancellation_pending_workflows[job_id] = {"wf-1"}

        state.clear_job_state(job_id)

        assert job_id not in state._job_leaders
        assert job_id not in state._job_leader_addrs
        assert job_id not in state._job_fencing_tokens
        assert job_id not in state._job_layer_version
        assert job_id not in state._job_callbacks
        assert job_id not in state._job_submissions
        assert job_id not in state._cancellation_pending_workflows


class TestManagerStateMetrics:
    """Tests for metrics collection methods."""

    def test_get_quorum_metrics(self):
        """get_quorum_metrics returns correct metrics."""
        state = ManagerState()

        state._active_manager_peers.add(("10.0.0.1", 8000))
        state._active_manager_peers.add(("10.0.0.2", 8000))
        state._known_manager_peers["m1"] = MagicMock()
        state._known_manager_peers["m2"] = MagicMock()
        state._known_manager_peers["m3"] = MagicMock()
        state._dead_managers.add(("10.0.0.3", 8000))
        state._pending_provisions["wf-1"] = MagicMock()

        metrics = state.get_quorum_metrics()

        assert metrics["active_peer_count"] == 2
        assert metrics["known_peer_count"] == 3
        assert metrics["dead_manager_count"] == 1
        assert metrics["pending_provision_count"] == 1

    def test_get_worker_metrics(self):
        """get_worker_metrics returns correct metrics."""
        state = ManagerState()

        state._workers["w1"] = MagicMock()
        state._workers["w2"] = MagicMock()
        state._worker_unhealthy_since["w1"] = time.monotonic()
        state._worker_circuits["w1"] = MagicMock()
        state._worker_circuits["w2"] = MagicMock()

        metrics = state.get_worker_metrics()

        assert metrics["worker_count"] == 2
        assert metrics["unhealthy_worker_count"] == 1
        assert metrics["worker_circuits_count"] == 2

    def test_get_gate_metrics(self):
        """get_gate_metrics returns correct metrics."""
        state = ManagerState()

        state._known_gates["g1"] = MagicMock()
        state._known_gates["g2"] = MagicMock()
        state._healthy_gate_ids.add("g1")
        state._gate_unhealthy_since["g2"] = time.monotonic()
        state._current_gate_leader_id = "g1"

        metrics = state.get_gate_metrics()

        assert metrics["known_gate_count"] == 2
        assert metrics["healthy_gate_count"] == 1
        assert metrics["unhealthy_gate_count"] == 1
        assert metrics["has_gate_leader"] is True

    def test_get_job_metrics(self):
        """get_job_metrics returns correct metrics."""
        state = ManagerState()

        state._job_leaders["j1"] = "m1"
        state._job_leaders["j2"] = "m2"
        state._job_callbacks["j1"] = ("10.0.0.1", 9000)
        state._job_submissions["j1"] = MagicMock()
        state._cancelled_workflows["wf-1"] = MagicMock()
        state._cancellation_pending_workflows["j1"] = {"wf-2"}

        metrics = state.get_job_metrics()

        assert metrics["job_leader_count"] == 2
        assert metrics["job_callback_count"] == 1
        assert metrics["job_submission_count"] == 1
        assert metrics["cancelled_workflow_count"] == 1
        assert metrics["pending_cancellation_count"] == 1


class TestManagerStateConcurrency:
    """Concurrency tests for ManagerState."""

    @pytest.mark.asyncio
    async def test_concurrent_lock_access(self):
        """Multiple coroutines can safely access different locks."""
        state = ManagerState()

        results = []

        async def access_peer_lock(peer_addr: tuple[str, int]):
            lock = await state.get_peer_state_lock(peer_addr)
            async with lock:
                results.append(f"peer-{peer_addr}")
                await asyncio.sleep(0.01)

        async def access_gate_lock(gate_id: str):
            lock = await state.get_gate_state_lock(gate_id)
            async with lock:
                results.append(f"gate-{gate_id}")
                await asyncio.sleep(0.01)

        await asyncio.gather(
            access_peer_lock(("10.0.0.1", 8000)),
            access_gate_lock("gate-1"),
            access_peer_lock(("10.0.0.2", 8000)),
            access_gate_lock("gate-2"),
        )

        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_same_lock_serializes_access(self):
        """Same lock serializes access."""
        state = ManagerState()
        peer_addr = ("10.0.0.1", 8000)

        execution_order = []

        async def accessor(accessor_id: int, delay: float):
            lock = await state.get_peer_state_lock(peer_addr)
            async with lock:
                execution_order.append(("start", accessor_id))
                await asyncio.sleep(delay)
                execution_order.append(("end", accessor_id))

        task1 = asyncio.create_task(accessor(1, 0.05))
        await asyncio.sleep(0.01)
        task2 = asyncio.create_task(accessor(2, 0.02))

        await asyncio.gather(task1, task2)

        assert execution_order[0] == ("start", 1)
        assert execution_order[1] == ("end", 1)
        assert execution_order[2] == ("start", 2)
        assert execution_order[3] == ("end", 2)

    @pytest.mark.asyncio
    async def test_concurrent_increment_operations(self):
        """Increment operations are not atomic but work correctly."""
        state = ManagerState()

        async def increment_many():
            for _ in range(100):
                await state.increment_fence_token()
                await asyncio.sleep(0)

        await asyncio.gather(
            increment_many(),
            increment_many(),
            increment_many(),
        )

        assert state._fence_token == 300


class TestManagerStateEdgeCases:
    """Edge case tests for ManagerState."""

    def test_empty_metrics(self):
        """Metrics work with empty state."""
        state = ManagerState()

        quorum = state.get_quorum_metrics()
        worker = state.get_worker_metrics()
        gate = state.get_gate_metrics()
        job = state.get_job_metrics()

        assert quorum["active_peer_count"] == 0
        assert worker["worker_count"] == 0
        assert gate["known_gate_count"] == 0
        assert job["job_leader_count"] == 0

    def test_multiple_clear_job_state_calls(self):
        """Multiple clear_job_state calls are safe."""
        state = ManagerState()
        job_id = "job-multi-clear"

        state._job_leaders[job_id] = "m1"

        state.clear_job_state(job_id)
        state.clear_job_state(job_id)  # Second call should not raise
        state.clear_job_state(job_id)  # Third call should not raise

        assert job_id not in state._job_leaders

    def test_versioned_clock_initialized(self):
        """VersionedStateClock is initialized."""
        state = ManagerState()

        assert state._versioned_clock is not None

    def test_throughput_tracking_initialized(self):
        """Throughput tracking fields are initialized."""
        state = ManagerState()

        assert state._dispatch_throughput_count == 0
        assert state._dispatch_throughput_interval_start == 0.0
        assert state._dispatch_throughput_last_value == 0.0

    def test_latency_tracking_initialized(self):
        """Latency tracking fields are initialized."""
        state = ManagerState()

        assert state._gate_latency_samples == []
        assert state._peer_manager_latency_samples == {}
        assert state._worker_latency_samples == {}
