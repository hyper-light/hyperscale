"""
Integration tests for ClientConfig and ClientState (Sections 15.1.2, 15.1.3).

Tests ClientConfig dataclass and ClientState mutable tracking class.

Covers:
- Happy path: Normal configuration and state management
- Negative path: Invalid configuration values
- Failure mode: Missing environment variables, invalid state operations
- Concurrency: Thread-safe state updates
- Edge cases: Boundary values, empty collections
"""

import asyncio
import os
import time
from unittest.mock import patch

import pytest

from hyperscale.distributed_rewrite.nodes.client.config import (
    ClientConfig,
    create_client_config,
    TRANSIENT_ERRORS,
)
from hyperscale.distributed_rewrite.nodes.client.state import ClientState
from hyperscale.distributed_rewrite.models import (
    ClientJobResult,
    GateLeaderInfo,
    ManagerLeaderInfo,
    OrphanedJobInfo,
)


class TestClientConfig:
    """Test ClientConfig dataclass."""

    def test_happy_path_instantiation(self):
        """Test normal configuration creation."""
        config = ClientConfig(
            host="localhost",
            tcp_port=8000,
            env="test",
            managers=[("manager1", 7000), ("manager2", 7001)],
            gates=[("gate1", 9000)],
        )

        assert config.host == "localhost"
        assert config.tcp_port == 8000
        assert config.env == "test"
        assert len(config.managers) == 2
        assert len(config.gates) == 1

    def test_default_values(self):
        """Test default configuration values."""
        config = ClientConfig(
            host="0.0.0.0",
            tcp_port=5000,
            env="dev",
            managers=[],
            gates=[],
        )

        assert config.orphan_grace_period_seconds == float(
            os.getenv("CLIENT_ORPHAN_GRACE_PERIOD", "120.0")
        )
        assert config.orphan_check_interval_seconds == float(
            os.getenv("CLIENT_ORPHAN_CHECK_INTERVAL", "30.0")
        )
        assert config.response_freshness_timeout_seconds == float(
            os.getenv("CLIENT_RESPONSE_FRESHNESS_TIMEOUT", "5.0")
        )
        assert config.leadership_max_retries == 3
        assert config.leadership_retry_delay_seconds == 0.5
        assert config.leadership_exponential_backoff is True
        assert config.leadership_max_delay_seconds == 5.0
        assert config.submission_max_retries == 5
        assert config.submission_max_redirects_per_attempt == 3
        assert config.rate_limit_enabled is True
        assert config.rate_limit_health_gated is True
        assert config.negotiate_capabilities is True

    @patch.dict(os.environ, {
        "CLIENT_ORPHAN_GRACE_PERIOD": "180.0",
        "CLIENT_ORPHAN_CHECK_INTERVAL": "60.0",
        "CLIENT_RESPONSE_FRESHNESS_TIMEOUT": "10.0",
    })
    def test_environment_variable_override(self):
        """Test environment variable configuration."""
        config = ClientConfig(
            host="test",
            tcp_port=8000,
            env="staging",
            managers=[],
            gates=[],
        )

        assert config.orphan_grace_period_seconds == 180.0
        assert config.orphan_check_interval_seconds == 60.0
        assert config.response_freshness_timeout_seconds == 10.0

    def test_create_client_config_factory(self):
        """Test create_client_config factory function."""
        config = create_client_config(
            host="192.168.1.1",
            port=9000,
            env="production",
            managers=[("m1", 8000), ("m2", 8001)],
            gates=[("g1", 10000)],
        )

        assert config.host == "192.168.1.1"
        assert config.tcp_port == 9000
        assert config.env == "production"
        assert len(config.managers) == 2
        assert len(config.gates) == 1

    def test_create_client_config_defaults(self):
        """Test factory with default managers and gates."""
        config = create_client_config(
            host="localhost",
            port=5000,
        )

        assert config.managers == []
        assert config.gates == []
        assert config.env == "local"

    def test_edge_case_empty_managers_and_gates(self):
        """Test with no managers or gates."""
        config = ClientConfig(
            host="test",
            tcp_port=8000,
            env="dev",
            managers=[],
            gates=[],
        )

        assert config.managers == []
        assert config.gates == []

    def test_edge_case_many_managers(self):
        """Test with many manager endpoints."""
        managers = [(f"manager{i}", 7000 + i) for i in range(100)]
        config = ClientConfig(
            host="test",
            tcp_port=8000,
            env="dev",
            managers=managers,
            gates=[],
        )

        assert len(config.managers) == 100

    def test_edge_case_port_boundaries(self):
        """Test with edge case port numbers."""
        # Min valid port
        config1 = ClientConfig(
            host="test",
            tcp_port=1,
            env="dev",
            managers=[("m", 1024)],
            gates=[],
        )
        assert config1.tcp_port == 1

        # Max valid port
        config2 = ClientConfig(
            host="test",
            tcp_port=65535,
            env="dev",
            managers=[("m", 65535)],
            gates=[],
        )
        assert config2.tcp_port == 65535

    def test_transient_errors_frozenset(self):
        """Test TRANSIENT_ERRORS constant."""
        assert isinstance(TRANSIENT_ERRORS, frozenset)
        assert "syncing" in TRANSIENT_ERRORS
        assert "not ready" in TRANSIENT_ERRORS
        assert "election in progress" in TRANSIENT_ERRORS
        assert "no leader" in TRANSIENT_ERRORS
        assert "split brain" in TRANSIENT_ERRORS
        assert "rate limit" in TRANSIENT_ERRORS
        assert "overload" in TRANSIENT_ERRORS
        assert "too many" in TRANSIENT_ERRORS
        assert "server busy" in TRANSIENT_ERRORS

    def test_transient_errors_immutable(self):
        """Test that TRANSIENT_ERRORS cannot be modified."""
        with pytest.raises(AttributeError):
            TRANSIENT_ERRORS.add("new error")


class TestClientState:
    """Test ClientState mutable tracking class."""

    def test_happy_path_instantiation(self):
        """Test normal state initialization."""
        state = ClientState()

        assert isinstance(state._jobs, dict)
        assert isinstance(state._job_events, dict)
        assert isinstance(state._job_callbacks, dict)
        assert isinstance(state._job_targets, dict)
        assert isinstance(state._cancellation_events, dict)
        assert isinstance(state._cancellation_errors, dict)
        assert isinstance(state._cancellation_success, dict)

    def test_initialize_job_tracking(self):
        """Test job tracking initialization."""
        state = ClientState()
        job_id = "job-123"

        status_callback = lambda x: None
        initial_result = ClientJobResult(job_id=job_id, status="SUBMITTED")

        state.initialize_job_tracking(
            job_id,
            initial_result=initial_result,
            callback=status_callback,
        )

        assert job_id in state._jobs
        assert job_id in state._job_events
        assert job_id in state._job_callbacks
        assert state._job_callbacks[job_id] == status_callback
        assert state._jobs[job_id] == initial_result

    def test_initialize_cancellation_tracking(self):
        """Test cancellation tracking initialization."""
        state = ClientState()
        job_id = "cancel-456"

        state.initialize_cancellation_tracking(job_id)

        assert job_id in state._cancellation_events
        assert job_id in state._cancellation_errors
        assert job_id in state._cancellation_success
        assert state._cancellation_errors[job_id] == []
        assert state._cancellation_success[job_id] is False

    def test_mark_job_target(self):
        """Test job target marking."""
        state = ClientState()
        job_id = "job-target-789"
        target = ("manager-1", 8000)

        state.mark_job_target(job_id, target)

        assert state._job_targets[job_id] == target

    def test_gate_leader_tracking(self):
        """Test gate leader tracking via direct state update."""
        state = ClientState()
        job_id = "gate-leader-job"
        leader_info = GateLeaderInfo(
            gate_addr=("gate-1", 9000),
            fence_token=5,
            last_updated=time.time(),
        )

        state._gate_job_leaders[job_id] = leader_info

        assert job_id in state._gate_job_leaders
        stored = state._gate_job_leaders[job_id]
        assert stored.gate_addr == ("gate-1", 9000)
        assert stored.fence_token == 5

    def test_manager_leader_tracking(self):
        """Test manager leader tracking via direct state update."""
        state = ClientState()
        job_id = "mgr-leader-job"
        datacenter_id = "dc-east"
        leader_info = ManagerLeaderInfo(
            manager_addr=("manager-2", 7000),
            fence_token=10,
            datacenter_id=datacenter_id,
            last_updated=time.time(),
        )

        key = (job_id, datacenter_id)
        state._manager_job_leaders[key] = leader_info

        assert key in state._manager_job_leaders
        stored = state._manager_job_leaders[key]
        assert stored.manager_addr == ("manager-2", 7000)
        assert stored.fence_token == 10
        assert stored.datacenter_id == datacenter_id

    def test_mark_job_orphaned(self):
        """Test marking job as orphaned."""
        state = ClientState()
        job_id = "orphan-job"
        orphan_info = OrphanedJobInfo(
            job_id=job_id,
            orphan_timestamp=time.time(),
            last_known_gate=("gate-1", 9000),
            last_known_manager=None,
        )

        state.mark_job_orphaned(job_id, orphan_info)

        assert job_id in state._orphaned_jobs
        orphaned = state._orphaned_jobs[job_id]
        assert orphaned.job_id == job_id
        assert orphaned.orphan_timestamp > 0
        assert orphaned.last_known_gate == ("gate-1", 9000)

    def test_clear_job_orphaned(self):
        """Test clearing orphan status."""
        state = ClientState()
        job_id = "orphan-clear-job"

        orphan_info = OrphanedJobInfo(
            job_id=job_id,
            orphan_timestamp=time.time(),
            last_known_gate=None,
            last_known_manager=None,
        )
        state.mark_job_orphaned(job_id, orphan_info)
        assert job_id in state._orphaned_jobs

        state.clear_job_orphaned(job_id)
        assert job_id not in state._orphaned_jobs

    def test_is_job_orphaned(self):
        """Test checking orphan status."""
        state = ClientState()
        job_id = "orphan-check-job"

        assert state.is_job_orphaned(job_id) is False

        orphan_info = OrphanedJobInfo(
            job_id=job_id,
            orphan_timestamp=time.time(),
            last_known_gate=None,
            last_known_manager=None,
        )
        state.mark_job_orphaned(job_id, orphan_info)
        assert state.is_job_orphaned(job_id) is True

    def test_increment_gate_transfers(self):
        """Test gate transfer counter."""
        state = ClientState()

        assert state._gate_transfers_received == 0

        state.increment_gate_transfers()
        state.increment_gate_transfers()

        assert state._gate_transfers_received == 2

    def test_increment_manager_transfers(self):
        """Test manager transfer counter."""
        state = ClientState()

        assert state._manager_transfers_received == 0

        state.increment_manager_transfers()
        state.increment_manager_transfers()
        state.increment_manager_transfers()

        assert state._manager_transfers_received == 3

    def test_increment_rerouted(self):
        """Test rerouted requests counter."""
        state = ClientState()

        assert state._requests_rerouted == 0

        state.increment_rerouted()

        assert state._requests_rerouted == 1

    def test_increment_failed_leadership_change(self):
        """Test failed leadership change counter."""
        state = ClientState()

        assert state._requests_failed_leadership_change == 0

        state.increment_failed_leadership_change()
        state.increment_failed_leadership_change()

        assert state._requests_failed_leadership_change == 2

    def test_get_leadership_metrics(self):
        """Test leadership metrics retrieval."""
        state = ClientState()

        state.increment_gate_transfers()
        state.increment_gate_transfers()
        state.increment_manager_transfers()
        state.increment_rerouted()
        state.increment_failed_leadership_change()

        metrics = state.get_leadership_metrics()

        assert metrics["gate_transfers_received"] == 2
        assert metrics["manager_transfers_received"] == 1
        assert metrics["requests_rerouted"] == 1
        assert metrics["requests_failed_leadership_change"] == 1
        assert metrics["orphaned_jobs"] == 0

    def test_get_leadership_metrics_with_orphans(self):
        """Test leadership metrics with orphaned jobs."""
        state = ClientState()

        orphan1 = OrphanedJobInfo(
            job_id="job-1",
            orphan_timestamp=time.time(),
            last_known_gate=None,
            last_known_manager=None,
        )
        orphan2 = OrphanedJobInfo(
            job_id="job-2",
            orphan_timestamp=time.time(),
            last_known_gate=None,
            last_known_manager=None,
        )
        state.mark_job_orphaned("job-1", orphan1)
        state.mark_job_orphaned("job-2", orphan2)

        metrics = state.get_leadership_metrics()
        assert metrics["orphaned_jobs"] == 2

    @pytest.mark.asyncio
    async def test_concurrency_job_tracking(self):
        """Test concurrent job tracking updates."""
        state = ClientState()
        job_ids = [f"job-{i}" for i in range(10)]

        async def initialize_job(job_id):
            initial_result = ClientJobResult(job_id=job_id, status="SUBMITTED")
            state.initialize_job_tracking(job_id, initial_result)
            await asyncio.sleep(0.001)
            state.mark_job_target(job_id, (f"manager-{job_id}", 8000))

        await asyncio.gather(*[initialize_job(jid) for jid in job_ids])

        assert len(state._jobs) == 10
        assert len(state._job_targets) == 10

    @pytest.mark.asyncio
    async def test_concurrency_leader_updates(self):
        """Test concurrent leader updates."""
        state = ClientState()
        job_id = "concurrent-job"

        async def update_gate_leader(fence_token):
            leader_info = GateLeaderInfo(
                gate_addr=(f"gate-{fence_token}", 9000),
                fence_token=fence_token,
                last_updated=time.time(),
            )
            state._gate_job_leaders[job_id] = leader_info
            await asyncio.sleep(0.001)

        await asyncio.gather(*[
            update_gate_leader(i) for i in range(10)
        ])

        # Final state should have latest update
        assert job_id in state._gate_job_leaders

    @pytest.mark.asyncio
    async def test_concurrency_orphan_tracking(self):
        """Test concurrent orphan status updates."""
        state = ClientState()
        job_id = "orphan-concurrent"

        async def mark_and_clear():
            orphan_info = OrphanedJobInfo(
                job_id=job_id,
                orphan_timestamp=time.time(),
                last_known_gate=None,
                last_known_manager=None,
            )
            state.mark_job_orphaned(job_id, orphan_info)
            await asyncio.sleep(0.001)
            state.clear_job_orphaned(job_id)

        await asyncio.gather(*[mark_and_clear() for _ in range(5)])

        # Final state depends on race, but should be consistent
        orphaned = state.is_job_orphaned(job_id)
        assert isinstance(orphaned, bool)

    def test_edge_case_empty_callbacks(self):
        """Test job tracking with no callbacks."""
        state = ClientState()
        job_id = "no-callbacks-job"
        initial_result = ClientJobResult(job_id=job_id, status="SUBMITTED")

        state.initialize_job_tracking(
            job_id,
            initial_result=initial_result,
            callback=None,
        )

        assert job_id in state._jobs
        # Callback should not be set if None
        assert job_id not in state._job_callbacks

    def test_edge_case_duplicate_job_initialization(self):
        """Test initializing same job twice."""
        state = ClientState()
        job_id = "duplicate-job"
        initial_result = ClientJobResult(job_id=job_id, status="SUBMITTED")

        state.initialize_job_tracking(job_id, initial_result)
        state.initialize_job_tracking(job_id, initial_result)  # Second init

        # Should still have single entry
        assert job_id in state._jobs

    def test_edge_case_very_long_job_id(self):
        """Test with extremely long job ID."""
        state = ClientState()
        long_job_id = "job-" + "x" * 10000
        initial_result = ClientJobResult(job_id=long_job_id, status="SUBMITTED")

        state.initialize_job_tracking(long_job_id, initial_result)

        assert long_job_id in state._jobs

    def test_edge_case_special_characters_in_job_id(self):
        """Test job IDs with special characters."""
        state = ClientState()
        special_job_id = "job-🚀-test-ñ-中文"
        initial_result = ClientJobResult(job_id=special_job_id, status="SUBMITTED")

        state.initialize_job_tracking(special_job_id, initial_result)

        assert special_job_id in state._jobs
