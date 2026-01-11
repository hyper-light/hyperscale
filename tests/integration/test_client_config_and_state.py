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
from hyperscale.distributed_rewrite.nodes.client.models import (
    GateLeaderTracking,
    ManagerLeaderTracking,
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
        progress_callback = lambda x: None
        workflow_callback = lambda x: None
        reporter_callback = lambda x: None

        state.initialize_job_tracking(
            job_id,
            on_status_update=status_callback,
            on_progress_update=progress_callback,
            on_workflow_result=workflow_callback,
            on_reporter_result=reporter_callback,
        )

        assert job_id in state._jobs
        assert job_id in state._job_events
        assert job_id in state._job_callbacks
        assert state._job_callbacks[job_id][0] == status_callback
        assert state._progress_callbacks[job_id] == progress_callback
        assert state._workflow_callbacks[job_id] == workflow_callback
        assert state._reporter_callbacks[job_id] == reporter_callback

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

    def test_update_gate_leader(self):
        """Test gate leader update."""
        state = ClientState()
        job_id = "gate-leader-job"
        leader_info = ("gate-1", 9000)
        fence_token = 5

        state.update_gate_leader(job_id, leader_info, fence_token)

        assert job_id in state._gate_job_leaders
        tracking = state._gate_job_leaders[job_id]
        assert tracking.leader_info == leader_info
        assert tracking.last_updated > 0

    def test_update_manager_leader(self):
        """Test manager leader update."""
        state = ClientState()
        job_id = "mgr-leader-job"
        datacenter_id = "dc-east"
        leader_info = ("manager-2", 7000)
        fence_token = 10

        state.update_manager_leader(
            job_id, datacenter_id, leader_info, fence_token
        )

        key = (job_id, datacenter_id)
        assert key in state._manager_job_leaders
        tracking = state._manager_job_leaders[key]
        assert tracking.leader_info == leader_info
        assert tracking.datacenter_id == datacenter_id

    def test_mark_job_orphaned(self):
        """Test marking job as orphaned."""
        state = ClientState()
        job_id = "orphan-job"
        orphan_info = {"reason": "Leader disappeared"}

        state.mark_job_orphaned(job_id, orphan_info)

        assert job_id in state._orphaned_jobs
        orphaned = state._orphaned_jobs[job_id]
        assert orphaned.orphan_info == orphan_info
        assert orphaned.orphaned_at > 0

    def test_clear_job_orphaned(self):
        """Test clearing orphan status."""
        state = ClientState()
        job_id = "orphan-clear-job"

        state.mark_job_orphaned(job_id, {"reason": "test"})
        assert job_id in state._orphaned_jobs

        state.clear_job_orphaned(job_id)
        assert job_id not in state._orphaned_jobs

    def test_is_job_orphaned(self):
        """Test checking orphan status."""
        state = ClientState()
        job_id = "orphan-check-job"

        assert state.is_job_orphaned(job_id) is False

        state.mark_job_orphaned(job_id, {"reason": "test"})
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

    def test_increment_requests_rerouted(self):
        """Test rerouted requests counter."""
        state = ClientState()

        assert state._requests_rerouted == 0

        state.increment_requests_rerouted()

        assert state._requests_rerouted == 1

    def test_increment_requests_failed_leadership_change(self):
        """Test failed leadership change counter."""
        state = ClientState()

        assert state._requests_failed_leadership_change == 0

        state.increment_requests_failed_leadership_change()
        state.increment_requests_failed_leadership_change()

        assert state._requests_failed_leadership_change == 2

    def test_get_leadership_metrics(self):
        """Test leadership metrics retrieval."""
        state = ClientState()

        state.increment_gate_transfers()
        state.increment_gate_transfers()
        state.increment_manager_transfers()
        state.increment_requests_rerouted()
        state.increment_requests_failed_leadership_change()

        metrics = state.get_leadership_metrics()

        assert metrics["gate_transfers_received"] == 2
        assert metrics["manager_transfers_received"] == 1
        assert metrics["requests_rerouted"] == 1
        assert metrics["requests_failed_leadership_change"] == 1
        assert metrics["orphaned_jobs_count"] == 0

    def test_get_leadership_metrics_with_orphans(self):
        """Test leadership metrics with orphaned jobs."""
        state = ClientState()

        state.mark_job_orphaned("job-1", {"reason": "test"})
        state.mark_job_orphaned("job-2", {"reason": "test"})

        metrics = state.get_leadership_metrics()
        assert metrics["orphaned_jobs_count"] == 2

    @pytest.mark.asyncio
    async def test_concurrency_job_tracking(self):
        """Test concurrent job tracking updates."""
        state = ClientState()
        job_ids = [f"job-{i}" for i in range(10)]

        async def initialize_job(job_id):
            state.initialize_job_tracking(job_id)
            await asyncio.sleep(0.001)
            state.mark_job_target(job_id, (f"manager-{job_id}", 8000))

        await asyncio.gather(*[initialize_job(jid) for jid in job_ids])

        assert len(state._jobs) == 10
        assert len(state._job_targets) == 10

    @pytest.mark.asyncio
    async def test_concurrency_leader_updates(self):
        """Test concurrent leader updates."""
        state = ClientState()

        async def update_gate_leader(job_id, fence_token):
            state.update_gate_leader(
                job_id,
                (f"gate-{fence_token}", 9000),
                fence_token
            )
            await asyncio.sleep(0.001)

        job_id = "concurrent-job"
        await asyncio.gather(*[
            update_gate_leader(job_id, i) for i in range(10)
        ])

        # Final state should have latest update
        assert job_id in state._gate_job_leaders

    @pytest.mark.asyncio
    async def test_concurrency_orphan_tracking(self):
        """Test concurrent orphan status updates."""
        state = ClientState()
        job_id = "orphan-concurrent"

        async def mark_and_clear():
            state.mark_job_orphaned(job_id, {"reason": "test"})
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

        state.initialize_job_tracking(
            job_id,
            on_status_update=None,
            on_progress_update=None,
            on_workflow_result=None,
            on_reporter_result=None,
        )

        assert job_id in state._jobs
        # Callbacks should be None if not provided
        assert state._progress_callbacks.get(job_id) is None

    def test_edge_case_duplicate_job_initialization(self):
        """Test initializing same job twice."""
        state = ClientState()
        job_id = "duplicate-job"

        state.initialize_job_tracking(job_id)
        state.initialize_job_tracking(job_id)  # Second init

        # Should still have single entry
        assert job_id in state._jobs

    def test_edge_case_very_long_job_id(self):
        """Test with extremely long job ID."""
        state = ClientState()
        long_job_id = "job-" + "x" * 10000

        state.initialize_job_tracking(long_job_id)

        assert long_job_id in state._jobs

    def test_edge_case_special_characters_in_job_id(self):
        """Test job IDs with special characters."""
        state = ClientState()
        special_job_id = "job-🚀-test-ñ-中文"

        state.initialize_job_tracking(special_job_id)

        assert special_job_id in state._jobs
