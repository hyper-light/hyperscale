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
import time

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.client.config import TRANSIENT_ERRORS
from hyperscale.distributed.nodes.client.models.client_config import ClientConfig
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.reporting.common import ReporterTypes
from hyperscale.distributed.models import (
    ClientJobResult,
    GateLeaderInfo,
    ManagerLeaderInfo,
)


class TestClientConfig:
    """Test ClientConfig, which reads every setting from the client's Env."""

    def make_config(self, env: Env, **addresses) -> ClientConfig:
        return ClientConfig.from_env(
            env,
            host=addresses.get("host", "localhost"),
            tcp_port=addresses.get("tcp_port", 8000),
            managers=addresses.get("managers", []),
            gates=addresses.get("gates", []),
        )

    def test_happy_path_addresses(self):
        """The node addresses are taken as given."""
        config = self.make_config(
            Env(),
            host="localhost",
            tcp_port=8000,
            managers=[("manager1", 7000), ("manager2", 7001)],
            gates=[("gate1", 9000)],
        )

        assert config.host == "localhost"
        assert config.tcp_port == 8000
        assert config.managers == [("manager1", 7000), ("manager2", 7001)]
        assert config.gates == [("gate1", 9000)]

    def test_every_setting_is_read_from_env(self):
        """Each timing and retry setting is the Env field of its name."""
        env = Env(
            CLIENT_ORPHAN_GRACE_PERIOD=11.0,
            CLIENT_ORPHAN_CHECK_INTERVAL=3.0,
            CLIENT_RESPONSE_FRESHNESS_TIMEOUT=7.0,
            CLIENT_STATUS_QUERY_TIMEOUT=2.5,
            CLIENT_SUBMISSION_TIMEOUT=12.0,
            CLIENT_SUBMISSION_MAX_RETRIES=9,
            CLIENT_SUBMISSION_MAX_REDIRECTS=4,
            CLIENT_RESULT_DRAIN_TIMEOUT=1.5,
            CLIENT_JOB_RETENTION_SECONDS=90.0,
        )

        config = self.make_config(env)

        assert config.orphan_grace_period_seconds == 11.0
        assert config.orphan_check_interval_seconds == 3.0
        assert config.response_freshness_timeout_seconds == 7.0
        assert config.status_query_timeout_seconds == 2.5
        assert config.submission_timeout_seconds == 12.0
        assert config.submission_max_retries == 9
        assert config.submission_max_redirects_per_attempt == 4
        assert config.result_drain_timeout_seconds == 1.5
        assert config.job_retention_seconds == 90.0

    def test_default_settings_are_the_env_defaults(self):
        """With no overrides, the settings are Env's defaults."""
        env = Env()

        config = self.make_config(env)

        assert config.orphan_grace_period_seconds == env.CLIENT_ORPHAN_GRACE_PERIOD
        assert config.orphan_check_interval_seconds == env.CLIENT_ORPHAN_CHECK_INTERVAL
        assert config.response_freshness_timeout_seconds == env.CLIENT_RESPONSE_FRESHNESS_TIMEOUT
        assert config.status_query_timeout_seconds == env.CLIENT_STATUS_QUERY_TIMEOUT
        assert config.submission_timeout_seconds == env.CLIENT_SUBMISSION_TIMEOUT
        assert config.submission_max_retries == env.CLIENT_SUBMISSION_MAX_RETRIES
        assert config.submission_max_redirects_per_attempt == env.CLIENT_SUBMISSION_MAX_REDIRECTS
        assert config.result_drain_timeout_seconds == env.CLIENT_RESULT_DRAIN_TIMEOUT
        assert config.job_retention_seconds == env.CLIENT_JOB_RETENTION_SECONDS

    def test_settings_have_no_defaults_of_their_own(self):
        """A config built without Env is refused rather than given hidden values."""
        with pytest.raises(TypeError):
            ClientConfig(host="localhost", tcp_port=8000, managers=[], gates=[])

    def test_local_reporters_are_the_file_reporters(self):
        """The client itself writes the file-based reporters."""
        config = self.make_config(Env())

        assert config.local_reporter_types == {
            ReporterTypes.JSON,
            ReporterTypes.CSV,
            ReporterTypes.XML,
        }

    def test_local_reporter_types_are_not_shared_between_configs(self):
        """Each config owns its reporter set."""
        first_config = self.make_config(Env())
        second_config = self.make_config(Env())

        first_config.local_reporter_types.add(ReporterTypes.Kafka)

        assert ReporterTypes.Kafka not in second_config.local_reporter_types

    def test_edge_case_empty_managers_and_gates(self):
        """Test with no managers or gates."""
        config = self.make_config(Env(), managers=[], gates=[])

        assert config.managers == []
        assert config.gates == []

    def test_edge_case_many_managers(self):
        """Test with many manager endpoints."""
        managers = [(f"manager{index}", 7000 + index) for index in range(100)]
        config = self.make_config(Env(), managers=managers)

        assert len(config.managers) == 100

    def test_edge_case_port_boundaries(self):
        """Test with edge case port numbers."""
        lowest_port_config = self.make_config(Env(), tcp_port=1, managers=[("m", 1024)])
        assert lowest_port_config.tcp_port == 1

        highest_port_config = self.make_config(Env(), tcp_port=65535, managers=[("m", 65535)])
        assert highest_port_config.tcp_port == 65535

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

    @pytest.mark.asyncio
    async def test_increment_gate_transfers(self):
        """Test gate transfer counter."""
        state = ClientState()

        assert state._gate_transfers_received == 0

        await state.increment_gate_transfers()
        await state.increment_gate_transfers()

        assert state._gate_transfers_received == 2

    @pytest.mark.asyncio
    async def test_increment_manager_transfers(self):
        """Test manager transfer counter."""
        state = ClientState()

        assert state._manager_transfers_received == 0

        await state.increment_manager_transfers()
        await state.increment_manager_transfers()
        await state.increment_manager_transfers()

        assert state._manager_transfers_received == 3

    @pytest.mark.asyncio
    async def test_increment_rerouted(self):
        """Test rerouted requests counter."""
        state = ClientState()

        assert state._requests_rerouted == 0

        await state.increment_rerouted()

        assert state._requests_rerouted == 1

    @pytest.mark.asyncio
    async def test_increment_failed_leadership_change(self):
        """Test failed leadership change counter."""
        state = ClientState()

        assert state._requests_failed_leadership_change == 0

        await state.increment_failed_leadership_change()
        await state.increment_failed_leadership_change()

        assert state._requests_failed_leadership_change == 2

    @pytest.mark.asyncio
    async def test_get_leadership_metrics(self):
        """Test leadership metrics retrieval."""
        state = ClientState()

        await state.increment_gate_transfers()
        await state.increment_gate_transfers()
        await state.increment_manager_transfers()
        await state.increment_rerouted()
        await state.increment_failed_leadership_change()

        metrics = state.get_leadership_metrics()

        assert metrics["gate_transfers_received"] == 2
        assert metrics["manager_transfers_received"] == 1
        assert metrics["requests_rerouted"] == 1
        assert metrics["requests_failed_leadership_change"] == 1

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

        await asyncio.gather(*[update_gate_leader(i) for i in range(10)])

        # Final state should have latest update
        assert job_id in state._gate_job_leaders

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
