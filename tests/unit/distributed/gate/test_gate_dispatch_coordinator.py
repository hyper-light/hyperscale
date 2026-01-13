"""
Integration tests for GateDispatchCoordinator (Section 15.3.7).

Tests job dispatch coordination to datacenter managers including:
- Rate limiting (AD-22, AD-24)
- Protocol version negotiation (AD-25)
- Circuit breaker and quorum checks
- Datacenter selection (AD-36)
"""

import asyncio
import pytest
import inspect
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

from hyperscale.distributed.nodes.gate.dispatch_coordinator import (
    GateDispatchCoordinator,
)
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.swim.core import CircuitState


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
        if inspect.iscoroutinefunction(coro):
            task = asyncio.create_task(coro(*args, **kwargs))
        else:
            task = asyncio.create_task(asyncio.coroutine(lambda: None)())
        self.tasks.append(task)
        return task


@dataclass
class MockGateJobManager:
    """Mock gate job manager."""

    jobs: dict = field(default_factory=dict)
    target_dcs: dict = field(default_factory=dict)
    callbacks: dict = field(default_factory=dict)
    job_count_val: int = 0

    def set_job(self, job_id: str, job):
        self.jobs[job_id] = job

    def set_target_dcs(self, job_id: str, dcs: set[str]):
        self.target_dcs[job_id] = dcs

    def set_callback(self, job_id: str, callback):
        self.callbacks[job_id] = callback

    def job_count(self) -> int:
        return self.job_count_val


@dataclass
class MockQuorumCircuit:
    """Mock quorum circuit breaker."""

    circuit_state: CircuitState = CircuitState.CLOSED
    half_open_after: float = 10.0
    successes: int = 0

    def record_success(self):
        self.successes += 1


@dataclass
class MockJobSubmission:
    """Mock job submission."""

    job_id: str = "job-123"
    workflows: bytes = b"test_workflows"
    vus: int = 10
    timeout_seconds: float = 60.0
    datacenter_count: int = 2
    datacenters: list[str] | None = None
    callback_addr: tuple[str, int] | None = None
    reporting_configs: bytes | None = None
    protocol_version_major: int = 1
    protocol_version_minor: int = 0
    capabilities: str = ""


# =============================================================================
# Async Mock Helpers
# =============================================================================


def make_async_rate_limiter(allowed: bool = True, retry_after: float = 0.0):
    """Create an async rate limiter function."""

    async def check_rate_limit(client_id: str, op: str) -> tuple[bool, float]:
        return (allowed, retry_after)

    return check_rate_limit


# =============================================================================
# _check_rate_and_load Tests
# =============================================================================


class TestCheckRateAndLoadHappyPath:
    """Tests for _check_rate_and_load happy path."""

    @pytest.mark.asyncio
    async def test_allows_when_no_limits(self):
        """Allows request when no rate limit or load shedding."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = await coordinator._check_rate_and_load("client-1", "job-1")

        assert result is None


class TestCheckRateAndLoadNegativePath:
    """Tests for _check_rate_and_load negative paths."""

    @pytest.mark.asyncio
    async def test_rejects_when_rate_limited(self):
        """Rejects request when rate limited."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=make_async_rate_limiter(allowed=False, retry_after=5.0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = await coordinator._check_rate_and_load("client-1", "job-1")

        assert result is not None
        assert result.accepted is False
        assert "Rate limited" in result.error

    @pytest.mark.asyncio
    async def test_rejects_when_shedding(self):
        """Rejects request when load shedding."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: True,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = await coordinator._check_rate_and_load("client-1", "job-1")

        assert result is not None
        assert result.accepted is False
        assert "under load" in result.error.lower()


# =============================================================================
# _check_protocol_version Tests
# =============================================================================


class TestCheckProtocolVersionHappyPath:
    """Tests for _check_protocol_version happy path."""

    def test_accepts_compatible_version(self):
        """Accepts compatible protocol version."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.protocol_version_major = 1
        submission.protocol_version_minor = 0

        rejection, negotiated = coordinator._check_protocol_version(submission)

        assert rejection is None


class TestCheckProtocolVersionNegativePath:
    """Tests for _check_protocol_version negative paths."""

    def test_rejects_incompatible_major_version(self):
        """Rejects incompatible major protocol version."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.protocol_version_major = 99  # Incompatible
        submission.protocol_version_minor = 0

        rejection, negotiated = coordinator._check_protocol_version(submission)

        assert rejection is not None
        assert rejection.accepted is False
        assert "Incompatible" in rejection.error


# =============================================================================
# _check_circuit_and_quorum Tests
# =============================================================================


class TestCheckCircuitAndQuorumHappyPath:
    """Tests for _check_circuit_and_quorum happy path."""

    def test_allows_when_healthy(self):
        """Allows request when circuit closed and quorum available."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(circuit_state=CircuitState.CLOSED),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = coordinator._check_circuit_and_quorum("job-1")

        assert result is None


class TestCheckCircuitAndQuorumNegativePath:
    """Tests for _check_circuit_and_quorum negative paths."""

    def test_rejects_when_circuit_open(self):
        """Rejects request when circuit breaker is open."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(circuit_state=CircuitState.OPEN),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = coordinator._check_circuit_and_quorum("job-1")

        assert result is not None
        assert result.accepted is False
        assert "Circuit" in result.error

    def test_rejects_when_no_quorum(self):
        """Rejects request when quorum unavailable."""
        state = GateRuntimeState()
        state.add_active_peer(("10.0.0.1", 9000))  # Has peers

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: False,  # No quorum
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(circuit_state=CircuitState.CLOSED),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = coordinator._check_circuit_and_quorum("job-1")

        assert result is not None
        assert result.accepted is False
        assert "Quorum" in result.error


# =============================================================================
# submit_job Tests
# =============================================================================


class TestSubmitJobHappyPath:
    """Tests for submit_job happy path."""

    @pytest.mark.asyncio
    async def test_successful_submission(self):
        """Successfully submits job."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()
        quorum_circuit = MockQuorumCircuit()
        broadcast = AsyncMock()
        dispatch = AsyncMock()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=quorum_circuit,
            select_datacenters=lambda count, dcs, job_id: (
                ["dc-east", "dc-west"],
                [],
                "healthy",
            ),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=broadcast,
            dispatch_to_dcs=dispatch,
        )

        submission = MockJobSubmission()
        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is True
        assert ack.job_id == "job-123"
        assert quorum_circuit.successes == 1
        broadcast.assert_called_once()


class TestSubmitJobNegativePath:
    """Tests for submit_job negative paths."""

    @pytest.mark.asyncio
    async def test_rejects_rate_limited(self):
        """Rejects rate-limited submission."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (False, 5.0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is False
        assert "Rate limited" in ack.error

    @pytest.mark.asyncio
    async def test_rejects_no_datacenters(self):
        """Rejects when no datacenters available."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: ([], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is False
        assert "No available datacenters" in ack.error

    @pytest.mark.asyncio
    async def test_rejects_initializing(self):
        """Rejects when datacenters are initializing."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (
                ["dc-1"],
                [],
                "initializing",
            ),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is False
        assert "initializing" in ack.error


# =============================================================================
# _setup_job_tracking Tests
# =============================================================================


class TestSetupJobTrackingHappyPath:
    """Tests for _setup_job_tracking happy path."""

    def test_sets_up_job_state(self):
        """Sets up job tracking state."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.callback_addr = ("10.0.0.1", 8000)

        coordinator._setup_job_tracking(submission, ["dc-east", "dc-west"])

        assert "job-123" in job_manager.jobs
        assert job_manager.target_dcs["job-123"] == {"dc-east", "dc-west"}
        assert job_manager.callbacks["job-123"] == ("10.0.0.1", 8000)
        assert state._progress_callbacks["job-123"] == ("10.0.0.1", 8000)

    def test_stores_submission_with_reporting(self):
        """Stores submission when reporting configs present."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.reporting_configs = b"config_data"

        coordinator._setup_job_tracking(submission, ["dc-east"])

        assert "job-123" in state._job_submissions


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_submissions(self):
        """Concurrent job submissions are handled safely."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submissions = [MockJobSubmission() for _ in range(10)]
        for i, sub in enumerate(submissions):
            sub.job_id = f"job-{i}"

        acks = await asyncio.gather(
            *[coordinator.submit_job(("10.0.0.1", 8000), sub) for sub in submissions]
        )

        # All should be accepted
        assert all(ack.accepted for ack in acks)
        assert len(job_manager.jobs) == 10


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_submission_with_no_callback(self):
        """Handles submission with no callback address."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.callback_addr = None

        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is True
        assert "job-123" not in state._progress_callbacks

    @pytest.mark.asyncio
    async def test_submission_with_many_dcs(self):
        """Handles submission targeting many datacenters."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()

        dcs = [f"dc-{i}" for i in range(50)]

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, specified, job_id: (dcs, [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        submission = MockJobSubmission()
        submission.datacenter_count = 50

        ack = await coordinator.submit_job(("10.0.0.1", 8000), submission)

        assert ack.accepted is True
        assert len(job_manager.target_dcs.get("job-123", set())) == 50

    def test_special_characters_in_client_id(self):
        """Handles special characters in client ID."""
        state = GateRuntimeState()

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        # Client ID is constructed from address
        result = coordinator._check_rate_and_load("10.0.0.1:8000", "job-1")
        assert result is None

    @pytest.mark.asyncio
    async def test_no_peers_quorum_check_skipped(self):
        """Quorum check is skipped when no peers."""
        state = GateRuntimeState()
        # No active peers

        coordinator = GateDispatchCoordinator(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: False,  # Would reject if checked
            quorum_size=lambda: 3,
            quorum_circuit=MockQuorumCircuit(),
            select_datacenters=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            assume_leadership=lambda job_id, count: None,
            broadcast_leadership=AsyncMock(),
            dispatch_to_dcs=AsyncMock(),
        )

        result = coordinator._check_circuit_and_quorum("job-1")

        # Should allow since no peers (quorum check skipped)
        assert result is None
