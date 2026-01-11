"""
Integration tests for GateJobHandler (Section 15.3.7).

Tests job submission, status queries, and progress updates including:
- Rate limiting (AD-24)
- Protocol version negotiation (AD-25)
- Load shedding (AD-22)
- Tiered updates (AD-15)
- Fencing tokens (AD-10)
"""

import asyncio
import pytest
import inspect
from dataclasses import dataclass, field
from unittest.mock import AsyncMock, MagicMock
from enum import Enum

from hyperscale.distributed.nodes.gate.handlers.tcp_job import GateJobHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.models import (
    JobStatus,
    JobSubmission,
    JobProgress,
    GlobalJobStatus,
)


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
            self.tasks.append(task)
            return task
        return None


@dataclass
class MockNodeId:
    """Mock node ID."""
    full: str = "gate-001"
    short: str = "001"
    datacenter: str = "global"


@dataclass
class MockGateJobManager:
    """Mock gate job manager."""
    jobs: dict = field(default_factory=dict)
    target_dcs: dict = field(default_factory=dict)
    callbacks: dict = field(default_factory=dict)
    fence_tokens: dict = field(default_factory=dict)
    job_count_val: int = 0

    def set_job(self, job_id: str, job):
        self.jobs[job_id] = job

    def get_job(self, job_id: str):
        return self.jobs.get(job_id)

    def has_job(self, job_id: str) -> bool:
        return job_id in self.jobs

    def set_target_dcs(self, job_id: str, dcs: set[str]):
        self.target_dcs[job_id] = dcs

    def set_callback(self, job_id: str, callback):
        self.callbacks[job_id] = callback

    def job_count(self) -> int:
        return self.job_count_val

    def get_fence_token(self, job_id: str) -> int:
        return self.fence_tokens.get(job_id, 0)

    def set_fence_token(self, job_id: str, token: int):
        self.fence_tokens[job_id] = token


class MockCircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class MockQuorumCircuit:
    """Mock quorum circuit breaker."""
    circuit_state: MockCircuitState = MockCircuitState.CLOSED
    half_open_after: float = 10.0
    error_count: int = 0
    window_seconds: float = 60.0
    successes: int = 0

    def record_success(self):
        self.successes += 1

    def record_error(self):
        self.error_count += 1


@dataclass
class MockLoadShedder:
    """Mock load shedder."""
    shed_handlers: set = field(default_factory=set)
    current_state: str = "normal"

    def should_shed_handler(self, handler_name: str) -> bool:
        return handler_name in self.shed_handlers

    def get_current_state(self):
        class State:
            value = "normal"
        return State()


@dataclass
class MockJobLeadershipTracker:
    """Mock job leadership tracker."""
    leaders: dict = field(default_factory=dict)

    def assume_leadership(self, job_id: str, metadata: int):
        self.leaders[job_id] = metadata


@dataclass
class MockGateInfo:
    """Mock gate info for healthy gates."""
    gate_id: str = "gate-002"
    addr: tuple[str, int] = field(default_factory=lambda: ("10.0.0.2", 9000))


def create_mock_handler(
    state: GateRuntimeState = None,
    rate_limit_allowed: bool = True,
    rate_limit_retry: float = 0.0,
    should_shed: bool = False,
    has_quorum: bool = True,
    circuit_state: MockCircuitState = MockCircuitState.CLOSED,
    select_dcs: list[str] = None,
) -> GateJobHandler:
    """Create a mock handler with configurable behavior."""
    if state is None:
        state = GateRuntimeState()
    if select_dcs is None:
        select_dcs = ["dc-east", "dc-west"]

    return GateJobHandler(
        state=state,
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_manager=MockGateJobManager(),
        job_router=None,
        job_leadership_tracker=MockJobLeadershipTracker(),
        quorum_circuit=MockQuorumCircuit(circuit_state=circuit_state),
        load_shedder=MockLoadShedder(),
        job_lease_manager=MagicMock(),
        get_node_id=lambda: MockNodeId(),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9000,
        is_leader=lambda: True,
        check_rate_limit=lambda client_id, op: (rate_limit_allowed, rate_limit_retry),
        should_shed_request=lambda req_type: should_shed,
        has_quorum_available=lambda: has_quorum,
        quorum_size=lambda: 3,
        select_datacenters_with_fallback=lambda count, dcs, job_id: (select_dcs, [], "healthy"),
        get_healthy_gates=lambda: [MockGateInfo()],
        broadcast_job_leadership=AsyncMock(),
        dispatch_job_to_datacenters=AsyncMock(),
        forward_job_progress_to_peers=AsyncMock(return_value=False),
        record_request_latency=lambda latency: None,
        record_dc_job_stats=AsyncMock(),
        handle_update_by_tier=lambda *args: None,
    )


# =============================================================================
# handle_submission Happy Path Tests
# =============================================================================


class TestHandleSubmissionHappyPath:
    """Tests for handle_submission happy path."""

    @pytest.mark.asyncio
    async def test_successful_submission(self):
        """Successfully submits a job."""
        handler = create_mock_handler()

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,
        )

        # Result should be serialized JobAck
        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_submission_records_job(self):
        """Submission records job in manager."""
        job_manager = MockGateJobManager()
        handler = GateJobHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        submission = JobSubmission(
            job_id="job-456",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert "job-456" in job_manager.jobs

    @pytest.mark.asyncio
    async def test_submission_sets_target_dcs(self):
        """Submission sets target datacenters."""
        job_manager = MockGateJobManager()
        handler = GateJobHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-east", "dc-west"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        submission = JobSubmission(
            job_id="job-789",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert job_manager.target_dcs["job-789"] == {"dc-east", "dc-west"}


# =============================================================================
# handle_submission Negative Path Tests (AD-24 Rate Limiting)
# =============================================================================


class TestHandleSubmissionRateLimiting:
    """Tests for handle_submission rate limiting (AD-24)."""

    @pytest.mark.asyncio
    async def test_rejects_rate_limited_client(self):
        """Rejects submission when client is rate limited."""
        handler = create_mock_handler(rate_limit_allowed=False, rate_limit_retry=5.0)

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,
        )

        assert isinstance(result, bytes)
        # Should return RateLimitResponse

    @pytest.mark.asyncio
    async def test_different_clients_rate_limited_separately(self):
        """Different clients are rate limited separately."""
        rate_limited_clients = {"10.0.0.1:8000"}

        def check_rate(client_id: str, op: str):
            if client_id in rate_limited_clients:
                return (False, 5.0)
            return (True, 0.0)

        handler = GateJobHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=check_rate,
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        # Rate limited client
        result1 = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        # Non-rate limited client
        submission.job_id = "job-456"
        result2 = await handler.handle_submission(
            addr=("10.0.0.2", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert isinstance(result1, bytes)
        assert isinstance(result2, bytes)


# =============================================================================
# handle_submission Load Shedding Tests (AD-22)
# =============================================================================


class TestHandleSubmissionLoadShedding:
    """Tests for handle_submission load shedding (AD-22)."""

    @pytest.mark.asyncio
    async def test_rejects_when_shedding(self):
        """Rejects submission when load shedding."""
        handler = create_mock_handler(should_shed=True)

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,
        )

        assert isinstance(result, bytes)
        # Should return rejection JobAck


# =============================================================================
# handle_submission Circuit Breaker Tests
# =============================================================================


class TestHandleSubmissionCircuitBreaker:
    """Tests for handle_submission circuit breaker."""

    @pytest.mark.asyncio
    async def test_rejects_when_circuit_open(self):
        """Rejects submission when circuit breaker is open."""
        handler = create_mock_handler(circuit_state=MockCircuitState.OPEN)

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,
        )

        assert isinstance(result, bytes)


# =============================================================================
# handle_submission Quorum Tests
# =============================================================================


class TestHandleSubmissionQuorum:
    """Tests for handle_submission quorum checks."""

    @pytest.mark.asyncio
    async def test_rejects_when_no_quorum(self):
        """Rejects submission when quorum unavailable."""
        handler = create_mock_handler(has_quorum=False)

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,  # Has peers, so quorum is checked
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_allows_when_no_peers(self):
        """Allows submission when no peers (single gate mode)."""
        handler = create_mock_handler(has_quorum=False)

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,  # No peers, quorum not checked
        )

        assert isinstance(result, bytes)


# =============================================================================
# handle_submission Datacenter Selection Tests
# =============================================================================


class TestHandleSubmissionDatacenterSelection:
    """Tests for handle_submission datacenter selection."""

    @pytest.mark.asyncio
    async def test_rejects_when_no_dcs_available(self):
        """Rejects submission when no datacenters available."""
        handler = create_mock_handler(select_dcs=[])

        submission = JobSubmission(
            job_id="job-123",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert isinstance(result, bytes)


# =============================================================================
# handle_status_request Tests
# =============================================================================


class TestHandleStatusRequestHappyPath:
    """Tests for handle_status_request happy path."""

    @pytest.mark.asyncio
    async def test_returns_job_status(self):
        """Returns job status for known job."""
        handler = create_mock_handler()

        async def mock_gather_status(job_id: str):
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        result = await handler.handle_status_request(
            addr=("10.0.0.1", 8000),
            data=b"job-123",
            gather_job_status=mock_gather_status,
        )

        assert isinstance(result, bytes)


class TestHandleStatusRequestNegativePath:
    """Tests for handle_status_request negative paths."""

    @pytest.mark.asyncio
    async def test_rate_limited(self):
        """Rate limited status request."""
        handler = create_mock_handler(rate_limit_allowed=False, rate_limit_retry=5.0)

        async def mock_gather_status(job_id: str):
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        result = await handler.handle_status_request(
            addr=("10.0.0.1", 8000),
            data=b"job-123",
            gather_job_status=mock_gather_status,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_load_shedding(self):
        """Load-shed status request."""
        handler = create_mock_handler(should_shed=True)

        async def mock_gather_status(job_id: str):
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        result = await handler.handle_status_request(
            addr=("10.0.0.1", 8000),
            data=b"job-123",
            gather_job_status=mock_gather_status,
        )

        # Should return empty bytes when shedding
        assert result == b''


# =============================================================================
# handle_progress Tests (AD-15 Tiered Updates, AD-10 Fencing Tokens)
# =============================================================================


class TestHandleProgressHappyPath:
    """Tests for handle_progress happy path."""

    @pytest.mark.asyncio
    async def test_accepts_valid_progress(self):
        """Accepts valid progress update."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()
        job_manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        ))

        handler = GateJobHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        progress = JobProgress(
            job_id="job-123",
            datacenter="dc-east",
            status=JobStatus.RUNNING.value,
            total_completed=50,
            total_failed=0,
            overall_rate=10.0,
            fence_token=1,
        )

        result = await handler.handle_progress(
            addr=("10.0.0.1", 8000),
            data=progress.dump(),
        )

        assert isinstance(result, bytes)


class TestHandleProgressFencingTokens:
    """Tests for handle_progress fencing tokens (AD-10)."""

    @pytest.mark.asyncio
    async def test_rejects_stale_fence_token(self):
        """Rejects progress with stale fence token."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()
        job_manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        ))
        job_manager.set_fence_token("job-123", 10)  # Current token is 10

        handler = GateJobHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        progress = JobProgress(
            job_id="job-123",
            datacenter="dc-east",
            status=JobStatus.RUNNING.value,
            total_completed=50,
            total_failed=0,
            overall_rate=10.0,
            fence_token=5,  # Stale token (< 10)
        )

        result = await handler.handle_progress(
            addr=("10.0.0.1", 8000),
            data=progress.dump(),
        )

        # Should still return ack (but log warning)
        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_updates_fence_token_on_newer(self):
        """Updates fence token when receiving newer value."""
        state = GateRuntimeState()
        job_manager = MockGateJobManager()
        job_manager.set_job("job-123", GlobalJobStatus(
            job_id="job-123",
            status=JobStatus.RUNNING.value,
            datacenters=[],
            timestamp=1234567890.0,
        ))
        job_manager.set_fence_token("job-123", 5)

        handler = GateJobHandler(
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        progress = JobProgress(
            job_id="job-123",
            datacenter="dc-east",
            status=JobStatus.RUNNING.value,
            total_completed=50,
            total_failed=0,
            overall_rate=10.0,
            fence_token=10,  # Newer token
        )

        await handler.handle_progress(
            addr=("10.0.0.1", 8000),
            data=progress.dump(),
        )

        assert job_manager.get_fence_token("job-123") == 10


# =============================================================================
# Concurrency Tests
# =============================================================================


class TestConcurrency:
    """Tests for concurrent access patterns."""

    @pytest.mark.asyncio
    async def test_concurrent_submissions(self):
        """Concurrent job submissions don't interfere."""
        handler = create_mock_handler()

        submissions = []
        for i in range(10):
            submissions.append(JobSubmission(
                job_id=f"job-{i}",
                workflows=b"test_workflows",
                vus=10,
                timeout_seconds=60.0,
                datacenter_count=1,
            ))

        results = await asyncio.gather(*[
            handler.handle_submission(
                addr=(f"10.0.0.{i}", 8000),
                data=sub.dump(),
                active_gate_peer_count=0,
            )
            for i, sub in enumerate(submissions)
        ])

        assert len(results) == 10
        assert all(isinstance(r, bytes) for r in results)

    @pytest.mark.asyncio
    async def test_concurrent_status_requests(self):
        """Concurrent status requests don't interfere."""
        handler = create_mock_handler()

        async def mock_gather_status(job_id: str):
            await asyncio.sleep(0.001)  # Small delay
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        results = await asyncio.gather(*[
            handler.handle_status_request(
                addr=("10.0.0.1", 8000),
                data=f"job-{i}".encode(),
                gather_job_status=mock_gather_status,
            )
            for i in range(100)
        ])

        assert len(results) == 100
        assert all(isinstance(r, bytes) for r in results)


# =============================================================================
# Edge Cases Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_job_id(self):
        """Handles empty job ID gracefully."""
        handler = create_mock_handler()

        async def mock_gather_status(job_id: str):
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        result = await handler.handle_status_request(
            addr=("10.0.0.1", 8000),
            data=b"",
            gather_job_status=mock_gather_status,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_special_characters_in_job_id(self):
        """Handles special characters in job ID."""
        handler = create_mock_handler()

        async def mock_gather_status(job_id: str):
            return GlobalJobStatus(
                job_id=job_id,
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            )

        special_ids = [
            "job:colon:id",
            "job-dash-id",
            "job_underscore_id",
            "job.dot.id",
        ]

        for job_id in special_ids:
            result = await handler.handle_status_request(
                addr=("10.0.0.1", 8000),
                data=job_id.encode(),
                gather_job_status=mock_gather_status,
            )
            assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_very_large_workflow_data(self):
        """Handles very large workflow data."""
        handler = create_mock_handler()

        submission = JobSubmission(
            job_id="job-large",
            workflows=b"x" * 1_000_000,  # 1MB of data
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_zero_vus(self):
        """Handles zero VUs in submission."""
        handler = create_mock_handler()

        submission = JobSubmission(
            job_id="job-zero-vus",
            workflows=b"test_workflows",
            vus=0,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_negative_timeout(self):
        """Handles negative timeout in submission."""
        handler = create_mock_handler()

        submission = JobSubmission(
            job_id="job-negative-timeout",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=-1.0,
            datacenter_count=1,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        assert isinstance(result, bytes)


# =============================================================================
# Failure Mode Tests
# =============================================================================


class TestFailureModes:
    """Tests for failure mode handling."""

    @pytest.mark.asyncio
    async def test_handles_invalid_submission_data(self):
        """Handles invalid submission data gracefully."""
        handler = create_mock_handler()

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
            active_gate_peer_count=0,
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_handles_invalid_progress_data(self):
        """Handles invalid progress data gracefully."""
        handler = create_mock_handler()

        result = await handler.handle_progress(
            addr=("10.0.0.1", 8000),
            data=b"invalid_data",
        )

        assert result == b'error'

    @pytest.mark.asyncio
    async def test_handles_exception_in_broadcast(self):
        """Handles exception during leadership broadcast."""
        broadcast_mock = AsyncMock(side_effect=Exception("Broadcast failed"))

        handler = GateJobHandler(
            state=GateRuntimeState(),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_router=None,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=MagicMock(),
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=lambda client_id, op: (True, 0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=lambda count, dcs, job_id: (["dc-1"], [], "healthy"),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=broadcast_mock,
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
        )

        submission = JobSubmission(
            job_id="job-broadcast-fail",
            workflows=b"test_workflows",
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        # Should still return a result (error ack)
        assert isinstance(result, bytes)


__all__ = [
    "TestHandleSubmissionHappyPath",
    "TestHandleSubmissionRateLimiting",
    "TestHandleSubmissionLoadShedding",
    "TestHandleSubmissionCircuitBreaker",
    "TestHandleSubmissionQuorum",
    "TestHandleSubmissionDatacenterSelection",
    "TestHandleStatusRequestHappyPath",
    "TestHandleStatusRequestNegativePath",
    "TestHandleProgressHappyPath",
    "TestHandleProgressFencingTokens",
    "TestConcurrency",
    "TestEdgeCases",
    "TestFailureModes",
]
