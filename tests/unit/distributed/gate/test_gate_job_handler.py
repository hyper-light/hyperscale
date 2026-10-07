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
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from enum import Enum

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.handlers.tcp_job import GateJobHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.protocol.transient_errors import is_transient_rejection
from hyperscale.distributed.idempotency.gate_cache import GateIdempotencyCache
from hyperscale.distributed.idempotency.idempotency_config import IdempotencyConfig
from hyperscale.distributed.idempotency.idempotency_key import IdempotencyKey
from hyperscale.distributed.models import JobStatusQuery
from hyperscale.distributed.models import (
    JobAck,
    JobStatus,
    JobSubmission,
    JobProgress,
    GlobalJobStatus,
)

# The gate's configured TCP timeouts, as a default Env gives them.
GATE_SETTINGS = Env()

cloudpickle.register_pickle_by_value(sys.modules[__name__])


class Checkout(Workflow):
    vus = 1

    @step()
    async def check_out(self) -> dict:
        return {}


class LargeCheckout(Workflow):
    vus = 1
    # About a megabyte of workflow state travels with the submission.
    catalog = "x" * 1_000_000

    @step()
    async def check_out(self) -> dict:
        return {}


# A job's workflows as a client submits them: (id, dependencies, workflow).
WORKFLOWS = cloudpickle.dumps([("wf-1", [], Checkout())])


class LoginStep(Workflow):
    vus = 1
    duration = "20s"

    @step()
    async def log_in(self) -> dict:
        return {}


class BrowseStep(Workflow):
    vus = 1
    duration = "20s"

    @step()
    async def browse(self) -> dict:
        return {}


class SearchStep(Workflow):
    vus = 1
    duration = "30s"

    @step()
    async def search(self) -> dict:
        return {}


# BrowseStep runs after LoginStep; SearchStep alongside both. Each
# workflow's workers observe its duration times the default multiplier.
CHAIN_WORKFLOWS = cloudpickle.dumps(
    [
        ("wf-login", [], LoginStep()),
        ("wf-browse", ["LoginStep"], BrowseStep()),
        ("wf-search", [], SearchStep()),
    ]
)
CHAIN_BUDGET_SECONDS = 40.0 * GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER


class BlockedModulePayload:
    """Unpickled by a plain unpickler, it runs ``subprocess.getoutput``."""

    def __reduce__(self):
        return (subprocess.getoutput, ("true",))


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
        # Production TaskRunner.run accepts run-level kwargs (e.g. ``alias``)
        # that are NOT forwarded to the coroutine, and returns a run handle
        # exposing ``.token``.
        if inspect.iscoroutinefunction(coro):
            task = asyncio.create_task(coro(*args))
            self.tasks.append(task)
            return SimpleNamespace(token=f"token-{len(self.tasks)}", task=task)
        return None

    async def cancel(self, token: str):
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

    def get_target_dcs(self, job_id: str) -> set[str]:
        return self.target_dcs.get(job_id, set())

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


def make_async_rate_limiter(allowed: bool = True, retry_after: float = 0.0):
    async def check_rate_limit(client_id: str, op: str, handler_name: str) -> tuple[bool, float]:
        return (allowed, retry_after)

    return check_rate_limit


def make_lease_manager(fence_token: int = 1, lease_duration: float = 30.0):
    """Build an async job lease manager mock that grants leases."""
    lease = SimpleNamespace(
        fence_token=fence_token,
        lease_duration=lease_duration,
    )
    manager = MagicMock()
    manager.acquire = AsyncMock(return_value=lease)
    manager.release = AsyncMock(return_value=None)
    manager.renew = AsyncMock(return_value=True)
    return manager


def make_replication_coordinator(committed: bool = True):
    """Build a replication coordinator mock whose quorum replication succeeds."""
    coordinator = MagicMock()
    coordinator.replicate_with_quorum = AsyncMock(return_value=committed)
    return coordinator


def make_recording_dispatch(job_manager):
    """Async dispatch callback that records the job, mirroring the real
    dispatch coordinator (which now owns job/target-DC recording)."""

    async def dispatch(submission, target_dcs):
        job_manager.set_job(
            submission.job_id,
            GlobalJobStatus(
                job_id=submission.job_id,
                status=JobStatus.SUBMITTED.value,
                datacenters=[],
                timestamp=0.0,
            ),
        )
        job_manager.set_target_dcs(submission.job_id, set(target_dcs))

    return dispatch


def answering(gather_status):
    """A gate's status answering over a gather stub: the gathered status,
    or no answer when the gate holds no such job."""

    async def answer_status_query(query: JobStatusQuery) -> bytes:
        status = await gather_status(query.job_id)
        return status.dump() if status is not None else b""

    return answer_status_query


def create_mock_handler(
    state: GateRuntimeState = None,
    rate_limit_allowed: bool = True,
    rate_limit_retry: float = 0.0,
    should_shed: bool = False,
    has_quorum: bool = True,
    circuit_state: MockCircuitState = MockCircuitState.CLOSED,
    select_dcs: list[str] = None,
    cluster_formed: Callable[[], bool] = lambda: True,
    cluster_read_only: Callable[[], bool] = lambda: False,
    job_manager: "MockGateJobManager | None" = None,
    job_lease_manager: MagicMock | None = None,
    idempotency_cache: GateIdempotencyCache[bytes] | None = None,
) -> GateJobHandler:
    """Create a mock handler with configurable behavior."""
    if state is None:
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
    if select_dcs is None:
        select_dcs = ["dc-east", "dc-west"]

    async def mock_check_rate_limit(client_id, op, handler_name):
        return (rate_limit_allowed, rate_limit_retry)

    return GateJobHandler(
        clock=RealClock(),
        client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        state=state,
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_manager=job_manager if job_manager is not None else MockGateJobManager(),
        job_leadership_tracker=MockJobLeadershipTracker(),
        quorum_circuit=MockQuorumCircuit(circuit_state=circuit_state),
        load_shedder=MockLoadShedder(),
        job_lease_manager=(
            job_lease_manager if job_lease_manager is not None else make_lease_manager()
        ),
        idempotency_cache=idempotency_cache,
        send_tcp=AsyncMock(),
        replication_coordinator=make_replication_coordinator(),
        get_active_peer_addrs=lambda: [],
        get_node_id=lambda: MockNodeId(),
        get_host=lambda: "127.0.0.1",
        get_tcp_port=lambda: 9000,
        is_leader=lambda: True,
        check_rate_limit=mock_check_rate_limit,
        should_shed_request=lambda req_type: should_shed,
        has_quorum_available=lambda: has_quorum,
        quorum_size=lambda: 3,
        select_datacenters_with_fallback=AsyncMock(return_value=(
            select_dcs,
            [],
            "healthy",
        )),
        get_healthy_gates=lambda: [MockGateInfo()],
        broadcast_job_leadership=AsyncMock(),
        dispatch_job_to_datacenters=AsyncMock(),
        forward_job_progress_to_peers=AsyncMock(return_value=False),
        record_request_latency=lambda latency: None,
        record_dc_job_stats=AsyncMock(),
        handle_update_by_tier=lambda *args: None,
        default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
        current_raft_members=lambda: frozenset({"gate-001"}),
        cluster_formed=cluster_formed,
        cluster_read_only=cluster_read_only,
        cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
        overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
        replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
    )


# =============================================================================
# handle_submission before the gate tier's membership forms (AD-52)
# =============================================================================


class TestDuplicateSubmissions:
    """AD-40: a submission whose idempotency key was already decided gets
    the original decision, for the original job, marked as a duplicate's
    -- even when the retry carried a fresh job id."""

    @pytest.mark.asyncio
    async def test_a_retry_with_a_decided_key_is_answered_for_the_original_job(self):
        handler = create_mock_handler(
            idempotency_cache=GateIdempotencyCache(IdempotencyConfig(), task_runner=None, logger=None),
        )
        idempotency_key = str(IdempotencyKey(client_id="client-1", sequence=1, nonce="nonce-1"))

        first = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000),
                data=JobSubmission(
                    job_id="job-original",
                    workflows=WORKFLOWS,
                    vus=10,
                    timeout_seconds=60.0,
                    datacenter_count=2,
                    idempotency_key=idempotency_key,
                ).dump(),
                active_gate_peer_count=2,
            )
        )
        retry = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000),
                data=JobSubmission(
                    job_id="job-retried-under-a-fresh-id",
                    workflows=WORKFLOWS,
                    vus=10,
                    timeout_seconds=60.0,
                    datacenter_count=2,
                    idempotency_key=idempotency_key,
                ).dump(),
                active_gate_peer_count=2,
            )
        )

        assert first.accepted and not first.was_duplicate
        assert retry.was_duplicate and retry.accepted == first.accepted
        assert retry.job_id == retry.original_job_id == "job-original"


class TestHandleSubmissionBeforeMembershipForms:
    """A job's Raft group is founded with the gate tier's committed
    members: before the membership group forms there are none, so the job
    is refused -- retryably, holding nothing -- and accepted once it has."""

    @pytest.mark.asyncio
    async def test_a_job_waits_for_the_gate_tiers_membership(self):
        membership = {"formed": False}
        job_manager = MockGateJobManager()
        job_lease_manager = make_lease_manager()
        handler = create_mock_handler(
            cluster_formed=lambda: membership["formed"],
            job_manager=job_manager,
            job_lease_manager=job_lease_manager,
        )
        submission = JobSubmission(
            job_id="job-123",
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        refused = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000), data=submission.dump(), active_gate_peer_count=2
            )
        )
        held_before_formation = (job_lease_manager.acquire.await_count, job_manager.has_job("job-123"))

        membership["formed"] = True
        accepted = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000), data=submission.dump(), active_gate_peer_count=2
            )
        )

        assert (refused.accepted, refused.error) == (
            False,
            "Gate cluster membership not formed yet; retry",
        )
        # Retryable: the client backs off and submits again.
        assert is_transient_rejection(refused.error)
        assert held_before_formation == (0, False)
        assert accepted.accepted, accepted.error


class TestHandleSubmissionWhileReadOnly:
    """AD-52 section 13: an operator's read-only mode refuses job
    submissions -- not retryably: the client hears it at once -- and holds
    nothing; open again, the same job is accepted."""

    @pytest.mark.asyncio
    async def test_a_read_only_cluster_refuses_jobs(self):
        mode = {"read_only": True}
        job_manager = MockGateJobManager()
        job_lease_manager = make_lease_manager()
        handler = create_mock_handler(
            cluster_read_only=lambda: mode["read_only"],
            job_manager=job_manager,
            job_lease_manager=job_lease_manager,
        )
        submission = JobSubmission(
            job_id="job-123",
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        refused = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000), data=submission.dump(), active_gate_peer_count=2
            )
        )
        held_while_read_only = (job_lease_manager.acquire.await_count, job_manager.has_job("job-123"))

        mode["read_only"] = False
        accepted = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000), data=submission.dump(), active_gate_peer_count=2
            )
        )

        assert (refused.accepted, refused.error) == (
            False,
            "Gate cluster is read-only: job submissions are refused",
        )
        assert not is_transient_rejection(refused.error)
        assert held_while_read_only == (0, False)
        assert accepted.accepted, accepted.error


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
            workflows=WORKFLOWS,
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
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=make_recording_dispatch(job_manager),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

        submission = JobSubmission(
            job_id="job-456",
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=1,
        )

        await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        # Job recording now happens in the dispatch coordinator, which the
        # handler schedules as a background task; let it run.
        for _ in range(5):
            await asyncio.sleep(0)

        assert "job-456" in job_manager.jobs

    @pytest.mark.asyncio
    async def test_submission_sets_target_dcs(self):
        """Submission sets target datacenters."""
        job_manager = MockGateJobManager()
        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-east", "dc-west"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=make_recording_dispatch(job_manager),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

        submission = JobSubmission(
            job_id="job-789",
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=0,
        )

        for _ in range(5):
            await asyncio.sleep(0)

        assert job_manager.target_dcs["job-789"] == {"dc-east", "dc-west"}


class TestHandleSubmissionReadsWorkflows:
    """A gate reads a job's workflows as its managers do, before admitting
    it: through the restricted unpickler (a plain one ran whatever a
    payload named, and a payload it could not read was admitted anyway),
    in dependency order, and -- submitted without a timeout of its own --
    for the budget of its longest workflow chain, which every gate times
    it by and its managers are given (taken as zero, it timed out at the
    first check)."""

    @pytest.mark.asyncio
    async def test_a_job_without_a_timeout_is_given_its_workflow_chain_budget(self):
        handler = create_mock_handler()
        dispatched: list[JobSubmission] = []

        async def record_dispatch(submission, target_dcs):
            dispatched.append(submission)

        handler._dispatch_job_to_datacenters = record_dispatch

        ack = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000),
                data=JobSubmission(
                    job_id="job-chain",
                    workflows=CHAIN_WORKFLOWS,
                    vus=1,
                    timeout_seconds=0.0,
                ).dump(),
                active_gate_peer_count=0,
            )
        )
        for _ in range(5):
            await asyncio.sleep(0)

        [replicated] = [
            JobSubmission.load(call.kwargs["replica"].submission_payload)
            for call in handler._replication_coordinator.replicate_with_quorum.await_args_list
        ]
        assert ack.accepted
        assert [submission.timeout_seconds for submission in dispatched] == [CHAIN_BUDGET_SECONDS]
        assert replicated.timeout_seconds == CHAIN_BUDGET_SECONDS
        # The workers' own deadlines still come from each workflow.
        assert not replicated.timeout_seconds_explicit

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("workflows", "reason"),
        [
            (b"not a pickle", "Invalid workflows: "),
            (
                cloudpickle.dumps([("wf-1", [], BlockedModulePayload())]),
                "Invalid workflows: SecurityError",
            ),
            (
                cloudpickle.dumps(
                    [
                        ("wf-login", ["BrowseStep"], LoginStep()),
                        ("wf-browse", ["LoginStep"], BrowseStep()),
                    ]
                ),
                "Invalid workflows: ValueError: workflow dependencies form a cycle",
            ),
        ],
        ids=["unreadable", "blocked-module", "dependency-cycle"],
    )
    async def test_workflows_no_manager_could_run_are_refused(self, workflows, reason):
        handler = create_mock_handler()

        ack = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000),
                data=JobSubmission(
                    job_id="job-unrunnable",
                    workflows=workflows,
                    vus=1,
                    timeout_seconds=60.0,
                ).dump(),
                active_gate_peer_count=0,
            )
        )

        assert not ack.accepted
        assert ack.error.startswith(reason)
        handler._replication_coordinator.replicate_with_quorum.assert_not_awaited()


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
            workflows=WORKFLOWS,
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

        async def check_rate(client_id: str, op: str, handler_name: str):
            if client_id in rate_limited_clients:
                return (False, 5.0)
            return (True, 0.0)

        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=check_rate,
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

        submission = JobSubmission(
            job_id="job-123",
            workflows=WORKFLOWS,
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
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=2,
        )

        result = await handler.handle_submission(
            addr=("10.0.0.1", 8000),
            data=submission.dump(),
            active_gate_peer_count=2,
        )

        # Refused, with when to retry: the gate's overload verdict cannot
        # change before its next sample.
        ack = JobAck.load(result)
        assert not ack.accepted
        assert ack.retry_after_seconds == GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS


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
            workflows=WORKFLOWS,
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
            workflows=WORKFLOWS,
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
            workflows=WORKFLOWS,
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
    @pytest.mark.parametrize(
        ("datacenter_count", "datacenters"),
        [(0, []), (-1, []), (3, ["dc-east", "dc-west"]), (2, ["dc-east", "dc-east"])],
    )
    async def test_rejects_a_job_that_cannot_be_placed_as_asked(
        self,
        datacenter_count: int,
        datacenters: list[str],
    ):
        """A job runs in at least one datacenter and in no more than it
        lists; anything else is refused before any datacenter is chosen
        (a non-positive count sliced the routing order from its end)."""
        selections: list[tuple[int, list[str] | None, str]] = []
        handler = create_mock_handler()

        async def select_datacenters_with_fallback(count, listed, job_id, dispatch_latency_budget_ms=0.0):
            selections.append((count, listed, job_id))
            return (["dc-east"], [], "healthy")

        handler._select_datacenters_with_fallback = select_datacenters_with_fallback

        submission = JobSubmission(
            job_id="job-123",
            workflows=WORKFLOWS,
            vus=10,
            timeout_seconds=60.0,
            datacenter_count=datacenter_count,
            datacenters=datacenters,
        )

        ack = JobAck.load(
            await handler.handle_submission(
                addr=("10.0.0.1", 8000),
                data=submission.dump(),
                active_gate_peer_count=0,
            )
        )

        assert ack.accepted is False
        assert ack.error.startswith("Unplaceable job")
        assert selections == []

    @pytest.mark.asyncio
    async def test_rejects_when_no_dcs_available(self):
        """Rejects submission when no datacenters available."""
        handler = create_mock_handler(select_dcs=[])

        submission = JobSubmission(
            job_id="job-123",
            workflows=WORKFLOWS,
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

        queries: list[JobStatusQuery] = []

        async def answer_status_query(query: JobStatusQuery) -> bytes:
            queries.append(query)
            return await answering(mock_gather_status)(query)

        result = await handler.handle_status_request(
            addr=("10.0.0.1", 8000),
            data=b"job-123",
            answer_status_query=answer_status_query,
        )

        # A bare job id (an older client) is an EVENTUAL read.
        assert [(query.job_id, query.consistency) for query in queries] == [("job-123", "eventual")]
        assert GlobalJobStatus.load(result).job_id == "job-123"


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
            answer_status_query=answering(mock_gather_status),
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
            answer_status_query=answering(mock_gather_status),
        )

        # Should return empty bytes when shedding
        assert result == b""


# =============================================================================
# handle_progress Tests (AD-15 Tiered Updates, AD-10 Fencing Tokens)
# =============================================================================


class TestHandleProgressHappyPath:
    """Tests for handle_progress happy path."""

    @pytest.mark.asyncio
    async def test_accepts_valid_progress(self):
        """Accepts valid progress update."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_manager = MockGateJobManager()
        job_manager.set_job(
            "job-123",
            GlobalJobStatus(
                job_id="job-123",
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            ),
        )

        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
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
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_manager = MockGateJobManager()
        job_manager.set_job(
            "job-123",
            GlobalJobStatus(
                job_id="job-123",
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            ),
        )
        job_manager.set_fence_token("job-123", 10)

        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

        progress = JobProgress(
            job_id="job-123",
            datacenter="dc-east",
            status=JobStatus.RUNNING.value,
            total_completed=50,
            total_failed=0,
            overall_rate=10.0,
            fence_token=5,
        )

        result = await handler.handle_progress(
            addr=("10.0.0.1", 8000),
            data=progress.dump(),
        )

        assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_updates_fence_token_on_newer(self):
        """Updates fence token when receiving newer value."""
        state = GateRuntimeState(forward_throughput_interval_start=0.0)
        job_manager = MockGateJobManager()
        job_manager.set_job(
            "job-123",
            GlobalJobStatus(
                job_id="job-123",
                status=JobStatus.RUNNING.value,
                datacenters=[],
                timestamp=1234567890.0,
            ),
        )
        job_manager.set_fence_token("job-123", 5)

        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=state,
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=job_manager,
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=AsyncMock(),
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
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
            submissions.append(
                JobSubmission(
                    job_id=f"job-{i}",
                    workflows=WORKFLOWS,
                    vus=10,
                    timeout_seconds=60.0,
                    datacenter_count=1,
                )
            )

        results = await asyncio.gather(
            *[
                handler.handle_submission(
                    addr=(f"10.0.0.{i}", 8000),
                    data=sub.dump(),
                    active_gate_peer_count=0,
                )
                for i, sub in enumerate(submissions)
            ]
        )

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

        results = await asyncio.gather(
            *[
                handler.handle_status_request(
                    addr=("10.0.0.1", 8000),
                    data=f"job-{i}".encode(),
                    answer_status_query=answering(mock_gather_status),
                )
                for i in range(100)
            ]
        )

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
            answer_status_query=answering(mock_gather_status),
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
                answer_status_query=answering(mock_gather_status),
            )
            assert isinstance(result, bytes)

    @pytest.mark.asyncio
    async def test_very_large_workflow_data(self):
        """Handles very large workflow data."""
        handler = create_mock_handler()

        submission = JobSubmission(
            job_id="job-large",
            workflows=cloudpickle.dumps([("wf-1", [], LargeCheckout())]),
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
            workflows=WORKFLOWS,
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
            workflows=WORKFLOWS,
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

        assert result == b"error"

    @pytest.mark.asyncio
    async def test_handles_exception_in_broadcast(self):
        """Handles exception during leadership broadcast."""
        broadcast_mock = AsyncMock(side_effect=Exception("Broadcast failed"))

        handler = GateJobHandler(
            clock=RealClock(),
            client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=MockLogger(),
            task_runner=MockTaskRunner(),
            job_manager=MockGateJobManager(),
            job_leadership_tracker=MockJobLeadershipTracker(),
            quorum_circuit=MockQuorumCircuit(),
            load_shedder=MockLoadShedder(),
            job_lease_manager=make_lease_manager(),
            idempotency_cache=None,
            send_tcp=AsyncMock(),
            replication_coordinator=make_replication_coordinator(),
            get_active_peer_addrs=lambda: [],
            get_node_id=lambda: MockNodeId(),
            get_host=lambda: "127.0.0.1",
            get_tcp_port=lambda: 9000,
            is_leader=lambda: True,
            check_rate_limit=make_async_rate_limiter(allowed=True, retry_after=0),
            should_shed_request=lambda req_type: False,
            has_quorum_available=lambda: True,
            quorum_size=lambda: 3,
            select_datacenters_with_fallback=AsyncMock(return_value=(
                ["dc-1"],
                [],
                "healthy",
            )),
            get_healthy_gates=lambda: [],
            broadcast_job_leadership=broadcast_mock,
            dispatch_job_to_datacenters=AsyncMock(),
            forward_job_progress_to_peers=AsyncMock(return_value=False),
            record_request_latency=lambda latency: None,
            record_dc_job_stats=AsyncMock(),
            handle_update_by_tier=lambda *args: None,
            default_timeout_multiplier=GATE_SETTINGS.HYPERSCALE_DEFAULT_WORKER_TIMEOUT_MULTIPLIER,
            current_raft_members=lambda: frozenset({"gate-001"}),
            cluster_formed=lambda: True,
            cluster_read_only=lambda: False,
            cluster_formation_retry_after_seconds=lambda: GATE_SETTINGS.CLUSTER_FORMATION_INTERVAL_SECONDS,
            overload_retry_after_seconds=GATE_SETTINGS.OVERLOAD_SAMPLE_INTERVAL_SECONDS,
            replication_retry_after_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )

        submission = JobSubmission(
            job_id="job-broadcast-fail",
            workflows=WORKFLOWS,
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
