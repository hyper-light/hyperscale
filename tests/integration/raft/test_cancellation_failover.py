"""
Integration tests for cancellation during leadership failover.

Tests the interaction between:
- Job leadership transfer (SWIM leader + per-job Raft leader)
- Workflow cancellation push notification chain
- Worker orphan grace period handling
- Gate orphan job detection
- Single workflow cancellation through gate

Every test drives the real components -- trackers, coordinators, handlers,
state, job managers, ledgers and Raft groups -- with only the network edge
(workers, clients, peer nodes) recorded or answered by the test, and
collaborators outside the path under test failing the test if reached.
"""

import asyncio
import contextvars
import time
from pathlib import Path
from typing import Any, Callable, Coroutine, TypeVar
from unittest.mock import MagicMock

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.jobs.gates import GateJobManager
from hyperscale.distributed.jobs.gates.consistent_hash_ring import ConsistentHashRing
from hyperscale.distributed.jobs.job_leadership_tracker import JobLeadershipTracker
from hyperscale.distributed.jobs.job_manager import JobManager
from hyperscale.distributed.ledger.durability_level import DurabilityLevel
from hyperscale.distributed.ledger.job_ledger import JobLedger
from hyperscale.distributed.models import (
    GlobalJobStatus,
    JobCancellationComplete,
    JobCancelRequest,
    JobCancelResponse,
    JobInfo,
    JobLeadershipAck,
    JobStatus,
    JobStatusPush,
    NodeInfo,
    NodeRole,
    SingleWorkflowCancelRequest,
    SingleWorkflowCancelResponse,
    WorkerRegistration,
    WorkflowCancellationComplete,
    WorkflowCancellationStatus,
    WorkflowCancelRequest,
    WorkflowCancelResponse,
    WorkflowProgress,
)
from hyperscale.distributed.nodes.client.handlers.tcp_cancellation_complete import (
    CancellationCompleteHandler,
)
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.gate.config import derive_gate_orphan_grace_seconds
from hyperscale.distributed.nodes.gate.handlers.tcp_cancellation import GateCancellationHandler
from hyperscale.distributed.nodes.gate.orphan_job_coordinator import GateOrphanJobCoordinator
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.nodes.manager.cancellation import (
    ManagerCancellationCoordinator,
)
from hyperscale.distributed.nodes.manager.config import create_manager_config_from_env
from hyperscale.distributed.nodes.manager.leases import ManagerLeaseCoordinator
from hyperscale.distributed.nodes.manager.server import ManagerServer
from hyperscale.distributed.nodes.manager.state import ManagerState
from hyperscale.distributed.nodes.worker.cancellation import WorkerCancellationHandler
from hyperscale.distributed.nodes.worker.config import WorkerConfig
from hyperscale.distributed.nodes.worker.state import WorkerState
from hyperscale.distributed.reliability.rate_limiting import ServerRateLimiter
from hyperscale.distributed.runtime import (
    RealClock,
    restore_defaults,
    snapshot_defaults,
    swap_defaults,
)
from hyperscale.distributed.slo import SLOConfig
from hyperscale.distributed.swim.core import NodeId
from hyperscale.distributed.workflow import WorkflowState
from hyperscale.logging import LoggingConfig
from tests.integration.raft.test_ledger_replication import JOB_ID as REPLICATED_JOB_ID
from tests.integration.raft.test_ledger_replication import LedgerCluster
from tests.simulation.harness.sim import SimFilesystem, SimulationLoop, VirtualClock
from tests.unit.distributed.hlc.hlc_factory import new_hybrid_logical_clock

ScenarioResult = TypeVar("ScenarioResult")

MANAGER_HOST = "127.0.0.1"
MANAGER_TCP_PORT = 9000
GATE_CALLBACK_ADDR = ("127.0.0.1", 8000)
CLIENT_CALLBACK_ADDR = ("127.0.0.1", 7000)
SETTINGS = Env()
GATE_ORPHAN_GRACE_SECONDS = derive_gate_orphan_grace_seconds(SETTINGS)
GATE_ORPHAN_CHECK_INTERVAL_SECONDS = SETTINGS.GATE_ORPHAN_CHECK_INTERVAL


# =========================================================================
# Fixtures and real-component builders
# =========================================================================


class RecordingLogger:
    """The node logger: records every entry it is given."""

    def __init__(self) -> None:
        self.entries: list = []

    async def log(self, entry) -> None:
        self.entries.append(entry)


@pytest.fixture
def recording_logger():
    return RecordingLogger()


@pytest.fixture
def mock_core_allocator():
    return MagicMock()


@pytest.fixture
def worker_state(mock_core_allocator):
    state = WorkerState(
        core_allocator=mock_core_allocator,
        throughput_interval_seconds=Env().WORKER_THROUGHPUT_INTERVAL_SECONDS,
        completion_times_max_samples=Env().WORKER_COMPLETION_TIMES_MAX_SAMPLES,
    )
    state.initialize_locks()
    return state


@pytest.fixture
def cancellation_handler(worker_state, recording_logger):
    """Built from the worker's Env-derived config, as WorkerServer builds it."""
    worker_config = WorkerConfig.from_env(
        Env(),
        host="127.0.0.1",
        tcp_port=9100,
        udp_port=9101,
        total_cores=1,
    )
    return WorkerCancellationHandler(
        state=worker_state,
        logger=recording_logger,
        poll_interval=worker_config.cancellation_poll_interval_seconds,
        query_timeout=worker_config.tcp_timeout_short_seconds,
        cancel_timeout=worker_config.workflow_cancel_timeout_seconds,
    )


@pytest.fixture
def leadership_tracker():
    """The tracker of the surviving node that scans for and takes over orphans."""
    return JobLeadershipTracker[int](
        node_id="manager-1",
        node_addr=("127.0.0.1", 9000),
    )


def record_peer_leadership(
    tracker: JobLeadershipTracker[int],
    job_id: str,
    leader_id: str,
    leader_addr: tuple[str, int],
    fencing_token: int = 1,
) -> None:
    """Teach the tracker that a peer leads a job, the way a peer's
    leadership announcement does (``process_leadership_claim``)."""
    accepted = tracker.process_leadership_claim(
        job_id=job_id,
        claimer_id=leader_id,
        claimer_addr=leader_addr,
        fencing_token=fencing_token,
    )
    assert accepted is True


def jobs_led_by_dead_nodes(
    tracker: JobLeadershipTracker[int],
    dead_addrs: set[tuple[str, int]],
) -> list[str]:
    """The orphan scan: every job whose recorded leader address is dead."""
    return [
        job_id
        for job_id, _leader_id, leader_addr, _fencing_token in tracker.get_all_leaderships()
        if leader_addr in dead_addrs
    ]


class RecordingTaskRunner:
    """Records submitted background calls so a test can run them itself."""

    def __init__(self) -> None:
        self.submitted_calls: list[tuple] = []

    def run(self, call, *args, **kwargs) -> None:
        self.submitted_calls.append((call, args, kwargs))


class RecordingSender:
    """Records every (addr, method, payload) the coordinator sends."""

    def __init__(self) -> None:
        self.sent_messages: list[tuple[tuple[str, int], str, bytes]] = []

    async def __call__(self, addr: tuple[str, int], method: str, payload: bytes, *args, **kwargs) -> bytes:
        self.sent_messages.append((addr, method, payload))
        return b"OK"


def unreachable_collaborator(name: str):
    """A collaborator the paths under test must never touch."""

    def fail(*args, **kwargs):
        raise AssertionError(f"{name} must not be reached on this path")

    return fail


class WorkerEdge:
    """The workers' end of the manager's ``cancel_workflow`` RPC: records
    each request and answers as each worker would -- an ack, or, from a
    worker that cannot answer, the transport error ``_send_to_worker``
    returns."""

    def __init__(self, silent_worker_addrs: frozenset[tuple[str, int]] = frozenset()) -> None:
        self.silent_worker_addrs = silent_worker_addrs
        self.cancel_requests: list[tuple[tuple[str, int], str]] = []

    async def __call__(self, worker_addr: tuple[str, int], method: str, payload: bytes, *, timeout: float):
        assert method == "cancel_workflow"
        request = WorkflowCancelRequest.load(payload)
        self.cancel_requests.append((worker_addr, request.workflow_id))
        if worker_addr in self.silent_worker_addrs:
            return ConnectionRefusedError(f"worker {worker_addr} unreachable")
        return WorkflowCancelResponse(
            job_id=request.job_id,
            workflow_id=request.workflow_id,
            success=True,
            was_running=True,
        ).dump()


class ManagerNode:
    """One manager's cancellation coordinator, wired as ManagerServer wires
    it. The network edge (workers, clients) is recorded or answered by the
    test; the rate-limit check runs the real limiter and a cluster-leader
    takeover claims the job through the real lease coordinator (the local
    effect of the quorum-committed takeover); every other collaborator
    fails the test if reached."""

    def __init__(
        self,
        datacenter: str,
        tcp_port: int,
        logger: RecordingLogger,
        *,
        is_cluster_leader: bool = False,
        worker_edge: WorkerEdge | None = None,
    ) -> None:
        self.env = Env()
        self.datacenter = datacenter
        self.tcp_addr = (MANAGER_HOST, tcp_port)
        self.node_id = NodeId.generate(datacenter, host=MANAGER_HOST, port=tcp_port)
        self.state = ManagerState(slo_config=SLOConfig.from_env(self.env))
        self.state.initialize_locks()
        self.config = create_manager_config_from_env(
            host=MANAGER_HOST,
            tcp_port=tcp_port,
            udp_port=tcp_port + 1,
            env=self.env,
            datacenter_id=datacenter,
        )
        self.task_runner = RecordingTaskRunner()
        self.send_to_client = RecordingSender()
        self.rate_limiter = ServerRateLimiter()
        self.job_manager = JobManager(
            datacenter=datacenter,
            manager_id=self.node_id.full,
            clock=RealClock(),
            max_budgeted_retries=self.env.RETRY_BUDGET_PER_WORKFLOW_MAX,
        )
        self.leases = ManagerLeaseCoordinator(
            state=self.state,
            config=self.config,
            logger=logger,
            node_id=self.node_id.full,
            task_runner=self.task_runner,
        )
        self.terminal_outcome_job_ids: list[str] = []
        self.completion_checked_job_ids: list[str] = []
        self.coordinator = ManagerCancellationCoordinator(
            state=self.state,
            config=self.config,
            env=self.env,
            logger=logger,
            node_id=self.node_id,
            node_host=MANAGER_HOST,
            node_port=tcp_port,
            task_runner=self.task_runner,
            clock=RealClock(),
            job_manager=self.job_manager,
            leases=self.leases,
            rate_limiter=self.rate_limiter,
            get_job_ledger=lambda: None,
            get_workflow_dispatcher=lambda: None,
            is_cluster_leader=lambda: is_cluster_leader,
            send_tcp=unreachable_collaborator("send_tcp"),
            send_to_worker=worker_edge if worker_edge is not None else unreachable_collaborator("send_to_worker"),
            send_to_client=self.send_to_client,
            check_rate_limit_for_operation=self.check_rate_limit_for_operation,
            take_over_job_leadership_as_cluster_leader=self.take_over_job_leadership,
            resolve_dc_leader_addr=unreachable_collaborator("resolve_dc_leader_addr"),
            manager_tcp_addr_is_live=unreachable_collaborator("manager_tcp_addr_is_live"),
            emit_outcomes_for_terminal_job=self.record_terminal_outcomes,
            discard_persisted_submission=unreachable_collaborator("discard_persisted_submission"),
            log_ledger_shortfall=unreachable_collaborator("log_ledger_shortfall"),
            complete_job_if_done=self.record_completion_check,
        )

    async def check_rate_limit_for_operation(self, client_id: str, operation: str) -> tuple[bool, float]:
        result = await self.rate_limiter.check_rate_limit(client_id, operation)
        return result.allowed, result.retry_after_seconds

    async def take_over_job_leadership(self, job_id: str, old_leader_id: str | None) -> bool:
        return await self.leases.claim_job_leadership(job_id, self.tcp_addr, force_takeover=True)

    def record_terminal_outcomes(self, job_id: str, outcome_kind) -> None:
        self.terminal_outcome_job_ids.append(job_id)

    async def record_completion_check(self, job_id: str) -> None:
        self.completion_checked_job_ids.append(job_id)

    async def register_job(self, job_id: str, workflow_id: str) -> JobInfo:
        """A job with one workflow, PENDING, registered as submission does."""
        job_token = self.job_manager.create_job_token(job_id)
        job = JobInfo(token=job_token, submission=None, workflows_total=1)
        self.job_manager._jobs[str(job_token)] = job
        await self.job_manager.register_workflow(
            job_id, workflow_id, "Workflow", dependency_workflow_ids=frozenset(), is_test=True
        )
        return job

    async def dispatch_to_workers(self, job_id: str, workflow_id: str, worker_ports: list[int]) -> list[str]:
        """Claim the workflow for dispatch and run one sub on each worker,
        as the dispatcher does. Returns the sub-workflow tokens."""
        assert await self.job_manager.claim_workflow_for_dispatch(job_id, workflow_id)
        sub_workflow_tokens: list[str] = []
        for worker_port in worker_ports:
            worker_id = f"worker-{worker_port}"
            self.state.add_worker(
                worker_id,
                WorkerRegistration(
                    node=NodeInfo(
                        node_id=worker_id,
                        role=NodeRole.WORKER.value,
                        host=MANAGER_HOST,
                        port=worker_port,
                        datacenter=self.datacenter,
                    ),
                    total_cores=1,
                    available_cores=0,
                    memory_mb=1024,
                    available_memory_mb=1024,
                ),
            )
            sub_workflow = await self.job_manager.register_sub_workflow(
                job_id, workflow_id, worker_id, cores_allocated=1
            )
            sub_workflow_tokens.append(str(sub_workflow.token))
        return sub_workflow_tokens


@pytest.fixture
def manager_node(recording_logger):
    return ManagerNode("dc-1", MANAGER_TCP_PORT, recording_logger)


async def seed_job_cancellation(
    node: ManagerNode,
    job_id: str,
    sub_workflow_tokens: list[str],
    callback_addr: tuple[str, int] = GATE_CALLBACK_ADDR,
    force_takeover: bool = False,
) -> None:
    """Make this manager the job's leader and seed its pending-cancellation
    tracker and origin callback the way cancel_job does."""
    claimed = await node.leases.claim_job_leadership(job_id, node.tcp_addr, force_takeover=force_takeover)
    assert claimed is True
    node.state.set_job_callback(job_id, callback_addr)
    for sub_workflow_token in sub_workflow_tokens:
        node.state.add_cancellation_pending_workflow(job_id, sub_workflow_token)


async def push_worker_completion(
    node: ManagerNode,
    job_id: str,
    sub_workflow_token: str,
    success: bool = True,
    errors: list[str] | None = None,
) -> bytes:
    """Deliver a worker's WorkflowCancellationComplete over the manager's
    TCP handler, serialized as on the wire."""
    notification = WorkflowCancellationComplete(
        job_id=job_id,
        workflow_id=sub_workflow_token,
        success=success,
        errors=errors or [],
        cancelled_at=time.monotonic(),
        node_id="worker-1",
    )
    return await node.coordinator.handle_workflow_cancellation_complete(
        ("127.0.0.1", 9100), notification.dump(), 0
    )


async def run_submitted_calls(task_runner: RecordingTaskRunner) -> None:
    """Run the background calls the component submitted, including any
    those calls submit in turn."""
    while task_runner.submitted_calls:
        call, args, kwargs = task_runner.submitted_calls.pop(0)
        await call(*args, **kwargs)


class GateNode:
    """One gate's orphan-job coordinator over its real runtime state, job
    leadership tracker and job manager, wired as GateServer wires it. Its
    TCP sends go to ``peer_gates`` (their real job_leadership_announcement
    endpoint) or are recorded; a takeover's quorum commit is its local
    effect on this gate's tracker."""

    def __init__(
        self,
        name: str,
        tcp_port: int,
        logger: RecordingLogger,
        clock,
        *,
        is_cluster_leader: bool,
    ) -> None:
        self.tcp_addr = ("10.0.0.1", tcp_port)
        self.node_id = NodeId.generate("dc-gates", host=self.tcp_addr[0], port=tcp_port)
        self.name = name
        self.logger = logger
        self.state = GateRuntimeState(forward_throughput_interval_start=clock.monotonic())
        self.tracker = JobLeadershipTracker[int](node_id=self.node_id.full, node_addr=self.tcp_addr)
        self.job_manager = GateJobManager()
        self.task_runner = RecordingTaskRunner()
        self.peer_gates: dict[tuple[str, int], "GateNode"] = {}
        self.sent_messages: list[tuple[tuple[str, int], str, bytes]] = []
        self.failed_jobs: list[tuple[str, str, float]] = []
        self.clock = clock
        self.coordinator = GateOrphanJobCoordinator(
            state=self.state,
            logger=logger,
            task_runner=self.task_runner,
            job_hash_ring=ConsistentHashRing(),
            job_leadership_tracker=self.tracker,
            job_manager=self.job_manager,
            get_node_id=lambda: self.node_id,
            get_node_addr=lambda: self.tcp_addr,
            send_tcp=self.send_tcp,
            get_active_peers=lambda: set(self.peer_gates),
            clock=clock,
            forward_status_push_to_peers=unreachable_collaborator("forward_status_push_to_peers"),
            state_repair_callback=None,
            commit_takeover_callback=self.commit_takeover,
            is_cluster_leader=lambda: is_cluster_leader,
            orphan_check_interval_seconds=GATE_ORPHAN_CHECK_INTERVAL_SECONDS,
            orphan_grace_period_seconds=GATE_ORPHAN_GRACE_SECONDS,
            orphan_extension_min_grant_seconds=SETTINGS.EXTENSION_MIN_GRANT,
            orphan_extension_max_extensions=SETTINGS.EXTENSION_MAX_EXTENSIONS,
            finalize_failed_job=self.record_failed_job,
        )
        self.server = object.__new__(GateServer)
        self.server._job_leadership_tracker = self.tracker
        self.server._orphan_job_coordinator = self.coordinator
        self.server._udp_logger = logger
        self.server._host, self.server._tcp_port = self.tcp_addr
        self.server._node_id = self.node_id

    async def send_tcp(self, addr: tuple[str, int], method: str, payload: bytes, timeout: float):
        self.sent_messages.append((addr, method, payload))
        if (peer_gate := self.peer_gates.get(addr)) is not None:
            assert method == "job_leadership_announcement"
            return await GateServer.job_leadership_announcement(peer_gate.server, self.tcp_addr, payload, 0), 0
        return b"OK", 0

    async def commit_takeover(self, job_id: str) -> int | None:
        return self.tracker.takeover_leadership(job_id, metadata=len(self.job_manager.get_target_dcs(job_id)))

    async def record_failed_job(self, job_id: str, datacenters: tuple[str, ...], reason: str) -> None:
        self.failed_jobs.append((job_id, reason, self.clock.monotonic()))

    def learn_job(self, job_id: str, leader: "GateNode | tuple[str, str, tuple[str, int]]") -> None:
        """This gate knows the job running, led by ``leader`` -- as a
        peer's leadership announcement teaches it."""
        if isinstance(leader, GateNode):
            leader_id, leader_addr = leader.node_id.full, leader.tcp_addr
        else:
            _name, leader_id, leader_addr = leader
        self.job_manager.set_job(job_id, GlobalJobStatus(job_id=job_id, status=JobStatus.RUNNING.value))
        self.job_manager.set_target_dcs(job_id, {"dc-1"})
        self.job_manager.set_callback(job_id, CLIENT_CALLBACK_ADDR)
        assert self.tracker.process_leadership_claim(
            job_id=job_id, claimer_id=leader_id, claimer_addr=leader_addr, fencing_token=1, metadata=1
        )


def dead_gate(name: str, tcp_port: int) -> tuple[str, str, tuple[str, int]]:
    """A gate peer that leads jobs and then dies: (name, node id, TCP address)."""
    return name, f"gate-{name}-node-id", ("10.0.0.1", tcp_port)


def simulate(
    scenario: Callable[[VirtualClock], Coroutine[Any, Any, ScenarioResult]],
    until: float,
) -> ScenarioResult:
    """Run ``scenario`` on a fresh SimulationLoop through virtual
    ``until``, every clock on its virtual time and logging off."""
    snapshot = snapshot_defaults()
    loop = SimulationLoop()
    clock = VirtualClock(loop)
    swap_defaults(clock=clock)

    def run_through_deadline() -> ScenarioResult:
        LoggingConfig().disable()
        scenario_task = loop.create_task(scenario(clock))
        loop.run_window(until)
        assert scenario_task.done(), f"the scenario was still running at virtual {until}"
        return scenario_task.result()

    try:
        return contextvars.copy_context().run(run_through_deadline)
    finally:
        loop.close()
        restore_defaults(snapshot)


def orphan_job_of_silent_dead_gate(observe_seconds: float) -> tuple[float, GateNode]:
    """A non-leader gate's job whose leader gate died unconfirmed (SWIM
    has not yet agreed; the gate tier never takes it over), observed on
    virtual time for ``observe_seconds`` after the death. Returns the
    virtual time of the death and the gate."""
    leader = dead_gate("a", 9010)

    async def scenario(clock: VirtualClock) -> tuple[float, GateNode]:
        gate = GateNode("c", 9030, RecordingLogger(), clock, is_cluster_leader=False)
        gate.learn_job("job-orphan", leader)
        await gate.coordinator.start()
        try:
            await clock.sleep(GATE_ORPHAN_CHECK_INTERVAL_SECONDS)
            died_at = clock.monotonic()
            gate.coordinator.mark_jobs_orphaned_by_gate(leader[2])
            await clock.sleep(observe_seconds)
            await run_submitted_calls(gate.task_runner)
            return died_at, gate
        finally:
            await gate.coordinator.stop()

    return simulate(scenario, until=GATE_ORPHAN_CHECK_INTERVAL_SECONDS + observe_seconds + 1.0)


# =========================================================================
# 4.1: SWIM Leader + Job Leader Fails
# =========================================================================


class TestSwimLeaderPlusJobLeaderFails:
    """
    Test scenario where the SWIM cluster leader is ALSO the job leader.

    When this node fails:
    1. SWIM detects failure, triggers _on_node_dead
    2. New SWIM leader elected, _on_manager_become_leader fires
    3. New leader scans for orphaned jobs
    4. Takes over orphaned jobs via Raft proposal
    5. Workers receive transfer notification
    """

    @pytest.mark.asyncio
    async def test_dead_manager_triggers_orphan_scan(self, leadership_tracker):
        """New SWIM leader's scan sees the job and its dead leader's address."""
        dead_manager_addr = ("127.0.0.2", 9000)

        record_peer_leadership(leadership_tracker, "job-1", "manager-2", dead_manager_addr)

        assert leadership_tracker.get_all_leaderships() == [
            ("job-1", "manager-2", dead_manager_addr, 1)
        ]
        assert jobs_led_by_dead_nodes(leadership_tracker, {dead_manager_addr}) == ["job-1"]

    @pytest.mark.asyncio
    async def test_orphan_detection_with_dead_set(self, leadership_tracker):
        """Orphan scan checks job leaders against dead manager set."""
        dead_managers: set[tuple[str, int]] = set()
        dead_manager_addr = ("127.0.0.2", 9000)
        live_manager_addr = ("127.0.0.3", 9000)

        record_peer_leadership(leadership_tracker, "job-orphan", "manager-2", dead_manager_addr)
        record_peer_leadership(leadership_tracker, "job-alive", "manager-3", live_manager_addr)
        dead_managers.add(dead_manager_addr)

        assert jobs_led_by_dead_nodes(leadership_tracker, dead_managers) == ["job-orphan"]
        assert leadership_tracker.get_jobs_led_by_addr(dead_manager_addr) == ["job-orphan"]

    @pytest.mark.asyncio
    async def test_takeover_increments_fencing_token(self, leadership_tracker):
        """Takeover must produce a higher fencing token than the previous leader."""
        old_addr = ("127.0.0.2", 9000)

        record_peer_leadership(leadership_tracker, "job-1", "manager-2", old_addr, fencing_token=4)
        old_token = leadership_tracker.get_fencing_token("job-1")

        new_token = leadership_tracker.takeover_leadership("job-1")

        assert old_token == 4
        assert new_token > old_token
        assert leadership_tracker.get_fencing_token("job-1") == new_token

    @pytest.mark.asyncio
    async def test_multiple_dead_managers_all_jobs_found(self, leadership_tracker):
        """When multiple managers die, ALL their jobs are detected as orphaned."""
        dead_addrs = {("10.0.0.1", 9000), ("10.0.0.2", 9000)}

        record_peer_leadership(leadership_tracker, "job-a", "manager-a", ("10.0.0.1", 9000))
        record_peer_leadership(leadership_tracker, "job-b", "manager-b", ("10.0.0.2", 9000))
        record_peer_leadership(leadership_tracker, "job-c", "manager-c", ("10.0.0.3", 9000))

        orphaned = jobs_led_by_dead_nodes(leadership_tracker, dead_addrs)

        assert sorted(orphaned) == ["job-a", "job-b"]


# =========================================================================
# 4.2: Job Leader Fails (Not SWIM Leader)
# =========================================================================


class TestJobLeaderFailsNotSwimLeader:
    """
    Test scenario where the job leader is NOT the SWIM cluster leader.

    The SWIM leader takes over the orphaned job via _handle_job_leader_failure.
    """

    @pytest.mark.asyncio
    async def test_non_swim_leader_job_detected_orphaned(self, leadership_tracker):
        """Job leader that's not SWIM leader -- detected via dead set."""
        job_leader_addr = ("10.0.0.5", 9000)
        dead_managers: set[tuple[str, int]] = {job_leader_addr}

        record_peer_leadership(leadership_tracker, "job-x", "manager-5", job_leader_addr)

        is_orphaned = leadership_tracker.get_leader_addr("job-x") in dead_managers
        assert is_orphaned is True
        assert leadership_tracker.is_leader("job-x") is False

    @pytest.mark.asyncio
    async def test_takeover_preserves_job_data(self, leadership_tracker):
        """Takeover keeps the job and its metadata but moves leadership here."""
        old_addr = ("10.0.0.5", 9000)

        record_peer_leadership(leadership_tracker, "job-keep", "manager-5", old_addr)
        leadership_tracker.set_metadata("job-keep", 3)
        leadership_tracker.takeover_leadership("job-keep")

        assert "job-keep" in leadership_tracker
        assert leadership_tracker.get_leader("job-keep") == leadership_tracker.node_id
        assert leadership_tracker.get_leader_addr("job-keep") == leadership_tracker.node_addr
        assert leadership_tracker.get_metadata("job-keep") == 3
        assert leadership_tracker.get_jobs_led_by_addr(old_addr) == []

    @pytest.mark.asyncio
    async def test_gate_notified_of_transfer(self, recording_logger):
        """The manager that took the job over tells its origin gate, and the
        gate routes the job's datacenter to it -- refusing a transfer from
        an older leadership epoch."""
        job_id = "job-transferred"
        old_manager_addr = (MANAGER_HOST, 9000)
        new_manager_addr = (MANAGER_HOST, 9002)

        gate_node_id = NodeId.generate("dc-gates", host=GATE_CALLBACK_ADDR[0], port=GATE_CALLBACK_ADDR[1])
        gate = object.__new__(GateServer)
        gate._modular_state = GateRuntimeState(forward_throughput_interval_start=0.0)
        gate._modular_state.set_job_dc_manager(job_id, "dc-1", old_manager_addr)
        gate._job_leadership_tracker = JobLeadershipTracker[int](
            node_id=gate_node_id.full, node_addr=GATE_CALLBACK_ADDR
        )
        gate._udp_logger = recording_logger
        gate._host, gate._tcp_port = GATE_CALLBACK_ADDR
        gate._node_id = gate_node_id

        def build_manager(tcp_addr: tuple[str, int]) -> ManagerServer:
            manager = object.__new__(ManagerServer)
            manager._manager_state = ManagerState(slo_config=SLOConfig.from_env(SETTINGS))
            manager._manager_state.set_job_origin_gate(job_id, GATE_CALLBACK_ADDR)
            manager._node_id = NodeId.generate("dc-1", host=tcp_addr[0], port=tcp_addr[1])
            manager._host, manager._tcp_port = tcp_addr
            manager._config = create_manager_config_from_env(
                host=tcp_addr[0], tcp_port=tcp_addr[1], udp_port=tcp_addr[1] + 1, env=SETTINGS, datacenter_id="dc-1"
            )
            manager._udp_logger = recording_logger

            async def deliver_to_gate(addr, method, payload, timeout):
                assert (addr, method) == (GATE_CALLBACK_ADDR, "job_leader_manager_transfer")
                return await GateServer.job_leader_manager_transfer(gate, tcp_addr, payload, 0), 0

            manager.send_tcp = deliver_to_gate
            return manager

        new_leader = build_manager(new_manager_addr)
        await ManagerServer._notify_origin_gate_job_leader_transfer(new_leader, job_id, "manager-old", 2)

        assert gate._job_leadership_tracker.get_dc_manager(job_id, "dc-1") == new_manager_addr
        assert gate._job_leadership_tracker.get_dc_manager_fencing_token(job_id, "dc-1") == 2
        assert gate._modular_state.get_job_dc_managers(job_id)["dc-1"] == new_manager_addr
        assert [entry for entry in recording_logger.entries if "rejected" in entry.message] == []

        stale_leader = build_manager(old_manager_addr)
        await ManagerServer._notify_origin_gate_job_leader_transfer(stale_leader, job_id, None, 1)

        assert gate._modular_state.get_job_dc_managers(job_id)["dc-1"] == new_manager_addr
        assert any(
            "rejected manager leader transfer" in entry.message for entry in recording_logger.entries
        )


# =========================================================================
# 4.3: Worker Orphan Grace Period
# =========================================================================


class TestWorkerOrphanGracePeriod:
    """
    Test that workers wait a grace period before cancelling orphaned workflows.

    When the job leader dies:
    1. Worker marks workflows as orphaned (with timestamp)
    2. Grace period timer starts
    3. If no transfer arrives, workflows are cancelled after grace expires
    """

    @pytest.mark.asyncio
    async def test_workflow_marked_orphaned_on_manager_death(self, worker_state):
        """Workflows are marked orphaned when their manager dies."""
        workflow_id = "wf-orphan-1"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )

        worker_state.mark_workflow_orphaned(workflow_id)

        assert worker_state.is_workflow_orphaned(workflow_id)
        assert workflow_id in worker_state._orphaned_workflows

    @pytest.mark.asyncio
    async def test_grace_period_not_expired(self, worker_state):
        """Within grace period, no workflows should be returned for cancellation."""
        workflow_id = "wf-grace"
        worker_state._orphaned_workflows[workflow_id] = time.monotonic()

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id not in expired

    @pytest.mark.asyncio
    async def test_grace_period_expired(self, worker_state):
        """After grace period, workflows are returned for cancellation."""
        workflow_id = "wf-expired"
        worker_state._orphaned_workflows[workflow_id] = time.monotonic() - 10.0

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id in expired

    @pytest.mark.asyncio
    async def test_cancel_event_set_on_orphan_expiry(
        self, worker_state, cancellation_handler
    ):
        """Cancellation event fires for orphaned workflows past grace period."""
        workflow_id = "wf-cancel-me"
        event = cancellation_handler.create_cancel_event(workflow_id)
        worker_state._orphaned_workflows[workflow_id] = time.monotonic() - 10.0

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        for expired_workflow_id in expired:
            cancellation_handler.signal_cancellation(expired_workflow_id)

        assert event.is_set()


# =========================================================================
# 4.4: Worker Receives Transfer Before Grace Expires
# =========================================================================


class TestWorkerReceivesTransferBeforeGrace:
    """
    Test that a timely leadership transfer prevents workflow cancellation.

    1. Job leader fails, workflows marked orphaned
    2. New leader sends transfer within grace period
    3. Worker clears orphan status, updates routing
    4. Workflow continues executing
    """

    @pytest.mark.asyncio
    async def test_transfer_clears_orphan_status(self, worker_state):
        """Leadership transfer clears orphan status for affected workflows."""
        workflow_id = "wf-saved"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )
        worker_state.mark_workflow_orphaned(workflow_id)
        assert worker_state.is_workflow_orphaned(workflow_id)

        worker_state.clear_workflow_orphaned(workflow_id)

        assert not worker_state.is_workflow_orphaned(workflow_id)

    @pytest.mark.asyncio
    async def test_transfer_updates_job_leader_addr(self, worker_state):
        """Transfer updates the job leader address for the workflow."""
        workflow_id = "wf-reroute"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        old_addr = ("10.0.0.1", 9000)
        new_addr = ("10.0.0.2", 9000)
        worker_state.add_active_workflow(workflow_id, progress, old_addr)

        worker_state.set_workflow_job_leader(workflow_id, new_addr)

        assert worker_state.get_workflow_job_leader(workflow_id) == new_addr

    @pytest.mark.asyncio
    async def test_cancel_event_not_set_after_transfer(
        self, worker_state, cancellation_handler
    ):
        """Workflow cancel event NOT set if transfer arrives before grace expires."""
        workflow_id = "wf-still-running"
        event = cancellation_handler.create_cancel_event(workflow_id)
        worker_state._orphaned_workflows[workflow_id] = time.monotonic()

        worker_state.clear_workflow_orphaned(workflow_id)

        expired = worker_state.get_orphaned_workflows_expired(
            grace_period_seconds=5.0
        )
        assert workflow_id not in expired
        assert not event.is_set()

    @pytest.mark.asyncio
    async def test_fence_token_accepted_on_valid_transfer(self, worker_state):
        """Worker accepts fence token from new leader if it's higher."""
        workflow_id = "wf-fenced"
        old_token = 5
        new_token = 6

        await worker_state.update_workflow_fence_token(workflow_id, old_token)
        accepted = await worker_state.update_workflow_fence_token(
            workflow_id, new_token
        )

        assert accepted is True
        current = await worker_state.get_workflow_fence_token(workflow_id)
        assert current == new_token

    @pytest.mark.asyncio
    async def test_stale_fence_token_rejected(self, worker_state):
        """Worker rejects stale fence token (prevents stale leaders)."""
        workflow_id = "wf-stale"
        await worker_state.update_workflow_fence_token(workflow_id, 10)

        rejected = await worker_state.update_workflow_fence_token(workflow_id, 5)

        assert rejected is False
        current = await worker_state.get_workflow_fence_token(workflow_id)
        assert current == 10


# =========================================================================
# 4.5: Cancellation Push Notification Chain (End-to-End)
# =========================================================================


class TestCancellationPushChainEndToEnd:
    """
    Test the full push notification chain:
    Worker → Manager → Gate → Client.

    Worker sends WorkflowCancellationComplete to manager.
    Manager aggregates, sends JobCancellationComplete to gate.
    Gate forwards to client. Client completion event fires.
    """

    @pytest.mark.asyncio
    async def test_manager_tracks_workflow_cancellation_completion(self, manager_node):
        """Manager coordinator tracks cancellation completion from workers."""
        job_id = "job-cancel-1"
        first_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-1", "worker-1"))
        second_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-2", "worker-1"))
        await seed_job_cancellation(manager_node, job_id, [first_sub, second_sub])

        response = await push_worker_completion(manager_node, job_id, first_sub)

        assert response == b"OK"
        assert manager_node.state.get_cancellation_pending_workflows(job_id) == {second_sub}
        assert manager_node.task_runner.submitted_calls == []

    @pytest.mark.asyncio
    async def test_manager_fires_event_when_all_workflows_complete(self, manager_node):
        """Last workflow's report pushes JobCancellationComplete to the
        origin gate exactly once and releases the job's tracker."""
        job_id = "job-all-done"
        last_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-last", "worker-1"))
        await seed_job_cancellation(manager_node, job_id, [last_sub])

        await push_worker_completion(manager_node, job_id, last_sub)
        await push_worker_completion(manager_node, job_id, last_sub)
        await run_submitted_calls(manager_node.task_runner)

        assert manager_node.state.get_cancellation_pending_workflows(job_id) == set()
        assert job_id not in manager_node.state._cancellation_pending_workflows
        assert len(manager_node.send_to_client.sent_messages) == 1
        callback_addr, method, payload = manager_node.send_to_client.sent_messages[0]
        assert callback_addr == GATE_CALLBACK_ADDR
        assert method == "job_cancellation_complete"
        pushed = JobCancellationComplete.load(payload)
        assert pushed.job_id == job_id
        assert pushed.success is True
        assert pushed.errors == []

    @pytest.mark.asyncio
    async def test_manager_aggregates_errors_from_workers(self, manager_node):
        """Manager aggregates errors from multiple worker cancellations."""
        job_id = "job-errors"
        first_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-1", "worker-1"))
        second_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-2", "worker-2"))
        await seed_job_cancellation(manager_node, job_id, [first_sub, second_sub])

        await push_worker_completion(
            manager_node, job_id, first_sub, success=False, errors=["Timeout waiting for workflow"]
        )
        await push_worker_completion(manager_node, job_id, second_sub, success=False, errors=["Worker stopping"])
        await run_submitted_calls(manager_node.task_runner)

        aggregated_errors = manager_node.state.get_cancellation_errors(job_id)
        assert len(aggregated_errors) == 2
        assert "Timeout waiting for workflow" in aggregated_errors[0]
        assert "Worker stopping" in aggregated_errors[1]
        assert len(manager_node.send_to_client.sent_messages) == 1
        pushed = JobCancellationComplete.load(manager_node.send_to_client.sent_messages[0][2])
        assert pushed.success is False
        assert pushed.errors == aggregated_errors

    @pytest.mark.asyncio
    async def test_client_completion_event_fires_on_notification(self, manager_node, recording_logger):
        """The manager's JobCancellationComplete push, delivered to the
        client's real endpoint, fires the event await_job_cancellation waits
        on with the aggregated outcome; a push for a job the client is not
        awaiting records nothing."""
        job_id = "job-client-done"
        client_state = ClientState()
        client_state.initialize_cancellation_tracking(job_id)
        client_handler = CancellationCompleteHandler(state=client_state, logger=recording_logger)
        sub_workflow_token = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-1", "worker-1"))
        await seed_job_cancellation(manager_node, job_id, [sub_workflow_token], callback_addr=CLIENT_CALLBACK_ADDR)

        await push_worker_completion(
            manager_node, job_id, sub_workflow_token, success=False, errors=["worker lost the workflow"]
        )
        await run_submitted_calls(manager_node.task_runner)
        [(callback_addr, method, payload)] = manager_node.send_to_client.sent_messages
        assert (callback_addr, method) == (CLIENT_CALLBACK_ADDR, "job_cancellation_complete")

        assert await client_handler.handle(manager_node.tcp_addr, payload, 0) == b"OK"

        assert client_state._cancellation_events[job_id].is_set()
        assert client_state._cancellation_success[job_id] is False
        assert client_state._cancellation_errors[job_id] == manager_node.state.get_cancellation_errors(job_id)

        unawaited = JobCancellationComplete(job_id="job-not-awaited", success=True, errors=[])
        assert await client_handler.handle(manager_node.tcp_addr, unawaited.dump(), 0) == b"OK"
        assert "job-not-awaited" not in client_state._cancellation_events
        assert "job-not-awaited" not in client_state._cancellation_success


# =========================================================================
# 4.6: Single Workflow Cancellation Through Gate
# =========================================================================


class TestSingleWorkflowCancellationThroughGate:
    """
    Test fine-grained workflow cancellation:
    Client → Gate → Manager(s) → Worker.

    Gate fans out to all DCs, aggregates results.
    """

    @pytest.mark.asyncio
    async def test_single_cancel_request_has_required_fields(self):
        """SingleWorkflowCancelRequest carries all needed data across the wire."""
        request = SingleWorkflowCancelRequest(
            job_id="job-1",
            workflow_id="wf-target",
            request_id="req-123",
            requester_id="client-1",
            timestamp=time.time(),
            origin_gate_addr=GATE_CALLBACK_ADDR,
        )

        received = SingleWorkflowCancelRequest.load(request.dump())

        assert received.job_id == "job-1"
        assert received.workflow_id == "wf-target"
        assert received.request_id == "req-123"
        assert received.requester_id == "client-1"
        assert received.timestamp == request.timestamp
        assert received.cancel_dependents is True
        assert received.origin_gate_addr == GATE_CALLBACK_ADDR

    @pytest.mark.asyncio
    async def test_gate_fans_out_to_multiple_dcs(self, recording_logger):
        """The gate sends the cancel to every datacenter the job runs in --
        through a live manager of each -- and never to one it does not; a
        workflow its workers are still stopping in one datacenter is not
        reported cancelled."""
        job_id = "job-fan-out"
        east = ManagerNode("dc-east", 9000, recording_logger)
        await east.register_job(job_id, "wf-target")
        assert await east.leases.claim_job_leadership(job_id, east.tcp_addr)

        silent_worker_addr = (MANAGER_HOST, 9201)
        west = ManagerNode(
            "dc-west",
            9100,
            recording_logger,
            worker_edge=WorkerEdge(silent_worker_addrs=frozenset({silent_worker_addr})),
        )
        await west.register_job(job_id, "wf-target")
        assert await west.leases.claim_job_leadership(job_id, west.tcp_addr)
        await west.dispatch_to_workers(job_id, "wf-target", [silent_worker_addr[1]])

        dead_west_manager_addr = (MANAGER_HOST, 9150)
        north_manager_addr = (MANAGER_HOST, 9300)
        managers_by_addr = {east.tcp_addr: east, west.tcp_addr: west}
        contacted: list[tuple[str, int]] = []

        async def gate_send_tcp(addr, method, payload, timeout):
            contacted.append(addr)
            assert method == "receive_cancel_single_workflow"
            if addr == dead_west_manager_addr:
                return ConnectionRefusedError(f"{addr} unreachable"), 0
            return await managers_by_addr[addr].coordinator.handle_cancel_single_workflow(
                GATE_CALLBACK_ADDR, payload, 0
            ), 0

        gate_rate_limiter = ServerRateLimiter()

        async def gate_check_rate_limit(client_id: str, operation: str) -> tuple[bool, float]:
            result = await gate_rate_limiter.check_rate_limit(client_id, operation)
            return result.allowed, result.retry_after_seconds

        gate_job_manager = GateJobManager()
        gate_job_manager.set_job(job_id, GlobalJobStatus(job_id=job_id, status=JobStatus.RUNNING.value))
        gate_job_manager.set_target_dcs(job_id, {"dc-east", "dc-west"})
        gate_node_id = NodeId.generate("dc-gates", host=GATE_CALLBACK_ADDR[0], port=GATE_CALLBACK_ADDR[1])
        gate_handler = GateCancellationHandler(
            state=GateRuntimeState(forward_throughput_interval_start=0.0),
            logger=recording_logger,
            task_runner=RecordingTaskRunner(),
            job_manager=gate_job_manager,
            datacenter_managers={
                "dc-east": [east.tcp_addr],
                "dc-west": [dead_west_manager_addr, west.tcp_addr],
                "dc-north": [north_manager_addr],
            },
            get_node_id=lambda: gate_node_id,
            get_host=lambda: GATE_CALLBACK_ADDR[0],
            get_tcp_port=lambda: GATE_CALLBACK_ADDR[1],
            check_rate_limit=gate_check_rate_limit,
            send_tcp=gate_send_tcp,
            record_cancellation=unreachable_collaborator("record_cancellation"),
            client_push_timeout_seconds=SETTINGS.GATE_TCP_TIMEOUT_SHORT,
            manager_request_timeout_seconds=SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        )
        request = SingleWorkflowCancelRequest(
            job_id=job_id,
            workflow_id="wf-target",
            request_id="req-fan-out",
            requester_id="client-1",
            timestamp=time.time(),
        )

        reply = await gate_handler.handle_cancel_single_workflow(
            CLIENT_CALLBACK_ADDR, request.dump(), unreachable_collaborator("handle_exception")
        )

        response = SingleWorkflowCancelResponse.load(reply)
        assert north_manager_addr not in contacted
        assert sorted(contacted) == sorted([east.tcp_addr, dead_west_manager_addr, west.tcp_addr])
        assert east.job_manager.workflow_lifecycle.get_state(job_id, "wf-target") == WorkflowState.CANCELLED
        assert west.job_manager.workflow_lifecycle.get_state(job_id, "wf-target") == WorkflowState.CANCELLING
        assert response.status == WorkflowCancellationStatus.CANCELLING.value
        assert response.errors == ["No response from worker"]

    @pytest.mark.asyncio
    async def test_cancelled_workflow_bucket_prevents_resurrection(self, manager_node):
        """A workflow cancelled before it ran is never dispatched afterwards."""
        job_id = "job-no-resurrection"
        await manager_node.register_job(job_id, "wf-dead")
        assert await manager_node.leases.claim_job_leadership(job_id, manager_node.tcp_addr)
        request = SingleWorkflowCancelRequest(
            job_id=job_id,
            workflow_id="wf-dead",
            request_id="req-dead",
            requester_id="client-1",
            timestamp=time.time(),
        )

        response = SingleWorkflowCancelResponse.load(
            await manager_node.coordinator.handle_cancel_single_workflow(CLIENT_CALLBACK_ADDR, request.dump(), 0)
        )

        assert response.status == WorkflowCancellationStatus.PENDING_CANCELLED.value
        assert await manager_node.job_manager.claim_workflow_for_dispatch(job_id, "wf-dead") is False
        assert manager_node.job_manager.workflow_lifecycle.get_state(job_id, "wf-dead") == WorkflowState.CANCELLED
        repeated = SingleWorkflowCancelResponse.load(
            await manager_node.coordinator.handle_cancel_single_workflow(CLIENT_CALLBACK_ADDR, request.dump(), 0)
        )
        assert repeated.status == WorkflowCancellationStatus.ALREADY_CANCELLED.value

    @pytest.mark.parametrize("cancel_first", [True, False])
    @pytest.mark.asyncio
    async def test_per_workflow_lock_prevents_race(self, manager_node, cancel_first):
        """A cancel racing a dispatch claim: exactly one wins, and the loser
        sees it -- a workflow claimed for dispatch is stopped through its
        workers (CANCELLING), one cancelled first is never dispatched."""
        job_id = "job-race"
        await manager_node.register_job(job_id, "wf-race")
        claim = manager_node.job_manager.claim_workflow_for_dispatch(job_id, "wf-race")
        cancel = manager_node.job_manager.cancel_workflows(job_id, {"wf-race"}, "cancelled by request")

        if cancel_first:
            (cancelled_now, cancelling), claimed = await asyncio.gather(cancel, claim)
        else:
            claimed, (cancelled_now, cancelling) = await asyncio.gather(claim, cancel)

        final_state = manager_node.job_manager.workflow_lifecycle.get_state(job_id, "wf-race")
        if claimed:
            assert (cancelled_now, cancelling, final_state) == ([], ["wf-race"], WorkflowState.CANCELLING)
        else:
            assert (cancelled_now, cancelling, final_state) == (["wf-race"], [], WorkflowState.CANCELLED)
        assert claimed is not cancel_first


# =========================================================================
# 4.7: Cancellation During Leadership Failover
# =========================================================================


class TestCancellationDuringLeadershipFailover:
    """
    Test cancellation that's in progress when the job leader fails.

    The new leader must pick up cancellation state and complete the flow.
    """

    @pytest.mark.asyncio
    async def test_pending_cancellations_survive_leader_change(self, recording_logger):
        """The pending-cancellation tracker lives only on the job leader. A
        new leader the re-sent cancel reaches takes the job over, rebuilds
        the tracker from the job's in-flight sub-workflows, adopts the
        callback the request carries, and pushes completion once the last
        worker confirms."""
        job_id = "job-mid-cancel"
        silent_worker_addr = (MANAGER_HOST, 9202)
        worker_edge = WorkerEdge(silent_worker_addrs=frozenset({silent_worker_addr}))
        new_leader = ManagerNode("dc-1", 9002, recording_logger, is_cluster_leader=True, worker_edge=worker_edge)
        await new_leader.register_job(job_id, "wf-1")
        new_leader.state.set_job_leader(job_id, "manager-dead")
        acked_sub, silent_sub = await new_leader.dispatch_to_workers(job_id, "wf-1", [9201, silent_worker_addr[1]])
        resent_cancel = JobCancelRequest(
            job_id=job_id,
            requester_id="client-1",
            timestamp=time.time(),
            reason="user cancel",
            callback_addr=CLIENT_CALLBACK_ADDR,
        )

        response = JobCancelResponse.load(
            await new_leader.coordinator.handle_cancel_job(CLIENT_CALLBACK_ADDR, resent_cancel.dump(), 0)
        )

        assert new_leader.leases.is_job_leader(job_id)
        assert sorted(sub for _addr, sub in worker_edge.cancel_requests) == sorted([acked_sub, silent_sub])
        assert response.cancelled_workflow_count == 1
        assert new_leader.state.get_cancellation_pending_workflows(job_id) == {silent_sub}
        assert new_leader.send_to_client.sent_messages == []

        await push_worker_completion(new_leader, job_id, silent_sub)
        await run_submitted_calls(new_leader.task_runner)

        assert new_leader.state.get_cancellation_pending_workflows(job_id) == set()
        [(callback_addr, method, payload)] = new_leader.send_to_client.sent_messages
        assert (callback_addr, method) == (CLIENT_CALLBACK_ADDR, "job_cancellation_complete")
        assert JobCancellationComplete.load(payload).success is True
        assert new_leader.job_manager.workflow_lifecycle.get_state(job_id, "wf-1") == WorkflowState.CANCELLED

    @pytest.mark.asyncio
    async def test_partial_completion_tracked(self, manager_node):
        """New leader correctly handles workflows that already reported completion."""
        job_id = "job-partial"
        manager_node.state.set_job_leader(job_id, "manager-dead")
        second_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-2", "worker-1"))
        third_sub = str(manager_node.job_manager.create_sub_workflow_token(job_id, "wf-3", "worker-1"))
        await seed_job_cancellation(manager_node, job_id, [second_sub, third_sub], force_takeover=True)

        await push_worker_completion(manager_node, job_id, second_sub)
        await push_worker_completion(manager_node, job_id, second_sub)

        assert manager_node.state.get_cancellation_pending_workflows(job_id) == {third_sub}
        assert manager_node.task_runner.submitted_calls == []

    @pytest.mark.asyncio
    async def test_raft_proposal_for_cancel_before_takeover(self):
        """A cancellation the old leader committed through the job's Raft
        group survives its death: the member taking the job over adopts it
        from its replica, and does not record it a second time."""
        cluster = LedgerCluster()
        await cluster.start()
        old_leader_addr, new_leader_addr = cluster.addresses[0], cluster.addresses[1]
        filesystem = SimFilesystem()

        async def open_ledger(member: str, regional_replicator) -> JobLedger:
            return await JobLedger.open(
                wal_path=Path(f"/{member}/ledger/wal"),
                checkpoint_dir=Path(f"/{member}/ledger/checkpoints"),
                archive_dir=Path(f"/{member}/ledger/archive"),
                region_code="dc-1",
                gate_id=member,
                clock=new_hybrid_logical_clock(),
                regional_replicator=regional_replicator,
                filesystem=filesystem,
            )

        old_ledger = await open_ledger("old-leader", cluster.replicators[old_leader_addr].replicate)
        new_ledger = await open_ledger("new-leader", cluster.replicators[new_leader_addr].replicate)
        try:
            _created_id, created = await old_ledger.create_job(
                spec_hash=b"spec",
                assigned_datacenters=("dc-1",),
                requestor_id="client-1",
                durability=DurabilityLevel.REGIONAL,
                job_id=REPLICATED_JOB_ID,
            )
            requested = await old_ledger.request_cancellation(
                REPLICATED_JOB_ID, reason="user cancel", requestor_id="client-1", durability=DurabilityLevel.REGIONAL
            )
            assert created.success and requested.success
            cluster.unreachable.add(old_leader_addr)
            new_leader_replica = cluster.replicas[new_leader_addr]
            await cluster.wait_until(
                lambda: (state := new_leader_replica.job_state(REPLICATED_JOB_ID)) is not None and state.is_cancelled
            )

            adopted = await new_ledger.adopt_replicated_history(
                REPLICATED_JOB_ID, new_leader_replica.history(REPLICATED_JOB_ID)
            )

            assert adopted == 2
            assert new_ledger.get_job(REPLICATED_JOB_ID).is_cancelled
            assert (
                await new_ledger.request_cancellation(
                    REPLICATED_JOB_ID, reason="re-sent cancel", requestor_id="client-1"
                )
                is None
            )
        finally:
            await old_ledger.close()
            await new_ledger.close()
            await cluster.stop()


# =========================================================================
# 4.8: Gate Orphan Job Handling
# =========================================================================


class TestGateOrphanJobHandling:
    """
    Test gate's response when a job's leader *gate* dies.

    Gate tracks dead job leaders, scans for orphaned jobs, waits grace
    period, then either receives transfer or marks job as failed. (A
    manager's death is the manager tier's to fail over -- the gate only
    records it against the manager's circuit breaker and learns the new
    leader from its transfer, TestJobLeaderFailsNotSwimLeader.)
    """

    @pytest.mark.asyncio
    async def test_gate_tracks_dead_leader_gate_addrs(self, recording_logger):
        """A dead leader gate is tracked as dead; a live one is not."""
        dead_leader = dead_gate("a", 9010)
        live_leader = dead_gate("b", 9020)
        gate = GateNode("c", 9030, recording_logger, RealClock(), is_cluster_leader=False)
        gate.learn_job("job-of-dead", dead_leader)
        gate.learn_job("job-of-live", live_leader)

        gate.coordinator.mark_jobs_orphaned_by_gate(dead_leader[2])

        assert gate.state.is_leader_dead(dead_leader[2])
        assert not gate.state.is_leader_dead(live_leader[2])

    @pytest.mark.asyncio
    async def test_gate_scans_jobs_for_dead_leaders(self, recording_logger):
        """Every job the dead gate led -- and only those -- is orphaned, and
        the SWIM leader evaluates the confirmed orphans at once."""
        dead_leader = dead_gate("a", 9010)
        live_leader = dead_gate("b", 9020)
        gate = GateNode("c", 9030, recording_logger, RealClock(), is_cluster_leader=True)
        gate.learn_job("job-1", dead_leader)
        gate.learn_job("job-2", live_leader)
        gate.learn_job("job-3", dead_leader)

        orphaned = gate.coordinator.mark_jobs_confirmed_orphaned_by_gate(dead_leader[2])

        assert sorted(orphaned) == ["job-1", "job-3"]
        assert sorted(gate.state.get_orphaned_jobs()) == ["job-1", "job-3"]
        [(call, args, _kwargs)] = gate.task_runner.submitted_calls
        assert call == gate.coordinator.evaluate_confirmed_orphans
        assert sorted(args[0]) == ["job-1", "job-3"]

    def test_gate_grace_period_before_failure(self):
        """An unconfirmed orphan no gate takes over waits the derived grace
        before it is even due, and one more failover window before it fails."""
        died_at, gate = orphan_job_of_silent_dead_gate(observe_seconds=3 * GATE_ORPHAN_GRACE_SECONDS)

        [(job_id, _reason, failed_at)] = gate.failed_jobs
        assert job_id == "job-orphan"
        assert failed_at - died_at >= 2 * GATE_ORPHAN_GRACE_SECONDS

    def test_gate_grace_expired_marks_job_failed(self):
        """Past its grace and takeover window, the orphan fails: the job is
        FAILED, finalized, its client told, and the orphan released."""
        died_at, gate = orphan_job_of_silent_dead_gate(observe_seconds=3 * GATE_ORPHAN_GRACE_SECONDS)

        [(job_id, reason, failed_at)] = gate.failed_jobs
        assert job_id == "job-orphan"
        assert reason.startswith("orphaned for ")
        assert failed_at - died_at < 2 * GATE_ORPHAN_GRACE_SECONDS + 2 * GATE_ORPHAN_CHECK_INTERVAL_SECONDS
        assert gate.job_manager.get_job("job-orphan").status == JobStatus.FAILED.value
        assert not gate.state.is_job_orphaned("job-orphan")
        [(callback_addr, method, payload)] = gate.sent_messages
        assert (callback_addr, method) == (CLIENT_CALLBACK_ADDR, "job_status_push")
        push = JobStatusPush.load(payload)
        assert (push.job_id, push.status, push.is_final) == ("job-orphan", JobStatus.FAILED.value, True)

    @pytest.mark.asyncio
    async def test_transfer_rescues_orphan_while_dead_leader_stays_tracked(self, recording_logger):
        """The SWIM leader's takeover announcement rescues the orphan on its
        peers: they record the new leader under a higher fence and stop
        waiting on the job. The dead gate stays tracked as dead -- only its
        rejoin clears it (GatePeerCoordinator) -- so its other orphans stay
        orphaned until they are taken over too."""
        dead_leader = dead_gate("a", 9010)
        swim_leader = GateNode("b", 9020, recording_logger, RealClock(), is_cluster_leader=True)
        peer = GateNode("c", 9030, recording_logger, RealClock(), is_cluster_leader=False)
        swim_leader.peer_gates[peer.tcp_addr] = peer
        for gate in (swim_leader, peer):
            gate.learn_job("job-recovered", dead_leader)
        peer.learn_job("job-still-orphaned", dead_leader)
        swim_leader.coordinator.mark_jobs_confirmed_orphaned_by_gate(dead_leader[2])
        peer.coordinator.mark_jobs_orphaned_by_gate(dead_leader[2])

        await run_submitted_calls(swim_leader.task_runner)

        assert swim_leader.tracker.is_leader("job-recovered")
        assert not swim_leader.state.is_job_orphaned("job-recovered")
        assert peer.tracker.get_leader("job-recovered") == swim_leader.node_id.full
        assert peer.tracker.get_leader_addr("job-recovered") == swim_leader.tcp_addr
        assert peer.tracker.get_fencing_token("job-recovered") == 2
        assert not peer.state.is_job_orphaned("job-recovered")
        assert peer.state.is_job_orphaned("job-still-orphaned")
        assert peer.state.is_leader_dead(dead_leader[2])
        [(_addr, method, payload)] = swim_leader.sent_messages
        assert method == "job_leadership_announcement"

    @pytest.mark.asyncio
    async def test_concurrent_failures_handled(self, recording_logger):
        """Two leader gates dying together: both are tracked dead, and the
        SWIM leader takes over every job either led -- not the live gate's."""
        first_dead = dead_gate("a", 9010)
        second_dead = dead_gate("b", 9020)
        live_leader = dead_gate("d", 9040)
        swim_leader = GateNode("c", 9030, recording_logger, RealClock(), is_cluster_leader=True)
        swim_leader.learn_job("job-a1", first_dead)
        swim_leader.learn_job("job-a2", first_dead)
        swim_leader.learn_job("job-b1", second_dead)
        swim_leader.learn_job("job-live", live_leader)

        swim_leader.coordinator.mark_jobs_confirmed_orphaned_by_gate(first_dead[2])
        swim_leader.coordinator.mark_jobs_confirmed_orphaned_by_gate(second_dead[2])
        await run_submitted_calls(swim_leader.task_runner)

        assert swim_leader.state.is_leader_dead(first_dead[2])
        assert swim_leader.state.is_leader_dead(second_dead[2])
        assert sorted(swim_leader.tracker.get_jobs_led_by(swim_leader.node_id.full)) == ["job-a1", "job-a2", "job-b1"]
        assert swim_leader.tracker.get_leader_addr("job-live") == live_leader[2]
        assert swim_leader.state.get_orphaned_jobs() == {}


# =========================================================================
# Worker Cancellation Cleanup Tests
# =========================================================================


class TestWorkerCancellationCleanup:
    """Test worker cancellation handler cleanup and observability."""

    @pytest.mark.asyncio
    async def test_stale_events_cleaned_up(
        self, worker_state, cancellation_handler
    ):
        """Stale cancel events are removed when workflows are no longer active."""
        cancellation_handler.create_cancel_event("wf-active")
        cancellation_handler.create_cancel_event("wf-stale")

        active_ids = {"wf-active"}
        removed = cancellation_handler.cleanup_stale_events(active_ids)

        assert removed == 1
        assert "wf-stale" not in worker_state._workflow_cancel_events
        assert "wf-active" in worker_state._workflow_cancel_events

    @pytest.mark.asyncio
    async def test_cleanup_removes_completion_events_too(
        self, worker_state, cancellation_handler
    ):
        """Stale cleanup also removes completion events and errors."""
        cancellation_handler.create_cancel_event("wf-gone")
        worker_state._cancellation_completion_events["wf-gone"] = asyncio.Event()
        worker_state._cancellation_errors["wf-gone"] = ["some error"]

        removed = cancellation_handler.cleanup_stale_events(set())

        assert "wf-gone" not in worker_state._cancellation_completion_events
        assert "wf-gone" not in worker_state._cancellation_errors

    @pytest.mark.asyncio
    async def test_cancellation_stats_observability(
        self, worker_state, cancellation_handler
    ):
        """Stats method returns accurate counts."""
        cancellation_handler.create_cancel_event("wf-1")
        cancellation_handler.create_cancel_event("wf-2")
        worker_state._cancellation_completion_events["wf-1"] = asyncio.Event()
        worker_state._cancellation_errors["wf-3"] = ["err"]

        stats = cancellation_handler.get_cancellation_stats()

        assert stats["cancel_events"] == 2
        assert stats["completion_events"] == 1
        assert stats["pending_errors"] == 1

    @pytest.mark.asyncio
    async def test_remove_active_workflow_cleans_cancellation_state(
        self, worker_state
    ):
        """Removing a workflow cleans up all cancellation tracking."""
        workflow_id = "wf-remove"
        progress = MagicMock(spec=WorkflowProgress)
        progress.job_id = "job-1"
        worker_state.add_active_workflow(
            workflow_id, progress, ("10.0.0.1", 9000)
        )
        worker_state._workflow_cancel_events[workflow_id] = asyncio.Event()
        worker_state._cancellation_completion_events[workflow_id] = asyncio.Event()
        worker_state._cancellation_errors[workflow_id] = ["err"]

        worker_state.remove_active_workflow(workflow_id)

        assert workflow_id not in worker_state._workflow_cancel_events
        assert workflow_id not in worker_state._cancellation_completion_events
        assert workflow_id not in worker_state._cancellation_errors


# =========================================================================
# Manager Cancellation Coordinator JobManager Wiring Tests
# =========================================================================


class TestManagerCancellationCoordinatorJobManagerWiring:
    """The coordinator cancels the job's actual sub-workflow tokens, read
    from the JobManager's JobInfo -- the strings workers key workflows by."""

    @pytest.mark.asyncio
    async def test_get_workflow_ids_returns_sub_workflows(self, manager_node):
        """Coordinator returns sub-workflow tokens from the job's JobInfo."""
        job = await manager_node.register_job("job-real", "wf-1")
        await manager_node.dispatch_to_workers("job-real", "wf-1", [9101, 9102, 9103])

        workflows_to_cancel = manager_node.coordinator.get_running_workflows_to_cancel(job, ["wf-1"])

        assert sorted(sub_token for sub_token, _worker_id, _worker_addr in workflows_to_cancel) == sorted(
            job.sub_workflows.keys()
        )
        assert sorted(worker_addr for _sub_token, _worker_id, worker_addr in workflows_to_cancel) == [
            (MANAGER_HOST, 9101),
            (MANAGER_HOST, 9102),
            (MANAGER_HOST, 9103),
        ]

    @pytest.mark.asyncio
    async def test_get_workflow_ids_returns_empty_for_unknown_job(self, manager_node):
        """Nothing to cancel for a workflow not in flight, and no JobInfo for an unknown job."""
        job = await manager_node.register_job("job-real", "wf-1")
        await manager_node.dispatch_to_workers("job-real", "wf-1", [9101])

        assert manager_node.coordinator.get_running_workflows_to_cancel(job, ["wf-unknown"]) == []
        assert manager_node.job_manager.get_job_by_id("job-nonexistent") is None
