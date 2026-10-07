"""
Gate -> manager dispatch and the per-manager circuit breaker.

A manager that answers -- even "Not DC leader, retry" while it elects --
is reachable, so its answers must never open the breaker. They used to:
every rejection counted as a circuit failure, so a manager's warmup
retries for one job opened its breaker, and the next job dispatched to
it (a job pinned to that datacenter has nowhere else to go) failed
without a send (measured in the dc_loss SIM). Only a manager that does
not answer opens it.

And a datacenter that answers "retry" is replacing its leader: its
dispatch retries for as long as that can take (the datacenter's leader
failover, derived from its election timings) and no longer, at one leader
heartbeat's pace -- and a follower that names its leader is taken at its
word at once rather than retried until the budget runs out.

Driven through the coordinator's real dispatch with a real
CircuitBreakerManager.
"""

import asyncio
import inspect
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.logging import Logger
from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.datacenter_manager_selector import DatacenterManagerSelector
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
import sys

import cloudpickle

from hyperscale.core.graph.workflow import Workflow
from hyperscale.core.hooks import step
from hyperscale.distributed.models import JobAck, JobSubmission
from hyperscale.distributed.nodes.gate.config import derive_datacenter_leader_failover_seconds
from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.routing import BlendedScoringConfig, ObservedLatencyTracker

# The gate's configured TCP timeouts, as a default Env gives them.
GATE_SETTINGS = Env()


def make_manager_selector(state: GateRuntimeState) -> DatacenterManagerSelector:
    """A real AD-28 selector reading the test's runtime state."""
    return DatacenterManagerSelector(
        create_discovery=lambda: DiscoveryService(
            Env().get_discovery_config(
                node_role="gate",
                static_seeds=[],
                allow_dynamic_registration=True,
            ),
            Logger(),
        ),
        get_manager_heartbeats=state.get_datacenter_manager_statuses,
    )


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

    def set_target_dcs(self, job_id: str, dcs: set[str]):
        self.target_dcs[job_id] = dcs

    def get_target_dcs(self, job_id: str) -> set[str]:
        return self.target_dcs.get(job_id, set())

    def set_callback(self, job_id: str, callback):
        self.callbacks[job_id] = callback

    def get_callback(self, job_id: str):
        return self.callbacks.get(job_id)

    def set_fence_token(self, job_id: str, token: int):
        self.fence_tokens[job_id] = token

    def get_fence_token(self, job_id: str) -> int:
        return self.fence_tokens.get(job_id, 0)

    def job_count(self) -> int:
        return self.job_count_val


@dataclass
class MockQuorumCircuit:
    """Mock quorum circuit breaker."""

    circuit_state: CircuitState = CircuitState.CLOSED
    half_open_after: float = 10.0
    successes: int = 0

    error_count: int = 0

    def record_success(self):
        self.successes += 1

    def record_error(self):
        self.error_count += 1


MANAGER = ("10.0.0.5", 9000)
LEADER = ("10.0.0.6", 9000)
DISPATCH_ROUNDS = 20
DATACENTER_LEADER_FAILOVER_SECONDS = derive_datacenter_leader_failover_seconds(GATE_SETTINGS)


class SleepAdvancedClock:
    """Virtual time that passes only while the dispatch sleeps."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def time(self) -> float:
        return self.now

    async def sleep(self, seconds: float) -> None:
        self.now += seconds


def make_coordinator(
    send_tcp: AsyncMock,
    breakers: CircuitBreakerManager,
    clock: RealClock | SleepAdvancedClock | None = None,
    managers: list[tuple[str, int]] | None = None,
) -> GateDispatchCoordinator:
    state = GateRuntimeState(forward_throughput_interval_start=0.0)
    return GateDispatchCoordinator(
        clock=clock if clock is not None else RealClock(),
        manager_dispatch_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        state=state,
        manager_selector=make_manager_selector(state),
        finalize_failed_job=AsyncMock(),
        on_job_dispatched=AsyncMock(),
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_timeout_tracker=MagicMock(),
        observed_latency_tracker=ObservedLatencyTracker(
            config=BlendedScoringConfig.from_env(GATE_SETTINGS),
            clock=RealClock(),
        ),
        estimate_datacenter_latencies_ms=lambda: {"dc-west": 1.0},
        circuit_breaker_manager=breakers,
        datacenter_managers={"dc-west": managers if managers is not None else [MANAGER]},
        send_tcp=send_tcp,
        increment_version=lambda: None,
        confirm_manager_for_dc=lambda *args, **kwargs: None,
        suspect_manager_for_dc=lambda *args, **kwargs: None,
        record_forward_throughput_event=lambda *args, **kwargs: None,
        record_forward_attempt_event=lambda *args, **kwargs: None,
        get_node_host=lambda: "127.0.0.1",
        get_node_port=lambda: 9000,
        get_node_id_short=lambda: "gate-a",
        job_manager=MockGateJobManager(),
        quorum_circuit=MockQuorumCircuit(),
        select_datacenters=AsyncMock(return_value=(["dc-west"], [], "healthy")),
        broadcast_leadership=AsyncMock(),
        datacenter_leader_failover_seconds=derive_datacenter_leader_failover_seconds(GATE_SETTINGS),
        leader_heartbeat_interval_seconds=GATE_SETTINGS.LEADER_HEARTBEAT_INTERVAL,
        record_fallback_used=lambda from_datacenter, to_datacenter: None,
    )


def submission() -> JobSubmission:
    return JobSubmission(job_id="job-1", workflows=b"", vus=1, timeout_seconds=30.0, datacenter_count=1)


async def dispatch_rounds(coordinator: GateDispatchCoordinator) -> list[tuple[tuple[str, int] | None, str | None]]:
    """Each round one send: its deadline is already reached."""
    return [
        await coordinator._try_dispatch_to_manager("dc-west", MANAGER, submission(), coordinator._clock.monotonic())
        for _ in range(DISPATCH_ROUNDS)
    ]


@pytest.mark.asyncio
async def test_a_manager_that_keeps_answering_retry_never_opens_its_breaker() -> None:
    electing = JobAck(job_id="job-1", accepted=False, error="Not DC leader, retry at leader: unknown").dump()
    send_tcp = AsyncMock(return_value=(electing, 0))
    breakers = CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False)
    coordinator = make_coordinator(send_tcp, breakers)

    outcomes = await dispatch_rounds(coordinator)

    assert all(accepting_manager is None for accepting_manager, _ in outcomes)
    assert not await breakers.is_circuit_open(MANAGER)
    assert send_tcp.await_count == DISPATCH_ROUNDS  # every round really sent


@pytest.mark.asyncio
async def test_a_manager_that_never_answers_opens_its_breaker() -> None:
    send_tcp = AsyncMock(return_value=(None, 0))
    breakers = CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False)
    coordinator = make_coordinator(send_tcp, breakers)

    outcomes = await dispatch_rounds(coordinator)

    assert await breakers.is_circuit_open(MANAGER)
    assert outcomes[-1] == (None, "Circuit breaker is OPEN")


def answering(
    clock: SleepAdvancedClock, answers: dict[tuple[str, int], bytes], sends: list[tuple[tuple[str, int], float]]
) -> AsyncMock:
    async def send_tcp(manager_addr, action, payload, timeout):
        sends.append((manager_addr, clock.now))
        return (answers[manager_addr], 0)

    return AsyncMock(side_effect=send_tcp)


ACCEPTED = JobAck(job_id="job-1", accepted=True).dump()
ELECTING = JobAck(job_id="job-1", accepted=False, error="Not DC leader, retry at leader: unknown").dump()


@pytest.mark.asyncio
async def test_a_follower_naming_its_leader_is_redirected_at_once() -> None:
    clock = SleepAdvancedClock()
    sends: list[tuple[tuple[str, int], float]] = []
    redirect = JobAck(
        job_id="job-1",
        accepted=False,
        error=f"Not DC leader, retry at leader: {LEADER[0]}:{LEADER[1]}",
        leader_addr=LEADER,
    ).dump()
    coordinator = make_coordinator(
        answering(clock, {MANAGER: redirect, LEADER: ACCEPTED}, sends),
        CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False),
        clock=clock,
        managers=[MANAGER, LEADER],
    )

    accepting_manager, error = await coordinator._try_dispatch_to_manager(
        "dc-west", MANAGER, submission(), clock.now + DATACENTER_LEADER_FAILOVER_SECONDS
    )

    assert (accepting_manager, error) == (LEADER, None)
    # The leader is asked straight away: no backoff, no second ask of the follower.
    assert sends == [(MANAGER, 1000.0), (LEADER, 1000.0)]


@pytest.mark.asyncio
async def test_a_redirect_to_a_manager_the_gate_does_not_know_is_not_followed() -> None:
    clock = SleepAdvancedClock()
    sends: list[tuple[tuple[str, int], float]] = []
    stranger = ("192.0.2.9", 9000)
    redirect = JobAck(
        job_id="job-1", accepted=False, error="Not DC leader, retry at leader: 192.0.2.9:9000", leader_addr=stranger
    ).dump()
    coordinator = make_coordinator(
        answering(clock, {MANAGER: redirect}, sends),
        CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False),
        clock=clock,
    )

    accepting_manager, _error = await coordinator._try_dispatch_to_manager(
        "dc-west", MANAGER, submission(), clock.now
    )

    assert accepting_manager is None
    assert [manager_addr for manager_addr, _sent_at in sends] == [MANAGER]


@pytest.mark.asyncio
async def test_a_datacenter_electing_is_retried_through_its_failover_and_no_longer() -> None:
    """Answered "retry" throughout: the gate keeps asking, at most one
    leader heartbeat apart, right up to the datacenter's failover -- and
    sends nothing after it."""
    clock = SleepAdvancedClock()
    sends: list[tuple[tuple[str, int], float]] = []
    coordinator = make_coordinator(
        answering(clock, {MANAGER: ELECTING}, sends),
        CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False),
        clock=clock,
    )
    started_at = clock.now
    deadline_at = started_at + DATACENTER_LEADER_FAILOVER_SECONDS

    accepting_manager, error = await coordinator._try_dispatch_to_manager(
        "dc-west", MANAGER, submission(), deadline_at
    )

    assert accepting_manager is None and error is not None and "Not DC leader" in error
    sent_at = [at for _manager_addr, at in sends]
    assert sent_at[0] == started_at and sent_at[-1] == deadline_at, sent_at
    assert all(at <= deadline_at for at in sent_at), sent_at
    gaps = [later - earlier for earlier, later in zip(sent_at, sent_at[1:])]
    assert all(gap <= GATE_SETTINGS.LEADER_HEARTBEAT_INTERVAL for gap in gaps), gaps


@pytest.mark.asyncio
async def test_an_election_finishing_just_inside_the_failover_lands_the_job() -> None:
    clock = SleepAdvancedClock()
    elected_at = clock.now + DATACENTER_LEADER_FAILOVER_SECONDS - GATE_SETTINGS.LEADER_HEARTBEAT_INTERVAL
    sends: list[tuple[tuple[str, int], float]] = []

    async def send_tcp(manager_addr, action, payload, timeout):
        sends.append((manager_addr, clock.now))
        return (ACCEPTED if clock.now >= elected_at else ELECTING, 0)

    coordinator = make_coordinator(
        AsyncMock(side_effect=send_tcp),
        CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False),
        clock=clock,
    )

    accepting_manager, error = await coordinator._try_dispatch_to_manager(
        "dc-west", MANAGER, submission(), clock.now + DATACENTER_LEADER_FAILOVER_SECONDS
    )

    assert (accepting_manager, error) == (MANAGER, None)
    # Landed within one leader heartbeat of the election.
    assert elected_at <= sends[-1][1] <= elected_at + GATE_SETTINGS.LEADER_HEARTBEAT_INTERVAL


@pytest.mark.asyncio
async def test_a_datacenter_without_room_is_left_for_a_fallback_at_its_first_refusal() -> None:
    """D-65/D-67: a leader refusing a job it has no room for -- a hinted
    refusal outside the transient vocabulary -- answers for its whole
    datacenter: the dispatch stops there, asks the leader no second time
    through another manager, and counts no manager failure."""
    clock = SleepAdvancedClock()
    sends: list[tuple[tuple[str, int], float]] = []
    no_room = JobAck(
        job_id="job-1",
        accepted=False,
        error="datacenter dc-west has no room for another job: its concurrency cap of 1 unfinished jobs is reached",
        retry_after_seconds=2.0,
    ).dump()
    redirect = JobAck(
        job_id="job-1",
        accepted=False,
        error=f"Not DC leader, retry at leader: {LEADER[0]}:{LEADER[1]}",
        leader_addr=LEADER,
    ).dump()
    breakers = CircuitBreakerManager(Env(), is_peer_suspected=lambda _peer_addr: False)
    coordinator = make_coordinator(
        answering(clock, {MANAGER: redirect, LEADER: no_room}, sends),
        breakers,
        clock=clock,
        managers=[MANAGER, LEADER],
    )

    success, error, accepting_manager = await coordinator._try_dispatch_to_dc("job-1", "dc-west", submission())

    assert (success, accepting_manager) == (False, None)
    assert error is not None and "no room" in error
    assert [manager_addr for manager_addr, _sent_at in sends].count(LEADER) == 1, sends
    assert not await breakers.is_circuit_open(MANAGER)
    assert not await breakers.is_circuit_open(LEADER)


cloudpickle.register_pickle_by_value(sys.modules[__name__])


class Browse(Workflow):
    vus = 6

    @step()
    async def browse(self) -> dict:
        return {}


class Search(Workflow):
    # No VUs of its own: it runs with the job's.
    vus = 0

    @step()
    async def search(self) -> dict:
        return {}


class Report(Workflow):
    vus = 50

    @step()
    async def report(self) -> dict:
        return {}


@pytest.mark.asyncio
async def test_spillover_weighs_the_cores_the_jobs_first_workflows_would_use(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A job names no core requirement; spillover read one that is not
    there, so every job asked for one core. It asks for what its first
    workflows -- those depending on none -- would use as a manager's
    dispatcher allocates them: one core per VU, the workflow's own or the
    job's."""
    weighed: list[int] = []

    async def evaluate_spillover(self, job_id, primary_dc, fallback_dcs, job_cores_required):
        weighed.append(job_cores_required)
        return None

    async def dispatch_to(self, job_id, datacenter, job_submission):
        return True, None, MANAGER

    monkeypatch.setattr(GateDispatchCoordinator, "_evaluate_spillover", evaluate_spillover)
    monkeypatch.setattr(GateDispatchCoordinator, "_try_dispatch_to_dc", dispatch_to)
    coordinator = make_coordinator(AsyncMock(), CircuitBreakerManager(GATE_SETTINGS, is_peer_suspected=lambda _peer_addr: False))
    job = JobSubmission(
        job_id="job-1",
        workflows=cloudpickle.dumps(
            [
                ("browse", [], Browse()),
                ("search", [], Search()),
                ("report", ["browse", "search"], Report()),
            ]
        ),
        vus=4,
        timeout_seconds=30.0,
        datacenter_count=1,
    )

    await coordinator._dispatch_job_with_fallback(job, ["dc-west"], [])

    assert weighed == [6 + 4]
