"""
Gate -> manager dispatch and the per-manager circuit breaker.

A manager that answers -- even "Not DC leader, retry" while it elects --
is reachable, so its answers must never open the breaker. They used to:
every rejection counted as a circuit failure, so a manager's warmup
retries for one job opened its breaker, and the next job dispatched to
it (a job pinned to that datacenter has nowhere else to go) failed
without a send (measured in the dc_loss SIM). Only a manager that does
not answer opens it.

Driven through the coordinator's real dispatch with a real
CircuitBreakerManager.
"""

import asyncio
import inspect
from dataclasses import dataclass, field
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.runtime import RealClock
from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.nodes.gate.datacenter_manager_selector import DatacenterManagerSelector
from hyperscale.distributed.swim.core import CircuitState
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import JobAck, JobSubmission
from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState

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
            )
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


def make_dispatch_time_tracker():
    """Build a dispatch-time tracker mock with an async record_dispatch."""
    tracker = MagicMock()
    tracker.record_dispatch = AsyncMock(return_value=None)
    return tracker


MANAGER = ("10.0.0.5", 9000)
DISPATCH_ROUNDS = 20
RETRIES_PER_ROUND = 2


def make_coordinator(send_tcp: AsyncMock, breakers: CircuitBreakerManager) -> GateDispatchCoordinator:
    state = GateRuntimeState()
    return GateDispatchCoordinator(
        clock=RealClock(),
        manager_dispatch_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        client_push_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_STANDARD,
        state=state,
        manager_selector=make_manager_selector(state),
        finalize_failed_job=AsyncMock(),
        on_job_dispatched=AsyncMock(),
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_timeout_tracker=MagicMock(),
        dispatch_time_tracker=make_dispatch_time_tracker(),
        circuit_breaker_manager=breakers,
        datacenter_managers={"dc-west": [MANAGER]},
        send_tcp=send_tcp,
        increment_version=lambda: None,
        confirm_manager_for_dc=lambda *args, **kwargs: None,
        suspect_manager_for_dc=lambda *args, **kwargs: None,
        record_forward_throughput_event=lambda *args, **kwargs: None,
        get_node_host=lambda: "127.0.0.1",
        get_node_port=lambda: 9000,
        get_node_id_short=lambda: "gate-a",
        job_manager=MockGateJobManager(),
        quorum_circuit=MockQuorumCircuit(),
        select_datacenters=lambda count, datacenters, job_id: (["dc-west"], [], "healthy"),
        broadcast_leadership=AsyncMock(),
    )


def submission() -> JobSubmission:
    return JobSubmission(job_id="job-1", workflows=b"", vus=1, timeout_seconds=30.0, datacenter_count=1)


async def dispatch_rounds(coordinator: GateDispatchCoordinator) -> list[tuple[bool, str | None]]:
    return [
        await coordinator._try_dispatch_to_manager(MANAGER, submission(), max_retries=RETRIES_PER_ROUND, base_delay=0.0)
        for _ in range(DISPATCH_ROUNDS)
    ]


@pytest.mark.asyncio
async def test_a_manager_that_keeps_answering_retry_never_opens_its_breaker() -> None:
    electing = JobAck(job_id="job-1", accepted=False, error="Not DC leader, retry at leader: unknown").dump()
    send_tcp = AsyncMock(return_value=(electing, 0))
    breakers = CircuitBreakerManager(Env())
    coordinator = make_coordinator(send_tcp, breakers)

    outcomes = await dispatch_rounds(coordinator)

    assert all(accepted is False for accepted, _ in outcomes)
    assert not await breakers.is_circuit_open(MANAGER)
    assert send_tcp.await_count == DISPATCH_ROUNDS * (RETRIES_PER_ROUND + 1)  # every round really sent


@pytest.mark.asyncio
async def test_a_manager_that_never_answers_opens_its_breaker() -> None:
    send_tcp = AsyncMock(return_value=(None, 0))
    breakers = CircuitBreakerManager(Env())
    coordinator = make_coordinator(send_tcp, breakers)

    outcomes = await dispatch_rounds(coordinator)

    assert await breakers.is_circuit_open(MANAGER)
    assert outcomes[-1] == (False, "Circuit breaker is OPEN")
