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

from unittest.mock import AsyncMock, MagicMock

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.models import JobAck, JobSubmission
from hyperscale.distributed.nodes.gate.dispatch_coordinator import GateDispatchCoordinator
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from tests.unit.distributed.gate.test_gate_dispatch_coordinator import (
    MockGateJobManager,
    MockLogger,
    MockQuorumCircuit,
    MockTaskRunner,
    make_dispatch_time_tracker,
    make_lease_manager,
    make_manager_selector,
)

MANAGER = ("10.0.0.5", 9000)
DISPATCH_ROUNDS = 20
RETRIES_PER_ROUND = 2


def make_coordinator(send_tcp: AsyncMock, breakers: CircuitBreakerManager) -> GateDispatchCoordinator:
    state = GateRuntimeState()
    return GateDispatchCoordinator(
        state=state,
        manager_selector=make_manager_selector(state),
        finalize_failed_job=AsyncMock(),
        on_job_dispatched=AsyncMock(),
        logger=MockLogger(),
        task_runner=MockTaskRunner(),
        job_timeout_tracker=MagicMock(),
        dispatch_time_tracker=make_dispatch_time_tracker(),
        circuit_breaker_manager=breakers,
        job_lease_manager=make_lease_manager(),
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
        check_rate_limit=AsyncMock(return_value=(True, 0.0)),
        should_shed_request=lambda request_type: False,
        has_quorum_available=lambda: True,
        quorum_size=lambda: 1,
        quorum_circuit=MockQuorumCircuit(),
        select_datacenters=lambda count, datacenters, job_id: (["dc-west"], [], "healthy"),
        assume_leadership=lambda job_id, count, initial_token=None: None,
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
