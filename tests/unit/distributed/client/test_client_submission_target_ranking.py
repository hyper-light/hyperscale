"""
AD-28 submission target ranking (ClientTargetSelector + ClientJobSubmitter).

Every submission from every client started at the first configured gate
(``all_targets[retry % len(all_targets)]`` with retry == 0), so the first
gate absorbed all submission load and a dead first gate cost every job a
transport timeout before failover. Submission outcomes never reached any
selection state.

Pinned: gates before managers, every target exactly once; first-choice
spread across the gate tier by job id; measured-slow and failing targets
rank last; outcomes of unconfigured (redirect) addresses do not grow
selector state; the submitter records transport failures and accepted
round trips against the target that produced them.
"""

from unittest.mock import AsyncMock, Mock

import pytest

from hyperscale.distributed.discovery import DiscoveryService
from hyperscale.distributed.env import Env
from hyperscale.distributed.idempotency.idempotency_key import (
    IdempotencyKeyGenerator,
)
from hyperscale.distributed.jobs.logical_id_generator import LogicalIdGenerator
from hyperscale.distributed.models import JobAck
from hyperscale.distributed.nodes.client.config import ClientConfig
from hyperscale.distributed.nodes.client.protocol import ClientProtocol
from hyperscale.distributed.nodes.client.state import ClientState
from hyperscale.distributed.nodes.client.submission import ClientJobSubmitter
from hyperscale.distributed.nodes.client.targets import ClientTargetSelector
from hyperscale.distributed.nodes.client.tracking import ClientJobTracker
from hyperscale.distributed.runtime import RealClock
from hyperscale.logging import Logger

GATES = [("10.0.0.1", 9000), ("10.0.0.2", 9000), ("10.0.0.3", 9000)]
MANAGERS = [("10.0.1.1", 7000), ("10.0.1.2", 7000)]
JOB_IDS = [f"job-{index}" for index in range(200)]


def _discovery() -> DiscoveryService:
    return DiscoveryService(
        Env().get_discovery_config(
            node_role="client",
            static_seeds=[],
            allow_dynamic_registration=True,
        )
    )


def _selector(
    gates: list[tuple[str, int]] = GATES,
    managers: list[tuple[str, int]] = MANAGERS,
) -> tuple[ClientTargetSelector, DiscoveryService]:
    discovery = _discovery()
    config = ClientConfig(
        host="localhost",
        tcp_port=8000,
        env="test",
        managers=managers,
        gates=gates,
    )
    return ClientTargetSelector(config, ClientState(), discovery), discovery


@pytest.mark.parametrize("job_id", JOB_IDS[:25])
def test_gates_precede_managers_and_every_target_appears_once(job_id: str) -> None:
    selector, _ = _selector()

    ordered = selector.get_submission_targets(job_id)

    assert sorted(ordered[: len(GATES)]) == sorted(GATES)
    assert sorted(ordered[len(GATES) :]) == sorted(MANAGERS)


def test_target_configured_in_both_tiers_appears_once() -> None:
    shared = GATES[0]
    selector, _ = _selector(managers=[shared, *MANAGERS])

    ordered = selector.get_submission_targets("job-1")

    assert ordered.count(shared) == 1
    assert sorted(ordered) == sorted({*GATES, *MANAGERS})


def test_first_choice_spreads_across_the_gate_tier() -> None:
    selector, _ = _selector()

    first_choices = {selector.get_submission_targets(job_id)[0] for job_id in JOB_IDS}

    assert first_choices == set(GATES)


def test_measured_slow_gate_ranks_last_in_its_tier() -> None:
    selector, _ = _selector()
    slow_gate, *fast_gates = GATES
    for _ in range(50):
        selector.record_target_success(slow_gate, 400.0)
        for gate in fast_gates:
            selector.record_target_success(gate, 2.0)

    last_gates = {selector.get_submission_targets(job_id)[len(GATES) - 1] for job_id in JOB_IDS}

    assert last_gates == {slow_gate}


def test_failing_gate_ranks_last_in_its_tier() -> None:
    selector, _ = _selector()
    failing_gate, *healthy_gates = GATES
    for _ in range(50):
        selector.record_target_failure(failing_gate)
        for gate in healthy_gates:
            selector.record_target_success(gate, 2.0)

    last_gates = {selector.get_submission_targets(job_id)[len(GATES) - 1] for job_id in JOB_IDS}

    assert last_gates == {failing_gate}


def test_unconfigured_redirect_targets_do_not_grow_selector_state() -> None:
    selector, discovery = _selector()
    tracked_before = discovery._selector._ewma.tracked_peer_count

    for port in range(10_000):
        redirect_target = ("10.9.9.9", port)
        selector.record_target_success(redirect_target, 1.0)
        selector.record_target_failure(redirect_target)

    assert discovery._selector._ewma.tracked_peer_count == tracked_before
    assert discovery.peer_count == len(GATES) + len(MANAGERS)


def _submitter(selector: ClientTargetSelector, send_tcp: AsyncMock) -> ClientJobSubmitter:
    state = selector._state
    logger = Mock(spec=Logger)
    logger.log = AsyncMock()
    return ClientJobSubmitter(
        state,
        selector._config,
        logger,
        selector,
        ClientJobTracker(state, logger),
        ClientProtocol(state, logger),
        send_tcp,
        IdempotencyKeyGenerator(client_id="test-client"),
        LogicalIdGenerator(scope="test-client", clock=RealClock()),
    )


def _workflow() -> Mock:
    workflow = Mock()
    workflow.reporting = None
    return workflow


@pytest.mark.asyncio
async def test_submitter_records_transport_failure_then_accepts_on_next_ranked_target() -> None:
    selector, _ = _selector()
    selector.record_target_failure = Mock(wraps=selector.record_target_failure)
    selector.record_target_success = Mock(wraps=selector.record_target_success)
    attempted: list[tuple[str, int]] = []

    async def send_tcp(target, handler, payload, timeout):
        attempted.append(target)
        if len(attempted) == 1:
            return ConnectionRefusedError("refused"), None
        return JobAck(job_id="accepted", accepted=True).dump(), None

    job_id = await _submitter(selector, AsyncMock(side_effect=send_tcp)).submit_job([([], _workflow())])

    first_target, second_target = attempted
    assert first_target in GATES and second_target in GATES
    assert first_target != second_target
    selector.record_target_failure.assert_called_once_with(first_target)
    (success_target, latency_ms), _ = selector.record_target_success.call_args
    assert success_target == second_target
    assert latency_ms >= 0.0
    assert selector._state.get_job_target(job_id) == second_target
