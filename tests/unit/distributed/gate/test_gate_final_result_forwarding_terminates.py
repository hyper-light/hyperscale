"""
A job's final result forwarded between gates ends after one hop (AD-13).

A gate that does not lead a job hands its final result on: to the gate it
believes leads the job, or to its peers when it knows no leader. The gate
that receives a forwarded result (``job_final_result_forwarded``) handles
it itself and never forwards it again. Without that, a result no gate
leads -- a late result for a job every gate has already cleaned up, or two
gates each holding a stale belief that the other leads -- circled between
the gates, each hop awaiting the next until a timeout unwound the chain.

Two real ``GateStateSyncHandler``s, each with its own job manager,
leadership tracker and circuit breakers, are joined by a network seam that
delivers each send to the addressed gate's handler for the action, exactly
as the gate server routes it.
"""

from __future__ import annotations

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.health.circuit_breaker_manager import CircuitBreakerManager
from hyperscale.distributed.jobs import JobLeadershipTracker
from hyperscale.distributed.jobs.gates import GateJobManager
from hyperscale.distributed.models import JobFinalResult
from hyperscale.distributed.nodes.gate.handlers.tcp_state_sync import GateStateSyncHandler
from hyperscale.distributed.nodes.gate.state import GateRuntimeState
from hyperscale.distributed.server.events.lamport_clock import VersionedStateClock
from hyperscale.distributed.swim.core.node_id import NodeId

FIRST_GATE_ADDR = ("10.0.0.1", 9000)
SECOND_GATE_ADDR = ("10.0.0.2", 9000)
JOB_ID = "job-final-result-forwarding"
GATE_SETTINGS = Env()


class RecordingLogger:
    """Collects log entries; the handler awaits ``log``."""

    def __init__(self) -> None:
        self.entries: list[object] = []

    async def log(self, entry: object) -> None:
        self.entries.append(entry)


class GateNetwork:
    """Delivers each send to the addressed gate's handler, by action, and
    records every hop."""

    def __init__(self) -> None:
        self.handlers: dict[tuple[str, int], GateStateSyncHandler] = {}
        self.hops: list[tuple[tuple[str, int], str]] = []

    async def deliver(
        self,
        destination: tuple[str, int],
        action: str,
        data: bytes,
        timeout: float | None = None,
    ) -> tuple[bytes, int]:
        self.hops.append((destination, action))
        handler = self.handlers[destination]
        assert action == "job_final_result_forwarded", f"a gate forwarded on {action}"
        reply = await handler.handle_job_final_result(
            destination, data, complete_job_never_called, raise_handled_exception, None
        )
        return reply, 0


async def complete_job_never_called(job_id: str, result: object) -> bool:
    raise AssertionError(f"no gate leads {job_id}; none may complete it")


async def raise_handled_exception(error: Exception, operation: str) -> None:
    raise error


def make_gate_handler(
    address: tuple[str, int],
    network: GateNetwork,
) -> tuple[GateStateSyncHandler, JobLeadershipTracker[int]]:
    node_id = NodeId.generate("dc-1", 50, host=address[0], port=address[1])
    leadership_tracker: JobLeadershipTracker[int] = JobLeadershipTracker(
        node_id=node_id.full,
        node_addr=address,
    )
    handler = GateStateSyncHandler(
        state=GateRuntimeState(forward_throughput_interval_start=0.0),
        logger=RecordingLogger(),
        task_runner=None,
        job_manager=GateJobManager(),
        job_leadership_tracker=leadership_tracker,
        versioned_clock=VersionedStateClock(),
        peer_circuit_breaker=CircuitBreakerManager(GATE_SETTINGS, is_peer_suspected=lambda _addr: False),
        send_tcp=network.deliver,
        get_node_id=lambda: node_id,
        get_host=lambda: address[0],
        get_tcp_port=lambda: address[1],
        is_leader=lambda: False,
        get_term=lambda: 0,
        get_state_snapshot=lambda: None,
        apply_state_snapshot=lambda snapshot: None,
        peer_forward_timeout_seconds=GATE_SETTINGS.GATE_TCP_TIMEOUT_FORWARD,
        # No manager heartbeat has been ingested: no term known yet.
        get_known_leader_manager_term=lambda datacenter: 0,
    )
    network.handlers[address] = handler
    return handler, leadership_tracker


def final_result_bytes() -> bytes:
    return JobFinalResult(job_id=JOB_ID, datacenter="dc-1", status="COMPLETED").dump()


@pytest.mark.asyncio
async def test_a_result_no_gate_knows_is_forwarded_once_and_stops() -> None:
    network = GateNetwork()
    first_gate, _ = make_gate_handler(FIRST_GATE_ADDR, network)
    make_gate_handler(SECOND_GATE_ADDR, network)

    async def forward_to_peers(data: bytes) -> bool:
        reply, _ = await network.deliver(SECOND_GATE_ADDR, "job_final_result_forwarded", data)
        return reply in (b"ok", b"already_completed")

    reply = await first_gate.handle_job_final_result(
        ("10.0.1.1", 8000),
        final_result_bytes(),
        complete_job_never_called,
        raise_handled_exception,
        forward_to_peers,
    )

    assert reply == b"unknown_job"
    assert network.hops == [(SECOND_GATE_ADDR, "job_final_result_forwarded")]


@pytest.mark.asyncio
async def test_two_gates_each_believing_the_other_leads_stop_after_one_hop() -> None:
    network = GateNetwork()
    first_gate, first_tracker = make_gate_handler(FIRST_GATE_ADDR, network)
    _, second_tracker = make_gate_handler(SECOND_GATE_ADDR, network)
    # Stale beliefs: each gate holds the other's claim to lead the job.
    first_tracker.process_leadership_claim(JOB_ID, "gate-two", SECOND_GATE_ADDR, fencing_token=1)
    second_tracker.process_leadership_claim(JOB_ID, "gate-one", FIRST_GATE_ADDR, fencing_token=1)

    async def forward_to_peers(data: bytes) -> bool:
        raise AssertionError("a gate that knows a leader forwards to it, not to its peers")

    reply = await first_gate.handle_job_final_result(
        ("10.0.1.1", 8000),
        final_result_bytes(),
        complete_job_never_called,
        raise_handled_exception,
        forward_to_peers,
    )

    assert reply == b"error"
    assert network.hops == [(SECOND_GATE_ADDR, "job_final_result_forwarded")]
