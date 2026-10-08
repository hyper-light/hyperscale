"""
A job's client gets one final result for it: the gate's, across every
datacenter.

* A job's leader gate forwarded each datacenter's final result to a peer
  gate, which pushed it to the client as the job's final result -- the
  message a gateless manager sends for its one datacenter. The client
  took the first datacenter to finish for the whole job: its status, its
  totals, its workflow results (each workflow keeps the first result it
  gets, so the cross-datacenter aggregates were dropped), and the job
  ended for it while other datacenters still ran it.
* With the client out of reach, the gate recorded the job's global result
  twice: once finishing the job, then again rebuilt (only a delivered push
  marked it sent) -- a second terminal in the client's replay, pushed
  without the claim that lets one path alone finish the job.

A real ``GateServer`` (never started) leads a job in two datacenters, with
a peer gate that accepts whatever it is sent and a client out of reach.
"""

import asyncio

import pytest

from hyperscale.distributed.env import Env
from hyperscale.distributed.models import (
    GateInfo,
    GlobalJobResult,
    GlobalJobStatus,
    JobFinalResult,
    JobStatus,
)
from hyperscale.distributed.nodes.gate.server import GateServer
from hyperscale.distributed.runtime import RealClock

JOB_ID = "job-1"
HOST = "127.0.0.1"
CLIENT_CALLBACK = (HOST, 19500)
PEER_GATE_ADDRESS = (HOST, 19471)
DATACENTERS = ("dc-a", "dc-b")


class BackoffFreeClock:
    """The real clock, except that a sleep only yields: the client push
    retries' backoff costs the test nothing."""

    def __init__(self) -> None:
        self._clock = RealClock()

    def monotonic(self) -> float:
        return self._clock.monotonic()

    def monotonic_ns(self) -> int:
        return self._clock.monotonic_ns()

    def time(self) -> float:
        return self._clock.time()

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(0)

    async def wait_for(self, awaitable, timeout: float | None):
        return await self._clock.wait_for(awaitable, timeout)


def make_gate(sent: list[tuple[tuple[str, int], str]]) -> GateServer:
    gate = GateServer(
        host=HOST,
        tcp_port=19461,
        udp_port=19462,
        env=Env(MERCURY_SYNC_AUTH_SECRET="job-final-result-once-secret-01234567"),
        datacenter_managers={
            "dc-a": [(HOST, 19561)],
            "dc-b": [(HOST, 19661)],
        },
        datacenter_manager_udp={
            "dc-a": [(HOST, 19562)],
            "dc-b": [(HOST, 19662)],
        },
        clock=BackoffFreeClock(),
    )
    gate._modular_state.add_known_gate(
        "gate-peer",
        GateInfo(
            node_id="gate-peer",
            tcp_host=PEER_GATE_ADDRESS[0],
            tcp_port=PEER_GATE_ADDRESS[1],
            udp_host=PEER_GATE_ADDRESS[0],
            udp_port=PEER_GATE_ADDRESS[1] + 1,
            datacenter="global",
        ),
    )

    async def record_send(address, action, payload, timeout=None):
        sent.append((address, action))
        if address == CLIENT_CALLBACK:
            return ConnectionRefusedError(f"{address} is not listening"), 0
        return b"ok", 0

    gate.send_tcp = record_send
    # As a started gate is.
    gate._accepting_requests = True
    gate._job_manager.set_job(
        JOB_ID,
        GlobalJobStatus(
            job_id=JOB_ID,
            status=JobStatus.RUNNING.value,
            timestamp=gate._clock.monotonic(),
        ),
    )
    gate._job_manager.set_target_dcs(JOB_ID, set(DATACENTERS))
    gate._job_manager.set_callback(JOB_ID, CLIENT_CALLBACK)
    return gate


def final_result_from(datacenter: str) -> bytes:
    return JobFinalResult(
        job_id=JOB_ID,
        datacenter=datacenter,
        status=JobStatus.COMPLETED.value,
        total_completed=10,
    ).dump()


async def recorded_global_results(gate: GateServer) -> list[GlobalJobResult]:
    updates, _oldest_sequence, _latest_sequence = await gate._modular_state.get_client_updates_since(
        JOB_ID, 0
    )
    return [
        GlobalJobResult.load(payload)
        for _sequence, message_type, payload, _recorded_at in updates
        if message_type == "receive_global_job_result"
    ]


@pytest.mark.asyncio
async def test_a_job_has_one_final_result_for_its_client_across_its_datacenters() -> None:
    sent: list[tuple[tuple[str, int], str]] = []
    gate = make_gate(sent)

    first_answer = await gate.job_final_result((HOST, 19561), final_result_from("dc-a"), 0)
    sent_while_running = list(sent)
    last_answer = await gate.job_final_result((HOST, 19661), final_result_from("dc-b"), 0)
    # Anything the completion set off in the background has run.
    for _ in range(10):
        await asyncio.sleep(0)

    global_results = await recorded_global_results(gate)
    single_datacenter_results = {"job_final_result_forward", "receive_job_final_result"}
    assert (first_answer, last_answer) == (b"ok", b"ok")
    # One datacenter done is no result for the client.
    assert sent_while_running == []
    assert not single_datacenter_results & {action for _address, action in sent}
    assert len(global_results) == 1
    assert global_results[0].status == JobStatus.COMPLETED.value
    assert sorted(
        datacenter_result.datacenter for datacenter_result in global_results[0].per_datacenter_results
    ) == list(DATACENTERS)
