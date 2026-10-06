"""
AD-32: a destination that stops answering cannot starve sends to others.

Every TCP request held one of the node's slots from its dial through its
reply, and a node-wide semaphore was the only bound: requests to a peer
that stopped answering held their slots for a full timeout each, and once
they held them all, a request to a healthy peer waited out theirs before
it could even be sent. Now each destination has its own bound, taken
before a node-wide slot, so requests queued behind a silent peer hold no
node-wide slot; the whole request -- its waits, its dial, its reply --
fits one deadline; and a destination is forgotten once its last request
settles.

A real ``MercurySyncBaseServer`` sends over the SIM transport: a node-wide
bound of 4 and a per-destination bound of 2; a peer that answers after
far longer than any request waits, and one that answers at once.
"""

import asyncio
import os

os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", "sim-test-secret-000000000000")

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.server.mercury_sync_base_server import MercurySyncBaseServer
from tests.simulation.harness.sim import InProcessTransport, SeededRandom, SimTransportFactory, SimulationLoop, VirtualClock

NODE_WIDE_BOUND = 4
PER_DESTINATION_BOUND = 2
REQUEST_TIMEOUT_SECONDS = 5.0
STALL_SECONDS = 1000.0
SILENT_PEER = ("127.0.0.1", 9002)
HEALTHY_PEER = ("127.0.0.1", 9004)


class _PeerServer(MercurySyncBaseServer):
    @tcp.receive()
    async def stall(self, addr, data, clock_time) -> bytes:
        await self._clock.sleep(STALL_SECONDS)
        return b"late"

    @tcp.receive()
    async def echo(self, addr, data, clock_time) -> bytes:
        return b"echo:" + data


def test_a_silent_destination_cannot_starve_a_healthy_one() -> None:
    loop = SimulationLoop()
    asyncio.set_event_loop(loop)
    factory = SimTransportFactory(InProcessTransport(loop))
    clock = VirtualClock(loop)
    env = Env(
        MERCURY_SYNC_AUTH_SECRET="sim-destination-isolation-secret-0123",
        MERCURY_SYNC_MAX_CONCURRENCY=NODE_WIDE_BOUND,
        OUTGOING_QUEUE_SIZE=PER_DESTINATION_BOUND,
    )
    sender, silent, healthy = (
        _PeerServer("127.0.0.1", port, port + 1, env, clock=clock, random_source=SeededRandom(port), transport_factory=factory)
        for port in (9000, SILENT_PEER[1], HEALTHY_PEER[1])
    )

    async def scenario() -> tuple[list[tuple[object, float]], tuple[object, float], dict, dict]:
        for server in (sender, silent, healthy):
            await server.start_server()
        started_at = clock.monotonic()

        async def send_to_silent() -> tuple[object, float]:
            response, _clock = await sender.send_tcp(SILENT_PEER, "stall", b"x", timeout=REQUEST_TIMEOUT_SECONDS)
            return response, clock.monotonic() - started_at

        silent_requests = [asyncio.ensure_future(send_to_silent()) for _ in range(3 * NODE_WIDE_BOUND)]
        await asyncio.sleep(0)
        healthy_response, _clock = await sender.send_tcp(HEALTHY_PEER, "echo", b"hi", timeout=REQUEST_TIMEOUT_SECONDS)
        healthy_answered_after = clock.monotonic() - started_at
        silent_outcomes = await asyncio.gather(*silent_requests)
        return (
            silent_outcomes,
            (healthy_response, healthy_answered_after),
            dict(sender._tcp_destination_slots),
            dict(sender._tcp_destination_requests),
        )

    try:
        silent_outcomes, (healthy_response, healthy_answered_after), slots_left, requests_left = (
            loop.run_until_complete(scenario())
        )
    finally:
        loop.run_until_complete(asyncio.gather(*(server.shutdown() for server in (sender, silent, healthy))))
        loop.close()
        asyncio.set_event_loop(None)

    # The healthy peer answers at once, the silent peer's requests filling
    # their own bound.
    assert healthy_response == b"echo:hi"
    assert healthy_answered_after < REQUEST_TIMEOUT_SECONDS
    # Every request to the silent peer ends within one deadline -- queued
    # or sent -- as a timeout.
    assert all(isinstance(response, TimeoutError) for response, _elapsed in silent_outcomes), silent_outcomes
    assert all(elapsed <= REQUEST_TIMEOUT_SECONDS for _response, elapsed in silent_outcomes), silent_outcomes
    # Nothing is left tracked for a destination once its requests settle.
    assert slots_left == {} and requests_left == {}
