"""
A real ``MercurySyncBaseServer`` TCP exchange exercised over the
multi-process coordinator — the picklable child entries a test drives.

This is the stream-boundary twin of ``udp_demo_server``: two REAL OS
processes each run the production base server (the cluster stack every
gate/manager/worker node builds on), and one dials the other over the
``CrossProcessTransport`` stream seam. The whole production TCP path —
encode, compress, encrypt, length-prefix framing,
``MercurySyncTCPProtocol.data_received``, deframe, decrypt, dispatch to
the ``@tcp.receive()`` handler, response back — runs unchanged across
the deterministic cross-process boundary; the connect handshake costs
one round trip of coordinator latency, exactly like a real SYN/ACK.

Also carries the negative/late scenarios the stream boundary must get
right: dialing a hosted address with no stream listener is refused
(RST analog), and a server that starts *mid-run* becomes routable at
the window it registers in (the coordinator's route map grows at
barriers, not just at readiness).

Lives in an importable module because ``spawn`` re-imports the child
entries by module + qualname.
"""

import asyncio
import os

from hyperscale.distributed.env.env import Env
from hyperscale.distributed.server import tcp
from hyperscale.distributed.server.server.mercury_sync_base_server import (
    MercurySyncBaseServer,
)

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


class _EchoServer(MercurySyncBaseServer):
    """Minimal real base server: one TCP echo handler."""

    @tcp.receive()
    async def echo_tcp(self, addr, data, clock_time) -> bytes:
        return b"tcp-echo:" + data


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def echo_server_entry(context, host, tcp_port, udp_port, start_at) -> None:
    """Server child: start a ``_EchoServer`` at virtual time ``start_at``.

    ``start_at=0.0`` starts during setup (present from the first
    window); a later value exercises dynamic address registration — the
    listener only becomes routable at the barrier of the window it
    starts in.
    """
    server = _EchoServer(host, tcp_port, udp_port, _env(), **context.sim_kwargs())
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await server.start_server()
        log.append(("started", round(context.loop.time(), 6)))

    context.loop.call_at(start_at, lambda: context.loop.create_task(run()))


def echo_client_entry(
    context, host, tcp_port, udp_port, server_tcp_address, send_at
) -> None:
    """Client child: dial the server at virtual ``send_at`` and log the
    production ``send_tcp`` response with its arrival time."""
    client = _EchoServer(host, tcp_port, udp_port, _env(), **context.sim_kwargs())
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await client.start_server()
        response, _clock_time = await client.send_tcp(
            server_tcp_address, "echo_tcp", b"hello"
        )
        log.append(("response", bytes(response), round(context.loop.time(), 6)))

    context.loop.call_at(send_at, lambda: context.loop.create_task(run()))


def refused_client_entry(context, client_sockname, target_address) -> None:
    """Client child: dial an address that is hosted (so the peer
    answers) but has no stream listener — must raise
    ``ConnectionRefusedError`` one round trip later, the RST analog."""
    log: list = []
    context.set_result(log)

    async def run() -> None:
        try:
            await context.transport.connect_stream(
                client_sockname, target_address, asyncio.Protocol
            )
            log.append(("connected", round(context.loop.time(), 6)))
        except ConnectionRefusedError:
            log.append(("refused", round(context.loop.time(), 6)))

    context.loop.create_task(run())
