"""
A real ``UDPProtocol`` server exercised over the multi-process
coordinator — the picklable child entries a test drives.

This proves the *production* worker-pool UDP stack (the same
``UDPProtocol`` base ``RemoteGraphController`` uses — encryption,
compression, pickling, the node-id connect handshake, the request/reply
waiter machinery) runs unchanged over the ``CrossProcessTransport`` and
the ``SimulationCoordinator``: two real OS processes, coherent virtual
time, deterministic. Lives in an importable module because ``spawn``
re-imports the child entry by module + qualname.
"""

import os

from hyperscale.core.jobs.hooks import receive, send
from hyperscale.core.jobs.models import Env, JobContext
from hyperscale.core.jobs.protocols.udp_protocol import UDPProtocol

_AUTH_SECRET = "sim-multiprocess-secret-00000000"


class _GreetServer(UDPProtocol):
    """Minimal real UDP server: a ``greet`` request → ``hello-<name>``."""

    @receive()
    async def greet(self, shard_id, context):
        return JobContext(f"hello-{context.data}")

    @send()
    async def do_greet(self, address, name):
        return await self.send("greet", JobContext(name), target_address=address)


def _env() -> Env:
    os.environ.setdefault("MERCURY_SYNC_AUTH_SECRET", _AUTH_SECRET)
    return Env(MERCURY_SYNC_AUTH_SECRET=_AUTH_SECRET)


def greet_server_entry(context, host, port) -> None:
    """Server child: start a ``_GreetServer`` and wait for requests."""
    server = _GreetServer(
        host, port, _env(), loop=context.loop, transport_factory=context.transport
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await server.start_server("sim_greet_server.log")
        log.append(("started", round(context.loop.time(), 6)))

    context.loop.create_task(run())


def greet_client_entry(context, host, port, server_addr) -> None:
    """Client child: connect to the server and issue one ``greet``."""
    client = _GreetServer(
        host, port, _env(), loop=context.loop, transport_factory=context.transport
    )
    log: list = []
    context.set_result(log)

    async def run() -> None:
        await client.start_server("sim_greet_client.log")
        await client.connect_client("sim_greet_client.log", server_addr)
        log.append(("connected", round(context.loop.time(), 6)))
        _shard_id, response = await client.do_greet(server_addr, "world")
        log.append(("response", response.data, round(context.loop.time(), 6)))

    context.loop.create_task(run())
