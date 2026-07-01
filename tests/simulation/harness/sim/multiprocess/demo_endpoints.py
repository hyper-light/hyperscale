"""
Picklable demo child entries for exercising the multi-process coordinator.

These live in an importable module (not a test file) because ``spawn``
re-imports the target by module + qualname in each child process. They
model a trivial datagram exchange so tests can assert cross-process
virtual-time coherence and replay-determinism without pulling in a full
production server.
"""


class _EchoEndpoint:
    """Datagram endpoint that logs receipts (with virtual time) and, in
    the ``pong`` role, replies ``b"pong"`` to any ``b"ping"``."""

    def __init__(self, loop, role, log) -> None:
        self._loop = loop
        self._role = role
        self._log = log
        self.transport = None

    def connection_made(self, transport) -> None:
        self.transport = transport

    def datagram_received(self, data, addr) -> None:
        self._log.append((round(self._loop.time(), 6), "recv", data.decode(), addr))
        if self._role == "pong" and data == b"ping":
            self.transport.sendto(b"pong", addr)


def ping_pong_entry(context, address, peer, role) -> None:
    """Child entry: register one echo endpoint; the ``ping`` role sends a
    single ``b"ping"`` to ``peer`` at virtual time 5.0."""
    log: list = []
    endpoint = _EchoEndpoint(context.loop, role, log)
    context.transport.register_datagram_endpoint(address, endpoint)

    if role == "ping":
        def send_ping() -> None:
            log.append((round(context.loop.time(), 6), "send", "ping", peer))
            endpoint.transport.sendto(b"ping", peer)

        context.loop.call_at(5.0, send_ping)

    context.set_result(log)
