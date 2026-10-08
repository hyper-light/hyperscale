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


def spawning_parent_entry(
    context, address, child_process_id, child_address, spawn_at
) -> None:
    """Child entry: echo endpoint in the ``pong`` role that, at virtual
    time ``spawn_at``, requests admission of a ``late_child_entry``
    process — exercising the coordinator's dynamic-spawn path."""
    log: list = []
    endpoint = _EchoEndpoint(context.loop, "pong", log)
    context.transport.register_datagram_endpoint(address, endpoint)

    context.loop.call_at(
        spawn_at,
        context.spawn_process,
        child_process_id,
        late_child_entry,
        child_address,
        address,
    )

    context.set_result(log)


def repeating_ping_entry(context, address, peer, interval) -> None:
    """Child entry: send ``b"ping"`` to ``peer`` every ``interval``
    virtual seconds, forever — the immortal heartbeat a kill test cuts
    short (without a kill this process never quiesces)."""
    log: list = []
    endpoint = _EchoEndpoint(context.loop, "ping", log)
    context.transport.register_datagram_endpoint(address, endpoint)

    def send_ping() -> None:
        log.append((round(context.loop.time(), 6), "send", "ping", peer))
        endpoint.transport.sendto(b"ping", peer)
        context.loop.call_later(interval, send_ping)

    context.loop.call_later(interval, send_ping)
    context.set_result(log)


def late_child_entry(context, address, peer) -> None:
    """Dynamically admitted child: records its (inherited) start time,
    pings ``peer`` immediately, and records the reply's arrival time —
    proving a mid-run process joins at global virtual time and exchanges
    messages coherently in both directions."""
    log: list = [("started", round(context.loop.time(), 6))]
    endpoint = _EchoEndpoint(context.loop, "ping", log)
    context.transport.register_datagram_endpoint(address, endpoint)

    def send_ping() -> None:
        log.append((round(context.loop.time(), 6), "send", "ping", peer))
        endpoint.transport.sendto(b"ping", peer)

    context.loop.call_soon(send_ping)
    context.set_result(log)
