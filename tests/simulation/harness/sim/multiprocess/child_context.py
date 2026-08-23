"""
Child-process transport + context for multi-process SIM.

``CrossProcessTransport`` is the child-side counterpart of the parent's
``SimulationCoordinator``. It implements the full ``TransportFactory``
seam — ``register_datagram_endpoint``, ``register_stream_server``, and
``connect_stream`` — so the production UDP servers (``UDPProtocol`` /
``MercurySyncBaseServer``) *and* the production TCP side plug into it
unchanged. Instead of routing in local memory it *buffers* every
outbound event for the coordinator to route, and *injects*
coordinator-delivered events as absolute virtual-time timers on this
process's ``SimulationLoop``.

Wire payloads (opaque to the coordinator, which routes purely by
destination sockname) are tagged tuples:

- ``("dgram", data)`` — a datagram for the endpoint at the destination.
- ``("s-conn", connection_key)`` — a stream-connect request (TCP SYN
  analog) for the listener at the destination.
- ``("s-accept", connection_key)`` / ``("s-refuse", connection_key)`` —
  the listener's answer, routed back to the initiator.
- ``("s-data", connection_key, data)`` — stream bytes on an
  established connection.
- ``("s-close", connection_key)`` — connection teardown.

Stream semantics mirror real TCP over the lockstep boundary: a connect
costs one round trip (the initiator's ``connect_stream`` awaitable
resolves when the accept arrives, ``2 * latency`` later); dialing an
address that is hosted but has no listener is *refused* (RST analog);
dialing an address no process hosts hangs silently (SYN-to-nowhere
analog — production connect timeouts surface it). Connection keys are
``(initiator_sockname, index)`` — globally unique and deterministic.

Every address this process comes to host — datagram endpoints, stream
listeners, and stream-client socknames — is announced to the
coordinator incrementally (``drain_new_addresses``), so servers that
start *after* the readiness barrier (delayed starts, restarts) become
routable at the window in which they registered.
"""

from tests.simulation.harness.sim.fake_tcp_transport import FakeTCPTransport


class _BufferingDatagramTransport:
    """A fake ``asyncio.DatagramTransport`` whose ``sendto`` buffers.

    Each ``sendto(data, addr)`` records ``(send_time, src, addr,
    ("dgram", data))`` into the shared outbound list; the child loop
    drains that list at the end of every window and hands it to the
    coordinator.
    """

    __slots__ = ("_loop", "_src", "_outbound", "_closed")

    def __init__(self, loop, src_sockname, outbound: list) -> None:
        self._loop = loop
        self._src = src_sockname
        self._outbound = outbound
        self._closed = False

    def sendto(self, data: bytes, addr) -> None:
        if self._closed:
            return
        self._outbound.append(
            (self._loop.time(), self._src, addr, ("dgram", bytes(data)))
        )

    def get_extra_info(self, name, default=None):
        if name == "sockname":
            return self._src
        return default

    def is_closing(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True

    def abort(self) -> None:
        self._closed = True


class _CrossProcessStreamTransport(FakeTCPTransport):
    """``FakeTCPTransport`` whose byte-transit crosses the coordinator
    boundary.

    ``write`` inherits the parent behavior (defensive copy +
    ``call_soon``) with a ``deliver`` callback that buffers
    ``("s-data", connection_key, data)`` toward the peer; ``close``
    additionally notifies the peer (``("s-close", connection_key)``)
    before running the parent's local close (mark closing + schedule
    ``connection_lost``). ``close_from_peer`` is the receiving side of
    that notification — a local-only close that must not echo another
    ``s-close`` back.
    """

    __slots__ = ("_on_close",)

    def __init__(self, loop, deliver, peername, sockname, on_close) -> None:
        super().__init__(loop, deliver, peername, sockname)
        self._on_close = on_close

    def close(self) -> None:
        if self.is_closing():
            return
        self._on_close()
        super().close()

    def close_from_peer(self) -> None:
        """Close initiated by the peer's ``s-close``: local effects only."""
        super().close()


class _StreamConnection:
    """One side of an established cross-process stream connection."""

    __slots__ = ("protocol", "transport", "remote_sockname")

    def __init__(self, protocol, transport, remote_sockname) -> None:
        self.protocol = protocol
        self.transport = transport
        self.remote_sockname = remote_sockname


class CrossProcessTransport:
    """Per-process transport factory for the lockstep simulation.

    Construct with the process's ``SimulationLoop``. Production servers
    plug in via the ``TransportFactory`` seam methods
    (``register_datagram_endpoint`` / ``register_stream_server`` /
    ``connect_stream``); ``drain_outbound`` / ``drain_new_addresses`` /
    ``inject`` are the coordinator interface, driven by
    ``run_child_loop``.
    """

    def __init__(self, loop) -> None:
        self._loop = loop
        self._endpoints: dict[tuple, object] = {}
        self._stream_listeners: dict[tuple, object] = {}
        self._stream_connections: dict[tuple, _StreamConnection] = {}
        self._pending_connects: dict[tuple, tuple] = {}
        self._next_connection_index = 0
        self._outbound: list[tuple] = []
        self._spawn_requests: list[tuple] = []
        self._spawned_process_exitcodes: dict[str, int | None] = {}
        self._announced_addresses: set[tuple] = set()
        self._new_addresses: list[tuple] = []

    # -- production transport_factory seam -----------------------------

    def register_datagram_endpoint(self, sockname, protocol):
        """Register ``protocol`` at ``sockname``; return its send transport.

        Mirrors ``loop.create_datagram_endpoint``: fires
        ``connection_made(transport)`` before returning.
        """
        transport = _BufferingDatagramTransport(
            self._loop, sockname, self._outbound
        )
        self._endpoints[sockname] = protocol
        self._announce_address(sockname)
        protocol.connection_made(transport)
        return transport

    def register_stream_server(self, sockname, protocol_factory) -> None:
        """Register ``protocol_factory`` as the stream listener at
        ``sockname``.

        Mirrors ``loop.create_server``: a fresh protocol instance is
        built per accepted connection, receiving its transport via
        ``connection_made`` when a peer dials in.
        """
        self._stream_listeners[sockname] = protocol_factory
        self._announce_address(sockname)

    async def connect_stream(self, self_sockname, peer_sockname, protocol_factory):
        """Dial the stream listener at ``peer_sockname``.

        Buffers a connect request the coordinator delivers ``latency``
        later; the listener's accept (or refusal) travels back on the
        same boundary, so the returned awaitable resolves one full
        round trip after the call — at which point the client protocol
        is built, wired, and ``connection_made`` has fired. Raises
        ``ConnectionRefusedError`` when the peer process hosts the
        address but no listener is registered there (RST analog). A
        dial toward an address no process hosts never resolves — the
        caller's connect timeout surfaces it, matching a dropped SYN.
        """
        self._announce_address(self_sockname)
        connection_key = (self_sockname, self._next_connection_index)
        self._next_connection_index += 1

        waiter = self._loop.create_future()
        self._pending_connects[connection_key] = (
            waiter,
            protocol_factory,
            self_sockname,
            peer_sockname,
        )
        self._outbound.append(
            (
                self._loop.time(),
                self_sockname,
                peer_sockname,
                ("s-conn", connection_key),
            )
        )
        return await waiter

    # -- coordinator interface -----------------------------------------

    def addresses(self) -> tuple:
        """Socknames this process currently hosts (diagnostics)."""
        return tuple(self._announced_addresses)

    def drain_new_addresses(self) -> list:
        """Return and clear the addresses registered since last drain.

        The coordinator merges these into its route map at the window
        barrier — everything registered by virtual time T is routable
        at T.
        """
        new_addresses = self._new_addresses[:]
        self._new_addresses.clear()
        return new_addresses

    def inject(self, delivery_time, dst_sockname, src_addr, payload) -> None:
        """Schedule delivery of one wire event at ``delivery_time``
        (absolute virtual time)."""
        kind = payload[0]
        if kind == "dgram":
            protocol = self._endpoints.get(dst_sockname)
            if protocol is None:
                return  # closed-port semantics: drop
            self._loop.call_at(
                delivery_time, protocol.datagram_received, payload[1], src_addr
            )
            return

        self._loop.call_at(
            delivery_time, self._handle_stream_event, dst_sockname, src_addr, payload
        )

    def drain_outbound(self) -> list:
        """Return and clear this window's buffered outbound events."""
        out = self._outbound[:]
        self._outbound.clear()
        return out

    # -- production process_spawner seam --------------------------------

    def spawn_process(self, process_id: str, entry, *entry_args) -> None:
        """Buffer a request for the coordinator to admit a new child
        process running ``entry(child_context, *entry_args)``.

        This is the SIM implementation of the production
        ``ProcessSpawner`` seam (``LocalServerPool`` calls it to start
        its executors). ``entry`` and ``entry_args`` must be picklable —
        they cross the coordinator pipe and are re-imported by ``spawn``
        in the new process. The child is admitted at the next window
        barrier with its virtual clock initialized to global time.
        """
        self._spawn_requests.append((process_id, entry, tuple(entry_args)))
        self._spawned_process_exitcodes[process_id] = None

    def drain_spawn_requests(self) -> list:
        """Return and clear this window's buffered spawn requests."""
        requests = self._spawn_requests[:]
        self._spawn_requests.clear()
        return requests

    def record_process_exit(self, process_id: str, exitcode: int) -> None:
        """Mark a fault-injected death (coordinator process event).

        Only processes this context spawned are tracked — the exit-code
        snapshot mirrors what a real ``ProcessPoolExecutor`` owner sees:
        its own children, nobody else's.
        """
        if process_id in self._spawned_process_exitcodes:
            self._spawned_process_exitcodes[process_id] = exitcode

    def get_process_exitcodes(self) -> dict:
        """Exit-code snapshot of the processes this context spawned.

        ``None`` means still running — the same contract as
        ``LocalServerPool.get_process_exitcodes`` in REAL mode, so the
        worker's pool-health polling runs unchanged over it.
        """
        return dict(self._spawned_process_exitcodes)

    # -- internals -------------------------------------------------------

    def _announce_address(self, sockname) -> None:
        """Track ``sockname`` for the coordinator's route map (idempotent)."""
        if sockname in self._announced_addresses:
            return
        self._announced_addresses.add(sockname)
        self._new_addresses.append(sockname)

    def _buffer_stream_event(self, dst_sockname, src_sockname, payload) -> None:
        self._outbound.append(
            (self._loop.time(), src_sockname, dst_sockname, payload)
        )

    def _handle_stream_event(self, dst_sockname, src_addr, payload) -> None:
        """Process one delivered stream event at its virtual instant."""
        kind = payload[0]
        connection_key = payload[1]

        if kind == "s-conn":
            self._accept_stream_connect(dst_sockname, src_addr, connection_key)
        elif kind == "s-accept":
            self._resolve_stream_connect(connection_key)
        elif kind == "s-refuse":
            self._refuse_stream_connect(connection_key)
        elif kind == "s-data":
            connection = self._stream_connections.get(connection_key)
            if connection is None or connection.transport.is_closing():
                return  # torn down; matches bytes racing a close
            connection.protocol.data_received(payload[2])
        elif kind == "s-close":
            connection = self._stream_connections.pop(connection_key, None)
            if connection is not None:
                connection.transport.close_from_peer()

    def _accept_stream_connect(self, dst_sockname, src_addr, connection_key) -> None:
        """Listener side of a connect: build the server protocol pair.

        The accept is buffered *before* ``connection_made`` fires, so
        any bytes the server protocol writes on connect are sequenced
        after the accept and reach the initiator after its own side is
        wired.
        """
        protocol_factory = self._stream_listeners.get(dst_sockname)
        if protocol_factory is None:
            self._buffer_stream_event(
                src_addr, dst_sockname, ("s-refuse", connection_key)
            )
            return

        server_protocol = protocol_factory()
        server_transport = _CrossProcessStreamTransport(
            self._loop,
            deliver=lambda data: self._buffer_stream_event(
                src_addr, dst_sockname, ("s-data", connection_key, data)
            ),
            peername=src_addr,
            sockname=dst_sockname,
            on_close=lambda: self._teardown_connection(
                connection_key, src_addr, dst_sockname
            ),
        )
        server_transport.set_protocol(server_protocol)
        self._stream_connections[connection_key] = _StreamConnection(
            server_protocol, server_transport, src_addr
        )
        self._buffer_stream_event(
            src_addr, dst_sockname, ("s-accept", connection_key)
        )
        server_protocol.connection_made(server_transport)

    def _resolve_stream_connect(self, connection_key) -> None:
        """Initiator side of an accept: build the client protocol pair."""
        pending = self._pending_connects.pop(connection_key, None)
        if pending is None:
            return
        waiter, protocol_factory, self_sockname, peer_sockname = pending

        if waiter.done():
            # The caller abandoned the connect (timeout/cancel) before
            # the accept arrived; close the half-open peer side.
            self._buffer_stream_event(
                peer_sockname, self_sockname, ("s-close", connection_key)
            )
            return

        client_protocol = protocol_factory()
        client_transport = _CrossProcessStreamTransport(
            self._loop,
            deliver=lambda data: self._buffer_stream_event(
                peer_sockname, self_sockname, ("s-data", connection_key, data)
            ),
            peername=peer_sockname,
            sockname=self_sockname,
            on_close=lambda: self._teardown_connection(
                connection_key, peer_sockname, self_sockname
            ),
        )
        client_transport.set_protocol(client_protocol)
        self._stream_connections[connection_key] = _StreamConnection(
            client_protocol, client_transport, peer_sockname
        )
        client_protocol.connection_made(client_transport)
        waiter.set_result((client_transport, client_protocol))

    def _refuse_stream_connect(self, connection_key) -> None:
        pending = self._pending_connects.pop(connection_key, None)
        if pending is None:
            return
        waiter, _protocol_factory, _self_sockname, peer_sockname = pending
        if waiter.done():
            return
        waiter.set_exception(
            ConnectionRefusedError(
                f"no SIM stream listener at {peer_sockname}"
            )
        )

    def _teardown_connection(self, connection_key, remote_sockname, local_sockname) -> None:
        """Local ``close()``: notify the peer and drop the registry entry."""
        self._stream_connections.pop(connection_key, None)
        self._buffer_stream_event(
            remote_sockname, local_sockname, ("s-close", connection_key)
        )


class ChildContext:
    """Handed to a child entry function: the loop, the transport factory,
    the per-process virtual clock + seeded random, and a result slot
    returned to the coordinator at shutdown.

    A child entry registers its endpoints on ``transport``, schedules its
    initial behavior on ``loop`` (via ``call_at`` / ``create_task``), and
    optionally calls ``set_result`` with a value (often a live-mutated log
    or a server handle's state) to be collected when the run ends.
    ``sim_kwargs()`` mirrors ``SimulationRuntime.sim_kwargs`` so node
    servers construct identically in single- and multi-process SIM.

    Implements the production ``SimulationChildContext`` /
    ``ProcessSpawner`` Protocols: ``spawn_process`` (delegated to the
    transport's buffer) lets production code running inside this child —
    ``LocalServerPool`` above all — request further coordinator children.
    """

    __slots__ = (
        "loop",
        "transport",
        "clock",
        "random",
        "filesystem",
        "_result",
    )

    def __init__(
        self,
        loop,
        transport: CrossProcessTransport,
        clock,
        random_source,
        filesystem=None,
    ) -> None:
        self.loop = loop
        self.transport = transport
        self.clock = clock
        self.random = random_source
        # This child's in-memory disk (SimFilesystem) — entries schedule
        # storage-fault knob toggles on it at virtual instants.
        self.filesystem = filesystem
        self._result = None

    def sim_kwargs(self) -> dict:
        """The DI kwargs to spread into a node server's ``__init__``."""
        return {
            "clock": self.clock,
            "random_source": self.random,
            "transport_factory": self.transport,
        }

    def spawn_process(self, process_id: str, entry, *entry_args) -> None:
        """Request a new coordinator child (see
        ``CrossProcessTransport.spawn_process``)."""
        self.transport.spawn_process(process_id, entry, *entry_args)

    def get_process_exitcodes(self) -> dict:
        """Exit-code snapshot of the children this context spawned (see
        ``CrossProcessTransport.get_process_exitcodes``)."""
        return self.transport.get_process_exitcodes()

    def set_result(self, value) -> None:
        self._result = value

    @property
    def result(self):
        return self._result
