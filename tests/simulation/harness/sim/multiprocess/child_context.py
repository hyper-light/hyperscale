"""
Child-process transport + context for multi-process SIM.

``CrossProcessTransport`` is the child-side counterpart of the parent's
``SimulationCoordinator``. It implements the same
``register_datagram_endpoint`` seam as the single-process
``InProcessTransport`` — so the production UDP servers (both the
``MercurySyncBaseServer`` SWIM side and the ``UDPProtocol`` worker-pool
side) plug into it unchanged — but instead of routing datagrams in local
memory it *buffers* every outbound ``sendto`` for the coordinator to
route, and *injects* coordinator-delivered datagrams as absolute
virtual-time timers on this process's ``SimulationLoop``.

Only ``register_datagram_endpoint`` is needed here: SWIM membership and
the worker pool are both datagram protocols, and datagram delivery is
what the lockstep boundary carries. (Stream/TCP transport across the
process boundary is a later addition.)
"""


class _BufferingDatagramTransport:
    """A fake ``asyncio.DatagramTransport`` whose ``sendto`` buffers.

    Each ``sendto(data, addr)`` records ``(send_time, src, addr, data)``
    into the shared outbound list; the child loop drains that list at the
    end of every window and hands it to the coordinator.
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
        self._outbound.append((self._loop.time(), self._src, addr, bytes(data)))

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


class CrossProcessTransport:
    """Per-process datagram transport for the lockstep simulation.

    Construct with the process's ``SimulationLoop``. Register each server
    endpoint via ``register_datagram_endpoint`` (fires ``connection_made``
    to match ``loop.create_datagram_endpoint``). ``drain_outbound`` /
    ``inject`` are the coordinator interface, driven by ``run_child_loop``.
    """

    def __init__(self, loop) -> None:
        self._loop = loop
        self._endpoints: dict[tuple, object] = {}
        self._outbound: list[tuple] = []

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
        protocol.connection_made(transport)
        return transport

    # -- coordinator interface -----------------------------------------

    def addresses(self) -> tuple:
        """Socknames this process hosts (for the coordinator's route map)."""
        return tuple(self._endpoints.keys())

    def inject(self, delivery_time, dst_sockname, src_addr, data) -> None:
        """Schedule delivery of ``data`` to a local endpoint at
        ``delivery_time`` (absolute virtual time)."""
        protocol = self._endpoints.get(dst_sockname)
        if protocol is None:
            return  # closed-port semantics: drop
        self._loop.call_at(
            delivery_time, protocol.datagram_received, data, src_addr
        )

    def drain_outbound(self) -> list:
        """Return and clear this window's buffered outbound datagrams."""
        out = self._outbound[:]
        self._outbound.clear()
        return out


class ChildContext:
    """Handed to a child entry function: the loop, the transport, and a
    result slot returned to the coordinator at shutdown.

    A child entry registers its endpoints on ``transport``, schedules its
    initial behavior on ``loop`` (via ``call_at`` / ``create_task``), and
    optionally calls ``set_result`` with a value (often a live-mutated log
    or a server handle's state) to be collected when the run ends.
    """

    __slots__ = ("loop", "transport", "_result")

    def __init__(self, loop, transport: CrossProcessTransport) -> None:
        self.loop = loop
        self.transport = transport
        self._result = None

    def set_result(self, value) -> None:
        self._result = value

    @property
    def result(self):
        return self._result
