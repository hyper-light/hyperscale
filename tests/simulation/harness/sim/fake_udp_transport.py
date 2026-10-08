"""
``asyncio.DatagramTransport`` implementation backed by in-process
delivery.

Why a fake datagram transport
-----------------------------

UDP is symmetric: one ``DatagramTransport`` per process serves
both inbound and outbound traffic. ``MercurySyncBaseServer`` creates
a single global UDP transport in ``_start_udp_server`` and reuses
it for every send via
``transport.sendto(encrypted_bytes, peer_addr)``. The receive side
is the same transport's protocol — ``MercurySyncUDPProtocol`` —
whose synchronous ``datagram_received(bytes, sender_addr)`` callback
fires on every inbound datagram.

``FakeUDPTransport`` substitutes ``asyncio.DatagramTransport`` and
routes ``sendto`` through ``InProcessTransport``'s registry to the
target's protocol's ``datagram_received``. The framing, encryption,
rate-limiting, and dispatch in ``MercurySyncUDPProtocol`` run
unchanged — the only thing that changes is the byte-transit layer.

The sender address (the ``addr`` argument to ``datagram_received``)
is set from this transport's own ``sockname`` so receive-side code
that inspects the sender (e.g., the SWIM gossip "who told me"
identifier) sees the correct virtual address.

Contract
--------

``FakeUDPTransport`` satisfies the subset of
``asyncio.DatagramTransport`` that ``MercurySyncBaseServer`` and
``MercurySyncUDPProtocol`` use:

- ``sendto(bytes, addr)`` — looks up ``addr`` in the InProcessTransport
  registry, schedules ``peer_protocol.datagram_received(bytes,
  sender_addr)`` via ``loop.call_soon``.
- ``close()`` / ``is_closing()`` — same closing-state semantics
  as ``FakeTCPTransport``.
- ``get_extra_info(name, default=None)`` — supports ``peername``,
  ``sockname``, ``socket``.
- ``abort()`` — same as ``close``.

Methods that don't apply (write buffer limits, flow control) are
preserved as no-ops because ``asyncio.DatagramTransport`` inherits
from ``asyncio.BaseTransport`` and the codebase can call any of
them safely.
"""

import asyncio
from typing import Any, Callable


class FakeUDPTransport(asyncio.DatagramTransport):
    """In-process substitute for an asyncio UDP datagram transport.

    Construct with a router callback that, given ``(bytes, target_addr)``,
    schedules delivery to the right peer's ``datagram_received``.
    The router lives in ``InProcessTransport`` so the registry
    lookup is centralized — when a server registers/deregisters,
    its protocol becomes routable/unroutable atomically without
    every transport having to update its own state.
    """

    __slots__ = (
        "_loop",
        "_route",
        "_sockname",
        "_closing",
        "_protocol",
        "_extra",
    )

    def __init__(
        self,
        loop,
        route: Callable[[bytes, tuple[str, int], tuple[str, int]], None],
        sockname: tuple[str, int],
    ) -> None:
        super().__init__()
        self._loop = loop
        self._route = route
        self._sockname = sockname
        self._closing = False
        self._protocol: asyncio.DatagramProtocol | None = None
        self._extra: dict[str, Any] = {
            "peername": None,
            "sockname": sockname,
            "socket": None,
        }

    def set_protocol(self, protocol: asyncio.DatagramProtocol) -> None:
        """Register the UDP protocol associated with this transport."""
        self._protocol = protocol

    def get_protocol(self) -> asyncio.DatagramProtocol | None:
        """Return the UDP protocol associated with this transport."""
        return self._protocol

    def sendto(
        self,
        data: bytes,
        addr: tuple[str, int] | None = None,
    ) -> None:
        """Route ``data`` to the peer at ``addr``.

        ``addr=None`` is only valid for connected datagram sockets;
        SIM doesn't model that (every ``sendto`` carries a target).
        Mismatched: silently drop, matching kernel behavior on
        ``sendto`` to an unbound peer.
        """
        if self._closing:
            raise ConnectionError(
                f"sendto on closed FakeUDPTransport "
                f"(sockname={self._sockname})"
            )
        if addr is None:
            return
        # Hand to the router along with our own sockname so the
        # peer sees us as the sender.
        self._route(bytes(data), addr, self._sockname)

    def close(self) -> None:
        """Mark transport closed; schedule ``connection_lost`` on
        the associated protocol."""
        if self._closing:
            return
        self._closing = True
        if self._protocol is not None:
            self._loop.call_soon(self._protocol.connection_lost, None)

    def abort(self) -> None:
        """Forcibly close. Same as ``close`` in SIM."""
        self.close()

    def is_closing(self) -> bool:
        """Return True after close/abort."""
        return self._closing

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Return transport metadata."""
        return self._extra.get(name, default)

    def set_write_buffer_limits(
        self,
        high: int | None = None,
        low: int | None = None,
    ) -> None:
        """No-op."""

    def get_write_buffer_size(self) -> int:
        """Always 0."""
        return 0

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return ``(low, high)`` watermarks. Both 0."""
        return (0, 0)
