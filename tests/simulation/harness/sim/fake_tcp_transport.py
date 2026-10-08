"""
``asyncio.Transport`` implementation backed by in-process delivery.

Why a fake transport
--------------------

The Phase 6 high-fidelity goal: SIM exercises the same production
code paths as REAL. ``MercurySyncBaseServer.send_tcp`` does the
encoding, compression, encryption, and length-prefix framing on
every send, then calls ``transport.write(framed_bytes)``. In REAL
that transport is the asyncio socket transport returned by
``loop.create_connection``. In SIM, ``FakeTCPTransport`` substitutes
— same ``asyncio.Transport`` interface, same ``write(bytes)`` call
site — but the bytes route through ``InProcessTransport``'s
registry to the peer's ``MercurySyncTCPProtocol.data_received``
rather than through a kernel socket.

The peer-side protocol code is **completely unchanged**. The
framing parser, decryption, dispatch table, response queue
management, semaphore handling — every layer that exists in REAL
runs in SIM exactly as today. The only thing that changes is the
byte-transit layer.

Contract
--------

``FakeTCPTransport`` satisfies the subset of ``asyncio.Transport``
that ``MercurySyncBaseServer`` and ``MercurySyncTCPProtocol``
actually use:

- ``write(bytes)`` — schedules ``peer_data_received(bytes)`` via
  ``loop.call_soon`` so delivery happens on the next loop tick,
  matching asyncio's ordering contract.
- ``close()`` / ``is_closing()`` — closing state; subsequent
  ``write`` calls raise ``ConnectionError``.
- ``get_extra_info(name, default=None)`` — supports ``peername``,
  ``sockname``, ``socket`` (returns ``None``), and ``compression``
  (returns ``None``).
- ``write_eof()`` / ``can_write_eof()`` — implemented but
  ``write_eof`` is a no-op (in-process has no half-close); the
  return of ``can_write_eof`` is ``False`` so callers that
  conditionally use it skip the path.
- ``set_write_buffer_limits()`` / ``get_write_buffer_size()`` —
  no-op / always-zero; in-process delivery is synchronous so
  backpressure is irrelevant.
- ``pause_reading()`` / ``resume_reading()`` — no-op; flow control
  is unnecessary in SIM.

Methods that don't apply (TLS handshake, abort, SSL extra info)
either no-op or raise ``NotImplementedError`` with a clear
message identifying the gap if a production code path reaches for
them.
"""

import asyncio
from typing import Any, Callable


class FakeTCPTransport(asyncio.Transport):
    """In-process substitute for an asyncio TCP transport.

    Construct with the deliver callback (a function that receives
    ``bytes`` and forwards them to the peer's protocol), the
    ``peername`` (peer's address), the ``sockname`` (our address),
    and the loop reference used to schedule deliveries.

    Internal state: ``_closing`` flag, an extras dict for
    ``get_extra_info`` answers, and a reference back to the
    associated protocol (set after ``connection_made``) so
    ``close`` can call ``protocol.connection_lost(None)``.
    """

    __slots__ = (
        "_loop",
        "_deliver",
        "_peername",
        "_sockname",
        "_closing",
        "_protocol",
        "_partner",
        "_extra",
    )

    def __init__(
        self,
        loop,
        deliver: Callable[[bytes], None],
        peername: tuple[str, int],
        sockname: tuple[str, int],
    ) -> None:
        super().__init__()
        self._loop = loop
        self._deliver = deliver
        self._peername = peername
        self._sockname = sockname
        self._closing = False
        self._protocol: asyncio.Protocol | None = None
        # The other end of the connection, closed when this end closes.
        self._partner: "FakeTCPTransport | None" = None
        self._extra: dict[str, Any] = {
            "peername": peername,
            "sockname": sockname,
            "socket": None,
            "compression": None,
            "ssl_object": None,
            "peercert": None,
        }

    def set_protocol(self, protocol: asyncio.Protocol) -> None:
        """Register the protocol associated with this transport.

        ``close`` uses this to call ``protocol.connection_lost``
        once delivery is drained. ``MercurySyncTCPProtocol``
        constructs the transport-protocol pair via
        ``connection_made``; this method lets the InProcessTransport
        wire the back-reference after construction.
        """
        self._protocol = protocol

    def set_partner(self, partner: "FakeTCPTransport") -> None:
        """Register the other end of the connection: closing this end
        closes it, as a REAL close or reset reaches the peer."""
        self._partner = partner

    def get_protocol(self) -> asyncio.Protocol | None:
        """Return the protocol associated with this transport."""
        return self._protocol

    def write(self, data: bytes) -> None:
        """Schedule ``data`` for delivery to the peer's protocol.

        Uses ``loop.call_soon`` so delivery happens at the start of
        the next loop iteration — matching asyncio's contract that
        ``transport.write(...)`` returns synchronously and the
        recipient sees the bytes asynchronously.
        """
        if self._closing:
            raise ConnectionError(
                f"write to closed FakeTCPTransport (peer={self._peername})"
            )
        # ``bytes(data)`` defensively copies so the caller can mutate
        # its buffer after returning. matches the asyncio Transport
        # contract.
        self._loop.call_soon(self._deliver, bytes(data))

    def writelines(self, list_of_data) -> None:
        """Schedule a sequence of byte chunks for delivery.

        asyncio's default implementation concatenates and calls
        ``write`` once; mirroring that here keeps the delivery
        ordering consistent with REAL.
        """
        self.write(b"".join(list_of_data))

    def write_eof(self) -> None:
        """No-op. In-process connections have no half-close."""

    def can_write_eof(self) -> bool:
        """Always False. SIM doesn't model half-close."""
        return False

    def close(self) -> None:
        """Mark transport closed; schedule ``connection_lost`` on
        the associated protocol so callers see the standard
        transport-close semantics.
        """
        if self._closing:
            return
        self._closing = True
        if self._protocol is not None:
            self._loop.call_soon(self._protocol.connection_lost, None)
        if self._partner is not None:
            self._loop.call_soon(self._partner.close)

    def abort(self) -> None:
        """Forcibly close. Same semantics as ``close`` for SIM —
        no kernel buffer to discard."""
        self.close()

    def is_closing(self) -> bool:
        """Return True after ``close`` / ``abort`` has been called."""
        return self._closing

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Return transport metadata.

        Supports ``peername``, ``sockname``, ``socket`` (None),
        ``compression`` (None), ``ssl_object`` (None), ``peercert``
        (None). Anything else returns ``default`` — production
        code that reads unsupported keys gets the same
        ``None``-or-default behavior as REAL would for an
        unsupported asyncio transport.
        """
        return self._extra.get(name, default)

    def set_write_buffer_limits(
        self,
        high: int | None = None,
        low: int | None = None,
    ) -> None:
        """No-op. In-process delivery has no buffer to limit."""

    def get_write_buffer_size(self) -> int:
        """Always 0. In-process delivery is synchronous within a
        loop iteration; nothing is buffered between iterations."""
        return 0

    def get_write_buffer_limits(self) -> tuple[int, int]:
        """Return ``(low, high)`` watermarks. Both 0 in SIM."""
        return (0, 0)

    def pause_reading(self) -> None:
        """No-op. SIM doesn't model receive-side flow control."""

    def resume_reading(self) -> None:
        """No-op. SIM doesn't model receive-side flow control."""

    def is_reading(self) -> bool:
        """Always True. SIM never pauses reads."""
        return not self._closing
