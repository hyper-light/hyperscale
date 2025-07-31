import asyncio
import socket
from types import TracebackType
from typing import Any, Awaitable, Callable, Dict, Optional
from typing import Type, cast, Self



SSHForwarderCoro = Callable[..., Awaitable]


class SSHForwarder(asyncio.BaseProtocol):
    """SSH port forwarding connection handler"""

    def __init__(self, peer: Optional['SSHForwarder'] = None,
                 extra: Optional[Dict[str, Any]] = None):
        self._peer = peer
        self._transport: Optional[asyncio.Transport] = None
        self._inpbuf = b''
        self._eof_received = False

        if peer:
            peer.set_peer(self)

        if extra is None:
            extra = {}

        self._extra = extra

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, _exc_type: Optional[Type[BaseException]],
                        _exc_value: Optional[BaseException],
                        _traceback: Optional[TracebackType]) -> bool:
        self.close()
        return False

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get additional information about the forwarder

           This method returns extra information about the forwarder.
           Currently, the only information available is the value
           ``interface`` for TUN/TAP forwarders, returning the name of the
           local TUN/TAP network interface created for this forwarder.

        """

        return self._extra.get(name, default)

    def set_peer(self, peer: 'SSHForwarder') -> None:
        """Set the peer forwarder to exchange data with"""

        self._peer = peer

    def write(self, data: bytes) -> None:
        """Write data to the transport"""

        assert self._transport is not None
        self._transport.write(data)

    def write_eof(self) -> None:
        """Write end of file to the transport"""

        assert self._transport is not None

        try:
            self._transport.write_eof()
        except OSError: # pragma: no cover
            pass

    def was_eof_received(self) -> bool:
        """Return whether end of file has been received or not"""

        return self._eof_received

    def pause_reading(self) -> None:
        """Pause reading from the transport"""

        assert self._transport is not None
        self._transport.pause_reading()

    def resume_reading(self) -> None:
        """Resume reading on the transport"""

        assert self._transport is not None
        self._transport.resume_reading()

    def connection_made(self, transport: asyncio.BaseTransport) -> None:
        """Handle a newly opened connection"""

        self._transport = cast(Optional['asyncio.Transport'], transport)

        sock = cast(socket.socket, transport.get_extra_info('socket'))

        if sock and sock.family in {socket.AF_INET, socket.AF_INET6}:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    def connection_lost(self, exc: Optional[Exception]) -> None:
        """Handle an incoming connection close"""

        # pylint: disable=unused-argument

        self.close()

    def session_started(self) -> None:
        """Handle session start"""

    def data_received(self, data: bytes,
                      datatype: Optional[int] = None) -> None:
        """Handle incoming data from the transport"""

        # pylint: disable=unused-argument

        if self._peer:
            self._peer.write(data)
        else:
            self._inpbuf += data

    def eof_received(self) -> bool:
        """Handle an incoming end of file from the transport"""

        self._eof_received = True

        if self._peer:
            self._peer.write_eof()

            return not self._peer.was_eof_received()
        else:
            return True

    def pause_writing(self) -> None:
        """Pause writing by asking peer to pause reading"""

        if self._peer: # pragma: no branch
            self._peer.pause_reading()

    def resume_writing(self) -> None:
        """Resume writing by asking peer to resume reading"""

        if self._peer: # pragma: no branch
            self._peer.resume_reading()

    def close(self) -> None:
        """Close this port forwarder"""

        if self._transport:
            self._transport.close()
            self._transport = None

        if self._peer:
            peer = self._peer
            self._peer = None
            peer.close()
