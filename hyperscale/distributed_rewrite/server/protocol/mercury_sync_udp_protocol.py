import asyncio
import re
from typing import TypeVar, Generic
from typing import Literal, Tuple
from .flow_control import FlowControl
from .server_state import ServerState
from .utils import (
    get_local_addr,
    get_remote_addr,
    is_ssl,
)
from .abstract_connection import AbstractConnection
from .receive_buffer import ReceiveBuffer


T = TypeVar("T", bound=AbstractConnection)

class MercurySyncUDPProtocol(asyncio.DatagramProtocol, Generic[T]):
    def __init__(
        self,
        conn: T,
        mode: Literal['client', 'server'] = 'client',
    ):
        super().__init__()
        self.transport: asyncio.Transport = None
        self.loop = asyncio.get_event_loop()
        self.conn = conn
        self.flow: FlowControl = None
        self.server_state = ServerState[MercurySyncUDPProtocol]()
        self.connections = self.server_state.connections
        self.mode: Literal['client', 'server']  = mode

        self.on_con_lost = self.loop.create_future()
        self.server: tuple[str, int] | None = None
        self.client: tuple[str, int] | None = None
        self.scheme: Literal["mudps", "mudp"] | None = None
        self.timeout_keep_alive_task: asyncio.TimerHandle | None = None

        self._receive_buffer = ReceiveBuffer()
        self._receive_buffer_closed = False
        self._active_requests: dict[bytes, bytes] = {}
        self._next_data: asyncio.Future = asyncio.Future()

        self.read = self.conn.read_server_udp
        if self.mode == 'client':
            self.read = self.conn.read_client_udp

    @property
    def trailing_data(self) -> tuple[bytes, bool]:
        """Data that has been received, but not yet processed, represented as
        a tuple with two elements, where the first is a byte-string containing
        the unprocessed data itself, and the second is a bool that is True if
        the receive connection was closed.

        See :ref:`switching-protocols` for discussion of why you'd want this.
        """
        return (bytes(self._receive_buffer), self._receive_buffer_closed)

    def connection_made(self, transport: asyncio.Transport):
        self.connections.add(self)
        self.transport = transport
        self.flow = FlowControl(transport)
        self.server = get_local_addr(transport)
        self.client = get_remote_addr(transport)
        self.scheme = "mudps" if is_ssl(transport) else "mudp"

    def datagram_received(self, data: bytes, addr: Tuple[str, int]) -> None:
        if data:
            if self._receive_buffer_closed:
                raise RuntimeError("received close, then received more data?")
            self._receive_buffer += data
        else:
            self._receive_buffer_closed = True

        if (
            self.conn.waiting_for_data.is_set() is False or self._next_data.done()
        ):
            self.read(
                self._receive_buffer,
                self.transport,
                self._next_data,
            )
            
        else:
            self._next_data.set_result(self._receive_buffer)

    def connection_lost(self, exc: Exception | None) -> None:
        self.connections.discard(self)

        if self.flow is not None:
            self.flow.resume_writing()

        if exc is None:
            self.transport.close()
            # self._unset_keepalive_if_required()

        self.on_con_lost.set_result(True)

    def eof_received(self) -> None:
        pass

    def on_response_complete(self) -> None:
        self.server_state.total_requests += 1

        if self.transport.is_closing():
            return

        # Set a short Keep-Alive timeout.
        # self._unset_keepalive_if_required()

        # self.timeout_keep_alive_task = self.loop.call_later(
        #     self.timeout_keep_alive, self.timeout_keep_alive_handler
        # )

        # Unpause data reads if needed.
        self.flow.resume_reading()

    def shutdown(self) -> None:
        """
        Called by the server to commence a graceful shutdown.
        """
        pass

    def pause_writing(self) -> None:
        """
        Called by the transport when the write buffer exceeds the high water mark.
        """
        self.flow.pause_writing()  # pragma: full coverage

    def resume_writing(self) -> None:
        """
        Called by the transport when the write buffer drops below the low water mark.
        """
        self.flow.resume_writing()  # pragma: full coverage
