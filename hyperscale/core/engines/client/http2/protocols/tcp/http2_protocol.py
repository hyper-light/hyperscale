from asyncio import AbstractEventLoop, BufferedProtocol, Future, Transport
from typing import Optional

from hyperscale.core.engines.client.shared.protocols.flow_control_mixin import FlowControlMixin

# A TLS record's plaintext at most (RFC 8446 5.1): the free space a read is
# given before room is made for it.
RECORD_SIZE = 16384

# Room for the largest frame we accept -- the 16,384 bytes we advertise as
# SETTINGS_MAX_FRAME_SIZE and its 9-byte header (RFC 9113 4.2); the pipe
# fails a larger one on its header -- held partly received, beside a record.
RECEIVE_BUFFER_SIZE = 65536


class HTTP2Protocol(FlowControlMixin, BufferedProtocol):
    """
    An HTTP/2 connection's protocol and the reader of what it receives: the
    transport reads -- decrypts, over TLS -- straight into the free space of
    a buffer this owns for the transport's lifetime, and the pipe parses the
    frames in place, from ``_start`` up to ``_end``. Nothing received is
    copied into a new object first, and the buffer is never refilled.

    Room is made by moving the unparsed bytes to the front, so nothing may
    hold a view of the buffer past the synchronous parse that took it. Once
    the buffer is full, reading waits for the pipe to parse.
    """

    __slots__ = (
        "_transport",
        "_buffer",
        "_view",
        "_start",
        "_end",
        "_eof",
        "_exception",
        "_waiter",
        "_reading_paused",
    )

    def __init__(self, loop: Optional[AbstractEventLoop] = None):
        super().__init__(loop=loop)
        self._transport: Optional[Transport] = None
        self._buffer = bytearray(RECEIVE_BUFFER_SIZE)
        # Held for the buffer's lifetime: a read into an empty buffer is
        # handed this view itself, and it keeps the buffer from resizing.
        self._view = memoryview(self._buffer)
        self._start = 0
        self._end = 0
        self._eof = False
        self._exception: Optional[BaseException] = None
        self._waiter: Optional[Future] = None
        self._reading_paused = False

    def connection_made(self, transport: Transport):
        self._transport = transport

    def connection_lost(self, exc: Optional[BaseException]):
        super().connection_lost(exc)

        if exc is None:
            self._eof = True

        else:
            self._exception = exc

        self._reading_paused = False
        self._transport = None

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                if exc is None:
                    waiter.set_result(None)

                else:
                    waiter.set_exception(exc)

    def eof_received(self) -> bool:
        self._eof = True

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

        # The peer sends nothing more, so the connection is done: the
        # transport closes (over TLS, True would only draw a warning).
        return False

    def get_buffer(self, sizehint: int) -> memoryview:
        if (end := self._end) == 0:
            return self._view

        if RECEIVE_BUFFER_SIZE - end < RECORD_SIZE and (start := self._start):
            # Room for a record: the unparsed bytes -- at most a frame not yet
            # whole -- move to the front.
            unparsed = end - start
            self._buffer[:unparsed] = self._buffer[start:end]
            self._start = 0
            self._end = end = unparsed

        return self._view[end:]

    def buffer_updated(self, nbytes: int):
        self._end = end = self._end + nbytes

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_result(None)

        if end == RECEIVE_BUFFER_SIZE:
            # Full: reading waits until the pipe parses and resumes it, or
            # waits for more and data_waiter() does.
            self._reading_paused = True
            self._transport.pause_reading()

    def exception(self) -> Optional[BaseException]:
        return self._exception

    def data_waiter(self) -> Future:
        """
        A future that completes when data, the end of the stream, or the
        transport's error (raised by awaiting it) arrives. One reader has one
        waiter at a time, as StreamReader allows.
        """
        if (waiter := self._waiter) is not None and not waiter.done():
            raise RuntimeError(
                "data_waiter() called while another coroutine is already waiting for incoming data"
            )

        if self._reading_paused:
            # Waiting while paused would deadlock.
            self._reading_paused = False
            self._transport.resume_reading()

        self._waiter = waiter = self._loop.create_future()
        return waiter
