import asyncio
import collections
import socket
import ssl
import warnings
from asyncio import constants, trsock
from asyncio.log import logger
from asyncio.sslproto import AppProtocolState, SSLProtocolState, add_flowcontrol_defaults
from typing import Any, Callable, Deque

# Ciphertext one socket read can take: asyncio's SSLProtocol read size.
RECEIVE_BUFFER_SIZE = 256 * 1024
# Plaintext one SSLObject.read() returns at most: a whole TLS record
# (RFC 8446 5.1). asyncio reads 256 KiB at a time, allocating that much for
# every read before shrinking it to the record.
PLAINTEXT_READ_SIZE = 16 * 1024

# What asyncio's SSLProtocol treats as "no more data for now".
_SSL_AGAIN_ERRORS = (ssl.SSLWantReadError, ssl.SSLSyscallError)
_SOCKET_AGAIN_ERRORS = (BlockingIOError, InterruptedError)
_BYTES_LIKE = (bytes, bytearray, memoryview)

_DO_HANDSHAKE = SSLProtocolState.DO_HANDSHAKE
_WRAPPED = SSLProtocolState.WRAPPED
_FLUSHING = SSLProtocolState.FLUSHING
_SHUTDOWN = SSLProtocolState.SHUTDOWN
_UNWRAPPED = SSLProtocolState.UNWRAPPED
_READING_STATES = (_WRAPPED, _FLUSHING)
_CLOSING_STATES = (_FLUSHING, _SHUTDOWN, _UNWRAPPED)

_APP_CONNECTION_MADE = AppProtocolState.STATE_CON_MADE
_APP_EOF = AppProtocolState.STATE_EOF
_APP_CONNECTION_LOST = AppProtocolState.STATE_CON_LOST


def _ignore_readiness() -> None:
    pass


def supports_readiness_callbacks(
    loop: asyncio.AbstractEventLoop,
    connected_socket: socket.socket,
) -> bool:
    """
    Whether ``loop`` can watch ``connected_socket`` for readiness
    (``add_reader``), which TLSTransport needs: selector loops can, the
    proactor loop cannot.
    """
    try:
        loop.add_reader(connected_socket.fileno(), _ignore_readiness)

    except NotImplementedError:
        return False

    loop.remove_reader(connected_socket.fileno())
    return True


async def open_tls_transport(
    loop: asyncio.AbstractEventLoop,
    connected_socket: socket.socket,
    ssl_context: ssl.SSLContext,
    server_hostname: str | None,
    protocol: asyncio.BaseProtocol,
    receive_buffer: memoryview,
    handshake_timeout: float | None = constants.SSL_HANDSHAKE_TIMEOUT,
) -> "TLSTransport":
    """
    TLS client over ``connected_socket`` -- connected and non-blocking --
    returning its TLSTransport once the handshake completes, with
    ``protocol.connection_made`` called: what
    ``loop.create_connection(ssl=..., sock=...)`` does, without asyncio's
    SSLProtocol and socket transport layers. The loop must support
    readiness callbacks (see ``supports_readiness_callbacks``).

    The transport runs the handshake from its own socket reads
    (``TLSTransport.begin_handshake``): the socket is registered for
    reading once, for the handshake and the connection after it, and the
    client's last handshake flight leaves with the caller's first write.

    ``receive_buffer`` takes each socket read before it is handed to
    OpenSSL; it is used only within one synchronous read callback, so the
    transports on one event loop may share it. The handshake fails as
    asyncio's does: a ConnectionAbortedError past ``handshake_timeout``, a
    ConnectionResetError if the peer closes, or the SSLError itself. On any
    failure, or if the caller is cancelled, the socket is closed.
    """
    incoming = ssl.MemoryBIO()
    outgoing = ssl.MemoryBIO()

    try:
        ssl_object = ssl_context.wrap_bio(
            incoming,
            outgoing,
            server_side=False,
            server_hostname=server_hostname,
        )

        transport = TLSTransport(
            loop,
            connected_socket,
            ssl_context,
            ssl_object,
            incoming,
            outgoing,
            receive_buffer,
            protocol,
        )

    except BaseException:
        connected_socket.close()
        raise

    handshake_complete = loop.create_future()

    try:
        transport.begin_handshake(handshake_complete, handshake_timeout)
        await handshake_complete

    except BaseException as error:
        transport._force_close(error)
        raise

    return transport


class TLSTransport(asyncio.Transport):
    """
    A client TLS transport over a connected, non-blocking socket: asyncio's
    SSLProtocol, its app transport, and the socket transport beneath them,
    collapsed into one layer with the same behavior.

    Each readable event reads the socket once into a reused buffer and hands
    the ciphertext to OpenSSL through a memory BIO, as asyncio does; each
    write encrypts and sends the ciphertext straight to the socket, queueing
    only what the socket does not take. OpenSSL's C methods are called
    directly, without ssl.SSLObject's Python wrappers.

    Everything the application sees follows asyncio's SSL transport: the
    WRAPPED -> FLUSHING -> SHUTDOWN -> UNWRAPPED states; a peer's
    close_notify or TCP close as end of stream (``eof_received`` once, then
    ``connection_lost(None)``), with plaintext already received delivered
    first; close() sending close_notify and waiting for the peer's, within
    the SSL shutdown timeout; read and write flow control with asyncio's
    limits; writes after close dropped with its warning; protocol errors as
    fatal errors; and BufferedProtocol support. One difference: the shutdown
    timeout runs until the socket is closed, where asyncio's stops once
    close_notify is queued and can leave a never-drained socket open.
    """

    _start_tls_compatible = True
    _sendfile_compatible = constants._SendfileMode.FALLBACK

    __slots__ = (
        "_loop",
        "_socket",
        "_fileno",
        "_ssl_object",
        "_ssl_read",
        "_ssl_write",
        "_ssl_pending",
        "_incoming",
        "_outgoing",
        "_receive_view",
        "_app_protocol",
        "_app_protocol_is_buffer",
        "_app_protocol_get_buffer",
        "_app_protocol_buffer_updated",
        "_state",
        "_app_state",
        "_closed",
        "_eof_received",
        "_app_reading_paused",
        "_ssl_reading_paused",
        "_socket_reading",
        "_socket_closing",
        "_connection_lost",
        "_conn_lost_writes",
        "_write_backlog",
        "_write_buffer_size",
        "_ciphertext_backlog",
        "_ciphertext_size",
        "_socket_writing",
        "_app_writing_paused",
        "_outgoing_high_water",
        "_outgoing_low_water",
        "_incoming_high_water",
        "_incoming_low_water",
        "_shutdown_timeout_handle",
        "_handshake_waiter",
        "_handshake_timeout_handle",
    )

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        connected_socket: socket.socket,
        ssl_context: ssl.SSLContext,
        ssl_object: ssl.SSLObject,
        incoming: ssl.MemoryBIO,
        outgoing: ssl.MemoryBIO,
        receive_buffer: memoryview,
        protocol: asyncio.BaseProtocol,
    ) -> None:
        # The peer's certificate, cipher and compression join these once the
        # handshake completes: before then getpeercert() raises.
        super().__init__(
            {
                "socket": trsock.TransportSocket(connected_socket),
                "sockname": _socket_address(connected_socket.getsockname),
                "peername": _socket_address(connected_socket.getpeername),
                "sslcontext": ssl_context,
                "ssl_object": ssl_object,
            }
        )
        self._loop = loop
        self._socket = connected_socket
        self._fileno = connected_socket.fileno()
        self._ssl_object = ssl_object

        # ssl.SSLObject's read, write, and pending only forward to the C
        # object it wraps; call that directly where it exists.
        native_ssl_object = getattr(ssl_object, "_sslobj", ssl_object)
        self._ssl_read: Callable[..., Any] = native_ssl_object.read
        self._ssl_write: Callable[[Any], int] = native_ssl_object.write
        self._ssl_pending: Callable[[], int] = native_ssl_object.pending

        self._incoming = incoming
        self._outgoing = outgoing
        self._receive_view = receive_buffer

        self._set_app_protocol(protocol)

        self._state = _WRAPPED
        self._app_state = AppProtocolState.STATE_INIT
        self._closed = False
        self._eof_received = False
        self._app_reading_paused = False
        self._ssl_reading_paused = False
        self._socket_reading = False
        self._socket_closing = False
        self._connection_lost = False
        self._conn_lost_writes = 0

        # Plaintext waits only while a TLS 1.2 renegotiation needs the
        # peer's answer; ciphertext waits for the socket, in order.
        self._write_backlog: Deque[bytes] = collections.deque()
        self._write_buffer_size = 0
        self._ciphertext_backlog: Deque[memoryview] = collections.deque()
        self._ciphertext_size = 0
        self._socket_writing = False
        self._app_writing_paused = False

        self._outgoing_high_water, self._outgoing_low_water = add_flowcontrol_defaults(
            None, None, constants.FLOW_CONTROL_HIGH_WATER_SSL_WRITE
        )
        self._incoming_high_water, self._incoming_low_water = add_flowcontrol_defaults(
            None, None, constants.FLOW_CONTROL_HIGH_WATER_SSL_READ
        )
        self._shutdown_timeout_handle: asyncio.TimerHandle | None = None
        self._handshake_waiter: asyncio.Future | None = None
        self._handshake_timeout_handle: asyncio.TimerHandle | None = None

    # --- handshake ---------------------------------------------------------

    def begin_handshake(
        self,
        handshake_complete: asyncio.Future,
        handshake_timeout: float | None,
    ) -> None:
        """
        Runs the client handshake from this transport's own socket reads:
        the socket is registered for reading once, for the handshake and the
        connection after it. ``handshake_complete`` resolves once the
        handshake succeeds -- the protocol's ``connection_made`` called -- or
        fails as asyncio's does: a ConnectionAbortedError past
        ``handshake_timeout``, a ConnectionResetError if the peer closes, or
        the SSLError itself, the transport closed. The loop must support
        readiness callbacks (see ``supports_readiness_callbacks``).
        """
        self._handshake_waiter = handshake_complete
        self._state = _DO_HANDSHAKE
        self._start_socket_reading()

        if handshake_timeout is not None:
            self._handshake_timeout_handle = self._loop.call_later(
                handshake_timeout,
                self._on_handshake_timeout,
                handshake_timeout,
            )

        self._do_handshake()

    def _do_handshake(self) -> None:
        ssl_object = self._ssl_object

        try:
            ssl_object.do_handshake()
            peercert = ssl_object.getpeercert()

        except ssl.SSLWantReadError:
            # Waiting on the peer: send what OpenSSL wrote for it first.
            if encrypted := self._outgoing.read():
                self._send_ciphertext(encrypted)

            return

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as error:
            self._fail_handshake(error)
            return

        handshake_complete = self._handshake_waiter
        if handshake_complete is None or handshake_complete.done():
            # The awaiter gave up (cancelled) and closes the transport.
            return

        self._handshake_waiter = None
        if (handshake_timeout_handle := self._handshake_timeout_handle) is not None:
            self._handshake_timeout_handle = None
            handshake_timeout_handle.cancel()

        self._extra.update(
            peercert=peercert,
            cipher=ssl_object.cipher(),
            compression=ssl_object.compression(),
        )
        self._state = _WRAPPED
        self._app_state = _APP_CONNECTION_MADE

        try:
            self._app_protocol.connection_made(self)

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as error:
            # Its error is the connect's, as when connection_made follows a
            # handshake run apart from the transport.
            handshake_complete.set_exception(error)
            self._force_close(error)
            return

        # Waking the awaiter first lets the write it makes in this loop
        # iteration carry the client's last handshake flight (TLS 1.3: its
        # Finished) in the same send. The flush queued after it sends the
        # flight on its own if nothing was written; on a transport closed by
        # then it sends nothing.
        handshake_complete.set_result(None)

        if self._outgoing.pending:
            self._loop.call_soon(self._process_outgoing)

        if self._incoming.pending:
            # Records that arrived with the handshake, delivered while the
            # flight above is still held.
            try:
                if not self._app_reading_paused:
                    if self._app_protocol_is_buffer:
                        self._do_read__buffered()

                    else:
                        self._do_read__copied()

                if self._ssl_reading_paused or self._incoming.pending >= self._incoming_high_water:
                    self._control_ssl_reading()

            except Exception as error:
                self._fatal_error(error, "Fatal error on SSL protocol")

    def _on_handshake_timeout(self, handshake_timeout: float) -> None:
        self._handshake_timeout_handle = None

        timeout_error = ConnectionAbortedError(
            f"SSL handshake is taking longer than {handshake_timeout} seconds: aborting the connection"
        )
        timeout_error.__cause__ = TimeoutError()
        self._fail_handshake(timeout_error)

    def _fail_handshake(self, error: BaseException) -> None:
        handshake_complete = self._handshake_waiter
        self._handshake_waiter = None

        if handshake_complete is not None and not handshake_complete.done():
            handshake_complete.set_exception(error)

        # Cancels the handshake timeout and closes the socket.
        self._force_close(error)

    def __del__(self, _warnings=warnings) -> None:
        connected_socket = getattr(self, "_socket", None)
        if connected_socket is not None and not getattr(self, "_connection_lost", True):
            _warnings.warn(f"unclosed transport {self!r}", ResourceWarning, source=self)
            connected_socket.close()

    # --- asyncio.Transport -------------------------------------------------

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        return self._extra.get(name, default)

    def set_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._set_app_protocol(protocol)

    def get_protocol(self) -> asyncio.BaseProtocol:
        return self._app_protocol

    def is_closing(self) -> bool:
        return self._closed or self._socket_closing

    def is_reading(self) -> bool:
        return not self._app_reading_paused

    def pause_reading(self) -> None:
        self._app_reading_paused = True

    def resume_reading(self) -> None:
        if not self._app_reading_paused:
            return

        self._app_reading_paused = False
        self._loop.call_soon(self._resume)

    def set_write_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._outgoing_high_water, self._outgoing_low_water = add_flowcontrol_defaults(
            high, low, constants.FLOW_CONTROL_HIGH_WATER_SSL_WRITE
        )
        self._control_app_writing()

    def get_write_buffer_limits(self) -> tuple[int, int]:
        return (self._outgoing_low_water, self._outgoing_high_water)

    def get_write_buffer_size(self) -> int:
        return self._ciphertext_size + self._write_buffer_size

    def set_read_buffer_limits(self, high: int | None = None, low: int | None = None) -> None:
        self._incoming_high_water, self._incoming_low_water = add_flowcontrol_defaults(
            high, low, constants.FLOW_CONTROL_HIGH_WATER_SSL_READ
        )
        self._control_ssl_reading()

    def get_read_buffer_limits(self) -> tuple[int, int]:
        return (self._incoming_low_water, self._incoming_high_water)

    def get_read_buffer_size(self) -> int:
        return self._incoming.pending

    @property
    def _protocol_paused(self) -> bool:
        # Read by asyncio's sendfile fallback.
        return self._app_writing_paused

    def can_write_eof(self) -> bool:
        return False

    def write_eof(self) -> None:
        raise NotImplementedError

    def write(self, data) -> None:
        if not isinstance(data, _BYTES_LIKE):
            raise TypeError(f"data: expecting a bytes-like instance, got {type(data).__name__}")

        if not data:
            return

        if self._state is not _WRAPPED:
            self._drop_write_after_close()
            return

        if self._write_backlog:
            self._write_backlog.append(bytes(data))
            self._write_buffer_size += len(data)
            self._do_write()
            return

        try:
            written = self._ssl_write(data)

        except _SSL_AGAIN_ERRORS:
            written = 0

        except Exception as error:
            self._fatal_error(error, "Fatal error on SSL protocol")
            return

        if written < len(data):
            remainder = bytes(memoryview(data)[written:])
            self._write_backlog.append(remainder)
            self._write_buffer_size += len(remainder)
            self._do_write()
            return

        if encrypted := self._outgoing.read():
            # _send_ciphertext, inline: this runs on every write.
            if self._socket_closing:
                pass

            elif self._ciphertext_backlog:
                self._queue_ciphertext(memoryview(encrypted))

            else:
                try:
                    sent = self._socket.send(encrypted)

                except _SOCKET_AGAIN_ERRORS:
                    sent = 0

                except (SystemExit, KeyboardInterrupt):
                    raise

                except BaseException as error:
                    self._fatal_error(error, "Fatal write error on socket transport")
                    return

                if sent < len(encrypted):
                    self._queue_ciphertext(memoryview(encrypted)[sent:])

        if self._app_writing_paused or self._ciphertext_size >= self._outgoing_high_water:
            self._control_app_writing()

    def writelines(self, list_of_data) -> None:
        for data in list_of_data:
            self.write(data)

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._start_shutdown()

    def abort(self) -> None:
        self._closed = True
        self._abort(None)

    # --- protocol plumbing -------------------------------------------------

    def _set_app_protocol(self, protocol: asyncio.BaseProtocol) -> None:
        self._app_protocol = protocol
        self._app_protocol_is_buffer = isinstance(protocol, asyncio.BufferedProtocol)

        if self._app_protocol_is_buffer:
            self._app_protocol_get_buffer = protocol.get_buffer
            self._app_protocol_buffer_updated = protocol.buffer_updated

        else:
            self._app_protocol_get_buffer = None
            self._app_protocol_buffer_updated = None

    def _resume(self) -> None:
        state = self._state

        if state is _WRAPPED:
            self._do_read()

        elif state is _FLUSHING:
            self._do_flush()

        elif state is _SHUTDOWN:
            self._do_shutdown()

    # --- incoming flow -----------------------------------------------------

    def _start_socket_reading(self) -> None:
        if not self._socket_reading and not self._socket_closing:
            self._socket_reading = True
            self._loop.add_reader(self._fileno, self._on_readable)

    def _stop_socket_reading(self) -> None:
        if self._socket_reading:
            self._socket_reading = False
            self._loop.remove_reader(self._fileno)

    def _on_readable(self) -> None:
        receive_view = self._receive_view

        try:
            received = self._socket.recv_into(receive_view)

        except _SOCKET_AGAIN_ERRORS:
            return

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as error:
            self._fatal_error(error, "Fatal read error on socket transport")
            return

        if not received:
            self._on_socket_eof()
            return

        incoming = self._incoming
        incoming.write(receive_view[:received])

        state = self._state
        if state is _WRAPPED:
            if self._app_reading_paused:
                self._do_read()
                return

            if self._app_protocol_is_buffer:
                # The steady state for a protocol with its own buffer, inline:
                # decrypt straight into it until OpenSSL holds no plaintext
                # and the incoming BIO no ciphertext -- the copy path's stop,
                # so no read ends in SSLWantReadError -- or it is full, when
                # the rest is read on the next turn, as _do_read__buffered
                # reads it.
                ssl_read = self._ssl_read
                ssl_pending = self._ssl_pending
                count = 1
                offset = 0

                try:
                    buffer = self._app_protocol_get_buffer(-1)
                    wants = len(buffer)

                    try:
                        while True:
                            count = ssl_read(wants - offset, buffer[offset:] if offset else buffer)
                            if not count:
                                break

                            offset += count
                            if not ssl_pending() and not incoming.pending:
                                break

                            if offset >= wants:
                                self._loop.call_soon(self._do_read)
                                break

                    except _SSL_AGAIN_ERRORS:
                        pass

                    if offset:
                        self._app_protocol_buffer_updated(offset)

                    if not count:
                        # close_notify
                        self._call_eof_received()
                        self._start_shutdown()

                    if self._write_backlog:
                        self._do_write()

                    elif self._outgoing.pending or self._app_writing_paused:
                        self._process_outgoing()

                    if self._ssl_reading_paused or incoming.pending >= self._incoming_high_water:
                        self._control_ssl_reading()

                except Exception as error:
                    self._fatal_error(error, "Fatal error on SSL protocol")

                return

            # The steady state, inline: what _do_read and _do_read__copied do
            # for a connection that is reading into an ordinary protocol --
            # decrypt everything available and deliver it in one call.
            ssl_read = self._ssl_read
            ssl_pending = self._ssl_pending
            chunk = b"1"
            first = None
            chunks = None

            try:
                try:
                    while True:
                        chunk = ssl_read(PLAINTEXT_READ_SIZE)
                        if not chunk:
                            break

                        if first is None:
                            first = chunk

                        elif chunks is None:
                            chunks = [first, chunk]

                        else:
                            chunks.append(chunk)

                        if not ssl_pending() and not incoming.pending:
                            break

                except _SSL_AGAIN_ERRORS:
                    pass

                if chunks is not None:
                    self._app_protocol.data_received(b"".join(chunks))

                elif first is not None:
                    self._app_protocol.data_received(first)

                if not chunk:
                    # close_notify
                    self._call_eof_received()
                    self._start_shutdown()

                if self._write_backlog:
                    self._do_write()

                elif self._outgoing.pending or self._app_writing_paused:
                    self._process_outgoing()

                if self._ssl_reading_paused or incoming.pending >= self._incoming_high_water:
                    self._control_ssl_reading()

            except Exception as error:
                self._fatal_error(error, "Fatal error on SSL protocol")

        elif state is _FLUSHING:
            self._do_flush()

        elif state is _SHUTDOWN:
            self._do_shutdown()

        elif state is _DO_HANDSHAKE:
            self._do_handshake()

    def _on_socket_eof(self) -> None:
        """The peer closed its side of the socket: asyncio's
        SSLProtocol.eof_received(), and the socket transport closing
        unless that keeps it open."""
        if self._state is _DO_HANDSHAKE:
            self._fail_handshake(ConnectionResetError("Connection lost during the SSL handshake"))
            return

        self._eof_received = True
        self._stop_socket_reading()
        state = self._state

        try:
            if state is _WRAPPED:
                self._state = _FLUSHING
                if self._app_reading_paused:
                    # resume_reading() delivers what is left, then closes.
                    return

                self._do_flush()

            elif state is _FLUSHING:
                self._do_write()
                self._state = _SHUTDOWN
                self._do_shutdown()

            elif state is _SHUTDOWN:
                self._do_shutdown()

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as error:
            self._fatal_error(error, "Fatal error: protocol.eof_received() call failed.")
            return

        self._close_socket()

    def _do_read(self) -> None:
        if self._state not in _READING_STATES:
            return

        try:
            if not self._app_reading_paused:
                if self._app_protocol_is_buffer:
                    self._do_read__buffered()

                else:
                    self._do_read__copied()

                if self._write_backlog:
                    self._do_write()

                elif self._outgoing.pending or self._app_writing_paused:
                    self._process_outgoing()

            if self._ssl_reading_paused or self._incoming.pending >= self._incoming_high_water:
                self._control_ssl_reading()

        except Exception as error:
            self._fatal_error(error, "Fatal error on SSL protocol")

    def _do_read__copied(self) -> None:
        """Decrypt everything available and deliver it in one call, as
        asyncio does -- stopping once OpenSSL holds no plaintext and the
        incoming BIO no ciphertext, exactly when the next read could only
        raise. One record, the common case, is delivered without a join."""
        ssl_read = self._ssl_read
        ssl_pending = self._ssl_pending
        incoming = self._incoming

        chunk = b"1"
        first = None
        chunks = None

        try:
            while True:
                chunk = ssl_read(PLAINTEXT_READ_SIZE)
                if not chunk:
                    break

                if first is None:
                    first = chunk

                elif chunks is None:
                    chunks = [first, chunk]

                else:
                    chunks.append(chunk)

                if not ssl_pending() and not incoming.pending:
                    break

        except _SSL_AGAIN_ERRORS:
            pass

        if chunks is not None:
            self._app_protocol.data_received(b"".join(chunks))

        elif first is not None:
            self._app_protocol.data_received(first)

        if not chunk:
            # close_notify
            self._call_eof_received()
            self._start_shutdown()

    def _do_read__buffered(self) -> None:
        offset = 0
        count = 1

        buffer = self._app_protocol_get_buffer(self._incoming.pending)
        wants = len(buffer)

        try:
            count = self._ssl_read(wants, buffer)

            if count > 0:
                offset = count
                while offset < wants:
                    count = self._ssl_read(wants - offset, buffer[offset:])
                    if count > 0:
                        offset += count

                    else:
                        break

                else:
                    self._loop.call_soon(self._do_read)

        except _SSL_AGAIN_ERRORS:
            pass

        if offset > 0:
            self._app_protocol_buffer_updated(offset)

        if not count:
            # close_notify
            self._call_eof_received()
            self._start_shutdown()

    def _call_eof_received(self) -> None:
        try:
            if self._app_state is _APP_CONNECTION_MADE:
                self._app_state = _APP_EOF
                keep_open = self._app_protocol.eof_received()
                if keep_open:
                    logger.warning("returning true from eof_received() has no effect when using ssl")

        except (KeyboardInterrupt, SystemExit):
            raise

        except BaseException as error:
            self._fatal_error(error, "Error calling eof_received()")

    def _control_ssl_reading(self) -> None:
        size = self._incoming.pending

        if size >= self._incoming_high_water and not self._ssl_reading_paused:
            self._ssl_reading_paused = True
            self._stop_socket_reading()

        elif size <= self._incoming_low_water and self._ssl_reading_paused:
            self._ssl_reading_paused = False
            if not self._eof_received:
                self._start_socket_reading()

    # --- outgoing flow -----------------------------------------------------

    def _drop_write_after_close(self) -> None:
        if self._conn_lost_writes >= constants.LOG_THRESHOLD_FOR_CONNLOST_WRITES:
            logger.warning("SSL connection is closed")

        self._conn_lost_writes += 1

    def _do_write(self) -> None:
        backlog = self._write_backlog

        try:
            while backlog:
                data = backlog[0]
                count = self._ssl_write(data)
                data_length = len(data)

                if count < data_length:
                    backlog[0] = data[count:]
                    self._write_buffer_size -= count

                else:
                    backlog.popleft()
                    self._write_buffer_size -= data_length

        except _SSL_AGAIN_ERRORS:
            pass

        self._process_outgoing()

    def _process_outgoing(self) -> None:
        if encrypted := self._outgoing.read():
            self._send_ciphertext(encrypted)

        self._control_app_writing()

    def _send_ciphertext(self, encrypted: bytes) -> None:
        if self._socket_closing:
            return

        if self._ciphertext_backlog:
            self._queue_ciphertext(memoryview(encrypted))
            return

        try:
            sent = self._socket.send(encrypted)

        except _SOCKET_AGAIN_ERRORS:
            sent = 0

        except (SystemExit, KeyboardInterrupt):
            raise

        except BaseException as error:
            self._fatal_error(error, "Fatal write error on socket transport")
            return

        if sent < len(encrypted):
            self._queue_ciphertext(memoryview(encrypted)[sent:])

    def _queue_ciphertext(self, remainder: memoryview) -> None:
        self._ciphertext_backlog.append(remainder)
        self._ciphertext_size += len(remainder)

        if not self._socket_writing:
            self._socket_writing = True
            self._loop.add_writer(self._fileno, self._on_writable)

    def _on_writable(self) -> None:
        backlog = self._ciphertext_backlog

        while backlog:
            pending = backlog[0]

            try:
                sent = self._socket.send(pending)

            except _SOCKET_AGAIN_ERRORS:
                return

            except (SystemExit, KeyboardInterrupt):
                raise

            except BaseException as error:
                self._fatal_error(error, "Fatal write error on socket transport")
                return

            self._ciphertext_size -= sent
            if sent < len(pending):
                backlog[0] = pending[sent:]
                return

            backlog.popleft()

        self._socket_writing = False
        self._loop.remove_writer(self._fileno)
        self._control_app_writing()

        if self._socket_closing:
            self._schedule_connection_lost(None)

    def _control_app_writing(self) -> None:
        size = self._ciphertext_size + self._write_buffer_size

        if size >= self._outgoing_high_water and not self._app_writing_paused:
            self._app_writing_paused = True
            self._call_flow_control(self._app_protocol.pause_writing, "protocol.pause_writing() failed")

        elif size <= self._outgoing_low_water and self._app_writing_paused:
            self._app_writing_paused = False
            self._call_flow_control(self._app_protocol.resume_writing, "protocol.resume_writing() failed")

    def _call_flow_control(self, callback: Callable[[], None], message: str) -> None:
        try:
            callback()

        except (KeyboardInterrupt, SystemExit):
            raise

        except BaseException as error:
            self._loop.call_exception_handler(
                {
                    "message": message,
                    "exception": error,
                    "transport": self,
                    "protocol": self._app_protocol,
                }
            )

    # --- shutdown flow -----------------------------------------------------

    def _start_shutdown(self) -> None:
        if self._state in _CLOSING_STATES:
            return

        self._closed = True
        self._state = _FLUSHING
        self._shutdown_timeout_handle = self._loop.call_later(
            constants.SSL_SHUTDOWN_TIMEOUT,
            self._check_shutdown_timeout,
        )
        self._do_flush()

    def _check_shutdown_timeout(self) -> None:
        self._shutdown_timeout_handle = None

        if not self._connection_lost:
            self._force_close(TimeoutError("SSL shutdown timed out"))

    def _do_flush(self) -> None:
        self._do_read()
        self._state = _SHUTDOWN
        self._do_shutdown()

    def _do_shutdown(self) -> None:
        try:
            if not self._eof_received:
                self._ssl_object.unwrap()

        except _SSL_AGAIN_ERRORS:
            # close_notify is written; the peer's has not arrived.
            self._process_outgoing()

        except ssl.SSLError as error:
            self._on_shutdown_complete(error)

        else:
            self._process_outgoing()
            self._call_eof_received()
            self._on_shutdown_complete(None)

    def _on_shutdown_complete(self, shutdown_error: BaseException | None) -> None:
        if shutdown_error is not None:
            self._fatal_error(shutdown_error, "Fatal error on transport")

        else:
            self._loop.call_soon(self._close_socket)

    def _abort(self, error: BaseException | None) -> None:
        self._state = _UNWRAPPED
        self._force_close(error)

    # --- socket closing ----------------------------------------------------

    def _close_socket(self) -> None:
        """The socket transport's close(): stop reading and, once the
        queued ciphertext is sent, report the connection lost."""
        if self._socket_closing:
            return

        self._socket_closing = True
        self._stop_socket_reading()

        if not self._ciphertext_backlog:
            self._schedule_connection_lost(None)

    def _fatal_error(self, error: BaseException, message: str) -> None:
        if self._handshake_waiter is not None:
            # Before the handshake completes the error is the handshake's,
            # raised by whoever awaits it.
            self._fail_handshake(error)
            return

        self._force_close(error)

        if isinstance(error, OSError):
            if self._loop.get_debug():
                logger.debug("%r: %s", self, message, exc_info=True)

        elif not isinstance(error, asyncio.CancelledError):
            self._loop.call_exception_handler(
                {
                    "message": message,
                    "exception": error,
                    "transport": self,
                    "protocol": self._app_protocol,
                }
            )

    def _force_close(self, error: BaseException | None) -> None:
        if self._connection_lost:
            return

        if (handshake_timeout_handle := self._handshake_timeout_handle) is not None:
            self._handshake_timeout_handle = None
            handshake_timeout_handle.cancel()

        if (handshake_complete := self._handshake_waiter) is not None:
            # Closed before the handshake completed, by the code that was
            # waiting for it: nothing awaits this future any more.
            self._handshake_waiter = None
            handshake_complete.cancel()

        self._closed = True
        self._socket_closing = True
        self._state = _UNWRAPPED

        if self._socket_writing:
            self._socket_writing = False
            self._loop.remove_writer(self._fileno)

        self._stop_socket_reading()
        self._ciphertext_backlog.clear()
        self._ciphertext_size = 0
        self._schedule_connection_lost(error)

    def _schedule_connection_lost(self, error: BaseException | None) -> None:
        if not self._connection_lost:
            self._connection_lost = True
            self._loop.call_soon(self._call_connection_lost, error)

    def _call_connection_lost(self, error: BaseException | None) -> None:
        if self._socket is None:
            return

        self._state = _UNWRAPPED
        self._write_backlog.clear()
        self._write_buffer_size = 0
        self._outgoing.read()

        if self._shutdown_timeout_handle is not None:
            self._shutdown_timeout_handle.cancel()
            self._shutdown_timeout_handle = None

        try:
            if self._app_state is _APP_CONNECTION_MADE or self._app_state is _APP_EOF:
                self._app_state = _APP_CONNECTION_LOST
                self._app_protocol.connection_lost(error)

        finally:
            self._socket.close()
            # Break the reference cycles through the protocol and buffers.
            self._socket = None
            self._app_protocol = None
            self._app_protocol_get_buffer = None
            self._app_protocol_buffer_updated = None
            self._receive_view = None


def _socket_address(address_of: Callable[[], Any]) -> Any:
    try:
        return address_of()

    except OSError:
        return None
