import asyncio
import codecs
import inspect
from typing import (
    Generic,
    AnyStr,
    Mapping,
    Awaitable,
    Any,
    Iterable,
    TYPE_CHECKING,
)
from types import MappingProxyType
from .constants import (
    MSG_CHANNEL_CLOSE,
    MSG_CHANNEL_DATA,
    MSG_CHANNEL_EOF,
    MSG_CHANNEL_EXTENDED_DATA,
    MSG_CHANNEL_FAILURE,
    MSG_CHANNEL_SUCCESS,
    OPEN_CONNECT_FAILED,
    MSG_CHANNEL_OPEN,
    MSG_CHANNEL_REQUEST,
    MSG_CHANNEL_WINDOW_ADJUST,
)
from .env import decode_env
from .error import ProtocolError, ChannelOpenError
from .packet import SSHPacket, UInt32, Boolean, String
from .ssh_packet_handler import SSHPacketHandler
from .types import DataType, MaybeAwait

if TYPE_CHECKING:
    from .ssh_connection import SSHConnection
    from .ssh_session import SSHSession



class SSHChannel(Generic[AnyStr], SSHPacketHandler):
    """Parent class for SSH channels"""

    _handler_names = get_symbol_names(globals(), 'MSG_CHANNEL_')

    _read_datatypes: set[int] = set()
    _write_datatypes: set[int] = set()

    def __init__(self, conn: SSHConnection,
                 loop: asyncio.AbstractEventLoop, encoding: str,
                 errors: str, window: int, max_pktsize: int):
        """Initialize an SSH channel

           If encoding is set, data sent and received will be in the form
           of strings, converted on the wire to bytes using the specified
           encoding. If encoding is None, data sent and received must be
           provided as bytes.

           Window specifies the initial receive window size.

           Max_pktsize specifies the maximum length of a single data packet.

        """

        self._conn: SSHConnection = conn
        self._loop = loop
        self._session: SSHSession[AnyStr] = None
        self._extra: dict[str, object] = {'connection': conn}
        self._encoding: str | None = None
        self._errors: str = ''
        self._send_high_water: int = 0
        self._send_low_water: int = 0

        self._env: dict[bytes, bytes] = {}
        self._str_env: dict[str, str] | None = None

        self._command: str | None = None
        self._subsystem: str | None = None

        self._send_state = 'closed'
        self._send_chan: int | None = None
        self._send_window: int = 0
        self._send_pktsize: int = 0
        self._send_paused = False
        self._send_buf: list[tuple[bytearray, DataType]] = []
        self._send_buf_len = 0

        self._recv_state = 'closed'
        self._init_recv_window = window
        self._recv_window = window
        self._recv_pktsize = max_pktsize
        self._recv_paused: bool | str = 'starting'
        self._recv_buf: list[tuple[bytes, DataType]] = []

        self._request_queue: list[tuple[str, SSHPacket, bool]] = []

        self._open_waiter: asyncio.Future[SSHPacket] | None = None
        self._request_waiters: list[asyncio.Future[bool]] = []

        self._close_event = asyncio.Event()

        self._recv_chan: int | None = conn.add_channel(self)

        self.set_encoding(encoding, errors)
        self.set_write_buffer_limits()

    def get_connection(self) -> SSHConnection:
        """Return the connection used by this channel"""

        assert self._conn is not None
        return self._conn

    def get_loop(self) -> asyncio.AbstractEventLoop:
        """Return the event loop used by this channel"""

        return self._loop

    def get_encoding(self) -> tuple[str | None, str]:
        """Return the encoding used by this channel"""

        return self._encoding, self._errors

    def set_encoding(self, encoding: str | None,
                     errors: str = 'strict') -> None:
        """Set the encoding on this channel"""

        self._encoding = encoding
        self._errors = errors

        if encoding:
            self._encoder: codecs.IncrementalEncoder | None = \
                codecs.getincrementalencoder(encoding)(errors)
            self._decoder: codecs.IncrementalDecoder | None = \
                codecs.getincrementaldecoder(encoding)(errors)
        else:
            self._encoder = None
            self._decoder = None

    def get_recv_window(self) -> int:
        """Return the configured receive window for this channel"""

        return self._init_recv_window

    def get_read_datatypes(self) -> set[int]:
        """Return the legal read data types for this channel"""

        return self._read_datatypes

    def get_write_datatypes(self) -> set[int]:
        """Return the legal write data types for this channel"""

        return self._write_datatypes

    def _cleanup(self, exc: Exception | None = None) -> None:
        """Clean up this channel"""

        if self._open_waiter:
            if not self._open_waiter.cancelled(): # pragma: no branch
                self._open_waiter.set_exception(
                    ChannelOpenError(OPEN_CONNECT_FAILED,
                                     'SSH connection closed'))

            self._open_waiter = None

        if self._request_waiters:
            for waiter in self._request_waiters:
                if not waiter.cancelled(): # pragma: no cover
                    if exc:
                        waiter.set_exception(exc)
                    else:
                        waiter.set_result(False)

            self._request_waiters = []

        if self._session is not None:
            # pylint: disable=broad-except
            try:
                self._session.connection_lost(exc)
            except Exception:
                pass

            self._session = None

        self._close_event.set()

        if self._conn: # pragma: no branch

            self._conn.detach_x11_listener(self)

            assert self._recv_chan is not None
            self._conn.remove_channel(self._recv_chan)
            self._send_chan = None
            self._recv_chan = None
            self._conn = None

    def _close_send(self) -> None:
        """Discard unsent data and close the channel for sending"""

        # Discard unsent data
        self._send_buf = []
        self._send_buf_len = 0

        if self._send_state != 'closed':
            self.send_packet(MSG_CHANNEL_CLOSE)
            self._send_chan = None
            self._send_state = 'closed'

    def _discard_recv(self) -> None:
        """Discard unreceived data and clean up if close received"""

        # Discard unreceived data
        self._recv_buf = []
        self._recv_paused = False

        # If recv is close_pending, we know send is already closed
        if self._recv_state == 'close_pending':
            self._recv_state = 'closed'
            self._loop.call_soon(self._cleanup)

    async def _start_reading(self) -> None:
        """Start processing data on a new connection"""

        # If owner of the channel  didn't explicitly pause it at
        # startup, begin processing incoming data.

        if self._recv_paused == 'starting':
            self._recv_paused = False
            self._flush_recv_buf()

    def _pause_resume_writing(self) -> None:
        """Pause or resume writing based on send buffer low/high water marks"""

        if self._send_paused:
            if self._send_buf_len <= self._send_low_water:

                self._send_paused = False
                assert self._session is not None
                self._session.resume_writing()
        else:
            if self._send_buf_len > self._send_high_water:

                self._send_paused = True
                assert self._session is not None
                self._session.pause_writing()

    def _flush_send_buf(self) -> None:
        """Flush as much data in send buffer as the send window allows"""

        while self._send_buf and self._send_window:
            pktsize = min(self._send_window, self._send_pktsize)
            buf, datatype = self._send_buf[0]

            if len(buf) > pktsize:
                data = buf[:pktsize]
                del buf[:pktsize]
            else:
                data = buf
                del self._send_buf[0]

            self._send_buf_len -= len(data)
            self._send_window -= len(data)

            if datatype is None:
                self.send_packet(MSG_CHANNEL_DATA, String(data))
            else:
                self.send_packet(MSG_CHANNEL_EXTENDED_DATA,
                                 UInt32(datatype), String(data))

        self._pause_resume_writing()

        if not self._send_buf:
            if self._send_state == 'eof_pending':
                self.send_packet(MSG_CHANNEL_EOF)
                self._send_state = 'eof'
            elif self._send_state == 'close_pending':
                self._close_send()

    def _flush_recv_buf(self, exc: Exception | None = None) -> None:
        """Flush as much data in the recv buffer as the application allows"""

        while self._recv_buf and not self._recv_paused:
            self._deliver_data(*self._recv_buf.pop(0))

        if not self._recv_buf and self._recv_paused != 'starting':
            if self._encoding and not exc and \
                    self._recv_state in ('eof_pending', 'close_pending'):
                try:
                    assert self._decoder is not None
                    self._decoder.decode(b'', True)
                except UnicodeDecodeError as unicode_exc:
                    raise ProtocolError(str(unicode_exc)) from None

            if self._recv_state == 'eof_pending':
                self._recv_state = 'eof'

                assert self._session is not None

                if (not self._session.eof_received() and
                        self._send_state == 'open'):
                    self.write_eof()

        if not self._recv_buf and self._recv_state == 'close_pending':
            self._recv_state = 'closed'
            self._loop.call_soon(self._cleanup, exc)

    def _deliver_data(self, data: bytes, datatype: DataType) -> None:
        """Deliver incoming data to the session"""

        self._recv_window -= len(data)

        if self._recv_window < self._init_recv_window / 2:
            adjust = self._init_recv_window - self._recv_window

            self.send_packet(MSG_CHANNEL_WINDOW_ADJUST, UInt32(adjust))
            self._recv_window = self._init_recv_window

        if self._encoding:
            try:
                assert self._decoder is not None
                decoded_data: AnyStr = self._decoder.decode(data)
            except UnicodeDecodeError as unicode_exc:
                raise ProtocolError(str(unicode_exc)) from None
        else:
            decoded_data: AnyStr = data

        if self._session is not None:
            self._session.data_received(decoded_data, datatype)

    def _accept_data(self, data: bytes, datatype: DataType = None) -> None:
        """Accept new data on the channel

           This method accepts new data on the channel, immediately
           delivering it to the session if it hasn't paused reading.
           If it has paused, data is buffered until reading is resumed.

           Data sent after the channel has been closed by the session
           is dropped.

        """

        if not data:
            return

        if self._send_state in {'close_pending', 'closed'}:
            return

        if self._recv_paused:
            self._recv_buf.append((data, datatype))
        else:
            self._deliver_data(data, datatype)

    def _service_next_request(self) -> None:
        """Process next item on channel request queue"""

        request, packet, _ = self._request_queue[0]

        name = '_process_' + map_handler_name(request) + '_request'
        handler = getattr(self, name, None)

        if handler:
            result: bool | None = handler(packet)
        else:
            result = False

        if result is not None:
            self._report_response(result)

    def _report_response(self, result: bool) -> None:
        """Report back the response to a previously issued channel request"""

        request, _, want_reply = self._request_queue.pop(0)

        if want_reply and self._send_state not in {'close_pending', 'closed'}:
            if result:
                self.send_packet(MSG_CHANNEL_SUCCESS)
            else:
                self.send_packet(MSG_CHANNEL_FAILURE)

        if result and request in {'shell', 'exec', 'subsystem'}:
            assert self._session is not None
            self._session.session_started()
            self.resume_reading()

        if self._request_queue:
            self._service_next_request()

    def process_connection_close(self, exc: Exception | None) -> None:
        """Process the SSH connection closing"""
        self._send_state = 'closed'
        self._close_send()
        self._cleanup(exc)

    def process_open(self, send_chan: int, send_window: int, send_pktsize: int,
                     session: MaybeAwait[SSHSession[AnyStr]]) -> None:
        """Process a channel open request"""

        self._send_chan = send_chan
        self._send_window = send_window
        self._send_pktsize = send_pktsize

        assert self._conn is not None
        self._conn.create_task(self._finish_open_request(session))

    def _wrap_session(self, session: SSHSession[AnyStr]) -> \
            tuple['SSHChannel[AnyStr]', SSHSession[AnyStr]]:
        """Hook to optionally wrap channel and session objects"""

        # By default, return the original channel and session objects
        return self, session

    async def _finish_open_request(
            self, result: MaybeAwait[SSHSession[AnyStr]]) -> None:
        """Finish processing a channel open request"""

        try:
            if inspect.isawaitable(result):
                session: Awaitable[SSHSession[AnyStr]] = await result
            else:
                session: SSHSession[AnyStr] = result

            if not self._conn:
                raise ChannelOpenError(OPEN_CONNECT_FAILED,
                                       'SSH connection closed')

            chan, self._session = self._wrap_session(session)

            assert self._send_chan is not None
            assert self._recv_chan is not None

            self._conn.send_channel_open_confirmation(self._send_chan,
                                                      self._recv_chan,
                                                      self._recv_window,
                                                      self._recv_pktsize)

            self._send_state = 'open'
            self._recv_state = 'open'

            self._session.connection_made(chan)
        except ChannelOpenError as exc:
            if self._conn:
                assert self._send_chan is not None
                self._conn.send_channel_open_failure(self._send_chan, exc.code,
                                                     exc.reason, exc.lang)

            self._loop.call_soon(self._cleanup)

    def process_open_confirmation(self, send_chan: int, send_window: int,
                                  send_pktsize: int, packet: SSHPacket) -> None:
        """Process a channel open confirmation"""

        if not self._open_waiter:
            raise ProtocolError('Channel not being opened')

        self._send_chan = send_chan
        self._send_window = send_window
        self._send_pktsize = send_pktsize

        self._send_state = 'open'
        self._recv_state = 'open'

        if not self._open_waiter.cancelled(): # pragma: no branch
            self._open_waiter.set_result(packet)

        self._open_waiter = None

    def process_open_failure(self, code: int, reason: str, lang: str) -> None:
        """Process a channel open failure"""

        if not self._open_waiter:
            raise ProtocolError('Channel not being opened')

        if not self._open_waiter.cancelled(): # pragma: no branch
            self._open_waiter.set_exception(
                ChannelOpenError(code, reason, lang))

        self._open_waiter = None
        self._loop.call_soon(self._cleanup)

    def _process_window_adjust(self, _pkttype: int, _pktid: int,
                               packet: SSHPacket) -> None:
        """Process a send window adjustment"""

        if self._recv_state not in {'open', 'eof_pending', 'eof'}:
            raise ProtocolError('Channel not open')

        adjust = packet.get_uint32()
        packet.check_end()

        self._send_window += adjust

        self._flush_send_buf()

    def _process_data(self, _pkttype: int, _pktid: int,
                      packet: SSHPacket) -> None:
        """Process incoming data"""

        if self._recv_state != 'open':
            raise ProtocolError('Channel not open for sending')

        data = packet.get_string()
        packet.check_end()

        datalen = len(data)

        if datalen > self._recv_window:
            raise ProtocolError('Window exceeded')

        self._accept_data(data)

    def _process_extended_data(self, _pkttype: int, _pktid: int,
                               packet: SSHPacket) -> None:
        """Process incoming extended data"""

        if self._recv_state != 'open':
            raise ProtocolError('Channel not open for sending')

        datatype = packet.get_uint32()
        data = packet.get_string()
        packet.check_end()

        if datatype not in self._read_datatypes:
            raise ProtocolError('Invalid extended data type')

        datalen = len(data)

        if datalen > self._recv_window:
            raise ProtocolError('Window exceeded')

        self._accept_data(data, datatype)

    def _process_eof(self, _pkttype: int, _pktid: int,
                     packet: SSHPacket) -> None:
        """Process an incoming end of file"""

        if self._recv_state != 'open':
            raise ProtocolError('Channel not open for sending')

        packet.check_end()

        self._recv_state = 'eof_pending'
        self._flush_recv_buf()

    def _process_close(self, _pkttype: int, _pktid: int,
                       packet: SSHPacket) -> None:
        """Process an incoming channel close"""

        if self._recv_state not in {'open', 'eof_pending', 'eof'}:
            raise ProtocolError('Channel not open')

        packet.check_end()

        self._close_send()

        self._recv_state = 'close_pending'
        self._flush_recv_buf()

    def _process_request(self, _pkttype: int, _pktid: int,
                         packet: SSHPacket) -> None:
        """Process an incoming channel request"""

        if self._recv_state not in {'open', 'eof_pending', 'eof'}:
            raise ProtocolError('Channel not open')

        request_bytes = packet.get_string()
        want_reply = packet.get_boolean()

        try:
            request = request_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid channel request') from None

        self._request_queue.append((request, packet, want_reply))
        if len(self._request_queue) == 1:
            self._service_next_request()

    def _process_response(self, pkttype: int, _pktid: int,
                          packet: SSHPacket) -> None:
        """Process a success or failure response"""

        packet.check_end()

        if self._request_waiters:
            waiter = self._request_waiters.pop(0)
            if not waiter.cancelled(): # pragma: no branch
                waiter.set_result(pkttype == MSG_CHANNEL_SUCCESS)
        else:
            raise ProtocolError('Unexpected channel response')

    def _process_keepalive_at_openssh_dot_com_request(
            self, packet: SSHPacket) -> bool:
        """Process an incoming OpenSSH keepalive request"""

        packet.check_end()
        return False

    _packet_handlers = {
        MSG_CHANNEL_WINDOW_ADJUST:      _process_window_adjust,
        MSG_CHANNEL_DATA:               _process_data,
        MSG_CHANNEL_EXTENDED_DATA:      _process_extended_data,
        MSG_CHANNEL_EOF:                _process_eof,
        MSG_CHANNEL_CLOSE:              _process_close,
        MSG_CHANNEL_REQUEST:            _process_request,
        MSG_CHANNEL_SUCCESS:            _process_response,
        MSG_CHANNEL_FAILURE:            _process_response
    }

    async def _open(self, chantype: bytes, *args: bytes) -> SSHPacket:
        """Make a request to open the channel"""

        if self._send_state != 'closed':
            raise OSError('Channel already open')

        self._open_waiter = self._loop.create_future()

        assert self._conn is not None
        assert self._recv_chan is not None

        self._conn.send_packet(MSG_CHANNEL_OPEN, String(chantype),
                               UInt32(self._recv_chan),
                               UInt32(self._recv_window),
                               UInt32(self._recv_pktsize), *args, handler=self)

        return await self._open_waiter

    def send_packet(self, pkttype: int, *args: bytes) -> None:
        """Send a packet on the channel"""

        if self._send_chan is None: # pragma: no cover
            return

        payload = UInt32(self._send_chan) + b''.join(args)

        assert self._conn is not None
        self._conn.send_packet(pkttype, payload, handler=self)

    def _send_request(self, request: bytes, *args: bytes,
                      want_reply: bool = False) -> None:
        """Send a channel request"""

        self.send_packet(MSG_CHANNEL_REQUEST, String(request),
                         Boolean(want_reply), *args)

    async def _make_request(self, request: bytes,
                            *args: bytes) -> bool | None:
        """Make a channel request and wait for the response"""

        if self._send_chan is None:
            return False

        waiter = self._loop.create_future()
        self._request_waiters.append(waiter)
        self._send_request(request, *args, want_reply=True)
        return await waiter

    def abort(self) -> None:
        """Forcibly close the channel

           This method can be called to forcibly close the channel, after
           which no more data can be sent or received. Any unsent buffered
           data and any incoming data in flight will be discarded.

        """

        if self._send_state not in {'close_pending', 'closed'}:
            # Send an immediate close, discarding unsent data
            self._close_send()

        if self._recv_state != 'closed':
            # Discard unreceived data
            self._discard_recv()

    def close(self) -> None:
        """Cleanly close the channel

           This method can be called to cleanly close the channel, after
           which no more data can be sent or received. Any unsent buffered
           data will be flushed asynchronously before the channel is
           closed.

        """

        if self._send_state not in {'close_pending', 'closed'}:
            # Send a close only after sending unsent data
            self._send_state = 'close_pending'
            self._flush_send_buf()

        if self._recv_state != 'closed':
            # Discard unreceived data
            self._discard_recv()

    def is_closing(self) -> bool:
        """Return if the channel is closing or is closed"""

        return self._send_state != 'open'

    async def wait_closed(self) -> None:
        """Wait for this channel to close

           This method is a coroutine which can be called to block until
           this channel has finished closing.

        """

        await self._close_event.wait()

    def get_extra_info(self, name: str, default: Any = None) -> Any:
        """Get additional information about the channel

           This method returns extra information about the channel once
           it is established. Supported values include `'connection'`
           to return the SSH connection this channel is running over plus
           all of the values supported on that connection.

           For TCP channels, the values `'local_peername'` and
           `'remote_peername'` are added to return the local and remote
           host and port information for the tunneled TCP connection.

           For UNIX channels, the values `'local_peername'` and
           `'remote_peername'` are added to return the local and remote
           path information for the tunneled UNIX domain socket connection.
           Since UNIX domain sockets provide no "source" address, only
           one of these will be filled in.

           See :meth:`get_extra_info() <SSHClientConnection.get_extra_info>`
           on :class:`SSHClientConnection` for more information.

           Additional information stored on the channel by calling
           :meth:`set_extra_info` can also be returned here.

        """

        return self._extra.get(name, self._conn.get_extra_info(name, default)
                               if self._conn else default)

    def set_extra_info(self, **kwargs: Any) -> None:
        """Store additional information associated with the channel

           This method allows extra information to be associated with the
           channel. The information to store should be passed in as
           keyword parameters and can later be returned by calling
           :meth:`get_extra_info` with one of the keywords as the name
           to retrieve.

        """

        self._extra.update(**kwargs)

    def can_write_eof(self) -> bool:
        """Return whether the channel supports :meth:`write_eof`

           This method always returns `True`.

        """

        # pylint: disable=no-self-use
        return True

    def get_write_buffer_size(self) -> int:
        """Return the current size of the channel's output buffer

           This method returns how many bytes are currently in the
           channel's output buffer waiting to be written.

        """

        return self._send_buf_len

    def set_write_buffer_limits(self, high: int | None = None,
                                low: int | None = None) -> None:
        """Set the high- and low-water limits for write flow control

           This method sets the limits used when deciding when to call
           the :meth:`pause_writing() <SSHClientSession.pause_writing>`
           and :meth:`resume_writing() <SSHClientSession.resume_writing>`
           methods on SSH sessions. Writing will be paused when the write
           buffer size exceeds the high-water mark, and resumed when the
           write buffer size equals or drops below the low-water mark.

        """

        if high is None:
            high = 4*low if low is not None else 65536

        if low is None:
            low = high // 4

        if not 0 <= low <= high:
            raise ValueError(f'high (high) must be >= low ({low}) '
                             'must be >= 0')

        self._send_high_water = high
        self._send_low_water = low
        self._pause_resume_writing()

    def write(self, data: AnyStr, datatype: DataType = None) -> None:
        """Write data on the channel

           This method can be called to send data on the channel. If
           an encoding was specified when the channel was created, the
           data should be provided as a string and will be converted
           using that encoding. Otherwise, the data should be provided
           as bytes.

           An extended data type can optionally be provided. For
           instance, this is used from a :class:`SSHServerSession`
           to write data to `stderr`.

           :param data:
               The data to send on the channel
           :param datatype: (optional)
               The extended data type of the data, from :ref:`extended
               data types <ExtendedDataTypes>`
           :type data: `str` or `bytes`
           :type datatype: `int`

           :raises: :exc:`OSError` if the channel isn't open for sending
                    or the extended data type is not valid for this type
                    of channel

        """

        if self._send_state != 'open':
            raise BrokenPipeError('Channel not open for sending')

        if datatype is not None and datatype not in self._write_datatypes:
            raise OSError('Invalid extended data type')

        if not data:
            return

        if self._encoding:
            assert self._encoder is not None
            encoded_data = self._encoder.encode(data)
        else:
            encoded_data: bytes = data

        datalen = len(encoded_data)

        self._send_buf.append((bytearray(encoded_data), datatype))
        self._send_buf_len += datalen
        self._flush_send_buf()

    def writelines(self, list_of_data: Iterable[AnyStr],
                   datatype: DataType = None) -> None:
        """Write a list of data bytes on the channel

           This method can be called to write a list (or any iterable) of
           data bytes to the channel. It is functionality equivalent to
           calling :meth:`write` on each element in the list.

           :param list_of_data:
               The data to send on the channel
           :param datatype: (optional)
               The extended data type of the data, from :ref:`extended
               data types <ExtendedDataTypes>`
           :type list_of_data: iterable of `str` or `bytes`
           :type datatype: `int`

           :raises: :exc:`OSError` if the channel isn't open for sending
                    or the extended data type is not valid for this type
                    of channel

        """

        if self._encoding:
            data: AnyStr = ''.join(list_of_data)
        else:
            data: AnyStr = b''.join(list_of_data)

        return self.write(data, datatype)

    def write_eof(self) -> None:
        """Write EOF on the channel

           This method sends an end-of-file indication on the
           channel, after which no more data can be sent. The
           channel remains open, though, and data may still be
           sent in the other direction.

           :raises: :exc:`OSError` if the channel isn't open for sending

        """
        if self._send_state == 'open':
            self._send_state = 'eof_pending'
            self._flush_send_buf()

    def pause_reading(self) -> None:
        """Pause delivery of incoming data

           This method is used to temporarily suspend delivery of incoming
           channel data. After this call, incoming data will no longer
           be delivered until :meth:`resume_reading` is called. Data will be
           buffered locally up to the configured SSH channel window size,
           but window updates will no longer be sent, eventually causing
           back pressure on the remote system.

           .. note:: Channel close notifications are not suspended by this
                     call. If the remote system closes the channel while
                     delivery is suspended, the channel will be closed even
                     though some buffered data may not have been delivered.

        """
        self._recv_paused = True

    def resume_reading(self) -> None:
        """Resume delivery of incoming data

           This method can be called to resume delivery of incoming data
           which was suspended by a call to :meth:`pause_reading`. As soon
           as this method is called, any buffered data will be delivered
           immediately. A pending end-of-file notification may also be
           delivered if one was queued while reading was paused.

        """

        if self._recv_paused:
            self._recv_paused = False
            self._flush_recv_buf()

    def get_environment(self) -> Mapping[str, str]:
        """Return the environment for this session

           This method returns the environment set by the client when
           the session was opened. Keys and values are of type `str`
           and this object only provides access to keys and values sent
           as valid UTF-8 strings. Use :meth:`get_environment_bytes`
           if you need to access environment variables with keys or
           values containing binary data or non-UTF-8 encodings.

           On the server, calls to this method should only be made after
           :meth:`session_started <SSHServerSession.session_started>` has
           been called on the :class:`SSHServerSession`. When using the
           stream-based API, calls to this can be made at any time after
           the handler function has started up.

           :returns: A dictionary containing the environment variables
                     set by the client

        """

        if self._str_env is None:
            self._str_env = dict(decode_env(self._env))

        return MappingProxyType(self._str_env)

    def get_environment_bytes(self) -> Mapping[bytes, bytes]:
        """Return the environment for this session

           This method returns the environment set by the client when
           the session was opened. Keys and values are of type `bytes`
           and can include arbitrary binary data, with the exception
           of NUL (\0) bytes.

           On the server, calls to this method should only be made after
           :meth:`session_started <SSHServerSession.session_started>` has
           been called on the :class:`SSHServerSession`. When using the
           stream-based API, calls to this can be made at any time after
           the handler function has started up.

           :returns: A dictionary containing the environment variables
                     set by the client

        """

        return MappingProxyType(self._env)

    def get_command(self) -> str | None:
        """Return the command the client requested to execute, if any

           This method returns the command the client requested to
           execute when the session was opened, if any. If the client
           did not request that a command be executed, this method
           will return `None`. On the server, calls to this method
           should only be made after :meth:`session_started
           <SSHServerSession.session_started>` has been called on the
           :class:`SSHServerSession`. When using the stream-based API,
           calls to this can be made at any time after the handler
           function has started up.

        """

        return self._command

    def get_subsystem(self) -> str | None:
        """Return the subsystem the client requested to open, if any

           This method returns the subsystem the client requested to
           open when the session was opened, if any. If the client
           did not request that a subsystem be opened, this method will
           return `None`. On the server, calls to this method should
           only be made after :meth:`session_started
           <SSHServerSession.session_started>` has been called on the
           :class:`SSHServerSession`. When using the stream-based API,
           calls to this can be made at any time after the handler
           function has started up.

        """

        return self._subsystem