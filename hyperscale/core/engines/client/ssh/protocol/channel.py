# Copyright (c) 2013-2024 by Ron Frederick <ronf@timeheart.net> and others.
#
# This program and the accompanying materials are made available under
# the terms of the Eclipse Public License v2.0 which accompanies this
# distribution and is available at:
#
#     http://www.eclipse.org/legal/epl-2.0/
#
# This program may also be made available under the following secondary
# licenses when the conditions for such availability set forth in the
# Eclipse Public License v2.0 are satisfied:
#
#    GNU General Public License, Version 2.0, or any later versions of
#    that license
#
# SPDX-License-Identifier: EPL-2.0 OR GPL-2.0-or-later
#
# Contributors:
#     Ron Frederick - initial implementation, API, and documentation

"""SSH channel and session handlers"""

import asyncio
import binascii
import codecs
import inspect
import re
import signal as _signal
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, AnyStr, Awaitable, Callable
from typing import Dict, Generic, Iterable, List, Mapping, Optional
from typing import Set, Tuple, Union

from .constants import EXTENDED_DATA_STDERR
from .constants import MSG_CHANNEL_OPEN, MSG_CHANNEL_WINDOW_ADJUST
from .constants import MSG_CHANNEL_DATA, MSG_CHANNEL_EXTENDED_DATA
from .constants import MSG_CHANNEL_EOF, MSG_CHANNEL_CLOSE, MSG_CHANNEL_REQUEST
from .constants import MSG_CHANNEL_SUCCESS, MSG_CHANNEL_FAILURE
from .constants import OPEN_CONNECT_FAILED, PTY_OP_RESERVED, PTY_OP_END
from .constants import OPEN_REQUEST_X11_FORWARDING_FAILED
from .constants import OPEN_REQUEST_PTY_FAILED, OPEN_REQUEST_SESSION_FAILED

from .misc import ChannelOpenError, MaybeAwait, ProtocolError
from .misc import TermModes, TermSize, TermSizeArg
from .misc import decode_env, get_symbol_names, map_handler_name

from .packet import Boolean, Byte, String, UInt32, SSHPacket, SSHPacketHandler

from .session import SSHSession, SSHClientSession
from .session import SSHTCPSession, SSHUNIXSession, SSHTunTapSession
from .session import SSHSessionFactory, SSHClientSessionFactory
from .session import SSHTCPSessionFactory, SSHUNIXSessionFactory
from .session import SSHTunTapSessionFactory

from .stream import DataType

from .tuntap import SSH_TUN_MODE_POINTTOPOINT, SSH_TUN_UNIT_ANY
from .tuntap import SSH_TUN_AF_INET, SSH_TUN_AF_INET6


if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from .connection import SSHConnection, SSHClientConnection

_signal_regex = re.compile(r'SIG[^_]')
_signal_numbers = {k[3:]: int(v) for (k, v) in vars(_signal).items()
                   if _signal_regex.match(k)}
_signal_names = {v: k for (k, v) in _signal_numbers.items()}

_ExitSignal = Tuple[str, bool, str, str]
_RequestHandler = Optional[Callable[[SSHPacket], Optional[bool]]]


class SSHChannel(Generic[AnyStr], SSHPacketHandler):
    """Parent class for SSH channels"""

    _handler_names = get_symbol_names(globals(), 'MSG_CHANNEL_')

    _read_datatypes: Set[int] = set()
    _write_datatypes: Set[int] = set()

    def __init__(self, conn: 'SSHConnection',
                 loop: asyncio.AbstractEventLoop, encoding: Optional[str],
                 errors: str, window: int, max_pktsize: int):
        """Initialize an SSH channel

           If encoding is set, data sent and received will be in the form
           of strings, converted on the wire to bytes using the specified
           encoding. If encoding is None, data sent and received must be
           provided as bytes.

           Window specifies the initial receive window size.

           Max_pktsize specifies the maximum length of a single data packet.

        """

        self._conn: Optional['SSHConnection'] = conn
        self._loop = loop
        self._session: Optional[SSHSession[AnyStr]] = None
        self._extra: Dict[str, object] = {'connection': conn}
        self._encoding: Optional[str]
        self._errors: str
        self._send_high_water: int
        self._send_low_water: int

        self._env: Dict[bytes, bytes] = {}
        self._str_env: Optional[Dict[str, str]] = None

        self._command: Optional[str] = None
        self._subsystem: Optional[str] = None

        self._send_state = 'closed'
        self._send_chan: Optional[int] = None
        self._send_window: int = 0
        self._send_pktsize: int = 0
        self._send_paused = False
        self._send_buf: List[Tuple[bytearray, DataType]] = []
        self._send_buf_len = 0

        self._recv_state = 'closed'
        self._init_recv_window = window
        self._recv_window = window
        self._recv_pktsize = max_pktsize
        self._recv_paused: Union[bool, str] = 'starting'
        self._recv_buf: List[Tuple[bytes, DataType]] = []

        self._request_queue: List[Tuple[str, SSHPacket, bool]] = []

        self._open_waiter: Optional[asyncio.Future[SSHPacket]] = None
        self._request_waiters: List[asyncio.Future[bool]] = []

        self._close_event = asyncio.Event()

        self._recv_chan: Optional[int] = conn.add_channel(self)

        self.set_encoding(encoding, errors)
        self.set_write_buffer_limits()

    def get_connection(self) -> 'SSHConnection':
        """Return the connection used by this channel"""

        assert self._conn is not None
        return self._conn

    def get_loop(self) -> asyncio.AbstractEventLoop:
        """Return the event loop used by this channel"""

        return self._loop

    def get_encoding(self) -> Tuple[Optional[str], str]:
        """Return the encoding used by this channel"""

        return self._encoding, self._errors

    def set_encoding(self, encoding: Optional[str],
                     errors: str = 'strict') -> None:
        """Set the encoding on this channel"""

        self._encoding = encoding
        self._errors = errors

        if encoding:
            self._encoder: Optional[codecs.IncrementalEncoder] = \
                codecs.getincrementalencoder(encoding)(errors)
            self._decoder: Optional[codecs.IncrementalDecoder] = \
                codecs.getincrementaldecoder(encoding)(errors)
        else:
            self._encoder = None
            self._decoder = None

    def get_recv_window(self) -> int:
        """Return the configured receive window for this channel"""

        return self._init_recv_window

    def get_read_datatypes(self) -> Set[int]:
        """Return the legal read data types for this channel"""

        return self._read_datatypes

    def get_write_datatypes(self) -> Set[int]:
        """Return the legal write data types for this channel"""

        return self._write_datatypes

    def _cleanup(self, exc: Optional[Exception] = None) -> None:
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

    def _flush_recv_buf(self, exc: Optional[Exception] = None) -> None:
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
                decoded_data = self._decoder.decode(data)
            except UnicodeDecodeError as unicode_exc:
                raise ProtocolError(str(unicode_exc)) from None
        else:
            decoded_data = data

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
        handler: _RequestHandler = getattr(self, name, None)

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

    def process_connection_close(self, exc: Optional[Exception]) -> None:
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
            Tuple['SSHChannel[AnyStr]', SSHSession[AnyStr]]:
        """Hook to optionally wrap channel and session objects"""

        # By default, return the original channel and session objects
        return self, session

    async def _finish_open_request(
            self, result: MaybeAwait[SSHSession[AnyStr]]) -> None:
        """Finish processing a channel open request"""

        try:
            if inspect.isawaitable(result):
                session: Awaitable[SSHSession[AnyStr]] = result
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
                            *args: bytes) -> Optional[bool]:
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

    def set_write_buffer_limits(self, high: Optional[int] = None,
                                low: Optional[int] = None) -> None:
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
            data = ''.join(list_of_data)
        else:
            data = b''.join(list_of_data)

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

    def get_command(self) -> Optional[str]:
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

    def get_subsystem(self) -> Optional[str]:
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


class SSHClientChannel(SSHChannel, Generic[AnyStr]):
    """SSH client channel"""

    _conn: 'SSHClientConnection'
    _session: SSHClientSession[AnyStr]

    _read_datatypes = {EXTENDED_DATA_STDERR}

    def __init__(self, conn: 'SSHClientConnection',
                 loop: asyncio.AbstractEventLoop, encoding: Optional[str],
                 errors: str, window: int, max_pktsize: int):
        super().__init__(conn, loop, encoding, errors, window, max_pktsize)

        self._exit_status: Optional[int] = None
        self._exit_signal: Optional[_ExitSignal] = None

    async def create(self, session_factory: SSHClientSessionFactory[AnyStr],
                     command: Optional[str], subsystem: Optional[str],
                     env: Dict[bytes, bytes], request_pty: bool,
                     term_type: Optional[str], term_size: TermSizeArg,
                     term_modes: TermModes, x11_forwarding: Union[bool, str],
                     x11_display: Optional[str], x11_auth_path: Optional[str],
                     x11_single_connection: bool,
                     agent_forwarding: bool) -> SSHClientSession[AnyStr]:
        """Create an SSH client session"""

        packet = await self._open(b'session')

        # Client sessions should have no extra data in the open confirmation
        packet.check_end()

        self._session = session_factory()
        self._session.connection_made(self)

        self._env = env
        self._command = command
        self._subsystem = subsystem

        for key, value in env.items():
            if not isinstance(key, (bytes, str)):
                key = str(key)

            if not isinstance(value, (bytes, str)):
                value = str(value)

            self._send_request(b'env', String(key), String(value))

        if request_pty:
            if not term_size:
                width = height = pixwidth = pixheight = 0
            elif len(term_size) == 2:
                width, height = term_size
                pixwidth = pixheight = 0

            elif len(term_size) == 4:
                width, height, pixwidth, pixheight = term_size

            else:
                raise ValueError('If set, terminal size must be a tuple of '
                                 '2 or 4 integers')

            modes = b''
            for mode, mode_value in term_modes.items():
                if mode <= PTY_OP_END or mode >= PTY_OP_RESERVED:
                    raise ValueError(f'Invalid pty mode: {mode}')

                modes += Byte(mode) + UInt32(mode_value)

            modes += Byte(PTY_OP_END)

            if not (await self._make_request(b'pty-req',
                                             String(term_type or ''),
                                             UInt32(width), UInt32(height),
                                             UInt32(pixwidth),
                                             UInt32(pixheight),
                                             String(modes))):
                self.close()
                raise ChannelOpenError(OPEN_REQUEST_PTY_FAILED,
                                       'PTY request failed')

        if x11_forwarding:
            try:
                attach_result: Optional[Tuple[bytes, bytes, int]] = \
                    await self._conn.attach_x11_listener(
                        self, x11_display, x11_auth_path, x11_single_connection)
            except ValueError as exc:
                if x11_forwarding != 'ignore_failure':
                    raise ChannelOpenError(OPEN_REQUEST_X11_FORWARDING_FAILED,
                                           str(exc)) from None
                else:
                    attach_result = None

            if attach_result:
                auth_proto, remote_auth, screen = attach_result

                result = await self._make_request(
                    b'x11-req', Boolean(x11_single_connection),
                    String(auth_proto), String(binascii.b2a_hex(remote_auth)),
                    UInt32(screen))

                if not result:
                    if self._conn: # pragma: no branch
                        self._conn.detach_x11_listener(self)

                    if x11_forwarding != 'ignore_failure':
                        raise ChannelOpenError(
                            OPEN_REQUEST_X11_FORWARDING_FAILED,
                            'X11 forwarding request failed')

        if agent_forwarding:
            self._send_request(b'auth-agent-req@openssh.com')

        if command:
            result = await self._make_request(b'exec', String(command))
        elif subsystem:
            result = await self._make_request(b'subsystem', String(subsystem))
        else:
            result = await self._make_request(b'shell')

        if not result:
            self.close()
            raise ChannelOpenError(OPEN_REQUEST_SESSION_FAILED,
                                   'Session request failed')

        self._session.session_started()
        self._conn.create_task(self._start_reading())

        return self._session

    def _process_xon_xoff_request(self, packet: SSHPacket) -> bool:
        """Process a request to set up XON/XOFF processing"""

        client_can_do = packet.get_boolean()
        packet.check_end()

        self._session.xon_xoff_requested(client_can_do)
        return True

    def _process_exit_status_request(self, packet: SSHPacket) -> bool:
        """Process a request to deliver exit status"""

        status = packet.get_uint32() & 0xff
        packet.check_end()

        self._exit_status = status
        self._session.exit_status_received(status)
        return True

    def _process_exit_signal_request(self, packet: SSHPacket) -> bool:
        """Process a request to deliver an exit signal"""

        signal_bytes = packet.get_string()
        core_dumped = packet.get_boolean()
        msg_bytes = packet.get_string()
        lang_bytes = packet.get_string()
        packet.check_end()

        try:
            signal = signal_bytes.decode('ascii')
            msg = msg_bytes.decode('utf-8')
            lang = lang_bytes.decode('ascii')
        except UnicodeDecodeError:
            raise ProtocolError('Invalid exit signal request') from None

        self._exit_signal = (signal, core_dumped, msg, lang)
        self._session.exit_signal_received(signal, core_dumped, msg, lang)
        return True

    def get_exit_status(self) -> Optional[int]:
        """Return the session's exit status

           This method returns the exit status of the session if one has
           been sent. If an exit signal was sent, this method returns -1
           and the exit signal information can be collected by calling
           :meth:`get_exit_signal`. If neither has been sent, this method
           returns `None`.

        """

        if self._exit_status is not None:
            return self._exit_status
        elif self._exit_signal:
            return -1
        else:
            return None

    def get_exit_signal(self) -> Optional[_ExitSignal]:
        """Return the session's exit signal, if one was sent

           This method returns information about the exit signal sent on
           this session. If an exit signal was sent, a tuple is returned
           containing the signal name, a boolean for whether a core dump
           occurred, a message associated with the signal, and the language
           the message was in. Otherwise, this method returns `None`.

        """

        return self._exit_signal

    def get_returncode(self) -> Optional[int]:
        """Return the session's exit status or signal

           This method returns the exit status of the session if one has
           been sent. If an exit signal was sent, this method returns
           the negative of the numeric value of that signal, matching
           the behavior of :meth:`asyncio.SubprocessTransport.get_returncode`.
           If neither has been sent, this method returns `None`.

           :returns: `int` or `None`

        """

        if self._exit_status is not None:
            return self._exit_status
        elif self._exit_signal:
            return -_signal_numbers.get(self._exit_signal[0], 99)
        else:
            return None

    def change_terminal_size(self, width: int, height: int,
                             pixwidth: int = 0, pixheight: int = 0) -> None:
        """Change the terminal window size for this session

           This method changes the width and height of the terminal
           associated with this session.

           :param width:
               The width of the terminal in characters
           :param height:
               The height of the terminal in characters
           :param pixwidth: (optional)
               The width of the terminal in pixels
           :param pixheight: (optional)
               The height of the terminal in pixels
           :type width: `int`
           :type height: `int`
           :type pixwidth: `int`
           :type pixheight: `int`

        """

        self._send_request(b'window-change', UInt32(width), UInt32(height),
                           UInt32(pixwidth), UInt32(pixheight))

    def send_break(self, msec: int) -> None:
        """Send a break to the remote process

           This method requests that the server perform a break
           operation on the remote process or service as described in
           :rfc:`4335`.

           :param msec:
               The duration of the break in milliseconds
           :type msec: `int`

           :raises: :exc:`OSError` if the channel is not open

        """
        self._send_request(b'break', UInt32(msec))

    def send_signal(self, signal: Union[str, int]) -> None:
        """Send a signal to the remote process

           This method can be called to deliver a signal to the remote
           process or service. Signal names should be as described in
           section 6.10 of :rfc:`RFC 4254 <4254#section-6.10>`, or
           can be integer values as defined in the :mod:`signal`
           module, in which case they will be translated to their
           corresponding signal name before being sent.

           .. note:: OpenSSH's SSH server implementation prior to version
                     7.9 does not support this message, so attempts to
                     use :meth:`send_signal`, :meth:`terminate`, or
                     :meth:`kill` with an older OpenSSH SSH server will
                     end up being ignored. This was tracked in OpenSSH
                     `bug 1424`__.

                     __ https://bugzilla.mindrot.org/show_bug.cgi?id=1424

           :param signal:
               The signal to deliver
           :type signal: `str` or `int`

           :raises: | :exc:`OSError` if the channel is not open
                    | :exc:`ValueError` if the signal number is unknown

        """

        if isinstance(signal, int):
            try:
                signal = _signal_names[signal]
            except KeyError:
                raise ValueError(f'Unknown signal: {signal}') from None

        self._send_request(b'signal', String(signal))

    def terminate(self) -> None:
        """Terminate the remote process

           This method can be called to terminate the remote process or
           service by sending it a `TERM` signal.

           :raises: :exc:`OSError` if the channel is not open

           .. note:: If your server-side runs on OpenSSH,
                     this might be ineffective;
                     for more details, see the note in
                     :meth:`send_signal`

        """

        self.send_signal('TERM')

    def kill(self) -> None:
        """Forcibly kill the remote process

           This method can be called to forcibly stop the remote process
           or service by sending it a `KILL` signal.

           :raises: :exc:`OSError` if the channel is not open

           .. note:: If your server-side runs on OpenSSH,
                     this might be ineffective;
                     for more details, see the note in
                     :meth:`send_signal`

        """

        self.send_signal('KILL')

class SSHForwardChannel(SSHChannel, Generic[AnyStr]):
    """SSH channel for forwarding TCP and UNIX domain connections"""

    async def _finish_open_request(
            self, result: MaybeAwait[SSHSession[AnyStr]]) -> None:
        """Finish processing a forward channel open request"""

        await super()._finish_open_request(result)

        if self._session is not None:
            self._session.session_started()
            self.resume_reading()

    async def _open_forward(self, session_factory: SSHSessionFactory[AnyStr],
                            chantype: bytes, *args: bytes) -> \
            SSHSession[AnyStr]:
        """Open a forward channel"""

        packet = await super()._open(chantype, *args)

        # Forward channels should have no extra data in the open confirmation
        packet.check_end()

        self._session = session_factory()
        self._session.connection_made(self)
        self._session.session_started()

        assert self._conn is not None
        self._conn.create_task(self._start_reading())

        return self._session


class SSHTCPChannel(SSHForwardChannel, Generic[AnyStr]):
    """SSH TCP channel"""

    async def _open_tcp(self, session_factory: SSHTCPSessionFactory[AnyStr],
                        chantype: bytes, host: str, port: int, orig_host: str,
                        orig_port: int) -> SSHTCPSession[AnyStr]:
        """Open a TCP channel"""

        self.set_extra_info(peername=('', 0),
                            local_peername=(orig_host, orig_port),
                            remote_peername=(host, port))

        return await self._open_forward(session_factory, chantype,
                                             String(host), UInt32(port),
                                             String(orig_host),
                                             UInt32(orig_port))

    async def connect(self, session_factory: SSHTCPSessionFactory[AnyStr],
                     host: str, port: int, orig_host: str, orig_port: int) -> \
            SSHTCPSession[AnyStr]:
        """Create a new outbound TCP session"""

        return await self._open_tcp(session_factory, b'direct-tcpip',
                                    host, port, orig_host, orig_port)

    async def accept(self, session_factory: SSHTCPSessionFactory[AnyStr],
                     host: str, port: int, orig_host: str,
                     orig_port: int) -> SSHTCPSession[AnyStr]:
        """Create a new forwarded TCP session"""

        return await self._open_tcp(session_factory, b'forwarded-tcpip',
                                    host, port, orig_host, orig_port)

    def set_inbound_peer_names(self, dest_host: str, dest_port: int,
                               orig_host: str, orig_port: int) -> None:
        """Set local and remote peer names for inbound connections"""

        self.set_extra_info(peername=('', 0),
                            local_peername=(dest_host, dest_port),
                            remote_peername=(orig_host, orig_port))


class SSHUNIXChannel(SSHForwardChannel, Generic[AnyStr]):
    """SSH UNIX channel"""

    async def _open_unix(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                         chantype: bytes, path: str,
                          *args: bytes) -> SSHUNIXSession[AnyStr]:
        """Open a UNIX channel"""

        self.set_extra_info(local_peername='', remote_peername=path)

        return await self._open_forward(session_factory, chantype,
                                             String(path), *args)

    async def connect(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                      path: str) -> SSHUNIXSession[AnyStr]:
        """Create a new outbound UNIX session"""

        # OpenSSH appears to have a bug which requires an originator
        # host and port to be sent after the path name to connect to
        # when opening a direct streamlocal channel.
        return await self._open_unix(session_factory,
                                     b'direct-streamlocal@openssh.com',
                                     path, String(''), UInt32(0))

    async def accept(self, session_factory: SSHUNIXSessionFactory[AnyStr],
                     path: str) -> SSHUNIXSession[AnyStr]:
        """Create a new forwarded UNIX session"""

        return await self._open_unix(session_factory,
                                     b'forwarded-streamlocal@openssh.com',
                                     path, String(''))

    def set_inbound_peer_names(self, dest_path: str) -> None:
        """Set local and remote peer names for inbound connections"""

        self.set_extra_info(local_peername=dest_path, remote_peername='')


class SSHTunTapChannel(SSHForwardChannel[bytes]):
    """SSH TunTap channel"""

    def __init__(self, conn: 'SSHConnection',
                 loop: asyncio.AbstractEventLoop, encoding: Optional[str],
                 errors: str, window: int, max_pktsize: int):
        super().__init__(conn, loop, encoding, errors, window, max_pktsize)

        self._mode: Optional[int] = None

    def _accept_data(self, data: bytes, datatype: DataType = None) -> None:
        """Strip off address family on incoming packets in TUN mode"""

        if self._mode == SSH_TUN_MODE_POINTTOPOINT:
            data = data[4:]

        super()._accept_data(data, datatype)

    def write(self, data: bytes, datatype: DataType = None) -> None:
        """Add address family in outbound packets in TUN mode"""

        if self._mode == SSH_TUN_MODE_POINTTOPOINT:
            version = data[0] >> 4
            family = SSH_TUN_AF_INET if version == 4 else SSH_TUN_AF_INET6
            data = UInt32(family) + data

        super().write(data, datatype)

    async def open(self, session_factory: SSHTunTapSessionFactory,
                   mode: int, unit: Optional[int]) -> SSHTunTapSession:
        """Open a TUN/TAP channel"""

        self._mode = mode

        if unit is None:
            unit = SSH_TUN_UNIT_ANY

        return await self._open_forward(session_factory,
                                             b'tun@openssh.com',
                                             UInt32(mode), UInt32(unit))

    def set_mode(self, mode: int) -> None:
        """Set mode for inbound connections"""

        self._mode = mode


class SSHX11Channel(SSHForwardChannel[bytes]):
    """SSH X11 channel"""

    async def open(self, session_factory: SSHTCPSessionFactory[bytes],
                   orig_host: str, orig_port: int) -> SSHTCPSession[bytes]:
        """Open an SSH X11 channel"""

        self.set_extra_info(local_peername=(orig_host, orig_port),
                            remote_peername=('', 0))

        return await self._open_forward(session_factory, b'x11',
                                             String(orig_host),
                                             UInt32(orig_port))

    def set_inbound_peer_names(self, orig_host: str, orig_port: int) -> None:
        """Set local and remote peer name for inbound connections"""

        self.set_extra_info(local_peername=('', 0),
                            remote_peername=(orig_host, orig_port))


class SSHAgentChannel(SSHForwardChannel[bytes]):
    """SSH agent channel"""

    async def open(self, session_factory: SSHUNIXSessionFactory[bytes]) -> \
            SSHUNIXSession[bytes]:
        """Open an SSH agent channel"""

        return await self._open_forward(session_factory,
                                             b'auth-agent@openssh.com')
