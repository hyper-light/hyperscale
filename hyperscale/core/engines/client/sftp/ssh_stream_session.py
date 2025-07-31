import asyncio
import re
from re import Pattern
from typing import AnyStr, AsyncIterator, Generic, TYPE_CHECKING
from .constants import NEWLINE
from .error import SoftEOFReceived
from .ssh_channel import SSHChannel
from .types import (
    DataType,
    ReadLocks,
    ReadWaiters,
    RecvBufMap,
    DrainWaiters,
)
from .ssh_client_session import SSHClientSession

if TYPE_CHECKING:
    from .ssh_connection import SSHConnection


class SSHStreamSession(Generic[AnyStr]):
    """SSH stream session handler"""

    def __init__(self) -> None:
        self._chan: SSHChannel[AnyStr] | None = None
        self._conn: SSHConnection | None = None
        self._encoding: str | None = None
        self._errors = 'strict'
        self._loop: asyncio.AbstractEventLoop | None = None
        self._limit = 0
        self._exception: Exception | None = None
        self._eof_received = False
        self._connection_lost = False
        self._read_paused = False
        self._write_paused = False
        self._recv_buf_len = 0
        self._recv_buf: RecvBufMap[AnyStr] = {None: []}
        self._read_locks: ReadLocks = {None: asyncio.Lock()}
        self._read_waiters: ReadWaiters = {None: None}
        self._drain_waiters: DrainWaiters = {None: set()}

    async def aiter(self, datatype: DataType) -> AsyncIterator[AnyStr]:
        """Allow SSHReader to be an async iterator"""

        while not self.at_eof(datatype):
            yield await self.readline(datatype)

    async def _block_read(self, datatype: DataType) -> None:
        """Wait for more data to arrive on the stream"""

        try:
            assert self._loop is not None
            waiter = self._loop.create_future()
            self._read_waiters[datatype] = waiter
            await waiter
        finally:
            self._read_waiters[datatype] = None

    def _unblock_read(self, datatype: DataType) -> None:
        """Signal that more data has arrived on the stream"""

        waiter = self._read_waiters[datatype]
        if waiter and not waiter.done():
            waiter.set_result(None)

    def _should_block_drain(self, datatype: DataType) -> bool:
        """Return whether output is still being written to the channel"""

        # pylint: disable=unused-argument

        return self._write_paused and not self._connection_lost

    def _unblock_drain(self, datatype: DataType) -> None:
        """Signal that more data can be written on the stream"""

        if not self._should_block_drain(datatype):
            for waiter in self._drain_waiters[datatype]:
                if not waiter.done(): # pragma: no branch
                    waiter.set_result(None)

    def _should_pause_reading(self) -> bool:
        """Return whether to pause reading from the channel"""

        return bool(self._limit) and self._recv_buf_len >= self._limit

    def _maybe_pause_reading(self) -> bool:
        """Pause reading if necessary"""

        if not self._read_paused and self._should_pause_reading():
            assert self._chan is not None
            self._read_paused = True
            self._chan.pause_reading()
            return True
        else:
            return False

    def _maybe_resume_reading(self) -> bool:
        """Resume reading if necessary"""

        if self._read_paused and not self._should_pause_reading():
            assert self._chan is not None
            self._read_paused = False
            self._chan.resume_reading()
            return True
        else:
            return False

    def connection_made(self, chan: SSHChannel[AnyStr]) -> None:
        """Handle a newly opened channel"""

        self._chan = chan
        self._conn = chan.get_connection()
        self._encoding, self._errors = chan.get_encoding()
        self._loop = chan.get_loop()
        self._limit = self._chan.get_recv_window()

        for datatype in chan.get_read_datatypes():
            self._recv_buf[datatype] = []
            self._read_locks[datatype] = asyncio.Lock()
            self._read_waiters[datatype] = None

        for datatype in chan.get_write_datatypes():
            self._drain_waiters[datatype] = set()

    def connection_lost(self, exc: Exception | None) -> None:
        """Handle an incoming channel close"""

        self._connection_lost = True
        self._exception = exc

        if not self._eof_received:
            if exc:
                for datatype in self._read_waiters:
                    self._recv_buf[datatype].append(exc)

            self.eof_received()

        for datatype in self._drain_waiters:
            self._unblock_drain(datatype)

    def data_received(self, data: AnyStr, datatype: DataType) -> None:
        """Handle incoming data on the channel"""

        self._recv_buf[datatype].append(data)
        self._recv_buf_len += len(data)
        self._unblock_read(datatype)
        self._maybe_pause_reading()

    def eof_received(self) -> bool:
        """Handle an incoming end of file on the channel"""

        self._eof_received = True

        for datatype in self._read_waiters:
            self._unblock_read(datatype)

        return True

    def at_eof(self, datatype: DataType) -> bool:
        """Return whether end of file has been received on the channel"""

        return self._eof_received and not self._recv_buf[datatype]

    def pause_writing(self) -> None:
        """Handle a request to pause writing on the channel"""

        self._write_paused = True

    def resume_writing(self) -> None:
        """Handle a request to resume writing on the channel"""

        self._write_paused = False

        for datatype in self._drain_waiters:
            self._unblock_drain(datatype)

    async def read(self, datatype: DataType, n: int, exact: bool) -> AnyStr:
        """Read data from the channel"""

        recv_buf = self._recv_buf[datatype]
        data: list[AnyStr] = []
        break_read = False

        async with self._read_locks[datatype]:
            while True:
                while recv_buf and n != 0:
                    if isinstance(recv_buf[0], Exception):
                        if data:
                            break_read = True
                            break
                        else:
                            exc: Exception = recv_buf.pop(0)

                            if isinstance(exc, SoftEOFReceived):
                                n = 0
                                break
                            else:
                                raise exc

                    l = len(recv_buf[0])

                    if l > n > 0:
                        data.append(recv_buf[0][:n])
                        recv_buf[0] = recv_buf[0][n:]
                        self._recv_buf_len -= n
                        n = 0
                        break

                    data.append(recv_buf.pop(0))
                    self._recv_buf_len -= l
                    n -= l

                if self._maybe_resume_reading():
                    continue

                if n == 0 or (n > 0 and data and not exact) or \
                        (n < 0 and recv_buf) or \
                        self._eof_received or break_read:
                    break

                await self._block_read(datatype)

        result = (
            '' if self._encoding else b''
        ).join(data)

        if n > 0 and exact:
            raise asyncio.IncompleteReadError(result,
                                              len(result) + n)

        return result

    async def readline(self, datatype: DataType) -> AnyStr:
        """Read one line from the stream"""

        try:
            return await self.readuntil(NEWLINE, datatype)
        except asyncio.IncompleteReadError as exc:
            return exc.partial

    async def readuntil(self, separator: object, datatype: DataType,
                        max_separator_len = 0) -> AnyStr:
        """Read data from the channel until a separator is seen"""

        if not separator:
            raise ValueError('Separator cannot be empty')

        buf = '' if self._encoding else b''
        recv_buf = self._recv_buf[datatype]

        if separator is NEWLINE:
            seplen = 1
            separators = '\n' if self._encoding else b'\n'
            pat = re.compile(separators)
        elif isinstance(separator, (bytes, str)):
            seplen = len(separator)
            pat = re.compile(re.escape(separator))
        elif isinstance(separator, Pattern):
            seplen = max_separator_len
            pat = separator
        else:
            bar = '|' if self._encoding else b'|'
            seplist = list(separator)
            seplen = max(len(sep) for sep in seplist)
            separators = bar.join(re.escape(sep) for sep in seplist)
            pat = re.compile(separators)

        curbuf = 0
        buflen = 0

        async with self._read_locks[datatype]:
            while True:
                while curbuf < len(recv_buf):
                    if isinstance(recv_buf[curbuf], Exception):
                        if buf:
                            recv_buf[:curbuf] = []
                            self._recv_buf_len -= buflen
                            raise asyncio.IncompleteReadError(
                                buf, None)
                        else:
                            exc = recv_buf.pop(0)

                            if isinstance(exc, SoftEOFReceived):
                                return buf
                            else:
                                raise exc

                    newbuf: AnyStr = recv_buf[curbuf]
                    buf += newbuf
                    start = 0 if seplen == 0 else max(buflen + 1 - seplen, 0)

                    match = pat.search(buf, start)
                    if match:
                        idx = match.end()
                        recv_buf[:curbuf] = []
                        recv_buf[0] = buf[idx:]
                        buf = buf[:idx]
                        self._recv_buf_len -= idx

                        if not recv_buf[0]:
                            recv_buf.pop(0)

                        self._maybe_resume_reading()
                        return buf

                    buflen += len(newbuf)
                    curbuf += 1

                if self._read_paused or self._eof_received:
                    recv_buf[:curbuf] = []
                    self._recv_buf_len -= buflen
                    self._maybe_resume_reading()
                    raise asyncio.IncompleteReadError(buf, None)

                await self._block_read(datatype)

    async def drain(self, datatype: DataType) -> None:
        """Wait for data written to the channel to drain"""

        while self._should_block_drain(datatype):
            try:
                assert self._loop is not None
                waiter = self._loop.create_future()
                self._drain_waiters[datatype].add(waiter)
                await waiter
            finally:
                self._drain_waiters[datatype].remove(waiter)

        if self._connection_lost:
            exc = self._exception

            if not exc and self._write_paused:
                exc = BrokenPipeError()

            if exc:
                raise exc


class SSHClientStreamSession(SSHStreamSession[AnyStr],
                             SSHClientSession[AnyStr]):
    """SSH client stream session handler"""
