# Copyright (c) 2017-2025 by Ron Frederick <ronf@timeheart.net> and others.
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
#     Jonathan Slenders - proposed changes to allow SFTP server callbacks
#                         to be coroutines

"""SCP handlers"""

import asyncio
from pathlib import PurePath
import posixpath
from types import TracebackType
from typing import (
    TYPE_CHECKING, 
    AsyncIterator, 
    List,
    Optional,
    Iterable,
    Tuple, 
    Type, 
    Union, 
    Any,
    Protocol, 
    Self,
)


from hyperscale.core.engines.client.ssh.protocol.constants import DEFAULT_LANG
from hyperscale.core.engines.client.ssh.protocol.connection import connect
from hyperscale.core.engines.client.ssh.protocol.misc import BytesOrStr, FilePath, HostPort
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPErrorHandler


if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnection
    from hyperscale.core.engines.client.ssh.protocol.stream import SSHReader, SSHWriter


_SCPConn = Union[None, bytes, str, HostPort, 'SSHClientConnection']
_SCPPath = Union[bytes, FilePath]
SCPConnPath = Union[Tuple[_SCPConn, _SCPPath], _SCPConn, _SCPPath]


SCP_BLOCK_SIZE = 256*1024    # 256 KiB


class SCPFileProtocol(Protocol):
    """Protocol for accessing a file during an SCP copy"""

    async def read(self, size: int, offset: int) -> bytes:
        """Read data from the local file"""

    async def write(self, data: bytes, offset: int) -> int:
        """Write data to the local file"""

    async def close(self) -> None:
        """Close the local file"""


def _scp_error(exc_class: Type[Exception], reason: BytesOrStr,
               path: Optional[bytes] = None, fatal: bool = False,
               suppress_send: bool = False,
               lang: str = DEFAULT_LANG) -> Exception:
    """Construct SCP version of SFTPError exception"""

    if isinstance(reason, bytes):
        reason = reason.decode('utf-8', errors='replace')

    if path:
        reason = reason + ': ' + path.decode('utf-8', errors='replace')

    exc = exc_class(reason, lang)

    setattr(exc, 'fatal', fatal)
    setattr(exc, 'suppress_send', suppress_send)

    return exc


def _parse_cd_args(args: bytes) -> Tuple[int, int, bytes]:
    """Parse arguments to an SCP copy or dir request"""

    try:
        permissions, size, name = args.split(None, 2)
        return int(permissions, 8), int(size), name
    except ValueError:
        raise _scp_error(SFTPBadMessage,
                         'Invalid copy or dir request') from None


def _parse_t_args(args: bytes) -> Tuple[int, int]:
    """Parse argument to an SCP time request"""

    try:
        mtime, _, atime, _ = args.split()
        return int(atime), int(mtime)
    except ValueError:
        raise _scp_error(SFTPBadMessage, 'Invalid time request') from None


async def parse_path(path: SCPConnPath, **kwargs) -> \
        Tuple[Optional['SSHClientConnection'], _SCPPath, bool]:
    """Convert an SCP path into an SSHClientConnection and path"""

    # pylint: disable=cyclic-import,import-outside-toplevel

    conn: _SCPConn

    if isinstance(path, tuple):
        conn, path = path
    elif isinstance(path, str) and ':' in path:
        conn, path = path.split(':', 1)
    elif isinstance(path, bytes) and b':' in path:
        conn, path = path.split(b':', 1)
        conn = conn.decode('utf-8')
    elif isinstance(path, (bytes, str, PurePath)):
        conn = None
    else:
        conn = path
        path = b'.'

    if isinstance(conn, str):
        close_conn = True
        conn = await connect(conn, **kwargs,)
    elif isinstance(conn, tuple):
        close_conn = True
        conn = await connect(*conn, **kwargs)
    else:
        close_conn = False

    return (
        conn,
        path, 
        close_conn,
    )


class SCPHandler:
    """SCP handler"""

    def __init__(self, reader: 'SSHReader[bytes]', writer: 'SSHWriter[bytes]',
                 error_handler: SFTPErrorHandler = None, server: bool = False):
        self._reader = reader
        self.writer = writer
        self._error_handler = error_handler
        self._server = server

    async def await_response(self) -> Optional[Exception]:
        """Wait for an SCP response"""

        result = await self._reader.read(1)

        if result != b'\0':
            reason = await self._reader.readline()

            if not result or not reason.endswith(b'\n'):
                raise _scp_error(SFTPConnectionLost, 'Connection lost',
                                 fatal=True, suppress_send=True)

            if result not in b'\x01\x02':
                reason = result + reason

            return _scp_error(SFTPFailure, reason[:-1],
                              fatal=result != b'\x01', suppress_send=True)

        return None

    def send_request(self, *args: bytes) -> None:
        """Send an SCP request"""

        request = b''.join(args)

        self.writer.write(request + b'\n')

    async def make_request(self, *args: bytes) -> None:
        """Send an SCP request and wait for a response"""

        self.send_request(*args)

        exc = await self.await_response()

        if exc:
            raise exc

    def send_ok(self) -> None:
        """Send an SCP OK response"""

        self.writer.write(b'\0')

    def send_error(self, exc: Exception) -> None:
        """Send an SCP error response"""

        if isinstance(exc, SFTPError):
            reason = exc.reason.encode('utf-8')
        elif isinstance(exc, OSError): # pragma: no branch (win32)
            reason = exc.strerror.encode('utf-8')

            filename: bytes | str = exc.filename

            if filename:
                if isinstance(filename, str): # pragma: no cover (win32)
                    filename = filename.encode('utf-8')

                reason += b': ' + filename
        else: # pragma: no cover (win32)
            reason = str(exc).encode('utf-8')

        fatal: bool = getattr(exc, 'fatal', False)

        self.writer.write((b'\x02' if fatal else b'\x01') +
                           b'scp: ' + reason + b'\n')

    async def recv_request(self) -> Tuple[Optional[bytes], Optional[bytes]]:
        """Receive SCP request"""

        request = await self._reader.readline()

        if not request:
            return None, None

        action, args = request[:1], request[1:-1]

        return action, args


    async def recv_data(self, n: int) -> bytes:
        """Receive SCP file data"""
        return await self._reader.read(n)

    def handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = _scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        if not getattr(exc, 'suppress_send', False):
            self.send_error(exc)

        if self._error_handler and not getattr(exc, 'fatal', False):
            self._error_handler(exc)
        elif not self._server:
            raise exc

    async def close(self, cancelled: bool = False) -> None:
        """Close an SCP session"""

        if cancelled:
            self.writer.channel.abort()
        else:
            self.writer.close()

            await self.writer.wait_closed()

