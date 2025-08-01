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

import argparse
import asyncio
from pathlib import PurePath
import posixpath
import shlex
from types import TracebackType
from typing import TYPE_CHECKING, AsyncIterator, List, NoReturn, Optional
from typing import Sequence, Tuple, Type, Union, cast
from typing_extensions import Protocol, Self

from hyperscale.core.engines.client.ssh.protocol.constants import DEFAULT_LANG
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.ssh.protocol.misc import BytesOrStr, FilePath, HostPort
from hyperscale.core.engines.client.ssh.protocol.misc import async_context_manager
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPAttrs, SFTPGlob, SFTPName
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from hyperscale.core.engines.client.sftp.protocol.sftp import SFTPErrorHandler, SFTPProgressHandler


if TYPE_CHECKING:
    # pylint: disable=cyclic-import
    from hyperscale.core.engines.client.ssh.protocol.connection import SSHClientConnection
    from hyperscale.core.engines.client.ssh.protocol.stream import SSHReader, SSHWriter


_SCPConn = Union[None, bytes, str, HostPort, 'SSHClientConnection']
_SCPPath = Union[bytes, FilePath]
SCPConnPath = Union[Tuple[_SCPConn, _SCPPath], _SCPConn, _SCPPath]


SCP_BLOCK_SIZE = 256*1024    # 256 KiB


class _SCPFileProtocol(Protocol):
    """Protocol for accessing a file during an SCP copy"""

    async def __aenter__(self) -> Self:
        """Allow _SCPFileProtocol to be used as an async context manager"""

    async def __aexit__(self, _exc_type: Optional[Type[BaseException]],
                        _exc_value: Optional[BaseException],
                        _traceback: Optional[TracebackType]) -> bool:
        """Wait for file close when used as an async context manager"""

    async def read(self, size: int, offset: int) -> bytes:
        """Read data from the local file"""

    async def write(self, data: bytes, offset: int) -> int:
        """Write data to the local file"""

    async def close(self) -> None:
        """Close the local file"""


class _SCPFSProtocol(Protocol):
    """Protocol for accessing a filesystem during an SCP copy"""

    @staticmethod
    def basename(path: bytes) -> bytes:
        """Return the final component of a POSIX-style path"""

    async def stat(self, path: bytes) -> 'SFTPAttrs':
        """Get attributes of a file or directory, following symlinks"""

    async def setstat(self, path: bytes, attrs: 'SFTPAttrs') -> None:
        """Set attributes of a file or directory"""

    async def exists(self, path: bytes) -> bool:
        """Return if a path exists"""

    async def isdir(self, path: bytes) -> bool:
        """Return if the path refers to a directory"""

    def scandir(self, path: bytes) -> AsyncIterator[SFTPName]:
        """Read the names and attributes of files in a directory"""

    async def mkdir(self, path: bytes) -> None:
        """Create a directory"""

    @async_context_manager
    async def open(self, path: bytes, mode: str) -> _SCPFileProtocol:
        """Open a file"""


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
    from . import connect

    conn: _SCPConn

    if isinstance(path, tuple):
        conn, path = cast(Tuple[_SCPConn, _SCPPath], path)
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
        conn = await connect(conn, **kwargs)
    elif isinstance(conn, tuple):
        close_conn = True
        conn = await connect(*conn, **kwargs)
    else:
        close_conn = False

    return (cast(Optional['SSHClientConnection'], conn),
            cast(_SCPPath, path), close_conn)


async def start_remote(conn: 'SSHClientConnection', source: bool,
                        must_be_dir: bool, preserve: bool,
                        recurse: bool, path: _SCPPath) -> \
        Tuple['SSHReader[bytes]', 'SSHWriter[bytes]']:
    """Start remote SCP server"""

    if isinstance(path, PurePath):
        path = str(path)

    if isinstance(path, str):
        path = path.encode('utf-8')

    command = (b'scp ' + (b'-f ' if source else b'-t ') +
               (b'-d ' if must_be_dir else b'') +
               (b'-p ' if preserve else b'') +
               (b'-r ' if recurse else b'') + path)

    writer, reader, _ = await conn.open_session(command, encoding=None)

    return reader, writer


class _SCPArgs(argparse.Namespace):
    """SCP command line arguments"""

    path: str
    source: bool
    must_be_dir: bool
    preserve: bool
    recurse: bool


class _SCPArgParser(argparse.ArgumentParser):
    """A parser for SCP arguments"""

    def __init__(self) -> None:
        super().__init__(add_help=False)

        group = self.add_mutually_exclusive_group(required=True)
        group.add_argument('-f', dest='source', action='store_true')
        group.add_argument('-t', dest='source', action='store_false')

        self.add_argument('-d', dest='must_be_dir', action='store_true')
        self.add_argument('-p', dest='preserve', action='store_true')
        self.add_argument('-r', dest='recurse', action='store_true')
        self.add_argument('-v', dest='verbose', action='store_true')

        self.add_argument('path')

    def error(self, message: str) -> NoReturn:
        raise ValueError(message)

    def parse(self, command: str) -> _SCPArgs:
        """Parse an SCP command"""

        return self.parse_args(shlex.split(command)[1:], namespace=_SCPArgs())


class _SCPHandler:
    """SCP handler"""

    def __init__(self, reader: 'SSHReader[bytes]', writer: 'SSHWriter[bytes]',
                 error_handler: SFTPErrorHandler = None, server: bool = False):
        self._reader = reader
        self._writer = writer
        self._error_handler = error_handler
        self._server = server

    async def __aenter__(self) -> Self: # pragma: no cover
        """Allow _SCPHandler to be used as an async context manager"""

        return self

    async def __aexit__(self, _exc_type: Optional[Type[BaseException]],
                        _exc_value: Optional[BaseException],
                        _traceback: Optional[TracebackType]) -> \
            bool: # pragma: no cover
        """Wait for file close when used as an async context manager"""

        await self.close()
        return False

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

        self._writer.write(request + b'\n')

    async def make_request(self, *args: bytes) -> None:
        """Send an SCP request and wait for a response"""

        self.send_request(*args)

        exc = await self.await_response()

        if exc:
            raise exc

    async def send_data(self, data: bytes) -> None:
        """Send SCP file data"""

        self._writer.write(data)
        await self._writer.drain()
        await asyncio.sleep(0)

    def send_ok(self) -> None:
        """Send an SCP OK response"""

        self._writer.write(b'\0')

    def send_error(self, exc: Exception) -> None:
        """Send an SCP error response"""

        if isinstance(exc, SFTPError):
            reason = exc.reason.encode('utf-8')
        elif isinstance(exc, OSError): # pragma: no branch (win32)
            reason = exc.strerror.encode('utf-8')

            filename = cast(BytesOrStr, exc.filename)

            if filename:
                if isinstance(filename, str): # pragma: no cover (win32)
                    filename = filename.encode('utf-8')

                reason += b': ' + filename
        else: # pragma: no cover (win32)
            reason = str(exc).encode('utf-8')

        fatal = cast(bool, getattr(exc, 'fatal', False))

        self._writer.write((b'\x02' if fatal else b'\x01') +
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

        data = await self._reader.read(n)
        return data

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
            self._writer.channel.abort()
        else:
            self._writer.close()

            await self._writer.wait_closed()


class SCPSource(_SCPHandler):
    """SCP handler for sending files"""

    def __init__(self, fs: _SCPFSProtocol, reader: 'SSHReader[bytes]',
                 writer: 'SSHWriter[bytes]', preserve: bool, recurse: bool,
                 block_size: int = SCP_BLOCK_SIZE,
                 progress_handler: SFTPProgressHandler = None,
                 error_handler: SFTPErrorHandler = None, server: bool = False):
        super().__init__(reader, writer, error_handler, server)

        self._fs = fs
        self._preserve = preserve
        self._recurse = recurse
        self._block_size = block_size
        self._progress_handler = progress_handler

    async def _make_cd_request(self, action: bytes, attrs: SFTPAttrs,
                               size: int, path: bytes) -> None:
        """Make an SCP copy or dir request"""

        assert attrs.permissions is not None

        args = f'{attrs.permissions & 0o7777:04o} {size} '
        await self.make_request(action, args.encode('ascii'),
                                self._fs.basename(path))

    async def _make_t_request(self, attrs: SFTPAttrs) -> None:
        """Make an SCP time request"""

        assert attrs.mtime is not None
        assert attrs.atime is not None

        args = f'{attrs.mtime} 0 {attrs.atime} 0'
        await self.make_request(b'T', args.encode('ascii'))

    async def _send_file(self, srcpath: bytes,
                         dstpath: bytes, attrs: SFTPAttrs) -> None:
        """Send a file over SCP"""

        assert attrs.size is not None

        file_obj = await self._fs.open(srcpath, 'rb')
        size = attrs.size
        local_exc = None
        offset = 0

        try:
            await self._make_cd_request(b'C', attrs, size, srcpath)

            if self._progress_handler and size == 0:
                self._progress_handler(srcpath, dstpath, 0, 0)

            while offset < size:
                blocklen = min(size - offset, self._block_size)

                if local_exc:
                    data = blocklen * b'\0'
                else:
                    try:
                        data = cast(bytes,
                                    await file_obj.read(blocklen, offset))

                        if not data:
                            raise _scp_error(SFTPFailure, 'Unexpected EOF')
                    except (OSError, SFTPError) as exc:
                        local_exc = exc

                await self.send_data(data)
                offset += len(data)

                if self._progress_handler:
                    self._progress_handler(srcpath, dstpath, offset, size)
        finally:
            await file_obj.close()

        if local_exc:
            self.send_error(local_exc)
            setattr(local_exc, 'suppress_send', True)
        else:
            self.send_ok()

        remote_exc = await self.await_response()
        final_exc = remote_exc or local_exc

        if final_exc:
            raise final_exc

    async def _send_dir(self, srcpath: bytes, dstpath: bytes,
                        attrs: SFTPAttrs) -> None:
        """Send directory over SCP"""

        await self._make_cd_request(b'D', attrs, 0, srcpath)

        async for entry in self._fs.scandir(srcpath):
            name = cast(bytes, entry.filename)

            if name in (b'.', b'..'):
                continue

            await self._send_files(posixpath.join(srcpath, name),
                                   posixpath.join(dstpath, name),
                                   entry.attrs)

        await self.make_request(b'E')

    async def _send_files(self, srcpath: bytes, dstpath: bytes,
                          attrs: SFTPAttrs) -> None:
        """Send files via SCP"""

        try:
            if self._preserve:
                await self._make_t_request(attrs)

            if self._recurse and attrs.type == FILEXFER_TYPE_DIRECTORY:
                await self._send_dir(srcpath, dstpath, attrs)
            elif attrs.type == FILEXFER_TYPE_REGULAR:
                await self._send_file(srcpath, dstpath, attrs)
            else:
                raise _scp_error(SFTPFailure, 'Not a regular file', srcpath)
        except (OSError, SFTPError, ValueError) as exc:
            self.handle_error(exc)

    async def run(self, srcpath: _SCPPath) -> None:
        """Start SCP transfer"""

        cancelled = False

        try:
            if isinstance(srcpath, PurePath):
                srcpath = str(srcpath)

            if isinstance(srcpath, str):
                srcpath = srcpath.encode('utf-8')

            exc = await self.await_response()

            if exc:
                raise exc

            for name in await SFTPGlob(self._fs).match(srcpath):
                await self._send_files(cast(bytes, name.filename),
                                            b'', name.attrs)
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self.handle_error(exc)
        finally:
            await self.close(cancelled)


class SCPSink(_SCPHandler):
    """SCP handler for receiving files"""

    def __init__(self, fs: _SCPFSProtocol, reader: 'SSHReader[bytes]',
                 writer: 'SSHWriter[bytes]', must_be_dir: bool, preserve: bool,
                 recurse: bool, block_size: int = SCP_BLOCK_SIZE,
                 progress_handler: SFTPProgressHandler = None,
                 error_handler: SFTPErrorHandler = None, server: bool = False):
        super().__init__(reader, writer, error_handler, server)

        self._fs = fs
        self._must_be_dir = must_be_dir
        self._preserve = preserve
        self._recurse = recurse
        self._block_size = block_size
        self._progress_handler = progress_handler

    async def _recv_file(self, srcpath: bytes,
                         dstpath: bytes, size: int) -> None:
        """Receive a file via SCP"""

        file_obj = await self._fs.open(dstpath, 'wb')
        local_exc = None
        offset = 0

        try:
            self.send_ok()

            if self._progress_handler and size == 0:
                self._progress_handler(srcpath, dstpath, 0, 0)

            while offset < size:
                blocklen = min(size - offset, self._block_size)
                data = await self.recv_data(blocklen)

                if not data:
                    raise _scp_error(SFTPConnectionLost, 'Connection lost',
                                     fatal=True, suppress_send=True)

                if not local_exc:
                    try:
                        await file_obj.write(data, offset)
                    except (OSError, SFTPError) as exc:
                        local_exc = exc

                offset += len(data)

                if self._progress_handler:
                    self._progress_handler(srcpath, dstpath, offset, size)
        finally:
            await file_obj.close()

        remote_exc = await self.await_response()

        if local_exc:
            self.send_error(local_exc)
            setattr(local_exc, 'suppress_send',True)
        else:
            self.send_ok()

        final_exc = remote_exc or local_exc

        if final_exc:
            raise final_exc

    async def _recv_dir(self, srcpath: bytes, dstpath: bytes) -> None:
        """Receive a directory over SCP"""

        if not self._recurse:
            raise _scp_error(SFTPBadMessage,
                             'Directory received without recurse')

        if await self._fs.exists(dstpath):
            if not await self._fs.isdir(dstpath):
                raise _scp_error(SFTPFailure, 'Not a directory', dstpath)
        else:
            await self._fs.mkdir(dstpath)

        await self._recv_files(srcpath, dstpath)

    async def _recv_files(self, srcpath: bytes, dstpath: bytes) -> None:
        """Receive files over SCP"""

        self.send_ok()

        attrs = SFTPAttrs()

        while True:
            action, args = await self.recv_request()

            if not action:
                break

            assert args is not None

            try:
                if action in b'\x01\x02':
                    raise _scp_error(SFTPFailure, args,
                                     fatal=action != b'\x01',
                                     suppress_send=True)
                elif action == b'T':
                    if self._preserve:
                        attrs.atime, attrs.mtime = _parse_t_args(args)

                    self.send_ok()
                elif action == b'E':
                    self.send_ok()
                    break
                elif action in b'CD':
                    try:
                        attrs.permissions, size, name = _parse_cd_args(args)

                        new_srcpath = posixpath.join(srcpath, name)

                        if await self._fs.isdir(dstpath):
                            new_dstpath = posixpath.join(dstpath, name)
                        else:
                            new_dstpath = dstpath

                        if action == b'D':
                            await self._recv_dir(new_srcpath, new_dstpath)
                        else:
                            await self._recv_file(new_srcpath,
                                                  new_dstpath, size)

                        if self._preserve:
                            await self._fs.setstat(new_dstpath, attrs)
                    finally:
                        attrs = SFTPAttrs()
                else:
                    raise _scp_error(SFTPBadMessage, 'Unknown request')
            except (OSError, SFTPError) as exc:
                self.handle_error(exc)

    async def run(self, dstpath: _SCPPath) -> None:
        """Start SCP file receive"""

        cancelled = False

        try:
            if isinstance(dstpath, PurePath):
                dstpath = str(dstpath)

            if isinstance(dstpath, str):
                dstpath = dstpath.encode('utf-8')

            if self._must_be_dir and not await self._fs.isdir(dstpath):
                self.handle_error(_scp_error(SFTPFailure, 'Not a directory',
                                             dstpath))
            else:
                await self._recv_files(b'', dstpath)
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError, ValueError) as exc:
            self.handle_error(exc)
        finally:
            await self.close(cancelled)


class SCPCopier:
    """SCP handler for remote-to-remote copies"""

    def __init__(self, src_reader: 'SSHReader[bytes]',
                 src_writer: 'SSHWriter[bytes]',
                 dst_reader: 'SSHReader[bytes]',
                 dst_writer: 'SSHWriter[bytes]',
                 block_size: int = SCP_BLOCK_SIZE,
                 progress_handler: SFTPProgressHandler = None,
                 error_handler: SFTPErrorHandler = None):
        self._source = _SCPHandler(src_reader, src_writer)
        self._sink = _SCPHandler(dst_reader, dst_writer)
        self._block_size = block_size
        self._progress_handler = progress_handler
        self._error_handler = error_handler

    def _handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = _scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        if self._error_handler and not getattr(exc, 'fatal', False):
            self._error_handler(exc)
        else:
            raise exc

    async def _forward_response(self, src: _SCPHandler,
                                dst: _SCPHandler) -> Optional[Exception]:
        """Forward an SCP response between two remote SCP servers"""

        # pylint: disable=no-self-use

        try:
            exc = await src.await_response()

            if exc:
                dst.send_error(exc)
                return exc
            else:
                dst.send_ok()
                return None
        except OSError as exc:
            return exc

    async def _copy_file(self, path: bytes, size: int) -> None:
        """Copy a file from one remote SCP server to another"""

        offset = 0

        if self._progress_handler and size == 0:
            self._progress_handler(path, path, 0, 0)

        while offset < size:
            blocklen = min(size - offset, self._block_size)
            data = await self._source.recv_data(blocklen)

            if not data:
                raise _scp_error(SFTPConnectionLost, 'Connection lost',
                                 fatal=True, suppress_send=True)

            await self._sink.send_data(data)
            offset += len(data)

            if self._progress_handler:
                self._progress_handler(path, path, offset, size)

        source_exc = await self._forward_response(self._source, self._sink)
        sink_exc = await self._forward_response(self._sink, self._source)

        exc = sink_exc or source_exc

        if exc:
            self._handle_error(exc)

    async def _copy_files(self) -> None:
        """Copy files from one SCP server to another"""

        exc = await self._forward_response(self._sink, self._source)

        if exc:
            self._handle_error(exc)

        pathlist: List[bytes] = []
        attrlist: List[SFTPAttrs] = []
        attrs = SFTPAttrs()

        while True:
            action, args = await self._source.recv_request()

            if not action:
                break

            assert args is not None

            self._sink.send_request(action, args)

            if action in b'\x01\x02':
                exc = _scp_error(SFTPFailure, args, fatal=action != b'\x01')
                self._handle_error(exc)
                continue

            exc = await self._forward_response(self._sink, self._source)

            if exc:
                self._handle_error(exc)
                continue

            if action in b'CD':
                try:
                    attrs.permissions, size, name = _parse_cd_args(args)

                    if action == b'C':
                        path = b'/'.join(pathlist + [name])
                        await self._copy_file(path, size)
                    else:
                        pathlist.append(name)
                        attrlist.append(attrs)
                finally:
                    attrs = SFTPAttrs()
            elif action == b'E':
                if pathlist:
                    pathlist.pop()
                    attrs = attrlist.pop()

                else:
                    break
            elif action == b'T':
                attrs.atime, attrs.mtime = _parse_t_args(args)
            else:
                raise _scp_error(SFTPBadMessage, 'Unknown SCP action')

    async def run(self) -> None:
        """Start SCP remote-to-remote transfer"""

        cancelled = False

        try:
            await self._copy_files()
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self._handle_error(exc)
        finally:
            await self._source.close(cancelled)
            await self._sink.close(cancelled)
