import asyncio
import pathlib
from typing import Any
from collections import defaultdict
from hyperscale.core.engines.client.shared.models import (
    URL as SFTPUrl,
)
from hyperscale.core.testing.models import (
    URL,
    Auth,
    Data,
)
from hyperscale.core.engines.client.shared.timeouts import Timeouts
from hyperscale.core.engines.client.ssh.protocol.constants import DEFAULT_LANG
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.ssh.protocol.connection import connect
from hyperscale.core.engines.client.ssh.protocol.known_hosts import KnownHostsArg
from hyperscale.core.engines.client.ssh.protocol.misc import async_context_manager
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, SFTPGlob
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPErrorHandler, SFTPProgressHandler, local_fs
from .protocols import SCPConnection


class MercurySyncSCPConnction:
    """SCP handler for remote-to-remote copies"""

    def __init__(
        self,
        pool_size: int | None = None,
        timeouts: Timeouts = Timeouts(),
        reset_connections: bool = False,
    ):
        self._concurrency = pool_size
        self.timeouts = timeouts
        self.reset_connections = reset_connections

        self._dns_lock: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._dns_waiters: dict[str, asyncio.Future] = defaultdict(asyncio.Future)
        self._pending_queue: list[asyncio.Future] = []

        self._client_waiters: dict[asyncio.Transport, asyncio.Future] = {}
        self._destination_connections: list[SCPConnection] = []
        self._source_connections: list[SCPConnection] = []

        self._hosts: dict[str, tuple[str, int]] = {}

        self._semaphore: asyncio.Semaphore = None
        self._connection_waiters: list[asyncio.Future] = []

        self._url_cache: dict[str, SFTPUrl] = {}
        self._optimized: dict[str, URL | Auth | Data ] = {}

        
    async def copy(self) -> None:
        """Start SCP remote-to-remote transfer"""

        cancelled = False

        try:
            await self._copy_files()
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self._handle_error(exc)
        finally:
            if self._source:
                await self._source.close(cancelled)

            if self._sink:
                await self._sink.close(cancelled)
    
    async def send(self, srcpath: _SCPPath) -> None:
        """Start SCP transfer"""

        cancelled = False

        try:

            if isinstance(srcpath, str):
                srcpath = srcpath.encode('utf-8')

            elif isinstance(srcpath, PurePath):
                srcpath = str(srcpath).encode('utf-8')

            exc = await self._sink.await_response()

            if exc:
                raise exc
            
            await asyncio.gather(*[
                self._send_files(
                    name.filename,
                    b'',
                    name.attrs,
                ) for name in await SFTPGlob(self._fs).match(srcpath)
            ])

        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError) as exc:
            self._handle_error(exc)
        finally:
            if self._sink:
                await self._sink.close(cancelled)

    async def receive(self, dstpath: _SCPPath) -> None:
        """Start SCP file receive"""

        cancelled = False
        try:
            if isinstance(dstpath, PurePath):
                dstpath = str(dstpath)

            if isinstance(dstpath, str):
                dstpath = dstpath.encode('utf-8')

            if self._must_be_dir and not await self._fs.isdir(dstpath):
                self._handle_error(_scp_error(SFTPFailure, 'Not a directory',
                                             dstpath))
            else:
                await self._recv_files(b'', dstpath)
        except asyncio.CancelledError:
            cancelled = True
        except (OSError, SFTPError, ValueError) as exc:
            self._handle_error(exc)
        finally:
            await self._source.close(cancelled)
    

    def _handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = _scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        if self._error_handler and not getattr(exc, 'fatal', False):
            self._error_handler(exc)
        else:
            raise exc

    async def _forward_response(self, src: SCPHandler,
                                dst: SCPHandler) -> Optional[Exception]:
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

            self._sink.writer.write(data)
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

    async def _recv_file(self, srcpath: bytes,
                         dstpath: bytes, size: int) -> None:
        """Receive a file via SCP"""

        file_obj = await self._fs.open(dstpath, 'wb')
        local_exc = None
        offset = 0

        try:
            self._source.send_ok()

            if self._progress_handler and size == 0:
                self._progress_handler(srcpath, dstpath, 0, 0)

            while offset < size:
                blocklen = min(size - offset, self._block_size)
                data = await self._source.recv_data(blocklen)

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

        remote_exc = await self._source.await_response()

        if local_exc:
            self._source.send_error(local_exc)
            setattr(local_exc, 'suppress_send',True)
        else:
            self._source.send_ok()

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

        self._source.send_ok()

        attrs = SFTPAttrs()

        while True:
            action, args = await self._source.recv_request()
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

                    self._source.send_ok()
                elif action == b'E':
                    self._source.send_ok()
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
                self._handle_error(exc)

    async def _make_cd_request(self, action: bytes, attrs: SFTPAttrs,
                               size: int, path: bytes) -> None:
        """Make an SCP copy or dir request"""

        assert attrs.permissions is not None

        args = f'{attrs.permissions & 0o7777:04o} {size} '
        await self._sink.make_request(action, args.encode('ascii'),
                                self._fs.basename(path))

    async def _make_t_request(self, attrs: SFTPAttrs) -> None:
        """Make an SCP time request"""

        assert attrs.mtime is not None
        assert attrs.atime is not None

        args = f'{attrs.mtime} 0 {attrs.atime} 0'
        await self._sink.make_request(b'T', args.encode('ascii'))

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
                        data = await file_obj.read(blocklen, offset)

                        if not data:
                            raise _scp_error(SFTPFailure, 'Unexpected EOF')
                    except (OSError, SFTPError) as exc:
                        local_exc = exc

                self._sink.writer.write(data)
                offset += len(data)

                if self._progress_handler:
                    self._progress_handler(srcpath, dstpath, offset, size)
        finally:
            await file_obj.close()

        if local_exc:
            self._sink.send_error(local_exc)
            setattr(local_exc, 'suppress_send', True)
        else:
            self._sink.send_ok()

        remote_exc = await self._sink.await_response()
        final_exc = remote_exc or local_exc

        if final_exc:
            raise final_exc

    async def _send_dir(self, srcpath: bytes, dstpath: bytes,
                        attrs: SFTPAttrs) -> None:
        """Send directory over SCP"""

        await self._make_cd_request(b'D', attrs, 0, srcpath)

        async for entry in self._fs.scandir(srcpath):
            name: bytes = entry.filename

            if name in (b'.', b'..'):
                continue

            await self._send_files(posixpath.join(srcpath, name),
                                   posixpath.join(dstpath, name),
                                   entry.attrs)

        await self._sink.make_request(b'E')

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
            self._handle_error(exc)

    async def _connect(
        self,
        source: str | pathlib.Path,
        destination: str | pathlib.Path,
        **kwargs: dict[str, Any],
    ):
        dstconn, _, _ = await parse_path(
            destination, 
            known_hosts=self._known_hosts,
            username=self._username,
            password=self._password,
            **kwargs,
        )

   
        srcconn, srcpath, _ = await parse_path(
            source, 
            known_hosts=self._known_hosts,
            username=self._username,
            password=self._password,
            **kwargs,
        )

        src_command = b'scp -f '
        dst_command = b'scp -t '

        if self._must_be_dir:
            src_command += b'-d '
            dst_command += b'-d '

        if self._preserve:
            src_command += b'-p '
            dst_command += b'-p '

        if self._recurse:
            src_command += b'-r '
            dst_command += b'-r '

        if srcconn:
            src_command += srcpath.encode()
            src_writer, src_reader, _ = await srcconn.open_session(src_command, encoding=None)
            self._source = SCPHandler(src_reader, src_writer)
        
        if dstconn:
            dst_command += dstpath.encode()
            dst_writer, dst_reader, _ = await dstconn.open_session(dst_command, encoding=None)
            self._sink = SCPHandler(dst_reader, dst_writer)