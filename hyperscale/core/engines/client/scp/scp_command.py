
import posixpath
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, LocalFS
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from .protocols import (
    SCPHandler,
    parse_cd_args,
    parse_time_args,
    scp_error,
    SCP_BLOCK_SIZE,
)



class SCPCommand:


    def __init__(
        self,
        source: SCPHandler,
        dest: SCPHandler,
        filesystem: LocalFS,
        block_size: int = SCP_BLOCK_SIZE,
        recurse: bool = False,
        preserve: bool = False,

    ):
        self._source = source
        self._dest = dest
        self._filesystem = filesystem

        self._block_size = block_size
        self._recurse = recurse
        self._preserve = preserve

    def handle_error(self, exc: Exception) -> None:
        """Handle an SCP error"""

        if isinstance(exc, BrokenPipeError):
            exc = scp_error(SFTPConnectionLost, 'Connection lost',
                             fatal=True, suppress_send=True)

        raise exc

    async def forward_response(
        self, 
        src: SCPHandler,
        dst: SCPHandler,
    ) -> Exception | None:
        """Forward an SCP response between two remote SCP servers"""

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

    async def copy_file(self, path: bytes, size: int) -> None:
        """Copy a file from one remote SCP server to another"""

        offset = 0

        while offset < size:
            blocklen = min(size - offset, self._block_size)
            data = await self._source.recv_data(blocklen)

            if not data:
                raise scp_error(SFTPConnectionLost, 'Connection lost',
                                 fatal=True, suppress_send=True)

            self._dest.writer.write(data)
            offset += len(data)

        source_exc = await self.forward_response(self._source, self._dest)
        sink_exc = await self.forward_response(self._dest, self._source)

        exc = sink_exc or source_exc

        if exc:
            self.handle_error(exc)

    async def copy_files(self) -> None:
        """Copy files from one SCP server to another"""

        exc = await self.forward_response(self._dest, self._source)

        if exc:
            self.handle_error(exc)

        pathlist: list[bytes] = []
        attrlist: list[SFTPAttrs] = []
        attrs = SFTPAttrs()

        while True:
            action, args = await self._source.recv_request()

            if not action:
                break

            assert args is not None

            self._dest.send_request(action, args)

            if action in b'\x01\x02':
                exc = scp_error(SFTPFailure, args, fatal=action != b'\x01')
                self.handle_error(exc)
                continue

            exc = await self.forward_response(self._dest, self._source)

            if exc:
                self.handle_error(exc)
                continue

            if action in b'CD':
                try:
                    attrs.permissions, size, name = parse_cd_args(args)

                    if action == b'C':
                        path = b'/'.join(pathlist + [name])
                        await self.copy_file(path, size)
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
                attrs.atime, attrs.mtime = parse_time_args(args)
            else:
                raise scp_error(SFTPBadMessage, 'Unknown SCP action')

    async def recv_file(self, srcpath: bytes,
                         dstpath: bytes, size: int) -> None:
        """Receive a file via SCP"""

        file_obj = await self._filesystem.open(dstpath, 'wb')
        local_exc = None
        offset = 0

        try:
            self._source.send_ok()

            while offset < size:
                blocklen = min(size - offset, self._block_size)
                data = await self._source.recv_data(blocklen)

                if not data:
                    raise scp_error(SFTPConnectionLost, 'Connection lost',
                                     fatal=True, suppress_send=True)

                if not local_exc:
                    try:
                        await file_obj.write(data, offset)
                    except (OSError, SFTPError) as exc:
                        local_exc = exc

                offset += len(data)

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

    async def recv_dir(self, srcpath: bytes, dstpath: bytes) -> None:
        """Receive a directory over SCP"""

        if not self._recurse:
            raise scp_error(SFTPBadMessage,
                             'Directory received without recurse')

        if await self._filesystem.exists(dstpath):
            if not await self._filesystem.isdir(dstpath):
                raise scp_error(SFTPFailure, 'Not a directory', dstpath)
        else:
            await self._filesystem.mkdir(dstpath)

        await self.recv_files(srcpath, dstpath)

    async def recv_files(self, srcpath: bytes, dstpath: bytes) -> None:
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
                    raise scp_error(SFTPFailure, args,
                                     fatal=action != b'\x01',
                                     suppress_send=True)
                elif action == b'T':
                    if self._preserve:
                        attrs.atime, attrs.mtime = parse_time_args(args)

                    self._source.send_ok()
                elif action == b'E':
                    self._source.send_ok()
                    break
                elif action in b'CD':
                    try:
                        attrs.permissions, size, name = parse_cd_args(args)

                        new_srcpath = posixpath.join(srcpath, name)

                        if await self._filesystem.isdir(dstpath):
                            new_dstpath = posixpath.join(dstpath, name)
                        else:
                            new_dstpath = dstpath

                        if action == b'D':
                            await self.recv_dir(new_srcpath, new_dstpath)
                        else:
                            await self.recv_file(new_srcpath,
                                                  new_dstpath, size)

                        if self._preserve:
                            await self._filesystem.setstat(new_dstpath, attrs)
                    finally:
                        attrs = SFTPAttrs()
                else:
                    raise scp_error(SFTPBadMessage, 'Unknown request')
            except (OSError, SFTPError) as exc:
                self.handle_error(exc)

    async def make_cd_request(self, action: bytes, attrs: SFTPAttrs,
                               size: int, path: bytes) -> None:
        """Make an SCP copy or dir request"""

        assert attrs.permissions is not None

        args = f'{attrs.permissions & 0o7777:04o} {size} '
        await self._dest.make_request(action, args.encode('ascii'),
                                self._filesystem.basename(path))

    async def make_transfer_request(self, attrs: SFTPAttrs) -> None:
        """Make an SCP time request"""

        assert attrs.mtime is not None
        assert attrs.atime is not None

        args = f'{attrs.mtime} 0 {attrs.atime} 0'
        await self._dest.make_request(b'T', args.encode('ascii'))

    async def send_file(
        self,
        srcpath: bytes,
        dstpath: bytes,
        attrs: SFTPAttrs,
    ) -> None:

        assert attrs.size is not None

        file_obj = await self._filesystem.open(srcpath, 'rb')
        size = attrs.size
        local_exc = None
        offset = 0

        try:
            await self.make_cd_request(b'C', attrs, size, srcpath)

            while offset < size:
                blocklen = min(size - offset, self._block_size)

                if local_exc:
                    data = blocklen * b'\0'
                else:
                    try:
                        data = await file_obj.read(blocklen, offset)

                        if not data:
                            raise scp_error(SFTPFailure, 'Unexpected EOF')
                    except (OSError, SFTPError) as exc:
                        local_exc = exc

                self._dest.writer.write(data)
                offset += len(data)

        finally:
            await file_obj.close()

        if local_exc:
            self._dest.send_error(local_exc)
            setattr(local_exc, 'suppress_send', True)
        else:
            self._dest.send_ok()

        remote_exc = await self._dest.await_response()
        final_exc = remote_exc or local_exc

        if final_exc:
            raise final_exc

    async def send_dir(
        self,
        srcpath: bytes,
        dstpath: bytes,
        attrs: SFTPAttrs,
    ) -> None:

        await self.make_cd_request(b'D', attrs, 0, srcpath)

        async for entry in self._filesystem.scandir(srcpath):
            name: bytes = entry.filename

            if name in (b'.', b'..'):
                continue

            await self.send_files(posixpath.join(srcpath, name),
                                   posixpath.join(dstpath, name),
                                   entry.attrs)

        await self._dest.make_request(b'E')

    async def send_files(self, srcpath: bytes, dstpath: bytes,
                          attrs: SFTPAttrs) -> None:
        """Send files via SCP"""

        try:
            if self._preserve:
                await self.make_transfer_request(attrs)

            if self._recurse and attrs.type == FILEXFER_TYPE_DIRECTORY:
                await self.send_dir(srcpath, dstpath, attrs)
            elif attrs.type == FILEXFER_TYPE_REGULAR:
                await self.send_file(srcpath, dstpath, attrs)
            else:
                raise scp_error(SFTPFailure, 'Not a regular file', srcpath)
        except (OSError, SFTPError, ValueError) as exc:
            self.handle_error(exc)