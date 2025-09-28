import asyncio
import time
import io
import pathlib
import posixpath

from typing import Literal


from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from .protocols import (
    SCPHandler,
    parse_cd_args,
    parse_time_args,
    scp_error,
    SCP_BLOCK_SIZE,
)

from hyperscale.core.engines.client.ssh.protocol.ssh.constants import FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.sftp.models import TransferResult, FileAttributes


TransferType = Literal["FILE", "DIRECTORY"]


class SCPCommand:


    def __init__(
        self,
        source: SCPHandler,
        dest: SCPHandler,
        loop: asyncio.AbstractEventLoop,
        block_size: int = SCP_BLOCK_SIZE,
        recurse: bool = False,
        preserve: bool = False,
        must_be_dir: bool = False,
        path_encoding: str = 'utf-8'

    ):
        self._source = source
        self._dest = dest
        self._filesystem: dict[str, TransferResult] = {}

        self._block_size = block_size
        self._recurse = recurse
        self._preserve = preserve
        self._must_be_dir = must_be_dir
        self._loop = loop
        self._path_encoding = path_encoding

    async def copy(self):
        return await self._copy_files()

    async def receive(
        self,
        dstpath: str | pathlib.PurePath,
    ):

        if isinstance(dstpath, pathlib.PurePath):
            dstpath = str(dstpath).encode()

        elif isinstance(dstpath, str):
            dstpath = dstpath.encode()


        return await self._recv_files(dstpath)
    
    async def send(
        self,
        files: list[
            tuple[
                bytes,
                bytes,
                FileAttributes,
            ]
        ],
    ):

        exc = await self._dest.await_response()
        if exc:
            raise exc
        
        return await self._send_files(files)

    def _handle_error(self, exc: Exception) -> None:

        if isinstance(exc, BrokenPipeError):
            exc = scp_error(
                SFTPConnectionLost,
                'Connection lost',
                fatal=True,
                suppress_send=True,
            )

        raise exc
    
    async def _forward_response(
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

    async def _copy_file(self, size: int):
        """Copy a file from one remote SCP server to another"""

        offset = 0
        buffer = bytearray()

        while offset < size:
            blocklen = min(size - offset, self._block_size)
            data = await self._source.recv_data(blocklen)

            if not data:
                raise scp_error(SFTPConnectionLost, 'Connection lost',
                                 fatal=True, suppress_send=True)

            buffer.extend(data)
            self._dest.writer.write(data)
            offset += len(data)

        source_exc = await self._forward_response(self._source, self._dest)
        sink_exc = await self._forward_response(self._dest, self._source)

        exc = sink_exc or source_exc

        if exc:
            self._handle_error(exc)

        return bytes(buffer)

    async def _copy_files(self):
        """Copy files from one SCP server to another"""

        exc = await self._forward_response(self._dest, self._source)

        if exc:
            self._handle_error(exc)

        pathlist: list[bytes] = []
        attrlist: list[SFTPAttrs] = []
        attrs = SFTPAttrs()

        transferred: dict[bytes, TransferResult] = {}

        operation_start = time.monotonic()

        while True:

            start = time.monotonic()

            action, args = await self._source.recv_request()

            if not action:
                break

            assert args is not None

            self._dest.send_request(action, args)

            if action in b'\x01\x02':
                exc = scp_error(SFTPFailure, args, fatal=action != b'\x01')
                self._handle_error(exc)
                continue

            exc = await self._forward_response(self._dest, self._source)

            if exc:
                self._handle_error(exc)
                continue

            if action in b'CD':
                try:
                    attrs.permissions, size, name = parse_cd_args(args)

                    transfer_type: TransferType = "FILE"
                    data: bytes | None = None

                    if action == b'C':
                        path = b'/'.join(pathlist + [name])
                        data = await self._copy_file(size)

                    else:
                        pathlist.append(name)
                        attrlist.append(attrs)

                        transfer_type = "DIRECTORY"

                        path = b'/'.join(pathlist)
                        
                    transferred[path] = TransferResult(
                        file_path=path,
                        file_type=transfer_type,
                        file_data=data,
                        file_transfer_elapsed=time.monotonic() - start,
                        file_attribues=TransferResult.to_attributes(attrs),
                    )

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
        
        elapsed = time.monotonic() - operation_start
        return (
            elapsed,
            transferred,
        )

    async def _recv_file(
        self,
        size: int,
    ):

        buffer = bytearray()
        local_exc = None
        offset = 0

        self._source.send_ok()

        while offset < size:
            blocklen = min(size - offset, self._block_size)
            data = await self._source.recv_data(blocklen)

            if not data:
                raise scp_error(SFTPConnectionLost, 'Connection lost',
                                    fatal=True, suppress_send=True)

            if not local_exc:
                try:
                    buffer.extend(data)
                except (OSError, SFTPError) as exc:
                    local_exc = exc

            offset += len(data)

        remote_exc = await self._source.await_response()
      
        if local_exc:
            self._source.send_error(local_exc)
            setattr(local_exc, 'suppress_send',True)
        else:
            self._source.send_ok()

        final_exc = remote_exc or local_exc

        if final_exc:
            raise final_exc
        
        return bytes(buffer)
        
    async def _recv_dir(self, dstpath: bytes) -> None:
        if not self._recurse:
            raise scp_error(SFTPBadMessage,
                             'Directory received without recurse')

        if self._filesystem.get(dstpath) and pathlib.Path(str(dstpath)).suffix:
            raise scp_error(SFTPFailure, 'Not a directory', dstpath)
        

        await self._recv_files(dstpath)

    async def _recv_files(
        self,
        dstpath: bytes,
    ):

        self._source.send_ok()

        attrs = SFTPAttrs()
        transferred: dict[bytes, TransferResult] = {}

        operation_start = time.monotonic()

        while True:

            start = time.monotonic()

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

                        if not pathlib.Path(str(dstpath)).suffix:
                            new_dstpath = posixpath.join(dstpath, name)
                        else:
                            new_dstpath = dstpath

                        transfer_type: TransferType = "FILE"
                        data: bytes | None = None    

                        if action == b'D':
                            await self._recv_dir(new_dstpath)

                            transfer_type = "DIRECTORY"

                        else:
                            data = await self._recv_file(size)

                        
                        file_attrs: FileAttributes | None = None
                        if self._preserve:
                            file_attrs = TransferResult.to_attributes(attrs)

                        
                        transferred[dstpath] = TransferResult(
                            file_path=dstpath,
                            file_type=transfer_type,
                            file_data=data,
                            file_transfer_elapsed=time.monotonic() - start,
                            file_attribues=file_attrs,
                        )

                    finally:
                        attrs = SFTPAttrs()
                else:
                    raise scp_error(SFTPBadMessage, 'Unknown request')
            except (OSError, SFTPError) as exc:
                self._handle_error(exc)

        elapsed = time.monotonic() - operation_start
        return (
            elapsed,
            transferred,
        )

    async def _make_cd_request(
        self,
        action: Literal[b"C", b"D"],
        attrs: FileAttributes,
        size: int,
        path: bytes,
    ):
        """Make an SCP copy or dir request"""

        start = time.monotonic()

        assert attrs.permissions is not None

        args = f'{attrs.permissions & 0o7777:04o} {size} '
        await self._dest.make_request(
            action,
            args.encode('ascii'),
            pathlib.Path(str(path)).name.encode(),
        )

        return TransferResult(
            file_path=path,
            file_type="DIRECTORY" if action == b"D" else "FILE",
            file_attribues=attrs,
            file_transfer_elapsed=time.monotonic() - start
        )

    async def _make_transfer_request(self, attrs: SFTPAttrs) -> None:
        """Make an SCP time request"""

        assert attrs.mtime is not None
        assert attrs.atime is not None

        args = f'{attrs.mtime} 0 {attrs.atime} 0'
        await self._dest.make_request(b'T', args.encode('ascii'))

    async def _send_file(
        self,
        srcpath: bytes,
        data: bytes,
        attrs: FileAttributes,
    ):
        
        start = time.monotonic()
        
        await self._make_cd_request(b'C', attrs, len(data), srcpath)

        self._dest.writer.write(data)
        self._dest.send_ok()

        remote_exc = await self._dest.await_response()

        if remote_exc:
            raise remote_exc
        
        return TransferResult(
            file_path=srcpath,
            file_data=data,
            file_type="FILE",
            file_attribues=attrs,
            file_transfer_elapsed=time.monotonic() - start,
        )

    async def _send_dir(
        self,
        srcpath: bytes,
        data: dict[bytes, tuple[bytes, FileAttributes]],
        attributes: FileAttributes,
    ) -> None:

        await self._make_cd_request(b'D', attributes, 0, srcpath)

        files = [
             (
                posixpath.join(srcpath, filename),
                file_data,
                file_attrs,
            ) for filename, (
                file_data,
                file_attrs
            ) in data.items()
             if filename not in [b'.', b'..']
        ]
        
        await asyncio.gather(*[
            self._send_file(
                path,
                data,
                attrs,
            ) for path, data, attrs in files
        ])

        await self._dest.make_request(b'E')

    async def _send_files(
        self,
        files: list[
            tuple[bytes, bytes, FileAttributes]
        ],
    ):
        
        transferred: dict[bytes, TransferResult] = {}

        start = time.monotonic()

        for path, data, attrs, in files:

            if attrs.type == FILEXFER_TYPE_DIRECTORY:
                result = await self._make_cd_request(
                    b'D',
                    attrs,
                    0,
                    path,
                )

            else:
                result = await self._send_file(
                    path,
                    data,
                    attrs,
                )

            transferred[path] = result

        elapsed = time.monotonic() - start
        return (
            elapsed,
            transferred,
        )
