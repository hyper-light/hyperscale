import asyncio
import time
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

from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_DIRECTORY
from .file import File
from .models.scp import FileResult, FileAttributes


TransferType = Literal["FILE", "DIRECTORY"]


class SCPCommand:


    def __init__(
        self,
        source: SCPHandler,
        dest: SCPHandler,
        block_size: int = SCP_BLOCK_SIZE,
        recurse: bool = False,
        preserve: bool = False,
        must_be_dir: bool = False,

    ):
        self._source = source
        self._dest = dest
        self._filesystem: dict[str, FileResult] = {}

        self._block_size = block_size
        self._recurse = recurse
        self._preserve = preserve
        self._must_be_dir = must_be_dir

    async def copy(self) -> None:
        await self._copy_files()

        return self._filesystem

    async def receive(
        self,
        dstpath: str | pathlib.PurePath,
    ) -> None:

        if isinstance(dstpath, pathlib.PurePath):
            dstpath = str(dstpath)

        if isinstance(dstpath, str):
            dstpath = dstpath.encode('utf-8')


        await self._recv_files(dstpath)

        return self._filesystem
    
    async def send(
        self,
        data: File | list[File],
    ):

        exc = await self._dest.await_response()
        if exc:
            raise exc
        
        await self._send_files(data)

        return self._filesystem

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

        return buffer

    async def _copy_files(self) -> None:
        """Copy files from one SCP server to another"""

        exc = await self._forward_response(self._dest, self._source)

        if exc:
            self._handle_error(exc)

        pathlist: list[bytes] = []
        attrlist: list[SFTPAttrs] = []
        attrs = SFTPAttrs()

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
                    data: bytearray | None = None

                    if action == b'C':
                        path = b'/'.join(pathlist + [name])
                        data = await self._copy_file(size)

                    else:
                        pathlist.append(name)
                        attrlist.append(attrs)

                        transfer_type = "DIRECTORY"

                        path = b'/'.join(pathlist)
                        
                    self._filesystem[path] = FileResult(
                        file_path=path,
                        file_type=transfer_type,
                        file_data=data,
                        file_transfer_elapsed=time.monotonic() - start,
                        file_attribues=FileResult.to_attributes(attrs),
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
        
        return buffer
        
    async def _recv_dir(self, dstpath: bytes) -> None:
        """Receive a directory over SCP"""

        if not self._recurse:
            raise scp_error(SFTPBadMessage,
                             'Directory received without recurse')

        if self._filesystem.get(dstpath) and pathlib.Path(str(dstpath)).suffix:
            raise scp_error(SFTPFailure, 'Not a directory', dstpath)
        

        await self._recv_files(dstpath)

    async def _recv_files(self, dstpath: bytes) -> None:
        """Receive files over SCP"""

        self._source.send_ok()

        attrs = SFTPAttrs()

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
                        data: bytearray | None = None    

                        if action == b'D':
                            await self._recv_dir(new_dstpath)

                            transfer_type = "DIRECTORY"

                        else:
                            data = await self._recv_file(size)

                        
                        file_attrs: FileAttributes | None = None
                        if self._preserve:
                            file_attrs = FileResult.to_attributes(attrs)

                        
                        self._filesystem[dstpath] = FileResult(
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

    async def _make_cd_request(self, action: bytes, attrs: SFTPAttrs,
                               size: int, path: bytes) -> None:
        """Make an SCP copy or dir request"""

        assert attrs.permissions is not None

        args = f'{attrs.permissions & 0o7777:04o} {size} '
        await self._dest.make_request(
            action,
            args.encode('ascii'),
            pathlib.Path(str(path)).name.encode(),
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
        attrs: SFTPAttrs,
    ) -> None:
        
        await self._make_cd_request(b'C', attrs, len(data), srcpath)

        self._dest.writer.write(data)
        self._dest.send_ok()

        remote_exc = await self._dest.await_response()

        if remote_exc:
            raise remote_exc

    async def _send_dir(
        self,
        srcpath: bytes,
        data: dict[bytes, bytes | File],
        attrs: SFTPAttrs,
    ) -> None:

        await self._make_cd_request(b'D', attrs, 0, srcpath)

        files = {
            filename: File(
                filename,
                file_data,
            ) if isinstance(
                file_data,
                bytes,
            ) else file_data for filename, file_data in data.items()
             if filename not in [b'.', b'..']
        }
        
        await asyncio.gather(*[
            self._send_file(
                posixpath.join(srcpath, file_item.name),
                file_item.data,
                file_item.attrs,
                
            ) for file_item in files.values()
        ])

        await self._dest.make_request(b'E')

    async def _send_files(
        self,
        file: File | list[File],
    ) -> None:
        

        transfer_type: TransferType = "FILE"
        data: bytes | None = None
             
        start = time.monotonic()

        if isinstance(file, list):
            await asyncio.gather(*[
                self._send_file(
                    item.path,
                    item.data,
                    item.attrs,
                ) for item in file
            ])

        
        elif file.file_type == FILEXFER_TYPE_DIRECTORY:
            await self._make_cd_request(
                b'D',
                file.attrs,
                0,
                file.path,
            )

            transfer_type = "DIRECTORY"
             
        else:
            await self._send_file(
                file.path,
                file.data,
                file.attrs,
            )

            data = file.data
             

        result = FileResult(
            file_path=file.path,
            file_type=transfer_type,
            file_data=data,
            file_transfer_elapsed=time.monotonic() - start,
            file_attribues=FileResult.to_attributes(file.attrs),
        )

        self._filesystem[file.path] = result

