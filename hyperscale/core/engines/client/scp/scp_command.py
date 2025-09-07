import asyncio
import datetime
import pathlib
import posixpath
from typing import Literal


from pydantic import BaseModel, StrictInt, StrictFloat, StrictStr

from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPAttrs, LocalFS, SFTPGlob, SFTPName
from hyperscale.core.engines.client.sftp.protocols.sftp import SFTPError, SFTPFailure, SFTPBadMessage, SFTPConnectionLost
from .protocols import (
    SCPHandler,
    parse_cd_args,
    parse_time_args,
    scp_error,
    SCP_BLOCK_SIZE,
)

from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_REGULAR, FILEXFER_TYPE_DIRECTORY
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_SYMLINK, FILEXFER_TYPE_SPECIAL
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_UNKNOWN, FILEXFER_TYPE_SOCKET
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_CHAR_DEVICE, FILEXFER_TYPE_BLOCK_DEVICE
from hyperscale.core.engines.client.ssh.protocol.constants import FILEXFER_TYPE_FIFO


FileType = Literal[
    "REGULAR",
    "DIRECTORY",
]


class FileAttributes(BaseModel):
    size: StrictInt | None = None
    alloc_size: StrictInt | None = None
    uid: StrictInt | None = None
    gid: StrictInt | None = None
    owner: StrictStr | None = None
    group: StrictStr | None = None
    permissions: StrictInt | None = None
    atime: StrictInt | None = None
    atime_ns: StrictInt | StrictFloat | None = None
    crtime: StrictInt | None = None
    crtime_ns: StrictInt | StrictFloat | None = None
    mtime: StrictInt | None = None
    mtime_ns: StrictInt | StrictFloat | None = None
    ctime: StrictInt | None = None
    ctime_ns: StrictInt | StrictFloat | None = None
    mime_type: StrictStr | None = None
    nlink: StrictInt | None = None


class FileResult:

    def __init__(
        self,
        result_path: bytes,
        result_type: Literal["FILE", "DIRECTORY"] | str = "FILE",
        result_data: bytes | bytearray | None = None,
        result_attributes: FileAttributes | None = None,
    ):
        self.path = result_path
        self.type = result_type
        self.data = result_data
        self.attributes = result_attributes

class File:

    def __init__(
        self,
        path: bytes,
        data:bytes,
        file_type: FileType = "REGULAR",
        user_id: int = 1000,
        group_id: int = 1000,
        owner: str = "test",
        group: str = "test",
        permissions: int = 644,
        file_time: int = 0,
        links_count: int = 1,
    ):
        self.path = path
        self.data = data

        file_type_value = FILEXFER_TYPE_REGULAR
        if file_type == FILEXFER_TYPE_DIRECTORY:
            file_type_value = FILEXFER_TYPE_DIRECTORY

        self.file_type = file_type_value
        self.user_id = user_id
        self.group_id = group_id
        self.owner = owner
        self.group = group
        self.permissions = permissions
        self.size = len(data)

        if file_time is None:  
            file_time = datetime.datetime.now(
                datetime.timezone.utc,
            ).timestamp()

        file_time_ns = file_time * 10**9

        self.atime = file_time
        self.atime_ns = file_time_ns

        self.crtime = file_time
        self.crtime_ns = file_time_ns

        self.mtime = file_time
        self.mtime_ns = file_time_ns
        self.ctime = file_time
        self.ctime_ns = file_time_ns
        self.nlink = links_count

        self.attrs = SFTPAttrs(
            type=self.file_type,
            size=self.size,
            alloc_size=self.size,
            uid=self.user_id,
            gid=self.group_id,
            owner=self.owner,
            group=self.group,
            permissions=self.permissions,
            atime=self.atime,
            atime_ns=self.atime_ns,
            crtime=self.crtime,
            crtime_ns=self.crtime_ns,
            mtime=self.mtime,
            mtime_ns=self.mtime_ns,
            ctime=self.ctime,
            ctime_ns=self.ctime_ns,
            nlink=self.nlink,
        )

        self.sftp_named_file = SFTPName(
            pathlib.Path(str(path)).name.encode(),
            self.path,
            self.attrs,
        )

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
        """Copy files from one SCP server to another"""

        exc = await self._forward_response(self._dest, self._source)

        if exc:
            raise exc

        pathlist: list[bytes] = []
        attrlist: list[SFTPAttrs] = []
        attrs = SFTPAttrs()

        while True:
            action, args = await self._source.recv_request()

            if not action:
                break

            assert args is not None

            self._dest.send_request(action, args)

            exc: Exception | None = None
            if action in b'\x01\x02':
                exc = scp_error(SFTPFailure, args, fatal=action != b'\x01')
             
            if exc:
                raise exc

            exc = await self._forward_response(self._dest, self._source)

            if exc:
                raise exc

            if action in b'CD':
                try:
                    attrs.permissions, size, name = parse_cd_args(args)

                    if action == b'C':
                        await self._copy_file(size)
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

    async def receive(
        self,
        dstpath: str | pathlib.PurePath,
    ) -> None:

        if isinstance(dstpath, pathlib.PurePath):
            dstpath = str(dstpath)

        if isinstance(dstpath, str):
            dstpath = dstpath.encode('utf-8')

        await self._recv_files(dstpath)
    
    async def send(
        self,
        srcpath: bytes,
        data: bytes | File | list[bytes | File],
    ) -> None:

        exc = await self._dest.await_response()
        if exc:
            raise exc
        
        await self._send_files(
            srcpath,
            data,
        )
    
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

    async def _copy_file(self, size: int) -> None:
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
            raise exc

    async def _recv_file(
        self,
        dstpath: bytes,
        size: int,
    ) -> None:

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
        
        self._filesystem[dstpath] = FileResult(
            dstpath,
            result_data=buffer,
            result_type="FILE",
        )

    async def _recv_dir(self, dstpath: bytes) -> None:
        """Receive a directory over SCP"""

        if not self._recurse:
            raise scp_error(SFTPBadMessage,
                             'Directory received without recurse')

        if self._filesystem.get(dstpath) and pathlib.Path(str(dstpath)).suffix:
            raise scp_error(SFTPFailure, 'Not a directory', dstpath)
        
        else:
            self._filesystem[dstpath] = FileResult(
                dstpath,
                result_type="DIRECTORY",
            )

        await self._recv_files(dstpath)

    async def _recv_files(self, dstpath: bytes) -> None:
        """Receive files over SCP"""

        self._source.send_ok()

        attrs = SFTPAttrs()

        while True:
            action, args = await self._source.recv_request()
            if not action:
                break

            assert args is not None

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

                    if action == b'D':
                        await self._recv_dir(new_dstpath)
                    else:
                        await self._recv_file(new_dstpath, size)


                    if (
                        result := self._filesystem.get(new_dstpath)
                    ) and self._preserve:
                        result.attributes = FileAttributes(
                            size=attrs.size,
                            alloc_size=attrs.alloc_size,
                            atime=attrs.atime,
                            atime_ns=attrs.atime_ns,
                            crtime=attrs.crtime,
                            crtime_ns=attrs.crtime_ns,
                            mime_type=attrs.mime_type,
                            mtime=attrs.mtime,
                            mtime_ns=attrs.mtime_ns,
                            ctime=attrs.ctime,
                            ctime_ns=attrs.ctime_ns,
                            nlink=attrs.nlink,
                            
                        )


                finally:
                    attrs = SFTPAttrs()
            else:
                raise scp_error(SFTPBadMessage, 'Unknown request')

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
                posixpath.join(srcpath, file_item.file_name_short),
                file_item.data,
                file_item.attrs,
                
            ) for file_item in files.values()
        ])

        await self._dest.make_request(b'E')

    async def _send_files(
        self,
        srcpath: bytes,
        data: bytes | File | list[File | bytes],
    ) -> None:

        if isinstance(data, File) and data.file_type == FILEXFER_TYPE_DIRECTORY:
            await self._make_cd_request(
                b'D',
                data.attrs,
                0,
                srcpath,
            )
             
        elif isinstance(data, File):
             await self._send_file(
                srcpath,
                data.data,
                data.attrs,
            )

             
        elif isinstance(data, list):
            await asyncio.gather(*[
                self._send_file(
                    srcpath,
                    file.data,
                    file.attrs,
                ) if isinstance(
                    file,
                    File,
                ) else File(
                    srcpath,
                    file,
                ) for file in data
            ])

        else:
            file = File(
                srcpath,
                data,
            )

            await self._send_file(
                srcpath,
                file.data,
                file.attrs,
            )
